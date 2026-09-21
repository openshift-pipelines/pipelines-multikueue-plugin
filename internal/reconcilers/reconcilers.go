package reconcilers

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	clusterv1 "open-cluster-management.io/api/cluster/v1"
	workv1 "open-cluster-management.io/api/work/v1"
	msav1beta1 "open-cluster-management.io/managed-serviceaccount/apis/authentication/v1beta1"
	cpv1alpha1 "sigs.k8s.io/cluster-inventory-api/apis/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// +kubebuilder:rbac:groups="cluster.open-cluster-management.io",resources=managedclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups="authentication.open-cluster-management.io",resources=managedserviceaccounts,verbs=get;list;watch;create;update

type MultiKueueReconciler struct {
	client.Client
}

const (
	msaName = "pipelines-multikueue"
)

func (r *MultiKueueReconciler) SetupWithManager(mgr ctrl.Manager) error {
	utilruntime.Must(clusterv1.Install(mgr.GetScheme())) // Register for OCM
	utilruntime.Must(workv1.Install(mgr.GetScheme()))
	utilruntime.Must(cpv1alpha1.AddToScheme(mgr.GetScheme())) // Register for ClusterProfile
	utilruntime.Must(msav1beta1.AddToScheme(mgr.GetScheme())) // Register for ManagedServiceAccount

	return ctrl.NewControllerManagedBy(mgr).
		For(&clusterv1.ManagedCluster{}).
		Watches(
			&msav1beta1.ManagedServiceAccount{},
			handler.EnqueueRequestsFromMapFunc(r.managedServiceAccountToManagedCluster),
		).
		Watches(
			&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(r.secretToManagedCluster),
		).
		Complete(r)
}

func (r *MultiKueueReconciler) managedServiceAccountToManagedCluster(
	ctx context.Context,
	obj client.Object,
) []reconcile.Request {
	msa, ok := obj.(*msav1beta1.ManagedServiceAccount)
	if !ok {
		return nil
	}

	return []reconcile.Request{
		{
			NamespacedName: types.NamespacedName{
				Name: msa.Namespace,
			},
		},
	}
}

func (r *MultiKueueReconciler) secretToManagedCluster(
	ctx context.Context,
	obj client.Object,
) []reconcile.Request {

	secret, ok := obj.(*corev1.Secret)
	if !ok {
		return nil
	}

	for _, owner := range secret.OwnerReferences {
		if owner.APIVersion == msav1beta1.GroupVersion.String() &&
			owner.Kind == "ManagedServiceAccount" && owner.Name == "multikueue" {
			return []reconcile.Request{
				{
					NamespacedName: types.NamespacedName{
						Name: secret.Namespace,
					},
				},
			}
		}
	}

	return nil
}

func (r *MultiKueueReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	managedCluster := &clusterv1.ManagedCluster{}
	if err := r.Get(ctx, req.NamespacedName, managedCluster); err != nil {
		return ctrl.Result{}, err
	}
	// If managed cluster is local cluster then   return
	if label, ok := managedCluster.Labels["local-cluster"]; ok && label == "true" {
		logger.Info("Skipping Local Cluster", "Namespace", req.Namespace, "Name", req.Name)
		return ctrl.Result{}, nil
	}

	logger.Info("Reconciling MultiKueue Cluster", "Namespace", req.Namespace, "Name", req.Name)
	clusterName := req.Name

	err := r.ensureMSA(ctx, clusterName)
	if err != nil {
		return ctrl.Result{}, err
	}

	secret, err := r.getMSASecret(ctx, clusterName)
	if err != nil {
		return ctrl.Result{}, err
	}

	if secret == nil {
		// Wait until MSA controller creates the Secret.
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	kueueSecret, err := r.ensureKubeConfigSecret(ctx, managedCluster, secret)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Create Bootstrap Manifest on managedCluster.
	// ACM does not expose the kubeconfig directly so work with managedClusterClient we need to create a Bootstep Role
	//	and RoleBinding we can Perform other operations using managedClusterClient with ManagedServiceAccount
	if err := r.ensureBootstrapManifestWork(ctx, managedCluster.Name); err != nil {
		return ctrl.Result{}, err
	}

	// Ensure all the required operators are installed on managed Cluster
	if err := r.ensureBootstrapOperators(ctx, managedCluster, secret); err != nil {
		return ctrl.Result{}, err
	}

	// secret.Name is the Secret to reference in MultiKueueCluster
	return ctrl.Result{}, r.ensureMultiKueueCluster(ctx, clusterName, kueueSecret)

	//return ctrl.Result{}, nil

}

func (r *MultiKueueReconciler) ensureMSA(ctx context.Context, clusterName string) error {
	logger := log.FromContext(ctx)
	logger.Info("Ensuring MSA for cluster", "Name", clusterName)
	msa := &msav1beta1.ManagedServiceAccount{}

	key := types.NamespacedName{
		Namespace: clusterName,
		Name:      msaName,
	}

	err := r.Get(ctx, key, msa)
	if apierrors.IsNotFound(err) {
		msa = &msav1beta1.ManagedServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: clusterName,
				Name:      msaName,
			},
		}

		_, err = controllerutil.CreateOrUpdate(ctx, r.Client, msa, func() error {
			msa.Spec.TTLSecondsAfterCreation = ptr.To(int32(86400))
			msa.Spec.Rotation = msav1beta1.ManagedServiceAccountRotation{
				Validity: metav1.Duration{
					Duration: time.Hour * 24,
				},
			}
			return nil
		})

		return err
	}
	return err
}

func (r *MultiKueueReconciler) getManagedClusterClient(cluster *clusterv1.ManagedCluster, secret *corev1.Secret) (client.Client, error) {
	serverURL := cluster.Spec.ManagedClusterClientConfigs[0].URL
	token := secret.Data["token"]
	ca := secret.Data["ca.crt"]
	cfg := &rest.Config{
		Host:        serverURL,
		BearerToken: string(token),
		TLSClientConfig: rest.TLSClientConfig{
			CAData: ca,
		},
	}
	spokeClient, err := client.New(cfg, client.Options{Scheme: r.Scheme()})
	if err != nil {
		return spokeClient, err
	}
	return spokeClient, nil
}

func (r *MultiKueueReconciler) getMSASecret(ctx context.Context, clusterName string) (*corev1.Secret, error) {
	logger := log.FromContext(ctx)
	logger.Info("Getting MSA Secret for cluster", "Name", clusterName)

	msa := &msav1beta1.ManagedServiceAccount{}
	err := r.Get(ctx, types.NamespacedName{
		Namespace: clusterName,
		Name:      msaName,
	}, msa)
	if err != nil {
		return nil, err
	}

	if msa.Status.TokenSecretRef == nil {
		return nil, nil
	}

	secret := &corev1.Secret{}
	err = r.Get(ctx, types.NamespacedName{
		Namespace: clusterName,
		Name:      msa.Status.TokenSecretRef.Name,
	}, secret)

	if apierrors.IsNotFound(err) {
		return nil, nil
	}

	return secret, err
}

func (r *MultiKueueReconciler) ensureBootstrapOperators(ctx context.Context, cluster *clusterv1.ManagedCluster, secret *corev1.Secret) error {
	logger := log.FromContext(ctx)
	spokeClient, err := r.newSpokeClient(cluster, secret)
	if err != nil {
		return err
	}

	bootstrap := &ClusterBootstrap{
		Client: spokeClient,
	}
	logger.Info("Installing Operators on Managed Cluster at Start", "Name", cluster.Name)

	return bootstrap.bootstrap(ctx)

}

func (r *MultiKueueReconciler) newSpokeClient(cluster *clusterv1.ManagedCluster, secret *corev1.Secret) (client.Client, error) {
	if len(cluster.Spec.ManagedClusterClientConfigs) == 0 {
		return nil, fmt.Errorf("managed cluster %q has no client config", cluster.Name)
	}

	cfg := &rest.Config{
		Host:        cluster.Spec.ManagedClusterClientConfigs[0].URL,
		BearerToken: string(secret.Data["token"]),
		TLSClientConfig: rest.TLSClientConfig{
			CAData: secret.Data["ca.crt"],
		},
	}

	return client.New(cfg, client.Options{
		Scheme: r.Scheme(),
	})
}
