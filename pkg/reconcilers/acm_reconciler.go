package reconcilers

import (
	"context"
	"time"

	"go.uber.org/zap"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/reconciler"
	ocmclient "open-cluster-management.io/api/client/cluster/clientset/versioned"
	"open-cluster-management.io/api/client/cluster/listers/cluster/v1"
	msav1beta1 "open-cluster-management.io/managed-serviceaccount/apis/authentication/v1beta1"
	msaclientset "open-cluster-management.io/managed-serviceaccount/pkg/generated/clientset/versioned"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var (
	_ reconciler.LeaderAware = (*ACMMultiKueueReconciler)(nil)
)

// +kubebuilder:rbac:groups="cluster.open-cluster-management.io",resources=managedclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups="authentication.open-cluster-management.io",resources=managedserviceaccounts,verbs=get;list;watch;create;update
// +genclient
// +genreconciler

type ACMMultiKueueReconciler struct {
	Logger        *zap.SugaredLogger
	HubKubeClient kubernetes.Interface
	OcmClient     ocmclient.Interface
	MsaClient     msaclientset.Interface
	ClusterLister v1.ManagedClusterLister
}

const (
	msaName = "pipelines-multikueue"
)

func (r *ACMMultiKueueReconciler) Reconcile(ctx context.Context, clusterName string) error {
	logger := logging.FromContext(ctx)

	managedCluster, err := r.ClusterLister.Get(clusterName)
	if err != nil {
		return err
	}

	// If managed cluster is local cluster then   return
	if label, ok := managedCluster.Labels["local-cluster"]; ok && label == "true" {
		logger.Info("Skipping Local Cluster", "Namespace", managedCluster.Namespace, "Name", managedCluster.Name)
		return nil
	}
	logger.Infof("Reconcile ACMMultiKueue, Cluster: %s", managedCluster.Name)

	if err := r.ensureMSA(ctx, clusterName); err != nil {
		return err
	}

	return nil

}

// Promote implements reconciler.LeaderAware
func (r *ACMMultiKueueReconciler) Promote(b reconciler.Bucket, enq func(reconciler.Bucket, types.NamespacedName)) error {
	// Return nil to indicate we don't need special promotion logic
	return nil
}

// Demote implements reconciler.LeaderAware
func (r *ACMMultiKueueReconciler) Demote(b reconciler.Bucket) {
	// Nothing to do on demotion
}

// MUST implement Reconcile with exact signature: (context.Context, string) error

func (r *ACMMultiKueueReconciler) ensureMSA(ctx context.Context, clusterName string) error {
	logger := log.FromContext(ctx)
	logger.Info("Ensuring MSA for cluster", "Name", clusterName)
	msa := &msav1beta1.ManagedServiceAccount{}

	msa, err := r.MsaClient.AuthenticationV1beta1().ManagedServiceAccounts(clusterName).Get(ctx, msaName, metav1.GetOptions{})

	if err != nil && apierrors.IsNotFound(err) {
		msa = &msav1beta1.ManagedServiceAccount{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: clusterName,
				Name:      msaName,
			},
			Spec: msav1beta1.ManagedServiceAccountSpec{
				TTLSecondsAfterCreation: ptr.To(int32(86400)),
				Rotation: msav1beta1.ManagedServiceAccountRotation{
					Validity: metav1.Duration{
						Duration: time.Hour * 24,
					},
				},
			},
		}
		msa, err = r.MsaClient.AuthenticationV1beta1().ManagedServiceAccounts(clusterName).Create(ctx, msa, metav1.CreateOptions{})
		if err != nil {
			return err
		}
	}
	return err
}
