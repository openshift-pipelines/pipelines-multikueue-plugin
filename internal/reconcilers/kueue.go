package reconcilers

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/openshift-pipelines/pipelines-multikueue-plugin/internal/common"
	v1 "github.com/openshift/api/operator/v1"
	kueuev1 "github.com/openshift/kueue-operator/pkg/apis/kueueoperator/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/klog/v2"
	clusterv1 "open-cluster-management.io/api/cluster/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"
)

const (
	MultiKueueConfigName      = "pipelines-multikueue-config"
	DefaultAdmissionCheckName = "pipelines-multikueue-ac"
	DefaultMultiClusterQueue  = "pipelines-multicluster-kueue"
	DefaultLocalQueue         = "pipelines-kueue"
	DefaultResourceFlavor     = "default-flavor"
	PipelineResourceName      = "tekton.dev/pipelineruns"
)

// +kubebuilder:rbac:groups="kueue.openshift.io",resources=kueues,verbs=get;list;create;patch;update;watch
// +kubebuilder:rbac:groups="kueue.x-k8s.io",resources=multikueueclusters;multikueueconfigs;admissionchecks,verbs=get;list;create;patch;update;watch
// +kubebuilder:rbac:groups="kueue.x-k8s.io",resources=resourceflavors;clusterqueues;localqueues,verbs=get;list;create;patch;update;watch

func (r *ClusterBootstrap) ensureKueue(ctx context.Context) error {
	utilruntime.Must(kueuev1.AddToScheme(r.Scheme()))
	utilruntime.Must(kueuev1beta2.AddToScheme(r.Scheme()))
	kueueName := "cluster"

	// Declare and initialize the variable using Kueue's native structs
	tektonFramework := kueuev1.ExternalFramework{
		Group:    "tekton.dev",
		Resource: "pipelineruns",
		Version:  "v1",
	}

	// Get Kueue

	kueue := &kueuev1.Kueue{}
	key := types.NamespacedName{Name: kueueName}

	// 1. Try to get the Kueue
	err := r.Get(ctx, key, kueue)
	if err == nil {
		klog.V(4).Infof("Namespace %s already exists.", kueueName)
		patch := client.MergeFrom(kueue.DeepCopy())
		//If KueueCR  Allready Exists then Patch.
		kueue.Spec.Config.Integrations.ExternalFrameworks = ensureExternalFramework(kueue.Spec.Config.Integrations.ExternalFrameworks, tektonFramework)

		if kueue.Spec.Config.MultiKueue == nil {
			kueue.Spec.Config.MultiKueue = &kueuev1.MultiKueue{}
		}
		kueue.Spec.Config.MultiKueue.ExternalFrameworks = ensureExternalFramework(kueue.Spec.Config.Integrations.ExternalFrameworks, tektonFramework)

		return r.Patch(ctx, kueue, patch)
	}

	// Create Kueue Object
	kueue = &kueuev1.Kueue{
		ObjectMeta: metav1.ObjectMeta{
			Name: kueueName,
		},
		Spec: kueuev1.KueueOperandSpec{
			Config: kueuev1.KueueConfiguration{
				Integrations: kueuev1.Integrations{
					Frameworks:         []kueuev1.KueueIntegration{kueuev1.KueueIntegrationBatchJob},
					ExternalFrameworks: []kueuev1.ExternalFramework{tektonFramework},
				},
				MultiKueue: &kueuev1.MultiKueue{
					ExternalFrameworks: []kueuev1.ExternalFramework{tektonFramework},
				},
			},
			OperatorSpec: v1.OperatorSpec{
				LogLevel:         "Normal",
				ManagementState:  v1.Unmanaged,
				OperatorLogLevel: "Normal",
			},
		},
	}
	if err := r.Create(ctx, kueue); err != nil {
		return err
	}

	return nil
}

func ensureExternalFramework(frameworks []kueuev1.ExternalFramework, framework kueuev1.ExternalFramework) []kueuev1.ExternalFramework {
	updatedFrameworks := frameworks
	if !slices.ContainsFunc(frameworks, func(f kueuev1.ExternalFramework) bool {
		return f.Group == framework.Group &&
			f.Version == framework.Version &&
			f.Resource == framework.Resource
	}) {
		updatedFrameworks = append(frameworks, framework)
	}
	return updatedFrameworks
}

func (r *ClusterBootstrap) ensureResourceFlavour(ctx context.Context) error {
	rf := &kueuev1beta2.ResourceFlavor{
		ObjectMeta: metav1.ObjectMeta{
			Name: DefaultResourceFlavor,
		},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, rf, func() error {
		return nil
	})
	return err

}

func (r *ClusterBootstrap) ensureLocalQueue(ctx context.Context, namespace string) error {
	logger := log.FromContext(ctx).WithName("ensureLocalQueue")
	lq := &kueuev1beta2.LocalQueue{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DefaultLocalQueue,
			Namespace: namespace,
		},
	}
	logger.Info("Creating local queue")
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, lq, func() error {
		lq.Spec.ClusterQueue = DefaultMultiClusterQueue
		return nil
	})
	return err

}

func (r *ClusterBootstrap) ensureClusterQueue(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("ensureClusterQueue")
	cq := &kueuev1beta2.ClusterQueue{
		ObjectMeta: metav1.ObjectMeta{
			Name: DefaultMultiClusterQueue,
		},
	}
	logger.Info("Creating ClusterQueue", "ClusterQueue", cq.Name)
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, cq, func() error {

		// ensure NameSpaceSelector
		if cq.Spec.NamespaceSelector == nil {
			logger.Info("Create AdmissionChecksStrategy", "ClusterQueue", cq.Name)
			cq.Spec.NamespaceSelector = &metav1.LabelSelector{}
		}
		
		// ensurePipelineRunResourceGroup
		ensurePipelineRunResourceGroup(cq, DefaultResourceFlavor, resource.MustParse("100"))

		// ensure Admission Check is added to multi-cluste-queue
		if cq.Spec.AdmissionChecksStrategy == nil {
			logger.Info("Create AdmissionChecksStrategy", "ClusterQueue", cq.Name)
			cq.Spec.AdmissionChecksStrategy = &kueuev1beta2.AdmissionChecksStrategy{}
		}
		if !slices.ContainsFunc(
			cq.Spec.AdmissionChecksStrategy.AdmissionChecks,
			func(ac kueuev1beta2.AdmissionCheckStrategyRule) bool {
				return ac.Name == DefaultAdmissionCheckName
			},
		) {
			cq.Spec.AdmissionChecksStrategy.AdmissionChecks = append(
				cq.Spec.AdmissionChecksStrategy.AdmissionChecks,
				kueuev1beta2.AdmissionCheckStrategyRule{
					Name: DefaultAdmissionCheckName,
				},
			)
		}
		return nil
	})
	return err
}

func ensurePipelineRunResourceGroup(
	cq *kueuev1beta2.ClusterQueue,
	flavor kueuev1beta2.ResourceFlavorReference,
	quota resource.Quantity,
) {

	// Find an existing ResourceGroup.
	for i := range cq.Spec.ResourceGroups {
		rg := &cq.Spec.ResourceGroups[i]

		if !slices.Contains(rg.CoveredResources, PipelineResourceName) {
			continue
		}

		// Found it. Ensure the flavor exists.
		for j := range rg.Flavors {
			fq := &rg.Flavors[j]
			if fq.Name != flavor {
				continue
			}

			// Ensure the resource quota exists.
			for k := range fq.Resources {
				if fq.Resources[k].Name == PipelineResourceName {
					fq.Resources[k].NominalQuota = quota
					return
				}
			}

			// Resource quota missing.
			fq.Resources = append(fq.Resources, kueuev1beta2.ResourceQuota{
				Name:         PipelineResourceName,
				NominalQuota: quota,
			})
			return
		}

		// Flavor missing.
		rg.Flavors = append(rg.Flavors, kueuev1beta2.FlavorQuotas{
			Name: flavor,
			Resources: []kueuev1beta2.ResourceQuota{
				{
					Name:         PipelineResourceName,
					NominalQuota: quota,
				},
			},
		})
		return
	}

	// ResourceGroup doesn't exist.
	cq.Spec.ResourceGroups = append(cq.Spec.ResourceGroups, kueuev1beta2.ResourceGroup{
		CoveredResources: []corev1.ResourceName{
			PipelineResourceName,
		},
		Flavors: []kueuev1beta2.FlavorQuotas{
			{
				Name: flavor,
				Resources: []kueuev1beta2.ResourceQuota{
					{
						Name:         PipelineResourceName,
						NominalQuota: quota,
					},
				},
			},
		},
	})
}

func (r *ClusterBootstrap) ensureAdmissionCheck(ctx context.Context) error {
	ac := &kueuev1beta2.AdmissionCheck{
		ObjectMeta: metav1.ObjectMeta{
			Name: DefaultAdmissionCheckName,
		},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, ac, func() error {
		ac.Spec.ControllerName = "kueue.x-k8s.io/multikueue"
		params := &kueuev1beta2.AdmissionCheckParametersReference{
			Name:     MultiKueueConfigName,
			Kind:     "MultiKueueConfig",
			APIGroup: "kueue.x-k8s.io",
		}
		ac.Spec.Parameters = params
		return nil
	})
	return err
}

func (r *MultiKueueReconciler) ensureMultiKueueCluster(ctx context.Context, clusterName string, secret *corev1.Secret) error {
	mkc := &kueuev1beta2.MultiKueueCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: clusterName,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, mkc, func() error {
		mkc.Spec.ClusterSource = kueuev1beta2.ClusterSource{
			KubeConfig: &kueuev1beta2.KubeConfig{
				LocationType: kueuev1beta2.SecretLocationType,
				Location:     secret.Name,
			},
		}
		return nil
	})
	if err != nil {
		return err
	}

	mkConfig := &kueuev1beta2.MultiKueueConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name: MultiKueueConfigName,
		},
	}

	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, mkConfig, func() error {
		if !slices.Contains(mkConfig.Spec.Clusters, clusterName) {
			mkConfig.Spec.Clusters = append(mkConfig.Spec.Clusters, clusterName)
		}
		return nil
	})
	return err
}

func (r *MultiKueueReconciler) ensureKubeConfigSecret(
	ctx context.Context,
	cluster *clusterv1.ManagedCluster,
	source *corev1.Secret,
) (*corev1.Secret, error) {

	logger := log.FromContext(ctx)
	logger.Info("reconciling Multikueue Secret for cluster", "Name", cluster.Name)

	token := string(source.Data["token"])
	ca := source.Data["ca.crt"]

	if len(cluster.Spec.ManagedClusterClientConfigs) == 0 {
		return nil, fmt.Errorf("managed cluster %q has no client configs", cluster.Name)
	}

	server := cluster.Spec.ManagedClusterClientConfigs[0].URL

	cfg := clientcmdapi.NewConfig()

	cfg.Clusters[cluster.Name] = &clientcmdapi.Cluster{
		Server:                   server,
		CertificateAuthorityData: ca,
	}

	cfg.AuthInfos[cluster.Name] = &clientcmdapi.AuthInfo{
		Token: token,
	}

	cfg.Contexts[cluster.Name] = &clientcmdapi.Context{
		Cluster:  cluster.Name,
		AuthInfo: cluster.Name,
	}

	cfg.CurrentContext = cluster.Name

	kubeconfig, err := clientcmd.Write(*cfg)
	if err != nil {
		return nil, err
	}

	target := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: common.KueueNamespace,
			Name:      cluster.Name,
		},
	}

	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, target, func() error {
		target.Type = corev1.SecretTypeOpaque

		if target.Data == nil {
			target.Data = map[string][]byte{}
		}
		logger.Info("reconciling Multikueue Secret for cluster", "Name", cluster.Name)

		target.Data["kubeconfig"] = kubeconfig

		if target.Labels == nil {
			target.Labels = map[string]string{}
		}
		if target.Annotations == nil {
			target.Annotations = map[string]string{}
		}

		target.Labels["multikueue.kueue.x-k8s.io/managed-cluster"] = "cluster-" + cluster.Name
		target.Annotations["multikueue.kueue.x-k8s.io/updated-at"] = time.Now().Format(time.RFC3339)

		return nil
	})

	return target, err
}
