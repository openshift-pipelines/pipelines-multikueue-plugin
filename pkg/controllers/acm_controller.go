package controllers

import (
	"context"
	"time"

	. "github.com/openshift-pipelines/pipelines-multikueue-plugin/pkg/common"
	"github.com/openshift-pipelines/pipelines-multikueue-plugin/pkg/reconcilers"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"

	// Native OCM Clients & Informers
	ocmclient "open-cluster-management.io/api/client/cluster/clientset/versioned"
	ocminformers "open-cluster-management.io/api/client/cluster/informers/externalversions"
	ocmclusterv1 "open-cluster-management.io/api/cluster/v1"

	msaclientset "open-cluster-management.io/managed-serviceaccount/pkg/generated/clientset/versioned"
)

const ACMControllerName = "acm-fleet-manager-controller"

// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups="coordination.k8s.io",resources=leases,verbs=get;list;watch;create;update;delete

func NewController() func(context.Context, configmap.Watcher) *controller.Impl {
	return func(ctx context.Context, cmw configmap.Watcher) *controller.Impl {
		logger := logging.FromContext(ctx)
		hubKubeClient, cfg, err := GetKubeClientAndConfig()
		if err != nil {
			logger.Fatalf("Failed to create Kubernetes client: %v", err)
		}

		ocmClient, err := ocmclient.NewForConfig(cfg)
		if err != nil {
			logger.Fatalf("Failed to create ocm client: %v", err)
		}

		msaClient, err := msaclientset.NewForConfig(cfg)
		if err != nil {
			logger.Fatalf("Failed to create ocm MSA client: %v", err)
		}

		factory := ocminformers.NewSharedInformerFactory(ocmClient, time.Minute*10)
		clusterInformer := factory.Cluster().V1().ManagedClusters().Informer()
		clusterLister := factory.Cluster().V1().ManagedClusters().Lister()

		r := &reconcilers.ACMMultiKueueReconciler{
			Logger:        logger,
			HubKubeClient: hubKubeClient,
			OcmClient:     ocmClient,
			MsaClient:     msaClient,
			ClusterLister: clusterLister,
		}

		impl := controller.NewContext(ctx, r, controller.ControllerOptions{
			Logger:        logger,
			WorkQueueName: ACMControllerName,
		})

		// 4. Attach handlers
		_, err = clusterInformer.AddEventHandler(controller.HandleAll(func(obj interface{}) {
			if cluster, ok := obj.(*ocmclusterv1.ManagedCluster); ok {
				impl.EnqueueKey(types.NamespacedName{Name: cluster.Name})
			}
		}))
		if err != nil {
			return nil
		}

		// 5. Start informer factory with context done signal
		go factory.Start(ctx.Done())

		return impl
	}
}
