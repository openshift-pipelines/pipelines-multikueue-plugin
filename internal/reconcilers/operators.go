package reconcilers

import (
	"context"
	"fmt"
	"time"

	operatorsv1 "github.com/operator-framework/api/pkg/operators/v1"
	olm "github.com/operator-framework/api/pkg/operators/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type ClusterBootstrap struct {
	client.Client
	ClusterRole string
}

func (b *ClusterBootstrap) Start(ctx context.Context) error {
	if err := b.bootstrap(ctx); err != nil {
		return err
	}
	if err := b.ensureResourceFlavour(ctx); err != nil {
		return err
	}
	if err := b.ensureClusterQueue(ctx); err != nil {
		return err
	}
	if err := b.ensureLocalQueue(ctx, "default"); err != nil {
		return err
	}
	if err := b.ensureAdmissionCheck(ctx); err != nil {
		return err
	}

	return nil
}

func (b *ClusterBootstrap) bootstrap(ctx context.Context) error {
	utilruntime.Must(olm.AddToScheme(b.Scheme()))
	utilruntime.Must(operatorsv1.AddToScheme(b.Scheme()))

	logger := log.FromContext(ctx)

	logger.Info("Installing Operators on Cluster")
	if err := b.ensureOperators(ctx); err != nil {
		return err
	} else {
		logger.Info("All Operators reconciled successfully")
	}

	// Ensure Kueue Resource
	if err := b.ensureKueue(ctx); err != nil {
		return err
	} else {
		logger.Info("Kueue Resource reconciled")
	}

	if b.ClusterRole == ClusterRoleSpoke {
		return b.enableKueueInTektonConfig(ctx)
	}

	return nil
}

// +kubebuilder:rbac:groups="",resources=namespaces;secrets,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups="operators.coreos.com",resources=subscriptions;operatorgroups,verbs=get;list;watch;create;update

func (r *ClusterBootstrap) ensureOperators(ctx context.Context) error {
	logger := log.FromContext(ctx)

	logger.Info("Installing Operators")

	// Ensure Pipelines Operator
	if err := r.ensureOperator(ctx, "openshift-pipelines-operator-rh", "latest", "openshift-operators"); err != nil {
		return err
	}

	// Ensure Kueue Operator
	if err := r.ensureOperator(ctx, "kueue-operator", "stable-v1.4", "openshift-kueue-operator"); err != nil {
		return err
	}

	// Ensure Cert-Manager Operator
	if err := r.ensureOperator(ctx, "openshift-cert-manager-operator", "stable-v1", "cert-manager-operator"); err != nil {
		return err
	}

	return nil
}

// EnsureOperators checks the state of the Subscription instantly.
// If it is not fully installed, it returns an error to trigger a controller requeue.
func (c *ClusterBootstrap) ensureOperator(ctx context.Context, packageName, channel, subNamespace string) error {
	logger := log.FromContext(ctx)
	found := false

	//	Ensure OperatorGroup
	if err := c.ensureOperatorGroup(ctx, subNamespace); err != nil {
		return err
	}

	// 1. Use the strongly typed SubscriptionList
	subsList := &olm.SubscriptionList{}
	subscription := &olm.Subscription{
		ObjectMeta: metav1.ObjectMeta{
			Name:      packageName,
			Namespace: subNamespace,
		},
		Spec: &olm.SubscriptionSpec{
			Channel:                channel,
			Package:                packageName,
			CatalogSource:          "redhat-operators",
			CatalogSourceNamespace: "openshift-marketplace",
		},
	}

	// 2. List all subscriptions cluster-wide
	if err := c.List(ctx, subsList); err != nil {
		return fmt.Errorf("failed to list subscriptions: %v", err)
	}

	// 3. Search for the pipeline operator natively
	for _, sub := range subsList.Items {
		// In the Go struct, the YAML 'name' field is represented as 'Package'
		if sub.Spec.Package == packageName {
			subscription.Name = sub.Name
			subscription.Namespace = sub.Namespace
			found = true
			break
		}
	}

	if !found {
		logger.Info("Subscription not found. Creating it.", "packageName", packageName)
		// Ensure Namespace
		if err := c.EnsureNamespace(ctx, subscription.Namespace); err != nil {
			return err
		}
		err := c.Create(ctx, subscription)
		if err != nil && !errors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create subscription: %v", err)
		}
	} else {
		logger.Info("Subscription found", "Package", subscription.Spec.Package, "subscription", subscription.Name, "Namespace", subscription.Namespace)
	}
	logger.Info("Waiting for operator to become ready...")

	// 5. Block and poll for readiness using the strongly typed object
	return wait.PollUntilContextTimeout(ctx, 5*time.Second, 30*time.Minute, true, func(ctx context.Context) (bool, error) {
		sub := &olm.Subscription{}

		err := c.Get(ctx, client.ObjectKey{Name: subscription.Name, Namespace: subscription.Namespace}, sub)
		if errors.IsNotFound(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}

		// Directly access the Status field instead of parsing unstructured maps
		if sub.Status.InstalledCSV == "" {
			logger.Info("Waiting for CSV to be installed, %v", "subscription", subscription.Name, "Status", sub.Status)
			return false, nil
		}

		logger.Info("Subscription is ready", "Namespace", subscription.Namespace, "subscription", subscription.Name, "InstalledCSV", sub.Status.InstalledCSV)
		return true, nil
	})
}

func (c *ClusterBootstrap) ensureOperatorGroup(ctx context.Context, namespace string) error {

	operatorGroupList := &operatorsv1.OperatorGroupList{}
	if err := c.List(ctx, operatorGroupList, client.InNamespace(namespace)); err != nil {
		return err
	}
	if len(operatorGroupList.Items) == 0 {
		if err := c.EnsureNamespace(ctx, namespace); err != nil {
			return err
		}
		operatorGroup := &operatorsv1.OperatorGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      namespace,
				Namespace: namespace,
			},
		}
		return c.Create(ctx, operatorGroup)
	}
	return nil
}

func (c *ClusterBootstrap) EnsureNamespace(ctx context.Context, namespaceName string) error {
	ns := &corev1.Namespace{}

	// 1. Try to get the namespace
	err := c.Get(ctx, client.ObjectKey{Name: namespaceName}, ns)
	if err == nil {
		klog.V(4).Infof("Namespace %s already exists.", namespaceName)
		return nil
	}

	// 2. If the error is anything OTHER than "Not Found", return the error
	if !errors.IsNotFound(err) {
		return fmt.Errorf("failed to get namespace %s: %v", namespaceName, err)
	}

	// 3. If we reach here, it means the namespace is Not Found. Let's create it.
	klog.Infof("Namespace %s not found. Creating it...", namespaceName)
	newNs := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespaceName,
		},
	}

	err = c.Create(ctx, newNs)
	// We also check IsAlreadyExists just in case another process created it in the few milliseconds since our Get call
	if err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create namespace %s: %v", namespaceName, err)
	}

	klog.Infof("Successfully created namespace %s.", namespaceName)
	return nil
}
