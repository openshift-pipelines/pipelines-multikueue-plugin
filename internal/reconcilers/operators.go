package reconcilers

import (
	"context"
	"fmt"
	"time"

	"github.com/openshift-pipelines/pipelines-multikueue-plugin/internal/common"
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

// operator describes an OLM operator the addon installs, and the channel it
// should be subscribed to.
type operator struct {
	packageName string
	channel     string
	namespace   string
}

// defaultOperators returns the operators the addon installs on the hub and on
// every managed cluster, with the channel and namespace each one should be
// subscribed in.
//
// Both come from the controller's environment, so a cluster can be moved onto
// a different operator release by editing the Deployment rather than
// rebuilding the image. Pointing a channel at a newer one upgrades that
// operator on the next reconcile, on every cluster where the addon installed
// it.
func defaultOperators() []operator {
	return []operator{
		{
			packageName: common.PipelinesOperatorPackage,
			channel:     common.GetPipelinesOperatorChannel(),
			namespace:   common.GetPipelinesOperatorNamespace(),
		},
		{
			packageName: common.KueueOperatorPackage,
			channel:     common.GetKueueOperatorChannel(),
			namespace:   common.GetKueueOperatorNamespace(),
		},
		{
			packageName: common.CertManagerOperatorPackage,
			channel:     common.GetCertManagerOperatorChannel(),
			namespace:   common.GetCertManagerOperatorNamespace(),
		},
	}
}

// +kubebuilder:rbac:groups="",resources=namespaces;secrets,verbs=get;list;watch;create;update
// +kubebuilder:rbac:groups="operators.coreos.com",resources=subscriptions;operatorgroups,verbs=get;list;watch;create;update;patch

func (r *ClusterBootstrap) ensureOperators(ctx context.Context) error {
	logger := log.FromContext(ctx)

	logger.Info("Installing Operators")

	for _, op := range defaultOperators() {
		if err := r.ensureOperator(ctx, op); err != nil {
			return err
		}
	}

	return nil
}

// ensureOperator subscribes the cluster to op's package on op's channel,
// reconciling the channel of an addon-installed Subscription that sits
// elsewhere, and then checks the state of the Subscription. If it is not fully
// installed, it returns an error to trigger a controller requeue.
func (c *ClusterBootstrap) ensureOperator(ctx context.Context, op operator) error {
	logger := log.FromContext(ctx)

	//	Ensure OperatorGroup
	if err := c.ensureOperatorGroup(ctx, op.namespace); err != nil {
		return err
	}

	existing, err := c.findSubscription(ctx, op.packageName)
	if err != nil {
		return err
	}

	key := client.ObjectKey{Name: op.packageName, Namespace: op.namespace}
	var previousCurrentCSV *string

	if existing == nil {
		logger.Info("Subscription not found. Creating it.", "packageName", op.packageName)
		// Ensure Namespace
		if err := c.EnsureNamespace(ctx, op.namespace); err != nil {
			return err
		}
		subscription := &olm.Subscription{
			ObjectMeta: metav1.ObjectMeta{
				Name:      op.packageName,
				Namespace: op.namespace,
			},
			Spec: &olm.SubscriptionSpec{
				Channel:                op.channel,
				Package:                op.packageName,
				CatalogSource:          "redhat-operators",
				CatalogSourceNamespace: "openshift-marketplace",
			},
		}
		// Stamp ownership so later reconciles know this operator is the
		// addon's to upgrade.
		setAddonOwnership(subscription)

		if err := c.Create(ctx, subscription); err != nil && !errors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create subscription: %v", err)
		}
	} else {
		key = client.ObjectKeyFromObject(existing)
		logger.Info("Subscription found", "Package", op.packageName, "subscription", existing.Name, "Namespace", existing.Namespace)

		previousChannel := ""
		if existing.Spec != nil {
			previousChannel = existing.Spec.Channel
		}
		currentCSV := existing.Status.CurrentCSV
		if err := c.upgradeSubscription(ctx, existing, op); err != nil {
			return err
		}
		if existing.Spec != nil && existing.Spec.Channel != previousChannel {
			previousCurrentCSV = &currentCSV
		}
	}

	logger.Info("Waiting for operator to become ready...")

	return c.waitForSubscription(ctx, key, previousCurrentCSV)
}

// findSubscription returns the Subscription for packageName wherever it lives
// on the cluster, or nil when the package is not subscribed to at all.
func (c *ClusterBootstrap) findSubscription(ctx context.Context, packageName string) (*olm.Subscription, error) {
	subsList := &olm.SubscriptionList{}
	if err := c.List(ctx, subsList); err != nil {
		return nil, fmt.Errorf("failed to list subscriptions: %v", err)
	}

	for i := range subsList.Items {
		sub := &subsList.Items[i]
		// In the Go struct, the YAML 'name' field is represented as 'Package'
		if sub.Spec != nil && sub.Spec.Package == packageName {
			return sub, nil
		}
	}

	return nil, nil
}

// upgradeSubscription moves an addon-installed Subscription onto op's channel,
// which is how OLM is asked to change the operator's version.
//
// A Subscription the addon did not install is always left untouched: that
// operator belongs to whoever put it there, and its lifecycle is theirs to
// manage.
//
// The configured channel is otherwise applied as written, the one exception
// being a rollback within a release series. See shouldMoveChannel.
func (c *ClusterBootstrap) upgradeSubscription(ctx context.Context, sub *olm.Subscription, op operator) error {
	logger := log.FromContext(ctx)

	if sub.Spec == nil || sub.Spec.Channel == op.channel {
		return nil
	}

	if !isAddonOwned(sub) {
		logger.Info("Subscription was not installed by the addon, leaving its channel alone",
			"subscription", sub.Name, "Namespace", sub.Namespace, "channel", sub.Spec.Channel)
		return nil
	}

	if !shouldMoveChannel(sub.Spec.Channel, op.channel) {
		logger.Info("Configured channel would roll the operator back, leaving it alone",
			"subscription", sub.Name, "Namespace", sub.Namespace,
			"installedChannel", sub.Spec.Channel, "configuredChannel", op.channel)
		return nil
	}

	logger.Info("Moving operator onto the configured channel",
		"subscription", sub.Name, "Namespace", sub.Namespace,
		"from", sub.Spec.Channel, "to", op.channel)

	patch := client.MergeFrom(sub.DeepCopy())
	sub.Spec.Channel = op.channel
	if err := c.Patch(ctx, sub, patch); err != nil {
		return fmt.Errorf("failed to move subscription %s/%s to channel %s: %v", sub.Namespace, sub.Name, op.channel, err)
	}

	return nil
}

// waitForSubscription blocks until OLM reports the Subscription as installed.
// After a channel change, OLM must first report a new CurrentCSV before matching
// CurrentCSV and InstalledCSV marks the upgrade as finished.
func (c *ClusterBootstrap) waitForSubscription(ctx context.Context, key client.ObjectKey, previousCurrentCSV *string) error {
	logger := log.FromContext(ctx)

	return wait.PollUntilContextTimeout(ctx, 5*time.Second, 30*time.Minute, true, func(ctx context.Context) (bool, error) {
		sub := &olm.Subscription{}

		err := c.Get(ctx, key, sub)
		if errors.IsNotFound(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}

		// Directly access the Status field instead of parsing unstructured maps
		if !subscriptionReady(sub.Status, previousCurrentCSV) {
			switch {
			case sub.Status.InstalledCSV == "":
				logger.Info("Waiting for CSV to be installed", "subscription", key.Name, "Status", sub.Status)
			case sub.Status.CurrentCSV != "" && sub.Status.CurrentCSV != sub.Status.InstalledCSV:
				logger.Info("Waiting for operator upgrade to complete", "subscription", key.Name,
					"InstalledCSV", sub.Status.InstalledCSV, "CurrentCSV", sub.Status.CurrentCSV)
			default:
				logger.Info("Waiting for OLM to process the channel change", "subscription", key.Name,
					"CurrentCSV", sub.Status.CurrentCSV)
			}
			return false, nil
		}

		logger.Info("Subscription is ready", "Namespace", key.Namespace, "subscription", key.Name, "InstalledCSV", sub.Status.InstalledCSV)
		return true, nil
	})
}

func subscriptionReady(status olm.SubscriptionStatus, previousCurrentCSV *string) bool {
	if status.InstalledCSV == "" {
		return false
	}

	if status.CurrentCSV != "" && status.CurrentCSV != status.InstalledCSV {
		return false
	}

	return previousCurrentCSV == nil || (status.CurrentCSV != "" && status.CurrentCSV != *previousCurrentCSV)
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
		setAddonOwnership(operatorGroup)
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
	setAddonOwnership(newNs)

	err = c.Create(ctx, newNs)
	// We also check IsAlreadyExists just in case another process created it in the few milliseconds since our Get call
	if err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create namespace %s: %v", namespaceName, err)
	}

	klog.Infof("Successfully created namespace %s.", namespaceName)
	return nil
}
