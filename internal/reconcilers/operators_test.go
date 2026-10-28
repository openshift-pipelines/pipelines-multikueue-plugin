package reconcilers

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/openshift-pipelines/pipelines-multikueue-plugin/internal/common"
	kueuev1 "github.com/openshift/kueue-operator/pkg/apis/kueueoperator/v1"
	operatorsv1 "github.com/operator-framework/api/pkg/operators/v1"
	olm "github.com/operator-framework/api/pkg/operators/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"
)

func TestEnsureNamespaceCreates(t *testing.T) {
	ctx := context.Background()
	b := &ClusterBootstrap{Client: newFakeClient(t)}

	if err := b.EnsureNamespace(ctx, "openshift-operators"); err != nil {
		t.Fatalf("EnsureNamespace() error = %v", err)
	}

	ns := &corev1.Namespace{}
	if err := b.Get(ctx, client.ObjectKey{Name: "openshift-operators"}, ns); err != nil {
		t.Fatalf("namespace was not created: %v", err)
	}
}

func TestEnsureNamespaceAlreadyExists(t *testing.T) {
	ctx := context.Background()
	existing := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "openshift-operators",
			Labels: map[string]string{"pre-existing": "yes"},
		},
	}
	b := &ClusterBootstrap{Client: newFakeClient(t, existing)}

	if err := b.EnsureNamespace(ctx, "openshift-operators"); err != nil {
		t.Fatalf("EnsureNamespace() error = %v", err)
	}

	ns := &corev1.Namespace{}
	if err := b.Get(ctx, client.ObjectKey{Name: "openshift-operators"}, ns); err != nil {
		t.Fatalf("failed to read back namespace: %v", err)
	}
	if ns.Labels["pre-existing"] != "yes" {
		t.Error("existing namespace was overwritten instead of left alone")
	}
}

func TestEnsureNamespaceIsIdempotent(t *testing.T) {
	ctx := context.Background()
	b := &ClusterBootstrap{Client: newFakeClient(t)}

	for i := 0; i < 3; i++ {
		if err := b.EnsureNamespace(ctx, "cert-manager-operator"); err != nil {
			t.Fatalf("EnsureNamespace() call %d error = %v", i, err)
		}
	}

	list := &corev1.NamespaceList{}
	if err := b.List(ctx, list); err != nil {
		t.Fatalf("failed to list namespaces: %v", err)
	}
	if len(list.Items) != 1 {
		t.Errorf("got %d namespaces, want 1", len(list.Items))
	}
}

func TestEnsureOperatorGroupCreates(t *testing.T) {
	ctx := context.Background()
	b := &ClusterBootstrap{Client: newFakeClient(t)}

	if err := b.ensureOperatorGroup(ctx, "openshift-kueue-operator"); err != nil {
		t.Fatalf("ensureOperatorGroup() error = %v", err)
	}

	og := &operatorsv1.OperatorGroup{}
	key := types.NamespacedName{Namespace: "openshift-kueue-operator", Name: "openshift-kueue-operator"}
	if err := b.Get(ctx, key, og); err != nil {
		t.Fatalf("OperatorGroup was not created: %v", err)
	}
	if !isAddonOwned(og) {
		t.Errorf("OperatorGroup labels = %v, want it marked as addon owned", og.Labels)
	}

	// The namespace must be created alongside the group.
	ns := &corev1.Namespace{}
	if err := b.Get(ctx, client.ObjectKey{Name: "openshift-kueue-operator"}, ns); err != nil {
		t.Errorf("namespace was not created for the OperatorGroup: %v", err)
	}
	if !isAddonOwned(ns) {
		t.Errorf("namespace labels = %v, want it marked as addon owned", ns.Labels)
	}
}

func TestEnsureOperatorGroupSkipsWhenOnePresent(t *testing.T) {
	ctx := context.Background()
	existing := &operatorsv1.OperatorGroup{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "openshift-kueue-operator",
			Name:      "some-other-group",
		},
	}
	b := &ClusterBootstrap{Client: newFakeClient(t, existing)}

	if err := b.ensureOperatorGroup(ctx, "openshift-kueue-operator"); err != nil {
		t.Fatalf("ensureOperatorGroup() error = %v", err)
	}

	list := &operatorsv1.OperatorGroupList{}
	if err := b.List(ctx, list, client.InNamespace("openshift-kueue-operator")); err != nil {
		t.Fatalf("failed to list OperatorGroups: %v", err)
	}
	if len(list.Items) != 1 {
		t.Errorf("got %d OperatorGroups, want the pre-existing one only", len(list.Items))
	}
	if len(list.Items) == 1 && list.Items[0].Name != "some-other-group" {
		t.Errorf("OperatorGroup name = %q, want the pre-existing %q", list.Items[0].Name, "some-other-group")
	}
}

// TestEnsureOperatorGroupIgnoresOtherNamespaces verifies the list is scoped to
// the target namespace, so a group elsewhere does not suppress creation.
func TestEnsureOperatorGroupIgnoresOtherNamespaces(t *testing.T) {
	ctx := context.Background()
	elsewhere := &operatorsv1.OperatorGroup{
		ObjectMeta: metav1.ObjectMeta{Namespace: "unrelated", Name: "unrelated"},
	}
	b := &ClusterBootstrap{Client: newFakeClient(t, elsewhere)}

	if err := b.ensureOperatorGroup(ctx, "cert-manager-operator"); err != nil {
		t.Fatalf("ensureOperatorGroup() error = %v", err)
	}

	og := &operatorsv1.OperatorGroup{}
	key := types.NamespacedName{Namespace: "cert-manager-operator", Name: "cert-manager-operator"}
	if err := b.Get(ctx, key, og); err != nil {
		t.Fatalf("OperatorGroup was not created in the target namespace: %v", err)
	}
}

// newInstalledSubscription returns a Subscription that already reports an
// installed CSV, so ensureOperator's readiness poll succeeds on the first tick.
func newInstalledSubscription(namespace, name, pkg string) *olm.Subscription {
	return &olm.Subscription{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: &olm.SubscriptionSpec{
			Package: pkg,
			Channel: "stable",
		},
		Status: olm.SubscriptionStatus{
			InstalledCSV: pkg + ".v1.0.0",
		},
	}
}

// operatorFor returns the entry for packageName, failing the test when the
// addon does not install that operator at all.
func operatorFor(t *testing.T, packageName string) operator {
	t.Helper()

	for _, op := range defaultOperators() {
		if op.packageName == packageName {
			return op
		}
	}
	t.Fatalf("%q is not one of the operators the addon installs", packageName)
	return operator{}
}

func TestDefaultOperatorsFallsBackToBuiltInChannels(t *testing.T) {
	for _, env := range []string{
		common.PipelinesOperatorChannelEnv,
		common.KueueOperatorChannelEnv,
		common.CertManagerOperatorChannelEnv,
	} {
		unsetEnv(t, env)
	}

	for _, tc := range []struct{ packageName, want string }{
		{common.PipelinesOperatorPackage, common.DefaultPipelinesOperatorChannel},
		{common.KueueOperatorPackage, common.DefaultKueueOperatorChannel},
		{common.CertManagerOperatorPackage, common.DefaultCertManagerOperatorChannel},
	} {
		t.Run(tc.packageName, func(t *testing.T) {
			if got := operatorFor(t, tc.packageName).channel; got != tc.want {
				t.Errorf("channel = %q, want the built-in default %q", got, tc.want)
			}
		})
	}
}

func TestDefaultOperatorsReadsChannelsFromEnv(t *testing.T) {
	for _, tc := range []struct{ env, packageName, channel string }{
		{common.PipelinesOperatorChannelEnv, common.PipelinesOperatorPackage, "pipelines-1.20"},
		{common.KueueOperatorChannelEnv, common.KueueOperatorPackage, "stable-v1.5"},
		{common.CertManagerOperatorChannelEnv, common.CertManagerOperatorPackage, "stable-v2"},
	} {
		t.Run(tc.env, func(t *testing.T) {
			t.Setenv(tc.env, tc.channel)

			if got := operatorFor(t, tc.packageName).channel; got != tc.channel {
				t.Errorf("channel = %q, want the %s override %q", got, tc.env, tc.channel)
			}
		})
	}
}

// An empty override is treated as no override, so a blank value in the
// Deployment cannot leave an operator subscribed to no channel at all.
func TestDefaultOperatorsIgnoresEmptyChannelOverride(t *testing.T) {
	t.Setenv(common.KueueOperatorChannelEnv, "")

	if got := operatorFor(t, common.KueueOperatorPackage).channel; got != common.DefaultKueueOperatorChannel {
		t.Errorf("channel = %q, want the built-in default %q", got, common.DefaultKueueOperatorChannel)
	}
}

func TestDefaultOperatorsFallsBackToBuiltInNamespaces(t *testing.T) {
	for _, env := range []string{
		common.KueueNamespaceEnv,
		common.PipelinesOperatorNamespaceEnv,
		common.CertManagerOperatorNamespaceEnv,
	} {
		unsetEnv(t, env)
	}

	for _, tc := range []struct{ packageName, want string }{
		{common.PipelinesOperatorPackage, common.DefaultPipelinesOperatorNamespace},
		{common.KueueOperatorPackage, common.DefaultKueueOperatorNamespace},
		{common.CertManagerOperatorPackage, common.DefaultCertManagerOperatorNamespace},
	} {
		t.Run(tc.packageName, func(t *testing.T) {
			if got := operatorFor(t, tc.packageName).namespace; got != tc.want {
				t.Errorf("namespace = %q, want the built-in default %q", got, tc.want)
			}
		})
	}
}

func TestDefaultOperatorsReadsNamespacesFromEnv(t *testing.T) {
	for _, tc := range []struct{ env, packageName, namespace string }{
		{common.PipelinesOperatorNamespaceEnv, common.PipelinesOperatorPackage, "pipelines-operators"},
		{common.KueueNamespaceEnv, common.KueueOperatorPackage, "kueue-system"},
		{common.CertManagerOperatorNamespaceEnv, common.CertManagerOperatorPackage, "cert-manager"},
	} {
		t.Run(tc.env, func(t *testing.T) {
			t.Setenv(tc.env, tc.namespace)

			if got := operatorFor(t, tc.packageName).namespace; got != tc.namespace {
				t.Errorf("namespace = %q, want the %s override %q", got, tc.env, tc.namespace)
			}
		})
	}
}

// The reconcile loop must pick up a channel changed after the process started,
// so the table cannot be resolved once at package initialization.
func TestEnsureOperatorsUsesCurrentEnvironment(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	t.Setenv(common.KueueOperatorChannelEnv, "stable-v1.9")

	b := &ClusterBootstrap{Client: newFakeClient(t,
		newInstalledSubscription("openshift-operators", common.PipelinesOperatorPackage, common.PipelinesOperatorPackage),
		addonOwned(newSubscriptionOnChannel("openshift-kueue-operator", common.KueueOperatorPackage, common.KueueOperatorPackage, "stable-v1.4")),
		newInstalledSubscription("cert-manager-operator", common.CertManagerOperatorPackage, common.CertManagerOperatorPackage),
	)}

	if err := b.ensureOperators(ctx); err != nil {
		t.Fatalf("ensureOperators() error = %v", err)
	}

	sub := getSubscription(t, b.Client, "openshift-kueue-operator", common.KueueOperatorPackage)
	if sub.Spec.Channel != "stable-v1.9" {
		t.Errorf("channel = %q, want the environment override %q", sub.Spec.Channel, "stable-v1.9")
	}
}

// unsetEnv removes envName for the duration of the test, restoring any
// pre-existing value afterwards.
func unsetEnv(t *testing.T, envName string) {
	t.Helper()

	original, ok := os.LookupEnv(envName)
	if !ok {
		return
	}
	// Register the restore first so it runs even if Unsetenv fails.
	t.Cleanup(func() {
		if err := os.Setenv(envName, original); err != nil {
			t.Fatalf("failed to restore %s: %v", envName, err)
		}
	})
	if err := os.Unsetenv(envName); err != nil {
		t.Fatalf("failed to unset %s: %v", envName, err)
	}
}

// testOperator returns an operator whose channel came from the compiled-in
// default, which is the unpinned, forward-only case.
func testOperator(packageName, channel, namespace string) operator {
	return operator{packageName: packageName, channel: channel, namespace: namespace}
}

// pinnedOperator returns an operator whose channel was set explicitly on the
// controller's environment.
func pinnedOperator(packageName, channel, namespace string) operator {
	op := testOperator(packageName, channel, namespace)
	op.channelPinned = true
	return op
}

// newSubscriptionOnChannel returns an installed Subscription pinned to a
// specific channel.
func newSubscriptionOnChannel(namespace, name, pkg, channel string) *olm.Subscription {
	sub := newInstalledSubscription(namespace, name, pkg)
	sub.Spec.Channel = channel
	return sub
}

// addonOwned marks sub as installed by the addon, the way ensureOperator does
// when it creates one.
func addonOwned(sub *olm.Subscription) *olm.Subscription {
	setAddonOwnership(sub)
	return sub
}

// getSubscription reads a Subscription back with a live context, so it works
// even after the test's own context has expired.
func getSubscription(t *testing.T, c client.Client, namespace, name string) *olm.Subscription {
	t.Helper()

	sub := &olm.Subscription{}
	key := types.NamespacedName{Namespace: namespace, Name: name}
	if err := c.Get(context.Background(), key, sub); err != nil {
		t.Fatalf("subscription %s/%s was not readable: %v", namespace, name, err)
	}
	return sub
}

func TestEnsureOperatorAdoptsExistingSubscription(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The subscription lives under a different name/namespace than the
	// defaults; ensureOperator must match on spec.package and adopt it.
	existing := newInstalledSubscription("operators", "custom-name", common.KueueOperatorPackage)
	b := &ClusterBootstrap{Client: newFakeClient(t, existing)}

	if err := b.ensureOperator(ctx, testOperator(common.KueueOperatorPackage, "stable-v1.4", "openshift-kueue-operator")); err != nil {
		t.Fatalf("ensureOperator() error = %v", err)
	}

	// No second subscription should have been created.
	list := &olm.SubscriptionList{}
	if err := b.List(ctx, list); err != nil {
		t.Fatalf("failed to list subscriptions: %v", err)
	}
	if len(list.Items) != 1 {
		t.Errorf("got %d subscriptions, want the existing one to be adopted", len(list.Items))
	}
}

func TestEnsureOperatorCreatesSubscription(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	b := &ClusterBootstrap{Client: newFakeClient(t)}

	// Nothing sets status.installedCSV on the fake cluster, so the readiness
	// poll runs until the context deadline. That is the point of the test: the
	// subscription is created and ensureOperator keeps waiting rather than
	// reporting success.
	err := b.ensureOperator(ctx, testOperator(common.KueueOperatorPackage, "stable-v1.4", "openshift-kueue-operator"))
	if err == nil {
		t.Fatal("ensureOperator() returned nil, want a timeout while the CSV is not installed")
	}

	// Read back with a live context: ctx is past its deadline by now.
	sub := &olm.Subscription{}
	key := types.NamespacedName{Namespace: "openshift-kueue-operator", Name: common.KueueOperatorPackage}
	if err := b.Get(context.Background(), key, sub); err != nil {
		t.Fatalf("subscription was not created: %v", err)
	}
	if sub.Spec.Channel != "stable-v1.4" {
		t.Errorf("channel = %q, want %q", sub.Spec.Channel, "stable-v1.4")
	}
	if sub.Spec.Package != common.KueueOperatorPackage {
		t.Errorf("package = %q, want %q", sub.Spec.Package, common.KueueOperatorPackage)
	}
	if sub.Spec.CatalogSource != "redhat-operators" {
		t.Errorf("catalog source = %q, want %q", sub.Spec.CatalogSource, "redhat-operators")
	}
	if sub.Spec.CatalogSourceNamespace != "openshift-marketplace" {
		t.Errorf("catalog source namespace = %q, want %q", sub.Spec.CatalogSourceNamespace, "openshift-marketplace")
	}

	// Ownership is what later reconciles key off when deciding whether this
	// operator is the addon's to upgrade.
	if !isAddonOwned(sub) {
		t.Errorf("created subscription labels = %v, want it marked as addon owned", sub.Labels)
	}

	ns := &corev1.Namespace{}
	if err := b.Get(context.Background(), client.ObjectKey{Name: "openshift-kueue-operator"}, ns); err != nil {
		t.Errorf("subscription namespace was not created: %v", err)
	}
}

func TestEnsureOperatorUpgradesOwnedSubscription(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	existing := addonOwned(newSubscriptionOnChannel("openshift-kueue-operator", common.KueueOperatorPackage, common.KueueOperatorPackage, "stable-v1.3"))
	b := &ClusterBootstrap{Client: newFakeClient(t, existing)}

	if err := b.ensureOperator(ctx, testOperator(common.KueueOperatorPackage, "stable-v1.4", "openshift-kueue-operator")); err != nil {
		t.Fatalf("ensureOperator() error = %v", err)
	}

	sub := getSubscription(t, b.Client, "openshift-kueue-operator", common.KueueOperatorPackage)
	if sub.Spec.Channel != "stable-v1.4" {
		t.Errorf("channel = %q, want the subscription moved to %q", sub.Spec.Channel, "stable-v1.4")
	}
}

// An operator the cluster already had is not the addon's to move, however old
// its channel is.
func TestEnsureOperatorLeavesForeignSubscriptionAlone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	existing := newSubscriptionOnChannel("openshift-kueue-operator", common.KueueOperatorPackage, common.KueueOperatorPackage, "stable-v1.3")
	b := &ClusterBootstrap{Client: newFakeClient(t, existing)}

	if err := b.ensureOperator(ctx, testOperator(common.KueueOperatorPackage, "stable-v1.4", "openshift-kueue-operator")); err != nil {
		t.Fatalf("ensureOperator() error = %v", err)
	}

	sub := getSubscription(t, b.Client, "openshift-kueue-operator", common.KueueOperatorPackage)
	if sub.Spec.Channel != "stable-v1.3" {
		t.Errorf("channel = %q, want the pre-existing %q left alone", sub.Spec.Channel, "stable-v1.3")
	}
}

// The addon upgrades, it never rolls an operator back.
func TestEnsureOperatorDoesNotDowngradeOwnedSubscription(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	existing := addonOwned(newSubscriptionOnChannel("openshift-kueue-operator", common.KueueOperatorPackage, common.KueueOperatorPackage, "stable-v1.5"))
	b := &ClusterBootstrap{Client: newFakeClient(t, existing)}

	if err := b.ensureOperator(ctx, testOperator(common.KueueOperatorPackage, "stable-v1.4", "openshift-kueue-operator")); err != nil {
		t.Fatalf("ensureOperator() error = %v", err)
	}

	sub := getSubscription(t, b.Client, "openshift-kueue-operator", common.KueueOperatorPackage)
	if sub.Spec.Channel != "stable-v1.5" {
		t.Errorf("channel = %q, want the newer %q kept", sub.Spec.Channel, "stable-v1.5")
	}
}

// Channels that cannot be ordered against each other are left as they are,
// rather than guessed at.
func TestEnsureOperatorKeepsIncomparableChannel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	existing := addonOwned(newSubscriptionOnChannel("openshift-operators", common.PipelinesOperatorPackage, common.PipelinesOperatorPackage, "pipelines-1.19"))
	b := &ClusterBootstrap{Client: newFakeClient(t, existing)}

	if err := b.ensureOperator(ctx, testOperator(common.PipelinesOperatorPackage, "latest", "openshift-operators")); err != nil {
		t.Fatalf("ensureOperator() error = %v", err)
	}

	sub := getSubscription(t, b.Client, "openshift-operators", common.PipelinesOperatorPackage)
	if sub.Spec.Channel != "pipelines-1.19" {
		t.Errorf("channel = %q, want the incomparable %q left alone", sub.Spec.Channel, "pipelines-1.19")
	}
}

// A channel set on the controller's environment is an instruction, so it is
// applied even across the `latest` / `pipelines-1.x` boundary, where neither
// side can be ordered against the other.
func TestEnsureOperatorAppliesPinnedIncomparableChannel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	existing := addonOwned(newSubscriptionOnChannel("openshift-operators", common.PipelinesOperatorPackage, common.PipelinesOperatorPackage, "latest"))
	b := &ClusterBootstrap{Client: newFakeClient(t, existing)}

	if err := b.ensureOperator(ctx, pinnedOperator(common.PipelinesOperatorPackage, "pipelines-1.23", "openshift-operators")); err != nil {
		t.Fatalf("ensureOperator() error = %v", err)
	}

	sub := getSubscription(t, b.Client, "openshift-operators", common.PipelinesOperatorPackage)
	if sub.Spec.Channel != "pipelines-1.23" {
		t.Errorf("channel = %q, want the pin to %q applied", sub.Spec.Channel, "pipelines-1.23")
	}
}

// Rolling an operator back is only ever allowed when someone asked for that
// exact channel.
func TestEnsureOperatorAppliesPinnedRollback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	existing := addonOwned(newSubscriptionOnChannel("openshift-kueue-operator", common.KueueOperatorPackage, common.KueueOperatorPackage, "stable-v1.5"))
	b := &ClusterBootstrap{Client: newFakeClient(t, existing)}

	if err := b.ensureOperator(ctx, pinnedOperator(common.KueueOperatorPackage, "stable-v1.4", "openshift-kueue-operator")); err != nil {
		t.Fatalf("ensureOperator() error = %v", err)
	}

	sub := getSubscription(t, b.Client, "openshift-kueue-operator", common.KueueOperatorPackage)
	if sub.Spec.Channel != "stable-v1.4" {
		t.Errorf("channel = %q, want the pinned rollback to %q applied", sub.Spec.Channel, "stable-v1.4")
	}
}

// Pinning does not override ownership: an operator the cluster already had
// still belongs to whoever installed it.
func TestEnsureOperatorPinDoesNotTouchForeignSubscription(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	existing := newSubscriptionOnChannel("openshift-kueue-operator", common.KueueOperatorPackage, common.KueueOperatorPackage, "stable-v1.3")
	b := &ClusterBootstrap{Client: newFakeClient(t, existing)}

	if err := b.ensureOperator(ctx, pinnedOperator(common.KueueOperatorPackage, "stable-v1.4", "openshift-kueue-operator")); err != nil {
		t.Fatalf("ensureOperator() error = %v", err)
	}

	sub := getSubscription(t, b.Client, "openshift-kueue-operator", common.KueueOperatorPackage)
	if sub.Spec.Channel != "stable-v1.3" {
		t.Errorf("channel = %q, want the pre-existing %q left alone", sub.Spec.Channel, "stable-v1.3")
	}
}

// defaultOperators must mark a channel as pinned exactly when its environment
// variable is set, since that flag is what unlocks the sideways moves above.
func TestDefaultOperatorsMarksConfiguredChannelsAsPinned(t *testing.T) {
	channelEnvs := []string{
		common.PipelinesOperatorChannelEnv,
		common.KueueOperatorChannelEnv,
		common.CertManagerOperatorChannelEnv,
	}

	t.Run("unset channels are not pinned", func(t *testing.T) {
		for _, env := range channelEnvs {
			unsetEnv(t, env)
		}
		for _, op := range defaultOperators() {
			if op.channelPinned {
				t.Errorf("%s: channelPinned = true, want false for a compiled-in default", op.packageName)
			}
		}
	})

	t.Run("only the configured channel is pinned", func(t *testing.T) {
		for _, env := range channelEnvs {
			unsetEnv(t, env)
		}
		t.Setenv(common.KueueOperatorChannelEnv, "stable-v1.5")

		for _, op := range defaultOperators() {
			want := op.packageName == common.KueueOperatorPackage
			if op.channelPinned != want {
				t.Errorf("%s: channelPinned = %v, want %v", op.packageName, op.channelPinned, want)
			}
		}
	})
}

// The subscription the addon adopted lives under a non-default name, so the
// upgrade has to follow it there.
func TestEnsureOperatorUpgradesSubscriptionInAnotherNamespace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	existing := addonOwned(newSubscriptionOnChannel("operators", "custom-name", common.KueueOperatorPackage, "stable-v1.3"))
	b := &ClusterBootstrap{Client: newFakeClient(t, existing)}

	if err := b.ensureOperator(ctx, testOperator(common.KueueOperatorPackage, "stable-v1.4", "openshift-kueue-operator")); err != nil {
		t.Fatalf("ensureOperator() error = %v", err)
	}

	sub := getSubscription(t, b.Client, "operators", "custom-name")
	if sub.Spec.Channel != "stable-v1.4" {
		t.Errorf("channel = %q, want the adopted subscription moved to %q", sub.Spec.Channel, "stable-v1.4")
	}

	list := &olm.SubscriptionList{}
	if err := b.List(context.Background(), list); err != nil {
		t.Fatalf("failed to list subscriptions: %v", err)
	}
	if len(list.Items) != 1 {
		t.Errorf("got %d subscriptions, want the existing one upgraded in place", len(list.Items))
	}
}

// Until OLM finishes installing the CSV the new channel points at, the upgrade
// is still in flight and ensureOperator must keep waiting.
func TestEnsureOperatorWaitsForUpgradeToComplete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	existing := addonOwned(newSubscriptionOnChannel("openshift-kueue-operator", common.KueueOperatorPackage, common.KueueOperatorPackage, "stable-v1.3"))
	existing.Status.InstalledCSV = "kueue-operator.v1.3.0"
	existing.Status.CurrentCSV = "kueue-operator.v1.4.0"
	b := &ClusterBootstrap{Client: newFakeClient(t, existing)}

	err := b.ensureOperator(ctx, testOperator(common.KueueOperatorPackage, "stable-v1.4", "openshift-kueue-operator"))
	if err == nil {
		t.Fatal("ensureOperator() returned nil, want a timeout while the new CSV is still installing")
	}

	sub := getSubscription(t, b.Client, "openshift-kueue-operator", common.KueueOperatorPackage)
	if sub.Spec.Channel != "stable-v1.4" {
		t.Errorf("channel = %q, want the upgrade to have been requested anyway", sub.Spec.Channel)
	}
}

func TestEnsureOperators(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Seed an installed subscription for each operator ensureOperators wants,
	// otherwise the readiness poll would block for its 30 minute timeout.
	b := &ClusterBootstrap{Client: newFakeClient(t,
		newInstalledSubscription("openshift-operators", common.PipelinesOperatorPackage, common.PipelinesOperatorPackage),
		newInstalledSubscription("openshift-kueue-operator", common.KueueOperatorPackage, common.KueueOperatorPackage),
		newInstalledSubscription("cert-manager-operator", common.CertManagerOperatorPackage, common.CertManagerOperatorPackage),
	)}

	if err := b.ensureOperators(ctx); err != nil {
		t.Fatalf("ensureOperators() error = %v", err)
	}

	list := &olm.SubscriptionList{}
	if err := b.List(ctx, list); err != nil {
		t.Fatalf("failed to list subscriptions: %v", err)
	}
	if len(list.Items) != 3 {
		t.Errorf("got %d subscriptions, want the 3 seeded ones with no duplicates", len(list.Items))
	}
}

func TestBootstrap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b := &ClusterBootstrap{Client: newFakeClient(t,
		newInstalledSubscription("openshift-operators", common.PipelinesOperatorPackage, common.PipelinesOperatorPackage),
		newInstalledSubscription("openshift-kueue-operator", common.KueueOperatorPackage, common.KueueOperatorPackage),
		newInstalledSubscription("cert-manager-operator", common.CertManagerOperatorPackage, common.CertManagerOperatorPackage),
	)}

	if err := b.bootstrap(ctx); err != nil {
		t.Fatalf("bootstrap() error = %v", err)
	}

	// bootstrap must leave the Kueue CR in place for the queue setup that follows.
	if err := b.Get(ctx, client.ObjectKey{Name: "cluster"}, &kueuev1.Kueue{}); err != nil {
		t.Errorf("Kueue CR was not reconciled by bootstrap: %v", err)
	}
}

func TestStart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b := &ClusterBootstrap{Client: newFakeClient(t,
		newInstalledSubscription("openshift-operators", common.PipelinesOperatorPackage, common.PipelinesOperatorPackage),
		newInstalledSubscription("openshift-kueue-operator", common.KueueOperatorPackage, common.KueueOperatorPackage),
		newInstalledSubscription("cert-manager-operator", common.CertManagerOperatorPackage, common.CertManagerOperatorPackage),
	)}

	if err := b.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	// Start wires up the full local queueing stack.
	for _, tc := range []struct {
		name string
		key  client.ObjectKey
		obj  client.Object
	}{
		{"resource flavor", client.ObjectKey{Name: DefaultResourceFlavor}, &kueuev1beta2.ResourceFlavor{}},
		{"cluster queue", client.ObjectKey{Name: DefaultMultiClusterQueue}, &kueuev1beta2.ClusterQueue{}},
		{"local queue", client.ObjectKey{Namespace: "default", Name: DefaultLocalQueue}, &kueuev1beta2.LocalQueue{}},
		{"admission check", client.ObjectKey{Name: DefaultAdmissionCheckName}, &kueuev1beta2.AdmissionCheck{}},
	} {
		if err := b.Get(ctx, tc.key, tc.obj); err != nil {
			t.Errorf("%s was not created by Start(): %v", tc.name, err)
		}
	}
}

// installedSubscriptions is the set bootstrap needs seeded so the readiness
// poll returns immediately instead of blocking for its 30 minute timeout.
func installedSubscriptions(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()

	return newFakeClient(t, append([]client.Object{
		newInstalledSubscription("openshift-operators", common.PipelinesOperatorPackage, common.PipelinesOperatorPackage),
		newInstalledSubscription("openshift-kueue-operator", common.KueueOperatorPackage, common.KueueOperatorPackage),
		newInstalledSubscription("cert-manager-operator", common.CertManagerOperatorPackage, common.CertManagerOperatorPackage),
	}, objs...)...)
}

// On a spoke, bootstrap finishes by enabling the Tekton scheduler.
func TestBootstrapEnablesSchedulerOnSpoke(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b := &ClusterBootstrap{
		Client:      installedSubscriptions(t, newTektonConfig(TektonConfigName, map[string]any{"disabled": true})),
		ClusterRole: ClusterRoleSpoke,
	}

	if err := b.bootstrap(ctx); err != nil {
		t.Fatalf("bootstrap() error = %v", err)
	}

	got := getTektonConfig(t, b.Client, TektonConfigName)
	if schedulerBool(t, got, "disabled") {
		t.Error("scheduler was not enabled on a spoke cluster")
	}
	if !schedulerBool(t, got, "multi-cluster-disabled") {
		t.Error("scheduler multi-cluster handling was not disabled on a spoke cluster")
	}
	if role := schedulerString(t, got, "multi-cluster-role"); role != "" {
		t.Errorf("multi-cluster-role = %q, want it cleared", role)
	}
}

// The hub carries no ClusterRole, so the TektonConfig must be left alone.
func TestBootstrapLeavesSchedulerAloneOnHub(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b := &ClusterBootstrap{
		Client: installedSubscriptions(t, newTektonConfig(TektonConfigName, map[string]any{"disabled": true})),
	}
	if b.ClusterRole == ClusterRoleSpoke {
		t.Fatalf("test setup: ClusterRole = %q, want it unset", b.ClusterRole)
	}

	if err := b.bootstrap(ctx); err != nil {
		t.Fatalf("bootstrap() error = %v", err)
	}

	got := getTektonConfig(t, b.Client, TektonConfigName)
	if !schedulerBool(t, got, "disabled") {
		t.Error("hub bootstrap modified the TektonConfig scheduler, want it untouched")
	}
	if _, found, _ := unstructured.NestedString(got.Object, "spec", "scheduler", "multi-cluster-role"); found {
		t.Error("hub bootstrap set a multi-cluster-role, want the TektonConfig untouched")
	}
}

// A spoke with no TektonConfig fails the whole bootstrap rather than
// silently skipping the scheduler.
func TestBootstrapFailsOnSpokeWithoutTektonConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b := &ClusterBootstrap{
		Client:      installedSubscriptions(t),
		ClusterRole: ClusterRoleSpoke,
	}

	if err := b.bootstrap(ctx); err == nil {
		t.Fatal("bootstrap() error = nil, want the TektonConfig NotFound to propagate")
	}
}
