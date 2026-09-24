package reconcilers

import (
	"context"
	"testing"
	"time"

	kueuev1 "github.com/openshift/kueue-operator/pkg/apis/kueueoperator/v1"
	operatorsv1 "github.com/operator-framework/api/pkg/operators/v1"
	olm "github.com/operator-framework/api/pkg/operators/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	// The namespace must be created alongside the group.
	ns := &corev1.Namespace{}
	if err := b.Get(ctx, client.ObjectKey{Name: "openshift-kueue-operator"}, ns); err != nil {
		t.Errorf("namespace was not created for the OperatorGroup: %v", err)
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

func TestEnsureOperatorAdoptsExistingSubscription(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// The subscription lives under a different name/namespace than the
	// defaults; ensureOperator must match on spec.package and adopt it.
	existing := newInstalledSubscription("operators", "custom-name", "kueue-operator")
	b := &ClusterBootstrap{Client: newFakeClient(t, existing)}

	if err := b.ensureOperator(ctx, "kueue-operator", "stable-v1.4", "openshift-kueue-operator"); err != nil {
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
	err := b.ensureOperator(ctx, "kueue-operator", "stable-v1.4", "openshift-kueue-operator")
	if err == nil {
		t.Fatal("ensureOperator() returned nil, want a timeout while the CSV is not installed")
	}

	// Read back with a live context: ctx is past its deadline by now.
	sub := &olm.Subscription{}
	key := types.NamespacedName{Namespace: "openshift-kueue-operator", Name: "kueue-operator"}
	if err := b.Get(context.Background(), key, sub); err != nil {
		t.Fatalf("subscription was not created: %v", err)
	}
	if sub.Spec.Channel != "stable-v1.4" {
		t.Errorf("channel = %q, want %q", sub.Spec.Channel, "stable-v1.4")
	}
	if sub.Spec.Package != "kueue-operator" {
		t.Errorf("package = %q, want %q", sub.Spec.Package, "kueue-operator")
	}
	if sub.Spec.CatalogSource != "redhat-operators" {
		t.Errorf("catalog source = %q, want %q", sub.Spec.CatalogSource, "redhat-operators")
	}
	if sub.Spec.CatalogSourceNamespace != "openshift-marketplace" {
		t.Errorf("catalog source namespace = %q, want %q", sub.Spec.CatalogSourceNamespace, "openshift-marketplace")
	}

	ns := &corev1.Namespace{}
	if err := b.Get(context.Background(), client.ObjectKey{Name: "openshift-kueue-operator"}, ns); err != nil {
		t.Errorf("subscription namespace was not created: %v", err)
	}
}

func TestEnsureOperators(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Seed an installed subscription for each operator ensureOperators wants,
	// otherwise the readiness poll would block for its 30 minute timeout.
	b := &ClusterBootstrap{Client: newFakeClient(t,
		newInstalledSubscription("openshift-operators", "openshift-pipelines-operator-rh", "openshift-pipelines-operator-rh"),
		newInstalledSubscription("openshift-kueue-operator", "kueue-operator", "kueue-operator"),
		newInstalledSubscription("cert-manager-operator", "openshift-cert-manager-operator", "openshift-cert-manager-operator"),
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
		newInstalledSubscription("openshift-operators", "openshift-pipelines-operator-rh", "openshift-pipelines-operator-rh"),
		newInstalledSubscription("openshift-kueue-operator", "kueue-operator", "kueue-operator"),
		newInstalledSubscription("cert-manager-operator", "openshift-cert-manager-operator", "openshift-cert-manager-operator"),
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
		newInstalledSubscription("openshift-operators", "openshift-pipelines-operator-rh", "openshift-pipelines-operator-rh"),
		newInstalledSubscription("openshift-kueue-operator", "kueue-operator", "kueue-operator"),
		newInstalledSubscription("cert-manager-operator", "openshift-cert-manager-operator", "openshift-cert-manager-operator"),
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
