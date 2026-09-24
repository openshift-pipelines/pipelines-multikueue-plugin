package reconcilers

import (
	"testing"

	kueuev1 "github.com/openshift/kueue-operator/pkg/apis/kueueoperator/v1"
	operatorsv1 "github.com/operator-framework/api/pkg/operators/v1"
	olm "github.com/operator-framework/api/pkg/operators/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	clusterv1 "open-cluster-management.io/api/cluster/v1"
	workv1 "open-cluster-management.io/api/work/v1"
	msav1beta1 "open-cluster-management.io/managed-serviceaccount/apis/authentication/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"
)

// newTestScheme returns a scheme with every API group the reconcilers touch.
// Each test gets its own scheme because several ensure* helpers register types
// into the client's scheme as a side effect.
func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	s := runtime.NewScheme()
	for name, add := range map[string]func(*runtime.Scheme) error{
		"client-go":     clientgoscheme.AddToScheme,
		"kueue":         kueuev1.AddToScheme,
		"kueue-v1beta2": kueuev1beta2.AddToScheme,
		"cluster":       clusterv1.Install,
		"work":          workv1.Install,
		"msa":           msav1beta1.AddToScheme,
		"olm":           olm.AddToScheme,
		"operators":     operatorsv1.AddToScheme,
	} {
		if err := add(s); err != nil {
			t.Fatalf("failed to add %s types to scheme: %v", name, err)
		}
	}
	return s
}

// newFakeClient returns a fake client seeded with objs.
func newFakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()

	return fake.NewClientBuilder().
		WithScheme(newTestScheme(t)).
		WithObjects(objs...).
		Build()
}

func newSecret(namespace, name string, data map[string][]byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Data:       data,
	}
}

func newManagedCluster(name string, urls ...string) *clusterv1.ManagedCluster {
	mc := &clusterv1.ManagedCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}
	for _, u := range urls {
		mc.Spec.ManagedClusterClientConfigs = append(
			mc.Spec.ManagedClusterClientConfigs,
			clusterv1.ClientConfig{URL: u},
		)
	}
	return mc
}
