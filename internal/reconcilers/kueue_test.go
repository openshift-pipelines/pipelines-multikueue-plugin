package reconcilers

import (
	"context"
	"testing"

	"github.com/openshift-pipelines/pipelines-multikueue-plugin/internal/common"
	kueuev1 "github.com/openshift/kueue-operator/pkg/apis/kueueoperator/v1"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"
)

var tektonFramework = kueuev1.ExternalFramework{
	Group:    "tekton.dev",
	Resource: "pipelineruns",
	Version:  "v1",
}

var dummyIntegrationFramework = kueuev1.ExternalFramework{
	Group:    "dummy.integration",
	Resource: "dummyResources",
	Version:  "v1",
}
var dummyMultiKueueFramework = kueuev1.ExternalFramework{
	Group:    "dummy.multikueue",
	Resource: "dummyResources",
	Version:  "v1",
}

func TestEnsureExternalFramework(t *testing.T) {
	other := kueuev1.ExternalFramework{Group: "ray.io", Resource: "rayjobs", Version: "v1"}

	tests := []struct {
		name     string
		existing []kueuev1.ExternalFramework
		want     []kueuev1.ExternalFramework
	}{
		{
			name:     "nil slice gets the framework",
			existing: nil,
			want:     []kueuev1.ExternalFramework{tektonFramework},
		},
		{
			name:     "empty slice gets the framework",
			existing: []kueuev1.ExternalFramework{},
			want:     []kueuev1.ExternalFramework{tektonFramework},
		},
		{
			name:     "already present is a no-op",
			existing: []kueuev1.ExternalFramework{tektonFramework},
			want:     []kueuev1.ExternalFramework{tektonFramework},
		},
		{
			name:     "appended alongside unrelated frameworks",
			existing: []kueuev1.ExternalFramework{other},
			want:     []kueuev1.ExternalFramework{other, tektonFramework},
		},
		{
			name:     "duplicate is not added twice",
			existing: []kueuev1.ExternalFramework{other, tektonFramework},
			want:     []kueuev1.ExternalFramework{other, tektonFramework},
		},
		{
			name:     "same group and resource but different version is added",
			existing: []kueuev1.ExternalFramework{{Group: "tekton.dev", Resource: "pipelineruns", Version: "v1beta1"}},
			want: []kueuev1.ExternalFramework{
				{Group: "tekton.dev", Resource: "pipelineruns", Version: "v1beta1"},
				tektonFramework,
			},
		},
		{
			name:     "same group and version but different resource is added",
			existing: []kueuev1.ExternalFramework{{Group: "tekton.dev", Resource: "taskruns", Version: "v1"}},
			want: []kueuev1.ExternalFramework{
				{Group: "tekton.dev", Resource: "taskruns", Version: "v1"},
				tektonFramework,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ensureExternalFramework(tt.existing, tektonFramework)

			if len(got) != len(tt.want) {
				t.Fatalf("got %d frameworks %v, want %d %v", len(got), got, len(tt.want), tt.want)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Errorf("framework[%d] = %+v, want %+v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

// findQuota returns the nominal quota recorded for PipelineResourceName under
// the given flavor, or nil when it is absent.
func findQuota(cq *kueuev1beta2.ClusterQueue, flavor kueuev1beta2.ResourceFlavorReference) *resource.Quantity {
	for _, rg := range cq.Spec.ResourceGroups {
		for _, fq := range rg.Flavors {
			if fq.Name != flavor {
				continue
			}
			for _, rq := range fq.Resources {
				if rq.Name == PipelineResourceName {
					q := rq.NominalQuota
					return &q
				}
			}
		}
	}
	return nil
}

func TestEnsurePipelineRunResourceGroup(t *testing.T) {
	const flavor = kueuev1beta2.ResourceFlavorReference(DefaultResourceFlavor)
	quota := resource.MustParse("100")

	tests := []struct {
		name string
		cq   *kueuev1beta2.ClusterQueue
		// wantGroups is the expected number of resource groups afterwards.
		wantGroups int
	}{
		{
			name:       "no resource groups creates one",
			cq:         &kueuev1beta2.ClusterQueue{},
			wantGroups: 1,
		},
		{
			name: "resource group covering an unrelated resource is left alone",
			cq: &kueuev1beta2.ClusterQueue{
				Spec: kueuev1beta2.ClusterQueueSpec{
					ResourceGroups: []kueuev1beta2.ResourceGroup{{
						CoveredResources: []corev1.ResourceName{"cpu"},
					}},
				},
			},
			wantGroups: 2,
		},
		{
			name: "covered resource but missing flavor adds the flavor",
			cq: &kueuev1beta2.ClusterQueue{
				Spec: kueuev1beta2.ClusterQueueSpec{
					ResourceGroups: []kueuev1beta2.ResourceGroup{{
						CoveredResources: []corev1.ResourceName{PipelineResourceName},
					}},
				},
			},
			wantGroups: 1,
		},
		{
			name: "flavor present but missing quota adds the quota",
			cq: &kueuev1beta2.ClusterQueue{
				Spec: kueuev1beta2.ClusterQueueSpec{
					ResourceGroups: []kueuev1beta2.ResourceGroup{{
						CoveredResources: []corev1.ResourceName{PipelineResourceName},
						Flavors:          []kueuev1beta2.FlavorQuotas{{Name: flavor}},
					}},
				},
			},
			wantGroups: 1,
		},
		{
			name: "existing quota is overwritten",
			cq: &kueuev1beta2.ClusterQueue{
				Spec: kueuev1beta2.ClusterQueueSpec{
					ResourceGroups: []kueuev1beta2.ResourceGroup{{
						CoveredResources: []corev1.ResourceName{PipelineResourceName},
						Flavors: []kueuev1beta2.FlavorQuotas{{
							Name: flavor,
							Resources: []kueuev1beta2.ResourceQuota{{
								Name:         PipelineResourceName,
								NominalQuota: resource.MustParse("5"),
							}},
						}},
					}},
				},
			},
			wantGroups: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ensurePipelineRunResourceGroup(tt.cq, flavor, quota)

			if got := len(tt.cq.Spec.ResourceGroups); got != tt.wantGroups {
				t.Fatalf("got %d resource groups, want %d", got, tt.wantGroups)
			}

			got := findQuota(tt.cq, flavor)
			if got == nil {
				t.Fatalf("no nominal quota recorded for %s under flavor %q", PipelineResourceName, flavor)
			}
			if got.Cmp(quota) != 0 {
				t.Errorf("nominal quota = %s, want %s", got.String(), quota.String())
			}
		})
	}
}

func TestEnsurePipelineRunResourceGroupIsIdempotent(t *testing.T) {
	const flavor = kueuev1beta2.ResourceFlavorReference(DefaultResourceFlavor)
	quota := resource.MustParse("100")
	cq := &kueuev1beta2.ClusterQueue{}

	ensurePipelineRunResourceGroup(cq, flavor, quota)
	first := len(cq.Spec.ResourceGroups)

	for i := 0; i < 3; i++ {
		ensurePipelineRunResourceGroup(cq, flavor, quota)
	}

	if got := len(cq.Spec.ResourceGroups); got != first {
		t.Errorf("repeated calls grew resource groups from %d to %d", first, got)
	}
	if got := len(cq.Spec.ResourceGroups[0].Flavors); got != 1 {
		t.Errorf("repeated calls produced %d flavors, want 1", got)
	}
	if got := len(cq.Spec.ResourceGroups[0].Flavors[0].Resources); got != 1 {
		t.Errorf("repeated calls produced %d resource quotas, want 1", got)
	}
}

func TestEnsurePipelineRunResourceGroupSecondFlavor(t *testing.T) {
	quota := resource.MustParse("100")
	cq := &kueuev1beta2.ClusterQueue{}

	ensurePipelineRunResourceGroup(cq, "flavor-a", quota)
	ensurePipelineRunResourceGroup(cq, "flavor-b", quota)

	if got := len(cq.Spec.ResourceGroups); got != 1 {
		t.Fatalf("got %d resource groups, want 1", got)
	}
	if got := len(cq.Spec.ResourceGroups[0].Flavors); got != 2 {
		t.Fatalf("got %d flavors, want 2", got)
	}
	for _, flavor := range []kueuev1beta2.ResourceFlavorReference{"flavor-a", "flavor-b"} {
		if findQuota(cq, flavor) == nil {
			t.Errorf("no quota recorded for flavor %q", flavor)
		}
	}
}

func TestEnsureKueueCreates(t *testing.T) {
	ctx := context.Background()
	r := &ClusterBootstrap{Client: newFakeClient(t)}

	if err := r.ensureKueue(ctx); err != nil {
		t.Fatalf("ensureKueue() error = %v", err)
	}

	kueue := &kueuev1.Kueue{}
	if err := r.Get(ctx, types.NamespacedName{Name: "cluster"}, kueue); err != nil {
		t.Fatalf("Kueue CR was not created: %v", err)
	}

	integrations := kueue.Spec.Config.Integrations
	if len(integrations.ExternalFrameworks) != 1 || integrations.ExternalFrameworks[0] != tektonFramework {
		t.Errorf("integrations external frameworks = %+v, want [%+v]", integrations.ExternalFrameworks, tektonFramework)
	}
	if len(integrations.Frameworks) != 1 || integrations.Frameworks[0] != kueuev1.KueueIntegrationBatchJob {
		t.Errorf("integrations frameworks = %+v, want [%v]", integrations.Frameworks, kueuev1.KueueIntegrationBatchJob)
	}
	if kueue.Spec.Config.MultiKueue == nil {
		t.Fatal("MultiKueue config is nil")
	}
	if len(kueue.Spec.Config.MultiKueue.ExternalFrameworks) != 1 {
		t.Errorf("multikueue external frameworks = %+v, want exactly one", kueue.Spec.Config.MultiKueue.ExternalFrameworks)
	}
	if kueue.Spec.ManagementState != "Unmanaged" {
		t.Errorf("management state = %q, want %q", kueue.Spec.ManagementState, "Unmanaged")
	}
}

func TestEnsureKueuePatchesExisting(t *testing.T) {
	ctx := context.Background()
	existing := &kueuev1.Kueue{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster"},
		Spec: kueuev1.KueueOperandSpec{
			Config: kueuev1.KueueConfiguration{
				Integrations: kueuev1.Integrations{
					Frameworks:         []kueuev1.KueueIntegration{kueuev1.KueueIntegrationBatchJob},
					ExternalFrameworks: []kueuev1.ExternalFramework{dummyIntegrationFramework},
				},
				MultiKueue: &kueuev1.MultiKueue{
					ExternalFrameworks: []kueuev1.ExternalFramework{dummyMultiKueueFramework},
				},
			},
		},
	}
	r := &ClusterBootstrap{Client: newFakeClient(t, existing)}

	if err := r.ensureKueue(ctx); err != nil {
		t.Fatalf("ensureKueue() error = %v", err)
	}

	kueue := &kueuev1.Kueue{}
	if err := r.Get(ctx, types.NamespacedName{Name: "cluster"}, kueue); err != nil {
		t.Fatalf("failed to read back Kueue CR: %v", err)
	}

	// The pre-existing batch/job integration must survive the patch.
	assert.Equal(t, 1, len(kueue.Spec.Config.Integrations.Frameworks))

	assert.Equal(t, 2, len(kueue.Spec.Config.Integrations.ExternalFrameworks))
	assert.Equal(t, dummyIntegrationFramework, kueue.Spec.Config.Integrations.ExternalFrameworks[0])
	assert.Equal(t, tektonFramework, kueue.Spec.Config.Integrations.ExternalFrameworks[1])
	assert.NotNil(t, kueue.Spec.Config.MultiKueue)
	assert.Equal(t, len(kueue.Spec.Config.MultiKueue.ExternalFrameworks), 2)
	assert.Equal(t, kueue.Spec.Config.MultiKueue.ExternalFrameworks[0], dummyMultiKueueFramework)
	assert.Equal(t, kueue.Spec.Config.MultiKueue.ExternalFrameworks[1], tektonFramework)

}

func TestEnsureKueueIsIdempotent(t *testing.T) {
	ctx := context.Background()
	r := &ClusterBootstrap{Client: newFakeClient(t)}

	for i := 0; i < 3; i++ {
		if err := r.ensureKueue(ctx); err != nil {
			t.Fatalf("ensureKueue() call %d error = %v", i, err)
		}
	}

	kueue := &kueuev1.Kueue{}
	if err := r.Get(ctx, types.NamespacedName{Name: "cluster"}, kueue); err != nil {
		t.Fatalf("failed to read back Kueue CR: %v", err)
	}
	if got := len(kueue.Spec.Config.Integrations.ExternalFrameworks); got != 1 {
		t.Errorf("repeated calls produced %d external frameworks, want 1", got)
	}
}

func TestEnsureResourceFlavour(t *testing.T) {
	ctx := context.Background()
	r := &ClusterBootstrap{Client: newFakeClient(t)}

	if err := r.ensureResourceFlavour(ctx); err != nil {
		t.Fatalf("ensureResourceFlavour() error = %v", err)
	}

	rf := &kueuev1beta2.ResourceFlavor{}
	if err := r.Get(ctx, types.NamespacedName{Name: DefaultResourceFlavor}, rf); err != nil {
		t.Fatalf("ResourceFlavor was not created: %v", err)
	}

	// Second call must not fail on the already-existing object.
	if err := r.ensureResourceFlavour(ctx); err != nil {
		t.Errorf("second ensureResourceFlavour() error = %v", err)
	}
}

func TestEnsureLocalQueue(t *testing.T) {
	ctx := context.Background()
	r := &ClusterBootstrap{Client: newFakeClient(t)}

	if err := r.ensureLocalQueue(ctx, "default"); err != nil {
		t.Fatalf("ensureLocalQueue() error = %v", err)
	}

	lq := &kueuev1beta2.LocalQueue{}
	key := types.NamespacedName{Namespace: "default", Name: DefaultLocalQueue}
	if err := r.Get(ctx, key, lq); err != nil {
		t.Fatalf("LocalQueue was not created: %v", err)
	}
	if got := string(lq.Spec.ClusterQueue); got != DefaultMultiClusterQueue {
		t.Errorf("LocalQueue cluster queue = %q, want %q", got, DefaultMultiClusterQueue)
	}
}

func TestEnsureLocalQueueRepointsExisting(t *testing.T) {
	ctx := context.Background()
	existing := &kueuev1beta2.LocalQueue{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: DefaultLocalQueue},
		Spec:       kueuev1beta2.LocalQueueSpec{ClusterQueue: "some-other-queue"},
	}
	r := &ClusterBootstrap{Client: newFakeClient(t, existing)}

	if err := r.ensureLocalQueue(ctx, "team-a"); err != nil {
		t.Fatalf("ensureLocalQueue() error = %v", err)
	}

	lq := &kueuev1beta2.LocalQueue{}
	key := types.NamespacedName{Namespace: "team-a", Name: DefaultLocalQueue}
	if err := r.Get(ctx, key, lq); err != nil {
		t.Fatalf("failed to read back LocalQueue: %v", err)
	}
	if got := string(lq.Spec.ClusterQueue); got != DefaultMultiClusterQueue {
		t.Errorf("LocalQueue cluster queue = %q, want it repointed to %q", got, DefaultMultiClusterQueue)
	}
}

func TestEnsureClusterQueue(t *testing.T) {
	ctx := context.Background()
	r := &ClusterBootstrap{Client: newFakeClient(t)}

	if err := r.ensureClusterQueue(ctx); err != nil {
		t.Fatalf("ensureClusterQueue() error = %v", err)
	}

	cq := &kueuev1beta2.ClusterQueue{}
	if err := r.Get(ctx, types.NamespacedName{Name: DefaultMultiClusterQueue}, cq); err != nil {
		t.Fatalf("ClusterQueue was not created: %v", err)
	}

	if cq.Spec.NamespaceSelector == nil {
		t.Error("namespace selector is nil, want an empty selector matching all namespaces")
	}
	if cq.Spec.AdmissionChecksStrategy == nil {
		t.Fatal("admission checks strategy is nil")
	}
	checks := cq.Spec.AdmissionChecksStrategy.AdmissionChecks
	if len(checks) != 1 || string(checks[0].Name) != DefaultAdmissionCheckName {
		t.Errorf("admission checks = %+v, want exactly %q", checks, DefaultAdmissionCheckName)
	}
	if q := findQuota(cq, DefaultResourceFlavor); q == nil {
		t.Errorf("no %s quota on the cluster queue", PipelineResourceName)
	} else if q.Cmp(resource.MustParse("100")) != 0 {
		t.Errorf("nominal quota = %s, want 100", q.String())
	}
}

func TestEnsureClusterQueueIsIdempotent(t *testing.T) {
	ctx := context.Background()
	r := &ClusterBootstrap{Client: newFakeClient(t)}

	for i := 0; i < 3; i++ {
		if err := r.ensureClusterQueue(ctx); err != nil {
			t.Fatalf("ensureClusterQueue() call %d error = %v", i, err)
		}
	}

	cq := &kueuev1beta2.ClusterQueue{}
	if err := r.Get(ctx, types.NamespacedName{Name: DefaultMultiClusterQueue}, cq); err != nil {
		t.Fatalf("failed to read back ClusterQueue: %v", err)
	}
	if got := len(cq.Spec.AdmissionChecksStrategy.AdmissionChecks); got != 1 {
		t.Errorf("repeated calls produced %d admission checks, want 1", got)
	}
	if got := len(cq.Spec.ResourceGroups); got != 1 {
		t.Errorf("repeated calls produced %d resource groups, want 1", got)
	}
}

func TestEnsureAdmissionCheck(t *testing.T) {
	ctx := context.Background()
	r := &ClusterBootstrap{Client: newFakeClient(t)}

	if err := r.ensureAdmissionCheck(ctx); err != nil {
		t.Fatalf("ensureAdmissionCheck() error = %v", err)
	}

	ac := &kueuev1beta2.AdmissionCheck{}
	if err := r.Get(ctx, types.NamespacedName{Name: DefaultAdmissionCheckName}, ac); err != nil {
		t.Fatalf("AdmissionCheck was not created: %v", err)
	}
	if got := string(ac.Spec.ControllerName); got != "kueue.x-k8s.io/multikueue" {
		t.Errorf("controller name = %q, want %q", got, "kueue.x-k8s.io/multikueue")
	}
	if ac.Spec.Parameters == nil {
		t.Fatal("parameters reference is nil")
	}
	if got := string(ac.Spec.Parameters.Name); got != MultiKueueConfigName {
		t.Errorf("parameters name = %q, want %q", got, MultiKueueConfigName)
	}
	if got := string(ac.Spec.Parameters.Kind); got != "MultiKueueConfig" {
		t.Errorf("parameters kind = %q, want %q", got, "MultiKueueConfig")
	}
	if got := string(ac.Spec.Parameters.APIGroup); got != "kueue.x-k8s.io" {
		t.Errorf("parameters api group = %q, want %q", got, "kueue.x-k8s.io")
	}
}

func TestEnsureMultiKueueCluster(t *testing.T) {
	ctx := context.Background()
	r := &MultiKueueReconciler{Client: newFakeClient(t)}
	secret := newSecret(common.KueueNamespace, "cluster1", nil)

	if err := r.ensureMultiKueueCluster(ctx, "cluster1", secret); err != nil {
		t.Fatalf("ensureMultiKueueCluster() error = %v", err)
	}

	mkc := &kueuev1beta2.MultiKueueCluster{}
	if err := r.Get(ctx, types.NamespacedName{Name: "cluster1"}, mkc); err != nil {
		t.Fatalf("MultiKueueCluster was not created: %v", err)
	}
	if mkc.Spec.ClusterSource.KubeConfig == nil {
		t.Fatal("kubeconfig source is nil")
	}
	if got := string(mkc.Spec.ClusterSource.KubeConfig.LocationType); got != string(kueuev1beta2.SecretLocationType) {
		t.Errorf("location type = %q, want %q", got, kueuev1beta2.SecretLocationType)
	}
	if mkc.Spec.ClusterSource.KubeConfig.Location != "cluster1" {
		t.Errorf("location = %q, want the secret name %q", mkc.Spec.ClusterSource.KubeConfig.Location, "cluster1")
	}

	cfg := &kueuev1beta2.MultiKueueConfig{}
	if err := r.Get(ctx, types.NamespacedName{Name: MultiKueueConfigName}, cfg); err != nil {
		t.Fatalf("MultiKueueConfig was not created: %v", err)
	}
	if len(cfg.Spec.Clusters) != 1 || cfg.Spec.Clusters[0] != "cluster1" {
		t.Errorf("config clusters = %v, want [cluster1]", cfg.Spec.Clusters)
	}
}

func TestEnsureMultiKueueClusterAccumulatesClusters(t *testing.T) {
	ctx := context.Background()
	r := &MultiKueueReconciler{Client: newFakeClient(t)}

	for _, name := range []string{"cluster1", "cluster2", "cluster1"} {
		secret := newSecret(common.KueueNamespace, name, nil)
		if err := r.ensureMultiKueueCluster(ctx, name, secret); err != nil {
			t.Fatalf("ensureMultiKueueCluster(%q) error = %v", name, err)
		}
	}

	cfg := &kueuev1beta2.MultiKueueConfig{}
	if err := r.Get(ctx, types.NamespacedName{Name: MultiKueueConfigName}, cfg); err != nil {
		t.Fatalf("failed to read back MultiKueueConfig: %v", err)
	}
	if len(cfg.Spec.Clusters) != 2 {
		t.Fatalf("config clusters = %v, want two distinct entries", cfg.Spec.Clusters)
	}
}

func TestEnsureKubeConfigSecret(t *testing.T) {
	ctx := context.Background()
	r := &MultiKueueReconciler{Client: newFakeClient(t)}

	cluster := newManagedCluster("cluster1", "https://api.cluster1.example.com:6443")
	source := newSecret("cluster1", "msa-token", map[string][]byte{
		"token":  []byte("s3cr3t-token"),
		"ca.crt": []byte("ca-bundle-bytes"),
	})

	target, err := r.ensureKubeConfigSecret(ctx, cluster, source)
	if err != nil {
		t.Fatalf("ensureKubeConfigSecret() error = %v", err)
	}
	if target.Name != "cluster1" {
		t.Errorf("secret name = %q, want %q", target.Name, "cluster1")
	}
	if target.Type != corev1.SecretTypeOpaque {
		t.Errorf("secret type = %q, want %q", target.Type, corev1.SecretTypeOpaque)
	}

	raw, ok := target.Data["kubeconfig"]
	if !ok {
		t.Fatalf("secret has no kubeconfig key, keys = %v", target.Data)
	}

	cfg, err := clientcmd.Load(raw)
	if err != nil {
		t.Fatalf("generated kubeconfig does not parse: %v", err)
	}
	if cfg.CurrentContext != "cluster1" {
		t.Errorf("current context = %q, want %q", cfg.CurrentContext, "cluster1")
	}
	if got := cfg.Clusters["cluster1"].Server; got != "https://api.cluster1.example.com:6443" {
		t.Errorf("server = %q, want the managed cluster URL", got)
	}
	if got := string(cfg.Clusters["cluster1"].CertificateAuthorityData); got != "ca-bundle-bytes" {
		t.Errorf("CA data = %q, want the source secret's ca.crt", got)
	}
	if got := cfg.AuthInfos["cluster1"].Token; got != "s3cr3t-token" {
		t.Errorf("token = %q, want the source secret's token", got)
	}
	if got := cfg.Contexts["cluster1"].Cluster; got != "cluster1" {
		t.Errorf("context cluster = %q, want %q", got, "cluster1")
	}

	if got := target.Labels["multikueue.kueue.x-k8s.io/managed-cluster"]; got != "cluster-cluster1" {
		t.Errorf("managed-cluster label = %q, want %q", got, "cluster-cluster1")
	}
	if _, ok := target.Annotations["multikueue.kueue.x-k8s.io/updated-at"]; !ok {
		t.Errorf("updated-at annotation is missing, annotations = %v", target.Annotations)
	}
}

func TestEnsureKubeConfigSecretNoClientConfigs(t *testing.T) {
	ctx := context.Background()
	r := &MultiKueueReconciler{Client: newFakeClient(t)}

	cluster := newManagedCluster("cluster1")
	source := newSecret("cluster1", "msa-token", map[string][]byte{"token": []byte("t")})

	target, err := r.ensureKubeConfigSecret(ctx, cluster, source)
	if err == nil {
		t.Fatal("ensureKubeConfigSecret() succeeded, want an error for a cluster with no client configs")
	}
	if target != nil {
		t.Errorf("returned secret = %+v, want nil on error", target)
	}
}

func TestEnsureKubeConfigSecretUpdatesExisting(t *testing.T) {
	ctx := context.Background()
	existing := newSecret(common.KueueNamespace, "cluster1", map[string][]byte{
		"kubeconfig": []byte("stale"),
	})
	existing.Labels = map[string]string{"keep-me": "yes"}
	r := &MultiKueueReconciler{Client: newFakeClient(t, existing)}

	cluster := newManagedCluster("cluster1", "https://api.cluster1.example.com:6443")
	source := newSecret("cluster1", "msa-token", map[string][]byte{"token": []byte("fresh")})

	target, err := r.ensureKubeConfigSecret(ctx, cluster, source)
	if err != nil {
		t.Fatalf("ensureKubeConfigSecret() error = %v", err)
	}
	if string(target.Data["kubeconfig"]) == "stale" {
		t.Error("kubeconfig was not refreshed")
	}
	if target.Labels["keep-me"] != "yes" {
		t.Error("pre-existing labels were dropped")
	}
	if target.Labels["multikueue.kueue.x-k8s.io/managed-cluster"] != "cluster-cluster1" {
		t.Error("managed-cluster label was not applied to the existing secret")
	}
}
