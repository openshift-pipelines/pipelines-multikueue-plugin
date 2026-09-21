package reconcilers

import (
	"context"
	"encoding/json"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	workv1 "open-cluster-management.io/api/work/v1"
)

// decodeManifest unmarshals the raw JSON carried by a ManifestWork manifest.
func decodeManifest(t *testing.T, m workv1.Manifest) map[string]any {
	t.Helper()

	var obj map[string]any
	if err := json.Unmarshal(m.Raw, &obj); err != nil {
		t.Fatalf("manifest is not valid JSON: %v (raw=%s)", err, string(m.Raw))
	}
	return obj
}

func TestManifest(t *testing.T) {
	tests := []struct {
		name     string
		file     string
		wantKind string
		wantName string
	}{
		{
			name:     "cluster role",
			file:     "manifests/cluster-role.yaml",
			wantKind: "ClusterRole",
			wantName: "multikueue",
		},
		{
			name:     "cluster role binding",
			file:     "manifests/cluster-role-binding.yaml",
			wantKind: "ClusterRoleBinding",
			wantName: "multikueue",
		},
	}

	r := &MultiKueueReconciler{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := r.manifest(tt.file)
			if err != nil {
				t.Fatalf("manifest(%q) error = %v", tt.file, err)
			}

			obj := decodeManifest(t, got)
			if obj["kind"] != tt.wantKind {
				t.Errorf("kind = %v, want %q", obj["kind"], tt.wantKind)
			}
			if obj["apiVersion"] != "rbac.authorization.k8s.io/v1" {
				t.Errorf("apiVersion = %v, want %q", obj["apiVersion"], "rbac.authorization.k8s.io/v1")
			}

			meta, ok := obj["metadata"].(map[string]any)
			if !ok {
				t.Fatalf("metadata is not an object: %v", obj["metadata"])
			}
			if meta["name"] != tt.wantName {
				t.Errorf("metadata.name = %v, want %q", meta["name"], tt.wantName)
			}
		})
	}
}

func TestManifestMissingFile(t *testing.T) {
	r := &MultiKueueReconciler{}

	got, err := r.manifest("manifests/does-not-exist.yaml")
	if err == nil {
		t.Fatal("manifest() succeeded for a missing file, want an error")
	}
	if got.Raw != nil {
		t.Errorf("returned manifest = %+v, want the zero value on error", got)
	}
}

func TestBuildBootstrapManifests(t *testing.T) {
	r := &MultiKueueReconciler{}

	manifests, err := r.buildBootstrapManifests()
	if err != nil {
		t.Fatalf("buildBootstrapManifests() error = %v", err)
	}
	if len(manifests) != 2 {
		t.Fatalf("got %d manifests, want 2", len(manifests))
	}

	wantKinds := []string{"ClusterRole", "ClusterRoleBinding"}
	for i, want := range wantKinds {
		if got := decodeManifest(t, manifests[i])["kind"]; got != want {
			t.Errorf("manifest[%d] kind = %v, want %q", i, got, want)
		}
	}
}

// TestBuildBootstrapManifestsBindingTargetsRole guards the invariant that the
// binding actually references the role shipped next to it.
func TestBuildBootstrapManifestsBindingTargetsRole(t *testing.T) {
	r := &MultiKueueReconciler{}

	manifests, err := r.buildBootstrapManifests()
	if err != nil {
		t.Fatalf("buildBootstrapManifests() error = %v", err)
	}

	role := decodeManifest(t, manifests[0])
	binding := decodeManifest(t, manifests[1])

	roleName := role["metadata"].(map[string]any)["name"]
	roleRef, ok := binding["roleRef"].(map[string]any)
	if !ok {
		t.Fatalf("binding has no roleRef: %v", binding)
	}
	if roleRef["name"] != roleName {
		t.Errorf("binding roleRef.name = %v, want the bundled role %v", roleRef["name"], roleName)
	}
	if roleRef["kind"] != "ClusterRole" {
		t.Errorf("binding roleRef.kind = %v, want %q", roleRef["kind"], "ClusterRole")
	}
}

func TestEnsureBootstrapManifestWork(t *testing.T) {
	ctx := context.Background()
	r := &MultiKueueReconciler{Client: newFakeClient(t)}

	if err := r.ensureBootstrapManifestWork(ctx, "cluster1"); err != nil {
		t.Fatalf("ensureBootstrapManifestWork() error = %v", err)
	}

	mw := &workv1.ManifestWork{}
	key := types.NamespacedName{Namespace: "cluster1", Name: manifestWorkName}
	if err := r.Get(ctx, key, mw); err != nil {
		t.Fatalf("ManifestWork was not created in the managed cluster namespace: %v", err)
	}
	if got := len(mw.Spec.Workload.Manifests); got != 2 {
		t.Errorf("got %d workload manifests, want 2", got)
	}
}

func TestEnsureBootstrapManifestWorkReplacesWorkload(t *testing.T) {
	ctx := context.Background()
	existing := &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{Namespace: "cluster1", Name: manifestWorkName},
		Spec: workv1.ManifestWorkSpec{
			Workload: workv1.ManifestsTemplate{
				Manifests: []workv1.Manifest{{
					RawExtension: runtime.RawExtension{Raw: []byte(`{"kind":"Stale"}`)},
				}},
			},
		},
	}
	r := &MultiKueueReconciler{Client: newFakeClient(t, existing)}

	if err := r.ensureBootstrapManifestWork(ctx, "cluster1"); err != nil {
		t.Fatalf("ensureBootstrapManifestWork() error = %v", err)
	}

	mw := &workv1.ManifestWork{}
	key := types.NamespacedName{Namespace: "cluster1", Name: manifestWorkName}
	if err := r.Get(ctx, key, mw); err != nil {
		t.Fatalf("failed to read back ManifestWork: %v", err)
	}
	if got := len(mw.Spec.Workload.Manifests); got != 2 {
		t.Fatalf("got %d workload manifests, want the stale one replaced by 2", got)
	}
	if got := decodeManifest(t, mw.Spec.Workload.Manifests[0])["kind"]; got == "Stale" {
		t.Error("stale manifest survived the update")
	}
}

// TestEnsureBootstrapManifestWorkPerCluster checks that each managed cluster
// gets its own ManifestWork in its own namespace.
func TestEnsureBootstrapManifestWorkPerCluster(t *testing.T) {
	ctx := context.Background()
	r := &MultiKueueReconciler{Client: newFakeClient(t)}

	for _, cluster := range []string{"cluster1", "cluster2"} {
		if err := r.ensureBootstrapManifestWork(ctx, cluster); err != nil {
			t.Fatalf("ensureBootstrapManifestWork(%q) error = %v", cluster, err)
		}
	}

	list := &workv1.ManifestWorkList{}
	if err := r.List(ctx, list); err != nil {
		t.Fatalf("failed to list ManifestWorks: %v", err)
	}
	if len(list.Items) != 2 {
		t.Fatalf("got %d ManifestWorks, want one per cluster", len(list.Items))
	}
	for _, mw := range list.Items {
		if mw.Name != manifestWorkName {
			t.Errorf("ManifestWork name = %q, want %q", mw.Name, manifestWorkName)
		}
	}
}
