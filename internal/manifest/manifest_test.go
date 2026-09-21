package manifest

import (
	"io/fs"
	"testing"

	"sigs.k8s.io/yaml"
)

// wantManifests lists every file the embedded FS is expected to carry, along
// with the identity the ManifestWork bootstrap relies on.
var wantManifests = []struct {
	file       string
	kind       string
	name       string
	apiVersion string
}{
	{
		file:       "manifests/cluster-role.yaml",
		kind:       "ClusterRole",
		name:       "multikueue",
		apiVersion: "rbac.authorization.k8s.io/v1",
	},
	{
		file:       "manifests/cluster-role-binding.yaml",
		kind:       "ClusterRoleBinding",
		name:       "multikueue",
		apiVersion: "rbac.authorization.k8s.io/v1",
	},
}

func TestManifestFSContents(t *testing.T) {
	for _, tt := range wantManifests {
		t.Run(tt.file, func(t *testing.T) {
			data, err := ManifestFS.ReadFile(tt.file)
			if err != nil {
				t.Fatalf("ReadFile(%q) error = %v", tt.file, err)
			}
			if len(data) == 0 {
				t.Fatalf("%s is empty", tt.file)
			}

			var obj struct {
				APIVersion string `json:"apiVersion"`
				Kind       string `json:"kind"`
				Metadata   struct {
					Name string `json:"name"`
				} `json:"metadata"`
			}
			if err := yaml.Unmarshal(data, &obj); err != nil {
				t.Fatalf("%s is not valid YAML: %v", tt.file, err)
			}

			if obj.APIVersion != tt.apiVersion {
				t.Errorf("apiVersion = %q, want %q", obj.APIVersion, tt.apiVersion)
			}
			if obj.Kind != tt.kind {
				t.Errorf("kind = %q, want %q", obj.Kind, tt.kind)
			}
			if obj.Metadata.Name != tt.name {
				t.Errorf("metadata.name = %q, want %q", obj.Metadata.Name, tt.name)
			}
		})
	}
}

// TestManifestFSHasNoExtraFiles keeps the embed pattern and the bootstrap in
// sync: a new manifest dropped into the directory is embedded automatically but
// is not wired into buildBootstrapManifests, so it should be a deliberate change.
func TestManifestFSHasNoExtraFiles(t *testing.T) {
	entries, err := fs.ReadDir(ManifestFS, "manifests")
	if err != nil {
		t.Fatalf("ReadDir error = %v", err)
	}

	got := make(map[string]bool, len(entries))
	for _, e := range entries {
		got["manifests/"+e.Name()] = true
	}

	for _, tt := range wantManifests {
		if !got[tt.file] {
			t.Errorf("%s is missing from the embedded FS", tt.file)
		}
		delete(got, tt.file)
	}
	for extra := range got {
		t.Errorf("unexpected embedded file %s; wire it into buildBootstrapManifests or update this test", extra)
	}
}

func TestManifestFSMissingFile(t *testing.T) {
	if _, err := ManifestFS.ReadFile("manifests/nope.yaml"); err == nil {
		t.Fatal("ReadFile succeeded for a missing file, want an error")
	}
}
