package common

import (
	"os"
	"testing"
)

func TestGetKueueNamespace(t *testing.T) {
	tests := []struct {
		name     string
		envSet   bool
		envValue string
		want     string
	}{
		{
			name:   "unset falls back to default",
			envSet: false,
			want:   "openshift-kueue-operator",
		},
		{
			name:     "empty falls back to default",
			envSet:   true,
			envValue: "",
			want:     "openshift-kueue-operator",
		},
		{
			name:     "override is used",
			envSet:   true,
			envValue: "kueue-system",
			want:     "kueue-system",
		},
		{
			name:     "whitespace is not trimmed",
			envSet:   true,
			envValue: " ",
			want:     " ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envSet {
				t.Setenv(KueueNamespaceEnv, tt.envValue)
			} else {
				unsetEnv(t, KueueNamespaceEnv)
			}

			if got := getKueueNamespace(); got != tt.want {
				t.Errorf("getKueueNamespace() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestKueueNamespaceDefault(t *testing.T) {
	// KueueNamespace is resolved once at package initialization, so it only
	// reflects the environment the test binary was started with.
	want := os.Getenv(KueueNamespaceEnv)
	if want == "" {
		want = "openshift-kueue-operator"
	}
	if KueueNamespace != want {
		t.Errorf("KueueNamespace = %q, want %q", KueueNamespace, want)
	}
}

func TestGetMultiKueueSecretName(t *testing.T) {
	tests := []struct {
		name        string
		clusterName string
		want        string
	}{
		{
			name:        "regular cluster name",
			clusterName: "cluster1",
			want:        "multikueue-cluster1",
		},
		{
			name:        "empty cluster name",
			clusterName: "",
			want:        "multikueue-",
		},
		{
			name:        "cluster name with dashes",
			clusterName: "my-spoke-cluster",
			want:        "multikueue-my-spoke-cluster",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetMultiKueueSecretName(tt.clusterName); got != tt.want {
				t.Errorf("GetMultiKueueSecretName(%q) = %q, want %q", tt.clusterName, got, tt.want)
			}
		})
	}
}

func TestGetMultiKueueSecretNameUsesResourceName(t *testing.T) {
	original := MultiKueueResourceName
	t.Cleanup(func() { MultiKueueResourceName = original })

	MultiKueueResourceName = "custom"
	if got, want := GetMultiKueueSecretName("cluster1"), "custom-cluster1"; got != want {
		t.Errorf("GetMultiKueueSecretName() = %q, want %q", got, want)
	}
}

func TestIsImpersonationMode(t *testing.T) {
	runBoolEnvTests(t, ClusterProxyImpersonationEnv, IsImpersonationMode)
}

func TestIsClusterProfileEnabled(t *testing.T) {
	runBoolEnvTests(t, EnableClusterProfileEnv, IsClusterProfileEnabled)
}

// runBoolEnvTests exercises a predicate that is true only when envName is
// exactly "true".
func runBoolEnvTests(t *testing.T, envName string, fn func() bool) {
	t.Helper()

	tests := []struct {
		name     string
		envSet   bool
		envValue string
		want     bool
	}{
		{name: "unset", envSet: false, want: false},
		{name: "empty", envSet: true, envValue: "", want: false},
		{name: "true", envSet: true, envValue: "true", want: true},
		{name: "false", envSet: true, envValue: "false", want: false},
		{name: "uppercase TRUE is not accepted", envSet: true, envValue: "TRUE", want: false},
		{name: "mixed case True is not accepted", envSet: true, envValue: "True", want: false},
		{name: "1 is not accepted", envSet: true, envValue: "1", want: false},
		{name: "padded true is not accepted", envSet: true, envValue: " true ", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envSet {
				t.Setenv(envName, tt.envValue)
			} else {
				unsetEnv(t, envName)
			}

			if got := fn(); got != tt.want {
				t.Errorf("%s=%q: got %v, want %v", envName, tt.envValue, got, tt.want)
			}
		})
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
	t.Cleanup(func() { os.Setenv(envName, original) })
	if err := os.Unsetenv(envName); err != nil {
		t.Fatalf("failed to unset %s: %v", envName, err)
	}
}

func TestConstants(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"ClusterProxyURLEnv", ClusterProxyURLEnv, "CLUSTER_PROXY_URL"},
		{"KueueNamespaceEnv", KueueNamespaceEnv, "KUEUE_NAMESPACE"},
		{"ClusterProxyImpersonationEnv", ClusterProxyImpersonationEnv, "CLUSTER_PROXY_IMPERSONATION_ENABLED"},
		{"EnableClusterProfileEnv", EnableClusterProfileEnv, "ENABLE_CLUSTERPROFILE"},
		{"MultiKueueResourceName", MultiKueueResourceName, "multikueue"},
		{"AdmissionCheckControllerName", AdmissionCheckControllerName, "open-cluster-management.io/placement"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("%s = %q, want %q", tt.name, tt.got, tt.want)
			}
		})
	}
}
