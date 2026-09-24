package reconcilers

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "open-cluster-management.io/api/cluster/v1"
	msav1beta1 "open-cluster-management.io/managed-serviceaccount/apis/authentication/v1beta1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestManagedServiceAccountToManagedCluster(t *testing.T) {
	r := &MultiKueueReconciler{}
	ctx := context.Background()

	tests := []struct {
		name string
		obj  client.Object
		want []reconcile.Request
	}{
		{
			name: "maps the MSA namespace to a cluster request",
			obj: &msav1beta1.ManagedServiceAccount{
				ObjectMeta: metav1.ObjectMeta{Namespace: "cluster1", Name: msaName},
			},
			want: []reconcile.Request{{NamespacedName: types.NamespacedName{Name: "cluster1"}}},
		},
		{
			name: "object of another type is ignored",
			obj:  &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "cluster1", Name: "x"}},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := r.managedServiceAccountToManagedCluster(ctx, tt.obj)

			if len(got) != len(tt.want) {
				t.Fatalf("got %d requests %v, want %d %v", len(got), got, len(tt.want), tt.want)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Errorf("request[%d] = %v, want %v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestSecretToManagedCluster(t *testing.T) {
	r := &MultiKueueReconciler{}
	ctx := context.Background()

	msaOwner := func(apiVersion, kind, name string) metav1.OwnerReference {
		return metav1.OwnerReference{APIVersion: apiVersion, Kind: kind, Name: name}
	}
	msaAPIVersion := msav1beta1.GroupVersion.String()

	tests := []struct {
		name string
		obj  client.Object
		want []reconcile.Request
	}{
		{
			name: "secret owned by the multikueue MSA maps to its namespace",
			obj: &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
				Namespace:       "cluster1",
				Name:            "token",
				OwnerReferences: []metav1.OwnerReference{msaOwner(msaAPIVersion, "ManagedServiceAccount", "multikueue")},
			}},
			want: []reconcile.Request{{NamespacedName: types.NamespacedName{Name: "cluster1"}}},
		},
		{
			name: "secret with no owners is ignored",
			obj: &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
				Namespace: "cluster1", Name: "token",
			}},
			want: nil,
		},
		{
			name: "owner with a different name is ignored",
			obj: &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
				Namespace:       "cluster1",
				Name:            "token",
				OwnerReferences: []metav1.OwnerReference{msaOwner(msaAPIVersion, "ManagedServiceAccount", "something-else")},
			}},
			want: nil,
		},
		{
			name: "owner with a different kind is ignored",
			obj: &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
				Namespace:       "cluster1",
				Name:            "token",
				OwnerReferences: []metav1.OwnerReference{msaOwner(msaAPIVersion, "ServiceAccount", "multikueue")},
			}},
			want: nil,
		},
		{
			name: "owner from a different API group is ignored",
			obj: &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
				Namespace:       "cluster1",
				Name:            "token",
				OwnerReferences: []metav1.OwnerReference{msaOwner("v1", "ManagedServiceAccount", "multikueue")},
			}},
			want: nil,
		},
		{
			name: "matching owner is found among several",
			obj: &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
				Namespace: "cluster1",
				Name:      "token",
				OwnerReferences: []metav1.OwnerReference{
					msaOwner("v1", "ConfigMap", "unrelated"),
					msaOwner(msaAPIVersion, "ManagedServiceAccount", "multikueue"),
				},
			}},
			want: []reconcile.Request{{NamespacedName: types.NamespacedName{Name: "cluster1"}}},
		},
		{
			name: "object of another type is ignored",
			obj:  &clusterv1.ManagedCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster1"}},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := r.secretToManagedCluster(ctx, tt.obj)

			if len(got) != len(tt.want) {
				t.Fatalf("got %d requests %v, want %d %v", len(got), got, len(tt.want), tt.want)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Errorf("request[%d] = %v, want %v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestReconcileSkipsLocalCluster(t *testing.T) {
	ctx := context.Background()
	local := newManagedCluster("local-cluster", "https://api.local.example.com:6443")
	local.Labels = map[string]string{"local-cluster": "true"}
	r := &MultiKueueReconciler{Client: newFakeClient(t, local)}

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "local-cluster"}})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Errorf("result = %+v, want no requeue for the local cluster", res)
	}

	// Nothing should have been provisioned for the local cluster.
	msa := &msav1beta1.ManagedServiceAccount{}
	key := types.NamespacedName{Namespace: "local-cluster", Name: msaName}
	if err := r.Get(ctx, key, msa); err == nil {
		t.Error("a ManagedServiceAccount was created for the local cluster")
	}
}

// TestReconcileLocalClusterLabelFalse checks that only the literal "true"
// suppresses reconciliation.
func TestReconcileLocalClusterLabelFalse(t *testing.T) {
	ctx := context.Background()
	cluster := newManagedCluster("cluster1", "https://api.cluster1.example.com:6443")
	cluster.Labels = map[string]string{"local-cluster": "false"}
	r := &MultiKueueReconciler{Client: newFakeClient(t, cluster)}

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "cluster1"}})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	// The MSA has no token secret yet, so reconcile parks on a requeue.
	if res.RequeueAfter != 10*time.Second {
		t.Errorf("requeue after = %v, want 10s while waiting for the MSA secret", res.RequeueAfter)
	}

	msa := &msav1beta1.ManagedServiceAccount{}
	key := types.NamespacedName{Namespace: "cluster1", Name: msaName}
	if err := r.Get(ctx, key, msa); err != nil {
		t.Errorf("ManagedServiceAccount was not created: %v", err)
	}
}

func TestReconcileMissingCluster(t *testing.T) {
	ctx := context.Background()
	r := &MultiKueueReconciler{Client: newFakeClient(t)}

	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "ghost"}})
	if err == nil {
		t.Fatal("Reconcile() returned nil, want an error for a missing ManagedCluster")
	}
}

func TestReconcileRequeuesUntilSecretExists(t *testing.T) {
	ctx := context.Background()
	cluster := newManagedCluster("cluster1", "https://api.cluster1.example.com:6443")
	r := &MultiKueueReconciler{Client: newFakeClient(t, cluster)}

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "cluster1"}})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if res.RequeueAfter != 10*time.Second {
		t.Errorf("requeue after = %v, want 10s", res.RequeueAfter)
	}
}

func TestEnsureMSACreates(t *testing.T) {
	ctx := context.Background()
	r := &MultiKueueReconciler{Client: newFakeClient(t)}

	if err := r.ensureMSA(ctx, "cluster1"); err != nil {
		t.Fatalf("ensureMSA() error = %v", err)
	}

	msa := &msav1beta1.ManagedServiceAccount{}
	key := types.NamespacedName{Namespace: "cluster1", Name: msaName}
	if err := r.Get(ctx, key, msa); err != nil {
		t.Fatalf("ManagedServiceAccount was not created: %v", err)
	}
	if msa.Spec.TTLSecondsAfterCreation == nil {
		t.Error("TTLSecondsAfterCreation is nil, want 86400")
	} else if *msa.Spec.TTLSecondsAfterCreation != 86400 {
		t.Errorf("TTLSecondsAfterCreation = %d, want 86400", *msa.Spec.TTLSecondsAfterCreation)
	}
	if got := msa.Spec.Rotation.Validity.Duration; got != 24*time.Hour {
		t.Errorf("rotation validity = %v, want 24h", got)
	}
}

// TestEnsureMSALeavesExistingAlone documents that an already-present MSA is
// returned untouched rather than reset to the default spec.
func TestEnsureMSALeavesExistingAlone(t *testing.T) {
	ctx := context.Background()
	existing := &msav1beta1.ManagedServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Namespace: "cluster1", Name: msaName},
		Spec: msav1beta1.ManagedServiceAccountSpec{
			Rotation: msav1beta1.ManagedServiceAccountRotation{
				Validity: metav1.Duration{Duration: time.Hour},
			},
		},
	}
	r := &MultiKueueReconciler{Client: newFakeClient(t, existing)}

	if err := r.ensureMSA(ctx, "cluster1"); err != nil {
		t.Fatalf("ensureMSA() error = %v", err)
	}

	msa := &msav1beta1.ManagedServiceAccount{}
	key := types.NamespacedName{Namespace: "cluster1", Name: msaName}
	if err := r.Get(ctx, key, msa); err != nil {
		t.Fatalf("failed to read back ManagedServiceAccount: %v", err)
	}
	if got := msa.Spec.Rotation.Validity.Duration; got != time.Hour {
		t.Errorf("rotation validity = %v, want the existing 1h to be preserved", got)
	}
}

func TestGetMSASecret(t *testing.T) {
	ctx := context.Background()

	msaWithRef := &msav1beta1.ManagedServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Namespace: "cluster1", Name: msaName},
		Status: msav1beta1.ManagedServiceAccountStatus{
			TokenSecretRef: &msav1beta1.SecretRef{Name: "msa-token"},
		},
	}
	msaNoRef := &msav1beta1.ManagedServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Namespace: "cluster2", Name: msaName},
	}
	secret := newSecret("cluster1", "msa-token", map[string][]byte{"token": []byte("t")})

	t.Run("returns the referenced secret", func(t *testing.T) {
		r := &MultiKueueReconciler{Client: newFakeClient(t, msaWithRef, secret)}

		got, err := r.getMSASecret(ctx, "cluster1")
		if err != nil {
			t.Fatalf("getMSASecret() error = %v", err)
		}
		if got == nil {
			t.Fatal("getMSASecret() = nil, want the token secret")
		}
		if got.Name != "msa-token" {
			t.Errorf("secret name = %q, want %q", got.Name, "msa-token")
		}
	})

	t.Run("no token secret ref yet", func(t *testing.T) {
		r := &MultiKueueReconciler{Client: newFakeClient(t, msaNoRef)}

		got, err := r.getMSASecret(ctx, "cluster2")
		if err != nil {
			t.Fatalf("getMSASecret() error = %v", err)
		}
		if got != nil {
			t.Errorf("getMSASecret() = %+v, want nil while the ref is unset", got)
		}
	})

	t.Run("referenced secret not created yet", func(t *testing.T) {
		r := &MultiKueueReconciler{Client: newFakeClient(t, msaWithRef)}

		got, err := r.getMSASecret(ctx, "cluster1")
		if err != nil {
			t.Fatalf("getMSASecret() error = %v, want nil for a not-yet-created secret", err)
		}
		if got != nil {
			t.Errorf("getMSASecret() = %+v, want nil", got)
		}
	})

	t.Run("missing managed service account is an error", func(t *testing.T) {
		r := &MultiKueueReconciler{Client: newFakeClient(t)}

		got, err := r.getMSASecret(ctx, "ghost")
		if err == nil {
			t.Fatal("getMSASecret() returned nil error, want a not-found error")
		}
		if got != nil {
			t.Errorf("getMSASecret() = %+v, want nil on error", got)
		}
	})
}

func TestNewSpokeClientNoClientConfigs(t *testing.T) {
	r := &MultiKueueReconciler{Client: newFakeClient(t)}
	cluster := newManagedCluster("cluster1")
	secret := newSecret("cluster1", "msa-token", map[string][]byte{"token": []byte("t")})

	got, err := r.newSpokeClient(cluster, secret)
	if err == nil {
		t.Fatal("newSpokeClient() succeeded, want an error for a cluster with no client config")
	}
	if got != nil {
		t.Errorf("newSpokeClient() = %+v, want nil on error", got)
	}
}

// testCAPEM returns a throwaway self-signed CA in PEM form. rest.Config
// validates CAData eagerly, so the bytes have to be a real PEM block.
func testCAPEM(t *testing.T) []byte {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestNewSpokeClientUsesClusterEndpointAndToken(t *testing.T) {
	r := &MultiKueueReconciler{Client: newFakeClient(t)}
	cluster := newManagedCluster("cluster1", "https://api.cluster1.example.com:6443")
	secret := newSecret("cluster1", "msa-token", map[string][]byte{
		"token":  []byte("s3cr3t"),
		"ca.crt": testCAPEM(t),
	})

	got, err := r.newSpokeClient(cluster, secret)
	if err != nil {
		t.Fatalf("newSpokeClient() error = %v", err)
	}
	if got == nil {
		t.Fatal("newSpokeClient() = nil, want a client")
	}
	// The spoke client must share the hub scheme so the bootstrap types resolve.
	if got.Scheme() != r.Scheme() {
		t.Error("spoke client does not use the reconciler's scheme")
	}
}

func TestEnsureBootstrapOperatorsNoClientConfig(t *testing.T) {
	ctx := context.Background()
	r := &MultiKueueReconciler{Client: newFakeClient(t)}
	cluster := newManagedCluster("cluster1")
	secret := newSecret("cluster1", "msa-token", map[string][]byte{"token": []byte("t")})

	if err := r.ensureBootstrapOperators(ctx, cluster, secret); err == nil {
		t.Fatal("ensureBootstrapOperators() succeeded, want an error when the spoke client cannot be built")
	}
}
