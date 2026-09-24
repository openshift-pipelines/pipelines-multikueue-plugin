package main

import (
	"testing"

	"github.com/openshift-pipelines/pipelines-multikueue-plugin/internal/reconcilers"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// stubManager implements only the handful of manager.Manager methods the
// wiring helpers touch. The embedded interface supplies the rest, so an
// unexpected call panics instead of silently succeeding.
//
// The helpers call os.Exit(1) when the manager returns an error, so only the
// success paths are exercised here.
type stubManager struct {
	manager.Manager

	added        []manager.Runnable
	healthChecks []string
	readyChecks  []string
}

func (s *stubManager) Add(r manager.Runnable) error {
	s.added = append(s.added, r)
	return nil
}

func (s *stubManager) AddHealthzCheck(name string, _ healthz.Checker) error {
	s.healthChecks = append(s.healthChecks, name)
	return nil
}

func (s *stubManager) AddReadyzCheck(name string, _ healthz.Checker) error {
	s.readyChecks = append(s.readyChecks, name)
	return nil
}

// TestSchemeRegistersRequiredTypes checks what init() put in the global scheme.
// Without these the manager's client cannot read the objects the reconcilers use.
func TestSchemeRegistersRequiredTypes(t *testing.T) {
	for _, kind := range []string{"Secret", "Namespace", "ConfigMap"} {
		if !scheme.Recognizes(corev1.SchemeGroupVersion.WithKind(kind)) {
			t.Errorf("core/v1 %s is not registered in the scheme", kind)
		}
	}
	for _, kind := range []string{"ClusterRole", "ClusterRoleBinding"} {
		if !scheme.Recognizes(rbacv1.SchemeGroupVersion.WithKind(kind)) {
			t.Errorf("rbac/v1 %s is not registered in the scheme", kind)
		}
	}
}

func TestAddRunnableOrDieAddsRunnable(t *testing.T) {
	mgr := &stubManager{}
	runnable := &reconcilers.ClusterBootstrap{}

	addRunnableOrDie(mgr, runnable)

	if len(mgr.added) != 1 {
		t.Fatalf("got %d runnables added, want 1", len(mgr.added))
	}
	if mgr.added[0] != runnable {
		t.Errorf("added runnable = %v, want %v", mgr.added[0], runnable)
	}
}

// TestAddRunnableOrDieSkipsNil covers the typed-nil guard: a nil
// *ClusterBootstrap is a non-nil manager.Runnable interface value, so the
// reflect check is what stops it from reaching the manager.
func TestAddRunnableOrDieSkipsNil(t *testing.T) {
	mgr := &stubManager{}
	var runnable *reconcilers.ClusterBootstrap

	addRunnableOrDie(mgr, runnable)

	if len(mgr.added) != 0 {
		t.Errorf("got %d runnables added, want the nil runnable to be skipped", len(mgr.added))
	}
}

func TestAddReadyAndHealthChecksToMgrOrDie(t *testing.T) {
	mgr := &stubManager{}

	addReadyAndHealthChecksToMgrOrDie(mgr)

	if len(mgr.healthChecks) != 1 || mgr.healthChecks[0] != "healthz" {
		t.Errorf("health checks = %v, want [healthz]", mgr.healthChecks)
	}
	if len(mgr.readyChecks) != 1 || mgr.readyChecks[0] != "readyz" {
		t.Errorf("ready checks = %v, want [readyz]", mgr.readyChecks)
	}
}
