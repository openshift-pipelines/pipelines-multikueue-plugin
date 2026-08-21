package main

import (
	"flag"
	"os"
	"reflect"

	"github.com/openshift-pipelines/pipelines-multikueue-plugin/internal/reconcilers"
	// Kubernetes core schemes
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	// Controller runtime
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

type ControllerFlags struct {
	ProbeAddr string
}

func init() {
	// Register standard Kubernetes API types to the scheme
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(rbacv1.AddToScheme(scheme))
}

func main() {
	// Configure logging
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// 1. Initialize the Manager
	// GetConfigOrDie() automatically uses ~/.kube/config if running locally outside a pod
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		HealthProbeBindAddress: ":8081",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// 2. Instantiate and register your Reconciler
	addRunnableOrDie(mgr, &reconcilers.ClusterBootstrap{
		Client: mgr.GetClient(),
	})
	addReadyAndHealthChecksToMgrOrDie(mgr)
	addMultiKueueReconciler(mgr)

	// 3. Start the Manager
	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

func addMultiKueueReconciler(mgr ctrl.Manager) {

	reconciler := &reconcilers.MultiKueueReconciler{
		Client: mgr.GetClient(),
	}

	if err := reconciler.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "SpokeCredential")
		os.Exit(1)
	}
}

func addReadyAndHealthChecksToMgrOrDie(mgr manager.Manager) {
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}
}

func addRunnableOrDie(mgr ctrl.Manager, runnable manager.Runnable) {
	if reflect.ValueOf(runnable).IsNil() {
		return
	}
	if err := mgr.Add(runnable); err != nil {
		os.Exit(1)
	}
}
