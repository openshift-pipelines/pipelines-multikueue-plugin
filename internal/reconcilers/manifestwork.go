package reconcilers

import (
	"context"
	"fmt"

	"github.com/openshift-pipelines/pipelines-multikueue-plugin/internal/manifest"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"

	workv1 "open-cluster-management.io/api/work/v1"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	manifestWorkName        = "multikueue-bootstrap"
	clusterRoleName         = "multikueue"
	serviceAccountName      = "multikueue"
	serviceAccountNamespace = "open-cluster-management-agent-addon" // Update if different
)

// +kubebuilder:rbac:groups="work.open-cluster-management.io",resources=manifestworks,verbs=get;list;create;patch;update;watch

// ensureBootstrapManifestWork creates/updates the ManifestWork that bootstraps
// RBAC on the managed cluster.
func (r *MultiKueueReconciler) ensureBootstrapManifestWork(ctx context.Context, managedClusterName string) error {
	log := ctrl.LoggerFrom(ctx)
	log.Info("Reconciling bootstrap manifestwork")
	manifestList, err := r.buildBootstrapManifests()
	if err != nil {
		return err
	}

	mw := &workv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      manifestWorkName,
			Namespace: managedClusterName, // ManagedCluster namespace on hub
		},
	}

	log.Info("Creating bootstrap manifestwork")
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, mw, func() error {
		mw.Spec.Workload.Manifests = manifestList
		return nil
	})

	if err != nil {
		return err
	}

	log.Info("ManifestWork reconciled",
		"name", mw.Name,
		"cluster", managedClusterName)

	return nil
}

func (r *MultiKueueReconciler) buildBootstrapManifests() ([]workv1.Manifest, error) {
	role, err := r.manifest("manifests/cluster-role.yaml")
	if err != nil {
		return nil, err
	}
	roleBinding, err := r.manifest("manifests/cluster-role-binding.yaml")
	if err != nil {
		return nil, err
	}

	manifestList := []workv1.Manifest{
		role,
		roleBinding,
	}

	return manifestList, nil
}

func (r *MultiKueueReconciler) manifest(file string) (workv1.Manifest, error) {

	data, err := manifest.ManifestFS.ReadFile(file)
	if err != nil {
		return workv1.Manifest{}, err
	}

	fmt.Printf("Yaml Data: %s\n", string(data))

	jsonData, err := yaml.YAMLToJSON(data)
	if err != nil {
		return workv1.Manifest{}, err
	}

	fmt.Printf("JSON Data: %s\n", string(jsonData))

	return workv1.Manifest{
		RawExtension: runtime.RawExtension{
			Raw: jsonData,
		},
	}, nil

}
