package reconcilers

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// managedByLabel records which component created a resource. The addon
	// upgrades only the operators it installed itself, and this label is how it
	// tells those apart from operators a cluster already had.
	//
	// A Kubernetes ownerReference cannot carry this: the controller runs on the
	// hub while most of these resources live on a managed cluster, and an
	// ownerReference is only resolvable inside a single cluster.
	managedByLabel = "app.kubernetes.io/managed-by"

	// managedByValue is the managedByLabel value stamped on addon resources.
	managedByValue = "pipelines-multikueue-plugin"
)

// setAddonOwnership marks obj as installed by the addon. It is applied when a
// resource is created, never when an existing one is adopted.
func setAddonOwnership(obj metav1.Object) {
	labels := obj.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[managedByLabel] = managedByValue
	obj.SetLabels(labels)
}

// isAddonOwned reports whether obj was installed by the addon.
func isAddonOwned(obj metav1.Object) bool {
	return obj.GetLabels()[managedByLabel] == managedByValue
}
