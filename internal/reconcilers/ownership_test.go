package reconcilers

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSetAddonOwnership(t *testing.T) {
	t.Run("adds the label to an unlabelled object", func(t *testing.T) {
		ns := &corev1.Namespace{}

		setAddonOwnership(ns)

		if !isAddonOwned(ns) {
			t.Errorf("labels = %v, want the object marked as addon owned", ns.Labels)
		}
	})

	t.Run("keeps labels the object already carries", func(t *testing.T) {
		ns := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"team": "pipelines"}},
		}

		setAddonOwnership(ns)

		if !isAddonOwned(ns) {
			t.Errorf("labels = %v, want the object marked as addon owned", ns.Labels)
		}
		if ns.Labels["team"] != "pipelines" {
			t.Errorf("labels = %v, want the pre-existing team label kept", ns.Labels)
		}
	})
}

func TestIsAddonOwned(t *testing.T) {
	for _, tc := range []struct {
		name   string
		labels map[string]string
		want   bool
	}{
		{"no labels", nil, false},
		{"unrelated labels", map[string]string{"team": "pipelines"}, false},
		{"managed by something else", map[string]string{managedByLabel: "olm"}, false},
		{"managed by the addon", map[string]string{managedByLabel: managedByValue}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Labels: tc.labels}}
			if got := isAddonOwned(ns); got != tc.want {
				t.Errorf("isAddonOwned() = %v, want %v", got, tc.want)
			}
		})
	}
}
