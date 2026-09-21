package common

import (
	"os"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	KueueNamespaceEnvName = "KUEUE_NAMESPACE"
	DefaultKueueNamespace = "kueue-system"
)

func GetKubeClientAndConfig() (kubernetes.Interface, *rest.Config, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		// Fallback to kubeconfig file for local development
		kubeconfig := os.Getenv("KUBECONFIG")
		if kubeconfig == "" {
			kubeconfig = os.Getenv("HOME") + "/.kube/config"
		}
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, nil, err
		}
	}

	hubKubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, nil, err
	}

	return hubKubeClient, cfg, nil
}
