package common

import (
	"fmt"
	"os"
)

const (
	// ClusterProxyURLEnv is the environment variable for cluster proxy URL
	ClusterProxyURLEnv = "CLUSTER_PROXY_URL"
	// KueueNamespaceEnv is the environment variable for kueue installed namespace
	KueueNamespaceEnv = "KUEUE_NAMESPACE"
	// ClusterProxyImpersonationEnv is the environment variable for enabling cluster proxy impersonation
	ClusterProxyImpersonationEnv = "CLUSTER_PROXY_IMPERSONATION_ENABLED"
	// EnableClusterProfileEnv is the environment variable for enabling ClusterProfile mode
	EnableClusterProfileEnv = "ENABLE_CLUSTERPROFILE"

	// PipelinesOperatorChannelEnv is the environment variable for the OLM
	// channel the OpenShift Pipelines Operator is subscribed to
	PipelinesOperatorChannelEnv = "PIPELINES_OPERATOR_CHANNEL"
	// KueueOperatorChannelEnv is the environment variable for the OLM channel
	// the Kueue Operator is subscribed to
	KueueOperatorChannelEnv = "KUEUE_OPERATOR_CHANNEL"
	// CertManagerOperatorChannelEnv is the environment variable for the OLM
	// channel the cert-manager Operator is subscribed to
	CertManagerOperatorChannelEnv = "CERT_MANAGER_OPERATOR_CHANNEL"

	// PipelinesOperatorNamespaceEnv is the environment variable for the
	// namespace the OpenShift Pipelines Operator is subscribed in
	PipelinesOperatorNamespaceEnv = "PIPELINES_OPERATOR_NAMESPACE"
	// CertManagerOperatorNamespaceEnv is the environment variable for the
	// namespace the cert-manager Operator is subscribed in
	CertManagerOperatorNamespaceEnv = "CERT_MANAGER_OPERATOR_NAMESPACE"

	// OLM package names of the operators the addon installs. These name
	// catalog content, not anything the addon owns, so they are fixed rather
	// than configurable.
	PipelinesOperatorPackage   = "openshift-pipelines-operator-rh"
	KueueOperatorPackage       = "kueue-operator"
	CertManagerOperatorPackage = "openshift-cert-manager-operator"

	// Channels each operator is subscribed to when the matching environment
	// variable is unset.
	DefaultPipelinesOperatorChannel   = "pipelines-1.22"
	DefaultKueueOperatorChannel       = "stable-v1.4"
	DefaultCertManagerOperatorChannel = "stable-v1"

	// Namespaces each operator is subscribed in when the matching environment
	// variable is unset.
	DefaultKueueOperatorNamespace       = "openshift-kueue-operator"
	DefaultPipelinesOperatorNamespace   = "openshift-operators"
	DefaultCertManagerOperatorNamespace = "cert-manager-operator"

	// MultiKueueResourceName is the name used for MultiKueue-related secrets
	MultiKueueResourceName = "multikueue"

	// AdmissionCheckControllerName is the name of the admission check controller
	AdmissionCheckControllerName = "open-cluster-management.io/placement"
)

// The operator getters below read the environment on every call rather than
// caching it, so a channel or namespace changed on the controller Deployment
// takes effect on the next reconcile.

// GetPipelinesOperatorChannel returns the OLM channel the OpenShift Pipelines
// Operator is subscribed to.
func GetPipelinesOperatorChannel() string {
	return GetEnvOrDefault(PipelinesOperatorChannelEnv, DefaultPipelinesOperatorChannel)
}

// GetKueueOperatorChannel returns the OLM channel the Kueue Operator is
// subscribed to.
func GetKueueOperatorChannel() string {
	return GetEnvOrDefault(KueueOperatorChannelEnv, DefaultKueueOperatorChannel)
}

// GetCertManagerOperatorChannel returns the OLM channel the cert-manager
// Operator is subscribed to.
func GetCertManagerOperatorChannel() string {
	return GetEnvOrDefault(CertManagerOperatorChannelEnv, DefaultCertManagerOperatorChannel)
}

// GetPipelinesOperatorNamespace returns the namespace the OpenShift Pipelines
// Operator is subscribed in.
func GetPipelinesOperatorNamespace() string {
	return GetEnvOrDefault(PipelinesOperatorNamespaceEnv, DefaultPipelinesOperatorNamespace)
}

// GetKueueOperatorNamespace returns the namespace Kueue components are
// deployed into.
func GetKueueOperatorNamespace() string {
	return GetEnvOrDefault(KueueNamespaceEnv, DefaultKueueOperatorNamespace)
}

// GetCertManagerOperatorNamespace returns the namespace the cert-manager
// Operator is subscribed in.
func GetCertManagerOperatorNamespace() string {
	return GetEnvOrDefault(CertManagerOperatorNamespaceEnv, DefaultCertManagerOperatorNamespace)
}

// GetEnvOrDefault returns the value of envName, falling back to defaultValue
// when the variable is unset or empty.
func GetEnvOrDefault(envName, defaultValue string) string {
	if value := os.Getenv(envName); value != "" {
		return value
	}
	return defaultValue
}

// IsEnvSet reports whether envName carries a value, which distinguishes a
// setting someone configured deliberately from one that fell back to its
// compiled-in default.
func IsEnvSet(envName string) bool {
	return os.Getenv(envName) != ""
}

// Get MultiKueue Kubeconfig Secret name
func GetMultiKueueSecretName(clusterName string) string {
	return fmt.Sprintf("%s-%s", MultiKueueResourceName, clusterName)
}

func IsImpersonationMode() bool {
	return os.Getenv(ClusterProxyImpersonationEnv) == "true"
}

// IsClusterProfileEnabled returns true if ClusterProfile mode is enabled
func IsClusterProfileEnabled() bool {
	return os.Getenv(EnableClusterProfileEnv) == "true"
}
