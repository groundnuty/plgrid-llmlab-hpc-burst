// Package v1alpha1 holds the custom resources of the plgrid-llmlab-hpc-burst
// control plane (HBEP-3, HBEP-11): Provider, PlatformProfile, ModelDeployment,
// Allocation and Instance, in the API group hpcburst.groundnuty.github.io.
//
// The CRD manifests under config/crd are generated from these types with
// hack/generate.sh. Admission rules are CEL rules in the markers; none needs a
// webhook.
//
// +kubebuilder:object:generate=true
// +groupName=hpcburst.groundnuty.github.io
package v1alpha1
