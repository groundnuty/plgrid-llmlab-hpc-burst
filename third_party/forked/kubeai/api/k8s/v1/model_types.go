/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Modified for plgrid-llmlab-hpc-burst (spike S10): trimmed copy of KubeAI's
// api/k8s/v1/model_types.go keeping only the load-balancing types that
// internal/loadbalancer/group.go uses. Field definitions and comments are
// unchanged; kubebuilder markers are kept. See ../../../ORIGIN.md.

package v1

type LoadBalancing struct {
	// +kubebuilder:validation:Optional
	// +kubebuilder:default=LeastLoad
	Strategy LoadBalancingStrategy `json:"strategy,omitempty"`
	// +kubebuilder:validation:Optional
	// +kubebuilder:default={}
	PrefixHash PrefixHash `json:"prefixHash,omitempty"`
}

// +kubebuilder:validation:Enum=LeastLoad;PrefixHash
type LoadBalancingStrategy string

const (
	LeastLoadStrategy  LoadBalancingStrategy = "LeastLoad"
	PrefixHashStrategy LoadBalancingStrategy = "PrefixHash"
)

type PrefixHash struct {
	// MeanLoadPercentage is the percentage that any given endpoint's load must not exceed
	// over the mean load of all endpoints in the hash ring. Defaults to 125% which is
	// a widely accepted value for the Consistent Hashing with Bounded Loads algorithm.
	// +kubebuilder:default=125
	// +kubebuilder:validation:Optional
	// +kubebuilder:validation:Minimum=100
	MeanLoadPercentage int `json:"meanLoadFactor,omitempty"`
	// Replication is the number of replicas of each endpoint on the hash ring.
	// Higher values will result in a more even distribution of load but will
	// decrease lookup performance.
	// +kubebuilder:validation:XValidation:rule="self == oldSelf", message="replication is immutable."
	// +kubebuilder:default=256
	// +kubebuilder:validation:Optional
	Replication int `json:"replication,omitempty"`
	// PrefixCharLength is the number of characters to count when building the prefix to hash.
	// +kubebuilder:validation:Optional
	// +kubebuilder:default=100
	PrefixCharLength int `json:"prefixCharLength,omitempty"`
}
