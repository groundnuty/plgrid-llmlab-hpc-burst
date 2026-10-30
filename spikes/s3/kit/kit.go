// Package kit is the shared test kit of spike S3: a small CRD set, helpers to
// install it, and one tiny controller-runtime controller. Custom resources are
// handled as unstructured objects, so no code generation is needed.
package kit

import (
	corev1 "k8s.io/api/core/v1"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	Group   = "s3.spike.invalid"
	Version = "v1alpha1"

	// LabelFleet marks a Worker with the Fleet it belongs to (inventory by selector).
	LabelFleet = Group + "/fleet"
	// LabelController set to "off" makes the controller ignore a Fleet. The cascade
	// test uses it so that only the host's garbage collector can remove children.
	LabelController = Group + "/controller"
	// FinalizerCancel stands for "cancel the provider job before the object goes" (HBEP-11).
	FinalizerCancel = Group + "/cancel-job"
)

var (
	FleetGVK  = schema.GroupVersionKind{Group: Group, Version: Version, Kind: "Fleet"}
	WorkerGVK = schema.GroupVersionKind{Group: Group, Version: Version, Kind: "Worker"}
	SlotGVK   = schema.GroupVersionKind{Group: Group, Version: Version, Kind: "Slot"}
)

// New returns an empty unstructured object of the given kind.
func New(gvk schema.GroupVersionKind) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	return u
}

// NewList returns an empty unstructured list for the given item kind.
func NewList(gvk schema.GroupVersionKind) *unstructured.UnstructuredList {
	l := &unstructured.UnstructuredList{}
	l.SetGroupVersionKind(schema.GroupVersionKind{Group: gvk.Group, Version: gvk.Version, Kind: gvk.Kind + "List"})
	return l
}

// Scheme returns a scheme that knows CRDs and Namespaces (custom resources are unstructured).
func Scheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = apiextv1.AddToScheme(s)
	_ = corev1.AddToScheme(s)
	return s
}
