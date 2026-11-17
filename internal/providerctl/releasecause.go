package providerctl

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// operationOf returns the Operation a release cause names, as the object
// JobEnded carries in related (HBEP-8, Allocation events, Planned or not; R8 of
// #625), or nil for a cause that names none. The parser is the one the
// lifecycle controller's executor writes with, so the writer and the reader
// cannot drift (v1alpha1.ParseReleaseCauseOperation); a cause it refuses, a cut
// one included, gives no related object, and the note still carries the cause.
func operationOf(cause string) runtime.Object {
	namespace, name, uid, ok := v1alpha1.ParseReleaseCauseOperation(cause)
	if !ok {
		return nil
	}
	return &v1alpha1.Operation{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, UID: uid}}
}
