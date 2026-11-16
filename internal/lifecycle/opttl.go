package lifecycle

// The removal of a finished Operation (HBEP-10, Operations, Removal): an Operation is deleted 7 days after it finished
// (status.finishedAt, set on Done, Stale and Refused), as a Job's ttlSecondsAfterFinished removes a finished Job. The
// audit log holds its content by then (HBEP-10, Audit). A small controller of its own, keyed by Operation: the pass of a
// ModelDeployment stops requeueing once its Operations are finished, and an Operation of a deleted deployment would
// never be reached by it.

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// operationTTL is how long a finished Operation stays (HBEP-10, Operations, Removal).
const operationTTL = 7 * 24 * time.Hour

// operationTTLController is the name of the removal controller.
const operationTTLController = "operation-ttl"

// operationTTLReconciler deletes a finished Operation once operationTTL has passed since its finish, with a UID
// precondition, so that an Operation made again under the same name is never the one removed.
type operationTTLReconciler struct {
	// Client deletes; Reader reads the Operation from the API, not from a cache.
	Client client.Client
	Reader client.Reader
	Clock  Clock
}

// Reconcile removes the Operation when it is due, and asks to be called again at the time it is due otherwise. An
// Operation that has not finished, or is gone, needs no call: its own status write, when it finishes, enqueues it.
func (r *operationTTLReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var op v1alpha1.Operation
	if err := r.Reader.Get(ctx, req.NamespacedName, &op); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get Operation %s: %w", req.NamespacedName, err)
	}
	if op.Status.FinishedAt == nil || !op.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	due := op.Status.FinishedAt.Add(operationTTL)
	if wait := due.Sub(r.Clock.Now()); wait > 0 {
		return ctrl.Result{RequeueAfter: wait}, nil
	}
	uid := op.UID
	if err := r.Client.Delete(ctx, &op, client.Preconditions{UID: &uid}); err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
		return ctrl.Result{}, fmt.Errorf("delete the finished Operation %s: %w", req.NamespacedName, err)
	}
	return ctrl.Result{}, nil
}

// addOperationTTL adds the removal controller to the manager, with the lifecycle controller's own queue options
// (controllerOptions: client-go's queue, not controller-runtime's priority queue, which deadlocks on shutdown, #136).
func addOperationTTL(mgr manager.Manager, clock Clock) error {
	r := &operationTTLReconciler{Client: mgr.GetClient(), Reader: mgr.GetAPIReader(), Clock: clock}
	if err := ctrl.NewControllerManagedBy(mgr).Named(operationTTLController).For(&v1alpha1.Operation{}).
		WithOptions(controllerOptions(1)).Complete(r); err != nil {
		return fmt.Errorf("lifecycle: the removal of finished Operations: %w", err)
	}
	return nil
}
