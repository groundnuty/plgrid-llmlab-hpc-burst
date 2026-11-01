package lifecycle

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// TunnelWriteAttempts is how often WriteTunnelConnected tries one condition's
// write against a conflicting writer (the reconciler updates the same status).
const TunnelWriteAttempts = 4

// WriteTunnelConnected writes want as the Instance's TunnelConnected condition
// (HBEP-6, Path state and the inventory: the lifecycle controller writes it,
// from frps's proxy list). It lives here, beside setStatus, because the
// condition and the reconciler's own conditions share one status and one
// resourceVersion: both are a read-modify-write Status().Update that carries
// the resourceVersion of the copy it read, so whichever writes second gets a
// Conflict and neither loses the other's conditions (a science-agent ruling,
// 1 Nov 2026, for #90). A JSON merge patch would break that on a CRD, where it
// replaces the whole conditions list with the writer's possibly stale copy.
// Both write under the lifecycle controller's field manager.
//
// It writes only when the stored status or reason differs from want's. The
// message is not compared: the Unknown message embeds the error's text, which
// varies per read, and a write at each change of it would churn the
// resourceVersion and keep conflicting with the reconciler.
//
// in is the copy the caller read, possibly from the cache. On a Conflict the
// Instance is read again from fresh, the API server's reader, and the decision
// is taken again on what it finds: the Instance may have ended, or already hold
// the condition. It gives up after TunnelWriteAttempts conflicts with an error
// saying so, and the caller's next read retries. A write for an ended Instance
// writes nothing: the condition of an ended Instance is history, frozen as it
// was at the end.
func WriteTunnelConnected(ctx context.Context, writer client.StatusClient, fresh client.Reader, in *v1alpha1.Instance, want metav1.Condition) error {
	cur := in
	for range TunnelWriteAttempts {
		if cur.Status.Phase == v1alpha1.InstanceEnded {
			return nil
		}
		have := apimeta.FindStatusCondition(cur.Status.Conditions, v1alpha1.ConditionTunnelConnected)
		if have != nil && have.Status == want.Status && have.Reason == want.Reason {
			return nil
		}
		next := cur.DeepCopy()
		c := want
		c.ObservedGeneration = cur.Generation
		apimeta.SetStatusCondition(&next.Status.Conditions, c) // keeps lastTransitionTime while the status holds
		err := writer.Status().Update(ctx, next, owner)
		switch {
		case err == nil, apierrors.IsNotFound(err):
			return nil
		case !apierrors.IsConflict(err):
			return fmt.Errorf("update Instance %s/%s status: %w", in.Namespace, in.Name, err)
		}
		latest := &v1alpha1.Instance{}
		switch err := fresh.Get(ctx, client.ObjectKeyFromObject(in), latest); {
		case apierrors.IsNotFound(err):
			return nil
		case err != nil:
			return fmt.Errorf("re-read Instance %s/%s: %w", in.Namespace, in.Name, err)
		}
		if latest.UID != in.UID {
			return nil // the name belongs to another Instance now; its UID is not the one read
		}
		cur = latest
	}
	return fmt.Errorf("update Instance %s/%s status: still conflicting after %d attempts; the next read retries", in.Namespace, in.Name, TunnelWriteAttempts)
}
