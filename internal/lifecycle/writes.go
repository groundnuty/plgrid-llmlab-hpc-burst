package lifecycle

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// Every write carries the resourceVersion it read (an Update), so a conflict
// fails the pass and the key is retried with backoff: re-read, never
// overwrite (HBEP-7, open question 11). Every write names the lifecycle
// controller's field manager.

var owner = client.FieldOwner(v1alpha1.FieldManagerLifecycle)

// setStatus applies mutate to a copy of the Instance's status and writes it when
// it changed. On success in holds the stored object.
func (p *pass) setStatus(in *v1alpha1.Instance, mutate func(*v1alpha1.InstanceStatus)) error {
	next := in.DeepCopy()
	mutate(&next.Status)
	if apiequality.Semantic.DeepEqual(in.Status, next.Status) {
		return nil
	}
	if p.md != nil {
		next.Status.ObservedGeneration = in.Generation
	}
	t := metav1.NewTime(p.now)
	next.Status.LastReconcileTime = &t
	if err := p.r.Client.Status().Update(p.ctx, next, owner); err != nil {
		return fmt.Errorf("update Instance %s status: %w", in.Name, err)
	}
	*in = *next
	return nil
}

// setMeta applies mutate to a copy of the Instance's metadata and writes it
// when it changed, keeping the status the caller holds.
func (p *pass) setMeta(in *v1alpha1.Instance, mutate func(*v1alpha1.Instance)) error {
	next := in.DeepCopy()
	mutate(next)
	if apiequality.Semantic.DeepEqual(in.ObjectMeta, next.ObjectMeta) {
		return nil
	}
	status := in.Status
	if err := p.r.Client.Update(p.ctx, next, owner); err != nil {
		if apierrors.IsNotFound(err) && !next.DeletionTimestamp.IsZero() {
			return nil // the finalizer's removal let it go
		}
		return fmt.Errorf("update Instance %s: %w", in.Name, err)
	}
	next.Status = status
	*in = *next
	return nil
}

// cond sets a condition on a status's list, keeping lastTransitionTime while
// the status does not change, as meta.SetStatusCondition does.
func cond(conds *[]metav1.Condition, typ string, status metav1.ConditionStatus, reason, message string, gen int64, now metav1.Time) {
	apimeta.SetStatusCondition(conds, metav1.Condition{
		Type: typ, Status: status, Reason: reason, Message: message, ObservedGeneration: gen, LastTransitionTime: now,
	})
}

func isTrue(conds []metav1.Condition, typ string) bool {
	return apimeta.IsStatusConditionTrue(conds, typ)
}

// restartGeneration reads a part's restart generation from its annotation,
// absent meaning 0 (v1alpha1.RestartGenerationAnnotation).
func restartGeneration(in *v1alpha1.Instance, part string) int64 {
	v, ok := in.Annotations[v1alpha1.RestartGenerationAnnotation(part)]
	if !ok {
		return 0
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// verificationGeneration is the n of the Instance's current verification,
// ver-<uid>-<n>: one at start, and one more after every restart, each of
// which reverifies (HBEP-7, Instance start and the alpha ladder).
func verificationGeneration(in *v1alpha1.Instance) int64 {
	return 1 + restartGeneration(in, v1alpha1.WorkerName(0)) + restartGeneration(in, v1alpha1.PartInstance)
}

// removeDrainFinalizer drops the Instance's drain finalizer (HBEP-11): the
// controller does so only once the Instance is ended.
func (p *pass) removeDrainFinalizer(in *v1alpha1.Instance) error {
	return p.setMeta(in, func(n *v1alpha1.Instance) { controllerutil.RemoveFinalizer(n, v1alpha1.FinalizerDrain) })
}

// maxReleaseCause bounds the release cause, in bytes.
const maxReleaseCause = 256

// releaseCause is why as one line of at most maxReleaseCause bytes, cut at a
// character's boundary.
func releaseCause(why string) string {
	why = strings.Join(strings.Fields(why), " ")
	if len(why) <= maxReleaseCause {
		return why
	}
	cut := maxReleaseCause - len("…")
	for cut > 0 && !utf8.RuneStart(why[cut]) {
		cut--
	}
	return why[:cut] + "…"
}

// endCause is the release cause of an ended Instance's Allocation: a clean
// end's words (a drain's, or a hedge loss's, "hedge lost to <winner>"), which
// the controller writes, or a failure's reason alone. A failure's message can
// carry text from outside the controller (Slurm's reason, a condition's
// message, an agent's report), and a cause holds none: no path, no host and no
// credential reaches it.
func endCause(in *v1alpha1.Instance, reason, msg string) string {
	if cleanEnd(reason) && msg != "" {
		return fmt.Sprintf("Instance %s ended: %s", in.Name, msg)
	}
	return fmt.Sprintf("Instance %s failed: %s", in.Name, reason)
}

// deleteAllocation releases an Allocation: deleting it lets its finalizer
// cancel every job with its tag (HBEP-7, Release). It is the only delete of an
// Allocation (TestEveryAllocationDeleteWritesItsCause). It writes the release
// cause first, an Update carrying the resourceVersion it read, and deletes only
// once that write succeeded: a failed write leaves the Allocation in place, and
// the next pass tries again, so no Allocation goes without its cause. The UID
// precondition makes sure a recreated object of the same name is not hit.
func (p *pass) deleteAllocation(a *v1alpha1.Allocation, why string) error {
	if !a.DeletionTimestamp.IsZero() {
		return nil
	}
	cause := releaseCause(why)
	if a.Annotations[v1alpha1.AnnotationReleaseCause] != cause {
		next := a.DeepCopy()
		if next.Annotations == nil {
			next.Annotations = map[string]string{}
		}
		next.Annotations[v1alpha1.AnnotationReleaseCause] = cause
		if err := p.r.Client.Update(p.ctx, next, owner); err != nil {
			if apierrors.IsNotFound(err) {
				return nil // gone already
			}
			return fmt.Errorf("write the release cause of Allocation %s (%s): %w", a.Name, cause, err)
		}
		*a = *next
	}
	uid := a.UID
	err := p.r.Client.Delete(p.ctx, a, client.Preconditions{UID: &uid})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("release Allocation %s (%s): %w", a.Name, cause, err)
	}
	now := metav1.NewTime(p.now)
	a.DeletionTimestamp = &now
	return nil
}
