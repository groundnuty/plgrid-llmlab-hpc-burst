package providerctl

// New for issue #304: a host-dialed relay that ends for good, other than by
// being superseded or by its job's end, marks its Allocation, so the lifecycle
// controller releases it through HBEP-7's step 5, as it does for
// SubmittedAsOther. Before, it raised RelayFailed and nothing else, and a
// ready Instance kept its job, and so its GPU, until its lease ended.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// markWriteTimeout bounds the failure path's write of the mark, its retries
// on a conflict included.
const markWriteTimeout = 30 * time.Second

// errMarkRefused: the Allocation of that name is not the one whose relay
// ended (another UID), or it records another job, so the mark is not written.
var errMarkRefused = errors.New("the RelayEndedForGood mark is refused")

// relayFailure is the first final failure of one Allocation's relays for one
// job, kept until the Allocation's relays are dropped.
type relayFailure struct {
	uid, jobID string
	msg        string
	at         time.Time
}

// relayFailedMessage is the RelayFailed event's message, which the condition
// repeats.
func relayFailedMessage(ch agentapi.RelayChannel, cause string) string {
	return fmt.Sprintf("the %s relay ended for good: %s", ch, cause)
}

// onRelayFailed is the relay owner's OnFailed for one Allocation: the owner
// calls it only for a final end that is neither superseded nor the job's end
// (hostdial.EndFailed). It records the failure, writes the mark to the
// Allocation's status at once (persistRelayFailed), so a controller restart
// cannot lose it, then raises RelayFailed regarding the Allocation, untyped as
// before (HBEP-6, What ends a relay; HBEP-8, open question 14). When the write
// fails, the record stays, and the next observation of the Allocation writes
// the mark (markRelayFailed). Both write the first recorded failure, so they
// agree, and the second finds the mark there.
func (e *external) onRelayFailed(ref *v1alpha1.Allocation, tag string) func(agentapi.RelayChannel, string) {
	s := e.s
	key := client.ObjectKeyFromObject(ref)
	uid, job := string(ref.UID), ref.Status.AtProvider.JobID
	return func(ch agentapi.RelayChannel, cause string) {
		msg := relayFailedMessage(ch, cause)
		f := s.relays.recordFailure(tag, relayFailure{uid: uid, jobID: job, msg: msg, at: e.now()})
		if err := s.persistRelayFailed(key, f); err != nil {
			s.o.Log.Info("the RelayEndedForGood mark waits for the next observation", "tag", tag, "error", err)
		}
		s.o.Recorder.Event(ref, event.Warning(v1alpha1.EventReasonRelayFailed, errors.New(msg)))
	}
}

// recordFailure keeps f for tag and returns the failure kept: the first one
// for the same Allocation and job, which names the cause. A failure recorded
// for another UID or job is replaced.
func (r *relayState) recordFailure(tag string, f relayFailure) relayFailure {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failures == nil {
		r.failures = map[string]relayFailure{}
	}
	if old, ok := r.failures[tag]; ok && old.uid == f.uid && old.jobID == f.jobID {
		return old
	}
	r.failures[tag] = f
	return f
}

// forgetFailure drops tag's failure once its relays are dropped: the job is
// not running any more, or the Allocation is being deleted.
func (r *relayState) forgetFailure(tag string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.failures, tag)
}

func (r *relayState) failure(tag string) (relayFailure, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	f, ok := r.failures[tag]
	return f, ok
}

// checkMark refuses a mark for an Allocation that is not f's: another UID
// under the same name, or another recorded job.
func checkMark(a *v1alpha1.Allocation, f relayFailure) error {
	if string(a.UID) != f.uid {
		return fmt.Errorf("%w: Allocation %s is UID %s, not the relay's %s", errMarkRefused, a.Name, a.UID, f.uid)
	}
	if a.Status.AtProvider.JobID != f.jobID {
		return fmt.Errorf("%w: Allocation %s records job %q, not the relay's %q", errMarkRefused, a.Name, a.Status.AtProvider.JobID, f.jobID)
	}
	return nil
}

// setRelayEndedForGood sets the condition RelayEndedForGood True, reason
// RelayFailed, with f's message and time, unless it is True already, and
// reports whether it changed a. Nothing clears it: the lifecycle controller
// ends the Allocation's Instances and releases it (HBEP-7, Reconcile loop,
// step 5).
func setRelayEndedForGood(a *v1alpha1.Allocation, f relayFailure) bool {
	if a.GetCondition(v1alpha1.ConditionRelayEndedForGood).Status == corev1.ConditionTrue {
		return false
	}
	a.SetConditions(xpv2.Condition{Type: v1alpha1.ConditionRelayEndedForGood, Status: corev1.ConditionTrue,
		Reason: v1alpha1.ReasonRelayFailed, Message: f.msg, LastTransitionTime: *mt(f.at)})
	return true
}

// persistRelayFailed writes f's mark to the Allocation's status from the
// failure path. It reads the Allocation from the API server, refuses when its
// UID or recorded job is not f's (errMarkRefused), does nothing when the mark
// is there, and otherwise patches the status under the read's
// resourceVersion, so a concurrent write is a conflict: it then reads again
// and retries (client-go's retry.DefaultRetry), within markWriteTimeout.
func (s *shared) persistRelayFailed(key client.ObjectKey, f relayFailure) error {
	ctx, cancel := context.WithTimeout(context.Background(), markWriteTimeout)
	defer cancel()
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cur := &v1alpha1.Allocation{}
		if err := s.reader.Get(ctx, key, cur); err != nil {
			return err
		}
		if err := checkMark(cur, f); err != nil {
			return err
		}
		orig := cur.DeepCopy()
		if !setRelayEndedForGood(cur, f) {
			return nil
		}
		return s.kube.Status().Patch(ctx, cur, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{}))
	})
}

// markRelayFailed is the mark's fallback, in every observation of a running
// Allocation: when the failure path's write failed, it sets the mark from the
// recorded failure, under the same checks, and the managed reconciler writes
// it with the status at the end of the reconcile.
func (r *relayState) markRelayFailed(a *v1alpha1.Allocation, tag string) {
	f, ok := r.failure(tag)
	if !ok || checkMark(a, f) != nil {
		return
	}
	setRelayEndedForGood(a, f)
}
