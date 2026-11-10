package providerctl

// New for issue #304: a host-dialed relay that ends for good, other than by
// being superseded or by its job's end, marks its Allocation, so the lifecycle
// controller releases it through HBEP-7's step 5, as it does for
// SubmittedAsOther. Before, it raised RelayFailed and nothing else, and a
// ready Instance kept its job, and so its GPU, until its lease ended.

import (
	"errors"
	"fmt"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// relayFailure is the first final failure of one Allocation's relays, kept
// until the Allocation's relays are dropped.
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
// (hostdial.EndFailed). It records the failure for the next observation, which
// marks the Allocation (markRelayFailed), then raises RelayFailed regarding
// the Allocation, untyped as before (HBEP-6, What ends a relay; HBEP-8, open
// question 14). The status is not written here: the relay's goroutine runs
// outside any reconcile, and the managed reconciler writes the Allocation's
// status at the end of each one.
func (e *external) onRelayFailed(ref *v1alpha1.Allocation, tag string) func(agentapi.RelayChannel, string) {
	r := e.s.relays
	uid, job := string(ref.UID), ref.Status.AtProvider.JobID
	return func(ch agentapi.RelayChannel, cause string) {
		msg := relayFailedMessage(ch, cause)
		r.recordFailure(tag, relayFailure{uid: uid, jobID: job, msg: msg, at: e.now()})
		e.s.o.Recorder.Event(ref, event.Warning(v1alpha1.EventReasonRelayFailed, errors.New(msg)))
	}
}

// recordFailure keeps f for tag unless a failure is kept for it already: the
// first one names the cause.
func (r *relayState) recordFailure(tag string, f relayFailure) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failures == nil {
		r.failures = map[string]relayFailure{}
	}
	if _, ok := r.failures[tag]; !ok {
		r.failures[tag] = f
	}
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

// markRelayFailed sets the condition RelayEndedForGood True, reason
// RelayFailed, with the event's message, on a running Allocation one of whose
// relays ended for good for this Allocation and job, and never clears it:
// the lifecycle controller ends the Allocation's Instances and releases it
// (HBEP-7, Reconcile loop, step 5). It runs in every observation of a running
// Allocation, so the mark is written within one poll interval of the end.
func (r *relayState) markRelayFailed(a *v1alpha1.Allocation, tag string) {
	if a.GetCondition(v1alpha1.ConditionRelayEndedForGood).Status == corev1.ConditionTrue {
		return
	}
	f, ok := r.failure(tag)
	if !ok || f.uid != string(a.UID) || f.jobID != a.Status.AtProvider.JobID {
		return
	}
	a.SetConditions(xpv2.Condition{Type: v1alpha1.ConditionRelayEndedForGood, Status: corev1.ConditionTrue,
		Reason: v1alpha1.ReasonRelayFailed, Message: f.msg, LastTransitionTime: *mt(f.at)})
}
