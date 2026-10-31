package fake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Controller is a stand-in for one Provider's controller, for the lifecycle
// controller's tests. The lifecycle controller never calls a Batch: it creates
// Allocations and reads Allocation.status.atProvider and Provider.status. This
// controller plays the Provider's side, simplified (no compare-and-swap
// protocol, no files), and writes the fields the real provider-slurm writes, so
// the meaning of those fields is pinned here.
//
// Sync is one pass over every Allocation, in any namespace, that names the
// Provider. It is level-triggered and idempotent, and one Allocation's failure
// does not stop the others: the pass returns the failures joined. A read
// failure (Batch.FailReads) is not an error: that Allocation's status stays as
// it was, and the Provider's lastSuccessfulStatusRead does not advance.
//
// Every write names the field manager v1alpha1.FieldManagerProvider.
//
// The semantics a test can rely on:
//
//   - After the Allocation has a job ID, or the pass has just established one
//     (an accepted Submit, or the adoption of a job found by tag), the same
//     pass observes the job. A first Sync of a fresh Allocation therefore ends
//     with the job's own phase (Held, Pending or Running), not Submitting.
//   - A Submit's Unknown outcome is not resolved in the same pass: the next
//     Sync finds the tag and adopts it. A job that is found is adopted whatever
//     the attempt's age, and the attempt is recorded Done. When several jobs
//     carry the tag, the one adopted follows HBEP-7's Adoption order: a live job
//     that is not held (no end time, no hold), then a job that ran (it has a
//     start time), then any; within each class the lowest job ID, with jobs
//     sorted by ID, never taken in the order given. The same choice names the
//     job a deleting Allocation records before it cancels every live one.
//   - A submission record found InFlight with no job ID (a crash between the
//     claim and the answer) is looked up by tag like an Unknown one. Inside the
//     window an InFlight attempt shows phase Submitting and an Unknown one
//     SubmissionUnknown, and nothing is submitted. The age of an attempt is the
//     time of its record, or, for a record with none, the Allocation's creation
//     time. The windows below are passed when that age is strictly above them,
//     so a Sync exactly one window after the attempt waits and one a second
//     later acts.
//   - A Rejected attempt (the scheduler's refusal, or a pre-call refusal) keeps
//     phase Submitting until RejectBackoff has passed, then attempt n+1 runs.
//   - Hold mode (HoldRelease true), the submission. The same pass that finds
//     InflightWindow passed since the attempt, with no job carrying the tag,
//     drops the attempt, whether it is Unknown or InFlight: it claims n+1 as
//     InFlight, which replaces record n, and submits. There is no "dropped"
//     state, because the API has none: Abandoned is an operator's decision. A
//     late job of the old attempt lands held and is cancelled by settle.
//   - Hold mode, settle (HBEP-7, open question 11). On every observe of a
//     recorded job while the Allocation is not deleting, in this order: every
//     held job with the tag except the recorded one is cancelled, then the
//     recorded job's hold is lifted once: only if the hold-lifted annotation
//     does not name the job and the job is held by its user (reason
//     JobHeldUser), has restart count 0 and has never started. The annotation
//     is written after the lift, and the status then shows the job as it is
//     after both steps. A lift that fails with an unknown error makes the pass
//     fail for that Allocation, after writing the status; the job stays held,
//     there is no annotation, and the next pass tries again. An admin-held job
//     gets no lift call. A read that fails stops settle: nothing is cancelled
//     and nothing is lifted.
//   - Fallback mode (HoldRelease false), the submission. An InFlight claim
//     older than InflightWindow with no job found becomes Unknown, keeping the
//     attempt's time. Unknown is never retried by time. When UnknownBound has
//     passed since the attempt, the Allocation gets the condition
//     SubmissionResolved False with reason NeedsOperator. When the annotation
//     abandon-attempt equals the attempt's number in decimal, the record
//     becomes Abandoned and the next pass submits n+1; any other value is
//     ignored. A job found by tag always wins over the annotation. Once an
//     attempt is resolved, by the tag or by an abandon, a SubmissionResolved
//     condition that had been raised becomes True with reason Resolved, and it
//     stays: it is never removed. If it was never raised, none is written.
//   - Deletion: every job with the tag that has no end time (held, pending,
//     running, or requeued and so Failed by phase but still holding its
//     allocation) is cancelled; then, with none live and no attempt
//     unresolved, the Allocation's files are removed and the finalizer goes. An
//     attempt with no job found is unresolved, and keeps the finalizer, until
//     the rules above release it: in hold mode InflightWindow has passed, in
//     fallback mode the abandon-attempt annotation names it. An Unknown attempt
//     whose job is found is recorded Done, and the job cancelled. Status is not
//     written for a deleting Allocation.
//   - Queue waits (docs/development.md, Queue waits): when the observed job has
//     a start time and the stored atProvider.queueWaitRecorded is false, the
//     wait (the job's submit and start times) is written to the Provider's
//     status at once, within that observe, and only after that write succeeded
//     is queueWaitRecorded set in the status written for the Allocation. If the
//     Provider write fails, queueWaitRecorded stays false, the Allocation's
//     status is still written, the Sync returns the error joined for that
//     Allocation, and the next Sync tries again. If the Provider write
//     succeeded and the Allocation's did not, the next Sync adds no second
//     entry (a job ID is kept once) and sets the flag. A job whose flag is true
//     adds none, even after its entry fell out of the 20 kept. The list is kept
//     oldest start first, whatever the order the jobs were observed in. A
//     restarted Controller (a new value on the same client) keeps all of this,
//     because it lives in the status.
//   - A later hard end: atProvider.hardEnd is re-read on every observe, so a
//     Batch.SetTimeLimit shows in it at the next Sync.
//   - The Provider's status, readiness (docs/development.md, Provider
//     readiness): a pass that made no Batch read (it observed no job and looked
//     up no tag) calls Batch.Poll once, so Ready rests on a real read. A pass
//     that read does not poll, and a failed read is not answered with a poll
//     that could hide it. On a pass with no failed read,
//     lastSuccessfulStatusRead is the pass's time and Ready is True with reason
//     Polled. On a pass with a failed read, lastSuccessfulStatusRead stays as it
//     was, and Ready becomes False with reason ReadsFailing when there is no
//     successful read yet or the last is older than ReadyStaleness (zero means
//     v1alpha1.ProviderReadyStaleness), strictly; until then it keeps its
//     value. The False message names the time of the last success, so it is the
//     same at every pass. Conditions this controller does not own, such as
//     DialHomeUnreachable, are kept as they were found.
//   - The read-back (HBEP-4, The Slurm dialect), on every observe of the
//     recorded job while the Allocation is not deleting. A job whose name is not
//     the tag puts the observation in phase Refused with reason ReasonJobRenamed
//     (exactly that string), sets the condition Synced False with that reason
//     and a message naming both names, and is cancelled while it is live (so one
//     scancel in the call log, however many Syncs follow). Refused, once set, is
//     kept: a later Sync never overwrites its phase or reason, and neither does
//     a Refused set by hand; the rest of the observation still follows the job.
//     Otherwise a job whose partition, time limit or GPU count differs from
//     spec.forProvider.batch sets the condition SubmittedAsOther True with
//     reason ReasonReadBackMismatch and a message naming each differing field
//     with both values (for example "timeLimit: requested 30m0s, got 20m0s");
//     the condition is never cleared, the job is not cancelled, and the phase
//     keeps mirroring the job.
//   - Retention: every status written mirrors status.retention.effectiveMode
//     from spec.retentionPolicy (RetentionPolicy.EffectiveMode: Keep when unset).
//   - Not modelled: the AdminHold condition, the sticky duplicates list of
//     fallback mode, and the compare-and-swap conflicts.
//   - Times are written in whole seconds: metav1.Time keeps no more.
type Controller struct {
	Client   client.Client
	Provider string
	Batch    *Batch
	Clock    *Clock
	// LostGrace is how long a recorded job may be absent from both sources
	// before the phase is Lost; zero means 2 minutes.
	LostGrace time.Duration
	// RejectBackoff is how long after a Rejected attempt the next one waits;
	// zero means 1 minute.
	RejectBackoff time.Duration
	// HoldRelease runs hold mode: jobs are submitted held and the hold is
	// lifted at once.
	HoldRelease bool
	// InflightWindow is how long an attempt is waited on before a hold-mode
	// controller drops it, or a fallback-mode one calls it Unknown; zero
	// means 5 minutes.
	InflightWindow time.Duration
	// UnknownBound is how long after an attempt a fallback-mode controller
	// waits before it asks an operator; zero means 10 minutes.
	UnknownBound time.Duration
	// ReadyStaleness is how old the Provider's last successful status read may
	// be before a pass with a failed read sets Ready False; zero means
	// v1alpha1.ProviderReadyStaleness.
	ReadyStaleness time.Duration
}

const (
	accountName   = "fake-account"
	listLookback  = time.Hour
	absentMessage = "absent from the queue listing and accounting"
	writerName    = "fake"
)

// pass is the state of one Sync.
type pass struct {
	now time.Time
	// read is true once the pass has made a Batch read (FindByTag or Observe),
	// whether it succeeded or not. A pass with none polls the platform instead.
	read bool
	// readFailed is true once any read of the pass failed.
	readFailed bool
}

// findByTag is Batch.FindByTag, noted in the pass: it counts as a read, and a
// failure marks the pass's reads as failed.
func (c *Controller) findByTag(ctx context.Context, p *pass, tag string, since time.Time) ([]provider.Job, error) {
	p.read = true
	jobs, err := c.Batch.FindByTag(ctx, tag, since)
	if err != nil {
		p.readFailed = true
	}
	return jobs, err
}

// observeJob is Batch.Observe, noted in the pass like findByTag.
func (c *Controller) observeJob(ctx context.Context, p *pass, id string) (provider.Job, bool, error) {
	p.read = true
	job, found, err := c.Batch.Observe(ctx, id)
	if err != nil {
		p.readFailed = true
	}
	return job, found, err
}

func (c *Controller) lostGrace() time.Duration {
	if c.LostGrace == 0 {
		return 2 * time.Minute
	}
	return c.LostGrace
}

func (c *Controller) rejectBackoff() time.Duration {
	if c.RejectBackoff == 0 {
		return time.Minute
	}
	return c.RejectBackoff
}

func (c *Controller) inflightWindow() time.Duration {
	if c.InflightWindow == 0 {
		return 5 * time.Minute
	}
	return c.InflightWindow
}

func (c *Controller) readyStaleness() time.Duration {
	if c.ReadyStaleness == 0 {
		return v1alpha1.ProviderReadyStaleness
	}
	return c.ReadyStaleness
}

func (c *Controller) unknownBound() time.Duration {
	if c.UnknownBound == 0 {
		return 10 * time.Minute
	}
	return c.UnknownBound
}

// mt returns t as a metav1.Time in whole seconds, UTC.
func mt(t time.Time) *metav1.Time {
	m := metav1.NewTime(t.UTC().Truncate(time.Second))
	return &m
}

func mtp(t *time.Time) *metav1.Time {
	if t == nil {
		return nil
	}
	return mt(*t)
}

// Sync makes one pass over the Provider's Allocations, then writes the
// Provider's status.
func (c *Controller) Sync(ctx context.Context) error {
	p := &pass{now: c.Clock.Now().UTC().Truncate(time.Second)}
	var list v1alpha1.AllocationList
	if err := c.Client.List(ctx, &list); err != nil {
		return fmt.Errorf("list Allocations: %w", err)
	}
	var errs []error
	for i := range list.Items {
		a := &list.Items[i]
		if ref := a.Spec.ProviderConfigReference; ref == nil || ref.Name != c.Provider {
			continue
		}
		if err := c.syncAllocation(ctx, p, a); err != nil {
			errs = append(errs, fmt.Errorf("allocation %s/%s: %w", a.Namespace, a.Name, err))
		}
	}
	if err := c.syncProvider(ctx, p); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// record reads the submission annotation; nil when absent.
func record(a *v1alpha1.Allocation) (*v1alpha1.SubmissionRecord, error) {
	raw, ok := a.Annotations[v1alpha1.AnnotationSubmission]
	if !ok {
		return nil, nil
	}
	var r v1alpha1.SubmissionRecord
	if err := json.Unmarshal([]byte(raw), &r); err != nil {
		return nil, fmt.Errorf("annotation %s: %w", v1alpha1.AnnotationSubmission, err)
	}
	return &r, nil
}

// setRecord writes the submission annotation into a, without an API call.
func setRecord(a *v1alpha1.Allocation, r v1alpha1.SubmissionRecord) {
	raw, _ := json.Marshal(r) // a struct of strings, numbers and a time cannot fail
	if a.Annotations == nil {
		a.Annotations = map[string]string{}
	}
	a.Annotations[v1alpha1.AnnotationSubmission] = string(raw)
}

func (c *Controller) newRecord(attempt int32, state v1alpha1.SubmissionState, reason string, now time.Time) v1alpha1.SubmissionRecord {
	return v1alpha1.SubmissionRecord{Attempt: attempt, State: state, Writer: writerName, Time: mt(now), Reason: reason}
}
