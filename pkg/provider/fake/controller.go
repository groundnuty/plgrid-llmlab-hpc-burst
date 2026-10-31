package fake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
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
// The semantics a test can rely on:
//
//   - After the Allocation has a job ID, or the pass has just established one
//     (an accepted Submit, or the adoption of a job found by tag), the same
//     pass observes the job. A first Sync of a fresh Allocation therefore ends
//     with the job's own phase (Held, Pending or Running), not Submitting.
//   - A Submit's Unknown outcome is not resolved in the same pass: the next
//     Sync finds the tag and adopts it. With no job found, the phase stays
//     SubmissionUnknown and Submit is never called again, whatever the time.
//   - A submission record found InFlight with no job ID (a crash between the
//     claim and the answer) is treated as Unknown.
//   - A Rejected attempt (the scheduler's refusal, or a pre-call refusal) keeps
//     phase Submitting until RejectBackoff has passed, then attempt n+1 runs.
//   - HoldRelease: the hold is lifted once, right after an accepted Submit or an
//     adoption of a held job (restart count 0, never started, user hold), and
//     the job ID is written to the hold-lifted annotation.
//   - Queue waits: a job's wait is added to the Provider's status the first time
//     the job is seen with a start time, that is when the persisted
//     atProvider.startTime goes from nil to set. A re-observed job adds none,
//     even after its entry fell out of the 20 kept.
//   - Deletion: every job with the tag that has no end time (held, pending,
//     running, or requeued and so Failed by phase but still holding its
//     allocation) is cancelled; then, with none live and no attempt InFlight
//     or Unknown, the Allocation's files are removed and the finalizer goes.
//     An Unknown attempt with no job found keeps the finalizer for good (HBEP-7,
//     open question 11; the fake has no abandon-attempt), so a test must not
//     delete in that state. An Unknown attempt whose job is found is recorded
//     Done, and the job cancelled. Status is not written for a deleting
//     Allocation.
//   - The Provider's status: queueWaits are updated on every pass.
//     lastSuccessfulStatusRead and the Ready condition (True, reason Polled)
//     are refreshed only on a pass in which no read failed, so a failing
//     platform leaves them to age.
//   - Not modelled: the read-back (a renamed job is not refused, a job with
//     another limit sets no SubmittedAsOther), duplicate-job settling, and every
//     Allocation condition (AdminHold, SubmissionResolved).
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
}

const (
	writerName    = "fake"
	accountName   = "fake-account"
	listLookback  = time.Hour
	absentMessage = "absent from the queue listing and accounting"
)

// pass is the state of one Sync.
type pass struct {
	now        time.Time
	readFailed bool
	waits      []v1alpha1.QueueWait
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

// syncProvider writes the Provider's status: the queue waits found in this
// pass, and, when no read failed, the time of the last successful read and the
// Ready condition.
func (c *Controller) syncProvider(ctx context.Context, p *pass) error {
	var prov v1alpha1.Provider
	if err := c.Client.Get(ctx, client.ObjectKey{Name: c.Provider}, &prov); err != nil {
		return fmt.Errorf("get Provider %s: %w", c.Provider, err)
	}
	old := prov.DeepCopy()
	// A list is ordered by name; the oldest wait is the earliest start.
	sort.SliceStable(p.waits, func(i, k int) bool { return p.waits[i].StartTime.Before(&p.waits[k].StartTime) })
	for _, w := range p.waits {
		prov.Status.QueueWaits = v1alpha1.AddQueueWait(prov.Status.QueueWaits, w)
	}
	prov.Status.ObservedGeneration = prov.Generation
	prov.Status.LastReconcileTime = mt(p.now)
	if !p.readFailed {
		prov.Status.LastSuccessfulStatusRead = mt(p.now)
		apimeta.SetStatusCondition(&prov.Status.Conditions, metav1.Condition{
			Type:               v1alpha1.ConditionProviderReady,
			Status:             metav1.ConditionTrue,
			Reason:             "Polled",
			ObservedGeneration: prov.Generation,
			LastTransitionTime: *mt(p.now),
		})
	}
	if apiequality.Semantic.DeepEqual(old.Status, prov.Status) {
		return nil
	}
	if err := c.Client.Status().Update(ctx, &prov); err != nil {
		return fmt.Errorf("update Provider %s status: %w", c.Provider, err)
	}
	return nil
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

// syncAllocation handles one Allocation, in the order: metadata, deletion, the
// job's establishment, then the observation of the recorded job.
func (c *Controller) syncAllocation(ctx context.Context, p *pass, a *v1alpha1.Allocation) error {
	tag := v1alpha1.JobTag(a.Namespace, a.Name, a.UID)
	prior := a.Status.AtProvider.DeepCopy() // before any Update overwrites the status

	// Metadata: the finalizer and the external name, one Update.
	changed := false
	if a.DeletionTimestamp.IsZero() && controllerutil.AddFinalizer(a, v1alpha1.FinalizerCancelAll) {
		changed = true
	}
	if a.Annotations[v1alpha1.AnnotationExternalName] != tag {
		if a.Annotations == nil {
			a.Annotations = map[string]string{}
		}
		a.Annotations[v1alpha1.AnnotationExternalName] = tag
		changed = true
	}
	if changed {
		if err := c.Client.Update(ctx, a); err != nil {
			return fmt.Errorf("update metadata: %w", err)
		}
	}
	if !a.DeletionTimestamp.IsZero() {
		return c.syncDeleting(ctx, p, a, tag)
	}

	obs := prior.DeepCopy()
	obs.Tag = tag
	rec, err := record(a)
	if err != nil {
		return err
	}
	if a.Annotations[v1alpha1.AnnotationJobID] == "" {
		settled, err := c.establish(ctx, p, a, rec, tag, obs)
		if err != nil || !settled {
			return c.writeStatus(ctx, a, prior, obs, err)
		}
		if rec, err = record(a); err != nil {
			return err
		}
	}
	err = c.observe(ctx, p, a, rec, prior, obs)
	return c.writeStatus(ctx, a, prior, obs, err)
}

// writeStatus writes obs as the Allocation's atProvider, unless it is
// unchanged, and returns err (the earlier failure of the same Allocation) or
// the write's own.
func (c *Controller) writeStatus(ctx context.Context, a *v1alpha1.Allocation, prior, obs *v1alpha1.AllocationObservation, err error) error {
	if err != nil {
		return err
	}
	if apiequality.Semantic.DeepEqual(prior, obs) {
		return nil
	}
	a.Status.AtProvider = *obs // after the last Update, which restores the stored status
	if err := c.Client.Status().Update(ctx, a); err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	return nil
}

// observe reads the recorded job and mirrors it into obs.
func (c *Controller) observe(ctx context.Context, p *pass, a *v1alpha1.Allocation, rec *v1alpha1.SubmissionRecord, prior, obs *v1alpha1.AllocationObservation) error {
	id := a.Annotations[v1alpha1.AnnotationJobID]
	job, found, err := c.Batch.Observe(ctx, id)
	if err != nil {
		// A failed read is not absence: nothing changes, not even the clock of
		// absentSince.
		p.readFailed = true
		*obs = *prior.DeepCopy()
		return nil
	}
	obs.JobID, obs.Submission = id, rec
	if !found {
		if obs.AbsentSince == nil {
			obs.AbsentSince = mt(p.now)
		}
		if p.now.Sub(obs.AbsentSince.Time) > c.lostGrace() {
			obs.Phase, obs.Reason = v1alpha1.AllocationLost, absentMessage
		}
		return nil
	}
	obs.AbsentSince = nil
	obs.Phase = job.Phase
	obs.Held = job.Held()
	obs.Requeued = obs.Requeued || job.Requeued // kept once seen
	obs.Reason = job.Reason
	if obs.Requeued {
		obs.Reason = v1alpha1.ReasonRequeued
	}
	obs.RestartCount = job.RestartCount
	obs.SubmitTime = mt(job.SubmitTime)
	obs.StartEstimate, obs.StartTime = mtp(job.StartEstimate), mtp(job.StartTime)
	obs.HardEnd, obs.EndTime = mtp(job.HardEnd), mtp(job.EndTime)
	if job.StartTime != nil && prior.StartTime == nil {
		p.waits = append(p.waits, v1alpha1.QueueWait{JobID: id, SubmitTime: *mt(job.SubmitTime), StartTime: *mt(*job.StartTime)})
	}
	return nil
}
