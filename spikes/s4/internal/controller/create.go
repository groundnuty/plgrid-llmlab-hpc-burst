package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	kerrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s4/apis/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s4/internal/slurm"
)

// Create claims an attempt (compare-and-swap), runs sbatch (with --hold in
// hold mode), records the job ID and, in hold mode, settles at once so the
// hold lasts seconds. It returns an error only for a definite rejection or
// when it could not claim; an ambiguous sbatch is recorded as Unknown and
// resolved by later tag lookups.
func (e *external) Create(ctx context.Context, mg resource.Managed) (managed.ExternalCreation, error) {
	a, ok := mg.(*v1alpha1.Allocation)
	if !ok {
		return managed.ExternalCreation{}, errNotAllocation
	}
	if e.opts.Mode == ModeJobID {
		return e.createClassic(ctx, a)
	}
	if err := canonical(a); err != nil {
		return managed.ExternalCreation{}, err
	}
	name := meta.GetExternalName(a)
	rec, err := readRecord(a)
	if err != nil {
		return managed.ExternalCreation{}, err
	}
	if rec.jobID != 0 && e.opts.GuardSucceeded {
		return managed.ExternalCreation{}, fmt.Errorf("job %d is already established for this Allocation; refusing to submit another", rec.jobID)
	}
	if t := rec.attempt; t.unresolved() && e.opts.GuardInflight && !(e.opts.HoldRelease && time.Since(t.At) >= e.opts.InflightWindow) {
		return managed.ExternalCreation{}, fmt.Errorf("submission attempt %d by %s is unresolved; refusing to submit another", t.N, t.By)
	}
	at := Attempt{N: rec.nextN(), Token: newToken(), State: AttemptInFlight, By: e.opts.Identity, At: time.Now().UTC()}
	if err := e.write(ctx, a, map[string]string{AnnoSubmission: at.value()}); err != nil {
		return managed.ExternalCreation{}, fmt.Errorf("cannot claim submission attempt %d, nothing submitted: %w", at.N, err)
	}
	e.opts.Log.Info("claimed submission attempt", "allocation", a.GetName(), "attempt", at.N)
	if e.opts.FaultBeforeSbatch != "" {
		faultBlock(e.opts.FaultBeforeSbatch, 0)
	}
	p := a.Spec.ForProvider
	id, outcome, serr := e.slurm.Submit(ctx, slurm.SubmitRequest{
		Tag: name, Hold: e.opts.HoldRelease, Partition: p.Partition, Account: p.Account, TimeLimit: p.TimeLimit, Wrap: p.Wrap,
		Comment: a.GetNamespace() + "/" + a.GetName(),
	})
	// Write the outcome with a fresh context: the reconcile's may have
	// expired with sbatch, and the record must not be left InFlight.
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	switch outcome {
	case slurm.Rejected:
		at.State, at.Reason = AttemptRejected, serr.Error()
		if err := e.writeOwn(wctx, a, at.Token, map[string]string{AnnoSubmission: at.value()}); err != nil {
			e.opts.Log.Info("cannot record the rejection", "error", err)
		}
		return managed.ExternalCreation{}, serr
	case slurm.Unknown:
		at.State, at.Reason = AttemptUnknown, serr.Error()
		if err := e.writeOwn(wctx, a, at.Token, map[string]string{AnnoSubmission: at.value()}); err != nil {
			e.opts.Log.Info("cannot record the unknown outcome; the claim stays InFlight", "error", err)
		}
		e.opts.Log.Info("sbatch outcome unknown; resolving by tag lookup", "allocation", a.GetName(), "attempt", at.N, "error", serr.Error())
		e.rec.Event(a, event.Warning("SubmissionUnknown", fmt.Errorf("attempt %d: %w; resolving by tag lookup", at.N, serr)))
		return managed.ExternalCreation{}, nil
	}
	e.opts.Log.Info("sbatch returned", "allocation", a.GetName(), "job", id, "hold", e.opts.HoldRelease)
	if e.opts.FaultAfterSbatch != "" {
		faultBlock(e.opts.FaultAfterSbatch, id)
	}
	est, wrote, err := e.establishOwn(wctx, a, at, id)
	if err != nil {
		return managed.ExternalCreation{}, fmt.Errorf("job %d submitted but not recorded; a later Observe adopts it by tag: %w", id, err)
	}
	if est != id {
		if e.opts.HoldRelease {
			// It is held and never ran; job est is the Allocation's.
			if err := e.slurm.Cancel(wctx, id); err != nil {
				e.opts.Log.Info("cannot cancel extra held job", "job", id, "error", err)
			}
			e.rec.Event(a, event.Normal("ExtraCancelled", fmt.Sprintf("job %d was cancelled while held: job %d was established first", id, est)))
		} else {
			e.rec.Event(a, event.Warning("DuplicateJob", fmt.Errorf("job %d has the tag but job %d was established first", id, est)))
		}
		return managed.ExternalCreation{}, nil
	}
	// Settle now, so the hold lasts seconds, not a poll interval. When a
	// peer adopted this job first (wrote is false), the peer settles.
	if e.opts.HoldRelease && wrote && !meta.WasDeleted(a) {
		live, err := e.slurm.SqueueByTag(wctx, name)
		if err != nil {
			e.opts.Log.Info("cannot settle after submit; the next Observe will", "error", err)
			return managed.ExternalCreation{}, nil
		}
		r, _ := readRecord(a)
		e.settle(wctx, a, &r, live)
	}
	return managed.ExternalCreation{}, nil
}

// establishOwn records the job this Create submitted, unless another job is
// established first (a concurrent or older attempt): then it returns that
// one. Either way it resolves its own attempt as Done, if the attempt is
// still current, so deletion never waits on it. wrote reports whether this
// call recorded the job ID. Conflicts re-read from the API server.
func (e *external) establishOwn(ctx context.Context, a *v1alpha1.Allocation, at Attempt, id int64) (est int64, wrote bool, err error) {
	for i := 0; i < 5; i++ {
		rec, err := readRecord(a)
		if err != nil {
			return 0, false, err
		}
		est = rec.jobID
		set := map[string]string{}
		if est == 0 {
			est = id
			set[AnnoJobID] = strconv.FormatInt(id, 10)
		}
		if rec.attempt != nil && rec.attempt.Token == at.Token && rec.attempt.State != AttemptDone {
			t := at
			t.State, t.JobID = AttemptDone, id
			if est != id {
				t.Reason = fmt.Sprintf("job %d is an extra: job %d was established first", id, est)
			}
			set[AnnoSubmission] = t.value()
		}
		if len(set) == 0 {
			return est, false, nil
		}
		err = e.write(ctx, a, set)
		if err == nil {
			_, wrote = set[AnnoJobID]
			if wrote {
				e.rec.Event(a, event.Normal("JobEstablished", fmt.Sprintf("job %d submitted by attempt %d", id, at.N)))
			}
			return est, wrote, nil
		}
		if !kerrors.IsConflict(err) {
			return 0, false, err
		}
		if err := e.refresh(ctx, a); err != nil {
			return 0, false, err
		}
	}
	return 0, false, errors.New("still conflicting after 5 re-reads")
}

// settle cancels every held job with the tag except the established one,
// then releases the established job once. It never releases a job that has
// a release marker, has started, has been requeued, or is held by an
// administrator: Slurm can put a requeued job back into JobHeldUser
// (job_mgr.c 24.11:17613-17628), and releasing it would re-run a job the
// provider reports as Failed. It returns live with the hold flags updated.
func (e *external) settle(ctx context.Context, a *v1alpha1.Allocation, rec *record, live []slurm.Job) []slurm.Job {
	out := append([]slurm.Job(nil), live...)
	for i, j := range out {
		if j.ID == rec.jobID || !j.Held || ended(j) {
			continue
		}
		if err := e.slurm.Cancel(ctx, j.ID); err != nil {
			e.opts.Log.Info("cannot cancel extra held job", "job", j.ID, "error", err)
			continue
		}
		out[i].State = []string{"CANCELLED"}
		e.opts.Log.Info("cancelled extra held job", "allocation", a.GetName(), "job", j.ID, "established", rec.jobID)
		e.rec.Event(a, event.Normal("ExtraCancelled", fmt.Sprintf("job %d was cancelled while held; job %d is the Allocation's", j.ID, rec.jobID)))
	}
	est := findJob(out, rec.jobID)
	if est == nil || ended(*est) {
		return out
	}
	o := &a.Status.AtProvider
	switch {
	case !est.Held && rec.released == 0:
		// Released by a peer (or by this provider before a crash) and the
		// marker is missing: record it, so a later hold is never released.
		e.markReleased(ctx, a, rec, est.ID)
	case !est.Held:
	case rec.released != 0, est.RestartCount > 0, !est.Start.IsZero():
		o.Reason = fmt.Sprintf("job %d is held again after its release (reason %s); not releasing it twice", est.ID, est.Reason)
		a.SetConditions(xpv2.Unavailable().WithMessage(o.Reason))
	case est.Reason == slurm.ReasonHeldAdmin:
		o.Reason = fmt.Sprintf("job %d is held by an administrator (JobHeldAdmin); only an operator can release it", est.ID)
		a.SetConditions(xpv2.Unavailable().WithMessage(o.Reason))
	default:
		if err := e.slurm.Release(ctx, est.ID); err != nil {
			o.Reason = err.Error()
			e.opts.Log.Info("release failed", "job", est.ID, "error", err)
			return out
		}
		e.opts.Log.Info("released job", "allocation", a.GetName(), "job", est.ID)
		est.Held, est.Reason = false, "None"
		e.markReleased(ctx, a, rec, est.ID)
	}
	return out
}

// markReleased writes the release marker. A conflict is harmless: the next
// Observe sees the job not held and writes the marker then.
func (e *external) markReleased(ctx context.Context, a *v1alpha1.Allocation, rec *record, id int64) {
	if err := e.write(ctx, a, map[string]string{AnnoReleased: strconv.FormatInt(id, 10)}); err != nil {
		e.opts.Log.Info("cannot write the release marker yet", "job", id, "error", err)
		return
	}
	rec.released = id
}

// ---- crossplane's classic pattern (control only) ----

func (e *external) observeClassic(ctx context.Context, a *v1alpha1.Allocation) (managed.ExternalObservation, error) {
	id, err := strconv.ParseInt(meta.GetExternalName(a), 10, 64)
	if err != nil || id <= 0 {
		return managed.ExternalObservation{ResourceExists: false}, nil // no job ID stored means "not created"
	}
	jobs, err := e.slurm.SqueueByID(ctx, id)
	if err == nil && len(jobs) == 0 {
		jobs, err = e.slurm.SacctByID(ctx, id)
	}
	if err != nil {
		return managed.ExternalObservation{}, err
	}
	if len(jobs) == 0 {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}
	a.Status.AtProvider.JobID = jobs[0].ID
	e.recordJob(a, jobs[0])
	e.toCancel = nil
	if !ended(jobs[0]) {
		e.toCancel = []int64{jobs[0].ID}
	}
	if meta.WasDeleted(a) && ended(jobs[0]) {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}
	return exists, nil
}

func (e *external) createClassic(ctx context.Context, a *v1alpha1.Allocation) (managed.ExternalCreation, error) {
	p := a.Spec.ForProvider
	id, _, err := e.slurm.Submit(ctx, slurm.SubmitRequest{
		Tag: Tag(a), Partition: p.Partition, Account: p.Account, TimeLimit: p.TimeLimit, Wrap: p.Wrap,
	})
	if err != nil {
		return managed.ExternalCreation{}, err
	}
	if e.opts.FaultAfterSbatch != "" {
		faultBlock(e.opts.FaultAfterSbatch, id)
	}
	meta.SetExternalName(a, strconv.FormatInt(id, 10))
	return managed.ExternalCreation{}, nil
}
