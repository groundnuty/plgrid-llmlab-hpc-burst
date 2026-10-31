package controller

import (
	"fmt"
	"sort"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s4/apis/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s4/internal/slurm"
)

// observeEstablished reports the established job. It never reports the
// Allocation missing (that would resubmit): a job absent from both squeue
// and sacct is Lost only after LostGrace, and a Lost job that reappears
// returns to its real state.
func (e *external) observeEstablished(a *v1alpha1.Allocation, rec record, all []slurm.Job) managed.ExternalObservation {
	o := &a.Status.AtProvider
	o.JobID = rec.jobID
	var others []int64
	for _, j := range all {
		if j.ID != rec.jobID {
			others = append(others, j.ID)
		}
	}
	// Duplicate evidence is sticky across polls and sources: a duplicate
	// seen once in squeue stays reported after it leaves both.
	if dups := union(o.Duplicates, others); len(dups) > len(o.Duplicates) {
		o.Duplicates = dups
		e.rec.Event(a, event.Warning("DuplicateJob", fmt.Errorf("jobs %v carry this Allocation's tag; job %d is the Allocation's", dups, rec.jobID)))
	}
	if job := findJob(all, rec.jobID); job != nil {
		if o.Phase == v1alpha1.PhaseLost {
			e.rec.Event(a, event.Normal("JobReappeared", fmt.Sprintf("job %d is back in %s after being reported Lost", job.ID, job.Source)))
		}
		o.AbsentSince = nil
		e.recordJob(a, *job)
		return exists
	}
	if !e.opts.GuardSucceeded {
		return managed.ExternalObservation{ResourceExists: false} // control only
	}
	if o.AbsentSince == nil {
		now := metav1.Now()
		o.AbsentSince = &now
	}
	since := o.AbsentSince.UTC().Format(time.RFC3339)
	if time.Since(o.AbsentSince.Time) < e.opts.LostGrace {
		o.Reason = fmt.Sprintf("job %d absent from squeue and sacct since %s; reported Lost after lost_grace %s", rec.jobID, since, e.opts.LostGrace)
		return exists
	}
	if o.Phase != v1alpha1.PhaseLost {
		e.rec.Event(a, event.Warning("JobLost", fmt.Errorf("job %d absent from squeue and sacct since %s", rec.jobID, since)))
	}
	o.Phase = v1alpha1.PhaseLost
	o.Reason = fmt.Sprintf("job %d has been absent from squeue and sacct since %s (lost_grace %s); not resubmitting", rec.jobID, since, e.opts.LostGrace)
	a.SetConditions(xpv2.Unavailable().WithMessage(o.Reason))
	return exists
}

// recordJob writes the job's observation into status.
func (e *external) recordJob(a *v1alpha1.Allocation, j slurm.Job) {
	o := &a.Status.AtProvider
	o.State = j.State
	o.Source = j.Source
	o.Held = j.Held
	o.RestartCount = j.RestartCount
	o.SubmitTime, o.StartTime, o.EndTime = mt(j.Submit), mt(j.Start), mt(j.End)
	// The requeue verdict is sticky: later output may carry no evidence
	// (v0.0.40 sacct has no restart_cnt, and accounting can purge the earlier
	// records), so the verdict must survive in status.
	o.Requeued = o.Requeued || j.Requeued
	o.Phase, o.Reason = phase(j, o.Requeued)
	switch o.Phase {
	case v1alpha1.PhaseRunning:
		a.SetConditions(xpv2.Available())
	case v1alpha1.PhasePending:
		a.SetConditions(xpv2.Creating())
	default:
		a.SetConditions(xpv2.Unavailable().WithMessage(o.Reason))
	}
}

func phase(j slurm.Job, requeued bool) (string, string) {
	if requeued {
		return v1alpha1.PhaseFailed, fmt.Sprintf("Requeued: Slurm requeued the job (restart count %d, %d accounting records) although it was submitted with --no-requeue", j.RestartCount, j.Records)
	}
	for _, s := range []string{"FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL", "PREEMPTED", "BOOT_FAIL", "DEADLINE", "OUT_OF_MEMORY"} {
		if j.HasState(s) {
			return v1alpha1.PhaseFailed, "Ended: " + s
		}
	}
	switch {
	case j.HasState("COMPLETED"):
		return v1alpha1.PhaseSucceeded, "Ended: COMPLETED"
	case j.HasState("RUNNING"), j.HasState("SUSPENDED"):
		return v1alpha1.PhaseRunning, ""
	case j.HasState("PENDING") && j.Held:
		return v1alpha1.PhasePending, "held (" + j.Reason + ")"
	case j.HasState("PENDING"):
		return v1alpha1.PhasePending, ""
	}
	return v1alpha1.PhasePending, fmt.Sprintf("unrecognised state %v", j.State)
}

// ended reports whether Slurm has finished the job: no PENDING, RUNNING or
// SUSPENDED state. A requeued job that is pending again has not ended, so
// deleting its Allocation still cancels it.
func ended(j slurm.Job) bool {
	return !j.HasState("PENDING") && !j.HasState("RUNNING") && !j.HasState("SUSPENDED")
}

// findJob returns a pointer into jobs, or nil.
func findJob(jobs []slurm.Job, id int64) *slurm.Job {
	for i := range jobs {
		if jobs[i].ID == id {
			return &jobs[i]
		}
	}
	return nil
}

// mergeJobs unions job lists by ID, keeping the first list's record for an ID
// seen in both (squeue is live; sacct can lag), sorted by job ID.
func mergeJobs(first, second []slurm.Job) []slurm.Job {
	out := append([]slurm.Job(nil), first...)
	for _, j := range second {
		if findJob(out, j.ID) == nil {
			out = append(out, j)
		}
	}
	sort.Slice(out, func(i, k int) bool { return out[i].ID < out[k].ID })
	return out
}

// union returns the sorted, deduplicated union of a and b.
func union(a, b []int64) []int64 {
	seen := map[int64]bool{}
	var out []int64
	for _, x := range append(append([]int64(nil), a...), b...) {
		if !seen[x] {
			seen[x] = true
			out = append(out, x)
		}
	}
	sort.Slice(out, func(i, k int) bool { return out[i] < out[k] })
	return out
}

func mt(t time.Time) *metav1.Time {
	if t.IsZero() {
		return nil
	}
	m := metav1.NewTime(t)
	return &m
}
