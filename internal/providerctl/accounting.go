package providerctl

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Provider accounting (HBEP-8, Provider accounting; HBEP-4, The Slurm dialect,
// Allocated time). The numbers are allocated, not billed. The arithmetic is
// provider.AccountingOf; this file decides when to read, where to store, and
// when the record that outlives the Allocation may be written.

// checkAccountingScheme refuses a scheme that cannot write the accounting
// record, a ControllerRevision of apps/v1: the finalizer would fail on every
// pass and the Allocation could never be deleted.
func checkAccountingScheme(s *runtime.Scheme) error {
	if gvk := appsv1.SchemeGroupVersion.WithKind("ControllerRevision"); !s.Recognizes(gvk) {
		return fmt.Errorf("providerctl: the scheme does not know %s; the accounting record of a deleted Allocation is one, so register k8s.io/api/apps/v1", gvk)
	}
	return nil
}

// allocatedRead is HBEP-4's accounting read for a recorded job, after the job
// is first seen started, to learn the CPUs and GPUs the scheduler allocated,
// which the queue listing does not carry. A job the listing shows ended or
// running gets its record's Accounting set from it. It is not made for a job
// accounting already gave (the listing did not show it), nor for one that has
// not started.
//
// Whether it is due is judged from the stored status alone, never from memory
// (review of PR 118, Astra B5), so a status write that fails, or a restart,
// costs one more read and never loses one: it is made while the stored
// accounting has no CPU count, at most once per provider.AccountingRefresh
// counted from the stored readTime, which every read moves (Astra nit 10: an
// empty first read, accounting lagging the queue, is retried until the first
// usable one, then never again). Nor is it made for a requeued job, whose
// counts are unknown whatever it would show (science agent, 31 Oct 2026). It
// reports whether it read.
func (e *external) allocatedRead(ctx context.Context, job provider.Job, prior *v1alpha1.AllocationObservation, now time.Time) (provider.Job, bool, error) {
	if job.StartTime == nil || job.Accounting != nil || prior.Requeued || provider.AccountingRequeued(job) {
		return job, false, nil
	}
	if s := prior.Accounting; s != nil && (s.AllocatedCPUs != nil || now.Sub(s.ReadTime.Time) < provider.AccountingRefresh) {
		return job, false, nil
	}
	read, found, err := e.p.ReadAccounting(ctx, job.ID)
	if err != nil {
		return job, true, fmt.Errorf("read accounting of job %s for the CPUs and GPUs allocated: %w", job.ID, err)
	}
	if found {
		job.Accounting = read.Accounting
	}
	return job, true, nil
}

// accountingFinal reports whether accounting's record of an ended job is the
// last read it needs (review of PR 118, Astra B7): the job has reached a
// terminal state, and that alone is not complete accounting. It is final when
// it gives the elapsed time over all the job's executions, or once lostGrace
// has passed since the job's end, which bounds the reads of a record that stays
// incomplete; until then, the job is read again on every poll.
func (e *external) accountingFinal(job provider.Job, now time.Time) bool {
	if job.Accounting != nil && job.Accounting.Elapsed != nil {
		return true
	}
	return job.EndTime != nil && now.Sub(*job.EndTime) > e.lostGrace()
}

// mirrorAccounting writes status.atProvider.accounting from the read of the
// recorded job (the caller has passed it through allocatedRead), at
// provider.MirrorAccounting's cadence: a running job's block is rewritten when
// a count changes or once it is a minute old, never only because readTime
// moved. A pass that made allocatedRead stores its readTime, so the next one
// is a minute later. It is unset while the job has not started.
func (e *external) mirrorAccounting(a *v1alpha1.Allocation, job provider.Job, prior *v1alpha1.AllocationObservation, read bool) {
	a.Status.AtProvider.Accounting = provider.MirrorAccounting(job, a.Spec.ForProvider.Batch.GPUs, prior, e.now(), read)
}

// keepAccounting is the finalizer's record step (HBEP-4, Two controllers,
// Delete; HBEP-8, Provider accounting): once the deleting pass's reads have
// succeeded and shown no live job with the tag, and before the Allocation's
// files and finalizer go, it writes acct-<Allocation UID> from those reads. It
// makes no scheduler call; a failed read never reaches it (the finalizer stays,
// PR 76's Astra B3). In order:
//   - a requeued job (provider.AccountingRequeued, or the stored status's
//     sticky flag) is written at once with its counts and elapsed time unknown
//     (science agent, 31 Oct 2026, on the review of PR 118: Astra B1 to B4);
//   - a read of the recorded job that gives its final elapsed time, or shows it
//     never started, is written at once;
//   - else a known final observation in the stored status (provider.FinalKnown)
//     is written at once: a later absence or incomplete read never erases it
//     (review of PR 118, Astra B6);
//   - else, for a recorded job that started, the step waits for the read that
//     gives the final elapsed time, whether the job shows in neither the queue
//     listing nor accounting or accounting's record of it is incomplete (Astra
//     B7). The wait is at most lostGrace, counted from
//     status.atProvider.absentSince, set here if unset; it survives a restart.
//     During deletion absentSince times this wait only: no Lost is judged. After
//     it the record is written with what is known, the missing fields unknown;
//   - an Allocation that never had a job writes no record.
//
// It reports false while it waits. The record is created if absent and never
// rewritten: an AlreadyExists is success, and nothing here reads or updates
// it. A failed create keeps the finalizer.
func (e *external) keepAccounting(ctx context.Context, a *v1alpha1.Allocation, rec record, name string, jobs []provider.Job, now time.Time) (bool, error) {
	if rec.jobID == "" && len(jobs) == 0 {
		return true, nil
	}
	o := &a.Status.AtProvider
	var recorded *provider.Job
	for i := range jobs {
		if rec.jobID != "" && jobs[i].ID == rec.jobID {
			recorded = &jobs[i]
		}
	}
	p := a.Spec.ForProvider.Batch
	data := v1alpha1.AccountingRecordData{Provider: e.s.o.Provider, Partition: p.Partition, Tag: name, JobIDs: jobIDs(jobs, rec.jobID)}
	var read *v1alpha1.AllocationAccounting
	requeued := o.Requeued
	if recorded != nil {
		j := *recorded
		j.Requeued = j.Requeued || o.Requeued
		requeued = provider.AccountingRequeued(j)
		read = provider.AccountingOf(j, p.GPUs, o.Accounting, now)
	}
	fromRead := func() {
		data.StartTime, data.EndTime = mtp(recorded.StartTime), mtp(recorded.EndTime)
		fillAccounting(&data, read)
	}
	switch {
	case requeued:
		// Its counts and elapsed time are unknown however long it waits.
		data.StartTime, data.EndTime = o.StartTime, o.EndTime
		if recorded != nil {
			data.StartTime, data.EndTime = mtp(recorded.StartTime), mtp(recorded.EndTime)
		}
	case recorded != nil && (recorded.StartTime == nil || read.ElapsedSeconds != nil):
		fromRead()
	case provider.FinalKnown(o):
		data.StartTime, data.EndTime = o.StartTime, o.EndTime
		fillAccounting(&data, o.Accounting)
	case rec.jobID == "" || (recorded == nil && o.StartTime == nil):
		// No recorded job, or one never seen started: no hours to wait for.
	default:
		if o.AbsentSince == nil {
			o.AbsentSince = mt(now)
		}
		if now.Sub(o.AbsentSince.Time) <= e.lostGrace() {
			return false, nil
		}
		if recorded != nil {
			fromRead()
		} else {
			data.StartTime, data.EndTime = o.StartTime, o.EndTime
			fillAccounting(&data, provider.AccountingLost(&o.StartTime.Time, p.GPUs, o.Accounting, now))
		}
	}
	return true, e.createAccountingRecord(ctx, a, data)
}

// fillAccounting copies the counts and the elapsed time into the record; nil
// leaves them unknown.
func fillAccounting(data *v1alpha1.AccountingRecordData, acct *v1alpha1.AllocationAccounting) {
	if acct != nil {
		data.AllocatedCPUs, data.AllocatedGPUs, data.ElapsedSeconds = acct.AllocatedCPUs, acct.AllocatedGPUs, acct.ElapsedSeconds
	}
}

// createAccountingRecord creates the Allocation's accounting record if it does
// not exist. It never updates, and never reads: a read through the manager's
// client would start a cluster-wide informer for ControllerRevisions.
func (e *external) createAccountingRecord(ctx context.Context, a *v1alpha1.Allocation, data v1alpha1.AccountingRecordData) error {
	rec, err := v1alpha1.NewAccountingRecord(a.Namespace, a.UID, data)
	if err != nil {
		return fmt.Errorf("build the accounting record: %w", err)
	}
	if err := e.s.kube.Create(ctx, rec); err != nil && !kerrors.IsAlreadyExists(err) {
		return fmt.Errorf("write the accounting record %s: %w", rec.Name, err)
	}
	return nil
}

// jobIDs are the IDs of the jobs, and the recorded one if it is not among them,
// in numeric order.
func jobIDs(jobs []provider.Job, recorded string) []string {
	seen := map[string]bool{}
	var ids []string
	for _, id := range append(idsOf(jobs), recorded) {
		if id != "" && !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, k int) bool {
		a, errA := strconv.ParseInt(ids[i], 10, 64)
		b, errB := strconv.ParseInt(ids[k], 10, 64)
		if errA != nil || errB != nil {
			return ids[i] < ids[k]
		}
		return a < b
	})
	return ids
}

func idsOf(jobs []provider.Job) []string {
	ids := make([]string, len(jobs))
	for i, j := range jobs {
		ids[i] = j.ID
	}
	return ids
}
