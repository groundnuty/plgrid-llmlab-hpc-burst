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
// usable one, then never again). It reports whether it read.
func (e *external) allocatedRead(ctx context.Context, job provider.Job, prior *v1alpha1.AllocationObservation, now time.Time) (provider.Job, bool, error) {
	if job.StartTime == nil || job.Accounting != nil {
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
// Delete; HBEP-8, Provider accounting): once no job with the tag is live, and
// before the Allocation's files and finalizer go, it writes acct-<Allocation
// UID> from the reads the deleting pass already made. It makes no scheduler
// call. It reports false while it waits for the final read of a job that started:
// the recorded job shows in neither the queue listing nor accounting, so its
// elapsed time cannot be known yet, and the wait is at most lostGrace, counted
// from status.atProvider.absentSince (set here when the job was not already
// absent), which survives a restart. After that the record is written with the
// elapsed time and the end time unknown. A job that never started has no hours
// to wait for and is written at once, and an Allocation that never had a job
// writes no record.
//
// The record is created if absent and never rewritten: an AlreadyExists is
// success, and nothing here reads or updates it. A failed create keeps the
// finalizer.
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
	var acct *v1alpha1.AllocationAccounting
	switch {
	case recorded != nil:
		acct = provider.AccountingOf(*recorded, p.GPUs, o.Accounting, now)
		data.StartTime, data.EndTime = mtp(recorded.StartTime), mtp(recorded.EndTime)
	case rec.jobID != "" && o.StartTime != nil:
		if o.AbsentSince == nil {
			o.AbsentSince = mt(now)
		}
		if now.Sub(o.AbsentSince.Time) <= e.lostGrace() {
			return false, nil
		}
		acct = provider.AccountingLost(&o.StartTime.Time, p.GPUs, o.Accounting, now)
		data.StartTime = o.StartTime
	}
	if acct != nil {
		data.AllocatedCPUs, data.AllocatedGPUs, data.ElapsedSeconds = acct.AllocatedCPUs, acct.AllocatedGPUs, acct.ElapsedSeconds
	}
	return true, e.createAccountingRecord(ctx, a, data)
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
