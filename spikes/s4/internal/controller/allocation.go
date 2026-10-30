// Package controller is the spike's Allocation controller: an ExternalClient
// for crossplane-runtime's managed reconciler that maps one Allocation to one
// Slurm job and never submits a job twice.
package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s4/apis/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s4/internal/slurm"
)

// Observe modes. Only ModeTag is the design; the others are controls that
// show what the tests would see if the design were wrong.
const (
	// ModeTag: external name = deterministic tag; Observe looks the tag up in
	// squeue, then sacct.
	ModeTag = "tag"
	// ModeSqueueOnly: like ModeTag but never asks sacct (the naive Observe the
	// issue warns about).
	ModeSqueueOnly = "squeue-only"
	// ModeJobID: crossplane's classic non-deterministic pattern. Create sets
	// the external name to the Slurm job ID, Observe looks only that ID up.
	ModeJobID = "jobid"
)

// Options configure the external client.
type Options struct {
	Mode string
	// GuardSucceeded: once a submission is known (create-succeeded annotation
	// or an observed job ID), "not found" is reported as Lost, never as
	// missing, so it can never cause a second sbatch.
	GuardSucceeded bool
	// GuardInflight: while a create-pending annotation is newer than any
	// succeeded/failed one and younger than InflightWindow, "not found" is an
	// error, not "missing": another reconciler's sbatch may still be running.
	GuardInflight  bool
	InflightWindow time.Duration
	// SacctMargin is added to the Allocation's age for sacct's -S window.
	SacctMargin time.Duration
	// Hooks for the kill tests; empty in normal runs.
	FaultBeforeSbatch string
	FaultAfterSbatch  string
	Log               logging.Logger
}

// Tag returns the deterministic job tag of an Allocation. It includes the UID,
// so a deleted and recreated Allocation with the same name never matches its
// predecessor's job in sacct.
func Tag(a metav1.Object) string {
	sum := sha256.Sum256([]byte(a.GetNamespace() + "/" + a.GetName() + "/" + string(a.GetUID())))
	name := a.GetName()
	if len(name) > 40 {
		name = name[:40]
	}
	return "hb-" + name + "-" + hex.EncodeToString(sum[:6])
}

// TagInitializer sets the external name to the tag before the first Observe.
// It replaces crossplane's NameAsExternalName and runs before the
// create-incomplete check, so the tag is persisted before any sbatch.
type TagInitializer struct{ Client client.Client }

// Initialize implements managed.Initializer.
func (t TagInitializer) Initialize(ctx context.Context, mg resource.Managed) error {
	if meta.GetExternalName(mg) != "" {
		return nil
	}
	meta.SetExternalName(mg, Tag(mg))
	return t.Client.Update(ctx, mg)
}

// Connector builds one external client per reconcile.
type Connector struct {
	Slurm *slurm.Client
	Opts  Options
}

// Connect implements managed.ExternalConnecter.
func (c *Connector) Connect(_ context.Context, _ resource.Managed) (managed.ExternalClient, error) {
	return &external{slurm: c.Slurm, opts: c.Opts}, nil
}

type external struct {
	slurm *slurm.Client
	opts  Options
	found int64 // job ID found by this reconcile's Observe, for Delete
}

var errNotAllocation = errors.New("managed resource is not an Allocation")

// ErrCreateInFlight is returned by Observe while another create may be running.
var ErrCreateInFlight = errors.New("a create for this Allocation started recently and has not completed; not treating the job as missing yet")

func (e *external) Observe(ctx context.Context, mg resource.Managed) (managed.ExternalObservation, error) {
	a, ok := mg.(*v1alpha1.Allocation)
	if !ok {
		return managed.ExternalObservation{}, errNotAllocation
	}
	jobs, err := e.lookup(ctx, a)
	if err != nil {
		return managed.ExternalObservation{}, err
	}
	if len(jobs) == 0 {
		return e.notFound(a)
	}
	// The oldest job is the Allocation's; any other is a duplicate to report
	// (HBEP-7: found by tag and reported).
	job := jobs[0]
	var dups []int64
	for _, j := range jobs[1:] {
		dups = append(dups, j.ID)
	}
	e.found = job.ID
	e.record(a, job, dups)

	obs := managed.ExternalObservation{ResourceExists: true, ResourceUpToDate: true}
	// A job found while the create looks incomplete (the reconciler died after
	// sbatch, or a peer is between sbatch and its annotation write): record
	// the success now. Returning LateInitialized makes the reconciler persist
	// the annotation with an Update.
	if e.opts.Mode != ModeJobID && meta.ExternalCreateIncomplete(a) {
		meta.SetExternalCreateSucceeded(a, time.Now())
		obs.ResourceLateInitialized = true
	}
	if meta.WasDeleted(a) && ended(job) {
		// Nothing left to cancel; let the finalizer go. Reporting a terminal
		// job as existing here would loop Delete forever.
		obs.ResourceExists = false
	}
	return obs, nil
}

func (e *external) lookup(ctx context.Context, a *v1alpha1.Allocation) ([]slurm.Job, error) {
	switch e.opts.Mode {
	case ModeJobID:
		id, err := strconv.ParseInt(meta.GetExternalName(a), 10, 64)
		if err != nil || id <= 0 {
			return nil, nil // classic pattern: no job ID stored means "not created"
		}
		jobs, err := e.slurm.SqueueByID(ctx, id)
		if err != nil || len(jobs) > 0 {
			return jobs, err
		}
		return e.slurm.SacctByID(ctx, id)
	case ModeSqueueOnly:
		return e.slurm.SqueueByTag(ctx, meta.GetExternalName(a))
	default:
		tag := meta.GetExternalName(a)
		jobs, err := e.slurm.SqueueByTag(ctx, tag)
		if err != nil || len(jobs) > 0 {
			return jobs, err
		}
		since := time.Since(a.GetCreationTimestamp().Time) + e.opts.SacctMargin
		return e.slurm.SacctByTag(ctx, tag, since)
	}
}

// notFound decides what "no job anywhere" means.
func (e *external) notFound(a *v1alpha1.Allocation) (managed.ExternalObservation, error) {
	if meta.WasDeleted(a) {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}
	if e.opts.GuardSucceeded {
		submitted := !meta.GetExternalCreateSucceeded(a).IsZero() || a.Status.AtProvider.JobID != 0
		if submitted {
			a.Status.AtProvider.Phase = v1alpha1.PhaseLost
			a.Status.AtProvider.Reason = "a submission was recorded but neither squeue nor sacct shows the job; not resubmitting"
			a.SetConditions(xpv2.Unavailable().WithMessage(a.Status.AtProvider.Reason))
			return managed.ExternalObservation{ResourceExists: true, ResourceUpToDate: true}, nil
		}
	}
	if e.opts.GuardInflight && meta.ExternalCreateIncomplete(a) {
		age := time.Since(meta.GetExternalCreatePending(a))
		if age < e.opts.InflightWindow {
			return managed.ExternalObservation{}, fmt.Errorf("%w (pending for %s, window %s)", ErrCreateInFlight, age.Round(time.Millisecond), e.opts.InflightWindow)
		}
	}
	return managed.ExternalObservation{ResourceExists: false}, nil
}

func (e *external) Create(ctx context.Context, mg resource.Managed) (managed.ExternalCreation, error) {
	a, ok := mg.(*v1alpha1.Allocation)
	if !ok {
		return managed.ExternalCreation{}, errNotAllocation
	}
	if e.opts.FaultBeforeSbatch != "" {
		faultBlock(e.opts.FaultBeforeSbatch, 0)
	}
	p := a.Spec.ForProvider
	id, err := e.slurm.Submit(ctx, slurm.SubmitRequest{
		Tag: Tag(a), Partition: p.Partition, Account: p.Account, TimeLimit: p.TimeLimit, Wrap: p.Wrap,
		Comment: a.GetNamespace() + "/" + a.GetName(),
	})
	if err != nil {
		return managed.ExternalCreation{}, err
	}
	e.opts.Log.Info("sbatch returned", "allocation", a.GetName(), "job", id)
	if e.opts.FaultAfterSbatch != "" {
		faultBlock(e.opts.FaultAfterSbatch, id)
	}
	if e.opts.Mode == ModeJobID {
		meta.SetExternalName(a, strconv.FormatInt(id, 10))
	}
	return managed.ExternalCreation{}, nil
}

// Update is never reached: Observe always reports up to date, and the
// management policies exclude Update. It refuses anyway.
func (e *external) Update(_ context.Context, mg resource.Managed) (managed.ExternalUpdate, error) {
	return managed.ExternalUpdate{}, fmt.Errorf("allocation %s: submitted jobs are never edited; cancel and create a new Allocation", mg.GetName())
}

func (e *external) Delete(ctx context.Context, mg resource.Managed) (managed.ExternalDelete, error) {
	if e.found == 0 {
		return managed.ExternalDelete{}, errors.New("delete: no job found by this reconcile's Observe")
	}
	return managed.ExternalDelete{}, e.slurm.Cancel(ctx, e.found)
}

func (e *external) Disconnect(_ context.Context) error { return nil }

// record writes the observation into status. Observe is the only writer of
// the job ID: status changes made in Create do not survive the reconciler's
// annotation update.
func (e *external) record(a *v1alpha1.Allocation, j slurm.Job, dups []int64) {
	o := &a.Status.AtProvider
	prevRequeued := o.Requeued
	o.Tag = Tag(a)
	o.JobID = j.ID
	o.State = j.State
	o.Source = j.Source
	o.RestartCount = j.RestartCount
	o.Duplicates = dups
	o.SubmitTime, o.StartTime, o.EndTime = mt(j.Submit), mt(j.Start), mt(j.End)
	// The requeue verdict is sticky: after the purge from squeue, v0.0.40
	// sacct has no restart_cnt, so the verdict must survive in status.
	o.Requeued = prevRequeued || j.Requeued
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

func mt(t time.Time) *metav1.Time {
	if t.IsZero() {
		return nil
	}
	m := metav1.NewTime(t)
	return &m
}
