// Package controller is the spike's Allocation controller: an ExternalClient
// for crossplane-runtime's managed reconciler that maps one Allocation to one
// Slurm job. The submission protocol is two-phase: sbatch --hold, record the
// job ID, then release exactly that job (see the README and record.go).
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
	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	corev1 "k8s.io/api/core/v1"
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
	Mode     string
	Identity string // this reconciler, recorded in the attempts it claims
	// HoldRelease is the platform profile's hold_release flag: submit with
	// --hold and release only the established job. When false (fallback),
	// an Unknown attempt is never retried without positive resolution.
	HoldRelease bool
	// GuardSucceeded: an established job that is not found is never
	// "missing" (Lost after LostGrace instead). Off only in controls.
	GuardSucceeded bool
	// GuardInflight: an unresolved attempt blocks a new one. Off only in
	// controls.
	GuardInflight bool
	// InflightWindow: how long an attempt may stay unresolved before hold
	// mode retries it (fallback mode: before InFlight becomes Unknown). Must
	// exceed the sbatch timeout.
	InflightWindow time.Duration
	// UnknownBound: fallback mode raises the operator condition after it.
	UnknownBound time.Duration
	// LostGrace: an established job is Lost only after it has been absent
	// from both squeue and sacct this long (profile lost_grace).
	LostGrace time.Duration
	// UnresolvedPoll is the poll interval while a submission is unresolved.
	UnresolvedPoll time.Duration
	// SacctMargin is added to the Allocation's age for sacct's -S window.
	SacctMargin time.Duration
	// Hooks for the kill tests; empty in normal runs.
	FaultBeforeSbatch string
	FaultAfterSbatch  string
	Log               logging.Logger
}

// Condition type and reasons for an unresolved submission.
const (
	TypeSubmissionResolved xpv2.ConditionType   = "SubmissionResolved"
	ReasonNeedsOperator    xpv2.ConditionReason = "NeedsOperator"
	ReasonResolved         xpv2.ConditionReason = "Resolved"
)

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

// canonical refuses a submission under an empty or non-canonical external
// name: a job submitted under any other name is one Observe of a restored or
// hand-edited object could not tell from someone else's.
func canonical(a metav1.Object) error {
	name := meta.GetExternalName(a)
	if name == "" {
		return errors.New("refusing to submit: the external name is empty")
	}
	if want := Tag(a); name != want {
		return fmt.Errorf("refusing to submit: external name %q is not this Allocation's tag %q (set by hand, or kept across a UID change); check Slurm for jobs named %q, then remove the annotation to let the provider set the tag", name, want, name)
	}
	return nil
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
	Slurm    *slurm.Client
	Kube     client.Client // writes (compare-and-swap on resourceVersion)
	Reader   client.Reader // reads from the API server, never the cache
	Recorder event.Recorder
	Opts     Options
}

// Connect implements managed.ExternalConnecter.
func (c *Connector) Connect(_ context.Context, _ resource.Managed) (managed.ExternalClient, error) {
	rec := c.Recorder
	if rec == nil {
		rec = event.NewNopRecorder()
	}
	return &external{slurm: c.Slurm, kube: c.Kube, reader: c.Reader, rec: rec, opts: c.Opts}, nil
}

type external struct {
	slurm    *slurm.Client
	kube     client.Client
	reader   client.Reader
	rec      event.Recorder
	opts     Options
	toCancel []int64 // live jobs with the tag found by this reconcile's Observe, for Delete
}

var errNotAllocation = errors.New("managed resource is not an Allocation")

var exists = managed.ExternalObservation{ResourceExists: true, ResourceUpToDate: true}

func (e *external) Observe(ctx context.Context, mg resource.Managed) (managed.ExternalObservation, error) {
	a, ok := mg.(*v1alpha1.Allocation)
	if !ok {
		return managed.ExternalObservation{}, errNotAllocation
	}
	if e.opts.Mode == ModeJobID {
		return e.observeClassic(ctx, a)
	}
	name := meta.GetExternalName(a)
	if name == "" {
		return managed.ExternalObservation{}, errors.New("no external name: the Initializer sets the tag before any submission")
	}
	rec, err := readRecord(a)
	if err != nil {
		return managed.ExternalObservation{}, err
	}
	o := &a.Status.AtProvider
	o.Tag = name // the persisted name, never a recomputed one

	if rec.attempt.unresolved() && a.GetAnnotations()[AnnoAbandon] == strconv.Itoa(rec.attempt.N) {
		t := *rec.attempt
		t.State, t.Reason = AttemptAbandoned, "abandoned by an operator ("+AnnoAbandon+")"
		if err := e.write(ctx, a, map[string]string{AnnoSubmission: t.value()}); err != nil {
			return managed.ExternalObservation{}, err
		}
		rec.attempt = &t
		e.rec.Event(a, event.Normal("SubmissionAbandoned", fmt.Sprintf("attempt %d abandoned by an operator", t.N)))
	}

	live, all, err := e.lookup(ctx, a, name, rec.jobID)
	if err != nil {
		return managed.ExternalObservation{}, err
	}
	// Adopt whatever the attempt's state: a job found by tag is the answer.
	if rec.jobID == 0 && len(all) > 0 {
		pick := choose(all)
		if err := e.establish(ctx, a, &rec, pick.ID, fmt.Sprintf("adopted job %d found by tag in %s", pick.ID, pick.Source)); err != nil {
			return managed.ExternalObservation{}, err
		}
		e.opts.Log.Info("adopted job", "allocation", a.GetName(), "job", pick.ID, "source", pick.Source)
	}
	// With a job established, an attempt whose claimant cannot still be
	// running sbatch (Unknown, or InFlight past the window) is resolved by
	// that job; one within the window is left to its claimant.
	if t := rec.attempt; rec.jobID != 0 && t.unresolved() && (t.State == AttemptUnknown || time.Since(t.At) >= e.opts.InflightWindow) {
		u := *t
		u.State, u.JobID, u.Reason = AttemptDone, rec.jobID, fmt.Sprintf("resolved by the established job %d", rec.jobID)
		if err := e.write(ctx, a, map[string]string{AnnoSubmission: u.value()}); err != nil {
			return managed.ExternalObservation{}, err
		}
		rec.attempt = &u
	}
	e.markResolved(a, rec)
	if e.opts.HoldRelease && rec.jobID != 0 && !meta.WasDeleted(a) {
		live = e.settle(ctx, a, &rec, live)
	}
	if meta.WasDeleted(a) {
		return e.observeDeletion(a, rec, live), nil
	}
	if rec.jobID != 0 {
		return e.observeEstablished(a, rec, mergeJobs(live, all)), nil
	}
	return e.observeNothing(ctx, a, rec)
}

// lookup lists the tag in squeue and, when that cannot answer (nothing live,
// or the established job has left the queue), in sacct over an explicit
// window. Both lists are sorted by job ID; all merges them, squeue first.
func (e *external) lookup(ctx context.Context, a *v1alpha1.Allocation, name string, established int64) (live, all []slurm.Job, err error) {
	live, err = e.slurm.SqueueByTag(ctx, name)
	if err != nil || e.opts.Mode == ModeSqueueOnly {
		return live, live, err
	}
	if (established != 0 && findJob(live, established) != nil) || (established == 0 && len(live) > 0) {
		return live, live, nil
	}
	since := time.Since(a.GetCreationTimestamp().Time) + e.opts.SacctMargin
	acct, err := e.slurm.SacctByTag(ctx, name, since)
	if err != nil {
		return nil, nil, err
	}
	return live, mergeJobs(live, acct), nil
}

// choose picks the job to adopt when none is established. Only an
// established job is ever released, so a job that is live and not held, or
// one that ran, proves an earlier establishment whose record was lost; it
// wins. Otherwise the lowest job ID wins. Input is sorted by job ID.
func choose(jobs []slurm.Job) slurm.Job {
	for _, j := range jobs {
		if !ended(j) && !j.Held {
			return j
		}
	}
	for _, j := range jobs {
		if !j.Start.IsZero() {
			return j
		}
	}
	return jobs[0]
}

// establish records id as the Allocation's job (compare-and-swap). It
// resolves the current attempt only when no live claimant may still be
// running its sbatch (Unknown, or InFlight past the window); a claimant
// within the window resolves its own attempt. A conflict is returned; the
// reconcile is retried from a fresh read.
func (e *external) establish(ctx context.Context, a *v1alpha1.Allocation, rec *record, id int64, why string) error {
	set := map[string]string{AnnoJobID: strconv.FormatInt(id, 10)}
	if t := rec.attempt; t.unresolved() && (t.State == AttemptUnknown || time.Since(t.At) >= e.opts.InflightWindow) {
		t := *rec.attempt
		t.State, t.JobID, t.Reason = AttemptDone, id, why
		set[AnnoSubmission] = t.value()
	}
	if err := e.write(ctx, a, set); err != nil {
		return err
	}
	r, err := readRecord(a)
	*rec = r
	e.rec.Event(a, event.Normal("JobEstablished", why))
	return err
}

func (e *external) markResolved(a *v1alpha1.Allocation, rec record) {
	o := &a.Status.AtProvider
	if rec.attempt != nil {
		o.Attempt, o.AttemptState = rec.attempt.N, rec.attempt.State
	}
	if !rec.attempt.unresolved() && a.GetCondition(TypeSubmissionResolved).Status == corev1.ConditionFalse {
		a.SetConditions(xpv2.Condition{Type: TypeSubmissionResolved, Status: corev1.ConditionTrue, Reason: ReasonResolved,
			LastTransitionTime: metav1.Now()})
	}
}

// observeNothing decides what "no job established, none found" means.
func (e *external) observeNothing(ctx context.Context, a *v1alpha1.Allocation, rec record) (managed.ExternalObservation, error) {
	o := &a.Status.AtProvider
	t := rec.attempt
	if t.unresolved() && e.opts.GuardInflight {
		age := time.Since(t.At)
		if e.opts.HoldRelease && age >= e.opts.InflightWindow {
			// Safe to retry: a late registration lands held and settle
			// cancels it, because only the established job is released.
			e.opts.Log.Info("unresolved attempt past the window; hold mode retries", "allocation", a.GetName(), "attempt", t.N)
		} else {
			if !e.opts.HoldRelease && t.State == AttemptInFlight && age >= e.opts.InflightWindow {
				u := *t
				u.State, u.Reason = AttemptUnknown, "claim older than the in-flight window with no job found; its claimant died or hangs"
				if err := e.write(ctx, a, map[string]string{AnnoSubmission: u.value()}); err != nil {
					return managed.ExternalObservation{}, err
				}
				t = &u
				o.AttemptState = u.State
			}
			msg := fmt.Sprintf("in-flight guard: submission attempt %d by %s is unresolved (%s since %s); not submitting", t.N, t.By, t.State, t.At.UTC().Format(time.RFC3339))
			e.opts.Log.Info(msg, "allocation", a.GetName())
			o.Phase, o.Reason = v1alpha1.PhaseSubmitting, msg
			if t.State == AttemptUnknown {
				o.Phase = v1alpha1.PhaseUnknown
			}
			if !e.opts.HoldRelease && age >= e.opts.UnknownBound {
				e.needOperator(a, t)
			}
			a.SetConditions(xpv2.Creating().WithMessage(msg))
			return exists, nil
		}
	}
	if err := canonical(a); err != nil {
		o.Phase, o.Reason = v1alpha1.PhaseRefused, err.Error()
		a.SetConditions(xpv2.Unavailable().WithMessage(err.Error()))
		return exists, nil
	}
	return managed.ExternalObservation{ResourceExists: false}, nil
}

func (e *external) needOperator(a *v1alpha1.Allocation, t *Attempt) {
	msg := fmt.Sprintf("submission attempt %d (%s) has no job in squeue or sacct after %s; an operator must decide: check Slurm for jobs named %q, then set %s=%d to allow a new submission",
		t.N, t.Reason, e.opts.UnknownBound, meta.GetExternalName(a), AnnoAbandon, t.N)
	if c := a.GetCondition(TypeSubmissionResolved); c.Status != corev1.ConditionFalse {
		e.rec.Event(a, event.Warning("SubmissionUnresolved", errors.New(msg)))
	}
	a.SetConditions(xpv2.Condition{Type: TypeSubmissionResolved, Status: corev1.ConditionFalse, Reason: ReasonNeedsOperator,
		Message: msg, LastTransitionTime: metav1.Now()})
}

// observeDeletion keeps the finalizer while any job with the tag is live, or
// a submission is unresolved; Delete cancels every live job with the tag.
func (e *external) observeDeletion(a *v1alpha1.Allocation, rec record, live []slurm.Job) managed.ExternalObservation {
	o := &a.Status.AtProvider
	e.toCancel = nil
	for _, j := range live {
		if !ended(j) {
			e.toCancel = append(e.toCancel, j.ID)
		}
	}
	wait := false
	if t := rec.attempt; t.unresolved() {
		wait = !e.opts.HoldRelease || time.Since(t.At) < e.opts.InflightWindow
		if !e.opts.HoldRelease && time.Since(t.At) >= e.opts.UnknownBound {
			e.needOperator(a, t)
		}
	}
	switch {
	case len(e.toCancel) > 0:
		o.Reason = fmt.Sprintf("deleting: cancelling every live job with the tag: %v", e.toCancel)
	case wait:
		o.Reason = fmt.Sprintf("deleting: waiting for submission attempt %d to resolve before releasing the finalizer", rec.attempt.N)
	default:
		return managed.ExternalObservation{ResourceExists: false}
	}
	return exists
}

func (e *external) Delete(ctx context.Context, mg resource.Managed) (managed.ExternalDelete, error) {
	var errs []error
	for _, id := range e.toCancel {
		if err := e.slurm.Cancel(ctx, id); err != nil {
			errs = append(errs, err)
		}
	}
	return managed.ExternalDelete{}, errors.Join(errs...)
}

// Update is never reached: Observe always reports up to date, and the
// management policies exclude Update. It refuses anyway.
func (e *external) Update(_ context.Context, mg resource.Managed) (managed.ExternalUpdate, error) {
	return managed.ExternalUpdate{}, fmt.Errorf("allocation %s: submitted jobs are never edited; cancel and create a new Allocation", mg.GetName())
}

func (e *external) Disconnect(_ context.Context) error { return nil }
