package providerctl

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// accountingLookback reaches the accounting window back to the Allocation's
// creation plus one hour (HBEP-4, The Slurm dialect, Accounting).
const accountingLookback = time.Hour

// shared is one Provider controller's state, common to every reconcile.
type shared struct {
	o      Options
	h      *holder
	kube   client.Client // writes, under the provider's field manager
	reader client.Reader // reads from the API server, not the cache
	builds *buildCache
	// finals are the recorded jobs whose end this controller read from
	// accounting: they are not read again (external.final).
	finals sync.Map
	// limitEvents holds, by Allocation UID, when the last SubmitLimitReached
	// event was emitted (limitEventDue). It is kept in memory, so a restart or
	// a new leader emits it once more, which is "when it starts" for that
	// process (HBEP-8, Repeating conditions).
	limitEvents sync.Map
}

// connector implements managed.ExternalConnector.
type connector struct{ s *shared }

var errNotAllocation = errors.New("the managed resource is not an Allocation")

// Connect is the profile reader (HBEP-4, Two controllers): it reads and
// validates the Provider, its PlatformProfile and its credentials, and makes no
// platform call when they fail.
func (c *connector) Connect(ctx context.Context, mg resource.Managed) (managed.ExternalClient, error) {
	a, ok := mg.(*v1alpha1.Allocation)
	if !ok {
		return nil, errNotAllocation
	}
	if ref := a.Spec.ProviderConfigReference; ref == nil || ref.Name != c.s.o.Provider {
		return nil, fmt.Errorf("the Allocation does not name Provider %s; this controller acts on no other Provider's Allocations", c.s.o.Provider)
	}
	v, p, err := c.s.h.get(ctx)
	var invalid *provider.InvalidProfileError
	if errors.As(err, &invalid) {
		return &external{s: c.s, refused: err}, nil
	}
	if err != nil {
		return nil, err
	}
	return &external{s: c.s, v: v, p: p}, nil
}

// external implements managed.ExternalClient for one reconcile.
type external struct {
	s *shared
	v slurm.Validated
	p Platform
	// refused is set when the profile is invalid: Observe makes no call.
	refused error
	// toCancel are the live jobs with the tag that Observe found on a
	// deleting Allocation, for Delete in the same reconcile.
	toCancel []string
}

var (
	exists  = managed.ExternalObservation{ResourceExists: true, ResourceUpToDate: true}
	missing = managed.ExternalObservation{ResourceExists: false}
)

func (e *external) now() time.Time { return e.s.o.Clock.Now().UTC() }

// mt is t as a metav1.Time in whole seconds, UTC: metav1.Time keeps no more.
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

func (e *external) hold() bool { return e.v.Profile.Spec.HoldRelease }

func (e *external) duration(d *v1alpha1.Duration, def time.Duration) time.Duration {
	if d == nil {
		return def
	}
	v, err := d.AsDuration()
	if err != nil || v <= 0 {
		return def
	}
	return v
}

func (e *external) inflightWindow() time.Duration {
	return e.duration(e.v.Profile.Spec.InflightWindow, 5*time.Minute)
}
func (e *external) lostGrace() time.Duration {
	return e.duration(e.v.Profile.Spec.LostGrace, slurm.DefaultLostGrace)
}
func (e *external) unknownBound() time.Duration {
	return e.duration(e.v.Profile.Spec.UnknownBound, slurm.DefaultUnknownBound)
}

func since(a *v1alpha1.Allocation) time.Time {
	return a.CreationTimestamp.Add(-accountingLookback)
}

// attemptTime is when the attempt was made: its record's time, or, for a
// record with none, the Allocation's creation.
func attemptTime(a *v1alpha1.Allocation, r *v1alpha1.SubmissionRecord) time.Time {
	if r.Time != nil {
		return r.Time.Time
	}
	return a.CreationTimestamp.Time
}

// needsResolution reports whether the record is an attempt whose job is not
// known: InFlight or Unknown, or Done without a recorded job ID (a record
// edited by hand), which is treated as conservatively as Unknown.
func needsResolution(r record) bool {
	return unresolved(r.sub) || (r.sub != nil && r.sub.State == v1alpha1.SubmissionDone && r.jobID == "")
}

func (e *external) event(a *v1alpha1.Allocation, ev event.Event) { e.s.o.Recorder.Event(a, ev) }

// Observe implements managed.ExternalClient.
func (e *external) Observe(ctx context.Context, mg resource.Managed) (managed.ExternalObservation, error) {
	a, ok := mg.(*v1alpha1.Allocation)
	if !ok {
		return managed.ExternalObservation{}, errNotAllocation
	}
	name := meta.GetExternalName(a)
	if name == "" {
		return managed.ExternalObservation{}, errors.New("the Allocation has no external name; the tag initializer sets it before any submission")
	}
	o := &a.Status.AtProvider
	o.Tag = name
	a.Status.Retention = &v1alpha1.RetentionStatus{EffectiveMode: a.Spec.RetentionPolicy.EffectiveMode()}
	rec, err := readRecord(a)
	if err != nil {
		return exists, err
	}
	if e.refused != nil {
		if rec.jobID == "" && rec.sub == nil && !meta.WasDeleted(a) {
			o.Phase, o.Reason = v1alpha1.AllocationRefused, truncate(e.refused.Error())
			a.SetConditions(xpv2.Unavailable().WithMessage(o.Reason))
		}
		return exists, e.refused
	}
	if a.Spec.ForProvider.Batch == nil {
		return exists, errors.New("forProvider.batch is not set: the Slurm provider serves batch Allocations only")
	}
	now := e.now()
	if meta.WasDeleted(a) {
		return e.observeDeleting(ctx, a, rec, name, now)
	}
	// Refused is kept once set, and a Refused Allocation with no job submits
	// nothing.
	if o.Phase == v1alpha1.AllocationRefused && rec.jobID == "" {
		return exists, nil
	}
	if rec.jobID == "" {
		obs, done, err := e.establish(ctx, a, &rec, name, now)
		if done || err != nil {
			return obs, err
		}
	}
	return e.observeRecorded(ctx, a, &rec, name, now)
}

// choose picks the job to adopt when none is recorded (HBEP-7, open question
// 11, Adoption): a live job that is not held, then a job that ran, then the
// lowest job ID. Only a recorded job's hold is ever lifted, so a live job that
// is not held, or one that ran, proves a lost record. jobs are sorted by job
// ID. Ported from spike S4's choose.
func choose(jobs []provider.Job) provider.Job {
	for _, j := range jobs {
		if j.EndTime == nil && !j.Held() {
			return j
		}
	}
	for _, j := range jobs {
		if j.StartTime != nil {
			return j
		}
	}
	return jobs[0]
}

// adopt records job as the Allocation's. It marks the attempt Done only when
// it is Unknown, or InFlight past the in-flight window; a claimant inside the
// window resolves its own attempt when its sbatch returns.
func (e *external) adopt(ctx context.Context, a *v1alpha1.Allocation, rec *record, job provider.Job, now time.Time) error {
	set := map[string]string{v1alpha1.AnnotationJobID: job.ID}
	if s := rec.sub; s != nil && needsResolution(*rec) &&
		(s.State != v1alpha1.SubmissionInFlight || now.Sub(attemptTime(a, s)) > e.inflightWindow()) {
		done := *s
		done.State, done.Writer, done.Time = v1alpha1.SubmissionDone, e.s.o.Identity, mt(now)
		done.Reason = fmt.Sprintf("resolved by job %s, found by tag in %s", job.ID, job.Source)
		set[v1alpha1.AnnotationSubmission] = recordValue(done)
	}
	if err := write(ctx, e.s.kube, a, set); err != nil {
		return fmt.Errorf("adopt job %s: %w", job.ID, err)
	}
	r, err := readRecord(a)
	if err != nil {
		return err
	}
	*rec = r
	e.event(a, event.Normal("JobAdopted", fmt.Sprintf("job %s, found by tag in %s, is the Allocation's", job.ID, job.Source)))
	e.markResolved(a, rec.sub, now)
	return nil
}

// establish handles an Allocation with no recorded job: it adopts a job found
// by tag, waits out an unresolved attempt or a backoff, refuses a
// non-canonical name, waits for the prerequisites, or reports the job missing
// so that crossplane calls Create. done is false only when a job was adopted
// and the recorded job is to be observed next.
func (e *external) establish(ctx context.Context, a *v1alpha1.Allocation, rec *record, name string, now time.Time) (managed.ExternalObservation, bool, error) {
	o := &a.Status.AtProvider
	jobs, err := e.p.FindByTag(ctx, name, since(a))
	if err != nil {
		return exists, true, e.readFailed(err)
	}
	if len(jobs) > 0 {
		if err := e.adopt(ctx, a, rec, choose(jobs), now); err != nil {
			return exists, true, err
		}
		return exists, false, nil
	}
	if needsResolution(*rec) {
		return e.stall(ctx, a, rec, now)
	}
	if s := rec.sub; s != nil && s.State == v1alpha1.SubmissionRejected && now.Sub(attemptTime(a, s)) < e.s.o.RejectBackoff {
		e.showRejected(a, s)
		e.p.SetBusy(name, true)
		return exists, true, nil
	}
	if tag := v1alpha1.JobTag(a.Namespace, a.Name, a.UID); name != tag {
		o.Phase = v1alpha1.AllocationRefused
		o.Reason = fmt.Sprintf("the external name %q is not this Allocation's job tag %q (set by hand, or kept across a UID change); nothing is submitted under it", name, tag)
		a.SetConditions(xpv2.Unavailable().WithMessage(o.Reason))
		return exists, true, nil
	}
	if why := e.prerequisites(a); why != "" {
		o.Phase, o.Reason = v1alpha1.AllocationSubmitting, why
		o.Submission = rec.sub
		a.SetConditions(xpv2.Creating().WithMessage(why))
		e.p.SetBusy(name, true)
		return exists, true, nil
	}
	e.p.SetBusy(name, false)
	return missing, true, nil
}

// showRejected reports a Rejected attempt whose backoff has not passed. A
// refusal for a full submit limit shows phase Submitting and reason
// SubmitLimitReached, read from the record's Code and never from its text, and
// the condition's message carries Slurm's reason line, which the record keeps;
// nothing in them depends on the attempt or the time, so they stay the same
// from one pass to the next (HBEP-4, QOS limits). Any other Rejected attempt
// shows the scheduler's reason as before.
func (e *external) showRejected(a *v1alpha1.Allocation, s *v1alpha1.SubmissionRecord) {
	o := &a.Status.AtProvider
	o.Phase, o.Submission = v1alpha1.AllocationSubmitting, s
	if s.Code == v1alpha1.ReasonSubmitLimitReached {
		o.Reason = s.Code
		a.SetConditions(xpv2.Creating().WithMessage(fmt.Sprintf("%s: the last attempt was refused for a full submit limit (%s); the next waits for backoff", s.Code, truncate(s.Reason))))
		return
	}
	o.Reason = truncate(s.Reason)
	a.SetConditions(xpv2.Creating().WithMessage("the last attempt was rejected; the next waits for backoff"))
}

// showRecord reports the record a holds now, for a Create that lost its attempt
// to another writer (writeOwn re-read the record and found another token). It
// is the derivation every pass makes: a Rejected attempt is showRejected, an
// attempt in flight or unknown is showAttempt, and a Done or Abandoned one, or
// a record that cannot be read, leaves nothing of the last observation, as an
// accepted Create does, for the next Observe to derive from the job.
func (e *external) showRecord(a *v1alpha1.Allocation) {
	rec, err := readRecord(a)
	switch s := rec.sub; {
	case err == nil && s != nil && s.State == v1alpha1.SubmissionRejected:
		e.showRejected(a, s)
	case err == nil && unresolved(s):
		e.showAttempt(a, s)
	default:
		a.Status.AtProvider.Reason, a.Status.AtProvider.Submission = "", nil
		a.SetConditions(xpv2.Creating())
	}
}

// stall applies the retry rules to an unresolved attempt whose tag finds no job
// (HBEP-7, open question 11: Hold mode, Fallback mode, The operator
// condition).
func (e *external) stall(ctx context.Context, a *v1alpha1.Allocation, rec *record, now time.Time) (managed.ExternalObservation, bool, error) {
	name := meta.GetExternalName(a)
	e.p.SetBusy(name, true)
	s := rec.sub
	age := now.Sub(attemptTime(a, s))
	pastWindow := age > e.inflightWindow()
	if e.hold() {
		// Past the window the attempt is dropped and n+1 replaces it: a late
		// job of the old attempt lands held, and settle cancels it.
		if pastWindow {
			return missing, true, nil
		}
		e.showAttempt(a, s)
		return exists, true, nil
	}
	if s.State == v1alpha1.SubmissionInFlight || s.State == v1alpha1.SubmissionDone {
		if !pastWindow {
			e.showAttempt(a, s)
			return exists, true, nil
		}
		u := *s
		u.State, u.Time = v1alpha1.SubmissionUnknown, mt(attemptTime(a, s))
		u.Reason = "no job found after the in-flight window"
		if err := write(ctx, e.s.kube, a, map[string]string{v1alpha1.AnnotationSubmission: recordValue(u)}); err != nil {
			return exists, true, err
		}
		if err := e.reread(a, rec); err != nil {
			return exists, true, err
		}
		s = rec.sub
	}
	// Unknown: only the tag or an operator resolves it.
	if v, ok := a.Annotations[v1alpha1.AnnotationAbandonAttempt]; ok && v == strconv.Itoa(int(s.Attempt)) {
		ab := *s
		ab.State, ab.Writer, ab.Time = v1alpha1.SubmissionAbandoned, e.s.o.Identity, mt(now)
		ab.Reason = "abandoned by an operator (" + v1alpha1.AnnotationAbandonAttempt + ")"
		if err := write(ctx, e.s.kube, a, map[string]string{v1alpha1.AnnotationSubmission: recordValue(ab)}); err != nil {
			return exists, true, err
		}
		if err := e.reread(a, rec); err != nil {
			return exists, true, err
		}
		e.markResolved(a, rec.sub, now)
		e.event(a, event.Normal("SubmissionAbandoned", fmt.Sprintf("attempt %d abandoned by an operator", s.Attempt)))
		a.Status.AtProvider.Phase, a.Status.AtProvider.Submission = v1alpha1.AllocationSubmitting, rec.sub
		return exists, true, nil
	}
	if age > e.unknownBound() {
		e.needOperator(a, s)
	}
	e.showAttempt(a, s)
	return exists, true, nil
}

func (e *external) reread(a *v1alpha1.Allocation, rec *record) error {
	r, err := readRecord(a)
	if err != nil {
		return err
	}
	*rec = r
	return nil
}

// showAttempt reports an attempt that is being waited on: InFlight is
// Submitting, Unknown is SubmissionUnknown. The message is stable, so the
// status does not change from one poll to the next.
func (e *external) showAttempt(a *v1alpha1.Allocation, s *v1alpha1.SubmissionRecord) {
	o := &a.Status.AtProvider
	o.Submission = s
	o.Phase = v1alpha1.AllocationSubmitting
	if s.State == v1alpha1.SubmissionUnknown {
		o.Phase = v1alpha1.AllocationSubmissionUnknown
	}
	o.Reason = fmt.Sprintf("submission attempt %d is %s and no job with the tag is found yet; nothing new is submitted", s.Attempt, s.State)
	a.SetConditions(xpv2.Creating().WithMessage(o.Reason))
}

// needOperator raises SubmissionResolved False, reason NeedsOperator, for an
// attempt still Unknown after unknownBound in fallback mode, with a Warning
// event the first time.
func (e *external) needOperator(a *v1alpha1.Allocation, s *v1alpha1.SubmissionRecord) {
	msg := fmt.Sprintf("attempt %d is still unknown after %s: check the platform for jobs named %s, then set the %s annotation to %d",
		s.Attempt, e.unknownBound(), meta.GetExternalName(a), v1alpha1.AnnotationAbandonAttempt, s.Attempt)
	if a.GetCondition(v1alpha1.ConditionSubmissionResolved).Status != corev1.ConditionFalse {
		e.event(a, event.Warning("SubmissionUnresolved", errors.New(msg)))
	}
	a.SetConditions(xpv2.Condition{Type: v1alpha1.ConditionSubmissionResolved, Status: corev1.ConditionFalse,
		Reason: v1alpha1.ReasonNeedsOperator, Message: msg, LastTransitionTime: *mt(e.now())})
}

// markResolved turns a raised SubmissionResolved condition True, reason
// Resolved, once the attempt is resolved. A condition that was never raised
// is not written; one that is True stays.
func (e *external) markResolved(a *v1alpha1.Allocation, s *v1alpha1.SubmissionRecord, now time.Time) {
	if unresolved(s) || a.GetCondition(v1alpha1.ConditionSubmissionResolved).Status != corev1.ConditionFalse {
		return
	}
	msg := "the attempt is resolved"
	if s != nil {
		msg = fmt.Sprintf("attempt %d is %s", s.Attempt, s.State)
	}
	a.SetConditions(xpv2.Condition{Type: v1alpha1.ConditionSubmissionResolved, Status: corev1.ConditionTrue,
		Reason: v1alpha1.ReasonResolved, Message: msg, LastTransitionTime: *mt(now)})
}

// readFailed is a read that failed: unknown, never absence. The status keeps
// what it showed, and the error is reported.
func (e *external) readFailed(err error) error {
	return fmt.Errorf("a platform read failed, so the job's state is unknown; nothing changes: %w", err)
}

// Update implements managed.ExternalClient. It is never reached: the
// management policies exclude Update, because a submitted job is never edited.
func (e *external) Update(_ context.Context, mg resource.Managed) (managed.ExternalUpdate, error) {
	return managed.ExternalUpdate{}, fmt.Errorf("allocation %s: a submitted job is never edited; delete the Allocation and create another", mg.GetName())
}

// Disconnect implements managed.ExternalClient. The connection is the
// Provider's, shared by every reconcile, so nothing closes here.
func (e *external) Disconnect(context.Context) error { return nil }

func truncate(s string) string {
	if len(s) > 1000 {
		return s[:1000] + "..."
	}
	return s
}
