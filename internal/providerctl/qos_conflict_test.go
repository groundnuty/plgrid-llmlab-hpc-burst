package providerctl

// A Create that loses its attempt to another writer while sbatch runs (a new
// leader after a hand-over, HBEP-4 and HBEP-7 on the submission record): the
// outcome of its own attempt is not recorded, and what the Allocation shows is
// derived from the record as it now stands, never from the lost attempt (PR 168's
// review, Astra B1 and Opus N4). The conflict is made through the real
// reconciler, over the real Batch and the fake Slurm: a wrapper around the
// platform lets sbatch answer, then replaces the record on the API server before
// the controller writes the outcome, so the controller's write conflicts, it
// re-reads, and finds another attempt. Expected texts are written by hand.

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// afterSubmit calls hook once, after the platform has answered a Submit and
// before the controller records the outcome.
type afterSubmit struct {
	Platform
	once sync.Once
	hook func()
}

func (s *afterSubmit) Submit(ctx context.Context, req provider.SubmitRequest) (string, provider.SubmitOutcome, error) {
	id, outcome, err := s.Platform.Submit(ctx, req)
	s.once.Do(s.hook)
	return id, outcome, err
}

func withAfterSubmit(s *afterSubmit) realOption {
	return func(e *realEnv) {
		inner := e.opts.NewPlatform
		e.opts.NewPlatform = func(v slurm.Validated) (Platform, io.Closer, error) {
			p, c, err := inner(v)
			s.Platform = p
			return s, c, err
		}
	}
}

// replaceRecord is another leader's write: the Allocation's record becomes
// attempt 2 with the token and state given, and a job ID when jobID is set.
func replaceRecord(e *realEnv, name string, r v1alpha1.SubmissionRecord, jobID string) {
	e.t.Helper()
	a := e.get(name)
	r.Attempt, r.Token, r.Writer, r.Time = 2, "another-leaders-token", "another-leader", mt(e.clock.Now())
	meta.AddAnnotations(a, map[string]string{v1alpha1.AnnotationSubmission: recordValue(r)})
	if jobID != "" {
		meta.AddAnnotations(a, map[string]string{v1alpha1.AnnotationJobID: jobID})
	}
	if err := e.c.Update(e.ctx, a); err != nil {
		e.t.Fatalf("the other leader's write: %v", err)
	}
}

func countReason(r *recorder, reason string) int {
	n := 0
	for _, x := range r.reasons() {
		if x == reason {
			n++
		}
	}
	return n
}

// While the first leader's sbatch runs, the record is replaced; the first
// leader's outcome (a full slot, or an unknown reply) must not reach the
// Allocation's display, whatever the replacement is. The table covers the
// replacement being a refusal with no code (Astra's case), a full slot, an
// attempt in flight, and an accepted job.
func TestALostRecordWriteShowsTheRecordThatNowStands(t *testing.T) {
	const (
		wallLine  = "sbatch: error: QOSMaxWallDurationPerJobLimit"
		limitLine = "sbatch: error: QOSMaxSubmitJobPerUserLimit"
	)
	for _, tc := range []struct {
		name        string
		step        fakeslurm.SubmitStep // attempt 1's outcome
		replacement v1alpha1.SubmissionRecord
		jobID       string
		reason      string // what the Allocation shows
		code        string // the code of the submission mirror
		phase       string // "" when the display leaves it to the next Observe
		wantErr     bool   // Create returns an error: attempt 1 was refused
	}{
		{"a full slot, replaced by a refusal with no code (Astra B1)", fakeslurm.RejectPolicy("QOSMaxSubmitJobPerUserLimit"),
			v1alpha1.SubmissionRecord{State: v1alpha1.SubmissionRejected, Reason: wallLine}, "", wallLine, "", "Submitting", true},
		{"a full slot, replaced by an attempt in flight", fakeslurm.RejectPolicy("QOSMaxSubmitJobPerUserLimit"),
			v1alpha1.SubmissionRecord{State: v1alpha1.SubmissionInFlight}, "",
			"submission attempt 2 is InFlight and no job with the tag is found yet; nothing new is submitted", "", "Submitting", true},
		{"a full slot, replaced by an accepted job", fakeslurm.RejectPolicy("QOSMaxSubmitJobPerUserLimit"),
			v1alpha1.SubmissionRecord{State: v1alpha1.SubmissionDone}, "4242", "", "", "", true},
		{"another reason, replaced by a full slot", fakeslurm.RejectPolicy("QOSMaxWallDurationPerJobLimit"),
			v1alpha1.SubmissionRecord{State: v1alpha1.SubmissionRejected, Reason: limitLine, Code: v1alpha1.ReasonSubmitLimitReached}, "",
			"SubmitLimitReached", "SubmitLimitReached", "Submitting", true},
		{"an unknown reply, replaced by a refusal with no code", fakeslurm.Transient(false),
			v1alpha1.SubmissionRecord{State: v1alpha1.SubmissionRejected, Reason: wallLine}, "", wallLine, "", "Submitting", false},
		{"an unknown reply, replaced by a full slot", fakeslurm.Transient(false),
			v1alpha1.SubmissionRecord{State: v1alpha1.SubmissionRejected, Reason: limitLine, Code: v1alpha1.ReasonSubmitLimitReached}, "",
			"SubmitLimitReached", "SubmitLimitReached", "Submitting", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := fake.NewClock(at(12, 0, 0))
			sl := nowOnlySlurm(clock)
			sl.Script(tc.step)
			rec := &recorder{}
			var e *realEnv
			hook := &afterSubmit{hook: func() { replaceRecord(e, "alloc-a", tc.replacement, tc.jobID) }}
			e = newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a",
				withQOS("now", "", nowProfile()...), withRecorder(rec), withAfterSubmit(hook))
			e.newAlloc("alloc-a", 1)

			e.run("alloc-a", 1)

			// The other leader's record stands, untouched by the lost write.
			a := e.get("alloc-a")
			cur, err := readRecord(a)
			if err != nil || cur.sub == nil || cur.sub.Token != "another-leaders-token" || cur.sub.Attempt != 2 ||
				cur.sub.State != tc.replacement.State || cur.sub.Code != tc.replacement.Code || cur.sub.Reason != tc.replacement.Reason {
				t.Fatalf("record %+v (%v), want the other leader's attempt 2 untouched", cur.sub, err)
			}
			if n := sl.Count("sbatch"); n != 1 {
				t.Fatalf("%d sbatch calls, want 1", n)
			}
			o := a.Status.AtProvider
			if o.Reason != tc.reason {
				t.Errorf("reason %q, want %q: derived from the current record", o.Reason, tc.reason)
			}
			if tc.phase != "" && string(o.Phase) != tc.phase {
				t.Errorf("phase %q, want %q", o.Phase, tc.phase)
			}
			switch {
			case tc.code != "" || tc.phase != "":
				if o.Submission == nil || o.Submission.Code != tc.code || o.Submission.Attempt != 2 || o.Submission.Token != "another-leaders-token" {
					t.Errorf("submission mirror %+v, want the other leader's attempt 2 with code %q", o.Submission, tc.code)
				}
			default:
				if o.Submission != nil {
					t.Errorf("submission mirror %+v, want none: the next Observe derives it from the job", o.Submission)
				}
			}
			// Attempt 1's full slot is nowhere in what is shown.
			if tc.reason != "SubmitLimitReached" {
				if msg := a.GetCondition(xpv2.TypeReady).Message; strings.Contains(msg, "SubmitLimitReached") {
					t.Errorf("Ready message %q names the lost attempt's full slot", msg)
				}
			}
			// The lost attempt reports nothing as if it were the record: no
			// gated full-slot event, and no creation, whatever Create returned.
			if n := countReason(rec, "SubmissionRejected"); n != 0 {
				t.Errorf("%d SubmissionRejected events for an outcome that was not recorded, want 0", n)
			}
			if n := countReason(rec, "CreatedExternalResource"); tc.wantErr && n != 0 {
				t.Errorf("%d CreatedExternalResource events after a refused attempt, want 0", n)
			}
			wantCannot := 0
			if tc.wantErr {
				wantCannot = 1
			}
			if n := countReason(rec, "CannotCreateExternalResource"); n != wantCannot {
				t.Errorf("%d CannotCreateExternalResource events, want %d (Create returns an error exactly when attempt 1 was refused)", n, wantCannot)
			}
			if _, held := e.ctl.shared.limitEvents.Load(a.UID); held {
				t.Error("the event gate was consumed by a refusal that was not recorded")
			}
		})
	}
}
