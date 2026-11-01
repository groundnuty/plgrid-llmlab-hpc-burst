package providerctl

// The event gate of a full submit limit (HBEP-8, Repeating conditions): the
// event is emitted when the refusal starts, then at most once an hour while it
// lasts, and a refusal that follows any other outcome starts a new run. These
// tests count the events by their notes over sequences that stay inside the
// hour: "submit limit reached" is the note of the gated event, and a plain
// refusal's event says only Slurm's reason line (PR 168's review, Opus N2 and
// N3). The counts are written by hand from the sequences.

import (
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// notes returns the notes of the recorder's events with the reason, in order.
func notes(r *recorder, reason string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, ev := range r.events {
		if string(ev.Reason) == reason {
			out = append(out, ev.Message)
		}
	}
	return out
}

// limitNotes are the notes of the gated event of a full slot.
func limitNotes(r *recorder) []string {
	var out []string
	for _, n := range notes(r, "SubmissionRejected") {
		if strings.Contains(n, "submit limit reached") {
			out = append(out, n)
		}
	}
	return out
}

// wantNotes fails unless the notes are exactly the given ones, in order.
func wantNotes(t *testing.T, what string, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d events %q, want %d %q", what, len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s: event %d is %q, want %q", what, i+1, got[i], want[i])
		}
	}
}

const (
	limitLineNote = "submit limit reached: sbatch: error: QOSMaxSubmitJobPerUserLimit"
	wallLineNote  = "sbatch: error: QOSMaxWallDurationPerJobLimit"
)

// attempt advances the clock past the Rejected backoff and runs the reconciles
// of one attempt.
func (e *realEnv) attempt(name string) {
	e.clock.Advance(61 * time.Second)
	e.run(name, 2)
}

// Opus N2: a second full slot within the hour, after another refusal ended the
// first run, is a new run and has its own event. Full slot, another reason,
// full slot again, all inside four minutes: the gated event is emitted at
// attempts 1 and 3, and attempt 2's plain event is not a gated one. Deleting
// the gate's entry when another refusal ends the run leaves one gated event.
func TestASecondFullSlotAfterAnotherReasonHasItsOwnEvent(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := nowOnlySlurm(clock)
	sl.Script(fakeslurm.RejectPolicy("QOSMaxSubmitJobPerUserLimit"), fakeslurm.RejectPolicy("QOSMaxWallDurationPerJobLimit"),
		fakeslurm.RejectPolicy("QOSMaxSubmitJobPerUserLimit"), fakeslurm.RejectPolicy("QOSMaxSubmitJobPerUserLimit"))
	rec := &recorder{}
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", withQOS("now", "", nowProfile()...), withRecorder(rec))
	e.newAlloc("alloc-a", 1)

	e.run("alloc-a", 2) // attempt 1: a full slot
	wantNotes(t, "after the first full slot", limitNotes(rec), "attempt 1: "+limitLineNote)

	e.attempt("alloc-a") // attempt 2: another reason
	wantNotes(t, "after another reason", limitNotes(rec), "attempt 1: "+limitLineNote)
	wantNotes(t, "plain refusals", filterOut(notes(rec, "SubmissionRejected"), "submit limit reached"), "attempt 2: "+wallLineNote)

	e.attempt("alloc-a") // attempt 3: a full slot again, inside the hour
	wantNotes(t, "after the second full slot", limitNotes(rec), "attempt 1: "+limitLineNote, "attempt 3: "+limitLineNote)

	e.attempt("alloc-a") // attempt 4: the same full slot, the gate holds
	wantNotes(t, "after a repeat of the second full slot", limitNotes(rec), "attempt 1: "+limitLineNote, "attempt 3: "+limitLineNote)
	if n := sl.Count("sbatch"); n != 4 {
		t.Errorf("%d sbatch calls, want 4 attempts", n)
	}
}

func filterOut(in []string, sub string) []string {
	var out []string
	for _, s := range in {
		if !strings.Contains(s, sub) {
			out = append(out, s)
		}
	}
	return out
}

// Opus N3, an Unknown outcome: a full slot, then an attempt whose reply is
// lost (Unknown), then, once the in-flight window has dropped it, a full slot
// again inside the hour. The Unknown outcome ended the run, so the second full
// slot is a new one with its own event.
func TestAFullSlotAfterAnUnknownOutcomeHasItsOwnEvent(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := nowOnlySlurm(clock)
	sl.Script(fakeslurm.RejectPolicy("QOSMaxSubmitJobPerUserLimit"), fakeslurm.Transient(false), fakeslurm.RejectPolicy("QOSMaxSubmitJobPerUserLimit"))
	rec := &recorder{}
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", withQOS("now", "", nowProfile()...), withRecorder(rec))
	e.newAlloc("alloc-a", 1)

	e.run("alloc-a", 2)  // attempt 1: a full slot
	e.attempt("alloc-a") // attempt 2: the reply is lost
	if r, _ := readRecord(e.get("alloc-a")); r.sub == nil || r.sub.Attempt != 2 || r.sub.State != v1alpha1.SubmissionUnknown {
		t.Fatalf("record %+v, want attempt 2 Unknown", r.sub)
	}
	// The in-flight window (5 min) passes; attempt 3 replaces the Unknown one.
	clock.Advance(6 * time.Minute)
	e.run("alloc-a", 2)
	if r, _ := readRecord(e.get("alloc-a")); r.sub == nil || r.sub.Attempt != 3 || r.sub.Code != v1alpha1.ReasonSubmitLimitReached {
		t.Fatalf("record %+v, want attempt 3 refused for the limit", r.sub)
	}
	wantNotes(t, "full slot, Unknown, full slot", limitNotes(rec), "attempt 1: "+limitLineNote, "attempt 3: "+limitLineNote)
}

// Opus N3, a refusal before any call: a full slot, then a request the profile's
// bounds refuse before sbatch (the partition's maxTime is cut below the
// Allocation's 30m), then the bound is restored and the slot is still full.
// The refusal before any call ended the run, so the second full slot has its
// own event. Attempt 2 makes no sbatch call.
func TestAFullSlotAfterARefusalBeforeAnyCallHasItsOwnEvent(t *testing.T) {
	clock := fake.NewClock(at(12, 0, 0))
	sl := nowOnlySlurm(clock)
	sl.Script(fakeslurm.RejectPolicy("QOSMaxSubmitJobPerUserLimit"), fakeslurm.RejectPolicy("QOSMaxSubmitJobPerUserLimit"))
	rec := &recorder{}
	e := newRealEnv(t, clock, sl, newFakeClient(t, "prov-a"), "prov-a", "user-a", withQOS("now", "", nowProfile()...), withRecorder(rec))
	e.newAlloc("alloc-a", 1)

	e.run("alloc-a", 2) // attempt 1: a full slot
	wantNotes(t, "after the first full slot", limitNotes(rec), "attempt 1: "+limitLineNote)

	short := v1alpha1.Duration("10m")
	long := v1alpha1.Duration("48h")
	e.profiles.set(func(v *slurm.Validated) { v.Profile.Partition.MaxTime = &short })
	e.attempt("alloc-a") // attempt 2: refused before any call
	if r, _ := readRecord(e.get("alloc-a")); r.sub == nil || r.sub.Attempt != 2 || r.sub.State != v1alpha1.SubmissionRejected || r.sub.Code != "" ||
		!strings.Contains(r.sub.Reason, "above maxTime") {
		t.Fatalf("record %+v, want attempt 2 refused before any call (maxTime)", r.sub)
	}
	if n := sl.Count("sbatch"); n != 1 {
		t.Fatalf("%d sbatch calls after the refusal before any call, want 1 (attempt 1's)", n)
	}

	e.profiles.set(func(v *slurm.Validated) { v.Profile.Partition.MaxTime = &long })
	e.attempt("alloc-a") // attempt 3: a full slot again
	wantNotes(t, "full slot, refused before any call, full slot", limitNotes(rec), "attempt 1: "+limitLineNote, "attempt 3: "+limitLineNote)
}
