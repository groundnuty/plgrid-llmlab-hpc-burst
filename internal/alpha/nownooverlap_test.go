package alpha

// Row B of issue #376 (nownooverlap.go): its figures against the hour and the profile, the plan's ReleaseWhenIdle as a
// property of the row, the slot rule of Q7 (which job holds the slot, and the decision for both orders of detection), the
// one-slot order on both sides, and its NOT RUNs through Run. Expected values are worked out by hand from HBEP-7, the
// profile's comment and the science agent's rulings on #376, written out here, never read off a run.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Row B's figures: two jobs of 21 minutes, the lead of athena-now.yaml 20 minutes; a hold of 10 + 21 + 3 + 10 + 12 + 3 = 59
// minutes inside the hour (Q3); ReleaseWhenIdle with the live idle limit of 2
// minutes; the owner's go of plgrid-now; two jobs; no QOS.
func TestRowBsFiguresFitTheHour(t *testing.T) {
	r := rowOf("alpha-athena-now-nooverlap")
	tm := r.Clock()
	if r.Lease != 21*time.Minute || tm.Lead() != 20*time.Minute || r.Lease <= tm.Lead() {
		t.Errorf("lease %s and lead %s: want 21 and 20 minutes, the lease longer", r.Lease, tm.Lead())
	}
	if r.SlotHold != 59*time.Minute || r.SlotHold > time.Hour {
		t.Errorf("slot hold %s, want 59 minutes, inside the hour", r.SlotHold)
	}
	if r.StartWait != 10*time.Minute || r.MostJobs != 2 || r.Partition != "plgrid-now" || r.QOS != "" || r.Profile != "athena-now" || r.OwnerGo != OwnerGoAthenaNow {
		t.Errorf("the row is %+v", r)
	}
	if tm.IdleLimit != 2*time.Minute {
		t.Errorf("idle limit %s, want the live 2 minutes", tm.IdleLimit)
	}
	if p := r.Plan(); !p.ReleaseWhenIdle || p.IdleLimit != 2*time.Minute || p.LeaseLimit != 21*time.Minute {
		t.Errorf("the plan is %+v, want ReleaseWhenIdle with 2 minutes and the 21-minute lease", p)
	}
}

// Row B's slot hold is at least the bounds its own run keeps on a pass, each read from what the run uses: the first job's
// start wait as its first clock (FirstClock), the predecessor's lease, the wait for the successor's Allocation
// (SuccessorAllocationWait) and the successor's own start wait (Row.StartWait, the bound of nowSuccessorStart), the
// successor's start to ready (the profile's health.startTimeout, past which the controller fails it and the run ends its
// deployment), and the end once the deployment goes (the idle limit, which ReleaseWhenIdle still waits, and a minute).
// So the stated hold cannot drift below the code's own bounds (the Fable review of PR #406, which found 55 against 59).
func TestRowBsSlotHoldIsAtLeastItsOwnBounds(t *testing.T) {
	r := rowOf("alpha-athena-now-nooverlap")
	tm := r.Clock()
	first, pastEnd := FirstClock(r, 2*time.Hour, r.Lease, DrainWindow(tm.MaxRequestDuration, tm.DrainMargin, 0))
	if pastEnd || first != r.StartWait {
		t.Fatalf("the first clock is %s (past the end %t), want the start wait %s", first, pastEnd, r.StartWait)
	}
	bounds := first + r.Lease + SuccessorAllocationWait + r.StartWait + tm.StartTimeout + tm.IdleLimit + time.Minute
	if r.SlotHold < bounds || r.SlotHold > time.Hour {
		t.Errorf("slot hold %s, below the run's own bounds on a pass, %s, or past the hour", r.SlotHold, bounds)
	}
}

// ReleaseWhenIdle is a property of the row, on exactly the two no-overlap rows (the plan's gap (b) on #376); every other
// row's plan keeps the default retention.
func TestReleaseWhenIdleIsOnTheNoOverlapRowsAlone(t *testing.T) {
	want := map[string]bool{"alpha-helios-nooverlap": true, "alpha-athena-now-nooverlap": true}
	for _, r := range Rows() {
		p := r.Plan()
		if r.ReleaseWhenIdle != want[r.Name] || p.ReleaseWhenIdle != want[r.Name] || (p.ReleaseWhenIdle && p.IdleLimit <= 0) {
			t.Errorf("%s: ReleaseWhenIdle %t, plan %+v; want %t", r.Name, r.ReleaseWhenIdle, p, want[r.Name])
		}
	}
}

// Which listed job holds the slot, as the script's slot guard counts it (slot_jobs): one of the slot's partition that has
// not ended, or that ended and is still completing; a job of another partition, or an ended one, holds nothing. Only a
// job that is not the run's counts as foreign. The run's prefix is matched as the run's ownership matches it, so a
// foreign name that only contains the prefix is still foreign.
func TestTheSlotIsHeldAsTheScriptsGuardCountsIt(t *testing.T) {
	row := rowOf("alpha-athena-now-nooverlap")
	prefix := TagPrefix("run123")
	owned := RunOwned(prefix, nil)
	job := func(name, partition string, phase v1alpha1.AllocationPhase, completing bool) provider.Job {
		return provider.Job{Name: name, Partition: partition, Phase: phase, Completing: completing}
	}
	for _, c := range []struct {
		name    string
		j       provider.Job
		holds   bool
		foreign int
	}{
		{"the owner's running job", job("owner-job", "plgrid-now", v1alpha1.AllocationRunning, false), true, 1},
		{"the owner's pending job", job("owner-job", "plgrid-now", v1alpha1.AllocationPending, false), true, 1},
		{"the owner's held job", job("owner-job", "plgrid-now", v1alpha1.AllocationHeld, false), true, 1},
		{"the owner's job, ended and still completing", job("owner-job", "plgrid-now", v1alpha1.AllocationEnded, true), true, 1},
		{"the owner's job, ended", job("owner-job", "plgrid-now", v1alpha1.AllocationEnded, false), false, 0},
		{"the owner's job, failed", job("owner-job", "plgrid-now", v1alpha1.AllocationFailed, false), false, 0},
		{"the owner's job on another partition", job("owner-job", "plgrid-gpu-a100", v1alpha1.AllocationRunning, false), false, 0},
		{"the run's own job", job(prefix+"0123456789ab", "plgrid-now", v1alpha1.AllocationRunning, false), true, 0},
		{"a foreign name holding the prefix inside it", job("x"+prefix+"0123456789ab", "plgrid-now", v1alpha1.AllocationRunning, false), true, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := HoldsSlot(row, c.j); got != c.holds {
				t.Errorf("HoldsSlot = %t, want %t", got, c.holds)
			}
			if got := ForeignInSlot(row, []provider.Job{c.j}, owned); got != c.foreign {
				t.Errorf("ForeignInSlot = %d, want %d", got, c.foreign)
			}
		})
	}
}

// Q7's decision, for both orders of detection: the owner's job seen in a read (with no refusal, before one, or in the read
// made at once after one) ends the row NOT RUN with no failure; a refusal for the submit limit with no job outside the run
// ever seen is a failure (the ruling: "anything else is a FAIL").
func TestASubmitLimitRefusalIsNotRunOnlyWithTheOwnersJobSeen(t *testing.T) {
	for _, c := range []struct {
		name string
		s    SlotObs
		fail bool
	}{
		{"the owner's job seen, no refusal", SlotObs{Reads: 2, ForeignSeen: true}, false},
		{"the owner's job seen first, then the refusal", SlotObs{Reads: 2, ForeignSeen: true, Refused: true}, false},
		{"the refusal first, the owner's job in the read at once after it", SlotObs{Reads: 3, Refused: true, ForeignSeen: true}, false},
		{"the refusal, no job outside the run seen", SlotObs{Reads: 30, Refused: true}, true},
		{"neither", SlotObs{Reads: 3, Listed: true}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := len(slotChecks(c.s)) > 0; got != c.fail {
				t.Errorf("slotChecks(%+v) = %v, want a failure %t", c.s, slotChecks(c.s), c.fail)
			}
		})
	}
}

// The controller's side of the one-slot order, from the watch's versions in delivery order: the successor's Allocation
// must first appear after a version of the predecessor's that shows its job gone (Ended with jobCompleting false, or its
// deletion). An Ended version still completing, or a Failed one, is not it.
func TestTheSuccessorsAllocationComesAfterThePredecessorLeftTheSlot(t *testing.T) {
	at := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	ver := func(name string, phase v1alpha1.AllocationPhase, completing, deleted bool) stack.AllocObservation {
		a := &v1alpha1.Allocation{}
		a.Name = name
		a.Status.AtProvider.Phase, a.Status.AtProvider.JobCompleting = phase, completing
		at = at.Add(time.Second)
		return stack.AllocObservation{At: at, Name: name, Phase: phase, Deleted: deleted, Allocation: a}
	}
	running, ended, completing, deleted := ver("pre", v1alpha1.AllocationRunning, false, false), ver("pre", v1alpha1.AllocationEnded, false, false),
		ver("pre", v1alpha1.AllocationEnded, true, false), ver("pre", v1alpha1.AllocationEnded, false, true)
	succ := ver("suc", v1alpha1.AllocationSubmitting, false, false)
	for _, c := range []struct {
		name      string
		allocs    []stack.AllocObservation
		ok, after bool
	}{
		{"ended, then the successor", []stack.AllocObservation{running, ended, succ}, true, true},
		{"deleted, then the successor", []stack.AllocObservation{running, deleted, succ}, true, true},
		{"the successor while the predecessor still completes, then the end", []stack.AllocObservation{running, completing, succ, ended}, true, false},
		{"the successor while the predecessor runs", []stack.AllocObservation{running, succ, ended}, true, false},
		{"the predecessor never seen leaving", []stack.AllocObservation{running, completing, succ}, false, false},
		{"no successor", []stack.AllocObservation{running, ended}, false, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			left, created, ok := OneSlotOrder(c.allocs, "pre", "suc")
			if ok != c.ok || (ok && (left < created) != c.after) {
				t.Errorf("OneSlotOrder = %d, %d, %t; want ok %t and after %t", left, created, ok, c.ok, c.after)
			}
		})
	}
}

// The cluster's side of the one-slot order, from its own times alone: submitted at or after the predecessor's end, and
// started after it. The positive control is the queued path's own order, which must fail here (the brief's forbidden
// verification: a successor submitted before the predecessor left is never a pass).
func TestTheOneSlotOrderIsTheClustersOwnTimes(t *testing.T) {
	t0 := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	p := func(d time.Duration) *time.Time { x := t0.Add(d); return &x }
	pre := provider.Job{SubmitTime: t0, StartTime: p(0), EndTime: p(17 * time.Minute)}
	for _, c := range []struct {
		name string
		suc  provider.Job
		want string
	}{
		{"submitted after the end", provider.Job{SubmitTime: t0.Add(17*time.Minute + 10*time.Second), StartTime: p(17*time.Minute + 10*time.Second)}, ""},
		{"submitted at the end", provider.Job{SubmitTime: t0.Add(17 * time.Minute), StartTime: p(17*time.Minute + time.Minute)}, ""},
		{"queued ahead, as the queued path does", provider.Job{SubmitTime: t0.Add(time.Minute), StartTime: p(17*time.Minute + time.Second)}, "the successor was submitted before the predecessor's job ended"},
		{"started beside it", provider.Job{SubmitTime: t0.Add(time.Minute), StartTime: p(2 * time.Minute)}, "the successor started before the predecessor's job ended"},
		{"no start time", provider.Job{SubmitTime: t0.Add(18 * time.Minute)}, "the cluster's times for the two jobs are incomplete"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := strings.Join(oneSlotChecks(pre, c.suc), "; ")
			if (c.want == "") != (got == "") || !strings.Contains(got, c.want) {
				t.Errorf("oneSlotChecks = %q, want %q", got, c.want)
			}
		})
	}
	if q, s, ok := OneSlotQueueWait(pre, provider.Job{SubmitTime: t0.Add(17*time.Minute + 10*time.Second), StartTime: p(19 * time.Minute)}); !ok || q != 110*time.Second || s != 10*time.Second {
		t.Errorf("OneSlotQueueWait = %s, %s, %t; want 1m50s and 10s", q, s, ok)
	}
}

// What a healthy dry run of row B posts, worked out by hand from the dry run's manual clock: the predecessor starts at 0
// and is ready at 8 minutes; its drain start is at 21 - 7 = 14 minutes; the stream runs from 13 to 15; the idle limit of
// 2 minutes ends its job at 17; the successor is submitted 10 s later and starts at once, ready 8 minutes after: 25 min
// 10 s. So the gap is 11 min 10 s, from the predecessor's end to the successor's submission 10 s, and its queue wait 0 s.
func TestRowBsHealthyDryRunPostsItsMeasurements(t *testing.T) {
	row := rowOf("alpha-athena-now-nooverlap")
	cfg := Config{Row: row, Dry: true, Out: filepath.Join(t.TempDir(), "out"), PendingMax: time.Minute, FrontLabel: testFrontLabel}
	rec, err := NewRecorder(cfg.Out)
	if err != nil {
		t.Fatal(err)
	}
	if f, err := Run(context.Background(), cfg, newDrySteps(t, cfg, "nownooverlap"), rec, t.Logf); err != nil || len(f) != 0 {
		t.Fatalf("failures %v, err %v", f, err)
	}
	posts, _ := os.ReadFile(filepath.Join(cfg.Out, PostsFile))
	for _, w := range []string{"alpha-athena-now-nooverlap: PASS", "gap from the drain start to the successor ready, its queue wait included: 11m10s",
		"from the predecessor's job end to the successor's submission: 10s; the successor's queue wait: 0s", "NoOverlapSlot seen: true",
		"ReleaseWhenIdle true, idleLimit 2m0s", "a job outside the run seen in it: false", "created after the predecessor's job left the slot: true",
		"the warm start of the predecessor's job, from its agent's own log: the image staged already", "the warm start of the successor's job, from its agent's own log: the image staged already",
		"the deployment ended at the row's end, every job of the run seen ended: true"} {
		if !strings.Contains(string(posts), w) {
			t.Errorf("the text to post lacks %q:\n%s", w, posts)
		}
	}
	if strings.Contains(string(posts), "sha256:") || strings.Contains(string(posts), "safetensors") || strings.Contains(string(posts), "hb-") {
		t.Errorf("the text to post names a digest, a file or a tag:\n%s", posts)
	}
}

// Row B's NOT RUNs, through Run over the fake cluster: the owner's job in the slot (with or without a refusal seen), a
// successor still waiting past its start wait, and each job's warm start not shown, are NOT RUN with no failure, their
// marker naming why; the owner's job is never touched; a failure of the predecessor's part (a cut stream) wins over each.
func TestRowBsNotRunsAreNotRunUnlessAnotherCheckFails(t *testing.T) {
	row := rowOf("alpha-athena-now-nooverlap")
	for _, c := range []struct {
		fault       string
		want        error
		note        string
		wantFailure string
	}{
		{"slot-taken", ErrSlotTaken, slotTakenWords, ""},
		{"slot-taken-refused", ErrSlotTaken, slotTakenWords, ""},
		{"slot-taken-cut", nil, "", "a stream was cut"},
		{"second-never-started", ErrSecondNotStarted, "within its start wait of 10m0s from its Allocation's creation", ""},
		{"second-never-started-cut", nil, "", "a stream was cut"},
		{"warm-pre-unshown", ErrWarmUnshown, "the predecessor's job: the agent's log of the job could not be read", ""},
		{"warm-suc-unshown", ErrWarmUnshown, "the successor's job: the agent's log of the job could not be read", ""},
	} {
		t.Run(c.fault, func(t *testing.T) {
			cfg := Config{Row: row, Dry: true, DryFault: c.fault, Out: filepath.Join(t.TempDir(), "out"), PendingMax: time.Minute, FrontLabel: testFrontLabel}
			rec, err := NewRecorder(cfg.Out)
			if err != nil {
				t.Fatal(err)
			}
			st := newDrySteps(t, cfg, "nownooverlap")
			failures, err := Run(context.Background(), cfg, st, rec, t.Logf)
			if c.wantFailure != "" {
				if err != nil || NotRun(row, err) || !strings.Contains(Summary(failures), c.wantFailure) {
					t.Fatalf("failures %v, err %v; want a FAIL with %q that outranks the NOT RUN", failures, err, c.wantFailure)
				}
				return
			}
			if !errors.Is(err, c.want) || !NotRun(row, err) || len(failures) != 0 {
				t.Fatalf("failures %v, err %v; want the NOT RUN of %v", failures, err, c.want)
			}
			if note := NotRunNote(row, cfg.PendingMax, err); !strings.Contains(note, c.note) || strings.Contains(note, "hb-") {
				t.Errorf("the marker's note %q does not say %q, or names a tag", note, c.note)
			}
			if c.want == ErrSecondNotStarted {
				return // no record and no text to post: the second job never started, and nothing else failed
			}
			posts, _ := os.ReadFile(filepath.Join(cfg.Out, PostsFile))
			if !strings.Contains(string(posts), row.Name+": NOT RUN") || strings.Contains(string(posts), ": PASS") || strings.Contains(string(posts), "owner-other-project-job") {
				t.Errorf("the text to post does not say NOT RUN, or names the owner's job:\n%s", posts)
			}
			if strings.HasPrefix(c.fault, "slot-taken") {
				// The owner's job is the fake cluster's own record: still running, never cancelled by the run.
				for _, j := range st.plat.Jobs() {
					if j.Name == "owner-other-project-job" && j.Finished {
						t.Errorf("the owner's job has finished: the run must never touch it")
					}
				}
			}
		})
	}
}
