package lifecycle

// An Instance that was ready before and ranks again: a marked peer that restarts inside the front's window, and an
// outranker that was ready before (HBEP-7, How it counts, What step 7 takes, 1; the Fable review of #635, majors 1
// and 2). Both rows are hand-worked on the cut-short fixture's timeline (rank_cutshort_test.go): P's job starts at
// 08:10:20 and X's at 08:15:20, an Instance reaches its ranking point at its start + its StartDelay + 40 s, and the
// front's window is 10 s.

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// TestAMarkedPeerThatRestartsNeverOutranks: X is chosen and ready at 00:40 measuring 20 ms; P outranks it at 00:45:00
// measuring 17.9 ms, so P is ready and X is marked for P. At 00:45:03, inside the 10 s window, X's worker exits and X
// restarts at rung 1; it re-verifies, and when it re-passes, 24 min 40 s later, it measures 14 ms, lower than P's 17.9
// by more than 10% (14 x 10 = 140 against 17.9 x 9 = 161.1). A candidate that carries the mark never outranks: X ends
// RankedOut, never ready again, its Allocation kept, with P in the event's related; P stays ready, chosen and
// unmarked, and in no pass is the deployment without a primary. Were X ranked, it would mark P and become ready, both
// would be marked and surplus, and both would drain.
func TestAMarkedPeerThatRestartsNeverOutranks(t *testing.T) {
	l := &writeLog{}
	h, p, x := rkCutShort(t, l)
	h.tick(10 * time.Second) // 00:45:00: P ready, X marked
	if got := h.mustInstance(x).Annotations[annotationRankedOut]; got != p || hoPhase(h.rig, p) != v1alpha1.InstanceReady {
		t.Fatalf("X's mark %q, P %s at 00:45:00, want X marked for %s and P ready", got, hoPhase(h.rig, p), p)
	}
	h.tick(3 * time.Second) // 00:45:03
	h.tpot[h.alloc(x)] = 14 * time.Millisecond
	h.agentOf(h.alloc(x)).agent.CrashWorker("worker-0", 1)
	h.tick(time.Second) // 00:45:04: X's part has exited and X restarts
	if n := len(h.mustInstance(x).Status.Restarts); n != 1 {
		t.Fatalf("X has %d restarts at 00:45:04, want its rung-1 restart", n)
	}
	h.until(45*time.Minute, 10*time.Second, "X ranked out", func() bool {
		if prim := h.passOf(hoMD).primaries(); len(prim) == 0 {
			t.Fatalf("no primary at %s", h.clock.Now().Format(time.TimeOnly))
		}
		if pi := h.mustInstance(p); pi.Status.Phase != v1alpha1.InstanceReady || pi.Annotations[annotationRankedOut] != "" {
			t.Fatalf("P is %s, marked %q at %s, want it ready, chosen and unmarked", pi.Status.Phase, pi.Annotations[annotationRankedOut], h.clock.Now().Format(time.TimeOnly))
		}
		if xi := h.mustInstance(x); xi.Status.Phase == v1alpha1.InstanceReady || xi.Status.Phase == v1alpha1.InstanceDraining {
			t.Fatalf("X is %s at %s, want it restarting and then ranked out, never ready again", xi.Status.Phase, h.clock.Now().Format(time.TimeOnly))
		}
		return ended(h.mustInstance(x))
	})
	xi := h.mustInstance(x)
	if xi.Status.EndReason != v1alpha1.ReasonRankedOut || !h.kept(x) {
		t.Fatalf("X ended for %q, Allocation kept %v, want RankedOut with its Allocation kept", xi.Status.EndReason, h.kept(x))
	}
	if evs := h.rankedOutEvents(x); len(evs) != 1 || evs[0].RelatedName != p {
		t.Fatalf("RankedOut events regarding X: %+v, want one with %s related", evs, p)
	}
	if hoPhase(h.rig, p) != v1alpha1.InstanceReady || h.readyCount(hoMD) != 1 {
		t.Fatal("P stopped serving")
	}
}

// TestAReReadyOutrankerWaitsTheFullWindowFromItsNewTransition: P is ready at 00:40 measuring 20 ms. At 00:41 its
// worker exits and P restarts, so it is no longer ready (Healthy goes False). X reaches its ranking point at 00:45:00
// measuring 20 ms and, P not being ready, is chosen and becomes ready. P re-passes, measuring 17.9 ms now, which is
// lower than X's 20 ms by more than 10% (179 against 180), and is ready again: P's status.readyTime still says 00:40,
// but its Healthy condition has just turned True. X is marked, and drains only 10 s after that transition: both stay
// ready through T+9 s, once a second, and X drains at T+10 s, never at T. Measured from status.readyTime, the front's
// window would have been over at 00:40:10, and X would drain in the pass P became ready, before the front admitted it.
func TestAReReadyOutrankerWaitsTheFullWindowFromItsNewTransition(t *testing.T) {
	h := newRkRig(t, false)
	p, x := h.start(nil, pfake.Script{QueueWait: 10 * time.Minute}, pfake.Script{QueueWait: rkXWait})
	h.stepTo(rkAt(0, 41, 0), 10*time.Second, nil)
	rkCalibration(t, h, p, x)
	if got := h.readyAt(p); !got.Equal(rkAt(0, 40, 0)) {
		t.Fatalf("P ready at %v, want 00:40", got)
	}
	h.tpot[h.alloc(p)] = rk179 // what P measures when it re-passes
	h.agentOf(h.alloc(p)).agent.CrashWorker("worker-0", 1)
	h.tick(10 * time.Second) // 00:41:10: P restarts
	if n := len(h.mustInstance(p).Status.Restarts); n != 1 || hoPhase(h.rig, p) == v1alpha1.InstanceReady {
		t.Fatalf("P: %d restarts, %s, want its rung-1 restart and not ready", n, hoPhase(h.rig, p))
	}
	h.stepTo(rkAt(0, 45, 0), 10*time.Second, nil)
	if got := h.readyAt(x); !got.Equal(rkAt(0, 45, 0)) {
		t.Fatalf("X ready at %v, want 00:45:00: P is not ready, so X is chosen", got)
	}
	reReady := func() bool {
		pi := h.mustInstance(p)
		at, ok := frontAdmitsAt(pi)
		return pi.Status.Phase == v1alpha1.InstanceReady && ok && at.After(rkAt(0, 41, 0).Add(frontProbeInterval))
	}
	h.until(45*time.Minute, 10*time.Second, "P ready again", reReady)
	tr := h.clock.Now()
	if got := h.readyAt(p); !got.Equal(rkAt(0, 40, 0)) {
		t.Fatalf("P's status.readyTime is %v, want 00:40, which becomeReady writes once: the premise of the row", got)
	}
	if got := h.mustInstance(x).Annotations[annotationRankedOut]; got != p {
		t.Fatalf("X's mark %q, want %q: P outranked X", got, p)
	}
	for _, n := range []string{p, x} {
		if ph := hoPhase(h.rig, n); ph != v1alpha1.InstanceReady {
			t.Fatalf("%s is %s in the pass P became ready again, want both ready: X drains only after the front's window", n, ph)
		}
	}
	h.stepTo(tr.Add(9*time.Second), time.Second, func() {
		for _, n := range []string{p, x} {
			if ph := hoPhase(h.rig, n); ph != v1alpha1.InstanceReady {
				t.Fatalf("%s is %s at T+%s, want both ready through the window", n, ph, h.clock.Now().Sub(tr))
			}
		}
	})
	h.tick(time.Second) // T+10 s
	if ph := hoPhase(h.rig, x); ph != v1alpha1.InstanceDraining {
		t.Fatalf("X is %s at T+10 s, want draining", ph)
	}
	if ph := hoPhase(h.rig, p); ph != v1alpha1.InstanceReady {
		t.Fatalf("P is %s, want ready", ph)
	}
}
