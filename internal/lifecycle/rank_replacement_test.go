package lifecycle

// A replacement waits for no ranking, and a waiting candidate is not failed by health.startTimeout (HBEP-7, Test
// plan, "A replacement waits for no ranking" and "The waiting candidate and startTimeout"; #616, PR 3). With no wait
// observed on A, a primary's replacement is due at the primary's start, so P, with a live replacement R from 00:10,
// stays chosen and is no peer: ranking never runs against it. Each row runs 18.0 ms and 17.9 ms against P's 20 ms,
// since the exclusion, not the margin, is what is tested.

import (
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// rkBoth runs a row at 18.0 ms and at 17.9 ms.
func rkBoth(t *testing.T, row func(t *testing.T, x time.Duration)) {
	for _, c := range []struct {
		name string
		x    time.Duration
	}{{"18.0 ms", rk180}, {"17.9 ms", rk179}} {
		t.Run(c.name, func(t *testing.T) { row(t, c.x) })
	}
}

// rkReplacementStart is the three scripted jobs of the rows: P at 00:10, X as the row says, and the replacement R
// (third), which pends behind a long wait.
func rkReplacementStart(h *rkRig, xWait time.Duration, pScript pfake.Script) (p, x string) {
	pScript.QueueWait = 10 * time.Minute
	return h.start(nil, pScript, pfake.Script{QueueWait: xWait}, pfake.Script{QueueWait: 3 * time.Hour})
}

// TestAnExtraIsRankedOutWhenItsPeerHasAReplacement: P starts at 00:10 and its replacement R is created
// at that pass, 08:10:20 (no wait observed). P is ready at 00:40 with R live, so it is no peer. X reaches its ranking
// point at 00:45, at 18.0 ms or 17.9 ms, and is ranked out against no peer: ended RankedOut without being ready,
// its Allocation kept, one event with nothing related. P stays ready and unmarked; R is not touched.
func TestAnExtraIsRankedOutWhenItsPeerHasAReplacement(t *testing.T) {
	rkBoth(t, func(t *testing.T, xTpot time.Duration) {
		h := newRkRig(t, true)
		p, x := rkReplacementStart(h, rkXWait, pfake.Script{})
		h.engineOf(p, rk200, 0)
		h.engineOf(x, xTpot, 0)
		h.stepTo(rkAt(0, 10, 20), 10*time.Second, nil)
		reps := hoRepl(h.rig, p)
		if len(reps) != 1 || !reps[0].CreationTimestamp.Time.Equal(rkAt(0, 10, 20)) {
			t.Fatalf("P's replacements at 00:10:20: %+v, want one, created at 08:10:20 (P's start)", reps)
		}
		h.stepTo(rkAt(0, 40, 0), 10*time.Second, nil)
		rkCalibration(t, h, p, x)
		if got := h.readyAt(p); !got.Equal(rkAt(0, 40, 0)) {
			t.Fatalf("P ready at %v, want 00:40", got)
		}
		h.stepTo(rkAt(0, 45, 0), 10*time.Second, nil)
		rkAssertRankedOut(t, h, x, "", rkAt(0, 45, 0))
		in := h.mustInstance(p)
		if in.Status.Phase != v1alpha1.InstanceReady || in.Annotations[annotationRankedOut] != "" {
			t.Fatalf("P is %s, marked %q: with a live replacement it is no peer and is left alone", in.Status.Phase, in.Annotations[annotationRankedOut])
		}
		if r := h.mustInstance(reps[0].Name); ended(r) {
			t.Fatalf("P's replacement ended (%s)", r.Status.EndReason)
		}
	})
}

// TestAnExtraThatPassesFirstWaitsForTheRanking: X reaches its point at 00:35, before P's 00:40. P has a live replacement,
// so it stays chosen, and X waits, passed, in verifying: it is not ready, not ended and not failed. In the pass in
// which P is ready, at 00:40:00, X is ranked out against no peer (P has R live), at 18.0 ms and at 17.9 ms alike.
func TestAnExtraThatPassesFirstWaitsForTheRanking(t *testing.T) {
	rkBoth(t, func(t *testing.T, xTpot time.Duration) {
		h := newRkRig(t, true)
		p, x := rkReplacementStart(h, rkXWait, pfake.Script{})
		h.engineOf(p, rk200, 0)
		h.engineOf(x, xTpot, 19*time.Minute) // 08:15:20 + 19:00 + 40 s = 08:35:00
		h.stepTo(rkAt(0, 34, 50), 10*time.Second, nil)
		rkCalibration(t, h, p, x)
		h.stepTo(rkAt(0, 39, 50), 10*time.Second, func() {
			xi := h.mustInstance(x)
			if now := h.clock.Now(); !now.Before(rkAt(0, 35, 0)) && !passedChecks(xi) {
				t.Fatalf("X has not passed its checks at %s, want it passed from 00:35", now.Format(time.TimeOnly))
			}
			if xi.Status.Phase == v1alpha1.InstanceReady || ended(xi) {
				t.Fatalf("X is %s at %s, want it waiting in verifying until P is ready", xi.Status.Phase, h.clock.Now().Format(time.TimeOnly))
			}
		})
		h.tick(10 * time.Second) // 00:40:00: P is ready
		if got := h.readyAt(p); !got.Equal(rkAt(0, 40, 0)) {
			t.Fatalf("P ready at %v, want 00:40", got)
		}
		// P and X are made in the same second, so a pass handles them in the order of their random names: X is ranked in the
		// pass that makes P ready, 00:40:00, when P is handled first, and in the next, 00:40:10, when X is.
		h.tick(10 * time.Second)
		at := rkAt(0, 40, 0)
		if end := h.mustInstance(x).Status.EndTime; end != nil && end.Time.Equal(rkAt(0, 40, 10)) {
			at = rkAt(0, 40, 10)
		}
		rkAssertRankedOut(t, h, x, "", at)
	})
}

// TestAFailedPrimaryWithAReplacementLeavesTheWaitingExtraChosen: X waits at its point from 00:35. P's job, started at
// 00:10, fails at 00:38 (27 min 40 s). In that pass X is chosen and becomes ready: no ranking holds it back, whether
// or not it would be faster, and P's replacement R, a primary now that its predecessor ended, does not outrank it.
func TestAFailedPrimaryWithAReplacementLeavesTheWaitingExtraChosen(t *testing.T) {
	rkBoth(t, func(t *testing.T, xTpot time.Duration) {
		h := newRkRig(t, true)
		fail := 27*time.Minute + 40*time.Second
		p, x := rkReplacementStart(h, rkXWait, pfake.Script{FailAfter: &fail})
		h.engineOf(p, rk200, 0)
		h.engineOf(x, xTpot, 19*time.Minute)
		h.stepTo(rkAt(0, 37, 50), 10*time.Second, func() {
			if xi := h.mustInstance(x); xi.Status.Phase == v1alpha1.InstanceReady || ended(xi) {
				t.Fatalf("X is %s at %s, want it waiting", xi.Status.Phase, h.clock.Now().Format(time.TimeOnly))
			}
		})
		h.tick(10 * time.Second) // 00:38:00
		if in := h.mustInstance(p); in.Status.Phase != v1alpha1.InstanceEnded || in.Status.EndReason != v1alpha1.ReasonAllocationFailed {
			t.Fatalf("P is %s, ended for %q at 00:38:00, want ended for AllocationFailed", in.Status.Phase, in.Status.EndReason)
		}
		// P and X are made in the same second, so a pass handles them in the order of their random names: X is ready in
		// the pass that sees P gone, 00:38:00, when P is handled first, and in the next, 00:38:10, when X is.
		h.tick(10 * time.Second)
		if got := h.readyAt(x); !got.Equal(rkAt(0, 38, 0)) && !got.Equal(rkAt(0, 38, 10)) {
			t.Fatalf("X ready at %v, want 00:38:00 or 00:38:10, in the pass that sees P gone or the next", got)
		}
	})
}

// TestAnExtraWhoseJobStartsFirstIsChosenAndGetsTheReplacement: X's job starts at 00:05 (a wait of 5 min from
// its submission at 08:00:20, with P's), before P's at 00:10. X is the more advanced, so it is chosen and its replacement is
// created at 00:05, 08:05:20. P, started later, is surplus: it gets no replacement. X is ready at 00:35. P reaches
// its point at 00:40 and is ranked out against no peer, since X, chosen and ready, has a live replacement.
func TestAnExtraWhoseJobStartsFirstIsChosenAndGetsTheReplacement(t *testing.T) {
	rkBoth(t, func(t *testing.T, pTpot time.Duration) {
		h := newRkRig(t, true)
		p, x := h.start(nil, pfake.Script{QueueWait: 10 * time.Minute}, pfake.Script{QueueWait: 5 * time.Minute}, pfake.Script{QueueWait: 3 * time.Hour})
		h.engineOf(x, rk200, 0)
		h.engineOf(p, pTpot, 0)
		h.stepTo(rkAt(0, 5, 20), 10*time.Second, nil)
		if got := h.jobStart(x); !got.Equal(rkAt(0, 5, 20)) {
			t.Fatalf("X's job started at %v, want 08:05:20", got)
		}
		reps := hoRepl(h.rig, x)
		if len(reps) != 1 || !reps[0].CreationTimestamp.Time.Equal(rkAt(0, 5, 20)) {
			t.Fatalf("X's replacements at 00:05:20: %+v, want one, created at 08:05:20", reps)
		}
		h.stepTo(rkAt(0, 35, 0), 10*time.Second, nil)
		if got := h.jobStart(p); !got.Equal(rkAt(0, 10, 20)) {
			t.Fatalf("P's job started at %v, want 08:10:20", got)
		}
		if got := h.readyAt(x); !got.Equal(rkAt(0, 35, 0)) {
			t.Fatalf("X ready at %v, want 00:35", got)
		}
		h.stepTo(rkAt(0, 40, 0), 10*time.Second, nil)
		rkAssertRankedOut(t, h, p, "", rkAt(0, 40, 0))
		if n := len(hoRepl(h.rig, p)); n != 0 {
			t.Fatalf("%d replacements of P, a surplus Instance: it gets none", n)
		}
		if h.mustInstance(x).Status.Phase != v1alpha1.InstanceReady {
			t.Fatal("X stopped serving")
		}
	})
}

// setTunnel writes the Instance's TunnelConnected condition, as the path-state writer does; the rig's own writer is
// off (noTunnels) in the rows that hold a tunnel down.
func (h *rkRig) setTunnel(instance string, on bool) {
	h.t.Helper()
	in := h.mustInstance(instance)
	status, reason := metav1.ConditionTrue, v1alpha1.ReasonProxyOnline
	if !on {
		status, reason = metav1.ConditionFalse, "ProxyOffline"
	}
	apimeta.SetStatusCondition(&in.Status.Conditions, metav1.Condition{
		Type: v1alpha1.ConditionTunnelConnected, Status: status, Reason: reason, Message: "set by the test", LastTransitionTime: metav1.NewTime(h.clock.Now()),
	})
	if err := h.c.Status().Update(h.ctx, in); err != nil {
		h.t.Fatal(err)
	}
}

// rkTimeoutRig is the startTimeout rows' rig: health.startTimeout 1 h, no wait observed (P has R from 00:10), and the
// tunnels written by the test. X's tunnel is as the row says. P's is down until set.
func rkTimeoutRig(t *testing.T, xTunnel bool, xTpot time.Duration) (h *rkRig, p, x string) {
	h = newRkRig(t, true, withProfile(func(s *v1alpha1.PlatformProfileSpec) {
		s.Health = &v1alpha1.HealthThresholds{StartTimeout: dur(time.Hour)}
	}))
	h.noTunnels = true
	p, x = rkReplacementStart(h, rkXWait, pfake.Script{})
	h.engineOf(p, rk200, 0)
	h.engineOf(x, xTpot, 19*time.Minute) // X reaches its point at 00:35
	h.setTunnel(p, false)
	h.setTunnel(x, xTunnel)
	return h, p, x
}

// TestAWaitingCandidateIsNotFailedByStartTimeout: health.startTimeout is 1 h. P's tunnel is down: it passes its checks
// at 00:40 and is held, not ready, by the tunnel's gate. At 01:00 its worker exits and is restarted at rung 1, so
// its deadline is 02:00; its new process passes 30 min after the restart and, with its tunnel up from 01:20, P is
// ready at about 01:30. X reaches its ranking point at 00:35 with its tunnel up: it waits, and is not failed at its
// own deadline, its start instruction plus 1 h, 09:15:20 on the rig's clock. It is ranked out, against no peer, when
// P is ready.
func TestAWaitingCandidateIsNotFailedByStartTimeout(t *testing.T) {
	rkBoth(t, func(t *testing.T, xTpot time.Duration) {
		h, p, x := rkTimeoutRig(t, true, xTpot)
		h.stepTo(rkAt(1, 0, 0), 10*time.Second, nil)
		rkCalibration(t, h, p, x)
		if pi := h.mustInstance(p); pi.Status.ReadyTime != nil || !passedChecks(pi) {
			t.Fatalf("P: ready %v, passed %v, want passed its checks and held by its tunnel", pi.Status.ReadyTime, passedChecks(pi))
		}
		h.agentOf(h.alloc(p)).agent.CrashWorker("worker-0", 1)
		h.tick(10 * time.Second)
		if n := len(h.mustInstance(p).Status.Restarts); n != 1 {
			t.Fatalf("P has %d restarts at 01:00:10, want the part exit's rung-1 restart", n)
		}
		h.stepTo(rkAt(1, 20, 0), 10*time.Second, nil)
		h.setTunnel(p, true)
		deadline := h.mustInstance(x).Status.StartInstructionTime.Add(time.Hour)
		h.stepTo(deadline.Add(time.Minute), 10*time.Second, func() {
			if xi := h.mustInstance(x); ended(xi) || xi.Status.Phase == v1alpha1.InstanceReady {
				t.Fatalf("X is %s (%s) at %s, want it waiting at its ranking point", xi.Status.Phase, xi.Status.EndReason, h.clock.Now().Format(time.TimeOnly))
			}
		})
		h.until(20*time.Minute, 10*time.Second, "P ready", func() bool { return !h.readyAt(p).IsZero() })
		if got := h.readyAt(p); got.Before(rkAt(1, 29, 0)) || got.After(rkAt(1, 31, 0)) {
			t.Fatalf("P ready at %v, want about 01:30 (a rung-1 restart at 01:00:10, then 30 min)", got)
		}
		// P and X are made in the same second, so the pass handles them in the order of their random names: X is ranked in
		// the pass that makes P ready when P comes first, and in the next one when X does. Either way it is the end time.
		ready := h.readyAt(p)
		h.tick(10 * time.Second)
		at := ready
		if end := h.mustInstance(x).Status.EndTime; end != nil && end.Time.Equal(ready.Add(10*time.Second)) {
			at = ready.Add(10 * time.Second)
		}
		rkAssertRankedOut(t, h, x, "", at)
	})
}

// TestACandidateWithNoTunnelFailsAtItsDeadline: the same row with X's tunnel down. X passed its checks at 00:35, but
// a surplus Instance whose tunnel is not connected could never serve, so it keeps today's rule: it fails at its
// deadline, 09:15:20, with TunnelNotConnected, and its Allocation is released. And a chosen Instance with every
// check passed and TunnelConnected False, P here, with no extra beside it, fails at its own deadline with
// TunnelNotConnected as on main: the exemption is keyed on the selection, not on the checks.
func TestACandidateWithNoTunnelFailsAtItsDeadline(t *testing.T) {
	t.Run("a surplus Instance", func(t *testing.T) {
		h, p, x := rkTimeoutRig(t, false, rk179)
		h.stepTo(rkAt(1, 0, 0), 10*time.Second, nil)
		h.agentOf(h.alloc(p)).agent.CrashWorker("worker-0", 1) // P's restart moves its deadline to 02:00
		deadline := h.mustInstance(x).Status.StartInstructionTime.Add(time.Hour)
		h.stepTo(deadline.Add(-10*time.Second), 10*time.Second, nil)
		if xi := h.mustInstance(x); ended(xi) {
			t.Fatalf("X ended (%s) before its deadline", xi.Status.EndReason)
		}
		h.stepTo(deadline.Add(10*time.Second), 10*time.Second, nil)
		xi := h.mustInstance(x)
		if xi.Status.Phase != v1alpha1.InstanceEnded || xi.Status.EndReason != v1alpha1.ReasonTunnelNotConnected {
			t.Fatalf("X is %s, ended for %q at its deadline, want ended for TunnelNotConnected", xi.Status.Phase, xi.Status.EndReason)
		}
		if h.kept(x) {
			t.Fatal("X's Allocation was kept, want it released: a failed Instance releases its Allocation")
		}
	})
	t.Run("a chosen Instance", func(t *testing.T) {
		h := newRkRig(t, true, withProfile(func(s *v1alpha1.PlatformProfileSpec) {
			s.Health = &v1alpha1.HealthThresholds{StartTimeout: dur(time.Hour)}
		}))
		h.noTunnels = true
		h.batch.Enqueue(pfake.Script{QueueWait: 10 * time.Minute})
		h.create(rkMD(nil))
		h.tick(10 * time.Second)
		p := h.instances(hoMD)[0].Name
		h.setTunnel(p, false)
		h.stepTo(rkAt(0, 45, 0), 10*time.Second, nil)
		if pi := h.mustInstance(p); !passedChecks(pi) || pi.Status.ReadyTime != nil {
			t.Fatalf("P: passed %v, ready %v at 00:45, want every check passed and not ready", passedChecks(pi), pi.Status.ReadyTime)
		}
		deadline := h.mustInstance(p).Status.StartInstructionTime.Add(time.Hour)
		h.stepTo(deadline.Add(10*time.Second), 10*time.Second, nil)
		pi := h.mustInstance(p)
		if pi.Status.Phase != v1alpha1.InstanceEnded || pi.Status.EndReason != v1alpha1.ReasonTunnelNotConnected {
			t.Fatalf("P is %s, ended for %q at its deadline, want ended for TunnelNotConnected", pi.Status.Phase, pi.Status.EndReason)
		}
	})
}
