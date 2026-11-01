package lifecycle

// The tunnel gate applies to an Instance's first entry to ready only
// (`@hpc-burst-science-agent`, 1 Nov 2026, 17:29Z, replacing the 17:14Z
// ruling): after a restart of either rung an Instance that was ready returns to
// ready once its checks pass, whatever its TunnelConnected; with the tunnel
// down it is ready and not eligible at the front (HBEP-6, The front's half),
// the state of a ready Instance whose tunnel dropped. The controller restarts,
// ends and releases nothing on the tunnel's account, and after a restart
// health.startTimeout bounds the checks only. An Instance restarted before it
// was ever ready is still held by the gate and ends with TunnelNotConnected at
// its deadline, not on the ladder. The first start's gate is
// TestReadyWaitsForTunnelConnected and TestStartTimeoutTunnelNotConnected; a
// ready Instance whose tunnel drops, TestReadyInstanceStaysReadyWhenTunnelDrops.
// The front's side is TestProbeRoutesOnTheProbeNotTheTunnelCondition in
// internal/front.

import (
	"slices"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// tunnelDownStartTimeout is a short health.startTimeout, far under the
// default hour, so that a deadline missed on the tunnel's account shows within
// a few rounds of the test. It is several times what the rig's checks need to
// pass after a restart (the tests assert the return to ready takes less than
// half of it), so a deadline missed here would be the gate's doing alone.
const tunnelDownStartTimeout = 10 * time.Minute

// tunnelDownStates are the two states of TunnelConnected that are not True.
var tunnelDownStates = []struct {
	name   string
	status metav1.ConditionStatus
	reason string
}{
	{"false-offline", metav1.ConditionFalse, v1alpha1.ReasonProxyOffline},
	{"unknown-unreadable", metav1.ConditionUnknown, v1alpha1.ReasonProxyListUnreadable},
}

// tunnelDownRestartWindow is restart.window for these tests: wide enough that the restarts the
// tests make, half an hour or more apart, all stay in status.restarts and count towards the
// ladder, so the third is the whole-Instance rung.
const tunnelDownRestartWindow = 12 * time.Hour

func tunnelRestartRig(t *testing.T) *rig {
	t.Helper()
	return newRig(t, withProfile(func(p *v1alpha1.PlatformProfileSpec) {
		p.Health = &v1alpha1.HealthThresholds{StartTimeout: dur(tunnelDownStartTimeout)}
		p.Restart = &v1alpha1.RestartPolicy{Window: dur(tunnelDownRestartWindow)}
	}))
}

// tunnelRestartLedger is what the tunnel's account would show: a restart, an
// end or a release that the engine's own signal does not explain.
type tunnelRestartLedger struct {
	restarts int
}

// checkQuiet fails the test when the Instance restarted more than the engine's
// crashes explain, ended, or lost its Allocation, or when anything other than
// the crashes the test made is recorded as a signal.
func (l tunnelRestartLedger) checkQuiet(rg *rig, name, alloc, when string) {
	rg.t.Helper()
	got := rg.instance(name)
	if got.Status.Phase == v1alpha1.InstanceEnded || got.Status.EndReason != "" || got.Status.EndTime != nil {
		rg.t.Fatalf("%s: the Instance ended (%s, %q) on the tunnel's account", when, got.Status.Phase, got.Status.EndReason)
	}
	if n := len(got.Status.Restarts); n != l.restarts {
		rg.t.Fatalf("%s: %d restarts recorded %+v, want the %d the engine's crashes caused", when, n, got.Status.Restarts, l.restarts)
	}
	for _, r := range got.Status.Restarts {
		if r.Signal != v1alpha1.ReasonPartExited {
			rg.t.Fatalf("%s: a restart on signal %q (%+v), want only PartExited, the crashes of the test", when, r.Signal, got.Status.Restarts)
		}
	}
	wantWorker, wantInstance := min(l.restarts, 2), max(l.restarts-2, 0)
	if g := restartGeneration(got, v1alpha1.WorkerName(0)); g != int64(wantWorker) {
		rg.t.Fatalf("%s: worker restart generation %d, want %d", when, g, wantWorker)
	}
	if g := restartGeneration(got, v1alpha1.PartInstance); g != int64(wantInstance) {
		rg.t.Fatalf("%s: Instance restart generation %d, want %d", when, g, wantInstance)
	}
	a, alive := rg.allocation(alloc)
	if !alive || !a.DeletionTimestamp.IsZero() || rg.batch.Count("scancel") != 0 || rg.submissions() != 1 {
		rg.t.Fatalf("%s: the Allocation was released or replaced (alive %t, scancel %d, sbatch %d)", when, alive, rg.batch.Count("scancel"), rg.submissions())
	}
	if evs := rg.rec.Find("failing", ""); len(evs) != l.restarts {
		rg.t.Fatalf("%s: failing events %+v, want the %d restart events only", when, evs, l.restarts)
	}
}

// TestTunnelDownAloneRestartsNothing covers HBEP-6 (The front's half: the
// controller restarts nothing on the path) and the ruling above: an Instance
// whose TunnelConnected is False or Unknown, with the agent passing every
// check, gets no new restart generation, no restart entry, no end and no
// Allocation release, past several health.startTimeouts, before any restart
// and after each rung. The agent passes every check throughout, so the
// tunnel is the only thing that could hold the Instance; the restarts the
// test makes are the engine's own crashes (the worker, then the worker again,
// then the whole Instance), and each of them is the only restart there is.
func TestTunnelDownAloneRestartsNothing(t *testing.T) {
	for _, tc := range tunnelDownStates {
		t.Run(tc.name, func(t *testing.T) {
			rg := tunnelRestartRig(t)
			rg.create(mdSpec("md"))
			in, alloc := startReady(rg)
			ag := rg.agentOf(alloc).agent
			first := rg.instance(in.Name).Status.ReadyTime
			rg.noTunnels = true // from here the test is the path-state writer
			setTunnel(t, rg, in.Name, tc.status, tc.reason)

			var ledger tunnelRestartLedger
			quietFor := func(d time.Duration, when string) {
				t.Helper()
				for end := rg.clock.Now().Add(d); rg.clock.Now().Before(end); {
					startStep(rg, "md", startTick)
					ledger.checkQuiet(rg, in.Name, alloc, when)
				}
			}
			// Before any restart: three health.startTimeouts, and past the
			// restart window.
			quietFor(3*tunnelDownStartTimeout, "tunnel down, ready, before a restart")
			if got := rg.instance(in.Name).Status.Phase; got != v1alpha1.InstanceReady {
				t.Fatalf("phase %s with the tunnel down, want ready", got)
			}

			// After each rung: the engine crashes, the controller restarts
			// it, the checks pass, the Instance is ready again, and the
			// tunnel alone does nothing more.
			for k, rung := range []string{"worker", "worker", "instance"} {
				starts := len(ag.Starts())
				ag.CrashWorker(healthWorker, 1)
				ledger.restarts++
				startUntil(rg, "md", 2*tunnelDownStartTimeout, "the restart of rung "+rung+" and the return to ready", func() bool {
					got := rg.instance(in.Name).Status
					return len(got.Restarts) == ledger.restarts && got.Phase == v1alpha1.InstanceReady && len(ag.Starts()) > starts
				})
				when := "tunnel down, after restart " + string(rune('1'+k)) + " (" + rung + " rung)"
				ledger.checkQuiet(rg, in.Name, alloc, when)
				got := rg.instance(in.Name)
				if c := apimeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionTunnelConnected); c == nil || c.Status != tc.status {
					t.Fatalf("%s: TunnelConnected %+v; the test's %s must still be there", when, c, tc.status)
				}
				if got.Status.ReadyTime == nil || !got.Status.ReadyTime.Equal(first) {
					t.Fatalf("%s: readyTime %v, want the first one %v kept", when, got.Status.ReadyTime, first)
				}
				quietFor(3*tunnelDownStartTimeout, when)
			}
			if got := healthFailing(rg, in.Name); !slices.Equal(got, []string{"WorkerRestart", "WorkerRestart", "InstanceRestart"}) {
				t.Fatalf("failing events %v, want the three rungs the crashes took", got)
			}
		})
	}
}

// TestRestartedInstanceReturnsToReadyWithItsTunnelDown covers the ruling's third test, the
// rung's return to ready: an Instance that was ready and is restarted is ready again as soon as
// its checks pass, whatever its TunnelConnected, well inside health.startTimeout from the
// restart, with its first ready time kept. It is the same state as a ready Instance whose tunnel
// dropped, so it counts as ready capacity like it. That the front does not route to it until the
// tunnel is back is the front's rule (TestProbeRoutesOnTheProbeNotTheTunnelCondition).
func TestRestartedInstanceReturnsToReadyWithItsTunnelDown(t *testing.T) {
	for _, tc := range tunnelDownStates {
		t.Run(tc.name, func(t *testing.T) {
			rg := tunnelRestartRig(t)
			rg.create(mdSpec("md"))
			in, alloc := startReady(rg)
			name := in.Name
			first := rg.instance(name).Status.ReadyTime
			rg.noTunnels = true
			setTunnel(t, rg, name, tc.status, tc.reason)

			ag := rg.agentOf(alloc).agent
			starts := len(ag.Starts())
			ag.CrashWorker(healthWorker, 1)
			var restartedAt time.Time
			startUntil(rg, "md", 2*tunnelDownStartTimeout, "the Instance ready again, its tunnel still down", func() bool {
				got := rg.instance(name).Status
				if n := len(got.Restarts); n > 0 && restartedAt.IsZero() {
					restartedAt = got.Restarts[n-1].Time.Time
				}
				return len(got.Restarts) > 0 && got.Phase == v1alpha1.InstanceReady && len(ag.Starts()) > starts
			})
			got := rg.instance(name)
			if n := len(got.Status.Restarts); n != 1 || got.Status.Restarts[0].Signal != v1alpha1.ReasonPartExited {
				t.Fatalf("restarts %+v, want the one PartExited the crash caused", got.Status.Restarts)
			}
			if took := rg.clock.Now().Sub(restartedAt); took >= tunnelDownStartTimeout/2 {
				t.Fatalf("ready %s after the restart, want well inside health.startTimeout %s (the checks alone bound it)", took, tunnelDownStartTimeout)
			}
			if c := apimeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionTunnelConnected); c == nil || c.Status != tc.status {
				t.Fatalf("TunnelConnected %+v; the test's %s must still be there", c, tc.status)
			}
			if got.Status.ReadyTime == nil || !got.Status.ReadyTime.Equal(first) {
				t.Fatalf("readyTime %v, want the first one %v kept", got.Status.ReadyTime, first)
			}
			if rg.readyCount("md") != 1 {
				t.Fatalf("ready Instances %d, want 1: a restarted Instance with its tunnel down is ready capacity like a ready one whose tunnel dropped", rg.readyCount("md"))
			}
			if n := healthFailing(rg, name); len(n) != 1 {
				t.Fatalf("failing events %v, want the one restart", n)
			}
			// The Allocation is untouched: a start deadline missed on the tunnel would
			// have ended the Instance and released it.
			if a, alive := rg.allocation(alloc); !alive || !a.DeletionTimestamp.IsZero() || rg.submissions() != 1 {
				t.Fatalf("the Allocation was released or replaced (alive %t, sbatch %d)", alive, rg.submissions())
			}
			// And it stays so past the deadline that would have ended it.
			for end := rg.clock.Now().Add(2 * tunnelDownStartTimeout); rg.clock.Now().Before(end); {
				startStep(rg, "md", startTick)
			}
			if got := rg.instance(name).Status; got.Phase != v1alpha1.InstanceReady || len(got.Restarts) != 1 {
				t.Fatalf("two health.startTimeouts later: phase %s, restarts %+v; want ready with the one restart", got.Phase, got.Restarts)
			}
		})
	}
}

// TestNeverReadyRestartedInstanceWithItsTunnelDownEndsTunnelNotConnected covers the Instance
// that is restarted before it was ever ready (its worker exits during the start), with its tunnel
// down. The gate holds it, since it has never been ready (HBEP-6, Key restriction: a tunnel that
// never connects never makes ready capacity). Its deadline runs from the restart, and at that
// deadline it ends with TunnelNotConnected, as on a first start (HBEP-7, Instance start). It is
// not restarted again on the tunnel's account, which would climb the ladder to RestartLoop for a
// tunnel that is down (HBEP-6, The front's half: the controller restarts nothing on the path):
// the one restart is the engine's crash. The agent passes every check, so the tunnel is the only
// thing that holds it.
func TestNeverReadyRestartedInstanceWithItsTunnelDownEndsTunnelNotConnected(t *testing.T) {
	for _, tc := range tunnelDownStates {
		t.Run(tc.name, func(t *testing.T) {
			rg := tunnelRestartRig(t)
			rg.noTunnels = true // the tunnel is down from the start
			rg.create(mdSpec("md"))
			var in v1alpha1.Instance
			startUntil(rg, "md", 10*time.Minute, "an Instance verifying", func() bool {
				insts := rg.instances("md")
				if len(insts) != 1 || insts[0].Status.Phase != v1alpha1.InstanceVerifying {
					return false
				}
				in = insts[0]
				return true
			})
			setTunnel(t, rg, in.Name, tc.status, tc.reason)
			alloc := in.Spec.AllocationRef.Name
			ag := rg.agentOf(alloc).agent
			ag.CrashWorker(healthWorker, 1)

			var restartedAt time.Time
			startUntil(rg, "md", 3*tunnelDownStartTimeout, "the Instance ended", func() bool {
				got := rg.instance(in.Name).Status
				if n := len(got.Restarts); n > 0 && restartedAt.IsZero() {
					restartedAt = got.Restarts[n-1].Time.Time
				}
				return got.Phase == v1alpha1.InstanceEnded
			})
			got := rg.instance(in.Name).Status
			if got.EndReason != v1alpha1.ReasonTunnelNotConnected || got.ReadyTime != nil {
				t.Fatalf("end reason %q, readyTime %v; want ended TunnelNotConnected, never ready", got.EndReason, got.ReadyTime)
			}
			if n := len(got.Restarts); n != 1 || got.Restarts[0].Signal != v1alpha1.ReasonPartExited {
				t.Fatalf("restarts %+v, want only the engine's crash: nothing restarted on the tunnel's account", got.Restarts)
			}
			// The deadline is the last restart's time plus health.startTimeout: not before
			// it, and within a minute of it.
			if at := got.EndTime.Time; at.Before(restartedAt.Add(tunnelDownStartTimeout)) || at.After(restartedAt.Add(tunnelDownStartTimeout+time.Minute)) {
				t.Fatalf("ended at %s, want the restart %s + health.startTimeout %s (within a minute after)", at.Format(time.TimeOnly), restartedAt.Format(time.TimeOnly), tunnelDownStartTimeout)
			}
			if ev := healthFailing(rg, in.Name); !slices.Equal(ev, []string{"WorkerRestart", "TunnelNotConnected"}) {
				t.Fatalf("failing events %v, want the crash's WorkerRestart and then TunnelNotConnected", ev)
			}
		})
	}
}
