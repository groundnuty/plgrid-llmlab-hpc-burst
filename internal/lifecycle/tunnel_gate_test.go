package lifecycle

import (
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// setTunnel writes the Instance's TunnelConnected condition as #90's
// path-state writer does (HBEP-6, Path state).
func setTunnel(t *testing.T, rg *rig, name string, status metav1.ConditionStatus, reason string) {
	t.Helper()
	in := rg.instance(name)
	apimeta.SetStatusCondition(&in.Status.Conditions, metav1.Condition{
		Type: v1alpha1.ConditionTunnelConnected, Status: status, Reason: reason,
		Message: "set by the test as the path-state writer would", LastTransitionTime: metav1.NewTime(rg.clock.Now()),
	})
	if err := rg.c.Status().Update(rg.ctx, in); err != nil {
		t.Fatal(err)
	}
}

// TestReadyWaitsForTunnelConnected covers HBEP-6, Path state, and HBEP-7,
// Instance start and ready: an Instance becomes ready only with
// TunnelConnected True, also while the condition is absent. Here the agent
// passes every check (verification, qualification and warm-up done), so the
// tunnel is the only thing between the Instance and ready. Without a True
// condition it stays verifying and fails at health.startTimeout:
// TunnelNotConnected when the condition is present and not True (False or
// Unknown), StartTimeout while it is absent (`@hpc-burst-science-agent`,
// 1 Nov 2026). With True written after the checks passed, it becomes ready no
// earlier than that write. The expected reasons are HBEP-7's names, written
// out.
func TestReadyWaitsForTunnelConnected(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status metav1.ConditionStatus // "" leaves the condition absent
		reason string
		end    string // "" means it becomes ready
	}{
		{name: "absent", end: "StartTimeout"},
		{name: "false-offline", status: metav1.ConditionFalse, reason: "ProxyOffline", end: "TunnelNotConnected"},
		{name: "unknown-unreadable", status: metav1.ConditionUnknown, reason: "ProxyListUnreadable", end: "TunnelNotConnected"},
		{name: "true-online", status: metav1.ConditionTrue, reason: "ProxyOnline"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg := newRig(t, withProfile(func(p *v1alpha1.PlatformProfileSpec) {
				p.Health = &v1alpha1.HealthThresholds{StartTimeout: dur(30 * time.Minute)}
			}))
			rg.noTunnels = true // the test writes TunnelConnected itself, or never
			rg.create(mdSpec("md"))
			// Every check passes: WarmedUp is the last condition before ready.
			startUntil(rg, "md", 20*time.Minute, "an Instance verified, qualified and warmed up", func() bool {
				insts := rg.instances("md")
				return len(insts) == 1 && isTrue(insts[0].Status.Conditions, v1alpha1.ConditionWarmedUp)
			})
			in := rg.instances("md")[0]
			// A few more rounds: the agent reports the warm-up done, which
			// alone made the Instance ready before the gate.
			for range 5 {
				startStep(rg, "md", startTick)
			}
			if got := rg.instance(in.Name).Status; got.Phase != v1alpha1.InstanceVerifying || got.ReadyTime != nil {
				t.Fatalf("with every check passed and no TunnelConnected: phase %q, readyTime %v; want verifying, never ready", got.Phase, got.ReadyTime)
			}
			var written time.Time
			if tc.status != "" {
				written = rg.clock.Now()
				setTunnel(t, rg, in.Name, tc.status, tc.reason)
			}
			startUntil(rg, "md", 40*time.Minute, "the Instance ready or ended", func() bool {
				p := rg.instance(in.Name).Status.Phase
				return p == v1alpha1.InstanceReady || p == v1alpha1.InstanceEnded
			})
			got := rg.instance(in.Name).Status
			if tc.end == "" {
				if got.Phase != v1alpha1.InstanceReady || got.ReadyTime == nil || got.ReadyTime.Time.Before(written) {
					t.Fatalf("TunnelConnected True written at %s: phase %q, readyTime %v; want ready, no earlier than the write", written.Format(time.TimeOnly), got.Phase, got.ReadyTime)
				}
				return
			}
			if got.Phase != v1alpha1.InstanceEnded || got.EndReason != tc.end || got.ReadyTime != nil {
				t.Fatalf("phase %q, end reason %q, readyTime %v; want ended with %s and never ready", got.Phase, got.EndReason, got.ReadyTime, tc.end)
			}
			if at, start := got.EndTime.Time, got.StartInstructionTime.Time; at.Before(start.Add(30 * time.Minute)) {
				t.Fatalf("ended at %s, before its start instruction %s + health.startTimeout 30m", at.Format(time.TimeOnly), start.Format(time.TimeOnly))
			}
			if evs := rg.rec.Find(eventReady, "Ready"); len(evs) != 0 {
				t.Fatalf("a ready event was recorded for an Instance whose tunnel never connected: %+v", evs)
			}
		})
	}
}

// TestReadyInstanceStaysReadyWhenTunnelDrops covers the other side of the
// gate: it gates the entry to ready only. A ready Instance whose
// TunnelConnected turns False (frpc gone) or Unknown (frps's list unreadable)
// stays ready, with no restart and no end: a tunnel down is not a dead
// Instance (HBEP-6, frpc in the job), the controller restarts nothing on the
// path (HBEP-6, The front's half: HBEP-7 decides restarts from its own health
// checks), and routing rests on the front's probe, not on the condition alone.
func TestReadyInstanceStaysReadyWhenTunnelDrops(t *testing.T) {
	for _, tc := range []struct {
		status metav1.ConditionStatus
		reason string
	}{
		{metav1.ConditionFalse, "ProxyOffline"},
		{metav1.ConditionUnknown, "ProxyListUnreadable"},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			rg := newRig(t)
			rg.create(mdSpec("md"))
			in, _ := startReady(rg)
			rg.noTunnels = true // from here the test is the path-state writer
			setTunnel(t, rg, in.Name, tc.status, tc.reason)
			// An hour of rounds, past the default health.startTimeout.
			startUntil(rg, "md", 2*time.Hour, "an hour of rounds", func() bool {
				return !rg.clock.Now().Before(in.Status.ReadyTime.Add(time.Hour))
			})
			got := rg.instance(in.Name).Status
			if got.Phase != v1alpha1.InstanceReady || len(got.Restarts) != 0 {
				t.Fatalf("an hour after TunnelConnected turned %s: phase %q, restarts %v; want ready with no restart", tc.status, got.Phase, got.Restarts)
			}
			if c := apimeta.FindStatusCondition(got.Conditions, v1alpha1.ConditionTunnelConnected); c == nil || c.Status != tc.status {
				t.Fatalf("TunnelConnected %+v; the test's %s must still be there", c, tc.status)
			}
		})
	}
}
