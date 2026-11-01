package lifecycle

import (
	"strings"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// TestStartTimeoutTunnelNotConnected covers HBEP-7, Instance start: an Instance
// that does not reach ready within health.startTimeout of its start
// instruction fails, with reason TunnelNotConnected when its TunnelConnected
// condition is not True at that moment, False or Unknown, and StartTimeout
// otherwise. An absent condition keeps StartTimeout until the controller writes
// it from frps's proxy list, #90 (`@hpc-burst-science-agent`, 31 Oct 2026, for
// #111). The test writes the condition as that writer would. The failing event
// comes before the status write that ends the Instance, on the shared sequence
// (HBEP-8), and the end reason is the event's reason.
func TestStartTimeoutTunnelNotConnected(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cond   *metav1.Condition
		reason string
	}{
		{"false-offline", &metav1.Condition{Type: v1alpha1.ConditionTunnelConnected, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonProxyOffline, Message: "frps lists the proxy as offline"}, "TunnelNotConnected"},
		{"unknown-unreadable", &metav1.Condition{Type: v1alpha1.ConditionTunnelConnected, Status: metav1.ConditionUnknown, Reason: v1alpha1.ReasonProxyListUnreadable, Message: "frps's list could not be read"}, "TunnelNotConnected"},
		{"true-online", &metav1.Condition{Type: v1alpha1.ConditionTunnelConnected, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonProxyOnline, Message: "frps lists the proxy as online"}, "StartTimeout"},
		{"absent", nil, "StartTimeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rg, writes := sequencedRig(t, withProfile(func(p *v1alpha1.PlatformProfileSpec) {
				p.Health = &v1alpha1.HealthThresholds{StartTimeout: dur(10 * time.Minute)}
			}))
			rg.noTunnels = true // the test writes TunnelConnected itself, or never
			rg.create(mdSpec("md"))
			startUntil(rg, "md", 10*time.Minute, "an Instance starting", func() bool {
				insts := rg.instances("md")
				return len(insts) == 1 && insts[0].Status.Phase == v1alpha1.InstanceStarting
			})
			in := rg.instances("md")[0]
			rg.agentOf(in.Spec.AllocationRef.Name).agent.Silence(true) // it never becomes ready
			if tc.cond != nil {
				c := *tc.cond
				c.LastTransitionTime = metav1.NewTime(rg.clock.Now())
				apimeta.SetStatusCondition(&in.Status.Conditions, c)
				if err := rg.c.Status().Update(rg.ctx, &in); err != nil {
					t.Fatal(err)
				}
			}
			start := in.Status.StartInstructionTime.Time
			startUntil(rg, "md", 15*time.Minute, "the Instance ended", func() bool { return ended(rg.instance(in.Name)) })
			got := rg.instance(in.Name)
			if got.Status.EndReason != tc.reason {
				t.Fatalf("end reason %q, want %s", got.Status.EndReason, tc.reason)
			}
			if at := got.Status.EndTime.Time; at.Before(start.Add(10 * time.Minute)) {
				t.Fatalf("ended at %s, before its start instruction %s + 10m", at.Format(time.TimeOnly), start.Format(time.TimeOnly))
			}
			var ev *RecordedEvent
			for _, e := range rg.rec.Find("failing", tc.reason) {
				if e.Name == in.Name {
					e := e
					ev = &e
				}
			}
			if ev == nil {
				t.Fatalf("no failing/%s event on %s", tc.reason, in.Name)
			}
			endWrite := 0
			for _, w := range *writes {
				if w.name == in.Name && w.phase == v1alpha1.InstanceEnded {
					endWrite = w.seq
					break
				}
			}
			if endWrite == 0 || ev.Seq > endWrite {
				t.Fatalf("failing/%s at seq %d, the ending status write at seq %d; the event comes first", tc.reason, ev.Seq, endWrite)
			}
			healthy := apimeta.FindStatusCondition(got.Status.Conditions, v1alpha1.ConditionHealthy)
			if tunnel := tc.reason == "TunnelNotConnected"; healthy == nil || strings.Contains(healthy.Message, "tunnel is not connected") != tunnel {
				t.Fatalf("Healthy %+v: its message must name the tunnel exactly when the reason is TunnelNotConnected", healthy)
			}
		})
	}
}
