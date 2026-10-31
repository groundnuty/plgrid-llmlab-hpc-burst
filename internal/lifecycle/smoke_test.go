package lifecycle

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// TestSmokeOneInstanceBecomesReady drives one deployment from nothing to one
// ready Instance through the provider fake and a fake agent.
func TestSmokeOneInstanceBecomesReady(t *testing.T) {
	rg := newRig(t)
	rg.create(mdSpec("md"))
	rg.until(10*time.Minute, 10*time.Second, "one ready Instance", func() bool { return rg.readyCount("md") == 1 })
	insts := rg.instances("md")
	if len(insts) != 1 {
		t.Fatalf("want 1 Instance, got %d", len(insts))
	}
	in := insts[0]
	if in.Status.VerificationRecord == "" || in.Status.Verdict != v1alpha1.VerdictPass {
		t.Fatalf("verification record %q verdict %q", in.Status.VerificationRecord, in.Status.Verdict)
	}
	if in.Status.LeaseEnd == nil {
		t.Fatal("no leaseEnd")
	}
	if got := rg.submissions(); got != 1 {
		t.Fatalf("submissions = %d, want 1", got)
	}
	md := rg.md("md")
	if md.Status.ReadyInstances != 1 || md.Status.DesiredInstances != 1 || len(md.Status.CurrentIDs) != 1 {
		t.Fatalf("status %+v", md.Status)
	}
	t.Logf("events: %+v", rg.rec.Events())
	t.Logf("agent errors: %v", rg.errs)
}

// TestSmokeHandover: a 4 h lease with the default drain window (2h10m). The
// primary's job starts at 08:00:20 with no queue wait, which the Provider
// records (one wait of 0 s); its Instance is ready at 08:00:50, so the class's
// start-to-ready time is 30 s. The replacement is planned at the drain start
// (12:00:20 - 2h10m = 09:50:20) minus (0 + 30 s + 30 min margin) = 09:19:50.
// Both serve, and the primary drains at its drain start.
func TestSmokeHandover(t *testing.T) {
	rg := newRig(t, withMaxTime(4*time.Hour))
	rg.create(mdSpec("md"))
	rg.until(10*time.Minute, 10*time.Second, "one ready Instance", func() bool { return rg.readyCount("md") == 1 })
	rg.until(2*time.Hour, 10*time.Second, "two Instances", func() bool { return len(rg.instances("md")) == 2 })
	if want := time.Date(2026, 11, 2, 9, 19, 50, 0, time.UTC); !rg.instances("md")[1].CreationTimestamp.Time.Equal(want) {
		t.Fatalf("replacement created at %s, want %s", rg.instances("md")[1].CreationTimestamp.Time, want)
	}
	rg.until(10*time.Minute, 10*time.Second, "two ready Instances", func() bool { return rg.readyCount("md") == 2 })
	insts := rg.instances("md")
	if len(insts) != 2 || insts[1].Annotations[v1alpha1.AnnotationPredecessor] != insts[0].Name {
		t.Fatalf("want a primary and its replacement, got %d", len(insts))
	}
	primary := insts[0].Name
	drain := insts[0].Status.LeaseEnd.Add(-(2*time.Hour + 10*time.Minute))
	rg.until(3*time.Hour, time.Minute, "primary draining or ended", func() bool {
		p := rg.instance(primary).Status.Phase
		return p == v1alpha1.InstanceDraining || p == v1alpha1.InstanceEnded
	})
	if now := rg.clock.Now(); now.Before(drain) {
		t.Fatalf("primary drained at %s, before its drain start %s", now, drain)
	}
	t.Logf("instances now: %d, submissions %d, events %d", len(rg.instances("md")), rg.submissions(), len(rg.rec.Events()))
	for _, e := range rg.rec.Events() {
		t.Logf("event %s/%s %s: %s", e.Type, e.Code, e.Name, e.Message)
	}
}
