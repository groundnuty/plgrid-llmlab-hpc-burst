package lifecycle

// Issue #338: the failure message of an Instance whose Allocation was submitted
// as another (condition SubmittedAsOther, HBEP-4 Read-back; HBEP-7 step 5)
// carries the condition's message, which the Provider's controller writes from
// the read-back and which names what differed. Before, step 5 built a generic
// text from the Allocation's phase and observed reason ("Allocation X is
// Running: None"), and an operator could not see what the job was established
// with. RelayEndedForGood's failure already used its condition's message.
//
// Both tests run the real pass (rg.Step, the rig's provider, agents and
// reconcile). The expected texts are literals, never built by a lifecycle
// formatter: a test that formats its expectation with the code it checks
// agrees with it by construction.
//
// The mismatch test does not write the condition by hand. The rig's Provider
// controller (pkg/provider/fake, the read-back of HBEP-4) writes it on the
// first observe of a job that the fake Slurm established with another time
// limit (Script.AsTimeLimit), so the message has the shape the Provider writes
// (internal/providerctl/observe.go, readBack): "job <ID> was established with
// other settings than requested: <field>: requested <a>, got <b>".

import (
	"strings"
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// markSubmittedAsOther sets SubmittedAsOther True on the Allocation, with
// reason ReadBackMismatch and the given message (empty: a condition with no
// message).
func markSubmittedAsOther(t *testing.T, rg *rig, name, msg string) {
	t.Helper()
	a, ok := rg.allocation(name)
	if !ok {
		t.Fatalf("Allocation %s is gone", name)
	}
	a.SetConditions(xpv2.Condition{Type: submittedAsOtherCond, Status: corev1.ConditionTrue, Reason: submittedAsOtherReason,
		Message: msg, LastTransitionTime: metav1.NewTime(rg.clock.Now())})
	if err := rg.c.Status().Update(rg.ctx, a); err != nil {
		t.Fatal(err)
	}
}

// The first job is established with a time limit of 11 h, whatever the request
// said. The Provider's controller raises SubmittedAsOther with the read-back's
// message at the first observe, the Instance ends in that pass with one failing
// event whose message names the Allocation and carries that message (the
// differing field and the limit the job got), the Instance's Healthy condition
// carries the same text, and one replacement Allocation with a job of its own
// serves.
func TestASubmittedAsOtherFailureCarriesTheReadBackMessage(t *testing.T) {
	rg := newRig(t)
	// Only the first job is established as another; the replacement's gets the
	// default script, so the deployment serves again.
	rg.batch.Enqueue(pfake.Script{AsTimeLimit: 11 * time.Hour})
	rg.create(mdSpec("md"))
	rg.until(10*time.Minute, 10*time.Second, "a failing SubmittedAsOther event", func() bool {
		return len(rg.rec.Find("failing", submittedAsOtherCond)) > 0
	})
	// The pass that failed the Instance deleted its Allocation, whose finalizer
	// goes at the Provider's next sync: read the condition the Provider's
	// controller wrote before the next round.
	failed := rg.instances("md")[0]
	allocName := failed.Spec.AllocationRef.Name
	a, ok := rg.allocation(allocName)
	if !ok || a.GetCondition(submittedAsOtherCond).Status != corev1.ConditionTrue {
		t.Fatalf("Allocation %s is gone or does not carry %s True: %+v", allocName, submittedAsOtherCond, a)
	}
	cm := a.GetCondition(submittedAsOtherCond).Message
	if !strings.Contains(cm, "was established with other settings than requested") || !strings.Contains(cm, "timeLimit: requested ") || !strings.Contains(cm, ", got 11h0m0s") {
		t.Fatalf("the Provider's condition message is %q; want the read-back's shape naming the time limit it got (11h0m0s), so this case proves nothing", cm)
	}
	rg.until(10*time.Minute, 10*time.Second, "one ready Instance", func() bool { return rg.readyCount("md") == 1 })

	ev := rg.rec.Find("failing", submittedAsOtherCond)
	if len(ev) != 1 {
		t.Fatalf("failing/%s events %+v; want exactly one", submittedAsOtherCond, ev)
	}
	var first *v1alpha1.Instance
	for _, in := range rg.instances("md") {
		if in.Status.EndReason == submittedAsOtherCond {
			first = &in
		}
	}
	if first == nil {
		t.Fatalf("no Instance ended with %s: %+v", submittedAsOtherCond, rg.instances("md"))
	}
	if ev[0].Name != first.Name {
		t.Errorf("the failing event is for %s, want the Instance that ended, %s", ev[0].Name, first.Name)
	}
	if first.Spec.AllocationRef.Name != allocName {
		t.Errorf("the Instance that ended is on Allocation %s, want %s", first.Spec.AllocationRef.Name, allocName)
	}
	healthy := apimeta.FindStatusCondition(first.Status.Conditions, "Healthy")
	// The exact text is the Allocation's name, a colon and the condition's own
	// message, written out here as the issue states it (not by a lifecycle
	// formatter). Substrings alone would also accept the generic text with the
	// message appended after it.
	wantMsg := "Allocation " + allocName + ": " + cm
	for _, c := range []struct{ what, got string }{{"the failing event", ev[0].Message}, {"the Instance's Healthy condition", healthyMessage(healthy)}} {
		if c.got != wantMsg {
			t.Errorf("%s message is %q; want exactly %q", c.what, c.got, wantMsg)
		}
		// The Provider's shape, as literals.
		for _, want := range []string{allocName, "was established with other settings than requested", "timeLimit: requested ", ", got 11h0m0s"} {
			if !strings.Contains(c.got, want) {
				t.Errorf("%s message %q does not contain %q", c.what, c.got, want)
			}
		}
	}
	if got := rg.submissions(); got != 2 {
		t.Errorf("sbatch calls = %d, want 2 (the first job and its one replacement)", got)
	}
}

func healthyMessage(c *metav1.Condition) string {
	if c == nil {
		return ""
	}
	return c.Message
}

// A SubmittedAsOther condition with no message falls back to the generic text
// (the Allocation, its phase and its observed reason), as before the change;
// the reason string is unchanged. The text is the one the issue quotes.
func TestASubmittedAsOtherWithNoMessageKeepsTheGenericText(t *testing.T) {
	rg := newRig(t)
	old, served := allocServe(rg)
	rg.run(5*time.Minute, 10*time.Second)
	if in := rg.instance(served.Name); in.Status.Phase != v1alpha1.InstanceReady {
		t.Fatalf("before the mark the Instance is %s; want ready", in.Status.Phase)
	}
	markSubmittedAsOther(t, rg, old.Name, "")
	rg.clock.Advance(time.Second)
	rg.Step()

	in := rg.instance(served.Name)
	if in.Status.Phase != v1alpha1.InstanceEnded || in.Status.EndReason != "SubmittedAsOther" {
		t.Errorf("after the next pass the Instance is %s with end reason %q; want ended with SubmittedAsOther", in.Status.Phase, in.Status.EndReason)
	}
	want := "Allocation " + old.Name + " is Running: None"
	ev := rg.rec.Find("failing", submittedAsOtherCond)
	if len(ev) != 1 || ev[0].Name != served.Name || ev[0].Message != want {
		t.Errorf("failing/%s events %+v: want one, for %s, with the message %q", submittedAsOtherCond, ev, served.Name, want)
	}
	if h := apimeta.FindStatusCondition(in.Status.Conditions, "Healthy"); h == nil || h.Message != want {
		t.Errorf("the Instance's Healthy condition is %+v; want the message %q", h, want)
	}
}
