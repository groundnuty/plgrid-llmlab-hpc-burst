package lifecycle

// What lifts a class's verification stop, said in the event and in ModelDeployment's Degraded message (issue #243;
// api/v1alpha1/conditions.go, ReasonVerificationFailed). A settings contradiction stops the class until the serving
// spec changes; a shape misfit stops it until the spec or the workload's resolved shapes change (science-agent ruling,
// 31 Oct 2026). The expected texts are the contract's words, written out here, and each message is checked for the
// text it must hold and for the other cause's text it must not: the two are not substrings of each other, so a
// message that held both would pass a check for only one.

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

const (
	liftsOnSpec        = "the serving spec changes"
	liftsOnSpecOrShape = "the serving spec or the workload's resolved shapes change"
)

// stopTexts steps the rig until the deployment's class is stopped by a failed verification and returns three texts: the
// event of the verifying pass, the Degraded message of that pass (the first the condition shows), and the Degraded
// message of the next pass, which is the first to read the class stop back from the verification record.
//
// The next pass is exactly one reconcile, made at the moment the first message shows: no clock moves and no further
// round follows. A wait of several rounds would read the message after a first read-back pass had already come and
// gone, so a wrong clause on that pass alone, put right by the next, would go unseen. If the first read-back pass does
// not show the record, the test fails here rather than waiting for a later pass to show it.
func stopTexts(rg *rig, md string) (event RecordedEvent, first, later string) {
	rg.t.Helper()
	rg.until(10*time.Minute, 10*time.Second, "the class stop", func() bool {
		c := rg.condition(md, v1alpha1.ConditionDegraded)
		if c.Status == metav1.ConditionTrue && c.Reason == v1alpha1.ReasonVerificationFailed {
			first = c.Message
			return true
		}
		return false
	})
	rg.reconcileAll()
	c := rg.condition(md, v1alpha1.ConditionDegraded)
	if c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonVerificationFailed || !strings.Contains(c.Message, "verification record") {
		rg.t.Fatalf("Degraded %s/%s/%q on the first pass after the stop, want the class stop read back from the verification record", c.Status, c.Reason, c.Message)
	}
	later = c.Message
	for _, e := range rg.rec.Events() {
		if e.Code == v1alpha1.ReasonVerificationFailed && strings.Contains(e.Message, "provisioning in its class stops until") {
			event = e
		}
	}
	return event, first, later
}

// wantRelatedInstance fails unless the event names the Instance that failed verification as its related object, the
// way CapacityDropped does (handover_test.go): HBEP-8 asks it of a deployment event that one Instance caused.
func wantRelatedInstance(t *testing.T, rg *rig, md string, e RecordedEvent) {
	t.Helper()
	ins := rg.instances(md)
	if len(ins) == 0 {
		t.Fatal("the deployment has no Instance")
	}
	if e.RelatedKind != "*v1alpha1.Instance" || e.RelatedName != ins[0].Name {
		t.Errorf("the event's related object is %s %q, want *v1alpha1.Instance %q", e.RelatedKind, e.RelatedName, ins[0].Name)
	}
	if e.Name != md || !e.Warning {
		t.Errorf("the event regards %q (warning %t), want the ModelDeployment %q as a warning", e.Name, e.Warning, md)
	}
}

// holdsOnly fails when text lacks the lifting clause or holds the other cause's.
func holdsOnly(t *testing.T, what, text, want, notWant string) {
	t.Helper()
	if !strings.Contains(text, "until "+want) {
		t.Errorf("%s = %q, want it to say %q", what, text, "until "+want)
	}
	if strings.Contains(text, notWant) {
		t.Errorf("%s = %q, which holds %q, the other cause's text", what, text, notWant)
	}
}

// A settings contradiction: the event, the Degraded message of the verifying pass and the later one all say the stop
// ends when the serving spec changes, and none says the shapes lift it. The two Degraded messages come from two code
// paths, told apart by the record's name that only the later one carries.
func TestAContradictionStopSaysItEndsWhenTheServingSpecChanges(t *testing.T) {
	rg := newRig(t)
	rg.engine = func(string) afake.Engine { return afake.Engine{Verify: afake.VerifyMaxModelLen("16384")} }
	rg.create(mdSpec("md"))
	event, first, later := stopTexts(rg, "md")
	if event.Message == "" {
		t.Fatal("no event said that the class stops")
	}
	wantRelatedInstance(t, rg, "md", event)
	holdsOnly(t, "the event", event.Message, liftsOnSpec, "resolved shapes")
	holdsOnly(t, "the first Degraded message", first, liftsOnSpec, "resolved shapes")
	holdsOnly(t, "the later Degraded message", later, liftsOnSpec, "resolved shapes")
	if strings.Contains(first, "verification record") || !strings.Contains(later, "verification record") {
		t.Errorf("the two Degraded messages should come from two paths (the record is named only by the later one): %q, %q", first, later)
	}
}

// A shape misfit: all three say the stop ends when the spec or the workload's resolved shapes change, and none says
// only the spec lifts it.
func TestAShapeMisfitStopSaysItEndsWhenTheSpecOrTheResolvedShapesChange(t *testing.T) {
	rg := newRig(t)
	rg.engine = func(string) afake.Engine { return afake.Engine{Verify: afake.VerifyMaxModelLen("4096")} }
	md := mdSpec(hoMD)
	md.Spec.Source.Served.Flags = nil
	rg.create(md)
	event, first, later := stopTexts(rg, hoMD)
	if event.Message == "" {
		t.Fatal("no event said that the class stops")
	}
	wantRelatedInstance(t, rg, hoMD, event)
	holdsOnly(t, "the event", event.Message, liftsOnSpecOrShape, "until "+liftsOnSpec)
	holdsOnly(t, "the first Degraded message", first, liftsOnSpecOrShape, "until "+liftsOnSpec)
	holdsOnly(t, "the later Degraded message", later, liftsOnSpecOrShape, "until "+liftsOnSpec)
	if strings.Contains(first, "verification record") || !strings.Contains(later, "verification record") {
		t.Errorf("the two Degraded messages should come from two paths (the record is named only by the later one): %q, %q", first, later)
	}
}

// stopLifts, on its own: the mapping from the cause to the clause, with the case of a shape misfit that cannot be
// lifted by the shapes.
func TestStopLiftsNamesWhatLiftsEachCause(t *testing.T) {
	for _, tc := range []struct {
		cause     string
		testable  bool
		want, not string
	}{
		{"contradiction", true, liftsOnSpec, "resolved shapes"},
		{"shapeDoesNotFit", true, liftsOnSpecOrShape, "until " + liftsOnSpec},
		{"shapeDoesNotFit", false, liftsOnSpec, "resolved shapes"},
	} {
		got := stopLifts(tc.cause, tc.testable)
		if got != tc.want || strings.Contains(got, tc.not) {
			t.Errorf("stopLifts(%q, %t) = %q, want %q and not %q", tc.cause, tc.testable, got, tc.want, tc.not)
		}
	}
}

// classBlocked, over records written by hand: what a later pass reads back. A contradiction lifts with the spec; a
// shape misfit with the spec or the shapes; a shape record that holds no context limit cannot be tested against the
// workload, so only a new spec lifts it, and it says so (the clause must not promise a lift that cannot come). A
// record of a workload that now fits, and a passed record, stop nothing.
func TestClassBlockedSaysWhatLiftsEachStop(t *testing.T) {
	scheme, err := Scheme()
	if err != nil {
		t.Fatal(err)
	}
	md := mdSpec("md")
	md.Spec.Source.Served.Flags = nil
	id := v1alpha1.FullDeploymentID(strings.Repeat("d", 64))
	record := func(name string, d verificationData) *appsv1.ControllerRevision {
		d.DeploymentID, d.Instance = string(id), "md-abcde"
		raw, err := json.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}
		return &appsv1.ControllerRevision{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: rigNS, Labels: map[string]string{
				v1alpha1.LabelRecord: v1alpha1.RecordVerification, v1alpha1.LabelDeploymentID: id.Display(), v1alpha1.LabelModelDeployment: v1alpha1.LabelValue(md.Name),
			}},
			Data: runtime.RawExtension{Raw: raw},
		}
	}
	for _, tc := range []struct {
		name      string
		d         verificationData
		blocked   bool
		want, not string
	}{
		{"a contradiction", verificationData{Verdict: v1alpha1.VerdictFail, Cause: causeContradiction, Message: "m"}, true, liftsOnSpec, "resolved shapes"},
		// The default workload (a typical context of 4096 and an output of 512) does not fit a limit of 4096.
		{"a shape misfit with the context limit", verificationData{Verdict: v1alpha1.VerdictFail, Cause: causeShape, Message: "m", MaxModelLen: 4096, Overhead: 12}, true, liftsOnSpecOrShape, "until " + liftsOnSpec},
		{"a shape misfit with no context limit", verificationData{Verdict: v1alpha1.VerdictFail, Cause: causeShape, Message: "m"}, true, liftsOnSpec, "resolved shapes"},
		{"a shape record whose workload now fits", verificationData{Verdict: v1alpha1.VerdictFail, Cause: causeShape, Message: "m", MaxModelLen: 32768, Overhead: 12}, false, "", ""},
		{"a passed verification", verificationData{Verdict: v1alpha1.VerdictPass}, false, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(record("ver-1", tc.d)).Build()
			r := &Reconciler{Client: c, Reader: c, Clock: SystemClock{}, Recorder: &MemoryRecorder{}}
			blocked, lifts, err := r.classBlocked(context.Background(), md, id, resolveShapes(md))
			if err != nil {
				t.Fatal(err)
			}
			if (blocked != "") != tc.blocked {
				t.Fatalf("blocked %q, want a stop: %t", blocked, tc.blocked)
			}
			if !tc.blocked {
				if lifts != "" {
					t.Errorf("lifts %q with no stop", lifts)
				}
				return
			}
			if lifts != tc.want || strings.Contains(lifts, tc.not) {
				t.Errorf("lifts %q, want %q and not %q", lifts, tc.want, tc.not)
			}
		})
	}
}
