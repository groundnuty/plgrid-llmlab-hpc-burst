package agentcli

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

const planAB = "0123456789abcdef"

func TestScaleWithoutATerminalPrintsThePlanAndConfirmsNothing(t *testing.T) {
	w := newWorld(t, nil)
	w.names = []string{"op-aaaaaaaaaa"}
	plan := simplePlan(planAB)
	plan.Targets[0].Cost = &v1alpha1.ReleaseCost{QueueAge: &v1alpha1.DurationFigure{Duration: &metav1.Duration{Duration: 48 * time.Hour}, ObservedAt: &plan.ObservedAt}}
	w.controller("op-aaaaaaaaaa", plan, "")
	// The name is printed before the create: at the create, the output already holds it.
	w.rec.before = func(o client.Object) {
		if !strings.Contains(w.out.String(), "operation: op-aaaaaaaaaa") {
			t.Errorf("at the create, the output is %q: the name was not printed first", w.out.String())
		}
	}
	if code := w.run("", "scale", "m", "--instances", "1", "--purpose", "free a slot", "-n", testNS); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w.errs.String())
	}
	op := w.operation("op-aaaaaaaaaa")
	if op.Spec.Action != v1alpha1.OperationActionScale || op.Spec.ModelDeploymentRef.Name != "m" || op.Spec.Scale == nil || op.Spec.Scale.Instances != 1 || op.Spec.Owner != userA {
		t.Errorf("stored spec %+v, want Scale of m to 1 owned by %s", op.Spec, userA)
	}
	if op.Spec.Confirm != nil {
		t.Errorf("confirm is %v: without a terminal nothing is confirmed", op.Spec.Confirm)
	}
	if op.Annotations[v1alpha1.AnnotationPurpose] != "free a slot" {
		t.Errorf("annotations %v, want the purpose", op.Annotations)
	}
	contains(t, "stdout", w.out.String(),
		"plan "+planAB, "Allocation alloc-p (provider sano): CancelPending, needs approval",
		"policy provider.sano: approvers serviceaccount hpcburst-agents/agent-a",
		"queue age: 48h0m0s (observed 2026-11-20T10:00:00Z)",
		"estimate: pending",
		"confirm with: hpcburst confirm op-aaaaaaaaaa --plan "+planAB+" -n team")
	if got := w.writes(); len(got) != 1 || got[0] != "create *v1alpha1.Operation" {
		t.Errorf("writes %v, want the one create", got)
	}
}

func TestScaleAtATerminalConfirmsOnYes(t *testing.T) {
	w := newWorld(t, nil)
	w.names = []string{"op-bbbbbbbbbb"}
	w.tty = true
	w.controller("op-bbbbbbbbbb", simplePlan(planAB), "Allocation alloc-p: needs an approval under provider.sano")
	if code := w.run("y\n", "scale", "m", "--instances", "1"); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w.errs.String())
	}
	contains(t, "stdout", w.out.String(), "confirm plan "+planAB+"? [y/N] ", "confirmed plan "+planAB, "phase: AwaitingApproval",
		"waiting: Allocation alloc-p: needs an approval under provider.sano",
		"must approve, policy provider.sano: serviceaccount hpcburst-agents/agent-a")
	lacks(t, "stdout", w.out.String(), "confirm with:")
	if op := w.operation("op-bbbbbbbbbb"); op.Spec.Confirm == nil || op.Spec.Confirm.PlanID != planAB {
		t.Errorf("confirm is %v, want the plan the caller saw", op.Spec.Confirm)
	}
	if got := w.writes(); len(got) != 2 || got[0] != "create *v1alpha1.Operation" || got[1] != "update *v1alpha1.Operation" {
		t.Errorf("writes %v, want an Operation create and an Operation update", got)
	}
}

func TestScaleAtATerminalConfirmsNothingOnAnythingButYes(t *testing.T) {
	for _, answer := range []string{"n\n", "\n", "", "yep\n", "no\n"} {
		w := newWorld(t, nil)
		w.names = []string{"op-cccccccccc"}
		w.tty = true
		w.controller("op-cccccccccc", simplePlan(planAB), "")
		if code := w.run(answer, "scale", "m", "--instances", "1"); code != exitOK {
			t.Fatalf("answer %q: exit %d", answer, code)
		}
		if op := w.operation("op-cccccccccc"); op.Spec.Confirm != nil {
			t.Errorf("answer %q confirmed %v", answer, op.Spec.Confirm)
		}
		contains(t, "stdout", w.out.String(), "not confirmed; confirm with: hpcburst confirm op-cccccccccc --plan "+planAB)
	}
}

// --json never asks, even at a terminal, and prints one document.
func TestScaleJSONNeverAsks(t *testing.T) {
	w := newWorld(t, nil)
	w.names = []string{"op-dddddddddd"}
	w.tty = true
	w.controller("op-dddddddddd", simplePlan(planAB), "")
	if code := w.run("y\n", "scale", "m", "--instances", "1", "--json", "-n", testNS); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w.errs.String())
	}
	var doc struct {
		SchemaVersion int `json:"schemaVersion"`
		Operations    []struct {
			Name          string `json:"name"`
			ConfirmWith   string `json:"confirmWith"`
			ConfirmedPlan string `json:"confirmedPlan"`
			Phase         string `json:"phase"`
		} `json:"operations"`
	}
	if err := json.Unmarshal(w.out.Bytes(), &doc); err != nil {
		t.Fatalf("stdout is not one JSON document: %v\n%s", err, w.out.String())
	}
	if doc.SchemaVersion != 1 || len(doc.Operations) != 1 || doc.Operations[0].Name != "op-dddddddddd" ||
		doc.Operations[0].ConfirmWith != "hpcburst confirm op-dddddddddd --plan "+planAB+" -n team" || doc.Operations[0].ConfirmedPlan != "" {
		t.Errorf("document %+v", doc)
	}
	contains(t, "stderr", w.errs.String(), "operation: op-dddddddddd")
	lacks(t, "stdout", w.out.String(), "[y/N]")
	if op := w.operation("op-dddddddddd"); op.Spec.Confirm != nil {
		t.Errorf("--json confirmed %v", op.Spec.Confirm)
	}
}

func TestScaleRefusedAndTimedOut(t *testing.T) {
	t.Run("a refused Operation prints its message and exits 1", func(t *testing.T) {
		w := newWorld(t, nil)
		w.names = []string{"op-eeeeeeeeee"}
		w.onPause = func() {
			op := w.operation("op-eeeeeeeeee")
			op.Status.Phase = v1alpha1.OperationPhaseRefused
			op.Status.Conditions = []metav1.Condition{{Type: v1alpha1.ConditionExecutable, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonRefused,
				Message: "ModelDeployment m is not there", LastTransitionTime: metav1.NewTime(w.now)}}
			if err := w.store.Status().Update(context.Background(), op); err != nil {
				t.Fatal(err)
			}
		}
		if code := w.run("", "scale", "m", "--instances", "1"); code != exitFail {
			t.Fatalf("exit %d, want 1", code)
		}
		contains(t, "stderr", w.errs.String(), "Operation op-eeeeeeeeee was refused: ModelDeployment m is not there")
	})
	t.Run("no plan within the timeout exits 1 and names the Operation", func(t *testing.T) {
		w := newWorld(t, nil)
		w.names = []string{"op-ffffffffff"}
		if code := w.run("", "scale", "m", "--instances", "1", "--timeout", "10s"); code != exitFail {
			t.Fatalf("exit %d, want 1", code)
		}
		contains(t, "stderr", w.errs.String(), "Operation op-ffffffffff has no plan after 10s", "hpcburst operation op-ffffffffff")
		if got := w.now.Sub(t0); got < 10*time.Second || got > 12*time.Second {
			t.Errorf("the clock moved %s while waiting, want about the timeout", got)
		}
		if len(w.operations()) != 1 {
			t.Errorf("Operations %v: the Operation stays", w.operations())
		}
	})
}

func TestEndMakesAnEndOperation(t *testing.T) {
	w := newWorld(t, nil)
	w.controller("end-m", simplePlan(planAB), "")
	if code := w.run("", "end", "m", "--key", "end-m"); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w.errs.String())
	}
	op := w.operation("end-m")
	if op.Spec.Action != v1alpha1.OperationActionEnd || op.Spec.End != nil || op.Spec.Owner != userA {
		t.Errorf("a plain end: %+v, want End with no end block", op.Spec)
	}
	w2 := newWorld(t, nil)
	w2.controller("end-del", simplePlan(planAB), "")
	if code := w2.run("", "end", "m", "--delete", "--key", "end-del"); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w2.errs.String())
	}
	if op := w2.operation("end-del"); op.Spec.End == nil || !op.Spec.End.Delete {
		t.Errorf("end --delete: %+v, want delete true", op.Spec)
	}
}

func TestCreateRefusesBadFlagsBeforeCreatingAnything(t *testing.T) {
	long := strings.Repeat("x", 257)
	for _, args := range [][]string{
		{"scale", "m"},                          // no --instances
		{"scale", "m", "--instances", "-1"},     // below zero
		{"scale", "--instances", "1"},           // no ModelDeployment
		{"scale", "m", "n", "--instances", "1"}, // two
		{"scale", "m", "--instances", "1", "--purpose", long},
		{"scale", "m", "--instances", "1", "--key", "Not_A_Name"},
		{"scale", "m", "--instances", "1", "--key", strings.Repeat("k", 254)},
		{"scale", "m", "--instances", "1", "--timeout", "0s"},
		{"end"}, {"release"}, {"release", "a", "-l", "x=y"},
		{"confirm", "op"}, {"approve", "op"},
		{"scale", "m", "--instances", "1", "--bogus"},
	} {
		w := newWorld(t, nil)
		if code := w.run("", args...); code != exitUsage {
			t.Errorf("%v: exit %d, want 2; stderr %q", args, code, w.errs.String())
		}
		if got := w.writes(); len(got) != 0 || len(w.operations()) != 0 {
			t.Errorf("%v: made writes %v", args, got)
		}
	}
	// 256 characters of purpose are accepted: the limit is on characters, not on bytes.
	w := newWorld(t, nil)
	w.names = []string{"op-gggggggggg"}
	w.controller("op-gggggggggg", simplePlan(planAB), "")
	if code := w.run("", "scale", "m", "--instances", "1", "--purpose", strings.Repeat("é", 256)); code != exitOK {
		t.Errorf("a purpose of 256 characters: exit %d, %q", code, w.errs.String())
	}
}

func TestAnOperationThatExistsAlreadyConfirmedIsNotAskedAgain(t *testing.T) {
	spec := scaleSpec("m", 1)
	spec.Confirm = &v1alpha1.OperationConfirm{PlanID: planAB}
	existing := opObject("op-hhhhhhhhhh", userA, spec, v1alpha1.OperationStatus{Phase: v1alpha1.OperationPhaseAwaitingApproval, Plan: simplePlan(planAB)})
	w := newWorld(t, []client.Object{existing})
	w.tty = true
	if code := w.run("y\n", "scale", "m", "--instances", "1", "--key", "op-hhhhhhhhhh"); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w.errs.String())
	}
	contains(t, "stdout", w.out.String(), "plan "+planAB+" is already confirmed", "phase: AwaitingApproval")
	lacks(t, "stdout", w.out.String(), "[y/N]")
	if got := w.writes(); len(got) != 1 {
		t.Errorf("writes %v: a retry of a confirmed request writes only its create, which is refused", got)
	}
}

// Without a terminal nothing is asked, whatever stdin holds: a "y" on a pipe is not a person at a terminal, so the
// Operation is made and its plan printed, and nothing is confirmed. A test that feeds no input cannot see the gate,
// since reading nothing answers no; and the line "confirm with:" is also a part of the line a declined prompt prints
// ("not confirmed; confirm with:"), so it proves nothing by itself. Four things are asserted: the stored confirm is
// absent, the create is the one write, no prompt was printed, and no decline was.
func assertNothingAsked(t *testing.T, w *world, name string) {
	t.Helper()
	if op := w.operation(name); op.Spec.Confirm != nil {
		t.Errorf("the stored confirm is %v: an answer on stdin confirmed a plan with no terminal", op.Spec.Confirm)
	}
	if got := w.writes(); len(got) != 1 || got[0] != "create *v1alpha1.Operation" {
		t.Errorf("writes %v, want the one create", got)
	}
	lacks(t, "stdout", w.out.String(), "[y/N]", "not confirmed")
	if !regexp.MustCompile(`(?m)^confirm with: hpcburst confirm ` + regexp.QuoteMeta(name) + ` --plan ` + planAB + ` -n ` + testNS + `$`).MatchString(w.out.String()) {
		t.Errorf("stdout has no line that starts with the confirm command:\n%s", w.out.String())
	}
}

func TestScaleWithoutATerminalNeverAsksEvenWhenStdinSaysYes(t *testing.T) {
	w := newWorld(t, nil)
	w.names = []string{"op-nnnnnnnnnn"}
	w.tty = false
	w.controller("op-nnnnnnnnnn", simplePlan(planAB), "")
	if code := w.run("y\ny\nyes\n", "scale", "m", "--instances", "1"); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w.errs.String())
	}
	assertNothingAsked(t, w, "op-nnnnnnnnnn")
}

func TestReleaseWithoutATerminalNeverAsksEvenWhenStdinSaysYes(t *testing.T) {
	w := newWorld(t, []client.Object{alloc("a1", "m1", nil)})
	w.names = []string{"op-rrrrrrrrrr"}
	w.tty = false
	w.controller("op-rrrrrrrrrr", simplePlan(planAB), "")
	if code := w.run("y\n", "release", "a1"); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w.errs.String())
	}
	assertNothingAsked(t, w, "op-rrrrrrrrrr")
}
