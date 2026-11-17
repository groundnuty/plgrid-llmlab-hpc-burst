package agentcli

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

const planCD = "fedcba9876543210"

// planned is an Operation of agent A with a plan, not confirmed.
func planned(name, planID string) *v1alpha1.Operation {
	return opObject(name, userA, scaleSpec("m", 1), v1alpha1.OperationStatus{Phase: v1alpha1.OperationPhasePlanned, Plan: simplePlan(planID)})
}

// moveStatus is what the controller does between the CLI's read and its write: it writes the Operation's
// status, which moves the resourceVersion, with a plan of the given ID.
func moveStatus(w *world, name, planID string) {
	op := w.operation(name)
	op.Status.Plan = simplePlan(planID)
	if err := w.store.Status().Update(context.Background(), op); err != nil {
		w.t.Fatal(err)
	}
}

// conflictingStore makes a control plane whose first n updates of an Operation are overtaken by a status
// write with the plan of ID after(i), so that the CLI's update carries a stale resourceVersion.
func conflictingStore(t *testing.T, objs []client.Object, n int, after func(i int) string) (*world, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	var w *world
	f := interceptor.Funcs{Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
		i := int(calls.Add(1))
		if i <= n {
			moveStatus(w, obj.GetName(), after(i))
		}
		return c.Update(ctx, obj, opts...)
	}}
	w = newWorld(t, objs, f)
	return w, &calls
}

func TestConfirmConfirmsTheNamedPlan(t *testing.T) {
	w := newWorld(t, []client.Object{planned("op", planAB)})
	w.controller("op", simplePlan(planAB), "")
	if code := w.run("", "confirm", "op", "--plan", planAB); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w.errs.String())
	}
	if op := w.operation("op"); op.Spec.Confirm == nil || op.Spec.Confirm.PlanID != planAB {
		t.Errorf("confirm %v, want the plan", op.Spec.Confirm)
	}
	contains(t, "stdout", w.out.String(), "confirmed plan "+planAB+" of op", "phase: AwaitingApproval")
	if got := w.writes(); len(got) != 1 || got[0] != "update *v1alpha1.Operation" {
		t.Errorf("writes %v, want one Operation update and nothing else", got)
	}
}

func TestConfirmRefusesAPlanThatIsNotTheCurrentOne(t *testing.T) {
	w := newWorld(t, []client.Object{planned("op", planAB)})
	if code := w.run("", "confirm", "op", "--plan", planCD); code != exitFail {
		t.Fatalf("exit %d, want 1", code)
	}
	contains(t, "stderr", w.errs.String(), "the current plan is "+planAB, "nothing was confirmed")
	contains(t, "stdout", w.out.String(), "plan "+planAB)
	if op := w.operation("op"); op.Spec.Confirm != nil {
		t.Errorf("confirm %v: a plan that is not current was confirmed", op.Spec.Confirm)
	}
	if got := w.writes(); len(got) != 0 {
		t.Errorf("writes %v, want none", got)
	}
}

// Row 7: a status write that keeps the plan ID makes the confirm conflict; it reads again and is accepted.
func TestConfirmRetriesAConflictWhileThePlanHolds(t *testing.T) {
	w, calls := conflictingStore(t, []client.Object{planned("op", planAB)}, 2, func(int) string { return planAB })
	w.controller("op", simplePlan(planAB), "")
	if code := w.run("", "confirm", "op", "--plan", planAB); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w.errs.String())
	}
	if n := calls.Load(); n != 3 {
		t.Errorf("%d updates, want 3: two conflicts and the accepted one", n)
	}
	if op := w.operation("op"); op.Spec.Confirm == nil || op.Spec.Confirm.PlanID != planAB {
		t.Errorf("confirm %v, want the plan", op.Spec.Confirm)
	}
}

// Row 7: a re-plan during the retry makes it stop: exit 1, the new plan shown, nothing confirmed.
func TestConfirmStopsWhenTheConflictBroughtANewPlan(t *testing.T) {
	w, calls := conflictingStore(t, []client.Object{planned("op", planAB)}, 1, func(int) string { return planCD })
	if code := w.run("", "confirm", "op", "--plan", planAB); code != exitFail {
		t.Fatalf("exit %d, want 1; stdout %q", code, w.out.String())
	}
	contains(t, "stderr", w.errs.String(), "the current plan is "+planCD, "nothing was confirmed")
	contains(t, "stdout", w.out.String(), "plan "+planCD)
	if n := calls.Load(); n != 1 {
		t.Errorf("%d updates, want 1: the retry must not write a plan the caller has not seen", n)
	}
	if op := w.operation("op"); op.Spec.Confirm != nil {
		t.Errorf("confirm %v after a re-plan", op.Spec.Confirm)
	}
}

func TestConfirmGivesUpAfterFiveConflicts(t *testing.T) {
	w, calls := conflictingStore(t, []client.Object{planned("op", planAB)}, 100, func(int) string { return planAB })
	if code := w.run("", "confirm", "op", "--plan", planAB); code != exitFail {
		t.Fatalf("exit %d, want 1", code)
	}
	if n := calls.Load(); n != 5 {
		t.Errorf("%d updates, want 5", n)
	}
	contains(t, "stderr", w.errs.String(), "changed under 5 confirms")
}

func TestConfirmWhenAlreadyConfirmed(t *testing.T) {
	op := planned("op", planAB)
	op.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: planAB}
	w := newWorld(t, []client.Object{op})
	if code := w.run("", "confirm", "op", "--plan", planAB); code != exitOK {
		t.Errorf("confirmed with the same plan: exit %d, want 0; stderr %q", code, w.errs.String())
	}
	contains(t, "stdout", w.out.String(), "already confirmed")
	if got := w.writes(); len(got) != 0 {
		t.Errorf("writes %v, want none", got)
	}

	// Confirmed with another plan, which is also the current one: the ask is for a different plan.
	other := planned("op2", planAB)
	other.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: planCD}
	w = newWorld(t, []client.Object{other})
	if code := w.run("", "confirm", "op2", "--plan", planAB); code != exitFail {
		t.Errorf("confirmed with another plan: exit %d, want 1", code)
	}
	contains(t, "stderr", w.errs.String(), "already confirmed with another plan ("+planCD+")")
}

func TestConfirmRefusalsAndErrors(t *testing.T) {
	noPlan := opObject("noplan", userA, scaleSpec("m", 1), v1alpha1.OperationStatus{})
	w := newWorld(t, []client.Object{noPlan})
	if code := w.run("", "confirm", "noplan", "--plan", planAB); code != exitFail {
		t.Errorf("no plan: exit %d", code)
	}
	contains(t, "stderr", w.errs.String(), "has no plan yet")
	if code := w.run("", "confirm", "absent", "--plan", planAB); code != exitFail {
		t.Errorf("absent: exit %d", code)
	}
	// The owner pin refuses another identity's confirm; the CLI reports it and exits 1.
	forbid := interceptor.Funcs{Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error {
		return apierrors.NewForbidden(schema.GroupResource{Resource: "operations"}, "op", errString("only an Operation's owner confirms its plan"))
	}}
	w = newWorld(t, []client.Object{planned("op", planAB)}, forbid)
	if code := w.run("", "confirm", "op", "--plan", planAB); code != exitFail {
		t.Errorf("forbidden: exit %d", code)
	}
	contains(t, "stderr", w.errs.String(), "only an Operation's owner confirms its plan")
}

type errString string

func (e errString) Error() string { return string(e) }

func TestConfirmJSON(t *testing.T) {
	w := newWorld(t, []client.Object{planned("op", planAB)})
	w.controller("op", simplePlan(planAB), "")
	if code := w.run("", "confirm", "op", "--plan", planAB, "--json"); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w.errs.String())
	}
	var doc struct {
		SchemaVersion int    `json:"schemaVersion"`
		Result        string `json:"result"`
		Operation     struct {
			ConfirmedPlan string `json:"confirmedPlan"`
			Phase         string `json:"phase"`
		} `json:"operation"`
	}
	if err := json.Unmarshal(w.out.Bytes(), &doc); err != nil {
		t.Fatalf("%v\n%s", err, w.out.String())
	}
	if doc.SchemaVersion != 1 || doc.Result != "confirmed" || doc.Operation.ConfirmedPlan != planAB || doc.Operation.Phase != "AwaitingApproval" {
		t.Errorf("document %+v", doc)
	}
	// A changed plan: exit 1, the new plan on stdout as JSON.
	w = newWorld(t, []client.Object{planned("op", planCD)})
	if code := w.run("", "confirm", "op", "--plan", planAB, "--json"); code != exitFail {
		t.Fatalf("exit %d, want 1", code)
	}
	if err := json.Unmarshal(w.out.Bytes(), &doc); err != nil || doc.Result != "planChanged" || !strings.Contains(w.out.String(), planCD) {
		t.Errorf("document %q (%v), want result planChanged with the new plan", w.out.String(), err)
	}
}

// moveTo is what the controller does to an Operation that was planned and not yet confirmed: it turns it into another
// phase, with the plan still in its status, as Refused leaves it (the plan is not cleared).
func moveTo(w *world, name string, phase v1alpha1.OperationPhase, finished bool, message string) {
	op := w.operation(name)
	op.Status.Phase = phase
	op.Status.Conditions = []metav1.Condition{{Type: v1alpha1.ConditionExecutable, Status: metav1.ConditionFalse, Reason: string(phase),
		Message: message, LastTransitionTime: metav1.NewTime(w.now)}}
	if finished {
		op.Status.FinishedAt = &metav1.Time{Time: w.now}
	}
	if err := w.store.Status().Update(context.Background(), op); err != nil {
		w.t.Fatal(err)
	}
}

// A confirm is for a plan that waits for it: the phase Planned. Any other phase, a terminal one above all, is refused with
// the condition's message, and nothing is written: a Refused Operation that kept its plan would otherwise be confirmed, and
// reported as a success.
func TestConfirmRefusesAnOperationThatIsNotPlanned(t *testing.T) {
	for _, r := range []struct {
		phase    v1alpha1.OperationPhase
		finished bool
	}{
		{v1alpha1.OperationPhaseRefused, true}, {v1alpha1.OperationPhaseDone, true}, {v1alpha1.OperationPhaseStale, true},
		{v1alpha1.OperationPhasePartlyFailed, true}, {v1alpha1.OperationPhaseExecuting, false},
	} {
		t.Run(string(r.phase), func(t *testing.T) {
			w := newWorld(t, []client.Object{planned("op", planAB)})
			moveTo(w, "op", r.phase, r.finished, "a live Instance runs on Allocation alloc-k")
			if code := w.run("", "confirm", "op", "--plan", planAB); code != exitFail {
				t.Fatalf("exit %d, want 1; stdout %q", code, w.out.String())
			}
			contains(t, "stderr", w.errs.String(), "Operation op is in phase "+string(r.phase), "a live Instance runs on Allocation alloc-k", "nothing was confirmed")
			if op := w.operation("op"); op.Spec.Confirm != nil {
				t.Errorf("confirm is %v: a plan of an Operation in phase %s was confirmed", op.Spec.Confirm, r.phase)
			}
			if got := w.writes(); len(got) != 0 {
				t.Errorf("writes %v, want none", got)
			}
		})
	}
}

// The same at a terminal: the answer is typed after the plan was shown, and the controller refused the Operation meanwhile.
func TestAnInteractiveYesDoesNotConfirmAnOperationRefusedMeanwhile(t *testing.T) {
	w := newWorld(t, nil)
	w.names = []string{"op-iiiiiiiiii"}
	w.tty = true
	w.controller("op-iiiiiiiiii", simplePlan(planAB), "")
	answered := false
	stdin := readerFunc(func(p []byte) (int, error) {
		if answered {
			return 0, io.EOF
		}
		answered = true
		moveTo(w, "op-iiiiiiiiii", v1alpha1.OperationPhaseRefused, true, "the deployment was deleted")
		return copy(p, "y\n"), nil
	})
	if code := w.runReader(stdin, "scale", "m", "--instances", "1"); code != exitFail {
		t.Fatalf("exit %d, want 1; stdout %q", code, w.out.String())
	}
	contains(t, "stderr", w.errs.String(), "Operation op-iiiiiiiiii is in phase Refused", "the deployment was deleted")
	if op := w.operation("op-iiiiiiiiii"); op.Spec.Confirm != nil {
		t.Errorf("confirm is %v: a typed y confirmed a refused Operation", op.Spec.Confirm)
	}
	if got := w.writes(); len(got) != 1 || got[0] != "create *v1alpha1.Operation" {
		t.Errorf("writes %v, want the one create", got)
	}
}

// A retry of a confirm that landed is a success while the Operation goes on or is over (Executing, Done); one the controller
// refused or found stale will not run, and the retry says so with a failure.
func TestConfirmWhenAlreadyConfirmedFollowsWhereTheOperationWent(t *testing.T) {
	for _, r := range []struct {
		phase v1alpha1.OperationPhase
		want  int
	}{
		{v1alpha1.OperationPhaseAwaitingApproval, exitOK}, {v1alpha1.OperationPhaseExecuting, exitOK}, {v1alpha1.OperationPhaseDone, exitOK},
		{v1alpha1.OperationPhaseRefused, exitFail}, {v1alpha1.OperationPhaseStale, exitFail},
	} {
		t.Run(string(r.phase), func(t *testing.T) {
			op := planned("op", planAB)
			op.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: planAB}
			w := newWorld(t, []client.Object{op})
			moveTo(w, "op", r.phase, r.phase == v1alpha1.OperationPhaseRefused || r.phase == v1alpha1.OperationPhaseStale || r.phase == v1alpha1.OperationPhaseDone, "the committed message")
			if code := w.run("", "confirm", "op", "--plan", planAB); code != r.want {
				t.Errorf("exit %d, want %d; stdout %q stderr %q", code, r.want, w.out.String(), w.errs.String())
			}
			if got := w.writes(); len(got) != 0 {
				t.Errorf("writes %v, want none", got)
			}
		})
	}
}

// noApprovalPlan is a plan whose one target needs no approval: a drain of an Instance while another stays ready.
func noApprovalPlan(id string) *v1alpha1.OperationPlan {
	return &v1alpha1.OperationPlan{ID: id, ObservedAt: metav1.NewTime(t0), ModelDeployment: v1alpha1.PlannedDeployment{Name: "m", UID: "uid-m", Generation: 4},
		Targets: []v1alpha1.PlanTarget{{Kind: v1alpha1.TargetKindInstance, Name: "inst-a", UID: "uid-inst-a", Provider: "sano", Effect: v1alpha1.PlanEffectDrainInstance}}}
}

// A plan that needs no approval does not move the phase where the execution is not built, so a confirm that waited for the phase
// would wait out its timeout and then say that the controller had not picked the confirm up. The controller has: it records the
// generation it saw, and the command reports what it says. The test's clock moves by one second at each wait, so a wait that
// used the timeout moved it by a minute.
func TestConfirmOfAPlanThatNeedsNoApprovalDoesNotWaitOutTheTimeout(t *testing.T) {
	op := opObject("op", userA, scaleSpec("m", 1), v1alpha1.OperationStatus{Phase: v1alpha1.OperationPhasePlanned, Plan: noApprovalPlan(planAB), ObservedGeneration: 1})
	op.Generation = 1
	w := newWorld(t, []client.Object{op})
	w.controllerNoApproval("op", noApprovalPlan(planAB))
	if code := w.run("", "confirm", "op", "--plan", planAB, "--timeout", "60s"); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w.errs.String())
	}
	if used := w.now.Sub(t0); used >= 10*time.Second {
		t.Errorf("the command waited %s of its 60s timeout", used)
	}
	contains(t, "stdout", w.out.String(), "confirmed plan "+planAB+" of op", "phase: Planned", "Executable=False ExecutionNotBuilt: confirmed, and no target needs approval")
	lacks(t, "stdout", w.out.String(), "has not picked up")
}

// The same at a terminal, through scale.
func TestAnInteractiveConfirmOfAPlanThatNeedsNoApprovalDoesNotWaitOutTheTimeout(t *testing.T) {
	w := newWorld(t, nil)
	w.names = []string{"op-aaaaaaaaaa"}
	w.tty = true
	w.controllerNoApproval("op-aaaaaaaaaa", noApprovalPlan(planAB))
	if code := w.run("y\n", "scale", "m", "--instances", "1", "--timeout", "60s"); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w.errs.String())
	}
	if used := w.now.Sub(t0); used >= 10*time.Second {
		t.Errorf("the command waited %s of its 60s timeout", used)
	}
	contains(t, "stdout", w.out.String(), "confirmed plan "+planAB, "Executable=False ExecutionNotBuilt")
}

// When the controller does not record the confirm within the timeout, the command says so, with the phase it saw, and the confirm
// stands: it is written, and the command's success is the write's.
func TestConfirmThatTheControllerNeverSeesSaysSo(t *testing.T) {
	op := planned("op", planAB)
	op.Generation = 1
	op.Status.ObservedGeneration = 1
	w := newWorld(t, []client.Object{op})
	if code := w.run("", "confirm", "op", "--plan", planAB, "--timeout", "5s"); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w.errs.String())
	}
	if used := w.now.Sub(t0); used < 5*time.Second {
		t.Errorf("the command gave up after %s, before its timeout of 5s", used)
	}
	contains(t, "stdout", w.out.String(), "phase: Planned (the controller has not picked up the confirm yet)")
	if op := w.operation("op"); op.Spec.Confirm == nil {
		t.Error("the confirm was not written")
	}
}
