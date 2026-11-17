package agentcli

import (
	"context"
	"errors"
	"fmt"
	"io"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// maxConfirmTries is how many times a confirm reads the Operation and writes again after a conflict.
const maxConfirmTries = 5

type confirmOutcome string

const (
	confirmedNow     confirmOutcome = "confirmed"
	confirmedAlready confirmOutcome = "alreadyConfirmed"
	// confirmChanged: the current plan is not the one asked for. Nothing is written.
	confirmChanged confirmOutcome = "planChanged"
)

// errConfirmedOther is a refusal: the Operation is confirmed with another plan.
var errConfirmedOther = errors.New("already confirmed with another plan")

// confirmPlan confirms the plan with ID planID: it reads the Operation, checks that the plan is that one,
// and updates spec.confirm.planID with the resourceVersion it just read. A conflict reads again and tries
// again, but only while the plan is still that one: it never confirms a plan the caller has not seen.
// The returned Operation is the last one read, so a changed plan can be shown. It writes the Operation
// and nothing else.
func confirmPlan(ctx context.Context, c client.Client, ns, name, planID string) (*v1alpha1.Operation, confirmOutcome, error) {
	var op *v1alpha1.Operation
	for try := 0; try < maxConfirmTries; try++ {
		var err error
		if op, err = getOperation(ctx, c, ns, name); err != nil {
			return nil, "", err
		}
		plan := op.Status.Plan
		switch {
		case plan == nil:
			return op, "", fmt.Errorf("Operation %s has no plan yet (phase %s)", name, phaseOf(op))
		case plan.ID != planID:
			return op, confirmChanged, nil
		case op.Spec.Confirm != nil && op.Spec.Confirm.PlanID == planID:
			return op, confirmedAlready, nil
		case op.Spec.Confirm != nil:
			return op, "", fmt.Errorf("Operation %s is %w (%s)", name, errConfirmedOther, op.Spec.Confirm.PlanID)
		case op.Status.Phase != v1alpha1.OperationPhasePlanned:
			// A plan that waits for its confirm is Planned. Refused keeps its plan, and Stale, Done and the rest are over or
			// under way: confirming any of them would write a confirm that nothing reads, and report it as a success.
			return op, "", fmt.Errorf("Operation %s is in phase %s%s: only a plan in phase Planned can be confirmed; nothing was confirmed", name, phaseOf(op), why(op))
		}
		op.Spec.Confirm = &v1alpha1.OperationConfirm{PlanID: planID}
		switch err := c.Update(ctx, op); {
		case err == nil:
			return op, confirmedNow, nil
		case !apierrors.IsConflict(err):
			return op, "", fmt.Errorf("confirm Operation %s: %w", name, err)
		}
	}
	return op, "", fmt.Errorf("Operation %s changed under %d confirms in a row; try again", name, maxConfirmTries)
}

// why is the Executable condition's message as a clause, or nothing.
func why(op *v1alpha1.Operation) string {
	if m := conditionMessage(op, v1alpha1.ConditionExecutable); m != "" {
		return ": " + m
	}
	return ""
}

// cmdConfirm is hpcburst confirm <operation> --plan <id>.
func (rt *cli) cmdConfirm(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs, o := newFlags("confirm", stderr, false)
	var planID string
	fs.StringVar(&planID, "plan", "", "the ID of the plan to confirm (required)")
	fs.DurationVar(&o.timeout, "timeout", defaultTimeout, "how long to wait for the controller to record the confirm")
	name, code, done := oneArg(fs, args, "Operation name")
	if done {
		return code
	}
	if planID == "" {
		return failf(stderr, exitUsage, "confirm: --plan <id> is required: the plan the caller has seen")
	}
	cl, ns, code := rt.open(ctx, o, stderr)
	if code != exitOK {
		return code
	}
	op, outcome, err := confirmPlan(ctx, cl.c, ns, name, planID)
	if err != nil {
		return failf(stderr, exitFail, "%v", err)
	}
	if outcome == confirmChanged {
		return rt.reportChanged(stdout, stderr, op, planID, o.asJSON)
	}
	if outcome == confirmedAlready && (op.Status.Phase == v1alpha1.OperationPhaseRefused || op.Status.Phase == v1alpha1.OperationPhaseStale) {
		return failf(stderr, exitFail, "Operation %s was confirmed with plan %s, and is in phase %s%s: it will not run", name, planID, phaseOf(op), why(op))
	}
	seen := true
	if outcome == confirmedNow {
		if op, seen, err = rt.awaitConfirmSeen(ctx, cl.c, op, o.timeout); err != nil {
			return failf(stderr, exitFail, "%v", err)
		}
	}
	v := operationViewOf(op, rt.now())
	if o.asJSON {
		return emitCode(stderr, emitJSON(stdout, map[string]any{"schemaVersion": schemaVersion, "result": string(outcome), "operation": v}))
	}
	if outcome == confirmedAlready {
		fmt.Fprintf(stdout, "plan %s of %s is already confirmed\n", planID, name)
	} else {
		fmt.Fprintf(stdout, "confirmed plan %s of %s\n", planID, name)
	}
	printWaiting(stdout, op, v, seen)
	return exitOK
}

// reportChanged says that the current plan is not the one asked for, and shows the current one: the
// caller reads it before confirming anything.
func (rt *cli) reportChanged(stdout, stderr io.Writer, op *v1alpha1.Operation, asked string, asJSON bool) int {
	cur := op.Status.Plan.ID
	fmt.Fprintf(stderr, "hpcburst: plan %s is not the current plan of Operation %s: the current plan is %s; nothing was confirmed\n", asked, op.Name, cur)
	v := operationViewOf(op, rt.now())
	if asJSON {
		emitCode(stderr, emitJSON(stdout, map[string]any{"schemaVersion": schemaVersion, "result": string(confirmChanged), "operation": v}))
		return exitFail
	}
	if v.Plan != nil {
		printPlan(stdout, *v.Plan)
	}
	return exitFail
}

// emitCode turns the result of writing JSON into an exit code.
func emitCode(stderr io.Writer, err error) int {
	if err != nil {
		return failf(stderr, exitFail, "write the output: %v", err)
	}
	return exitOK
}

// open connects as the caller and settles the namespace. A non-zero code is the exit, already reported.
func (rt *cli) open(ctx context.Context, o *options, stderr io.Writer) (*caller, string, int) {
	cl, err := rt.connect(ctx, o.kubeconfig, stderr)
	if err != nil {
		var u usageError
		if errors.As(err, &u) {
			return nil, "", failf(stderr, exitUsage, "%v", err)
		}
		return nil, "", failf(stderr, exitFail, "%v", err)
	}
	ns := o.namespace
	if ns == "" {
		ns = cl.namespace
	}
	return cl, ns, exitOK
}
