package agentcli

import (
	"context"
	"fmt"
	"io"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// waitUntil calls check until it reports done or fails, or until timeout has passed on the runtime's
// clock. timedOut is true when the time ran out first. A pause between two calls is the runtime's, so a
// test moves the clock with it and never sleeps.
func (rt *cli) waitUntil(ctx context.Context, timeout time.Duration, check func() (bool, error)) (timedOut bool, err error) {
	deadline := rt.now().Add(timeout)
	for {
		done, err := check()
		if err != nil || done {
			return false, err
		}
		if !rt.now().Before(deadline) {
			return true, nil
		}
		if err := rt.pause(ctx, rt.poll); err != nil {
			return false, err
		}
	}
}

// getOperation reads one Operation.
func getOperation(ctx context.Context, c client.Client, ns, name string) (*v1alpha1.Operation, error) {
	op := &v1alpha1.Operation{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, op); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("Operation %s not found in namespace %s", name, ns)
		}
		return nil, fmt.Errorf("read Operation %s: %w", name, err)
	}
	return op, nil
}

// awaitPlan reads the Operation until it holds a plan or is Refused. op is the last read.
func (rt *cli) awaitPlan(ctx context.Context, c client.Client, op *v1alpha1.Operation, timeout time.Duration) (cur *v1alpha1.Operation, timedOut bool, err error) {
	cur = op
	timedOut, err = rt.waitUntil(ctx, timeout, func() (bool, error) {
		if hasPlanOrRefusal(cur) {
			return true, nil
		}
		got, err := getOperation(ctx, c, cur.Namespace, cur.Name)
		if err != nil {
			return false, err
		}
		cur = got
		return hasPlanOrRefusal(cur), nil
	})
	return cur, timedOut, err
}

func hasPlanOrRefusal(op *v1alpha1.Operation) bool {
	return op.Status.Plan != nil || op.Status.Phase == v1alpha1.OperationPhaseRefused
}

// awaitConfirmSeen reads the Operation until the controller has seen the confirm. The confirm's update moved the spec's
// metadata.generation, and the controller records the generation it acted on in status.observedGeneration; the phase cannot be
// waited for, since a plan that needs no approval does not move it where the execution is not built. A phase that has moved
// on counts as seen too. seen is false when the timeout passed first. confirmed is the Operation as the update answered.
func (rt *cli) awaitConfirmSeen(ctx context.Context, c client.Client, confirmed *v1alpha1.Operation, timeout time.Duration) (cur *v1alpha1.Operation, seen bool, err error) {
	cur = confirmed
	isSeen := func() bool {
		moved := cur.Status.Phase != "" && cur.Status.Phase != v1alpha1.OperationPhasePlanned && cur.Status.Phase != v1alpha1.OperationPhasePlanning
		return moved || cur.Status.ObservedGeneration >= confirmed.Generation
	}
	timedOut, err := rt.waitUntil(ctx, timeout, func() (bool, error) {
		got, err := getOperation(ctx, c, cur.Namespace, cur.Name)
		if err != nil {
			return false, err
		}
		cur = got
		return isSeen(), nil
	})
	return cur, !timedOut && err == nil, err
}

// conditionMessage is the message of the named condition, or the first condition that is false.
func conditionMessage(op *v1alpha1.Operation, typ string) string {
	for _, c := range op.Status.Conditions {
		if c.Type == typ && c.Message != "" {
			return c.Message
		}
	}
	for _, c := range op.Status.Conditions {
		if c.Status == "False" && c.Message != "" {
			return c.Message
		}
	}
	return ""
}

// printWaiting writes what the controller says after a confirm: the phase, the Executable condition (its reason and its message),
// and who must approve when the plan waits. seen is false when the controller had not recorded the confirm within the timeout.
func printWaiting(w io.Writer, op *v1alpha1.Operation, v operationView, seen bool) {
	if !seen {
		fmt.Fprintf(w, "phase: %s (the controller has not picked up the confirm yet)\n", v.Phase)
		return
	}
	fmt.Fprintf(w, "phase: %s\n", v.Phase)
	if op.Status.Phase == v1alpha1.OperationPhaseAwaitingApproval {
		if m := conditionMessage(op, v1alpha1.ConditionExecutable); m != "" {
			fmt.Fprintf(w, "  waiting: %s\n", m)
		}
		printApprovers(w, v.Plan)
		return
	}
	if c := apimeta.FindStatusCondition(op.Status.Conditions, v1alpha1.ConditionExecutable); c != nil {
		fmt.Fprintf(w, "  %s=%s %s: %s\n", c.Type, c.Status, c.Reason, c.Message)
	}
}
