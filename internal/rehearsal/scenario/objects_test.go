package scenario_test

// The API objects of the rehearsal: one batch PlatformProfile whose partition
// is the fake cluster's, one Provider over the login node, and one served
// ModelDeployment. The engine double's modes travel as the engine's env, the
// way the real controller writes a launch (internal/lifecycle/desired.go).

import (
	"testing"

	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// conditionTrue reports whether the object's condition has status True.
func conditionTrue(conds []metav1.Condition, typ string) bool {
	return meta.IsStatusConditionTrue(conds, typ)
}

// orphanEvents lists the events.k8s.io Events whose reason is TunnelOrphaned:
// frps listed a proxy online with no current Instance for TunnelOrphanGrace
// (HBEP-6, an orphan). The controller raises it regarding the Allocation that
// admitted the tunnel, or its Lease.
func orphanEvents(t *testing.T, c client.Client) []eventsv1.Event {
	t.Helper()
	var out []eventsv1.Event
	for _, e := range stack.Events(t, c) {
		if e.Reason == v1alpha1.EventReasonTunnelOrphaned {
			out = append(out, e)
		}
	}
	return out
}

// reportOrphanedTunnels logs each TunnelOrphaned event so far as a FINDING
// about the agent's teardown order, and returns how many there are.
func reportOrphanedTunnels(t *testing.T, c client.Client, step string) int {
	t.Helper()
	evs := orphanEvents(t, c)
	for _, e := range evs {
		t.Logf("FINDING step (%s): %s regarding %s %s: %.300s", step, e.Reason, e.Regarding.Kind, e.Regarding.Name, e.Note)
	}
	t.Logf("step (%s): %d TunnelOrphaned events", step, len(evs))
	return len(evs)
}
