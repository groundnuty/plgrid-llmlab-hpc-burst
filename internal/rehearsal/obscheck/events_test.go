package obscheck

// Issue #193's checks on their own, in an ordinary go test: each is fed a fixture that a correct run gives, which must
// pass, and fixtures that break one rule each, which must fail. A check that passes a broken fixture would pass a
// broken product too.

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

func event(action, reason, kind, name, controller, typ string) eventsv1.Event {
	return eventsv1.Event{ObjectMeta: metav1.ObjectMeta{Name: "e-" + reason}, Action: action, Reason: reason, Type: typ,
		Regarding: corev1.ObjectReference{Kind: kind, Namespace: stack.Namespace, Name: name}, ReportingController: controller, Note: "n"}
}

func TestEventProblems(t *testing.T) {
	want := []WantEvent{
		{Action: "ready", Reason: "Ready", Kind: "Instance", Namespace: stack.Namespace, Name: "p", Controller: LifecycleController},
		{Action: "degraded", Reason: "CapacityDropped", Kind: "ModelDeployment", Namespace: stack.Namespace, Name: "md", Controller: LifecycleController, Related: "p"},
	}
	forbid := []WantEvent{{Action: "degraded", Reason: "ReplacementLate", Kind: "ModelDeployment", Namespace: stack.Namespace, Name: "md"}}
	good := func() []eventsv1.Event {
		cd := event("degraded", "CapacityDropped", "ModelDeployment", "md", LifecycleController, provider.EventKindWarning)
		cd.Related = &corev1.ObjectReference{Kind: "Instance", Name: "p"}
		return []eventsv1.Event{
			event("ready", "Ready", "Instance", "p", LifecycleController, provider.EventKindOrdinary), cd,
			// The Provider controller's untyped diagnostics are left out.
			{ObjectMeta: metav1.ObjectMeta{Name: "x"}, Reason: "SubmittedJob", Type: provider.EventKindOrdinary,
				Regarding: corev1.ObjectReference{Kind: "Allocation", Name: "a"}, ReportingController: "provider"},
		}
	}
	if m, p := EventProblems(good(), want, forbid); len(m) != 0 || len(p) != 0 {
		t.Fatalf("a correct set: missing %v, problems %v", m, p)
	}
	for name, breakIt := range map[string]func([]eventsv1.Event) []eventsv1.Event{
		"an expected event missing":  func(e []eventsv1.Event) []eventsv1.Event { return e[1:] },
		"ready emitted as a Warning": func(e []eventsv1.Event) []eventsv1.Event { e[0].Type = provider.EventKindWarning; return e },
		"degraded emitted as Normal": func(e []eventsv1.Event) []eventsv1.Event { e[1].Type = provider.EventKindOrdinary; return e },
		"a compound reason":          func(e []eventsv1.Event) []eventsv1.Event { e[0].Reason = "ready/Ready"; return e },
		"no action":                  func(e []eventsv1.Event) []eventsv1.Event { e[0].Action = ""; return e },
		"no related Instance":        func(e []eventsv1.Event) []eventsv1.Event { e[1].Related = nil; return e },
		"regarding another object":   func(e []eventsv1.Event) []eventsv1.Event { e[1].Regarding.Name = "other"; return e },
		"a note over 1,024 bytes":    func(e []eventsv1.Event) []eventsv1.Event { e[0].Note = strings.Repeat("x", 1025); return e },
		"a forbidden ReplacementLate": func(e []eventsv1.Event) []eventsv1.Event {
			return append(e, event("degraded", "ReplacementLate", "ModelDeployment", "md", LifecycleController, provider.EventKindWarning))
		},
		"a TunnelOrphaned": func(e []eventsv1.Event) []eventsv1.Event {
			return append(e, event("degraded", v1alpha1.EventReasonTunnelOrphaned, "Allocation", "a", LifecycleController, provider.EventKindWarning))
		},
	} {
		if m, p := EventProblems(breakIt(good()), want, forbid); len(m) == 0 && len(p) == 0 {
			t.Errorf("%s: eventProblems found nothing", name)
		}
	}
}
