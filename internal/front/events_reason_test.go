package front

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// capturingRecorder is an events.EventRecorder that keeps each call's
// reason and action.
type capturingRecorder struct{ reasons, actions []string }

func (c *capturingRecorder) Eventf(_ runtime.Object, _ runtime.Object, _, reason, action, _ string, _ ...interface{}) {
	c.reasons = append(c.reasons, reason)
	c.actions = append(c.actions, action)
}

// TestKubeEventReasonIsTheCodeAlone (@hpc-burst-science-agent's HBEP-8
// draft, 31 Oct 2026): an event's reason is the CamelCase code alone, and
// its HBEP-8 type is the action; never "<type>/<Code>". Each of the front's
// three codes is checked.
func TestKubeEventReasonIsTheCodeAlone(t *testing.T) {
	c := &capturingRecorder{}
	k := KubeRecorder{R: c}
	md := &v1alpha1.ModelDeployment{}
	for _, code := range []string{reasonCountsDrift, reasonCapacityDrop, reasonDrainWinShort} {
		k.Event(md, true, eventDegraded, code, "a message")
	}
	want := []string{"CountsDrift", "CapacityDropped", "DrainWindowShort"}
	for i := range want {
		if c.reasons[i] != want[i] || c.actions[i] != eventDegraded {
			t.Errorf("event %d: reason %q action %q, want reason %q and action %q", i, c.reasons[i], c.actions[i], want[i], eventDegraded)
		}
	}
}
