package front

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// capturingRecorder is an events.EventRecorder that keeps each call's
// reason and action.
type capturingRecorder struct{ reasons, actions, notes []string }

func (c *capturingRecorder) Eventf(_ runtime.Object, _ runtime.Object, _, reason, action, note string, args ...interface{}) {
	c.reasons = append(c.reasons, reason)
	c.actions = append(c.actions, action)
	c.notes = append(c.notes, fmt.Sprintf(note, args...))
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

// TestKubeRecorderRecordsAnEventAboutALease (HBEP-8 review,
// @hpc-burst-science-agent, 31 Oct 2026): the recorder drops an event whose
// reference it cannot build, so its scheme must know coordination.k8s.io/v1
// for an event about a Lease to reach the API.
func TestKubeRecorderRecordsAnEventAboutALease(t *testing.T) {
	cs := kubefake.NewSimpleClientset()
	rec, stop, err := NewKubeRecorder(cs)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "front", UID: "lease-uid-1"}}
	rec.Event(lease, true, eventFailing, reasonUsageWriteFailed, "a message")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		l, err := cs.EventsV1().Events("ns").List(context.Background(), metav1.ListOptions{})
		if err == nil && len(l.Items) > 0 {
			ev := l.Items[0]
			if ev.Regarding.Kind != "Lease" || ev.Reason != reasonUsageWriteFailed || ev.Action != eventFailing {
				t.Errorf("event regarding %s/%s reason %q action %q, want a Lease, %s and %s", ev.Regarding.Kind, ev.Regarding.Name, ev.Reason, ev.Action, reasonUsageWriteFailed, eventFailing)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no event about the Lease reached the API: its reference could not be built")
}

// TestUsageWriteFailedIsThrottled (HBEP-8 review): the event goes out when a
// failure starts, then at most once an hour while it lasts; a write that
// succeeds ends the failure, so the next one raises an event at once.
func TestUsageWriteFailedIsThrottled(t *testing.T) {
	f := newScFix(t, scOpts{})
	boom := errors.New("write usage.jsonl: no space left on device")
	n := func() int { return len(f.rec.Find(eventFailing, reasonUsageWriteFailed)) }
	for i := 0; i < 5; i++ {
		f.router.usageFailed(scDep, "r", boom)
		f.clk.Advance(10 * time.Minute)
	}
	if got := n(); got != 1 {
		t.Fatalf("five failures within an hour gave %d events, want 1", got)
	}
	f.clk.Advance(20 * time.Minute) // an hour since the event
	f.router.usageFailed(scDep, "r", boom)
	if got := n(); got != 2 {
		t.Fatalf("a failure an hour on gave %d events in all, want 2", got)
	}
	// A request whose record is written ends the failure.
	scChatOnce(t, f.harness, "s")
	errWaitFor(t, "the request's record", func() bool { return len(f.records()) == 1 })
	f.router.usageFailed(scDep, "r", boom)
	if got := n(); got != 3 {
		t.Fatalf("a new failure after a written record gave %d events in all, want 3", got)
	}
}

// TestKubeEventNoteIsCutTo1024Bytes (HBEP-8, Events, Form; Test plan, Event
// form): the API refuses a note over 1,024 bytes and the recorder does not
// retry a refusal, so the front cuts it. The cut falls on a character
// boundary: 2-byte characters fill 1,024 bytes exactly, 3-byte ones stop at
// 1,023, and a note of exactly 1,024 bytes or less is sent whole.
func TestKubeEventNoteIsCutTo1024Bytes(t *testing.T) {
	c := &capturingRecorder{}
	k := KubeRecorder{R: c}
	md := &v1alpha1.ModelDeployment{}
	exact := strings.Repeat("a", 1024)
	cases := []struct{ name, note string }{
		{"ascii over the limit", strings.Repeat("a", 5000)},
		{"two-byte characters", strings.Repeat("\u00e9", 3000)},
		{"three-byte characters", strings.Repeat("\u20ac", 3000)},
		{"four-byte characters at the edge", "a" + strings.Repeat("\U0001F600", 1000)},
		{"exactly 1024 bytes", exact},
		{"short", "usage records are not being written"},
	}
	wantLen := []int{1024, 1024, 1023, 1021, 1024, len("usage records are not being written")}
	for i, tc := range cases {
		k.Event(md, true, eventFailing, reasonUsageWriteFailed, tc.note)
		got := c.notes[i]
		if len(got) != wantLen[i] || !utf8.ValidString(got) || !strings.HasPrefix(tc.note, got) {
			t.Errorf("%s: note is %d bytes (valid UTF-8 %v, prefix of the original %v), want %d bytes, valid, a prefix",
				tc.name, len(got), utf8.ValidString(got), strings.HasPrefix(tc.note, got), wantLen[i])
		}
	}
}
