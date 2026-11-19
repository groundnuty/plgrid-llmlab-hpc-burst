package front

import (
	"slices"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The pinned target's state, a Workload's edits, and the FIFO's order (HBEP-5, Workloads, Sticky routes: Requests after the pin, A target that's gone;
// Edits). Rows that show a conversation is refused or kept make a fresh decision go elsewhere, so a refusal is not the ring's doing.

// removeMD deletes the ModelDeployment and its Instances from the snapshot, as a delete and the garbage collector do.
func (f *skFix) removeMD(name string) {
	f.t.Helper()
	uid := f.mdUID(name)
	f.snap.Instances = slices.DeleteFunc(f.snap.Instances, func(in v1alpha1.Instance) bool {
		return len(in.OwnerReferences) == 1 && in.OwnerReferences[0].UID == uid
	})
	f.snap.Deployments = slices.DeleteFunc(f.snap.Deployments, func(md v1alpha1.ModelDeployment) bool { return md.UID == uid })
	f.apply()
}

// A target that's gone (Sticky routes): the ModelDeployment deleted, or no current deployment ID of it equal to the pinned one, as after a finished rollout,
// is 503 model_inactive with detail pinned_target_gone, and the conversation reaches no engine, while a new conversation whose home is the same place is served
// by what is left, so the refusal is the pin's. An Instance of the pinned ID that is verifying, or none of it at all with the ID still current, as in a
// handover's gap, is a wait: 503 capacity after queueTimeout, never gone, and never a move to a-0.
func TestStickyTargetThatIsGoneRefusesAndOneThatIsOnlyAbsentWaits(t *testing.T) {
	gone := wantError{status: 503, code: "model_inactive", detail: "pinned_target_gone"}
	wait := func(c *Config) { c.QueueTimeout = Duration(300 * time.Millisecond) }
	ask := func(f *skFix, s string, want wantError) {
		t.Helper()
		before := f.totalSaw()
		resp := f.errPost(scChat, errChatBody(f.alpha(), false, "hello there"), "x-session-id", s)
		checkError(t, resp, readAll(t, resp), want)
		if n := f.totalSaw(); n != before {
			t.Errorf("the engines saw %d requests of a conversation whose pinned target was not there", n-before)
		}
	}

	t.Run("the ModelDeployment is deleted", func(t *testing.T) {
		f := newSkFix(t, skOpts{})
		s, fresh := f.session("ns/b-0", 0), f.session("ns/b-0", 1)
		f.at(s, "ns/b-0", "the first request")
		f.removeMD("md-b")
		ask(f, s, gone)
		f.at(fresh, "ns/a-0", "a new conversation, whose home was the deleted b-0")
	})
	t.Run("no current deployment ID equals the pinned one", func(t *testing.T) {
		f := newSkFix(t, skOpts{})
		s, fresh := f.session("ns/b-0", 0), f.session("ns/b-0", 1)
		f.at(s, "ns/b-0", "the first request")
		next := v1alpha1.FullDeploymentID(f.idOf("ns/a-0")) // a rollout finished: the Instance and the current ID are another
		f.inst(f.uids["ns/b-0"]).Spec.DeploymentID = next
		f.md("md-b").Status.CurrentIDs = []v1alpha1.ClassDeploymentID{{ID: next}}
		f.apply()
		ask(f, s, gone)
		f.at(fresh, "ns/b-0", "a new conversation takes the ModelDeployment's new ID")
	})
	t.Run("the ID is still current but has no Instance, a handover's gap", func(t *testing.T) {
		f := newSkFix(t, skOpts{edit: wait})
		s := f.session("ns/b-0", 0)
		f.at(s, "ns/b-0", "the first request")
		f.snap.Instances = slices.DeleteFunc(f.snap.Instances, func(in v1alpha1.Instance) bool { return in.UID == f.uids["ns/b-0"] })
		f.apply()
		ask(f, s, wantError{status: 503, code: "capacity", retryAfter: "10"})
	})
	t.Run("the ModelDeployment's status lists no current ID, and the pinned Instance is full", func(t *testing.T) {
		// Gone needs positive evidence (the science agent's ruling of 19 Nov 2026 on #795): an empty list waits, here for a full Instance, and is 503 capacity
		// after queueTimeout, never pinned_target_gone.
		f := newSkFix(t, skOpts{edit: wait})
		s := f.session("ns/b-0", 0)
		f.at(s, "ns/b-0", "the first request")
		f.md("md-b").Status.CurrentIDs = nil
		f.apply()
		release := f.hold("ns/b-0")
		held, _ := f.start(s)
		errWaitFor(t, "the held request to reach b-0", func() bool { return f.engines["ns/b-0"].Open() == 1 })
		ask(f, s, wantError{status: 503, code: "capacity", retryAfter: "10"})
		release()
		if resp, body := held.wait(t); resp.StatusCode != 200 {
			t.Errorf("the held request: %d %s", resp.StatusCode, body)
		}
		f.at(s, "ns/b-0", "with b-0 free again, still on its pin")
	})
	t.Run("the Instance is verifying", func(t *testing.T) {
		f := newSkFix(t, skOpts{edit: wait})
		s := f.session("ns/b-0", 0)
		f.at(s, "ns/b-0", "the first request")
		f.inst(f.uids["ns/b-0"]).Status.Phase = v1alpha1.InstanceVerifying
		f.apply()
		ask(f, s, wantError{status: 503, code: "capacity", retryAfter: "10"})
		f.inst(f.uids["ns/b-0"]).Status.Phase = v1alpha1.InstanceReady
		f.apply()
		f.at(s, "ns/b-0", "once the Instance is ready again")
	})
}

// An edit of a ModelDeployment's spec.workloads moves no pin (Sticky routes; Edits). The conversation is pinned to a-0, its home. md-a then lists only another
// Workload, so alpha no longer names it among those that serve it: the conversation is still on a-0, and a new conversation with the same home is not: it goes
// to b-0. When md-b stops serving alpha too, no ModelDeployment serves it: the pinned conversation is still on a-0, and a new conversation is 503 model_inactive
// with detail not_attached, as for any request of a Workload that no deployment serves.
func TestStickySpecWorkloadsEditMovesNoPin(t *testing.T) {
	f := newSkFix(t, skOpts{})
	s, fresh := f.session("ns/a-0", 0), f.session("ns/a-0", 1)
	f.at(s, "ns/a-0", "the first request")
	f.md("md-a").Spec.Workloads = []string{"beta"}
	f.apply()
	f.at(s, "ns/a-0", "after md-a stopped listing alpha")
	f.at(fresh, "ns/b-0", "a new conversation, which md-a no longer serves")
	f.md("md-b").Spec.Workloads = []string{"beta"}
	f.apply()
	f.at(s, "ns/a-0", "after no ModelDeployment lists alpha")
	resp := f.errPost(scChat, errChatBody(f.alpha(), false, "hello there"), "x-session-id", f.session("ns/b-0", 5))
	checkError(t, resp, readAll(t, resp), wantError{status: 503, code: "model_inactive", detail: "not_attached", retryAfter: "30"})
}

// Recreating the Workload clears its pins, since they key on its UID (Lifetime). The conversation is pinned to b-0 by a spill. The Workload is deleted and
// created again, with a new UID: the conversation is a new one, decided afresh, and goes to its home a-0, and the journal still holds the old Workload's pin
// beside the new one's.
func TestStickyRecreatedWorkloadStartsWithNoPins(t *testing.T) {
	f := newSkFix(t, skOpts{})
	s := f.session("ns/a-0", 0)
	f.pinAway(s)
	old := f.alphaUID()
	f.dropWorkload("ns", "alpha")
	w := f.workload("ns", "alpha", scModel)
	w.Spec.Shape.ContextTokens.Typical, w.Spec.Shape.OutputTokens.Typical = 100, 25
	w.Spec.SessionAffinity = v1alpha1.SessionAffinityRoute
	f.apply()
	if f.alphaUID() == old {
		t.Fatal("the recreated Workload has the old UID")
	}
	f.at(s, "ns/a-0", "after the Workload was recreated")
	var uids []types.UID
	for _, r := range f.journalOf("pin", s) {
		uids = append(uids, r.Workload)
	}
	if !slices.Contains(uids, old) || !slices.Contains(uids, f.alphaUID()) {
		t.Errorf("the journal's pins of the key name Workloads %v, want the old %s and the new %s", uids, old, f.alphaUID())
	}
}

// A pinned head that can't be placed blocks no other request (Sticky routes, Requests after the pin). a-0 and b-0 each hold one request of the conversation
// pinned to them, at cap 1. A second request of the a-0 conversation waits: its target is full. A request of the b-0 conversation, a new conversation with its
// home a-0 and a request that names no Workload are then served at once, though the FIFO holds the older waiter. When b-0 is held again and a younger
// request of the b-0 conversation waits behind the first, freeing b-0 places the younger one, which is behind the stuck a-0 waiter in the queue.
func TestStickyPinnedWaiterThatCannotBePlacedBlocksNoOtherRequest(t *testing.T) {
	f := newSkFix(t, skOpts{edit: func(c *Config) { c.QueueTimeout = Duration(time.Minute) }})
	sa, sb, fresh := f.session("ns/a-0", 0), f.session("ns/b-0", 0), f.session("ns/a-0", 1)
	f.at(sa, "ns/a-0", "the a-0 conversation")
	f.at(sb, "ns/b-0", "the b-0 conversation")
	releaseA := f.hold("ns/a-0")
	holdA, _ := f.start(sa)
	errWaitFor(t, "a-0 to hold a request", func() bool { return f.engines["ns/a-0"].Open() == 1 })
	stuck, _ := f.start(sa)
	errWaitFor(t, "the second request of the a-0 conversation to wait", func() bool { return f.waiting() == 1 })

	f.at(sb, "ns/b-0", "the b-0 conversation behind the stuck waiter")
	f.at(fresh, "ns/b-0", "a new conversation whose home a-0 is full, behind the stuck waiter")
	if got, resp, body := f.postAt(f.session("ns/b-0", 2), scModel); got != "ns/b-0" || resp.StatusCode != 200 {
		t.Errorf("a request that names no Workload went to %q (%d %s), want b-0", got, resp.StatusCode, body)
	}
	if n := f.waiting(); n != 1 {
		t.Fatalf("%d requests wait, want only the stuck a-0 one", n)
	}

	releaseB := f.hold("ns/b-0")
	holdB, _ := f.start(sb)
	errWaitFor(t, "b-0 to hold a request", func() bool { return f.engines["ns/b-0"].Open() == 1 })
	younger, _ := f.start(sb)
	errWaitFor(t, "the b-0 conversation's second request to wait behind the a-0 one", func() bool { return f.waiting() == 2 })
	releaseB()
	if resp, body := holdB.wait(t); resp.StatusCode != 200 {
		t.Fatalf("the b-0 holder: %d %s", resp.StatusCode, body)
	}
	if resp, body := younger.wait(t); resp.StatusCode != 200 || scInstanceOf(resp) != "ns/b-0" {
		t.Fatalf("the younger b-0 request: %d from %q, want 200 from b-0, though an older request waits for a-0: %s", resp.StatusCode, scInstanceOf(resp), body)
	}
	if n := f.waiting(); n != 1 {
		t.Errorf("%d requests wait after b-0 was freed, want only the a-0 one", n)
	}
	releaseA()
	for _, c := range []*scCall{holdA, stuck} {
		if resp, body := c.wait(t); resp.StatusCode != 200 || scInstanceOf(resp) != "ns/a-0" {
			t.Errorf("an a-0 conversation request: %d from %q: %s", resp.StatusCode, scInstanceOf(resp), body)
		}
	}
}

// A pinned managed request counts the Workload's managedSessions and waits while it is reached (Sticky routes, Requests after the pin). The Workload is
// ManagedFirst with managedSessions 1: the conversation's first request goes to the LLMLab double, which holds it, and is pinned to that managed target by its
// Provider and its managed ID. A second request of the conversation waits in the Workload's FIFO, though a-0 is idle, and a new conversation, to which the bound
// applies as well, is sent to a-0, the proof that a-0 takes requests. When the first is let go the waiter is sent to the double, never to a-0. An entry that the
// Provider no longer lists is a target that's gone.
func TestStickyManagedPinCountsManagedSessionsAndNeverMovesToAnInstance(t *testing.T) {
	f := newSkFix(t, skOpts{managed: true, stallManaged: true, deps: []pwDep{{name: "md-a", instances: []string{"a-0"}}}})
	s, fresh := f.session("ns/a-0", 0), f.session("ns/a-0", 1)
	first, _ := f.start(s)
	errWaitFor(t, "the first request to reach the LLMLab double", func() bool { return f.llm.Arrived("POST", scChat) == 1 })
	pins := f.journalOf("pin", s)
	if len(pins) != 1 || pins[0].MD != f.mdUID("md-llm") || pins[0].Provider != "llm-a" || pins[0].ManagedID != strings.Repeat("0", 58)+"abc000" {
		t.Fatalf("the pin of the first request is %+v, want the managed target: its source md-llm, Provider llm-a and the catalog entry's managed ID", pins)
	}
	f.at(fresh, "ns/a-0", "a new conversation, with the managed bound reached")
	second, _ := f.start(s)
	errWaitFor(t, "the second request to wait for the managed bound", func() bool { return f.waiting() == 1 })
	if n := f.engineSaw("ns/a-0"); n != 1 {
		t.Fatalf("a-0 saw %d requests, want only the new conversation's: the pinned request moved to our Instance", n)
	}
	f.llm.Stall(false)
	for i, c := range []*scCall{first, second} {
		if resp, body := c.wait(t); resp.StatusCode != 200 || scEmitterOf(resp) != "provider" {
			t.Errorf("request %d of the managed conversation: %d from emitter %q: %s", i+1, resp.StatusCode, scEmitterOf(resp), body)
		}
	}
	if n := f.engineSaw("ns/a-0"); n != 1 {
		t.Errorf("a-0 saw %d requests in all, want 1", n)
	}

	for i := range f.snap.Providers {
		for j := range f.snap.Providers[i].Status.Catalog {
			f.snap.Providers[i].Status.Catalog[j].Listed = false
		}
	}
	f.apply()
	resp := f.errPost(scChat, errChatBody(f.alpha(), false, "hello there"), "x-session-id", s)
	checkError(t, resp, readAll(t, resp), wantError{status: 503, code: "model_inactive", detail: "pinned_target_gone"})
}
