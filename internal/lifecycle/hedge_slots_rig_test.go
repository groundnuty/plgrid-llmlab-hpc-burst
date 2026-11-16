package lifecycle

// HBEP-7's hedge slots row (Test plan, The hedge's slots), host-dialed (HBEP-6: 2 Allocations per Provider), and its
// row of a hedge across two accelerator classes. Every count is worked out by hand.

import (
	"context"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// slotsOn reads a Provider's count as a reservation would: the Allocations holding a slot, and the reserved slots
// as "kind:Instance's Allocation" with the deployment holding each.
func (rg *rig) slotsOn(provider string) (listed int, reserved []reservedSlot) {
	rg.t.Helper()
	_, l, allocs, err := listProviderAllocations(context.Background(), rg.r.Reader, provider)
	if err != nil {
		rg.t.Fatal(err)
	}
	var prof v1alpha1.PlatformProfile
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Name: rigProfile}, &prof); err != nil {
		rg.t.Fatal(err)
	}
	slots, err := reservedOn(context.Background(), rg.r.Reader, provider, &prof.Spec, allocs)
	if err != nil {
		rg.t.Fatal(err)
	}
	return len(l), slots
}

// hostDialedHedge starts D (hoMD) on A and B, host-dialed, with P on A created at 08:00:10 and A's waits all 10 h
// written then, so at 08:00:20, as P's job starts, P's replacement R1 is made on A (pending 10 h) and its hedge is
// due a member at once. before runs before that pass (nil: nothing). It returns P's name.
func hostDialedHedge(t *testing.T, h *hedgeRig, before func()) string {
	t.Helper()
	p := replacementStart(t, h, 10*time.Hour)
	setWaits(h.rig, rigProvider, 20, 10*time.Hour)
	if before != nil {
		before()
	}
	return p
}

// TestTheHedgesSlotsHostDialed: D's primary P runs on A with no end time, so A counts P and the slot P holds for its
// replacement.
//   - B empty: D's replacement hedge gets R1 on A, in P's reserved slot (A: 2 listed, none reserved), and W on B
//     (B: 1 listed).
//   - Then W, whose job starts at once, wins: R1 is cancelled, and once its deletion completes A counts P alone, with
//     no reserved slot, since P has a live replacement. B counts W and the slot W holds from its win: 2. A new
//     deployment F that allows B then A is refused on B and created on A.
//   - With deployment E's primary running on B first (B: 1 listed and 1 reserved), D's hedge gets R1 on A alone, and
//     D has no SubmitLimitReached.
func TestTheHedgesSlotsHostDialed(t *testing.T) {
	t.Run("B empty", func(t *testing.T) {
		h := newHedgeRig(t, withHostDialed())
		p := hostDialedHedge(t, h, nil)
		h.clock.Advance(10 * time.Second)
		h.Step() // 08:00:20
		var r1, w string
		for _, r := range hoRepl(h.rig, p) {
			switch r.Labels[v1alpha1.LabelProvider] {
			case rigProvider:
				r1 = r.Name
			case hedgeB:
				w = r.Name
			}
		}
		if r1 == "" || w == "" {
			t.Fatalf("P's replacements at 08:00:20: %+v, want R1 on A and a member on B", hoRepl(h.rig, p))
		}
		if n, res := h.slotsOn(rigProvider); n != 2 || len(res) != 0 {
			t.Fatalf("A counts %d listed and %d reserved (%+v), want 2 and 0", n, len(res), res)
		}
		if n, res := h.slotsOn(hedgeB); n != 1 || len(res) != 0 {
			t.Fatalf("B counts %d listed and %d reserved (%+v), want 1 and 0", n, len(res), res)
		}
		r1Alloc := h.mustInstance(r1).Spec.AllocationRef.Name
		h.until(5*time.Minute, 10*time.Second, "W won and R1's Allocation is gone", func() bool {
			_, there := h.allocation(r1Alloc)
			return h.mustInstance(w).Status.Phase == v1alpha1.InstanceReady && !there
		})
		if n, res := h.slotsOn(rigProvider); n != 1 || len(res) != 0 {
			t.Fatalf("after the win, A counts %d listed and %d reserved (%+v), want P alone, 1 and 0", n, len(res), res)
		}
		n, res := h.slotsOn(hedgeB)
		wAlloc := rigNS + "/" + h.mustInstance(w).Spec.AllocationRef.Name
		if n != 1 || len(res) != 1 || res[0].key != wAlloc || res[0].deployment != rigNS+"/"+hoMD {
			t.Fatalf("after the win, B counts %d listed and reserved %+v, want W and W's slot for D: 1 and %s", n, res, wAlloc)
		}
		f := mdSpec("md-f")
		f.Spec.Providers.Names = []string{hedgeB, rigProvider}
		f.Spec.HedgeWidth = ptrTo(int32(1)) // F's own placement, not a hedge of it
		h.create(f)
		h.clock.Advance(10 * time.Second)
		h.Step()
		fs := h.liveInstances("md-f")
		if len(fs) != 1 || fs[0].Labels[v1alpha1.LabelProvider] != rigProvider {
			t.Fatalf("F's Instances %+v, want one, on A: B's two slots are W and W's reserved one", fs)
		}
	})
	t.Run("E serving on B", func(t *testing.T) {
		h := newHedgeRig(t, withHostDialed())
		e := mdSpec("md-e")
		e.Spec.Providers.Names = []string{hedgeB}
		h.create(e)
		h.until(2*time.Minute, 10*time.Second, "E ready on B", func() bool { return h.readyCount("md-e") == 1 })
		if n, res := h.slotsOn(hedgeB); n != 1 || len(res) != 1 {
			t.Fatalf("B counts %d listed and %d reserved, want E's primary and its reserved slot, 1 and 1", n, len(res))
		}
		p := hostDialedHedge(t, h, nil)
		h.clock.Advance(10 * time.Second)
		h.Step()
		var onA, onB int
		for _, r := range hoRepl(h.rig, p) {
			switch r.Labels[v1alpha1.LabelProvider] {
			case rigProvider:
				onA++
			case hedgeB:
				onB++
			}
		}
		if onA != 1 || onB != 0 {
			t.Fatalf("P's replacements: %d on A and %d on B, want R1 on A alone", onA, onB)
		}
		if c := h.condition(hoMD, v1alpha1.ConditionDegraded); c.Reason == v1alpha1.ReasonSubmitLimitReached {
			t.Fatalf("D has Degraded %s %q: a hedge passed over B sets no reason", c.Reason, c.Message)
		}
	})
}

// TestAHedgeWinnerCarriesItsOwnClasssID: A and B are in different accelerator classes. P's replacement hedge (A's
// waits 10 h) gets W on B, which wins. W carries B's class's current ID, not P's: the same ID a control deployment
// of the same spec, allowed on B alone, gives its primary there, and a different one from P's. Neither ID is read
// from a status.
func TestAHedgeWinnerCarriesItsOwnClasssID(t *testing.T) {
	h := newHedgeRig(t)
	p := replacementStart(t, h, 10*time.Hour)
	setWaits(h.rig, rigProvider, 20, 10*time.Hour)
	h.clock.Advance(10 * time.Second)
	h.Step()
	var w string
	for _, r := range hoRepl(h.rig, p) {
		if r.Labels[v1alpha1.LabelProvider] == hedgeB {
			w = r.Name
		}
	}
	if w == "" {
		t.Fatal("no member of P's hedge on B")
	}
	ctl := mdSpec("md-ctl")
	ctl.Spec.Providers.Names = []string{hedgeB}
	h.create(ctl)
	h.until(2*time.Minute, 10*time.Second, "W ready and the control placed", func() bool {
		return h.mustInstance(w).Status.Phase == v1alpha1.InstanceReady && len(h.liveInstances("md-ctl")) == 1
	})
	wID, pID, bID := h.mustInstance(w).Spec.DeploymentID, h.mustInstance(p).Spec.DeploymentID, h.liveInstances("md-ctl")[0].Spec.DeploymentID
	if wID == "" || wID != bID || wID == pID {
		t.Fatalf("W's ID %q, B's class's (the control's) %q, P's %q: want W's equal to B's and different from P's", wID, bID, pID)
	}
	if h.mustInstance(w).Labels[v1alpha1.LabelDeploymentID] == h.mustInstance(p).Labels[v1alpha1.LabelDeploymentID] {
		t.Fatal("W's deployment-id label is P's")
	}
}
