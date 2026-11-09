package scenario_test

// Step (g)'s read of the Allocation watch (issue #281).

import (
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// allocView is what the watch delivered of one Allocation.
type allocView struct {
	name    string
	phases  []v1alpha1.AllocationPhase // each phase in turn, repeats dropped
	deleted bool
}

// delivered reports whether the watch has delivered the Allocation's end or
// its deletion: it was seen deleted, or its last version is Ended.
func (v *allocView) delivered() bool {
	return v != nil && (v.deleted || v.phases[len(v.phases)-1] == v1alpha1.AllocationEnded)
}

// viewsOf folds the watch's versions into one view per Allocation, and returns
// the UIDs in the order each first appeared.
func viewsOf(obs []stack.AllocObservation) (map[types.UID]*allocView, []types.UID) {
	views := map[types.UID]*allocView{}
	var order []types.UID
	for _, a := range obs {
		v := views[a.UID]
		if v == nil {
			v = &allocView{name: a.Name}
			views[a.UID] = v
			order = append(order, a.UID)
		}
		if n := len(v.phases); n == 0 || v.phases[n-1] != a.Phase {
			v.phases = append(v.phases, a.Phase)
		}
		v.deleted = v.deleted || a.Deleted
	}
	return views, order
}

// checkAllocationEvents holds every Allocation of the run to its end, from the
// watch's versions (read), deleted ones included: none was ever seen Failed,
// Lost or Refused, and each of atZero was last seen Ended or deleted. The
// caller has first made the watch catch up with the API (syncWatch), whose
// result is res: waitAllEnded ends on an API LIST, which leads the watch, so a
// read straight after it can precede an event that is only late. What a
// catch-up found lost, this one or an earlier one, it has reported, with its own
// message, and is not judged again here; an Allocation last seen Running after a
// catch-up that found the watch current stayed Running.
func checkAllocationEvents(t stack.T, read func() []stack.AllocObservation, res stack.CatchUpResult, atZero map[types.UID]string, failedOK map[types.UID]bool) {
	t.Helper()
	obs := read()
	views, order := viewsOf(obs)
	for _, a := range obs {
		switch a.Phase {
		case v1alpha1.AllocationFailed, v1alpha1.AllocationLost, v1alpha1.AllocationRefused:
			if !failedOK[a.UID] {
				t.Errorf("Allocation %s was seen %s at %s", a.Name, a.Phase, a.At.Format(time.RFC3339Nano))
			}
		}
	}
	for uid, name := range atZero {
		s := views[uid]
		switch {
		case res.Lost(uid):
		case s == nil && res.Unknown != "":
			t.Errorf("step (g): the watch never saw Allocation %s, and was not shown to have caught up with the API (%s)", name, res.Unknown)
		case s == nil:
			t.Errorf("step (g): the watch never saw Allocation %s", name)
		case !s.delivered() && res.Unknown != "":
			t.Errorf("step (g): Allocation %s was last seen %s, neither Ended nor deleted; the watch was not shown to have caught up with the API (%s)", name, s.phases[len(s.phases)-1], res.Unknown)
		case !s.delivered():
			t.Errorf("step (g): Allocation %s was last seen %s, neither Ended nor deleted", name, s.phases[len(s.phases)-1])
		}
	}
	for _, uid := range order {
		t.Logf("Allocation %s: phases %v, deleted %v", views[uid].name, views[uid].phases, views[uid].deleted)
	}
}
