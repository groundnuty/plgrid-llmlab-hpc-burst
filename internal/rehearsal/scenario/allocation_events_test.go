package scenario_test

// Step (g)'s read of the Allocation watch (issue #281).

import (
	"fmt"
	"sort"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// allocAPI is what the API says of one Allocation of the run.
type allocAPI struct {
	exists bool // the API still lists the object, under the same UID
	phase  v1alpha1.AllocationPhase
	err    error // the API could not say
}

// allocView is what the watch delivered of one Allocation.
type allocView struct {
	name    string
	phases  []v1alpha1.AllocationPhase // each phase in turn, repeats dropped
	deleted bool
	// endedAt is when the watch first delivered the deletion or an Ended
	// version; zero until it has.
	endedAt time.Time
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
		if v.endedAt.IsZero() && (a.Deleted || a.Phase == v1alpha1.AllocationEnded) {
			v.endedAt = a.At
		}
	}
	return views, order
}

// undelivered returns those of atZero whose end or deletion the views do not
// show, in the order of their names.
func undelivered(views map[types.UID]*allocView, atZero map[types.UID]string) []types.UID {
	var out []types.UID
	for uid := range atZero {
		if !views[uid].delivered() {
			out = append(out, uid)
		}
	}
	sort.Slice(out, func(i, j int) bool { return atZero[out[i]] < atZero[out[j]] })
	return out
}

// allocDeliveryWait is how long step (g) waits for the watch to deliver an
// Allocation's end or deletion once the API lists it Ended or gone. waitAllEnded
// ends on an API LIST, which leads the watch: the server sends a deletion to its
// watchers after a LIST has stopped showing the object. On a healthy server the
// difference is microseconds to milliseconds, so this is far more than late
// needs, and an event that has not come in this long is lost.
const allocDeliveryWait = 30 * time.Second

// allocDeliveryPoll is how often the wait reads the watch.
const allocDeliveryPoll = 50 * time.Millisecond

// checkAllocationEvents holds every Allocation of the run to its end, from the
// watch's versions (read), deleted ones included: none was ever seen Failed,
// Lost or Refused, and each of atZero was last seen Ended or deleted. The
// caller has seen the API list each of atZero Ended or gone, so first the check
// waits up to wait for the watch to deliver that end or deletion (issue #281):
// the watch is a separate stream, and a read of it straight after a LIST can
// precede an event that is only late. One that does not come in time is
// reported as lost when the API says the Allocation is Ended or gone, and as an
// Allocation that stayed Running when it says so. Everything is judged from the
// versions as of the end of the wait. api answers only for those left.
func checkAllocationEvents(t stack.T, read func() []stack.AllocObservation, api func(name string, uid types.UID) allocAPI,
	atZero map[types.UID]string, failedOK map[types.UID]bool, wait time.Duration) {
	t.Helper()
	start := time.Now()
	deadline, _ := stack.BoundedDeadline(t, wait)
	obs := read()
	views, order := viewsOf(obs)
	behind := undelivered(views, atZero) // the watch trailed the API on these
	for left := behind; len(left) > 0 && time.Now().Before(deadline); {
		time.Sleep(allocDeliveryPoll)
		obs = read()
		views, order = viewsOf(obs)
		left = undelivered(views, atZero)
	}
	waited := time.Since(start)
	if len(behind) == 0 {
		t.Logf("step (g): the watch had already delivered the end or deletion of all %d Allocations the API listed Ended or gone", len(atZero))
	}
	for _, uid := range behind {
		if v := views[uid]; v.delivered() {
			t.Logf("step (g): the watch delivered Allocation %s's end or deletion %s after the API listed it Ended or gone", v.name, v.endedAt.Sub(start).Round(time.Microsecond))
		}
	}
	for _, a := range obs {
		switch a.Phase {
		case v1alpha1.AllocationFailed, v1alpha1.AllocationLost, v1alpha1.AllocationRefused:
			if !failedOK[a.UID] {
				t.Errorf("Allocation %s was seen %s at %s", a.Name, a.Phase, a.At.Format(time.RFC3339Nano))
			}
		}
	}
	for _, uid := range undelivered(views, atZero) {
		name, s := atZero[uid], views[uid]
		if s == nil {
			t.Errorf("step (g): the watch never saw Allocation %s", name)
			continue
		}
		t.Errorf("%s", undeliveredProblem(name, s.phases[len(s.phases)-1], api(name, uid), waited))
	}
	for _, uid := range order {
		t.Logf("Allocation %s: phases %v, deleted %v", views[uid].name, views[uid].phases, views[uid].deleted)
	}
}

// undeliveredProblem is what step (g) reports of an Allocation whose end or
// deletion the watch had not delivered after waited: seen is the last phase it
// delivered, and now what the API says. The API tells the cases apart, the
// watch cannot.
func undeliveredProblem(name string, seen v1alpha1.AllocationPhase, now allocAPI, waited time.Duration) string {
	waited = waited.Round(time.Millisecond)
	switch {
	case now.err != nil:
		return fmt.Sprintf("step (g): Allocation %s: the watch delivered neither its end nor its deletion in %s (the last version it delivered is %s), and the API could not say what became of it (%v): whether the event was lost is not known",
			name, waited, seen, now.err)
	case !now.exists:
		return fmt.Sprintf("step (g): Allocation %s: the API says it is deleted, and the watch delivered neither its end nor its deletion in %s (the last version it delivered is %s): the event was lost",
			name, waited, seen)
	case now.phase == v1alpha1.AllocationEnded:
		return fmt.Sprintf("step (g): Allocation %s: the API says it is Ended, and the watch delivered neither its end nor its deletion in %s (the last version it delivered is %s): the event was lost",
			name, waited, seen)
	}
	return fmt.Sprintf("step (g): Allocation %s was last seen %s, neither Ended nor deleted", name, seen)
}

// apiAllocation is what the API says now of the Allocation with this name and
// UID.
func (r *rig) apiAllocation(name string, uid types.UID) allocAPI {
	var a v1alpha1.Allocation
	ctx, cancel := stack.CallCtx(r.t)
	defer cancel()
	err := r.c.Get(ctx, client.ObjectKey{Name: name, Namespace: stack.Namespace}, &a)
	switch {
	case apierrors.IsNotFound(err), err == nil && a.UID != uid: // gone, or another object under the name
		return allocAPI{}
	case err != nil:
		return allocAPI{err: err}
	}
	return allocAPI{exists: true, phase: a.Status.AtProvider.Phase}
}
