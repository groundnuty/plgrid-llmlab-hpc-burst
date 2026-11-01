package lifecycle

// A failed no-overlap check (the controller lists the Allocations of every
// namespace to count a Provider's submit slots) must not hold anything else
// back: not the drain at the drain start, not retention, not the status write
// (the review of PR #172, Opus N2). The tests fail that one list, and only the
// controller's pass sees the failure.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

var errListDown = errors.New("the Allocation list is down")

// listFailer fails the list of every Allocation of the cluster (no namespace,
// no selector), which is the submit-slot count's, while it is on.
type listFailer struct {
	mu sync.Mutex
	on bool
}

func (l *listFailer) set(on bool) { l.mu.Lock(); l.on = on; l.mu.Unlock() }

func (l *listFailer) funcs() interceptor.Funcs {
	return interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		l.mu.Lock()
		on := l.on
		l.mu.Unlock()
		if _, ok := list.(*v1alpha1.AllocationList); ok && on && len(opts) == 0 {
			return errListDown
		}
		return c.List(ctx, list, opts...)
	}}
}

// stepArmed is a pass of the controller with the list failing, after the
// provider's pass and the agents'; it returns the pass's error.
func (l *listFailer) stepArmed(rg *rig) error {
	rg.t.Helper()
	if err := rg.prov.Sync(rg.ctx); err != nil {
		rg.t.Fatalf("provider sync: %v", err)
	}
	rg.syncAgents()
	l.set(true)
	defer l.set(false)
	_, err := rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: hoMD}})
	return err
}

// TestNoOverlapMarkErrorDoesNotHoldBackTheDrain: on the one-slot path, the
// pass at the drain start, 09:50:20, cannot count the submit slots. The
// primary drains all the same, the pass reports the error, and the next pass
// (the list back) records the mode and shows NoOverlapSlot. Before, the drain
// start waited for the list and slipped to the retry's backoff.
func TestNoOverlapMarkErrorDoesNotHoldBackTheDrain(t *testing.T) {
	l := &listFailer{}
	rg := noOverlapRig(t, oneSlotOpts(withInterceptors(l.funcs()), withBatchConfig(func(c *pfake.Config) { c.MaxSubmitPerUser, c.MaxGPUsPerUser = 1, 1 }))...)
	p := noOverlapStart(rg)
	hoStepTo(rg, hoT(9, 50, 10), 5*time.Minute, nil)
	if got := rg.instance(p.Name); got.Status.Phase != v1alpha1.InstanceReady {
		t.Fatalf("at 09:50:10 the primary is %s, want ready", got.Status.Phase)
	}

	rg.clock.Advance(10 * time.Second) // 09:50:20, the drain start
	if err := l.stepArmed(rg); !errors.Is(err, errListDown) {
		t.Fatalf("the pass at the drain start returned %v, want the list's error", err)
	}
	got := rg.instance(p.Name)
	if got.Status.Phase != v1alpha1.InstanceDraining {
		t.Fatalf("at 09:50:20, with the check failing, the primary is %s, want draining", got.Status.Phase)
	}
	if got.Annotations[annotationNoOverlap] != "" {
		t.Fatal("the primary is marked although the check failed")
	}

	rg.clock.Advance(10 * time.Second)
	rg.Step()
	if got := rg.instance(p.Name); got.Annotations[annotationNoOverlap] == "" {
		t.Fatal("the next pass did not record the mode")
	}
	if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Reason != v1alpha1.ReasonNoOverlapSlot {
		t.Fatalf("at 09:50:30 Degraded = %s/%s %q, want NoOverlapSlot", c.Status, c.Reason, c.Message)
	}
}

// TestNoOverlapCheckErrorDoesNotSkipRetentionOrStatus: from the replacement's
// lateness deadline, 09:49:30, every pass checks the mode for the primary, and
// the check fails (the list is down from 09:49:20 to 09:50:00). Retention still
// releases the idle Allocation whose idle limit ends in that window, and every
// pass still writes the status, though each returns the error. Before, the error
// left the pass before both.
//
// The deployment is on a Provider with maxSubmitPerUser 4, so the replacement
// runs beside the primary (an ordinary handover). At 09:44 the deployment is
// scaled up to two Instances and back to one while the second still starts,
// which leaves a warm Allocation that is released 5 min after the second
// Instance ends.
func TestNoOverlapCheckErrorDoesNotSkipRetentionOrStatus(t *testing.T) {
	l := &listFailer{}
	qos := []v1alpha1.QOSProfile{{Name: "example-s4", MaxSubmitPerUser: i32(4)}}
	rg := newRig(t, append(qosOpts(qos, "", "example-s4"), withMaxTime(4*time.Hour), withInterceptors(l.funcs()))...)
	rg.create(releaseWhenIdle5m(mdSpec(hoMD)))
	for range 7 {
		rg.clock.Advance(10 * time.Second)
		rg.Step()
	}
	p := rg.instances(hoMD)[0]
	hoStepTo(rg, hoT(9, 44, 0), 5*time.Minute, nil)
	rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 2 })
	var warm string
	rg.until(5*time.Minute, 10*time.Second, "the second Allocation started", func() bool {
		for _, a := range rg.allocations(hoMD) {
			if a.Status.AtProvider.StartTime != nil && a.Status.AtProvider.StartTime.After(hoT(9, 43, 0)) {
				warm = a.Name
				return true
			}
		}
		return false
	})
	rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1 })
	var victim *v1alpha1.Instance
	rg.until(5*time.Minute, 10*time.Second, "the second Instance ended", func() bool {
		for _, in := range rg.instances(hoMD) {
			if in.Spec.AllocationRef.Name == warm && in.Status.Phase == v1alpha1.InstanceEnded {
				victim = &in
				return true
			}
		}
		return false
	})
	// The premise: the warm Allocation's idle limit (5 min after the second
	// Instance's end) falls inside the window in which the list is down, after
	// the lateness deadline.
	release := victim.Status.EndTime.Add(5 * time.Minute)
	if release.Before(hoT(9, 49, 30)) || !release.Before(hoT(9, 50, 0)) {
		t.Fatalf("the warm Allocation's idle limit ends at %s, want it within 09:49:30 to 09:50:00", release.Format(time.TimeOnly))
	}
	if allocReleased(rg, warm) {
		t.Fatalf("the warm Allocation %s was released before the window", warm)
	}
	hoStepTo(rg, hoT(9, 49, 20), 10*time.Second, nil)

	errored, releasedWhileFailing := 0, false
	for rg.clock.Now().Before(hoT(9, 50, 0)) {
		rg.clock.Advance(10 * time.Second)
		before := allocReleased(rg, warm)
		err := l.stepArmed(rg)
		if rg.clock.Now().Before(hoT(9, 49, 30)) {
			if err != nil {
				t.Fatalf("at %s, before the lateness deadline, the pass returned %v", rg.clock.Now().Format(time.TimeOnly), err)
			}
			continue
		}
		if !errors.Is(err, errListDown) {
			t.Fatalf("at %s the pass returned %v, want the list's error", rg.clock.Now().Format(time.TimeOnly), err)
		}
		errored++
		if got := rg.md(hoMD).Status.LastReconcileTime; got == nil || !got.Time.Equal(rg.clock.Now()) {
			t.Fatalf("at %s the status was last written at %v: a failed check skipped the status write", rg.clock.Now().Format(time.TimeOnly), got)
		}
		if !before && allocReleased(rg, warm) {
			releasedWhileFailing = true
		}
	}
	if errored == 0 {
		t.Fatal("no pass failed; the test proves nothing")
	}
	if !releasedWhileFailing {
		t.Fatal("the warm Allocation was not released in a pass whose check failed: a failed check skipped retention")
	}
	if got := rg.instance(p.Name); got.Status.Phase != v1alpha1.InstanceReady {
		t.Fatalf("the primary is %s, want ready until its drain start", got.Status.Phase)
	}
}
