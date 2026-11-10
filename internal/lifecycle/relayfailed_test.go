package lifecycle

// Issue #304, the lifecycle controller's half: an Allocation marked
// RelayEndedForGood, as the Provider's controller marks it on the observation
// after one of its host-dialed relays ends for good, is released in the next
// pass. A ready Instance on it ends with that reason and a failing event, the
// Allocation is deleted (its finalizer cancels the job), and the planner
// replaces the Instance by its usual rules; an empty Allocation that its
// retention policy keeps is released too, since nothing placed on it could
// be reached.
//
// The rig's Provider is the provider fake, so the mark is written here as the
// Provider's controller writes it (internal/providerctl/relayfailed.go). The
// end itself, through the real relay owner, and this controller's pass over
// it are tested together in internal/providerctl (relayfailed_release_test.go),
// whose Instance is pending; the one here is ready, the case the issue names.
// The condition's names are the issue's, spelt out as literals.

import (
	"strings"
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

const (
	relayCond   = "RelayEndedForGood"
	relayReason = "RelayFailed"
	relayMsg    = "the serving relay ended for good: srun refused the step: step limit reached for this job"
)

// relayMark sets RelayEndedForGood True on the Allocation, with reason
// RelayFailed and the RelayFailed event's message, as the Provider's
// controller does.
func relayMark(t *testing.T, rg *rig, name string) {
	t.Helper()
	a, ok := rg.allocation(name)
	if !ok {
		t.Fatalf("Allocation %s is gone", name)
	}
	a.SetConditions(xpv2.Condition{Type: relayCond, Status: corev1.ConditionTrue, Reason: relayReason,
		Message: relayMsg, LastTransitionTime: metav1.NewTime(rg.clock.Now())})
	if err := rg.c.Status().Update(rg.ctx, a); err != nil {
		t.Fatal(err)
	}
}

// A ready Instance whose Allocation is marked ends in the next pass with
// RelayEndedForGood and one failing event naming the cause, its Allocation is
// released in the same pass, and one replacement with a job of its own serves
// in its place.
func TestARelayEndedForGoodEndsAReadyInstanceAndIsReplaced(t *testing.T) {
	rg := newRig(t, withHostDialed())
	old, served := allocServe(rg)
	rg.run(5*time.Minute, 10*time.Second)
	if in := rg.instance(served.Name); in.Status.Phase != v1alpha1.InstanceReady || allocReleased(rg, old.Name) {
		t.Fatalf("before the mark the Instance is %s and the Allocation released %v; want ready and kept", in.Status.Phase, allocReleased(rg, old.Name))
	}
	relayMark(t, rg, old.Name)
	rg.clock.Advance(time.Second)
	rg.Step()
	var in v1alpha1.Instance
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Namespace: served.Namespace, Name: served.Name}, &in); err != nil {
		t.Fatal(err)
	}
	if in.Status.Phase != v1alpha1.InstanceEnded || in.Status.EndReason != relayCond {
		t.Errorf("after the next pass the Instance is %s with end reason %q; want ended with %s", in.Status.Phase, in.Status.EndReason, relayCond)
	}
	if !allocReleased(rg, old.Name) {
		t.Error("the next pass did not release the marked Allocation")
	}
	ev := rg.rec.Find("failing", relayCond)
	if len(ev) != 1 || ev[0].Name != served.Name || !strings.Contains(ev[0].Message, "step limit") || !strings.Contains(ev[0].Message, old.Name) {
		t.Errorf("failing/%s events %+v: want one, for %s, naming %s and the cause", relayCond, ev, served.Name, old.Name)
	}
	allocReplaced(t, rg, &old)
}

// An empty Allocation that the default policy keeps (Keep, with no end time:
// until its hard end) is released in the next pass once marked; unmarked, it
// is kept through the same passes.
func TestARelayEndedForGoodReleasesAKeptEmptyAllocation(t *testing.T) {
	rg := newRig(t, withHostDialed())
	a, _ := retentionEmpty(t, rg, nil)
	rg.run(10*time.Minute, time.Minute)
	if allocReleased(rg, a.Name) {
		t.Fatal("the unmarked empty Allocation was released; Keep must keep it")
	}
	relayMark(t, rg, a.Name)
	rg.clock.Advance(time.Second)
	rg.Step()
	if !allocReleased(rg, a.Name) {
		t.Error("the next pass did not release the marked empty Allocation")
	}
}
