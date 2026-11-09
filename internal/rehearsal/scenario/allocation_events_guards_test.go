package scenario_test

// Step (g)'s read of the Allocation watch (issue #281), checked without the
// rehearsal: these run in an ordinary go test, with no HPCBURST_REHEARSAL. The
// wait for a late event is the stack's catch-up barrier, tested there
// (stack/catchup_test.go); these pin what step (g) says of an Allocation once
// the barrier has run.

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// recordT is a test that keeps what a check reports instead of failing.
type recordT struct {
	stack.T
	errs, logs []string
}

func (r *recordT) Errorf(format string, args ...any) {
	r.errs = append(r.errs, fmt.Sprintf(format, args...))
}
func (r *recordT) Logf(format string, args ...any) {
	r.logs = append(r.logs, fmt.Sprintf(format, args...))
}

func version(uid types.UID, name string, phase v1alpha1.AllocationPhase, deleted bool) stack.AllocObservation {
	return stack.AllocObservation{At: time.Now(), Name: name, UID: uid, Phase: phase, Deleted: deleted}
}

func reads(obs ...stack.AllocObservation) func() []stack.AllocObservation {
	return func() []stack.AllocObservation { return obs }
}

const uidA = types.UID("uid-a")

var oneAtZero = map[types.UID]string{uidA: "rehearsal-a"}

// An Allocation whose deletion or end the watch delivered passes.
func TestAllocationEventsPassWhenDelivered(t *testing.T) {
	running := version(uidA, "rehearsal-a", v1alpha1.AllocationRunning, false)
	for name, obs := range map[string][]stack.AllocObservation{
		"deleted": {running, version(uidA, "rehearsal-a", v1alpha1.AllocationRunning, true)},
		"ended":   {running, version(uidA, "rehearsal-a", v1alpha1.AllocationEnded, false)},
	} {
		rt := &recordT{T: t}
		checkAllocationEvents(rt, reads(obs...), stack.CatchUpResult{}, oneAtZero, nil)
		if len(rt.errs) > 0 {
			t.Errorf("%s: reported %q, want nothing", name, rt.errs)
		}
	}
}

// An Allocation last seen Running after a catch-up that found the watch current
// stayed Running, with the message it has always had.
func TestAllocationEventsReportAnAllocationThatStayedRunning(t *testing.T) {
	rt := &recordT{T: t}
	checkAllocationEvents(rt, reads(version(uidA, "rehearsal-a", v1alpha1.AllocationRunning, false)), stack.CatchUpResult{}, oneAtZero, nil)
	want := "step (g): Allocation rehearsal-a was last seen Running, neither Ended nor deleted"
	if len(rt.errs) != 1 || rt.errs[0] != want {
		t.Errorf("reported %q, want exactly [%q]", rt.errs, want)
	}
}

// A catch-up that found the event lost reports it itself, with a message of its
// own that is not the one of an Allocation that stayed Running; the check does
// not report the Allocation a second time.
func TestAllocationEventsLeaveALostEventToTheCatchUp(t *testing.T) {
	res := stack.CatchUpResult{Waited: 30 * time.Second, Missing: []stack.Behind{{Kind: stack.KindAllocations, Name: "rehearsal-a", UID: uidA, Delivered: "150"}}}
	rt := &recordT{T: t}
	reportCatchUp(rt, "step (g): the Allocations", res)
	if len(rt.errs) != 1 || !strings.HasPrefix(rt.errs[0], "step (g): the Allocations: ") || !strings.Contains(rt.errs[0], "Allocation rehearsal-a: gone from the API") ||
		!strings.HasSuffix(rt.errs[0], ": the event was lost") || strings.Contains(rt.errs[0], "neither Ended nor deleted") {
		t.Errorf("the catch-up reported %q; want a lost event of its own", rt.errs)
	}
	rt = &recordT{T: t}
	checkAllocationEvents(rt, reads(version(uidA, "rehearsal-a", v1alpha1.AllocationRunning, false)), res, oneAtZero, nil)
	if len(rt.errs) > 0 {
		t.Errorf("the check reported %q again, want nothing", rt.errs)
	}
	rt = &recordT{T: t}
	reportCatchUp(rt, "x", stack.CatchUpResult{})
	if len(rt.errs) > 0 {
		t.Errorf("a catch-up that found the watch current reported %q", rt.errs)
	}
}

// A catch-up that could not tell leaves the Running message with that said, and
// is itself reported as not known.
func TestAllocationEventsWhenTheCatchUpCouldNotTell(t *testing.T) {
	res := stack.CatchUpResult{Unknown: "the API could not be listed: connection refused"}
	rt := &recordT{T: t}
	reportCatchUp(rt, "step (g)", res)
	if len(rt.errs) != 1 || !strings.Contains(rt.errs[0], "is not known") || strings.Contains(rt.errs[0], "the event was lost") {
		t.Errorf("the catch-up reported %q; want not known", rt.errs)
	}
	rt = &recordT{T: t}
	checkAllocationEvents(rt, reads(version(uidA, "rehearsal-a", v1alpha1.AllocationRunning, false)), res, oneAtZero, nil)
	if len(rt.errs) != 1 || !strings.Contains(rt.errs[0], "neither Ended nor deleted") || !strings.Contains(rt.errs[0], "not shown to have caught up") ||
		!strings.Contains(rt.errs[0], "connection refused") {
		t.Errorf("the check reported %q; want the Running message with the catch-up's doubt", rt.errs)
	}
}

// An event an earlier catch-up found lost and reported (part 3's requeue, say) is
// still lost at step (g), whose own catch-up reports nothing new: the Allocation
// is not called one that stayed Running, nor one the watch never saw.
func TestAllocationEventsDoNotMislabelAnEarlierLoss(t *testing.T) {
	res := stack.CatchUpResult{EarlierLost: []stack.Behind{{Kind: stack.KindAllocations, Name: "rehearsal-a", UID: uidA, Delivered: "150"}}}
	rt := &recordT{T: t}
	reportCatchUp(rt, "step (g): the Allocations", res)
	if len(rt.errs) != 0 {
		t.Errorf("the catch-up reported %q again, want nothing new", rt.errs)
	}
	for name, obs := range map[string][]stack.AllocObservation{
		"last seen Running": {version(uidA, "rehearsal-a", v1alpha1.AllocationRunning, false)},
		"never seen":        nil,
	} {
		rt = &recordT{T: t}
		checkAllocationEvents(rt, reads(obs...), res, oneAtZero, nil)
		if len(rt.errs) != 0 {
			t.Errorf("%s: the check reported %q, want nothing: the earlier catch-up reported it lost", name, rt.errs)
		}
	}
}

// An Allocation the watch never saw, after a catch-up that could not tell, is
// not said flatly to have been missed: the doubt goes with it (PR #292's B1).
func TestAllocationEventsNeverSeenWhenTheCatchUpCouldNotTell(t *testing.T) {
	res := stack.CatchUpResult{Unknown: "the Allocation watch was reset"}
	rt := &recordT{T: t}
	checkAllocationEvents(rt, reads(), res, oneAtZero, nil)
	if len(rt.errs) != 1 || !strings.Contains(rt.errs[0], "never saw Allocation rehearsal-a") || !strings.Contains(rt.errs[0], "was reset") {
		t.Errorf("reported %q; want the Allocation with the catch-up's doubt", rt.errs)
	}
}

// An Allocation the watch never saw has a message of its own.
func TestAllocationEventsReportAnAllocationNeverSeen(t *testing.T) {
	rt := &recordT{T: t}
	checkAllocationEvents(rt, reads(), stack.CatchUpResult{}, oneAtZero, nil)
	want := "step (g): the watch never saw Allocation rehearsal-a"
	if len(rt.errs) != 1 || rt.errs[0] != want {
		t.Errorf("reported %q, want exactly [%q]", rt.errs, want)
	}
}

// A Failed, Lost or Refused version of any Allocation is reported, except those
// a step failed on purpose (failedOK), and every Allocation's phases are logged.
func TestAllocationEventsScanEveryVersion(t *testing.T) {
	const b, c = types.UID("uid-b"), types.UID("uid-c")
	obs := []stack.AllocObservation{
		version(uidA, "rehearsal-a", v1alpha1.AllocationRunning, false), version(uidA, "rehearsal-a", v1alpha1.AllocationRunning, true),
		version(b, "rehearsal-b", v1alpha1.AllocationFailed, false), version(c, "rehearsal-c", v1alpha1.AllocationFailed, false),
	}
	rt := &recordT{T: t}
	checkAllocationEvents(rt, reads(obs...), stack.CatchUpResult{}, oneAtZero, map[types.UID]bool{c: true})
	if len(rt.errs) != 1 || !strings.HasPrefix(rt.errs[0], "Allocation rehearsal-b was seen Failed at ") {
		t.Errorf("reported %q, want exactly the Failed version of rehearsal-b", rt.errs)
	}
	joined := strings.Join(rt.logs, "\n")
	for _, want := range []string{"Allocation rehearsal-a: phases [Running], deleted true", "Allocation rehearsal-b: phases [Failed], deleted false"} {
		if !strings.Contains(joined, want) {
			t.Errorf("the log does not say %q:\n%s", want, joined)
		}
	}
}
