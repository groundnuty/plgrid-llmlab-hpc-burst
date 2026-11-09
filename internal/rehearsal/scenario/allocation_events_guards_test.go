package scenario_test

// Step (g)'s read of the Allocation watch (issue #281), checked without the
// rehearsal: these run in an ordinary go test, with no HPCBURST_REHEARSAL. The
// watch is a controlled one: a function that returns the versions "delivered so
// far", which a test makes grow between two reads, so that the order of the API
// and the watch is the test's, not the machine's.

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
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

// watchAfter is a watch that has delivered `before` at its first read and
// `after` from its second read on, and counts its reads.
func watchAfter(before, after []stack.AllocObservation) (read func() []stack.AllocObservation, reads *atomic.Int32) {
	reads = new(atomic.Int32)
	return func() []stack.AllocObservation {
		if reads.Add(1) == 1 {
			return before
		}
		return after
	}, reads
}

// apiNever is an API that must not be asked: an event the watch delivers late
// is not a question for it.
func apiNever(t *testing.T) func(string, types.UID) allocAPI {
	return func(name string, uid types.UID) allocAPI {
		t.Errorf("the API was asked about %s, which the watch delivered", name)
		return allocAPI{}
	}
}

// apiSays is an API that answers as given.
func apiSays(a allocAPI) func(string, types.UID) allocAPI {
	return func(string, types.UID) allocAPI { return a }
}

// The API no longer lists the Allocation, and the watch delivers its deletion
// only after the first read: the check waits for it and passes. Before issue
// #281 it read once and failed with "was last seen Running, neither Ended nor
// deleted". The wait's bound is the full WaitDeadline, which a delivery ends at
// once, so a loaded machine cannot fail it.
func TestAllocationEventsWaitForALateDelivery(t *testing.T) {
	const uid = types.UID("uid-a")
	running := version(uid, "rehearsal-a", v1alpha1.AllocationRunning, false)
	for name, c := range map[string]struct {
		before, after []stack.AllocObservation
	}{
		"the deletion": {
			before: []stack.AllocObservation{running},
			after:  []stack.AllocObservation{running, version(uid, "rehearsal-a", v1alpha1.AllocationRunning, true)},
		},
		"the end": {
			before: []stack.AllocObservation{running},
			after:  []stack.AllocObservation{running, version(uid, "rehearsal-a", v1alpha1.AllocationEnded, false)},
		},
	} {
		t.Run(name, func(t *testing.T) {
			read, reads := watchAfter(c.before, c.after)
			rt := &recordT{T: t}
			checkAllocationEvents(rt, read, apiNever(t), map[types.UID]string{uid: "rehearsal-a"}, nil, stack.WaitDeadline)
			if len(rt.errs) > 0 {
				t.Errorf("a watch that delivers %s after the API stopped listing the Allocation: reported %q, want nothing", name, rt.errs)
			}
			if got := reads.Load(); got < 2 {
				t.Errorf("the watch was read %d times: the check did not wait for the delivery", got)
			}
		})
	}
}

// A delivery that never comes is a lost event, with a message of its own that
// says what the API says, and not the message of an Allocation that stayed
// Running. An API that cannot answer leaves the question open.
func TestAllocationEventsReportALostDelivery(t *testing.T) {
	const uid = types.UID("uid-a")
	running := []stack.AllocObservation{version(uid, "rehearsal-a", v1alpha1.AllocationRunning, false)}
	for name, c := range map[string]struct {
		api  allocAPI
		want []string
	}{
		"deleted in the API": {allocAPI{}, []string{"Allocation rehearsal-a", "the API says it is deleted", "the event was lost"}},
		"Ended in the API":   {allocAPI{exists: true, phase: v1alpha1.AllocationEnded}, []string{"Allocation rehearsal-a", "the API says it is Ended", "the event was lost"}},
		"the API cannot say": {allocAPI{err: errors.New("connection refused")}, []string{"Allocation rehearsal-a", "connection refused", "not known"}},
	} {
		t.Run(name, func(t *testing.T) {
			read, _ := watchAfter(running, running)
			rt := &recordT{T: t}
			checkAllocationEvents(rt, read, apiSays(c.api), map[types.UID]string{uid: "rehearsal-a"}, nil, 300*time.Millisecond)
			if len(rt.errs) != 1 {
				t.Fatalf("%d problems %q, want exactly one", len(rt.errs), rt.errs)
			}
			for _, w := range c.want {
				if !strings.Contains(rt.errs[0], w) {
					t.Errorf("the problem %q does not say %q", rt.errs[0], w)
				}
			}
			if strings.Contains(rt.errs[0], "neither Ended nor deleted") {
				t.Errorf("the problem %q is the message of an Allocation that stayed Running", rt.errs[0])
			}
		})
	}
}

// An Allocation the API still lists as Running fails with the message it has
// always had, after the wait.
func TestAllocationEventsReportAnAllocationThatStayedRunning(t *testing.T) {
	const uid = types.UID("uid-a")
	running := []stack.AllocObservation{version(uid, "rehearsal-a", v1alpha1.AllocationRunning, false)}
	read, _ := watchAfter(running, running)
	rt := &recordT{T: t}
	api := apiSays(allocAPI{exists: true, phase: v1alpha1.AllocationRunning})
	checkAllocationEvents(rt, read, api, map[types.UID]string{uid: "rehearsal-a"}, nil, 300*time.Millisecond)
	want := "step (g): Allocation rehearsal-a was last seen Running, neither Ended nor deleted"
	if len(rt.errs) != 1 || rt.errs[0] != want {
		t.Errorf("reported %q, want exactly [%q]", rt.errs, want)
	}
}

// An Allocation the watch never saw has a message of its own, as before.
func TestAllocationEventsReportAnAllocationNeverSeen(t *testing.T) {
	read, _ := watchAfter(nil, nil)
	rt := &recordT{T: t}
	checkAllocationEvents(rt, read, apiSays(allocAPI{}), map[types.UID]string{"uid-a": "rehearsal-a"}, nil, 300*time.Millisecond)
	want := "step (g): the watch never saw Allocation rehearsal-a"
	if len(rt.errs) != 1 || rt.errs[0] != want {
		t.Errorf("reported %q, want exactly [%q]", rt.errs, want)
	}
}

// The versions are judged as of the end of the wait: a Failed version of another
// Allocation that arrives while the check waits is still reported, and a
// requeued Allocation (failedOK) is not.
func TestAllocationEventsJudgeTheWatchAfterTheWait(t *testing.T) {
	const a, b, c = types.UID("uid-a"), types.UID("uid-b"), types.UID("uid-c")
	runningA := version(a, "rehearsal-a", v1alpha1.AllocationRunning, false)
	before := []stack.AllocObservation{runningA}
	after := []stack.AllocObservation{runningA, version(a, "rehearsal-a", v1alpha1.AllocationRunning, true),
		version(b, "rehearsal-b", v1alpha1.AllocationFailed, false), version(c, "rehearsal-c", v1alpha1.AllocationFailed, false)}
	read, _ := watchAfter(before, after)
	rt := &recordT{T: t}
	checkAllocationEvents(rt, read, apiNever(t), map[types.UID]string{a: "rehearsal-a"}, map[types.UID]bool{c: true}, stack.WaitDeadline)
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
