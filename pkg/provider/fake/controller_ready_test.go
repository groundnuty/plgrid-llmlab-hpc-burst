package fake

import (
	"errors"
	"strings"
	"testing"
	"time"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The tests for the Provider's Ready condition: it rests on a real read of the
// platform (HBEP-4, Operational readiness 1; docs/development.md, Provider
// readiness). Expected values are written out by hand from that rule: Ready
// True after a pass with no failed read, False once reads have failed and the
// last success is more than v1alpha1.ProviderReadyStaleness (5m) old.

func (e *env) ready() *metav1.Condition {
	e.t.Helper()
	return apimeta.FindStatusCondition(e.provider().Status.Conditions, v1alpha1.ConditionProviderReady)
}

func (e *env) wantReady(status metav1.ConditionStatus, reason string) *metav1.Condition {
	e.t.Helper()
	c := e.ready()
	if c == nil || c.Status != status || c.Reason != reason {
		e.t.Fatalf("at %s Ready = %+v, want %s with reason %s", e.clock.Now().Format("15:04:05"), c, status, reason)
	}
	return c
}

func TestIdleHealthyProviderPollsAndIsReady(t *testing.T) {
	e := newEnv(t) // no Allocation: the pass reads nothing by itself
	e.sync()       // 12:00:00
	if n := e.batch.Count("squeue-me"); n != 1 {
		t.Fatalf("squeue-me logged %d times, want 1: an idle pass polls the platform", n)
	}
	if n := e.batch.Count("squeue"); n != 0 {
		t.Fatalf("squeue logged %d times, want 0: an idle pass observes no job", n)
	}
	e.wantReady(metav1.ConditionTrue, "Polled")
	prov := e.provider()
	wantMT(t, "lastSuccessfulStatusRead", prov.Status.LastSuccessfulStatusRead, at(12, 0, 0))
	if !v1alpha1.ProviderReady(prov, at(12, 0, 0)) {
		t.Fatal("v1alpha1.ProviderReady = false for a Provider that just polled")
	}
	e.syncAt(at(12, 1, 0))
	if n := e.batch.Count("squeue-me"); n != 2 {
		t.Fatalf("squeue-me logged %d times after two passes, want 2", n)
	}
	wantMT(t, "lastSuccessfulStatusRead", e.provider().Status.LastSuccessfulStatusRead, at(12, 1, 0))
}

func TestFreshIdleProviderUnderFailingReadsIsNeverReady(t *testing.T) {
	e := newEnv(t)
	e.batch.FailReads(errors.New("example transport failure"))
	e.sync() // 12:00:00: the poll fails, and there never was a success
	c := e.wantReady(metav1.ConditionFalse, "ReadsFailing")
	if !strings.Contains(c.Message, "no successful read") {
		t.Fatalf("message = %q, want it to say there was no successful read", c.Message)
	}
	prov := e.provider()
	wantNilMT(t, "lastSuccessfulStatusRead", prov.Status.LastSuccessfulStatusRead)
	if v1alpha1.ProviderReady(prov, e.clock.Now()) {
		t.Fatal("v1alpha1.ProviderReady = true for a Provider that never read the platform")
	}
	if n := e.batch.Count("squeue-me"); n != 1 {
		t.Fatalf("squeue-me logged %d times, want 1", n)
	}
	// It stays False however long the reads fail.
	e.syncAt(at(13, 0, 0))
	e.wantReady(metav1.ConditionFalse, "ReadsFailing")
	wantNilMT(t, "lastSuccessfulStatusRead", e.provider().Status.LastSuccessfulStatusRead)

	// And turns True with the first successful poll.
	e.batch.FailReads(nil)
	e.syncAt(at(13, 1, 0))
	e.wantReady(metav1.ConditionTrue, "Polled")
	wantMT(t, "lastSuccessfulStatusRead", e.provider().Status.LastSuccessfulStatusRead, at(13, 1, 0))
}

func TestReadyStaysTrueWithinTheStalenessBoundThenTurnsFalse(t *testing.T) {
	e := newEnv(t)
	e.sync() // 12:00:00: a successful poll
	e.wantReady(metav1.ConditionTrue, "Polled")
	e.batch.FailReads(errors.New("example transport failure"))

	e.syncAt(at(12, 5, 0)) // exactly 5m after the last success: still within the bound
	e.wantReady(metav1.ConditionTrue, "Polled")
	prov := e.provider()
	wantMT(t, "lastSuccessfulStatusRead", prov.Status.LastSuccessfulStatusRead, at(12, 0, 0))
	if !v1alpha1.ProviderReady(prov, at(12, 5, 0)) {
		t.Fatal("v1alpha1.ProviderReady = false at exactly the bound")
	}

	e.syncAt(at(12, 5, 1)) // one second past it
	c := e.wantReady(metav1.ConditionFalse, "ReadsFailing")
	const wantMessage = "reads are failing: the last successful read was at 2026-10-30T12:00:00Z"
	if c.Message != wantMessage {
		t.Fatalf("message = %q, want %q", c.Message, wantMessage)
	}
	prov = e.provider()
	wantMT(t, "lastSuccessfulStatusRead", prov.Status.LastSuccessfulStatusRead, at(12, 0, 0)) // not advanced
	if v1alpha1.ProviderReady(prov, at(12, 5, 1)) {
		t.Fatal("v1alpha1.ProviderReady = true one second past the bound")
	}

	// The message does not change from pass to pass (it names the time of the
	// last success, not an age), nor does the transition time: a failing
	// Provider is not rewritten for its condition.
	e.syncAt(at(12, 9, 30))
	c2 := e.wantReady(metav1.ConditionFalse, "ReadsFailing")
	if c2.Message != wantMessage {
		t.Fatalf("message at the next failing pass = %q, want it unchanged: %q", c2.Message, wantMessage)
	}
	if !c2.LastTransitionTime.Equal(&c.LastTransitionTime) {
		t.Fatalf("lastTransitionTime moved from %v to %v with no change of status", c.LastTransitionTime, c2.LastTransitionTime)
	}

	e.batch.FailReads(nil)
	e.syncAt(at(12, 10, 0))
	e.wantReady(metav1.ConditionTrue, "Polled")
	wantMT(t, "lastSuccessfulStatusRead", e.provider().Status.LastSuccessfulStatusRead, at(12, 10, 0))
}

func TestReadyStalenessFromTheController(t *testing.T) {
	e := newEnv(t)
	e.ctl.ReadyStaleness = 2 * time.Minute
	e.sync() // 12:00:00
	e.batch.FailReads(errors.New("example transport failure"))
	e.syncAt(at(12, 2, 0))
	e.wantReady(metav1.ConditionTrue, "Polled")
	e.syncAt(at(12, 2, 1))
	e.wantReady(metav1.ConditionFalse, "ReadsFailing")
}

func TestAPassThatObservedAJobDoesNotPoll(t *testing.T) {
	e := newEnv(t)
	e.newAlloc("a1", testProvider)
	e.sync() // submits and observes job 998
	e.syncAt(at(12, 1, 0))
	if n := e.batch.Count("squeue-me"); n != 0 {
		t.Fatalf("squeue-me logged %d times, want 0: the observe was the read", n)
	}
	wantMT(t, "lastSuccessfulStatusRead", e.provider().Status.LastSuccessfulStatusRead, at(12, 1, 0))
	e.wantReady(metav1.ConditionTrue, "Polled")

	// A failed observe is a read too: it is not answered with a poll that could
	// succeed and so hide the failure.
	e.batch.FailReads(errors.New("example transport failure"))
	e.syncAt(at(12, 2, 0))
	if n := e.batch.Count("squeue-me"); n != 0 {
		t.Fatalf("squeue-me logged %d times after a failed observe, want 0", n)
	}
	wantMT(t, "lastSuccessfulStatusRead", e.provider().Status.LastSuccessfulStatusRead, at(12, 1, 0))
}

func TestAPassWhoseOnlyReadWasADeletionLookupDoesNotPoll(t *testing.T) {
	e := newEnv(t)
	e.newAlloc("a1", testProvider)
	e.sync()
	e.del("a1")
	e.syncAt(at(12, 1, 0)) // cancels, looks up again, removes the finalizer
	if !e.gone("a1") {
		t.Fatal("setup: the Allocation was not released")
	}
	if n := e.batch.Count("squeue-me"); n != 0 {
		t.Fatalf("squeue-me logged %d times, want 0: the lookups were the reads", n)
	}
}

func TestAnAllocationOfAnotherProviderIsNotAReadOfThisOne(t *testing.T) {
	e := newEnv(t)
	e.newAlloc("other", "example-other-provider")
	e.sync()
	if n := e.batch.Count("squeue-me"); n != 1 {
		t.Fatalf("squeue-me logged %d times, want 1: this Provider observed no job", n)
	}
}
