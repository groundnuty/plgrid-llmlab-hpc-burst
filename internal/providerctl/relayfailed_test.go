package providerctl

// Issue #304, the Provider controller's half: a relay that ends for good
// through the real relay owner (hostdial.Owner, its own loop, its own reading
// of the relay's end) marks its Allocation RelayEndedForGood on the next
// observation, and an end that is superseded, the job's end or retried marks
// nothing. The relays' ssh is a stub that prints what srun or the relay
// prints and exits as they do; the owner classifies it, so nothing here hands
// the controller an end kind. The expected names and texts are written from
// the issue and HBEP-6's What ends a relay, not read from the code: the
// condition's type and reason are spelt out as literals.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
)

// The condition's names, as issue #304's contract change gives them.
const (
	wantRelayCondition = "RelayEndedForGood"
	wantRelayReason    = "RelayFailed"
)

// The stubs' lines: srun's step-limit refusal (Slurm's ESLURM_STEP_LIMIT),
// srun's refusal for a job that does not exist (ESLURM_INVALID_JOB_ID), and
// the relay's own superseded line, which it ends with exit code 5.
const (
	stubStepLimit  = "echo 'srun: error: Unable to create step for job 1: Step limit reached for this job' >&2\nexit 1\n"
	stubJobGone    = "echo 'srun: error: Unable to create step for job 1: Invalid job id specified' >&2\nexit 1\n"
	stubSuperseded = "echo 'hb-relay: superseded: a relay with a higher fence holds the channel' >&2\nexit 5\n"
)

// relayStub writes an ssh stub that runs body, and returns its path.
func relayStub(t *testing.T, body string) string {
	t.Helper()
	stub := filepath.Join(t.TempDir(), "ssh")
	if err := os.WriteFile(stub, []byte("#!/bin/sh\n"+body), 0o700); err != nil {
		t.Fatal(err)
	}
	return stub
}

// stubbedRelays makes the env host-dialed with its relays' ssh at stub.
func stubbedRelays(stub string) envOption {
	return func(e *env) {
		hostDialedWith(e, hostdial.NewHosts(), 1000)
		e.opts.Relays.SSHBinary = stub
	}
}

// relayCondition is the Allocation's RelayEndedForGood condition, by its
// literal type.
func relayCondition(a *v1alpha1.Allocation) (status corev1.ConditionStatus, reason, msg string) {
	for _, c := range a.Status.Conditions {
		if string(c.Type) == wantRelayCondition {
			return c.Status, string(c.Reason), c.Message
		}
	}
	return "", "", ""
}

// relayFailedMessages are the messages of the RelayFailed events raised.
func (r *recorder) relayFailedMessages() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, e := range r.events {
		if string(e.Reason) == "RelayFailed" {
			out = append(out, e.Message)
		}
	}
	return out
}

// A relay that ends for good with srun's step-limit refusal marks its
// Allocation on the next observation after its end, and not before: the
// stub waits for a gate file, so the relays run while the first observations
// pass, and the end comes only once the gate opens. The mark is True with
// reason RelayFailed and the RelayFailed event's message, which names the
// channel and srun's refusal; it stays through later observations, and the
// Provider's controller cancels nothing itself: the release is the lifecycle
// controller's (relayfailed_release_test.go).
func TestARelayEndedForGoodMarksItsAllocationOnTheNextObservation(t *testing.T) {
	// The relays' ssh -G (the login host's name) answers at once; each relay
	// waits for the gate, at most 60 s, so no stub outlives a failed test.
	gate := filepath.Join(t.TempDir(), "open")
	stub := relayStub(t, "case \" $* \" in *' -G '*) exit 1;; esac\nn=0\n"+
		"while [ ! -e '"+gate+"' ] && [ $n -lt 3000 ]; do sleep 0.02; n=$((n+1)); done\n"+stubStepLimit)
	e := newEnv(t, stubbedRelays(stub))
	runRelays(e)
	e.newAlloc("alloc-a")
	e.run("alloc-a", 4)
	a := e.get("alloc-a")
	tag := tagOf(a)
	if a.Status.AtProvider.Phase != v1alpha1.AllocationRunning {
		t.Fatalf("the Allocation is %s, want Running", a.Status.AtProvider.Phase)
	}
	waitRelays(t, "both relays' ssh running", func() bool {
		rs := relaysOf(e, tag)
		return len(rs) == 2 && rs[0].PID != 0 && rs[1].PID != 0
	})
	e.run("alloc-a", 2)
	if st, _, _ := relayCondition(e.get("alloc-a")); st != "" {
		t.Fatalf("the Allocation is marked %s while its relays run", st)
	}
	if err := os.WriteFile(gate, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	waitRelays(t, "RelayFailed raised", func() bool { return len(e.rec.relayFailedMessages()) > 0 })
	e.run("alloc-a", 1)
	st, reason, msg := relayCondition(e.get("alloc-a"))
	if st != corev1.ConditionTrue || reason != wantRelayReason {
		t.Fatalf("after the next observation the condition is %q, reason %q; want True, %s", st, reason, wantRelayReason)
	}
	evs := e.rec.relayFailedMessages()
	if msg != evs[0] || !strings.Contains(msg, "relay ended for good") || !strings.Contains(msg, "step limit") ||
		!(strings.Contains(msg, "control") || strings.Contains(msg, "serving")) {
		t.Errorf("the condition's message %q: want the first RelayFailed event's %q, naming the channel and the step limit", msg, evs[0])
	}
	waitRelays(t, "both relays ended for good", func() bool {
		rs := relaysOf(e, tag)
		return len(rs) == 2 && rs[0].Final && rs[1].Final
	})
	e.run("alloc-a", 3)
	a = e.get("alloc-a")
	if st, _, msg2 := relayCondition(a); st != corev1.ConditionTrue || msg2 != msg {
		t.Errorf("later observations left the condition %q with %q; want it kept, with %q", st, msg2, msg)
	}
	if a.Status.AtProvider.Phase != v1alpha1.AllocationRunning || !a.DeletionTimestamp.IsZero() {
		t.Errorf("the Provider's controller changed the Allocation itself: phase %s, deleted %v", a.Status.AtProvider.Phase, !a.DeletionTimestamp.IsZero())
	}
	if n := e.count("scancel"); n != 0 {
		t.Errorf("the Provider's controller cancelled %d jobs; the release is the lifecycle controller's", n)
	}
}

// What releases nothing (issue #304): a relay superseded by a newer copy's,
// one whose job srun says is gone, and one that is retried. Each ends through
// the real owner, as the state the owner keeps shows, and the observations
// after it leave the Allocation unmarked, Running, with no RelayFailed.
func TestRelayEndsThatReleaseNothingLeaveTheAllocationUnmarked(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stub  string
		ended func(hostdial.RelayState) bool
	}{
		{"superseded", stubSuperseded, func(r hostdial.RelayState) bool {
			return r.Final && strings.HasPrefix(r.FinalCause, "superseded: ")
		}},
		{"the job's end", stubJobGone, func(r hostdial.RelayState) bool {
			return r.Final && strings.HasPrefix(r.FinalCause, "the job's end: ")
		}},
		{"retried", "exit 1\n", func(r hostdial.RelayState) bool {
			return !r.Final && r.Starts >= 3 && strings.HasPrefix(r.LastEnd, "retried")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, stubbedRelays(relayStub(t, tc.stub)))
			runRelays(e)
			e.newAlloc("alloc-a")
			e.run("alloc-a", 4)
			tag := tagOf(e.get("alloc-a"))
			waitRelays(t, "both relays ended as "+tc.name, func() bool {
				rs := relaysOf(e, tag)
				return len(rs) == 2 && tc.ended(rs[0]) && tc.ended(rs[1])
			})
			e.run("alloc-a", 3)
			a := e.get("alloc-a")
			if st, reason, msg := relayCondition(a); st != "" {
				t.Errorf("a relay ended as %s marked the Allocation %s, %s: %q", tc.name, st, reason, msg)
			}
			if got := e.rec.relayFailedMessages(); len(got) != 0 {
				t.Errorf("a relay ended as %s raised RelayFailed: %q", tc.name, got)
			}
			if a.Status.AtProvider.Phase != v1alpha1.AllocationRunning {
				t.Errorf("the Allocation is %s, want Running", a.Status.AtProvider.Phase)
			}
		})
	}
}
