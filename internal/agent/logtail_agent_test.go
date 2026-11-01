package agent_test

import (
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The engine log is appended to across the Instances of a job, so a later
// Instance's LogTail can hold an earlier one's output. Every key the agent has
// drawn is replaced in it, the earlier Instance's too (PR #138 review, Astra
// B1). Instance A's engine prints its key and exits; Instance B's engine
// exits, and B's tail, which still holds A's line, holds no part of A's key.
func TestLogTailReplacesAnEarlierInstancesKey(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	exited := func(uid string) func(agentapi.Report) bool {
		return func(r agentapi.Report) bool {
			w := worker0(r)
			return r.Instance != nil && r.Instance.UID == uid && w != nil && w.Phase == agentapi.PartExited && w.LogTail != ""
		}
	}
	h.set(h.runState("print-key,exit-after=50ms:1", 1, 1, nil))
	ra := h.waitReport("Instance A's worker exited", 30*time.Second, exited(hInstUID))
	keyA := ra.Instance.Key
	if len(keyA) != 64 {
		t.Fatalf("Instance A's report carries no key (%d bytes)", len(keyA))
	}
	line := "enginedouble: api key " + agentapi.RedactedValue
	if lt := worker0(ra).LogTail; !strings.Contains(lt, line) || strings.Contains(lt, keyA[:8]) {
		t.Fatalf("A's LogTail %q: want its key replaced", lt)
	}

	b := h.runState("exit-after=50ms:1", 1, 1, nil)
	const uidB = "bbbbbbbb-bbbb-cccc-dddd-eeeeeeeeeeee"
	b.Instance.UID, b.Instance.Name = uidB, "inst-b"
	h.set(b)
	rb := h.waitReport("Instance B's worker exited", 30*time.Second, exited(uidB))
	if rb.Instance.Key == keyA || rb.Instance.Key == "" {
		t.Fatalf("Instance B has key %q, want a new one", rb.Instance.Key)
	}
	lt := worker0(rb).LogTail
	if !strings.Contains(lt, line) {
		t.Fatalf("B's LogTail does not hold A's line, so it tests nothing: %q", lt)
	}
	for i := 0; i+8 <= len(keyA); i++ {
		if strings.Contains(lt, keyA[i:i+8]) {
			t.Fatalf("B's LogTail holds %q of Instance A's key", keyA[i:i+8])
		}
	}
}
