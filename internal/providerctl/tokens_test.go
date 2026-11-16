package providerctl

import (
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// A job ID is matched as a whole token: the tests below write each case out by
// hand, never from the code under test, and each has a case that a substring
// match would get wrong.
func TestTokenHelpers(t *testing.T) {
	for _, c := range []struct {
		s, tok string
		in     bool
	}{
		{"job 12345 started", "12345", true},
		{"12345", "12345", true},
		{"(12345)", "12345", true},
		{"123456", "12345", false},
		{"x12345", "12345", false},
		{"12345x", "12345", false},
		{"a 123456 then 12345", "12345", true},
		{"2026-11-16T12:34:56Z", "12345", false},
		{"anything", "", false},
	} {
		if got := containsToken(c.s, c.tok); got != c.in {
			t.Errorf("containsToken(%q, %q) = %v, want %v", c.s, c.tok, got, c.in)
		}
	}
	if got := redactToken("job 12345 and 123456 and (12345)", "12345", "the job"); got != "job the job and 123456 and (the job)" {
		t.Errorf("redactToken = %q", got)
	}
}

// No job ID in the messages the lifecycle controller copies into its events on
// the Instance (HBEP-8, Allocation events, No job ID in a note; R2 of #625).

// The relay's cause can name the job ("job <ID> is not running"): the RelayEndedForGood
// message, which the lifecycle controller copies into its failing event, does not.
func TestARelayFailedMessageNamesNoJobID(t *testing.T) {
	const id = "7340211"
	for _, cause := range []string{
		"srun refused the step: step limit reached for this job",
		"more than 120 restarts of the control channel within 1h0m0s after sessions that came up; the last end: job 7340211 is Pending, not running yet",
		"the relay line: job ID \"7340211\" is not a Slurm job ID",
	} {
		msg := relayFailedMessage(agentapi.RelayControl, cause, id)
		if containsToken(msg, id) {
			t.Errorf("message %q names the job ID", msg)
		}
		if !strings.HasPrefix(msg, "the control relay ended for good: ") {
			t.Errorf("message %q lost its shape", msg)
		}
	}
	// What is not the job ID stays: a count that holds its digits is no token.
	if msg := relayFailedMessage(agentapi.RelayControl, "more than 7340211 restarts", id); containsToken(msg, id) {
		t.Errorf("message %q: a count that equals the job ID is a token too, and goes", msg)
	}
	if msg := relayFailedMessage(agentapi.RelayControl, "73402110 restarts", id); !strings.Contains(msg, "73402110") {
		t.Errorf("message %q: a longer number that holds the digits was redacted", msg)
	}
}

// The Provider's SubmittedAsOther condition message, which the lifecycle
// controller copies, keeps the settings that differed and names no job ID.
func TestTheSubmittedAsOtherMessageNamesNoJobID(t *testing.T) {
	e := newEnv(t)
	e.batch.Enqueue(fake.Script{AsTimeLimit: 20 * time.Minute})
	e.newAlloc("a", threeHours)
	e.run("a", 4)
	a := e.get("a")
	id := a.Annotations[v1alpha1.AnnotationJobID]
	c := a.GetCondition(v1alpha1.ConditionSubmittedAsOther)
	if c.Status != "True" {
		t.Fatalf("SubmittedAsOther %s, want True", c.Status)
	}
	if id == "" || containsToken(c.Message, id) {
		t.Errorf("message %q names the job ID %q (or there is none to look for)", c.Message, id)
	}
	if want := "the job was established with other settings than requested: timeLimit: requested 3h0m0s, got 20m0s"; c.Message != want {
		t.Errorf("message %q, want %q", c.Message, want)
	}
}

func threeHours(a *v1alpha1.Allocation) { a.Spec.ForProvider.Batch.TimeLimit = "3h" }
