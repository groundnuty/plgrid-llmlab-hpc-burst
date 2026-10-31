package fake_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// The forms are written out by hand here: an agent instance ID is 32 lowercase
// hex characters, a key 64.
func TestBackendRefusesAnAgentInstanceIDOfAnotherForm(t *testing.T) {
	bad := map[string]string{
		"empty":               "",
		"words":               "agent-example",
		"31 characters":       strings.Repeat("a", 31),
		"33 characters":       strings.Repeat("a", 33),
		"a key's length":      strings.Repeat("a", 64),
		"upper case":          strings.Repeat("A", 32),
		"not hex":             strings.Repeat("g", 32),
		"a trailing newline":  strings.Repeat("a", 32) + "\n",
		"a leading space":     " " + strings.Repeat("a", 31),
		"dashes as in a UUID": "01234567-89ab-cdef-0123-456789abcdef",
	}
	for name, id := range bad {
		t.Run(name, func(t *testing.T) {
			b := fake.NewBackend()
			_, err := b.Register(bgndCtx, caller(id), agentapi.RegisterRequest{})
			wantCode(t, "Register", err, agentapi.CodeBadRequest)
			_, err = b.Desired(bgndCtx, caller(id))
			wantCode(t, "Desired", err, agentapi.CodeBadRequest)
			err = b.Report(bgndCtx, caller(id), agentapi.Report{Time: t0})
			wantCode(t, "Report", err, agentapi.CodeBadRequest)
			if id != "" && strings.Contains(err.Error(), strings.TrimSpace(id)) {
				t.Errorf("the refusal repeats the caller's ID: %q", err)
			}
			eq(t, "registrations", len(b.Registrations(allocUID)), 0)
			eq(t, "reports", len(b.Reports(allocUID)), 0)
		})
	}

	// The positive control: the pinned form is accepted by all three.
	b := fake.NewBackend()
	good := strings.Repeat("0123456789abcdef", 2)
	if _, err := b.Register(bgndCtx, caller(good), agentapi.RegisterRequest{}); err != nil {
		t.Fatalf("Register with a pinned-form ID: %v", err)
	}
	if _, err := b.Desired(bgndCtx, caller(good)); err != nil {
		t.Errorf("Desired with a pinned-form ID: %v", err)
	}
	if err := b.Report(bgndCtx, caller(good), agentapi.Report{Time: t0}); err != nil {
		t.Errorf("Report with a pinned-form ID: %v", err)
	}
}

func TestBackendRefusesAReportWhoseKeyIsNotOfThePinnedForm(t *testing.T) {
	bad := map[string]string{
		"words":              "key-example",
		"63 characters":      strings.Repeat("a", 63),
		"65 characters":      strings.Repeat("a", 65),
		"32 characters":      strings.Repeat("a", 32),
		"upper case":         strings.Repeat("A", 64),
		"not hex":            strings.Repeat("g", 64),
		"a trailing newline": strings.Repeat("a", 64) + "\n",
	}
	withKey := func(key string) agentapi.Report {
		return agentapi.Report{Time: t0, Instance: &agentapi.InstanceReport{UID: "inst-uid-1", Key: key}}
	}
	for name, key := range bad {
		t.Run(name, func(t *testing.T) {
			b := fake.NewBackend()
			if _, err := b.Register(bgndCtx, caller(agent1), agentapi.RegisterRequest{}); err != nil {
				t.Fatal(err)
			}
			err := b.Report(bgndCtx, caller(agent1), withKey(key))
			wantCode(t, "Report", err, agentapi.CodeBadRequest)
			if strings.Contains(err.Error(), strings.TrimSpace(key)) {
				t.Errorf("the refusal repeats the key: %q", err)
			}
			eq(t, "reports recorded", len(b.Reports(allocUID)), 0)
		})
	}

	// Positive controls: the pinned form, no key (after KeyStored) and no
	// Instance are accepted and recorded.
	b := fake.NewBackend()
	if _, err := b.Register(bgndCtx, caller(agent1), agentapi.RegisterRequest{}); err != nil {
		t.Fatal(err)
	}
	for name, rep := range map[string]agentapi.Report{
		"a pinned-form key": withKey(strings.Repeat("0123456789abcdef", 4)),
		"no key":            withKey(""),
		"no Instance":       {Time: t0},
	} {
		if err := b.Report(bgndCtx, caller(agent1), rep); err != nil {
			t.Errorf("Report with %s: %v", name, err)
		}
	}
	eq(t, "reports recorded", len(b.Reports(allocUID)), 3)
}

func TestTheAgentSendsAKeyAndAnAgentInstanceIDOfThePinnedForms(t *testing.T) {
	// Every rig runs against the Backend, which refuses another form, so a
	// drifting form would fail the Step; the checks below say what was sent.
	for n := range 20 {
		r := newRig(t, fake.Engine{})
		r.set(run(instance()))
		r.step()
		if err := agentapi.CheckAgentInstanceID(r.a.AgentInstanceID()); err != nil {
			t.Fatalf("agent %d: %v", n, err)
		}
		regs := r.b.Registrations(allocUID)
		eq(t, fmt.Sprintf("agent %d: the registered ID is the one it presents", n), regs[0].Caller.AgentInstanceID, r.a.AgentInstanceID())
		if err := agentapi.CheckKey([]byte(r.lastInstance().Key)); err != nil {
			t.Fatalf("agent %d: %v", n, err)
		}
	}

	r := newRig(t, fake.Engine{})
	r.set(run(instance()))
	r.step()
	first, firstKey := r.a.AgentInstanceID(), r.lastInstance().Key
	r.a.Requeue()
	r.step()
	if err := agentapi.CheckAgentInstanceID(r.a.AgentInstanceID()); err != nil || r.a.AgentInstanceID() == first {
		t.Errorf("after a requeue: ID %q (first %q): %v", r.a.AgentInstanceID(), first, err)
	}
	if err := agentapi.CheckKey([]byte(r.lastInstance().Key)); err != nil || r.lastInstance().Key == firstKey {
		t.Errorf("after a requeue: key differs from the first %v, form: %v", r.lastInstance().Key != firstKey, err)
	}
	r.set(run(instance(func(i *agentapi.DesiredInstance) { i.UID = "inst-uid-2" })))
	r.step()
	if err := agentapi.CheckKey([]byte(r.lastInstance().Key)); err != nil {
		t.Errorf("the next Instance's key: %v", err)
	}
}

func TestAGivenAgentInstanceIDIsKeptAsTheTestSetItAndTheBackendRefusesItIfMalformed(t *testing.T) {
	b := fake.NewBackend()
	a := fake.NewAgent(b, caller("agent-example"), (&clock{t: t0}).Now, fake.Engine{})
	eq(t, "AgentInstanceID", a.AgentInstanceID(), "agent-example")
	err := a.Step(bgndCtx)
	wantCode(t, "Step", err, agentapi.CodeBadRequest)
	eq(t, "registrations", len(b.Registrations(allocUID)), 0)
}
