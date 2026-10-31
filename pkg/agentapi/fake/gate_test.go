package fake_test

import (
	"errors"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

const (
	gateTag        = "tag-example"
	gateCredential = "credential-example-1"
	gateJob        = "job-1"
)

// gated is a Backend behind a Gate with one Allocation in its table, whose job
// ID is not recorded yet.
type gated struct {
	t   *testing.T
	b   *fake.Backend
	g   *fake.Gate
	clk *clock
}

func newGated(t *testing.T) *gated {
	t.Helper()
	b := fake.NewBackend()
	g := fake.NewGate(b)
	g.Add(gateTag, fake.GateAllocation{Namespace: "ns-example", Name: "alloc-example", UID: allocUID, Credential: gateCredential})
	return &gated{t: t, b: b, g: g, clk: &clock{t: t0}}
}

// agent returns an Agent presenting the credential, tag and job ID. Its Caller
// holds only an agent instance ID, as a real agent's does: the Gate fills the
// rest.
func (x *gated) agent(credential, tag, jobID string) *fake.Agent {
	return fake.NewAgent(x.g.As(credential, tag, jobID), agentapi.Caller{}, x.clk.Now, fake.Engine{})
}

// wantNoInnerCalls fails the test when the inner Backend has seen any call.
func (x *gated) wantNoInnerCalls() {
	x.t.Helper()
	for _, m := range []string{fake.MethodRegister, fake.MethodDesired, fake.MethodReport} {
		if n := x.b.Calls(m); n != 0 {
			x.t.Errorf("the inner Backend saw %d %s calls from a refused agent, want 0", n, m)
		}
	}
}

func TestAWrongOrUnknownCredentialIsRefusedBeforeTheBackend(t *testing.T) {
	cases := []struct {
		name, credential, tag string
	}{
		{"wrong credential", "credential-example-2", gateTag},
		{"a prefix of the credential", "credential-example-", gateTag},
		{"the credential with a suffix", gateCredential + "x", gateTag},
		{"the credential in another case", "CREDENTIAL-EXAMPLE-1", gateTag},
		{"no credential", "", gateTag},
		{"an unknown tag, with the right credential", gateCredential, "tag-unknown"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			x := newGated(t)
			x.g.SetJobID(gateTag, gateJob)
			a := x.agent(c.credential, c.tag, gateJob)

			err := a.Step(bgndCtx)
			wantCode(t, "Step", err, agentapi.CodeUnauthenticated)
			x.wantNoInnerCalls()
			eq(t, "registrations", len(x.b.Registrations(allocUID)), 0)
			eq(t, "refusals", x.g.Refusals(), []fake.Refusal{{Method: fake.MethodRegister, Tag: c.tag, Code: agentapi.CodeUnauthenticated}})
		})
	}
}

func TestTheRightCredentialWithAnotherJobIDIsRefused(t *testing.T) {
	x := newGated(t)
	x.g.SetJobID(gateTag, gateJob)
	a := x.agent(gateCredential, gateTag, "job-2")

	wantCode(t, "Step", a.Step(bgndCtx), agentapi.CodeJobMismatch)
	x.wantNoInnerCalls()
	eq(t, "refusals", x.g.Refusals(), []fake.Refusal{{Method: fake.MethodRegister, Tag: gateTag, Code: agentapi.CodeJobMismatch}})

	// The right job ID, from another agent, is accepted: the gate holds no session.
	ok := x.agent(gateCredential, gateTag, gateJob)
	if err := ok.Step(bgndCtx); err != nil {
		t.Fatalf("Step of the agent with the recorded job ID: %v", err)
	}
	eq(t, "the accepted agent's Register calls", x.b.Calls(fake.MethodRegister), 1)
}

func TestBeforeAJobIDIsRecordedTheAgentIsRefusedThenAcceptedOnceItIs(t *testing.T) {
	x := newGated(t) // JobID not recorded
	a := x.agent(gateCredential, gateTag, gateJob)

	wantCode(t, "Step before the job ID is recorded", a.Step(bgndCtx), agentapi.CodeJobMismatch)
	x.wantNoInnerCalls()
	// An agent that presents no job ID matches no unrecorded one either.
	wantCode(t, "Step with no job ID", x.agent(gateCredential, gateTag, "").Step(bgndCtx), agentapi.CodeJobMismatch)
	x.wantNoInnerCalls()

	x.g.SetJobID(gateTag, gateJob)
	if err := a.Step(bgndCtx); err != nil {
		t.Fatalf("Step once the job ID is recorded: %v", err)
	}
	reg, des, rep := x.b.Calls(fake.MethodRegister), x.b.Calls(fake.MethodDesired), x.b.Calls(fake.MethodReport)
	eq(t, "inner calls from the accepted agent: (register, desired, report)", [3]int{reg, des, rep}, [3]int{1, 1, 1})
	eq(t, "refusals: the two earlier ones only", len(x.g.Refusals()), 2)
}

func TestTheGateFillsTheCallerFromItsTableAndKeepsTheAgentInstanceID(t *testing.T) {
	x := newGated(t)
	x.g.SetJobID(gateTag, gateJob)
	// An agent that claims another Allocation, Tag and job ID in its Caller is
	// not believed: the table decides.
	forged := agentapi.Caller{AllocationUID: "forged-uid", Allocation: "forged", Namespace: "forged-ns", Tag: "forged-tag", JobID: "forged-job", AgentInstanceID: "aaaabbbbccccdddd0000111122223333"}
	a := fake.NewAgent(x.g.As(gateCredential, gateTag, gateJob), forged, x.clk.Now, fake.Engine{})
	if err := a.Step(bgndCtx); err != nil {
		t.Fatalf("Step: %v", err)
	}

	regs := x.b.Registrations(allocUID)
	if len(regs) != 1 {
		t.Fatalf("registrations under the table's UID = %d, want 1", len(regs))
	}
	eq(t, "the Caller the inner Backend saw", regs[0].Caller, agentapi.Caller{
		Namespace: "ns-example", Allocation: "alloc-example", AllocationUID: allocUID,
		Tag: gateTag, JobID: gateJob, AgentInstanceID: "aaaabbbbccccdddd0000111122223333",
	})
	eq(t, "registrations under the forged UID", len(x.b.Registrations("forged-uid")), 0)
	eq(t, "reports under the table's UID", len(x.b.Reports(allocUID)), 1)
}

func TestEveryMethodIsAuthenticatedOnEveryCall(t *testing.T) {
	x := newGated(t)
	x.g.SetJobID(gateTag, gateJob)
	c := agentapi.Caller{AgentInstanceID: "0123456789abcdef0123456789abcdef"}

	t.Run("a wrong credential reaches no method", func(t *testing.T) {
		bad := x.g.As("credential-wrong", gateTag, gateJob)
		_, err := bad.Register(bgndCtx, c, agentapi.RegisterRequest{})
		wantCode(t, "Register", err, agentapi.CodeUnauthenticated)
		_, err = bad.Desired(bgndCtx, c)
		wantCode(t, "Desired", err, agentapi.CodeUnauthenticated)
		wantCode(t, "Report", bad.Report(bgndCtx, c, agentapi.Report{}), agentapi.CodeUnauthenticated)
		x.wantNoInnerCalls()
	})

	t.Run("a credential that stops being the Allocation's is refused at once", func(t *testing.T) {
		a := x.agent(gateCredential, gateTag, gateJob)
		if err := a.Step(bgndCtx); err != nil {
			t.Fatalf("Step: %v", err)
		}
		desired := x.b.Calls(fake.MethodDesired)

		x.g.Add(gateTag, fake.GateAllocation{Namespace: "ns-example", Name: "alloc-example", UID: allocUID, Credential: "credential-rotated", JobID: gateJob})
		wantCode(t, "Step after the rotation", a.Step(bgndCtx), agentapi.CodeUnauthenticated)
		eq(t, "Desired calls reaching the Backend after the rotation", x.b.Calls(fake.MethodDesired), desired)
	})

	t.Run("a job ID that stops being the recorded one is refused at once", func(t *testing.T) {
		x := newGated(t)
		x.g.SetJobID(gateTag, gateJob)
		a := x.agent(gateCredential, gateTag, gateJob)
		if err := a.Step(bgndCtx); err != nil {
			t.Fatalf("Step: %v", err)
		}
		reports := x.b.Calls(fake.MethodReport)

		x.g.SetJobID(gateTag, "job-other")
		wantCode(t, "Step after the job ID changed", a.Step(bgndCtx), agentapi.CodeJobMismatch)
		eq(t, "Report calls reaching the Backend", x.b.Calls(fake.MethodReport), reports)
		eq(t, "the refusal was at Desired", x.g.Refusals()[0].Method, fake.MethodDesired)
	})
}

func TestTwoAllocationsThroughOneGateEachGetTheirOwnIdentity(t *testing.T) {
	x := newGated(t)
	x.g.Add("tag-2", fake.GateAllocation{Namespace: "ns-example", Name: "alloc-2", UID: "alloc-uid-2", Credential: "credential-example-2", JobID: "job-9"})
	x.g.SetJobID(gateTag, gateJob)

	a1 := x.agent(gateCredential, gateTag, gateJob)
	a2 := x.agent("credential-example-2", "tag-2", "job-9")
	// One Allocation's credential does not open the other's tag.
	cross := x.agent(gateCredential, "tag-2", "job-9")
	for _, a := range []*fake.Agent{a1, a2} {
		if err := a.Step(bgndCtx); err != nil {
			t.Fatalf("Step: %v", err)
		}
	}
	wantCode(t, "Step with the other Allocation's credential", cross.Step(bgndCtx), agentapi.CodeUnauthenticated)

	eq(t, "agent 1's registration", x.b.Registrations(allocUID)[0].Caller.AgentInstanceID, a1.AgentInstanceID())
	eq(t, "agent 2's registration", x.b.Registrations("alloc-uid-2")[0].Caller.AgentInstanceID, a2.AgentInstanceID())
	eq(t, "agent 2's Allocation name", x.b.Registrations("alloc-uid-2")[0].Caller.Allocation, "alloc-2")
	eq(t, "Register calls: the refused one is not among them", x.b.Calls(fake.MethodRegister), 2)
}

func TestGateRefusesMisuse(t *testing.T) {
	mustPanic := func(what string, f func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf("%s: no panic", what)
			}
		}()
		f()
	}
	g := fake.NewGate(fake.NewBackend())
	mustPanic("Add with no credential", func() { g.Add("tag", fake.GateAllocation{UID: "u"}) })
	mustPanic("Add with no tag", func() { g.Add("", fake.GateAllocation{UID: "u", Credential: "c"}) })
	mustPanic("Add with no UID", func() { g.Add("tag", fake.GateAllocation{Credential: "c"}) })
	mustPanic("SetJobID of an unknown tag", func() { g.SetJobID("tag", "job") })

	// An empty gate refuses everyone, whatever they present.
	_, err := g.As("", "", "").Register(bgndCtx, agentapi.Caller{}, agentapi.RegisterRequest{})
	var e *agentapi.Error
	if !errors.As(err, &e) || e.Code != agentapi.CodeUnauthenticated {
		t.Errorf("an empty gate answered %v, want Unauthenticated", err)
	}
}
