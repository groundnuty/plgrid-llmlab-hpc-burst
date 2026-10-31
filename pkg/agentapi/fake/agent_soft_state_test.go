package fake_test

import (
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// A controller restart forgets every registration (Backend.Forget). Every agent
// registers again on its next Step, with the same agent instance ID, and goes on:
// no worker restarts, no generation rises, and its report says what it said.
func TestAControllerRestartGivesEveryAgentAReRegistrationAndNothingElse(t *testing.T) {
	b := fake.NewBackend()
	clk := &clock{t: t0} // frozen, so reports before and after compare equal
	type node struct {
		a      *fake.Agent
		uid    string
		count  int32
		before agentapi.Report
	}
	nodes := []*node{{uid: "alloc-uid-1", count: 0}, {uid: "alloc-uid-2", count: 1}}
	for n, nd := range nodes {
		c := caller("")
		c.AllocationUID, c.Allocation = nd.uid, "alloc-example-"+nd.uid
		nd.a = fake.NewAgent(b, c, clk.Now, fake.Engine{})
		nd.a.Registration.RestartCount = nd.count
		inst := instance(func(i *agentapi.DesiredInstance) { i.UID = "inst-uid-" + string(rune('1'+n)) })
		b.SetDesired(nd.uid, run(inst))
	}
	step := func() {
		t.Helper()
		for _, nd := range nodes {
			if err := nd.a.Step(bgndCtx); err != nil {
				t.Fatalf("Step of %s: %v", nd.uid, err)
			}
		}
	}
	ids := map[string]string{}
	for range 3 {
		step()
	}
	for _, nd := range nodes {
		rep, ok := b.LastReport(nd.uid)
		if !ok {
			t.Fatalf("no report from %s", nd.uid)
		}
		nd.before = rep
		ids[nd.uid] = nd.a.AgentInstanceID()
	}
	eq(t, "the two agents have different IDs", ids["alloc-uid-1"] != ids["alloc-uid-2"], true)

	b.Forget()
	step()

	for _, nd := range nodes {
		regs := b.Registrations(nd.uid)
		if len(regs) != 1 || regs[0].Caller.AgentInstanceID != ids[nd.uid] {
			t.Fatalf("%s: registrations = %+v, want one with agent instance ID %s", nd.uid, regs, ids[nd.uid])
		}
		eq(t, nd.uid+": RestartCount is not raised by a re-registration", regs[0].Request.RestartCount, nd.count)
		eq(t, nd.uid+": AgentInstanceID", nd.a.AgentInstanceID(), ids[nd.uid])
		after, _ := b.LastReport(nd.uid)
		eq(t, nd.uid+": report content", after, nd.before)
		eq(t, nd.uid+": reports", len(b.Reports(nd.uid)), 4)
		starts := nd.a.Starts()
		eq(t, nd.uid+": no worker restarted", len(starts), 1)
		eq(t, nd.uid+": the same PID", starts[0].PID, 4000)
		eq(t, nd.uid+": no new desired state: no generation raised", len(nd.a.Seen()), 1)
		eq(t, nd.uid+": Instance generation", after.Instance.Generation, int64(1))
		eq(t, nd.uid+": worker generation", after.Instance.Workers[0].Generation, int64(0))
	}
	// Per agent: Register on the first Step and again after Forget; Desired on
	// four Steps and once more for the refused poll; Report on four.
	eq(t, "Register calls", b.Calls(fake.MethodRegister), 4)
	eq(t, "Desired calls", b.Calls(fake.MethodDesired), 10)
	eq(t, "Report calls", b.Calls(fake.MethodReport), 8)
}

func TestRequeueRaisesTheRestartCountByOneAndDrawsANewAgentInstanceID(t *testing.T) {
	r := newRig(t, fake.Engine{})
	r.step()
	for want := int32(1); want <= 2; want++ {
		before := r.a.AgentInstanceID()
		r.a.Requeue()
		if r.a.AgentInstanceID() == before {
			t.Fatalf("Requeue %d kept the agent instance ID %s", want, before)
		}
		r.step()
		regs := r.b.Registrations(allocUID)
		if int32(len(regs)) != want+1 {
			t.Fatalf("registrations after requeue %d = %d, want %d", want, len(regs), want+1)
		}
		got := regs[want]
		eq(t, "registered agent instance ID", got.Caller.AgentInstanceID, r.a.AgentInstanceID())
		eq(t, "RestartCount", got.Request.RestartCount, want)
	}
	eq(t, "the first execution registered with count 0", r.b.Registrations(allocUID)[0].Request.RestartCount, int32(0))
}

func TestRequeueRaisesTheCountFromWhateverRegistrationHolds(t *testing.T) {
	r := newRig(t, fake.Engine{})
	r.a.Registration.RestartCount = 4 // the job's fifth execution, say
	r.step()
	eq(t, "first registration", r.b.Registrations(allocUID)[0].Request.RestartCount, int32(4))
	r.a.Requeue()
	r.step()
	eq(t, "after a requeue", r.b.Registrations(allocUID)[1].Request.RestartCount, int32(5))
}
