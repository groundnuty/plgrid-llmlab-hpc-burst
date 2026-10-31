package fake_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

var errAnswerLost = errors.New("the answer never arrived")

// lossyDesired computes each answer to Desired, as the controller does, and
// then loses the next ones before they reach the agent. FailNext cannot do
// that: it fails the call before the Backend looks at its desired state, so
// no message ever existed.
type lossyDesired struct {
	agentapi.Backend
	lose int
	lost []string // the Versions of the answers that were computed and lost
}

func (l *lossyDesired) Desired(ctx context.Context, c agentapi.Caller) (agentapi.DesiredState, error) {
	d, err := l.Backend.Desired(ctx, c)
	if err == nil && l.lose > 0 {
		l.lose--
		l.lost = append(l.lost, d.Version)
		return agentapi.DesiredState{}, errAnswerLost
	}
	return d, err
}

func newLossyRig(t *testing.T) (*rig, *lossyDesired) {
	t.Helper()
	l := &lossyDesired{}
	r := newRigOn(t, fake.Engine{}, func(b *fake.Backend) agentapi.Backend { l.Backend = b; return l })
	return r, l
}

func TestALostDesiredStateAnswerIsCorrectedByTheNextPoll(t *testing.T) {
	r, l := newLossyRig(t)
	r.step() // registered, idle, one report
	r.set(run(instance()))
	l.lose = 1

	if err := r.a.Step(bgndCtx); !errors.Is(err, errAnswerLost) {
		t.Fatalf("Step: err = %v, want the lost answer", err)
	}
	// The Backend did compute the answer, and the agent did not get it.
	eq(t, "answers computed and lost", len(l.lost), 1)
	eq(t, "Desired calls reaching the Backend", r.b.Calls(fake.MethodDesired), 2)
	eq(t, "len(Seen): the lost answer was not seen", len(r.a.Seen()), 1)
	eq(t, "reports: the lost step sent none", len(r.reports()), 1)
	eq(t, "no Instance reported", r.last().Instance == nil, true)
	eq(t, "no worker started", len(r.a.Starts()), 0)

	r.step() // the next poll gets the same state
	seen := r.a.Seen()
	eq(t, "len(Seen) after the next poll", len(seen), 2)
	eq(t, "the state seen is the one that was lost", seen[1].Version, l.lost[0])
	eq(t, "worker phase", r.workerOf("worker-0").Phase, agentapi.PartStarting)
	eq(t, "PID", r.workerOf("worker-0").PID, 4000)
}

func TestAStateThatChangesAfterALostAnswerIsTheOneTheAgentConvergesOn(t *testing.T) {
	r, l := newLossyRig(t)
	r.step()
	r.set(run(instance(withGeneration(1))))
	l.lose = 1
	if err := r.a.Step(bgndCtx); !errors.Is(err, errAnswerLost) {
		t.Fatalf("Step: err = %v, want the lost answer", err)
	}

	r.set(run(instance(withGeneration(2)))) // the controller moved on meanwhile
	r.step()

	seen := r.a.Seen()
	eq(t, "len(Seen)", len(seen), 2)
	if seen[1].Version == l.lost[0] {
		t.Errorf("the agent saw the lost state %s, want the newer one", l.lost[0])
	}
	eq(t, "the state the agent acted on: generation", seen[1].Instance.Generation, int64(2))
	starts := r.a.Starts()
	eq(t, "starts: the lost state was never run", len(starts), 1)
	eq(t, "the one start's Instance generation", starts[0].InstanceGeneration, int64(2))
	eq(t, "reported Instance generation", r.lastInstance().Generation, int64(2))
}

func TestASilentAgentMakesNoCallAndItsReportsResumeAfterwards(t *testing.T) {
	r := newRig(t, fake.Engine{})
	r.a.Silence(true)
	r.steps(3)
	reg, des, rep := r.calls()
	eq(t, "calls while silent: (register, desired, report)", [3]int{reg, des, rep}, [3]int{})
	eq(t, "registrations while silent", len(r.b.Registrations(allocUID)), 0)

	r.set(run(instance()))
	r.a.Silence(false)
	r.clk.Advance(time.Hour)
	r.step()
	reg, des, rep = r.calls()
	eq(t, "calls after the outage: (register, desired, report)", [3]int{reg, des, rep}, [3]int{1, 1, 1})
	if got := r.last().Time; !got.Equal(t0.Add(time.Hour)) {
		t.Errorf("Report.Time = %v, want the time of the first audible Step", got)
	}
}
