package alpha

// The keeper row's live flow (issue #521; betakeeper.go): row A's world in host-dialed mode with the relay keeper. Nothing here judges: Run
// does (VerifyBetaKeeper). What it reads of the keeper and the controller it reads through internal/rehearsal/keeperprobe, which only test files
// may import: the supervisor and the keeper pinned by pidfd, so that the one signal it sends, the SIGKILL of the controller, goes through a pin
// verified along its chain of parents and never to a numeric PID (the rule of #308).

import (
	"context"
	"fmt"
	"syscall"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/keeperprobe"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/obscheck"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// BetaKeeper is the keeper row: the Instance ready, the engine's rate, the stream sized to the window, the keeper's relays read and the controller
// pinned, the stream opened, the controller killed KeeperKillAfter after its first token, the outcome read (the next controller leading, the keeper
// accepting its epoch, the agent reporting to it, the relays the same processes), the stream read to its end, then the deployment goes and its job
// is seen ended.
func (r *liveRun) BetaKeeper(ctx context.Context, p Plan) (KeeperObs, error) {
	r.start(p)
	in, err := r.firstReady(p)
	if err != nil {
		return KeeperObs{}, err
	}
	var o KeeperObs
	if in.Status.ReadyTime != nil {
		o.ReadyAt = in.Status.ReadyTime.Time
	}
	ds, ok := DrainStartOf(&in, r.dw)
	if !ok {
		r.t.Fatal("the Instance has no lease end")
	}
	o.DrainStart = ds
	o.Deployment = r.observedDeployment()
	key := r.fc.KeyFor(&in)
	o.Rate = r.rate(key)
	fit, err := FitKeeper(r.tm, r.maxModelLenOrDefault(), o.Rate)
	if err != nil {
		p.Logf("%s: %v", p.Row.Name, err)
		return KeeperObs{}, err
	}
	n, err := r.tokensFor(fit, o.Rate)
	if err != nil {
		return KeeperObs{}, err
	}
	o.Planned, o.Asked = fit, n
	p.Logf("%s: the stream is planned at %s, %d tokens at %.0f tokens/s (the window %s and %s more, capped at what the context holds)", p.Row.Name, fit, n, o.Rate, KeeperWindow, KeeperStreamExtra)
	jobID := r.ourJobFor(ctx, in, "the keeper's relays")
	pr := keeperprobe.Probe{T: r.t, H: r.h, C: r.c, Ports: r.h.Ports, Provider: r.w.provider}
	before := pr.Relays("before the restart", jobID)
	o.Before = keeperRelaysOf(before, r.h.StatusPID("controller"))
	l0, ok := pr.Lease()
	if !ok || l0.Holder == "" {
		r.t.Fatalf("the Provider's Lease has no holder: %+v", l0)
	}
	ctl, _, err := keeperprobe.PinEntry(r.t, r.h.State, "controller", before.Sup)
	if err != nil {
		r.t.Fatalf("pin the controller: %v", err)
	}
	was := r.current(in)
	o.RestartsBefore = len(was.Status.Restarts)
	var agentBefore string
	if was.Status.Agent != nil {
		agentBefore = was.Status.Agent.AgentInstanceID
	}
	cuts0 := r.frontCuts()
	s := r.openStream(n, key, "")
	select {
	case <-s.first:
	case <-time.After(r.fc.RequestTimeout()):
		r.t.Fatalf("the stream across the restart sent no token within %s", r.fc.RequestTimeout())
	}
	time.Sleep(KeeperKillAfter)
	if err := ctl.Signal(syscall.SIGKILL); err != nil {
		r.t.Fatalf("SIGKILL to the controller %d through its pidfd: %v", ctl.PID, err)
	}
	o.KilledAt = time.Now()
	p.Logf("%s: the controller was killed by SIGKILL through its pinned pidfd, %s after the stream's first token", p.Row.Name, KeeperKillAfter)
	ctl.WaitExited(r.t, stack.WaitDeadline)
	l1 := pr.NewLeader("after the kill", ctl, l0.Holder)
	pr.WaitFence("after the kill: the next leader's", l1.Transitions)
	r.keeperAgentReported(in, o.KilledAt)
	after := pr.Relays("after the restart", jobID)
	o.After = keeperRelaysOf(after, r.h.StatusPID("controller"))
	o.OutcomeAt = time.Now()
	fence, _ := pr.Fence()
	o.LeaseBefore, o.LeaseAfter, o.HolderChanged, o.FenceAfter = l0.Transitions, l1.Transitions, l1.Holder != "" && l1.Holder != l0.Holder, fence
	o.ControllerReplaced = r.h.StatusPID("controller") != ctl.PID
	p.Logf("%s: the next controller leads, the keeper accepted its epoch %d and the relays are confirmed, %s after the kill", p.Row.Name, l1.Transitions, o.OutcomeAt.Sub(o.KilledAt).Round(time.Second))
	o.Stream = r.finish(s)
	if cuts1, ok := r.frontCutsOK(); ok {
		o.FrontCuts = int(cuts1 - cuts0)
	} else {
		o.FrontCuts = -1
	}
	if err := r.syncWatch(ctx, stack.KindInstances, "the Instance's versions across the restart"); err != nil {
		r.t.Errorf("%v", err)
	}
	for _, ob := range r.watch.Between(in.UID, o.KilledAt, time.Now()) {
		if ob.Unreachable {
			o.Unreachable = true
		}
	}
	now := r.current(in)
	o.RestartsAfter = len(now.Status.Restarts)
	o.AgentSame = was.Status.Agent != nil && now.Status.Agent != nil && now.Status.Agent.AgentInstanceID == agentBefore
	o.AgentReported = true // keeperAgentReported fails the run when the new controller holds none
	r.endDeployment(ctx, p)
	tags, err := r.runTags()
	if err != nil {
		return KeeperObs{}, err
	}
	o.Tags = tags
	o.Responses, o.Attributed = r.attribution()
	return o, nil
}

// keeperRelaysOf is what the run keeps of a view of the keeper's relays: each relay's process and fence, the keeper's E, and how many relays the
// controller with this PID runs itself.
func keeperRelaysOf(v keeperprobe.View, controllerPID int) KeeperRelaysObs {
	o := KeeperRelaysObs{Read: len(v.Relays) == 2 && len(v.States) == 2, KeeperEpoch: v.Epoch}
	if controllerPID > 0 {
		o.ControllerOwn = len(keeperprobe.RelayChildren(controllerPID))
	}
	for ch, rs := range v.States {
		pin := v.Relays[ch]
		if pin == nil {
			o.Read = false
			continue
		}
		ro := RelayObs{PID: rs.PID, StartTicks: pin.Ticks, Epoch: rs.Epoch, Seq: rs.Seq}
		switch ch {
		case agentapi.RelayControl:
			o.Control = ro
		case agentapi.RelayServing:
			o.Serving = ro
		}
	}
	return o
}

// frontCuts is the front's cuts at every hop, summed, or a failure of the run when its metrics cannot be read.
func (r *liveRun) frontCuts() float64 {
	c, ok := r.frontCutsOK()
	if !ok {
		r.t.Fatalf("the front's /metrics could not be read")
	}
	return c
}

// frontCutsOK is the sum of hpcburst_front_cuts_total over every hop, and whether the metrics could be read.
func (r *liveRun) frontCutsOK() (float64, bool) {
	ss, err := obscheck.Scrape(r.h.Ports.URL(4, "/metrics"))
	if err != nil {
		return 0, false
	}
	var sum float64
	for _, s := range obscheck.Find(ss, "hpcburst_front_cuts_total", nil) {
		sum += s.Value
	}
	return sum, true
}

// keeperAgentReported waits until the controller that started after the kill holds a report of the Instance's agent: its start time is not
// before the kill, and it has read the Instance's metrics.
func (r *liveRun) keeperAgentReported(in v1alpha1.Instance, since time.Time) {
	r.t.Helper()
	stack.WaitState(r.t, stack.WaitDeadline, "the new controller holding a report of the agent", func() (bool, string) {
		ss, err := obscheck.Scrape(r.h.Ports.URL(12, "/metrics"))
		if err != nil {
			return false, err.Error()
		}
		start := obscheck.Find(ss, "process_start_time_seconds", nil)
		if len(start) != 1 || start[0].Value < float64(since.Unix()-1) {
			return false, fmt.Sprintf("process_start_time_seconds %+v, the kill at %d", start, since.Unix())
		}
		read := obscheck.Find(ss, "hpcburst_instance_metrics_read_timestamp_seconds", map[string]string{"instance_uid": string(in.UID)})
		return len(read) > 0, fmt.Sprintf("%d read-time series for the Instance", len(read))
	})
}
