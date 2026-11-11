package scenario_test

// HBEP-6's first relay keeper item on the whole stack (Test plan, The relay
// keeper (beta), and Controller restart; issue #343, PR 5): "a controller
// restart and a leadership hand-over while streams run cut no serving stream,
// and the relays' E doesn't change"; "no relay is replaced, and the agent
// re-registers over the same control relay". The launcher runs one controller
// copy under its supervisor, so the two events are the two ends a leader can
// have, as in internal/relaykeeper/keeperproc's real-controller tests
// (ctlhandover_test.go): a restart is a SIGKILL, after which the Provider's
// Lease expires before the supervisor's next copy takes it; a hand-over is a
// SIGTERM, on which the leader stops sending sets, releases the Lease, and the
// next copy takes it at once. The standby that leads at once is PR 4's
// real-controller test; here the next leader is the supervisor's next copy.

import (
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// TestRehearsalKeeperControllerRestartAndHandOverCutNoServingStream: with the
// relay keeper, a controller restart and a leadership hand-over, each with a
// serving stream running across it, cut no stream: it arrives whole and in
// order, the front counts no cut, the relays' ssh are the very processes
// pinned before, the keeper's E and the relays' (E, N) are unchanged, and the
// agent registers with the next leader over the same control relay.
//
//livecheck:none the keeper rehearsal; hack/rehearsal.sh runs it, with REHEARSAL_RUN naming it
func TestRehearsalKeeperControllerRestartAndHandOverCutNoServingStream(t *testing.T) {
	r := startKeeperRig(t, rigSetup{})
	in, _ := r.keeperStart()
	key := r.fc.KeyFor(in)
	v := r.keeperRelays("after the start", in.Status.JobID)
	ref := r.quietReference("the quiet reference stream", in, key, nil)
	r.keeperControllerEvent("restart", syscall.SIGKILL, in, key, ref, v)
	r.keeperControllerEvent("hand-over", syscall.SIGTERM, in, key, ref, v)
}

// keeperControllerEvent ends the leading controller with sig, through its
// pinned pidfd, while a serving stream runs, and checks what HBEP-6 says the
// keeper leaves standing.
func (r *rig) keeperControllerEvent(event string, sig syscall.Signal, in *v1alpha1.Instance, key, ref string, v keeperView) {
	r.t.Helper()
	defer r.stepTimer("keeper " + event)()
	r.beforeDrain("keeper "+event, in)
	before, ok := r.instance(in.Name)
	if !ok || before.Status.Phase != v1alpha1.InstanceReady || before.Status.Agent == nil {
		r.t.Fatalf("%s: needs a ready, registered Instance %s: %+v", event, in.Name, before.Status)
	}
	l0, ok := r.providerLease()
	if !ok || l0.holder == "" {
		r.t.Fatalf("%s: the Provider's Lease has no holder: %+v", event, l0)
	}
	r.waitFence(event+", before it", l0.transitions)
	rep0 := r.keeperReport()
	st0, _ := upRelays(rep0, v.job)
	refused0 := refusedOf(providerPart(rep0))
	cuts0, ok := r.frontCounter("hpcburst_front_cuts_total")
	if !ok {
		r.t.Fatalf("%s: the front's /metrics did not answer", event)
	}
	ctl, _, err := pinEntry(r.t, r.h.State, "controller", v.sup)
	if err != nil {
		r.t.Fatalf("%s: pin the controller: %v", event, err)
	}
	first, done := r.fc.StartStream(keeperStreamBody(), key)
	r.firstToken(event, first)
	if err := ctl.signal(sig); err != nil {
		r.t.Fatalf("%s: %s to the controller %d through its pidfd: %v", event, sig, ctl.pid, err)
	}
	at := time.Now()
	ctl.waitExited(r.t, stack.WaitDeadline)
	l1 := r.newLeader(event, ctl, l0.holder)
	ledAt := time.Now()
	if l1.transitions != l0.transitions+1 {
		r.t.Errorf("%s: the Lease's leaseTransitions went %d -> %d; client-go adds one on a holder change", event, l0.transitions, l1.transitions)
	}
	acceptedAt := r.waitFence(event+", the next leader's", l1.transitions)
	reportedAt := r.agentReported(event, in, at)
	r.t.Logf("%s: the next controller led %s after the %s, the keeper accepted its epoch %d %s after, the agent reported to it %s after",
		event, ledAt.Sub(at).Round(100*time.Millisecond), sig, l1.transitions, acceptedAt.Sub(at).Round(100*time.Millisecond), reportedAt.Sub(at).Round(100*time.Millisecond))

	// The stream ran across the event, from before the signal to after the
	// next leader's epoch was accepted and the agent reported to it, and
	// arrived whole and in order.
	_, row := r.wholeStream(event, r.keeperStream(event, done), in, ref)
	rs, re := parseRecordTime(row.StartTime), parseRecordTime(row.EndTime)
	if !rs.Before(at) || !re.After(acceptedAt) || !re.After(reportedAt) {
		r.t.Errorf("%s: the stream ran %s to %s, not across the signal %s, the accepted epoch %s and the agent's report %s", event, row.StartTime, row.EndTime,
			at.UTC().Format(time.RFC3339Nano), acceptedAt.UTC().Format(time.RFC3339Nano), reportedAt.UTC().Format(time.RFC3339Nano))
	}
	if cuts, _ := r.frontCounter("hpcburst_front_cuts_total"); cuts != cuts0 {
		r.t.Errorf("%s: the front counted %g cuts across it, want none", event, cuts-cuts0)
	}

	// What ended was the controller, as the supervisor saw it end.
	rec, err := readSupervisor(r.h.State)
	e, _ := rec.entryOf("controller")
	switch {
	case err != nil || e.LastExit == nil:
		r.t.Errorf("%s: the supervisor records no exit of the controller: %v %+v", event, err, e)
	case sig == syscall.SIGTERM && e.LastExit.Code != 0:
		r.t.Errorf("%s: the controller exited %+v after SIGTERM, want 0: a clean stop, releasing its Lease", event, *e.LastExit)
	case sig == syscall.SIGKILL && !strings.Contains(e.LastExit.Detail, "killed"):
		r.t.Errorf("%s: the controller's exit %+v, want killed", event, *e.LastExit)
	}

	// No relay was replaced: the same ssh processes, still the same keeper's
	// children, at the same (E, N), started once; the keeper's E is the same,
	// as its report and the launcher's status read it; nothing was refused.
	if err := v.keeper.stillChild(v.sup); err != nil {
		r.t.Errorf("%s: the relay keeper: %v", event, err)
	}
	rep1 := r.keeperReport()
	st1, n := upRelays(rep1, v.job)
	if rep1.KeeperEpoch != v.epoch {
		r.t.Errorf("%s: the keeper's E is %d, was %d", event, rep1.KeeperEpoch, v.epoch)
	}
	for _, ch := range agentapi.RelayChannels {
		if err := v.relays[ch].stillChild(v.keeper); err != nil {
			r.t.Errorf("%s: the %s relay was replaced: %v", event, ch, err)
		}
		a, b := st0[ch], st1[ch]
		if n[ch] != 1 || b.PID != a.PID || b.Epoch != a.Epoch || b.Seq != a.Seq || b.Starts != a.Starts || b.Epoch != v.epoch {
			r.t.Errorf("%s: the keeper's %s relay is %+v (%d up), was %+v: want the same process at the same (E, N), with no start", event, ch, b, n[ch], a)
		}
	}
	if got := refusedOf(providerPart(rep1)); !sameCounts(got, refused0) {
		r.t.Errorf("%s: the keeper's refusals went %v -> %v", event, refused0, got)
	}
	if st, _, _ := r.statusJSON(); !keeperAtE(st, v.keeper.pid, v.epoch) {
		r.t.Errorf("%s: hpcburst status does not show the keeper %d at E %d: %+v", event, v.keeper.pid, v.epoch, st.Controllers)
	}
	if ctl := r.h.StatusPID("controller"); len(relayChildren(ctl)) != 0 {
		r.t.Errorf("%s: the next controller %d runs relays of its own: %+v", event, ctl, relayChildren(ctl))
	}

	// The agent: the same registration, never judged unreachable, nothing
	// restarted, and the Instance ready throughout.
	now, _ := r.instance(in.Name)
	if now.Status.Agent == nil || now.Status.Agent.AgentInstanceID != before.Status.Agent.AgentInstanceID || len(now.Status.Restarts) != len(before.Status.Restarts) {
		r.t.Errorf("%s: the agent's registration %+v and restarts %+v, were %+v and %+v", event, now.Status.Agent, now.Status.Restarts, before.Status.Agent, before.Status.Restarts)
	}
	r.syncWatch(stack.KindInstances, "the Instance's versions across the "+event)
	for _, o := range r.watch.Between(in.UID, at, time.Now()) {
		if o.Unreachable {
			r.t.Errorf("%s: Instance %s was judged AgentUnreachable at %s", event, in.Name, o.At.Format(time.RFC3339Nano))
		}
		if o.Phase != v1alpha1.InstanceReady {
			r.t.Errorf("%s: Instance %s left ready (%s)", event, in.Name, o.Phase)
		}
	}
}

// keeperAtE reports whether a launcher status shows the relay keeper pid
// running and answering its ping at E.
func keeperAtE(st launcherStatus, pid int, epoch int64) bool {
	for _, c := range st.Controllers {
		if c.Name == "relaykeeper" {
			return c.Running && c.PID == pid && c.PingOK != nil && *c.PingOK && c.KeeperEpoch == epoch
		}
	}
	return false
}
