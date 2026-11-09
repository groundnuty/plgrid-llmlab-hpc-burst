package scenario_test

// HBEP-6's host-dialed mode on the whole stack (PR #298, Test plan,
// Host-dialed mode): the controller's relays, ssh from this host to the fake
// login node running srun -u --overlap steps of the real hb-agent's "relay
// <channel>", carry the job's two connections; nothing in the job dials this
// host. The parts that need frpc, frps and the front are here: frpc logs in
// and serves through the serving relay while the agent's control traffic uses
// only the control relay; a relay killed mid-stream cuts the stream, which
// the front records as cut, and frpc logs in again once the relay is back;
// a restart of the controller stops its relays by PID and the new leader
// starts both again; and at the job's end no relay is left.

import (
	"context"
	"net/http"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// portsHostDialed are #298's block, 29780-29799: the launcher's 29780-29792
// (the dial-home listener at its loopback default, base+7) and the client
// 29793. There is no dial-home listener for the nodes: nothing dials in.
var portsHostDialed = stack.Ports{Base: 29780, ClientAddr: "127.0.0.1:29793"}

// relayProc is one of the controller's relays: its ssh process and channel.
type relayProc struct {
	pid int
	ch  agentapi.RelayChannel
}

// relaysOf lists the controller's relays for a job: its ssh children whose
// command line runs HBEP-6's relay line for the job.
func (r *rig) relaysOf(jobID string) map[agentapi.RelayChannel][]relayProc {
	r.t.Helper()
	out := map[agentapi.RelayChannel][]relayProc{}
	for _, pid := range stack.ChildrenOf(r.h.StatusPID("controller")) {
		b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/cmdline")
		if err != nil || !stack.Alive(pid) {
			continue
		}
		args := strings.Split(strings.TrimRight(string(b), "\x00"), "\x00")
		line := args[len(args)-1]
		if !strings.HasPrefix(line, "srun -u --overlap --jobid="+jobID+" --ntasks=1 ") {
			continue
		}
		for _, ch := range agentapi.RelayChannels {
			if strings.HasSuffix(line, " "+agentapi.RelaySubcommand+" "+string(ch)) {
				out[ch] = append(out[ch], relayProc{pid: pid, ch: ch})
			}
		}
	}
	return out
}

// oneRelayEach waits until the controller keeps exactly one relay per
// channel for the job, and returns them.
func (r *rig) oneRelayEach(what, jobID string) map[agentapi.RelayChannel]relayProc {
	r.t.Helper()
	var got map[agentapi.RelayChannel][]relayProc
	stack.WaitState(r.t, stack.WaitDeadline, what+": one relay per channel for job "+jobID, func() (bool, string) {
		got = r.relaysOf(jobID)
		return len(got[agentapi.RelayControl]) == 1 && len(got[agentapi.RelayServing]) == 1, strings.TrimSpace(strconv.Itoa(len(got[agentapi.RelayControl])) + " control, " + strconv.Itoa(len(got[agentapi.RelayServing])) + " serving")
	})
	return map[agentapi.RelayChannel]relayProc{agentapi.RelayControl: got[agentapi.RelayControl][0], agentapi.RelayServing: got[agentapi.RelayServing][0]}
}

// TestRehearsalHostDialed is the whole stack in host-dialed mode.
//
//livecheck:none CI's rehearsal job runs it (hack/rehearsal.sh)
func TestRehearsalHostDialed(t *testing.T) {
	r := startRigWith(t, rigSetup{mode: string(agentapi.DialHomeHostDialed), ports: portsHostDialed})
	r.engineEnv = []v1alpha1.EnvVar{{Name: "HB_ENGINE_DOUBLE", Value: "token-delay=" + tokenDelay.String()}}
	in := r.stepStart()
	jobID := in.Status.JobID
	id, _ := strconv.Atoi(jobID)
	seen := map[int]bool{}
	relays := r.oneRelayEach("after the start", jobID)
	for _, p := range relays {
		seen[p.pid] = true
	}
	// Channels stay apart: two relays, and the job dials nothing out (no ssh
	// forward), so frpc's login and the agent's registration, which step (a)
	// saw, crossed the relays.
	if relays[agentapi.RelayControl].pid == relays[agentapi.RelayServing].pid {
		t.Fatalf("both channels run in one relay %d", relays[agentapi.RelayControl].pid)
	}
	if fwd := r.jobSSHForwards(id); len(fwd) != 0 {
		t.Errorf("the agent of job %d runs ssh forwards %v in host-dialed mode", id, fwd)
	}
	key := r.fc.KeyFor(in)
	r.relayKilledMidStream(in, key, relays, seen)
	r.hostDialedControllerRestart(in, key, seen)
	// No Instance has ended yet; the restarted controller serves the step that
	// ends them, (g), so the absence check's first read is taken from it
	// (HBEP-8's condition 2: both reads from one process).
	r.orphanBase = r.baseline("after the controller restart", keyTunnelOrphaned)
	r.stepScaleToZero("g")
	for pid := range seen {
		if stack.Alive(pid) {
			t.Errorf("relay %d outlived job %s (HBEP-6: no relay outlives the job)", pid, jobID)
		}
	}
}

// relayKilledMidStream: the serving relay's ssh is killed while a stream is
// in flight. The stream is cut and the front records it as cut; the relay
// starts again within its backoff; frpc logs in again (frps lists the proxy
// online, TunnelConnected is True and the front registers the transport);
// the next request completes on the Instance; and the agent, whose control
// relay was not touched, is never judged unreachable.
func (r *rig) relayKilledMidStream(in *v1alpha1.Instance, key string, relays map[agentapi.RelayChannel]relayProc, seen map[int]bool) {
	r.t.Helper()
	defer r.stepTimer("host-dialed kill")()
	r.beforeDrain("host-dialed kill", in)
	start := time.Now()
	first, done := r.fc.StartStream(stack.ChatBody(stack.Model, 1500, 1500, true, ""), key)
	select {
	case <-first:
	case <-time.After(stack.WaitDeadline):
		r.t.Fatal("the stream sent no first token")
	}
	old := relays[agentapi.RelayServing]
	if err := syscall.Kill(old.pid, syscall.SIGKILL); err != nil {
		r.t.Fatalf("kill the serving relay %d: %v", old.pid, err)
	}
	killed := time.Now()
	var s stack.Streamed
	select {
	case s = <-done:
	case <-time.After(r.fc.RequestTimeout()):
		r.t.Fatal("the stream did not end after its relay was killed")
	}
	row := r.fc.RowOf(s.ID)
	if row.Finish == nil || (*row.Finish != "cut" && *row.Finish != "truncated") || row.CutBy == nil {
		r.t.Errorf("the stream across the relay's kill: finish %s, cut by %s; want it recorded as cut (HBEP-6: cut and counted as cut)", strOrNil(row.Finish), strOrNil(row.CutBy))
	}
	r.t.Logf("the stream across the relay's kill ended %s after it, finish %s, cut by %s", s.EndAt.Sub(killed).Round(100*time.Millisecond), strOrNil(row.Finish), strOrNil(row.CutBy))
	var again relayProc
	stack.WaitFor(r.t, stack.WaitDeadline, "the serving relay to start again", func() bool {
		for _, p := range r.relaysOf(in.Status.JobID)[agentapi.RelayServing] {
			if p.pid != old.pid {
				again = p
				return true
			}
		}
		return false
	})
	seen[again.pid] = true
	// Within the backoff: its first wait is 1 s and at most 30 s, then an ssh
	// and srun set-up.
	r.t.Logf("the serving relay %d started again %s after the kill", again.pid, time.Since(killed).Round(100*time.Millisecond))
	stack.WaitFor(r.t, stack.WaitDeadline, "frpc logged in again: the proxy online, TunnelConnected True and the front's registration back", func() bool {
		now, ok := r.instance(in.Name)
		reg, known := stack.Registered(r.ports, in)
		return r.frpsProxies()[string(in.UID)] && ok && conditionTrue(now.Status.Conditions, v1alpha1.ConditionTunnelConnected) && known && reg
	})
	rp := r.fc.SendCtx(context.Background(), "/v1/chat/completions", stack.ChatBody(stack.Model, 4, 0, false, ""), key)
	if rp.Err != nil || rp.Status != http.StatusOK {
		r.t.Fatalf("the request after the relay came back: %d %v: %.300s", rp.Status, rp.Err, rp.Body)
	}
	if row := r.fc.RowOf(rp.ID); row.InstanceUID != string(in.UID) {
		r.t.Errorf("the request after the relay came back went to %s, not %s", row.Instance, in.Name)
	}
	if c := r.relaysOf(in.Status.JobID)[agentapi.RelayControl]; len(c) != 1 || c[0].pid != relays[agentapi.RelayControl].pid {
		r.t.Errorf("the control relay changed (%v, was %d) when only the serving relay was killed", c, relays[agentapi.RelayControl].pid)
	}
	r.syncWatch(stack.KindInstances, "the Instance's versions across the relay's kill")
	for _, o := range r.watch.Between(in.UID, start, time.Now()) {
		if o.Unreachable {
			r.t.Errorf("Instance %s was judged AgentUnreachable at %s: its control relay was not touched", in.Name, o.At.Format(time.RFC3339Nano))
		}
		if o.Phase != v1alpha1.InstanceReady {
			r.t.Errorf("Instance %s left ready (%s) across the serving relay's kill", in.Name, o.Phase)
		}
	}
}

// hostDialedControllerRestart stops the controller with TERM; the launcher's
// supervisor starts it again. The stopping controller stops its relays by
// PID; the new leader starts both relays of the running Allocation; the agent
// registers again over the new control relay without being judged
// unreachable or restarting anything; frpc logs in again over the new
// serving relay; and a request completes on the Instance.
func (r *rig) hostDialedControllerRestart(in *v1alpha1.Instance, key string, seen map[int]bool) {
	r.t.Helper()
	defer r.stepTimer("host-dialed controller restart")()
	r.beforeDrain("host-dialed controller restart", in)
	before, ok := r.instance(in.Name)
	if !ok || before.Status.Phase != v1alpha1.InstanceReady {
		r.t.Fatalf("the controller restart needs a ready Instance %s: %+v", in.Name, before.Status)
	}
	old := r.oneRelayEach("before the controller restart", in.Status.JobID)
	oldPID := r.h.StatusPID("controller")
	start := time.Now()
	if err := syscall.Kill(oldPID, syscall.SIGTERM); err != nil {
		r.t.Fatalf("TERM the controller %d: %v", oldPID, err)
	}
	stack.WaitFor(r.t, stack.WaitDeadline, "the old controller to exit", func() bool { return !stack.Alive(oldPID) })
	for ch, p := range old {
		if stack.Alive(p.pid) {
			r.t.Errorf("the %s relay %d outlived its controller %d: relays are stopped by PID", ch, p.pid, oldPID)
		}
	}
	stack.WaitFor(r.t, stack.WaitDeadline, "a new controller that serves", func() bool {
		p := r.h.StatusPID("controller")
		return p != 0 && p != oldPID && stack.HTTPStatus(r.ports.URL(6, "/readyz")) == http.StatusOK
	})
	nu := r.oneRelayEach("after the controller restart", in.Status.JobID)
	for ch, p := range nu {
		seen[p.pid] = true
		if p.pid == old[ch].pid {
			r.t.Errorf("the new leader's %s relay is the old one, %d", ch, p.pid)
		}
	}
	stack.WaitFor(r.t, stack.WaitDeadline, "frpc logged in again over the new serving relay", func() bool {
		now, ok := r.instance(in.Name)
		reg, known := stack.Registered(r.ports, in)
		return r.frpsProxies()[string(in.UID)] && ok && conditionTrue(now.Status.Conditions, v1alpha1.ConditionTunnelConnected) && known && reg
	})
	rp := r.fc.SendCtx(context.Background(), "/v1/chat/completions", stack.ChatBody(stack.Model, 4, 0, false, ""), key)
	if rp.Err != nil || rp.Status != http.StatusOK {
		r.t.Fatalf("a request after the controller restart: %d %v: %.300s", rp.Status, rp.Err, rp.Body)
	}
	// The registration is soft state: the agent registers again with the same
	// instance ID, within one poll, and nothing restarts.
	stack.WaitFor(r.t, stack.WaitDeadline, "the agent registered with the new controller", func() bool {
		now, ok := r.instance(in.Name)
		return ok && now.Status.Agent != nil && now.Status.Agent.AgentInstanceID == before.Status.Agent.AgentInstanceID
	})
	r.syncWatch(stack.KindInstances, "the Instance's versions across the controller restart")
	for _, o := range r.watch.Between(in.UID, start, time.Now()) {
		if o.Unreachable {
			r.t.Errorf("Instance %s was judged AgentUnreachable at %s across the controller restart", in.Name, o.At.Format(time.RFC3339Nano))
		}
		if o.Phase != v1alpha1.InstanceReady {
			r.t.Errorf("Instance %s left ready (%s) across the controller restart", in.Name, o.Phase)
		}
	}
	now, _ := r.instance(in.Name)
	if len(now.Status.Restarts) != len(before.Status.Restarts) {
		r.t.Errorf("a restart was recorded across the controller restart: %+v", now.Status.Restarts)
	}
}
