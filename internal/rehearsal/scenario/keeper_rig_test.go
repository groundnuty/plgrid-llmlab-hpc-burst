package scenario_test

// The keeper rehearsal's rig (issue #343, PR 5; HBEP-6, The relay keeper and
// its Test plan). The stack is alpha's host-dialed one (hostdialed_test.go)
// with hpcburst up --relay-keeper: the launcher's supervisor runs the relay
// keeper beside the controller, the controller sends it desired sets over its
// socket, and the relays' ssh are the keeper's children. What a test reads:
//   - the launcher's records (state.json, supervisor.json) and its status;
//   - the keeper's status report over its socket, read-only and unfenced, as the
//     serving controller's collector reads it;
//   - the keeper's state file;
//   - the /proc tree, the relays' command lines and the keeper's sockets;
//   - the Provider's Lease, the front's usage records and both /metrics.
// Never a log line. Streams through the front carry the engine double's
// greedy output, which is the same for the same request every time
// (internal/agent/enginedouble, doc.go), so a stream across an event is
// compared byte for byte with the same request's reply taken when nothing
// happened: whole and in order, or, where the event cuts it, a prefix of it.

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// portsKeeper are the keeper rehearsal's ports, in #343's block (30000-30039):
// the launcher's 30020-30032 (the dial-home listener at its loopback default,
// base+7) and the front's client listener 30033.
var portsKeeper = stack.Ports{Base: 30020, ClientAddr: "127.0.0.1:30033"}

// keeperTiming is rehearsalTiming with a 25 min lease (under the partition's
// 30 min), so a test's events, an upgrade, a down and a restore among them,
// all come before the primary's drain start; the drain window is the same, so
// drainStartOf and beforeDrain hold for it.
var keeperTiming = stack.Timing{Lease: "25m", MaxRequest: "90s", DrainMargin: "30s", DrainWindow: "10m", StopGrace: "5s", AgentTimeout: "20s", StartTimeout: "4m"}

// keeperStreamTokens is each keeper stream: 3,500 tokens at tokenDelay, about
// 70 s, under maxRequestDuration (90 s) and within max-model-len 4096. It is
// long enough to run from before a controller's end until well after the next
// leader's first set is accepted, and to outlast a down.
const keeperStreamTokens = 3500

// keeperStreamBody is every keeper stream's request: one greedy prompt, so one
// output.
func keeperStreamBody() string {
	return stack.ChatBody(stack.Model, keeperStreamTokens, keeperStreamTokens, true, "")
}

// hostOptions are up's options of the rig's setup: none for an alpha stack.
func (s rigSetup) hostOptions() []stack.HostOption {
	var o []stack.HostOption
	if s.relayKeeper {
		o = append(o, stack.RelayKeeper)
	}
	if s.k3sVersion != "" {
		o = append(o, stack.K3sVersion(s.k3sVersion))
	}
	return o
}

// startKeeperRig is the rig in host-dialed mode with the relay keeper, the
// keeper's ports and timing, and the engine double's token delay. s adds the
// rest (noWatch, k3sVersion).
func startKeeperRig(t *testing.T, s rigSetup) *rig {
	t.Helper()
	s.mode, s.ports, s.relayKeeper = string(agentapi.DialHomeHostDialed), portsKeeper, true
	s.objects = func(_ *rig, arch, creds string) []client.Object {
		return []client.Object{stack.BatchProfile(arch, keeperTiming), stack.BatchProvider(creds)}
	}
	r := startRigWith(t, s)
	r.engineEnv = []v1alpha1.EnvVar{{Name: "HB_ENGINE_DOUBLE", Value: "token-delay=" + tokenDelay.String()}}
	return r
}

// keeperStart creates the served deployment with keeperTiming once the
// Provider is Ready, and returns the first Instance to be ready, whose job runs
// in the fake cluster.
func (r *rig) keeperStart() *v1alpha1.Instance {
	r.t.Helper()
	defer r.stepTimer("keeper start")()
	r.providerReady()
	stack.Create(r.t, r.c, stack.ServedDeployment(keeperTiming, r.engineEnv...))
	var ready *v1alpha1.Instance
	last := ""
	stack.WaitFor(r.t, 2*stack.WaitDeadline, "an Instance ready on a running job", func() bool {
		for _, in := range r.instances() {
			if in.Status.Phase != v1alpha1.InstanceReady || in.Status.JobID == "" {
				continue
			}
			id, err := strconv.Atoi(in.Status.JobID)
			if j, ok := r.cl.Plat.Job(id); err == nil && ok && j.State == "RUNNING" {
				in := in
				ready = &in
				return true
			}
		}
		if d := r.describe(); d != last {
			r.t.Logf("waiting:%s", d)
			last = d
		}
		return false
	})
	return ready
}

// keeperReport is the keeper's status report, read over its socket.
func (r *rig) keeperReport() wire.StatusReport {
	r.t.Helper()
	var rep wire.StatusReport
	ctx, cancel := stack.CallCtx(r.t)
	defer cancel()
	if err := wire.Exchange(ctx, r.h.AgentHost().RelayKeeperSocket(), wire.KindStatus, struct{}{}, &rep); err != nil {
		r.t.Fatalf("the relay keeper's status: %v", err)
	}
	return rep
}

// providerPart is the rehearsal Provider's part of a keeper report.
func providerPart(rep wire.StatusReport) wire.ProviderStatus {
	for _, p := range rep.Providers {
		if p.Provider == stack.ProviderName {
			return p
		}
	}
	return wire.ProviderStatus{}
}

// upRelays are the relays the keeper reports up for jobID, by channel, and
// how many it reports up for each.
func upRelays(rep wire.StatusReport, jobID string) (map[agentapi.RelayChannel]wire.RelayState, map[agentapi.RelayChannel]int) {
	out, n := map[agentapi.RelayChannel]wire.RelayState{}, map[agentapi.RelayChannel]int{}
	for _, rs := range providerPart(rep).Relays {
		if rs.JobID == jobID && rs.State == wire.StateUp && rs.PID != 0 {
			out[rs.Channel] = rs
			n[rs.Channel]++
		}
	}
	return out, n
}

// relayLine is a relay ssh's remote command as its command line carries it
// (pkg/agentapi, RelayArgs.Command): the job, the channel and the fence.
type relayLine struct {
	job        string
	ch         agentapi.RelayChannel
	epoch, seq int64
}

// relayLineOf parses process pid's command line as a relay's ssh, whose last
// argument is HBEP-6's srun -u --overlap line; ok is false for any other.
func relayLineOf(pid int) (relayLine, bool) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/cmdline")
	if err != nil {
		return relayLine{}, false
	}
	args := strings.Split(strings.TrimRight(string(b), "\x00"), "\x00")
	f := strings.Fields(args[len(args)-1])
	if len(f) < 8 || strings.Join(f[:3], " ") != "srun -u --overlap" || !strings.HasPrefix(f[3], "--jobid=") || f[4] != "--ntasks=1" || f[6] != agentapi.RelaySubcommand {
		return relayLine{}, false
	}
	l := relayLine{job: strings.TrimPrefix(f[3], "--jobid="), ch: agentapi.RelayChannel(f[7]), epoch: -1, seq: -1}
	for i := 8; i+1 < len(f); i++ {
		switch f[i] {
		case "--epoch":
			l.epoch, _ = strconv.ParseInt(f[i+1], 10, 64)
		case "--seq":
			l.seq, _ = strconv.ParseInt(f[i+1], 10, 64)
		}
	}
	return l, true
}

// relayChildren are the relay ssh children of pid, by PID.
func relayChildren(pid int) map[int]relayLine {
	out := map[int]relayLine{}
	for _, c := range stack.ChildrenOf(pid) {
		if l, ok := relayLineOf(c); ok {
			out[c] = l
		}
	}
	return out
}

// keeperView is the keeper as a test holds it: the supervisor and the keeper
// pinned, the keeper's E, and one job's two relays, their ssh pinned and their
// state as the keeper reported it.
type keeperView struct {
	sup, keeper *stackPin
	epoch       int64
	job         string
	relays      map[agentapi.RelayChannel]*stackPin
	states      map[agentapi.RelayChannel]wire.RelayState
}

// keeperRelays waits until the keeper runs exactly one relay of each channel
// for jobID, each its own ssh child running the job's relay line at the
// keeper's E and the sequence it reports, and pins them, the keeper and the
// supervisor. The controller then runs no relay of its own.
func (r *rig) keeperRelays(what, jobID string) keeperView {
	r.t.Helper()
	sup, err := pinSupervisor(r.t, r.h.State)
	if err != nil {
		r.t.Fatalf("%s: pin the supervisor: %v", what, err)
	}
	keeper, _, err := pinEntry(r.t, r.h.State, "relaykeeper", sup)
	if err != nil {
		r.t.Fatalf("%s: pin the relay keeper: %v", what, err)
	}
	v := keeperView{sup: sup, keeper: keeper, job: jobID, relays: map[agentapi.RelayChannel]*stackPin{}}
	stack.WaitState(r.t, stack.WaitDeadline, what+": both relays of job "+jobID+" up under the keeper, as its own ssh children", func() (bool, string) {
		rep := r.keeperReport()
		up, n := upRelays(rep, jobID)
		kids := relayChildren(keeper.pid)
		v.epoch, v.states = rep.KeeperEpoch, up
		for _, ch := range agentapi.RelayChannels {
			rs, ok := up[ch]
			if !ok || n[ch] != 1 {
				return false, fmt.Sprintf("the keeper reports %d %s relays up for the job: %+v", n[ch], ch, providerPart(rep).Relays)
			}
			l, child := kids[rs.PID]
			if !child || l.job != jobID || l.ch != ch || l.epoch != rep.KeeperEpoch || rs.Epoch != rep.KeeperEpoch || l.seq != rs.Seq {
				return false, fmt.Sprintf("the %s relay %+v: the keeper's children %+v, its E %d", ch, rs, kids, rep.KeeperEpoch)
			}
		}
		return true, ""
	})
	if v.epoch < relaykeeper.EpochBase {
		r.t.Errorf("%s: the keeper's E is %d, below 2^31", what, v.epoch)
	}
	for ch, rs := range v.states {
		p, err := pinChild(r.t, "the "+string(ch)+" relay's ssh", rs.PID, keeper, 0)
		if err != nil {
			r.t.Fatalf("%s: pin the %s relay's ssh as the keeper's child: %v", what, ch, err)
		}
		v.relays[ch] = p
	}
	if ctl := r.h.StatusPID("controller"); ctl == 0 {
		r.t.Errorf("%s: no controller runs", what)
	} else if own := relayChildren(ctl); len(own) != 0 {
		r.t.Errorf("%s: the controller %d runs relays of its own, %+v: in keeper mode they are the keeper's", what, ctl, own)
	}
	r.t.Logf("%s: the keeper %d at E %d runs job %s's relays: control %d (N %d), serving %d (N %d)", what, keeper.pid, v.epoch, jobID,
		v.states[agentapi.RelayControl].PID, v.states[agentapi.RelayControl].Seq, v.states[agentapi.RelayServing].PID, v.states[agentapi.RelayServing].Seq)
	return v
}

// leaseState is the Provider's Lease as the API server holds it.
type leaseState struct {
	holder      string
	transitions int64
}

// providerLease reads the Provider's Lease, provider-<name>; ok is false when it
// cannot be read.
func (r *rig) providerLease() (leaseState, bool) {
	var l coordinationv1.Lease
	ctx, cancel := stack.CallCtx(r.t)
	defer cancel()
	name := v1alpha1.ProviderLeaseName(stack.ProviderName)
	if err := r.c.Get(ctx, types.NamespacedName{Namespace: v1alpha1.ControllerNamespace, Name: name}, &l); err != nil {
		return leaseState{}, false
	}
	var s leaseState
	if l.Spec.HolderIdentity != nil {
		s.holder = *l.Spec.HolderIdentity
	}
	if l.Spec.LeaseTransitions != nil {
		s.transitions = int64(*l.Spec.LeaseTransitions)
	}
	return s, true
}

// keeperFence is the keeper's highest accepted Lease epoch for the Provider,
// from its state file, and whether it holds one.
func (r *rig) keeperFence() (int64, bool) {
	r.t.Helper()
	ks, err := relaykeeper.LoadState(relaykeeper.StatePath(r.h.State))
	if err != nil {
		r.t.Fatalf("the relay keeper's state file: %v", err)
	}
	f, ok := ks.Providers[stack.ProviderName]
	return f.HighestEpoch, ok
}

// waitFence waits until the keeper's state file holds epoch as the Provider's
// highest accepted Lease epoch: a set or acknowledgement of that leader was
// accepted. It returns when that was first seen.
func (r *rig) waitFence(what string, epoch int64) time.Time {
	r.t.Helper()
	stack.WaitState(r.t, stack.WaitDeadline, what+": the keeper accepting Lease epoch "+strconv.FormatInt(epoch, 10), func() (bool, string) {
		got, ok := r.keeperFence()
		return ok && got == epoch, fmt.Sprintf("the keeper's fence %d (held %v)", got, ok)
	})
	return time.Now()
}

// newLeader waits until a controller other than the one pinned as old leads
// the Provider's Lease, under a holder other than was, and serves /readyz; it
// returns the Lease.
func (r *rig) newLeader(what string, old *stackPin, was string) leaseState {
	r.t.Helper()
	var l leaseState
	stack.WaitState(r.t, stack.WaitDeadline, what+": a new controller leading the Provider's Lease", func() (bool, string) {
		l, _ = r.providerLease()
		rec, err := readSupervisor(r.h.State)
		e, running := rec.entryOf("controller")
		fresh := err == nil && running && (old == nil || e.Proc.PID != old.pid || e.Proc.StartTicks != old.ticks)
		return fresh && l.holder != "" && l.holder != was && stack.HTTPStatus(r.ports.URL(6, "/readyz")) == http.StatusOK,
			fmt.Sprintf("Lease %+v, a new controller %v", l, fresh)
	})
	return l
}

// tunnelBack waits until the Instance's frpc is logged in over its serving
// relay: frps lists its proxy online, TunnelConnected is True and the front
// registers its transport.
func (r *rig) tunnelBack(what string, in *v1alpha1.Instance) {
	r.t.Helper()
	stack.WaitFor(r.t, stack.WaitDeadline, what+": frpc logged in, the proxy online, TunnelConnected True and the front's registration back", func() bool {
		now, ok := r.instance(in.Name)
		reg, known := stack.Registered(r.ports, in)
		return r.frpsProxies()[string(in.UID)] && ok && conditionTrue(now.Status.Conditions, v1alpha1.ConditionTunnelConnected) && known && reg
	})
}

// runningAllocationNames are the deployment's Allocations in phase Running,
// as namespace/name.
func (r *rig) runningAllocationNames() []string {
	r.t.Helper()
	var out []string
	for _, a := range r.allocations() {
		if a.Status.AtProvider.Phase == v1alpha1.AllocationRunning {
			out = append(out, a.Namespace+"/"+a.Name)
		}
	}
	return out
}

// retiredMarker is the keeper retired marker's path (HBEP-6, Compatibility
// and upgrades): beside the keeper's state file.
func (r *rig) retiredMarker() string {
	return agentapi.Host{StateDir: r.h.State}.RelayKeeperDir() + "/retired"
}

// keeperStarts is the keeper's start count from its state file.
func (r *rig) keeperStarts(what string) int64 {
	r.t.Helper()
	ks, err := relaykeeper.LoadState(relaykeeper.StatePath(r.h.State))
	if err != nil {
		r.t.Fatalf("%s: the relay keeper's state file: %v", what, err)
	}
	return ks.Starts
}

// refusedOf copies a Provider's refusal counts, by reason.
func refusedOf(p wire.ProviderStatus) map[string]uint64 {
	out := map[string]uint64{}
	for k, v := range p.Refused {
		out[k] = v
	}
	return out
}

// sameCounts reports whether two refusal counts are the same.
func sameCounts(a, b map[string]uint64) bool {
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	for k, v := range b {
		if a[k] != v {
			return false
		}
	}
	return true
}
