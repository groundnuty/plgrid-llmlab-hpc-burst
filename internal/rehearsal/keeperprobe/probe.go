// Package keeperprobe reads and pins what the relay keeper's stack runs: the supervisor's and the keeper's processes by pidfd, the keeper's
// report over its socket, its relays' ssh in /proc, the Provider's Lease and the keeper's fence (HBEP-6, The relay keeper and its Test
// plan). It is the keeper rehearsal's observation machinery (internal/rehearsal/scenario, #343), moved here so that a live row can use it
// too (issue #521), taking a stack.T, the host and the API client where the rehearsal's rig gave it its own. Test support: no product code
// imports it (TestOnlyTestFilesImportIt).
package keeperprobe

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
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

// Probe is what the helpers read from: the test, the stack's host (its state directory and its status), the API client and the stack's ports, and the
// name of the Provider whose relays, Lease and fence are read: the rehearsal's (stack.ProviderName) when empty, and the run's own where a run names its
// Provider after itself, as an alpha row's does (issue #521).
type Probe struct {
	T        stack.T
	H        *stack.Host
	C        client.Client
	Ports    stack.Ports
	Provider string
}

// provider is the name of the Provider the probe reads.
func (pr Probe) provider() string {
	if pr.Provider != "" {
		return pr.Provider
	}
	return stack.ProviderName
}

// keeperReport is the keeper's status report, read over its socket.
func (pr Probe) Report() wire.StatusReport {
	pr.T.Helper()
	var rep wire.StatusReport
	ctx, cancel := stack.CallCtx(pr.T)
	defer cancel()
	if err := wire.Exchange(ctx, pr.H.AgentHost().RelayKeeperSocket(), wire.KindStatus, struct{}{}, &rep); err != nil {
		pr.T.Fatalf("the relay keeper's status: %v", err)
	}
	return rep
}

// ProviderPart is the rehearsal Provider's part of a keeper report.
func ProviderPart(rep wire.StatusReport) wire.ProviderStatus { return PartOf(rep, stack.ProviderName) }

// PartOf is the named Provider's part of a keeper report, empty when the report has none.
func PartOf(rep wire.StatusReport, provider string) wire.ProviderStatus {
	for _, p := range rep.Providers {
		if p.Provider == provider {
			return p
		}
	}
	return wire.ProviderStatus{}
}

// UpRelays are the relays the keeper reports up for jobID, by channel, and
// how many it reports up for each.
func UpRelays(rep wire.StatusReport, jobID string) (map[agentapi.RelayChannel]wire.RelayState, map[agentapi.RelayChannel]int) {
	return UpRelaysOf(rep, stack.ProviderName, jobID)
}

// UpRelaysOf is UpRelays for the named Provider.
func UpRelaysOf(rep wire.StatusReport, provider, jobID string) (map[agentapi.RelayChannel]wire.RelayState, map[agentapi.RelayChannel]int) {
	out, n := map[agentapi.RelayChannel]wire.RelayState{}, map[agentapi.RelayChannel]int{}
	for _, rs := range PartOf(rep, provider).Relays {
		if rs.JobID == jobID && rs.State == wire.StateUp && rs.PID != 0 {
			out[rs.Channel] = rs
			n[rs.Channel]++
		}
	}
	return out, n
}

// RelayLine is a relay ssh's remote command as its command line carries it
// (pkg/agentapi, RelayArgs.Command): the job, the channel and the fence.
type RelayLine struct {
	Job        string
	Ch         agentapi.RelayChannel
	Epoch, Seq int64
}

// RelayLineOf parses process pid's command line as a relay's ssh, whose last
// argument is HBEP-6's srun -u --overlap line; ok is false for any other.
func RelayLineOf(pid int) (RelayLine, bool) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/cmdline")
	if err != nil {
		return RelayLine{}, false
	}
	args := strings.Split(strings.TrimRight(string(b), "\x00"), "\x00")
	f := strings.Fields(args[len(args)-1])
	if len(f) < 8 || strings.Join(f[:3], " ") != "srun -u --overlap" || !strings.HasPrefix(f[3], "--jobid=") || f[4] != "--ntasks=1" || f[6] != agentapi.RelaySubcommand {
		return RelayLine{}, false
	}
	l := RelayLine{Job: strings.TrimPrefix(f[3], "--jobid="), Ch: agentapi.RelayChannel(f[7]), Epoch: -1, Seq: -1}
	for i := 8; i+1 < len(f); i++ {
		switch f[i] {
		case "--epoch":
			l.Epoch, _ = strconv.ParseInt(f[i+1], 10, 64)
		case "--seq":
			l.Seq, _ = strconv.ParseInt(f[i+1], 10, 64)
		}
	}
	return l, true
}

// RelayChildren are the relay ssh children of pid, by PID.
func RelayChildren(pid int) map[int]RelayLine {
	out := map[int]RelayLine{}
	for _, c := range stack.ChildrenOf(pid) {
		if l, ok := RelayLineOf(c); ok {
			out[c] = l
		}
	}
	return out
}

// View is the keeper as a test holds it: the supervisor and the keeper
// pinned, the keeper's E, and one job's two relays, their ssh pinned and their
// state as the keeper reported it.
type View struct {
	Sup, Keeper *Pin
	Epoch       int64
	Job         string
	Relays      map[agentapi.RelayChannel]*Pin
	States      map[agentapi.RelayChannel]wire.RelayState
}

// keeperRelays waits until the keeper runs exactly one relay of each channel
// for jobID, each its own ssh child running the job's relay line at the
// keeper's E and the sequence it reports, and pins them, the keeper and the
// supervisor. The controller then runs no relay of its own.
func (pr Probe) Relays(what, jobID string) View {
	pr.T.Helper()
	sup, err := PinSupervisor(pr.T, pr.H.State)
	if err != nil {
		pr.T.Fatalf("%s: pin the supervisor: %v", what, err)
	}
	keeper, _, err := PinEntry(pr.T, pr.H.State, "relaykeeper", sup)
	if err != nil {
		pr.T.Fatalf("%s: pin the relay keeper: %v", what, err)
	}
	v := View{Sup: sup, Keeper: keeper, Job: jobID, Relays: map[agentapi.RelayChannel]*Pin{}}
	stack.WaitState(pr.T, stack.WaitDeadline, what+": both relays of job "+jobID+" up under the keeper, as its own ssh children", func() (bool, string) {
		rep := pr.Report()
		up, n := UpRelaysOf(rep, pr.provider(), jobID)
		kids := RelayChildren(keeper.PID)
		v.Epoch, v.States = rep.KeeperEpoch, up
		for _, ch := range agentapi.RelayChannels {
			rs, ok := up[ch]
			if !ok || n[ch] != 1 {
				return false, fmt.Sprintf("the keeper reports %d %s relays up for the job: %+v (it reports on %s)", n[ch], ch, PartOf(rep, pr.provider()).Relays, providersOf(rep))
			}
			l, child := kids[rs.PID]
			if !child || l.Job != jobID || l.Ch != ch || l.Epoch != rep.KeeperEpoch || rs.Epoch != rep.KeeperEpoch || l.Seq != rs.Seq {
				return false, fmt.Sprintf("the %s relay %+v: the keeper's children %+v, its E %d", ch, rs, kids, rep.KeeperEpoch)
			}
		}
		return true, ""
	})
	if v.Epoch < relaykeeper.EpochBase {
		pr.T.Errorf("%s: the keeper's E is %d, below 2^31", what, v.Epoch)
	}
	for ch, rs := range v.States {
		p, err := PinChild(pr.T, "the "+string(ch)+" relay's ssh", rs.PID, keeper, 0)
		if err != nil {
			pr.T.Fatalf("%s: pin the %s relay's ssh as the keeper's child: %v", what, ch, err)
		}
		v.Relays[ch] = p
	}
	if ctl := pr.H.StatusPID("controller"); ctl == 0 {
		pr.T.Errorf("%s: no controller runs", what)
	} else if own := RelayChildren(ctl); len(own) != 0 {
		pr.T.Errorf("%s: the controller %d runs relays of its own, %+v: in keeper mode they are the keeper's", what, ctl, own)
	}
	pr.T.Logf("%s: the keeper %d at E %d runs job %s's relays: control %d (N %d), serving %d (N %d)", what, keeper.PID, v.Epoch, jobID,
		v.States[agentapi.RelayControl].PID, v.States[agentapi.RelayControl].Seq, v.States[agentapi.RelayServing].PID, v.States[agentapi.RelayServing].Seq)
	return v
}

// LeaseState is the Provider's Lease as the API server holds it.
type LeaseState struct {
	Holder      string
	Transitions int64
}

// providerLease reads the Provider's Lease, provider-<name>; ok is false when it
// cannot be read.
func (pr Probe) Lease() (LeaseState, bool) {
	var l coordinationv1.Lease
	ctx, cancel := stack.CallCtx(pr.T)
	defer cancel()
	name := v1alpha1.ProviderLeaseName(pr.provider())
	if err := pr.C.Get(ctx, types.NamespacedName{Namespace: v1alpha1.ControllerNamespace, Name: name}, &l); err != nil {
		return LeaseState{}, false
	}
	var s LeaseState
	if l.Spec.HolderIdentity != nil {
		s.Holder = *l.Spec.HolderIdentity
	}
	if l.Spec.LeaseTransitions != nil {
		s.Transitions = int64(*l.Spec.LeaseTransitions)
	}
	return s, true
}

// keeperFence is the keeper's highest accepted Lease epoch for the Provider,
// from its state file, and whether it holds one.
func (pr Probe) Fence() (int64, bool) {
	pr.T.Helper()
	ks, err := relaykeeper.LoadState(relaykeeper.StatePath(pr.H.State))
	if err != nil {
		pr.T.Fatalf("the relay keeper's state file: %v", err)
	}
	f, ok := ks.Providers[pr.provider()]
	return f.HighestEpoch, ok
}

// waitFence waits until the keeper's state file holds epoch as the Provider's
// highest accepted Lease epoch: a set or acknowledgement of that leader was
// accepted. It returns when that was first seen.
func (pr Probe) WaitFence(what string, epoch int64) time.Time {
	pr.T.Helper()
	stack.WaitState(pr.T, stack.WaitDeadline, what+": the keeper accepting Lease epoch "+strconv.FormatInt(epoch, 10), func() (bool, string) {
		got, ok := pr.Fence()
		return ok && got == epoch, fmt.Sprintf("the keeper's fence %d (held %v)", got, ok)
	})
	return time.Now()
}

// newLeader waits until a controller other than the one pinned as old leads
// the Provider's Lease, under a holder other than was, and serves /readyz; it
// returns the Lease.
func (pr Probe) NewLeader(what string, old *Pin, was string) LeaseState {
	pr.T.Helper()
	var l LeaseState
	stack.WaitState(pr.T, stack.WaitDeadline, what+": a new controller leading the Provider's Lease", func() (bool, string) {
		l, _ = pr.Lease()
		rec, err := ReadSupervisor(pr.H.State)
		e, running := rec.EntryOf("controller")
		fresh := err == nil && running && (old == nil || e.Proc.PID != old.PID || e.Proc.StartTicks != old.Ticks)
		return fresh && l.Holder != "" && l.Holder != was && stack.HTTPStatus(pr.Ports.URL(6, "/readyz")) == http.StatusOK,
			fmt.Sprintf("Lease %+v, a new controller %v", l, fresh)
	})
	return l
}

// providersOf names the Providers a keeper report has a part for, and how many relays each reports: what a wait that finds none for the Provider it
// reads says it saw instead.
func providersOf(rep wire.StatusReport) string {
	var parts []string
	for _, p := range rep.Providers {
		parts = append(parts, fmt.Sprintf("%s: %d relays", p.Provider, len(p.Relays)))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}
