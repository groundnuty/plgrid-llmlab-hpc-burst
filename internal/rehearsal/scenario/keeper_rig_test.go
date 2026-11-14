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
	"strconv"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// portsKeeper are the keeper rehearsal's ports, in #343's block (28000-28039):
// the launcher's 28020-28032 (the dial-home listener at its loopback default,
// base+7) and the front's client listener 28033.
var portsKeeper = stack.Ports{Base: 28020, ClientAddr: "127.0.0.1:28033"}

// keeperTiming is the keeper rehearsal's clock: a 25 min lease (under the
// partition's 30 min), and a maxRequestDuration of 20 min, so an open stream
// (openTokens) is never cut by the front's deadline before its client ends
// it. Its drain window is keeperDrainWindow, the profile's 4 min drainWindow,
// shorter than maxRequestDuration plus drainMargin (api/v1alpha1,
// InstanceDrainWindow), so a test's events, an upgrade, a down and a restore
// among them, all come before the primary's drain start, 21 min after its
// start. HBEP-7's lead (4 min plus 0 plus the 4 min startTimeout) is under the
// lease.
var keeperTiming = stack.Timing{Lease: "25m", MaxRequest: "20m", DrainMargin: "30s", DrainWindow: "4m", StopGrace: "5s", AgentTimeout: "20s", StartTimeout: "4m"}

// keeperDrainWindow is keeperTiming's drain window, from its spec alone.
const keeperDrainWindow = 4 * time.Minute

// keeperMaxModelLen is the keeper deployment's engine max-model-len: room for
// an open stream's prompt and openTokens.
const keeperMaxModelLen = "65536"

// keeperBeforeDrain fails an event that would start less than two minutes
// before the primary's drain start (keeperDrainWindow before its lease end).
func (r *rig) keeperBeforeDrain(event string, in *v1alpha1.Instance) {
	r.t.Helper()
	now, _ := r.instance(in.Name)
	if now.Status.LeaseEnd == nil {
		r.t.Fatalf("%s: Instance %s has no lease end", event, in.Name)
	}
	if left := time.Until(now.Status.LeaseEnd.Add(-keeperDrainWindow)); left < 2*time.Minute {
		r.t.Fatalf("%s would start %s before the primary's drain start: lengthen the lease", event, left.Round(time.Second))
	}
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
	if s.bulkPath {
		o = append(o, stack.BulkPath)
	}
	if s.instanceRouter {
		o = append(o, stack.InstanceRouter)
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
// Provider is Ready, and returns its primary Instance, ready on a job that runs
// in the fake cluster, and the primary's replacement (HBEP-7: created at the
// primary's start when no queue wait is observed), once both are ready and
// registered at the front, so the ring holds both before a key is chosen.
func (r *rig) keeperStart() (primary, repl *v1alpha1.Instance) {
	r.t.Helper()
	defer r.stepTimer("keeper start")()
	r.providerReady()
	stack.Create(r.t, r.c, stack.ServedDeploymentAs(stack.Served{Name: stack.MDName, Model: stack.Model, Provider: stack.ProviderName, MaxModelLen: keeperMaxModelLen}, keeperTiming, r.engineEnv...))
	var ready *v1alpha1.Instance
	last := ""
	stack.WaitFor(r.t, 2*stack.WaitDeadline, "the primary Instance ready on a running job", func() bool {
		for _, in := range r.instances() {
			if in.Status.Phase != v1alpha1.InstanceReady || in.Status.JobID == "" || in.Annotations[v1alpha1.AnnotationPredecessor] != "" {
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
	rp := r.replacementReady(ready)
	return ready, &rp
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
