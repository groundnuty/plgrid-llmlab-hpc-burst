package scenario_test

// The keeper rehearsal's observation machinery, moved to internal/rehearsal/keeperprobe (issue #521) so that a live row can use it too:
// how the rehearsal holds and signals the stack's processes (no signal ever goes to a numeric PID: a target is pinned by a pidfd and
// verified along its chain of parents, the rule of #308), the keeper's report, its relays' ssh, the Provider's Lease and the keeper's fence.
// What stays here is the rig's own: its names for them, so that the rehearsals read as they did, and the one-line calls that give the
// package its test, host, client and ports.

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/keeperprobe"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

type (
	launcherProc     = keeperprobe.LauncherProc
	supervisorEntry  = keeperprobe.SupervisorEntry
	supervisorRecord = keeperprobe.SupervisorRecord
	stackPin         = keeperprobe.Pin
	relayLine        = keeperprobe.RelayLine
	keeperView       = keeperprobe.View
	leaseState       = keeperprobe.LeaseState
)

func readSupervisor(state string) (supervisorRecord, error) { return keeperprobe.ReadSupervisor(state) }

func pinSupervisor(t *testing.T, state string) (*stackPin, error) {
	t.Helper()
	return keeperprobe.PinSupervisor(t, state)
}

func pinChild(t *testing.T, what string, pid int, parent *stackPin, want uint64) (*stackPin, error) {
	t.Helper()
	return keeperprobe.PinChild(t, what, pid, parent, want)
}

func pinEntry(t *testing.T, state, name string, sup *stackPin) (*stackPin, supervisorEntry, error) {
	t.Helper()
	return keeperprobe.PinEntry(t, state, name, sup)
}

func relayLineOf(pid int) (relayLine, bool)   { return keeperprobe.RelayLineOf(pid) }
func relayChildren(pid int) map[int]relayLine { return keeperprobe.RelayChildren(pid) }

func providerPart(rep wire.StatusReport) wire.ProviderStatus { return keeperprobe.ProviderPart(rep) }

func upRelays(rep wire.StatusReport, jobID string) (map[agentapi.RelayChannel]wire.RelayState, map[agentapi.RelayChannel]int) {
	return keeperprobe.UpRelays(rep, jobID)
}

// probe is the rig as keeperprobe reads it.
func (r *rig) probe() keeperprobe.Probe {
	return keeperprobe.Probe{T: r.t, H: r.h, C: r.c, Ports: r.ports}
}

func (r *rig) keeperReport() wire.StatusReport { r.t.Helper(); return r.probe().Report() }

func (r *rig) keeperRelays(what, jobID string) keeperView {
	r.t.Helper()
	return r.probe().Relays(what, jobID)
}

func (r *rig) providerLease() (leaseState, bool) { return r.probe().Lease() }

func (r *rig) keeperFence() (int64, bool) { r.t.Helper(); return r.probe().Fence() }

func (r *rig) waitFence(what string, epoch int64) time.Time {
	r.t.Helper()
	return r.probe().WaitFence(what, epoch)
}

func (r *rig) newLeader(what string, old *stackPin, was string) leaseState {
	r.t.Helper()
	return r.probe().NewLeader(what, old, was)
}
