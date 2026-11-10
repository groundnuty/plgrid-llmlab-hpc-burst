package launcher

// The relay keeper under the launcher (HBEP-11, Single-user host, as amended 10 Nov 2026; HBEP-6,
// The relay keeper; issue #343, PR 5). In beta's host-dialed mode, with up --relay-keeper, the
// supervisor runs the keeper, hpcburst-controller's relaykeeper subcommand, as a sibling of the
// controller, and the controller gets the keeper's socket (--relay-keeper-socket). The keeper is
// recorded by process ID and start time like every child, and its health is a live pidfd plus a
// ping answered with pong{E} within 1 s on its Unix socket, which replaces the host:port /healthz
// of the other children. It binds no port, so status also checks that it listens on none. It runs
// in a session of its own (setsid), so no signal to the supervisor's process group or a terminal's
// reaches it or the relays' ssh it starts (Opus's note on #349). down stops it with the
// supervisor and keeps its state file; restore clears its accepted Lease epochs; upgrade stops it
// with the whole group.
//
// The switch back: up without --relay-keeper refuses while the keeper's state file shows it ran and
// Allocations are running, since alpha's in-controller relays, at their lower epochs, would be
// refused as superseded by every job the keeper's relays reached (HBEP-6, Compatibility and
// upgrades). The launcher's own record does not survive down, but the keeper's state file does.
// The check runs once k3s is ready, before the controllers start.

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strconv"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// keeperEntryName is the keeper's entry in the supervised set and in supervisor.json: the
// subcommand's name.
const keeperEntryName = "relaykeeper"

// keeperPingTimeout is the keeper's health check's bound (HBEP-11: a ping answered within 1 s).
const keeperPingTimeout = time.Second

// errRelayKeeperMode is up's refusal of --relay-keeper outside host-dialed mode (HBEP-6, Test
// plan: "--relay-keeper without --front-mode host-dialed is refused").
var errRelayKeeperMode = fmt.Errorf("--relay-keeper is for --front-mode %s only: the keeper holds host-dialed relays", agentapi.DialHomeHostDialed)

// keeperEntry is the keeper's supervised entry, or nil when the launcher record does not run one.
// Its command line is the subcommand's contract (cmd/hpcburst-controller/relaykeeper.go): the
// state directory and the port base the controller is given too, so both derive the relays'
// target ports from one place (agentapi.Host.RelayChannelPorts).
func (s *State) keeperEntry(l layout) *supervised {
	if !s.RelayKeeper || s.Controllers == nil {
		return nil
	}
	h := l.host(s.Ports)
	return &supervised{
		Name: keeperEntryName, Path: s.Controllers.Binary,
		Args:         []string{keeperEntryName, "--state-dir", l.dir, "--port-base", strconv.Itoa(h.PortBase)},
		HealthSocket: h.RelayKeeperSocket(),
		Setsid:       true,
		// It stops every relay's ssh through its pidfd with hostdial's stop grace, so it gets the
		// controller's time between SIGTERM and SIGKILL.
		TermGrace: controllerTermGrace,
		WaitReady: true,
	}
}

// pingKeeper asks the keeper at socket for its E, within timeout.
func pingKeeper(socket string, timeout time.Duration) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var pong wire.Pong
	if err := wire.Exchange(ctx, socket, wire.KindPing, struct{}{}, &pong); err != nil {
		return 0, err
	}
	if pong.KeeperEpoch < relaykeeper.EpochBase {
		return 0, fmt.Errorf("the keeper answered E %d, below 2^31: not a keeper's epoch", pong.KeeperEpoch)
	}
	return pong.KeeperEpoch, nil
}

// checkKeeperHealth is checkEntry's part for the keeper, an entry with a socket for its health: it
// listens on no port, TCP or UDP, on any address, and answers a ping with pong{E} within 1 s. It
// returns whether the ping was answered; eh.KeeperEpoch is the E it answered.
func checkKeeperHealth(h *procHandle, e EntryRecord, eh *EntryHealth, add func(string, ...any)) bool {
	owned, others, err := treeBound(h)
	if err != nil {
		add("cannot list the sockets of pid %d: %v", e.Proc.PID, err)
		return false
	}
	if probs := verifyExactListeners(nil, owned, others); len(probs) > 0 {
		for _, p := range probs {
			add("pid %d binds a port, which the relay keeper never does: %s", e.Proc.PID, p)
		}
		return false
	}
	ok := true
	epoch, err := pingKeeper(e.HealthSocket, keeperPingTimeout)
	if err != nil {
		ok = false
		add("ping on %s within %s: %v", e.HealthSocket, keeperPingTimeout, err)
	} else {
		eh.KeeperEpoch = epoch
	}
	eh.PingOK = &ok
	return ok
}

// allocations is the resource of the framework's Allocations.
var allocations = v1alpha1.GroupVersion.WithResource("allocations")

// checkKeeperSwitchBack is up's switch-back guard. With the keeper asked for, or with no keeper
// state file, it passes. A state file the keeper itself would refuse to load refuses too, as the
// keeper does: it cannot show the keeper did not run. Otherwise the keeper ran on this host, and
// up refuses while any Allocation is Running, naming them. Host-dialed mode is the host's, not an
// Allocation's (api/v1alpha1 has no per-Allocation mode), so every running Allocation counts.
func checkKeeperSwitchBack(ctx context.Context, cfg *rest.Config, l layout, relayKeeper bool) error {
	if relayKeeper {
		return nil
	}
	path := relaykeeper.StatePath(l.dir)
	if _, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("the relay keeper's state file %s: %w", path, err)
	}
	ks, err := relaykeeper.LoadState(path)
	if err != nil {
		return fmt.Errorf("the relay keeper's state file %s cannot be read, so it cannot show the keeper never ran: %w", path, err)
	}
	running, err := runningAllocations(ctx, cfg)
	if err != nil {
		return fmt.Errorf("list the running Allocations for the relay keeper's switch back: %w", err)
	}
	if len(running) == 0 {
		return nil
	}
	return fmt.Errorf("the relay keeper ran on this host (%s, %d start(s)) and %d Allocation(s) are running: %v. "+
		"Starting without --relay-keeper would hand their relays back to the controller, whose lower epochs "+
		"their jobs refuse as superseded (HBEP-6, Compatibility and upgrades). Run up with --relay-keeper, "+
		"or release those Allocations first", path, ks.Starts, len(running), running)
}

// runningAllocations names every Allocation in phase Running, as namespace/name, sorted. A server
// without the Allocation CRD has none.
func runningAllocations(ctx context.Context, cfg *rest.Config) ([]string, error) {
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	list, err := dyn.Resource(allocations).List(ctx, metav1.ListOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if want := v1alpha1.GroupVersion.String(); list.GetAPIVersion() != want || list.GetKind() != "AllocationList" {
		return nil, fmt.Errorf("the API server answered the list of Allocations with kind %q of %q, not an AllocationList of %s", list.GetKind(), list.GetAPIVersion(), want)
	}
	var running []string
	for _, item := range list.Items {
		phase, _, err := unstructuredString(item.Object, "status", "phase")
		if err != nil {
			return nil, fmt.Errorf("Allocation %s/%s: %w", item.GetNamespace(), item.GetName(), err)
		}
		if phase == string(v1alpha1.AllocationRunning) {
			running = append(running, item.GetNamespace()+"/"+item.GetName())
		}
	}
	sort.Strings(running)
	return running, nil
}

// unstructuredString reads a string field of an unstructured object; a missing one is "".
func unstructuredString(obj map[string]any, fields ...string) (string, bool, error) {
	cur := any(obj)
	for i, f := range fields {
		m, ok := cur.(map[string]any)
		if !ok {
			return "", false, fmt.Errorf("%v is not an object", fields[:i])
		}
		if cur, ok = m[f]; !ok {
			return "", false, nil
		}
	}
	s, ok := cur.(string)
	if !ok {
		return "", false, fmt.Errorf("%v is not a string", fields)
	}
	return s, true, nil
}
