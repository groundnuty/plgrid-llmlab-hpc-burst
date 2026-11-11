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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"golang.org/x/sys/unix"

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

// pingKeeper asks the keeper at socket for its E, within timeout, and returns the PID of the
// process that answered: the peer of the connection, from SO_PEERCRED, the process that bound the
// listener. A pathname alone names no process, since /proc/net/unix keeps an unlinked listener's
// path, so the caller checks that PID against the recorded keeper (the delta review of PR 361).
func pingKeeper(socket string, timeout time.Duration) (epoch int64, peer int, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var d net.Dialer
	c, err := d.DialContext(ctx, "unix", socket)
	if err != nil {
		return 0, 0, err
	}
	defer c.Close()
	uc, ok := c.(*net.UnixConn)
	if !ok {
		return 0, 0, fmt.Errorf("the connection to %s is not a Unix socket", socket)
	}
	raw, err := uc.SyscallConn()
	if err != nil {
		return 0, 0, err
	}
	var cred *unix.Ucred
	var credErr error
	if err := raw.Control(func(fd uintptr) { cred, credErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED) }); err != nil {
		return 0, 0, err
	}
	if credErr != nil {
		return 0, 0, fmt.Errorf("the peer of %s: %w", socket, credErr)
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = c.SetDeadline(dl)
	}
	if err := wire.Write(c, wire.KindPing, struct{}{}); err != nil {
		return 0, int(cred.Pid), err
	}
	env, err := wire.Read(wire.NewReader(c))
	if err != nil {
		return 0, int(cred.Pid), fmt.Errorf("the reply to the ping: %w", err)
	}
	if env.Kind != wire.KindPong {
		return 0, int(cred.Pid), fmt.Errorf("the reply to the ping is %s, not pong", env.Kind)
	}
	var pong wire.Pong
	if err := env.Decode(&pong); err != nil {
		return 0, int(cred.Pid), err
	}
	if pong.KeeperEpoch < relaykeeper.EpochBase {
		return 0, int(cred.Pid), fmt.Errorf("the keeper answered E %d, below 2^31: not a keeper's epoch", pong.KeeperEpoch)
	}
	return pong.KeeperEpoch, int(cred.Pid), nil
}

// checkKeeperHealth is checkEntry's part for the keeper, an entry with a socket for its health
// (the review of PR 361 and its delta made each of these strict):
//   - the keeper's own descriptors are read, and any failure to read them makes it unhealthy:
//     an unreadable /proc/<pid>/fd is unknown, never "no port";
//   - the keeper process itself listens on no port, TCP or UDP, on any address. Its relays' ssh
//     children are left out: they are ssh clients, whose sockets the keeper's checks do not own;
//   - a ping on its health socket is answered with pong{E} within 1 s, by the keeper itself: the
//     connection's peer (SO_PEERCRED) is the recorded keeper's PID, and the pidfd still names that
//     process after the answer, so no other process bound at that path counts;
//
// It returns whether the ping was answered by the keeper; eh.KeeperEpoch is the E it answered.
func checkKeeperHealth(h *procHandle, e EntryRecord, eh *EntryHealth, add func(string, ...any)) bool {
	pid := e.Proc.PID
	ok := false
	eh.PingOK = &ok
	inodes, err := keeperFdInodes(pid)
	if err != nil {
		add("cannot read the descriptors of the relay keeper pid %d, so its sockets are unknown: %v", pid, err)
		return false
	}
	if v, d := h.verify(); v != Ours {
		add("pid %d changed while its descriptors were read: %s", pid, d)
		return false
	}
	all, err := boundSockets()
	if err != nil {
		add("cannot list the host's sockets: %v", err)
		return false
	}
	var bound []string
	for _, s := range all {
		if inodes[s.inode] {
			bound = append(bound, s.key())
		}
	}
	if len(bound) > 0 {
		sort.Strings(bound)
		add("pid %d binds a port, which the relay keeper never does: %v", pid, bound)
		return false
	}
	epoch, peer, err := pingKeeper(e.HealthSocket, keeperPingTimeout)
	if err != nil {
		add("ping on %s within %s: %v", e.HealthSocket, keeperPingTimeout, err)
		return false
	}
	if peer != pid {
		add("the ping on %s was answered by pid %d, not the relay keeper pid %d", e.HealthSocket, peer, pid)
		return false
	}
	if v, d := h.verify(); v != Ours {
		add("the relay keeper pid %d changed during its ping: %s", pid, d)
		return false
	}
	ok = true
	eh.KeeperEpoch = epoch
	return true
}

// keeperFdInodes reads the socket inodes the process pid itself holds, from /proc/<pid>/fd. A
// directory that cannot be read, or a descriptor link that fails, is an error, never an empty
// answer (the review of PR 361: fdSocketInodes, alpha's, drops them). A descriptor that closes
// between the directory's read and its link's could hide a listener duplicated into a new one, so
// the whole snapshot is read again once, and a second such change is an error too (its delta). A
// variable, so a test can make it fail.
var keeperFdInodes = func(pid int) (map[string]bool, error) {
	inodes, err := fdInodesOnce(pid)
	if errors.Is(err, errFdClosed) {
		if inodes, err = fdInodesOnce(pid); errors.Is(err, errFdClosed) {
			return nil, fmt.Errorf("the descriptors of pid %d changed while they were read, twice: %w", pid, err)
		}
	}
	return inodes, err
}

// fdReadlink is os.Readlink; a test replaces it, to close a descriptor mid-snapshot.
var fdReadlink = os.Readlink

// errFdClosed is a descriptor that closed between /proc/<pid>/fd's read and its link's.
var errFdClosed = errors.New("a descriptor closed while the snapshot was read")

// fdInodesOnce is one snapshot of keeperFdInodes.
func fdInodesOnce(pid int) (map[string]bool, error) {
	fds, err := os.ReadDir(procPath(pid, "fd"))
	if err != nil {
		return nil, err
	}
	inodes := map[string]bool{}
	for _, fd := range fds {
		l, err := fdReadlink(procPath(pid, "fd/"+fd.Name()))
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("fd %s: %w", fd.Name(), errFdClosed)
		}
		if err != nil {
			return nil, err
		}
		if ino, ok := strings.CutPrefix(l, "socket:["); ok {
			inodes[strings.TrimSuffix(ino, "]")] = true
		}
	}
	return inodes, nil
}

// allocations is the resource of the framework's Allocations.
var allocations = v1alpha1.GroupVersion.WithResource("allocations")

// checkKeeperSwitchBack is up's switch-back guard (HBEP-6, Compatibility and upgrades, as amended
// 10 Nov 2026 with the keeper retired marker).
//   - With the keeper asked for (up --relay-keeper), it removes the retired marker, synced, BEFORE
//     the keeper starts, and refuses when it cannot: the keeper's E then continues above its last
//     run's, from its state file.
//   - With no keeper state file, the keeper never ran here, and it passes.
//   - While the retired marker exists, it passes whatever runs: all of it started under alpha,
//     after a drain the marker recorded.
//   - Otherwise it reads the Allocations. A failed read refuses and writes nothing (fail closed).
//     With none Running, it writes the marker atomically beside the keeper's state file and
//     passes, saying so, and saying when it ignored a state file that does not load. With
//     Allocations Running, it refuses, naming them and the way out; a state file the keeper would
//     refuse to load refuses too, since it cannot show the keeper never ran.
//
// Host-dialed mode is the launcher's, not an Allocation's (api/v1alpha1 has no per-Allocation
// mode), so every Running Allocation of the host counts: conservative, it refuses more, never
// less, and the marker is written only on a read that saw none.
func checkKeeperSwitchBack(ctx context.Context, cfg *rest.Config, l layout, relayKeeper bool, log io.Writer) error {
	marker := retiredMarker(l)
	if relayKeeper {
		removed, err := removeRetiredMarker(l)
		if err != nil {
			return fmt.Errorf("the relay keeper's retired marker %s could not be removed, so the keeper is not started: %w", marker, err)
		}
		if removed {
			logf(log, "removed the relay keeper's retired marker %s: the keeper runs again", marker)
		}
		return nil
	}
	path := relaykeeper.StatePath(l.dir)
	if _, err := os.Stat(path); errors.Is(err, fs.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("the relay keeper's state file %s: %w", path, err)
	}
	if _, err := os.Stat(marker); err == nil {
		logf(log, "the relay keeper was retired here (%s): its switch back is done, and up goes on", marker)
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("the relay keeper's retired marker %s: %w", marker, err)
	}
	running, err := runningAllocations(ctx, cfg)
	if err != nil {
		return fmt.Errorf("list the running Allocations for the relay keeper's switch back (nothing was recorded; up refuses until a read succeeds): %w", err)
	}
	ks, loadErr := relaykeeper.LoadState(path)
	if len(running) == 0 {
		if loadErr != nil {
			logf(log, "warning: the relay keeper's state file %s cannot be read (%v); no Allocation runs, so nothing is handed back from the keeper", path, loadErr)
		}
		if err := writeRetiredMarker(l, time.Now()); err != nil {
			return fmt.Errorf("no Allocation runs, but the relay keeper's retired marker %s could not be written: %w", marker, err)
		}
		logf(log, "no Allocation runs: the relay keeper is retired (%s); until up --relay-keeper removes the marker, this guard passes", marker)
		return nil
	}
	if loadErr != nil {
		return fmt.Errorf("the relay keeper's state file %s cannot be read, so it cannot show the keeper never ran (%v), and %d Allocation(s) are running: %v. "+
			"Release those Allocations first; with none running, up goes on and retires the keeper", path, loadErr, len(running), running)
	}
	return fmt.Errorf("the relay keeper ran on this host (%s, %d start(s)) and %d Allocation(s) are running: %v. "+
		"Starting without --relay-keeper would hand their relays back to the controller, whose lower epochs "+
		"their jobs refuse as superseded (HBEP-6, Compatibility and upgrades). Run up with --relay-keeper, "+
		"or release those Allocations first", path, ks.Starts, len(running), running)
}

// retiredMarker is the keeper retired marker, beside the keeper's state file (HBEP-6,
// Compatibility and upgrades). down keeps it, with the keeper's state file.
func retiredMarker(l layout) string {
	return filepath.Join(agentapi.Host{StateDir: l.dir}.RelayKeeperDir(), "retired")
}

// writeRetiredMarker writes the marker atomically: a temporary file, written and synced, renamed
// into place, and the directory synced, so it either exists whole or not at all after a crash.
func writeRetiredMarker(l layout, at time.Time) error {
	path := retiredMarker(l)
	b, err := json.Marshal(struct {
		RetiredAt time.Time `json:"retired_at"`
		Why       string    `json:"why"`
	}{at.UTC(), "a switch back found no host-dialed Allocation running"})
	if err != nil {
		return err
	}
	if err := writeFileAtomic(path, append(b, '\n'), 0o600); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

// removeRetiredMarker removes the marker, and syncs its directory; removed says whether there was
// one.
func removeRetiredMarker(l layout) (removed bool, err error) {
	path := retiredMarker(l)
	if err := os.Remove(path); errors.Is(err, fs.ErrNotExist) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, syncDir(filepath.Dir(path))
}

// pidfdProbe checks that the kernel has pidfd_open, as hostdial's NewOwner does: up --relay-keeper
// refuses without it, so the keeper never starts where a start could get no pidfd (the launcher's
// own spawn would then stop it by number; the review of PR 361). A variable, so a test can fail it.
var pidfdProbe = func() error {
	fd, err := unix.PidfdOpen(os.Getpid(), 0)
	if err != nil {
		return err
	}
	return unix.Close(fd)
}

// runningAllocations names every Allocation in phase Running, as namespace/name, sorted, from a
// COMPLETE list: every page, following each continue token. Anything else is an error, a server
// that does not serve Allocations (404) included, so neither the guard nor the keeper retired
// marker ever reads "none Running" from a list that could not show it (the delta review of PR
// 361: a 404 had read as none, and a first page as the whole list).
func runningAllocations(ctx context.Context, cfg *rest.Config) ([]string, error) {
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	want := v1alpha1.GroupVersion.String()
	var running []string
	opts := metav1.ListOptions{Limit: 500}
	for page := 1; ; page++ {
		list, err := dyn.Resource(allocations).List(ctx, opts)
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("the API server does not serve Allocations (%w); apply the framework's CRDs (up --manifests) or stop the keeper's switch back", err)
		}
		if err != nil {
			return nil, err
		}
		if list.GetAPIVersion() != want || list.GetKind() != "AllocationList" {
			return nil, fmt.Errorf("the API server answered page %d of the list of Allocations with kind %q of %q, not an AllocationList of %s", page, list.GetKind(), list.GetAPIVersion(), want)
		}
		for _, item := range list.Items {
			// Read through the API type, whose fields the CRD is generated from: the phase is the
			// observed one, status.atProvider.phase.
			var a v1alpha1.Allocation
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(item.Object, &a); err != nil {
				return nil, fmt.Errorf("Allocation %s/%s: %w", item.GetNamespace(), item.GetName(), err)
			}
			if a.Status.AtProvider.Phase == v1alpha1.AllocationRunning {
				running = append(running, item.GetNamespace()+"/"+item.GetName())
			}
		}
		if list.GetContinue() == "" {
			break
		}
		opts.Continue = list.GetContinue()
	}
	sort.Strings(running)
	return running, nil
}
