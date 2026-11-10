package relaykeeper

// The keeper process (issue #343, PR 3): `hpcburst-controller relaykeeper`
// runs Run. It counts its start on disk (E), makes the Runner for E, binds
// its socket, and serves until ctx ends; then it stops serving, stops every
// relay by pidfd, and returns once their processes are gone (HBEP-6, The
// relay keeper; the science agent's ruling on #343's Q9). Its relays' ssh
// start from hostdial's thread locked for good, with a KILL when this
// process dies (PR 1's OwnerOptions.Pidfd).

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// StatePath is the keeper's state file under the launcher's state directory.
func StatePath(stateDir string) string { return filepath.Join(stateDir, "relaykeeper", "state.json") }

// SocketPath is the keeper's socket under the launcher's state directory.
func SocketPath(stateDir string) string { return filepath.Join(stateDir, "relaykeeper.sock") }

// SSHDir is the keeper's directory of -F files under the launcher's state
// directory, mode 700.
func SSHDir(stateDir string) string { return filepath.Join(stateDir, "relaykeeper", "ssh") }

// DefaultSettleEvery is how often a pending end is written again with no
// request (Keeper.Settle).
const DefaultSettleEvery = 5 * time.Second

// ProcessConfig configures the keeper process.
type ProcessConfig struct {
	// StateDir is the launcher's state directory: the state file, the
	// socket and the ssh directory are under it (StatePath, SocketPath,
	// SSHDir).
	StateDir string
	// Ports are each channel's loopback port on this host, as the controller
	// derives them from the host's port base (agentapi.Host.RelayChannelPorts).
	Ports map[agentapi.RelayChannel]int
	// SSHBinary and SSHEnv are the relays' ssh; empty means sshrun's.
	SSHBinary string
	SSHEnv    []string
	// MinBackoff, MaxBackoff and StopGrace are hostdial's; zero means its
	// defaults.
	MinBackoff, MaxBackoff, StopGrace time.Duration
	// ClaimLapse is the keeper's; zero means DefaultClaimLapse.
	ClaimLapse time.Duration
	// SettleEvery is DefaultSettleEvery when zero.
	SettleEvery time.Duration
	// Now is the keeper's clock (Options.Now); nil means time.Now. A test of
	// the keeper process sets it, so it can move the keeper's clock past a
	// set's validity and wait on that, rather than on a window.
	Now func() time.Time
	// Logger, nil for none.
	Logger *slog.Logger
	// Ready, when set, is told once the socket serves, with E and the
	// socket's path.
	Ready func(epoch int64, socket string)
}

// Run is the keeper process: it returns once ctx ends and every relay's
// process is gone, or on a failure to start, before any relay starts.
func Run(ctx context.Context, c ProcessConfig) error {
	if c.StateDir == "" {
		return errors.New("relaykeeper: no state directory")
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
	if c.SettleEvery <= 0 {
		c.SettleEvery = DefaultSettleEvery
	}
	var runner *OwnerRunner
	k, err := Open(Options{State: StatePath(c.StateDir), ClaimLapse: c.ClaimLapse, Logger: c.Logger, Now: c.Now,
		Runner: func(epoch int64) (Runner, error) {
			r, err := NewOwnerRunner(context.Background(), OwnerRunnerOptions{Epoch: epoch, Ports: c.Ports, Slots: hostdial.NewSlots(0),
				SSHDir: SSHDir(c.StateDir), SSHBinary: c.SSHBinary, SSHEnv: c.SSHEnv,
				MinBackoff: c.MinBackoff, MaxBackoff: c.MaxBackoff, StopGrace: c.StopGrace, Logger: c.Logger})
			runner = r
			return r, err
		}})
	if err != nil {
		return err
	}
	defer runner.Stop()
	ln, err := wire.Listen(SocketPath(c.StateDir))
	if err != nil {
		return fmt.Errorf("relaykeeper: the socket: %w", err)
	}
	c.Logger.Info("the relay keeper serves", "epoch", k.Epoch(), "socket", SocketPath(c.StateDir))
	if c.Ready != nil {
		c.Ready(k.Epoch(), SocketPath(c.StateDir))
	}
	sctx, stop := context.WithCancel(ctx)
	defer stop()
	served := make(chan error, 1)
	go func() { served <- wire.Serve(sctx, ln, k.Handle, c.Logger) }()
	tick := time.NewTicker(c.SettleEvery)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			if err := k.Settle(); err != nil {
				c.Logger.Error("the state file still does not sync", "err", err)
			}
		case err := <-served:
			if err != nil {
				return fmt.Errorf("relaykeeper: serving: %w", err)
			}
			return nil
		case <-ctx.Done():
			stop()
			if err := <-served; err != nil {
				return fmt.Errorf("relaykeeper: serving: %w", err)
			}
			c.Logger.Info("the relay keeper stops: every relay is stopped")
			return nil
		}
	}
}
