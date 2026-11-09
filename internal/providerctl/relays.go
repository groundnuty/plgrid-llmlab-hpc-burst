package providerctl

// New for HBEP-6's host-dialed mode (PR 298): the Provider's controller owns
// the relays of its running Allocations.

import (
	"context"
	"errors"
	"log/slog"
	"os/exec"
	"path"
	"reflect"
	"sync"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/sshrun"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// RelaySettings are host-dialed mode's relay settings (HBEP-6, Dial-home
// modes, host-dialed mode). They are required exactly when Front's mode is
// host-dialed.
type RelaySettings struct {
	// Ports are each channel's loopback port on this host:
	// agentapi.Host.RelayChannelPorts, the agent endpoint (base+3) and the
	// dial-home listener (base+7).
	Ports map[agentapi.RelayChannel]int
	// Slots caps relays per login node across every Provider of the process;
	// nil means hostdial.SharedSlots.
	Slots *hostdial.Slots
	// ControlDir holds the relays' private ssh configuration file: the
	// transport's control directory does.
	ControlDir string
	// SSHBinary is the ssh client; "" means ssh on PATH.
	SSHBinary string
	// MinBackoff, MaxBackoff, ResetAfter and StopGrace are the owner's
	// timings; zero means hostdial's defaults (1 s, 30 s, 60 s, 5 s). Tests
	// shorten them.
	MinBackoff, MaxBackoff, ResetAfter, StopGrace time.Duration
}

// relayState is one controller's relay owner and the ssh client maker for
// the Provider's current credentials.
type relayState struct {
	settings RelaySettings
	owner    *hostdial.Owner

	mu  sync.Mutex
	cfg slurm.SSHConfig
	ssh *sshrun.Relay
}

func newRelayState(o Options) (*relayState, error) {
	if o.Front == nil || o.Front.Mode != agentapi.DialHomeHostDialed {
		if o.Relays != nil {
			return nil, errors.New("providerctl: relay settings are for host-dialed mode only")
		}
		return nil, nil
	}
	if o.Relays == nil {
		return nil, errors.New("providerctl: host-dialed mode needs the relay settings (Options.Relays)")
	}
	s := *o.Relays
	if s.ControlDir == "" {
		return nil, errors.New("providerctl: host-dialed mode needs a control directory for the relays' ssh")
	}
	owner, err := hostdial.NewOwner(hostdial.OwnerOptions{
		Ports: s.Ports, Slots: s.Slots, MinBackoff: s.MinBackoff, MaxBackoff: s.MaxBackoff,
		ResetAfter: s.ResetAfter, StopGrace: s.StopGrace, Logger: slogOf(o.Log),
	})
	if err != nil {
		return nil, err
	}
	return &relayState{settings: s, owner: owner}, nil
}

// command returns the ssh client maker for the Provider's credentials, made
// again when they change.
func (r *relayState) command(provider string, cfg slurm.SSHConfig) (func(string) (*exec.Cmd, error), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ssh == nil || !reflect.DeepEqual(r.cfg, cfg) {
		rel, err := sshrun.NewRelay(sshrun.RelayOptions{Name: provider, Config: cfg, ControlDir: r.settings.ControlDir, Binary: r.settings.SSHBinary})
		if err != nil {
			return nil, err
		}
		if r.ssh != nil {
			_ = r.ssh.Close()
		}
		r.ssh, r.cfg = rel, cfg
	}
	return r.ssh.Command, nil
}

func (r *relayState) close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ssh != nil {
		_ = r.ssh.Close()
		r.ssh = nil
	}
}

// freshJobs reads the listing fresh: *slurm.Batch does; any other Platform
// (the tests' fakes) has no cached listing to skip.
type freshJobs struct{ p Platform }

func (f freshJobs) ListByTagFresh(ctx context.Context, tag string) ([]provider.Job, error) {
	if b, ok := f.p.(hostdial.JobReader); ok {
		return b.ListByTagFresh(ctx, tag)
	}
	return f.p.ListByTag(ctx, tag)
}

// syncRelays tells the relay owner what this observation found (HBEP-6,
// Dial-home modes, host-dialed mode): an Allocation whose recorded job runs
// keeps both relays, and one that is being deleted, refused, or whose job is
// not running (pending, held, completing, ended, lost) has them stopped by
// PID. A failed read leaves the observed phase as it was, so the relays stay
// as they were. It is a no-op outside host-dialed mode.
func (e *external) syncRelays(ctx context.Context, a *v1alpha1.Allocation, tag string) {
	r := e.s.relays
	if r == nil {
		return
	}
	o := a.Status.AtProvider
	if meta.WasDeleted(a) || o.Phase != v1alpha1.AllocationRunning || o.JobID == "" || o.JobCompleting || a.Spec.ForProvider.Batch == nil {
		r.owner.Drop(tag)
		return
	}
	paths, err := e.p.Paths(ctx)
	if err != nil {
		e.s.o.Log.Info("the relays wait for the login node's paths", "tag", tag, "error", err)
		return
	}
	cmd, err := r.command(e.s.o.Provider, e.v.SSH)
	if err != nil {
		e.s.o.Log.Info("the relays' ssh client cannot be made", "tag", tag, "error", err)
		return
	}
	r.owner.Want(hostdial.Target{
		Tag: tag, JobID: o.JobID,
		AgentPath: path.Join(paths.Staging, agentapi.StagedAgentPath(a.Spec.ForProvider.Batch.AgentDigest)),
		Login:     sshrun.LoginOf(e.v.SSH), Jobs: freshJobs{e.p}, Command: cmd,
	})
}

// Relays lists the relays the controller keeps (host-dialed mode only; nil
// otherwise).
func (c *Controller) Relays() []hostdial.RelayState {
	if c.shared.relays == nil {
		return nil
	}
	return c.shared.relays.owner.Relays()
}

// RunRelays runs the relay owner until ctx ends, then stops every relay by
// PID; Setup adds it to the manager as a leader-only runnable. Outside
// host-dialed mode it waits for ctx and returns.
func (c *Controller) RunRelays(ctx context.Context) error {
	if c.shared.relays == nil {
		<-ctx.Done()
		return nil
	}
	defer c.shared.relays.close()
	return c.shared.relays.owner.Start(ctx)
}

// relayRunnable is the owner as a manager's runnable: leader-only, so only
// the leader's relays run, and stopped with the manager.
type relayRunnable struct{ c *Controller }

func (r relayRunnable) Start(ctx context.Context) error { return r.c.RunRelays(ctx) }
func (relayRunnable) NeedLeaderElection() bool          { return true }

// slogOf is the controller's crossplane logger as a slog.Logger, for the
// relay owner: Info and above go to Info, the rest to Debug.
func slogOf(l logging.Logger) *slog.Logger { return slog.New(logHandler{l: l}) }

type logHandler struct {
	l     logging.Logger
	attrs []any
}

func (h logHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h logHandler) Handle(_ context.Context, r slog.Record) error {
	kv := append([]any(nil), h.attrs...)
	r.Attrs(func(a slog.Attr) bool {
		kv = append(kv, a.Key, a.Value.Any())
		return true
	})
	if r.Level >= slog.LevelInfo {
		h.l.Info(r.Message, kv...)
	} else {
		h.l.Debug(r.Message, kv...)
	}
	return nil
}

func (h logHandler) WithAttrs(as []slog.Attr) slog.Handler {
	kv := append([]any(nil), h.attrs...)
	for _, a := range as {
		kv = append(kv, a.Key, a.Value.Any())
	}
	return logHandler{l: h.l, attrs: kv}
}

func (h logHandler) WithGroup(string) slog.Handler { return h }
