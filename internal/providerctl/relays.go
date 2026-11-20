package providerctl

// New for HBEP-6's host-dialed mode (PR 298, final text at b12aa9f): the
// Provider controller owns the relays of its running Allocations.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/sshrun"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
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
	// Hosts is the process's one-Provider-per-login-host register; nil means
	// hostdial.SharedHosts.
	Hosts *hostdial.Hosts
	// ControlDir holds the relays' private ssh configuration file: the
	// transport's control directory does.
	ControlDir string
	// SSHBinary is the ssh client; "" means ssh on PATH.
	SSHBinary string
	// LoginTimeout bounds the ssh -G that names the login host; zero means
	// sshrun.DefaultLoginTimeout. Tests shorten it.
	LoginTimeout time.Duration
	// Epoch reads the fence's epoch when the relays start, the Provider
	// Lease's leaseTransitions; Setup fills it from the manager's
	// configuration when it is nil.
	Epoch func(ctx context.Context) (int64, error)
	// MinBackoff, MaxBackoff, ResetAfter, StopGrace, RestartLimit and
	// RestartWindow are the owner's; zero means hostdial's defaults (1 s,
	// 30 s, 60 s, 5 s, 120 in an hour). Tests shorten them.
	MinBackoff, MaxBackoff, ResetAfter, StopGrace time.Duration
	RestartLimit                                  int
	RestartWindow                                 time.Duration
	// KeeperSocket is the relay keeper's socket (beta; HBEP-6, The relay
	// keeper; #343). Set, the Provider's leader sends the keeper fenced
	// desired sets and acknowledges its final ends, and runs no relay of its
	// own (keeper.go); "" is alpha's relay owner, as before. The launcher
	// chooses the path, so the controller computes none.
	KeeperSocket string
	// KeeperSendInterval is how often the leader sends its desired set when
	// no observation changed it; zero means keeperSendInterval. Tests
	// shorten it.
	KeeperSendInterval time.Duration
	// RenewDeadline is the Provider manager's renew deadline, which a desired
	// set's validity adds to the poll interval in force (#343's Q5); zero
	// means controller-runtime's 10 s, which the controller binary uses.
	// Tests shorten it.
	RenewDeadline time.Duration
}

// relayState is one controller's relay owner and the ssh client maker for
// the Provider's current credentials.
type relayState struct {
	settings RelaySettings
	owner    *hostdial.Owner
	provider string
	hosts    *hostdial.Hosts
	log      *slog.Logger // the controller's log, for the relays' ssh -G (issue 403)

	reader client.Reader // the Provider's age, for the login host's register

	mu     sync.Mutex
	cfg    slurm.SSHConfig
	ssh    *sshrun.Relay
	closed bool // after RunRelays: no claim and no relay is made any more
	// lead is done when this copy does not lead (relayfailed.go, startLead).
	lead   context.Context
	unlead context.CancelFunc
	// failures are the final failures of running Allocations' relays, by
	// tag, kept until the relays are dropped: the failure path writes the
	// mark, and an observation writes it from here when that write failed
	// (relayfailed.go).
	failures map[string]relayFailure
	// keeper is set in keeper mode (RelaySettings.KeeperSocket): then owner
	// and ssh stay nil, and the keeper holds the relays (keeper.go).
	keeper *keeperState

	// transport reads the profile's transport block as the holder has it now
	// (Setup fills it); nil reads as no budget.
	transport func() *v1alpha1.TransportProfile
	// admitted is the Provider's Allocations submitted or being submitted, as
	// syncRelays last counted them from a fresh list: one number the relay
	// limit reads, never a stored limit.
	admitted atomic.Int64
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
	if s.KeeperSocket != "" {
		return newKeeperRelayState(o, s)
	}
	if s.ControlDir == "" {
		return nil, errors.New("providerctl: host-dialed mode needs a control directory for the relays' ssh")
	}
	if s.Hosts == nil {
		s.Hosts = hostdial.SharedHosts
	}
	r := &relayState{settings: s, provider: o.Provider, hosts: s.Hosts, log: slogOf(o.Log)}
	epoch := func(ctx context.Context) (int64, error) {
		if r.settings.Epoch == nil {
			return 0, errors.New("no epoch reader is set")
		}
		return r.settings.Epoch(ctx)
	}
	owner, err := hostdial.NewOwner(hostdial.OwnerOptions{
		Ports: s.Ports, Epoch: epoch, Slots: s.Slots, MinBackoff: s.MinBackoff, MaxBackoff: s.MaxBackoff,
		ResetAfter: s.ResetAfter, StopGrace: s.StopGrace, RestartLimit: s.RestartLimit, RestartWindow: s.RestartWindow,
		Logger: slogOf(o.Log),
	})
	if err != nil {
		return nil, err
	}
	r.owner = owner
	return r, nil
}

// relay returns the ssh client maker for the Provider's credentials, made
// again when they change.
func (r *relayState) relay(cfg slurm.SSHConfig) (*sshrun.Relay, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errRelaysClosed
	}
	if r.ssh == nil || !reflect.DeepEqual(r.cfg, cfg) {
		rel, err := sshrun.NewRelay(sshrun.RelayOptions{Name: r.provider, Config: cfg, ControlDir: r.settings.ControlDir, Binary: r.settings.SSHBinary,
			LoginTimeout: r.settings.LoginTimeout, Logger: r.log})
		if err != nil {
			return nil, err
		}
		if r.ssh != nil {
			_ = r.ssh.Close()
		}
		r.ssh, r.cfg = rel, cfg
	}
	return r.ssh, nil
}

// errRelaysClosed: the Provider's manager is stopping; no claim and no relay
// is made any more, so nothing claims a host after the release.
var errRelaysClosed = errors.New("the Provider's relays have stopped")

// claimLogin claims the Provider's login host, as ssh -G resolves its
// HostName through the relays' own -F file, in lower case, for this
// Provider's relays. It returns the host, and when the claim is refused the
// Provider that holds it, the oldest that uses it (pending: one that has not
// said which host it uses yet), and the refusal's text in inUse: one
// host-dialed Provider per login host (HBEP-6, Capacity).
func (r *relayState) claimLogin(cfg slurm.SSHConfig) (login, inUse string, err error) {
	if r.keeper != nil {
		return r.keeper.claim()
	}
	if !r.hosts.Enrolled(r.provider) {
		// The manager's start enrolls it (enrollRunnable); a first claim that
		// comes sooner enrolls it here, rather than reading as a host in use.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := r.enroll(ctx, r.reader)
		cancel()
		if err != nil {
			return "", "", err
		}
		if !r.hosts.Enrolled(r.provider) {
			return "", "", errRelaysClosed
		}
	}
	rel, err := r.relay(cfg)
	if err != nil {
		return "", "", err
	}
	host, err := rel.Login()
	if err != nil {
		// ssh -G timed out (issue 403): no host to claim this pass. The status
		// says only that (the error names no part of the ssh configuration;
		// the Match exec command is in the log, Relay.Logger), and the next
		// pass runs ssh -G again, bounded again.
		return "", "", err
	}
	login = strings.ToLower(host)
	if holder, pending, ok := r.hosts.Claim(login, r.provider); !ok {
		return login, loginHostInUse(login, holder, pending), nil
	}
	return login, "", nil
}

// loginHostInUse is the refusal's text, naming the host and its holder.
func loginHostInUse(login, holder string, pending bool) string {
	if pending {
		return fmt.Sprintf("login host %s may be in use by older host-dialed Provider %s, which has not said which login host it uses yet: one host-dialed Provider per login host (HBEP-6, Capacity)", login, holder)
	}
	return fmt.Sprintf("login host %s is in use by host-dialed Provider %s: one host-dialed Provider per login host (HBEP-6, Capacity)", login, holder)
}

// hostDialedCap is the Provider's host-dialed cap for the profile spec this
// pass read (v1alpha1.HostDialedCap; HBEP-4, open question 17, amended
// 2026-11-20), the planner's cap read again from the Provider's side. M, the
// stack's Providers on the login host, is the login-host register's count,
// which the Provider's own claim has just filled. known is false while the
// Provider has not said which host it uses, and only when the profile sets a
// budget: unset, the cap is 2 and reads no M. In relay-keeper mode the cap is
// 2 whatever the budget, since the keeper's wire carries no budget (K2).
func (r *relayState) hostDialedCap(spec *v1alpha1.PlatformProfileSpec) (c int32, known bool) {
	if r.keeper != nil || spec.Transport == nil || spec.Transport.LoginHostBudget == nil {
		return v1alpha1.HostDialedCap(spec, 0, r.keeper != nil), true
	}
	m, ok := r.hosts.OnHostOf(r.provider)
	if !ok {
		return 0, false
	}
	return v1alpha1.HostDialedCap(spec, m, false), true
}

// relayLimit is the most relays this Provider's login node may hold, for the
// relay owner to read under the slot lock at every attempt to take an
// Allocation's pair (Target.RelayLimit): max(2 x the cap, 2 x the admitted
// Allocations), 2 being len(agentapi.RelayChannels), so that with the stack's
// masters the connections stay within the budget (2 x cap + M <= budget) for
// every Allocation the cap admitted, and an Allocation submitted before a
// reduction still gets its pair when its job starts: a reduction only stops
// new admissions, which the re-check before each submit enforces (HBEP-4, open
// question 17). The cap reads the profile as the holder has it now and M as
// the register has it now (1 while this Provider has not claimed its host);
// the admitted count is the last observation of the fresh list the re-check
// reads (syncRelays). It reports false, and the default limit holds, when the
// profile sets no budget, and in keeper mode. It never revokes a pair.
func (r *relayState) relayLimit() (int, bool) {
	if r.keeper != nil || r.transport == nil {
		return 0, false
	}
	tp := r.transport()
	if tp == nil || tp.LoginHostBudget == nil {
		return 0, false
	}
	m := 1
	if k, ok := r.hosts.OnHostOf(r.provider); ok {
		m = k
	}
	c := v1alpha1.HostDialedCap(&v1alpha1.PlatformProfileSpec{Transport: tp}, m, false)
	return len(agentapi.RelayChannels) * int(max(c, int32(r.admitted.Load()))), true
}

// claimBeforeMaster records the Provider's login host in the register BEFORE
// the Provider's first command opens its master (the holder calls it before it
// returns a Platform), so the stack's other Providers count this master in M
// from the moment it can exist (HBEP-4, open question 17: "each holding its own
// master"). A refusal (another Provider holds the host) is not an error here:
// the status loop and Create report it, and the refused Provider's master is
// open all the same, so it counts. An error is a host that could not be
// resolved (ssh -G timed out): it is not claimed this pass (the holder logs it
// and goes on, and the status loop reports it).
func (r *relayState) claimBeforeMaster(v slurm.Validated) error {
	_, _, err := r.claimLogin(v.SSH)
	return err
}

// enroll records the Provider's age in the login-host register, from its
// object's creationTimestamp.
func (r *relayState) enroll(ctx context.Context, reader client.Reader) error {
	var prov v1alpha1.Provider
	if err := reader.Get(ctx, client.ObjectKey{Name: r.provider}, &prov); err != nil {
		return fmt.Errorf("the Provider's age for its login host: %w", err)
	}
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if !closed {
		r.hosts.Enroll(r.provider, prov.CreationTimestamp.Time)
	}
	return nil
}

// reportNoLogin records that the Provider uses no login host now: its
// profile cannot be read, so it does not hold back a younger Provider.
func (r *relayState) reportNoLogin() {
	if r.hosts.Enrolled(r.provider) {
		r.hosts.Claim("", r.provider)
	}
}

func (r *relayState) close() {
	r.endLead()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	if r.ssh != nil {
		_ = r.ssh.Close()
		r.ssh = nil
	}
	r.hosts.Release(r.provider)
}

// credentialsGen names the Provider's ssh credentials: a change of any of
// them, the user, the key, the known hosts or the login node, is a new
// generation, so the relay owner stops the relays on the old ones and starts
// them on the new (review of PR 301, F1). The holder makes a new transport on
// the same change, so the generation also names the listing reader.
func credentialsGen(c slurm.SSHConfig) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%#v", c)))
	return hex.EncodeToString(sum[:8])
}

// syncRelays tells the relay owner what this observation found (HBEP-6,
// Dial-home modes, host-dialed mode): an Allocation whose recorded job runs
// keeps both relays, and one that is being deleted, refused, or whose job is
// not running (pending, held, completing, ended, lost) has them stopped by
// PID. A running Allocation one of whose relays ended for good, other than
// superseded or by its job's end, is marked RelayEndedForGood, which the
// lifecycle controller releases (issue #304). A failed read leaves the
// observed phase as it was, so the relays stay as they were. A Provider
// whose login host another host-dialed Provider of this process holds gets no
// relays; its Ready condition says so (LoginHostInUse, statusLoop), so no
// event is raised here. It is a no-op outside host-dialed mode.
func (e *external) syncRelays(ctx context.Context, a *v1alpha1.Allocation, tag string) {
	r := e.s.relays
	if r == nil {
		return
	}
	if r.keeper != nil {
		e.syncKeeper(ctx, a, tag)
		return
	}
	o := a.Status.AtProvider
	if meta.WasDeleted(a) || o.Phase != v1alpha1.AllocationRunning || o.JobID == "" || o.JobCompleting || a.Spec.ForProvider.Batch == nil {
		r.owner.Drop(tag)
		r.forgetFailure(tag)
		return
	}
	r.markRelayFailed(a, tag)
	paths, err := e.p.Paths(ctx)
	if err != nil {
		e.s.o.Log.Info("the relays wait for the login node's paths", "tag", tag, "error", err)
		return
	}
	rel, err := r.relay(e.v.SSH)
	if err != nil {
		e.s.o.Log.Info("the relays' ssh client cannot be made", "tag", tag, "error", err)
		return
	}
	login, inUse, err := r.claimLogin(e.v.SSH)
	if err != nil {
		e.s.o.Log.Info("the relays wait for the login host's claim", "tag", tag, "error", err)
		return
	}
	if inUse != "" {
		r.owner.Drop(tag)
		e.s.o.Log.Info("no relays for this Provider", "tag", tag, "why", inUse)
		return
	}
	// The admitted count the relay limit reads is the fresh list the final
	// check before a submit reads (admittedAllocations), taken before the pair
	// is asked for, so a restart that finds five Allocations serving under a
	// budget cut asks for its limit with five admitted. A list that cannot be
	// read leaves the relays as they were until the next pass.
	n, err := e.admittedAllocations(ctx, e.s.reader, "")
	if err != nil {
		e.s.o.Log.Info("the relays wait for the Provider's Allocations to be counted", "tag", tag, "error", err)
		return
	}
	r.admitted.Store(int64(n))
	ref := a.DeepCopy()
	r.owner.Want(hostdial.Target{
		Tag: tag, JobID: o.JobID,
		AgentPath: path.Join(paths.Staging, agentapi.StagedAgentPath(a.Spec.ForProvider.Batch.AgentDigest)),
		AllocDir:  path.Join(paths.Staging, agentapi.AllocationDir(tag)),
		UID:       string(a.UID),
		Login:     login, Jobs: e.p, Command: rel.Command, Stderr: sshrun.Stderr, Describe: sshrun.Classify,
		OnFailed: e.onRelayFailed(ref, tag),
		Gen:      credentialsGen(e.v.SSH), RelayLimit: r.relayLimit,
	})
}

// Relays lists the relays the controller keeps (host-dialed mode only; nil
// otherwise).
func (c *Controller) Relays() []hostdial.RelayState {
	if c.shared.relays != nil && c.shared.relays.keeper != nil {
		return nil // the keeper holds them (keeper.go)
	}
	if c.shared.relays == nil {
		return nil
	}
	return c.shared.relays.owner.Relays()
}

// RunRelays runs the relay owner until ctx ends, then stops every relay by
// PID; Setup adds it to the manager as a leader-only runnable, which
// controller-runtime stops before it releases the Lease. Outside host-dialed
// mode it waits for ctx and returns.
func (c *Controller) RunRelays(ctx context.Context) error {
	if c.shared.relays == nil {
		<-ctx.Done()
		return nil
	}
	if c.shared.relays.keeper != nil {
		return c.runKeeper(ctx)
	}
	defer c.shared.relays.close()
	c.shared.relays.startLead(ctx)
	defer c.shared.relays.endLead()
	return c.shared.relays.owner.Start(ctx)
}

// StopRelays ends this copy's lead for the relays' failure path, then stops
// every relay of this controller by PID and returns once their processes are
// gone (the Lease's release hook, RelayFence).
func (c *Controller) StopRelays() {
	if c.shared.relays != nil && c.shared.relays.keeper != nil {
		c.stopKeeper()
		return
	}
	if c.shared.relays != nil {
		c.shared.relays.endLead()
		c.shared.relays.owner.Stop()
	}
}

// enrollRunnable enrolls the Provider for a login host when its manager
// starts, leading or not, and forgets it when the manager stops, so a younger
// Provider is not admitted to a host before this one has said which it uses,
// and an older one that stopped holds back none (HBEP-6, Capacity; the delta
// review of PR 301, D4).
type enrollRunnable struct{ c *Controller }

func (r enrollRunnable) Start(ctx context.Context) error {
	rs := r.c.shared.relays
	for {
		if err := rs.enroll(ctx, r.c.shared.reader); err == nil {
			break
		} else {
			r.c.shared.o.Log.Info("cannot enroll the Provider for its login host yet", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(2 * time.Second):
		}
	}
	<-ctx.Done()
	rs.hosts.Release(rs.provider)
	return nil
}

func (enrollRunnable) NeedLeaderElection() bool { return false }

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
