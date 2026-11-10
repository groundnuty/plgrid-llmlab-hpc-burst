package hostdial

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os/exec"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/sshrun"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The relay owner's timings (HBEP-6, What ends a relay: "a backoff of 1 s
// doubling to 30 s with jitter (first starts jittered too ...), reset once a
// relay has stayed up for 60 s"; "more than 120 restarts of one channel
// within an hour" is final, counting only restarts whose session came up and
// then dropped: the science agent's ruling on #301, 9 Nov 2026).
const (
	DefaultMinBackoff    = time.Second
	DefaultMaxBackoff    = 30 * time.Second
	DefaultResetAfter    = 60 * time.Second
	DefaultRestartLimit  = 120
	DefaultRestartWindow = time.Hour
	// DefaultStopGrace is how long a stopped relay's ssh has after TERM
	// before KILL.
	DefaultStopGrace = 5 * time.Second
)

// RelayStartWorstCase is the longest from the moment a relay is due until its
// session is up, once its pair of slots is held and the listing shows the
// job: the longest backoff, ssh's ConnectTimeout to the login node, and the
// wait for the relay's preamble. A live run's derived timeouts count it by
// this name.
const RelayStartWorstCase = DefaultMaxBackoff + sshrun.RelayConnectTimeout + PreambleTimeout

// JobReader is the Provider's latest queue listing (one read per poll
// interval, HBEP-4), under an Allocation's tag; *slurm.Batch implements it.
// The relay reads no listing of its own (HBEP-6: "the Provider's latest queue
// listing ... must show the recorded job ID RUNNING").
type JobReader interface {
	ListByTag(ctx context.Context, tag string) ([]provider.Job, error)
}

// Target is what the relays of one running Allocation need.
type Target struct {
	// Tag is the Allocation's job tag; it names the relays.
	Tag string
	// JobID is the Allocation's recorded job: a relay starts only when the
	// listing shows this job RUNNING with the tag as its name.
	JobID string
	// AgentPath is the staged agent binary's absolute path on the cluster,
	// the relay step's program.
	AgentPath string
	// AllocDir is the Allocation's directory on the cluster, where the agent
	// publishes its socket (--dir).
	AllocDir string
	// UID is the Allocation's UID (--allocation), which the agent's greeting
	// and the relay's preamble name.
	UID string
	// Login names the login node for the slots: as ssh -G resolves its host
	// name (sshrun.Relay.LoginHost).
	Login string
	// Jobs is the Provider's listing.
	Jobs JobReader
	// Command returns the ssh client that runs line on the login node, on a
	// connection of its own (sshrun.Relay's Command). The owner sets its
	// standard streams and process group and starts it.
	Command func(line string) (*exec.Cmd, error)
	// Stderr returns what an ended relay's command kept of its standard
	// error (sshrun.Stderr), which ClassifyEnd reads; it is never logged.
	Stderr func(cmd *exec.Cmd) string
	// Describe, when set, names an ssh failure's kind without repeating its
	// output (sshrun.Classify), for the log.
	Describe func(cmd *exec.Cmd) string
	// OnFailed, when set, is told each final end of a channel that is
	// neither superseded nor the job's end, with its cause: the controller
	// raises RelayFailed (HBEP-6, What ends a relay).
	OnFailed func(ch agentapi.RelayChannel, cause string)
	// Gen names the credentials and the transport that Jobs and Command use:
	// a change (a rotated key, another known-hosts file, a new transport) is
	// another Target, so the running relays are stopped and started again
	// with the new ones, never left on the old.
	Gen string
	// OnFinal, when set, is told each final end of a channel, of every kind
	// (superseded, the job's end, failed), with its cause, after OnFailed. The
	// relay keeper records an end before its relay's loop returns, and in beta
	// a superseded end needs the mark too (HBEP-6, The relay keeper; issue
	// #343). Nil is alpha's.
	OnFinal func(ch agentapi.RelayChannel, kind EndKind, cause string)
	// Final, when set, names channels that already ended for good, each with
	// the cause as RelayState.FinalCause showed it: their relays are kept as
	// ended, read as final from the Want on, slots or no slots, and are never
	// started, while the Allocation's pair of slots is taken and held as
	// before. The relay keeper seeds it from the ends it holds, so a
	// final-ended channel is never restarted after a keeper restart or on a
	// new Gen (HBEP-6, The relay keeper; issue #343). same ignores it, so
	// seeding it restarts no relay. Nil is alpha's.
	Final map[agentapi.RelayChannel]string
	// OnStart, when set, is told of each ssh start of a channel's relay, for
	// the keeper's count of restarts, which outlives a Drop (HBEP-8,
	// hpcburst_relay_restarts_total; issue #343). Nil is alpha's.
	OnStart func(ch agentapi.RelayChannel)
	// BeforeStart, when set, is asked right before each ssh start, with the
	// (E, N) the start would carry, after the listing, the relay line and the
	// ssh command, and after N was taken: a false answer starts nothing, and
	// the attempt is retried after the backoff. The relay keeper decides
	// there, under its own lock, whether the start may go ahead, so a
	// replacement it accepted in the meantime keeps the old start from
	// launching (HBEP-6, The relay keeper; issue #343). Nil is alpha's.
	BeforeStart func(ch agentapi.RelayChannel, epoch, seq int64) bool
	// OnStartFailed, when set, is told when an ssh start that BeforeStart
	// allowed, or that was made with no BeforeStart, fails to start a
	// process; OnStart is told when it does. So every start is resolved by
	// exactly one of the two (issue #343). Nil is alpha's. One failed start
	// may leave a process: with OwnerOptions.Pidfd, a start that clone gave
	// no pidfd is refused though its process started (startPidfdProcess); its
	// pipes are closed, so it normally ends by itself, unlisted meanwhile, and
	// NewOwner's probe of pidfd_open makes it unlikely.
	OnStartFailed func(ch agentapi.RelayChannel)
	// OnExit, when set, is told once a started ssh process has exited and
	// been reaped, after OnStart was told of its start, so its holder knows
	// when none of a stopped relay's processes runs any more (issue #343).
	// Nil is alpha's.
	OnExit func(ch agentapi.RelayChannel)
	//
	// BeforeStart, OnStartFailed and OnExit are hooks too, as below.
	//
	// The hooks (OnFailed, OnFinal and OnStart) run on the relay's own
	// goroutine, which its Allocation's stop waits for. So a hook that calls
	// Drop or Stop for its own Allocation, or waits on a lock that is held
	// around such a call, deadlocks. A final end that comes while the relay
	// is being stopped, by a Drop or by a Want of another Gen, is not told:
	// the stop's cancellation wins, and a new Gen's relay starts that channel
	// again unless Final names it.
}

// same reports whether u needs no change of t's relays.
func (t Target) same(u Target) bool {
	return t.Tag == u.Tag && t.JobID == u.JobID && t.AgentPath == u.AgentPath && t.AllocDir == u.AllocDir &&
		t.UID == u.UID && t.Login == u.Login && t.Gen == u.Gen
}

// OwnerOptions configure an Owner.
type OwnerOptions struct {
	// Ports are each channel's loopback port on this host, the only port its
	// relays dial: agentapi.Host.RelayChannelPorts.
	Ports map[agentapi.RelayChannel]int
	// Epoch reads the fence's epoch E when the Owner starts: the
	// provider-<name> Lease's leaseTransitions now that this copy leads
	// (HBEP-6, Fencing). Required.
	Epoch func(ctx context.Context) (int64, error)
	// Slots caps relays per login node; nil means SharedSlots.
	Slots *Slots
	// MinBackoff, MaxBackoff and ResetAfter are the restart backoff; zero
	// means the defaults.
	MinBackoff, MaxBackoff, ResetAfter time.Duration
	// RestartLimit and RestartWindow: more restarts of one channel than the
	// limit within the window, counting only those after a session that came
	// up and dropped, is a final end; zero means 120 an hour. A relay that
	// fails before its session comes up (the login node down, ssh refused,
	// agent not ready, no preamble) is never capped: it retries at the
	// backoff's cap, bounded by the job's steps and by its end.
	RestartLimit  int
	RestartWindow time.Duration
	// StopGrace is how long a stopped relay has after TERM; zero means
	// DefaultStopGrace.
	StopGrace time.Duration
	// Logger reports starts, stops and refusals; nil means none.
	Logger *slog.Logger
	// Pidfd, for the relay keeper (beta; issue #343), starts each relay's ssh
	// with a pidfd of its own and no process group of its own, and signals it
	// only through that pidfd, after a check of its recorded start time
	// (HBEP-6, The relay keeper, Signals). NewOwner fails when this kernel
	// gives no pidfd, so nothing falls back to signalling a PID. False is
	// alpha's: a process group of its own, signalled by its number.
	Pidfd bool
}

// seqs numbers every relay start of the process: each channel's N is the next
// value, so a later relay of one epoch always has a higher (E, N), across
// owners, restarts of a Provider's manager and Providers alike.
var seqs atomic.Int64

// Owner is the batch Provider's controller's relay owner (HBEP-6, Dial-home
// modes, host-dialed mode: the control-plane side). It keeps two relays, one
// per channel, for each Allocation it is told is running (Want), and stops
// them by PID when told it is not (Drop) or when its own run ends.
//
// It is level-triggered: each relay is supervised in its own loop, started
// again after the backoff when its end is a retried one, and the controller
// calls Want for every running Allocation on every observation, so a
// controller that takes the lead starts the relays of every running
// Allocation. It is a leader-only runnable (Start, NeedLeaderElection), which
// controller-runtime stops before it releases the Lease, so a copy that stops
// leading stops its relays by PID first. An Owner runs once; a new leader
// builds a new one, with a higher epoch.
type Owner struct {
	o OwnerOptions

	mu      sync.Mutex
	want    map[string]Target
	allocs  map[string]*allocRelays
	ctx     context.Context // set by Start
	epoch   int64
	running bool
	done    bool
	wg      sync.WaitGroup
}

// NewOwner returns an Owner. It starts nothing until Start.
func NewOwner(o OwnerOptions) (*Owner, error) {
	for _, ch := range agentapi.RelayChannels {
		if p := o.Ports[ch]; p < 1 || p > 65535 {
			return nil, fmt.Errorf("hostdial: no loopback port for the %s channel", ch)
		}
	}
	if len(o.Ports) != len(agentapi.RelayChannels) {
		return nil, errors.New("hostdial: a port for a channel that does not exist")
	}
	if o.Epoch == nil {
		return nil, errors.New("hostdial: the relay owner needs the Lease's epoch (OwnerOptions.Epoch)")
	}
	if o.Slots == nil {
		o.Slots = SharedSlots
	}
	if o.MinBackoff <= 0 {
		o.MinBackoff = DefaultMinBackoff
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = DefaultMaxBackoff
	}
	if o.ResetAfter <= 0 {
		o.ResetAfter = DefaultResetAfter
	}
	if o.RestartLimit <= 0 {
		o.RestartLimit = DefaultRestartLimit
	}
	if o.RestartWindow <= 0 {
		o.RestartWindow = DefaultRestartWindow
	}
	if o.StopGrace <= 0 {
		o.StopGrace = DefaultStopGrace
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if o.Pidfd {
		if err := checkPidfd(); err != nil {
			return nil, err
		}
	}
	return &Owner{o: o, want: map[string]Target{}, allocs: map[string]*allocRelays{}}, nil
}

// NeedLeaderElection makes the Owner a leader-only runnable of a
// controller-runtime manager.
func (*Owner) NeedLeaderElection() bool { return true }

// Want records that t's Allocation is running, and starts its two relays when
// the Owner runs and they are not already kept for the same job. A Want for
// the same tag with another job, agent, directory, UID, login or credentials
// (Gen) stops the old relays and starts new ones.
func (o *Owner) Want(t Target) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.done {
		return
	}
	if cur, ok := o.want[t.Tag]; ok && cur.same(t) {
		return
	}
	o.want[t.Tag] = t
	if !o.running {
		return
	}
	if a := o.allocs[t.Tag]; a != nil {
		delete(o.allocs, t.Tag)
		o.stopLocked(a)
	}
	o.allocs[t.Tag] = o.startLocked(t)
}

// Drop records that the Allocation tagged tag is no longer running (its job
// ended, or the Allocation is being deleted) and stops its relays by PID. It
// returns once their processes are gone.
func (o *Owner) Drop(tag string) {
	o.mu.Lock()
	delete(o.want, tag)
	a := o.allocs[tag]
	delete(o.allocs, tag)
	o.mu.Unlock()
	if a != nil {
		a.stop()
	}
}

// Start reads the epoch, then runs the relays of every wanted Allocation
// until ctx ends, then stops each by PID and returns once every relay's
// process is gone. An epoch that cannot be read is read again every few
// seconds, and no relay starts meanwhile.
func (o *Owner) Start(ctx context.Context) error {
	var epoch int64
	for {
		e, err := o.o.Epoch(ctx)
		if err == nil {
			epoch = e
			break
		}
		o.o.Logger.Info("the relays wait for the Lease's epoch", "err", err)
		select {
		case <-ctx.Done():
			o.mu.Lock()
			o.done = true
			o.mu.Unlock()
			return nil
		case <-time.After(2 * time.Second):
		}
	}
	o.mu.Lock()
	if o.running || o.done {
		o.mu.Unlock()
		return errors.New("hostdial: the relay owner runs once")
	}
	o.ctx, o.epoch, o.running = ctx, epoch, true
	for tag, t := range o.want {
		o.allocs[tag] = o.startLocked(t)
	}
	o.mu.Unlock()
	o.o.Logger.Info("relays running", "epoch", epoch)
	<-ctx.Done()
	o.mu.Lock()
	o.done, o.running = true, false
	all := o.allocs
	o.allocs = map[string]*allocRelays{}
	o.mu.Unlock()
	var wg sync.WaitGroup
	for _, a := range all {
		wg.Add(1)
		go func() { defer wg.Done(); a.stop() }()
	}
	wg.Wait()
	o.wg.Wait()
	return nil
}

// Stop stops every relay by PID and returns once their processes are gone;
// no relay starts afterwards, and a later Want or Start does nothing. It is
// the Provider's Lease's release hook (providerctl.RelayFence): the relays
// stop before the Lease is released, on a renewal that failed as on a
// requested stop (HBEP-6, Fencing).
func (o *Owner) Stop() {
	o.mu.Lock()
	o.done = true
	all := o.allocs
	o.allocs = map[string]*allocRelays{}
	o.mu.Unlock()
	var wg sync.WaitGroup
	for _, a := range all {
		wg.Add(1)
		go func() { defer wg.Done(); a.stop() }()
	}
	wg.Wait()
	o.wg.Wait()
}

// RelayState is one relay as the Owner keeps it.
type RelayState struct {
	Tag     string
	Channel agentapi.RelayChannel
	JobID   string
	// PID is the relay's ssh process, 0 while none runs.
	PID int
	// Starts counts the relay's ssh starts.
	Starts int
	// Since is when the current process started.
	Since time.Time
	// Epoch and Seq are the current or last process's fence.
	Epoch, Seq int64
	// LastRefusal says why the last start did not happen ("" when none).
	LastRefusal string
	// LastEnd is how the last process ended ("" when none ended), and
	// LastFailure its ssh failure's kind, if any.
	LastEnd, LastFailure string
	// Final is set once the relay ended for good; FinalCause says how.
	Final      bool
	FinalCause string
	// Waiting is true while the Allocation waits for its pair of slots on the
	// login node; no relay of it has started then.
	Waiting bool
	// Gen is the Target's credentials generation.
	Gen string
	// LastEnded is when the last process ended, and LastWait the backoff the
	// relay waited before its latest attempt.
	LastEnded time.Time
	LastWait  time.Duration
	// Streams and Joined count the streams the job opened over this relay
	// and those joined to the channel's port, and Sessions the sessions that
	// came up, over all its processes.
	Streams, Joined, Sessions uint64
}

// Relays lists the relays the Owner keeps, by tag then channel.
func (o *Owner) Relays() []RelayState {
	o.mu.Lock()
	var out []RelayState
	for _, a := range o.allocs {
		w := a.isWaiting()
		for _, r := range a.relays {
			st := r.state()
			st.Waiting = w
			out = append(out, st)
		}
	}
	o.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Tag != out[j].Tag {
			return out[i].Tag < out[j].Tag
		}
		return out[i].Channel < out[j].Channel
	})
	return out
}

// allocRelays are one Allocation's two relays, and its pair of slots.
type allocRelays struct {
	cancel context.CancelFunc
	relays []*relay
	done   chan struct{}

	mu      sync.Mutex
	waiting bool
}

func (a *allocRelays) isWaiting() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.waiting
}

func (a *allocRelays) stop() {
	a.cancel()
	<-a.done
}

func (o *Owner) stopLocked(a *allocRelays) {
	// Stopping waits for processes; Want's caller does not wait for it. The
	// new relays may start meanwhile: theirs is the higher (E, N).
	o.wg.Add(1)
	go func() { defer o.wg.Done(); a.stop() }()
}

// startLocked starts an Allocation's relays: it takes the pair of slots on
// the login node first, waiting while the node has no two free, and holds
// them until both relays have stopped, through every restart and backoff.
func (o *Owner) startLocked(t Target) *allocRelays {
	ctx, cancel := context.WithCancel(o.ctx)
	a := &allocRelays{cancel: cancel, done: make(chan struct{}), waiting: true}
	for _, ch := range agentapi.RelayChannels {
		a.relays = append(a.relays, &relay{o: o, t: t, ch: ch, epoch: o.epoch, target: agentapi.RelayTargetAddr(o.o.Ports[ch])})
	}
	// A seeded channel reads as final at once, not only once its pair of
	// slots is taken (Target.Final).
	for _, r := range a.relays {
		if cause, ok := t.Final[r.ch]; ok {
			r.seedFinal(cause)
		}
	}
	go func() {
		defer close(a.done)
		release, err := o.o.Slots.Acquire(ctx, t.Login, len(a.relays))
		if err != nil {
			return
		}
		defer release()
		a.mu.Lock()
		a.waiting = false
		a.mu.Unlock()
		var wg sync.WaitGroup
		for _, r := range a.relays {
			wg.Add(1)
			go func() { defer wg.Done(); r.loop(ctx) }()
		}
		wg.Wait()
	}()
	return a
}

// relay is one channel's relay of one Allocation.
type relay struct {
	o      *Owner
	t      Target
	ch     agentapi.RelayChannel
	epoch  int64
	target string

	mu         sync.Mutex
	pid        int
	starts     int
	since      time.Time
	seq        int64
	refusal    string
	lastEnd    string
	failure    string
	final      bool
	finalCause string
	lastEnded  time.Time
	lastWait   time.Duration
	restarts   []time.Time // the drops of sessions that came up, within the restart window
	stats      ServeStats
}

func (r *relay) state() RelayState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return RelayState{Tag: r.t.Tag, Channel: r.ch, JobID: r.t.JobID, PID: r.pid, Starts: r.starts, Since: r.since,
		Epoch: r.epoch, Seq: r.seq, LastRefusal: r.refusal, LastEnd: r.lastEnd, LastFailure: r.failure,
		Final: r.final, FinalCause: r.finalCause, Gen: r.t.Gen, LastEnded: r.lastEnded, LastWait: r.lastWait,
		Streams: r.stats.Streams.Load(), Joined: r.stats.Joined.Load(), Sessions: r.stats.Sessions.Load()}
}

func (r *relay) refuse(why string) {
	r.mu.Lock()
	r.refusal = why
	r.mu.Unlock()
	r.o.o.Logger.Info("a relay did not start", "tag", r.t.Tag, "channel", r.ch, "why", why)
}

// end makes the relay's end final, and tells OnFailed of an EndFailed one.
func (r *relay) end(kind EndKind, cause string) {
	r.mu.Lock()
	r.final, r.finalCause = true, kind.String()+": "+cause
	r.mu.Unlock()
	r.o.o.Logger.Info("a relay ended for good", "tag", r.t.Tag, "channel", r.ch, "end", kind.String(), "cause", cause)
	if kind == EndFailed && r.t.OnFailed != nil {
		r.t.OnFailed(r.ch, cause)
	}
	if r.t.OnFinal != nil {
		r.t.OnFinal(r.ch, kind, cause)
	}
}

// seedFinal keeps the relay as ended for good with cause (Target.Final),
// telling neither OnFailed nor OnFinal: its end was told when it happened.
func (r *relay) seedFinal(cause string) {
	r.mu.Lock()
	r.final, r.finalCause = true, cause
	r.mu.Unlock()
	r.o.o.Logger.Info("a relay that ended for good is not started", "tag", r.t.Tag, "channel", r.ch, "cause", cause)
}

// loop keeps the relay up until ctx ends or its end is final: a jittered wait
// (the first included, since every relay starts together after a restart),
// the job-is-ours read of the listing, the ssh start, the session until it
// drops, the stop by PID, and again. The Allocation's slots stay taken
// throughout.
func (r *relay) loop(ctx context.Context) {
	if _, ok := r.t.Final[r.ch]; ok {
		return // seeded final by startLocked
	}
	o := r.o.o
	backoff := o.MinBackoff
	wait := time.Duration(rand.Int64N(int64(o.MinBackoff) + 1))
	for {
		r.mu.Lock()
		r.lastWait = wait
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		before := r.stats.Sessions.Load()
		up, started, kind, cause := r.once(ctx)
		cameUp := r.stats.Sessions.Load() > before
		if ctx.Err() != nil {
			return
		}
		if kind.Final() {
			r.end(kind, cause)
			return
		}
		if started {
			if up >= o.ResetAfter {
				backoff = o.MinBackoff
			}
			// Only a session that came up and dropped counts (the science
			// agent's ruling on #301): a relay that never comes up retries
			// at the backoff's cap for as long as the job runs.
			if cameUp && r.restartsInWindow(time.Now()) > o.RestartLimit {
				r.mu.Lock()
				last := r.lastEnd
				r.mu.Unlock()
				r.end(EndFailed, fmt.Sprintf("more than %d restarts of the %s channel within %s after sessions that came up; the last end: %s", o.RestartLimit, r.ch, o.RestartWindow, last))
				return
			}
		}
		wait = jitter(backoff)
		backoff = min(2*backoff, o.MaxBackoff)
	}
}

// restartsInWindow records, at now, the drop of a session that came up, which
// a restart follows, and counts those within the restart window: the 121st
// in an hour, at the default limit of 120, is final.
func (r *relay) restartsInWindow(now time.Time) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	keep := r.restarts[:0]
	for _, t := range r.restarts {
		if now.Sub(t) < r.o.o.RestartWindow {
			keep = append(keep, t)
		}
	}
	r.restarts = append(keep, now)
	return len(r.restarts)
}

// jitter is d less a random part of up to half of it, so restarts of many
// relays spread out and none waits longer than its backoff.
func jitter(d time.Duration) time.Duration {
	if d <= 1 {
		return d
	}
	return d - time.Duration(rand.Int64N(int64(d/2)+1))
}

// once makes one attempt: it returns how long the relay was up, whether its
// process started, and what its end means.
func (r *relay) once(ctx context.Context) (time.Duration, bool, EndKind, string) {
	o := r.o.o
	id, kind, why := r.jobIsOurs(ctx)
	if why != "" {
		r.refuse(why)
		return 0, false, kind, why
	}
	seq := seqs.Add(1)
	line, err := agentapi.RelayLine(id, r.t.AgentPath, agentapi.RelayArgs{Channel: r.ch, Dir: r.t.AllocDir, Allocation: r.t.UID, Epoch: r.epoch, Seq: seq})
	if err != nil {
		r.refuse(err.Error())
		return 0, false, EndFailed, "the relay line: " + err.Error()
	}
	cmd, err := r.t.Command(line)
	if err != nil {
		r.refuse("the ssh client: " + err.Error())
		return 0, false, EndRetried, ""
	}
	// The relay keeper's start (OwnerOptions.Pidfd) in place of alpha's,
	// which is startProcess, unchanged.
	startProcess := startProcess
	if o.Pidfd {
		startProcess = func(cmd *exec.Cmd) (*process, error) { return startPidfdProcess(cmd, o.Logger) }
	}
	if r.t.BeforeStart != nil && !r.t.BeforeStart(r.ch, r.epoch, seq) {
		r.refuse("the start was not allowed (Target.BeforeStart)")
		return 0, false, EndRetried, ""
	}
	p, err := startProcess(cmd)
	if err != nil {
		r.refuse("the ssh client could not be started: " + err.Error())
		if r.t.OnStartFailed != nil {
			r.t.OnStartFailed(r.ch)
		}
		return 0, false, EndRetried, ""
	}
	start := time.Now()
	r.mu.Lock()
	r.pid, r.starts, r.since, r.seq, r.refusal = p.pid, r.starts+1, start, seq, ""
	r.mu.Unlock()
	o.Logger.Info("relay started", "tag", r.t.Tag, "channel", r.ch, "job", id, "pid", p.pid, "epoch", r.epoch, "seq", seq)
	if r.t.OnStart != nil {
		r.t.OnStart(r.ch)
	}
	sctx, cancel := context.WithCancel(ctx)
	served := make(chan error, 1)
	go func() { served <- Serve(sctx, p, r.target, r.t.UID, o.Logger, &r.stats) }()
	var serveErr error
	serveEnded := false
	select {
	case <-p.done:
	case serveErr = <-served:
		serveEnded = true
		// The relay's output ended first: give its process the stop grace
		// to exit by itself, so its exit code (ExitSuperseded, ExitNotReady)
		// is read, not lost to the TERM below.
		select {
		case <-p.done:
		case <-time.After(o.StopGrace):
		case <-ctx.Done():
		}
	case <-ctx.Done():
	}
	p.stop(o.StopGrace)
	cancel()
	if !serveEnded {
		serveErr = <-served
	}
	up := time.Since(start)
	exit := -1
	if ps := p.cmd.ProcessState; ps != nil {
		exit = ps.ExitCode()
	}
	var stderr, failure string
	if r.t.Stderr != nil {
		stderr = r.t.Stderr(p.cmd)
	}
	kind, cause := classifyEnd(exit, stderr)
	switch {
	case errors.Is(serveErr, ErrUnexpectedOutput), errors.Is(serveErr, ErrNoPreamble):
		kind, cause = EndRetried, serveErr.Error()
	case kind == EndRetried && cause == "" && exit != 0 && r.t.Describe != nil:
		failure = r.t.Describe(p.cmd)
		cause = failure
	}
	r.mu.Lock()
	r.pid, r.lastEnded, r.failure, r.lastEnd = 0, time.Now(), failure, kind.String()+": "+cause
	r.mu.Unlock()
	o.Logger.Info("relay ended", "tag", r.t.Tag, "channel", r.ch, "job", id, "up", up.Round(time.Millisecond), "exit", exit, "end", kind.String(), "cause", cause)
	if r.t.OnExit != nil {
		r.t.OnExit(r.ch)
	}
	return up, true, kind, cause
}

// jobIsOurs reads the Provider's latest listing under the Allocation's tag
// and returns the job ID to start on: the recorded job, listed RUNNING with
// the tag as its name (HBEP-6: "Before each start, the Provider's latest queue
// listing ... must show the recorded job ID RUNNING, with its name equal to
// the Allocation's tag. Otherwise the relay isn't started."). A job listed
// ended or completing is the job's end, final. A job listed pending or held,
// or not listed under the tag, is not started on and is read again after the
// backoff: a listing can miss a running job for a while (the Provider's
// absence grace, LostGrace), and a final end there would leave a running
// Allocation without relays for good, since its observation never changes.
// The controller's Drop stops the relays once it sees the job end, and srun's
// own refusal that the job does not exist or is completing ends one for good
// (ClassifyEnd).
func (r *relay) jobIsOurs(ctx context.Context) (string, EndKind, string) {
	jobs, err := r.t.Jobs.ListByTag(ctx, r.t.Tag)
	if err != nil {
		return "", EndRetried, "the listing under the tag could not be read: " + err.Error()
	}
	for _, j := range jobs {
		if j.ID != r.t.JobID || j.Name != r.t.Tag {
			continue
		}
		switch {
		case j.Completing:
			return "", EndJobGone, fmt.Sprintf("job %s is completing", j.ID)
		case j.Phase == v1alpha1.AllocationRunning:
			return j.ID, EndRetried, ""
		case j.Phase == v1alpha1.AllocationPending || j.Phase == v1alpha1.AllocationHeld:
			return "", EndRetried, fmt.Sprintf("job %s is %s, not running yet", j.ID, j.Phase)
		}
		return "", EndJobGone, fmt.Sprintf("job %s is %s, not running", j.ID, j.Phase)
	}
	return "", EndRetried, fmt.Sprintf("job %s is not listed under tag %s", r.t.JobID, r.t.Tag)
}

// process is a started relay ssh: its pipes are the session's transport.
type process struct {
	cmd  *exec.Cmd
	pid  int
	in   io.WriteCloser
	out  io.ReadCloser
	done chan struct{}
	// pf is the relay keeper's pidfd for the process (startPidfdProcess);
	// nil for alpha's.
	pf *pidfdHold
}

// startProcess starts cmd in a process group of its own, with pipes for its
// standard input and output, and a KILL when this process dies
// (startLockedThread).
func startProcess(cmd *exec.Cmd) (*process, error) {
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	if err := startLockedThread(cmd); err != nil {
		return nil, err
	}
	p := &process{cmd: cmd, pid: cmd.Process.Pid, in: in, out: out, done: make(chan struct{})}
	go func() { _ = cmd.Wait(); close(p.done) }()
	return p, nil
}

// The starter: every relay is created on one goroutine that locks its thread
// for good (HBEP-6, The ssh process: "started from a thread that stays locked
// for good, with Pdeathsig=SIGKILL ... so it dies with the controller
// process"). A parent-death signal is sent when the thread that created the
// process exits, not when the process does (go.dev/issue/27505); this thread
// exits only with the process. The pattern is the fake Slurm's (startProc,
// #274).
var (
	starterOnce sync.Once
	starterReqs chan startReq
)

type startReq struct {
	cmd  *exec.Cmd
	done chan error
}

func startLockedThread(cmd *exec.Cmd) error {
	starterOnce.Do(func() {
		starterReqs = make(chan startReq)
		go func() {
			runtime.LockOSThread() // never unlocked: see startLockedThread
			for q := range starterReqs {
				q.done <- q.cmd.Start()
			}
		}()
	})
	done := make(chan error, 1)
	starterReqs <- startReq{cmd: cmd, done: done}
	return <-done
}

func (p *process) Read(b []byte) (int, error)  { return p.out.Read(b) }
func (p *process) Write(b []byte) (int, error) { return p.in.Write(b) }

// Close closes the pipes; the process is stopped by stop.
func (p *process) Close() error {
	return errors.Join(p.in.Close(), p.out.Close())
}

// stop stops the relay by its PID: TERM to its process group, KILL after
// grace, and returns once it is reaped. A relay that already exited is only
// waited for.
func (p *process) stop(grace time.Duration) {
	if p.pf != nil {
		p.pf.stop(p, grace)
		return
	}
	select {
	case <-p.done:
		return
	default:
	}
	_ = syscall.Kill(-p.pid, syscall.SIGTERM)
	t := time.NewTimer(grace)
	defer t.Stop()
	select {
	case <-p.done:
	case <-t.C:
		_ = syscall.Kill(-p.pid, syscall.SIGKILL)
		<-p.done
	}
}
