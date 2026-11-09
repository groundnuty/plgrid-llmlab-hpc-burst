package hostdial

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os/exec"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The relay owner's timings (HBEP-6, Dial-home modes, host-dialed mode:
// "Restart backoff: 1 s, doubling to 30 s, with jitter, and reset once a
// relay has stayed up for 60 s").
const (
	DefaultMinBackoff = time.Second
	DefaultMaxBackoff = 30 * time.Second
	DefaultResetAfter = 60 * time.Second
	// DefaultStopGrace is how long a stopped relay's ssh has after TERM
	// before KILL.
	DefaultStopGrace = 5 * time.Second
	// DefaultCheckTimeout bounds the job-is-ours read before each start.
	DefaultCheckTimeout = time.Minute
)

// RelayStartWorstCase is the longest from the moment a relay is due until its
// ssh runs, once a slot is free: the job-is-ours read's bound and the longest
// backoff. A live run's derived timeouts count it by this name.
const RelayStartWorstCase = DefaultCheckTimeout + DefaultMaxBackoff

// JobReader reads the queue listing fresh, under an Allocation's tag: squeue
// --me, never a cached listing. *slurm.Batch implements it.
type JobReader interface {
	ListByTagFresh(ctx context.Context, tag string) ([]provider.Job, error)
}

// Target is what the relays of one running Allocation need.
type Target struct {
	// Tag is the Allocation's job tag; it names the relays.
	Tag string
	// JobID is the Allocation's recorded job. A relay starts only when a fresh
	// listing under Tag shows this job running (the job-is-ours check).
	JobID string
	// AgentPath is the staged agent binary's absolute path on the cluster,
	// the relay step's program.
	AgentPath string
	// Login names the login node for the per-login-node cap: its host and
	// port as the Provider's credentials give them.
	Login string
	// Jobs reads the listing for the job-is-ours check.
	Jobs JobReader
	// Command returns the ssh client that runs line on the login node, on a
	// connection of its own (sshrun.Relay's Command). The owner sets its
	// standard streams and process group and starts it.
	Command func(line string) (*exec.Cmd, error)
	// Describe, when set, names the kind of an ended relay's failure from its
	// command, without repeating its output (sshrun.Classify); the owner logs
	// it with the exit code.
	Describe func(cmd *exec.Cmd) string
	// Gen names the credentials and the transport that Jobs and Command use:
	// a change (a rotated key, another known-hosts file, a new transport)
	// is another Target, so the running relays are stopped and started again
	// with the new ones, never left on the old.
	Gen string
}

// same reports whether u needs no change of t's relays: the same job, agent,
// login node and credentials.
func (t Target) same(u Target) bool {
	return t.Tag == u.Tag && t.JobID == u.JobID && t.AgentPath == u.AgentPath && t.Login == u.Login && t.Gen == u.Gen
}

// OwnerOptions configure an Owner.
type OwnerOptions struct {
	// Ports are each channel's loopback port on this host, the only port its
	// relays dial: agentapi.Host.RelayChannelPorts.
	Ports map[agentapi.RelayChannel]int
	// Slots caps relays per login node; nil means SharedSlots.
	Slots *Slots
	// MinBackoff, MaxBackoff and ResetAfter are the restart backoff; zero
	// means the defaults.
	MinBackoff, MaxBackoff, ResetAfter time.Duration
	// StopGrace is how long a stopped relay has after TERM; zero means
	// DefaultStopGrace.
	StopGrace time.Duration
	// CheckTimeout bounds the job-is-ours read; zero means
	// DefaultCheckTimeout.
	CheckTimeout time.Duration
	// Logger reports starts, stops and refusals; nil means none.
	Logger *slog.Logger
}

// Owner is the batch Provider's controller's relay owner (HBEP-6, Dial-home
// modes, host-dialed mode: the control-plane side). It keeps two relays, one
// per channel, for each Allocation it is told is running (Want), and stops
// them by PID when told it is not (Drop) or when its own run ends.
//
// It is level-triggered: each relay is supervised in its own loop, started
// again after the backoff when it drops, and the controller calls Want for
// every running Allocation on every observation, so a controller that starts
// or takes the lead starts the relays of every running Allocation. It is a
// leader-only runnable (Start, NeedLeaderElection): only the leader's relays
// run, and when the leader stops or loses the lead, Start stops every relay
// by PID before it returns. An Owner runs once; a new leader builds a new one.
type Owner struct {
	o OwnerOptions

	mu      sync.Mutex
	want    map[string]Target
	allocs  map[string]*allocRelays
	ctx     context.Context // set by Start
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
	if o.StopGrace <= 0 {
		o.StopGrace = DefaultStopGrace
	}
	if o.CheckTimeout <= 0 {
		o.CheckTimeout = DefaultCheckTimeout
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	return &Owner{o: o, want: map[string]Target{}, allocs: map[string]*allocRelays{}}, nil
}

// NeedLeaderElection makes the Owner a leader-only runnable of a
// controller-runtime manager.
func (*Owner) NeedLeaderElection() bool { return true }

// Want records that t's Allocation is running, and starts its two relays when
// the Owner runs and they are not already kept for the same job. A Want for
// the same tag with another job, agent, login or credentials (Gen) stops the
// old relays and starts new ones.
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

// Start runs the relays of every wanted Allocation until ctx ends, then stops
// each by PID and returns once every relay's process is gone.
func (o *Owner) Start(ctx context.Context) error {
	o.mu.Lock()
	if o.running || o.done {
		o.mu.Unlock()
		return errors.New("hostdial: the relay owner runs once")
	}
	o.ctx, o.running = ctx, true
	for tag, t := range o.want {
		o.allocs[tag] = o.startLocked(t)
	}
	o.mu.Unlock()
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
	// LastRefusal says why the last start did not happen ("" when none).
	LastRefusal string
	// LastFailure is the kind of the last process's failure (Target.Describe),
	// "" when it exited 0 or none ended.
	LastFailure string
	// Waiting is true while the Allocation waits for its pair of slots on the
	// login node; no relay of it has started then.
	Waiting bool
	// Gen is the Target's credentials generation.
	Gen string
	// LastEnded is when the last process ended, and LastWait the backoff the
	// relay waited after it before its next attempt.
	LastEnded time.Time
	LastWait  time.Duration
	// Streams and Joined count the streams the job opened over this relay
	// and those joined to the channel's port, over all its processes.
	Streams, Joined uint64
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
	// Stopping waits for processes; it runs outside the lock's holder's path
	// by a goroutine, and Want's caller does not wait for it. The new relays
	// may start meanwhile: the agent keeps the newer session.
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
		a.relays = append(a.relays, &relay{o: o, t: t, ch: ch, target: agentapi.RelayTargetAddr(o.o.Ports[ch])})
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
	target string

	mu        sync.Mutex
	pid       int
	starts    int
	since     time.Time
	refusal   string
	failure   string
	lastEnded time.Time
	lastWait  time.Duration
	stats     ServeStats
}

func (r *relay) state() RelayState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return RelayState{Tag: r.t.Tag, Channel: r.ch, JobID: r.t.JobID, PID: r.pid, Starts: r.starts, Since: r.since, LastRefusal: r.refusal,
		LastFailure: r.failure, Gen: r.t.Gen, LastEnded: r.lastEnded, LastWait: r.lastWait,
		Streams: r.stats.Streams.Load(), Joined: r.stats.Joined.Load()}
}

func (r *relay) refuse(why string) {
	r.mu.Lock()
	r.refusal = why
	r.mu.Unlock()
	r.o.o.Logger.Info("a relay did not start", "tag", r.t.Tag, "channel", r.ch, "why", why)
}

// loop keeps the relay up until ctx ends: check that the job is still the
// Allocation's and running, start the ssh, serve the session until it drops,
// stop the process by PID, wait the backoff, and again. The Allocation's
// slots stay taken throughout.
func (r *relay) loop(ctx context.Context) {
	o := r.o.o
	backoff := o.MinBackoff
	for ctx.Err() == nil {
		up, started := r.once(ctx)
		if ctx.Err() != nil {
			return
		}
		if started && up >= o.ResetAfter {
			backoff = o.MinBackoff
		}
		wait := jitter(backoff)
		r.mu.Lock()
		r.lastWait = wait
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		backoff = min(2*backoff, o.MaxBackoff)
	}
}

// jitter is d less a random part of up to half of it, so restarts of many
// relays spread out and none waits longer than its backoff.
func jitter(d time.Duration) time.Duration {
	if d <= 1 {
		return d
	}
	return d - time.Duration(rand.Int64N(int64(d/2)+1))
}

// once makes one attempt: it returns how long the relay was up, and whether
// its process started at all.
func (r *relay) once(ctx context.Context) (time.Duration, bool) {
	o := r.o.o
	id, why := r.jobIsOurs(ctx)
	if why != "" {
		r.refuse(why)
		return 0, false
	}
	line, err := agentapi.RelayLine(id, r.t.AgentPath, r.ch)
	if err != nil {
		r.refuse(err.Error())
		return 0, false
	}
	cmd, err := r.t.Command(line)
	if err != nil {
		r.refuse("the ssh client: " + err.Error())
		return 0, false
	}
	p, err := startProcess(cmd)
	if err != nil {
		r.refuse("the ssh client could not be started: " + err.Error())
		return 0, false
	}
	start := time.Now()
	r.mu.Lock()
	r.pid, r.starts, r.since, r.refusal = p.pid, r.starts+1, start, ""
	r.mu.Unlock()
	o.Logger.Info("relay started", "tag", r.t.Tag, "channel", r.ch, "job", id, "pid", p.pid)
	sctx, cancel := context.WithCancel(ctx)
	served := make(chan error, 1)
	go func() { served <- Serve(sctx, p, r.target, r.ch, o.Logger, &r.stats) }()
	serveEnded := false
	select {
	case <-p.done:
	case <-served:
		serveEnded = true
	case <-ctx.Done():
	}
	p.stop(o.StopGrace)
	cancel()
	if !serveEnded {
		<-served
	}
	up := time.Since(start)
	exit, kind := -1, ""
	if ps := p.cmd.ProcessState; ps != nil {
		exit = ps.ExitCode()
	}
	if exit != 0 && r.t.Describe != nil {
		kind = r.t.Describe(p.cmd)
	}
	r.mu.Lock()
	r.pid, r.lastEnded, r.failure = 0, time.Now(), kind
	r.mu.Unlock()
	o.Logger.Info("relay ended", "tag", r.t.Tag, "channel", r.ch, "job", id, "up", up.Round(time.Millisecond), "exit", exit, "failure", kind)
	return up, true
}

// jobIsOurs reads the listing under the Allocation's tag again and returns
// the job ID to start on: the recorded job, listed running. Otherwise it
// returns why not, and the relay does not start (HBEP-6: "Before each start,
// the job ID is read again from squeue --me under the Allocation's tag", so a
// relay never lands on another job).
func (r *relay) jobIsOurs(ctx context.Context) (string, string) {
	ctx, cancel := context.WithTimeout(ctx, r.o.o.CheckTimeout)
	defer cancel()
	jobs, err := r.t.Jobs.ListByTagFresh(ctx, r.t.Tag)
	if err != nil {
		return "", "the listing under the tag could not be read: " + err.Error()
	}
	for _, j := range jobs {
		if j.ID != r.t.JobID || j.Name != r.t.Tag {
			continue
		}
		if j.Phase != v1alpha1.AllocationRunning || j.Completing {
			return "", fmt.Sprintf("job %s is %s, not running", j.ID, j.Phase)
		}
		return j.ID, ""
	}
	return "", fmt.Sprintf("job %s is not listed under tag %s", r.t.JobID, r.t.Tag)
}

// process is a started relay ssh: its pipes are the session's transport.
type process struct {
	cmd  *exec.Cmd
	pid  int
	in   io.WriteCloser
	out  io.ReadCloser
	done chan struct{}
}

// startProcess starts cmd in a process group of its own, with pipes for its
// standard input and output; its standard error is left as cmd has it. A
// TERM reaches it if this process dies first.
func startProcess(cmd *exec.Cmd) (*process, error) {
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGTERM}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	p := &process{cmd: cmd, pid: cmd.Process.Pid, in: in, out: out, done: make(chan struct{})}
	go func() { _ = cmd.Wait(); close(p.done) }()
	return p, nil
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
