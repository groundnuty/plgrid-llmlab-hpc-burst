package relaykeeper

// The keeper's real Runner (issue #343, PR 3): one hostdial.Owner per
// Provider, with pidfd signals, the keeper's epoch E, and one hostdial.Slots
// shared by every Owner, so HBEP-4's 4 relays per login host hold across
// Providers and generations. hostdial is reused whole (HBEP-6, The relay
// keeper; the plan's section 3): the relay loop, its backoff and cap, the
// session and its preamble, the job side.
//
// The keeper calls Want and Drop with its lock held. hostdial's Drop blocks
// until the relays' processes are gone, the relays' hooks take the keeper's
// lock, and a Want's seeds (Want.Finals) take it too. So neither call reaches
// hostdial at once: each is queued for the Provider, and one worker per
// Provider, holding no keeper lock, reads the seeds, builds the Target and
// calls the Owner. The queue coalesces: it keeps only the latest pending
// Want or Drop per tag, level-triggered, latest wins, so a worker stalled in
// a blocking Drop while sets keep arriving holds at most one entry per
// Allocation, and the keeper never blocks on it.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/sshrun"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// OwnerRunnerOptions configure the real Runner.
type OwnerRunnerOptions struct {
	// Epoch is the keeper's E, every relay's epoch.
	Epoch int64
	// Ports are each channel's loopback port on this host, the only port its
	// streams dial (hostdial.OwnerOptions.Ports).
	Ports map[agentapi.RelayChannel]int
	// Slots is shared by every Provider's Owner; nil means a new one of
	// hostdial's default size.
	Slots *hostdial.Slots
	// SSHDir holds the keeper's -F files, mode 700: one per Provider and ssh
	// settings, under a stable name, so a restarted keeper writes the same
	// file again (sshrun.RelayOptions.ConfigFile).
	SSHDir string
	// SSHBinary and SSHEnv are the relays' ssh; empty means sshrun's
	// defaults.
	SSHBinary string
	SSHEnv    []string
	// MinBackoff, MaxBackoff and StopGrace are hostdial's; zero means its
	// defaults.
	MinBackoff, MaxBackoff, StopGrace time.Duration
	// Logger, nil for none.
	Logger *slog.Logger
	// newOwner makes an Owner; a test replaces it. Nil is hostdial.NewOwner.
	newOwner func(hostdial.OwnerOptions) (*hostdial.Owner, error)
	// beforeOp, when set, is called by a Provider's worker before each queued
	// Want or Drop, with its tag; a test stalls the worker with it.
	beforeOp func(provider, tag string)
}

// OwnerRunner is the real Runner. Start it before the keeper takes sets; Stop
// stops every relay by pidfd and returns once their processes are gone.
type OwnerRunner struct {
	o      OwnerRunnerOptions
	ctx    context.Context
	cancel context.CancelFunc

	mu    sync.Mutex
	provs map[string]*provRunner
	wg    sync.WaitGroup
}

// NewOwnerRunner checks o and makes the Runner, whose Owners run under ctx.
func NewOwnerRunner(ctx context.Context, o OwnerRunnerOptions) (*OwnerRunner, error) {
	if o.Epoch <= 0 || len(o.Ports) != len(agentapi.RelayChannels) || o.SSHDir == "" {
		return nil, errors.New("relaykeeper: the Runner needs E, a port per channel and an ssh directory")
	}
	if err := os.MkdirAll(o.SSHDir, 0o700); err != nil {
		return nil, fmt.Errorf("relaykeeper: the ssh directory: %w", err)
	}
	if err := os.Chmod(o.SSHDir, 0o700); err != nil {
		return nil, fmt.Errorf("relaykeeper: the ssh directory: %w", err)
	}
	if o.Slots == nil {
		o.Slots = hostdial.NewSlots(0)
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if o.newOwner == nil {
		o.newOwner = hostdial.NewOwner
	}
	ctx, cancel := context.WithCancel(ctx)
	return &OwnerRunner{o: o, ctx: ctx, cancel: cancel, provs: map[string]*provRunner{}}, nil
}

// Stop stops every Provider's relays and returns once their processes are
// gone.
func (r *OwnerRunner) Stop() {
	r.cancel()
	r.wg.Wait()
}

// provRunner is one Provider's Owner, its ssh settings and its queue.
type provRunner struct {
	r    *OwnerRunner
	name string
	own  *hostdial.Owner
	// queue is the worker's pending work, coalesced per tag.
	queue opQueue

	mu sync.Mutex
	// relays are the Provider's ssh settings' relay makers, by settings.
	relays map[string]*sshrun.Relay
	// cur is each tag's current instance; stopping are replaced or dropped
	// instances, kept while a process of theirs runs or a start of theirs
	// is in flight.
	cur      map[string]*relayInstance
	stopping []*relayInstance
}

// relayInstance is the relays of one Want of an Allocation: their identity, the
// login host and Provider UID they run under, and what their hooks told.
type relayInstance struct {
	want  Want
	host  string
	ident string

	mu      sync.Mutex
	pending map[agentapi.RelayChannel]bool      // a start at or past the gate, not resolved
	up      map[agentapi.RelayChannel]bool      // a process runs
	pid     map[agentapi.RelayChannel]int       // the running process's PID
	fence   map[agentapi.RelayChannel][2]int64  // the last start's (E, N)
	cmd     map[agentapi.RelayChannel]*exec.Cmd // the last ssh command made, whose process the start launches
}

func (in *relayInstance) busy() bool {
	in.mu.Lock()
	defer in.mu.Unlock()
	for _, ch := range agentapi.RelayChannels {
		if in.pending[ch] || in.up[ch] {
			return true
		}
	}
	return false
}

// prov is the Provider's runner, made, with its Owner started, on its first
// use: the Owner runs before its first Want (PR 1's limit: Want keeps the
// first of two equal targets).
func (r *OwnerRunner) prov(name string) *provRunner {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p := r.provs[name]; p != nil {
		return p
	}
	p := &provRunner{r: r, name: name, queue: newOpQueue(), relays: map[string]*sshrun.Relay{}, cur: map[string]*relayInstance{}}
	r.provs[name] = p
	epochRead := make(chan struct{})
	var once sync.Once
	own, err := r.o.newOwner(hostdial.OwnerOptions{
		Ports: r.o.Ports, Slots: r.o.Slots, Pidfd: true,
		Epoch: func(context.Context) (int64, error) {
			once.Do(func() { close(epochRead) })
			return r.o.Epoch, nil
		},
		MinBackoff: r.o.MinBackoff, MaxBackoff: r.o.MaxBackoff, StopGrace: r.o.StopGrace,
		Logger: r.o.Logger.With("provider", name),
	})
	if err != nil {
		// A kernel with no pidfd: no relay of this Provider ever starts, and
		// every Want is logged as refused.
		r.o.Logger.Error("the Provider's relay owner could not be made: its relays never start", "provider", name, "err", err)
		return p
	}
	p.own = own
	r.wg.Add(2)
	go func() {
		defer r.wg.Done()
		if err := own.Start(r.ctx); err != nil {
			r.o.Logger.Error("the Provider's relay owner", "provider", name, "err", err)
		}
	}()
	go func() {
		defer r.wg.Done()
		select {
		case <-epochRead:
		case <-r.ctx.Done():
			return
		}
		for {
			for _, op := range p.queue.take() {
				if r.o.beforeOp != nil {
					r.o.beforeOp(name, op.tag)
				}
				op.do()
			}
			select {
			case <-p.queue.wake:
			case <-r.ctx.Done():
				return
			}
		}
	}()
	return p
}

// opQueue is a Provider's pending Wants and Drops, coalesced: one entry per
// tag, the latest put. It never blocks.
type opQueue struct {
	mu    sync.Mutex
	order []string
	ops   map[string]func()
	wake  chan struct{}
}

type queuedOp struct {
	tag string
	do  func()
}

func newOpQueue() opQueue {
	return opQueue{ops: map[string]func(){}, wake: make(chan struct{}, 1)}
}

// put makes f the tag's pending work, replacing any not yet taken.
func (q *opQueue) put(tag string, f func()) {
	q.mu.Lock()
	if _, ok := q.ops[tag]; !ok {
		q.order = append(q.order, tag)
	}
	q.ops[tag] = f
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// take is the pending work, one per tag in the order the tags were first
// put, emptying the queue.
func (q *opQueue) take() []queuedOp {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]queuedOp, 0, len(q.order))
	for _, tag := range q.order {
		out = append(out, queuedOp{tag: tag, do: q.ops[tag]})
	}
	q.order, q.ops = nil, map[string]func(){}
	return out
}

// pending is how many tags have work pending.
func (q *opQueue) pending() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.order)
}

// relay is the Provider's relay maker for s, its -F file written under a
// stable name; the caller holds p.mu. A stated limit (the review of PR 356):
// the makers of a Provider's earlier ssh settings, and their -F files, are
// kept until the keeper stops, one per settings the Provider has used, and
// the files stay in the keeper's ssh directory after it stops; a restarted
// keeper writes the same names again.
func (p *provRunner) relay(s wire.SSH) (*sshrun.Relay, error) {
	b, _ := json.Marshal(s)
	sum := sha256.Sum256(b)
	key := hex.EncodeToString(sum[:])[:16]
	if rl := p.relays[key]; rl != nil {
		return rl, nil
	}
	rl, err := sshrun.NewRelay(sshrun.RelayOptions{
		Name: p.name, Binary: p.r.o.SSHBinary, Env: p.r.o.SSHEnv, ControlDir: p.r.o.SSHDir,
		ConfigFile: filepath.Join(p.r.o.SSHDir, p.name+"-"+key+".conf"),
		Config:     slurm.SSHConfig{HostName: s.HostName, User: s.User, Port: s.Port, IdentityFile: s.IdentityFile, UserKnownHostsFile: s.KnownHostsFile},
	})
	if err != nil {
		return nil, err
	}
	p.relays[key] = rl
	return rl, nil
}

// LoginHost is the login host the Provider's relays for s reach, as ssh -G
// resolves it, lower case; when s cannot be made into a relay's settings, it
// is the configured HostName, lower case, and every start of it fails.
func (r *OwnerRunner) LoginHost(provider string, s wire.SSH) string {
	p := r.prov(provider)
	p.mu.Lock()
	rl, err := p.relay(s)
	p.mu.Unlock()
	if err != nil {
		r.o.Logger.Error("the Provider's ssh settings cannot be used", "provider", provider, "err", err)
		return strings.ToLower(s.HostName)
	}
	return strings.ToLower(rl.Login())
}

// Want queues w's Allocation's relays for the Provider's worker, replacing
// the tag's pending Want or Drop.
func (r *OwnerRunner) Want(provider string, s wire.SSH, w Want) {
	p := r.prov(provider)
	p.enqueue(w.Entry.Tag, func() { p.want(s, w) })
}

// Drop queues the stop of the Allocation's relays for the Provider's worker,
// replacing the tag's pending Want or Drop.
func (r *OwnerRunner) Drop(provider, tag string) {
	p := r.prov(provider)
	p.enqueue(tag, func() { p.drop(tag) })
}

func (p *provRunner) enqueue(tag string, f func()) {
	if p.own == nil {
		return // no Owner: nothing would run it
	}
	p.queue.put(tag, f)
}

// want is a Want on the worker: the seeds read now, with no keeper lock
// held, then the Owner's Want. A Want of the same identity changes nothing;
// another identity replaces the instance, which is kept as stopping.
func (p *provRunner) want(s wire.SSH, w Want) {
	if p.own == nil {
		return
	}
	p.mu.Lock()
	rl, err := p.relay(s)
	p.mu.Unlock()
	if err != nil {
		p.r.o.Logger.Error("a relay's ssh settings cannot be used: it is not started", "provider", p.name, "tag", w.Entry.Tag, "err", err)
		return
	}
	host := strings.ToLower(rl.Login())
	e := w.Entry
	// The Want's incarnation is the relays' identity (Want.Incarnation): a
	// re-created Provider, a new generation, a move, and a Drop followed by
	// the same entry wanted again each start their own relays, and only a
	// re-sent Want of the same incarnation is the same.
	ident := e.Gen + "/" + w.ProviderUID + "/" + strconv.FormatUint(w.Incarnation, 10)
	p.mu.Lock()
	if in := p.cur[e.Tag]; in != nil && in.want.Entry == e && in.host == host && in.ident == ident {
		p.mu.Unlock()
		return
	}
	in := &relayInstance{want: w, host: host, ident: ident,
		pending: map[agentapi.RelayChannel]bool{}, up: map[agentapi.RelayChannel]bool{},
		pid: map[agentapi.RelayChannel]int{}, fence: map[agentapi.RelayChannel][2]int64{}, cmd: map[agentapi.RelayChannel]*exec.Cmd{}}
	if old := p.cur[e.Tag]; old != nil {
		p.stopping = append(p.stopping, old)
	}
	p.cur[e.Tag] = in
	p.mu.Unlock()
	p.own.Want(hostdial.Target{
		Tag: e.Tag, JobID: e.JobID, AgentPath: e.AgentPath, AllocDir: e.AllocDir, UID: e.UID, Login: host,
		Jobs: w.Jobs, Stderr: sshrun.Stderr, Describe: sshrun.Classify, Gen: ident,
		Final: w.Finals(),
		// The command is kept per channel, so the started process's PID is
		// known at OnStart even for a replaced instance, which hostdial no
		// longer lists.
		Command: func(line string) (*exec.Cmd, error) {
			cmd, err := rl.Command(line)
			if ch, ok := relayChannelOf(line); ok && err == nil {
				in.mu.Lock()
				in.cmd[ch] = cmd
				in.mu.Unlock()
			}
			return cmd, err
		},
		// The start is marked pending BEFORE the keeper's gate is asked, and
		// cleared on its refusal, so the instance is never reaped while a
		// start of it is at or past the gate (duty 4).
		BeforeStart: func(ch agentapi.RelayChannel, epoch, seq int64) bool {
			// Only the tag's current instance may start: a stopping or
			// forgotten one, whose attempt was already past its listing when
			// it was replaced or dropped, never launches (the review of PR
			// 356). Checked and marked pending under p.mu together, so reap
			// cannot forget the instance in between.
			p.mu.Lock()
			if p.cur[e.Tag] != in {
				p.mu.Unlock()
				return false
			}
			in.mu.Lock()
			in.pending[ch] = true
			in.mu.Unlock()
			p.mu.Unlock()
			if w.BeforeStart != nil && !w.BeforeStart(ch, epoch, seq) {
				in.mu.Lock()
				in.pending[ch] = false
				in.mu.Unlock()
				p.reap()
				return false
			}
			in.mu.Lock()
			in.fence[ch] = [2]int64{epoch, seq}
			in.mu.Unlock()
			return true
		},
		OnStart: func(ch agentapi.RelayChannel) {
			in.mu.Lock()
			pid := 0
			if c := in.cmd[ch]; c != nil && c.Process != nil {
				pid = c.Process.Pid
			}
			in.pending[ch], in.up[ch], in.pid[ch] = false, true, pid
			f := in.fence[ch]
			in.mu.Unlock()
			w.OnStart(ch, f[0], f[1])
		},
		OnStartFailed: func(ch agentapi.RelayChannel) {
			in.mu.Lock()
			in.pending[ch] = false
			in.mu.Unlock()
			w.StartFailed(ch)
			p.reap()
		},
		OnExit: func(ch agentapi.RelayChannel) {
			in.mu.Lock()
			in.up[ch], in.pid[ch] = false, 0
			in.mu.Unlock()
			p.reap()
		},
		OnFinal: func(ch agentapi.RelayChannel, kind hostdial.EndKind, cause string) {
			in.mu.Lock()
			f := in.fence[ch]
			in.mu.Unlock()
			w.OnFinal(ch, kind, cause, f[0], f[1])
		},
	})
}

// relayChannelOf is the channel a relay line starts ("... relay <channel>
// ...", agentapi.RelayLine).
func relayChannelOf(line string) (agentapi.RelayChannel, bool) {
	f := strings.Fields(line)
	for i := 0; i+1 < len(f); i++ {
		if f[i] == agentapi.RelaySubcommand {
			for _, ch := range agentapi.RelayChannels {
				if f[i+1] == string(ch) {
					return ch, true
				}
			}
		}
	}
	return "", false
}

// drop is a Drop on the worker: the instance is kept as stopping, and the
// Owner's Drop returns once its processes are gone.
func (p *provRunner) drop(tag string) {
	if p.own == nil {
		return
	}
	p.mu.Lock()
	if in := p.cur[tag]; in != nil {
		p.stopping = append(p.stopping, in)
		delete(p.cur, tag)
	}
	p.mu.Unlock()
	p.own.Drop(tag)
	p.reap()
}

// reap forgets the stopping instances with no process running and no start
// in flight.
func (p *provRunner) reap() {
	p.mu.Lock()
	defer p.mu.Unlock()
	kept := p.stopping[:0]
	for _, in := range p.stopping {
		if in.busy() {
			kept = append(kept, in)
		}
	}
	p.stopping = kept
}

// Relays lists the Provider's relays: each current instance's, as its Owner
// shows them, and each stopping relayInstance's that still run, as up.
func (r *OwnerRunner) Relays(provider string) []wire.RelayState {
	r.mu.Lock()
	p := r.provs[provider]
	r.mu.Unlock()
	if p == nil || p.own == nil {
		return nil
	}
	states := p.own.Relays()
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []wire.RelayState
	for _, s := range states {
		in := p.cur[s.Tag]
		if in == nil || in.ident != s.Gen {
			continue
		}
		st := wire.StateRetrying
		switch {
		case s.Final:
			st = wire.StateFinal
		case s.PID != 0:
			st = wire.StateUp
		}
		lastEnd, _, _ := strings.Cut(s.LastEnd, ":")
		out = append(out, wire.RelayState{UID: in.want.Entry.UID, Tag: s.Tag, JobID: s.JobID, Channel: s.Channel, State: st, PID: s.PID,
			Epoch: s.Epoch, Seq: s.Seq, Starts: s.Starts, Since: s.Since, LastEnd: lastEnd, LoginHost: in.host, ProviderUID: in.want.ProviderUID})
	}
	for _, in := range p.stopping {
		in.mu.Lock()
		for _, ch := range agentapi.RelayChannels {
			if in.up[ch] {
				out = append(out, wire.RelayState{UID: in.want.Entry.UID, Tag: in.want.Entry.Tag, JobID: in.want.Entry.JobID, Channel: ch,
					State: wire.StateUp, PID: in.pid[ch], Epoch: in.fence[ch][0], Seq: in.fence[ch][1], LoginHost: in.host, ProviderUID: in.want.ProviderUID})
			}
		}
		in.mu.Unlock()
	}
	return out
}

// inFlight reports whether a start of the Provider's relays on host is in
// flight, allowed and not resolved, for a test.
func (r *OwnerRunner) inFlight(provider string) int {
	r.mu.Lock()
	p := r.provs[provider]
	r.mu.Unlock()
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	all := append([]*relayInstance{}, p.stopping...)
	for _, in := range p.cur {
		all = append(all, in)
	}
	for _, in := range all {
		in.mu.Lock()
		for _, ch := range agentapi.RelayChannels {
			if in.pending[ch] {
				n++
			}
		}
		in.mu.Unlock()
	}
	return n
}
