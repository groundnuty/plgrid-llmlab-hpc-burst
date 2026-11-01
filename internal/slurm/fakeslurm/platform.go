package fakeslurm

// New. The Platform (one cluster), its Config, and the Sessions that are the
// slurm.Runner a test hands to the plugin.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	_ "time/tzdata" // the zone of Config.TimeZone must not depend on the host

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

// Clock reads the time. *fake.Clock of pkg/provider/fake satisfies it.
type Clock interface{ Now() time.Time }

// NotListedMode says how squeue treats a job after it ends (S7's measured
// shapes).
type NotListedMode string

const (
	// NotListedHelios: the --me listing drops a job as soon as it ends. For
	// the job alone, squeue --jobs=<id> answers exit 0 with an empty list
	// until MinJobAge has passed since the end, then exit 1 with "Invalid job
	// id specified".
	NotListedHelios NotListedMode = "helios"
	// NotListedAthena: both list the ended job in its end state until
	// MinJobAge has passed since the end; then --me drops it and --jobs=<id>
	// gives the error.
	NotListedAthena NotListedMode = "athena"
)

// Partition is one partition of the fake cluster.
type Partition struct {
	Name string
	// GPUOnly refuses a job without --gres=gpu: as a definite rejection.
	GPUOnly bool
	// OneJobPerUser refuses a second live (pending or running) job of one
	// user.
	OneJobPerUser bool
	// Slots is the most jobs the partition runs at once; 0 is unlimited.
	Slots int
	// QOS is the partition's own QOS (listed in Config.QOS): every job on the
	// partition counts against it, and its limits come before the job QOS's.
	QOS string
}

// Config configures a Platform. The zero value is a v0.0.42 cluster in the
// Helios shape, MinJobAge 10 s, no partition list (any partition name is
// accepted), UTC.
type Config struct {
	// DataParser is "v0.0.40" or "v0.0.42" (default): the shape of the JSON
	// and the value of meta.plugin.data_parser.
	DataParser string
	// ReplyDataParser, when set, replaces the meta value only, for mismatch
	// tests. It may hold the "data_parser/" prefix or not; the fake adds it.
	ReplyDataParser string
	// MinJobAge is how long an ended job stays known to slurmctld; default
	// 10 s.
	MinJobAge time.Duration
	// NotListed is the "not listed" shape; default NotListedHelios.
	NotListed NotListedMode
	// Partitions lists the cluster's partitions. Empty means any name is
	// accepted without policy; a job for a name not listed is refused.
	Partitions []Partition
	// TimeZone names the zone of the text output and of sacct's calendar
	// dates, for example "Europe/Warsaw"; default "UTC".
	TimeZone string
	// SqueueDesc lists squeue jobs by descending job ID, to show that a reader
	// sorts by ID itself.
	SqueueDesc bool
	// QOS lists the cluster's QOSes with their per-user limits (qos.go). Empty
	// means no QOS is modelled: --qos is accepted with any name and no QOS
	// limit applies.
	QOS []QOS
	// DefaultQOS is the QOS of a job that sends no --qos: the association's
	// default. Empty means none.
	DefaultQOS string
	// Run, when set, puts the platform in run mode (doc.go, Run mode): the
	// file system is a real directory, and each job's --wrap runs as a local
	// process. A Platform in run mode must be closed.
	Run *RunMode
}

// Call is one simple command that ran on the login node (doc.go, Call log).
type Call struct {
	User string
	// Words are the command's words with quotes stripped. "cat > f" is
	// ["cat", ">", "f"].
	Words []string
	// StdinLen is the length of the standard input of a cat command; the
	// content is never stored.
	StdinLen int
	Time     time.Time
}

// Platform is one fake Slurm cluster. It is safe for concurrent use.
type Platform struct {
	clock Clock
	cfg   Config
	zone  *time.Location

	// run is the normalized run mode; nil outside it.
	run   *RunMode
	knobs runKnobs // run mode only

	mu       sync.Mutex
	jobs     []*job
	nextID   int
	fs       fileSystem
	closed   bool // run mode: Close was called, no job starts any more
	phantoms map[string]int
	calls    []Call
	faulted  []Fault
	rules    []*faultRule
	script   []SubmitStep
	late     []lateJob
	gates    map[string][]*Gate

	inflight, maxInflight int
}

// New returns a Platform that reads clock. It panics on a Config it cannot
// use (an unknown data parser, mode or zone), since that is a test bug.
func New(clock Clock, cfg Config) *Platform {
	if cfg.DataParser == "" {
		cfg.DataParser = "v0.0.42"
	}
	if cfg.DataParser != "v0.0.40" && cfg.DataParser != "v0.0.42" {
		panic(fmt.Sprintf("fakeslurm: unknown DataParser %q", cfg.DataParser))
	}
	if cfg.MinJobAge == 0 {
		cfg.MinJobAge = 10 * time.Second
	}
	switch cfg.NotListed {
	case "":
		cfg.NotListed = NotListedHelios
	case NotListedHelios, NotListedAthena:
	default:
		panic(fmt.Sprintf("fakeslurm: unknown NotListed mode %q", cfg.NotListed))
	}
	if cfg.TimeZone == "" {
		cfg.TimeZone = "UTC"
	}
	zone, err := time.LoadLocation(cfg.TimeZone)
	if err != nil {
		panic(fmt.Sprintf("fakeslurm: TimeZone: %v", err))
	}
	cfg.Partitions = append([]Partition(nil), cfg.Partitions...)
	p := &Platform{
		clock: clock, cfg: cfg, zone: zone, nextID: firstJobID, fs: newFS(),
		phantoms: map[string]int{}, gates: map[string][]*Gate{},
	}
	if cfg.Run != nil {
		p.run = normalizeRun(*cfg.Run)
		p.cfg.Run = p.run
		d, err := newDiskFS(p.run.Root)
		if err != nil {
			panic(fmt.Sprintf("fakeslurm: RunMode.Root: %v", err))
		}
		p.fs = d
		p.knobs = hostKnobs()
	}
	return p
}

// Login is a user's login: a name and the environment of the user's shell.
type Login struct {
	User string
	// Env is the environment printenv reads (for example HOME and SCRATCH).
	// A variable not in it is unset: printenv exits 1.
	Env map[string]string
}

// Session is one user's connection to the platform's login node. It implements
// slurm.Runner. A Session is safe for concurrent use.
type Session struct {
	p    *Platform
	user string
	env  map[string]string
	gen  atomic.Uint64
}

var _ slurm.Runner = (*Session)(nil)

// Login returns a Session for l. An empty user is "example-user".
func (p *Platform) Login(l Login) *Session {
	if l.User == "" {
		l.User = "example-user"
	}
	env := make(map[string]string, len(l.Env))
	for k, v := range l.Env {
		env[k] = v
	}
	s := &Session{p: p, user: l.User, env: env}
	s.gen.Store(1)
	return s
}

// User is the Session's user name.
func (s *Session) User() string { return s.user }

// Generation implements slurm.Runner. It starts at 1 and rises on every
// reconnect: Reconnect, or a command refused with slurm.ErrNotStarted.
func (s *Session) Generation() uint64 { return s.gen.Load() }

// Reconnect simulates a new connection to the login node.
func (s *Session) Reconnect() { s.gen.Add(1) }

// Run implements slurm.Runner. See doc.go for the grammar. The context ends a
// command that waits at a gate; the command's Timeout is ignored.
func (s *Session) Run(ctx context.Context, cmd slurm.Command) (slurm.Result, error) {
	p := s.p
	p.enter()
	defer p.leave()
	if err := ctx.Err(); err != nil {
		return slurm.Result{}, fmt.Errorf("%w: %v", slurm.ErrNotStarted, err)
	}
	if err := p.takeFault(s, cmd.Line); err != nil {
		return slurm.Result{}, err
	}
	cmds, perr := parseLine(cmd.Line)
	if perr != nil {
		return slurm.Result{Stderr: []byte("bash: " + perr.Error() + "\n"), ExitCode: 2}, nil
	}
	if len(cmds) > 0 {
		if err := p.waitGate(ctx, cmds[0].words[0]); err != nil {
			return slurm.Result{}, err
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.execLine(s, cmds, cmd.Stdin)
}

func (p *Platform) enter() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inflight++
	if p.inflight > p.maxInflight {
		p.maxInflight = p.inflight
	}
}

func (p *Platform) leave() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inflight--
}

// MaxConcurrent is the most Run calls in progress at once so far, counted from
// entry to exit, including time spent blocked at a gate.
func (p *Platform) MaxConcurrent() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.maxInflight
}

// ctxErr turns an ended context into the runner error the plugin reads: a
// deadline is slurm.ErrTimeout, a cancellation slurm.ErrTransport.
func ctxErr(ctx context.Context) error {
	if ctx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("%w: %v", slurm.ErrTimeout, ctx.Err())
	}
	return fmt.Errorf("%w: %v", slurm.ErrTransport, ctx.Err())
}

// Calls returns a copy of the call log.
func (p *Platform) Calls() []Call {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Call, len(p.calls))
	for i, c := range p.calls {
		c.Words = append([]string(nil), c.Words...)
		out[i] = c
	}
	return out
}

// Count is the number of calls, by any user, whose first word is word.
func (p *Platform) Count(word string) int { return p.CountFor("", word) }

// CountFor is the number of calls of user whose first word is word; an empty
// user means any user.
func (p *Platform) CountFor(user, word string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, c := range p.calls {
		if c.Words[0] == word && (user == "" || c.User == user) {
			n++
		}
	}
	return n
}

// logCall appends one call; the caller holds mu.
func (p *Platform) logCall(s *Session, c simpleCmd, stdinLen int, now time.Time) {
	p.calls = append(p.calls, Call{User: s.user, Words: c.callWords(), StdinLen: stdinLen, Time: now})
}
