package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Defaults of Options.
const (
	// DefaultProbeInterval applies when a desired state carries no
	// Settings.ProbeInterval.
	DefaultProbeInterval = 10 * time.Second
	// DefaultStopGrace applies when a desired state carries no
	// Settings.StopGrace, and to the stop at the agent's own exit.
	DefaultStopGrace = 30 * time.Second
	// DefaultStartupProbeInterval is how often a starting worker is probed
	// until its engine answers.
	DefaultStartupProbeInterval = 2 * time.Second
	// finalReportTimeout bounds the tries of the last report after a stop.
	finalReportTimeout = 30 * time.Second
)

// Options configure an Agent.
type Options struct {
	// API is the agent endpoint.
	API API
	// Engines gives the Engine behind a worker's socket.
	Engines EngineFactory
	// Stager stages images and checkpoints.
	Stager Stager
	// Apptainer is the apptainer binary.
	Apptainer string
	// RunDir is the job's private run directory (MakeRunDir).
	RunDir string
	// LogDir receives one log file per worker, the engine's output.
	LogDir string
	// Registration is what the agent registers with.
	Registration agentapi.RegisterRequest
	// BaseEnv is the environment apptainer runs with, before the launch's
	// variables (os.Environ() in production).
	BaseEnv []string
	// Logger receives the agent's log; it never gets a secret.
	Logger *slog.Logger

	// ReportInterval: a report at least this often (agentapi.ReportInterval
	// when 0).
	ReportInterval time.Duration
	// StartupProbeInterval: how often a starting worker is probed
	// (DefaultStartupProbeInterval when 0).
	StartupProbeInterval time.Duration
	// MinBackoff and MaxBackoff bound the retries of a failed call (1 s and
	// 30 s when 0).
	MinBackoff, MaxBackoff time.Duration
	// Now is the clock (time.Now when nil).
	Now func() time.Time
}

// Agent is the allocation agent (HBEP-7, The allocation agent): it registers,
// long-polls its Allocation's desired state, converges on it and reports full
// snapshots. The controller decides and the agent carries out: it never
// restarts a part by itself (agentapi's documentation, Who decides).
type Agent struct {
	o   Options
	log *slog.Logger

	// events carries work for the loop goroutine, which alone touches the
	// state below.
	events chan func()
	// desiredCh carries the newest desired state from the poller.
	desiredCh chan agentapi.DesiredState

	// regMu guards registered, which the poller and the reporter share.
	regMu      sync.Mutex
	registered bool

	rep *reporter

	// ---- loop state ----
	ctx         context.Context
	haveDesired bool
	desired     agentapi.DesiredState
	stopping    bool
	inst        *instance
	fwd         *Forwarder
	lastOffered []byte
	// settings from the newest desired state with an Action the agent knows.
	stopGrace     time.Duration
	probeInterval time.Duration
	imageFormat   agentapi.ImageFormat
	// probeEvery mirrors probeInterval for the probe goroutines.
	probeEvery atomic.Int64
	wg         sync.WaitGroup
}

// New returns an Agent.
func New(o Options) (*Agent, error) {
	var errs []error
	if o.API == nil {
		errs = append(errs, errors.New("agent: no API"))
	}
	if o.Engines == nil {
		errs = append(errs, errors.New("agent: no engine factory"))
	}
	if o.Stager == nil {
		errs = append(errs, errors.New("agent: no stager"))
	}
	if o.RunDir == "" {
		errs = append(errs, errors.New("agent: no run directory"))
	}
	if o.Apptainer == "" {
		errs = append(errs, errors.New("agent: no apptainer binary"))
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if o.ReportInterval <= 0 {
		o.ReportInterval = agentapi.ReportInterval
	}
	if o.StartupProbeInterval <= 0 {
		o.StartupProbeInterval = DefaultStartupProbeInterval
	}
	if o.MinBackoff <= 0 {
		o.MinBackoff = time.Second
	}
	if o.MaxBackoff < o.MinBackoff {
		o.MaxBackoff = max(30*time.Second, o.MinBackoff)
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.LogDir == "" {
		o.LogDir = o.RunDir
	}
	a := &Agent{
		o:             o,
		log:           o.Logger,
		events:        make(chan func(), 256),
		desiredCh:     make(chan agentapi.DesiredState, 1),
		stopGrace:     DefaultStopGrace,
		probeInterval: DefaultProbeInterval,
		imageFormat:   agentapi.ImageFormatSIF,
	}
	a.probeEvery.Store(int64(DefaultProbeInterval))
	a.rep = newReporter(a)
	return a, nil
}

// Run runs the agent until it is told to stop (ActionStop), which returns nil
// once every part has stopped and the last report is sent, or until ctx ends,
// which stops every part, sends a last report if it can and returns ctx's
// error. Losing the endpoint changes nothing the agent runs: it keeps its
// parts and its last desired state, and retries (HBEP-7, Unreachable).
func (a *Agent) Run(ctx context.Context) error {
	fwd, err := NewForwarder(a.o.RunDir + "/serve.sock")
	if err != nil {
		return fmt.Errorf("agent: serving socket: %w", err)
	}
	a.fwd = fwd
	defer fwd.Close()

	loopCtx, cancelLoop := context.WithCancel(ctx)
	defer cancelLoop()
	a.ctx = loopCtx
	repCtx, cancelRep := context.WithCancel(context.Background())
	defer cancelRep()

	go a.poll(loopCtx)
	go a.rep.run(repCtx)

	report := time.NewTicker(a.o.ReportInterval)
	defer report.Stop()
	a.offer(true)
	for {
		select {
		case <-ctx.Done():
			cancelLoop()
			a.shutdown()
			cancelRep()
			<-a.rep.done
			sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := a.rep.send(sctx, a.snapshot()); err != nil {
				a.log.Warn("last report not sent", "err", err)
			}
			cancel()
			a.wg.Wait()
			return ctx.Err()
		case d := <-a.desiredCh:
			a.receive(d)
		case ev := <-a.events:
			ev()
		case <-report.C:
			a.offer(true)
			continue
		}
		a.converge()
		a.offer(false)
		if a.stopping && (a.inst == nil || !a.inst.anyLive()) {
			a.log.Info("stopped on instruction; sending the last report")
			cancelLoop()
			cancelRep()
			<-a.rep.done
			err := a.rep.final(a.snapshot(), finalReportTimeout)
			a.wg.Wait()
			if err != nil {
				a.log.Warn("last report not sent", "err", err)
			}
			return nil
		}
	}
}

// post queues work for the loop goroutine. It never blocks the caller past
// the loop's end.
func (a *Agent) post(f func()) {
	select {
	case a.events <- f:
	case <-a.ctx.Done():
	}
}

// receive takes a desired state the poller fetched.
func (a *Agent) receive(d agentapi.DesiredState) {
	if !a.haveDesired || d.Version != a.desired.Version {
		a.log.Info("desired state", "version", d.Version, "action", d.Action)
	}
	a.haveDesired = true
	a.desired = d
}

// offer hands the reporter a snapshot when it differs from the last one
// offered, or always when force is set (the report interval).
func (a *Agent) offer(force bool) {
	r := a.snapshot()
	key := snapshotKey(r)
	if !force && string(key) == string(a.lastOffered) {
		return
	}
	a.lastOffered = key
	a.rep.offer(r)
}

// snapshotKey is the snapshot without its time, to tell a change.
func snapshotKey(r agentapi.Report) []byte {
	r.Time = time.Time{}
	b, _ := json.Marshal(r)
	return b
}

// ---- registration and polling ----

// ensureRegistered registers unless the endpoint already knows this agent
// instance ID.
func (a *Agent) ensureRegistered(ctx context.Context) error {
	a.regMu.Lock()
	defer a.regMu.Unlock()
	if a.registered {
		return nil
	}
	resp, err := a.o.API.Register(ctx, a.o.Registration)
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}
	if resp.ProtocolVersion != agentapi.ProtocolVersion {
		return fmt.Errorf("register: the endpoint speaks protocol %q, the agent %q", resp.ProtocolVersion, agentapi.ProtocolVersion)
	}
	a.registered = true
	a.log.Info("registered", "restartCount", a.o.Registration.RestartCount)
	return nil
}

// forget marks the registration lost, after CodeNotRegistered: the next call
// registers again at once, with the same agent instance ID.
func (a *Agent) forget() {
	a.regMu.Lock()
	a.registered = false
	a.regMu.Unlock()
}

// poll long-polls the desired state until ctx ends. A CodeNotRegistered answer
// registers again at once; any other failure retries with backoff, keeping the
// last desired state in force.
func (a *Agent) poll(ctx context.Context) {
	since := ""
	b := newBackoff(a.o.MinBackoff, a.o.MaxBackoff)
	for ctx.Err() == nil {
		if err := a.ensureRegistered(ctx); err != nil {
			a.log.Warn("registration failed; retrying", "code", codeOf(err), "err", err)
			b.wait(ctx)
			continue
		}
		d, err := a.o.API.Desired(ctx, since)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if codeOf(err) == agentapi.CodeNotRegistered {
				a.forget()
				continue
			}
			a.log.Warn("desired state poll failed; retrying", "code", codeOf(err), "err", err)
			b.wait(ctx)
			continue
		}
		b.reset()
		since = d.Version
		select {
		case <-a.desiredCh: // drop a state the loop has not taken: the newest wins
		default:
		}
		select {
		case a.desiredCh <- d:
		case <-ctx.Done():
			return
		}
	}
}

// backoff doubles a wait from min to max.
type backoff struct{ min, max, cur time.Duration }

func newBackoff(minD, maxD time.Duration) *backoff { return &backoff{min: minD, max: maxD, cur: minD} }

func (b *backoff) wait(ctx context.Context) {
	t := time.NewTimer(b.cur)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
	b.cur = min(2*b.cur, b.max)
}

func (b *backoff) reset() { b.cur = b.min }

// ---- reporting ----

// reporter sends the newest snapshot it was offered, retrying failures with
// backoff. It runs on its own goroutine, so a slow or lost endpoint never
// holds up the loop.
type reporter struct {
	a    *Agent
	mu   sync.Mutex
	next *agentapi.Report
	kick chan struct{}
	done chan struct{} // closed when run returns
}

func newReporter(a *Agent) *reporter {
	return &reporter{a: a, kick: make(chan struct{}, 1), done: make(chan struct{})}
}

func (r *reporter) offer(rep agentapi.Report) {
	r.mu.Lock()
	r.next = &rep
	r.mu.Unlock()
	select {
	case r.kick <- struct{}{}:
	default:
	}
}

func (r *reporter) take() *agentapi.Report {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := r.next
	r.next = nil
	return n
}

func (r *reporter) run(ctx context.Context) {
	defer close(r.done)
	b := newBackoff(r.a.o.MinBackoff, r.a.o.MaxBackoff)
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.kick:
		}
		for {
			rep := r.take()
			if rep == nil {
				break
			}
			if err := r.send(ctx, *rep); err != nil {
				if ctx.Err() != nil {
					return
				}
				r.a.log.Warn("report failed; retrying", "code", codeOf(err), "err", err)
				r.mu.Lock()
				if r.next == nil { // keep the failed one unless a newer came
					r.next = rep
				}
				r.mu.Unlock()
				b.wait(ctx)
				continue
			}
			b.reset()
		}
	}
}

// send sends one report, registering again at once after CodeNotRegistered.
func (r *reporter) send(ctx context.Context, rep agentapi.Report) error {
	if err := r.a.ensureRegistered(ctx); err != nil {
		return err
	}
	err := r.a.o.API.Report(ctx, rep)
	if codeOf(err) == agentapi.CodeNotRegistered {
		r.a.forget()
		if err := r.a.ensureRegistered(ctx); err != nil {
			return err
		}
		err = r.a.o.API.Report(ctx, rep)
	}
	return err
}

// final sends the last report, retrying until it is sent or the timeout
// passes. Any report still queued is superseded by it. run must have
// returned, so no older report can land after it.
func (r *reporter) final(rep agentapi.Report, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	r.take()
	b := newBackoff(r.a.o.MinBackoff, r.a.o.MaxBackoff)
	for {
		err := r.send(ctx, rep)
		if err == nil || ctx.Err() != nil {
			return err
		}
		b.wait(ctx)
	}
}
