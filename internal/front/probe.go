package front

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
)

// reasonPathProbeFailing is HBEP-6's reason for the degraded event a ready
// Instance gets when the front's probe takes it out of routing while its
// TunnelConnected condition is True (proposed in HBEP-6, distinct from
// HBEP-7's ProbeFailing); it awaits the contracts' list of reasons.
const reasonPathProbeFailing = "PathProbeFailing"

// ProbeResult is one probe of one Instance's path.
type ProbeResult struct {
	UID      types.UID
	Instance string // namespace/name
	Phase    v1alpha1.InstancePhase
	Passed   bool
	Status   int    // the /health status; 0 when none arrived
	Error    string // why it failed; empty when it passed
	// Failures is the run of failed probes, this one included; 0 after a pass.
	Failures int
	// Registered is whether the transport counts as registered after it.
	Registered bool
	Duration   time.Duration
}

// PathEvents receives what the probe finds (HBEP-6, The front's half:
// Reporting). Run's default, recorderPathEvents, raises the degraded event
// through the front's Recorder.
type PathEvents interface {
	// Probed is called after every probe.
	Probed(r ProbeResult)
	// PathProbeFailing is called once when a ready Instance whose
	// TunnelConnected condition is True stops being registered after
	// probeFailures failed probes: HBEP-6's degraded event, reason
	// PathProbeFailing. The controller restarts nothing on it.
	PathProbeFailing(inst *v1alpha1.Instance, r ProbeResult)
}

// NopPathEvents drops what the probe reports.
type NopPathEvents struct{}

// Probed implements PathEvents.
func (NopPathEvents) Probed(ProbeResult) {}

// PathProbeFailing implements PathEvents.
func (NopPathEvents) PathProbeFailing(*v1alpha1.Instance, ProbeResult) {}

// Prober probes every Instance in phase verifying, ready or draining end to
// end through its tunnel, every probeInterval (HBEP-6, The front's half; HBEP-5,
// Reachability): a new connection through the Tunnel, then GET /health with
// no key, which vLLM serves unguarded. A probe passes on a 200 within
// probeTimeout; anything else fails, a set-up failure included.
//
// What it decides: the transport counts as registered from the first passing
// probe; probeFailures failures in a row make it not registered, so the
// Instance is not eligible (HBEP-5's skip, which keeps its place in its group
// and its in-flight count); one passing probe makes it eligible again.
//
// A new registration of the UID's proxy (the front's plugin, Reregistered)
// takes the registration away at once (Tunnel.Bump) and puts the UID on the
// re-probe schedule: it is probed at once, then after reprobeFirst, doubling,
// until a probe passes or the wait would reach probeInterval, when the rounds
// carry on alone. Each re-probe is an ordinary probe. The first ones often
// come before frps has registered the proxy, or before frpc has started it,
// which is why they repeat. A pass counts only if the UID's epoch has not
// moved since the probe began (#371).
type Prober struct {
	tun      *Tunnel
	clk      clock.Clock
	interval time.Duration
	timeout  time.Duration
	failures int
	source   func() []v1alpha1.Instance
	events   PathEvents
	log      *slog.Logger

	mu    sync.Mutex
	state map[types.UID]*probeState
	// beat is the probe's heartbeat: when its latest round ended, which holds
	// every probed Instance's result (a round with none to probe counts), or
	// when the prober was made. missedAt is when HeartbeatMissed last went
	// out in this staleness; zero once a round ends (probe_report.go).
	beat     time.Time
	missedAt time.Time
	// awaiting is the re-probe schedule: the UIDs registered again whose
	// probe has not passed since. kick wakes Run for it.
	awaiting map[types.UID]*reprobe
	kick     chan struct{}
}

// reprobe is one UID's place on the re-probe schedule.
type reprobe struct {
	next time.Time     // when it is due
	wait time.Duration // the wait after the next attempt
}

// reprobeFirst is the wait after the first re-probe; each later wait doubles,
// and the schedule ends once a wait would reach probeInterval.
const reprobeFirst = 100 * time.Millisecond

type probeState struct {
	name    string // namespace/name
	fails   int
	running bool
	// The latest verdict: passed, at the front's clock time at; has is false
	// before the first.
	has    bool
	passed bool
	at     time.Time
}

// NewProber returns a prober over the Tunnel's registry. source returns the
// Instances the control plane holds now; events may be nil.
func NewProber(cfg Config, tun *Tunnel, clk clock.Clock, source func() []v1alpha1.Instance, events PathEvents, log *slog.Logger) *Prober {
	if events == nil {
		events = NopPathEvents{}
	}
	return &Prober{
		tun: tun, clk: clk, interval: cfg.ProbeInterval.D(), timeout: cfg.ProbeTimeout.D(), failures: cfg.ProbeFailures,
		source: source, events: events, log: log, state: map[types.UID]*probeState{}, beat: clk.Now(),
		awaiting: map[types.UID]*reprobe{}, kick: make(chan struct{}, 1),
	}
}

// Reregistered puts the UID on the re-probe schedule, due now, and wakes Run.
// The front's plugin calls it after Tunnel.Bump, for frps's NewProxy. It never
// blocks.
func (p *Prober) Reregistered(uid types.UID) {
	p.mu.Lock()
	p.awaiting[uid] = &reprobe{next: p.clk.Now(), wait: reprobeFirst}
	p.mu.Unlock()
	select {
	case p.kick <- struct{}{}:
	default:
	}
}

// reprobeAt is when the next re-probe is due, and false when none is.
func (p *Prober) reprobeAt() (time.Time, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var at time.Time
	for _, r := range p.awaiting {
		if at.IsZero() || r.next.Before(at) {
			at = r.next
		}
	}
	return at, !at.IsZero()
}

// Run probes every probeInterval until ctx ends. A round starts every
// interval, or as soon as the previous one ended when that took longer; a
// round waits for its probes, which probeTimeout bounds. Between rounds it
// runs the re-probes as they fall due (Reprobe).
func (p *Prober) Run(ctx context.Context) {
	for {
		start := p.clk.Now()
		p.Round(ctx)
		due := start.Add(p.interval)
		for p.clk.Now().Before(due) {
			wait := due.Sub(p.clk.Now())
			if at, ok := p.reprobeAt(); ok {
				wait = min(wait, max(at.Sub(p.clk.Now()), 0))
			}
			if !p.sleep(ctx, wait) {
				return
			}
			p.Reprobe(ctx)
		}
	}
}

// sleep waits for d, or for a kick, and reports false once ctx has ended.
func (p *Prober) sleep(ctx context.Context, d time.Duration) bool {
	next := make(chan struct{})
	t := p.clk.AfterFunc(d, func() { close(next) })
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-next:
	case <-p.kick:
	}
	return true
}

// Reprobe probes, concurrently, each probed Instance whose re-probe is due and
// whose previous probe has ended, moves each one's next attempt on, and waits
// for the probes. A UID whose wait would reach probeInterval leaves the
// schedule after this attempt; the rounds go on probing it. So does one the
// front does not probe (not yet in a probed phase, or gone), at once. One whose
// probe still runs is put off by reprobeFirst, so Run never spins on it.
func (p *Prober) Reprobe(ctx context.Context) {
	insts := p.source()
	live := make(map[types.UID]*v1alpha1.Instance, len(insts))
	for i := range insts {
		if in := &insts[i]; probed(in) {
			live[in.UID] = in
		}
	}
	now := p.clk.Now()
	var start []*v1alpha1.Instance
	var states []*probeState
	p.mu.Lock()
	for uid, r := range p.awaiting {
		in := live[uid]
		switch {
		case in == nil:
			delete(p.awaiting, uid)
			continue
		case now.Before(r.next):
			continue
		}
		st := p.state[uid]
		if st == nil {
			st = &probeState{name: in.Namespace + "/" + in.Name}
			p.state[uid] = st
		}
		if st.running {
			r.next = now.Add(reprobeFirst)
			continue
		}
		if r.wait >= p.interval {
			delete(p.awaiting, uid)
		} else {
			r.next, r.wait = now.Add(r.wait), 2*r.wait
		}
		st.running = true
		start, states = append(start, in), append(states, st)
	}
	p.mu.Unlock()
	var wg sync.WaitGroup
	for i := range start {
		wg.Add(1)
		go func(in *v1alpha1.Instance, st *probeState) {
			defer wg.Done()
			p.probeOne(ctx, in, st)
		}(start[i], states[i])
	}
	wg.Wait()
}

// probed reports whether the front probes an Instance in this state: phase
// verifying, ready or draining, not being deleted (HBEP-6, The front's half).
func probed(in *v1alpha1.Instance) bool {
	switch in.Status.Phase {
	case v1alpha1.InstanceVerifying, v1alpha1.InstanceReady, v1alpha1.InstanceDraining:
		return in.DeletionTimestamp == nil
	}
	return false
}

// Round probes each probed Instance once, concurrently, and waits for the
// probes. An Instance whose previous probe still runs is skipped. An Instance
// that is no longer probed loses its state and its registration.
func (p *Prober) Round(ctx context.Context) {
	insts := p.source()
	live := make(map[types.UID]*v1alpha1.Instance, len(insts))
	for i := range insts {
		if in := &insts[i]; probed(in) {
			live[in.UID] = in
		}
	}
	var gone []types.UID
	var start []*v1alpha1.Instance
	var states []*probeState
	p.mu.Lock()
	for uid := range p.state {
		if live[uid] == nil {
			delete(p.state, uid)
			gone = append(gone, uid)
		}
	}
	// A re-probe for an Instance the front does not probe (not yet placed in a
	// probed phase, or gone) waits no longer: the rounds take it up.
	for uid := range p.awaiting {
		if live[uid] == nil {
			delete(p.awaiting, uid)
		}
	}
	for uid, in := range live {
		st := p.state[uid]
		if st == nil {
			st = &probeState{name: in.Namespace + "/" + in.Name}
			p.state[uid] = st
		}
		if st.running {
			continue
		}
		st.running = true
		start, states = append(start, in), append(states, st)
	}
	p.mu.Unlock()
	for _, uid := range gone {
		p.tun.setRegistered(uid, false)
		p.tun.forget(uid)
	}
	var wg sync.WaitGroup
	for i := range start {
		wg.Add(1)
		go func(in *v1alpha1.Instance, st *probeState) {
			defer wg.Done()
			p.probeOne(ctx, in, st)
		}(start[i], states[i])
	}
	wg.Wait()
	p.mu.Lock()
	p.beat, p.missedAt = p.clk.Now(), time.Time{}
	p.mu.Unlock()
}

// probeOne probes one Instance and applies the verdict.
func (p *Prober) probeOne(ctx context.Context, in *v1alpha1.Instance, st *probeState) {
	began := time.Now()
	epoch := p.tun.Epoch(in.UID)
	status, err := p.check(ctx, in.UID)
	if ctx.Err() != nil {
		// The front is stopping: a probe it cut says nothing about the path.
		p.mu.Lock()
		st.running = false
		p.mu.Unlock()
		return
	}
	if err == nil && p.tun.Epoch(in.UID) != epoch {
		// The proxy registered again while the probe ran, so its pass proves
		// the replaced registration only: no verdict, and a re-probe soon.
		p.mu.Lock()
		st.running = false
		p.mu.Unlock()
		p.log.Info("the Instance's tunnel was registered again while its probe ran; the pass does not count", "instance", in.Namespace+"/"+in.Name, "uid", string(in.UID))
		p.Reregistered(in.UID)
		return
	}
	res := ProbeResult{UID: in.UID, Instance: in.Namespace + "/" + in.Name, Phase: in.Status.Phase, Status: status, Passed: err == nil, Duration: time.Since(began)}
	p.mu.Lock()
	st.running = false
	if p.state[in.UID] != st {
		p.mu.Unlock() // dropped from the probed set while it ran
		return
	}
	if err == nil {
		st.fails = 0
	} else {
		st.fails++
		res.Error = err.Error()
	}
	res.Failures = st.fails
	st.has, st.passed, st.at = true, err == nil, p.clk.Now()
	register := err == nil || (st.fails < p.failures && p.tun.Registered(in.UID))
	if err == nil {
		delete(p.awaiting, in.UID)
	}
	p.mu.Unlock()
	was, applied := p.tun.setRegisteredAt(in.UID, register, &epoch)
	if !applied {
		// The epoch moved between the check above and here: the same case,
		// with the pass already counted against the failure run, which a UID
		// without a registration does not use.
		register = false
		p.Reregistered(in.UID)
	}
	res.Registered = register
	switch {
	case !was && register:
		p.log.Info("the Instance's tunnel passed its probe; its transport counts as registered", "instance", res.Instance, "uid", string(in.UID))
	case was && !register:
		p.log.Warn("the Instance's tunnel failed its probe in a row; its transport no longer counts as registered, so it is not eligible",
			"instance", res.Instance, "uid", string(in.UID), "failures", res.Failures, "error", res.Error)
		if in.Status.Phase == v1alpha1.InstanceReady && meta.IsStatusConditionTrue(in.Status.Conditions, v1alpha1.ConditionTunnelConnected) {
			p.events.PathProbeFailing(in, res)
		}
	}
	p.events.Probed(res)
}

// check is one probe: a new connection through the tunnel, GET /health with
// no key, and a 200 within probeTimeout. It returns the status it read.
func (p *Prober) check(ctx context.Context, uid types.UID) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	c, err := p.tun.Dial(ctx, uid)
	if err != nil {
		return 0, err
	}
	defer c.Close()
	deadline, _ := ctx.Deadline()
	_ = c.SetDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { _ = c.SetDeadline(time.Unix(1, 0)) })
	defer stop()
	if _, err := io.WriteString(c, "GET /health HTTP/1.1\r\nHost: instance\r\nUser-Agent: hpcburst-front-probe\r\nConnection: close\r\n\r\n"); err != nil {
		return 0, fmt.Errorf("probe: write GET /health: %w", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(c), &http.Request{Method: http.MethodGet})
	if err != nil {
		return 0, fmt.Errorf("probe: read the /health reply: %w", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, fmt.Errorf("probe: GET /health answered %s", resp.Status)
	}
	return resp.StatusCode, nil
}
