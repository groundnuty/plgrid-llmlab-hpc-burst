package fake

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

const (
	// firstPID is the PID of the first worker process an Agent starts; each
	// start takes the next number.
	firstPID = 4000
	// socketDir holds the engines' sockets on the fake compute node.
	socketDir = "/tmp/fake-job"
)

// Agent is a scripted allocation agent. It does what the real agent does at
// the protocol's level and nothing else: no process starts, no file is read, no
// goroutine runs and nothing sleeps. A test calls Step, once per convergence
// step, and the Agent takes its time from the function given to NewAgent.
//
// An Agent is not safe for concurrent use: one goroutine drives it, and the
// OnDesired hook runs on that goroutine.
type Agent struct {
	// Registration is what Register sends. NewAgent sets one worker node of
	// aarch64 with one GH200; a test may replace it before the first Step.
	Registration agentapi.RegisterRequest

	// OnDesired, when set, is called with each desired state whose Version
	// differs from the last one seen, before the Agent applies it and before
	// any report shows it applied.
	OnDesired func(agentapi.DesiredState)

	backend agentapi.Backend
	caller  agentapi.Caller
	now     func() time.Time
	engine  Engine

	registered bool // the Backend knows the current agent instance ID
	silenced   bool
	stopping   bool // a stop was applied, and the final report is not yet sent
	exited     bool

	haveVersion bool
	version     string

	// seen and lastReport are what a test observed. They outlive Requeue.
	seen       []agentapi.DesiredState
	lastReport *agentapi.Report

	nextPID int // survives Requeue, so a second execution never reuses a PID
	inst    *instance

	// stopGrace and imageFormat come from the latest desired state applied.
	stopGrace   time.Duration
	imageFormat agentapi.ImageFormat
	starts      []Start // survives Requeue
}

// instance is the state of the Instance the Agent runs or last ran.
type instance struct {
	uid        string
	generation int64 // the whole-Instance restart generation last applied
	key        string
	keyStored  bool
	workers    []*worker
	checks     *agentapi.CheckResults

	// stageStart is when the current staging began: the Step that first took
	// the Instance up, or the Step that re-staged it after a failure. stageGen
	// is the Instance generation that staging is for (staging.go).
	stageStart time.Time
	stageGen   int64
}

// worker is one fake engine process.
type worker struct {
	name       string
	phase      agentapi.PartPhase
	pid        int
	start      time.Time
	exitCode   *int
	generation int64 // the worker's restart generation this process was started for
	instGen    int64 // the Instance's restart generation this process was started for
	socket     string
	killAt     time.Time // when a stopping worker is killed (stop.go)
}

// NewAgent returns an Agent that talks to b as c and takes the time from now
// (time.Now when nil). When c.AgentInstanceID is empty, the Agent draws a random one of
// 32 hex characters.
func NewAgent(b agentapi.Backend, c agentapi.Caller, now func() time.Time, e Engine) *Agent {
	if now == nil {
		now = time.Now
	}
	if c.AgentInstanceID == "" {
		c.AgentInstanceID = randHex(16)
	}
	return &Agent{
		Registration: agentapi.RegisterRequest{
			Node:    "node-example",
			CPUArch: "aarch64",
			GPUs:    []agentapi.GPU{{Index: 0, Name: "NVIDIA GH200 480GB", MemoryMiB: 97871}},
		},
		backend: b,
		caller:  c,
		now:     now,
		engine:  e,
		nextPID: firstPID,

		imageFormat: agentapi.ImageFormatSIF,
	}
}

// AgentInstanceID returns the agent instance ID the Agent presents.
func (a *Agent) AgentInstanceID() string { return a.caller.AgentInstanceID }

// Exited reports whether the Agent has stopped on instruction. Every Step after
// that returns nil and makes no call.
func (a *Agent) Exited() bool { return a.exited }

// Silence(true) makes every Step a no-op that makes no call, which is a
// path outage: the agent runs on, and the controller hears nothing. Silence(false)
// ends it. The workers' state does not change meanwhile, except that the clock
// keeps running: a kill that came due during the outage shows in the first
// report after it.
func (a *Agent) Silence(on bool) { a.silenced = on }

// Seen returns the desired states the Agent has seen, in order, each the first
// of its Version after a different one. It spans Requeue, so a state the new
// execution sees again appears again.
func (a *Agent) Seen() []agentapi.DesiredState {
	out := make([]agentapi.DesiredState, len(a.seen))
	for i, d := range a.seen {
		out[i] = cloneDesired(d)
	}
	return out
}

// LastReport returns the last report the Backend accepted, and false when it
// has accepted none. It spans Requeue.
func (a *Agent) LastReport() (agentapi.Report, bool) {
	if a.lastReport == nil {
		return agentapi.Report{}, false
	}
	return cloneReport(*a.lastReport), true
}

// Requeue makes the Agent a second execution of its job: a new agent instance
// ID, a restart count one higher in Registration (as SLURM_RESTART_COUNT is
// after a requeue), and no local state, so the next Step registers afresh and
// starts whatever it is told to run from nothing, with a new key. Seen,
// LastReport, Starts, the PID counter and a Silence in force carry over.
func (a *Agent) Requeue() {
	a.caller.AgentInstanceID = randHex(16)
	a.Registration.RestartCount++
	a.registered = false
	a.stopping = false
	a.exited = false
	a.haveVersion = false
	a.version = ""
	a.inst = nil
}

// CrashWorker makes a starting or running worker exit without being told to,
// with the exit code (0 too: a clean exit is a failure). The worker stays
// exited until its restart generation, or the Instance's, rises. It panics when
// no such live worker exists, because a test that crashes nothing would pass
// for the wrong reason.
func (a *Agent) CrashWorker(name string, exitCode int) {
	var w *worker
	if a.inst != nil {
		w = a.inst.find(name)
	}
	if w == nil || !w.alive() {
		panic(fmt.Sprintf("fake: CrashWorker(%q): no starting or running worker of that name", name))
	}
	w.phase = agentapi.PartExited
	w.exitCode = &exitCode
}

// Step is one convergence step. It registers if it must, polls the desired
// state once (no long poll), applies it and sends a full report. A
// CodeNotRegistered answer to the poll or the report makes it register again
// and retry once. Any other error is returned, wrapped, so errors.As and
// errors.Is see through it. A step that fails before the Agent has applied the
// desired state changes nothing in the Agent; one that fails at the report
// has applied it, and the next Step reports it again without restarting
// anything.
//
// A desired state with an Action the Agent does not know (agentapi's
// documentation, Versioning) is not an error: the Agent records it (Seen,
// OnDesired), changes nothing it runs or has been told (no part restarts or
// stops, a pending stop stays pending, the settings in it are not taken, no
// check advances), and reports as always. Only the passing of time shows: a
// starting worker whose delay has passed runs, and a stopping worker whose kill
// is due is killed. A desired state that runs an Instance but carries none is
// malformed, and is an error.
//
// After ActionStop, the Agent exits with the report it sends once every worker
// is gone: the Step that applied it when the workers stopped at once, a later
// Step when one ignores TERM until its kill (Engine.IgnoreTerm).
func (a *Agent) Step(ctx context.Context) error {
	if a.exited || a.silenced {
		return nil
	}
	now := a.now()
	if !a.registered {
		if err := a.register(ctx); err != nil {
			return err
		}
	}
	d, err := a.backend.Desired(ctx, a.caller)
	if isNotRegistered(err) {
		a.registered = false
		if err := a.register(ctx); err != nil {
			return err
		}
		d, err = a.backend.Desired(ctx, a.caller)
	}
	if err != nil {
		return fmt.Errorf("fake agent: desired: %w", err)
	}
	if err := checkDesired(d); err != nil {
		return err
	}
	a.observe(d)
	if knownAction(d.Action) {
		a.apply(d, now)
	} else {
		// An Action this agent does not know (doc.go, Versioning): it changes
		// nothing, and the Agent goes on polling and reporting. Only the time
		// that has passed shows in what it runs.
		a.elapse(now)
	}
	return a.report(ctx, now)
}

func (a *Agent) register(ctx context.Context) error {
	resp, err := a.backend.Register(ctx, a.caller, cloneRegister(a.Registration))
	if err != nil {
		return fmt.Errorf("fake agent: register: %w", err)
	}
	if resp.ProtocolVersion != agentapi.ProtocolVersion {
		return fmt.Errorf("fake agent: register: the endpoint speaks protocol %q, want %q", resp.ProtocolVersion, agentapi.ProtocolVersion)
	}
	a.registered = true
	return nil
}

func (a *Agent) report(ctx context.Context, now time.Time) error {
	r := a.snapshot(now)
	err := a.backend.Report(ctx, a.caller, cloneReport(r))
	if isNotRegistered(err) {
		a.registered = false
		if err := a.register(ctx); err != nil {
			return err
		}
		err = a.backend.Report(ctx, a.caller, cloneReport(r))
	}
	if err != nil {
		return fmt.Errorf("fake agent: report: %w", err)
	}
	a.lastReport = &r
	if a.stopping && (a.inst == nil || !a.inst.anyLive()) {
		a.exited = true
	}
	return nil
}

// observe records a desired state whose Version differs from the last one seen
// and calls the hook with it.
func (a *Agent) observe(d agentapi.DesiredState) {
	if a.haveVersion && d.Version == a.version {
		return
	}
	a.haveVersion, a.version = true, d.Version
	a.seen = append(a.seen, cloneDesired(d))
	if a.OnDesired != nil {
		a.OnDesired(cloneDesired(d))
	}
}

// knownAction reports whether the Agent knows the Action (the protocol version
// it speaks has it).
func knownAction(a agentapi.Action) bool {
	switch a {
	case agentapi.ActionIdle, agentapi.ActionRun, agentapi.ActionStop:
		return true
	}
	return false
}

// checkDesired rejects a desired state the Agent cannot act on: one that is
// malformed, not one with an Action it does not know, which it ignores.
func checkDesired(d agentapi.DesiredState) error {
	if d.Action == agentapi.ActionRun {
		if d.Instance == nil {
			return errors.New("fake agent: desired state runs an Instance but carries none")
		}
	}
	return nil
}

func isNotRegistered(err error) bool {
	var e *agentapi.Error
	return errors.As(err, &e) && e.Code == agentapi.CodeNotRegistered
}

func randHex(nBytes int) string {
	b := make([]byte, nBytes)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("fake: random bytes: %v", err))
	}
	return hex.EncodeToString(b)
}
