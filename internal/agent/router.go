package agent

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/envoypin"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The router part (beta, opt-in; HBEP-5, Inside an Instance; HBEP-7, Several workers and Health and
// restarts; #429). When the desired Instance names a router, the agent runs Envoy in front of its
// worker and the forwarder serves the Instance's path to Envoy's socket instead of the worker's, as
// forward.go always planned. frpc keeps dialing the forwarder's socket (the science agent's ruling on
// #429, TL1). Like a worker, the router is restarted only when its restart generation rises, never by
// the agent itself, and a router that exits is reported exited.

// envoyReady bounds the wait for a started Envoy's sockets.
const envoyReady = 60 * time.Second

// routerKeepAliveVar is vLLM's keep-alive for idle client connections, which the workers behind a
// router get at routerKeepAlive: above Envoy's 50 s upstream idle, so Envoy, which reuses
// connections, closes them first (HBEP-5, Inside an Instance; L-2.5). vLLM v0.32.0 defaults it to 5 s.
const (
	routerKeepAliveVar = "VLLM_HTTP_TIMEOUT_KEEP_ALIVE"
	routerKeepAlive    = "120"
)

// StagedEnvoyPath is where the Envoy pinned for this machine's architecture is staged under a
// staging directory: <stagingDir>/envoy/<sha256>/envoy (agentapi.StagedEnvoyPath), by the digest of
// the pin in the agent's own build, where the provider stages it when the controller runs Instances
// behind a router (#454); "" when no Envoy is pinned for the architecture, which refuses any router
// start.
func StagedEnvoyPath(stagingDir string) string {
	p, err := envoypin.For(runtime.GOARCH)
	if err != nil || stagingDir == "" {
		return ""
	}
	return filepath.Join(stagingDir, filepath.FromSlash(agentapi.StagedEnvoyPath(p.SHA256)))
}

// routerPart is the Instance's router.
type routerPart struct {
	kind       agentapi.RouterKind
	phase      agentapi.PartPhase
	generation int64 // the router's restart generation it was started for
	instGen    int64 // the Instance's restart generation it was started for
	proc       *EnvoyProc
	exitCode   *int
	logTail    string
	gone       bool // its process group has ended
}

func (r *routerPart) alive() bool {
	return r.phase == agentapi.PartStarting || r.phase == agentapi.PartRunning
}

// live reports whether a process of the router may still run.
func (r *routerPart) live() bool { return r.alive() || r.phase == agentapi.PartStopping }

// listener is the router's listener in the job's run directory: the forwarder's target.
func (a *Agent) routerListener() string { return filepath.Join(a.o.RunDir, EnvoyListenerSocket) }

// convergeRouter starts, restarts and stops the router to match the desired Instance, and points the
// forwarder at it. It runs after applyRun on every pass. An Instance with no router is alpha's: the
// forwarder stays out of router mode, and nothing here touches it.
func (a *Agent) convergeRouter(in *instance, i agentapi.DesiredInstance) {
	r := in.router
	if i.Router == nil {
		if r != nil {
			a.stopRouter(r)
		}
		if a.fwd.Routed() && (r == nil || !r.live()) {
			a.fwd.LeaveRouter()
		}
		return
	}
	if r != nil && (i.Generation > r.instGen || i.Router.Generation > r.generation) {
		a.stopRouter(r)
		if !r.live() {
			in.router, r = nil, nil
		}
	}
	// The router starts with the Instance's parts, once the staging for the desired Instance
	// generation has passed. On a raised generation (rung 2) applyRun stops every part and restages
	// only once none is live; a router started before that restage would be a live part at the new
	// generation, never stopped, and the restage would never come (#442's review).
	if r == nil && in.staged && in.stageGen == i.Generation {
		in.router = a.startRouter(in, i)
		r = in.router
	}
	target := ""
	if r != nil && r.phase == agentapi.PartRunning {
		target = a.routerListener()
	}
	// Router mode from the first pass: no worker is ever the target, even before the router runs.
	a.fwd.RouteTo(target)
}

// startRouter starts the router asynchronously: Envoy's start waits for its sockets, which the loop
// must not. The result comes back to the loop as routerStarted.
func (a *Agent) startRouter(in *instance, i agentapi.DesiredInstance) *routerPart {
	r := &routerPart{kind: i.Router.Kind, phase: agentapi.PartStarting, generation: i.Router.Generation, instGen: i.Generation}
	fail := func(err error) *routerPart {
		code := startFailedExitCode
		r.phase, r.exitCode, r.logTail = agentapi.PartExited, &code, tailUTF8(err.Error(), agentapi.MaxLogTailBytes)
		a.log.Error("router did not start", "router", r.kind, "err", err)
		return r
	}
	if r.kind != agentapi.RouterEnvoy {
		return fail(fmt.Errorf("unknown router %q: this agent runs %q", r.kind, agentapi.RouterEnvoy))
	}
	if a.o.EnvoyBinary == "" {
		return fail(fmt.Errorf("no Envoy binary for %s is configured", runtime.GOARCH))
	}
	cfg := EnvoyConfig{RunDir: a.o.RunDir}
	for _, dw := range i.Workers {
		cfg.Workers = append(cfg.Workers, EnvoyWorker{
			Name: dw.Name, Socket: filepath.Join(a.o.RunDir, strings.TrimPrefix(dw.EngineSocket, agentapi.ContainerRunDir+"/")),
		})
	}
	bin, grace, now := a.o.EnvoyBinary, a.stopGrace, a.o.Now()
	a.log.Info("starting router", "router", r.kind, "generation", r.generation, "instanceGeneration", r.instGen)
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		p, err := StartEnvoy(EnvoyStart{Binary: bin, Config: cfg, Ready: envoyReady}, now)
		routerStartSeam("started", p)
		if p != nil && !a.holdRouter(p) {
			// The agent is ending: no loop will take this Envoy, so it is stopped here.
			stopRouterProc(p, grace)
			return
		}
		routerStartSeam("held", p)
		// A hand-over the ending loop never runs leaves p held, and shutdown stops it.
		a.post(func() { a.routerStarted(in, r, p, err) })
	}()
	return r
}

// routerStarted takes a router's start result on the loop.
func (a *Agent) routerStarted(in *instance, r *routerPart, p *EnvoyProc, err error) {
	if p != nil {
		a.releaseRouter(p) // the loop owns it now
	}
	if err != nil {
		code := startFailedExitCode
		r.phase, r.exitCode, r.logTail = agentapi.PartExited, &code, tailUTF8(err.Error(), agentapi.MaxLogTailBytes)
		a.log.Error("router did not start", "router", r.kind, "err", err)
		return
	}
	r.proc = p
	if r.phase == agentapi.PartStopping {
		// Told to stop while it started.
		p.Group.Stop(a.stopGrace)
	} else {
		r.phase = agentapi.PartRunning
		a.log.Info("router is serving", "router", r.kind, "pid", p.Group.PID(), "generation", r.generation)
	}
	g := p.Group
	a.wg.Add(2)
	go func() {
		defer a.wg.Done()
		select {
		case <-g.Exited():
		case <-a.ctx.Done():
			return // shutdown waits on the group itself
		}
		a.post(func() { a.routerExited(r) })
	}()
	go func() {
		defer a.wg.Done()
		select {
		case <-g.Done():
		case <-a.ctx.Done():
			return
		}
		a.post(func() { a.routerGone(r) })
	}()
}

// routerExited records the router's process leaving: unbidden, it is exited, whatever its code, and
// the controller decides what follows (the router rung); the agent does not start it again.
func (a *Agent) routerExited(r *routerPart) {
	if r.phase != agentapi.PartRunning {
		return
	}
	code := r.proc.Group.ExitCode()
	r.phase, r.exitCode = agentapi.PartExited, &code
	r.logTail = routerLogTail(a.o.RunDir, a.keys)
	a.log.Error("router exited without being told to", "router", r.kind, "pid", r.proc.Group.PID(), "exitCode", code)
	r.proc.Group.Kill() // whatever is left of its group
}

// routerGone records the end of the router's process group.
func (a *Agent) routerGone(r *routerPart) {
	if r.gone {
		return
	}
	r.gone = true
	r.proc.Close()
	if r.phase == agentapi.PartStopping {
		r.phase = agentapi.PartStopped
		if r.exitCode == nil {
			code := r.proc.Group.ExitCode()
			r.exitCode = &code
		}
		a.log.Info("router stopped", "router", r.kind, "pid", r.proc.Group.PID(), "exitCode", *r.exitCode)
	}
}

// stopRouter stops the router politely: TERM, then KILL after the stop grace.
func (a *Agent) stopRouter(r *routerPart) {
	if !r.alive() {
		return
	}
	r.phase = agentapi.PartStopping
	if r.proc == nil {
		return // still starting: routerStarted stops it
	}
	a.log.Info("stopping router", "router", r.kind, "pid", r.proc.Group.PID(), "grace", a.stopGrace)
	if r.gone {
		r.phase = agentapi.PartStopped
		return
	}
	r.proc.Group.Stop(a.stopGrace)
}

// report is the router part's report.
func (r *routerPart) report(listener string) *agentapi.RouterReport {
	rr := &agentapi.RouterReport{Kind: r.kind, Phase: r.phase, Generation: r.generation, Socket: listener, LogTail: r.logTail}
	if r.proc != nil {
		start := r.proc.Group.Start().UTC()
		rr.PID, rr.StartTime = r.proc.Group.PID(), &start
	}
	if r.exitCode != nil {
		c := *r.exitCode
		rr.ExitCode = &c
	}
	return rr
}

// routerLogTail is the end of Envoy's log in the run directory, with every Instance key the agent has
// drawn replaced, at most MaxLogTailBytes (as a worker's, readLogTail).
func routerLogTail(runDir string, keys []string) string {
	if _, err := os.Stat(filepath.Join(runDir, EnvoyLogFile)); err != nil {
		return ""
	}
	return readLogTail(filepath.Join(runDir, EnvoyLogFile), keys)
}

// idleRouter stops the router when the Instance's parts stop, and leaves the forwarder with no target,
// in router mode still, so no worker becomes the target meanwhile.
func (a *Agent) idleRouter(in *instance) {
	if in.router != nil {
		a.stopRouter(in.router)
	}
	if a.fwd.Routed() {
		a.fwd.RouteTo("")
	}
}

// routerHold is the hand-over of started routers from their start goroutines to the loop (#442's
// review): a start goroutine holds its Envoy here until the loop takes it (routerStarted). shutdown,
// once the loop has ended, marks the agent down and stops whatever is still held; a start that ends
// after that stops its own Envoy. So an Envoy started while the agent ends is never left running.
type routerHold struct {
	mu   sync.Mutex
	down bool
	held map[*EnvoyProc]bool
}

// holdRouter holds a started Envoy for the loop; false once the agent is ending.
func (a *Agent) holdRouter(p *EnvoyProc) bool {
	h := &a.routerHold
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.down {
		return false
	}
	if h.held == nil {
		h.held = map[*EnvoyProc]bool{}
	}
	h.held[p] = true
	return true
}

// releaseRouter hands a held Envoy to the loop.
func (a *Agent) releaseRouter(p *EnvoyProc) {
	h := &a.routerHold
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.held, p)
}

// takeHeldRouters marks the agent down and returns the Envoys still held, which no loop will take.
func (a *Agent) takeHeldRouters() []*EnvoyProc {
	h := &a.routerHold
	h.mu.Lock()
	defer h.mu.Unlock()
	h.down = true
	var out []*EnvoyProc
	for p := range h.held {
		out = append(out, p)
	}
	h.held = nil
	return out
}

// stopRouterProc stops an Envoy no part owns: TERM, KILL after grace, and waits for its group.
func stopRouterProc(p *EnvoyProc, grace time.Duration) {
	p.Group.Stop(grace)
	<-p.Group.Done()
	p.Close()
}

// routerStartSeam is a test seam (export_test.go): it is told each stage of a router start's
// hand-over, "started" and then "held", with the started Envoy. Production never sets it.
func routerStartSeam(stage string, p *EnvoyProc) {
	if f := routerStartHook.Load(); f != nil {
		(*f)(stage, p)
	}
}

var routerStartHook atomic.Pointer[func(stage string, p *EnvoyProc)]
