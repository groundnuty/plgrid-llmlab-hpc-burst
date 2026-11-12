package fake

import (
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The Instance's router (beta, opt-in; #429), as the real agent runs and reports it
// (internal/agent, router.go and routerwatch.go). The fake router starts at once and runs at the
// desired router generation; it restarts only when that generation rises, never by itself. Its view
// of the workers is Envoy's behind the agent's readiness endpoint: a worker is healthy when it runs at
// the generation the controller verified (DesiredWorker.VerifiedGeneration), and failing otherwise,
// counted in a row; the probe through it passes when it holds a worker healthy. RouterView reshapes
// that view, and CrashRouter ends the router.
type router struct {
	kind       agentapi.RouterKind
	generation int64
	phase      agentapi.PartPhase
	exitCode   *int
	verified   map[string]*int64 // each desired worker's VerifiedGeneration, as last applied
	failed     map[string]int32  // each worker's failed checks in a row
	probeFails int32
}

// applyRouter starts, or restarts at a raised generation, the router the desired Instance names.
func (a *Agent) applyRouter(i agentapi.DesiredInstance) {
	in := a.inst
	if in == nil {
		return
	}
	if i.Router == nil {
		in.router = nil
		return
	}
	r := in.router
	if r == nil || i.Router.Generation > r.generation {
		r = &router{kind: i.Router.Kind, generation: i.Router.Generation, phase: agentapi.PartRunning, failed: map[string]int32{}}
		in.router = r
	}
	r.verified = map[string]*int64{}
	for _, dw := range i.Workers {
		r.verified[dw.Name] = dw.VerifiedGeneration
	}
}

// CrashRouter makes the router exit without being told to, with the code. It stays exited until the
// desired router generation rises.
func (a *Agent) CrashRouter(exitCode int) {
	if a.inst == nil || a.inst.router == nil || a.inst.router.phase != agentapi.PartRunning {
		panic("fake: CrashRouter: no running router")
	}
	r := a.inst.router
	r.phase, r.exitCode = agentapi.PartExited, &exitCode
}

// routerReport is the router part of a report, nil when the Instance has no router.
func (a *Agent) routerReport(now time.Time) *agentapi.RouterReport {
	in := a.inst
	if in == nil || in.router == nil {
		return nil
	}
	r := in.router
	rr := &agentapi.RouterReport{Kind: r.kind, Phase: r.phase, Generation: r.generation, Socket: "/fake/run/router.sock", ExitCode: r.exitCode}
	if r.phase == agentapi.PartRunning {
		read := now
		rr.ClustersReadAt = &read
		healthy := false
		for _, w := range in.workers {
			v := r.verified[w.name]
			ok := w.phase == agentapi.PartRunning && v != nil && *v == w.generation
			rw := agentapi.RouterWorker{Name: w.name, Healthy: ok, FailedActiveCheck: !ok}
			if ok {
				r.failed[w.name] = 0
				healthy = true
			} else {
				r.failed[w.name]++
				rw.ConsecutiveFailedChecks = r.failed[w.name]
			}
			rr.Workers = append(rr.Workers, rw)
		}
		if healthy {
			r.probeFails = 0
		} else {
			r.probeFails++
		}
		rr.Probe = &agentapi.RouterProbe{Time: now, OK: healthy, ConsecutiveFailures: r.probeFails}
		if a.RouterView != nil {
			a.RouterView(rr)
		}
	}
	return rr
}
