package lifecycle

import (
	"fmt"
	"strconv"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// A ready Instance of several workers (HBEP-7, Several workers, Health and restarts with several
// workers): each worker is judged on its own signals and restarted at its own rung while the Instance
// stays ready and routable on the others; a restarted worker verifies and warms up again on its own
// checks, which the desired state asks while the Instance is ready, and takes traffic once its
// verifiedGeneration is its generation. The router's failed checks count against a worker only from the
// first report in which the router holds it healthy after its verification, its router admission, kept
// in its status.workers entry as routerAdmittedGeneration so a restarted controller keeps it (the science
// agent's rulings on #439, Q2 and where it is kept).

// signalWarmUp is the signal of a rejoining worker whose warm-up failed while its Instance is ready.
const signalWarmUp = "WarmUpFailed"

// readyWorkers is ready for an Instance of several workers, after the router rung: a rejoining worker's
// verification and warm-up, the router admissions, then each worker's signal.
func (p *pass) readyWorkers(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, av agentView, st settings) error {
	ir := av.report
	if ok, err := p.verifyWorkers(in, alloc, ir, st); !ok || err != nil {
		return err
	}
	if sh, ok := p.shapesOf(in); ok {
		// A rejoining worker whose warm-up fails is one failed attempt at its own rung: the Instance stays
		// ready on its siblings, and exhausted attempts climb to the whole-Instance rung (HBEP-7, Several
		// workers; the science agent's ruling on #439, PR 3b's question). A contradiction verdict, in
		// verifyWorkers, still ends the Instance, as with one worker.
		if ok, err := p.warmWorkers(in, alloc, ir, sh, func(w, msg string) error {
			return p.restartWorker(in, alloc, signalWarmUp, w, st, w+": "+msg)
		}); !ok || err != nil {
			return err
		}
	}
	if err := p.admitWorkers(in, av); err != nil {
		return err
	}
	w, signal, msg := p.workerSignal(in, alloc, av, st)
	if signal != "" {
		return p.restartWorker(in, alloc, signal, w, st, msg)
	}
	// Healthy once every worker is back: while one rejoins, the condition keeps its restart's signal.
	names := workerNames(in)
	if !everyWorker(names, func(w string) bool { return workerWarmed(in, w) }) {
		return nil
	}
	now := metav1.NewTime(p.now)
	return p.setStatus(in, func(s *v1alpha1.InstanceStatus) {
		cond(&s.Conditions, v1alpha1.ConditionHealthy, metav1.ConditionTrue, v1alpha1.ReasonHealthy, "no malfunction signal", in.Generation, now)
	})
}

// routerAdmitted reports whether the router has held worker w healthy since its current generation was
// verified: its router admission is that generation.
func routerAdmitted(in *v1alpha1.Instance, w string) bool {
	g := workerEntry(in, w).RouterAdmittedGeneration
	return g != nil && *g == restartGeneration(in, w)
}

// admitWorkers records the router admission of each worker warmed up at its current generation that the
// current router report holds healthy and that is not yet admitted at that generation. A healthy
// worker's count of failed checks is 0, so the failures the router counted while the worker was not
// yet verified, during its own warm-up, are never judged against it.
func (p *pass) admitWorkers(in *v1alpha1.Instance, av agentView) error {
	rr := currentRouter(in, av)
	if rr == nil || rr.Phase != agentapi.PartRunning {
		return nil
	}
	healthy := map[string]bool{}
	for _, rw := range rr.Workers {
		healthy[rw.Name] = rw.Healthy
	}
	admit := map[string]int64{}
	for _, w := range workerNames(in) {
		if workerWarmed(in, w) && healthy[w] && !routerAdmitted(in, w) {
			admit[w] = restartGeneration(in, w)
		}
	}
	if len(admit) == 0 {
		return nil
	}
	return p.setStatus(in, func(s *v1alpha1.InstanceStatus) {
		for w, g := range admit {
			g := g
			setWorker(s, w, func(e *v1alpha1.InstanceWorker) { e.RouterAdmittedGeneration = &g })
		}
	})
}

// workerSignal returns the first worker's malfunction signal of a ready Instance of several workers, in
// the workers' order. A worker whose raised generation the agent has not applied yet raises none, so its
// exit and the router's view of it count as one restart (HBEP-7: one restart per death). A worker that
// rejoins, until the router admits it, is judged only by health.startTimeout from its restart; an
// admitted one by its probe, the router's check and its gauges.
func (p *pass) workerSignal(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, av agentView, st settings) (worker, signal, msg string) {
	ir := av.report
	if ir == nil || av.unreachable || ir.Generation != restartGeneration(in, v1alpha1.PartInstance) {
		return "", "", ""
	}
	for _, w := range workerNames(in) {
		r := reportOf(ir, w)
		if r == nil || r.Generation != restartGeneration(in, w) {
			continue
		}
		if r.Phase == agentapi.PartExited {
			code := "unknown"
			if r.ExitCode != nil {
				code = strconv.Itoa(*r.ExitCode)
			}
			return w, v1alpha1.ReasonPartExited, fmt.Sprintf("%s exited without being told to, with code %s", w, code)
		}
		// Until the router admits it, health.startTimeout from its restart judges it, not the router's
		// checks (Q2): a restarted worker is unhealthy by design during its own warm-up.
		if !workerWarmed(in, w) || (routed(in) && !routerAdmitted(in, w)) {
			deadline := p.workerSince(in, r, w).Add(st.startTimeout)
			if !p.now.Before(deadline) {
				return w, v1alpha1.ReasonStartTimeout, fmt.Sprintf("%s was not verified, warmed up and held healthy by the router within %s of its restart", w, st.startTimeout)
			}
			p.at(deadline)
			continue
		}
		if r.Phase != agentapi.PartRunning || r.Health == nil {
			continue
		}
		if h := r.Health; !h.ProbeOK && h.ConsecutiveProbeFailures >= st.failureThreshold {
			return w, v1alpha1.ReasonProbeFailing, fmt.Sprintf("%s's health probe failed %d times in a row", w, h.ConsecutiveProbeFailures)
		}
		if sig, m := workerRouterCheck(in, av, st, w); sig != "" {
			return w, sig, m
		}
		if sig, m := p.gaugeSignal(in, alloc, av, st, r); sig != "" {
			return w, sig, w + ": " + m
		}
	}
	return "", "", ""
}

// workerRouterCheck is worker w's router signal: the router holds it failing its active check for
// health.failureThreshold reports in a row. workerSignal asks it only for a worker the router has admitted
// at its current generation, so the failures counted before then never judge it (Q2).
func workerRouterCheck(in *v1alpha1.Instance, av agentView, st settings, w string) (string, string) {
	rr := currentRouter(in, av)
	if rr == nil || rr.Phase != agentapi.PartRunning {
		return "", ""
	}
	for _, rw := range rr.Workers {
		if rw.Name == w && rw.FailedActiveCheck && rw.ConsecutiveFailedChecks >= st.failureThreshold {
			return v1alpha1.ReasonRouterCheckFailing, fmt.Sprintf("the router held %s failing its active check for %d reports in a row", w, rw.ConsecutiveFailedChecks)
		}
	}
	return "", ""
}

// workerSince is when worker w's current generation was asked for, from which health.startTimeout judges
// it: its own restart, or the whole Instance's, the newer, while status.restarts still holds it; else,
// once restart.window has dropped it, the start of the process the agent runs for that generation; else
// the start instruction.
func (p *pass) workerSince(in *v1alpha1.Instance, r *agentapi.WorkerReport, w string) time.Time {
	for i := len(in.Status.Restarts) - 1; i >= 0; i-- {
		if rs := in.Status.Restarts[i]; rs.Part == w || rs.Part == v1alpha1.PartInstance {
			return rs.Time.Time
		}
	}
	if r != nil && r.StartTime != nil {
		return *r.StartTime
	}
	if t := in.Status.StartInstructionTime; t != nil {
		return t.Time
	}
	return p.now
}
