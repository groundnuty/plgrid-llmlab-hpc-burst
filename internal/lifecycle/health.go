package lifecycle

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// ready applies the health rules to a ready Instance (HBEP-7, Health and
// restarts) and fires the ending-soon event.
func (p *pass) ready(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, av agentView, st settings, pv *providerView) error {
	p.endingSoon(in, pv)
	if av.unreachable || av.report == nil {
		return nil // a path that is down is not a dead Instance (HBEP-6)
	}
	// Behind a router, the router's own signal first: its rung (routerhealth.go).
	if acted, err := p.routerRung(in, alloc, av, st); acted || err != nil {
		return err
	}
	if several(in) {
		return p.readyWorkers(in, alloc, av, st)
	}
	signal, msg := p.healthSignal(in, alloc, av, st)
	if signal == "" {
		now := metav1.NewTime(p.now)
		return p.setStatus(in, func(s *v1alpha1.InstanceStatus) {
			cond(&s.Conditions, v1alpha1.ConditionHealthy, metav1.ConditionTrue, v1alpha1.ReasonHealthy, "no malfunction signal", in.Generation, now)
		})
	}
	return p.restart(in, alloc, signal, "", st, msg)
}

// healthSignal returns the malfunction signal of a ready Instance, if any:
// the probe failing failureThreshold times in a row; only server errors over
// errorOnlyWindow; requests running with the generated-token counter unchanged
// for noProgressTimeout; a part that exited without being told to. Plain load
// is never a signal (HBEP-7, Health and restarts).
func (p *pass) healthSignal(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, av agentView, st settings) (string, string) {
	ir := av.report
	if workerExited(ir, in) {
		w := worker0(ir)
		code := "unknown"
		if w.ExitCode != nil {
			code = strconv.Itoa(*w.ExitCode)
		}
		return v1alpha1.ReasonPartExited, fmt.Sprintf("worker-0 exited without being told to, with code %s", code)
	}
	if !workerRunning(ir, in) {
		return "", ""
	}
	w := worker0(ir)
	h := w.Health
	if h == nil {
		return "", ""
	}
	if !h.ProbeOK && h.ConsecutiveProbeFailures >= st.failureThreshold {
		return v1alpha1.ReasonProbeFailing, fmt.Sprintf("the health probe failed %d times in a row", h.ConsecutiveProbeFailures)
	}
	// Behind a router, the router holding the verified worker failing its active check.
	if sig, msg := routerCheckSignal(in, av, st); sig != "" {
		return sig, msg
	}
	return p.gaugeSignal(in, alloc, av, st, w)
}

// gaugeSignal is a running worker's gauge-based signal: no token progress, or only server errors. Both
// need fresh metrics: stale gauges are unknown.
func (p *pass) gaugeSignal(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, av agentView, st settings, w *agentapi.WorkerReport) (string, string) {
	h := w.Health
	if h == nil || !av.metricsFresh(w) {
		return "", ""
	}
	t, ok := p.r.Backend.healthOf(alloc.UID, string(in.UID), *w)
	if !ok {
		return "", ""
	}
	// No progress needs a known running count above zero and a known counter
	// in both this sample and the tracker's baseline: nil is unknown.
	if h.RunningRequests != nil && *h.RunningRequests > 0 && h.GeneratedTokens != nil && t.tokens != nil {
		since := t.tokensSince
		if p.mem.started.After(since) {
			since = p.mem.started
		}
		if deadline := since.Add(st.noProgressTimeout); !p.now.Before(deadline) {
			return v1alpha1.ReasonNoProgress, fmt.Sprintf("%d requests run and the generated-token counter has not moved for %s", *h.RunningRequests, p.now.Sub(since))
		} else {
			p.at(deadline)
		}
	}
	if errorsOnly(t.samples, p.now, st.errorOnlyWindow) {
		return v1alpha1.ReasonErrorsOnly, fmt.Sprintf("every answer over the last %s was a server error", st.errorOnlyWindow)
	}
	return "", ""
}

// errorsOnly reports whether, over the window that ends now, the engine
// answered requests and every answer was a server error. It needs a sample at
// least window old to compare with.
func errorsOnly(samples []healthSample, now time.Time, window time.Duration) bool {
	if len(samples) < 2 {
		return false
	}
	cur := samples[len(samples)-1]
	var base *healthSample
	for i := len(samples) - 2; i >= 0; i-- {
		if !samples[i].at.After(now.Add(-window)) {
			base = &samples[i]
			break
		}
	}
	if base == nil {
		return false
	}
	dr, de := cur.responses-base.responses, cur.errors-base.errors
	return dr > 0 && de == dr
}

// recentRestarts returns the Instance's restarts within restart.window: its
// status.restarts, which the controller appends before it raises a generation,
// so the ladder survives a controller restart (HBEP-7, Health and restarts).
func (p *pass) recentRestarts(in *v1alpha1.Instance, window time.Duration) []v1alpha1.InstanceRestart {
	var out []v1alpha1.InstanceRestart
	for _, r := range in.Status.Restarts {
		if p.now.Sub(r.Time.Time) <= window {
			out = append(out, r)
		}
	}
	return out
}

// finishRestarts raises a part's restart generation to the latest recorded
// restart of it, when a controller recorded the restart and stopped before
// it raised the generation: the decision was made, so it is carried out, not
// taken again.
func (p *pass) finishRestarts(in *v1alpha1.Instance) error {
	want := map[string]int64{}
	for _, r := range in.Status.Restarts {
		if r.Generation > want[r.Part] {
			want[r.Part] = r.Generation
		}
	}
	return p.setMeta(in, func(n *v1alpha1.Instance) {
		for part, gen := range want {
			if restartGeneration(n, part) < gen {
				if n.Annotations == nil {
					n.Annotations = map[string]string{}
				}
				n.Annotations[v1alpha1.RestartGenerationAnnotation(part)] = strconv.FormatInt(gen, 10)
			}
		}
	})
}

// restart climbs the alpha ladder (HBEP-7, Health and restarts): the worker,
// then the whole Instance, each tried restart.attemptsPerRung times within
// restart.window and restart.backoff apart, then a new Allocation: the
// Instance fails with RestartLoop and its Allocation is released at once.
// force names a rung to take directly (a failed rehash takes the
// whole-Instance rung). The event is emitted first, then the restart is
// recorded in status.restarts, then the generation is raised by
// compare-and-swap, so a deliberate restart is never read as a crash.
func (p *pass) restart(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, signal, force string, st settings, detail ...string) error {
	return p.climb(in, alloc, signal, force, v1alpha1.WorkerName(0), st, detail...)
}

// restartWorker is restart with worker w's signal, for an Instance of several workers: the ladder starts
// at w's own rung, which counts w's own attempts (HBEP-7, Several workers).
func (p *pass) restartWorker(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, signal, w string, st settings, detail ...string) error {
	return p.climb(in, alloc, signal, "", w, st, detail...)
}

// climb is restart, whose worker rung is worker's. Its attempts are worker's own restarts, and the
// whole-Instance rung's are the Instance's; with one worker, worker-0's are every worker restart.
func (p *pass) climb(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, signal, force, worker string, st settings, detail ...string) error {
	msg := signal
	if len(detail) > 0 && detail[0] != "" {
		msg = detail[0]
	}
	recent := p.recentRestarts(in, st.restartWindow)
	var workerTries, instTries int32
	var last time.Time
	for _, r := range recent {
		// The router rung counts its own tries (routerhealth.go); its restarts still space the others.
		if r.Part == v1alpha1.PartRouter {
			if r.Time.After(last) {
				last = r.Time.Time
			}
			continue
		}
		switch r.Part {
		case v1alpha1.PartInstance:
			instTries++
		case worker:
			workerTries++
		}
		if r.Time.After(last) {
			last = r.Time.Time
		}
	}
	if !last.IsZero() && force == "" {
		if next := last.Add(st.restartBackoff); p.now.Before(next) {
			p.at(next)
			return nil
		}
	}
	// The ladder: the worker, then the whole Instance, each attemptsPerRung
	// times. A forced rung (a failed rehash or staging takes the whole
	// Instance) starts the ladder there; a rung that is spent climbs on.
	part := ""
	switch {
	case force != v1alpha1.PartInstance && workerTries < st.attemptsPerRung:
		part = worker
	case instTries < st.attemptsPerRung:
		part = v1alpha1.PartInstance
	}
	if part == "" {
		return p.fail(in, alloc, v1alpha1.ReasonRestartLoop,
			fmt.Sprintf("%s: the worker and whole-Instance rungs are spent within %s; a new Allocation replaces it", msg, st.restartWindow))
	}
	reason := v1alpha1.ReasonWorkerRestart
	if part == v1alpha1.PartInstance {
		reason = v1alpha1.ReasonInstanceRestart
	}
	gen := restartGeneration(in, part) + 1
	p.r.Recorder.Event(in, nil, eventFailing, reason,
		fmt.Sprintf("signal %s (%s): the lifecycle controller restarts %s, generation %d; in-flight requests are cut", signal, msg, part, gen))
	now := metav1.NewTime(p.now)
	if err := p.setStatus(in, func(s *v1alpha1.InstanceStatus) {
		s.Restarts = append(p.recentRestarts(in, st.restartWindow), v1alpha1.InstanceRestart{Part: part, Generation: gen, Time: now, Signal: signal})
		if n := len(s.Restarts); n > maxRestartsKept {
			s.Restarts = s.Restarts[n-maxRestartsKept:]
		}
		// Several workers: a worker's restart keeps the phase, so a ready Instance stays ready and
		// routable on its other workers (HBEP-7, Several workers); the whole Instance's verifies again.
		if !several(in) || part == v1alpha1.PartInstance {
			s.Phase = v1alpha1.InstanceVerifying
		}
		switch {
		case several(in) && part != v1alpha1.PartInstance:
			// Several workers: a worker's restart verifies and warms up that worker alone, and leaves the
			// Instance's own conditions True (HBEP-7, Several workers). Its record moves with its
			// generation, so it is no longer verified; its warm-up is to run again.
			setWorker(s, part, func(e *v1alpha1.InstanceWorker) { e.WarmedUp = false })
		default:
			// The checks run again: Unknown, not False (api/v1alpha1/conditions.go, ReasonWorkerRestart and
			// ReasonInstanceRestart). A worker restart keeps Qualified.
			cond(&s.Conditions, v1alpha1.ConditionVerified, metav1.ConditionUnknown, reason, "reverifying after the restart", in.Generation, now)
			cond(&s.Conditions, v1alpha1.ConditionWarmedUp, metav1.ConditionUnknown, reason, "warming up again after the restart", in.Generation, now)
			if part == v1alpha1.PartInstance {
				cond(&s.Conditions, v1alpha1.ConditionQualified, metav1.ConditionUnknown, reason, "qualifying again after the restart", in.Generation, now)
			}
			// Several workers, the whole Instance: every worker verifies and warms up again, and none is
			// taken as verified meanwhile.
			for i := range s.Workers {
				s.Workers[i].WarmedUp, s.Workers[i].VerifiedGeneration = false, nil
			}
		}
		cond(&s.Conditions, v1alpha1.ConditionHealthy, metav1.ConditionFalse, signal, msg, in.Generation, now)
	}); err != nil {
		return err
	}
	if err := p.setMeta(in, func(n *v1alpha1.Instance) {
		if n.Annotations == nil {
			n.Annotations = map[string]string{}
		}
		n.Annotations[v1alpha1.RestartGenerationAnnotation(part)] = strconv.FormatInt(gen, 10)
	}); err != nil {
		return err
	}
	p.at(p.now.Add(st.restartBackoff))
	return nil
}

// maxRestartsKept is status.restarts' bound (the CRD's maxItems).
const maxRestartsKept = 32

// failedEnd reports whether an ended Instance failed: any end but a drain. A
// failed Instance counts towards its class's failure streak (HBEP-7, Restart
// and provisioning loops).
func failedEnd(in *v1alpha1.Instance) bool {
	return ended(in) && in.Status.EndReason != v1alpha1.ReasonDrained && in.Status.EndTime != nil
}

// classStreak returns the end times of the class's failed Instances since an
// Instance of the class last became ready, oldest first. It is read from the
// ended Instances' EndTime and ReadyTime, which the controller keeps for
// restart.window after a failure, so a restarted controller sees the same
// streak.
func (p *pass) classStreak(c v1alpha1.AcceleratorClass) []time.Time {
	var lastReady time.Time
	var fails []time.Time
	for _, in := range p.insts {
		pv := p.pvOf(in.Labels)
		if p.stale[in.UID] || pv == nil || !pv.hasClass || pv.class != c {
			continue
		}
		if in.Status.ReadyTime != nil && in.Status.ReadyTime.After(lastReady) {
			lastReady = in.Status.ReadyTime.Time
		}
		if failedEnd(in) {
			fails = append(fails, in.Status.EndTime.Time)
		}
	}
	var out []time.Time
	for _, t := range fails {
		if t.After(lastReady) {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Before(out[j]) })
	return out
}

// loopPaused reports whether provisioning in a class pauses: its last three
// failures fell within restart.window, and the pause, restart.backoff doubled
// for every failure beyond the third and capped at restart.window, has not
// run out. It returns when the pause ends, and fires the RestartLoop event
// once per streak (HBEP-7, Restart and provisioning loops).
func (p *pass) loopPaused(c v1alpha1.AcceleratorClass, st settings) (bool, time.Time) {
	streak := p.classStreak(c)
	if len(streak) < 3 {
		return false, time.Time{}
	}
	last := streak[len(streak)-1]
	if last.Sub(streak[len(streak)-3]) > st.restartWindow {
		return false, time.Time{}
	}
	pause := st.restartBackoff << min(len(streak)-3, 16)
	if pause > st.restartWindow || pause <= 0 {
		pause = st.restartWindow
	}
	until := last.Add(pause)
	if p.md != nil && p.mem.once(fmt.Sprintf("restart-loop/%s/%s/%s/%d", p.md.UID, c.GPUModel, c.CPUArch, streak[2].Unix())) {
		p.r.Recorder.Event(p.md, nil, eventDegraded, v1alpha1.ReasonRestartLoop,
			fmt.Sprintf("three Allocations in a row failed in class %s/%s within %s; provisioning there pauses", c.GPUModel, c.CPUArch, st.restartWindow))
	}
	return p.now.Before(until), until
}

// endingSoon fires the ending-soon event once, when a ready Instance's drain
// start is less than an hour away and no replacement is ready (HBEP-7, Events
// and conditions).
func (p *pass) endingSoon(in *v1alpha1.Instance, pv *providerView) {
	ds, ok := p.drainStart(in, pv)
	if !ok {
		return
	}
	if p.now.Before(ds.Add(-endingSoonLead)) {
		p.at(ds.Add(-endingSoonLead))
		return
	}
	if repl := p.replacementOf(in); repl != nil && repl.Status.Phase == v1alpha1.InstanceReady {
		return
	}
	if p.mem.once("ending-soon/" + string(in.UID)) {
		p.r.Recorder.Event(in, nil, eventEndingSoon, v1alpha1.EventReasonNoReplacementReady,
			fmt.Sprintf("drain start at %s is less than %s away and no replacement is ready", ds.UTC().Format(time.RFC3339), endingSoonLead))
	}
}
