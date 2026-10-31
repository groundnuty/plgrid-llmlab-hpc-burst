package lifecycle

import (
	"fmt"
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
	signal, msg := p.healthSignal(in, alloc, av.report, st)
	if signal == "" {
		now := metav1.NewTime(p.now)
		return p.setStatus(in, func(s *v1alpha1.InstanceStatus) {
			cond(&s.Conditions, v1alpha1.ConditionHealthy, metav1.ConditionTrue, reasonHealthy, "no malfunction signal", in.Generation, now)
		})
	}
	return p.restart(in, alloc, signal, "", st, msg)
}

// healthSignal returns the malfunction signal of a ready Instance, if any:
// the probe failing failureThreshold times in a row; only server errors over
// errorOnlyWindow; requests running with the generated-token counter unchanged
// for noProgressTimeout; a part that exited without being told to. Plain load
// is never a signal (HBEP-7, Health and restarts).
func (p *pass) healthSignal(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, ir *agentapi.InstanceReport, st settings) (string, string) {
	if workerExited(ir, in) {
		w := worker0(ir)
		code := "unknown"
		if w.ExitCode != nil {
			code = strconv.Itoa(*w.ExitCode)
		}
		return reasonPartExited, fmt.Sprintf("worker-0 exited without being told to, with code %s", code)
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
		return reasonProbeFailing, fmt.Sprintf("the health probe failed %d times in a row", h.ConsecutiveProbeFailures)
	}
	t, ok := p.r.Backend.healthOf(alloc.UID, string(in.UID), *w)
	if !ok {
		return "", ""
	}
	if h.RunningRequests > 0 {
		since := t.tokensSince
		if p.mem.started.After(since) {
			since = p.mem.started
		}
		if deadline := since.Add(st.noProgressTimeout); !p.now.Before(deadline) {
			return reasonNoProgress, fmt.Sprintf("%d requests run and the generated-token counter has not moved for %s", h.RunningRequests, p.now.Sub(since))
		} else {
			p.at(deadline)
		}
	}
	if errorsOnly(t.samples, p.now, st.errorOnlyWindow) {
		return reasonErrorsOnly, fmt.Sprintf("every answer over the last %s was a server error", st.errorOnlyWindow)
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

// restart climbs the alpha ladder (HBEP-7, Health and restarts): the worker,
// then the whole Instance, each tried restart.attemptsPerRung times within
// restart.window and restart.backoff apart, then a new Allocation: the
// Instance fails and its Allocation is released at once. force names a rung to
// take directly (a failed rehash takes the whole-Instance rung). The event is
// emitted before the generation is raised by compare-and-swap, so a deliberate
// restart is never read as a crash.
func (p *pass) restart(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, signal, force string, st settings, detail ...string) error {
	msg := signal
	if len(detail) > 0 && detail[0] != "" {
		msg = detail[0]
	}
	hist := p.mem.history(in)
	var workerTries, instTries int32
	var last time.Time
	for _, h := range hist {
		if p.now.Sub(h.at) > st.restartWindow {
			continue
		}
		if h.part == v1alpha1.PartInstance {
			instTries++
		} else {
			workerTries++
		}
		if h.at.After(last) {
			last = h.at
		}
	}
	if !last.IsZero() && force == "" {
		if next := last.Add(st.restartBackoff); p.now.Before(next) {
			p.at(next)
			return nil
		}
	}
	part := force
	if part == "" {
		switch {
		case workerTries < st.attemptsPerRung:
			part = v1alpha1.WorkerName(0)
		case instTries < st.attemptsPerRung:
			part = v1alpha1.PartInstance
		}
	}
	if part == v1alpha1.PartInstance && force != "" && instTries >= st.attemptsPerRung {
		part = ""
	}
	if part == "" {
		p.classFailed(in)
		return p.fail(in, alloc, endRestartLadder,
			fmt.Sprintf("%s: the worker and whole-Instance rungs are spent within %s; a new Allocation replaces it", msg, st.restartWindow))
	}
	reason := reasonWorkerRestart
	if part == v1alpha1.PartInstance {
		reason = reasonInstanceRestart
	}
	gen := restartGeneration(in, part) + 1
	p.r.Recorder.Event(in, true, eventFailing, reason,
		fmt.Sprintf("signal %s (%s): the lifecycle controller restarts %s, generation %d; in-flight requests are cut", signal, msg, part, gen))
	if err := p.setMeta(in, func(n *v1alpha1.Instance) {
		if n.Annotations == nil {
			n.Annotations = map[string]string{}
		}
		n.Annotations[v1alpha1.RestartGenerationAnnotation(part)] = strconv.FormatInt(gen, 10)
	}); err != nil {
		return err
	}
	p.mem.addRestart(in.UID, restartRec{part: part, gen: gen, at: p.now, signal: signal})
	now := metav1.NewTime(p.now)
	if err := p.setStatus(in, func(s *v1alpha1.InstanceStatus) {
		s.Phase = v1alpha1.InstanceVerifying
		cond(&s.Conditions, v1alpha1.ConditionVerified, metav1.ConditionFalse, reason, "reverifying after the restart", in.Generation, now)
		cond(&s.Conditions, v1alpha1.ConditionWarmedUp, metav1.ConditionFalse, reason, "warming up again after the restart", in.Generation, now)
		if part == v1alpha1.PartInstance {
			cond(&s.Conditions, v1alpha1.ConditionQualified, metav1.ConditionFalse, reason, "qualifying again after the restart", in.Generation, now)
		}
		cond(&s.Conditions, v1alpha1.ConditionHealthy, metav1.ConditionFalse, signal, msg, in.Generation, now)
	}); err != nil {
		return err
	}
	p.at(p.now.Add(st.restartBackoff))
	return nil
}

// classFailed counts a failed Allocation in its class (a refused or failed job,
// a failed check, a spent ladder). Three in a row within restart.window degrade
// the deployment and pause provisioning in the class with backoff (HBEP-7,
// Restart and provisioning loops).
func (p *pass) classFailed(in *v1alpha1.Instance) {
	pv := p.pvOf(in.Labels)
	if pv == nil || !pv.hasClass {
		return
	}
	streak := p.mem.classFailed(pv.class, p.now)
	if paused, _ := p.loopPaused(pv.class, pv.settings); paused && len(streak) == 3 {
		p.r.Recorder.Event(p.mdObject(in), true, eventDegraded, v1alpha1.ReasonRestartLoop,
			fmt.Sprintf("three Allocations in a row failed in class %s/%s within %s; provisioning there pauses", pv.class.GPUModel, pv.class.CPUArch, pv.settings.restartWindow))
	}
}

// loopPaused reports whether provisioning in a class pauses: its last three
// failures fell within restart.window, and the pause, restart.backoff doubled
// for every failure beyond the third and capped at restart.window, has not
// run out. It returns when the pause ends.
func (p *pass) loopPaused(c v1alpha1.AcceleratorClass, st settings) (bool, time.Time) {
	streak := p.mem.streak(c)
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
		p.r.Recorder.Event(in, true, eventEndingSoon, "NoReplacementReady",
			fmt.Sprintf("drain start at %s is less than %s away and no replacement is ready", ds.UTC().Format(time.RFC3339), endingSoonLead))
	}
}
