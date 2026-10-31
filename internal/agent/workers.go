package agent

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// startFailedExitCode is reported for a worker whose process could not be
// started at all (the shell's "command not found" code). The part counts as
// exited without being told to, which is the controller's to judge.
const startFailedExitCode = 127

// startWorker starts one engine worker with Apptainer from the Instance's
// launch (HBEP-7, What it runs: the worker). The engine listens on its Unix
// socket in the job's run directory, bound into the container at
// agentapi.ContainerRunDir.
func (a *Agent) startWorker(in *instance, dw agentapi.DesiredWorker, instGen int64) *worker {
	w := &worker{
		name:       dw.Name,
		phase:      agentapi.PartStarting,
		generation: dw.Generation,
		instGen:    instGen,
		socket:     filepath.Join(a.o.RunDir, strings.TrimPrefix(dw.EngineSocket, agentapi.ContainerRunDir+"/")),
	}
	now := a.o.Now()
	fail := func(err error) *worker {
		a.log.Error("worker did not start", "worker", dw.Name, "err", err)
		code := startFailedExitCode
		w.phase = agentapi.PartExited
		w.exitCode = &code
		return w
	}
	_ = os.Remove(w.socket)
	if err := writeChatTemplate(a.o.RunDir, in.launch); err != nil {
		return fail(fmt.Errorf("chat template: %w", err))
	}
	ex := WorkerExec{
		Apptainer:     a.o.Apptainer,
		Image:         in.imagePath,
		CheckpointDir: in.ckptDir,
		RunDir:        a.o.RunDir,
		Launch:        in.launch,
		Key:           in.key,
		BaseEnv:       a.o.BaseEnv,
	}
	env, err := ex.Env()
	if err != nil {
		return fail(err)
	}
	logf, err := os.OpenFile(filepath.Join(a.o.LogDir, "engine-"+dw.Name+".log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fail(fmt.Errorf("engine log: %w", err))
	}
	fmt.Fprintf(logf, "---- %s start: instance %s, instance generation %d, worker generation %d\n", now.UTC().Format(time.RFC3339), in.uid, instGen, dw.Generation)
	p, err := StartProc(ProcSpec{Path: a.o.Apptainer, Args: ex.Args(), Env: env, Dir: a.o.RunDir, Log: logf}, now)
	if err != nil {
		logf.Close()
		return fail(err)
	}
	w.proc = p
	a.log.Info("worker started", "worker", dw.Name, "pid", p.PID(), "generation", dw.Generation, "instanceGeneration", instGen)
	a.watch(in, w, p, logf)
	return w
}

// watch follows one worker process: it probes the engine until it answers
// (starting to running), samples its health every probe interval while it
// runs, and reports its exit.
func (a *Agent) watch(in *instance, w *worker, p *Proc, logf io.Closer) {
	ctx, cancel := context.WithCancel(a.ctx)
	w.stopProbe = cancel
	eng := a.o.Engines(w.socket, in.key)
	a.wg.Add(2)
	go func() {
		defer a.wg.Done()
		select {
		case <-p.Done():
		case <-a.ctx.Done():
			return // shutdown waits on the process itself
		}
		logf.Close()
		cancel()
		a.post(func() { a.exited(in, p) })
	}()
	go func() {
		defer a.wg.Done()
		a.probeLoop(ctx, in, p, eng)
	}()
}

// probeLoop probes a starting engine every StartupProbeInterval until it
// answers, then samples it every probe interval.
func (a *Agent) probeLoop(ctx context.Context, in *instance, p *Proc, eng Engine) {
	interval := a.o.StartupProbeInterval
	ready := false
	for {
		t := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
		pctx, cancel := context.WithTimeout(ctx, max(interval, 5*time.Second))
		perr := eng.Probe(pctx)
		var h agentapi.Health
		var serr error
		if ready || perr == nil {
			h, serr = eng.Sample(pctx)
		}
		cancel()
		if ctx.Err() != nil {
			return
		}
		now := a.o.Now()
		if !ready {
			if perr != nil {
				continue
			}
			ready = true
		}
		a.post(func() { a.sampled(in, p, now, perr, h, serr) })
		interval = time.Duration(a.probeEvery.Load())
	}
}

// sampled records a probe and a metrics read of a worker process.
func (a *Agent) sampled(in *instance, p *Proc, now time.Time, perr error, h agentapi.Health, serr error) {
	w := a.workerOf(in, p)
	if w == nil {
		return
	}
	if w.phase == agentapi.PartStarting && perr == nil {
		w.phase = agentapi.PartRunning
		a.log.Info("worker is serving", "worker", w.name, "pid", p.PID())
		if w.name == agentapi.WorkerName(0) {
			a.fwd.SetTarget(w.socket)
		}
	}
	if w.phase != agentapi.PartRunning {
		return
	}
	if serr == nil {
		w.lastHealth = h
	} else {
		// The counters are cumulative since the process started, so the last
		// ones read stay true while a read fails; the gauges are carried too.
		h = w.lastHealth
		h.ResponsesByStatus = cloneCounts(w.lastHealth.ResponsesByStatus)
	}
	if perr == nil {
		w.failures = 0
	} else {
		w.failures++
	}
	h.Time = now
	h.ProbeOK = perr == nil
	h.ConsecutiveProbeFailures = w.failures
	w.health = &h
}

func cloneCounts(m map[string]int64) map[string]int64 {
	if m == nil {
		return nil
	}
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// workerOf returns the worker whose current process is p, or nil when p is
// no longer any worker's (a stale event).
func (a *Agent) workerOf(in *instance, p *Proc) *worker {
	if a.inst != in {
		return nil
	}
	for _, w := range in.workers {
		if w.proc == p {
			return w
		}
	}
	return nil
}

// exited records a worker process's exit: stopped when the agent stopped it,
// exited when it exited without being told to, whatever its code; a clean exit
// is a failure too (HBEP-7, Health and restarts; L-2.9). Whatever is left of
// its process group is killed.
func (a *Agent) exited(in *instance, p *Proc) {
	p.Kill()
	w := a.workerOf(in, p)
	if w == nil {
		return
	}
	code := p.ExitCode()
	w.exitCode = &code
	w.health = nil
	if w.phase == agentapi.PartStopping {
		w.phase = agentapi.PartStopped
		a.log.Info("worker stopped", "worker", w.name, "pid", p.PID(), "exitCode", code)
	} else {
		w.phase = agentapi.PartExited
		a.log.Warn("worker exited without being told to", "worker", w.name, "pid", p.PID(), "exitCode", code)
	}
	if w.name == agentapi.WorkerName(0) {
		a.fwd.SetTarget("")
	}
	if in.unit != nil {
		a.cancelUnit(in) // its process is gone; a result from it is never used
	}
}

// terminate stops a worker politely: TERM to its process group, then KILL to
// the group once the stop grace has passed, if it still runs (HBEP-7,
// Restarts and stops). The deadline is fixed by the first instruction, with
// the grace of the desired state that carried it.
func (a *Agent) terminate(w *worker) {
	if !w.alive() {
		return
	}
	if w.name == agentapi.WorkerName(0) {
		a.fwd.SetTarget("")
	}
	w.phase = agentapi.PartStopping
	w.health = nil
	p := w.proc
	if w.stopProbe != nil {
		w.stopProbe()
	}
	a.log.Info("stopping worker", "worker", w.name, "pid", p.PID(), "grace", a.stopGrace)
	p.Term()
	grace := a.stopGrace
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		t := time.NewTimer(grace)
		defer t.Stop()
		select {
		case <-p.Done():
		case <-t.C:
			a.log.Warn("worker ignored TERM; killing it", "worker", w.name, "pid", p.PID())
			p.Kill()
		case <-a.ctx.Done():
		}
	}()
}

func (a *Agent) stopWorkers(in *instance) {
	for _, w := range in.workers {
		a.terminate(w)
	}
}

// shutdown stops every worker at the agent's own exit: TERM, then KILL after
// the stop grace, waiting on the processes directly, as the loop's events no
// longer flow.
func (a *Agent) shutdown() {
	in := a.inst
	if in == nil {
		return
	}
	var procs []*Proc
	for _, w := range in.workers {
		if w.live() && w.proc != nil {
			w.proc.Term()
			procs = append(procs, w.proc)
		}
	}
	deadline := time.NewTimer(a.stopGrace)
	defer deadline.Stop()
	for _, p := range procs {
		select {
		case <-p.Done():
		case <-deadline.C:
			for _, q := range procs {
				q.Kill()
			}
			<-p.Done()
		}
	}
	for _, w := range in.workers {
		if w.proc != nil && w.live() {
			w.proc.Kill() // the rest of each group
			code := w.proc.ExitCode()
			w.exitCode = &code
			w.phase = agentapi.PartStopped
		}
	}
}
