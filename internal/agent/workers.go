package agent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/procgroup"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// startFailedExitCode is reported for a worker whose process could not be
// started at all (the shell's "command not found" code). The part counts as
// exited without being told to, which is the controller's to judge.
const startFailedExitCode = 127

// shutdownMargin is how long the agent's own exit waits, past the stop grace,
// for the KILL of a worker's group to take effect.
const shutdownMargin = 10 * time.Second

// startWorker starts one engine worker with Apptainer from the Instance's
// launch (HBEP-7, What it runs: the worker), in a process group the agent owns
// until every member is gone (package procgroup). The engine listens on its
// Unix socket in the job's run directory, bound into the container at
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
		w.logTail = startFailureTail(err, a.keys)
		return w
	}
	_ = os.Remove(w.socket)
	if a.o.CacheDir == "" {
		return fail(errors.New("no cache directory is configured"))
	}
	if err := os.MkdirAll(a.o.CacheDir, 0o700); err != nil {
		return fail(fmt.Errorf("cache directory: %w", err))
	}
	if err := writeChatTemplate(a.o.RunDir, in.launch); err != nil {
		return fail(fmt.Errorf("chat template: %w", err))
	}
	ex := WorkerExec{
		Apptainer:     a.o.Apptainer,
		Image:         in.imagePath,
		CheckpointDir: in.ckptDir,
		RunDir:        a.o.RunDir,
		CacheDir:      a.o.CacheDir,
		Launch:        in.launch,
		Key:           in.key,
		BaseEnv:       a.o.BaseEnv,
	}
	env, err := ex.Env()
	if err != nil {
		return fail(err)
	}
	logf, err := os.OpenFile(engineLogPath(a.o.LogDir, dw.Name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fail(fmt.Errorf("engine log: %w", err))
	}
	fmt.Fprintf(logf, "---- %s start: instance %s, instance generation %d, worker generation %d\n", now.UTC().Format(time.RFC3339), in.uid, instGen, dw.Generation)
	g, err := procgroup.Start(procgroup.Spec{Path: a.o.Apptainer, Args: ex.Args(), Env: env, Dir: a.o.RunDir, Stdout: logf, Stderr: logf}, now)
	if err != nil {
		logf.Close()
		return fail(err)
	}
	w.proc = g
	a.log.Info("worker started", "worker", dw.Name, "pid", g.PID(), "startTime", g.ID().StartTime, "generation", dw.Generation, "instanceGeneration", instGen)
	a.watch(in, w, g, logf)
	return w
}

// watch follows one worker process: it probes the engine until it answers
// (starting to running), samples its health every probe interval while it
// runs, and reports the leader's exit and, apart from it, the end of its whole
// process group.
func (a *Agent) watch(in *instance, w *worker, g *procgroup.Group, logf *os.File) {
	ctx, cancel := context.WithCancel(a.ctx)
	w.stopProbe = cancel
	eng := a.o.Engines(w.socket, in.key, in.launch)
	a.wg.Add(3)
	go func() {
		defer a.wg.Done()
		select {
		case <-g.Exited():
		case <-a.ctx.Done():
			return // shutdown waits on the group itself
		}
		cancel()
		a.post(func() { a.leaderExited(in, g) })
	}()
	go func() {
		defer a.wg.Done()
		select {
		case <-g.Done():
		case <-a.ctx.Done():
			return
		}
		logf.Close()
		a.post(func() { a.groupDone(in, g) })
	}()
	go func() {
		defer a.wg.Done()
		a.probeLoop(ctx, in, g, eng)
	}()
}

// probeLoop probes a starting engine every StartupProbeInterval until it
// answers, then samples it every probe interval.
func (a *Agent) probeLoop(ctx context.Context, in *instance, g *procgroup.Group, eng Engine) {
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
		a.post(func() { a.sampled(in, g, now, perr, h, serr) })
		interval = time.Duration(a.probeEvery.Load())
	}
}

// sampled records a probe and a metrics read of a worker process.
func (a *Agent) sampled(in *instance, g *procgroup.Group, now time.Time, perr error, h agentapi.Health, serr error) {
	w := a.workerOf(in, g)
	if w == nil {
		return
	}
	if w.phase == agentapi.PartStarting && perr == nil {
		w.phase = agentapi.PartRunning
		a.log.Info("worker is serving", "worker", w.name, "pid", g.PID())
		if w.name == agentapi.WorkerName(0) {
			a.fwd.SetTarget(w.socket)
		}
	}
	if w.phase != agentapi.PartRunning {
		return
	}
	w.recordHealth(now, perr, h, serr)
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

// workerOf returns the worker whose current process group is g, or nil when g
// is no longer any worker's (a stale event).
func (a *Agent) workerOf(in *instance, g *procgroup.Group) *worker {
	if a.inst != in {
		return nil
	}
	for _, w := range in.workers {
		if w.proc == g {
			return w
		}
	}
	return nil
}

// leaderExited records the exit of a worker's leader process. A worker the
// agent was stopping stays stopping until its whole group is gone. One that
// exited without being told to is exited, whatever its code, a clean exit too
// (HBEP-7, Health and restarts; L-2.9), and what is left of its group is
// stopped with the same grace as an intentional stop: TERM, then KILL. The
// worker counts as live until the group is gone, so no restart starts beside
// a leftover process.
func (a *Agent) leaderExited(in *instance, g *procgroup.Group) {
	w := a.workerOf(in, g)
	if w == nil {
		return
	}
	code := g.ExitCode()
	w.exitCode = &code
	w.health = nil
	if w.stopProbe != nil {
		w.stopProbe()
	}
	if w.name == agentapi.WorkerName(0) {
		a.fwd.SetTarget("")
	}
	if in.unit != nil {
		a.cancelUnit(in) // its process is gone; a result from it is never used
	}
	if w.phase == agentapi.PartStopping {
		return
	}
	w.phase = agentapi.PartExited
	w.cleaning = true
	w.logTail = readLogTail(engineLogPath(a.o.LogDir, w.name), a.keys)
	a.log.Warn("worker exited without being told to", "worker", w.name, "pid", g.PID(), "exitCode", code)
	g.Stop(a.stopGrace)
}

// groupDone records that every process of a worker's group is gone.
func (a *Agent) groupDone(in *instance, g *procgroup.Group) {
	w := a.workerOf(in, g)
	if w == nil {
		return
	}
	w.cleaning = false
	if w.phase == agentapi.PartStopping {
		w.phase = agentapi.PartStopped
		if w.exitCode == nil {
			code := g.ExitCode()
			w.exitCode = &code
		}
		a.log.Info("worker stopped", "worker", w.name, "pid", g.PID(), "exitCode", *w.exitCode)
	}
}

// terminate stops a worker politely: TERM to its process group, then KILL to
// the group once the stop grace has passed, if any member still runs (HBEP-7,
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
	if w.stopProbe != nil {
		w.stopProbe()
	}
	a.log.Info("stopping worker", "worker", w.name, "pid", w.proc.PID(), "grace", a.stopGrace)
	w.proc.Stop(a.stopGrace)
}

func (a *Agent) stopWorkers(in *instance) {
	for _, w := range in.workers {
		a.terminate(w)
	}
}

// shutdown stops every worker at the agent's own exit: TERM, then KILL after
// the stop grace, waiting on the groups directly, as the loop's events no
// longer flow.
func (a *Agent) shutdown() {
	in := a.inst
	if in == nil {
		return
	}
	var groups []*procgroup.Group
	for _, w := range in.workers {
		if w.live() && w.proc != nil {
			w.proc.Stop(a.stopGrace)
			groups = append(groups, w.proc)
		}
	}
	deadline := time.NewTimer(a.stopGrace + shutdownMargin)
	defer deadline.Stop()
	for _, g := range groups {
		select {
		case <-g.Done():
		case <-deadline.C:
			a.log.Error("a worker's process group outlived the stop grace and its KILL", "pid", g.PID())
		}
	}
	for _, w := range in.workers {
		if w.proc != nil && w.live() {
			code := w.proc.ExitCode()
			w.exitCode = &code
			w.phase = agentapi.PartStopped
			w.cleaning = false
		}
	}
}

// engineLogPath is a worker's engine log in the log directory.
func engineLogPath(logDir, worker string) string {
	return filepath.Join(logDir, "engine-"+worker+".log")
}

// readLogTail returns the last agentapi.MaxLogTailBytes of a worker's engine
// log, starting at a UTF-8 boundary, with any invalid byte replaced and every
// key in keys replaced (WorkerReport.LogTail). The keys are every Instance key
// the agent has drawn in this job, the current one and the earlier ones: the
// log is appended to across Instances, so its tail can hold an earlier
// Instance's output. The keys are replaced before the tail is cut (PR #138
// review, Astra B1, Opus 1): the read starts len(key)-1 bytes before the
// window, so a key that crosses the window's edge is read whole and replaced,
// and those first bytes, which can hold only the end of a key that began
// before the read, are never shown. An unreadable log gives "".
func readLogTail(path string, keys []string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return ""
	}
	overlap := int64(max(longest(keys)-1, 0))
	off := max(0, fi.Size()-agentapi.MaxLogTailBytes-overlap)
	buf := make([]byte, fi.Size()-off)
	n, _ := f.ReadAt(buf, off)
	drop := 0
	if off > 0 {
		drop = int(overlap)
	}
	b := redactKeys(buf[:n], drop, keys)
	for len(b) > 0 && !utf8.RuneStart(b[0]) {
		b = b[1:]
	}
	return tailUTF8(strings.ToValidUTF8(string(b), "\uFFFD"), agentapi.MaxLogTailBytes)
}

// startFailureTail is the LogTail of a worker that could not start: why, with
// every key in keys replaced before the text is cut to
// agentapi.MaxLogTailBytes, so a cut never leaves part of a key.
func startFailureTail(err error, keys []string) string {
	b := redactKeys([]byte("the worker did not start: "+err.Error()), 0, keys)
	return agentapi.TruncateUTF8(strings.ToValidUTF8(string(b), "\uFFFD"), agentapi.MaxLogTailBytes)
}

// redactKeys returns b with every occurrence of each key replaced by
// agentapi.RedactedValue, one marker for each run of overlapping or adjacent
// occurrences, and without b[:drop]: bytes that can hold the end of a key
// whose start is before b. An occurrence that starts in b[:drop] and ends past
// it is replaced by the marker.
func redactKeys(b []byte, drop int, keys []string) []byte {
	cover := make([]bool, len(b))
	for _, k := range keys {
		if k == "" {
			continue
		}
		kb := []byte(k)
		for i := 0; i+len(kb) <= len(b); {
			j := bytes.Index(b[i:], kb)
			if j < 0 {
				break
			}
			for x := i + j; x < i+j+len(kb); x++ {
				cover[x] = true
			}
			i += j + 1
		}
	}
	out := make([]byte, 0, len(b))
	for i := 0; i < len(b); {
		if cover[i] {
			j := i
			for j < len(b) && cover[j] {
				j++
			}
			if j > drop {
				out = append(out, agentapi.RedactedValue...)
			}
			i = j
			continue
		}
		if i >= drop {
			out = append(out, b[i])
		}
		i++
	}
	return out
}

// longest returns the length of the longest string in ss.
func longest(ss []string) int {
	n := 0
	for _, s := range ss {
		n = max(n, len(s))
	}
	return n
}

// tailUTF8 returns the last at most n bytes of s, starting at a rune boundary.
func tailUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	s = s[len(s)-n:]
	for len(s) > 0 && !utf8.RuneStart(s[0]) {
		s = s[1:]
	}
	return s
}
