package agent

import (
	"context"
	"maps"
	"strconv"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Kinds of check units.
const (
	unitVerify  = "verify"
	unitQualify = "qualify"
	unitWarmUp  = "warmUp"
)

// checker is one set of checks and their state: the Instance's own, which
// runs against worker-0 while every desired worker runs (one worker, as
// alpha), or one worker's, which runs against that worker's own socket while
// that worker runs (several workers; HBEP-7, Several workers; the science
// agent's ruling on #439, Q6). Each runs one unit at a time, so warm-up never
// overlaps the checks it follows, and several workers' checks run side by
// side.
type checker struct {
	// checks of the current verification generation, and the one check unit
	// running, if any.
	checks *agentapi.CheckResults
	unit   *checkUnit
	// sectionKeys holds, for each section of checks, the processes it was
	// taken against.
	sectionKeys map[string]procKey
	// sampling is the sampling of the current verification generation's
	// checks, computed once from the staged file (genSampling); nil until a
	// check unit needs it, and again after a new generation or staging.
	sampling *genSampling
	// usedStaging is the staging (instance.stagingSeq) whose rehash this
	// checker's verification has used: each staging's rehash serves each
	// checker's first verification after it, and a later one rehashes (PR #87
	// review, Astra B2, Opus B1).
	usedStaging int
}

// checkUnit is one piece of check work of one checker, running against its
// workers: verification, qualification, or one warm-up round. One runs at a
// time per checker, so warm-up never overlaps the checks it follows. A cancelled unit
// stays the current one until its goroutine has returned, so nothing that
// follows it (a warm-up's Done, the next unit) starts while its requests are
// still unwinding (PR #87 review, Astra B7).
type checkUnit struct {
	kind      string
	gen       int64   // the verification generation it belongs to
	key       procKey // the processes it runs against
	round     int32
	cancel    context.CancelFunc
	cancelled bool
}

// procKey names the processes a check ran against: the Instance generation,
// and each worker's restart generation and process (PID and start time). A
// result taken against one set of processes is never shown for another (PR
// #87 review, Opus B1).
type procKey string

// processKey returns the key of the Instance's desired workers as they run
// now, and false when they do not all run a process started for the desired
// generations (runsFor).
func (in *instance) processKey(i agentapi.DesiredInstance) (procKey, bool) {
	if !in.runsFor(i) {
		return "", false
	}
	var b strings.Builder
	b.WriteString(strconv.FormatInt(in.generation, 10))
	for _, dw := range i.Workers {
		w := in.find(dw.Name)
		id := w.proc.ID()
		b.WriteString(";" + w.name + ":" + strconv.FormatInt(w.generation, 10) + ":" + strconv.Itoa(id.PID) + ":" + strconv.FormatUint(id.StartTime, 10))
	}
	return procKey(b.String()), true
}

// workerKey is processKey for one worker's own checks: the Instance
// generation and that worker's restart generation and process, and false when
// that worker does not run a process started for the desired generations. It
// covers no other worker, so one worker's restart never drops another's
// results: each is verified on its own (HBEP-7, Several workers).
func (in *instance) workerKey(i agentapi.DesiredInstance, dw agentapi.DesiredWorker) (procKey, bool) {
	w := in.find(dw.Name)
	if w == nil || w.proc == nil || w.phase != agentapi.PartRunning || w.instGen < i.Generation || w.generation < dw.Generation {
		return "", false
	}
	id := w.proc.ID()
	return procKey(strconv.FormatInt(in.generation, 10) + ";" + w.name + ":" + strconv.FormatInt(w.generation, 10) + ":" + strconv.Itoa(id.PID) + ":" + strconv.FormatUint(id.StartTime, 10)), true
}

// workerChecker is worker name's checker, made on first use.
func (in *instance) workerChecker(name string) *checker {
	if in.wck == nil {
		in.wck = map[string]*checker{}
	}
	ck := in.wck[name]
	if ck == nil {
		ck = &checker{}
		in.wck[name] = ck
	}
	return ck
}

// runChecks runs the checks the desired Instance asks for: the Instance's own
// (DesiredInstance.Checks, one worker), against worker-0, and each worker's
// (DesiredWorker.Checks, several workers), against that worker's own socket,
// never through the router (HBEP-7, Several workers).
func (a *Agent) runChecks(in *instance, i agentapi.DesiredInstance) {
	a.runChecker(in, &in.ck, i.Checks, func() (procKey, bool) { return in.processKey(i) }, func() Engine { return a.engine0(in) })
	for _, dw := range i.Workers {
		if dw.Checks == nil && in.wck[dw.Name] == nil {
			continue
		}
		dw := dw
		a.runChecker(in, in.workerChecker(dw.Name), dw.Checks, func() (procKey, bool) { return in.workerKey(i, dw) }, func() Engine { return a.engineOf(in, dw.Name) })
	}
}

// runChecker runs one checker's checks, once per
// verification generation and set of running processes, against the engine's
// local socket (HBEP-7, Instance start). The agent reports raw results; the
// controller judges.
//
//   - A section runs only while every desired worker runs a process started
//     for the desired generations (runsFor). A unit whose process stops is
//     cancelled, and its result is dropped.
//   - Each section remembers the processes it ran against. When they change,
//     as when a worker restarts under a verification generation the
//     controller sent before raising the worker's generation, the section is
//     dropped and runs again against the new processes.
//   - Results carry their verification generation; a new generation drops the
//     old results and cancels the running unit, so a late result of n-1
//     never lands in n.
//   - Warm-up runs round after round, at most agentapi.MaxWarmUpRounds,
//     until a desired state without WarmUp says the controller has decided;
//     the agent then cancels the round in flight, waits for it to return, and
//     only then reports WarmUp.Done (agentapi's documentation, Warm-up gates
//     ready).
func (a *Agent) runChecker(in *instance, ck *checker, c *agentapi.Checks, keyOf func() (procKey, bool), eng func() Engine) {
	if c == nil {
		a.finishWarmUpOf(ck)
		return
	}
	if ck.checks == nil || ck.checks.VerificationGeneration != c.VerificationGeneration {
		a.cancelUnitOf(ck)
		ck.checks = &agentapi.CheckResults{VerificationGeneration: c.VerificationGeneration}
		ck.sectionKeys = map[string]procKey{}
		ck.sampling = nil // the new generation reads the staged file again
	}
	// Results stand only while the processes they were taken against run:
	// when the processes do not all run (one restarts, exits, or its
	// generation was raised), every section is dropped at once, so no report
	// shows a result of a process that is gone.
	key, runs := keyOf()
	a.dropStaleSections(ck, key)
	if c.WarmUp == nil {
		a.finishWarmUpOf(ck)
	}
	if ck.unit != nil {
		if !runs || ck.unit.key != key {
			a.cancelUnitOf(ck)
		}
		return
	}
	if !runs || !in.staged {
		return
	}
	res := ck.checks
	if ck.sampling.refused() && (c.Verify == nil || res.Verify != nil) {
		return // the file was refused: nothing more is checked in this generation
	}
	switch {
	case c.Verify != nil && res.Verify == nil:
		a.startVerify(in, ck, *c.Verify, key, eng())
	case c.Qualify != nil && res.Qualify == nil:
		a.startQualify(in, ck, *c.Qualify, key, eng())
	case c.WarmUp != nil && (res.WarmUp == nil || !res.WarmUp.Done):
		if res.WarmUp == nil {
			res.WarmUp = &agentapi.WarmUpResult{}
			ck.sectionKeys[unitWarmUp] = key
		}
		n := int32(len(res.WarmUp.Rounds)) + 1
		if n > agentapi.MaxWarmUpRounds {
			res.WarmUp.Done = true
			return
		}
		a.startWarmUp(in, ck, n, *c.WarmUp, key, eng())
	}
}

// dropStaleSections drops every completed or started section taken against
// other processes than key; with an empty key, every section.
func (a *Agent) dropStaleSections(ck *checker, key procKey) {
	res := ck.checks
	for kind, k := range ck.sectionKeys {
		if k == key {
			continue
		}
		switch kind {
		case unitVerify:
			res.Verify = nil
		case unitQualify:
			res.Qualify = nil
		case unitWarmUp:
			res.WarmUp = nil
		}
		delete(ck.sectionKeys, kind)
		a.log.Info("check results dropped: the processes they were taken against are gone", "section", kind, "verificationGeneration", res.VerificationGeneration)
	}
}

// engine0 is the Engine of the Instance's first worker, which the checks run
// against: alpha has one worker per Instance.
func (a *Agent) engine0(in *instance) Engine {
	w := in.find(agentapi.WorkerName(0))
	if w == nil {
		w = in.workers[0]
	}
	return a.o.Engines(w.socket, in.key, in.launch)
}

// engineOf is the Engine of worker name, on its own socket, which its own
// checks run against.
func (a *Agent) engineOf(in *instance, name string) Engine {
	return a.o.Engines(in.find(name).socket, in.key, in.launch)
}

func (a *Agent) newUnit(ck *checker, kind string, round int32, key procKey) (*checkUnit, context.Context) {
	ctx, cancel := context.WithCancel(a.ctx)
	u := &checkUnit{kind: kind, gen: ck.checks.VerificationGeneration, key: key, round: round, cancel: cancel}
	ck.unit = u
	return u, ctx
}

// unitDone takes a unit's completion: the unit stops being the current one,
// and its result is accepted when it was not cancelled and still belongs to
// the current verification generation.
func (a *Agent) unitDone(in *instance, ck *checker, u *checkUnit) bool {
	if a.inst != in || ck.unit != u {
		return false
	}
	ck.unit = nil
	u.cancel()
	return !u.cancelled && ck.checks != nil && ck.checks.VerificationGeneration == u.gen
}

func (a *Agent) startVerify(in *instance, ck *checker, req agentapi.VerifyRequest, key procKey, eng Engine) {
	u, ctx := a.newUnit(ck, unitVerify, 0, key)
	digest := ""
	if in.imageVerified {
		// The image the agent ran, checked at its staging against the
		// integrity record of the pinned pull (Stager.Image). An image that
		// was not checked gets no digest, which the controller fails.
		digest = in.launch.ImageDigest
	}
	files := req.Files
	reuse := len(files) > 0 && in.stagingSeq != 0 && ck.usedStaging != in.stagingSeq && in.filesKey == filesKey(files)
	if reuse {
		ck.usedStaging = in.stagingSeq // the staged rehash serves one verification of each checker
	}
	cached := append([]agentapi.FileResult(nil), in.fileResults...)
	launch := in.launch
	ckptDir := in.ckptDir
	gs := ck.genSampling()
	reasoning := DeclaresReasoningParser(launch.Command)
	gen := u.gen
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		// The sampling of this generation's checks, read from the staged file
		// before the first request goes out: verification's own fingerprint
		// request carries it, and the record reports the same file's values.
		// A file with a value vLLM refuses is reported and nothing is sent to
		// the engine: no fingerprint, no qualification, no warm-up.
		g := gs.resolve(ckptDir, reasoning, a.log, gen)
		var res agentapi.VerifyResult
		if g.file.refused != nil {
			res.GenerationConfigRefused = g.file.refused
		} else {
			res = eng.Verify(ctx, g.sampling)
		}
		res.ImageDigest = digest
		res.GenerationConfig = maps.Clone(g.file.config)
		res.DoSample = copyBool(g.file.doSample)
		res.GenerationConfigFile = g.file.state
		switch {
		case len(files) == 0:
		case reuse:
			res.Files = cached
		default:
			// After the start nothing is staged under the running engine:
			// the files are only rehashed, and a missing one is absent.
			ck, err := a.o.Stager.Checkpoint(ctx, CheckpointRequest{
				Repository: launch.Checkpoint.Repository,
				Revision:   launch.Checkpoint.Revision,
				Files:      files,
				RehashOnly: true,
				LoadFormat: LoadFormat(launch.Command),
			})
			if err != nil {
				res.Error = agentapi.TruncateUTF8("rehash: "+err.Error(), agentapi.MaxMessageBytes)
			} else {
				res.Files = ck.Files
			}
		}
		res.Done = true
		a.post(func() {
			if a.unitDone(in, ck, u) {
				ck.checks.Verify = &res
				ck.sectionKeys[unitVerify] = u.key
			}
		})
	}()
}

func (a *Agent) startQualify(in *instance, ck *checker, req agentapi.QualifyRequest, key procKey, eng Engine) {
	u, ctx := a.newUnit(ck, unitQualify, 0, key)
	gs, ckptDir, reasoning, gen := ck.genSampling(), in.ckptDir, DeclaresReasoningParser(in.launch.Command), u.gen
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		g := gs.resolve(ckptDir, reasoning, a.log, gen)
		if g.file.refused != nil {
			// No check runs against a refused file; the unit ends with no result.
			a.post(func() { a.unitDone(in, ck, u) })
			return
		}
		res := eng.Qualify(ctx, req, g.sampling)
		res.Done = true
		a.post(func() {
			if a.unitDone(in, ck, u) {
				ck.checks.Qualify = &res
				ck.sectionKeys[unitQualify] = u.key
			}
		})
	}()
}

func (a *Agent) startWarmUp(in *instance, ck *checker, n int32, req agentapi.WarmUpRequest, key procKey, eng Engine) {
	u, ctx := a.newUnit(ck, unitWarmUp, n, key)
	gs, ckptDir, reasoning, gen := ck.genSampling(), in.ckptDir, DeclaresReasoningParser(in.launch.Command), u.gen
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		g := gs.resolve(ckptDir, reasoning, a.log, gen)
		if g.file.refused != nil {
			a.post(func() { a.unitDone(in, ck, u) })
			return
		}
		round := eng.WarmUpRound(ctx, n, req, g.sampling)
		round.Round = n
		a.post(func() {
			if !a.unitDone(in, ck, u) {
				return
			}
			w := ck.checks.WarmUp
			if w == nil || w.Done || ck.sectionKeys[unitWarmUp] != u.key {
				return
			}
			w.Rounds = append(w.Rounds, round)
			if n >= agentapi.MaxWarmUpRounds {
				w.Done = true
			}
		})
	}()
}

// cancelUnit cancels the running unit of every checker of the Instance.
func (a *Agent) cancelUnit(in *instance) {
	a.cancelUnitOf(&in.ck)
	for _, ck := range in.wck {
		a.cancelUnitOf(ck)
	}
}

// cancelUnitOf cancels a checker's running unit. The unit stays the current
// one until its goroutine returns (unitDone), and its result is then dropped.
func (a *Agent) cancelUnitOf(ck *checker) {
	if ck.unit == nil || ck.unit.cancelled {
		return
	}
	ck.unit.cancelled = true
	ck.unit.cancel()
}

// cancelUnitsFor cancels the units a worker's processes ran: the Instance's
// own checks, which run while every worker runs, and that worker's.
func (a *Agent) cancelUnitsFor(in *instance, name string) {
	a.cancelUnitOf(&in.ck)
	if ck := in.wck[name]; ck != nil {
		a.cancelUnitOf(ck)
	}
}

// finishWarmUp ends warm-up for every checker of the Instance.
func (a *Agent) finishWarmUp(in *instance) {
	a.finishWarmUpOf(&in.ck)
	for _, ck := range in.wck {
		a.finishWarmUpOf(ck)
	}
}

// finishWarmUpOf ends a checker's warm-up: the round in flight is cancelled,
// and the warm-up held, once a round has started, is Done only after that
// round has returned.
func (a *Agent) finishWarmUpOf(ck *checker) {
	if ck.unit != nil && ck.unit.kind == unitWarmUp {
		a.cancelUnitOf(ck)
		return // Done once the cancelled round has returned
	}
	if ck.checks != nil && ck.checks.WarmUp != nil {
		ck.checks.WarmUp.Done = true
	}
}
