package fake

import "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"

// applyChecks runs the checks the desired Instance asks for, once per
// verification generation. Results are held, and reported, until the generation
// changes.
//
// A section that has not completed for the current verification generation
// starts (verification, qualification) or runs its next round (warm-up) only
// when runs is true: every desired worker is running a process started for the
// desired generations (runsFor). Until then the section is absent, and warm-up
// rounds pause; the scripted Engine is not asked. That includes a new
// generation's checks while the old process is still stopping, so a result
// never comes from a process the desired state has replaced. A section that is
// complete stays as it is, whatever the worker does afterwards. Staging being
// done is part of the gate: no worker starts before it.
//
// Warm-up runs one round per Step, up to agentapi.MaxWarmUpRounds. It stops,
// and its result is Done, after the last round, or in the first Step in which
// the desired state no longer asks for warm-up after rounds were run.
func (in *instance) applyChecks(e Engine, i agentapi.DesiredInstance, runs bool) {
	applyChecksTo(e, i, i.Checks, &in.checks, runs)
}

// applyWorkerChecks runs each worker's own checks (DesiredWorker.Checks), with several workers
// (HBEP-7, Several workers): each against its own process, gated on that worker alone running for the
// desired generations (workerRunsFor), so one worker's restart never holds another's checks back. A
// worker's results are held on its process and go with it.
func (in *instance) applyWorkerChecks(e Engine, i agentapi.DesiredInstance) {
	for _, dw := range i.Workers {
		w := in.find(dw.Name)
		if w == nil {
			continue
		}
		applyChecksTo(e, i, dw.Checks, &w.checks, w.runsFor(i, dw))
	}
}

// runsFor reports whether this worker runs a process started for its desired generation and the
// desired Instance generation (or a later one): runsFor for one worker.
func (w *worker) runsFor(i agentapi.DesiredInstance, dw agentapi.DesiredWorker) bool {
	return w.phase == agentapi.PartRunning && w.instGen >= i.Generation && w.generation >= dw.Generation
}

// applyChecksTo runs the checks c asks for into *held, once per verification generation: the
// Instance's own (applyChecks) or one worker's (applyWorkerChecks).
func applyChecksTo(e Engine, i agentapi.DesiredInstance, c *agentapi.Checks, held **agentapi.CheckResults, runs bool) {
	if c == nil {
		finishWarmUp(*held)
		return
	}
	if *held == nil || (*held).VerificationGeneration != c.VerificationGeneration {
		*held = &agentapi.CheckResults{VerificationGeneration: c.VerificationGeneration}
	}
	res := *held

	if runs && c.Verify != nil && res.Verify == nil {
		v := e.verify(i, *c.Verify)
		v.Done = true
		res.Verify = &v
	}
	if runs && c.Qualify != nil && res.Qualify == nil {
		q := e.judgeCorruption(i, *c.Qualify, e.qualify(i, *c.Qualify))
		q.Done = true
		res.Qualify = &q
	}
	if c.WarmUp == nil {
		finishWarmUp(res)
		return
	}
	if !runs {
		return
	}
	if res.WarmUp == nil {
		res.WarmUp = &agentapi.WarmUpResult{}
	}
	w := res.WarmUp
	if w.Done {
		return
	}
	n := int32(len(w.Rounds)) + 1
	round := e.warmUpRound(n, *c.WarmUp)
	round.Round = n
	w.Rounds = append(w.Rounds, round)
	if n >= agentapi.MaxWarmUpRounds {
		w.Done = true
	}
}

// finishWarmUp marks the held warm-up done, when rounds were run: the desired
// state no longer asks for more. Results exist only once a round has run.
func finishWarmUp(res *agentapi.CheckResults) {
	if res != nil && res.WarmUp != nil {
		res.WarmUp.Done = true
	}
}

// runsFor reports whether the Instance has a desired worker and every desired
// worker is in phase running with a process started for its desired generation
// and the desired Instance generation (or a later one). It is the exact
// complement of convergeWorkers' restart condition: a process that condition
// would replace is not one the checks may run against. An Instance with no
// desired worker has no process to check, so it is false.
func (in *instance) runsFor(i agentapi.DesiredInstance) bool {
	if len(i.Workers) == 0 {
		return false
	}
	for _, dw := range i.Workers {
		w := in.find(dw.Name)
		if w == nil || w.phase != agentapi.PartRunning || w.instGen < i.Generation || w.generation < dw.Generation {
			return false
		}
	}
	return true
}
