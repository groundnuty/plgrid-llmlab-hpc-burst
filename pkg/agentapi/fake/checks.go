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
	c := i.Checks
	if c == nil {
		in.finishWarmUp()
		return
	}
	if in.checks == nil || in.checks.VerificationGeneration != c.VerificationGeneration {
		in.checks = &agentapi.CheckResults{VerificationGeneration: c.VerificationGeneration}
	}
	res := in.checks

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
		in.finishWarmUp()
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
func (in *instance) finishWarmUp() {
	if in.checks != nil && in.checks.WarmUp != nil {
		in.checks.WarmUp.Done = true
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
