package fake

import "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"

// applyChecks runs the checks the desired Instance asks for, once per
// verification generation. Results are held, and reported, until the generation
// changes. The Agent runs a check whenever it is asked, whether or not the
// worker is running; the scripted Engine answers.
//
// Warm-up runs one round per Step, up to agentapi.MaxWarmUpRounds. It stops,
// and its result is Done, after the last round, or in the first Step in which
// the desired state no longer asks for warm-up after rounds were run.
func (in *instance) applyChecks(e Engine, i agentapi.DesiredInstance) {
	c := i.Checks
	if c == nil {
		in.finishWarmUp()
		return
	}
	if in.checks == nil || in.checks.VerificationGeneration != c.VerificationGeneration {
		in.checks = &agentapi.CheckResults{VerificationGeneration: c.VerificationGeneration}
	}
	res := in.checks

	if c.Verify != nil && res.Verify == nil {
		v := e.verify(i, *c.Verify)
		v.Done = true
		res.Verify = &v
	}
	if c.Qualify != nil && res.Qualify == nil {
		q := e.qualify(i, *c.Qualify)
		q.Done = true
		res.Qualify = &q
	}
	if c.WarmUp == nil {
		in.finishWarmUp()
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
