package agent

import (
	"context"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Kinds of check units.
const (
	unitVerify  = "verify"
	unitQualify = "qualify"
	unitWarmUp  = "warmUp"
)

// checkUnit is one piece of check work running against the Instance's
// workers: verification, qualification, or one warm-up round. One runs at a
// time, so warm-up never overlaps the checks it follows.
type checkUnit struct {
	kind      string
	gen       int64 // the verification generation it belongs to
	round     int32
	cancel    context.CancelFunc
	cancelled bool
}

// runChecks runs the checks the desired Instance asks for, once per
// verification generation, against the engine's local socket (HBEP-7,
// Instance start). The agent reports raw results; the controller judges.
//
//   - A section runs only while every desired worker runs a process started
//     for the desired generations (runsFor). A unit whose process stops is
//     cancelled, and its result is dropped.
//   - Results carry their verification generation; a new generation drops the
//     old results and cancels the running unit, so a late result of n-1
//     never lands in n.
//   - Warm-up runs round after round, at most agentapi.MaxWarmUpRounds,
//     until a desired state without WarmUp says the controller has decided;
//     the agent then cancels the round in flight and reports WarmUp.Done
//     (agentapi's documentation, Warm-up gates ready).
func (a *Agent) runChecks(in *instance, i agentapi.DesiredInstance) {
	c := i.Checks
	if c == nil {
		a.finishWarmUp(in)
		return
	}
	if in.checks == nil || in.checks.VerificationGeneration != c.VerificationGeneration {
		a.cancelUnit(in)
		in.checks = &agentapi.CheckResults{VerificationGeneration: c.VerificationGeneration}
	}
	runs := in.runsFor(i)
	if c.WarmUp == nil {
		a.finishWarmUp(in)
	}
	if in.unit != nil {
		if !runs {
			a.cancelUnit(in)
		}
		return
	}
	if !runs || !in.staged {
		return
	}
	res := in.checks
	switch {
	case c.Verify != nil && res.Verify == nil:
		a.startVerify(in, *c.Verify)
	case c.Qualify != nil && res.Qualify == nil:
		a.startQualify(in, *c.Qualify)
	case c.WarmUp != nil && (res.WarmUp == nil || !res.WarmUp.Done):
		if res.WarmUp == nil {
			res.WarmUp = &agentapi.WarmUpResult{}
		}
		n := int32(len(res.WarmUp.Rounds)) + 1
		if n > agentapi.MaxWarmUpRounds {
			res.WarmUp.Done = true
			return
		}
		a.startWarmUp(in, n, *c.WarmUp)
	}
}

// engine0 is the Engine of the Instance's first worker, which the checks run
// against: alpha has one worker per Instance.
func (a *Agent) engine0(in *instance) Engine {
	w := in.find(agentapi.WorkerName(0))
	if w == nil {
		w = in.workers[0]
	}
	return a.o.Engines(w.socket, in.key)
}

func (a *Agent) newUnit(in *instance, kind string, round int32) (*checkUnit, context.Context) {
	ctx, cancel := context.WithCancel(a.ctx)
	u := &checkUnit{kind: kind, gen: in.checks.VerificationGeneration, round: round, cancel: cancel}
	in.unit = u
	return u, ctx
}

// unitDone accepts a unit's result when the unit is still the current one of
// the same verification generation, and was not cancelled.
func (a *Agent) unitDone(in *instance, u *checkUnit) bool {
	if a.inst != in || in.unit != u {
		return false
	}
	in.unit = nil
	u.cancel()
	return !u.cancelled && in.checks != nil && in.checks.VerificationGeneration == u.gen
}

func (a *Agent) startVerify(in *instance, req agentapi.VerifyRequest) {
	u, ctx := a.newUnit(in, unitVerify, 0)
	eng := a.engine0(in)
	digest := in.launch.ImageDigest
	files := req.Files
	reuse := len(files) > 0 && in.fileResults != nil && in.filesKey == filesKey(files)
	cached := append([]agentapi.FileResult(nil), in.fileResults...)
	launch, token := in.launch, in.hubToken
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		res := eng.Verify(ctx)
		// The image the agent ran: it pulled and staged exactly this digest
		// (agentapi.StagedImagePath).
		res.ImageDigest = digest
		switch {
		case len(files) == 0:
		case reuse:
			res.Files = cached
		default:
			// The list arrived after staging: rehash now.
			ck, err := a.o.Stager.Checkpoint(ctx, CheckpointRequest{
				Repository: launch.Checkpoint.Repository,
				Revision:   launch.Checkpoint.Revision,
				Files:      files,
				HubToken:   token,
			})
			if err != nil {
				res.Error = agentapi.TruncateUTF8("rehash: "+err.Error(), agentapi.MaxMessageBytes)
			} else {
				res.Files = ck.Files
			}
		}
		res.Done = true
		a.post(func() {
			if a.unitDone(in, u) {
				in.checks.Verify = &res
			}
		})
	}()
}

func (a *Agent) startQualify(in *instance, req agentapi.QualifyRequest) {
	u, ctx := a.newUnit(in, unitQualify, 0)
	eng := a.engine0(in)
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		res := eng.Qualify(ctx, req)
		res.Done = true
		a.post(func() {
			if a.unitDone(in, u) {
				in.checks.Qualify = &res
			}
		})
	}()
}

func (a *Agent) startWarmUp(in *instance, n int32, req agentapi.WarmUpRequest) {
	u, ctx := a.newUnit(in, unitWarmUp, n)
	eng := a.engine0(in)
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		round := eng.WarmUpRound(ctx, n, req)
		round.Round = n
		a.post(func() {
			if !a.unitDone(in, u) {
				return
			}
			w := in.checks.WarmUp
			if w == nil || w.Done {
				return
			}
			w.Rounds = append(w.Rounds, round)
			if n >= agentapi.MaxWarmUpRounds {
				w.Done = true
			}
		})
	}()
}

// cancelUnit cancels the running unit; its result, when it comes, is dropped.
func (a *Agent) cancelUnit(in *instance) {
	if in.unit == nil {
		return
	}
	in.unit.cancelled = true
	in.unit.cancel()
	in.unit = nil
}

// finishWarmUp ends warm-up: the round in flight is cancelled and dropped, and
// the warm-up held, once a round has started, is Done.
func (a *Agent) finishWarmUp(in *instance) {
	if in.unit != nil && in.unit.kind == unitWarmUp {
		a.cancelUnit(in)
	}
	if in.checks != nil && in.checks.WarmUp != nil {
		in.checks.WarmUp.Done = true
	}
}
