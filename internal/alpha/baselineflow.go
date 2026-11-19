package alpha

// The baseline row's sequencer (baseline.go): the order of its steps, the guard on LLMLab's requests, the stop when the served Instance stops being
// eligible, and the detach, over one small seam. The live run (baseline_live_test.go) and the dry run (dry_baseline_test.go) both implement the seam,
// so the same code is what the dry faults and the mutants run; nothing of the order or the guard is written twice.

import (
	"context"
	"fmt"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// The waits of the baseline row, named here because its worst case is added up from them (budget.go).
const (
	// BaselineP0Wait bounds everything before the GPU job: the managed ModelDeployment's catalog read (the Provider's controller lists LLMLab's
	// catalog once a minute) until the managed target answers, and P0's request.
	BaselineP0Wait = 4 * time.Minute
	// BaselineWaitingPoll is how often the detach phase reads the front's own count of requests waiting in its FIFO (/healthz, local, cheap), so that two waits
	// of about one request's time are not missed between the guard's looks.
	BaselineWaitingPoll = 250 * time.Millisecond
	// BaselinePoll is how often a running phase looks at LLMLab's request count, the Instance's eligibility and the engine.
	BaselinePoll = 2 * time.Second
	// BaselineDetachWait bounds the wait, after the managed ModelDeployment is deleted, for the front to drop its target, and BaselineDetachEvery is how
	// often it looks: each look is a request at the managed Provider's level, which LLMLab answers (one token) while the target is still attached.
	BaselineDetachWait  = 2 * time.Minute
	BaselineDetachEvery = 5 * time.Second
	// BaselineFitMargin is what must remain before the Instance's drain start beyond a phase's bound: the wait for the front's records after it.
	BaselineFitMargin = stack.WaitDeadline + time.Minute
)

// ServedReady is what the served ModelDeployment showed once its Instance was ready and registered at the front.
type ServedReady struct {
	ReadyAt, DrainStart time.Time
	Deployment          DeploymentSpec
	Served              ServedMD
}

// Generator is one phase's run of the load generator.
type Generator interface {
	// Done is closed when the generator has ended: by itself, or after Stop.
	Done() <-chan struct{}
	// Stop ends it early; its records so far stay.
	Stop()
	// Result is the generator's records, for the join, and an error when the run broke (exit status 2, records that cannot be read). Called after Done.
	Result() ([]GenRow, error)
}

// BaselineWorld is the seam of the sequencer: what the row asks of the stack, the cluster and the generator.
type BaselineWorld interface {
	Now() time.Time
	// AttachManaged brings the stack up, creates the managed ModelDeployment beside the managed Provider, and waits until the managed target answers.
	AttachManaged(ctx context.Context) (ManagedMD, error)
	// Probe is P0: one request of P0Tokens tokens at the plain model, with the front's usage record of it.
	Probe(ctx context.Context) (ProbeObs, error)
	// StartServed creates the served ModelDeployment and waits for its job to start and its Instance to be ready and registered at the front.
	StartServed(ctx context.Context) (ServedReady, error)
	// LoggedKV is the engine's logged kv_cache_size_tokens, read after ready; false when it could not be read.
	LoggedKV(ctx context.Context) (int64, bool)
	// Gate is one request at model@<provider>, which only the served Instance answers.
	Gate(ctx context.Context) (GateObs, error)
	// DialFailures reads the front's own log, the stack's private state, and returns the requests it reports resent after a failed dial of the served Instance
	// and the number of lines it parsed (ParseDialFailures). False when the log could not be read, which fails closed. Nothing of the log but these requests
	// and the count leaves the world.
	DialFailures(ctx context.Context) ([]DialFailure, int, bool)
	// FrontWaiting is the front's own count of requests waiting in its FIFO, read from its /healthz ("waiting", internal/front/server.go:554): it sums every
	// group's queue (router.go:609). False when it could not be read.
	FrontWaiting(ctx context.Context) (int, bool)
	// ServedDeploymentsOfModel is how many served ModelDeployments of the row's model the stack holds: the front's waiting count is one deployment's only when
	// there is exactly one. False when it could not be read.
	ServedDeploymentsOfModel(ctx context.Context) (int, bool)
	// LLMLabCount is the front's request metric for the managed Provider, hpcburst_front_requests_total{provider}, summed over its codes.
	LLMLabCount(ctx context.Context) (float64, bool)
	// ServedEligible says the served Instance is ready and eligible: its phase ready and AgentUnreachable not true.
	ServedEligible(ctx context.Context) bool
	// EngineSample reads the served Instance's engine gauges.
	EngineSample(ctx context.Context) (EngineSample, bool)
	// StartGenerator runs the phase's generator.
	StartGenerator(ctx context.Context, ph Phase) (Generator, error)
	// FrontRows are the front's usage records written since a time, once every request ID in want has a record or the wait for them has run out.
	FrontRows(ctx context.Context, since time.Time, want []string) ([]FrontRow, error)
	// DetachManaged deletes the row's managed ModelDeployment, which releases no Allocation.
	DetachManaged(ctx context.Context) error
	// EndServed deletes the served ModelDeployment and waits until the cluster's record shows its job ended: the last step. The release removes the
	// Allocation's directory, the engine's log with it, so nothing reads the directory after it.
	EndServed(ctx context.Context) error
	// ManagedDetached says the front no longer holds the managed target.
	ManagedDetached(ctx context.Context) bool
}

// BaselineSettings are the sequencer's parameters.
type BaselineSettings struct {
	Phases []Phase
	Cap    int
	// Row is the row's name (BetaBaselineName or BetaAthenaBaselineName): the observation carries it, and the judge names the row's facts by it.
	Row string
	// OmitMinTokens is the run-wide min_tokens switch, recorded in the observation.
	OmitMinTokens bool
	// Poll, FitMargin, DetachWait and DetachEvery are BaselinePoll, BaselineFitMargin, BaselineDetachWait and BaselineDetachEvery unless a test sets them.
	Poll, FitMargin, DetachWait, DetachEvery, WaitingPoll time.Duration
	Logf                                                  func(format string, args ...any)
	// lastRead is the agent's read time of the last engine read taken anywhere in the run, shared by every phase (RunBaseline sets it).
	lastRead *time.Time
}

// takeRead adds an engine read to the phase if it is a NEW read: the controller's gauges repeat the last read between reads, so a scrape that shows the read
// time it showed before is the same read, and one with no read time is never fresh (#776's review, point 7).
func (s BaselineSettings) takeRead(po *PhaseObs, smp EngineSample) {
	if smp.ReadAt.IsZero() || (s.lastRead != nil && !smp.ReadAt.After(*s.lastRead)) {
		return
	}
	if s.lastRead != nil {
		*s.lastRead = smp.ReadAt
	}
	po.Engine = append(po.Engine, smp)
}

func (s BaselineSettings) logf(f string, a ...any) {
	if s.Logf != nil {
		s.Logf(f, a...)
	}
}

// RunBaseline runs Row 1: the managed leg and P0 before the GPU job; the served Instance ready; the logged KV as a check; the gate; then each phase,
// the detach phase after the managed ModelDeployment is deleted. It returns what it saw. A phase the time left before the drain start cannot hold ends the
// run (DidNotFit); a logged KV under the cap's need ends it with no phase run. An error is a run that broke: a scenario that could not go on.
func RunBaseline(ctx context.Context, w BaselineWorld, s BaselineSettings) (BaselineObs, error) {
	o, err := runBaselineSteps(ctx, w, s)
	if err != nil || !o.ServedCreated {
		return o, err // no served deployment, so no job and nothing to end
	}
	// Every read of the Allocation's directory is above: the engine's logged KV, while the Instance was up, after it was ready and before any phase.
	if err := w.EndServed(ctx); err != nil {
		return o, err
	}
	return o, nil
}

// runBaselineSteps is RunBaseline up to the end of the served deployment: it returns when a phase, the KV check or the clock ended the run.
func runBaselineSteps(ctx context.Context, w BaselineWorld, s BaselineSettings) (BaselineObs, error) {
	if s.Poll <= 0 {
		s.Poll = BaselinePoll
	}
	if s.FitMargin <= 0 {
		s.FitMargin = BaselineFitMargin
	}
	if s.DetachWait <= 0 {
		s.DetachWait = BaselineDetachWait
	}
	if s.DetachEvery <= 0 {
		s.DetachEvery = BaselineDetachEvery
	}
	if s.WaitingPoll <= 0 {
		s.WaitingPoll = BaselineWaitingPoll
	}
	s.lastRead = new(time.Time)
	o := BaselineObs{Row: s.Row, Cap: s.Cap, K: LLMLabK, LLMLabProvider: LLMLabProviderName, OmitMinTokens: s.OmitMinTokens, Plan: append([]Phase(nil), s.Phases...)}
	var err error
	if o.Managed, err = w.AttachManaged(ctx); err != nil {
		return o, err
	}
	if o.P0, err = w.Probe(ctx); err != nil {
		return o, err
	}
	s.logf("P0 is sent: the managed leg answers before any Instance exists")
	// The served ModelDeployment, and with it the first sbatch, is created only once P0 has completed: a managed leg that does not answer, a Provider that never
	// becomes Ready or an LLMLab that fails ends the run here, before any GPU job and so on no A100 time. P0 FAILS the run when the front answered it (the managed
	// path is ours to get right) and makes it INVALID when LLMLab did (a failed LLMLab says nothing of the framework); it is no NOT RUN, since that is a job that
	// never started and here none was submitted.
	if f, i := p0Problems(o); len(f)+len(i) > 0 {
		return o, nil
	}
	ready, err := w.StartServed(ctx)
	if err != nil {
		return o, err
	}
	o.ServedCreated = true
	o.ReadyAt, o.DrainStart, o.Deployment, o.Served = ready.ReadyAt, ready.DrainStart, ready.Deployment, ready.Served
	// A check and never an input: the cap was fixed before the run. It is read here, while the Instance is up, and nowhere later: the Allocation's directory,
	// with the engine's log, is removed once its job is released (internal/providerctl/delete.go:110, internal/slurm/files.go:109-113).
	if o.LoggedKV, o.KVRead = w.LoggedKV(ctx); o.KVRead && o.LoggedKV < int64(s.Cap)*MaxModelLen {
		return o, nil
	}
	if o.Gate, err = w.Gate(ctx); err != nil {
		return o, err
	}
	// A gate the served Instance did not answer stops the row here, before any phase: the generator is never started, so LLMLab is exposed to none of the load.
	// It is a FAIL, never an INVALID: the gate's request is model@<provider> for the served Provider, which LLMLab can never answer, so its failure is the front's
	// or the Instance's own (an INVALID is for a request LLMLab answered and failed).
	if !gateAnswered(o.Gate) {
		o.StoppedBefore = "the gate"
		return o, nil
	}
	phases, err := resolveLongShape(ctx, w, s, &o)
	if err != nil {
		return o, err
	}
	if o.StoppedBefore != "" {
		return o, nil
	}
	for _, ph := range phases {
		if w.Now().Add(ph.Bound + s.FitMargin).After(o.DrainStart) {
			o.DidNotFit = ph.Name
			break
		}
		if ph.Kind == KindDetach {
			if err := w.DetachManaged(ctx); err != nil {
				return o, err
			}
			o.Detached = waitUntil(ctx, w, s.DetachWait, s.DetachEvery, w.ManagedDetached)
			s.logf("%s: the managed ModelDeployment is deleted; the front dropped its target: %t", ph.Name, o.Detached)
			// The wait above (up to DetachWait) comes after the fit was checked, and a slow detach can eat the margin: the fit is checked again IMMEDIATELY
			// before the phase is launched, and a phase that no longer fits is not started, recorded as DidNotFit (point 5 of PR 809's review).
			if w.Now().Add(ph.Bound + s.FitMargin).After(o.DrainStart) {
				o.DidNotFit = ph.Name
				break
			}
		}
		po, perr := runPhase(ctx, w, s, ph)
		o.Phases = append(o.Phases, po)
		if perr != nil {
			return o, perr
		}
		if po.Stopped != "" && po.Stopped != StopBound {
			// A generator stopped for LLMLab's guard or the Instance's eligibility leaves nothing to run the next phase on.
			break
		}
	}
	return o, nil
}

// waitUntil polls until the condition holds, for at most d.
func waitUntil(ctx context.Context, w BaselineWorld, d, every time.Duration, cond func(context.Context) bool) bool {
	deadline := w.Now().Add(d)
	for {
		if cond(ctx) {
			return true
		}
		if !w.Now().Before(deadline) || ctx.Err() != nil {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(every):
		}
	}
}

// runPhase runs one phase's generator and, every poll, looks: LLMLab's request count against the guard (past it the generator is stopped), the Instance's
// eligibility (when it is not, the generator is stopped: its whole share would otherwise go to LLMLab), and the engine. Then it reads the front's records.
func runPhase(ctx context.Context, w BaselineWorld, s BaselineSettings, ph Phase) (PhaseObs, error) {
	po := PhaseObs{Phase: ph, Start: w.Now()}
	before, beforeOK := w.LLMLabCount(ctx)
	if !beforeOK {
		// The guard FAILS CLOSED: with no count of LLMLab's requests the 144-request guard cannot run, so the generator is never started (the science
		// agent's ruling, #776's review, point 8). It is never a guard that is simply off for the phase.
		po.Stopped, po.End = StopGuardUnread, w.Now()
		s.logf("%s: the front's request metric could not be read, so the guard cannot run and the generator was not started", ph.Name)
		return po, nil
	}
	if smp, ok := w.EngineSample(ctx); ok {
		s.takeRead(&po, smp)
	}
	gen, err := w.StartGenerator(ctx, ph)
	if err != nil {
		return po, fmt.Errorf("%s: the generator could not start: %w", ph.Name, err)
	}
	// The detach phase reads the front's own waiting count at its own, faster cadence, and does the guard's looks every Poll/WaitingPoll ticks of it.
	detach := ph.Kind == KindDetach
	interval, slowEvery := s.Poll, 1
	if detach {
		po.ServedOfModel, po.ServedOfModelRead = w.ServedDeploymentsOfModel(ctx)
		interval, slowEvery = s.WaitingPoll, max(1, int(s.Poll/s.WaitingPoll))
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	ticks := 0
	deadline := po.Start.Add(ph.Bound)
loop:
	for {
		select {
		case <-gen.Done():
			break loop
		case <-ctx.Done():
			gen.Stop()
			<-gen.Done()
			return po, ctx.Err()
		case <-tick.C:
		}
		if detach {
			if n, ok := w.FrontWaiting(ctx); ok {
				po.Waiting = append(po.Waiting, WaitSample{At: w.Now(), N: n})
			}
			if ticks++; ticks%slowEvery != 0 {
				continue
			}
		}
		n, ok := w.LLMLabCount(ctx)
		if !ok {
			po.Stopped = StopGuardUnread // a scrape that fails stops the phase: the guard fails closed
			gen.Stop()
			break loop
		}
		if n-before > LLMLabGuard {
			po.Stopped = StopGuard
			gen.Stop()
			break loop
		}
		if !w.ServedEligible(ctx) {
			po.Stopped = StopIneligible
			gen.Stop()
			break loop
		}
		if smp, ok := w.EngineSample(ctx); ok {
			s.takeRead(&po, smp)
		}
		if w.Now().After(deadline) {
			po.Stopped = StopBound
			gen.Stop()
			break loop
		}
	}
	<-gen.Done()
	if po.Gen, err = gen.Result(); err != nil {
		return po, fmt.Errorf("%s: the generator's run broke: %w", ph.Name, err)
	}
	po.End = w.Now()
	if detach {
		// One more read once the generator has ended: the waiters must have been placed by then.
		if n, ok := w.FrontWaiting(ctx); ok {
			po.Waiting = append(po.Waiting, WaitSample{At: w.Now(), N: n})
		}
	}
	if smp, ok := w.EngineSample(ctx); ok {
		s.takeRead(&po, smp)
	}
	if after, ok := w.LLMLabCount(ctx); ok {
		po.LLMLabMetric, po.MetricRead = after-before, true
	}
	ids := make([]string, 0, len(po.Gen))
	for _, g := range po.Gen {
		if g.RequestID != "" {
			ids = append(ids, g.RequestID)
		}
	}
	if po.Rows, err = w.FrontRows(ctx, po.Start, ids); err != nil {
		return po, fmt.Errorf("%s: the front's usage records could not be read: %w", ph.Name, err)
	}
	// Guard 2: the front's own log, read after the phase while the stack is up. A request of the phase that it reports resent after a failed dial of the served
	// Instance makes the phase INVALID: the record cannot tell it from an overflow. Only the counts are logged.
	if fails, parsed, ok := w.DialFailures(ctx); ok {
		po.DialLogRead, po.DialLines = true, parsed
		inPhase := map[string]bool{}
		for _, r := range po.Rows {
			inPhase[r.RequestID] = true
		}
		seen := map[string]bool{}
		for _, d := range fails {
			if inPhase[d.Request] && !seen[d.Request] {
				seen[d.Request] = true
				po.DialFailed = append(po.DialFailed, d)
			}
		}
		po.DialFailures = len(po.DialFailed)
	}
	s.logf("%s: %d generator records, %d front records, %d log lines parsed, %d resent after a failed dial, stopped %q", ph.Name, len(po.Gen), len(po.Rows), po.DialLines, po.DialFailures, po.Stopped)
	return po, nil
}

// gateAnswered says the gate's request was answered whole by the served Instance: sent, status 200, no cut, emitter instance.
func gateAnswered(g GateObs) bool {
	return g.Sent && g.Row.Status == 200 && g.Row.CutBy == "" && g.Row.Emitter == "instance"
}

// ratioAnswered says the ratio probe's request was answered whole by the served Instance: it has a record, status 200, no cut, emitter instance.
func ratioAnswered(r RatioObs) bool {
	return r.Sent && r.Row.Status == 200 && r.Row.CutBy == "" && r.Row.Emitter == "instance"
}

// BaselineProbeBound bounds the ratio probe's one request.
const BaselineProbeBound = 3 * time.Minute

// resolveLongShape sets the words of every phase that derives its context from a token target (Phase.TargetTokens): after the gate, ONE probe at the served
// Instance alone, by the generator's binary with one session of one turn, RatioProbeWords words and one output token, whose prompt tokens the front's usage
// record gives. LongContextWords turns the ratio into words. A phase whose words cannot be set is left out of the run and the reading is INVALID, never a
// guess. It sets the shape of a phase and no cap. A run with no such phase sends no probe.
func resolveLongShape(ctx context.Context, w BaselineWorld, s BaselineSettings, o *BaselineObs) ([]Phase, error) {
	phases := append([]Phase(nil), s.Phases...)
	need := false
	for _, ph := range phases {
		need = need || (ph.TargetTokens > 0 && ph.Context < 1)
	}
	if !need {
		return phases, nil
	}
	o.Ratio = RatioObs{Needed: true, Words: RatioProbeWords}
	probe := Phase{Name: PhaseRatio, Kind: KindProbe, OnServed: true, Sessions: 1, Turns: 1, Context: RatioProbeWords, Output: 1, Bound: BaselineProbeBound}
	po, err := runPhase(ctx, w, s, probe)
	if err != nil {
		return nil, err
	}
	if joined, _, _, _ := joinPhase(po); len(joined) == 1 {
		row := joined[0].Front
		o.Ratio.Sent, o.Ratio.Row = true, row
		if row.Status == 200 && row.CutBy == "" && row.PromptTokens != nil {
			o.Ratio.PromptTokens = *row.PromptTokens
			o.Ratio.TokensPerWord = float64(*row.PromptTokens) / RatioProbeWords
		}
	}
	// A probe the served Instance did not answer is the gate's hole by a second door: the row stops before any phase, as a FAIL. A probe it answered whole with
	// no prompt tokens only leaves the long shape unset (INVALID, P1-long not run).
	o.Ratio.Stopped = po.Stopped
	if !ratioAnswered(o.Ratio) || po.Stopped != "" {
		o.StoppedBefore = "the ratio probe"
		return phases, nil
	}
	out := phases[:0]
	for _, ph := range phases {
		if ph.TargetTokens > 0 && ph.Context < 1 {
			words, ok := LongContextWords(RatioProbeWords, o.Ratio.PromptTokens, ph.TargetTokens)
			if !ok {
				o.Ratio.Invalid = true
				continue
			}
			ph.Context, o.Ratio.LongWords = words, words
			s.logf("%s: the ratio probe read %d prompt tokens for %d words (%.4f a word), so the contexts are %d words", ph.Name, o.Ratio.PromptTokens, RatioProbeWords, o.Ratio.TokensPerWord, words)
		}
		out = append(out, ph)
	}
	return out, nil
}
