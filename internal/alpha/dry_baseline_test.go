package alpha

// The baseline row (baseline.go) over the fake cluster: RunBaseline, the real sequencer, over a world whose generator and front are stood in for, with the
// one job on the fake Slurm. Each fault departs from the healthy run in one way, and ends it FAIL (baselineCases, through TestDryRunsOfEveryRowAndEveryFault)
// or INVALID (baselineInvalidCases, TestTheBaselinesInvalidRunsThroughRun). The healthy world is built from the row's own constants and phase shapes:
// sessions of the phase, their turns, and the routes the front's rule gives them at the cap.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// baselineCases are the baseline row's dry faults that end FAIL, each with the failed check's words.
var baselineCases = []dryCase{
	{"", ""},
	{"llmlab-in-the-control", "request(s), and the control gives it none"},
	{"llmlab-one-too-many-first-turns", "LLMLab answered the first turn of 3 distinct session(s), want exactly k = 2"},
	// Two first-turn requests of ONE session count once; and a turn repeated is a failed check of its own.
	{"llmlab-one-spiller-twice", "LLMLab answered the first turn of 1 distinct session(s), want exactly k = 2"},
	{"llmlab-first-turn-twice", "request(s) repeat a session's turn that was already sent"},
	// Guard 2: the front's own log names the requests resent after a failed dial. A line for another target, at another level, in another message's error field
	// or cut short is no failure of ours; a log that cannot be read fails closed.
	{"dial-failure-other-target", ""},
	{"dial-failure-wrong-level", ""},
	{"dial-failure-in-error-field", ""},
	{"dial-failure-malformed", ""},
	{"front-log-unread", "the front's log could not be read, so a failed dial of the Instance could not be ruled out"},
	// A burst stamped a tick apart is healthy: the rule's tolerance (ArrivalTick) passes it.
	{"llmlab-spill-1ms-before-the-cap", ""},
	{"llmlab-over-its-guard", "LLMLab was asked for more than 144 requests in one phase, so the generator was stopped"},
	{"llmlab-after-the-detach", "request(s) after it was detached"},
	// The rest of the detach phase's conditions: the count back at 0 when the phase ends, the count read, ONE served deployment, all 13 first turns 200.
	{"waiting-not-returning", "the front's waiting count was 2 when the phase ended"},
	{"waiting-unread", "the front's waiting count (/healthz) was not read in the detach phase"},
	{"two-served-mds", "the front holds 2"},
	{"detach-session-missing", "only 12 of the 13 sessions' first turns ended with 200: the others were never sent or never answered"},
	{"detach-not-seen", "the managed ModelDeployment was not seen detached at the front before the phase"},
	{"instance-ineligible", "the served Instance became ineligible, so the generator was stopped"},
	{"engine-over-the-cap", "the engine ran more than the cap of 11 requests"},
	// Waiting requests fail only when sustained for 3 consecutive samples (EngineWaitingSustain); a blip, two in a row, and two runs of two are readings.
	{"engine-waiting", "the engine had requests waiting in 5 consecutive distinct reads"},
	{"engine-waiting-thrice", "the engine had requests waiting in 3 consecutive distinct reads"},
	{"engine-waiting-twice", ""},
	{"engine-waiting-blip", ""},
	{"engine-waiting-split", ""},
	{"engine-preempts", "the engine preempted"},
	{"engine-unread", "fewer than 2 distinct engine reads"},
	// The controller's gauges repeat a read between reads: repeated idle data is no read, and one read scraped four times is one read (#776's review, point 7).
	{"engine-cached-idle", "fewer than 2 distinct engine reads"},
	{"engine-waiting-cached", ""},
	// In the long phase the engine's waiting requests and preemptions are readings (ruling H0), posted and never failed on.
	{"long-engine-readings", ""},
	{"front-503-in-the-control", "a request failed (status 503, emitter \"front\")"},
	{"instance-fails", "a request failed (status 500, emitter \"instance\")"},
	{"session-moves", "a session did not stay on its Instance"},
	{"detach-skipped", "request(s) after it was detached"},
	{"generator-names-another-route", "generator record(s) name another route than the front's usage record"},
	// The guard fails closed: a scrape that fails stops the phase, and one that fails before a phase means its generator is never started.
	{"metric-unread", "the front's per-provider request metric could not be read, so the 144-request guard could not run"},
	{"metric-unread-at-p2-start", "P2: the front's per-provider request metric could not be read, so the 144-request guard could not run"},
	{"metric-unread-midphase", "P2: the front's per-provider request metric could not be read, so the 144-request guard could not run"},
	// The guard polls; what LLMLab answered between its last look and the generator's exit is counted from the front's records.
	{"llmlab-over-final-count", "LLMLab answered 150 requests in the phase by the front's own records, over the guard of 144"},
	{"metric-disagrees", "the front's request metric says LLMLab was asked"},
	{"unlisted-llmlab-request", "LLMLab answered a request the generator did not send"},
	{"no-answer", "request(s) got no answer from the front"},
	{"no-record", "request(s) the generator sent have no usage record at the front"},
	{"over-the-cap-in-flight", "the Instance had more than its cap of 11 requests in flight"},
	{"no-gate", "the gate was not sent"},
	// A gate the served Instance did not answer stops the row before any phase (TestAFailedGateStopsTheRowBeforeAnyPhase).
	{"gate-503", "the gate's request at model@<provider> was not answered by the served Instance (status 503"},
	{"gate-engine-500", "the gate's request at model@<provider> was not answered by the served Instance (status 500"},
	{"gate-cut-200", "the gate's request at model@<provider> was not answered by the served Instance (status 200"},
	{"p0-not-sent", "P0 was not sent"},
	{"p0-answered-by-an-instance", "P0 was not answered by LLMLab"},
	// A stream that asked for no usage has null counts and the front's own count of content events: a completed stream with tokens, so no failure.
	{"p0-counts-null-events", ""},
	{"p0-front-fails", "P0's request failed and the front answered it"},
	{"ratio-probe-fails", "the ratio probe was not answered by the served Instance (status 500"},
	// A word that comes to 1.3 tokens: the contexts are fewer words, the run passes (TestTheRatioSetsTheLongContextsWords).
	{"ratio-wide", ""},
	{"kv-unread", "the engine's logged KV was not read"},
	{"no-routing-block", "does not hold routing.instanceCap at the row's cap of 11"},
	{"two-instances", "does not ask for exactly 1 Instance"},
	{"hedged", "hedgeWidth is not 1"},
	{"over-allocated", "overAllocation is not 0"},
	{"managed-not-attached", "the managed ModelDeployment is not attached to the managed Provider by name"},
	{"wrong-deployment-id", "the served Instance's deployment ID is not the one the shipped example derives (965fa2409193)"},
	{"untagged", "a job tag was not written down before its submission"},
	{"wrong-partition", "a job ran on another partition than the row's"},
	{"default-lease", "the deployment's leaseLimit is not the row's shortest lease"},
	{"short-lease", "not longer than the lead an Instance needs (PartitionTooShort)"},
	{"duplicate-tag", "a tag names more than one job"},
	{"extra-allocation", "exactly one Allocation"},
}

// baselineInvalidCases are the dry faults that end INVALID, each with the reason's words: the reading measures nothing, and no check failed.
var baselineInvalidCases = []struct{ fault, why string }{
	{"cap-never-binds", "the cap never bound: LLMLab was asked for no request, so no spill was shown"},
	{"detach-503", "a request was answered 503 (emitter \"front\"): the detach phase's waiting requests must be served after waiting under queueTimeout, never refused"},
	{"detach-no-wait", "the front's own waiting count reached at most 0, want 2"},
	// The detach phase passes only if the front's own waiting count reaches 2 (#776's review, point 6; the science agent's ruling).
	{"waiting-never-two", "the front's own waiting count reached at most 1, want 2"},
	{"kv-below-the-cap", "the engine's logged KV is 100000 tokens, below the cap's need of 11 x 32768 = 360448"},
	{"llmlab-fails", "a request LLMLab answered failed (status 500)"},
	{"p0-llmlab-fails", "P0's request failed and LLMLab answered it (status 500)"},
	// An HTTP 200 that is no completed stream with tokens authorises no GPU job, with min_tokens left out (the dry fault sets the switch).
	{"p0-empty-200", "P0's reply was a 200 that is no completed stream with tokens"},
	{"p0-no-finish", "P0's reply was a 200 that is no completed stream with tokens"},
	{"p0-zero-tokens", "P0's reply was a 200 that is no completed stream with tokens"},
	{"p0-counts-null-no-events", "P0's reply was a 200 that is no completed stream with tokens"},
	{"p0-cut-200", "P0's request failed and LLMLab answered it (status 200)"},
	{"min-tokens-ignored", "LLMLab did not give exactly that: it does not honour min_tokens"},
	{"long-contexts-short", "outside the long phase's 24000 to 28672"},
	// The control and the long phase prove nothing when their sessions did not run together at the cap (#776's review, point 3).
	{"p1-serialized", "P1: the front's records show at most 1 requests in flight at once on the Instance, want the cap's 11"},
	{"p1-sessions-missing", "P1: only 10 of the 11 sessions had a request answered by the Instance"},
	{"long-serialized", "P1-long: the front's records show at most 1 requests in flight at once on the Instance, want the cap's 11"},
	// All the Instance's requests together reach the cap, but those whose prompt is in the long range never do: only the long range's own count catches it.
	{"long-staggered-short-later-turns", "P1-long: the front's records show at most 10 requests in flight at once whose prompt is in the long range of 24000 to 28672 tokens, want the cap's 11"},
	{"phases-do-not-fit", "did not fit before the Instance's drain start"},
	{"ratio-impossible", "no word count reaches 24000 to 28672 tokens"},
	// A request the front's own log reports resent after a failed dial of the Instance makes its phase INVALID, in every phase (#776's review, point 5, guard 2).
	{"dial-failure-p1", "P1: 1 request(s) of the phase were resent after a failed dial of the Instance, by the front's own log"},
	{"dial-failure-p1long", "P1-long: 1 request(s) of the phase were resent after a failed dial of the Instance"},
	{"dial-failure-p2", "P2: 1 request(s) of the phase were resent after a failed dial of the Instance"},
	{"dial-failure-p3", "P3: 1 request(s) of the phase were resent after a failed dial of the Instance"},
	// A log read with no line parsed as a slog JSON object (empty, or only lines that are none) rules nothing out: INVALID, with the count of 0 posted.
	{"front-log-empty", "the front's log held no parsed line, so a failed dial of the Instance could not be ruled out"},
	{"front-log-malformed-only", "the front's log held no parsed line, so a failed dial of the Instance could not be ruled out"},
	// A P2 LLMLab request with fewer than the cap in flight on the Instance at its arrival, or within one tick after it, is no cap spill (#776's review,
	// point 5): INVALID, and posted as such. 2 s, 50 ms (the reviewer's case) and 2 ms apart; a LLMLab request in P1 is still a FAIL (llmlab-in-the-control).
	{"llmlab-spill-below-the-cap", "LLMLab request(s) arrived while the front's records showed fewer than the cap's 11 requests in flight on the Instance, at the arrival or within 1ms after it: not a cap spill"},
	{"llmlab-spill-50ms-before-the-cap", "not a cap spill"},
	{"llmlab-spill-2ms-before-the-cap", "not a cap spill"},
	// A KV figure is posted as at the cap only from a read taken inside the cap's stretch (#776's review, point 7).
	{"long-no-read-in-window", "no distinct engine read was taken inside the stretch with the cap's 11 requests in flight"},
	{"ratio-unread", "the ratio probe's usage record holds no prompt tokens"},
}

func init() {
	faults := []string{}
	for _, c := range baselineCases {
		if c.fault != "" {
			faults = append(faults, c.fault)
		}
	}
	for _, c := range baselineInvalidCases {
		faults = append(faults, c.fault)
	}
	// Faults that end the scenario with an error and so have no row of the table (TestAFailedGateStopsTheRowBeforeAnyPhase).
	faults = append(faults, "gate-error")
	dryFaults["baseline"] = append(faults, "pend-past-bound")
	dryCases[BetaBaselineName] = baselineCases
}

// dryReadEpoch is the agent's clock for the dry engine's reads: its own, unrelated to the fake clock.
var dryReadEpoch = time.Date(2026, 11, 18, 3, 0, 0, 0, time.UTC)

const (
	dryServedUID      = "00000000-0000-4000-8000-0000000000a1"
	dryServedInstance = "default/dry-inst-1"
	dryServedProvider = "alpha-dry"
)

// Baseline is the baseline row over the fake cluster.
func (d *drySteps) Baseline(ctx context.Context, p Plan) (BaselineObs, error) {
	if d.fault("pend-past-bound") {
		return BaselineObs{}, d.pendPastBound(p)
	}
	cap := int(p.Row.InstanceCap)
	phases := d.cfg.Phases
	if len(phases) == 0 {
		phases = DefaultPhases(cap)
	}
	w := &dryBaseline{d: d, p: p, cap: cap}
	d.baseline = w
	o, err := RunBaseline(ctx, w, BaselineSettings{Phases: phases, Cap: cap, Row: p.Row.Name, OmitMinTokens: d.cfg.OmitMinTokens || strings.HasPrefix(d.cfg.DryFault, "p0-"), Poll: time.Millisecond, DetachEvery: time.Millisecond, WaitingPoll: time.Millisecond, Logf: p.Logf})
	if err != nil {
		return BaselineObs{}, err
	}
	o.Tags = append([]string(nil), d.tags...)
	return o, nil
}

// dryBaseline is the sequencer's world over the fake cluster.
type dryBaseline struct {
	d    *drySteps
	p    Plan
	cap  int
	n    int
	id   string
	mu   sync.Mutex
	rows []FrontRow
	// detached says DetachManaged ran; generated counts the phases started and reads the reads of LLMLab's count since the current one started; phaseLLM
	// is how many LLMLab requests the current phase made and cum how many the earlier ones made; running is the engine's running gauge in the phase; pre
	// counts the preemptions the fault adds.
	detached  bool
	generated int
	reads     int
	phaseLLM  int
	cum       float64
	running   float64
	kv        float64
	pre       float64
	samples   int
	phase     Phase
	gens      []*dryGenerator
	// calls are the world's calls in order, by name, for the tests of the sequencer's order. released says the job was released and its Allocation's directory
	// with it; readAfterRelease says something read that directory after.
	calls            []string
	configs          map[string][]byte
	preBumped        bool
	frontLog         string
	waitCalls        int
	phaseWaits       bool
	phaseCalls       int
	readSeq          int
	phaseT0          time.Time
	p1LongRowsRead   bool
	phaseSamples     int
	released         bool
	readAfterRelease bool
}

func (w *dryBaseline) call(name string) {
	w.mu.Lock()
	w.calls = append(w.calls, name)
	w.mu.Unlock()
}

func (w *dryBaseline) Now() time.Time { return w.d.now() }

func (w *dryBaseline) AttachManaged(context.Context) (ManagedMD, error) {
	w.call("AttachManaged")
	m := ManagedMD{Model: BaselineModel, Platform: "llmlab", Providers: []string{LLMLabProviderName}}
	switch {
	case w.d.fault("managed-not-attached"):
		m.Providers = []string{"another-provider"}
	}
	return m, nil
}

func (w *dryBaseline) row(id, provider, emitter string, status int, start time.Time, dur time.Duration, prompt, completion int64, first int64) FrontRow {
	r := FrontRow{RequestID: id, Provider: provider, ModelDeployment: "default/dry-md", Emitter: emitter, Status: status, Start: start, End: start.Add(dur),
		FirstTokenMs: &first, PromptTokens: &prompt, CompletionTokens: &completion, DurationMs: dur.Milliseconds()}
	if emitter == "instance" {
		r.Instance, r.InstanceUID = dryServedInstance, dryServedUID
	}
	if status == 200 {
		finish, events := "length", completion
		r.Finish, r.ContentEvents = &finish, &events
	}
	return r
}

func (w *dryBaseline) Probe(context.Context) (ProbeObs, error) {
	w.call("Probe")
	if w.d.fault("p0-not-sent") {
		return ProbeObs{}, nil
	}
	tokens := int64(P0Tokens)
	if w.d.fault("min-tokens-ignored") {
		tokens = 2
	}
	r := w.row("p0", LLMLabProviderName, "provider", 200, w.d.now(), 2*time.Second, 14, tokens, 300)
	switch {
	case w.d.fault("p0-answered-by-an-instance"):
		r.Emitter, r.Provider, r.Instance, r.InstanceUID = "instance", dryServedProvider, dryServedInstance, dryServedUID
	case w.d.fault("p0-front-fails"):
		r.Emitter, r.Provider, r.Status = "front", "", 503
	case w.d.fault("p0-llmlab-fails"):
		r.Status = 500
	case w.d.fault("p0-empty-200"):
		// LLMLab answers 200 and an empty stream: no tokens, no events, no finish reason. With min_tokens left out nothing else catches it.
		zero := int64(0)
		r.CompletionTokens, r.ContentEvents, r.Finish = &zero, &zero, nil
	case w.d.fault("p0-no-finish"):
		r.Finish = nil
	case w.d.fault("p0-cut-200"):
		r.CutBy = "upstream"
	case w.d.fault("p0-zero-tokens"):
		zero := int64(0)
		r.CompletionTokens = &zero
	case w.d.fault("p0-counts-null-events"):
		// A stream that asked for no usage: the counts are null and the front's own count of content events stands (usage's countsSource front_events).
		events := int64(5)
		r.CompletionTokens, r.ContentEvents = nil, &events
	case w.d.fault("p0-counts-null-no-events"):
		r.CompletionTokens, r.ContentEvents = nil, nil
	}
	return ProbeObs{Sent: true, Row: r}, nil
}

func (w *dryBaseline) StartServed(context.Context) (ServedReady, error) {
	w.call("StartServed")
	d, p := w.d, w.p
	tm := p.Row.Clock()
	tag := d.tag(p, "baseline")
	n, id := d.submit(tag, d.partitionFor(true), d.cfg.Row.QOS)
	w.n, w.id = n, id
	j := d.running(id)
	started := *j.StartTime
	d.extras(p, tag)
	d.tick(8 * time.Minute) // a warm start, inside the profile's 12 minutes
	ready := d.now()
	drain := started.Add(d.deployment().LeaseLimit - DrainWindow(tm.MaxRequestDuration, tm.DrainMargin, 0))
	if d.fault("phases-do-not-fit") {
		drain = ready.Add(16 * time.Minute) // P1 (10 minutes and the wait for the records) fits; P1-long (15 and the wait) cannot
	}
	one, zero := int32(1), int32(0)
	c := int32(w.cap)
	sr := ServedReady{ReadyAt: ready, DrainStart: drain, Deployment: d.deployment(),
		Served: ServedMD{Model: BaselineModel, Providers: []string{dryServedProvider}, Instances: 1, OverAllocation: &zero, HedgeWidth: &one, InstanceCap: &c, DeploymentID: baselineFactsOf(p.Row.Name).DeploymentID}}
	switch {
	case d.fault("no-routing-block"):
		sr.Served.InstanceCap = nil
	case d.fault("two-instances"):
		sr.Served.Instances = 2
	case d.fault("hedged"):
		two := int32(2)
		sr.Served.HedgeWidth = &two
	case d.fault("over-allocated"):
		sr.Served.OverAllocation = &one
	case d.fault("wrong-deployment-id"):
		sr.Served.DeploymentID = "000000000000"
	}
	return sr, nil
}

// EndServed releases the job, and with it the Allocation's directory: the engine's log is gone (internal/providerctl/delete.go:110), so a read of it after
// this fails and is marked.
func (w *dryBaseline) EndServed(context.Context) error {
	w.call("EndServed")
	w.d.tick(time.Minute)
	if w.n > 0 {
		w.d.end(w.n, w.id)
	}
	w.mu.Lock()
	w.released = true
	w.mu.Unlock()
	return nil
}

func (w *dryBaseline) LoggedKV(context.Context) (int64, bool) {
	w.call("LoggedKV")
	w.mu.Lock()
	released := w.released
	if released {
		w.readAfterRelease = true
	}
	w.mu.Unlock()
	if released {
		return 0, false
	}
	switch {
	case w.d.fault("kv-unread"):
		return 0, false
	case w.d.fault("kv-below-the-cap"):
		// Below the cap's need on every row: Row 1's figure of 100,000, or half the need where that is the smaller (the Athena twin's need is 98,304).
		return min(100000, int64(w.cap)*MaxModelLen/2), true
	}
	return baselineFactsOf(w.p.Row.Name).KV, true
}

func (w *dryBaseline) Gate(context.Context) (GateObs, error) {
	w.call("Gate")
	r := w.row("gate", dryServedProvider, "instance", 200, w.d.now(), time.Second, 9, 1, 40)
	switch {
	case w.d.fault("no-gate"):
		return GateObs{}, nil
	case w.d.fault("gate-error"):
		return GateObs{}, errors.New("the gate's request got no answer from the front")
	case w.d.fault("gate-503"):
		// The front's final answer after its wait: no served Instance routable.
		r.Emitter, r.Provider, r.Instance, r.InstanceUID, r.Status, r.Finish = "front", "", "", "", 503, nil
	case w.d.fault("gate-engine-500"):
		r.Status, r.Finish = 500, nil
	case w.d.fault("gate-cut-200"):
		r.CutBy = "tunnel"
	}
	return GateObs{Sent: true, Row: r}, nil
}

// LLMLabCount is the front's metric for the managed Provider: the earlier phases' requests, and the current phase's, all of them at each look, or, for
// the guard's fault, 100 at the first look and all of them at the second. The read that starts a phase is made before its generator exists, so it reads
// the earlier phase's end, which is the same number.
func (w *dryBaseline) LLMLabCount(context.Context) (float64, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.generated == 0 {
		return 0, !w.d.fault("metric-unread")
	}
	w.reads++
	done := float64(w.phaseLLM)
	if w.d.fault("llmlab-over-its-guard") && w.phase.Name == PhaseP2 && w.reads == 1 {
		done = 100
	}
	genDone := w.genDone()
	if w.d.fault("llmlab-over-final-count") && w.phase.Name == PhaseP2 && !genDone {
		done = min(done, 100) // every look of the guard saw at most 100: the rest came between its last look and the generator's exit
	}
	// The guard's scrape fails in P2: from its first look of the phase (metric-unread-midphase), or only the look that starts P2 (metric-unread-at-p2-start,
	// the first read after P1-long's front records were read, which a phase does last).
	if w.d.fault("metric-unread-midphase") && w.phase.Name == PhaseP2 && !genDone && w.reads >= 1 {
		return w.cum + done, false
	}
	if w.d.fault("metric-unread-at-p2-start") && w.phase.Name == PhaseP1Long && w.p1LongRowsRead {
		return 0, false // the look that starts P2, after P1-long's front records were read
	}
	if w.d.fault("metric-disagrees") && w.phase.Name == PhaseP2 {
		done++
	}
	// A phase's generator ends once the sequencer has looked at the count twice, unless a fault keeps it running.
	for _, g := range w.gens {
		g.looked(w.reads)
	}
	return w.cum + done, !w.d.fault("metric-unread")
}

// genDone says the current phase's generator has ended: the reads after it are the phase's final one and the next phase's first.
func (w *dryBaseline) genDone() bool {
	if len(w.gens) == 0 {
		return false
	}
	select {
	case <-w.gens[len(w.gens)-1].Done():
		return true
	default:
		return false
	}
}

// The fake front log's invented text: distinctive strings that must reach no post, no test log and no record (TestTheFrontLogIsPrivateAndOnlyItsCountIsPosted).
const (
	dryDialError   = "dial tcp 192.0.2.77:4242: connect: refused-by-INVENTED-9c1f"
	dryOtherTarget = "other-ns-4b2e/other-inst-71"
)

// dryLogLines are the lines the dry front writes to its log in a phase: one unrelated INFO line, and, in the faults, a line about a request of the phase
// (its first turn) in the form of internal/front/proxy.go:253's, each with the invented error text and the target in its field.
func (w *dryBaseline) dryLogLines(ph Phase) string {
	req := ph.Name + "-s000-t01"
	line := func(level, msg, target string) string {
		return fmt.Sprintf(`{"time":"2026-11-18T12:00:00Z","level":%q,"msg":%q,"request":%q,"target":%q,"error":%q}`+"\n", level, msg, req, target, dryDialError)
	}
	switch {
	case w.d.fault("front-log-empty"):
		return "" // the wrong file, or one the front never wrote to
	case w.d.fault("front-log-malformed-only"):
		// Lines that are no slog JSON object: one cut short, one that is no JSON, an array, and an object with no message.
		return `{"time":"2026-11-18T12:00:00Z","level":"INFO","msg":"serving a request","request":"unrelated"` + "\nnot json at all\n[1,2]\n" + `{"level":"WARN"}` + "\n"
	}
	out := `{"time":"2026-11-18T12:00:00Z","level":"INFO","msg":"serving a request","request":"unrelated","target":"` + dryServedInstance + `"}` + "\n"
	for fault, phase := range map[string]string{"dial-failure-p1": PhaseP1, "dial-failure-p1long": PhaseP1Long, "dial-failure-p2": PhaseP2, "dial-failure-p3": PhaseP3} {
		if w.d.fault(fault) && ph.Name == phase {
			out += line("WARN", dialFailureMsg, dryServedInstance)
		}
	}
	if ph.Name == PhaseP2 {
		switch {
		case w.d.fault("dial-failure-other-target"):
			out += line("WARN", dialFailureMsg, dryOtherTarget) // another target's failed dial: not ours
		case w.d.fault("dial-failure-wrong-level"):
			out += line("INFO", dialFailureMsg, dryServedInstance)
		case w.d.fault("dial-failure-in-error-field"):
			// The message of another line, whose error field quotes the text: never a substring of the whole line.
			out += fmt.Sprintf(`{"time":"2026-11-18T12:00:00Z","level":"WARN","msg":"another message","request":%q,"target":%q,"error":%q}`+"\n", req, dryServedInstance, dialFailureMsg)
		case w.d.fault("dial-failure-malformed"):
			// A line cut short, and one that is no JSON object: skipped, never a failure found and never a pass.
			out += `{"time":"2026-11-18T12:00:00Z","level":"WARN","msg":"` + dialFailureMsg + `","request":"` + req + `","target":"` + dryServedInstance + "\n" + "not json at all\n"
		}
	}
	return out
}

// DialFailures reads the dry front's log.
func (w *dryBaseline) DialFailures(context.Context) ([]DialFailure, int, bool) {
	if w.d.fault("front-log-unread") {
		return nil, 0, false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	fails, parsed := ParseDialFailures([]byte(w.frontLog), dryServedInstance)
	return fails, parsed, true
}

// FrontWaiting is the front's own count of requests in its FIFO: while the detach phase's generator runs, none, then two for two reads, then one, then none; at
// the end of the phase none. The faults change that pattern; a phase in which nobody waited reads none throughout.
func (w *dryBaseline) FrontWaiting(context.Context) (int, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.d.fault("waiting-unread") {
		return 0, false
	}
	w.waitCalls++
	if !w.phaseWaits {
		return 0, true
	}
	i := w.waitCalls
	switch {
	case w.d.fault("waiting-not-returning"):
		if i >= 2 {
			return 2, true // the FIFO still holds two requests when the phase ends
		}
		return 0, true
	case w.d.fault("waiting-never-two"):
		return []int{0, 1, 1, 0}[min(i-1, 3)], true
	}
	return []int{0, 2, 2, 1, 0}[min(i-1, 4)], true
}

// ServedDeploymentsOfModel is 1 in the row's stack, 2 in the fault where another served deployment of the model sits beside the row's.
func (w *dryBaseline) ServedDeploymentsOfModel(context.Context) (int, bool) {
	if w.d.fault("two-served-mds") {
		return 2, true
	}
	return 1, true
}

func (w *dryBaseline) ServedEligible(context.Context) bool {
	return !(w.d.fault("instance-ineligible") && w.phase.Name == PhaseP1Long)
}

// inPhase says the named phase's generator is running: the sample before a phase starts, and the one after it ends, belong to the neighbours.
func (w *dryBaseline) inPhase(name string) bool {
	if w.phase.Name != name || len(w.gens) == 0 {
		return false
	}
	select {
	case <-w.gens[len(w.gens)-1].Done():
		return false
	default:
		return true
	}
}

func (w *dryBaseline) EngineSample(context.Context) (EngineSample, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.d.fault("engine-unread") {
		return EngineSample{}, false
	}
	w.d.tick(time.Second) // each look is a second of the fake clock, so a generator that never ends reaches the phase's bound
	w.phaseCalls++
	// Every scrape is a new read of the engine, 10 s after the last, except in the faults whose gauges repeat a read: the controller's gauges do between reads,
	// and only the agent's read time tells two scrapes of one read apart (#776's review, point 7).
	newRead := true
	switch {
	case w.d.fault("engine-cached-idle") && w.phase.Name == PhaseP2:
		newRead = false // idle data, repeated: it must not satisfy the minimum number of reads
	case w.d.fault("engine-waiting-cached") && w.phase.Name == PhaseP2:
		newRead = w.phaseCalls%4 == 1 // one read every four scrapes
	}
	if newRead {
		w.readSeq++
	}
	// A scrape while the phase's generator runs is first seen inside the phase's stretch, 20 s apart; the one before a phase starts and the one after its generator
	// ended are first seen outside it, at the clock's own time.
	at := w.d.now()
	if !w.genDone() {
		at = w.phaseT0.Add(time.Duration(w.phaseCalls) * 20 * time.Second)
	}
	s := EngineSample{At: at, ReadAt: dryReadEpoch.Add(time.Duration(w.readSeq) * EngineProbeInterval), Running: w.running, KVUsage: w.kv, Preemptions: w.pre}
	if w.d.fault("long-no-read-in-window") && w.phase.Name == PhaseP1Long {
		s.At = w.phaseT0.Add(time.Hour) // every read first seen long after the cap's stretch
	}
	switch {
	case w.d.fault("engine-waiting-cached") && w.phase.Name == PhaseP2:
		// The single read with requests waiting is scraped four times.
		if (w.phaseCalls-1)/4 == 1 {
			s.Waiting = 1
		}
	case w.d.fault("engine-over-the-cap") && w.phase.Name == PhaseP2:
		s.Running = float64(w.cap + 1)
	case w.waitingFault() && w.inPhase(PhaseP2):
		// P2 is looked at five times (dryBaseline.lookedAt), and the samples numbered in waitingPatterns have a request waiting.
		w.phaseSamples++
		if waitingPatterns[w.faultName()][w.phaseSamples] {
			s.Waiting = 1
		}
	case w.d.fault("engine-preempts") && w.phase.Name == PhaseP2:
		w.pre += 3
		s.Preemptions = w.pre
	case w.d.fault("long-engine-readings") && w.inPhase(PhaseP1Long):
		// Eleven long prompts queue in the engine's own prefill, and the engine preempts three requests once: readings, not failures.
		s.Waiting = 2
		if !w.preBumped {
			w.pre, w.preBumped = w.pre+3, true
		}
		s.Preemptions = w.pre
	}
	w.samples++
	return s, true
}

// waitingPatterns are the in-phase samples of P2 that show a request waiting, by fault: the whole phase, exactly the sustained run of 3, and the blips and runs
// short of it that are readings (EngineWaitingSustain).
var waitingPatterns = map[string]map[int]bool{
	"engine-waiting":        {1: true, 2: true, 3: true, 4: true, 5: true},
	"engine-waiting-thrice": {2: true, 3: true, 4: true},
	"engine-waiting-twice":  {2: true, 3: true},
	"engine-waiting-blip":   {2: true},
	"engine-waiting-split":  {1: true, 2: true, 4: true, 5: true},
}

// ratio is the tokens a word comes to on the dry engine: 1.05, wider and absurd in the faults that test the long shape.
func (w *dryBaseline) ratio() float64 {
	switch {
	case w.d.fault("ratio-wide"):
		return 1.3
	case w.d.fault("ratio-impossible"):
		return 10000
	}
	return 1.05
}

func (w *dryBaseline) faultName() string { return w.d.cfg.DryFault }
func (w *dryBaseline) waitingFault() bool {
	_, ok := waitingPatterns[w.d.cfg.DryFault]
	return ok
}

// dryGenerator ends when the world says its phase has been looked at enough, or when it is stopped.
type dryGenerator struct {
	done   chan struct{}
	once   sync.Once
	gen    []GenRow
	needed int
	never  bool
}

func (g *dryGenerator) Done() <-chan struct{} { return g.done }
func (g *dryGenerator) Stop()                 { g.once.Do(func() { close(g.done) }) }
func (g *dryGenerator) Result() ([]GenRow, error) {
	return g.gen, nil
}
func (g *dryGenerator) looked(n int) {
	if !g.never && n >= g.needed {
		g.Stop()
	}
}

// StartGenerator builds the phase the front's rule would give: the first cap sessions on the Instance, the rest on LLMLab (or waiting, when LLMLab is
// detached), each of the phase's turns in turn. The generator's config is made too, as the live world makes it, and its record kept.
func (w *dryBaseline) StartGenerator(_ context.Context, ph Phase) (Generator, error) {
	cfgJSON, err := StudyConfig(ph, StudyRun{BaseURL: "http://127.0.0.1:1/v1", RunID: PhaseRunID("0123456789", ph.Name), ServedProvider: dryServedProvider, RequestTimeout: 5 * time.Minute})
	if err != nil {
		return nil, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.configs == nil {
		w.configs = map[string][]byte{}
	}
	w.configs[ph.Name] = cfgJSON // the generator's config of the phase, as the live world writes it
	w.calls = append(w.calls, "StartGenerator "+ph.Name)
	if w.generated > 0 {
		w.cum += float64(w.phaseLLM)
	}
	w.generated++
	w.phase, w.reads = ph, 0
	w.phaseT0, w.phaseCalls, w.waitCalls = w.d.now(), 0, 0
	w.frontLog += w.dryLogLines(ph)
	gen, rows, llm, instance, length := w.build(ph)
	w.rows = append(w.rows, rows...)
	w.phaseLLM, w.running = llm, float64(min(instance, w.cap))
	w.kv = 0.2
	if ph.Kind == KindLong {
		w.kv = 0.74
	}
	g := &dryGenerator{done: make(chan struct{}), gen: gen, needed: 2}
	w.phaseSamples = 0
	if w.waitingFault() && ph.Name == PhaseP2 {
		g.needed = 6 // five looks of the engine in the phase, to show a run of consecutive samples
	}
	if ph.Kind == KindDetach {
		g.needed = 9 // the front's waiting count is read at every look of the phase: seven reads show it rise, hold and fall
	}
	if w.d.fault("engine-waiting-cached") && ph.Name == PhaseP2 {
		g.needed = 10 // twelve scrapes: three reads, one of them with requests waiting
	}
	if w.d.fault("llmlab-over-its-guard") && ph.Name == PhaseP2 || w.d.fault("instance-ineligible") && ph.Name == PhaseP1Long {
		g.never = true // the phase ends only by the sequencer's stop
	}
	w.gens = append(w.gens, g)
	w.d.tick(length)
	return g, nil
}

// build is a phase's sessions, rows and generator records. The first turns of all sessions arrive together; a turn takes two seconds, with the phase's
// think time between turns. llm is the LLMLab rows made, instance the sessions on the Instance.
func (w *dryBaseline) build(ph Phase) (gen []GenRow, rows []FrontRow, llm, instance int, length time.Duration) {
	t0 := w.d.now()
	cap := w.cap
	instanceSessions, llmSessions, waiting := ph.Sessions, 0, 0
	switch ph.Kind {
	case KindFull:
		instanceSessions, llmSessions = min(ph.Sessions, cap), max(ph.Sessions-cap, 0)
		if w.d.fault("cap-never-binds") {
			instanceSessions, llmSessions = ph.Sessions, 0
		}
		if w.d.fault("llmlab-one-too-many-first-turns") {
			instanceSessions, llmSessions = ph.Sessions-1, 3
		}
		if w.d.fault("llmlab-one-spiller-twice") {
			// One session spills, and its first turn is sent twice: two first-turn requests, one distinct session.
			instanceSessions, llmSessions = cap, 1
		}
	case KindDetach:
		instanceSessions, waiting = ph.Sessions, max(ph.Sessions-cap, 0)
		if !w.detached {
			// The managed ModelDeployment was never deleted: LLMLab answers the overflow, as it does in P2.
			instanceSessions, llmSessions, waiting = min(ph.Sessions, cap), max(ph.Sessions-cap, 0), 0
		}
		if w.d.fault("detach-no-wait") {
			waiting = 0
		}
		if w.d.fault("detach-session-missing") {
			instanceSessions-- // one session never ran
			waiting = max(waiting-1, 0)
		}
		w.phaseWaits = waiting > 0
	}
	if w.d.fault("llmlab-in-the-control") && ph.Name == PhaseP1 {
		instanceSessions, llmSessions = ph.Sessions-1, 1
	}
	if w.d.fault("llmlab-after-the-detach") && ph.Kind == KindDetach {
		instanceSessions, llmSessions = ph.Sessions-3, 3
	}
	if (w.d.fault("llmlab-over-its-guard") || w.d.fault("llmlab-over-final-count")) && ph.Name == PhaseP2 {
		llmSessions = 150 // one request each: the guard is the metric's, past 144
	}
	// A turn takes two seconds, and in the long phase forty (eleven prompts of 26k tokens prefill together, so the cap's stretch is long enough to hold a read
	// taken after the probe interval); the next turn starts after the think time.
	turnDur := 2 * time.Second
	if ph.Kind == KindLong {
		turnDur = 40 * time.Second
	}
	step := turnDur + ph.Think
	delay := time.Duration(0)
	// late is when the sessions beyond the cap start in the faults where the cap is never reached: after the others have ended.
	late := time.Duration(ph.Turns)*step + time.Second
	neverReached := (w.d.fault("cap-never-binds") && ph.Kind == KindFull) || (w.d.fault("detach-no-wait") && ph.Kind == KindDetach)
	// The Instance's sessions are stamped after LLMLab's: at LLMLab's arrival the Instance held none. By how much decides the rule (ArrivalTick): 2 s and 50 ms
	// (the reviewer's case) are no cap spill, and one tick is a healthy burst.
	if ph.Kind == KindFull {
		switch {
		case w.d.fault("llmlab-spill-below-the-cap"):
			delay = 2 * time.Second
		case w.d.fault("llmlab-spill-50ms-before-the-cap"):
			delay = 50 * time.Millisecond
		case w.d.fault("llmlab-spill-1ms-before-the-cap"):
			delay = time.Millisecond
		case w.d.fault("llmlab-spill-2ms-before-the-cap"):
			delay = 2 * time.Millisecond
		}
	}
	// serialized runs the phase's sessions one after another, each of two turns, so that at most one request is ever in flight: the control that proves nothing.
	serialized := (w.d.fault("p1-serialized") && ph.Name == PhaseP1) || (w.d.fault("long-serialized") && ph.Name == PhaseP1Long)
	if w.d.fault("p1-sessions-missing") && ph.Name == PhaseP1 {
		instanceSessions-- // one session never ran
	}
	idSuffix := ""
	add := func(session string, turn int, provider, emitter string, status int, start time.Time, dur time.Duration, prompt, first int64) {
		id := fmt.Sprintf("%s-%s-t%02d%s", ph.Name, session, turn, idSuffix)
		r := w.row(id, provider, emitter, status, start, dur, prompt, int64(ph.Output), first)
		g := GenRow{Session: session, Turn: turn, RequestID: id, Emitter: r.Emitter, Provider: r.Provider, Instance: r.Instance}
		rows, gen = append(rows, r), append(gen, g)
		if end := r.End.Sub(t0); end > length {
			length = end
		}
	}
	// The prompt of a turn, in tokens: the words of the first turn and of each later one at the engine's ratio of tokens to a word, and the template's 7.
	promptOf := func(turn int) int64 {
		if ph.Kind == KindLong && w.d.fault("long-contexts-short") {
			return 5000
		}
		if turn > 1 && w.d.fault("long-staggered-short-later-turns") && ph.Kind == KindLong {
			return 5000 // the later turns are short: out of the long range, yet in flight beside the first turns
		}
		return int64(float64(ph.Context)*w.ratio()) + 7 + int64(float64(turn-1)*float64(ph.Turn+ph.Output)*w.ratio())
	}
	for i := 0; i < instanceSessions+llmSessions; i++ {
		session := fmt.Sprintf("s%03d", i)
		onLLMLab := i >= instanceSessions
		isWaiting := !onLLMLab && waiting > 0 && i >= instanceSessions-waiting
		turns := ph.Turns
		if onLLMLab && (w.d.fault("llmlab-over-its-guard") || w.d.fault("llmlab-over-final-count")) {
			turns = 1
		}
		if serialized {
			turns = 2
			if ph.Kind == KindLong {
				turns = 1 // forty seconds a request: one turn each keeps the phase inside its bound
			}
		}
		// A turn takes two seconds, on the Instance and on LLMLab, and the next starts after the think time.
		for t := 1; t <= turns; t++ {
			start := t0.Add(time.Duration(t-1) * step)
			if w.d.fault("long-staggered-short-later-turns") && ph.Kind == KindLong {
				start = start.Add(time.Duration(i) * 4 * time.Second) // the first turns, each forty seconds long, overlap ten at a time at most
			}
			if serialized {
				start = start.Add(time.Duration(i*turns) * step) // each session starts after the one before it has ended
			}
			if neverReached && i >= cap {
				start = start.Add(late)
			}
			dur, status, emitter, provider, first := turnDur, 200, "instance", dryServedProvider, int64(50)
			switch {
			case onLLMLab:
				emitter, provider, first = "provider", LLMLabProviderName, 300
			default:
				if delay > 0 {
					start = start.Add(delay)
				}
				if isWaiting && t == 1 {
					// Arrived with the cap's sessions in the Instance, served after one turn's time: a wait in the front's FIFO.
					dur, first = 2*dur+500*time.Millisecond, 2500
				}
				if isWaiting && t > 1 {
					start = start.Add(2500 * time.Millisecond)
				}
			}
			if w.d.fault("session-moves") && ph.Name == PhaseP1 && i == 0 && t == 3 {
				emitter, provider, first = "provider", LLMLabProviderName, 300
			}
			if w.d.fault("front-503-in-the-control") && ph.Name == PhaseP1 && i == 1 && t == 2 {
				emitter, provider, status = "front", "", 503
			}
			if w.d.fault("instance-fails") && ph.Name == PhaseP1 && i == 1 && t == 2 {
				status = 500
			}
			if w.d.fault("llmlab-fails") && ph.Kind == KindFull && onLLMLab && i == instanceSessions && t == 5 {
				status = 500
			}
			if w.d.fault("detach-503") && ph.Kind == KindDetach && isWaiting && t == 1 {
				emitter, provider, status = "front", "", 503
			}
			add(session, t, provider, emitter, status, start, dur, promptOf(t), first)
			if status != 200 {
				break // a failed request ends its session
			}
			if w.d.fault("over-the-cap-in-flight") && ph.Name == PhaseP1 && i == 0 && t == 1 {
				// a request the generator sent that stays open past its session's next turn: more than the cap in flight at once
				extra := w.row(ph.Name+"-extra", dryServedProvider, "instance", 200, start, 10*time.Second, 9, 1, 40)
				rows = append(rows, extra)
				gen = append(gen, GenRow{Session: "extra", Turn: 1, RequestID: extra.RequestID, Emitter: "instance", Provider: dryServedProvider, Instance: dryServedInstance})
			}
		}
	}
	if ph.Kind == KindFull && (w.d.fault("llmlab-first-turn-twice") || w.d.fault("llmlab-one-spiller-twice")) {
		// The first LLMLab session's first turn once more, as a request of its own: the same session and turn, sent twice.
		idSuffix = "-again"
		add(fmt.Sprintf("s%03d", instanceSessions), 1, LLMLabProviderName, "provider", 200, t0, 2*time.Second, promptOf(1), 300)
	}
	llm = 0
	for _, r := range rows {
		if r.Emitter == "provider" {
			llm++
		}
	}
	if w.d.fault("generator-names-another-route") && len(gen) > 0 {
		gen[0].Emitter = "provider"
	}
	if w.d.fault("no-answer") && len(gen) > 0 {
		gen = append(gen, GenRow{Session: "lost", Turn: 1})
	}
	if w.d.fault("no-record") && len(gen) > 0 {
		gen = append(gen, GenRow{Session: "lost", Turn: 1, RequestID: "never-recorded"})
	}
	if w.d.fault("unlisted-llmlab-request") && ph.Kind == KindControl {
		rows = append(rows, w.row("unlisted", LLMLabProviderName, "provider", 200, t0.Add(time.Second), 2*time.Second, 9, 8, 300))
	}
	instance = instanceSessions
	if ph.Kind == KindProbe && len(rows) == 1 {
		switch {
		case w.d.fault("ratio-probe-fails"):
			rows[0].Status = 500
		case w.d.fault("ratio-unread"):
			rows[0].PromptTokens = nil
		}
	}
	return gen, rows, llm, instance, length + time.Second
}

func (w *dryBaseline) FrontRows(_ context.Context, since time.Time, _ []string) ([]FrontRow, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.phase.Name == PhaseP1Long {
		w.p1LongRowsRead = true
	}
	var out []FrontRow
	for _, r := range w.rows {
		if !r.Start.Before(since) {
			out = append(out, r)
		}
	}
	return out, nil
}

func (w *dryBaseline) DetachManaged(context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls = append(w.calls, "DetachManaged")
	if w.d.fault("detach-skipped") {
		return nil // the managed ModelDeployment stays, and LLMLab answers the overflow
	}
	w.detached = true
	return nil
}

// ManagedDetached is the wait for the front to drop the target: each look is half a minute of the fake clock, so a front that never does reaches the
// sequencer's bound.
func (w *dryBaseline) ManagedDetached(context.Context) bool {
	w.d.tick(30 * time.Second)
	return w.detached && !w.d.fault("detach-not-seen")
}
