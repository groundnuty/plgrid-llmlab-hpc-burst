package alpha

// The baseline row's judge (baseline.go): the verdicts, from the front's usage records and the engine's gauges. It is pure: an observation in, the failed
// checks and the reasons the reading is INVALID out.
//
// Where every number comes from. The front's usage records carry no session and no turn (internal/front/usage/usage.go:35-60), so the generator's own records
// are used for ONE thing, the join of a request ID to the session and turn it was, and for nothing else: the route of a request, its status, its times and
// its tokens are always the front's record's, and what the generator's headers said of the route is only compared with it (a disagreement is a failed check).
// The engine's figures are the controller's gauges for the served Instance, and LLMLab's count is the front's own per-provider request metric.
//
// FAIL means the property the row tests was absent: LLMLab over its guard, a failed request the front or an Instance answered, a session that moved in
// the control, the engine over its cap. INVALID means the run measured nothing it can be judged by: a failed request LLMLab answered, a cap that never
// bound (a run where the cap never binds proves nothing, HBEP-5's Test plan), a 503 in the detach phase, an engine whose KV is below the cap's need. A failure wins over
// INVALID, as on the bulk row (run.go, ErrInvalid), and nothing here is a NOT RUN: the job started and ran.

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Route is where a request was answered, in the post's fixed words.
type Route string

// The routes of Row 1.
const (
	RouteServed Route = "the served Instance (Sano)"
	RouteLLMLab Route = "LLMLab (managed)"
	RouteFront  Route = "the front itself"
	RouteOther  Route = "another route"
)

// routeOfRow is the route the FRONT's usage record names: emitter provider and the managed Provider is LLMLab, emitter instance is an Instance, emitter
// front is an answer the front raised. managed is the managed Provider's name.
func routeOfRow(r FrontRow, managed string) Route {
	switch {
	case r.Emitter == "provider" && r.Provider == managed:
		return RouteLLMLab
	case r.Emitter == "instance":
		return RouteServed
	case r.Emitter == "front":
		return RouteFront
	}
	return RouteOther
}

// joinedRow is a request: the generator's record of which session and turn it was, and the front's usage record of what happened to it.
type joinedRow struct {
	Gen   GenRow
	Front FrontRow
}

// route is the route the front's record names. It is the one place a route is read, so that no count can come from the generator's headers.
func (j joinedRow) route(managed string) Route { return routeOfRow(j.Front, managed) }

// failed says the request did not end well: a status other than 200, or a cut.
func (j joinedRow) failed() bool { return j.Front.Status != 200 || j.Front.CutBy != "" }

// joinPhase joins the generator's records to the front's usage records by request ID. It returns the joined requests, how many generator records got no
// answer from the front (no request ID), the request IDs the front has no record of, and the front's records in the phase's window that the generator
// did not send.
func joinPhase(po PhaseObs) (joined []joinedRow, noAnswer int, noRecord []string, unlisted []FrontRow) {
	byID := map[string]FrontRow{}
	for _, r := range po.Rows {
		byID[r.RequestID] = r
	}
	named := map[string]bool{}
	for _, g := range po.Gen {
		if g.RequestID == "" {
			noAnswer++
			continue
		}
		named[g.RequestID] = true
		f, ok := byID[g.RequestID]
		if !ok {
			noRecord = append(noRecord, g.RequestID)
			continue
		}
		joined = append(joined, joinedRow{Gen: g, Front: f})
	}
	for _, r := range po.Rows {
		if !named[r.RequestID] {
			unlisted = append(unlisted, r)
		}
	}
	return joined, noAnswer, noRecord, unlisted
}

// inFlightStats reads a set of requests' intervals, [start, end) from arrival to the record's end. peak is the most at once; over is the longest stretch
// and top the highest count above limit (limit < 0 skips the reading).
func inFlightStats(rows []FrontRow, limit int) (peak int, longestOver time.Duration, top int) {
	type ev struct {
		at    time.Time
		delta int
	}
	var evs []ev
	for _, r := range rows {
		evs = append(evs, ev{r.Start, +1}, ev{r.End, -1})
	}
	// At one instant the ends come first: an interval is [start, end).
	sort.SliceStable(evs, func(i, j int) bool {
		if !evs[i].at.Equal(evs[j].at) {
			return evs[i].at.Before(evs[j].at)
		}
		return evs[i].delta < evs[j].delta
	})
	n := 0
	var overFrom time.Time
	for _, e := range evs {
		was := n
		n += e.delta
		peak = max(peak, n)
		if limit < 0 {
			continue
		}
		if was <= limit && n > limit {
			overFrom = e.at
		}
		if n > limit {
			top = max(top, n)
		}
		if was > limit && n <= limit {
			longestOver = max(longestOver, e.at.Sub(overFrom))
		}
	}
	return peak, longestOver, top
}

// countAt is how many of the rows are in flight at t: start at or before t, end after it.
func countAt(rows []FrontRow, t time.Time) int {
	n := 0
	for _, r := range rows {
		if !r.Start.After(t) && r.End.After(t) {
			n++
		}
	}
	return n
}

// heldItsCapAt says the Instance held its cap at an LLMLab request's arrival: with ArrivalTolerance each way (milliseconds in the records, and the
// release that follows the relay), its count reached the cap at the arrival or at an instant within the tolerance of it. That is how this row reads the
// tolerance of "one request within 100 ms": a count one short of the cap at the arrival is accepted only as the edge of an instant at the cap within 100 ms,
// and a count that stays below the cap for longer is a spill that was not the cap's.
func heldItsCapAt(served []FrontRow, arrival time.Time, cap int) bool {
	cands := []time.Time{arrival, arrival.Add(-ArrivalTolerance), arrival.Add(ArrivalTolerance)}
	lo, hi := arrival.Add(-ArrivalTolerance), arrival.Add(ArrivalTolerance)
	for _, r := range served {
		for _, t := range []time.Time{r.Start, r.End.Add(-time.Nanosecond)} {
			if !t.Before(lo) && !t.After(hi) {
				cands = append(cands, t)
			}
		}
	}
	for _, t := range cands {
		if countAt(served, t) >= cap {
			return true
		}
	}
	return false
}

// nearestRank is the nearest-rank percentile p (0 to 1) of v: zero for none. It sorts a copy.
func nearestRank(v []float64, p float64) float64 {
	if len(v) == 0 {
		return 0
	}
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	rank := int(p*float64(len(s)) + 0.999999999)
	rank = min(max(rank, 1), len(s))
	return s[rank-1]
}

// RouteReading is the post's figures for one route in one phase, all from the front's usage records.
type RouteReading struct {
	Route        Route   `json:"route"`
	Requests     int     `json:"requests"`
	Sessions     int     `json:"sessions"`
	MostInFlight int     `json:"mostInFlight"`
	SpillsIn     int     `json:"spillsIn"`
	SpillsOut    int     `json:"spillsOut"`
	Errors       int     `json:"errors"`
	TTFTp50      float64 `json:"ttftP50Ms"`
	TTFTp95      float64 `json:"ttftP95Ms"`
	// SpeedP50 and SpeedMin are tokens a second per session, over the tokens after each reply's first token and the time after it.
	SpeedP50 float64 `json:"speedP50"`
	SpeedMin float64 `json:"speedMin"`
	// PromptTokens and CompletionTokens are the sums of the records' counts.
	PromptTokens     int64 `json:"promptTokens"`
	CompletionTokens int64 `json:"completionTokens"`
}

// ReadRoutes is the per-route reading of a phase's joined requests. A session's home is the served Instance: Row 1 has one, so a request answered
// anywhere else is a spill out of it, and into where it was answered.
func ReadRoutes(joined []joinedRow, managed string) []RouteReading {
	by := map[Route][]joinedRow{}
	for _, j := range joined {
		r := j.route(managed)
		by[r] = append(by[r], j)
	}
	var out []RouteReading
	for _, route := range []Route{RouteServed, RouteLLMLab, RouteFront, RouteOther} {
		js := by[route]
		if len(js) == 0 && route != RouteServed && route != RouteLLMLab {
			continue
		}
		rr := RouteReading{Route: route, Requests: len(js)}
		sessions := map[string]bool{}
		var rows []FrontRow
		var ttft []float64
		type acc struct{ tokens, ms float64 }
		perSession := map[string]*acc{}
		for _, j := range js {
			sessions[j.Gen.Session] = true
			rows = append(rows, j.Front)
			if j.failed() {
				rr.Errors++
			}
			if f := j.Front.FirstTokenMs; f != nil {
				ttft = append(ttft, float64(*f))
			}
			if j.Front.PromptTokens != nil {
				rr.PromptTokens += *j.Front.PromptTokens
			}
			if j.Front.CompletionTokens != nil {
				rr.CompletionTokens += *j.Front.CompletionTokens
			}
			if c, f := j.Front.CompletionTokens, j.Front.FirstTokenMs; c != nil && f != nil && *c > 1 && j.Front.DurationMs > *f {
				a := perSession[j.Gen.Session]
				if a == nil {
					a = &acc{}
					perSession[j.Gen.Session] = a
				}
				a.tokens += float64(*c - 1)
				a.ms += float64(j.Front.DurationMs - *f)
			}
		}
		// Every request LLMLab answers is a spill (HBEP-5): out of the home Instance, into LLMLab. "First route = home" is wrong, since a first turn may
		// already have spilled (a first turn may already have spilled), so the spill is read from where the request was answered, not from the session's first request.
		switch route {
		case RouteLLMLab:
			rr.SpillsIn = len(js)
		case RouteServed:
			rr.SpillsOut = len(by[RouteLLMLab])
		}
		rr.Sessions = len(sessions)
		rr.MostInFlight, _, _ = inFlightStats(rows, -1)
		rr.TTFTp50, rr.TTFTp95 = nearestRank(ttft, 0.5), nearestRank(ttft, 0.95)
		var speeds []float64
		for _, a := range perSession {
			speeds = append(speeds, a.tokens/(a.ms/1000))
		}
		if len(speeds) > 0 {
			rr.SpeedP50 = nearestRank(speeds, 0.5)
			rr.SpeedMin = nearestRank(speeds, 0)
		}
		out = append(out, rr)
	}
	return out
}

// rowsOn is the front's records of the joined requests that were answered on a route.
func rowsOn(joined []joinedRow, managed string, route Route) []FrontRow {
	var out []FrontRow
	for _, j := range joined {
		if j.route(managed) == route {
			out = append(out, j.Front)
		}
	}
	return out
}

// firstTurnRows are the front's records of the joined requests that were a session's first turn, answered on a route.
func firstTurnRows(joined []joinedRow, managed string, route Route) []FrontRow {
	var out []FrontRow
	for _, j := range joined {
		if j.Gen.Turn == 1 && j.route(managed) == route {
			out = append(out, j.Front)
		}
	}
	return out
}

// judgeBaseline reads a run of Row 1. fail are the failed checks, and invalid the reasons the reading measures nothing. The caller gives the
// failures precedence.
func judgeBaseline(o BaselineObs) (fail, invalid []string) {
	f := func(s string, a ...any) { fail = append(fail, fmt.Sprintf(s, a...)) }
	inv := func(s string, a ...any) { invalid = append(invalid, fmt.Sprintf(s, a...)) }

	// P0, before any job: the managed ModelDeployment attached by name, the plain Hub ID on it, and one request at the plain model answered by LLMLab.
	if o.Managed.Model != BaselineModel {
		f("a ModelDeployment's spec.model is not the plain Hub ID, so one name does not reach every route")
	}
	if o.LLMLabProvider == "" || !slices.Equal(o.Managed.Providers, []string{o.LLMLabProvider}) {
		f("the managed ModelDeployment is not attached to the managed Provider by name")
	}
	pf, pi := p0Problems(o)
	fail, invalid = append(fail, pf...), append(invalid, pi...)
	if !o.ServedCreated {
		if len(pf)+len(pi) == 0 {
			f("the run ended before the served ModelDeployment was created, with P0 answered")
		}
		return dedupe(fail), dedupe(invalid)
	}

	// The live world: the served ModelDeployment's routing block kept with its cap, one Instance, no extra Allocation, no hedge; the managed one attached
	// by name; the plain Hub ID on both.
	if o.Served.InstanceCap == nil || int(*o.Served.InstanceCap) != o.Cap {
		f("the served ModelDeployment does not hold routing.instanceCap at the row's cap of %d, so the front's cap is not the one the row was fixed on", o.Cap)
	}
	if o.Served.Instances != 1 {
		f("the served ModelDeployment does not ask for exactly 1 Instance")
	}
	if o.Served.OverAllocation == nil || *o.Served.OverAllocation != 0 {
		f("the served ModelDeployment's overAllocation is not 0")
	}
	if o.Served.HedgeWidth == nil || *o.Served.HedgeWidth != 1 {
		f("the served ModelDeployment's hedgeWidth is not 1")
	}
	if o.Served.Model != BaselineModel {
		f("a ModelDeployment's spec.model is not the plain Hub ID, so one name does not reach every route")
	}
	if o.K != LLMLabK {
		f("the row's k is not %d", LLMLabK)
	}
	if o.Served.DeploymentID != BaselineDeploymentID {
		f("the served Instance's deployment ID is not the one the shipped example derives (%s), or was not read", BaselineDeploymentID)
	}

	// The logged KV is a CHECK, never an input of the cap. Below the cap's need, nothing after it was run.
	switch {
	case !o.KVRead:
		f("the engine's logged KV was not read, so the check that the cap fits the engine was not made")
	case o.LoggedKV < int64(o.Cap)*MaxModelLen:
		inv("the engine's logged KV is %d tokens, below the cap's need of %d x %d = %d: the cap does not fit this engine, and no phase was run", o.LoggedKV, o.Cap, MaxModelLen, int64(o.Cap)*MaxModelLen)
		return dedupe(fail), dedupe(invalid)
	}
	if o.ReadyAt.IsZero() {
		f("the Instance was not seen ready")
	}
	if !o.Gate.Sent {
		f("the gate was not sent: no request showed the served Instance routable before the phases")
	} else if o.Gate.Row.Status != 200 || o.Gate.Row.CutBy != "" || o.Gate.Row.Emitter != "instance" {
		f("the gate's request at model@<provider> was not answered by the served Instance (status %d, emitter %q), so the row stopped before any phase", o.Gate.Row.Status, o.Gate.Row.Emitter)
	}
	if o.DidNotFit != "" {
		inv("%s did not fit before the Instance's drain start, so the phases that follow were not run", o.DidNotFit)
	}
	// The ratio probe: one request at the served Instance alone, which only it can answer. A probe the Instance did not answer is a failed check; one that
	// gave no prompt tokens, or a ratio at which no word count reaches the long range, leaves the long shape unset: INVALID, and P1-long was not run.
	if o.Ratio.Needed {
		if o.Ratio.Stopped == StopGuardUnread {
			f("the front's per-provider request metric could not be read, so the %d-request guard could not run and the ratio probe's generator was stopped, or never started", LLMLabGuard)
		}
		switch {
		case !o.Ratio.Sent:
			f("the ratio probe's request has no usage record at the front, so the long shape was not measured")
		case o.Ratio.Row.Status != 200 || o.Ratio.Row.CutBy != "" || o.Ratio.Row.Emitter != "instance":
			f("the ratio probe was not answered by the served Instance (status %d, emitter %q), so the row stopped before any phase", o.Ratio.Row.Status, o.Ratio.Row.Emitter)
		case o.Ratio.Invalid:
			inv("the ratio probe read %d prompt tokens for %d words (%.4f a word), at which no word count reaches %d to %d tokens: P1-long's shape could not be set and it was not run",
				o.Ratio.PromptTokens, o.Ratio.Words, o.Ratio.TokensPerWord, LongPromptMin, LongPromptMax)
		}
		if o.Ratio.Sent && o.Ratio.Row.Status == 200 && o.Ratio.Row.PromptTokens == nil {
			inv("the ratio probe's usage record holds no prompt tokens, so P1-long's shape could not be set and it was not run")
		}
	}

	done := map[string]PhaseObs{}
	for _, po := range o.Phases {
		done[po.Phase.Name] = po
		pf, pi := judgePhase(o, po)
		fail, invalid = append(fail, pf...), append(invalid, pi...)
	}
	for _, ph := range o.Plan {
		if _, ok := done[ph.Name]; !ok && o.DidNotFit == "" && !(o.Ratio.Invalid && ph.TargetTokens > 0 && ph.Context < 1) {
			// A phase after one that was stopped is no more than the stop's mark: the stop is named by that phase.
			stopped := o.StoppedBefore != ""
			for _, po := range o.Phases {
				stopped = stopped || po.Stopped != ""
			}
			if !stopped {
				f("%s was not run", ph.Name)
			}
		}
	}
	return dedupe(fail), dedupe(invalid)
}

// p0Problems judges P0: one request of P0Tokens tokens at the plain model, sent before any served ModelDeployment exists, which LLMLab alone can answer. A
// request the front answered (the managed Provider or its ModelDeployment never became usable) is a failed check; one LLMLab answered and failed, or answered
// short of the tokens min_tokens asked, makes the reading INVALID. The sequencer ends the run at either, before it creates the served ModelDeployment.
func p0Problems(o BaselineObs) (fail, invalid []string) {
	switch {
	case !o.P0.Sent:
		fail = append(fail, "P0 was not sent: the managed leg was never shown to answer on its own")
	case o.P0.Row.Status != 200 || o.P0.Row.CutBy != "":
		if o.P0.Row.Emitter == "provider" {
			invalid = append(invalid, fmt.Sprintf("P0's request failed and LLMLab answered it (status %d): LLMLab was not shown to serve", o.P0.Row.Status))
		} else {
			fail = append(fail, fmt.Sprintf("P0's request failed and the %s answered it, not LLMLab", o.P0.Row.Emitter))
		}
	case routeOfRow(o.P0.Row, o.LLMLabProvider) != RouteLLMLab:
		fail = append(fail, fmt.Sprintf("P0 was not answered by LLMLab: with no Instance ready everything is (HBEP-5, Test plan), and the emitter was %q", o.P0.Row.Emitter))
	case !p0Completed(o.P0.Row):
		// An HTTP 200 is not an answer: with min_tokens left out, an empty stream from LLMLab passes every check below, and the GPU job would be authorised
		// by a managed leg that never served a token.
		invalid = append(invalid, "P0's reply was a 200 that is no completed stream with tokens (no finish reason, a cut, or no completion tokens and no content events): LLMLab was not shown to serve, and no GPU job was authorised")
	case !o.OmitMinTokens && (o.P0.Row.CompletionTokens == nil || *o.P0.Row.CompletionTokens != P0Tokens):
		invalid = append(invalid, fmt.Sprintf("P0 asked for %d tokens with min_tokens and LLMLab did not give exactly that: it does not honour min_tokens, so the shapes' output lengths are not the config's (set HPCBURST_ALPHA_OMIT_MIN_TOKENS=1, the probe's verdict)", P0Tokens))
	}
	return fail, invalid
}

// p0Completed says P0's reply was a completed stream that carried tokens: a finish reason, no cut, and a positive count of completion tokens or, with no
// counts, of content events (usage's Record: finish, cutBy, completionTokens, contentEvents).
func p0Completed(r FrontRow) bool {
	if r.Finish == nil || *r.Finish == "" || r.CutBy != "" {
		return false
	}
	if r.CompletionTokens != nil {
		return *r.CompletionTokens > 0
	}
	return r.ContentEvents != nil && *r.ContentEvents > 0
}

// judgePhase judges one phase. The checks prove that a spill is the cap's and not forced by loading LLMLab, from the front's records and the engine alone: LLMLab gets none in the control; exactly k first-turn requests in P2, at most k in flight, with the Instance at its cap at each arrival; the engine at or under the cap with nothing waiting and nothing preempted; and what LLMLab got is posted.
func judgePhase(o BaselineObs, po PhaseObs) (fail, invalid []string) {
	f := func(s string, a ...any) {
		fail = append(fail, fmt.Sprintf("%s: "+s, append([]any{po.Phase.Name}, a...)...))
	}
	inv := func(s string, a ...any) {
		invalid = append(invalid, fmt.Sprintf("%s: "+s, append([]any{po.Phase.Name}, a...)...))
	}
	ph, managed, cap := po.Phase, o.LLMLabProvider, o.Cap

	switch po.Stopped {
	case StopGuard:
		f("LLMLab was asked for more than %d requests in one phase, so the generator was stopped", LLMLabGuard)
	case StopGuardUnread:
		f("the front's per-provider request metric could not be read, so the %d-request guard could not run and the generator was stopped, or never started", LLMLabGuard)
	case StopIneligible:
		f("the served Instance became ineligible, so the generator was stopped and the phase is not judged by what it measured")
	case StopBound:
		f("the generator was still running after the phase's bound of %s, and was stopped", ph.Bound)
	}

	joined, noAnswer, noRecord, unlisted := joinPhase(po)
	if noAnswer > 0 {
		f("%d request(s) got no answer from the front, so no usage record names them", noAnswer)
	}
	if len(noRecord) > 0 {
		f("%d request(s) the generator sent have no usage record at the front", len(noRecord))
	}
	if len(joined) == 0 && po.Stopped == "" && noAnswer == 0 && len(noRecord) == 0 {
		f("the generator made no request the front recorded, so the phase measured nothing")
	}
	llm := rowsOn(joined, managed, RouteLLMLab)
	for _, r := range unlisted {
		if routeOfRow(r, managed) == RouteLLMLab {
			f("LLMLab answered a request the generator did not send")
			break
		}
	}
	// What the generator's own headers said is compared, never counted, and only for a request that was answered whole: an error the front raises after it
	// chose a target may name the target in the header and not in the record.
	differ := 0
	for _, j := range joined {
		if j.failed() {
			continue
		}
		if j.Gen.Emitter != j.Front.Emitter || j.Gen.Provider != j.Front.Provider || j.Gen.Instance != j.Front.Instance {
			differ++
		}
	}
	if differ > 0 {
		f("%d generator record(s) name another route than the front's usage record of the same request", differ)
	}
	// The front's own count of LLMLab's requests, read live, against the records': two readings of one number.
	if !po.MetricRead && po.Stopped != StopGuardUnread {
		f("the front's per-provider request metric was not read, so LLMLab's count was checked against nothing")
	} else if int(po.LLMLabMetric) != len(llm) && po.Stopped == "" {
		f("the front's request metric says LLMLab was asked %.0f time(s) and its usage records name %d", po.LLMLabMetric, len(llm))
	}

	// The FINAL count, from the front's own records of every request LLMLab answered in the phase's window, sent by the generator or not, above the guard FAILS
	// the verdict, whatever the last poll said: the guard polls, and a burst between its last look and the generator's exit escapes it.
	if final := len(rowsFrom(po.Rows, managed)); final > LLMLabGuard {
		f("LLMLab answered %d requests in the phase by the front's own records, over the guard of %d", final, LLMLabGuard)
	}

	// A failed request: LLMLab's makes the run INVALID, the front's or an Instance's fails it; in the detach phase a 503 makes it INVALID.
	for _, j := range joined {
		if !j.failed() {
			continue
		}
		switch {
		case ph.Kind == KindDetach && j.Front.Status == 503:
			inv("a request was answered 503 (emitter %q): the detach phase's waiting requests must be served after waiting under queueTimeout, never refused", j.Front.Emitter)
		case j.Front.Emitter == "provider":
			inv("a request LLMLab answered failed (status %d)", j.Front.Status)
		default:
			f("a request failed (status %d, emitter %q)", j.Front.Status, j.Front.Emitter)
		}
	}
	invalid = dedupe(invalid)
	fail = dedupe(fail)

	switch ph.Kind {
	case KindControl, KindLong:
		// (a) LLMLab gets none.
		if len(llm) != 0 {
			f("LLMLab was asked for %d request(s), and the control gives it none", len(llm))
		}
		// Pinning, shown in the usage records: each session's requests answered by one route (the front's ring only classifies spills).
		moved := 0
		route := map[string]Route{}
		for _, j := range joined {
			r := j.route(managed)
			if first, ok := route[j.Gen.Session]; ok && first != r {
				moved++
			}
			route[j.Gen.Session] = r
		}
		if moved > 0 {
			f("%d request(s) were answered away from their session's earlier route: a session did not stay on its Instance", moved)
		}
	case KindFull:
		k := ph.Sessions - cap
		first := firstTurnRows(joined, managed, RouteLLMLab)
		switch {
		case k < 1:
			inv("%d sessions against a cap of %d: N is not above the cap, so the cap cannot bind", ph.Sessions, cap)
		case len(first) == 0 && len(llm) == 0:
			// A run where the cap never binds proves nothing (HBEP-5's Test plan).
			inv("the cap never bound: LLMLab was asked for no request, so no spill was shown")
		case len(first) != k:
			// (b) exactly k first-turn requests.
			f("LLMLab was asked for %d first-turn request(s), want exactly k = %d", len(first), k)
		}
		if k >= 1 && len(llm) > 0 {
			// (b) at most k in flight at any time.
			if peak, _, _ := inFlightStats(llm, -1); peak > k {
				f("LLMLab had %d requests in flight at once, want at most k = %d", peak, k)
			}
			// (c) at each LLMLab arrival the Instance held its cap.
			served := rowsOn(joined, managed, RouteServed)
			notHeld := 0
			for _, r := range llm {
				if !heldItsCapAt(served, r.Start, cap) {
					notHeld++
				}
			}
			if notHeld > 0 {
				f("%d LLMLab request(s) arrived while the Instance was below its cap of %d (by the usage records' intervals, within %s): the spill was not the cap's", notHeld, cap, ArrivalTolerance)
			}
		}
	case KindDetach:
		if n := len(rowsFrom(po.Rows, managed)); n != 0 {
			f("LLMLab answered %d request(s) after it was detached", n)
		}
		if !o.Detached {
			f("the managed ModelDeployment was not seen detached at the front before the phase")
		}
		// The waiting requests are served after waiting: by the records the front held more requests than the cap at once.
		if peak, _, _ := inFlightStats(rowsOn(joined, managed, RouteServed), -1); len(joined) > 0 && peak <= cap && len(invalid) == 0 {
			inv("no request waited: by the usage records no more than the cap of %d was in the front at once, so the cap never bound", cap)
		}
	}

	// Over the cap by the usage records' intervals, outside the detach phase, where waiters are in the front by design.
	if ph.Kind != KindDetach {
		if _, over, top := inFlightStats(rowsOn(joined, managed, RouteServed), cap); top >= cap+2 || over > ArrivalTolerance {
			f("the Instance had more than its cap of %d requests in flight for longer than %s (%d at most), by the usage records", cap, ArrivalTolerance, max(top, cap))
		}
	}

	// P1-long: the contexts reached the long range, read from the front's own count.
	if ph.Kind == KindLong {
		var shortest, longest int64 = -1, 0
		missing := 0
		for _, r := range firstTurnRows(joined, managed, RouteServed) {
			if r.PromptTokens == nil {
				missing++
				continue
			}
			if shortest < 0 || *r.PromptTokens < shortest {
				shortest = *r.PromptTokens
			}
			longest = max(longest, *r.PromptTokens)
		}
		switch {
		case missing > 0 || shortest < 0:
			inv("a first turn's prompt tokens are not in the front's records, so the long contexts were not confirmed")
		case shortest < LongPromptMin || longest > LongPromptMax:
			inv("the first turns' prompts held %d to %d tokens, outside the long phase's %d to %d", shortest, longest, LongPromptMin, LongPromptMax)
		}
	}

	// (d) the engine: running at most the cap, nothing waiting, no new preemption; a phase with no fresh sample checks nothing. In the long phase the engine's
	// requests waiting and its preemptions are readings (ruling H0, #764): eleven prompts of 24k tokens that arrive together queue in the engine's own
	// prefill, and the reading is what R4's budget is compared against; only the cap is a check there.
	if len(po.Engine) < 2 {
		f("fewer than 2 engine samples were read, so the engine's running, waiting and preemptions were not checked")
	} else {
		over := 0
		for _, s := range po.Engine {
			if s.Running > float64(cap) {
				over++
			}
		}
		if over > 0 {
			f("the engine ran more than the cap of %d requests in %d sample(s)", cap, over)
		}
		if ph.Kind != KindLong {
			if run := longestWaitingRun(po.Engine); run >= EngineWaitingSustain {
				f("the engine had requests waiting in %d consecutive samples, past the %d that a burst of first prefills explains (%s at the %s cadence)", run, EngineWaitingSustain-1, time.Duration(run)*BaselinePoll, BaselinePoll)
			}
			if d := po.Engine[len(po.Engine)-1].Preemptions - po.Engine[0].Preemptions; d != 0 {
				f("the engine preempted %.0f request(s) during the phase", d)
			}
		}
	}
	return dedupe(fail), dedupe(invalid)
}

// longestWaitingRun is the longest stretch of consecutive engine samples with requests waiting.
func longestWaitingRun(samples []EngineSample) int {
	best, run := 0, 0
	for _, s := range samples {
		if s.Waiting > 0 {
			run++
			best = max(best, run)
		} else {
			run = 0
		}
	}
	return best
}

// rowsFrom are the front's records of a phase answered by a Provider, whoever sent them.
func rowsFrom(rows []FrontRow, provider string) []FrontRow {
	var out []FrontRow
	for _, r := range rows {
		if r.Provider == provider && r.Emitter == "provider" {
			out = append(out, r)
		}
	}
	return out
}

// BaselineInvalid is the reasons a run of Row 1 measured nothing, none when it did. The caller gives a failure precedence (run.go).
func BaselineInvalid(o BaselineObs) []string {
	_, invalid := judgeBaseline(o)
	return invalid
}

// VerifyBaseline judges the run of Row 1: the checks of every other one-job row (the deployment, the job's record), and the baseline's own.
func VerifyBaseline(row Row, o BaselineObs, jobs []provider.Job) []string {
	var f []string
	if o.ServedCreated {
		f = deploymentChecks(row, o.Deployment)
		lf, _ := ledgerChecks(row, o.Tags, jobs)
		f = append(f, lf...)
		f = append(f, tagCountChecks(row, o.Tags, "the row makes exactly one Allocation")...)
	} else if len(o.Tags) > 0 || len(jobs) > 0 {
		// The served ModelDeployment is created only after P0 completed, so a run that never created it submitted nothing.
		f = append(f, "a job was submitted although the served ModelDeployment was never created: nothing is submitted before P0 completes")
	}
	bf, _ := judgeBaseline(o)
	return dedupe(append(f, bf...))
}

// baselineVerdictWord is the post's verdict for a run.
func baselineVerdictWord(failures, invalid []string) string {
	switch {
	case len(failures) > 0:
		return "FAIL"
	case len(invalid) > 0:
		return "INVALID"
	}
	return "PASS"
}

// phaseSummaryLine is a phase's shape, in the post's words.
func phaseSummaryLine(p Phase) string {
	return fmt.Sprintf("N = %d, %d turns, context %d, turn %d, output %d words, think %s, stagger %s", p.Sessions, p.Turns, p.Context, p.Turn, p.Output, p.Think, p.Stagger)
}

// baselinePost is the text to post on #764: per phase and per route the sessions (distinct, and the most in flight), spills in and out, waits at the
// front, the time to first token and the speed per session, with LLMLab's requests and tokens, then #769's mapping. It holds counts and fixed labels,
// never a name from the cluster.
func baselinePost(row Row, o BaselineObs, failures, invalid []string) string {
	var b strings.Builder
	switch baselineVerdictWord(failures, invalid) {
	case "FAIL":
		fmt.Fprintf(&b, "%s: FAIL (posts to %s)\n- failed checks: %s\n", row.Name, row.PostsTo, Summary(failures))
	case "INVALID":
		fmt.Fprintf(&b, "%s: INVALID (posts to %s)\n- the reading is invalid: %s\n- %s\n", row.Name, row.PostsTo, Summary(invalid), invalidNotNotRunWords)
	default:
		fmt.Fprintf(&b, "%s: PASS (posts to %s)\n", row.Name, row.PostsTo)
	}
	fmt.Fprintf(&b, "%s\n", deploymentLine(o.Deployment))
	fmt.Fprintf(&b, "- the model: %s at the pinned revision, as shipped, max-model-len %d; one Instance on Sano and LLMLab attached by a managed Provider; no Workload (today's code)\n", BaselineModel, MaxModelLen)
	fmt.Fprintf(&b, "- the cap is %d, a constant fixed before the run: floor(0.95 x %d / %d) from the engine's KV as shipped; the engine's logged KV after ready was %d tokens (a check, never an input)\n", o.Cap, KVSano, MaxModelLen, o.LoggedKV)
	fmt.Fprintf(&b, "- R4's declared charge for comparison: the typical shape's 1,152 tokens against KV %d is %d sessions, capped at max-num-seqs 256, where the baseline's cap is %d\n", KVSano, KVSano/1152, o.Cap)
	fmt.Fprintf(&b, "- min_tokens: %s\n", map[bool]string{true: "left out of every request (the probe's verdict)", false: "sent on every request"}[o.OmitMinTokens])
	if o.Ratio.Needed {
		if o.Ratio.LongWords > 0 {
			fmt.Fprintf(&b, "- the long shape, MEASURED: one probe of %d words and 1 output token at the served Instance alone read %d prompt tokens, %.4f tokens a word, so P1-long's contexts are %d words for %d tokens (a shape, never a cap)\n",
				o.Ratio.Words, o.Ratio.PromptTokens, o.Ratio.TokensPerWord, o.Ratio.LongWords, LongTargetTokens)
		} else {
			fmt.Fprintf(&b, "- the long shape could NOT be set: the probe of %d words read %d prompt tokens (%.4f a word), at which no word count reaches %d to %d tokens\n",
				o.Ratio.Words, o.Ratio.PromptTokens, o.Ratio.TokensPerWord, LongPromptMin, LongPromptMax)
		}
	}
	if o.P0.Sent {
		fmt.Fprintf(&b, "- P0 (no Instance ready): answered by %s, status %d\n", routeOfRow(o.P0.Row, o.LLMLabProvider), o.P0.Row.Status)
	}
	if o.DidNotFit != "" {
		fmt.Fprintf(&b, "- %s did not fit before the drain start\n", o.DidNotFit)
	}
	for _, po := range o.Phases {
		joined, _, _, _ := joinPhase(po)
		fmt.Fprintf(&b, "- %s (%s): %s\n", po.Phase.Name, po.Phase.Kind, phaseSummaryLine(po.Phase))
		if po.Stopped != "" {
			fmt.Fprintf(&b, "  - the generator was stopped: %s\n", po.Stopped)
		}
		for _, rr := range ReadRoutes(joined, o.LLMLabProvider) {
			if rr.Requests == 0 && rr.Route != RouteLLMLab {
				continue
			}
			fmt.Fprintf(&b, "  - %s: %d requests, %d sessions (distinct), %d at most in flight; spills in %d, out %d; errors %d; time to first token p50 %.0f ms, p95 %.0f ms; speed per session p50 %.1f and minimum %.1f tokens/s; tokens %d prompt, %d completion\n",
				rr.Route, rr.Requests, rr.Sessions, rr.MostInFlight, rr.SpillsIn, rr.SpillsOut, rr.Errors, rr.TTFTp50, rr.TTFTp95, rr.SpeedP50, rr.SpeedMin, rr.PromptTokens, rr.CompletionTokens)
		}
		b.WriteString(waitsLine(o, po, joined))
		if len(po.Engine) > 0 {
			peak, kv, waiting, waitPeak := 0.0, 0.0, 0, 0.0
			pre := po.Engine[len(po.Engine)-1].Preemptions - po.Engine[0].Preemptions
			for _, s := range po.Engine {
				peak, kv = max(peak, s.Running), max(kv, s.KVUsage)
				if s.Waiting > 0 {
					waiting++
					waitPeak = max(waitPeak, s.Waiting)
				}
			}
			run := longestWaitingRun(po.Engine)
			reading := ""
			if po.Phase.Kind == KindLong {
				reading = " (a READING, ruling H0: the declared charge of 1,152 tokens a session against the cache's real use at the cap)"
			}
			fmt.Fprintf(&b, "  - the engine, over %d samples: running at most %.0f (the cap %d); requests waiting in %d samples, the longest run %d, at most %.0f; preemptions %.0f; KV use at most %.1f%% of the cache (vllm:kv_cache_usage_perc)%s\n",
				len(po.Engine), peak, o.Cap, waiting, run, waitPeak, pre, kv*100, reading)
		}
	}
	b.WriteString("- the mapping to #769 (hbep-764-amend at 9cc8368d), measurement then the rule it will be compared against:\n")
	for _, m := range Mapping769 {
		fmt.Fprintf(&b, "  - %s: %s (%s)\n", m.Measurement, m.Rule, m.Where)
	}
	return b.String()
}

// waitsLine is the phase's waits at the front. Today the front has no wait series and no per-request wait (HBEP-5, Usage records; #769 proposes
// hpcburst_front_waiting), so with LLMLab attached, whose overflow is unbounded (internal/front/route.go:119-129), a wait is 0 by construction and the
// line says so; the detach phase's are measured by the usage records against the engine: the most requests in the front at once less the most running at the
// engine.
func waitsLine(o BaselineObs, po PhaseObs, joined []joinedRow) string {
	capped := 0
	for _, j := range joined {
		if j.Front.Status == 503 && j.Front.Emitter == "front" {
			capped++
		}
	}
	if po.Phase.Kind != KindDetach {
		return fmt.Sprintf("  - waits at the front: 0 by construction with LLMLab attached (its overflow is unbounded, so the front never queues; the front has no wait series yet, #769 proposes one); 503 capacity answers: %d\n", capped)
	}
	peak, _, _ := inFlightStats(rowsOn(joined, o.LLMLabProvider, RouteServed), -1)
	running := 0.0
	for _, s := range po.Engine {
		running = max(running, s.Running)
	}
	return fmt.Sprintf("  - waits at the front, MEASURED (LLMLab detached): at most %d requests in the front at once by the usage records, at most %.0f running at the engine, so at most %.0f waited in the front's FIFO, served after waiting (time to first token above includes the wait); 503 answers: %d\n",
		peak, running, float64(peak)-running, capped)
}
