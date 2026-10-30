package sim

import (
	"fmt"
	"math"
	"sort"
)

// Metrics collects one run's results. The load counters it reads are the
// simulator's own per-Instance counts, kept apart from KubeAI's, and every
// placement cross-checks the two.
type Metrics struct {
	pol Policy
	// Measurement window on request dispatch time, and the bucket width for
	// the hit-rate time series.
	WinStart, WinEnd, Bucket float64

	// Window totals.
	Requests      int
	PromptTokens  int64
	HitTokens     int64
	PromptTokens1 int64 // turns after the first
	HitTokens1    int64
	PrefillTokens int64 // prompt tokens computed (misses), window only
	OutputTokens  int64 // throughput: output tokens of requests completed in the window
	Moves1        int   // turn>0 requests on a different Instance than the previous turn
	Requests1     int
	latencies     []float64

	// Whole run.
	Compactions int
	Aborted     int

	// Bound at placement (CHWBL only). KubeRule: the chosen Instance's load
	// before the request exceeded c*(T+1)/n, T and n over current members.
	// Paper: its load after the request exceeded ceil(c*(T+1)/n).
	Placements        int
	KubeRuleViol      int
	PaperViol         int
	MaxPlaceExcess    float64 // max of load_after - c*(T+1)/n, in requests
	MaxRuleExcess     float64 // max of load_before - c*(T+1)/n over rule violations
	MaxPlaceRatio     float64 // max of load_after / mean_after, when mean_after >= 2
	MaxKubeInflation  int64   // KubeAI's total in-flight minus the members' total
	PeakLoad          int     // max in-flight on any member, over time
	timeAbove, timeOn float64 // time with max load > c*mean, time with load > 0
	// Time integrals over the window: active conversations, their context
	// tokens, and in-flight requests on members.
	winTime, convTime, tokTime, loadTime float64

	// Re-homing after the latest membership event.
	lastEvent  int
	lastEvKind EventKind
	lastEvInst int
	Rehomed    []RehomeCount

	firstKeys map[string]int
	buckets   map[int]*[2]int64 // bucket -> {hit, prompt}

	// PerInst holds window totals per Instance, and each one's peak load.
	PerInst []InstStat

	// Trace, when set before Run, records every completed request in Recs.
	Trace bool
	Recs  []Rec
}

// InstStat is one Instance's share of the window.
type InstStat struct {
	Requests     int
	Prompt, Hit  int64
	PeakInFlight int
}

// Rec is one completed request.
type Rec struct {
	Conv, Turn, Inst, Prompt, Hit int
	Dispatch, End                 float64
}

// RehomeCount counts, for one event, conversations whose first request
// after the event went elsewhere than their last request before it.
type RehomeCount struct {
	Event      Event
	Checked    int // conversations seen before and after the event
	Forced     int // their Instance left the ring
	Collateral int // their Instance stayed, yet they moved
	ToJoined   int // moved onto a joining Instance
}

func newMetrics(pol Policy, n int) *Metrics {
	return &Metrics{pol: pol, WinEnd: math.Inf(1), Bucket: 60, lastEvent: -1, PerInst: make([]InstStat, n),
		firstKeys: map[string]int{}, buckets: map[int]*[2]int64{}}
}

func (m *Metrics) inWindow(t float64) bool { return t >= m.WinStart && t < m.WinEnd }

func membersLoad(e *Engine) (total, peak int) {
	for _, i := range e.rt.members {
		total += e.inst[i].inflight
		peak = max(peak, e.inst[i].inflight)
	}
	return total, peak
}

// advance accumulates time-weighted load until time t.
func (m *Metrics) advance(e *Engine, t float64) {
	dt := t - e.now
	if dt <= 0 || len(e.rt.members) == 0 {
		return
	}
	total, peak := membersLoad(e)
	if m.inWindow(e.now) {
		m.winTime += dt
		m.convTime += dt * float64(e.activeConvs)
		m.tokTime += dt * float64(e.activeTokens)
		m.loadTime += dt * float64(total)
	}
	if total == 0 {
		return
	}
	m.timeOn += dt
	if float64(peak) > m.pol.Bound()*float64(total)/float64(len(e.rt.members))+1e-9 {
		m.timeAbove += dt
	}
}

func (m *Metrics) placement(e *Engine, r *request, idx int) {
	if err := m.crossCheck(e, idx); err != nil {
		e.err = err
		return
	}
	n := float64(len(e.rt.members))
	total, _ := membersLoad(e)
	pre := float64(e.inst[idx].inflight)
	meanAfter := float64(total+1) / n
	m.Placements++
	m.PeakLoad = max(m.PeakLoad, e.inst[idx].inflight+1)
	if m.inWindow(e.now) {
		st := &m.PerInst[idx]
		st.PeakInFlight = max(st.PeakInFlight, e.inst[idx].inflight+1)
	}
	if meanAfter >= 2 {
		m.MaxPlaceRatio = max(m.MaxPlaceRatio, (pre+1)/meanAfter)
	}
	if c := m.pol.Bound(); !math.IsInf(c, 1) {
		bound := c * meanAfter
		m.MaxPlaceExcess = max(m.MaxPlaceExcess, pre+1-bound)
		if pre > bound+1e-9 {
			m.KubeRuleViol++
			m.MaxRuleExcess = max(m.MaxRuleExcess, pre-bound)
		}
		if pre+1 > math.Ceil(bound-1e-9) {
			m.PaperViol++
		}
	}
	if r.turn == 0 && r.cs.lastInst < 0 && m.pol.Hashed() {
		k, _ := m.pol.Key.Key(r.cs, r.id)
		m.firstKeys[k]++
	}
	m.rehome(r, idx)
	if r.turn > 0 && m.inWindow(r.dispatchT) {
		m.Requests1++
		if r.cs.lastInst >= 0 && r.cs.lastInst != idx {
			m.Moves1++
		}
	}
}

// crossCheck compares KubeAI's in-flight counters with the simulator's. It
// runs after KubeAI counted the new request on idx and before the simulator
// does, so KubeAI is ahead by one there.
func (m *Metrics) crossCheck(e *Engine, idx int) error {
	g := e.rt.group
	if g == nil {
		return nil
	}
	var all int64
	for _, in := range e.inst {
		all += int64(in.inflight - in.detached)
	}
	if g.TotalInFlight() != all+1 {
		return fmt.Errorf("kubeai total in-flight %d, simulator %d+1", g.TotalInFlight(), all)
	}
	for _, i := range e.rt.members {
		want := int64(e.inst[i].inflight)
		if i == idx {
			want++
		}
		if v, ok := g.InFlight(e.inst[i].name); !ok || v != want {
			return fmt.Errorf("kubeai in-flight for %s is %d (member %v), simulator expects %d", e.inst[i].name, v, ok, want)
		}
	}
	total, _ := membersLoad(e)
	m.MaxKubeInflation = max(m.MaxKubeInflation, all-int64(total))
	return nil
}

func (m *Metrics) rehome(r *request, idx int) {
	cs := r.cs
	if m.lastEvent < 0 || cs.evSeen >= m.lastEvent {
		return
	}
	if cs.lastInst < 0 {
		// Started after the event: it had no Instance to be moved from.
		cs.evSeen = m.lastEvent
		return
	}
	cs.evSeen = m.lastEvent
	rc := &m.Rehomed[len(m.Rehomed)-1]
	rc.Checked++
	if idx == cs.lastInst {
		return
	}
	switch {
	case m.lastEvKind != Join && cs.lastInst == m.lastEvInst:
		rc.Forced++
	case m.lastEvKind == Join && idx == m.lastEvInst:
		rc.ToJoined++
	default:
		rc.Collateral++
	}
}

func (m *Metrics) event(e *Engine, k int, ev Event) {
	m.lastEvent, m.lastEvKind, m.lastEvInst = k, ev.Kind, ev.Instance
	m.Rehomed = append(m.Rehomed, RehomeCount{Event: ev})
}

func (m *Metrics) completed(e *Engine, r *request) {
	if m.Trace {
		m.Recs = append(m.Recs, Rec{Conv: r.cs.c.ID, Turn: r.turn, Inst: r.inst, Prompt: r.promptLen,
			Hit: r.hold.HitTokens, Dispatch: r.dispatchT, End: e.now})
	}
	b := int(r.dispatchT / m.Bucket)
	if m.buckets[b] == nil {
		m.buckets[b] = &[2]int64{}
	}
	m.buckets[b][0] += int64(r.hold.HitTokens)
	m.buckets[b][1] += int64(r.promptLen)
	if !m.inWindow(r.dispatchT) {
		return
	}
	m.Requests++
	st := &m.PerInst[r.inst]
	st.Requests++
	st.Prompt += int64(r.promptLen)
	st.Hit += int64(r.hold.HitTokens)
	m.PromptTokens += int64(r.promptLen)
	m.HitTokens += int64(r.hold.HitTokens)
	m.PrefillTokens += int64(r.promptLen - r.hold.HitTokens)
	m.OutputTokens += int64(r.outLen)
	if r.turn > 0 {
		m.PromptTokens1 += int64(r.promptLen)
		m.HitTokens1 += int64(r.hold.HitTokens)
	}
	m.latencies = append(m.latencies, e.now-r.dispatchT)
}

func (m *Metrics) finish(e *Engine) error {
	if g := e.rt.group; g != nil && g.TotalInFlight() != 0 {
		return fmt.Errorf("kubeai in-flight leaked: %d at the end", g.TotalInFlight())
	}
	for _, in := range e.inst {
		if in.inflight != 0 {
			return fmt.Errorf("%s ended with %d in flight", in.name, in.inflight)
		}
		if !in.cache.Check() {
			return fmt.Errorf("%s cache accounting broken", in.name)
		}
	}
	sort.Float64s(m.latencies)
	return nil
}

// WorkingSet returns time averages over the window: active conversations,
// their total context tokens, and in-flight requests.
func (m *Metrics) WorkingSet() (convs, tokens, inflight float64) {
	if m.winTime == 0 {
		return 0, 0, 0
	}
	return m.convTime / m.winTime, m.tokTime / m.winTime, m.loadTime / m.winTime
}

// HitRate is the share of prompt tokens served from cache in the window.
func (m *Metrics) HitRate() float64 { return ratio(m.HitTokens, m.PromptTokens) }

// HitRate1 is HitRate over turns after the first.
func (m *Metrics) HitRate1() float64 { return ratio(m.HitTokens1, m.PromptTokens1) }

// MoveRate is the share of later-turn requests that changed Instance.
func (m *Metrics) MoveRate() float64 { return ratio(int64(m.Moves1), int64(m.Requests1)) }

// TimeAboveBound is the share of loaded time some member exceeded c*mean.
func (m *Metrics) TimeAboveBound() float64 {
	if m.timeOn == 0 {
		return 0
	}
	return m.timeAbove / m.timeOn
}

// Latency returns the q-quantile of request latency in the window.
func (m *Metrics) Latency(q float64) float64 {
	if len(m.latencies) == 0 {
		return 0
	}
	return m.latencies[min(len(m.latencies)-1, int(q*float64(len(m.latencies))))]
}

// MeanLatency is the mean request latency in the window.
func (m *Metrics) MeanLatency() float64 {
	var s float64
	for _, v := range m.latencies {
		s += v
	}
	return s / math.Max(1, float64(len(m.latencies)))
}

// Keys returns the number of distinct first-turn keys and the largest
// share of conversations that shared one key.
func (m *Metrics) Keys() (distinct int, topShare float64) {
	var total, top int
	for _, n := range m.firstKeys {
		total += n
		top = max(top, n)
	}
	return len(m.firstKeys), ratio(int64(top), int64(total))
}

// BucketHitRate returns the hit rate of requests dispatched in [from, to).
func (m *Metrics) BucketHitRate(from, to float64) float64 {
	var h, p int64
	for b, v := range m.buckets {
		t := float64(b) * m.Bucket
		if t >= from && t < to {
			h += v[0]
			p += v[1]
		}
	}
	return ratio(h, p)
}

func ratio(a, b int64) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}
