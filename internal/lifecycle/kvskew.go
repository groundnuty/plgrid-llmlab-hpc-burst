package lifecycle

import (
	"fmt"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The KV-skew signal (HBEP-7, Health and restarts, by #520; #518): with several workers behind the
// router, one worker at a full KV cache with requests waiting and its token counter flat, while a sibling
// the router routes to idles, for health.kvSkewTimeout. Bounded loads don't spill a few heavy sessions off
// one worker (HBEP-5), so load alone can fill it while a sibling idles, but a loaded worker still
// generates tokens: a full worker whose counter is flat is stuck, and a sibling could relieve it. One
// worker never raises it: only the several-workers path asks (workerSignal), and HBEP-7 says a full KV
// cache on the only worker with requests waiting is plain load.

// kvSkewFullAt is the KV-cache usage at or above which a worker is full (HBEP-7, by #520).
const kvSkewFullAt = 0.95

// workerSkewed reports whether worker w is KV-skewed in the report av holds, one snapshot of every
// worker:
//   - w's gauges are fresh, its KV cache full and requests waiting (Q1, Q2);
//   - a sibling idles, 0 running and 0 waiting, with fresh gauges (Q3), and the router routes to it: it is
//     warmed up and router-admitted at its current generation, and the report's router part, running,
//     holds it healthy and not ejected (Q3: an unverified, rejoining or unhealthy sibling idles because
//     Envoy sends it nothing, not because of the hash).
//
// The fourth condition, not draining, is the clock's (Backend.skewClock): it needs the counter over time.
//
// A nil gauge is unknown, never 0, and a stale sample is unknown (HBEP-7, Only fresh gauges act): unknown
// is not skewed, which stops the clock (Q4).
func workerSkewed(in *v1alpha1.Instance, av agentView, w string) bool {
	if av.report == nil || av.unreachable {
		return false
	}
	r := reportOf(av.report, w)
	if r == nil || !av.metricsFresh(r) {
		return false
	}
	h := r.Health
	if h.KVCacheUsage == nil || *h.KVCacheUsage < kvSkewFullAt || h.WaitingRequests == nil || *h.WaitingRequests <= 0 {
		return false
	}
	rr := av.report.Router
	if rr == nil || rr.Phase != agentapi.PartRunning {
		return false
	}
	routed := map[string]bool{}
	for _, rw := range rr.Workers {
		routed[rw.Name] = rw.Healthy && !rw.Ejected
	}
	for i := range av.report.Workers {
		s := &av.report.Workers[i]
		if s.Name == w || !routed[s.Name] || !workerWarmed(in, s.Name) || !routerAdmitted(in, s.Name) || !av.metricsFresh(s) {
			continue
		}
		if g := s.Health; g.RunningRequests != nil && *g.RunningRequests == 0 && g.WaitingRequests != nil && *g.WaitingRequests == 0 {
			return true
		}
	}
	return false
}

// kvSkewSignal is worker w's KV-skew signal: its skew clock, advanced by the current report, has run
// health.kvSkewTimeout. The clock is kept per process in the Backend's tracker and floored at this
// controller's start, so a restarted controller starts it again (Q4): it delays the signal and never
// hastens it. Until the signal is due, the pass is asked back at its deadline.
func (p *pass) kvSkewSignal(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, av agentView, st settings, w *agentapi.WorkerReport) (string, string) {
	since := p.advanceSkew(in, alloc, av, w)
	if since.IsZero() {
		return "", ""
	}
	if p.mem.started.After(since) {
		since = p.mem.started
	}
	deadline := since.Add(st.kvSkewTimeout)
	if p.now.Before(deadline) {
		p.at(deadline)
		return "", ""
	}
	return v1alpha1.ReasonKVSkew, fmt.Sprintf("%s has sat at a full KV cache with requests waiting and its token counter flat, while a sibling idles, for %s", w.Name, p.now.Sub(since))
}

// advanceSkew advances worker w's skew clock by the current report and returns its start.
func (p *pass) advanceSkew(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, av agentView, w *agentapi.WorkerReport) time.Time {
	return p.r.Backend.skewClock(alloc.UID, string(in.UID), *w, av.reportAt, workerSkewed(in, av, w.Name))
}

// advanceSkewClocks advances every worker's skew clock by the current report, once per pass, before any
// signal is taken: a worker whose signal is not asked this pass, behind another's, still has every report
// judged, a clear included.
func (p *pass) advanceSkewClocks(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, av agentView) {
	if av.report == nil || av.unreachable {
		return
	}
	for i := range av.report.Workers {
		p.advanceSkew(in, alloc, av, &av.report.Workers[i])
	}
}
