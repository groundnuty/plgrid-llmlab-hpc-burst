package lifecycle

import (
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The Instance's KV cache size, status.kvCacheTokens (HBEP-7, The KV cache size; HBEP-764, R4;
// `@hpc-burst-science-agent`, 18 Nov 2026, #764, Q4 as revised, comment 6988932328). The agent reads it
// once per engine process and reports it with each worker's health, in every report, past a failed metrics
// read; the controller writes what is reported, from the report of this pass alone:
//   - the field is the smallest value reported by the workers the cap counts NOW: with one worker, the
//     running worker; with several, the router's healthy workers, taken as kvRouterWorkers says;
//   - it is UNSET when none of them reports a value, whatever it was before;
//   - the controller keeps no memory of its own, so a restart of it changes nothing. A pass in which no
//     counted worker has reported yet unsets the field, which switches the front's token budget off until
//     the next report: the safe direction. A replacement process whose metric is absent never serves on its
//     predecessor's size;
//   - it is not gated on the metrics' freshness: the value is fixed for its process, which is why the agent
//     carries it, and a stale MetricsReadAt says nothing against it.

// kvTokens is the Instance's KV cache size from the report of this pass: the smallest value among the
// counted workers that carry one, nil (unset) when none does or the pass has no report. A value below 1 is
// no size (the field's minimum, and an agent that reports one is wrong): unknown.
func kvTokens(in *v1alpha1.Instance, av agentView) *int64 {
	if av.report == nil {
		return nil
	}
	var smallest *int64
	for _, name := range kvCounted(in, av) {
		w := reportOf(av.report, name)
		if w == nil || w.Health == nil || w.Health.KVCacheTokens == nil || *w.Health.KVCacheTokens < 1 {
			continue
		}
		if v := *w.Health.KVCacheTokens; smallest == nil || v < *smallest {
			n := v
			smallest = &n
		}
	}
	return smallest
}

// kvCounted is the workers whose values count: with one worker, the running worker, router or not; with
// several, the router's healthy workers, whose healthy flag is the router's view of the worker's current
// process (kvRouterWorkers).
func kvCounted(in *v1alpha1.Instance, av agentView) []string {
	if several(in) {
		return kvRouterWorkers(in, av)
	}
	name := v1alpha1.WorkerName(0)
	if w := reportOf(av.report, name); w != nil && w.Phase == agentapi.PartRunning {
		return []string{name}
	}
	return nil
}

// kvRouterWorkers is the router's healthy workers (routerHealthy, the set healthyCount counts) that the
// KV cache size may be taken over: a subset of it, for two reasons healthyCount does not need to meet
// (`@hpc-burst-science-agent`, 18 Nov 2026, #764, Q4 as revised, and PR 780's review):
//   - Identity. The router's report names a worker and no process, so its healthy flag could be the flag of
//     a worker's predecessor: a replacement's size would count under it before the router has seen the
//     replacement. A worker counts only if the router's read postdates its current process's start. The
//     agent takes both times on its own clock and holds a restarted worker out of the router (its
//     readiness answers 503) until its new generation is verified, so a read taken after the start shows
//     the replacement's own state. The agent cannot stamp a generation on an Envoy host, which has none,
//     so the join is on time.
//   - A failed read is unknown. When the agent cannot read /clusters, the last read stands and its time
//     says how old it is (the agent's routerRead). A read older than agentapi.MetricsStaleProbeIntervals
//     probe intervals at the report's own time, or a probe interval that is not known, makes every worker
//     unknown: none counts.
//
// healthyWorkers is unchanged by both: healthyCount keeps its own rule (#429).
func kvRouterWorkers(in *v1alpha1.Instance, av agentView) []string {
	names, ok := routerHealthy(in, av)
	if !ok || len(names) == 0 {
		return nil
	}
	rr := currentRouter(in, av)
	bound := agentapi.MetricsStaleProbeIntervals * av.probeInterval
	if rr == nil || rr.ClustersReadAt == nil || av.probeInterval <= 0 || av.reportTime.IsZero() || av.reportTime.Sub(*rr.ClustersReadAt) > bound {
		return nil
	}
	var out []string
	for _, name := range names {
		if w := reportOf(av.report, name); w != nil && w.StartTime != nil && rr.ClustersReadAt.After(*w.StartTime) {
			out = append(out, name)
		}
	}
	return out
}
