package lifecycle

import (
	"maps"
	"slices"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The Instance's KV cache size, status.kvCacheTokens (HBEP-7, The KV cache size; HBEP-764, R4;
// `@hpc-burst-science-agent`, 18 Nov 2026, #764, Q4). The agent reads it once per engine process and
// reports it with each worker's health, in every report, past a failed metrics read; the controller writes
// what is reported:
//   - with one worker, the running worker's value; with several, the smallest value among the workers the
//     router holds healthy, the set healthyCount counts and the front's cap counts;
//   - a report that carries no value leaves the field as it is, unless the worker's process has changed
//     since the value was taken, which unsets it;
//   - it is not gated on the metrics' freshness: the value is fixed for its process, which is why the agent
//     carries it, and a stale MetricsReadAt says nothing against it.
//
// "Since the value was taken" needs what the API object has no field for: the process the value came from.
// The controller keeps it in memory, per Allocation (Backend.kv), as the key of each worker whose report
// gave the value now written: the whole-Instance restart generation and the worker's own, which a report
// echoes. The agent starts a process only when one of them rises (convergeWorkers), so a different key is a
// different process. The record is lost with the controller's process, and then a report without a value
// leaves the field until a report brings one; that stays harmless, since a restarted worker is outside the
// counted set until its new generation is verified (the agent answers the router's check only for the
// verified generation), an Instance of one worker is unroutable while it verifies again, and the agent reads
// the new value at its first good sample, before verification.

// kvKey is the engine process a worker's value came from: the Instance's restart generation and the
// worker's own, as the report echoes them (InstanceReport.Generation, WorkerReport.Generation).
type kvKey struct{ instance, worker int64 }

type kvAction int

const (
	// kvLeave: the field stays as it is.
	kvLeave kvAction = iota
	// kvSet: the field is the decision's tokens.
	kvSet
	// kvUnset: the field is removed.
	kvUnset
)

// kvDecision is what one report does to status.kvCacheTokens, and the record to keep after it is done.
type kvDecision struct {
	action kvAction
	tokens int64
	// took is, for kvSet, the key of every worker whose value the field now rests on; it replaces the
	// record. For kvUnset the record is emptied, and for kvLeave it stays.
	took map[string]kvKey
}

// kvDecide judges a report from the Instance's registered agent: the smallest value among the counted
// workers that carry one, else leave, else unset when a worker the field rests on is no longer in the
// process its value came from. rec is the record of the last decision (workers and their keys); it is
// read and never changed.
func kvDecide(in *v1alpha1.Instance, av agentView, rec map[string]kvKey) kvDecision {
	ir := av.report
	if ir == nil {
		return kvDecision{}
	}
	took := map[string]kvKey{}
	var smallest int64
	for _, name := range kvCounted(in, av) {
		w := reportOf(ir, name)
		// A value below 1 is no size (the field's minimum, and an agent that reports one is wrong): unknown.
		if w == nil || w.Health == nil || w.Health.KVCacheTokens == nil || *w.Health.KVCacheTokens < 1 {
			continue
		}
		v := *w.Health.KVCacheTokens
		if len(took) == 0 || v < smallest { // the first value is the smallest so far
			smallest = v
		}
		took[name] = kvKey{ir.Generation, w.Generation}
	}
	if len(took) > 0 {
		return kvDecision{action: kvSet, tokens: smallest, took: took}
	}
	// No counted worker reports a value. The field rests on the workers of the record: unset it when any of
	// them runs another process now (a worker with no report says nothing).
	for _, name := range slices.Sorted(maps.Keys(rec)) {
		if w := reportOf(ir, name); w != nil && rec[name] != (kvKey{ir.Generation, w.Generation}) {
			return kvDecision{action: kvUnset}
		}
	}
	return kvDecision{}
}

// kvCounted is the workers whose values count: with several workers the router's healthy set, from a
// report that says something current about the router (routerHealthy, the one set healthyCount counts);
// with one, the running worker, router or not.
func kvCounted(in *v1alpha1.Instance, av agentView) []string {
	if several(in) {
		names, _ := routerHealthy(in, av)
		return names
	}
	name := v1alpha1.WorkerName(0)
	if w := reportOf(av.report, name); w != nil && w.Phase == agentapi.PartRunning {
		return []string{name}
	}
	return nil
}

// writeKVCacheTokens writes status.kvCacheTokens when a report from the Instance's agent changes it
// (kvDecide), and keeps the record of whose processes the value came from.
func (p *pass) writeKVCacheTokens(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, av agentView) error {
	d := kvDecide(in, av, p.r.Backend.kvRecord(alloc.UID, in.UID))
	switch d.action {
	case kvSet:
		if in.Status.KVCacheTokens == nil || *in.Status.KVCacheTokens != d.tokens {
			n := d.tokens
			if err := p.setStatus(in, func(s *v1alpha1.InstanceStatus) { s.KVCacheTokens = &n }); err != nil {
				return err
			}
		}
		p.r.Backend.setKVRecord(alloc.UID, in.UID, d.took)
	case kvUnset:
		if in.Status.KVCacheTokens != nil {
			if err := p.setStatus(in, func(s *v1alpha1.InstanceStatus) { s.KVCacheTokens = nil }); err != nil {
				return err
			}
		}
		p.r.Backend.setKVRecord(alloc.UID, in.UID, nil)
	}
	return nil
}

// kvRecord returns a copy of the record of an Instance's value: the key of each worker it rests on.
func (b *Backend) kvRecord(alloc types.UID, inst types.UID) map[string]kvKey {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.agents[string(alloc)]
	if !ok {
		return nil
	}
	out := make(map[string]kvKey, len(s.kv[string(inst)]))
	for w, k := range s.kv[string(inst)] {
		out[w] = k
	}
	return out
}

// setKVRecord replaces the record of an Instance's value; an empty one forgets it. The record lives with
// the Allocation's state, which prune drops once the Allocation is gone.
func (b *Backend) setKVRecord(alloc types.UID, inst types.UID, took map[string]kvKey) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, ok := b.agents[string(alloc)]
	if !ok {
		return
	}
	if len(took) == 0 {
		delete(s.kv, string(inst))
		return
	}
	if s.kv == nil {
		s.kv = map[string]map[string]kvKey{}
	}
	s.kv[string(inst)] = took
}
