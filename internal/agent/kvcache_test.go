package agent_test

// The KV cache size in a worker's health (agentapi.Health.KVCacheTokens; HBEP-7, The KV cache size;
// `@hpc-burst-science-agent`, 18 Nov 2026, #764, Q4): read once per engine process through the real
// engine client, carried in every sample past a failed /metrics read, and unknown again in a new
// process until its own read. The engine double's size is a literal here, 400,000, typed by hand.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// switchedMetrics fails every metrics read while fail is set, and counts the reads it failed, so a test
// shows its failed read reached the agent: a sample that was merely late would look the same.
type switchedMetrics struct {
	agent.Engine
	fail   *atomic.Bool
	failed *atomic.Int64
}

func (e switchedMetrics) Sample(ctx context.Context) (agentapi.Health, error) {
	if e.fail.Load() {
		e.failed.Add(1)
		return agentapi.Health{}, context.DeadlineExceeded
	}
	return e.Engine.Sample(ctx)
}

// A worker reads 400,000, and then its metrics read fails: its next sample still carries 400,000, with
// every gauge left out, which is what a failed read does (the path of recordHealth that replaces the
// sample). Its process then restarts at a new generation, and while the read keeps failing the new
// process carries nil, not the old one's value and not 0. When the read works again, the new process
// carries its own: 400,000 again, from the double of the same mode.
func TestTheKVCacheSizeIsCarriedPastAFailedReadAndStartsUnknownInANewProcess(t *testing.T) {
	var fail atomic.Bool
	var failed atomic.Int64
	h := newHarness(t, harnessOptions{engineWrap: func(e agent.Engine) agent.Engine { return switchedMetrics{e, &fail, &failed} }})
	h.set(h.runState("kv-tokens=400000", 1, 0, nil))

	first := h.waitReport("a sample with the KV cache size", 20*time.Second, func(r agentapi.Report) bool {
		w := worker0(r)
		return w != nil && w.Health != nil && w.Health.MetricsReadAt != nil && w.Health.KVCacheTokens != nil
	})
	fw := worker0(first)
	if *fw.Health.KVCacheTokens != 400000 {
		t.Fatalf("KVCacheTokens %d, want 400000", *fw.Health.KVCacheTokens)
	}
	pid, readAt := fw.PID, *fw.Health.MetricsReadAt

	// The read fails from now on. A sample taken after it, whose gauges are all gone, still carries the size.
	fail.Store(true)
	r := h.waitReport("a sample after the failed reads", 20*time.Second, func(r agentapi.Report) bool {
		w := worker0(r)
		return w != nil && w.Health != nil && w.Health.Time.After(readAt.Add(300*time.Millisecond)) && w.Health.RunningRequests == nil
	})
	if failed.Load() == 0 {
		t.Fatal("no metrics read failed: the sample is not the failed-read path's")
	}
	hl := worker0(r).Health
	if hl.GeneratedTokens != nil || hl.KVCacheUsage != nil || hl.ResponsesByStatus != nil {
		t.Fatalf("gauges reported after the metrics read failed: %+v", hl)
	}
	if hl.KVCacheTokens == nil || *hl.KVCacheTokens != 400000 {
		t.Fatalf("after a failed read KVCacheTokens %v, want the carried 400000", hl.KVCacheTokens)
	}

	// The worker's process restarts at generation 1, and its metrics are still unreadable.
	h.set(h.runState("kv-tokens=400000", 1, 1, nil))
	r = h.waitReport("worker-0 restarted at generation 1", 20*time.Second, func(r agentapi.Report) bool {
		w := worker0(r)
		return w != nil && w.Phase == agentapi.PartRunning && w.Generation == 1 && w.PID != pid && w.Health != nil
	})
	if w := worker0(r); w.Health.KVCacheTokens != nil || w.Health.RunningRequests != nil {
		t.Fatalf("a new process whose reads all fail reports KVCacheTokens %v and running %v, want both nil", w.Health.KVCacheTokens, w.Health.RunningRequests)
	}
	newPID := worker0(r).PID

	// Its reads work: the new process carries its own size.
	fail.Store(false)
	r = h.waitReport("the new process's own read", 20*time.Second, func(r agentapi.Report) bool {
		w := worker0(r)
		return w != nil && w.PID == newPID && w.Health != nil && w.Health.KVCacheTokens != nil
	})
	if v := *worker0(r).Health.KVCacheTokens; v != 400000 {
		t.Fatalf("the new process's KVCacheTokens %d, want 400000", v)
	}
}

// An engine that exports no vllm:cache_config_info (the double's no-cache-config-info) reports a healthy
// worker with every gauge known and the KV cache size nil, in every sample: unknown, not 0.
func TestAnEngineWithoutCacheConfigInfoReportsNoKVCacheSize(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.set(h.runState("no-cache-config-info", 1, 0, nil))
	r := h.waitReport("a sample with the gauges", 20*time.Second, func(r agentapi.Report) bool {
		w := worker0(r)
		return w != nil && w.Health != nil && w.Health.MetricsReadAt != nil && w.Health.RunningRequests != nil
	})
	if w := worker0(r); w.Health.KVCacheTokens != nil {
		t.Fatalf("KVCacheTokens %d, want nil", *w.Health.KVCacheTokens)
	}
	// A later sample too.
	read := *worker0(r).Health.MetricsReadAt
	r = h.waitReport("a later read", 20*time.Second, func(r agentapi.Report) bool {
		w := worker0(r)
		return w != nil && w.Health != nil && w.Health.MetricsReadAt != nil && w.Health.MetricsReadAt.After(read)
	})
	if w := worker0(r); w.Health.KVCacheTokens != nil {
		t.Fatalf("a later sample: KVCacheTokens %d, want nil", *w.Health.KVCacheTokens)
	}
}
