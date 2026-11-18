package lifecycle

// The KV cache size through the whole controller, the fake agent and the rig's fake clock: the call site
// in running (instances.go), the Backend's record, and a report that arrives each step (kvcache.go;
// HBEP-7, The KV cache size; `@hpc-burst-science-agent`, 18 Nov 2026, #764, Q4). The unit tests of the
// decision are in kvcache_test.go. Every expected size is a literal typed here.

import (
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

func kvTokensOf(rg *rig, name string) string { return kvString(rg.instance(name).Status.KVCacheTokens) }

// workerHealth is worker w's Health in the fake agent's last report, nil when it has none.
func lastWorkerHealth(ag *afake.Agent, w string) *agentapi.Health {
	rep, ok := ag.LastReport()
	if !ok || rep.Instance == nil {
		return nil
	}
	if wr := reportOf(rep.Instance, w); wr != nil {
		return wr.Health
	}
	return nil
}

// Two workers behind the router report 400000 (worker-0) and 380000 (worker-1): the Instance's field is
// 380000. The router ejects worker-1: it is 400000. Worker-1 back: 380000 again. Each step is checked by
// the report the fake agent last sent, so a field that is right because no report arrived shows.
func TestTwoWorkersKVCacheSizeFollowsTheRoutersHealthySet(t *testing.T) {
	sizes := map[string]int64{"worker-0": 400000, "worker-1": 380000}
	e := afake.Engine{Health: afake.HealthGauges(func(w string) afake.Gauges {
		return afake.Gauges{KVCache: 0.1, Flat: true, KVCacheTokens: sizes[w]}
	})}
	rg, ag, name := readyTwoWith(t, e)
	startUntil(rg, "md", 2*time.Minute, "the field written", func() bool { return kvTokensOf(rg, name) != "unset" })
	if h := lastWorkerHealth(ag, "worker-1"); h == nil || h.KVCacheTokens == nil || *h.KVCacheTokens != 380000 {
		t.Fatalf("worker-1's last report carries %+v, want KVCacheTokens 380000", h)
	}
	if got := kvTokensOf(rg, name); got != "380000" {
		t.Fatalf("two healthy workers reporting 400000 and 380000: the field is %s, want 380000", got)
	}

	ag.RouterView = func(rr *agentapi.RouterReport) {
		for i := range rr.Workers {
			if rr.Workers[i].Name == "worker-1" {
				rr.Workers[i].Healthy, rr.Workers[i].Ejected = false, true
			}
		}
	}
	startUntil(rg, "md", 2*time.Minute, "worker-1 out of the healthy set, healthyWorkers 1", func() bool {
		h := rg.instance(name).Status.HealthyWorkers
		return h != nil && *h == 1
	})
	startUntil(rg, "md", 2*time.Minute, "the field 400000", func() bool { return kvTokensOf(rg, name) == "400000" })
	if got := restartParts(rg.instance(name)); len(got) != 0 {
		t.Errorf("an ejection restarted %v, want nothing", got)
	}

	ag.RouterView = nil
	startUntil(rg, "md", 2*time.Minute, "worker-1 healthy again", func() bool {
		h := rg.instance(name).Status.HealthyWorkers
		return h != nil && *h == 2
	})
	startUntil(rg, "md", 2*time.Minute, "the field 380000", func() bool { return kvTokensOf(rg, name) == "380000" })
}

// One worker, no router: the field is its reported size, 400000. A report with no size, at the same
// generation, leaves it (the agent carries the size past a failed read, so a report without one is an
// agent that has none). The worker's process restarts at generation 1, with no size at first: the field
// is unset, and when the new process reports its own, 420000, that is the field.
func TestOneWorkersKVCacheSizeSurvivesASizelessReportAndNotARestart(t *testing.T) {
	silent, hold := false, false
	rg := newRig(t)
	rg.engine = func(string) afake.Engine {
		return afake.Engine{Health: func(now time.Time, p afake.Process) agentapi.Health {
			h := afake.DefaultHealth(now, p)
			switch {
			case silent || (p.Generation >= 1 && hold):
				h.KVCacheTokens = nil
			case p.Generation >= 1:
				h.KVCacheTokens = agentapi.Int64(420000)
			default:
				h.KVCacheTokens = agentapi.Int64(400000)
			}
			return h
		}}
	}
	rg.create(mdSpec("md"))
	in, alloc := startReady(rg)
	ag := rg.agentOf(alloc).agent
	startUntil(rg, "md", 2*time.Minute, "the field written", func() bool { return kvTokensOf(rg, in.Name) != "unset" })
	if got := kvTokensOf(rg, in.Name); got != "400000" {
		t.Fatalf("one worker reporting 400000: the field is %s", got)
	}

	// No size in the reports, the same generation: the field stays. The last report is shown to have none.
	silent = true
	for range 3 {
		startStep(rg, "md", startTick)
	}
	if h := lastWorkerHealth(ag, "worker-0"); h == nil || h.KVCacheTokens != nil {
		t.Fatalf("the last report carries %+v, want a Health with no KVCacheTokens", h)
	}
	if got := kvTokensOf(rg, in.Name); got != "400000" {
		t.Fatalf("after reports with no size at the same generation the field is %s, want 400000 left", got)
	}
	silent = false

	// The worker's process dies and restarts at generation 1, whose reports carry no size yet.
	hold = true
	ag.CrashWorker("worker-0", 1)
	startUntil(rg, "md", 10*time.Minute, "worker-0 running at generation 1 with no size", func() bool {
		rep, ok := ag.LastReport()
		if !ok || rep.Instance == nil {
			return false
		}
		w := reportOf(rep.Instance, "worker-0")
		return w != nil && w.Phase == agentapi.PartRunning && w.Generation == 1 && w.Health != nil && w.Health.KVCacheTokens == nil
	})
	startStep(rg, "md", startTick)
	if got := kvTokensOf(rg, in.Name); got != "unset" {
		t.Fatalf("worker-0 at generation 1 with no size: the field is %s, want unset", got)
	}
	if g := restartGeneration(rg.instance(in.Name), v1alpha1.WorkerName(0)); g != 1 {
		t.Fatalf("worker-0's restart generation is %d, want 1", g)
	}

	// Its own read.
	hold = false
	startUntil(rg, "md", 2*time.Minute, "the new process's size", func() bool { return kvTokensOf(rg, in.Name) == "420000" })
}
