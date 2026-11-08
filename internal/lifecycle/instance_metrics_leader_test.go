package lifecycle

import (
	"context"
	"maps"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// The tests in this file cover HBEP-8, Unknown samples, for #249: the leader
// builds the Instance inventory from its informer cache, not from its
// reconciles, so a new leader lists every live Instance at once, each unknown
// until its first report to it; a copy that does not serve exports no
// hpcburst_ series; and a report to this leader supersedes a persisted
// AgentUnreachable. The store of the rig stands for the cache. The clock is
// the rig's fake one, so no test waits for time to pass: each waits for state.

// infoLabels returns the labels of the info series of each Instance, by UID.
func infoLabels(ex exposition) map[string]map[string]string {
	out := map[string]map[string]string{}
	for _, s := range ex.find("hpcburst_instance_info", nil) {
		out[s.labels["instance_uid"]] = s.labels
	}
	return out
}

// gateStore is a store with what each of the collector's reads exports: one
// Instance with no agent yet, and one Allocation whose accounting is known.
func gateStore(t *testing.T) client.Client {
	t.Helper()
	read := metav1.NewTime(time.Date(2026, 11, 1, 8, 0, 0, 0, time.UTC))
	in := &v1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: "inst-pending", UID: "uid-pending",
			OwnerReferences: []metav1.OwnerReference{{Kind: "ModelDeployment", Name: "md", Controller: ptrTo(true)}}},
		Status: v1alpha1.InstanceStatus{Phase: v1alpha1.InstancePending},
	}
	return allocationReader(t, in, accountedAllocation("team-a", "alloc-1", "helios-gpu",
		&v1alpha1.AllocationAccounting{AllocatedGPUs: i32(4), AllocatedCPUs: i32(64), ElapsedSeconds: i64(3600), ReadTime: read}))
}

// hpcburstSamples returns the samples whose name has the hpcburst_ prefix.
func hpcburstSamples(ex exposition) []expoSample {
	var out []expoSample
	for _, s := range ex.samples {
		if strings.HasPrefix(s.name, "hpcburst_") {
			out = append(out, s)
		}
	}
	return out
}

// checkServes fails unless the scrape has everything gateStore's state gives a
// serving copy: both success gauges at 1, the Instance's info series and its
// unknown samples, and the accounting gauges.
func checkServes(t *testing.T, when string, ex exposition) {
	t.Helper()
	for _, c := range []string{collectorInstances, collectorAccounting} {
		if got := collectorSuccess(t, ex, c); got != "1" {
			t.Fatalf("%s: hpcburst_collector_success{collector=%s} %s, want 1", when, c, got)
		}
	}
	if len(ex.find("hpcburst_instance_info", map[string]string{"instance_uid": "uid-pending"})) != 1 {
		t.Fatalf("%s: no info series of the Instance", when)
	}
	if u := ex.unknowns("uid-pending", "worker-0"); len(u) != 1 || !slices.Equal(u[unknownStale], sorted(engineNames)) {
		t.Fatalf("%s: unknown series %v, want every engine metric of worker-0 stale", when, u)
	}
	if n := len(ex.find("hpcburst_allocation_allocated_gpu_seconds", nil)) + len(ex.find("hpcburst_allocation_allocated_cpu_seconds", nil)); n != 2 {
		t.Fatalf("%s: %d accounting series, want 2", when, n)
	}
}

// TestMetricsACopyThatDoesNotServeExportsNothing covers the collector's gate:
// a copy whose serving check fails exports no hpcburst_ series at all, no
// Instance series, no accounting series and neither success gauge; once it
// serves it exports them all, and when it stops serving, none again (HBEP-8,
// Unknown samples; `@hpc-burst-science-agent`, 6 Nov 2026, for #249). The
// serving check is a test switch here, so this covers only what the collector
// does with it. That a real standby's check fails and its /metrics has no
// hpcburst_ series is TestMetricsStandbyOnTwoManagers, on two real managers.
func TestMetricsACopyThatDoesNotServeExportsNothing(t *testing.T) {
	var serving atomic.Bool
	started := time.Now()
	c := newStateCollector(gateStore(t), &Backend{}, SystemClock{}, func() time.Time { return started }, serving.Load)
	if got := hpcburstSamples(scrapeCollectors(t, c)); len(got) != 0 {
		t.Fatalf("a copy that does not serve exported %d hpcburst_ samples, want none: %v", len(got), got)
	}
	serving.Store(true)
	checkServes(t, "once it serves", scrapeCollectors(t, c))
	serving.Store(false)
	if got := hpcburstSamples(scrapeCollectors(t, c)); len(got) != 0 {
		t.Fatalf("a copy that stopped serving exported %d hpcburst_ samples, want none: %v", len(got), got)
	}
}

// hookReader runs onList before each List of Instances it passes on.
type hookReader struct {
	client.Reader
	onList func()
}

func (h hookReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*v1alpha1.InstanceList); ok {
		h.onList()
	}
	return h.Reader.List(ctx, list, opts...)
}

// TestMetricsACopyThatStopsServingDuringTheReadsExportsNothing: the collector
// reads serving again after its reads, and a copy that lost its Lease while it
// read exports nothing of that scrape, the success gauges included (PR #254
// review, B2: the check ran only once, before the reads). The control is the
// same scrape with serving left on.
func TestMetricsACopyThatStopsServingDuringTheReadsExportsNothing(t *testing.T) {
	var serving, flip atomic.Bool
	reader := hookReader{Reader: gateStore(t), onList: func() {
		if flip.Load() {
			serving.Store(false)
		}
	}}
	started := time.Now()
	c := newStateCollector(reader, &Backend{}, SystemClock{}, func() time.Time { return started }, serving.Load)
	serving.Store(true)
	checkServes(t, "a scrape while the copy serves throughout", scrapeCollectors(t, c))
	flip.Store(true)
	serving.Store(true)
	if got := hpcburstSamples(scrapeCollectors(t, c)); len(got) != 0 {
		t.Fatalf("a copy that stopped serving during its reads exported %d hpcburst_ samples, want none: %v", len(got), got)
	}
}

// TestMetricsAFreshReportSupersedesAPersistedUnreachable (PR #254 review, B3):
// the previous leader judged the agent unreachable and persisted
// AgentUnreachable=True. A valid fresh report to the new leader shows real
// values at once, before any reconcile of the new leader could clear the
// condition: the report this leader received supersedes the persisted
// judgement. No reconcile runs after the restart, only the agents' steps, and
// the store still says True at the scrape.
func TestMetricsAFreshReportSupersedesAPersistedUnreachable(t *testing.T) {
	rg, in, allocName := healthRig(t, func() afake.Engine { return afake.Engine{Health: knownHealth} })
	uid := string(in.UID)
	ra := rg.agentOf(allocName)
	// The old leader's passes judge the silent agent unreachable.
	ra.stopped = true
	rg.until(10*time.Minute, startTick, "the old leader persists AgentUnreachable=True", func() bool {
		return isTrue(rg.instance(in.Name).Status.Conditions, v1alpha1.ConditionAgentUnreachable)
	})
	if u := scrape(t, rg).unknowns(uid, "worker-0"); len(u) != 1 || !slices.Equal(u[unknownAgentUnreachable], sorted(engineNames)) {
		t.Fatalf("unknown series %v on the old leader, want every engine metric of worker-0 agent_unreachable; the case proves nothing", u)
	}

	// The hand-over: a new Reconciler and Backend over the same store. From
	// here only the agent steps, with the clock; no reconcile runs.
	rg.restartController()
	ra.stopped = false
	alloc, ok := rg.allocation(allocName)
	if !ok {
		t.Fatalf("no Allocation %s", allocName)
	}
	for end := rg.clock.Now().Add(10 * time.Minute); ; {
		if rep, _, from := rg.backend.latest(alloc.UID); rep != nil && from != "" {
			break
		}
		if !rg.clock.Now().Before(end) {
			t.Fatalf("no report of the Instance reached the new leader within 10 min of agent steps; errors %v", rg.errs)
		}
		rg.clock.Advance(startTick)
		rg.syncAgents()
	}
	if !isTrue(rg.instance(in.Name).Status.Conditions, v1alpha1.ConditionAgentUnreachable) {
		t.Fatal("AgentUnreachable is no longer True in the store; the case proves nothing")
	}
	ex := scrape(t, rg)
	if got := len(ex.engineSeries(uid)); got != 9 {
		t.Errorf("%d engine samples after the first fresh report to the new leader, want all 9", got)
	}
	if u := ex.unknowns(uid, "worker-0"); len(u) != 0 {
		t.Errorf("unknown series %v after the first fresh report to the new leader, want none: the persisted judgement won", u)
	}
}

// TestMetricsHandOverListsEveryLiveInstanceAtOnce: a new leader, with no
// report from any agent and before its first reconcile, has the info series of
// every Instance that has not ended, with the labels the old leader gave them,
// and every engine metric of worker-0 unknown with reason stale. An ended
// Instance is not listed. Once an agent reports to the new leader, its
// Instance's values are real and none is unknown; the Instance still waiting
// for an agent stays unknown (HBEP-8, Unknown samples; `@hpc-burst-science-agent`,
// 2 Nov 2026, for #249). The fixture has three Instances: one that reported to
// the old leader, one pending with no agent, and one ended.
func TestMetricsHandOverListsEveryLiveInstanceAtOnce(t *testing.T) {
	rg, ready, readyAlloc := healthRig(t, func() afake.Engine { return afake.Engine{Health: knownHealth} })
	// A second deployment whose job runs but whose agent never registers: a
	// pending Instance.
	rg.noAgents = true
	rg.create(mdSpec("md-pending"))
	rg.until(10*time.Minute, startTick, "a pending Instance of md-pending", func() bool { return len(rg.instances("md-pending")) == 1 })
	// A third whose Instance ends: its deployment is deleted before the agent
	// ever registers.
	rg.create(mdSpec("md-ended"))
	rg.until(10*time.Minute, startTick, "an Instance of md-ended", func() bool { return len(rg.instances("md-ended")) == 1 })
	if err := rg.c.Delete(rg.ctx, rg.md("md-ended")); err != nil {
		t.Fatal(err)
	}
	rg.until(10*time.Minute, startTick, "the Instance of md-ended ended", func() bool {
		insts := rg.instances("md-ended")
		return len(insts) == 1 && ended(&insts[0])
	})
	pending := rg.instances("md-pending")[0]
	gone := rg.instances("md-ended")[0]
	if pending.Status.Phase != v1alpha1.InstancePending || pending.Status.Agent != nil {
		t.Fatalf("the pending Instance is %s with agent %+v; the case proves nothing", pending.Status.Phase, pending.Status.Agent)
	}
	if len(rg.agents) != 1 {
		t.Fatalf("%d agents run, want only the ready Instance's", len(rg.agents))
	}
	uids := []string{string(ready.UID), string(pending.UID)}

	// The old leader: real values for the ready Instance, and the labels of
	// both live ones.
	old := scrape(t, rg)
	if got := len(old.engineSeries(uids[0])); got != 9 {
		t.Fatalf("the old leader has %d engine samples for the ready Instance, want all 9; the case proves nothing", got)
	}
	oldInfo := infoLabels(old)
	if len(oldInfo) != 2 || oldInfo[uids[0]] == nil || oldInfo[uids[1]] == nil || oldInfo[uids[0]]["gpu_model"] != rigGPU {
		t.Fatalf("the old leader's info series %v, want the two live Instances, with gpu_model %s", oldInfo, rigGPU)
	}

	// The hand-over: a new Reconciler and Backend over the same store, and no
	// pass has run.
	rg.noAgents = false
	rg.restartController()
	ex := scrape(t, rg)
	got := infoLabels(ex)
	if len(got) != len(oldInfo) {
		t.Fatalf("the new leader lists %d info series, want %d (the live Instances; the ended one %s is not one): %v", len(got), len(oldInfo), gone.Name, got)
	}
	for uid, want := range oldInfo {
		if !maps.Equal(got[uid], want) {
			t.Errorf("info labels of %s on the new leader %v, want the old leader's %v", uid, got[uid], want)
		}
	}
	for _, uid := range uids {
		if series := ex.engineSeries(uid); len(series) != 0 {
			t.Errorf("engine series %v of %s on the new leader before any report; an unknown value is absent", series, uid)
		}
		if u := ex.unknowns(uid, "worker-0"); len(u) != 1 || !slices.Equal(u[unknownStale], sorted(engineNames)) {
			t.Errorf("unknown series %v of %s on the new leader, want every engine metric of worker-0 with reason stale", u, uid)
		}
	}
	for _, s := range ex.samples {
		if s.labels["instance_uid"] == string(gone.UID) {
			t.Fatalf("series %v of an ended Instance", s)
		}
	}

	// The ready Instance's agent registers with the new leader and reports.
	alloc, ok := rg.allocation(readyAlloc)
	if !ok {
		t.Fatalf("no Allocation %s", readyAlloc)
	}
	rg.until(10*time.Minute, startTick, "a report of the ready Instance to the new leader", func() bool {
		rep, _, from := rg.backend.latest(alloc.UID)
		return rep != nil && from != ""
	})
	ex = scrape(t, rg)
	if got := len(ex.engineSeries(uids[0])); got != 9 {
		t.Errorf("%d engine samples for the ready Instance after its report, want all 9", got)
	}
	if u := ex.unknowns(uids[0], "worker-0"); len(u) != 0 {
		t.Errorf("unknown series %v for the ready Instance after its report, want none", u)
	}
	if u := ex.unknowns(uids[1], "worker-0"); len(u) != 1 || !slices.Equal(u[unknownStale], sorted(engineNames)) {
		t.Errorf("unknown series %v for the pending Instance, want it still unknown with reason stale", u)
	}
}
