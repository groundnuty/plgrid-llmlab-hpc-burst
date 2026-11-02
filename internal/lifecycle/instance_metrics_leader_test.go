package lifecycle

import (
	"maps"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// The tests in this file cover HBEP-8, Unknown samples, for #249: the leader
// builds the Instance inventory from its informer cache, not from its
// reconciles, so a new leader lists every live Instance at once, each unknown
// until its first report to it; and a standby emits no Instance series. The
// store of the rig stands for the cache. The clock is the rig's fake one, so
// no test waits for time to pass: each waits for state.

// infoLabels returns the labels of the info series of each Instance, by UID.
func infoLabels(ex exposition) map[string]map[string]string {
	out := map[string]map[string]string{}
	for _, s := range ex.find("hpcburst_instance_info", nil) {
		out[s.labels["instance_uid"]] = s.labels
	}
	return out
}

// TestMetricsStandbyEmitsNoInstanceSeries: a replica that does not lead emits
// none of the Instance series, whatever the store holds, so two replicas never
// give duplicate series; the same replica, once elected, lists every live
// Instance at once (HBEP-8, Unknown samples; `@hpc-burst-science-agent`,
// 2 Nov 2026, for #249). The standby is a collector with a Backend of its own,
// which holds no report; the control is that it emits when it leads.
func TestMetricsStandbyEmitsNoInstanceSeries(t *testing.T) {
	rg, in, _ := healthRig(t, func() afake.Engine { return afake.Engine{Health: knownHealth} })
	uid := string(in.UID)
	var leading atomic.Bool
	standby := &instanceCollector{cache: rg.c, backend: &Backend{}, clock: rg.clock, serving: leading.Load}
	if ex := scrapeCollectors(t, standby); len(ex.samples) != 0 {
		t.Fatalf("a standby emitted %d Instance samples, want none: %v", len(ex.samples), ex.samples)
	}
	// The leader of the same store shows the Instance, so "none" is not an
	// empty store.
	if got := len(scrape(t, rg).find("hpcburst_instance_info", map[string]string{"instance_uid": uid})); got != 1 {
		t.Fatalf("the leader has %d info series for the ready Instance, want 1; the case proves nothing", got)
	}
	leading.Store(true)
	ex := scrapeCollectors(t, standby)
	if len(ex.find("hpcburst_instance_info", map[string]string{"instance_uid": uid})) != 1 {
		t.Fatal("no info series on the elected replica")
	}
	if u := ex.unknowns(uid, "worker-0"); len(u) != 1 || !slices.Equal(u[unknownStale], sorted(engineNames)) {
		t.Fatalf("unknown series %v on the elected replica, want every engine metric of worker-0 with reason stale", u)
	}
	leading.Store(false)
	if ex := scrapeCollectors(t, standby); len(ex.samples) != 0 {
		t.Fatalf("a replica that lost the Lease emitted %d Instance samples, want none", len(ex.samples))
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
