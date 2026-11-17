package lifecycle

// The suspension's caps err toward recording only (HBEP-7, The throughput gate: a suspect reference; the science
// agent's ruling on #652). A miss that the full status.referenceMisses (32) can't hold suspends its entry at once,
// evicting no other entry's miss; a full status.suspendedReferences (32) makes the gate record only for the
// deployment, with one event when it fills, and never drops a listing. The other entries are seeded as a controller
// that ran before would have left them, against a profile none of the deployment's Providers uses now, so no pass
// can lift them; the fixture is gate_rig_test.go's, with an engine that measures 41 ms against a 20 ms entry (limit
// 40 ms). Every value is a literal worked out by hand.

import (
	"fmt"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// otherProfile is a profile none of the rig deployment's Providers uses, so its seeded items are never lifted.
const otherProfile = "example-other-profile"

// otherKey is a key of otherProfile's n-th entry: the fixture key with n sessions.
func otherKey(n int) v1alpha1.ReferenceKey {
	k := gateKey()
	k.Sessions = int32(n)
	return k
}

// seedStatus writes the deployment's record and listing as a controller that ran before would have left them.
func seedStatus(rg *rig, misses []v1alpha1.ReferenceMiss, listed []v1alpha1.SuspendedReference) {
	rg.t.Helper()
	md := rg.md("md")
	md.Status.ReferenceMisses, md.Status.SuspendedReferences = misses, listed
	if err := rg.c.Status().Update(rg.ctx, md); err != nil {
		rg.t.Fatal(err)
	}
}

// fullEvents are the ReferenceSuspect events that say the listing is full.
func fullEvents(rg *rig) int {
	n := 0
	for _, e := range suspectEvents(rg) {
		if strings.Contains(e.Message, "is full (32)") {
			n++
		}
	}
	return n
}

// The record is full: 32 other entries each hold one pending miss. The deployment's own entry then gets its first
// miss, which the record can't hold: it suspends the entry at once, and evicts none of the 32. So one Allocation is
// released, not two, and the replacement records Suspended.
func TestGateAMissTheFullRecordCantHoldSuspendsItsEntry(t *testing.T) {
	log := newCauseLog()
	rg := newRig(t, withInterceptors(log.funcs()))
	rg.engine = gateSequence(41 * time.Millisecond)
	_, id := gateEntries(rg, gateMD(), oneEntry())
	at := metav1.NewTime(rg.clock.Now())
	var misses []v1alpha1.ReferenceMiss
	for n := 1; n <= 32; n++ {
		misses = append(misses, v1alpha1.ReferenceMiss{PlatformProfile: otherProfile, ReferenceKey: otherKey(n), Date: "2026-10-23",
			InstanceUID: types.UID(fmt.Sprintf("seed-instance-%d", n)), AllocationUID: types.UID(fmt.Sprintf("seed-allocation-%d", n)), Time: at})
	}
	seedStatus(rg, misses, nil)

	startUntil(rg, "md", time.Hour, "the first release", func() bool { return gateReleases(log, rigProvider+"-") == 1 })
	st := rg.md("md").Status
	if len(st.SuspendedReferences) != 1 || st.SuspendedReferences[0].PlatformProfile != rigProfile || !sameKey(st.SuspendedReferences[0].ReferenceKey, suspectKey(id)) {
		t.Fatalf("suspendedReferences %+v at the first release, want the deployment's entry suspended at once", st.SuspendedReferences)
	}
	if len(st.ReferenceMisses) != 32 {
		t.Fatalf("%d recorded misses, want the 32 seeded ones: a full record evicts none", len(st.ReferenceMisses))
	}
	for i, m := range st.ReferenceMisses {
		if m.PlatformProfile != otherProfile || m.Sessions != int32(i+1) {
			t.Errorf("recorded miss %d is %+v, want the seeded one of %s with %d sessions", i, m, otherProfile, i+1)
		}
	}
	startUntil(rg, "md", 4*time.Hour, "an Instance ready", func() bool { return rg.readyCount("md") == 1 })
	rg.run(time.Hour, 10*time.Second)
	if n := gateReleases(log, rigProvider+"-"); n != 1 {
		t.Errorf("%d Allocations released for QualificationFailed, want exactly one: the first miss suspended the entry", n)
	}
	for _, in := range rg.liveInstances("md") {
		if g := in.Status.Gate; in.Status.Phase == v1alpha1.InstanceReady && (g == nil || g.Verdict != v1alpha1.GateSuspended) {
			t.Errorf("the ready Instance %s has gate %+v, want Suspended", in.Name, g)
		}
	}
}

// The listing fills: 31 other entries are listed, and the deployment's 8-session entry holds a pending miss from an
// Allocation long gone. The Instance misses twice; its count suspends the entry, which fills the listing (32), so one
// event says the entry is suspended and one that the listing is full. With the workload edited to 16 sessions and
// instances raised to 2, the new Instance runs against the 16-session entry, which nothing has suspended: the gate
// records only for the deployment, with no restart and no release, and no listing is dropped.
func TestGateAFullListingMakesTheGateRecordOnly(t *testing.T) {
	log := newCauseLog()
	rg := newRig(t, withInterceptors(log.funcs()))
	rg.engine = gateSequence(41 * time.Millisecond)
	first, id := gateEntries(rg, gateMD(), func(id string) []v1alpha1.ExpectedPerformance {
		return []v1alpha1.ExpectedPerformance{gateEntryFor(id), gateEntryFor(id, func(e *v1alpha1.ExpectedPerformance) { e.Sessions = 16 })}
	})
	at := metav1.NewTime(rg.clock.Now())
	var listed []v1alpha1.SuspendedReference
	for n := 1; n <= 31; n++ {
		listed = append(listed, v1alpha1.SuspendedReference{PlatformProfile: otherProfile, ReferenceKey: otherKey(n), Date: "2026-10-23", SuspendedTime: at})
	}
	seedStatus(rg, []v1alpha1.ReferenceMiss{{PlatformProfile: rigProfile, ReferenceKey: suspectKey(id), Date: "2026-10-23",
		InstanceUID: "seed-instance", AllocationUID: "seed-allocation", Time: at}}, listed)

	startUntil(rg, "md", time.Hour, "the first Instance failed", func() bool { return ended(rg.instance(first)) })
	rg.clock.Advance(10 * time.Second)
	rg.Step()
	st := rg.md("md").Status
	if len(st.SuspendedReferences) != 32 || st.SuspendedReferences[31].PlatformProfile != rigProfile || st.SuspendedReferences[31].Sessions != 8 {
		t.Fatalf("%d listings, the last %+v; want 32, the deployment's 8-session entry last", len(st.SuspendedReferences), st.SuspendedReferences[len(st.SuspendedReferences)-1])
	}
	if n := fullEvents(rg); n != 1 || len(suspectEvents(rg)) != 2 {
		t.Errorf("%d ReferenceSuspect events, %d saying the listing is full; want two, one of them", len(suspectEvents(rg)), n)
	}

	rg.mutateMD("md", func(m *v1alpha1.ModelDeployment) {
		m.Spec.Workload.TargetSessions = 16
		m.Spec.Instances = 2
	})
	startUntil(rg, "md", 4*time.Hour, "two Instances ready", func() bool { return rg.readyCount("md") == 2 })
	rg.run(time.Hour, 10*time.Second)
	var sixteen *v1alpha1.Instance
	for _, in := range rg.liveInstances("md") {
		if g := in.Status.Gate; g != nil && g.Verdict == v1alpha1.GateRecordOnly {
			sixteen = &in
		}
	}
	if sixteen == nil {
		t.Fatalf("no Instance recorded RecordOnly: the 16-session one was gated though the listing is full")
	}
	if msg := conditionMessage(sixteen, v1alpha1.ConditionQualified); !strings.Contains(msg, "list of suspended entries is full (32)") || len(sixteen.Status.Restarts) != 0 {
		t.Errorf("the 16-session Instance: Qualified %q, restarts %v; want the full listing named and no restart", msg, gateRestartSignals(sixteen))
	}
	if n := gateReleases(log, rigProvider+"-"); n != 1 {
		t.Errorf("%d Allocations released, want the one before the listing filled", n)
	}
	got := rg.md("md").Status.SuspendedReferences
	if len(got) != 32 {
		t.Fatalf("%d listings, want 32: none is dropped", len(got))
	}
	for i := range 31 {
		if got[i].PlatformProfile != otherProfile || got[i].Sessions != int32(i+1) {
			t.Errorf("listing %d is %+v, want the seeded one of %s with %d sessions", i, got[i], otherProfile, i+1)
		}
	}
	if n := fullEvents(rg); n != 1 {
		t.Errorf("%d events say the listing is full, want the one when it filled", n)
	}
}
