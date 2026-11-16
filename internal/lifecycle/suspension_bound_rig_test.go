package lifecycle

// At most two releases for one reference (HBEP-7, Over-allocation and qualification (beta), The throughput gate: a
// suspect reference, At most two releases for one reference; #616, PR 2's review). Each row is a way a wrong entry
// could release more than two Allocations: Instances placed on warm Allocations, whose failures add no start-up
// outcome; two Providers of one class, each with its own profile, whose outcomes would pool into one run; and one
// profile's two entries with one date, for two shapes, which a workload edit and back would pool the same way. The
// fixture is gate_rig_test.go's (max-num-seqs 16, 8 sessions, contexts of 1,024 and outputs of 128 tokens), and every
// entry and measurement is a literal worked out by hand. Each row counts the Allocations released for
// QualificationFailed and asserts at most two.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// setEntries replaces the expected-performance entries of the named PlatformProfile.
func setEntries(rg *rig, profile string, entries ...v1alpha1.ExpectedPerformance) {
	rg.t.Helper()
	var prof v1alpha1.PlatformProfile
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Name: profile}, &prof); err != nil {
		rg.t.Fatal(err)
	}
	prof.Spec.ExpectedPerformance = entries
	if err := rg.c.Update(rg.ctx, &prof); err != nil {
		rg.t.Fatal(err)
	}
}

// gateReleases is how many Allocations whose name starts with prefix were deleted with a QualificationFailed release
// cause, read from the cause log, which outlives the Instances (an ended Instance is pruned after restart.window).
func gateReleases(log *causeLog, prefix string) int {
	log.mu.Lock()
	defer log.mu.Unlock()
	n := 0
	for name, cause := range log.cause {
		if strings.HasPrefix(name, prefix) && strings.HasSuffix(cause, "failed: "+v1alpha1.ReasonQualificationFailed) {
			n++
		}
	}
	return n
}

// A wrong entry and Instances placed on warm Allocations. W+1 Instances are ready with no entry (each records only),
// then instances goes W+1 -> 1: W drain, and their Allocations are kept warm (retention Keep, the default, with no idle
// limit). A wrong entry is then added (20 ms per output token, limit 40 ms; every warm-up measures 41 ms), and
// instances goes back to W+1: W Instances are placed on the W warm Allocations in one pass, and each misses twice, on
// its retry in one pass. A failure there adds no start-up outcome, since the Allocation already has one from its first
// Instance; if the gate did not count it either, the fresh Allocations that replace them would be gated and released
// as well, W+2 in all. With W = 3 the pass-local count must count the warm second misses it has made, or that pass
// alone releases three. At most two Allocations are released for the entry, and the W+1 end ready.
func TestGateAWrongEntryReleasesAtMostTwoWhenInstancesGoOnWarmAllocations(t *testing.T) {
	for _, w := range []int{2, 3} {
		t.Run(fmt.Sprintf("%d warm Allocations", w), func(t *testing.T) { warmAllocationsRow(t, w) })
	}
}

func warmAllocationsRow(t *testing.T, w int) {
	all := int32(w + 1)
	log := newCauseLog()
	rg := newRig(t, withInterceptors(log.funcs()))
	rg.engine = gateSequence(41 * time.Millisecond)
	rg.create(gateMD(func(md *v1alpha1.ModelDeployment) { md.Spec.Instances = all }))
	startUntil(rg, "md", time.Hour, "every Instance ready with no entry", func() bool { return rg.readyCount("md") == int(all) })
	id := rg.instances("md")[0].Spec.DeploymentID.Display()

	rg.mutateMD("md", func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1 })
	startUntil(rg, "md", time.Hour, "the others drained", func() bool { return len(rg.liveInstances("md")) == 1 })
	inUse := map[string]bool{}
	for _, in := range rg.liveInstances("md") {
		inUse[in.Spec.AllocationRef.Name] = true
	}
	warm := map[string]bool{}
	for _, a := range rg.allocations("md") {
		if !inUse[a.Name] && !rg.released(a.Name) {
			warm[a.Name] = true
		}
	}
	if len(warm) != w {
		t.Fatalf("%d warm Allocations after %d -> 1 under Keep, want %d: the row proves nothing", len(warm), all, w)
	}
	before := map[string]bool{}
	for _, in := range rg.instances("md") {
		before[in.Name] = true
	}

	setEntries(rg, rigProfile, gateEntryFor(id))
	rg.mutateMD("md", func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = all })
	onWarm := map[string]metav1.Time{} // new Instances placed on a warm Allocation, by creation, seen while they exist
	seeWarm := func() {
		for _, in := range rg.instances("md") {
			if !before[in.Name] && warm[in.Spec.AllocationRef.Name] {
				onWarm[in.Name] = in.CreationTimestamp
			}
		}
	}
	startUntil(rg, "md", 4*time.Hour, "every Instance ready again", func() bool { seeWarm(); return rg.readyCount("md") == int(all) })
	rg.run(time.Hour, 10*time.Second)

	if len(onWarm) != w {
		t.Fatalf("%d new Instances were placed on the warm Allocations, want %d: the row proves nothing", len(onWarm), w)
	}
	created := map[time.Time]bool{}
	for _, c := range onWarm {
		created[c.Time] = true
	}
	if len(created) != 1 {
		t.Fatalf("the warm Instances were created at %v, want one pass: their retries are not in one pass, so the row proves nothing", created)
	}
	if n := gateReleases(log, rigProvider+"-"); n > 2 {
		t.Errorf("%d Allocations released for QualificationFailed against one wrong entry, want at most two (HBEP-7, At most two releases for one reference)", n)
	}
	if n := rg.readyCount("md"); n != int(all) {
		t.Errorf("%d Instances ready an hour later, want %d", n, all)
	}
}

// A miss lost with its pass (Fable's same root): the first Instance's second miss fails it and releases its Allocation,
// and that pass's status write fails, so step 10 never records its outcome; the deployment's status writes keep
// failing until the Allocation's delete has finished, so no later pass can record it either (an outcome needs its
// Allocation). The miss is still the entry's first: the Instance itself is kept, with its verdict Missed, for
// restart.window (30 min) after its end. The replacement misses twice too and is the second; the third records
// Suspended. At most two releases.
func TestGateAMissWhosePassWasLostStillCounts(t *testing.T) {
	log := newCauseLog()
	f := log.funcs()
	var mu sync.Mutex
	failMD, failedOnce := false, false
	f.SubResourceUpdate = func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
		mu.Lock()
		if in, ok := obj.(*v1alpha1.Instance); ok && in.Status.EndReason == v1alpha1.ReasonQualificationFailed && !failedOnce {
			failMD, failedOnce = true, true
		}
		fail := failMD
		mu.Unlock()
		if _, ok := obj.(*v1alpha1.ModelDeployment); ok && fail {
			return apierrors.NewServiceUnavailable("the deployment's status write fails")
		}
		return c.SubResource(sub).Update(ctx, obj, opts...)
	}
	rg := newRig(t, withInterceptors(f))
	rg.engine = gateSequence(41 * time.Millisecond)
	first, _ := gateEntries(rg, gateMD(), oneEntry())
	alloc := rg.instance(first).Spec.AllocationRef.Name
	// Rig.Step fails the test on any reconcile error; this loop steps as it does, and lets through only the
	// injected status-write failure.
	for end := rg.clock.Now().Add(time.Hour); ; {
		if _, ok := rg.allocation(alloc); !ok {
			break
		}
		if !rg.clock.Now().Before(end) {
			t.Fatalf("the first Instance's Allocation still exists at %s", rg.clock.Now().Format(time.RFC3339))
		}
		stepLettingStatusFail(rg, "md")
	}
	mu.Lock()
	if !failedOnce {
		mu.Unlock()
		t.Fatal("the first Instance never failed: the row proves nothing")
	}
	failMD = false
	mu.Unlock()
	if outs := rg.md("md").Status.StartOutcomes; len(outs) != 0 {
		t.Fatalf("outcomes %+v recorded while the status writes failed: the row proves nothing", outs)
	}
	startUntil(rg, "md", 4*time.Hour, "an Instance ready", func() bool { return rg.readyCount("md") == 1 })
	rg.run(time.Hour, 10*time.Second)
	if n := gateReleases(log, rigProvider+"-"); n > 2 {
		t.Errorf("%d Allocations released for QualificationFailed against one wrong entry, want at most two: the first miss was lost with its pass", n)
	}
}

// stepLettingStatusFail is rig.Step for one deployment, which lets a reconcile fail on the injected status-write
// fault and fails the test on any other error.
func stepLettingStatusFail(rg *rig, md string) {
	rg.t.Helper()
	rg.clock.Advance(startTick)
	if err := rg.prov.Sync(rg.ctx); err != nil {
		rg.t.Fatalf("provider sync: %v", err)
	}
	rg.syncAgents()
	if err := rg.reconcileOne(md); err != nil && !strings.Contains(err.Error(), "the deployment's status write fails") {
		rg.t.Fatalf("reconcile %s at %s: %v", md, rg.clock.Now().Format(time.RFC3339), err)
	}
	if err := rg.r.checkHeartbeats(rg.ctx); err != nil {
		rg.t.Fatalf("checkHeartbeats: %v", err)
	}
}

// The control for the key of a run: two Providers sharing ONE profile hold one entry, so their misses are one run. Each
// Provider takes one Allocation at a time, and instances is 2, so one primary goes to each; every warm-up measures
// 41 ms against the shared wrong entry (20 ms, limit 40 ms). At most two releases in all: a run kept per Provider would
// give each Provider its own two.
func TestGateTwoProvidersSharingAProfileReleaseAtMostTwoInAll(t *testing.T) {
	log := newCauseLog()
	qos := []v1alpha1.QOSProfile{{Name: "example-a1", MaxSubmitPerUser: i32(1)}, {Name: "example-b1", MaxSubmitPerUser: i32(1)}}
	rg := newRig(t, append(qosOpts(qos, "", "example-a1"), withInterceptors(log.funcs()), withBatchConfig(func(c *pfake.Config) { c.MaxSubmitPerUser = 1 }))...)
	rg.addProvider(bProvider, bPartition, 48*time.Hour, func(pp *v1alpha1.PartitionProfile) { pp.QOS = "example-b1" }, func(c *pfake.Config) { c.MaxSubmitPerUser = 1 })
	rg.engine = gateSequence(41 * time.Millisecond)
	rg.create(gateMD(func(md *v1alpha1.ModelDeployment) {
		md.Spec.Instances = 2
		md.Spec.Providers.Names = []string{rigProvider, bProvider}
	}))
	startUntil(rg, "md", 5*time.Minute, "one Instance on each Provider", func() bool {
		return len(rg.onProvider("md", rigProvider)) == 1 && len(rg.onProvider("md", bProvider)) == 1
	})
	setEntries(rg, rigProfile, gateEntryFor(rg.instances("md")[0].Spec.DeploymentID.Display()))
	startUntil(rg, "md", 4*time.Hour, "an Instance ready on each Provider", func() bool { return rg.readyCount("md") == 2 })
	rg.run(time.Hour, 10*time.Second)
	a, b := gateReleases(log, rigProvider+"-"), gateReleases(log, bProvider+"-")
	if a+b > 2 {
		t.Errorf("%d releases on A and %d on B against one shared entry, want at most two in all", a, b)
	}
	if a+b == 0 {
		t.Errorf("no release on either Provider: the entry gated nothing, so the row proves nothing")
	}
}

// bProfile and bPartition are the second Provider's own PlatformProfile and partition, of the rig's accelerator class.
const (
	bProvider  = "example-b"
	bProfile   = "example-b-profile"
	bPartition = "gpu-b"
)

// addProviderWithProfile adds a Provider of the rig's accelerator class with a PlatformProfile of its own, holding one
// partition, and its fake Slurm configured by cfg.
func (rg *rig) addProviderWithProfile(name, profile, partition string, qos []v1alpha1.QOSProfile, cfg func(*pfake.Config)) *rigExtra {
	rg.t.Helper()
	var base v1alpha1.PlatformProfile
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Name: rigProfile}, &base); err != nil {
		rg.t.Fatal(err)
	}
	spec := *base.Spec.DeepCopy()
	spec.ExpectedPerformance, spec.QOS = nil, qos
	pp := base.Spec.Partitions[0]
	pp.Name = partition
	if len(qos) > 0 {
		pp.QOS = qos[0].Name
	}
	pp.MemPerGPU = ptrTo(resource.MustParse("120Gi"))
	spec.Partitions = []v1alpha1.PartitionProfile{pp}
	rg.create(&v1alpha1.PlatformProfile{ObjectMeta: metav1.ObjectMeta{Name: profile}, Spec: spec})
	rg.create(&v1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: newUID(), Generation: 1, Labels: map[string]string{v1alpha1.LabelPlatform: "helios"}},
		Spec:       v1alpha1.ProviderSpec{PlatformProfileRef: v1alpha1.ObjectReference{Name: profile}, AllowedPartitions: []string{partition}},
	})
	bcfg := pfake.Config{Partitions: []pfake.Partition{{Name: partition, GPUModel: rigGPU, GPUsPerNode: 4, MaxTime: 48 * time.Hour}}}
	if cfg != nil {
		cfg(&bcfg)
	}
	b := pfake.NewBatch(rg.clock, bcfg)
	x := &rigExtra{name: name, batch: b, prov: &pfake.Controller{Client: rg.c, Provider: name, Batch: b, Clock: rg.clock, HoldRelease: spec.HoldRelease}}
	rg.extra = append(rg.extra, x)
	return x
}

// Two Providers of one class, each with its own profile, and the entries of one reference run: one deployment ID (it
// is the class's), one shape and one date (2026-10-23), so one key. A's entry is wrong (10 ms per output token, limit
// 20 ms) and B's is right (20 ms, limit 40 ms); every warm-up measures 30 ms, so A's Instances miss and B's pass. Each
// Provider takes one Allocation at a time (a submit limit of 1, declared and enforced), and instances is 2, so the
// first primary goes to A and the second to B. B's job is held in its queue by another user's job on B's GPU cap until
// A's first failure, so B's pass comes between A's first and second failures: Missed (A), Passed (B), Missed (A). B's
// pass is against another profile's entry; if it broke A's run, A's third Instance would be released too. A's entry is
// suspended within two of A's releases, and listed.
func TestGateTwoProvidersOfOneClassSuspendAWrongEntryWithinTwoReleases(t *testing.T) {
	log := newCauseLog()
	qosA := []v1alpha1.QOSProfile{{Name: "example-a1", MaxSubmitPerUser: i32(1)}}
	rg := newRig(t, append(qosOpts(qosA, "", "example-a1"), withInterceptors(log.funcs()), withBatchConfig(func(c *pfake.Config) { c.MaxSubmitPerUser = 1 }))...)
	b := rg.addProviderWithProfile(bProvider, bProfile, bPartition, []v1alpha1.QOSProfile{{Name: "example-b1", MaxSubmitPerUser: i32(1)}},
		func(c *pfake.Config) { c.MaxSubmitPerUser, c.MaxGPUsPerUser = 2, 1 })
	other := b.batch.AddOutsideJob("example-other-job", bPartition, 1, 48*time.Hour)
	rg.engine = func(string) afake.Engine {
		return afake.Engine{WarmUpRound: gateRound(30*time.Millisecond, 300*time.Millisecond)}
	}
	rg.create(gateMD(func(md *v1alpha1.ModelDeployment) {
		md.Spec.Instances = 2
		md.Spec.Providers.Names = []string{rigProvider, bProvider}
	}))
	startUntil(rg, "md", 5*time.Minute, "one Instance on each Provider", func() bool {
		return len(rg.onProvider("md", rigProvider)) == 1 && len(rg.onProvider("md", bProvider)) == 1
	})
	id := rg.instances("md")[0].Spec.DeploymentID.Display()
	setEntries(rg, rigProfile, gateEntryFor(id, func(e *v1alpha1.ExpectedPerformance) { e.TimePerOutputToken = "10ms" }))
	setEntries(rg, bProfile, gateEntryFor(id))

	startUntil(rg, "md", 2*time.Hour, "A's first failure", func() bool { return gateReleases(log, rigProvider+"-") == 1 })
	if n := rg.readyCount("md"); n != 0 {
		t.Fatalf("%d Instances ready at A's first failure, want none: B's pass must come after it", n)
	}
	b.batch.CancelExternally(other)
	startUntil(rg, "md", time.Hour, "B's Instance ready", func() bool { return rg.readyCount("md") == 1 })
	if n := gateReleases(log, rigProvider+"-"); n != 1 {
		t.Fatalf("%d of A's Allocations released by B's pass, want 1: the run is not Missed, Passed, Missed", n)
	}
	startUntil(rg, "md", 4*time.Hour, "an Instance ready on each Provider", func() bool { return rg.readyCount("md") == 2 })
	rg.run(time.Hour, 10*time.Second)

	if n := gateReleases(log, rigProvider+"-"); n > 2 {
		t.Errorf("%d of A's Allocations released for QualificationFailed against A's wrong entry, want at most two: B's pass on its own profile's entry broke A's run", n)
	}
	if n := gateReleases(log, bProvider+"-"); n != 0 {
		t.Errorf("%d of B's Allocations released, want none: B's entry is right", n)
	}
	if got := rg.md("md").Status.SuspendedReferences; len(got) == 0 || got[0].Date != "2026-10-23" || got[0].DeploymentID != id {
		t.Errorf("suspendedReferences %+v, want A's entry dated 2026-10-23 listed", got)
	}
}

// One profile's two entries with one date, for two shapes: 8 sessions (20 ms per output token, limit 40 ms, which 41 ms
// misses) and 16 sessions (30 ms, limit 60 ms, which 41 ms passes). The first Instance runs 8 sessions and fails. The
// workload is then edited to 16 sessions: the second passes the other entry. Edited back to 8, and instances raised to
// 2, the third and fourth run 8 sessions against the first entry. Its two most recent Passed or Missed outcomes are
// both Missed once the third fails, since the second's pass was against another key; were the run counted by date
// alone, the pass would break it and the fourth would be released too. At most two releases.
func TestGateTwoShapesOfOneDateDoNotPoolIntoOneRun(t *testing.T) {
	log := newCauseLog()
	rg := newRig(t, withInterceptors(log.funcs()))
	rg.engine = gateSequence(41 * time.Millisecond)
	gateEntries(rg, gateMD(), func(id string) []v1alpha1.ExpectedPerformance {
		eight := gateEntryFor(id)
		sixteen := gateEntryFor(id, func(e *v1alpha1.ExpectedPerformance) { e.Sessions, e.TimePerOutputToken = 16, "30ms" })
		return []v1alpha1.ExpectedPerformance{eight, sixteen}
	})
	startUntil(rg, "md", time.Hour, "the first Instance failed", func() bool { return gateFailures(rg) == 1 })
	rg.mutateMD("md", func(m *v1alpha1.ModelDeployment) { m.Spec.Workload.TargetSessions = 16 })
	startUntil(rg, "md", time.Hour, "the second Instance ready", func() bool { return rg.readyCount("md") == 1 })
	var second v1alpha1.Instance
	for _, in := range rg.instances("md") {
		if in.Status.Phase == v1alpha1.InstanceReady {
			second = in
		}
	}
	if g := second.Status.Gate; g == nil || g.Verdict != v1alpha1.GatePassed || g.ReferenceDate != "2026-10-23" {
		t.Fatalf("the second Instance's gate %+v, want Passed against the 16-session entry dated 2026-10-23: the row proves nothing", g)
	}
	rg.mutateMD("md", func(m *v1alpha1.ModelDeployment) {
		m.Spec.Workload.TargetSessions = 8
		m.Spec.Instances = 2
	})
	startUntil(rg, "md", 4*time.Hour, "two Instances ready", func() bool { return rg.readyCount("md") == 2 })
	rg.run(time.Hour, 10*time.Second)
	if n := gateReleases(log, rigProvider+"-"); n > 2 {
		t.Errorf("%d Allocations released for QualificationFailed against the 8-session entry, want at most two: a pass against the 16-session entry broke its run", n)
	}
	got := rg.md("md").Status.SuspendedReferences
	if len(got) != 1 || got[0].Sessions != 8 || got[0].Date != "2026-10-23" {
		t.Errorf("suspendedReferences %+v, want the 8-session entry dated 2026-10-23 alone", got)
	}
}
