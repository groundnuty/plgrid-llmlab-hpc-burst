package lifecycle

// HBEP-7's hedging Test plan rows that a hedge as a unit answers (Hedging (beta); #604 PR 1): one count, the winner
// and the losers, a loser's end being no failure, and a hedge going whole in a scale-down. PR 1 makes no member by
// itself, so each later member is made here through the package's own create path (seedHedgeMember, provisionWith
// with placeOpts.hedgeGroup), the call PR 2's growth makes.
//
// The fixture: the rig's Provider A (helios-gpu) and a second batch Provider B in another accelerator class, both
// able to host, both allowed, A first. Two classes, since a loser's streak and a winner's ready in one class would
// reset each other, and a fixture whose Providers share a class proves nothing about the streak. Which member wins is
// fixed by which job the fakes start first and how fast their engines get ready, never computed by the code tested.

import (
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

const (
	hedgeB      = "example-b"
	hedgeBGPU   = "example-b100"
	hedgeBSlurm = "gpu-b"
)

// hedgeRig is a rig with Provider B beside A, B's partition in its own accelerator class, and engines that the
// test slows by Allocation name (slow): a slowed Allocation's worker stays starting for 6 h.
type hedgeRig struct {
	*rig
	b    *rigExtra
	slow map[string]bool
}

func newHedgeRig(t *testing.T, opts ...rigOption) *hedgeRig {
	rg := newRig(t, append([]rigOption{withMaxTime(4 * time.Hour)}, opts...)...)
	b := rg.addProvider(hedgeB, hedgeBSlurm, 4*time.Hour,
		func(pp *v1alpha1.PartitionProfile) {
			pp.AcceleratorClass.GPUModel = hedgeBGPU
			pp.MemPerGPU = ptrTo(resource.MustParse("120Gi"))
		},
		func(c *pfake.Config) { c.Partitions[0].GPUModel = hedgeBGPU })
	h := &hedgeRig{rig: rg, b: b, slow: map[string]bool{}}
	rg.engine = func(alloc string) afake.Engine {
		if h.slow[alloc] {
			return afake.Engine{StartDelay: 6 * time.Hour}
		}
		return afake.Engine{}
	}
	return h
}

// onBoth is an edit that lets the deployment use A and then B.
func onBoth(md *v1alpha1.ModelDeployment) { md.Spec.Providers.Names = []string{rigProvider, hedgeB} }

// passOf builds a pass of the deployment as Reconcile does, up to its plan: read, observed, prepared.
func (rg *rig) passOf(md string) *pass {
	rg.t.Helper()
	p := &pass{r: rg.r, ctx: rg.ctx, key: types.NamespacedName{Namespace: rigNS, Name: md}, mem: rg.r.memory()}
	var m v1alpha1.ModelDeployment
	if err := rg.r.Reader.Get(rg.ctx, p.key, &m); err != nil {
		rg.t.Fatal(err)
	}
	p.md = &m
	if err := p.observe(); err != nil {
		rg.t.Fatal(err)
	}
	p.now = rg.r.Clock.Now().UTC().Truncate(time.Second)
	if err := p.prepareServed(); err != nil {
		rg.t.Fatal(err)
	}
	return p
}

// seedHedgeMember makes a member of the hedge whose first member is first, for the need of pred (a replacement
// hedge; "" for a missing primary's), on Provider prov, through provisionWith, and returns its name.
func (rg *rig) seedHedgeMember(md, first, pred, prov string) string {
	rg.t.Helper()
	p := rg.passOf(md)
	var predIn *v1alpha1.Instance
	for _, in := range p.insts {
		if in.Name == pred {
			predIn = in
		}
	}
	if pred != "" && predIn == nil {
		rg.t.Fatalf("no predecessor %s to seed a member for", pred)
	}
	ok, err := p.provisionWith(predIn, placeOpts{only: prov, hedgeGroup: first})
	if err != nil || !ok {
		rg.t.Fatalf("seeding a member of %s on %s: placed %v, %v", first, prov, ok, err)
	}
	for _, in := range rg.instances(md) {
		if in.Annotations[v1alpha1.AnnotationHedgeGroup] == first && in.Labels[v1alpha1.LabelProvider] == prov && !ended(&in) {
			return in.Name
		}
	}
	rg.t.Fatalf("the seeded member of %s on %s is not among %s's Instances", first, prov, md)
	return ""
}

// onProvider returns the deployment's live Instances on the Provider, oldest first.
func (rg *rig) onProvider(md, prov string) []v1alpha1.Instance {
	var out []v1alpha1.Instance
	for _, in := range rg.liveInstances(md) {
		if in.Labels[v1alpha1.LabelProvider] == prov {
			out = append(out, in)
		}
	}
	return out
}

// allocOrNil returns the named Allocation, or nil.
func (rg *rig) allocOrNil(name string) *v1alpha1.Allocation {
	if a, ok := rg.allocation(name); ok {
		return a
	}
	return nil
}

// mustInstance returns the named Instance, or fails.
func (rg *rig) mustInstance(name string) *v1alpha1.Instance {
	rg.t.Helper()
	in := rg.instance(name)
	if in == nil {
		rg.t.Fatalf("Instance %s is gone", name)
	}
	return in
}

// released reports whether the Allocation is gone or being deleted.
func (rg *rig) released(name string) bool {
	a, ok := rg.allocation(name)
	return !ok || !a.DeletionTimestamp.IsZero()
}

// TestAHedgeCountsOnce: a deployment of one Instance whose missing primary has members on A and B, both pending,
// keeps both across ten passes: step 7 reads one primary, not two, so it cancels neither and creates no third. While
// it has no winner, Progressing names the hedge's Providers.
func TestAHedgeCountsOnce(t *testing.T) {
	h := newHedgeRig(t)
	h.batch.Enqueue(pfake.Script{QueueWait: 10 * time.Hour})
	h.b.batch.Enqueue(pfake.Script{QueueWait: 10 * time.Hour})
	h.Step() // the Providers' first status: Ready
	md := mdSpec(hoMD)
	onBoth(md)
	h.create(md)
	h.Step()
	firsts := h.onProvider(hoMD, rigProvider)
	if len(firsts) != 1 {
		t.Fatalf("%d Instances on A after the first pass, want the missing primary's one", len(firsts))
	}
	first := firsts[0].Name
	second := h.seedHedgeMember(hoMD, first, "", hedgeB)
	for i := range 10 {
		h.clock.Advance(10 * time.Second)
		h.Step()
		for _, n := range []string{first, second} {
			if in := h.mustInstance(n); ended(in) {
				t.Fatalf("pass %d: member %s ended (%s): a hedge of two read as two primaries", i, n, in.Status.EndReason)
			}
		}
		if n := len(h.allAllocations()); n != 2 {
			t.Fatalf("pass %d: %d Allocations, want the hedge's two", i, n)
		}
	}
	c := h.condition(hoMD, v1alpha1.ConditionProgressing)
	want := "hedge " + first + " spans Providers " + rigProvider + ", " + hedgeB
	if c.Reason != v1alpha1.ReasonProvisioning || !strings.Contains(c.Message, want) {
		t.Fatalf("Progressing = %s %q, want Provisioning naming %q", c.Reason, c.Message, want)
	}
}

// TestAHedgeGoesWholeInAScaleDown: with instances 2, a ready primary R on A and a two-member hedge for the second
// primary (A's member and B's, both pending), lowering instances to 1 takes the hedge, the least advanced primary,
// whole: both members end and both Allocations are deleted, and R serves on. The selection a Scale Operation's plan
// reads (scaleDownTargets) lists both members, each cancelPending. With B's job started and its Instance still
// starting, B's member ends but its Allocation is kept, endKeepAllocation, while A's pending one is cancelled.
func TestAHedgeGoesWholeInAScaleDown(t *testing.T) {
	for _, bStarted := range []bool{false, true} {
		name := "both pending"
		if bStarted {
			name = "B's job started"
		}
		t.Run(name, func(t *testing.T) {
			h := newHedgeRig(t)
			h.batch.Enqueue(pfake.Script{}, pfake.Script{QueueWait: 10 * time.Hour})
			if bStarted {
				h.b.batch.Enqueue(pfake.Script{})
			} else {
				h.b.batch.Enqueue(pfake.Script{QueueWait: 10 * time.Hour})
			}
			h.Step()
			md := mdSpec(hoMD)
			onBoth(md)
			md.Spec.Instances = 2
			h.create(md)
			// The first primary R gets ready on A; the second, M1, is created on A too and pends.
			h.until(5*time.Minute, 10*time.Second, "one ready Instance", func() bool { return h.readyCount(hoMD) == 1 })
			var r, m1 string
			for _, in := range h.onProvider(hoMD, rigProvider) {
				if in.Status.Phase == v1alpha1.InstanceReady {
					r = in.Name
				} else {
					m1 = in.Name
				}
			}
			if r == "" || m1 == "" {
				t.Fatalf("want a ready R and a pending M1 on A, got %+v", h.onProvider(hoMD, rigProvider))
			}
			m2 := h.seedHedgeMember(hoMD, m1, "", hedgeB)
			if bStarted {
				h.slow[h.mustInstance(m2).Spec.AllocationRef.Name] = true
				h.until(time.Minute, 10*time.Second, "B's member started", func() bool {
					a, ok := h.allocation(h.mustInstance(m2).Spec.AllocationRef.Name)
					return ok && a.Status.AtProvider.StartTime != nil
				})
			}
			p := h.passOf(hoMD)
			targets := p.scaleDownTargets(p.primaries(), 1)
			wantB := effectCancelPending
			if bStarted {
				wantB = effectEndKeepAllocation
			}
			got := map[string]scaleEffect{}
			for _, tg := range targets {
				got[tg.instance.Name] = tg.effect
			}
			if len(targets) != 2 || got[m1] != effectCancelPending || got[m2] != wantB {
				t.Fatalf("scale-down targets %v, want %s cancelPending and %s %s", got, m1, m2, wantB)
			}
			m1Alloc, m2Alloc := h.mustInstance(m1).Spec.AllocationRef.Name, h.mustInstance(m2).Spec.AllocationRef.Name
			h.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 1 })
			h.clock.Advance(10 * time.Second)
			h.Step()
			for _, n := range []string{m1, m2} {
				if in := h.instance(n); in != nil && !ended(in) {
					t.Fatalf("member %s is %s after the scale-down, want ended", n, in.Status.Phase)
				}
			}
			if !h.released(m1Alloc) {
				t.Fatalf("A's pending member's Allocation %s was not cancelled", m1Alloc)
			}
			if h.released(m2Alloc) == bStarted {
				t.Fatalf("B's member's Allocation released %v, want %v", h.released(m2Alloc), !bStarted)
			}
			if in := h.mustInstance(r); in.Status.Phase != v1alpha1.InstanceReady {
				t.Fatalf("R is %s after the scale-down, want ready", in.Status.Phase)
			}
		})
	}
}

// replacementHedge steps the shared handover timeline (handover_test.go) to its replacement time: P is ready on A at
// 08:01:10, and its replacement R1 is created on A at 09:19:30, where it pends behind a 10 h wait. Then B's member
// R2 is seeded for the same need. It returns the names of P, R1 and R2.
func replacementHedge(t *testing.T, h *hedgeRig, b pfake.Script) (string, string, string) {
	t.Helper()
	h.batch.Enqueue(pfake.Script{}, pfake.Script{QueueWait: 10 * time.Hour})
	h.b.batch.Enqueue(b)
	reservedStart(h.rig, hoMD, onBoth)
	p := h.instances(hoMD)[0]
	hoStepTo(h.rig, hoT(9, 19, 40), 5*time.Minute, nil)
	reps := hoRepl(h.rig, p.Name)
	if len(reps) != 1 || reps[0].Labels[v1alpha1.LabelProvider] != rigProvider {
		t.Fatalf("P's replacements at 09:19:40: %+v, want one, on A", reps)
	}
	r2 := h.seedHedgeMember(hoMD, reps[0].Name, p.Name, hedgeB)
	return p.Name, reps[0].Name, r2
}

// TestTheFirstToServeWinsAndAPendingLoserIsCancelled: P's replacement hedge has R1 on A, pending, and R2 on B, whose
// job starts at once. In the pass that sees R2 ready, R1 ends with HedgeLost and its Allocation is deleted; a
// replaced HedgeLost event regards R1, with no Approval asked for. R1's Allocation goes with its release cause in the
// store at the delete, read through the delete: "Instance R1 ended: hedge lost to R2, on Provider example-b (hedge
// R1)", R1 being the hedge's first member. P serves on until its drain start, 09:50:20, and drains then, R2 serving
// in its place.
func TestTheFirstToServeWinsAndAPendingLoserIsCancelled(t *testing.T) {
	log := newCauseLog()
	h := newHedgeRig(t, withInterceptors(log.funcs()))
	p, r1, r2 := replacementHedge(t, h, pfake.Script{})
	r1Alloc := h.mustInstance(r1).Spec.AllocationRef.Name
	h.until(10*time.Minute, 10*time.Second, "R2 ready", func() bool { return h.mustInstance(r2).Status.Phase == v1alpha1.InstanceReady })
	lost := h.mustInstance(r1)
	if lost.Status.Phase != v1alpha1.InstanceEnded || lost.Status.EndReason != v1alpha1.ReasonHedgeLost {
		t.Fatalf("R1 is %s, ended for %q, in the pass R2 is ready; want ended for HedgeLost", lost.Status.Phase, lost.Status.EndReason)
	}
	if !h.released(r1Alloc) {
		t.Fatalf("R1's pending Allocation %s was not deleted", r1Alloc)
	}
	want := "Instance " + r1 + " ended: hedge lost to " + r2 + ", on Provider " + hedgeB + " (hedge " + r1 + ")"
	if got, deleted := log.deletedWith(r1Alloc); !deleted || got != want {
		t.Fatalf("R1's Allocation was deleted %v with the stored release cause %q, want %q", deleted, got, want)
	}
	if n := len(h.rec.Find(eventReplaced, v1alpha1.ReasonHedgeLost)); n < 1 {
		t.Fatal("no replaced event with reason HedgeLost")
	}
	var approvals v1alpha1.ApprovalList
	if err := h.c.List(h.ctx, &approvals); err != nil {
		t.Fatal(err)
	}
	if n := len(approvals.Items); n != 0 {
		t.Fatalf("%d Approvals, want none: cancelling a hedge loser needs no approval", n)
	}
	if in := h.mustInstance(p); in.Status.Phase != v1alpha1.InstanceReady {
		t.Fatalf("P is %s once the hedge is resolved, want ready until its drain start", in.Status.Phase)
	}
	hoStepTo(h.rig, hoT(9, 50, 30), time.Minute, nil)
	if ph := hoPhase(h.rig, p); ph != v1alpha1.InstanceDraining && ph != v1alpha1.InstanceEnded {
		t.Fatalf("P is %s at 09:50:30, want draining or ended", ph)
	}
	if in := h.mustInstance(r2); in.Status.Phase != v1alpha1.InstanceReady {
		t.Fatalf("R2 is %s at 09:50:30, want ready, serving in P's place", in.Status.Phase)
	}
	if n := len(h.rec.Find(eventReplaced, v1alpha1.EventReasonHandedOver)); n < 1 {
		t.Fatal("no replaced HandedOver event at P's drain start")
	}
}

// TestAStartedLoserIsKept: R1's job on A starts, but its engine stays starting, so R1 is not ready when R2 wins. R1
// ends with HedgeLost, no failure; its Allocation is not deleted and carries idle-since from the win's pass. Under
// Keep it stays; under ReleaseWhenIdle with a 15 min idle limit it is released 15 min after, with no approval.
func TestAStartedLoserIsKept(t *testing.T) {
	for _, c := range []struct {
		name  string
		edit  func(*v1alpha1.ModelDeployment)
		after time.Duration // when it is released after the win; 0 for kept
	}{
		{"Keep", nil, 0},
		{"ReleaseWhenIdle 15 min", func(md *v1alpha1.ModelDeployment) {
			md.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur(15 * time.Minute)}
		}, 15 * time.Minute},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newHedgeRig(t)
			h.batch.Enqueue(pfake.Script{}, pfake.Script{})
			reservedStart(h.rig, hoMD, func(md *v1alpha1.ModelDeployment) {
				onBoth(md)
				if c.edit != nil {
					c.edit(md)
				}
			})
			p := h.instances(hoMD)[0]
			hoStepTo(h.rig, hoT(9, 19, 30), 5*time.Minute, nil)
			reps := hoRepl(h.rig, p.Name)
			if len(reps) != 1 {
				t.Fatalf("%d replacements at 09:19:30, want one", len(reps))
			}
			r1 := reps[0].Name
			r1Alloc := reps[0].Spec.AllocationRef.Name
			h.slow[r1Alloc] = true // R1's job starts at the next pass, its engine slowed
			h.clock.Advance(10 * time.Second)
			h.Step()
			if a, ok := h.allocation(r1Alloc); !ok || a.Status.AtProvider.StartTime == nil {
				t.Fatalf("R1's job has not started at 09:19:40: %+v", a)
			}
			r2 := h.seedHedgeMember(hoMD, r1, p.Name, hedgeB)
			h.until(10*time.Minute, 10*time.Second, "R2 ready", func() bool { return h.mustInstance(r2).Status.Phase == v1alpha1.InstanceReady })
			won := h.clock.Now().UTC().Truncate(time.Second)
			lost := h.mustInstance(r1)
			if lost.Status.EndReason != v1alpha1.ReasonHedgeLost || !ended(lost) {
				t.Fatalf("R1 is %s, ended for %q; want ended for HedgeLost", lost.Status.Phase, lost.Status.EndReason)
			}
			a, ok := h.allocation(r1Alloc)
			if !ok || !a.DeletionTimestamp.IsZero() {
				t.Fatalf("R1's started Allocation was released at the win: %+v", a)
			}
			if since, ok, err := v1alpha1.IdleSince(a); err != nil || !ok || !since.Equal(won) {
				t.Fatalf("R1's Allocation idle-since %v (%v, %v), want %s, the win's pass", since, ok, err, won.Format(time.TimeOnly))
			}
			if c.after == 0 {
				h.run(30*time.Minute, time.Minute)
				if h.released(r1Alloc) {
					t.Fatal("under Keep, R1's Allocation was released within 30 min of the win")
				}
				return
			}
			h.until(c.after+time.Minute, 10*time.Second, "R1's Allocation released", func() bool { return h.released(r1Alloc) })
			if at := h.clock.Now(); at.Before(won.Add(c.after)) {
				t.Fatalf("R1's Allocation was released at %s, before the win plus the idle limit, %s", at.Format(time.TimeOnly), won.Add(c.after).Format(time.TimeOnly))
			}
		})
	}
}

// TestALosersEndIsNoFailure: a deployment of three Instances has three hedges, each with a member on A and one on
// B. A's jobs start, their engines slowed, and B's members win, so three HedgeLost ends fall in A's class within its
// restart window. No RestartLoop is set, provisioning in A's class does not pause, the streak is empty, and A's three
// Allocations are kept. A fixture with fewer than three loser ends proves nothing, since the streak needs three.
func TestALosersEndIsNoFailure(t *testing.T) {
	h := newHedgeRig(t)
	h.batch.Enqueue(pfake.Script{}, pfake.Script{}, pfake.Script{})
	h.Step()
	md := mdSpec(hoMD)
	onBoth(md)
	md.Spec.Instances = 3
	h.create(md)
	h.rig.engine = func(alloc string) afake.Engine {
		if a, ok := h.allocation(alloc); ok && a.Labels[v1alpha1.LabelProvider] == rigProvider {
			return afake.Engine{StartDelay: 6 * time.Hour}
		}
		return afake.Engine{}
	}
	h.Step()
	firsts := h.onProvider(hoMD, rigProvider)
	if len(firsts) != 3 {
		t.Fatalf("%d Instances on A, want the three missing primaries", len(firsts))
	}
	var allocsA []string
	for _, f := range firsts {
		allocsA = append(allocsA, f.Spec.AllocationRef.Name)
		h.seedHedgeMember(hoMD, f.Name, "", hedgeB)
	}
	h.until(15*time.Minute, 10*time.Second, "three ready on B", func() bool { return h.readyCount(hoMD) == 3 })
	h.clock.Advance(10 * time.Second)
	h.Step()
	lost := 0
	for _, in := range h.instances(hoMD) {
		if in.Labels[v1alpha1.LabelProvider] == rigProvider && in.Status.EndReason == v1alpha1.ReasonHedgeLost {
			lost++
		}
	}
	if lost != 3 {
		t.Fatalf("%d of A's members ended for HedgeLost, want 3: the test proves nothing with fewer", lost)
	}
	if c := h.condition(hoMD, v1alpha1.ConditionDegraded); c.Reason == v1alpha1.ReasonRestartLoop || strings.Contains(c.Message, v1alpha1.ReasonRestartLoop) {
		t.Fatalf("Degraded = %s %q: three hedge losses set RestartLoop", c.Reason, c.Message)
	}
	if n := len(h.rec.Find(eventDegraded, v1alpha1.ReasonRestartLoop)); n != 0 {
		t.Fatalf("%d RestartLoop events after three hedge losses, want none", n)
	}
	p := h.passOf(hoMD)
	classA := v1alpha1.AcceleratorClass{GPUModel: rigGPU, CPUArch: rigArch}
	if s := p.classStreak(classA); len(s) != 0 {
		t.Fatalf("A's class has a failure streak of %d after three hedge losses, want none", len(s))
	}
	if paused, until := p.loopPaused(classA, p.pvByName[rigProvider].settings); paused {
		t.Fatalf("provisioning in A's class pauses until %s after three hedge losses", until)
	}
	for _, a := range allocsA {
		if h.released(a) {
			t.Fatalf("A's started Allocation %s was released, want it kept", a)
		}
	}
}

// TestAWinnerWhoseAllocationGoesResolvesNothing: a hedge's member that reads ready, but whose Allocation is being
// deleted in the same pass, resolves nothing, so no loser ends beside a winner that no longer serves. The same pass
// with the Allocation in place ends the loser.
func TestAWinnerWhoseAllocationGoesResolvesNothing(t *testing.T) {
	h := newHedgeRig(t)
	h.batch.Enqueue(pfake.Script{QueueWait: 10 * time.Hour})
	h.b.batch.Enqueue(pfake.Script{QueueWait: 10 * time.Hour})
	h.Step()
	md := mdSpec(hoMD)
	onBoth(md)
	h.create(md)
	h.Step()
	m1 := h.onProvider(hoMD, rigProvider)[0].Name
	m2 := h.seedHedgeMember(hoMD, m1, "", hedgeB)
	for _, goes := range []bool{true, false} {
		p := h.passOf(hoMD)
		var w *v1alpha1.Instance
		for _, in := range p.insts {
			if in.Name == m2 {
				w = in
			}
		}
		now := metav1.NewTime(p.now)
		w.Status.Phase = v1alpha1.InstanceReady
		w.Status.ReadyTime = &now
		if goes {
			p.allocOf(w).DeletionTimestamp = &now
		}
		if err := p.resolveHedges(); err != nil {
			t.Fatal(err)
		}
		l := h.mustInstance(m1)
		if goes && (ended(l) || l.Status.EndReason != "") {
			t.Fatalf("the loser %s ended (%s) beside a winner whose Allocation is being deleted", m1, l.Status.EndReason)
		}
		if !goes && l.Status.EndReason != v1alpha1.ReasonHedgeLost {
			t.Fatalf("the control: the loser %s is %s, ended for %q; want HedgeLost", m1, l.Status.Phase, l.Status.EndReason)
		}
	}
}

// TestAReadyLoserDrainsAndEndsHedgeLost: a missing primary's two members start in the same pass with like engines,
// so both read ready in one pass, at one ready time, 08:01:00. A's member is created at 08:00:00 and B's a second
// later, so A's is the older and wins (HBEP-7: the earliest ready time, then the older member); B's member, ready
// too, drains first, as a surplus Instance does, and ends with HedgeLost once drained, its Allocation kept.
func TestAReadyLoserDrainsAndEndsHedgeLost(t *testing.T) {
	h := newHedgeRig(t)
	h.Step()
	md := mdSpec(hoMD)
	onBoth(md)
	h.create(md)
	h.Step()
	m1 := h.onProvider(hoMD, rigProvider)[0].Name
	h.clock.Advance(time.Second) // B's member is the newer; both jobs still start at the next pass
	m2 := h.seedHedgeMember(hoMD, m1, "", hedgeB)
	h.until(5*time.Minute, 10*time.Second, "a member resolved", func() bool {
		return h.mustInstance(m1).Status.Phase == v1alpha1.InstanceReady && h.mustInstance(m2).Status.Phase != v1alpha1.InstanceReady
	})
	a, b := h.mustInstance(m1), h.mustInstance(m2)
	if b.Status.ReadyTime == nil || !b.Status.ReadyTime.Equal(a.Status.ReadyTime) {
		t.Fatalf("B's member ready at %v, A's at %v: the fixture did not make both ready in one pass, so it proves nothing", b.Status.ReadyTime, a.Status.ReadyTime)
	}
	if b.Status.Phase != v1alpha1.InstanceDraining && b.Status.Phase != v1alpha1.InstanceEnded {
		t.Fatalf("the ready loser is %s, want draining", b.Status.Phase)
	}
	bAlloc := b.Spec.AllocationRef.Name
	h.until(5*time.Minute, 10*time.Second, "the loser ended", func() bool { return ended(h.mustInstance(m2)) })
	if r := h.mustInstance(m2).Status.EndReason; r != v1alpha1.ReasonHedgeLost {
		t.Fatalf("the ready loser's drain ended for %q, want HedgeLost", r)
	}
	if h.released(bAlloc) {
		t.Fatal("the ready loser's Allocation was released, want it kept")
	}
	if in := h.mustInstance(m1); in.Status.Phase != v1alpha1.InstanceReady {
		t.Fatalf("the winner is %s, want ready", in.Status.Phase)
	}
}

// TestAFailedMemberLeavesTheHedge: R1's job on A starts at 09:19:50 and fails 30 s later, before its engine is ready
// (50 s) and before B's member R2, queued 10 min, starts. R1 ends for AllocationFailed, its Allocation is released, and its class counts one failure,
// as any failed Instance does. R2 then wins alone: no HedgeLost.
func TestAFailedMemberLeavesTheHedge(t *testing.T) {
	h := newHedgeRig(t)
	fail := 30 * time.Second
	h.batch.Enqueue(pfake.Script{}, pfake.Script{FailAfter: &fail})
	h.b.batch.Enqueue(pfake.Script{QueueWait: 10 * time.Minute})
	reservedStart(h.rig, hoMD, onBoth)
	p := h.instances(hoMD)[0]
	hoStepTo(h.rig, hoT(9, 19, 40), 5*time.Minute, nil)
	reps := hoRepl(h.rig, p.Name)
	if len(reps) != 1 {
		t.Fatalf("%d replacements at 09:19:40, want one", len(reps))
	}
	r1, r1Alloc := reps[0].Name, reps[0].Spec.AllocationRef.Name
	r2 := h.seedHedgeMember(hoMD, r1, p.Name, hedgeB)
	h.until(5*time.Minute, 10*time.Second, "R1 failed", func() bool { in := h.instance(r1); return in != nil && ended(in) })
	if r := h.mustInstance(r1).Status.EndReason; r != v1alpha1.ReasonAllocationFailed {
		t.Fatalf("R1 ended for %q, want AllocationFailed", r)
	}
	if !h.released(r1Alloc) {
		t.Fatal("R1's failed Allocation was not released")
	}
	classA := v1alpha1.AcceleratorClass{GPUModel: rigGPU, CPUArch: rigArch}
	if s := h.passOf(hoMD).classStreak(classA); len(s) != 1 {
		t.Fatalf("A's class streak is %d after R1's failure, want 1", len(s))
	}
	h.until(20*time.Minute, 10*time.Second, "R2 ready", func() bool { return h.mustInstance(r2).Status.Phase == v1alpha1.InstanceReady })
	if n := len(h.rec.Find(eventReplaced, v1alpha1.ReasonHedgeLost)); n != 0 {
		t.Fatalf("%d HedgeLost events, want none: the failed member had left the hedge", n)
	}
}

// TestAHedgeIsLateOnlyWhenNoMemberStarted: P's replacement hedge has R1 on A, pending behind 10 h, and R2 on B. At the
// lateness deadline, 09:49:30 (P's drain start 09:50:20 minus the measured start-to-ready 50 s), ReplacementLate is
// set only when no member's job has started: with R2's job started, its engine slowed, it is not.
func TestAHedgeIsLateOnlyWhenNoMemberStarted(t *testing.T) {
	for _, c := range []struct {
		name string
		b    pfake.Script
		late bool
	}{
		{"B's member started", pfake.Script{}, false},
		{"both pending", pfake.Script{QueueWait: 10 * time.Hour}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newHedgeRig(t)
			_, r1, r2 := replacementHedge(t, h, c.b)
			h.slow[h.mustInstance(r2).Spec.AllocationRef.Name] = true
			hoStepTo(h.rig, hoT(9, 49, 20), 5*time.Minute, nil)
			// The fixture's state, or the case proves nothing: R1 pends, and R2 has started, not ready, or pends too.
			in1, in2 := h.mustInstance(r1), h.mustInstance(r2)
			a2, _ := h.allocation(in2.Spec.AllocationRef.Name)
			if ended(in1) || jobStarted(h.allocOrNil(in1.Spec.AllocationRef.Name)) || ended(in2) || in2.Status.Phase == v1alpha1.InstanceReady || jobStarted(a2) == c.late {
				t.Fatalf("at 09:49:20 R1 is %s and R2 is %s (its job started %v): not the fixture this case needs", in1.Status.Phase, in2.Status.Phase, jobStarted(a2))
			}
			if n := len(h.rec.Find(eventDegraded, v1alpha1.ReasonReplacementLate)); n != 0 {
				t.Fatalf("ReplacementLate before 09:49:30")
			}
			hoStepTo(h.rig, hoT(9, 49, 40), 10*time.Second, nil)
			if got := len(h.rec.Find(eventDegraded, v1alpha1.ReasonReplacementLate)) > 0; got != c.late {
				t.Fatalf("ReplacementLate at 09:49:40: %v, want %v", got, c.late)
			}
		})
	}
}
