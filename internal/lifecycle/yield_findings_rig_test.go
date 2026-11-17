package lifecycle

// Rows for the review findings of #708 (#616 PR 4) that concern the yield: each was written FIRST, to fail on head 9ccc6408,
// asserting the correct behaviour (so that failure showed the finding), and passes with the reworked protocol. They sit
// beside yield_rig_test.go and share its fixtures (yieldRig's limit of 4, tick10, yieldMD, marks, pending10h). Every
// expected count is worked by hand in the row.
//
//	1a. over-yield across Providers: one missing primary, refused on two Providers, marks one extra, not one on each;
//	1b. a shortage that falls: two marks outstanding for a shortage of one cancel one extra, not both (both reconcile orders);
//	1c. two refusals in one pass mark one extra even when the Reader lags (the pass's own count);
//	2.  a mark on a Provider the named deployment no longer lists is cleared, and its extra is not cancelled;
//	2b. the owner waits while the named deployment's last pass has not seen its current spec (observedGeneration);
//	3.  a cut-short primary's retry, refused for the limit alone, yields (stated limit (b); both reconcile orders);
//	5.  a yield that could not admit the primary (other deployments' reserved slots fill the limit) is not made;
//	5b. a mark whose cancel stops being able to admit its primary is cleared, and no second is made;
//	and shortageOf, which counts by Allocation presence, in a table.

import (
	"context"
	"errors"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

const (
	yieldC          = "example-c"
	yieldCProfile   = "example-c-profile"
	yieldCPartition = "gpu-c"
)

// yieldRigTwo is yieldRig with a second Provider C beside A, the rig's own. A's submit limit is limitA and C's limitC.
// C has a PlatformProfile of its own (the rig's accelerator class, its limit in its partition's QOS) and its own fake
// Slurm, which Step syncs.
func yieldRigTwo(t *testing.T, log *causeLog, limitA, limitC int32) (*rig, *rigExtra) {
	t.Helper()
	qosA := []v1alpha1.QOSProfile{{Name: "example-a", MaxSubmitPerUser: i32(limitA)}}
	rg := newRig(t, append(qosOpts(qosA, "", "example-a"), withMaxTime(12*time.Hour), withInterceptors(log.funcs()))...)
	c := rg.addProviderWithProfile(yieldC, yieldCProfile, yieldCPartition, []v1alpha1.QOSProfile{{Name: "example-c", MaxSubmitPerUser: i32(limitC)}}, nil)
	return rg, c
}

// allocCutter cuts an Allocation's creation short, as a pass killed between the Instance's create and the
// Allocation's is: while armed for a deployment, its Allocation creates from the skip+1-th on fail with errHOKilled.
type allocCutter struct {
	mu     sync.Mutex
	md     string
	skip   int
	seen   int
	failed int
}

func (c *allocCutter) arm(md string, skip int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.md, c.skip, c.seen = md, skip, 0
}

func (c *allocCutter) disarm() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.md = ""
}

func (c *allocCutter) failures() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.failed
}

// funcs is base with a Create that fails the armed deployment's Allocation creates.
func (c *allocCutter) funcs(base interceptor.Funcs) interceptor.Funcs {
	base.Create = func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if a, ok := obj.(*v1alpha1.Allocation); ok {
			c.mu.Lock()
			cut := false
			if c.md != "" && a.Labels[v1alpha1.LabelModelDeployment] == c.md {
				c.seen++
				if c.seen > c.skip {
					c.failed++
					cut = true
				}
			}
			c.mu.Unlock()
			if cut {
				return errHOKilled
			}
		}
		return cl.Create(ctx, obj, opts...)
	}
	return base
}

// stepTolerantAll is rg.Step that tolerates the cut-short creates' errors: every deployment but those named in skip is
// reconciled in name order (skip: a deployment whose pass the processing order delays), and an error is fatal unless it
// is errHOKilled.
func stepTolerantAll(rg *rig, skip ...string) {
	rg.t.Helper()
	if err := rg.prov.Sync(rg.ctx); err != nil {
		rg.t.Fatalf("provider sync: %v", err)
	}
	for _, x := range rg.extra {
		if err := x.prov.Sync(rg.ctx); err != nil {
			rg.t.Fatalf("provider %s sync: %v", x.name, err)
		}
	}
	rg.syncAgents()
	var mds v1alpha1.ModelDeploymentList
	if err := rg.c.List(rg.ctx, &mds); err != nil {
		rg.t.Fatal(err)
	}
	names := make([]string, 0, len(mds.Items))
	for _, md := range mds.Items {
		names = append(names, md.Name)
	}
	sort.Strings(names)
	for _, n := range names {
		if slices.Contains(skip, n) {
			continue
		}
		_, err := rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: n}})
		if err != nil && !errors.Is(err, errHOKilled) {
			rg.t.Fatalf("reconcile %s at %s: %v", n, rg.clock.Now().Format(time.RFC3339), err)
		}
	}
}

// cancelsOf is how many Allocations the controller has deleted.
func cancelsOf(log *causeLog) int {
	log.mu.Lock()
	defer log.mu.Unlock()
	return len(log.cause)
}

// TestOneMissingPrimaryRefusedOnTwoProvidersYieldsOneExtra (finding 1, over-yield across Providers). A and C each have a
// submit limit of 2 and are full: md-a (A only) holds its primary and its pending extra there, md-b (C only) the same on C,
// 2 + 2 listed Allocations, every job pending 10 h. md-c, created at 08:00:40, allows A then C, k = 0, and misses its one
// primary; at 08:00:50 it is refused for the limit alone on both (A: 2 listed >= 2; C: 2 listed >= 2). One missing
// primary is one slot needed: the shortage is 1, so exactly one extra may be marked and cancelled, wherever it is. Each
// Provider's yield counts md-c's marks among the Instances on THAT Provider only (yield.go:96-102), so A marks md-a's
// extra and C marks md-b's, two marks at 08:00:50 for a shortage of 1, and both owners cancel at 08:01:10.
// Expected: at most one mark at any time through 08:02:30, one Instance ever marked, one Allocation deleted, md-c's primary
// submitted (its one Allocation), and 5 jobs in all: md-a's two, md-b's two, md-c's one.
func TestOneMissingPrimaryRefusedOnTwoProvidersYieldsOneExtra(t *testing.T) {
	log := newCauseLog()
	rg, c := yieldRigTwo(t, log, 2, 2)
	rg.batch.Enqueue(pending10h(2)...)
	c.batch.Enqueue(pending10h(2)...)
	rg.create(yieldMD("md-a"))
	tick10(rg) // 08:00:10: md-a makes P1 and X1 on A
	tick10(rg) // 08:00:20: their jobs are submitted
	d2 := yieldMD("md-b")
	d2.Spec.Providers.Names = []string{yieldC}
	rg.create(d2)
	tick10(rg) // 08:00:30: md-b makes P2 and X2 on C
	tick10(rg) // 08:00:40
	b := mdSpec("md-c")
	b.Spec.Providers.Names = []string{rigProvider, yieldC}
	rg.create(b)
	tick10(rg) // 08:00:50: md-c is refused on both
	if a, bb, cc := len(rg.allocations("md-a")), len(rg.allocations("md-b")), len(rg.allocations("md-c")); a != 2 || bb != 2 || cc != 0 {
		t.Fatalf("md-a has %d Allocations, md-b %d and md-c %d at 08:00:50, want 2, 2 and none: both Providers are full at their limit of 2", a, bb, cc)
	}
	seen := map[string]bool{}
	check := func() {
		t.Helper()
		ms := marks(rg)
		for _, m := range ms {
			seen[m.Name] = true
		}
		if len(ms) > 1 {
			t.Fatalf("at %s %d extras are marked %v, want at most one: md-c misses one primary, refused on both Providers, and each Provider's yield marked its own extra", rg.clock.Now().Format(time.TimeOnly), len(ms), markNames(ms))
		}
	}
	check()
	for range 10 { // to 08:02:30
		tick10(rg)
		check()
	}
	if len(seen) != 1 {
		t.Fatalf("%d Instances were ever marked, want one: one missing primary needs one slot", len(seen))
	}
	if n := cancelsOf(log); n != 1 {
		t.Fatalf("%d Allocations were deleted by 08:02:30, want 1: one extra yields for one missing primary", n)
	}
	if n := len(rg.allocations("md-c")); n != 1 {
		t.Fatalf("md-c has %d Allocations at 08:02:30, want its primary, submitted once in the freed slot", n)
	}
	if jobs := rg.submissions() + c.batch.Count("sbatch"); jobs != 5 {
		t.Fatalf("%d jobs in all, want 5: md-a's two, md-b's two and md-c's primary", jobs)
	}
}

// TestAShortageThatFallsLeavesOneMarkNotTwo (finding 1, a shortage that falls between passes), in both reconcile orders
// (Y6): the deployment that misses primaries, B, sorts last by name, as in yield_rig_test.go, or first. One Provider A with a
// limit of 4 holds D1's and D2's primaries and pending extras (2 + 2 listed, all jobs pending 10 h), created at 08:00:00
// and 08:00:20. B (created at 08:00:40, k = 0, instances 2, A only) misses two primaries and makes one attempt per pass
// (capacity() breaks at the first refusal): at 08:00:50 it marks D2's extra (the newest pending), at 08:01:00 D1's, so
// two marks outstanding for a shortage of 2. A second Provider C, limit 1, joins B's list before the 08:01:00 pass, and
// in that pass the first primary is placed on C, so the shortage falls to 1 (the second is refused on A, full at 4, and on
// C, full at 1). One extra may be cancelled, not two: with both marks kept, D2's owner cancels its extra and D1's owner,
// at its next pass, the other, since B is refused again on A while the first cancelled Allocation still holds its slot.
// Expected through 08:02:50, in either order: exactly one Allocation deleted, so 3 live Instances of D1 and D2 (four less
// one), and B with 2 live: one on C and one on A, in the slot the cancel frees; no mark left.
func TestAShortageThatFallsLeavesOneMarkNotTwo(t *testing.T) {
	for _, c := range []struct{ name, b, d1, d2 string }{
		{"the deployment that misses primaries sorts last", "md-c", "md-a", "md-b"},
		{"the deployment that misses primaries sorts first", "md-a", "md-b", "md-c"},
	} {
		t.Run(c.name, func(t *testing.T) {
			log := newCauseLog()
			rg, _ := yieldRigTwo(t, log, 4, 1)
			rg.batch.Enqueue(pending10h(8)...)
			rg.create(yieldMD(c.d1))
			tick10(rg) // 08:00:10
			tick10(rg) // 08:00:20
			rg.create(yieldMD(c.d2))
			tick10(rg) // 08:00:30
			tick10(rg) // 08:00:40
			b := mdSpec(c.b)
			b.Spec.Instances = 2
			rg.create(b)
			tick10(rg) // 08:00:50: B's first attempt is refused on A and marks D2's extra
			if ms := marks(rg); len(ms) != 1 || deploymentOfInstance(ms[0]) != c.d2 {
				t.Fatalf("the marks at 08:00:50 are %v, want one, %s's extra", markNames(ms), c.d2)
			}
			rg.mutateMD(c.b, func(m *v1alpha1.ModelDeployment) { m.Spec.Providers.Names = []string{rigProvider, yieldC} })
			for range 12 { // 08:01:00 to 08:02:50
				tick10(rg)
			}
			if n := len(rg.onProvider(c.b, yieldC)); n != 1 {
				t.Fatalf("%s has %d Instances on C, want its first primary: the row proves nothing otherwise", c.b, n)
			}
			if n := cancelsOf(log); n != 1 {
				t.Fatalf("%d Allocations were deleted by 08:02:50, want 1: %s's shortage fell to 1 at 08:01:00, so one extra yields and the other's mark is cleared", n, c.b)
			}
			if a, bb := len(rg.liveInstances(c.d1)), len(rg.liveInstances(c.d2)); a+bb != 3 {
				t.Fatalf("%s has %d live Instances and %s %d, want 3 in all: four less the one extra that yielded", c.d1, a, c.d2, bb)
			}
			if a, bb := len(rg.liveInstances(c.d1)), len(rg.liveInstances(c.d2)); a != 2 || bb != 1 {
				t.Fatalf("%s has %d live Instances and %s %d, want 2 and 1: the extra that yielded is the NEWEST pending one, %s's (made at 08:00:30, %s's at 08:00:10), the oldest mark having been cleared", c.d1, a, c.d2, bb, c.d2, c.d1)
			}
			if n := len(rg.liveInstances(c.b)); n != 2 {
				t.Fatalf("%s has %d live Instances, want 2: one on C and one in the slot the cancel freed on A", c.b, n)
			}
			if ms := marks(rg); len(ms) != 0 {
				t.Fatalf("%v are marked at 08:02:50, want none", markNames(ms))
			}
		})
	}
}

// TestAMarkOnAProviderTheNamedDeploymentNoLongerListsIsCleared (finding 2). The yield rig at A's limit of 4 (md-a's and md-b's
// primaries and pending extras) plus C, limit 1, held by md-d's primary (k = 0). md-c, created at 08:00:40 and allowing A,
// marks md-b's extra at 08:00:50. Then md-c's spec drops A and lists C alone, where it is refused too (1 listed >= 1) with no
// pending extra to take: it is still short, but it can no longer use a slot on A. Its pass at 08:01:00 does not look at A:
// clearYieldMarks visits only the Providers of its own pass (yield.go:144-147), so the mark stays, and at 08:01:10
// md-b's owner finds md-c short and its status newer than the mark and cancels the extra for a slot md-c cannot take.
// Expected: the mark is cleared and md-b's extra is not cancelled, now or through 08:02:40.
func TestAMarkOnAProviderTheNamedDeploymentNoLongerListsIsCleared(t *testing.T) {
	log := newCauseLog()
	rg, _ := yieldRigTwo(t, log, 4, 1)
	rg.batch.Enqueue(pending10h(8)...)
	rg.create(yieldMD("md-a"))
	d := mdSpec("md-d")
	d.Spec.Providers.Names = []string{yieldC}
	rg.create(d)
	tick10(rg) // 08:00:10: md-a makes P1 and X1 on A, md-d its primary on C
	tick10(rg) // 08:00:20
	rg.create(yieldMD("md-b"))
	tick10(rg) // 08:00:30
	tick10(rg) // 08:00:40
	rg.create(mdSpec("md-c"))
	tick10(rg) // 08:00:50: md-c is refused on A and marks md-b's extra
	if n := len(rg.allocations("md-d")); n != 1 {
		t.Fatalf("md-d has %d Allocations, want its primary, which fills C's limit of 1", n)
	}
	ms := marks(rg)
	if len(ms) != 1 || deploymentOfInstance(ms[0]) != "md-b" {
		t.Fatalf("the marks at 08:00:50 are %v, want one, md-b's extra", markNames(ms))
	}
	e2, e2Alloc := ms[0].Name, ms[0].Spec.AllocationRef.Name
	rg.mutateMD("md-c", func(m *v1alpha1.ModelDeployment) { m.Spec.Providers.Names = []string{yieldC} })
	for range 10 { // 08:01:00 to 08:02:40
		tick10(rg)
	}
	if _, deleted := log.deletedWith(e2Alloc); deleted {
		t.Fatalf("md-b's extra %s was cancelled for md-c, which no longer lists Provider A and could not use its slot", e2)
	}
	if in := instOrNil(rg, e2); in == nil || ended(in) {
		t.Fatalf("md-b's extra %v at 08:02:40, want it alive", in)
	}
	if ms := marks(rg); len(ms) != 0 {
		t.Fatalf("%v are marked at 08:02:40, want none: md-c's mark on A should have been cleared when A left its list", markNames(ms))
	}
}

// TestACutShortPrimarysRetryRefusedForTheLimitYieldsAnExtra (finding 3, stated limit (b), ruled a fix), in both reconcile
// orders (Y6). Limit 4. D1 makes its primary and extra at 08:00:10 (jobs pending 10 h, 2 listed). B's primary Instance is
// made at 08:00:30 and its Allocation's create is cut short (3 reserved, none listed); the controller restarts, which
// empties the slot table, and D2, created at 08:00:30, makes its primary and its extra at 08:00:40: 2 + 2 = 4 listed,
// the limit. B's retry in ensureAllocation is refused for the limit alone (4 >= 4), and the newest pending extra, D2's
// (made at 08:00:40, D1's at 08:00:10), must yield: it is cancelled and B's Allocation is made in its slot. B sorts last by
// name: its retry at 08:00:40 follows D2's pass. B sorting first: its pass at 08:00:40 is delayed (D1 and D2 are
// reconciled, B is not: the processing order, which the rig's name order cannot give) so D2's Allocations exist when B
// retries at 08:00:50. Today the guard counts B's chosen Instance with no Allocation as a primary that is not missing, and
// ensureAllocation passes no onLimit, so nothing yields and B waits for ever behind two pending extras.
// Expected by 08:04:20: one Allocation deleted, B with its one Allocation, D2 with its primary alone and D1 with both.
func TestACutShortPrimarysRetryRefusedForTheLimitYieldsAnExtra(t *testing.T) {
	for _, c := range []struct {
		name, b, d1, d2 string
		bFirst          bool
	}{
		{"the deployment with the cut-short primary sorts last", "md-c", "md-a", "md-b", false},
		{"the deployment with the cut-short primary sorts first", "md-a", "md-b", "md-c", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			log := newCauseLog()
			cut := &allocCutter{}
			qos := []v1alpha1.QOSProfile{{Name: "example-s4", MaxSubmitPerUser: i32(4)}}
			rg := newRig(t, append(qosOpts(qos, "", "example-s4"), withMaxTime(12*time.Hour), withInterceptors(cut.funcs(log.funcs())))...)
			rg.batch.Enqueue(pending10h(8)...)
			rg.create(yieldMD(c.d1))
			tick10(rg) // 08:00:10
			tick10(rg) // 08:00:20
			cut.arm(c.b, 0)
			rg.create(mdSpec(c.b))
			rg.clock.Advance(10 * time.Second) // 08:00:30
			stepTolerantAll(rg)
			if cut.failures() != 1 || len(rg.instances(c.b)) != 1 || len(rg.allocations(c.b)) != 0 {
				t.Fatalf("%s: %d cut creates, %d Instances, %d Allocations, want 1, 1 and 0: its pass must be cut short between the two creates", c.b, cut.failures(), len(rg.instances(c.b)), len(rg.allocations(c.b)))
			}
			cut.disarm()
			rg.restartController()
			rg.create(yieldMD(c.d2))
			rg.clock.Advance(10 * time.Second) // 08:00:40: D2 makes its two
			if c.bFirst {
				stepTolerantAll(rg, c.b)
			} else {
				stepTolerantAll(rg)
			}
			if a, b := len(rg.allocations(c.d1)), len(rg.allocations(c.d2)); a != 2 || b != 2 {
				t.Fatalf("%s has %d Allocations and %s %d at 08:00:40, want 2 each: the limit of 4 is full", c.d1, a, c.d2, b)
			}
			if c.bFirst {
				tick10(rg) // 08:00:50: B's retry
			}
			if d := rg.condition(c.b, v1alpha1.ConditionDegraded); d.Reason != v1alpha1.ReasonSubmitLimitReached {
				t.Fatalf("%s Degraded = %s/%s %q after its retry, want SubmitLimitReached: the retry is refused for the limit alone", c.b, d.Status, d.Reason, d.Message)
			}
			for rg.clock.Now().Before(hoT(8, 4, 20)) {
				tick10(rg)
			}
			if n := len(rg.allocations(c.b)); n != 1 {
				t.Fatalf("%s has %d Allocations at 08:04:20, want its primary's, made in the slot a pending extra yielded: its cut-short retry was refused for the limit alone and yielded nothing", c.b, n)
			}
			if n := cancelsOf(log); n != 1 {
				t.Fatalf("%d Allocations deleted, want %s's newest pending extra alone", n, c.d2)
			}
			if a, b := len(rg.liveInstances(c.d1)), len(rg.liveInstances(c.d2)); a != 2 || b != 1 {
				t.Fatalf("%s has %d live Instances and %s %d, want 2 (untouched) and 1 (its primary: its extra is the newest pending, and yielded)", c.d1, a, c.d2, b)
			}
		})
	}
}

// TestAYieldThatCouldNotAdmitThePrimaryIsNotMade (finding 5). Limit 4 on one Provider, no end time on any deployment, so
// every Running primary reserves one slot for its replacement. md-a (k = 1) makes P1 and the extra X1 at 08:00:10, jobs
// submitted at 08:00:20: one starts after 20 s (08:00:40: P1, the earlier start, is the primary) and one pends 10 h (X1);
// md-b (k = 0) makes P2 at 08:00:30, submitted at 08:00:40 and started after 20 s at 08:01:00. At 08:01:20, when md-c (k = 0)
// is first reconciled, P1 and P2 are Running. Count for md-c: listed P1, X1, P2 = 3, plus the slots reserved for P1's and
// P2's replacements = 2, total 5 >= 4: refused for the limit alone. Cancelling X1 leaves listed P1, P2 = 2 plus the 2
// reserved = 4 >= 4: md-c would still be refused, so the cancel admits nothing, and X1 would lose its queue age for
// nothing. Expected: md-c marks nothing, nothing is ever marked or cancelled through 08:03:00, X1 lives and md-c is
// Degraded SubmitLimitReached.
func TestAYieldThatCouldNotAdmitThePrimaryIsNotMade(t *testing.T) {
	log := newCauseLog()
	rg := yieldRig(t, log)
	rg.batch.Enqueue(pfake.Script{QueueWait: 20 * time.Second}, pfake.Script{QueueWait: 10 * time.Hour}, pfake.Script{QueueWait: 20 * time.Second})
	a := mdSpec("md-a")
	a.Spec.OverAllocation = ptrTo(int32(1))
	rg.create(a)
	tick10(rg) // 08:00:10: P1 and X1
	tick10(rg) // 08:00:20: submitted
	rg.create(mdSpec("md-b"))
	for range 5 { // 08:00:30 to 08:01:10
		tick10(rg)
	}
	running, pending := 0, 0
	for _, al := range rg.allAllocations() {
		switch al.Status.AtProvider.Phase {
		case v1alpha1.AllocationRunning:
			running++
		case v1alpha1.AllocationPending:
			pending++
		}
	}
	if len(rg.allAllocations()) != 3 || running != 2 || pending != 1 {
		t.Fatalf("%d Allocations, %d Running and %d Pending at 08:01:10, want 3, 2 and 1: P1 and P2 Running, X1 pending", len(rg.allAllocations()), running, pending)
	}
	rg.create(mdSpec("md-c"))
	tick10(rg) // 08:01:20: md-c is refused
	if c := rg.condition("md-c", v1alpha1.ConditionDegraded); c.Reason != v1alpha1.ReasonSubmitLimitReached {
		t.Fatalf("md-c Degraded = %s/%s %q at 08:01:20, want SubmitLimitReached: refused for the limit alone", c.Status, c.Reason, c.Message)
	}
	for range 10 { // to 08:03:00
		if ms := marks(rg); len(ms) != 0 {
			t.Fatalf("at %s %v is marked, want nothing: cancelling it leaves 2 listed + 2 reserved = 4 >= 4, so md-c is still refused", rg.clock.Now().Format(time.TimeOnly), markNames(ms))
		}
		tick10(rg)
	}
	if n := cancelsOf(log); n != 0 {
		t.Fatalf("%d Allocations were deleted, want none: a cancel could not admit md-c", n)
	}
	if n := len(rg.liveInstances("md-a")); n != 2 {
		t.Fatalf("md-a has %d live Instances at 08:03:00, want its primary and its extra", n)
	}
	if n := len(rg.allocations("md-c")); n != 0 {
		t.Fatalf("md-c has %d Allocations, want none: it stays refused while the two primaries and their reserved slots fill the limit", n)
	}
}

// markBlindReader is a Reader whose cache has not seen the yield marks: a List selecting by the yield-mark label returns
// nothing, as a lagging cache would right after the pass's own patch. Everything else reads through.
type markBlindReader struct{ client.Reader }

func (r markBlindReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*v1alpha1.InstanceList); ok {
		lo := &client.ListOptions{}
		for _, o := range opts {
			o.ApplyToList(lo)
		}
		if lo.LabelSelector != nil && lo.LabelSelector.Matches(labels.Set{labelYieldMark: "true"}) {
			return nil
		}
	}
	return r.Reader.List(ctx, list, opts...)
}

// TestTwoRefusalsInOnePassMarkOneExtraEvenIfTheReaderLags (Y2, the pass-local count). TestOneMissingPrimary... 's fixture:
// A and C full at their limit of 2 with a pending extra each, and md-c missing one primary, refused on both in ONE pass at
// 08:00:50. The marks are read through the Reader, which here has not seen the first mark when the second refusal asks
// (a lagging cache), so only the pass's own count tells that md-c already marked for its one missing primary.
// Expected: exactly one mark after the pass.
func TestTwoRefusalsInOnePassMarkOneExtraEvenIfTheReaderLags(t *testing.T) {
	log := newCauseLog()
	rg, c := yieldRigTwo(t, log, 2, 2)
	rg.batch.Enqueue(pending10h(2)...)
	c.batch.Enqueue(pending10h(2)...)
	rg.create(yieldMD("md-a"))
	tick10(rg)
	tick10(rg)
	d2 := yieldMD("md-b")
	d2.Spec.Providers.Names = []string{yieldC}
	rg.create(d2)
	tick10(rg)
	tick10(rg) // 08:00:40
	b := mdSpec("md-c")
	b.Spec.Providers.Names = []string{rigProvider, yieldC}
	rg.create(b)
	rg.r.Reader = markBlindReader{rg.c}
	tick10(rg) // 08:00:50
	rg.r.Reader = rg.c
	if ms := marks(rg); len(ms) != 1 {
		t.Fatalf("%d extras are marked after md-c's pass %v, want one: the pass's own count of its marks must hold when the Reader lags", len(ms), markNames(ms))
	}
}

// TestTheOwnerWaitsForTheNamedDeploymentsGeneration (Y5). The yield rig at A's limit of 4: md-c marks md-b's extra at
// 08:00:50 and, refused again at 08:01:00, keeps the mark, its status written at 08:01:00 (later than the mark, observed
// generation 1). Then its spec is edited (generation 2): its next pass, not yet run, would see the edit. md-b's owner pass,
// reconciled alone at that moment, finds the mark kept and md-c's status later than it, but md-c's observedGeneration (1)
// is not its generation (2): it WAITS. Expected: nothing cancelled, the mark kept and a requeue within 10 s. Once md-c has
// passed again at the new generation, still refused, the owner cancels.
func TestTheOwnerWaitsForTheNamedDeploymentsGeneration(t *testing.T) {
	log := newCauseLog()
	rg := yieldRig(t, log)
	rg.batch.Enqueue(pending10h(8)...)
	yieldStart(rg) // 08:00:50
	ms := marks(rg)
	if len(ms) != 1 || deploymentOfInstance(ms[0]) != "md-b" {
		t.Fatalf("the marks at 08:00:50 are %v, want one, md-b's extra", markNames(ms))
	}
	e2, e2Alloc := ms[0].Name, ms[0].Spec.AllocationRef.Name
	tick10(rg) // 08:01:00: md-c is refused again and keeps the mark
	if m := rg.md("md-c"); m.Status.LastReconcileTime == nil || !m.Status.LastReconcileTime.Time.Equal(hoT(8, 1, 0)) || m.Status.ObservedGeneration != m.Generation {
		t.Fatalf("md-c's status %+v at generation %d, want its pass of 08:01:00 observed at the current generation", m.Status.LastReconcileTime, m.Generation)
	}
	rg.mutateMD("md-c", func(m *v1alpha1.ModelDeployment) { m.Spec.Workload.TargetSessions = 8 })
	res, err := rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: "md-b"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, deleted := log.deletedWith(e2Alloc); deleted || len(marks(rg)) != 1 {
		t.Fatalf("md-b's extra was cancelled (%v) or its mark changed (%v): md-c's last pass did not see its current spec", deleted, markNames(marks(rg)))
	}
	if res.RequeueAfter <= 0 || res.RequeueAfter > 10*time.Second {
		t.Fatalf("md-b's pass asks to run again in %s, want within 10 s: it waits for md-c's next pass", res.RequeueAfter)
	}
	if _, err := rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: "md-c"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: "md-b"}}); err != nil {
		t.Fatal(err)
	}
	if in := instOrNil(rg, e2); in != nil && !ended(in) {
		t.Fatalf("md-b's extra is %s after md-c passed again at generation 2, want it cancelled", in.Status.Phase)
	}
}

// TestAMarkIsClearedWhenAnotherDeploymentsReservedSlotMakesTheCancelUseless (Y3 and Y4, the count rises after the mark).
// Limit 4 on one Provider. md-a (k = 1, no end time) makes P1 and X1 at 08:00:10, jobs submitted 08:00:20, one starting after
// 40 s (08:01:00: P1, the earlier start) and one pending 10 h (X1). md-b (k = 1, end time 14:00, so its handover reserves no
// slot) makes P2 and X2 at 08:00:30, both pending 10 h: 4 listed. md-c (k = 0, instances 2), created 08:00:40, is refused at
// 08:00:50: 4 listed + 0 reserved = 4 <= 4, so it marks md-b's extra (the newest pending), for a shortage of 2. At 08:01:00
// P1 is Running and reserves one slot for its replacement (md-a has no end time): 4 listed + 1 reserved = 5 > 4. Cancelling
// the marked extra would leave 4, still the limit, so md-c's cancel can admit nothing now: md-c marks NO second extra, and
// its outstanding mark is CLEARED (it keeps at most 1 mark less the excess of 1 over the limit = 0).
// Expected: one mark at 08:00:50; none at 08:01:00 and after; nothing deleted through 08:02:30; md-c without an Allocation.
func TestAMarkIsClearedWhenAnotherDeploymentsReservedSlotMakesTheCancelUseless(t *testing.T) {
	log := newCauseLog()
	rg := yieldRig(t, log)
	rg.batch.Enqueue(append([]pfake.Script{{QueueWait: 40 * time.Second}, {QueueWait: 10 * time.Hour}}, pending10h(2)...)...)
	a := mdSpec("md-a")
	a.Spec.OverAllocation = ptrTo(int32(1))
	rg.create(a)
	tick10(rg) // 08:00:10
	tick10(rg) // 08:00:20
	rg.create(yieldMD("md-b"))
	tick10(rg) // 08:00:30
	tick10(rg) // 08:00:40
	c := mdSpec("md-c")
	c.Spec.Instances = 2
	rg.create(c)
	tick10(rg) // 08:00:50
	if n := len(rg.allAllocations()); n != 4 {
		t.Fatalf("%d Allocations at 08:00:50, want 4: the limit is full", n)
	}
	ms := marks(rg)
	if len(ms) != 1 || deploymentOfInstance(ms[0]) != "md-b" {
		t.Fatalf("the marks at 08:00:50 are %v, want one, md-b's extra: 4 listed + 0 reserved = 4, a cancel admits md-c", markNames(ms))
	}
	for range 10 { // 08:01:00 to 08:02:30
		tick10(rg)
		if ms := marks(rg); len(ms) != 0 {
			t.Fatalf("at %s %v is marked, want nothing: P1 is Running, 4 listed + 1 reserved = 5, a cancel leaves 4 and admits nothing", rg.clock.Now().Format(time.TimeOnly), markNames(ms))
		}
	}
	if n := cancelsOf(log); n != 0 {
		t.Fatalf("%d Allocations were deleted, want none", n)
	}
	if n := len(rg.allocations("md-c")); n != 0 {
		t.Fatalf("md-c has %d Allocations, want none: its two primaries wait", n)
	}
}

// TestShortageIsCountedByAllocationPresence (Y1): shortageOf is the desired count less the units, one per hedge, that have
// an Allocation. An Instance with no Allocation adds nothing, whichever the selection chooses.
func TestShortageIsCountedByAllocationPresence(t *testing.T) {
	inst := func(name, uid string) *v1alpha1.Instance {
		return &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(uid), CreationTimestamp: metav1.NewTime(hoT(8, 0, 0))}}
	}
	alloc := &v1alpha1.Allocation{}
	p, x := inst("p", "bbbbbbbb"), inst("x", "aaaaaaaa") // x has the lower UID and wins the selection's tie
	for _, c := range []struct {
		name    string
		insts   []*v1alpha1.Instance
		has     map[string]bool
		desired int
		want    int
	}{
		{"nothing made", nil, nil, 1, 1},
		{"a cut-short primary alone: missing", []*v1alpha1.Instance{p}, nil, 1, 1},
		{"the primary has its Allocation, the extra with the lower UID is cut short: none missing", []*v1alpha1.Instance{p, x}, map[string]bool{"p": true}, 1, 0},
		{"the extra with the lower UID has its Allocation, the primary is cut short: none missing", []*v1alpha1.Instance{p, x}, map[string]bool{"x": true}, 1, 0},
		{"two wanted, one with its Allocation, one cut short: one missing", []*v1alpha1.Instance{p, x}, map[string]bool{"p": true}, 2, 1},
		{"both have theirs and two wanted: none", []*v1alpha1.Instance{p, x}, map[string]bool{"p": true, "x": true}, 2, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			sel := selection{insts: c.insts, allocOf: func(in *v1alpha1.Instance) *v1alpha1.Allocation {
				if c.has[in.Name] {
					return alloc
				}
				return nil
			}}
			if got := shortageOf(sel, c.desired); got != c.want {
				t.Fatalf("shortage = %d, want %d", got, c.want)
			}
		})
	}
}
