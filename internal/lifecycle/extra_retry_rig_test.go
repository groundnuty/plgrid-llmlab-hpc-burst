package lifecycle

// The rows for review finding 4 of #708 (#616 PR 4): an extra whose Allocation create was cut short is retried by
// ensureAllocation (instances.go), and the retry takes the extra's own checks, the bound, the slot beyond its primary's
// handover, the admission headroom and the per-user caps, silently, through the one function createExtras uses (extraChecks);
// when one refuses, the Instance is ended, so it never takes a slot its primary's handover needs and never sets
// SubmitLimitReached on a deployment that is fully served (D7: a refused extra is silent).
//
// The state is built through the real path. The Provider's submit limit is 3. The deployment has k = 1 and an end time of
// 14:00, before the primary's drain start 08:00:10 + 12 h - 2 h 10 min = 17:50:10, so its handover reserves no slot and the
// extra is admitted at 08:00:10 (1 listed + alsoLeave 0 + 1 + headroom 1 = 3 within 3); the extra's Allocation create is cut
// short. The end time is then lifted (the deployment now serves past the drain start: the primary's handover will need a
// slot, which is what makes the retry a violation). With the end time kept, the same checks pass.

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// TestACutShortExtrasRetryTakesTheExtrasChecks: the two ways the retry goes wrong, one subtest each.
//   - the primary is still pending (its job queued 10 h): it holds no reserved slot yet, so a retry with no checks sees 1
//     listed + 0 reserved = 1 < 3 and submits the extra, 2 Allocations, and the primary's handover finds a slot short. The
//     extra's own checks leave 1 slot beside it for the primary (alsoLeave 1) and the headroom 1: 1 + 1 + 1 + 1 = 4 > 3, and
//     refuse it. Expected: one Allocation (the primary's) at 08:01:10.
//   - the primary serves (job started at 08:00:20, ready at 08:01:10): it holds the reserved slot, 1 listed + 1 reserved
//   - 1 + 1 = 4 > 3, so the retry is refused, and a retry that reported it would set SubmitLimitReached on a deployment
//     that is fully served. Expected: ready 1 and Degraded False/AsDesired at 08:02:00, one Allocation.
func TestACutShortExtrasRetryTakesTheExtrasChecks(t *testing.T) {
	for _, c := range []struct {
		name     string
		pPending bool
	}{
		{"the primary pends: the retry must not take the slot its handover needs", true},
		{"the primary serves: the retry must not set SubmitLimitReached on a fully served deployment", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			log := newCauseLog()
			cut := &allocCutter{}
			rg := newRig(t, append(limitOpts(3), withInterceptors(cut.funcs(log.funcs())))...)
			if c.pPending {
				rg.batch.Enqueue(pfake.Script{QueueWait: 10 * time.Hour})
			}
			cut.arm(hoMD, 1) // the primary's Allocation is made, the extra's is cut short
			rg.create(extraMD(hoMD))
			rg.clock.Advance(10 * time.Second)
			stepTolerantAll(rg) // 08:00:10
			if cut.failures() != 1 || len(rg.instances(hoMD)) != 2 || len(rg.allocations(hoMD)) != 1 {
				t.Fatalf("%d cut creates, %d Instances and %d Allocations at 08:00:10, want 1, 2 and 1: the extra's Instance exists without its Allocation", cut.failures(), len(rg.instances(hoMD)), len(rg.allocations(hoMD)))
			}
			rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.EndTime = nil })
			if c.pPending {
				cut.disarm()
			}
			for range 6 { // to 08:01:10
				rg.clock.Advance(10 * time.Second)
				if c.pPending {
					rg.Step()
				} else {
					stepTolerantAll(rg)
				}
			}
			if c.pPending {
				if n := len(rg.allocations(hoMD)); n != 1 {
					t.Fatalf("%d Allocations at 08:01:10, want the primary's alone: the extra's retry took the slot its primary's handover needs", n)
				}
				return
			}
			for range 5 { // to 08:02:00
				rg.clock.Advance(10 * time.Second)
				stepTolerantAll(rg)
			}
			if n := rg.readyCount(hoMD); n != 1 {
				t.Fatalf("%d ready at 08:02:00, want the primary: the row proves nothing on a deployment that is not served", n)
			}
			d := rg.condition(hoMD, v1alpha1.ConditionDegraded)
			if d.Status != metav1.ConditionFalse || d.Reason != v1alpha1.ReasonAsDesired {
				t.Fatalf("Degraded = %s/%s %q on a fully served deployment at 08:02:00, want False/AsDesired: the cut-short extra's retry was refused and reported", d.Status, d.Reason, d.Message)
			}
			if n := len(rg.allocations(hoMD)); n != 1 {
				t.Fatalf("%d Allocations at 08:02:00, want the primary's alone", n)
			}
		})
	}
}

// TestACutShortExtrasRetryThatPassesItsChecksMakesItsAllocation is the positive control of the rows above: with the end time
// kept at 14:00, before the primary's drain start, the extra's checks pass at the retry exactly as they did at its creation
// (1 listed + alsoLeave 0 + 1 + headroom 1 = 3 within the limit of 3), so the retry must make the extra's Allocation:
// 2 Allocations at 08:01:10, and a deployment that is not Degraded. A fix that ended every cut-short extra would pass the
// rows above and fail this one.
func TestACutShortExtrasRetryThatPassesItsChecksMakesItsAllocation(t *testing.T) {
	log := newCauseLog()
	cut := &allocCutter{}
	rg := newRig(t, append(limitOpts(3), withInterceptors(cut.funcs(log.funcs())))...)
	cut.arm(hoMD, 1)
	rg.create(extraMD(hoMD))
	rg.clock.Advance(10 * time.Second)
	stepTolerantAll(rg) // 08:00:10
	if cut.failures() != 1 || len(rg.instances(hoMD)) != 2 || len(rg.allocations(hoMD)) != 1 {
		t.Fatalf("%d cut creates, %d Instances and %d Allocations at 08:00:10, want 1, 2 and 1", cut.failures(), len(rg.instances(hoMD)), len(rg.allocations(hoMD)))
	}
	cut.disarm()
	for range 6 { // to 08:01:10
		rg.clock.Advance(10 * time.Second)
		rg.Step()
	}
	if n := len(rg.allocations(hoMD)); n != 2 {
		t.Fatalf("%d Allocations at 08:01:10, want 2: the extra's checks pass, so its retry makes its Allocation", n)
	}
	if d := rg.condition(hoMD, v1alpha1.ConditionDegraded); d.Reason == v1alpha1.ReasonSubmitLimitReached {
		t.Fatalf("Degraded = %s/%s %q, want no SubmitLimitReached", d.Status, d.Reason, d.Message)
	}
}

// uidSetter gives the Instances of one deployment fixed UIDs, in the order they are created: the Nth Instance created
// gets uids[N], so a row decides which of two same-second Instances the selection's tie takes (the lower UID).
type uidSetter struct {
	mu   sync.Mutex
	md   string
	uids []types.UID
	n    int
}

func (u *uidSetter) funcs(base interceptor.Funcs) interceptor.Funcs {
	prev := base.Create
	base.Create = func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		if in, ok := obj.(*v1alpha1.Instance); ok && in.Labels[v1alpha1.LabelModelDeployment] == u.md {
			u.mu.Lock()
			if u.n < len(u.uids) {
				obj.SetUID(u.uids[u.n])
			}
			u.n++
			u.mu.Unlock()
		}
		if prev != nil {
			return prev(ctx, cl, obj, opts...)
		}
		return cl.Create(ctx, obj, opts...)
	}
	return base
}

// TestACutShortExtraWithTheLowerUIDTakesTheExtrasChecks (Y1 and E: the selection's tie never decides). Limit
// 5. md-a (k = 1, end time 14:00) holds its primary and pending extra: 2 listed. md-c (the same) makes its primary P, with UID
// bbbbbbbb, and its Allocation at 08:00:30 (3 listed), then its extra X, with the LOWER UID aaaaaaaa (3 + 0 + 1 + headroom 1 =
// 5 <= 5: admitted), whose Allocation's create is cut short; the controller restarts (the slot table is lost) and md-b (k = 0),
// created at 08:00:30, takes a slot at 08:00:40: P1, X1, P and P2, 4 listed. Both of md-c's Instances are pending with no job started, made in one
// second, so the selection's tie chooses the lower UID, X, with no Allocation, as md-c's primary. The deployment's units
// that HAVE an Allocation number its desired count (1: P's), so X is the extra, not a missing primary: shortage 0.
// X's retry is refused by the extra's own checks (4 listed + 1 + headroom 1 = 6 > the limit of 5), silently, and X is ended.
// Expected by 08:02:00: X ended with no Allocation, P alive with its Allocation, nothing deleted, md-c with no
// SubmitLimitReached.
func TestACutShortExtraWithTheLowerUIDTakesTheExtrasChecks(t *testing.T) {
	log := newCauseLog()
	cut := &allocCutter{}
	uids := &uidSetter{md: "md-c", uids: []types.UID{"bbbbbbbb-0000-0000-0000-000000000000", "aaaaaaaa-0000-0000-0000-000000000000"}}
	rg := newRig(t, append(limitOpts(5), withInterceptors(uids.funcs(cut.funcs(log.funcs()))))...)
	rg.batch.Enqueue(pending10h(8)...)
	rg.create(extraMD("md-a"))
	tick10(rg)         // 08:00:10
	tick10(rg)         // 08:00:20
	cut.arm("md-c", 1) // P's Allocation is made, X's is cut short
	rg.create(extraMD("md-c"))
	rg.clock.Advance(10 * time.Second) // 08:00:30
	stepTolerantAll(rg)
	if cut.failures() != 1 || len(rg.instances("md-c")) != 2 || len(rg.allocations("md-c")) != 1 {
		t.Fatalf("md-c: %d cut creates, %d Instances, %d Allocations, want 1, 2 and 1: X's Instance exists without its Allocation", cut.failures(), len(rg.instances("md-c")), len(rg.allocations("md-c")))
	}
	var x, pr v1alpha1.Instance
	for _, in := range rg.instances("md-c") {
		if strings.HasPrefix(string(in.UID), "aaaaaaaa") {
			x = in
		} else {
			pr = in
		}
	}
	if x.Name == "" || pr.Name == "" || x.Spec.AllocationRef.Name == pr.Spec.AllocationRef.Name {
		t.Fatalf("the Instances %q and %q: the row needs P (UID bbbbbbbb) and X (UID aaaaaaaa)", pr.Name, x.Name)
	}
	if _, ok := rg.allocation(x.Spec.AllocationRef.Name); ok {
		t.Fatalf("X's Allocation %s exists: the row proves nothing", x.Spec.AllocationRef.Name)
	}
	cut.disarm()
	rg.restartController()
	rg.create(mdSpec("md-b"))
	for range 9 { // 08:00:40 to 08:02:00
		tick10(rg)
	}
	if got := instOrNil(rg, x.Name); got != nil && !ended(got) {
		t.Fatalf("X is %s at 08:02:00, want it ended (or gone): the extra's checks refuse its retry (4 listed + 1 + 1 > 5)", got.Status.Phase)
	}
	if _, ok := rg.allocation(x.Spec.AllocationRef.Name); ok {
		t.Fatalf("X's Allocation %s was made, want none", x.Spec.AllocationRef.Name)
	}
	if got := instOrNil(rg, pr.Name); got == nil || ended(got) {
		t.Fatalf("P %v at 08:02:00, want it alive with its Allocation", got)
	}
	if n := len(rg.liveInstances("md-c")); n != 1 {
		t.Fatalf("md-c has %d live Instances at 08:02:00, want P alone", n)
	}
	if n := cancelsOf(log); n != 0 {
		t.Fatalf("%d Allocations were deleted, want none", n)
	}
	if d := rg.condition("md-c", v1alpha1.ConditionDegraded); d.Reason == v1alpha1.ReasonSubmitLimitReached {
		t.Fatalf("md-c Degraded = %s/%s %q, want no SubmitLimitReached: a refused extra is silent", d.Status, d.Reason, d.Message)
	}
}

// TestACutShortExtrasRetryIsHeldToTheCaps: the extra's per-user caps check at the retry. A QOS caps our GPUs at 2 per user, with
// no submit limit. md-b (k = 1, end time 14:00) makes its primary P and the extra X at 08:00:10 (1 + 1 = 2 GPUs, within the
// cap) and X's Allocation's create is cut short; the controller restarts (X's own reservation is lost), and md-a (k = 0), created
// then, makes its primary at 08:00:20, before md-b's pass in the step (the names sort so): our Allocations hold 1 + 1 = 2 GPUs
// and X's would be the third, 3 > 2. X's retry is refused by the caps (silently) and X is ended.
// Expected at 08:01:10: X ended with no Allocation, md-b with its primary's alone, md-a with its own, no SubmitLimitReached.
func TestACutShortExtrasRetryIsHeldToTheCaps(t *testing.T) {
	log := newCauseLog()
	cut := &allocCutter{}
	qos := []v1alpha1.QOSProfile{{Name: "example-g2", MaxGPUsPerUser: i32(2)}}
	rg := newRig(t, append(qosOpts(qos, "", "example-g2"), withMaxTime(12*time.Hour), withInterceptors(cut.funcs(log.funcs())))...)
	cut.arm("md-b", 1) // P's Allocation is made, X's is cut short
	rg.create(extraMD("md-b"))
	rg.clock.Advance(10 * time.Second)
	stepTolerantAll(rg) // 08:00:10
	if cut.failures() != 1 || len(rg.instances("md-b")) != 2 || len(rg.allocations("md-b")) != 1 {
		t.Fatalf("md-b: %d cut creates, %d Instances, %d Allocations, want 1, 2 and 1: the extra's Instance exists without its Allocation (the extra was admitted: 2 GPUs within the cap of 2)", cut.failures(), len(rg.instances("md-b")), len(rg.allocations("md-b")))
	}
	var xName string
	for _, in := range rg.instances("md-b") {
		if _, ok := rg.allocation(in.Spec.AllocationRef.Name); !ok {
			xName = in.Name
		}
	}
	cut.disarm()
	rg.restartController()
	rg.create(mdSpec("md-a"))
	for range 6 { // 08:00:20 to 08:01:10
		tick10(rg)
	}
	if got := instOrNil(rg, xName); got != nil && !ended(got) {
		t.Fatalf("X is %s at 08:01:10, want it ended (or gone): its retry would be the third GPU against a cap of 2", got.Status.Phase)
	}
	if n := len(rg.liveInstances("md-b")); n != 1 {
		t.Fatalf("md-b has %d live Instances at 08:01:10, want its primary alone", n)
	}
	if a, b := len(rg.allocations("md-a")), len(rg.allocations("md-b")); a != 1 || b != 1 {
		t.Fatalf("md-a has %d Allocations and md-b %d, want 1 each: no Allocation for the extra", a, b)
	}
	if d := rg.condition("md-b", v1alpha1.ConditionDegraded); d.Reason == v1alpha1.ReasonSubmitLimitReached {
		t.Fatalf("md-b Degraded = %s/%s %q, want no SubmitLimitReached", d.Status, d.Reason, d.Message)
	}
}
