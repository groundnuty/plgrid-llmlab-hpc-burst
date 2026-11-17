package lifecycle

// A pending extra yields its submit slot (HBEP-7, Over-allocation and qualification (beta), Its checks; amendment #674;
// #616, PR 4): a missing primary of any deployment that is refused on a Provider for its submit limit alone makes the
// newest pending extra there yield, which its owner cancels as over-allocation's own act. Three deployments share one
// Provider with a submit limit of 4: md-a, md-b and md-c, created at 08:00:00, 08:00:20 and 08:00:40, so their passes are
// at 08:00:10, 08:00:30 and 08:00:50 and the rig reconciles them in that order (the names sort) in every later step.
// md-a and md-b each make a primary and PR 4's extra, 2 + 2 = 4 Allocations at the limit; md-c's primary, refused at
// 08:00:50, needs a slot. Every expected time and count is worked by hand in the row.
//
// The mark is the annotation annotationYieldTo, "<ns>/<deployment> <RFC3339 time of the marking pass>"; the owner
// cancels once the named deployment's status.lastReconcileTime is LATER than the mark's time, since the marking pass
// writes its own time there: a later pass of it was refused again and kept the mark.

import (
	"context"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

const yieldDeployment = rigNS + "/md-c"

func yieldRig(t *testing.T, log *causeLog) *rig {
	t.Helper()
	qos := []v1alpha1.QOSProfile{{Name: "example-s4", MaxSubmitPerUser: i32(4)}}
	return newRig(t, append(qosOpts(qos, "", "example-s4"), withMaxTime(12*time.Hour), withInterceptors(log.funcs()))...)
}

func tick10(rg *rig) {
	rg.t.Helper()
	rg.clock.Advance(10 * time.Second)
	rg.Step()
}

// yieldMD is a deployment with k = 1 and an end time, 14:00, before its primary's expected drain start (08:00:10 + 12 h
// - 2 h 10 min = 17:50:10 and later): it holds no slot for a replacement, so an extra's slot is the one beyond what is
// listed and expected (1 + 0 + 1), and four Allocations fill the limit of 4.
func yieldMD(name string) *v1alpha1.ModelDeployment {
	md := mdSpec(name)
	md.Spec.OverAllocation = ptrTo(int32(1))
	end := metav1.NewTime(hoT(14, 0, 0))
	md.Spec.EndTime = &end
	return md
}

// yieldStart makes md-a and md-b, each with its primary and extra, and ends with md-c's pass at 08:00:50, where its
// primary is refused for the limit.
func yieldStart(rg *rig) {
	rg.t.Helper()
	rg.create(yieldMD("md-a"))
	tick10(rg) // 08:00:10: md-a makes P1 and X1
	tick10(rg) // 08:00:20: their jobs are submitted
	rg.create(yieldMD("md-b"))
	tick10(rg) // 08:00:30: md-b makes P2 and X2
	tick10(rg) // 08:00:40: their jobs are submitted
	rg.create(yieldMD("md-c"))
	tick10(rg) // 08:00:50: md-c's primary is refused
	if !rg.clock.Now().Equal(hoT(8, 0, 50)) {
		rg.t.Fatalf("the rig is at %s, want 08:00:50", rg.clock.Now().Format(time.TimeOnly))
	}
	if a, b := len(rg.allocations("md-a")), len(rg.allocations("md-b")); a != 2 || b != 2 {
		rg.t.Fatalf("md-a has %d Allocations and md-b %d at 08:00:50, want 2 each: the limit of 4 is full", a, b)
	}
	if n := len(rg.allocations("md-c")); n != 0 {
		rg.t.Fatalf("md-c has %d Allocations at 08:00:50, want none: its primary is refused for the limit", n)
	}
}

// marks returns the Instances of the store that carry a yield mark.
func marks(rg *rig) []v1alpha1.Instance {
	rg.t.Helper()
	var il v1alpha1.InstanceList
	if err := rg.c.List(rg.ctx, &il); err != nil {
		rg.t.Fatal(err)
	}
	var out []v1alpha1.Instance
	for _, in := range il.Items {
		if in.Annotations[annotationYieldTo] != "" {
			out = append(out, in)
		}
	}
	return out
}

func deploymentOfInstance(in v1alpha1.Instance) string {
	return in.Labels[v1alpha1.LabelModelDeployment]
}

func pending10h(n int) []pfake.Script {
	out := make([]pfake.Script, n)
	for i := range out {
		out[i] = pfake.Script{QueueWait: 10 * time.Hour}
	}
	return out
}

// TestAPendingExtraYieldsItsSlotToAMissingPrimary: the ruling's row. All four jobs pend 10 h. At 08:00:50 md-c's primary
// is refused (4 listed Allocations at the limit of 4, and the caps pass), and md-c marks the NEWEST pending surplus
// Instance on the Provider: md-b's, made at 08:00:30, not md-a's, made at 08:00:10. Nothing else is marked, then or
// later, through 08:10:00: one refused primary across many passes yields exactly one extra. md-b's pass cancels it with
// the trim cause and the release cause that names the yield; md-a's extra is untouched. md-c's primary is submitted once
// the slot frees, and neither md-b nor md-c makes a new extra while the count leaves no slot.
func TestAPendingExtraYieldsItsSlotToAMissingPrimary(t *testing.T) {
	log := newCauseLog()
	rg := yieldRig(t, log)
	rg.batch.Enqueue(pending10h(8)...)
	yieldStart(rg)
	ms := marks(rg)
	if len(ms) != 1 || deploymentOfInstance(ms[0]) != "md-b" || ms[0].Annotations[annotationYieldTo] != yieldDeployment+" 2026-11-02T08:00:50Z" {
		t.Fatalf("the marks at 08:00:50 are %v, want one, on md-b's surplus Instance, %q", markNames(ms), yieldDeployment+" 2026-11-02T08:00:50Z")
	}
	e2, e2Alloc := ms[0].Name, ms[0].Spec.AllocationRef.Name
	seen := map[string]bool{e2: true}
	for range 57 { // to 08:10:20
		tick10(rg)
		for _, m := range marks(rg) {
			seen[m.Name] = true
			if deploymentOfInstance(m) == "md-a" {
				t.Fatalf("at %s md-a's Instance %s is marked: the newest pending extra is md-b's, and one refused primary yields one", rg.clock.Now().Format(time.TimeOnly), m.Name)
			}
		}
	}
	if len(seen) != 1 {
		t.Fatalf("%d Instances were ever marked, want exactly md-b's %s: one refused primary across many passes yields one extra", len(seen), e2)
	}
	if in := instOrNil(rg, e2); in != nil && !ended(in) {
		t.Fatalf("md-b's extra %s is %s at 08:10:20, want it cancelled by its owner", e2, in.Status.Phase)
	}
	if cause, ok := log.deletedWith(e2Alloc); !ok || cause != "Instance "+e2+" ended: yielded its submit slot to "+yieldDeployment {
		t.Fatalf("md-b's extra's Allocation was deleted with the cause %q (deleted %v), want the one that names the yield", cause, ok)
	}
	if a, b, c := len(rg.liveInstances("md-a")), len(rg.liveInstances("md-b")), len(rg.liveInstances("md-c")); a != 2 || b != 1 || c != 1 {
		t.Fatalf("live Instances: md-a %d, md-b %d, md-c %d, want 2 (primary and extra untouched), 1 (its primary) and 1 (its primary, submitted in the freed slot)", a, b, c)
	}
	if n, s := len(rg.allAllocations()), rg.submissions(); n != 4 || s != 5 {
		t.Fatalf("%d Allocations and %d jobs, want 4 (at the limit: no new extra for md-b or md-c) and 5 (md-b's cancelled extra's job was submitted)", n, s)
	}
}

// instOrNil is the Instance of the store by name, whatever its deployment, or nil once it is gone.
func instOrNil(rg *rig, name string) *v1alpha1.Instance {
	rg.t.Helper()
	var in v1alpha1.Instance
	switch err := rg.c.Get(rg.ctx, types.NamespacedName{Namespace: rigNS, Name: name}, &in); {
	case apierrors.IsNotFound(err):
		return nil
	case err != nil:
		rg.t.Fatal(err)
	}
	return &in
}

func markNames(ms []v1alpha1.Instance) []string {
	var out []string
	for _, m := range ms {
		out = append(out, deploymentOfInstance(m)+"/"+m.Name)
	}
	return out
}

// TestAStartedExtraNeverYields: all four jobs start at once, so the extras are started when md-c's primary is refused at
// 08:00:50: no pending extra is there to yield. Nothing is marked, nothing is cancelled, md-c's primary stays refused and
// md-c is Degraded SubmitLimitReached.
func TestAStartedExtraNeverYields(t *testing.T) {
	log := newCauseLog()
	rg := yieldRig(t, log)
	yieldStart(rg)
	for range 60 { // to 08:10:50
		tick10(rg)
		if ms := marks(rg); len(ms) != 0 {
			t.Fatalf("at %s %v are marked, want none: the extras started", rg.clock.Now().Format(time.TimeOnly), markNames(ms))
		}
	}
	for _, md := range []string{"md-a", "md-b"} {
		if n := len(rg.allocations(md)); n != 2 {
			t.Fatalf("%s has %d Allocations, want its primary's and its extra's: none was cancelled", md, n)
		}
	}
	if n := len(rg.allocations("md-c")); n != 0 {
		t.Fatalf("md-c has %d Allocations, want none: the slot never freed", n)
	}
	if c := rg.condition("md-c", v1alpha1.ConditionDegraded); c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonSubmitLimitReached {
		t.Fatalf("md-c Degraded = %s/%s %q, want True/SubmitLimitReached", c.Status, c.Reason, c.Message)
	}
}

// TestTheMarkedExtraStartsBeforeItsOwnersPass: md-b's jobs wait 20 s, submitted at 08:00:40, so they start at 08:01:00. md-c
// marks md-b's surplus Instance at 08:00:50. At 08:01:00 md-b's pass finds the job started, so it is never cancelled: the
// mark is cleared. md-c's pass that follows in the same step is refused again, and marks the next newest pending extra,
// md-a's, at 08:01:00, which md-a's pass then cancels.
func TestTheMarkedExtraStartsBeforeItsOwnersPass(t *testing.T) {
	log := newCauseLog()
	rg := yieldRig(t, log)
	short := pfake.Script{QueueWait: 20 * time.Second}
	rg.batch.Enqueue(append(pending10h(2), append([]pfake.Script{short, short}, pending10h(4)...)...)...)
	yieldStart(rg)
	ms := marks(rg)
	if len(ms) != 1 || deploymentOfInstance(ms[0]) != "md-b" {
		t.Fatalf("the marks at 08:00:50 are %v, want one, on md-b's surplus Instance", markNames(ms))
	}
	e2, e2Alloc := ms[0].Name, ms[0].Spec.AllocationRef.Name
	tick10(rg) // 08:01:00: md-b's jobs start, md-b clears the mark, md-c marks md-a's extra
	if in := instOrNil(rg, e2); in == nil || in.Annotations[annotationYieldTo] != "" || ended(in) {
		t.Fatalf("md-b's extra %v at 08:01:00, want it alive and unmarked: it started before its owner's pass", in)
	}
	ms = marks(rg)
	if len(ms) != 1 || deploymentOfInstance(ms[0]) != "md-a" || ms[0].Annotations[annotationYieldTo] != yieldDeployment+" 2026-11-02T08:01:00Z" {
		t.Fatalf("the marks at 08:01:00 are %v, want one, on md-a's surplus Instance, %q", markNames(ms), yieldDeployment+" 2026-11-02T08:01:00Z")
	}
	e1 := ms[0].Name
	rg.run(4*time.Minute, 10*time.Second)
	if _, deleted := log.deletedWith(e2Alloc); deleted {
		t.Fatal("md-b's started extra's Allocation was deleted: a started extra is never cancelled for a yield")
	}
	if in := instOrNil(rg, e1); in != nil && !ended(in) {
		t.Fatalf("md-a's extra %s is %s at 08:05:00, want it cancelled by its owner", e1, in.Status.Phase)
	}
	if n := len(rg.allocations("md-c")); n != 1 {
		t.Fatalf("md-c has %d Allocations at 08:05:00, want its primary, submitted in the freed slot", n)
	}
}

// TestAMarkNamesADeploymentWhoseNeedWentAway: md-c marks at 08:00:50, and its need goes away (it is scaled to zero) before
// md-b's pass. At 08:01:00 md-b re-reads md-c, which has no missing primary any more: the mark is cleared and md-b's
// extra is NOT cancelled, now or later.
func TestAMarkNamesADeploymentWhoseNeedWentAway(t *testing.T) {
	log := newCauseLog()
	rg := yieldRig(t, log)
	rg.batch.Enqueue(pending10h(8)...)
	yieldStart(rg)
	ms := marks(rg)
	if len(ms) != 1 {
		t.Fatalf("the marks at 08:00:50 are %v, want one", markNames(ms))
	}
	e2, e2Alloc := ms[0].Name, ms[0].Spec.AllocationRef.Name
	rg.mutateMD("md-c", func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 0 })
	tick10(rg) // 08:01:00
	if in := instOrNil(rg, e2); in == nil || in.Annotations[annotationYieldTo] != "" || ended(in) {
		t.Fatalf("md-b's extra %v at 08:01:00, want it alive with its mark cleared", in)
	}
	rg.run(5*time.Minute, 10*time.Second)
	if _, deleted := log.deletedWith(e2Alloc); deleted {
		t.Fatal("md-b's extra's Allocation was deleted for a deployment that no longer needs the slot")
	}
	if in := instOrNil(rg, e2); in == nil || ended(in) || len(marks(rg)) != 0 {
		t.Fatalf("md-b's extra %v and marks %v at 08:06:00, want it alive and no mark anywhere", in, markNames(marks(rg)))
	}
}

// TestTheOwnerWaitsForTheNamedDeploymentsNextPass: md-c's marking pass wrote its status at 08:00:50, the time the mark
// carries. md-b's pass at the same moment finds status.lastReconcileTime no later than the mark: it waits, keeps the mark,
// cancels nothing, and asks to run again within 10 s. Once md-c has passed again, still refused, at 08:01:00, the pass at
// 08:01:10 cancels.
func TestTheOwnerWaitsForTheNamedDeploymentsNextPass(t *testing.T) {
	log := newCauseLog()
	rg := yieldRig(t, log)
	rg.batch.Enqueue(pending10h(8)...)
	yieldStart(rg)
	ms := marks(rg)
	if len(ms) != 1 {
		t.Fatalf("the marks at 08:00:50 are %v, want one", markNames(ms))
	}
	e2, e2Alloc := ms[0].Name, ms[0].Spec.AllocationRef.Name
	res, err := rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: "md-b"}})
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter <= 0 || res.RequeueAfter > 10*time.Second {
		t.Fatalf("md-b's pass asks to run again in %s, want within 10 s: it waits for md-c's next pass", res.RequeueAfter)
	}
	if _, deleted := log.deletedWith(e2Alloc); deleted || len(marks(rg)) != 1 {
		t.Fatalf("md-b's extra was cancelled or its mark cleared before md-c's next pass (deleted %v, marks %v)", deleted, markNames(marks(rg)))
	}
	tick10(rg) // 08:01:00: md-b still waits (md-c's status is from 08:00:50), then md-c passes again
	if _, deleted := log.deletedWith(e2Alloc); deleted || len(marks(rg)) != 1 {
		t.Fatalf("at 08:01:00 md-b's extra was cancelled (deleted %v) or the mark changed (%v): md-c's status shows no pass after the mark yet", deleted, markNames(marks(rg)))
	}
	tick10(rg) // 08:01:10: md-c's status shows 08:01:00, after the mark
	if in := instOrNil(rg, e2); in != nil && !ended(in) {
		t.Fatalf("md-b's extra is %s at 08:01:10, want it cancelled: md-c passed again, refused, and kept the mark", in.Status.Phase)
	}
	if cause, ok := log.deletedWith(e2Alloc); !ok || cause != "Instance "+e2+" ended: yielded its submit slot to "+yieldDeployment {
		t.Fatalf("cause %q (deleted %v), want the yield's", cause, ok)
	}
}

// TestAHigherDegradedReasonDoesNotMaskARefusal: SubmitLimitReached is the ninth of degradedOrder, so a higher reason on md-c,
// RestartLoop here, would hide it from anything that read md-c's Degraded: the owner would clear the mark and md-c would
// re-mark on every pass, a loop that starves its primary. The owner reads md-c's lastReconcileTime and the mark's
// survival instead. md-c's status is written with Degraded True/RestartLoop and a pass at 08:01:00, and md-b's pass at
// that time cancels once. Across the ten passes that follow there is exactly one cancel and one submission of md-c's
// primary.
func TestAHigherDegradedReasonDoesNotMaskARefusal(t *testing.T) {
	log := newCauseLog()
	rg := yieldRig(t, log)
	rg.batch.Enqueue(pending10h(8)...)
	yieldStart(rg)
	ms := marks(rg)
	if len(ms) != 1 {
		t.Fatalf("the marks at 08:00:50 are %v, want one", markNames(ms))
	}
	e2, e2Alloc := ms[0].Name, ms[0].Spec.AllocationRef.Name
	rg.clock.Advance(10 * time.Second) // 08:01:00
	var md v1alpha1.ModelDeployment
	if err := rg.c.Get(rg.ctx, types.NamespacedName{Namespace: rigNS, Name: "md-c"}, &md); err != nil {
		t.Fatal(err)
	}
	at := metav1.NewTime(hoT(8, 1, 0))
	md.Status.LastReconcileTime = &at
	md.Status.Conditions = []metav1.Condition{{Type: v1alpha1.ConditionDegraded, Status: metav1.ConditionTrue, Reason: v1alpha1.ReasonRestartLoop, Message: "RestartLoop: a class is paused; SubmitLimitReached is below it", LastTransitionTime: at}}
	if err := rg.c.Status().Update(rg.ctx, &md); err != nil {
		t.Fatal(err)
	}
	if _, err := rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: "md-b"}}); err != nil {
		t.Fatal(err)
	}
	if cause, ok := log.deletedWith(e2Alloc); !ok || cause != "Instance "+e2+" ended: yielded its submit slot to "+yieldDeployment {
		t.Fatalf("md-b's extra: cause %q (deleted %v), want it cancelled for the yield though md-c's top Degraded reason is RestartLoop", cause, ok)
	}
	rg.run(100*time.Second, 10*time.Second)
	log.mu.Lock()
	cancels := len(log.cause) // every Allocation the controller deleted: md-b's yielded extra alone
	log.mu.Unlock()
	if c, s := len(rg.allocations("md-c")), rg.submissions(); cancels != 1 || c != 1 || s != 5 {
		t.Fatalf("after ten passes %d Allocations deleted, md-c has %d and %d jobs in all, want one cancel, one submission of md-c's primary and 5 jobs", cancels, c, s)
	}
}

// TestARefusalForTheCapsCallsNoYield: slotTable.reserveAsk calls slotAsk.onLimit only when the submit limit alone refuses, and
// after the per-user caps passed. Provider provB: a Running Allocation of 1 GPU, a cap of 2 GPUs.
//   - a new Allocation of 4 GPUs is refused by the caps (1 + 4 = 5 > 2) though the limit of 10 leaves room: no call;
//   - a new Allocation of 1 GPU fits the caps (1 + 1 = 2) but the limit of 1 is full (1 listed): refused for the limit
//     alone: one call;
//   - the same with a limit of 3: reserved, no call.
func TestARefusalForTheCapsCallsNoYield(t *testing.T) {
	running := v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: "ours"}}
	running.Spec.ProviderConfigReference = providerRef(provB)
	one := batch(1, 8, "60Gi")
	running.Spec.ForProvider.Batch = &one
	running.Status.AtProvider.Phase = v1alpha1.AllocationRunning
	r := allocsReader{allocs: []v1alpha1.Allocation{running}}
	caps := v1alpha1.QOSLimits{MaxGPUsPerUser: ptrTo(int32(2)), From: map[string]string{v1alpha1.LimitMaxGPUsPerUser: "now"}}
	tab := newSlotTable()
	calls := 0
	ask := func(key string, limit int32, size v1alpha1.BatchParameters) slotAnswer {
		a, err := tab.reserveAsk(context.Background(), r, slotAsk{provider: provB, key: rigNS + "/" + key, limit: &limit, params: &size, caps: caps,
			onLimit: func(context.Context, slotReads) error { calls++; return nil }}, fixedAt(hoT(9, 0, 0)))
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	if a := ask("big", 10, batch(4, 32, "240Gi")); a.ok || a.capsWhy == "" || calls != 0 {
		t.Fatalf("4 GPUs: ok %v, caps %q, %d calls, want refused for the caps and no call", a.ok, a.capsWhy, calls)
	}
	if a := ask("full", 1, batch(1, 8, "60Gi")); a.ok || a.capsWhy != "" || calls != 1 {
		t.Fatalf("limit 1: ok %v, caps %q, %d calls, want refused for the limit alone and one call", a.ok, a.capsWhy, calls)
	}
	if a := ask("room", 3, batch(1, 8, "60Gi")); !a.ok || calls != 1 {
		t.Fatalf("limit 3: ok %v, %d calls, want reserved and no new call", a.ok, calls)
	}
}

// TestACancelledExtrasMarkStaysWhileItsAllocationHoldsTheSlot: md-b cancels its marked extra at 08:01:10 and the extra's
// Allocation is deleting, still holding the slot, while only the controller runs (no provider pass, which would complete
// the deletion): six controller-only passes, 08:01:20 to 08:02:10. md-c, still refused for the limit alone, marks no second
// extra in them: the cancelled extra's mark counts as the slot md-c waits for until its Allocation is gone, and md-b does not
// clear it from the cancelled Instance. md-a's Instances are never marked. Once the provider completes the deletion, md-c's
// primary is submitted, and nothing else was cancelled.
func TestACancelledExtrasMarkStaysWhileItsAllocationHoldsTheSlot(t *testing.T) {
	log := newCauseLog()
	rg := yieldRig(t, log)
	rg.batch.Enqueue(pending10h(8)...)
	yieldStart(rg)
	ms := marks(rg)
	if len(ms) != 1 {
		t.Fatalf("the marks at 08:00:50 are %v, want one", markNames(ms))
	}
	e2, e2Alloc := ms[0].Name, ms[0].Spec.AllocationRef.Name
	for range 2 { // 08:01:00, 08:01:10
		tick10(rg)
	}
	if _, deleted := log.deletedWith(e2Alloc); !deleted {
		t.Fatal("md-b's extra was not cancelled by 08:01:10")
	}
	if a, ok := rg.allocation(e2Alloc); !ok || a.DeletionTimestamp.IsZero() {
		t.Fatalf("the cancelled extra's Allocation is %+v (found %v), want it deleting: the row proves nothing otherwise", a, ok)
	}
	for range 6 {
		rg.clock.Advance(10 * time.Second)
		rg.reconcileAll() // no provider pass: the deletion does not complete
		for _, m := range marks(rg) {
			if m.Name != e2 {
				t.Fatalf("at %s %s/%s is marked: md-c marked a second extra for one primary while the first still holds the slot", rg.clock.Now().Format(time.TimeOnly), deploymentOfInstance(m), m.Name)
			}
		}
		if n := len(rg.allocations("md-c")); n != 0 {
			t.Fatalf("md-c has %d Allocations at %s, want none: the slot is still held", n, rg.clock.Now().Format(time.TimeOnly))
		}
	}
	rg.run(2*time.Minute, 10*time.Second)
	if n, c := len(rg.allocations("md-c")), len(rg.allocations("md-a")); n != 1 || c != 2 {
		t.Fatalf("md-c has %d Allocations and md-a %d, want 1 (its primary, submitted in the freed slot) and 2 (untouched)", n, c)
	}
	log.mu.Lock()
	cancels := len(log.cause)
	log.mu.Unlock()
	if cancels != 1 {
		t.Fatalf("%d Allocations deleted, want md-b's extra alone", cancels)
	}
}

// TestAMarkNamesADeploymentPastItsEndTime: md-c marks at 08:00:50 and its end time passes (set to 08:00:50) before md-b's
// pass: a deployment past its end time makes no primary, and its shutdown pass never reaches the clearing of its own marks,
// so md-b reads it. At 08:01:00 md-b re-reads md-c, finds it past its end time, clears the mark and cancels nothing.
func TestAMarkNamesADeploymentPastItsEndTime(t *testing.T) {
	log := newCauseLog()
	rg := yieldRig(t, log)
	rg.batch.Enqueue(pending10h(8)...)
	yieldStart(rg)
	ms := marks(rg)
	if len(ms) != 1 {
		t.Fatalf("the marks at 08:00:50 are %v, want one", markNames(ms))
	}
	e2, e2Alloc := ms[0].Name, ms[0].Spec.AllocationRef.Name
	rg.mutateMD("md-c", func(m *v1alpha1.ModelDeployment) { now := metav1.NewTime(rg.clock.Now()); m.Spec.EndTime = &now })
	tick10(rg) // 08:01:00
	if in := instOrNil(rg, e2); in == nil || in.Annotations[annotationYieldTo] != "" || ended(in) {
		t.Fatalf("md-b's extra %v at 08:01:00, want it alive with its mark cleared", in)
	}
	rg.run(5*time.Minute, 10*time.Second)
	if _, deleted := log.deletedWith(e2Alloc); deleted {
		t.Fatal("md-b's extra's Allocation was deleted for a deployment that is past its end time")
	}
}
