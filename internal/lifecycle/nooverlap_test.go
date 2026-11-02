package lifecycle

// HBEP-7's Test plan items on no-overlap mode (Handover, No-overlap mode),
// both paths, each with a stream running across the primary's drain start.
//
// The shared timeline (handover_test.go): the primary's job starts at
// 08:00:20, it is ready at 08:01:10 (start-to-ready 50 s), and with the rig's
// 4 h limit its hard end is 12:00:20. The default drain window is 2h10m, so its
// drain start is 09:50:20. The Provider has observed one wait, the primary's
// own 0 s, so the replacement's planned time is 09:50:20 - (0 + 50 s + 30 min)
// = 09:19:30, and its lateness deadline 09:50:20 - 50 s = 09:49:30.
//
// The stream: the primary's engine reports one running request, with its
// generated-token counter rising, from 09:45:00 until 09:56:00, and none after.
// So the primary drains at 09:50:20 with the stream in flight, and ends on the
// first report read after 09:56:00. The deployment releases when idle, with an
// idle limit of 5 min, so the primary's Allocation goes 5 min after the
// primary's end, and its job is cancelled with it.
//
// The front answers new requests with 503 model_inactive, detail
// no_ready_target, Retry-After 30, when the deployment's one Instance reaches
// its drain start with nothing ready to replace it: internal/front's
// TestDrainWithNoReplacementLeavesNoTarget (drain_test.go) covers that, the
// row "Attached, with no eligible target at that level" of HBEP-5, Errors.
// Here the tests show that no Instance is ready during the gap, which is what
// the front routes by.

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

var (
	noStreamFrom = hoT(9, 45, 0)
	noStreamTo   = hoT(9, 56, 0)
)

// streamEngine is an engine with one request running, its token counter
// rising one per second, from from until to, and none after.
func streamEngine(from, to time.Time) afake.Engine {
	return afake.Engine{Health: func(now time.Time, p afake.Process) agentapi.Health {
		h := afake.DefaultHealth(now, p)
		var running, tokens int64
		if !now.Before(from) && now.Before(to) {
			running = 1
		}
		if now.After(from) {
			end := now
			if end.After(to) {
				end = to
			}
			tokens = int64(end.Sub(from) / time.Second)
		}
		h.RunningRequests = agentapi.Int64(running)
		h.GeneratedTokens = agentapi.Int64(tokens)
		return h
	}}
}

// noOverlapRig is a rig whose first Allocation's engine runs the stream, and
// whose deployment releases when idle after 5 min.
func noOverlapRig(t *testing.T, opts ...rigOption) *rig {
	rg := newRig(t, append([]rigOption{withMaxTime(4 * time.Hour)}, opts...)...)
	first := ""
	rg.engine = func(alloc string) afake.Engine {
		if first == "" || first == alloc {
			first = alloc
			return streamEngine(noStreamFrom, noStreamTo)
		}
		return afake.Engine{}
	}
	return rg
}

func releaseWhenIdle5m(md *v1alpha1.ModelDeployment) *v1alpha1.ModelDeployment {
	md.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dur(5 * time.Minute)}
	return md
}

// noOverlapStart is hoStart with the deployment releasing when idle.
func noOverlapStart(rg *rig) *v1alpha1.Instance {
	rg.t.Helper()
	rg.create(releaseWhenIdle5m(mdSpec(hoMD)))
	for range 7 {
		rg.clock.Advance(10 * time.Second)
		rg.Step()
	}
	p := rg.instances(hoMD)[0]
	if p.Status.ReadyTime == nil || !p.Status.ReadyTime.Time.Equal(hoT(8, 1, 10)) || p.Status.LeaseEnd == nil || !p.Status.LeaseEnd.Time.Equal(hoT(12, 0, 20)) {
		rg.t.Fatalf("the primary should be ready at 08:01:10 with leaseEnd 12:00:20, got %v and %v", p.Status.ReadyTime, p.Status.LeaseEnd)
	}
	return &p
}

func noReplacementLate(t *testing.T, rg *rig) {
	t.Helper()
	if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Reason == v1alpha1.ReasonReplacementLate || strings.Contains(c.Message, v1alpha1.ReasonReplacementLate) {
		t.Fatalf("at %s Degraded = %s %q: ReplacementLate in no-overlap mode", rg.clock.Now().Format(time.TimeOnly), c.Reason, c.Message)
	}
	if n := len(rg.rec.Find(eventDegraded, v1alpha1.ReasonReplacementLate)); n != 0 {
		t.Fatalf("at %s %d ReplacementLate events, want none", rg.clock.Now().Format(time.TimeOnly), n)
	}
}

// streamNotCut fails when the primary stopped serving the stream before it
// ended: it must stay draining, with no restart, until 09:56:00.
func streamNotCut(t *testing.T, rg *rig, p string) {
	t.Helper()
	in := rg.instancesByName()[p]
	if in == nil {
		if rg.clock.Now().Before(noStreamTo) {
			t.Fatalf("at %s the primary is gone with its stream in flight", rg.clock.Now().Format(time.TimeOnly))
		}
		return // ended and removed with its Allocation, after the stream
	}
	if len(in.Status.Restarts) != 0 {
		t.Fatalf("at %s the primary restarted: %+v", rg.clock.Now().Format(time.TimeOnly), in.Status.Restarts)
	}
	if rg.clock.Now().Before(noStreamTo) && rg.clock.Now().After(hoT(9, 50, 20)) && in.Status.Phase != v1alpha1.InstanceDraining {
		t.Fatalf("at %s the primary is %s with its stream in flight, want draining", rg.clock.Now().Format(time.TimeOnly), in.Status.Phase)
	}
}

// TestNoOverlapOneSlotPath covers "No-overlap mode, with a stream running at
// the drain start, on the one-slot path". The Provider's QOS sets
// maxSubmitPerUser 1 and maxGpusPerUser 1, and the partition has no QOS, so
// the effective submit limit is 1; the fake user's QOS is the same (one job,
// one GPU).
func TestNoOverlapOneSlotPath(t *testing.T) {
	rg := noOverlapRig(t, oneSlotOpts(withBatchConfig(func(c *pfake.Config) { c.MaxSubmitPerUser, c.MaxGPUsPerUser = 1, 1 }))...)
	p := noOverlapStart(rg)
	pAlloc := p.Spec.AllocationRef.Name
	// Before the drain start: no replacement Allocation, not at the planned
	// time 09:19:30 nor after; no ReplacementLate at the lateness deadline
	// 09:49:30; and no Degraded reason at all: the primary serves and no
	// primary is missing (`@hpc-burst-science-agent`'s reading, relayed by
	// qos, 1 Nov 2026).
	before := func() {
		if n := len(rg.allAllocations()); n != 1 {
			t.Fatalf("at %s %d Allocations, want only the primary's before its drain start", rg.clock.Now().Format(time.TimeOnly), n)
		}
		if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Status != metav1.ConditionFalse || c.Reason != v1alpha1.ReasonAsDesired {
			t.Fatalf("at %s Degraded = %s/%s %q before the drain start, want False/AsDesired", rg.clock.Now().Format(time.TimeOnly), c.Status, c.Reason, c.Message)
		}
		noReplacementLate(t, rg)
	}
	hoStepTo(rg, hoT(9, 19, 0), 5*time.Minute, before)
	hoStepTo(rg, hoT(9, 50, 10), 10*time.Second, before)

	// The drain start: the primary drains with the stream in flight.
	hoStepTo(rg, hoT(9, 50, 20), 10*time.Second, nil)
	if ph := hoPhase(rg, p.Name); ph != v1alpha1.InstanceDraining {
		t.Fatalf("at 09:50:20 the primary is %s, want draining", ph)
	}
	if n := len(rg.rec.Find(eventDegraded, v1alpha1.ReasonCapacityDropped)); n == 0 {
		t.Fatal("no CapacityDropped event at the drain start")
	}
	c := rg.condition(hoMD, v1alpha1.ConditionDegraded)
	if c.Reason != v1alpha1.ReasonNoOverlapSlot || !strings.Contains(c.Message, "Provider "+rigProvider) || !strings.Contains(c.Message, "maxSubmitPerUser 1 of QOS example-now") {
		t.Fatalf("at 09:50:20 Degraded = %s/%s %q, want NoOverlapSlot naming Provider %s and its maxSubmitPerUser 1", c.Status, c.Reason, c.Message, rigProvider)
	}

	// The gap, until the successor is ready. The successor's Allocation is
	// created only once the primary's is gone or Ended; the primary ends
	// after the stream; the controller restarts once in the gap.
	var succAlloc string
	var succCreated time.Time
	sawSubmitLimit, restarted := false, false
	gap := func() {
		now := rg.clock.Now()
		streamNotCut(t, rg, p.Name)
		noReplacementLate(t, rg)
		for _, a := range rg.allAllocations() {
			if a.Name == pAlloc || succAlloc != "" {
				continue
			}
			pa, alive := rg.allocation(pAlloc)
			if alive && pa.Status.AtProvider.Phase != v1alpha1.AllocationEnded {
				t.Fatalf("at %s the successor's Allocation %s exists while the primary's is %s", now.Format(time.TimeOnly), a.Name, pa.Status.AtProvider.Phase)
			}
			succAlloc, succCreated = a.Name, a.CreationTimestamp.Time
		}
		ready := rg.readyCount(hoMD) > 0
		if ready && succAlloc == "" {
			t.Fatalf("at %s an Instance is ready in the gap before any successor exists", now.Format(time.TimeOnly))
		}
		c := rg.condition(hoMD, v1alpha1.ConditionDegraded)
		if !ready {
			if c.Reason != v1alpha1.ReasonNoOverlapSlot {
				t.Fatalf("at %s in the gap Degraded = %s/%s %q, want NoOverlapSlot", now.Format(time.TimeOnly), c.Status, c.Reason, c.Message)
			}
			if strings.Contains(c.Message, v1alpha1.ReasonSubmitLimitReached+": ") {
				sawSubmitLimit = true
			}
		}
		if !restarted && !now.Before(hoT(9, 58, 0)) {
			restarted = true
			rg.restartController()
		}
	}
	hoStepTo(rg, hoT(10, 0, 0), time.Minute, gap)
	hoStepUntil(rg, hoT(10, 30, 0), 10*time.Second, func() bool { return rg.readyCount(hoMD) > 0 }, gap)
	if !restarted {
		t.Fatal("the controller was not restarted in the gap")
	}
	pi := rg.instancesByName()[p.Name]
	if pi != nil && (pi.Status.EndTime == nil || pi.Status.EndTime.Time.Before(noStreamTo)) {
		t.Fatalf("the primary ended at %v, before its stream ended at 09:56:00", pi.Status.EndTime)
	}
	if !sawSubmitLimit {
		t.Fatal("the held-back successor never added SubmitLimitReached to the message")
	}
	if succAlloc == "" {
		t.Fatal("no successor Allocation once an Instance is ready")
	}
	// ReleaseWhenIdle: the primary ended on the first report after 09:56:00
	// and its Allocation went 5 min later, so the successor came no earlier
	// than 10:01:00.
	if succCreated.Before(hoT(10, 1, 0)) {
		t.Fatalf("the successor's Allocation was created at %s, before the primary's end plus the 5 min idle limit", succCreated.Format(time.TimeOnly))
	}
	var succ *v1alpha1.Instance
	for _, in := range rg.instances(hoMD) {
		if in.Spec.AllocationRef.Name == succAlloc {
			succ = &in
		}
	}
	if succ == nil || succ.Annotations[v1alpha1.AnnotationPredecessor] != p.Name || succ.Annotations[annotationNoOverlapSuccessor] == "" {
		t.Fatalf("successor %+v, want the primary's successor, marked", succ)
	}
	if succ.Status.Phase != v1alpha1.InstanceReady {
		t.Fatalf("the successor is %s once an Instance is ready, want ready", succ.Status.Phase)
	}
	if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Status != metav1.ConditionFalse {
		t.Fatalf("Degraded = %s/%s %q once the successor is ready, want False", c.Status, c.Reason, c.Message)
	}
}

// noOverlapQueuedRig is noOverlapRig for the queued path, as on Helios: the
// partition QOS allows 2,000 jobs (it comes first, so the effective submit
// limit is 2000) and the Provider's QOS one GPU per user; the fake user's QOS
// is the same (2,000 jobs, one GPU).
func noOverlapQueuedRig(t *testing.T, extra ...rigOption) *rig {
	qos := []v1alpha1.QOSProfile{
		{Name: "example-plgrid", MaxSubmitPerUser: i32(2000)},
		{Name: "example-now", MaxSubmitPerUser: i32(1), MaxGPUsPerUser: i32(1)},
	}
	opts := append(qosOpts(qos, "example-plgrid", "example-now"), withBatchConfig(func(c *pfake.Config) { c.MaxSubmitPerUser, c.MaxGPUsPerUser = 2000, 1 }))
	return noOverlapRig(t, append(opts, extra...)...)
}

// hoStepUntil moves the clock in steps of at most coarse and runs one rig step
// after each, then each, until done reports true. A wait for a state that the
// controller reaches by itself ends on that state, not on a fixed date; the
// test fails if deadline comes first. The explicit clock advances of the
// tests stay at the submission and drain boundaries.
func hoStepUntil(rg *rig, deadline time.Time, coarse time.Duration, done func() bool, each func()) {
	rg.t.Helper()
	for !done() {
		now := rg.clock.Now()
		if !now.Before(deadline) {
			rg.t.Fatalf("the awaited state was not reached by %s", deadline.Format(time.TimeOnly))
		}
		rg.clock.Advance(min(coarse, deadline.Sub(now)))
		rg.Step()
		if each != nil {
			each()
		}
	}
}

// TestNoOverlapQueuedPath covers "No-overlap mode on the queued path, as on
// Helios".
func TestNoOverlapQueuedPath(t *testing.T) {
	rg := noOverlapQueuedRig(t)
	p := noOverlapStart(rg)
	pAlloc := p.Spec.AllocationRef.Name
	// The replacement is created at its planned time, 09:19:30, not before.
	hoStepTo(rg, hoT(9, 19, 20), 5*time.Minute, func() {
		if n := len(hoRepl(rg, p.Name)); n != 0 {
			t.Fatalf("at %s a replacement exists before its planned time 09:19:30", rg.clock.Now().Format(time.TimeOnly))
		}
	})
	hoStepTo(rg, hoT(9, 19, 30), 10*time.Second, nil)
	reps := hoRepl(rg, p.Name)
	if len(reps) != 1 || !reps[0].CreationTimestamp.Time.Equal(hoT(9, 19, 30)) || reps[0].Annotations[annotationNoOverlapSuccessor] == "" {
		t.Fatalf("replacements %+v, want one created at 09:19:30, marked as a no-overlap successor", reps)
	}
	r := reps[0]
	rAlloc := r.Spec.AllocationRef.Name
	pa, ok := rg.allocation(pAlloc)
	if !ok || pa.Annotations[v1alpha1.AnnotationJobID] == "" {
		t.Fatalf("the primary's Allocation %s has no job: %+v", pAlloc, pa)
	}
	pJob := pa.Annotations[v1alpha1.AnnotationJobID]
	// Accepted, and pending on the fake's GPU cap while the predecessor runs;
	// never started before the predecessor's job ends. No ReplacementLate at
	// 09:49:30 or after.
	var pJobEnd, rStart time.Time
	// Once the predecessor is removed with its Allocation, and before the
	// replacement is ready, only the replacement's own mark carries the gap:
	// the controller restarts there, and the gap must hold on.
	restarted, gapAfterRestart := false, 0
	watch := func() {
		now := rg.clock.Now()
		if restarted && rg.readyCount(hoMD) == 0 {
			gapAfterRestart++
		}
		if !restarted && rg.instancesByName()[p.Name] == nil && rg.readyCount(hoMD) == 0 {
			restarted = true
			rg.restartController()
		}
		noReplacementLate(t, rg)
		streamNotCut(t, rg, p.Name)
		// Before the drain start the primary serves and nothing is missing:
		// the pending successor is no gap (one rule, nooverlap.go).
		if now.Before(hoT(9, 50, 20)) {
			if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Status != metav1.ConditionFalse || c.Reason != v1alpha1.ReasonAsDesired {
				t.Fatalf("at %s Degraded = %s/%s %q before the drain start, want False/AsDesired", now.Format(time.TimeOnly), c.Status, c.Reason, c.Message)
			}
		}
		ra, ok := rg.allocation(rAlloc)
		if !ok {
			t.Fatalf("at %s the replacement's Allocation is gone", now.Format(time.TimeOnly))
		}
		if j, ok := rg.batch.Job(pJob); pJobEnd.IsZero() && ok && j.EndTime != nil {
			pJobEnd = *j.EndTime
		}
		if ra.Status.AtProvider.StartTime != nil && rStart.IsZero() {
			rStart = ra.Status.AtProvider.StartTime.Time
			if pJobEnd.IsZero() || rStart.Before(pJobEnd) {
				t.Fatalf("the replacement started at %s, before the predecessor's job ended (%v)", rStart.Format(time.TimeOnly), pJobEnd)
			}
		}
		if rStart.IsZero() && now.After(hoT(9, 20, 0)) {
			if ra.Annotations[v1alpha1.AnnotationJobID] == "" || ra.Status.AtProvider.Phase != v1alpha1.AllocationPending {
				t.Fatalf("at %s the replacement's Allocation is %s with job %q, want accepted and Pending on the GPU cap", now.Format(time.TimeOnly), ra.Status.AtProvider.Phase, ra.Annotations[v1alpha1.AnnotationJobID])
			}
		}
		if !now.Before(hoT(9, 50, 20)) && rg.readyCount(hoMD) == 0 {
			if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Reason != v1alpha1.ReasonNoOverlapSlot || !strings.Contains(c.Message, "maxGpusPerUser 1 of QOS example-now") || !strings.Contains(c.Message, "Provider "+rigProvider) {
				t.Fatalf("at %s in the gap Degraded = %s/%s %q, want NoOverlapSlot naming Provider %s and maxGpusPerUser 1", now.Format(time.TimeOnly), c.Status, c.Reason, c.Message, rigProvider)
			}
		}
	}
	hoStepTo(rg, hoT(9, 49, 0), 5*time.Minute, watch)
	hoStepTo(rg, hoT(9, 51, 0), 10*time.Second, watch)
	if ph := hoPhase(rg, p.Name); ph != v1alpha1.InstanceDraining {
		t.Fatalf("at 09:51:00 the primary is %s, want draining with its stream", ph)
	}
	if n := len(rg.rec.Find(eventDegraded, v1alpha1.ReasonCapacityDropped)); n == 0 {
		t.Fatal("no CapacityDropped event at the drain start")
	}
	hoStepUntil(rg, hoT(10, 30, 0), 10*time.Second, func() bool { return rg.instance(r.Name).Status.Phase == v1alpha1.InstanceReady }, watch)
	if pJobEnd.IsZero() || rStart.IsZero() {
		t.Fatalf("the predecessor's job ended at %v and the replacement started at %v, want both once the replacement is ready", pJobEnd, rStart)
	}
	if !restarted || gapAfterRestart == 0 {
		t.Fatalf("restarted %v with %d in-gap steps after it, want a restart after the predecessor's removal and the gap checked after it", restarted, gapAfterRestart)
	}
	// ReleaseWhenIdle: the predecessor ended after 09:56:00, its Allocation
	// and job went 5 min later.
	if pJobEnd.Before(hoT(10, 1, 0)) {
		t.Fatalf("the predecessor's job ended at %s, before its end plus the 5 min idle limit", pJobEnd.Format(time.TimeOnly))
	}
	if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Status != metav1.ConditionFalse {
		t.Fatalf("Degraded = %s/%s %q once the replacement is ready, want False", c.Status, c.Reason, c.Message)
	}
}

// TestNoOverlapNotWhenAnotherProviderHasRoom: the rig's Provider caps one GPU
// per user, so the replacement cannot run beside the primary there; a second
// allowed Provider without limits has room. The deployment is not in
// no-overlap mode: the replacement goes to the second Provider at its planned
// time, and the normal handover follows (it is ready before the drain start,
// so nothing is degraded).
func TestNoOverlapNotWhenAnotherProviderHasRoom(t *testing.T) {
	qos := []v1alpha1.QOSProfile{{Name: "example-now", MaxGPUsPerUser: i32(1)}}
	opts := append(qosOpts(qos, "", "example-now"), withBatchConfig(func(c *pfake.Config) { c.MaxGPUsPerUser = 1 }))
	rg := noOverlapRig(t, opts...)
	rg.addProvider("example-second", "gpu-long", 4*time.Hour, nil, nil)
	md := releaseWhenIdle5m(mdSpec(hoMD))
	md.Spec.Providers.Names = []string{rigProvider, "example-second"}
	rg.create(md)
	for range 7 {
		rg.clock.Advance(10 * time.Second)
		rg.Step()
	}
	p := rg.instances(hoMD)[0]
	if p.Labels[v1alpha1.LabelProvider] != rigProvider {
		t.Fatalf("the primary is on %s, want %s", p.Labels[v1alpha1.LabelProvider], rigProvider)
	}
	hoStepTo(rg, hoT(9, 19, 20), 5*time.Minute, nil)
	hoStepTo(rg, hoT(9, 21, 0), 10*time.Second, nil)
	reps := hoRepl(rg, p.Name)
	if len(reps) != 1 || reps[0].Labels[v1alpha1.LabelProvider] != "example-second" || reps[0].Annotations[annotationNoOverlapSuccessor] != "" {
		t.Fatalf("replacements %+v, want one on example-second, not marked", reps)
	}
	hoStepTo(rg, hoT(9, 51, 0), time.Minute, nil)
	if c := rg.condition(hoMD, v1alpha1.ConditionDegraded); c.Status != metav1.ConditionFalse {
		t.Fatalf("Degraded = %s/%s %q after a normal handover, want False", c.Status, c.Reason, c.Message)
	}
}

// instancesByName returns the deployment's Instances by name.
func (rg *rig) instancesByName() map[string]*v1alpha1.Instance {
	out := map[string]*v1alpha1.Instance{}
	for _, in := range rg.instances(hoMD) {
		out[in.Name] = &in
	}
	return out
}
