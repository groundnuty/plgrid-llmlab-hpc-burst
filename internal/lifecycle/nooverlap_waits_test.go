package lifecycle

// Queue waits and no-overlap mode (issue #195; HBEP-7, Handover, No-overlap mode, and
// Test plan, "Queue waits on the queued path").
//
// A successor made on the queued path pends behind its own predecessor's per-user cap, not
// in the scheduler's queue, so no queue wait is recorded for it: the lifecycle controller
// creates its Instance and its Allocation with v1alpha1.AnnotationQueuedSuccessor, and the
// Provider's controller skips a marked Allocation. Otherwise the Provider's observed waits
// would grow with every generation, and a deployment planning in normal mode on that
// Provider, such as one that also allows a second Provider, would plan its replacement as
// early as its primary's start while the inflated waits dominate. A successor made on the
// one-slot path is created after its predecessor's job left the slot, pends in the
// scheduler's queue like any job, and keeps its wait.
//
// Timeline, nooverlap_test.go's: the first primary's job starts at 08:00:20 with a 4 h
// limit, its drain start is 09:50:20, and the deployment releases when idle after 5 min.
// On the queued path the successor is created at 09:19:30, pends while the primary's job
// runs, and starts a few minutes after 10:01.

import (
	"context"
	"maps"
	"sync"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// createdObject is an object the controller created, as it was when the Create call was made.
type createdObject struct {
	name        string
	annotations map[string]string
	provider    string // the label of its Provider
}

// creationLog records the Allocations and Instances the lifecycle controller creates, with
// their annotations at the moment of the Create call, so a test sees what a recorder would
// see the first time it reads the Allocation. It can also fail the next Allocation create
// once, a pass cut short between the two creates of a successor.
type creationLog struct {
	mu            sync.Mutex
	allocs, insts []createdObject
	failNextAlloc bool
	failed        int
}

func (l *creationLog) funcs() interceptor.Funcs {
	return interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		l.mu.Lock()
		defer l.mu.Unlock()
		o := createdObject{name: obj.GetName(), annotations: maps.Clone(obj.GetAnnotations()), provider: obj.GetLabels()[v1alpha1.LabelProvider]}
		switch obj.(type) {
		case *v1alpha1.Allocation:
			if l.failNextAlloc {
				l.failNextAlloc = false
				l.failed++
				return errHOKilled
			}
			if err := c.Create(ctx, obj, opts...); err != nil {
				return err
			}
			l.allocs = append(l.allocs, o)
			return nil
		case *v1alpha1.Instance:
			if err := c.Create(ctx, obj, opts...); err != nil {
				return err
			}
			l.insts = append(l.insts, o)
			return nil
		}
		return c.Create(ctx, obj, opts...)
	}}
}

func (l *creationLog) allocations() []createdObject {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]createdObject(nil), l.allocs...)
}

func (l *creationLog) instances() []createdObject {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]createdObject(nil), l.insts...)
}

func (l *creationLog) armAllocFailure() {
	l.mu.Lock()
	l.failNextAlloc = true
	l.mu.Unlock()
}

// providerWaits returns the queue waits in a Provider's status.
func providerWaits(rg *rig, name string) []v1alpha1.QueueWait {
	rg.t.Helper()
	var p v1alpha1.Provider
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Name: name}, &p); err != nil {
		rg.t.Fatal(err)
	}
	return p.Status.QueueWaits
}

// TestAQueuedPathSuccessorRecordsNoWaitAndNormalModePlansFromGenuineWaits runs a queued-path
// handover to the successor's readiness, then a deployment in normal mode on the same Provider.
//   - The first primary's Allocation is created unmarked, the successor's marked, at the Create
//     call. The successor's job pends behind the primary's for over 40 min, as the fake Slurm
//     records it, and starts.
//   - The Provider holds the primary's wait and none for the successor's job.
//   - A second deployment, which also allows a second Provider with room, runs its primary on the
//     first Provider and plans its replacement at its drain start minus (the largest wait the
//     Provider holds + the start-to-ready time + the margin). The waits are the two primaries',
//     worked out from the fake Slurm's records of their jobs, never read from the Provider's
//     status. Had the successor's 40 min been recorded, the plan would be over 40 min earlier.
func TestAQueuedPathSuccessorRecordsNoWaitAndNormalModePlansFromGenuineWaits(t *testing.T) {
	log := &creationLog{}
	rg := noOverlapQueuedRig(t, withInterceptors(log.funcs()))
	slurm := &slurmView{rg: rg}
	noOverlapStart(rg)
	hoStepTo(rg, hoT(9, 19, 0), 5*time.Minute, nil)
	hoStepUntil(rg, hoT(10, 30, 0), 10*time.Second, func() bool { return successorReady(rg) }, nil)

	// Marked at creation, the successor's Allocation and Instance; the primary's neither.
	allocs, insts := log.allocations(), log.instances()
	if len(allocs) != 2 || len(insts) != 2 {
		t.Fatalf("%d Allocations and %d Instances created, want the primary's and the successor's each: %+v %+v", len(allocs), len(insts), allocs, insts)
	}
	for _, c := range []struct {
		what string
		o    createdObject
		want bool
	}{{"the primary's Allocation", allocs[0], false}, {"the primary's Instance", insts[0], false},
		{"the successor's Allocation", allocs[1], true}, {"the successor's Instance", insts[1], true}} {
		if got := c.o.annotations[v1alpha1.AnnotationQueuedSuccessor] != ""; got != c.want {
			t.Errorf("%s %s has the queued-successor mark at its creation: %v, want %v (annotations %v)", c.what, c.o.name, got, c.want, c.o.annotations)
		}
	}

	// The fake Slurm's records: the primary's job, then the successor's, which pended a long time.
	jobs := slurm.jobs()
	if len(jobs) != 2 || jobs[0].StartTime == nil || jobs[1].StartTime == nil {
		t.Fatalf("the fake Slurm holds %+v, want the primary's and the successor's job, both started", jobs)
	}
	primary, successor := jobs[0], jobs[1]
	primaryWait, successorWait := primary.StartTime.Sub(primary.SubmitTime), successor.StartTime.Sub(successor.SubmitTime)
	if successorWait < 40*time.Minute {
		t.Fatalf("the successor's job waited %v (submitted %s, started %s), want over 40 min behind the primary's job, which ended no sooner than 10:01:00", successorWait, successor.SubmitTime.Format(time.TimeOnly), successor.StartTime.Format(time.TimeOnly))
	}

	waits := providerWaits(rg, rigProvider)
	if len(waits) != 1 || waits[0].JobID != primary.ID || !waits[0].SubmitTime.Time.Equal(primary.SubmitTime) || !waits[0].StartTime.Time.Equal(*primary.StartTime) {
		t.Fatalf("the Provider's waits %+v, want only the primary's job %s (submitted %s, started %s) and none for the successor's job %s", waits, primary.ID, primary.SubmitTime.Format(time.TimeOnly), primary.StartTime.Format(time.TimeOnly), successor.ID)
	}
	if a, ok := rg.allocation(allocs[1].name); !ok || !a.Status.AtProvider.QueueWaitRecorded {
		t.Fatalf("the successor's Allocation %+v: want queueWaitRecorded, the wait being settled", a)
	}

	// A normal-mode deployment on the same Provider. A's successor holds the GPU, so scale A to
	// zero and wait, on the fake Slurm, until every job has ended.
	rg.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = 0 })
	rg.until(40*time.Minute, 10*time.Second, "A's jobs ended", func() bool { return slurm.live() == 0 })
	const mdB = "md-b"
	b := mdSpec(mdB)
	b.Spec.Providers.Names = []string{rigProvider, "example-second"}
	rg.addProvider("example-second", "gpu-long", 4*time.Hour, nil, nil)
	rg.create(b)
	rg.until(10*time.Minute, 10*time.Second, "B's primary is ready", func() bool { return rg.readyCount(mdB) == 1 })
	bPrimary := rg.instances(mdB)[0]
	if bPrimary.Labels[v1alpha1.LabelProvider] != rigProvider {
		t.Fatalf("B's primary is on %s, want %s", bPrimary.Labels[v1alpha1.LabelProvider], rigProvider)
	}
	if s := rg.md(mdB).Status.StartToReady; len(s) != 1 || s[0].Duration != "50s" {
		t.Fatalf("B's start-to-ready %+v, want 50s (job started, ready 50 s later)", s)
	}
	jobs = slurm.jobs()
	if len(jobs) != 3 || jobs[2].StartTime == nil {
		t.Fatalf("the fake Slurm holds %+v, want A's two jobs and B's primary's", jobs)
	}
	bJob := jobs[2]
	bWait := bJob.StartTime.Sub(bJob.SubmitTime)

	// The waits are the two primaries', the largest is the planning wait (fewer than 20 are kept),
	// the start-to-ready time is 50 s and the default margin and drain window are 30 min and 2h10m.
	got := map[string]bool{}
	for _, w := range providerWaits(rg, rigProvider) {
		got[w.JobID] = true
	}
	if len(got) != 2 || !got[primary.ID] || !got[bJob.ID] || got[successor.ID] {
		t.Fatalf("the Provider's waits are for jobs %v, want the two primaries' %s and %s, not the successor's %s", got, primary.ID, bJob.ID, successor.ID)
	}
	const s2r, margin = 50 * time.Second, 30 * time.Minute
	drainStart := bJob.StartTime.Add(4*time.Hour - 2*time.Hour - 10*time.Minute)
	genuine := max(primaryWait, bWait)
	planned := drainStart.Add(-(genuine + s2r + margin))
	inflated := drainStart.Add(-(successorWait + s2r + margin))
	if !inflated.Before(planned.Add(-30 * time.Minute)) {
		t.Fatalf("setup: the plan from the successor's wait would be %s, the genuine one %s, want them over 30 min apart", inflated.Format(time.TimeOnly), planned.Format(time.TimeOnly))
	}
	replacements := func() []v1alpha1.Instance {
		var out []v1alpha1.Instance
		for _, in := range rg.instances(mdB) {
			if in.Annotations[v1alpha1.AnnotationPredecessor] == bPrimary.Name {
				out = append(out, in)
			}
		}
		return out
	}
	hoStepTo(rg, planned.Add(-10*time.Second), 5*time.Minute, func() {
		if n := len(replacements()); n != 0 {
			t.Fatalf("at %s B has %d replacements, before its planned time %s", rg.clock.Now().Format(time.TimeOnly), n, planned.Format(time.TimeOnly))
		}
	})
	hoStepTo(rg, planned, 10*time.Second, nil)
	reps := replacements()
	if len(reps) != 1 || !reps[0].CreationTimestamp.Time.Equal(planned) {
		t.Fatalf("B's replacements %+v at %s, want one created exactly at %s (the largest genuine wait %v, start-to-ready 50s, margin 30m before the drain start %s)", reps, rg.clock.Now().Format(time.TimeOnly), planned.Format(time.TimeOnly), genuine, drainStart.Format(time.TimeOnly))
	}
	if reps[0].Labels[v1alpha1.LabelProvider] != "example-second" || reps[0].Annotations[v1alpha1.AnnotationQueuedSuccessor] != "" {
		t.Errorf("B's replacement %+v, want one on example-second with no queued-successor mark: normal mode", reps[0])
	}
	if last := log.allocations(); last[len(last)-1].annotations[v1alpha1.AnnotationQueuedSuccessor] != "" {
		t.Errorf("B's replacement Allocation %+v carries the queued-successor mark", last[len(last)-1])
	}
}

// TestAQueuedSuccessorsAllocationIsMarkedWhenThePassWasCutShortBetweenTheTwoCreates: the
// Instance is the durable intent and is created before its Allocation. A pass that stops
// between the two leaves a pending successor, and the Allocation that a later pass creates
// (ensureAllocation) gets the mark from the Instance, at its creation like any other.
func TestAQueuedSuccessorsAllocationIsMarkedWhenThePassWasCutShortBetweenTheTwoCreates(t *testing.T) {
	log := &creationLog{}
	rg := noOverlapQueuedRig(t, withInterceptors(log.funcs()))
	p := noOverlapStart(rg)
	hoStepTo(rg, hoT(9, 19, 0), 5*time.Minute, nil)
	log.armAllocFailure() // the successor's Allocation is the next one created
	died := false
	for !died {
		if !rg.clock.Now().Before(hoT(9, 25, 0)) {
			t.Fatal("the controller never reached the successor's Allocation by 09:25:00")
		}
		rg.clock.Advance(10 * time.Second)
		switch err := stepTolerant(rg); {
		case err == nil:
		case log.failed == 1:
			died = true
		default:
			t.Fatalf("reconcile at %s: %v", rg.clock.Now().Format(time.TimeOnly), err)
		}
	}
	reps := hoRepl(rg, p.Name)
	if len(reps) != 1 || reps[0].Annotations[v1alpha1.AnnotationQueuedSuccessor] == "" {
		t.Fatalf("after the cut-short pass the successors are %+v, want one Instance with the queued-successor mark", reps)
	}
	if _, ok := rg.allocation(reps[0].Spec.AllocationRef.Name); ok {
		t.Fatalf("the successor's Allocation %s exists although its creation failed", reps[0].Spec.AllocationRef.Name)
	}
	if n := len(log.allocations()); n != 1 {
		t.Fatalf("%d Allocations created, want the primary's alone", n)
	}

	rg.restartController()
	rg.until(5*time.Minute, 10*time.Second, "the successor's Allocation is created", func() bool { return len(log.allocations()) == 2 })
	succ := log.allocations()[1]
	if succ.name != reps[0].Spec.AllocationRef.Name || succ.annotations[v1alpha1.AnnotationQueuedSuccessor] == "" {
		t.Fatalf("the successor's Allocation %+v, want %s, marked at its creation", succ, reps[0].Spec.AllocationRef.Name)
	}
	if succ.annotations[v1alpha1.AnnotationQueuedSuccessor] != reps[0].Annotations[v1alpha1.AnnotationQueuedSuccessor] {
		t.Errorf("the Allocation's mark %q differs from the Instance's %q", succ.annotations[v1alpha1.AnnotationQueuedSuccessor], reps[0].Annotations[v1alpha1.AnnotationQueuedSuccessor])
	}
}

// TestAOneSlotPathSuccessorIsNotMarkedAndKeepsItsWait: on the one-slot path the successor is
// created after the predecessor's job left the slot, so it pends in the scheduler's queue like
// any job, and its wait is recorded (HBEP-7, "On the one-slot path the successor's whole queue
// wait is added"). It carries annotationNoOverlapSuccessor and no queued-successor mark.
func TestAOneSlotPathSuccessorIsNotMarkedAndKeepsItsWait(t *testing.T) {
	log := &creationLog{}
	rg := noOverlapRig(t, oneSlotOpts(withInterceptors(log.funcs()), withBatchConfig(func(c *pfake.Config) { c.MaxSubmitPerUser, c.MaxGPUsPerUser = 1, 1 }))...)
	slurm := &slurmView{rg: rg}
	noOverlapStart(rg)
	hoStepTo(rg, hoT(9, 19, 0), 5*time.Minute, nil)
	hoStepUntil(rg, hoT(10, 30, 0), 10*time.Second, func() bool { return successorReady(rg) }, nil)

	allocs, insts := log.allocations(), log.instances()
	if len(allocs) != 2 || len(insts) != 2 {
		t.Fatalf("%d Allocations and %d Instances created, want the primary's and the successor's each", len(allocs), len(insts))
	}
	if insts[1].annotations[annotationNoOverlapSuccessor] == "" {
		t.Fatalf("the successor's Instance %+v is not a no-overlap successor", insts[1])
	}
	for _, o := range []createdObject{allocs[0], allocs[1], insts[0], insts[1]} {
		if m := o.annotations[v1alpha1.AnnotationQueuedSuccessor]; m != "" {
			t.Errorf("%s carries the queued-successor mark %q on the one-slot path", o.name, m)
		}
	}
	jobs := slurm.jobs()
	if len(jobs) != 2 || jobs[0].StartTime == nil || jobs[1].StartTime == nil {
		t.Fatalf("the fake Slurm holds %+v, want two started jobs", jobs)
	}
	if jobs[1].SubmitTime.Before(*jobs[0].EndTime) {
		t.Fatalf("setup: the successor's job was submitted at %s, before the primary's ended at %s", jobs[1].SubmitTime.Format(time.TimeOnly), jobs[0].EndTime.Format(time.TimeOnly))
	}
	waits := providerWaits(rg, rigProvider)
	if len(waits) != 2 || waits[0].JobID != jobs[0].ID || waits[1].JobID != jobs[1].ID {
		t.Fatalf("the Provider's waits %+v, want the primary's job %s and the successor's %s", waits, jobs[0].ID, jobs[1].ID)
	}
	if w, want := waits[1].Wait(), jobs[1].StartTime.Sub(jobs[1].SubmitTime); w != want {
		t.Errorf("the successor's recorded wait is %v, want %v, its job's own", w, want)
	}
}
