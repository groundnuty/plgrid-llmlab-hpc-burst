package lifecycle

// An untaken unit gives back its share of a Scale's count (HBEP-10, The report, as amended by #698): a Scale to a count above zero
// whose committed Instance stays because its target failed finally (LastReadySinceCommit, or JobStartedSinceCommit under a started
// pair) writes spec.instances = N + U, absolute, with the resourceVersion, only while spec.instances still equals N; records it in
// status.countGivenBack; and finishes only after the record lands.

import (
	"errors"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// lastReadyScale is a Scale to one that commits X (EndStarting, with its Allocation) while the spec write fails; X turns ready as the
// last ready Instance while R still starts. It returns once the write lands and the final failures are recorded, before the pass
// that gives the count back.
func lastReadyScale(t *testing.T) (w *execWorld, r, x string, stop func()) {
	t.Helper()
	w, fleet, _ := newStartingFleetWorld(t, []time.Duration{20 * time.Minute, 6 * time.Minute}, false)
	r, x = fleet[0], fleet[1]
	stop = w.failSpecWrites()
	w.approveScale("scale", 1, 2)
	w.stepsUntil(30*time.Minute, 30*time.Second, "X ready", func() bool { return w.instance(x).Status.Phase == v1alpha1.InstanceReady })
	if w.instance(r).Status.Phase == v1alpha1.InstanceReady {
		t.Fatal("R is ready: X is not the last ready one")
	}
	return w, r, x, stop
}

// The count is given back: after the pass in which X's final failure is recorded, the next raises spec.instances from the
// plan's 1 to 2, records it, and finishes the Operation; R and X stay, and nothing outside committed[] is taken.
func TestAScaleGivesBackTheCountOfAnUntakenUnitAndFinishesOnlyAfterTheRecord(t *testing.T) {
	w, r, x, stop := lastReadyScale(t)
	rAlloc := w.mustInstance(r).Spec.AllocationRef.Name
	stop()
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err != nil {
		t.Fatalf("the pass in which the spec write lands: %v", err)
	}
	if op := w.getOp("scale"); op.Status.FinishedAt != nil || op.Status.CountGivenBack != nil {
		t.Fatalf("the Operation is %q finished %v with the record %+v after the pass that recorded the failures, want it unfinished until the count is given back", op.Status.Phase, op.Status.FinishedAt, op.Status.CountGivenBack)
	}
	w.stepsUntil(2*time.Minute, 10*time.Second, "the give-back", func() bool { return w.getOp("scale").Status.CountGivenBack != nil })
	op := w.getOp("scale")
	if rec := op.Status.CountGivenBack; rec.Units != 1 || rec.Instances == nil || *rec.Instances != 2 {
		t.Errorf("the record is %+v, want 1 unit and 2 instances written", rec)
	}
	if got := w.md(hoMD).Spec.Instances; got != 2 {
		t.Errorf("spec.instances is %d, want 2: the plan's 1 plus the untaken unit", got)
	}
	if op.Status.Phase != v1alpha1.OperationPhasePartlyFailed || op.Status.FinishedAt == nil {
		t.Errorf("the Operation is %q finished %v with the record landed, want PartlyFailed and finished", op.Status.Phase, op.Status.FinishedAt)
	}
	for i := 0; i < 20; i++ { // nothing outside committed[] is ever taken
		w.clock.Advance(30 * time.Second)
		if err := w.stepKeepingErrors(); err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
		if in := w.instance(r); ended(in) || isDraining(in) {
			t.Fatalf("R is %q: the controller's own scale-down met a lowered count and took an Instance the plan did not name", in.Status.Phase)
		}
	}
	if a, ok := w.allocation(rAlloc); !ok || !a.DeletionTimestamp.IsZero() {
		t.Errorf("R's Allocation is %+v (found %v)", a, ok)
	}
	if in := w.instance(x); ended(in) || isDraining(in) {
		t.Errorf("X is %q: it was not taken", in.Status.Phase)
	}
	if got := w.md(hoMD).Spec.Instances; got != 2 {
		t.Errorf("spec.instances is %d at the end, want 2 (given back once)", got)
	}
}

// A person's count meets the give-back by the max rule (HBEP-10, The report, Kept warm): raised above N, which needs no approval, the
// larger of V and N + U is written, so a raise never takes back the untaken unit; lowered below N, theirs stands and nothing is given
// back, the record holding its units with no count written. Here N is 1 and U is 1.
func TestAPersonsRaisedOrLoweredCountMeetsTheGiveBackByTheMaxRule(t *testing.T) {
	for _, tc := range []struct {
		name      string
		set       int32
		wantCount *int32 // the count the record holds; nil: none written
		wantSpec  int32
	}{
		{"raised to 5, above N + U: the larger stands and is recorded", 5, ptr32(5), 5},
		{"lowered to 0, below N: theirs stands and nothing is given back", 0, nil, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, _, _, stop := lastReadyScale(t)
			stop()
			w.clock.Advance(10 * time.Second)
			if err := w.stepKeepingErrors(); err != nil {
				t.Fatalf("the pass in which the spec write lands: %v", err)
			}
			w.mutateMD(hoMD, func(m *v1alpha1.ModelDeployment) { m.Spec.Instances = tc.set })
			w.stepsUntil(2*time.Minute, 10*time.Second, "the record", func() bool { return w.getOp("scale").Status.CountGivenBack != nil })
			op := w.getOp("scale")
			rec := op.Status.CountGivenBack
			if rec.Units != 1 || (tc.wantCount == nil) != (rec.Instances == nil) || (tc.wantCount != nil && *rec.Instances != *tc.wantCount) {
				t.Errorf("the record is %+v, want 1 unit and count %v", rec, tc.wantCount)
			}
			if got := w.md(hoMD).Spec.Instances; got != tc.wantSpec {
				t.Errorf("spec.instances is %d, want %d", got, tc.wantSpec)
			}
			if op.Status.FinishedAt == nil {
				t.Error("the Operation did not finish after the record")
			}
		})
	}
}

func ptr32(v int32) *int32 { return &v }

// A record that does not land is written again, and the count is never given back twice: the first record's status write fails
// after the spec write landed; the next pass finds N + U written and no record, and writes the record alone.
func TestAGiveBackWhoseRecordDidNotLandIsRecordedAloneNotWrittenTwice(t *testing.T) {
	w, _, _, stop := lastReadyScale(t)
	stop()
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err != nil {
		t.Fatalf("the pass in which the spec write lands: %v", err)
	}
	failed := false
	w.fc.set(func(method string, obj client.Object) error {
		if op, ok := obj.(*v1alpha1.Operation); ok && method == "Status.Update" && op.Name == "scale" && op.Status.CountGivenBack != nil && !failed {
			failed = true
			return errors.New("injected: the record's write fails")
		}
		return nil
	}, nil)
	w.clock.Advance(10 * time.Second)
	if err := w.stepKeepingErrors(); err == nil || !strings.Contains(err.Error(), "injected") {
		t.Fatalf("the give-back pass returned %v, want the injected fault", err)
	}
	if got := w.md(hoMD).Spec.Instances; got != 2 {
		t.Fatalf("spec.instances is %d after the pass whose record failed, want the 2 written", got)
	}
	if op := w.getOp("scale"); op.Status.CountGivenBack != nil || op.Status.FinishedAt != nil {
		t.Fatalf("the record %+v and finishedAt %v landed although its write failed", op.Status.CountGivenBack, op.Status.FinishedAt)
	}
	w.restart(false) // a restart finds 2 written and no record
	w.stepsUntil(2*time.Minute, 10*time.Second, "the record", func() bool { return w.getOp("scale").Status.CountGivenBack != nil })
	op := w.getOp("scale")
	if rec := op.Status.CountGivenBack; rec.Units != 1 || rec.Instances == nil || *rec.Instances != 2 {
		t.Errorf("the record is %+v, want 1 unit and 2 written", rec)
	}
	if got := w.md(hoMD).Spec.Instances; got != 2 {
		t.Errorf("spec.instances is %d: the count was given back twice or not at all, want 2", got)
	}
	if op.Status.FinishedAt == nil || op.Status.Phase != v1alpha1.OperationPhasePartlyFailed {
		t.Errorf("the Operation is %q finished %v after the record landed", op.Status.Phase, op.Status.FinishedAt)
	}
}

// Under a Scale above zero a started-since-commit pair is not taken: P's Instance, committed CancelPending, and its Allocation both
// fail JobStartedSinceCommit; the Instance keeps starting and serving, and the count is given back. P's Allocation is MARKED during the
// Operation (before any act, so before the job started), and its mark is CLEARED once the Operation has finished and the pair serves on:
// a serving pair is no kept-warm capacity, and when its Instance later ends it follows its retention policy as any other does.
func TestAScaleDoesNotTakeAStartedSinceCommitPairAndGivesItsCountBack(t *testing.T) {
	w := newExecWorld(t, time.Hour)
	stop := w.failSpecWrites()
	w.approveScale("scale", 1, 2)
	if a, ok := w.allocation(w.pendingAlloc); !ok || !keptWarm(a) {
		t.Errorf("P's Allocation is %+v (found %v) after the commit, want it MARKED before any act", a, ok)
	}
	w.stepsUntil(90*time.Minute, 2*time.Minute, "P ready", func() bool { return w.instance(w.pending).Status.Phase == v1alpha1.InstanceReady })
	if a, ok := w.allocation(w.pendingAlloc); !ok || !keptWarm(a) {
		t.Errorf("P's Allocation is %+v (found %v) while the Operation is unfinished, want it still marked: it is a target of an unfinished Operation", a, ok)
	}
	stop()
	w.stepsUntil(5*time.Minute, 10*time.Second, "the Operation finished", func() bool { return w.getOp("scale").Status.FinishedAt != nil })
	w.passes(3, nil)
	op := w.getOp("scale")
	for _, name := range []string{w.pending, w.pendingAlloc} {
		if o, ok := outcomeOf(op, name); !ok || o.State != v1alpha1.TargetStateFailed || o.Reason != reasonJobStartedSinceCommit {
			t.Errorf("%s's outcome is %+v (%v), want Failed with %s", name, o, ok, reasonJobStartedSinceCommit)
		}
	}
	if o, ok := outcomeOf(op, w.pendingAlloc); !ok || !strings.Contains(o.Message, "kept warm") {
		t.Errorf("P's Allocation outcome is %+v (%v), want the words kept warm: the report is historical", o, ok)
	}
	if in := w.instance(w.pending); isDraining(in) || ended(in) || in.Status.Phase != v1alpha1.InstanceReady {
		t.Errorf("P is %q: under a Scale above zero it is not taken, and serves", in.Status.Phase)
	}
	if a, ok := w.allocation(w.pendingAlloc); !ok || !a.DeletionTimestamp.IsZero() || keptWarm(a) {
		t.Errorf("P's Allocation is %+v (found %v), want it present with its mark CLEARED: the Operation has finished and the pair serves on", a, ok)
	}
	if rec := op.Status.CountGivenBack; rec == nil || rec.Units != 1 || rec.Instances == nil || *rec.Instances != 2 {
		t.Errorf("the record is %+v, want 1 unit and 2 written", rec)
	}
	if got := w.md(hoMD).Spec.Instances; got != 2 {
		t.Errorf("spec.instances is %d, want 2", got)
	}
	if got := w.batch.Count("scancel"); got != 0 {
		t.Errorf("%d scancel calls", got)
	}
}

// A hedge, with its members and its replacement, is ONE unit: three live Instances of one primary's capacity whose targets failed
// finally count once, and a second primary's Instance counts again.
func TestAHedgeWithItsMembersAndReplacementIsOneUntakenUnit(t *testing.T) {
	inst := func(name, hedge, pred string) *v1alpha1.Instance {
		in := standInstance2(name, hedge, pred)
		return in
	}
	h1, m2, rep := inst("h1", "", ""), inst("m2", "h1", ""), inst("rep", "", "h1")
	other := inst("o1", "", "")
	p := &pass{insts: []*v1alpha1.Instance{h1, m2, rep, other}}
	op := executingOp("scale", scaleOp(1), nil)
	var outcomes []v1alpha1.TargetOutcome
	for _, in := range p.insts {
		op.Status.Committed = append(op.Status.Committed, v1alpha1.CommittedTarget{Kind: v1alpha1.TargetKindInstance, Name: in.Name, UID: in.UID})
		outcomes = append(outcomes, v1alpha1.TargetOutcome{Kind: v1alpha1.TargetKindInstance, Name: in.Name, UID: in.UID, State: v1alpha1.TargetStateFailed, Reason: reasonLastReadySinceCommit})
	}
	if got := p.untakenUnits(op, outcomes); got != 2 {
		t.Errorf("%d untaken units, want 2: the hedge's two members and the replacement are one, the other primary another", got)
	}
	if got := p.untakenUnits(executingOp("scale", scaleOp(0), op.Status.Committed), outcomes); got != 0 {
		t.Errorf("%d untaken units under a Scale to zero, want none: a Scale to zero gives nothing back", got)
	}
}

// standInstance2 is a live Instance named name, a member of the hedge first (when not empty), the replacement of pred (when not empty).
func standInstance2(name, first, pred string) *v1alpha1.Instance {
	in := &v1alpha1.Instance{}
	in.Name, in.UID = name, types.UID("uid-"+name)
	in.Status.Phase = v1alpha1.InstanceReady
	in.Annotations = map[string]string{}
	if first != "" {
		in.Annotations[v1alpha1.AnnotationHedgeGroup] = first
	}
	if pred != "" {
		in.Annotations[v1alpha1.AnnotationPredecessor] = pred
	}
	return in
}
