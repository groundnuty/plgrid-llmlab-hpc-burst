package lifecycle

// Start-up outcomes (HBEP-7, Hedging (beta), The start-up failure rate; #604 PR 4). Every expected outcome and count
// is written out by hand.

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// started is a Running Allocation whose job started at 08:00:20, with uid.
func started(name, uid string) *v1alpha1.Allocation {
	a := &v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: name, UID: types.UID(uid)}}
	s := metav1.NewTime(hoT(8, 0, 20))
	a.Status.AtProvider.StartTime = &s
	a.Status.AtProvider.Phase = v1alpha1.AllocationRunning
	return a
}

// outcomeInstance is an Instance on Allocation alloc of the rig's Provider, created at created, ready at ready (nil:
// never), ended at end with reason (end nil: not ended).
func outcomeInstance(name, alloc string, created time.Time, ready, end *time.Time, reason, record string) *v1alpha1.Instance {
	in := &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: name, UID: types.UID("uid-" + name),
		CreationTimestamp: metav1.NewTime(created), Labels: map[string]string{v1alpha1.LabelProvider: rigProvider}}}
	in.Spec.AllocationRef.Name = alloc
	in.Status.Phase = v1alpha1.InstanceStarting
	if ready != nil {
		t := metav1.NewTime(*ready)
		in.Status.ReadyTime = &t
		in.Status.Phase = v1alpha1.InstanceReady
	}
	if end != nil {
		t := metav1.NewTime(*end)
		in.Status.EndTime = &t
		in.Status.Phase = v1alpha1.InstanceEnded
		in.Status.EndReason = reason
	}
	in.Status.VerificationRecord = record
	return in
}

func TestTheStartOutcomeOfAnInstance(t *testing.T) {
	r, e := hoT(8, 1, 10), hoT(8, 5, 0)
	for _, c := range []struct {
		name   string
		in     *v1alpha1.Instance
		alloc  *v1alpha1.Allocation
		want   string // "" for none
		reason string
		at     time.Time
	}{
		{"ready", outcomeInstance("i", "a", hoT(8, 0, 0), &r, nil, "", ""), started("a", "u1"), v1alpha1.StartOutcomeReady, "", r},
		{"ready, then drained", outcomeInstance("i", "a", hoT(8, 0, 0), &r, &e, v1alpha1.ReasonDrained, ""), started("a", "u1"), v1alpha1.StartOutcomeReady, "", r},
		{"ranked out before ready", outcomeInstance("i", "a", hoT(8, 0, 0), nil, &e, v1alpha1.ReasonRankedOut, ""), started("a", "u1"), v1alpha1.StartOutcomeReady, "", e},
		{"its registration timed out", outcomeInstance("i", "a", hoT(8, 0, 0), nil, &e, v1alpha1.ReasonRegistrationTimeout, ""), started("a", "u1"), v1alpha1.StartOutcomeFailed, v1alpha1.ReasonRegistrationTimeout, e},
		{"requeued", outcomeInstance("i", "a", hoT(8, 0, 0), nil, &e, v1alpha1.ReasonRequeued, ""), started("a", "u1"), v1alpha1.StartOutcomeFailed, v1alpha1.ReasonRequeued, e},
		{"a hedge loss", outcomeInstance("i", "a", hoT(8, 0, 0), nil, &e, v1alpha1.ReasonHedgeLost, ""), started("a", "u1"), "", "", time.Time{}},
		{"scaled down before ready", outcomeInstance("i", "a", hoT(8, 0, 0), nil, &e, v1alpha1.ReasonDrained, ""), started("a", "u1"), "", "", time.Time{}},
		{"still starting", outcomeInstance("i", "a", hoT(8, 0, 0), nil, nil, "", ""), started("a", "u1"), "", "", time.Time{}},
		{"its job never started", outcomeInstance("i", "a", hoT(8, 0, 0), nil, &e, v1alpha1.ReasonRegistrationTimeout, ""), &v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{UID: "u1"}}, "", "", time.Time{}},
		{"no Allocation", outcomeInstance("i", "a", hoT(8, 0, 0), &r, nil, "", ""), nil, "", "", time.Time{}},
	} {
		o, ok := startOutcomeOf(c.in, c.alloc)
		got := ""
		if ok {
			got = o.Result
		}
		if got != c.want || (ok && (o.Reason != c.reason || !o.Time.Time.Equal(c.at) || o.AllocationUID != "u1")) {
			t.Errorf("%s: %v %+v, want %q reason %q at %s", c.name, ok, o, c.want, c.reason, c.at.Format(time.TimeOnly))
		}
	}
}

func TestClassCounts(t *testing.T) {
	o := func(result, reason string) v1alpha1.StartOutcome {
		return v1alpha1.StartOutcome{Result: result, Reason: reason}
	}
	f, q, n := classCounts([]v1alpha1.StartOutcome{
		o("Ready", ""), o("Failed", v1alpha1.ReasonVerificationFailed), o("Failed", v1alpha1.ReasonQualificationFailed),
		o("Failed", v1alpha1.ReasonRegistrationTimeout), o("Ready", ""),
	})
	if f != 3 || q != 2 || n != 5 {
		t.Fatalf("classCounts: %d failures, %d in qualification, %d kept; want 3, 2 and 5", f, q, n)
	}
	if f, q, n := classCounts(nil); f != 0 || q != 0 || n != 0 {
		t.Fatalf("classCounts of none: %d, %d, %d; want 0, 0, 0", f, q, n)
	}
}

// records is a client.Reader that holds verification records, each a cause, by name.
type records struct {
	client.Reader
	cause map[string]string
}

func (r records) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	rev, ok := obj.(*appsv1.ControllerRevision)
	cause, found := r.cause[key.Name]
	if !ok || !found {
		return apierrors.NewNotFound(appsv1.Resource("controllerrevisions"), key.Name)
	}
	raw, err := json.Marshal(verificationData{Cause: cause})
	if err != nil {
		return err
	}
	rev.Data = runtime.RawExtension{Raw: raw}
	return nil
}

// outcomePass is a pass over insts and allocs on the rig's Provider, in class (rigGPU, rigArch), its verification
// records' causes by name.
func outcomePass(insts []*v1alpha1.Instance, allocs []*v1alpha1.Allocation, causes map[string]string) *pass {
	am := map[string]*v1alpha1.Allocation{}
	for _, a := range allocs {
		am[a.Name] = a
	}
	pv := &providerView{name: rigProvider, hasClass: true, class: v1alpha1.AcceleratorClass{GPUModel: rigGPU, CPUArch: rigArch}}
	return &pass{r: &Reconciler{Reader: records{cause: causes}}, ctx: context.Background(), insts: insts, allocs: am,
		pvByName: map[string]*providerView{rigProvider: pv}}
}

// TestRecordStartOutcomes is HBEP-7's Test plan row: a class with 16 ready outcomes and 4 failures (a
// RegistrationTimeout, a rehash VerificationFailed, an AllocationFailed before ready, a Requeued) has a rate of 4 in
// 20, one of them in verification or qualification. A HedgeLost before ready, an Instance scaled down before ready, a
// settings contradiction and a second Instance on an Allocation add none. Read again (a restarted controller), the
// same Instances add nothing more. With 19, the count is 19 and the rate shown as such. A 21st keeps the latest 20.
func TestRecordStartOutcomes(t *testing.T) {
	var insts []*v1alpha1.Instance
	var allocs []*v1alpha1.Allocation
	add := func(i int, ready bool, reason, record string) {
		name := fmt.Sprintf("i%02d", i)
		an := fmt.Sprintf("a%02d", i)
		allocs = append(allocs, started(an, fmt.Sprintf("u%02d", i)))
		created := hoT(8, 0, 0).Add(time.Duration(i) * time.Minute)
		at := created.Add(5 * time.Minute)
		if ready {
			insts = append(insts, outcomeInstance(name, an, created, &at, nil, "", ""))
		} else {
			insts = append(insts, outcomeInstance(name, an, created, nil, &at, reason, record))
		}
	}
	for i := 0; i < 16; i++ {
		add(i, true, "", "")
	}
	add(16, false, v1alpha1.ReasonRegistrationTimeout, "")
	add(17, false, v1alpha1.ReasonVerificationFailed, "ver-rehash")
	add(18, false, v1alpha1.ReasonAllocationFailed, "")
	add(19, false, v1alpha1.ReasonRequeued, "")
	add(20, false, v1alpha1.ReasonHedgeLost, "")
	add(21, false, v1alpha1.ReasonDrained, "")
	add(22, false, v1alpha1.ReasonVerificationFailed, "ver-contradiction")
	// A second Instance on i00's Allocation, failed: not the Allocation's first, so it adds nothing. It is read before
	// i00, so the first-Instance rule, not the Allocation's UID being kept, is what keeps it out.
	second := outcomeInstance("i00-again", "a00", hoT(9, 0, 0), nil, ptrTo(hoT(9, 5, 0)), v1alpha1.ReasonRegistrationTimeout, "")
	insts = append([]*v1alpha1.Instance{second}, insts...)
	causes := map[string]string{"ver-rehash": causeRehash, "ver-contradiction": causeContradiction}

	p := outcomePass(insts, allocs, causes)
	var s v1alpha1.ModelDeploymentStatus
	p.recordStartOutcomes(&s)
	if len(s.StartOutcomes) != 1 {
		t.Fatalf("%d classes, want 1", len(s.StartOutcomes))
	}
	c := s.StartOutcomes[0]
	if c.Count != 20 || c.Failures != 4 || c.QualificationFailures != 1 || len(c.Outcomes) != 20 {
		t.Fatalf("the class counts %d of %d (%d in qualification), %d kept; want 4 of 20, 1, 20", c.Failures, c.Count, c.QualificationFailures, len(c.Outcomes))
	}
	var failed []string
	for _, o := range c.Outcomes {
		if o.Result == v1alpha1.StartOutcomeFailed {
			failed = append(failed, string(o.AllocationUID)+":"+o.Reason)
		}
	}
	want := []string{"u16:RegistrationTimeout", "u17:VerificationFailed", "u18:AllocationFailed", "u19:Requeued"}
	if !slices.Equal(failed, want) {
		t.Fatalf("the failures are %v, want %v", failed, want)
	}
	// Read again, as a restarted controller would, from the status: nothing more.
	again := s.DeepCopy()
	outcomePass(insts, allocs, causes).recordStartOutcomes(again)
	if again.StartOutcomes[0].Count != 20 || len(again.StartOutcomes[0].Outcomes) != 20 {
		t.Fatalf("a second read counts %d, want the same 20", again.StartOutcomes[0].Count)
	}
	// A 21st outcome, i23 ready latest: the oldest, u00, goes.
	add(23, true, "", "")
	more := s.DeepCopy()
	outcomePass(insts, allocs, causes).recordStartOutcomes(more)
	m := more.StartOutcomes[0]
	if m.Count != 20 || m.Outcomes[0].AllocationUID != "u01" || m.Outcomes[19].AllocationUID != "u23" || m.Failures != 4 {
		t.Fatalf("with a 21st: count %d, first %s, last %s, failures %d; want 20, u01, u23, 4", m.Count, m.Outcomes[0].AllocationUID, m.Outcomes[19].AllocationUID, m.Failures)
	}
	// The next pass, over the same Instances and Allocations, a00 still live (PR 650's review): u00, trimmed, is found
	// again, sorts to the head by its time and is trimmed again. The list is unchanged, still oldest first.
	next := more.DeepCopy()
	outcomePass(insts, allocs, causes).recordStartOutcomes(next)
	var was, now []string
	for _, o := range m.Outcomes {
		was = append(was, string(o.AllocationUID))
	}
	for _, o := range next.StartOutcomes[0].Outcomes {
		now = append(now, string(o.AllocationUID))
	}
	if !slices.Equal(was, now) || next.StartOutcomes[0].Failures != 4 || next.StartOutcomes[0].Count != 20 {
		t.Fatalf("a second pass changed the window: %v (failures %d of %d), was %v", now, next.StartOutcomes[0].Failures, next.StartOutcomes[0].Count, was)
	}
	// 19 outcomes: drop the last ready one.
	var few v1alpha1.ModelDeploymentStatus
	outcomePass(insts[2:21], allocs[1:20], causes).recordStartOutcomes(&few)
	if f := few.StartOutcomes[0]; f.Count != 19 || f.Failures != 4 {
		t.Fatalf("with 19 outcomes: %d of %d, want 4 of 19", f.Failures, f.Count)
	}
}

// TestAnUnreadableRecordWaits: a VerificationFailed Instance whose record can't be read yet adds nothing this pass,
// rather than a failure that might be a spec fault.
func TestAnUnreadableRecordWaits(t *testing.T) {
	e := hoT(8, 5, 0)
	in := outcomeInstance("i", "a", hoT(8, 0, 0), nil, &e, v1alpha1.ReasonVerificationFailed, "ver-gone")
	p := outcomePass([]*v1alpha1.Instance{in}, []*v1alpha1.Allocation{started("a", "u1")}, map[string]string{})
	p.r.Reader = failingReader{}
	var s v1alpha1.ModelDeploymentStatus
	p.recordStartOutcomes(&s)
	if len(s.StartOutcomes) != 0 {
		t.Fatalf("an unreadable record's Instance added %+v", s.StartOutcomes)
	}
}

type failingReader struct{ client.Reader }

func (failingReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return apierrors.NewServiceUnavailable("the record can't be read")
}

// TestAReadyPrimaryRecordsItsOutcome: through the rig's passes, a deployment's primary, ready at 08:01:10, adds one
// Ready outcome to its class, keyed by its Allocation's UID, 0 of 1. After a controller restart and more passes the
// class still keeps that one outcome.
func TestAReadyPrimaryRecordsItsOutcome(t *testing.T) {
	rg := newRig(t, withMaxTime(4*time.Hour))
	reservedStart(rg, hoMD, nil)
	p := rg.instances(hoMD)[0]
	a, ok := rg.allocation(p.Spec.AllocationRef.Name)
	if !ok {
		t.Fatal("no Allocation for the primary")
	}
	check := func(when string) {
		so := rg.md(hoMD).Status.StartOutcomes
		if len(so) != 1 || so[0].Count != 1 || so[0].Failures != 0 || len(so[0].Outcomes) != 1 ||
			so[0].Outcomes[0].AllocationUID != a.UID || so[0].Outcomes[0].Result != v1alpha1.StartOutcomeReady ||
			!so[0].Outcomes[0].Time.Time.Equal(hoT(8, 1, 10)) {
			t.Fatalf("%s: startOutcomes %+v, want one Ready outcome of %s at 08:01:10", when, so, a.UID)
		}
	}
	check("once ready")
	rg.restartController()
	for range 5 {
		rg.clock.Advance(10 * time.Second)
		rg.Step()
	}
	check("after a restart")
}
