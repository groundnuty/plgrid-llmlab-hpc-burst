package providerctl

// The Provider's estimate controller (beta; HBEP-4, Estimate; HBEP-10, The
// release cost; #661), driven by calling Reconcile over controller-runtime's
// fake client against pkg/provider/fake.Batch. The server-side apply itself is
// shown against a real API server in e2e (P1, P2); here the write is a recorder
// that stores what an apply of this Provider's manager would leave. Starts are
// literals worked out by hand, and counts come from the fake Batch's call log,
// never from return values.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	"github.com/go-logr/logr/funcr"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// startE1 is #661's probe: epoch 1796017690, which is 2026-11-30T05:48:10Z.
var startE1 = time.Date(2026, 11, 30, 5, 48, 10, 0, time.UTC)

// shapeOfTheStandardAllocation is the SHA-256 of the canonical form of the
// parameters newAlloc gives (partition example-gpu, 1 GPU, 8 CPUs, 16 GiB, 30 min,
// one node, account example-account, no qos, no constraint), first 16 hex
// characters, from printf of that form into sha256sum.
const shapeOfTheStandardAllocation = "165ad3f29c2206bc"

// applyLog records what the controller applies and stores it as the API server
// would: the Provider's manager owns this Provider's entries, so the apply
// replaces them, and another Provider's stay.
type applyLog struct {
	mu      sync.Mutex
	c       client.Client
	applies [][]v1alpha1.OperationEstimate
	failure error
}

func (l *applyLog) apply(ctx context.Context, op *v1alpha1.Operation, entries []v1alpha1.OperationEstimate) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.failure != nil {
		return l.failure
	}
	l.applies = append(l.applies, slices.Clone(entries))
	var cur v1alpha1.Operation
	if err := l.c.Get(ctx, client.ObjectKeyFromObject(op), &cur); err != nil {
		return err
	}
	kept := slices.DeleteFunc(slices.Clone(cur.Status.Estimates), func(e v1alpha1.OperationEstimate) bool { return e.Provider == testProvider })
	cur.Status.Estimates = append(kept, entries...)
	return l.c.Update(ctx, &cur)
}

func (l *applyLog) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.applies)
}

func (l *applyLog) last() []v1alpha1.OperationEstimate {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.applies) == 0 {
		return nil
	}
	return l.applies[len(l.applies)-1]
}

type estEnv struct {
	*env
	est *estimator
	log *applyLog
}

func newEstEnv(t *testing.T) *estEnv {
	t.Helper()
	e := newEnv(t)
	log := &applyLog{c: e.c}
	est := &estimator{s: e.ctl.shared, provider: testProvider, cache: e.c, api: e.c, apply: log.apply,
		bound: time.Second, inflight: map[string]struct{}{}}
	return &estEnv{env: e, est: est, log: log}
}

func allocTarget(a *v1alpha1.Allocation) v1alpha1.PlanTarget {
	return v1alpha1.PlanTarget{Kind: v1alpha1.TargetKindAllocation, Name: a.Name, UID: a.UID, Generation: 1, Provider: testProvider,
		Effect: v1alpha1.PlanEffectReleaseStarted, NeedsApproval: true, Cost: &v1alpha1.ReleaseCost{}}
}

// newOperation creates a Release Operation planned now, with the plan's targets.
func (e *estEnv) newOperation(name, planID string, phase v1alpha1.OperationPhase, targets ...v1alpha1.PlanTarget) *v1alpha1.Operation {
	e.t.Helper()
	return e.newOperationAs(name, types.UID("uid-op-"+name), planID, phase, targets...)
}

// newOperationAs is newOperation with the UID the API server would have given.
func (e *estEnv) newOperationAs(name string, uid types.UID, planID string, phase v1alpha1.OperationPhase, targets ...v1alpha1.PlanTarget) *v1alpha1.Operation {
	e.t.Helper()
	op := &v1alpha1.Operation{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, UID: uid},
		Spec: v1alpha1.OperationSpec{Action: v1alpha1.OperationActionRelease, ModelDeploymentRef: v1alpha1.ObjectReference{Name: "md"},
			Owner: "example-owner"},
		Status: v1alpha1.OperationStatus{Phase: phase, Plan: &v1alpha1.OperationPlan{
			ID: planID, ObservedAt: metav1.NewTime(e.clock.Now()),
			ModelDeployment: v1alpha1.PlannedDeployment{Name: "md", UID: "uid-md", Generation: 1}, Targets: targets,
		}},
	}
	if err := e.c.Create(e.ctx, op); err != nil {
		e.t.Fatal(err)
	}
	return op
}

func (e *estEnv) reconcileOp(name string) error {
	e.t.Helper()
	_, err := e.est.Reconcile(e.ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: testNS, Name: name}})
	return err
}

// asked is the number of Estimate calls the fake Slurm saw.
func (e *estEnv) asked() int { return e.batch.Count("sbatch-test-only") }

func (e *estEnv) stored(name string) []v1alpha1.OperationEstimate {
	e.t.Helper()
	var op v1alpha1.Operation
	if err := e.c.Get(e.ctx, client.ObjectKey{Namespace: testNS, Name: name}, &op); err != nil {
		e.t.Fatal(err)
	}
	return op.Status.Estimates
}

// wantNoSubmit: an estimate never submits, cancels or lifts: the call log holds
// none, whatever the controller asked.
func (e *estEnv) wantNoSubmit() {
	e.t.Helper()
	for _, op := range []string{"sbatch", "scancel", "scontrol-release", "ensure-file"} {
		if n := e.count(op); n != 0 {
			e.t.Errorf("%d %s calls after estimates, want none", n, op)
		}
	}
}

// The shape is the SHA-256 of the canonical form, worked out by hand.
func TestEstimateShapeIsTheHashOfItsCanonicalForm(t *testing.T) {
	std := provider.SubmitRequest{Tag: "tag-a", Hold: true, Account: "example-account", Params: v1alpha1.BatchParameters{
		Partition: gpuPart, TimeLimit: "30m", GPUs: 1, CPUsPerTask: 8, Memory: resource.MustParse("16Gi"), Nodes: 1, AgentDigest: strings.Repeat("a", 64)}}
	const wantForm = "hpcburst-estimate-shape/1\npartition=example-gpu\nqos=\ngpus=1\ncpus=8\nmemMiB=16384\nminutes=30\nnodes=1\nconstraint=\naccount=example-account\n"
	if got := estimateShapeForm(std, ""); got != wantForm {
		t.Fatalf("form\n got  %q\n want %q", got, wantForm)
	}
	if got := estimateShape(std, ""); got != shapeOfTheStandardAllocation {
		t.Fatalf("shape %s, want %s", got, shapeOfTheStandardAllocation)
	}
	other := provider.SubmitRequest{Tag: "tag-b", Account: "SEEDACCT", QOS: "now", Params: v1alpha1.BatchParameters{
		Partition: gpuPart, TimeLimit: "24h", GPUs: 2, CPUsPerTask: 16, Memory: resource.MustParse("32Gi")}} // nodes unset: one
	const wantOther = "hpcburst-estimate-shape/1\npartition=example-gpu\nqos=now\ngpus=2\ncpus=16\nmemMiB=32768\nminutes=1440\nnodes=1\nconstraint=localfs\naccount=SEEDACCT\n"
	if got := estimateShapeForm(other, "localfs"); got != wantOther {
		t.Fatalf("form\n got  %q\n want %q", got, wantOther)
	}
	if got, want := estimateShape(other, "localfs"), "5735112791c28186"; got != want {
		t.Fatalf("shape %s, want %s", got, want)
	}
}

func TestEstimateShapeIgnoresTheTagTheHoldAndTheScriptAndSeesTheRest(t *testing.T) {
	base := provider.SubmitRequest{Tag: "tag-a", Hold: true, Account: "example-account", QOS: "now", Params: v1alpha1.BatchParameters{
		Partition: gpuPart, TimeLimit: "30m", GPUs: 1, CPUsPerTask: 8, Memory: resource.MustParse("16Gi"), Nodes: 1, AgentDigest: strings.Repeat("a", 64)}}
	want := estimateShape(base, "localfs")
	same := base
	same.Tag, same.Hold, same.Params.AgentDigest, same.Params.Nodes = "tag-b", false, strings.Repeat("b", 64), 0
	same.Params.Memory = resource.MustParse("16384Mi") // the same memory, written another way
	same.Params.TimeLimit = "1800s"                    // the same limit, written another way
	if got := estimateShape(same, "localfs"); got != want {
		t.Errorf("tag, hold, script, an unset node count and other spellings changed the shape: %s, want %s", got, want)
	}
	for name, mod := range map[string]func(*provider.SubmitRequest, *string){
		"partition":  func(r *provider.SubmitRequest, _ *string) { r.Params.Partition = "example-other" },
		"qos":        func(r *provider.SubmitRequest, _ *string) { r.QOS = "normal" },
		"gpus":       func(r *provider.SubmitRequest, _ *string) { r.Params.GPUs = 2 },
		"cpus":       func(r *provider.SubmitRequest, _ *string) { r.Params.CPUsPerTask = 16 },
		"memory":     func(r *provider.SubmitRequest, _ *string) { r.Params.Memory = resource.MustParse("32Gi") },
		"time limit": func(r *provider.SubmitRequest, _ *string) { r.Params.TimeLimit = "31m" },
		"nodes":      func(r *provider.SubmitRequest, _ *string) { r.Params.Nodes = 2 },
		"constraint": func(_ *provider.SubmitRequest, c *string) { *c = "otherfs" },
		"account":    func(r *provider.SubmitRequest, _ *string) { r.Account = "SEEDACCT" },
	} {
		r, constraint := base, "localfs"
		mod(&r, &constraint)
		if estimateShape(r, constraint) == want {
			t.Errorf("a different %s kept the shape", name)
		}
	}
}

// P1 at the unit level: one Operation, two targets of one shape on its Provider:
// exactly one Estimate call, one entry with both target UIDs. The estimate is
// asked as a read: nothing is submitted, cancelled or lifted.
func TestTwoTargetsOfOneShapeAreOneEstimate(t *testing.T) {
	e := newEstEnv(t)
	e.batch.SetEstimate(startE1)
	a1, a2 := e.newAlloc("alloc-1"), e.newAlloc("alloc-2")
	e.newOperation("op-1", "plan-a", v1alpha1.OperationPhasePlanned, allocTarget(a1), allocTarget(a2))

	if err := e.reconcileOp("op-1"); err != nil {
		t.Fatal(err)
	}
	if n := e.asked(); n != 1 {
		t.Fatalf("%d Estimate calls for two targets of one shape, want 1", n)
	}
	if reqs := e.batch.EstimateRequests(); reqs[0].Hold || reqs[0].Account != "example-account" || reqs[0].Params.GPUs != 1 {
		t.Errorf("the request %+v, want Create's without the hold", reqs[0])
	}
	e.wantNoSubmit()
	if n := e.log.count(); n != 1 {
		t.Fatalf("%d applies, want 1", n)
	}
	got := e.stored("op-1")
	if len(got) != 1 {
		t.Fatalf("entries %+v, want one", got)
	}
	est := got[0]
	if est.Provider != testProvider || est.PlanID != "plan-a" || est.Shape != shapeOfTheStandardAllocation {
		t.Errorf("entry %s/%s/%s, want %s/plan-a/%s", est.Provider, est.PlanID, est.Shape, testProvider, shapeOfTheStandardAllocation)
	}
	if !slices.Equal(est.Targets, []string{"uid-alloc-1", "uid-alloc-2"}) {
		t.Errorf("targets %v, want both UIDs", est.Targets)
	}
	if est.Start.Time == nil || !est.Start.Time.Equal(&metav1.Time{Time: startE1}) || est.Start.Unknown != "" || !est.Start.Advisory {
		t.Errorf("start %+v, want %v, advisory", est.Start, startE1)
	}
	if est.Start.ObservedAt == nil || !est.Start.ObservedAt.Equal(&metav1.Time{Time: at(12, 0, 0)}) {
		t.Errorf("observedAt %v, want the read time %v", est.Start.ObservedAt, at(12, 0, 0))
	}
}

// Two shapes of one Provider are two estimates, and every apply carries all of
// the Provider's entries: the second apply holds both.
func TestTwoShapesAreTwoEstimatesAndEveryApplyCarriesBoth(t *testing.T) {
	e := newEstEnv(t)
	e.batch.SetEstimate(startE1)
	a1 := e.newAlloc("alloc-1")
	a2 := e.newAlloc("alloc-2", func(a *v1alpha1.Allocation) { a.Spec.ForProvider.Batch.GPUs = 2 })
	e.newOperation("op-1", "plan-a", v1alpha1.OperationPhasePlanned, allocTarget(a1), allocTarget(a2))

	if err := e.reconcileOp("op-1"); err != nil {
		t.Fatal(err)
	}
	if n := e.asked(); n != 2 {
		t.Fatalf("%d Estimate calls for two shapes, want 2", n)
	}
	if n := e.log.count(); n != 2 || len(e.log.last()) != 2 {
		t.Fatalf("%d applies, the last carrying %d entries, want 2 and 2: an apply carries all of the Provider's entries", n, len(e.log.last()))
	}
	if got := e.stored("op-1"); len(got) != 2 || got[0].Shape == got[1].Shape {
		t.Fatalf("entries %+v, want two of different shapes", got)
	}
}

// A shape that already has its entry is not asked again while another shape of the plan is still due (an apply that
// failed after the first shape, or a controller that restarted between the two): only the missing one is asked, and
// the apply that follows carries both.
func TestAShapeThatHasItsEntryIsNotAskedAgainWhileAnotherIsDue(t *testing.T) {
	e := newEstEnv(t)
	e.batch.SetEstimate(startE1)
	a1 := e.newAlloc("alloc-1")
	a2 := e.newAlloc("alloc-2", func(a *v1alpha1.Allocation) { a.Spec.ForProvider.Batch.GPUs = 2 })
	op := e.newOperation("op-1", "plan-a", v1alpha1.OperationPhasePlanned, allocTarget(a1), allocTarget(a2))
	observed := metav1.NewTime(at(11, 59, 0))
	earlier := v1alpha1.OperationEstimate{Provider: testProvider, PlanID: "plan-a", Shape: shapeOfTheStandardAllocation, Targets: []string{"uid-alloc-1"},
		Start: v1alpha1.TimeFigure{Unknown: "an answer from before", ObservedAt: &observed, Advisory: true}}
	var cur v1alpha1.Operation
	if err := e.c.Get(e.ctx, client.ObjectKeyFromObject(op), &cur); err != nil {
		t.Fatal(err)
	}
	cur.Status.Estimates = []v1alpha1.OperationEstimate{earlier}
	if err := e.c.Update(e.ctx, &cur); err != nil {
		t.Fatal(err)
	}
	if err := e.reconcileOp("op-1"); err != nil {
		t.Fatal(err)
	}
	if reqs := e.batch.EstimateRequests(); len(reqs) != 1 || reqs[0].Params.GPUs != 2 {
		t.Fatalf("asked %+v, want only the 2-GPU shape, whose entry is missing", reqs)
	}
	got := e.stored("op-1")
	if len(got) != 2 {
		t.Fatalf("entries %+v, want the earlier one and the new one", got)
	}
	for _, est := range got {
		if est.Shape == shapeOfTheStandardAllocation && est.Start.Unknown != "an answer from before" {
			t.Errorf("the earlier entry was asked for again: %+v", est.Start)
		}
	}
	if last := e.log.last(); len(last) != 2 {
		t.Errorf("the apply carried %d entries, want both: an apply carries all of the Provider's entries", len(last))
	}
}

// P2: one estimate per plan ID and shape; a new plan ID asks again, and its
// entries replace the old plan's.
func TestAnEstimateIsAskedOncePerPlanAndAgainForANewPlan(t *testing.T) {
	e := newEstEnv(t)
	e.batch.SetEstimate(startE1)
	a1 := e.newAlloc("alloc-1")
	op := e.newOperation("op-1", "plan-a", v1alpha1.OperationPhasePlanned, allocTarget(a1))
	for range 3 {
		if err := e.reconcileOp("op-1"); err != nil {
			t.Fatal(err)
		}
	}
	if n := e.asked(); n != 1 {
		t.Fatalf("%d calls for the same plan ID asked three times, want 1", n)
	}

	var cur v1alpha1.Operation
	if err := e.c.Get(e.ctx, client.ObjectKeyFromObject(op), &cur); err != nil {
		t.Fatal(err)
	}
	cur.Status.Plan.ID = "plan-b"
	if err := e.c.Update(e.ctx, &cur); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := e.reconcileOp("op-1"); err != nil {
			t.Fatal(err)
		}
	}
	if n := e.asked(); n != 2 {
		t.Fatalf("%d calls after a re-plan with a new ID, want 2", n)
	}
	got := e.stored("op-1")
	if len(got) != 1 || got[0].PlanID != "plan-b" {
		t.Fatalf("entries %+v, want only the new plan's: the apply left the old plan's out", got)
	}
}

// Another Provider's entries are not this controller's: its apply never carries
// them, and they stay.
func TestAnotherProvidersEntriesAreNeitherCarriedNorTouched(t *testing.T) {
	e := newEstEnv(t)
	e.batch.SetEstimate(startE1)
	a1 := e.newAlloc("alloc-1")
	op := e.newOperation("op-1", "plan-a", v1alpha1.OperationPhasePlanned, allocTarget(a1))
	observed := metav1.NewTime(at(12, 0, 0))
	other := v1alpha1.OperationEstimate{Provider: "another-provider", PlanID: "plan-a", Shape: "cccccccccccccccc", Targets: []string{"uid-x"},
		Start: v1alpha1.TimeFigure{Unknown: "a reason", ObservedAt: &observed, Advisory: true}}
	var cur v1alpha1.Operation
	if err := e.c.Get(e.ctx, client.ObjectKeyFromObject(op), &cur); err != nil {
		t.Fatal(err)
	}
	cur.Status.Estimates = []v1alpha1.OperationEstimate{other}
	if err := e.c.Update(e.ctx, &cur); err != nil {
		t.Fatal(err)
	}
	if err := e.reconcileOp("op-1"); err != nil {
		t.Fatal(err)
	}
	for _, est := range e.log.last() {
		if est.Provider != testProvider {
			t.Errorf("the apply carries %s's entry", est.Provider)
		}
	}
	if got := e.stored("op-1"); len(got) != 2 {
		t.Fatalf("entries %+v, want this Provider's and the other's", got)
	}
}

// It never asks for an Operation that is terminal, planned more than 60 s ago,
// without a plan or being deleted, for a target without a cost, of another
// Provider or of another kind, or whose Allocation is not this Provider's. The
// first row is the control: the same fixture, with none of these, asks once.
func TestNothingIsAskedWhereNoEstimateIsDue(t *testing.T) {
	var deleteAfterCreate bool
	for _, tc := range []struct {
		name string
		want int
		mod  func(e *estEnv, op *v1alpha1.Operation, a *v1alpha1.Allocation)
	}{
		{"control: a planned Operation with a costed target", 1, func(*estEnv, *v1alpha1.Operation, *v1alpha1.Allocation) {}},
		{"AwaitingApproval is asked too", 1, func(_ *estEnv, op *v1alpha1.Operation, _ *v1alpha1.Allocation) {
			op.Status.Phase = v1alpha1.OperationPhaseAwaitingApproval
		}},
		{"Executing", 0, func(_ *estEnv, op *v1alpha1.Operation, _ *v1alpha1.Allocation) {
			op.Status.Phase = v1alpha1.OperationPhaseExecuting
		}},
		{"PartlyFailed", 0, func(_ *estEnv, op *v1alpha1.Operation, _ *v1alpha1.Allocation) {
			op.Status.Phase = v1alpha1.OperationPhasePartlyFailed
		}},
		{"Done", 0, func(_ *estEnv, op *v1alpha1.Operation, _ *v1alpha1.Allocation) {
			op.Status.Phase = v1alpha1.OperationPhaseDone
		}},
		{"Stale", 0, func(_ *estEnv, op *v1alpha1.Operation, _ *v1alpha1.Allocation) {
			op.Status.Phase = v1alpha1.OperationPhaseStale
		}},
		{"Refused", 0, func(_ *estEnv, op *v1alpha1.Operation, _ *v1alpha1.Allocation) {
			op.Status.Phase = v1alpha1.OperationPhaseRefused
		}},
		{"no phase yet", 0, func(_ *estEnv, op *v1alpha1.Operation, _ *v1alpha1.Allocation) { op.Status.Phase = "" }},
		{"no plan", 0, func(_ *estEnv, op *v1alpha1.Operation, _ *v1alpha1.Allocation) { op.Status.Plan = nil }},
		{"a plan older than 60 s", 0, func(e *estEnv, op *v1alpha1.Operation, _ *v1alpha1.Allocation) {
			op.Status.Plan.ObservedAt = metav1.NewTime(e.clock.Now().Add(-61 * time.Second))
		}},
		{"a plan exactly 60 s old is still asked", 1, func(e *estEnv, op *v1alpha1.Operation, _ *v1alpha1.Allocation) {
			op.Status.Plan.ObservedAt = metav1.NewTime(e.clock.Now().Add(-60 * time.Second))
		}},
		{"being deleted", 0, func(_ *estEnv, op *v1alpha1.Operation, _ *v1alpha1.Allocation) {
			// A finalizer keeps the Operation; the Delete after the create sets its deletionTimestamp.
			op.Finalizers = []string{"example.org/keep"}
			deleteAfterCreate = true
		}},
		{"a target without a cost", 0, func(_ *estEnv, op *v1alpha1.Operation, _ *v1alpha1.Allocation) { op.Status.Plan.Targets[0].Cost = nil }},
		{"a target on another Provider", 0, func(_ *estEnv, op *v1alpha1.Operation, _ *v1alpha1.Allocation) {
			op.Status.Plan.Targets[0].Provider = "another-provider"
		}},
		{"a target that is a ModelDeployment", 0, func(_ *estEnv, op *v1alpha1.Operation, _ *v1alpha1.Allocation) {
			op.Status.Plan.Targets[0].Kind = v1alpha1.TargetKindModelDeployment
		}},
		{"an Allocation that names another Provider", 0, func(_ *estEnv, _ *v1alpha1.Operation, a *v1alpha1.Allocation) {
			a.Spec.ProviderConfigReference = &xpv2.ProviderConfigReference{Kind: v1alpha1.ProviderConfigKind, Name: "another-provider"}
		}},
		{"an Allocation replaced since the plan (another UID)", 0, func(_ *estEnv, op *v1alpha1.Operation, _ *v1alpha1.Allocation) {
			op.Status.Plan.Targets[0].UID = "uid-replaced"
		}},
		{"an Allocation that is gone", 0, func(_ *estEnv, op *v1alpha1.Operation, _ *v1alpha1.Allocation) {
			op.Status.Plan.Targets[0].Name = "alloc-gone"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEstEnv(t)
			e.batch.SetEstimate(startE1)
			var a *v1alpha1.Allocation
			a = e.newAlloc("alloc-1")
			op := &v1alpha1.Operation{
				ObjectMeta: metav1.ObjectMeta{Name: "op-1", Namespace: testNS, UID: "uid-op-1"},
				Spec: v1alpha1.OperationSpec{Action: v1alpha1.OperationActionRelease, ModelDeploymentRef: v1alpha1.ObjectReference{Name: "md"},
					Owner: "example-owner"},
				Status: v1alpha1.OperationStatus{Phase: v1alpha1.OperationPhasePlanned, Plan: &v1alpha1.OperationPlan{
					ID: "plan-a", ObservedAt: metav1.NewTime(e.clock.Now()),
					ModelDeployment: v1alpha1.PlannedDeployment{Name: "md", UID: "uid-md", Generation: 1}, Targets: []v1alpha1.PlanTarget{allocTarget(a)}}},
			}
			tc.mod(e, op, a)
			if a.Spec.ProviderConfigReference.Name != testProvider {
				if err := e.c.Update(e.ctx, a); err != nil {
					t.Fatal(err)
				}
			}
			if err := e.c.Create(e.ctx, op); err != nil {
				t.Fatal(err)
			}
			if deleteAfterCreate {
				if err := e.c.Delete(e.ctx, op); err != nil {
					t.Fatal(err)
				}
				deleteAfterCreate = false
				var got v1alpha1.Operation
				if err := e.c.Get(e.ctx, client.ObjectKeyFromObject(op), &got); err != nil || got.DeletionTimestamp == nil {
					t.Fatalf("the Operation is not being deleted: %v, %v", err, got.DeletionTimestamp)
				}
			}
			if err := e.reconcileOp("op-1"); err != nil {
				t.Fatal(err)
			}
			if n := e.asked(); n != tc.want {
				t.Fatalf("%d Estimate calls, want %d", n, tc.want)
			}
			if n := e.log.count(); n != tc.want {
				t.Fatalf("%d applies, want %d", n, tc.want)
			}
			e.wantNoSubmit()
		})
	}
}

// A Provider whose profile or credentials cannot be read (a managed Provider's
// profile is no batch profile) is asked nothing and writes nothing: no entry,
// so the next pass, within the plan's bound, may succeed.
func TestAProviderThatCannotBeReadAsksNothing(t *testing.T) {
	e := newEstEnv(t)
	e.batch.SetEstimate(startE1)
	a1 := e.newAlloc("alloc-1")
	e.newOperation("op-1", "plan-a", v1alpha1.OperationPhasePlanned, allocTarget(a1))
	e.profiles.mu.Lock()
	e.profiles.err = errors.New("the profile is not a batch profile")
	e.profiles.mu.Unlock()
	if err := e.reconcileOp("op-1"); err == nil {
		t.Fatal("no error, want the pass retried")
	}
	if e.asked() != 0 || e.log.count() != 0 {
		t.Fatalf("%d calls and %d applies for a Provider that cannot be read, want none", e.asked(), e.log.count())
	}
	e.profiles.mu.Lock()
	e.profiles.err = nil
	e.profiles.mu.Unlock()
	if err := e.reconcileOp("op-1"); err != nil || e.asked() != 1 {
		t.Fatalf("after the Provider can be read: error %v, %d calls, want the estimate asked", err, e.asked())
	}
}

// An Instance is estimated through the Allocation it runs on.
func TestAnInstanceTargetIsEstimatedThroughItsAllocation(t *testing.T) {
	e := newEstEnv(t)
	e.batch.SetEstimate(startE1)
	a1 := e.newAlloc("alloc-1")
	in := &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: "inst-1", Namespace: testNS, UID: "uid-inst-1"},
		Spec: v1alpha1.InstanceSpec{AllocationRef: v1alpha1.ObjectReference{Name: a1.Name}}}
	if err := e.c.Create(e.ctx, in); err != nil {
		t.Fatal(err)
	}
	target := v1alpha1.PlanTarget{Kind: v1alpha1.TargetKindInstance, Name: in.Name, UID: in.UID, Generation: 1, Provider: testProvider,
		Effect: v1alpha1.PlanEffectDrainInstance, Cost: &v1alpha1.ReleaseCost{}}
	e.newOperation("op-1", "plan-a", v1alpha1.OperationPhasePlanned, target)
	if err := e.reconcileOp("op-1"); err != nil {
		t.Fatal(err)
	}
	got := e.stored("op-1")
	if e.asked() != 1 || len(got) != 1 || !slices.Equal(got[0].Targets, []string{"uid-inst-1"}) {
		t.Fatalf("%d calls, entries %+v, want one estimate naming the Instance's UID, not its Allocation's", e.asked(), got)
	}
}

// A failure is a final unknown entry with a fixed reason, never an absent entry
// and never an error's text; and it ends the asking for that plan.
func TestAFailedEstimateIsAFinalUnknownEntryWithAFixedReason(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"a command that failed", errors.New("sbatch --test-only: exit 1: Invalid account SEEDACCT"), provider.EstimateNoAnswer},
		{"a time-out", fmt.Errorf("sbatch --test-only: %w", slurm.ErrTimeout), provider.EstimateTimedOut},
		{"a refused request", fmt.Errorf("%w: partition is not allowed", provider.ErrInvalidRequest), estimateRefused},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEstEnv(t)
			e.batch.FailEstimate(tc.err)
			a1 := e.newAlloc("alloc-1")
			e.newOperation("op-1", "plan-a", v1alpha1.OperationPhasePlanned, allocTarget(a1))
			if err := e.reconcileOp("op-1"); err != nil {
				t.Fatal(err)
			}
			got := e.stored("op-1")
			if len(got) != 1 || got[0].Start.Time != nil || got[0].Start.Unknown != tc.want || !got[0].Start.Advisory || got[0].Start.ObservedAt == nil {
				t.Fatalf("entries %+v, want one unknown with the reason %q, advisory, with its time", got, tc.want)
			}
			if strings.Contains(got[0].Start.Unknown, "SEEDACCT") {
				t.Fatalf("the reason %q names the account", got[0].Start.Unknown)
			}
			if err := e.reconcileOp("op-1"); err != nil || e.asked() != 1 {
				t.Fatalf("error %v, %d calls: a final unknown ends the asking", err, e.asked())
			}
		})
	}
}

// What a failed estimate's error says (Slurm's own text keeps the account, a node and a job ID) never reaches the log, the
// entry written into status (which every agent reads, and the audit logs at RequestResponse) or the error the pass
// returns: the log names a fixed class of failure, and the status a fixed reason. The values are invented.
func TestAFailedEstimatesTextNeverReachesTheLogTheStatusOrTheReturnedError(t *testing.T) {
	const seedText = "sbatch --test-only: exit 1: sbatch: error: Invalid account SEEDACCT on node n0042 for job 1234567"
	secrets := []string{"SEEDACCT", "n0042", "1234567"}
	for _, tc := range []struct {
		name  string
		err   error
		class string
	}{
		{"a command that failed", errors.New(seedText), "command"},
		{"a time-out", fmt.Errorf("%s: %w", seedText, slurm.ErrTimeout), "timeout"},
		{"a transport error", fmt.Errorf("%s: %w", seedText, slurm.ErrTransport), "transport"},
		{"a refused request", fmt.Errorf("%w: %s", provider.ErrInvalidRequest, seedText), "refused"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEstEnv(t)
			var mu sync.Mutex
			var logged strings.Builder
			sink := funcr.New(func(prefix, args string) {
				mu.Lock()
				defer mu.Unlock()
				logged.WriteString(prefix + " " + args + "\n")
			}, funcr.Options{Verbosity: 4})
			e.ctl.shared.o.Log = logging.NewLogrLogger(sink)
			e.batch.FailEstimate(tc.err)
			a1 := e.newAlloc("alloc-1")
			e.newOperation("op-1", "plan-a", v1alpha1.OperationPhasePlanned, allocTarget(a1))
			reconcileErr := e.reconcileOp("op-1")

			mu.Lock()
			text := logged.String()
			mu.Unlock()
			applied, err := json.Marshal(e.log.last())
			if err != nil {
				t.Fatal(err)
			}
			returned := ""
			if reconcileErr != nil {
				returned = reconcileErr.Error()
			}
			for _, secret := range secrets {
				for where, got := range map[string]string{"the log": text, "the applied entries": string(applied), "the returned error": returned} {
					if strings.Contains(got, secret) {
						t.Errorf("%s names %q:\n%s", where, secret, got)
					}
				}
			}
			// The control, so that the checks can fail: the log does say that it failed and which class, and the
			// entry is written, with one of the fixed reasons.
			if !strings.Contains(text, "the estimate failed") || !strings.Contains(text, tc.class) {
				t.Errorf("the log %q does not say that the estimate failed with class %q", text, tc.class)
			}
			if e.log.count() != 1 || len(e.log.last()) != 1 || e.log.last()[0].Start.Unknown == "" {
				t.Errorf("applies %d, last %+v, want one apply with an unknown entry", e.log.count(), e.log.last())
			}
		})
	}
}

// A Provider whose profile cannot be read fails the pass with a fixed text: the reader's error may name a credentials path or
// an account, and the controller-runtime logs a pass's error.
func TestAProviderThatCannotBeReadFailsThePassWithAFixedText(t *testing.T) {
	e := newEstEnv(t)
	e.batch.SetEstimate(startE1)
	a1 := e.newAlloc("alloc-1")
	e.newOperation("op-1", "plan-a", v1alpha1.OperationPhasePlanned, allocTarget(a1))
	e.profiles.mu.Lock()
	e.profiles.err = errors.New("the credentials file /home/example-user/creds names the account SEEDACCT")
	e.profiles.mu.Unlock()
	err := e.reconcileOp("op-1")
	if err == nil {
		t.Fatal("no error, want the pass retried")
	}
	for _, secret := range []string{"SEEDACCT", "/home/example-user"} {
		if strings.Contains(err.Error(), secret) {
			t.Errorf("the pass's error %q names %q", err, secret)
		}
	}
}

// Eligibility to ask is decided when the estimate STARTS, from a time read once for the step; the answer is then written
// whatever the plan's age has become. An estimate that ran the whole bound ends after the plan's own 60 s have passed, and a
// timeout is an entry with an unknown start (HBEP-4, Estimate): the age is not asked again after the call. The fake clock MOVES
// across the boundary while the estimate runs, which a frozen clock hides.
func TestATimedOutEstimateIsStillWrittenWhenThePlanAgesPastTheBoundWhileItRuns(t *testing.T) {
	e := newEstEnv(t)
	a1 := e.newAlloc("alloc-1")
	e.newOperation("op-1", "plan-a", v1alpha1.OperationPhasePlanned, allocTarget(a1)) // observed at 12:00:00
	e.batch.FailEstimate(fmt.Errorf("sbatch --test-only: %w", slurm.ErrTimeout))
	hold := e.batch.HoldEstimates()

	e.clock.Advance(50 * time.Second) // 12:00:50: the plan is 50 s old, so the estimate starts
	done := make(chan error, 1)
	go func() { done <- e.reconcileOp("op-1") }()
	<-hold.Blocked()
	e.clock.Advance(20 * time.Second) // 12:01:10: the plan is 70 s old when the estimate ends, as one that ran the whole bound is
	hold.Release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if n := e.log.count(); n != 1 {
		t.Fatalf("%d applies, want the timed-out estimate's entry: an estimate that started in time is written, whatever the plan's age became", n)
	}
	got := e.stored("op-1")
	if len(got) != 1 || got[0].Start.Time != nil || got[0].Start.Unknown != provider.EstimateTimedOut {
		t.Fatalf("entries %+v, want one unknown with the time-out reason", got)
	}
	// The control, so that the age check is a real one: a plan that is already older than 60 s when the step starts is not asked.
	e.newOperation("op-2", "plan-b", v1alpha1.OperationPhasePlanned, allocTarget(a1)) // observed at 12:01:10
	e.clock.Advance(61 * time.Second)
	before := e.asked()
	if err := e.reconcileOp("op-2"); err != nil || e.asked() != before {
		t.Fatalf("a plan 61 s old at the step's start: error %v, %d calls, want none", err, e.asked()-before)
	}
}

// A shape whose turn comes after the plan has passed the bound is not started: the age is decided at each estimate's start, so
// the first shape's slow answer is written and the second shape reads as unknown for want of an entry.
func TestAShapeWhoseTurnComesAfterTheBoundIsNotStarted(t *testing.T) {
	e := newEstEnv(t)
	e.batch.SetEstimate(startE1)
	a1 := e.newAlloc("alloc-1")
	a2 := e.newAlloc("alloc-2", func(a *v1alpha1.Allocation) { a.Spec.ForProvider.Batch.GPUs = 2 })
	e.newOperation("op-1", "plan-a", v1alpha1.OperationPhasePlanned, allocTarget(a1), allocTarget(a2))
	hold := e.batch.HoldEstimates()
	e.clock.Advance(50 * time.Second)
	done := make(chan error, 1)
	go func() { done <- e.reconcileOp("op-1") }()
	<-hold.Blocked()
	e.clock.Advance(20 * time.Second) // the first shape's call ends 70 s after the plan
	hold.Release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if n, got := e.asked(), e.stored("op-1"); n != 1 || len(got) != 1 {
		t.Fatalf("%d calls and entries %+v, want the first shape's one, and the second not started", n, got)
	}
}

// An estimate that does not answer within the bound is unknown, with that reason,
// and the controller does not wait past it.
func TestAnEstimateThatHangsIsUnknownAtTheBound(t *testing.T) {
	e := newEstEnv(t)
	e.est.bound = 50 * time.Millisecond
	e.batch.SetEstimate(startE1)
	hold := e.batch.HoldEstimates() // never released
	a1 := e.newAlloc("alloc-1")
	e.newOperation("op-1", "plan-a", v1alpha1.OperationPhasePlanned, allocTarget(a1))
	if err := e.reconcileOp("op-1"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-hold.Blocked():
	default:
		t.Fatal("the estimate never waited at the hold")
	}
	got := e.stored("op-1")
	if len(got) != 1 || got[0].Start.Time != nil || got[0].Start.Unknown != provider.EstimateTimedOut {
		t.Fatalf("entries %+v, want one unknown with the time-out reason, never a time", got)
	}
}

// The default platform (no answer set) is undecided: the entry says so.
func TestAnUndecidedPlatformsEntryNamesTheReason(t *testing.T) {
	e := newEstEnv(t)
	a1 := e.newAlloc("alloc-1")
	e.newOperation("op-1", "plan-a", v1alpha1.OperationPhasePlanned, allocTarget(a1))
	if err := e.reconcileOp("op-1"); err != nil {
		t.Fatal(err)
	}
	got := e.stored("op-1")
	if len(got) != 1 || got[0].Start.Time != nil || got[0].Start.Unknown != provider.EstimateUndecided {
		t.Fatalf("entries %+v, want one unknown with provider.EstimateUndecided", got)
	}
}

// An answer is written only onto the plan it was asked for: a plan replaced while
// the estimate ran gets none of it, and the next pass asks for the new plan.
func TestAnAnswerForAReplacedPlanIsNotWritten(t *testing.T) {
	e := newEstEnv(t)
	e.batch.SetEstimate(startE1)
	hold := e.batch.HoldEstimates()
	a1 := e.newAlloc("alloc-1")
	op := e.newOperation("op-1", "plan-a", v1alpha1.OperationPhasePlanned, allocTarget(a1))
	done := make(chan error, 1)
	go func() { done <- e.reconcileOp("op-1") }()
	<-hold.Blocked()
	var cur v1alpha1.Operation
	if err := e.c.Get(e.ctx, client.ObjectKeyFromObject(op), &cur); err != nil {
		t.Fatal(err)
	}
	cur.Status.Plan.ID = "plan-b"
	if err := e.c.Update(e.ctx, &cur); err != nil {
		t.Fatal(err)
	}
	hold.Release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if n := e.log.count(); n != 0 {
		t.Fatalf("%d applies for an answer to a plan that was replaced, want none", n)
	}
	if err := e.reconcileOp("op-1"); err != nil {
		t.Fatal(err)
	}
	if got := e.stored("op-1"); len(got) != 1 || got[0].PlanID != "plan-b" {
		t.Fatalf("entries %+v, want the new plan's one", got)
	}
}

// An Operation deleted and created again under its name while the estimate ran, with the same plan ID, is another
// Operation: the answer is not written onto it, since the API server checks no UID on an apply.
func TestAnAnswerForARecreatedOperationIsNotWritten(t *testing.T) {
	e := newEstEnv(t)
	e.batch.SetEstimate(startE1)
	hold := e.batch.HoldEstimates()
	a1 := e.newAlloc("alloc-1")
	op := e.newOperation("op-1", "plan-a", v1alpha1.OperationPhasePlanned, allocTarget(a1))
	done := make(chan error, 1)
	go func() { done <- e.reconcileOp("op-1") }()
	<-hold.Blocked()
	if err := e.c.Delete(e.ctx, op); err != nil {
		t.Fatal(err)
	}
	e.newOperationAs("op-1", "uid-op-1-again", "plan-a", v1alpha1.OperationPhasePlanned, allocTarget(a1))
	hold.Release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if n := e.log.count(); n != 0 {
		t.Fatalf("%d applies for an Operation that was deleted and created again, want none", n)
	}
	// The next pass is for the new Operation, and asks for it.
	if err := e.reconcileOp("op-1"); err != nil || e.log.count() != 1 {
		t.Fatalf("the next pass: error %v, %d applies, want the new Operation's estimate", err, e.log.count())
	}
}

func TestTheInFlightSetRefusesASecondRunOfOneEstimate(t *testing.T) {
	e := newEstEnv(t)
	if !e.est.begin("op/plan/shape") || e.est.begin("op/plan/shape") {
		t.Fatal("a second begin of one key was not refused")
	}
	if !e.est.begin("op/plan/another") {
		t.Fatal("another key was refused")
	}
	e.est.end("op/plan/shape")
	if !e.est.begin("op/plan/shape") {
		t.Fatal("a key was refused after its run ended")
	}
}

// Its own writes change neither the phase nor the plan ID, so they raise no new
// pass; a new Operation, and a new phase or plan, do.
func TestTheTriggerIgnoresStatusWritesThatChangeNeitherPhaseNorPlan(t *testing.T) {
	trigger := estimateTrigger()
	op := func(phase v1alpha1.OperationPhase, plan string, estimates int) *v1alpha1.Operation {
		o := &v1alpha1.Operation{Status: v1alpha1.OperationStatus{Phase: phase}}
		if plan != "" {
			o.Status.Plan = &v1alpha1.OperationPlan{ID: plan}
		}
		o.Status.Estimates = make([]v1alpha1.OperationEstimate, estimates)
		return o
	}
	for _, tc := range []struct {
		name     string
		old, new *v1alpha1.Operation
		want     bool
	}{
		{"a new estimate", op("Planned", "p", 0), op("Planned", "p", 1), false},
		{"a new phase", op("Planned", "p", 0), op("AwaitingApproval", "p", 0), true},
		{"a new plan", op("Planned", "p", 1), op("Planned", "q", 1), true},
		{"a first plan", op("", "", 0), op("Planned", "p", 0), true},
	} {
		if got := trigger.Update(event.UpdateEvent{ObjectOld: tc.old, ObjectNew: tc.new}); got != tc.want {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
	if !trigger.Create(event.CreateEvent{Object: op("Planned", "p", 0)}) {
		t.Error("a new Operation does not raise a pass")
	}
}

// IMPL-COMMON: a new controller gets a 200-round stop test. Its options are the
// Allocation controller's, with its own number of workers, and two keys held in
// flight need two of them.
func TestEstimateControllerStopsWithWorkInFlight(t *testing.T) {
	const stopRounds = 200
	for round := 0; round < stopRounds && !t.Failed(); round++ {
		stopWithWorkInFlight(t, round, controllerOptions(estimateWorkers))
	}
}
