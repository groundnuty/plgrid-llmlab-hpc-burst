package lifecycle

// Fixtures of the Operation planner's tests (#589, PR 4). Every time is a literal: the Allocations' hard
// end is T0+4h, their jobs started at T0, and a plan is made at T0+1h, so the lease left is 3h. No
// expectation is computed by the planner's own code.

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

var (
	opT0      = time.Date(2026, 11, 16, 9, 0, 0, 0, time.UTC)
	opHardEnd = opT0.Add(4 * time.Hour)
	opAt      = opT0.Add(time.Hour)
)

const (
	opNS = "ns"
	opMD = "md"
)

func opTime(t time.Time) *metav1.Time { m := metav1.NewTime(t); return &m }

// opFixture is the state a plan reads: one ModelDeployment and its Instances and Allocations, in the
// pass's fields.
type opFixture struct {
	t        *testing.T
	md       *v1alpha1.ModelDeployment
	insts    []*v1alpha1.Instance
	allocs   map[string]*v1alpha1.Allocation
	pvs      map[string]*providerView
	policies []v1alpha1.ApprovalPolicy
}

// newOpFixture is a served ModelDeployment of n Instances (generation 3) and one Provider, sano, whose
// class is H100 on x86_64. Its over-allocation is 0, so no surplus Instance is left alone and a Scale takes
// every one it names; the fixtures of over-allocation's trim set k themselves (overAllocation).
func newOpFixture(t *testing.T, n int32) *opFixture {
	md := &v1alpha1.ModelDeployment{ObjectMeta: metav1.ObjectMeta{Name: opMD, Namespace: opNS, UID: "uid-md", Generation: 3}}
	md.Spec.Instances = n
	md.Spec.OverAllocation = new(int32)
	md.Spec.Source.Served = &v1alpha1.ServingSpec{}
	f := &opFixture{t: t, md: md, allocs: map[string]*v1alpha1.Allocation{}, pvs: map[string]*providerView{}}
	f.addProvider("sano")
	return f
}

func (f *opFixture) addProvider(name string) *providerView {
	pv := &providerView{
		name: name, prov: &v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: name}},
		profile: &v1alpha1.PlatformProfile{}, hasClass: true,
		class: v1alpha1.AcceleratorClass{GPUModel: "H100", CPUArch: "x86_64"},
	}
	f.pvs[name] = pv
	return pv
}

// instOpts describes one Instance and the Allocation it runs on.
type instOpts struct {
	name  string
	phase v1alpha1.InstancePhase
	// started: the Allocation's job started at T0.
	started bool
	// hardEnd is the Allocation's hard end; nil is unknown.
	hardEnd *time.Time
	// created is the Instance's age order: its creation time is T0 plus this.
	created time.Duration
	// pred names the primary this Instance replaces.
	pred      string
	retention *v1alpha1.RetentionPolicy
	provider  string
	// submit is a pending Allocation's submit time; nil is unknown.
	submit *time.Time
	// rankedOut names the candidate that outranked this Instance: it carries the ranked-out mark (rank.go).
	rankedOut string
	// healthySince is when the Instance's Healthy condition last turned True; nil is no Healthy condition.
	healthySince *time.Time
	// hedge names the hedge's first member: this Instance is a later member of that hedge (AnnotationHedgeGroup).
	hedge string
}

func (f *opFixture) addInstance(o instOpts) (*v1alpha1.Instance, *v1alpha1.Allocation) {
	if o.provider == "" {
		o.provider = "sano"
	}
	aname := "alloc-" + o.name
	in := &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{
		Name: o.name, Namespace: opNS, UID: types.UID("uid-" + o.name), Generation: 1,
		CreationTimestamp: metav1.NewTime(opT0.Add(o.created)),
		Labels:            map[string]string{v1alpha1.LabelProvider: o.provider},
	}}
	in.Spec.AllocationRef.Name = aname
	in.Status.Phase = o.phase
	if o.started && o.hardEnd != nil {
		in.Status.LeaseEnd = opTime(*o.hardEnd) // the copy of the hard end that the scale-down ranks ready Instances by
	}
	if o.pred != "" {
		in.Annotations = map[string]string{v1alpha1.AnnotationPredecessor: o.pred}
	}
	if o.rankedOut != "" {
		if in.Annotations == nil {
			in.Annotations = map[string]string{}
		}
		in.Annotations[annotationRankedOut] = o.rankedOut
	}
	if o.hedge != "" {
		if in.Annotations == nil {
			in.Annotations = map[string]string{}
		}
		in.Annotations[v1alpha1.AnnotationHedgeGroup] = o.hedge
	}
	if o.healthySince != nil {
		in.Status.Conditions = append(in.Status.Conditions, metav1.Condition{
			Type: v1alpha1.ConditionHealthy, Status: metav1.ConditionTrue, LastTransitionTime: metav1.NewTime(*o.healthySince),
		})
	}
	a := f.newAllocation(aname, o)
	f.insts = append(f.insts, in)
	f.allocs[aname] = a
	return in, a
}

func (f *opFixture) newAllocation(name string, o instOpts) *v1alpha1.Allocation {
	if o.provider == "" {
		o.provider = "sano"
	}
	a := &v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: opNS, UID: types.UID("uid-" + name), Generation: 1,
		Labels: map[string]string{v1alpha1.LabelProvider: o.provider, v1alpha1.LabelModelDeployment: opMD},
	}}
	a.Spec.ModelDeploymentRef = &v1alpha1.ObjectReference{Name: opMD}
	a.Spec.RetentionPolicy = o.retention
	a.Spec.ForProvider.Batch = &v1alpha1.BatchParameters{GPUs: 4}
	if o.started {
		a.Status.AtProvider.Phase = v1alpha1.AllocationRunning
		a.Status.AtProvider.StartTime = opTime(opT0)
		a.Status.AtProvider.HardEnd = optTimePtr(o.hardEnd)
	} else {
		a.Status.AtProvider.Phase = v1alpha1.AllocationPending
		a.Status.AtProvider.SubmitTime = optTimePtr(o.submit)
	}
	return a
}

func optTimePtr(t *time.Time) *metav1.Time {
	if t == nil {
		return nil
	}
	return opTime(*t)
}

func tp(t time.Time) *time.Time { return &t }

// emptyAllocation adds a started Allocation that hosts no Instance (a Release's candidate).
func (f *opFixture) emptyAllocation(name string, retention *v1alpha1.RetentionPolicy) *v1alpha1.Allocation {
	a := f.newAllocation(name, instOpts{started: true, hardEnd: tp(opHardEnd), retention: retention})
	f.allocs[name] = a
	return a
}

// overAllocation sets k, the deployment's extra Allocations per Provider.
func (f *opFixture) overAllocation(k int32) { f.md.Spec.OverAllocation = &k }

// pass is the pass the planner reads, at T0+1h: the pass's clock decides when a marked peer's drain is due.
func (f *opFixture) pass() *pass {
	return &pass{md: f.md, key: types.NamespacedName{Namespace: opNS, Name: opMD}, insts: f.insts, allocs: f.allocs, pvByName: f.pvs, now: opAt}
}

func (f *opFixture) request(op *v1alpha1.Operation, at time.Time) planRequest {
	req := planRequest{op: op, at: at, policies: f.policies}
	for _, name := range orderedKeys(f.allocs) {
		req.allocs = append(req.allocs, f.allocs[name])
	}
	return req
}

// plan plans op at T0+1h.
func (f *opFixture) plan(op *v1alpha1.Operation) (*v1alpha1.OperationPlan, string) {
	return f.pass().planOperation(f.request(op, opAt))
}

func scaleOp(n int32) *v1alpha1.Operation {
	return &v1alpha1.Operation{
		ObjectMeta: metav1.ObjectMeta{Name: "op", Namespace: opNS, UID: "uid-op", Generation: 1},
		Spec: v1alpha1.OperationSpec{
			Action: v1alpha1.OperationActionScale, ModelDeploymentRef: v1alpha1.ObjectReference{Name: opMD},
			Scale: &v1alpha1.ScaleParameters{Instances: n}, Owner: "agent-a",
		},
	}
}

func endOp(del bool) *v1alpha1.Operation {
	op := scaleOp(0)
	op.Spec.Action, op.Spec.Scale = v1alpha1.OperationActionEnd, nil
	if del {
		op.Spec.End = &v1alpha1.EndParameters{Delete: true}
	}
	return op
}

func releaseOp(r v1alpha1.ReleaseParameters) *v1alpha1.Operation {
	op := scaleOp(0)
	op.Spec.Action, op.Spec.Scale, op.Spec.Release = v1alpha1.OperationActionRelease, nil, &r
	return op
}

// wantTarget is one target of an expected plan, written out by hand.
type wantTarget struct {
	kind   v1alpha1.TargetKind
	name   string
	effect v1alpha1.PlanEffect
	needs  bool
	rule   string
	// cost: the target lowers capacity, so it carries a release cost.
	cost bool
}

func ptrTime(t time.Time) metav1.Time { return metav1.NewTime(t) }
