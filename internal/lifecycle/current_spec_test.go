package lifecycle

// An EXISTING unit is charged at its own recorded size; a FUTURE replacement or successor at the deployment's CURRENT spec
// (the ruling on #756's review; HBEP-4, QOS limits, "charged to the QOS it would submit under"). Every count below is
// worked by hand: a deployment that launched 1 GPU (gpu1) and was then raised to tensor-parallel-size 4 (gpu4).

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// TestAReplacementsReservedSlotIsChargedAtTheCurrentSpec (Astra 2, Carbon C2):
//   - a Running 1-GPU primary P, whose replacement will be sized at the deployment's current 4 GPUs: its slot is 4 GPUs
//     (gpu4's), not P's own launch of 1;
//   - a drained carrier C of 1 GPU, whose successor is made at the current spec: 4 GPUs;
//   - a pending replacement Instance with no Allocation yet, an existing unit with its launch immutable: its own slot
//     stays at its own 1 GPU;
//   - a deployment whose spec cannot be read (no served source) leaves the slot at the launch's 1 GPU.
func TestAReplacementsReservedSlotIsChargedAtTheCurrentSpec(t *testing.T) {
	md := withFlags(mdSpec(hoMD), v1alpha1.EngineFlag{Name: "tensor-parallel-size", Value: "4"})
	md.Namespace = rigNS
	unreadable := &v1alpha1.ModelDeployment{ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: hoMD}}
	unreadable.Spec.Instances = 1 // wants an Instance, so its primary is no surplus; no served source, so its spec yields no plan
	profile := &v1alpha1.PlatformProfileSpec{DrainWindow: dur(2*time.Hour + 10*time.Minute), DrainMargin: dur(10 * time.Minute)}
	oneGPU := func(in v1alpha1.Instance) v1alpha1.Instance { in.Spec.Launch.GPUs = 1; return in }
	for _, tc := range []struct {
		name   string
		md     *v1alpha1.ModelDeployment
		insts  []v1alpha1.Instance
		allocs []string // Running Allocations, by Instance name
		kind   slotKind
		want   int32
	}{
		{"a Running primary's replacement slot", md,
			[]v1alpha1.Instance{oneGPU(crossInstance("p", provA, v1alpha1.InstanceReady, nil))}, []string{"p"}, slotForReplacement, 4},
		{"a drained carrier's successor slot", md,
			[]v1alpha1.Instance{oneGPU(crossInstance("c", provA, v1alpha1.InstanceEnded, map[string]string{annotationNoOverlap: "the one slot", annotationDrainStarted: hoT(9, 50, 20).Format(time.RFC3339)}))}, []string{"c"}, slotForSuccessor, 4},
		{"a pending replacement's own slot, an existing unit", md,
			[]v1alpha1.Instance{oneGPU(crossInstance("r", provA, v1alpha1.InstancePending, map[string]string{v1alpha1.AnnotationPredecessor: "p0"}))}, nil, slotForOwnAllocation, 1},
		{"a deployment whose spec cannot be read", unreadable,
			[]v1alpha1.Instance{oneGPU(crossInstance("p", provA, v1alpha1.InstanceReady, nil))}, []string{"p"}, slotForReplacement, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			allocs := map[string]*v1alpha1.Allocation{}
			for _, n := range tc.allocs {
				k, a := runningAlloc(n)
				allocs[k] = a
			}
			slots, err := reservedOn(context.Background(), fixedInstances{insts: tc.insts, md: tc.md}, provA, profile, allocs)
			if err != nil {
				t.Fatal(err)
			}
			var got []reservedSlot
			for _, s := range slots {
				if s.kind == tc.kind {
					got = append(got, s)
				}
			}
			if len(got) != 1 || got[0].gpus != tc.want {
				t.Fatalf("the %v slots are %+v, want one of %d GPUs", tc.kind, got, tc.want)
			}
		})
	}
}

// TestAnExtrasHeadroomSizesAPendingUnitAtItsOwnLaunch (Astra 6): another deployment o1 launched 1 GPU, and its Instance has no
// Allocation yet; o1 is now tensor-parallel-size 4. Its missing primary, an existing unit, will be sized at ITS OWN launch
// when its Allocation is made, so it counts under gpu1, not gpu4. o4 is a deployment of 4 GPUs with no unit made at all:
// a unit not yet made is sized at the current spec, gpu4.
//   - over gpu1: H = 1 (o1); over gpu4: H = 1 (o4); over gpu2: 0; Provider-wide: 2.
func TestAnExtrasHeadroomSizesAPendingUnitAtItsOwnLaunch(t *testing.T) {
	f := &headroomFixture{}
	f.md("this", 1, []string{"prov-x"}, nil)
	f.objs = append(f.objs, &v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: "prov-x"}})
	f.md("o1", 1, []string{"prov-x"}, func(md *v1alpha1.ModelDeployment) { withFlags(md, tpFlag("4")) })
	f.md("o4", 1, []string{"prov-x"}, func(md *v1alpha1.ModelDeployment) { withFlags(md, tpFlag("4")) })
	f.served("o1", false) // an Instance, no Allocation
	for _, o := range f.objs {
		if in, ok := o.(*v1alpha1.Instance); ok {
			in.Spec.Launch.GPUs = 1
			in.CreationTimestamp = metav1.NewTime(hoT(7, 0, 0))
		}
	}
	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(f.objs...).Build()
	p := &pass{r: &Reconciler{Client: c, Reader: c, Clock: pfake.NewClock(hoT(8, 0, 0))}, key: types.NamespacedName{Namespace: "ns", Name: "this"}, now: hoT(8, 0, 0)}
	pv := sanoView(sanoQOS(), false)
	pv.name = "prov-x"
	for _, tc := range []struct {
		name string
		gpus int32 // 0: Provider-wide
		want int
	}{{"Provider-wide", 0, 2}, {"under gpu1", 1, 1}, {"under gpu4", 4, 1}, {"under gpu2", 2, 0}} {
		var fn func(context.Context) (int, error)
		if tc.gpus == 0 {
			fn = p.headroomOf(pv)
		} else {
			rv, err := pv.forGPUs(tc.gpus)
			if err != nil {
				t.Fatal(err)
			}
			fn = p.headroomWhere(pv, rv.scope)
		}
		got, err := fn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Errorf("headroom %s = %d, want %d", tc.name, got, tc.want)
		}
	}
}

// editQOS rewrites one QOS of the rig's PlatformProfile.
func (rg *rig) editQOS(name string, f func(*v1alpha1.QOSProfile)) {
	rg.t.Helper()
	var prof v1alpha1.PlatformProfile
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Name: rigProfile}, &prof); err != nil {
		rg.t.Fatal(err)
	}
	for i := range prof.Spec.QOS {
		if prof.Spec.QOS[i].Name == name {
			f(&prof.Spec.QOS[i])
			if err := rg.c.Update(rg.ctx, &prof); err != nil {
				rg.t.Fatal(err)
			}
			return
		}
	}
	rg.t.Fatalf("PlatformProfile %s has no QOS %s", rigProfile, name)
}

// TestAPendingInstancesLeaseIsCheckedAtItsOwnLaunch (Astra 7): a 1-GPU Instance was made and its Allocation's create was cut
// short. Then gpu1's wall fell to 2 h, under the lead an Instance needs (drain window 2 h 10 min + handover margin 30 min +
// start timeout 1 h = 3 h 40 min), and the deployment became tensor-parallel-size 4, whose gpu4 wall is 72 h. The retry sizes
// the Allocation at the Instance's OWN launch, so under gpu1's 2 h: too short to serve. It is refused, and no Allocation is
// made, where the plan's check, which reads gpu4's 72 h, let it through. The row uses a profile copy: Sano's four walls are
// all 72 h, so the case is latent there.
func TestAPendingInstancesLeaseIsCheckedAtItsOwnLaunch(t *testing.T) {
	cut := &allocCutter{}
	rg := newRig(t, append(sanoRigOpts(nil), withInterceptors(cut.funcs(interceptor.Funcs{})))...)
	cut.arm("md", 0)
	rg.create(mdSpec("md"))
	rg.clock.Advance(10 * time.Second)
	stepTolerantAll(rg)
	if n, m := len(rg.instances("md")), len(rg.allocations("md")); n != 1 || m != 0 {
		t.Fatalf("%d Instances and %d Allocations after the cut create, want 1 and 0", n, m)
	}
	cut.disarm()
	rg.editQOS("gpu1", func(q *v1alpha1.QOSProfile) { q.MaxWall = dur(2 * time.Hour) })
	rg.mutateMD("md", func(m *v1alpha1.ModelDeployment) { withFlags(m, tpFlag("4")) })
	for range 3 {
		rg.clock.Advance(10 * time.Second)
		stepTolerantAll(rg)
	}
	// The deployment's new 4-GPU Instance gets its own Allocation under gpu4; the old 1-GPU one must get none.
	for _, a := range rg.allocations("md") {
		if b := a.Spec.ForProvider.Batch; b.GPUs == 1 {
			t.Errorf("a 1-GPU Allocation %s was made with time limit %s: gpu1's 2 h lease cannot serve", a.Name, b.TimeLimit)
		}
	}
}
