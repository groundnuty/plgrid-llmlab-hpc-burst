package lifecycle

// The extra's unit set through full passes (extra_units.go; #751's fix round): the sizes it counts come from the CURRENT spec,
// and a handover is counted once however the passes' reads line up.

import (
	"context"
	"sync"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// TestTheExtrasCapsCountAReplacementAtTheSizeItsLaunchNeedsNow (A2): a per-user cap of 100 CPUs, no other limit, 1 GPU per
// Allocation, the partition at 16 CPUs per GPU. md-o (k = 0, no end time) has a Running primary po on 16 CPUs, which will
// hand over. The operator raises the partition's cpusPerGpu to 32, then md-e (k = 1, ends at 14:00, so it hands nothing
// over) is created: its primary pe is made at 32 CPUs, and its extra would be the second. By hand, listed po 16 + pe 32 + the
// extra 32 = 80, and po's replacement, which a launch would size at 32 now: 112 > 100, so NO extra. Counted at po's own
// Allocation's 16, as the old reader did, it is 96 <= 100 and the extra is made.
func TestTheExtrasCapsCountAReplacementAtTheSizeItsLaunchNeedsNow(t *testing.T) {
	rg := newRig(t, append(qosOpts([]v1alpha1.QOSProfile{{Name: "example-cpu", MaxCPUsPerUser: i32(100)}}, "", "example-cpu"), withMaxTime(12*time.Hour))...)
	rg.create(mdSpec("md-o"))
	for range 7 {
		tick10(rg) // 08:01:10: po is ready on 16 CPUs
	}
	if as := rg.allocations("md-o"); len(as) != 1 || as[0].Spec.ForProvider.Batch.CPUsPerTask != 16 || as[0].Status.AtProvider.Phase != v1alpha1.AllocationRunning {
		t.Fatalf("md-o's Allocations %+v, want po Running on 16 CPUs", as)
	}
	var prof v1alpha1.PlatformProfile
	if err := rg.c.Get(rg.ctx, client.ObjectKey{Name: rigProfile}, &prof); err != nil {
		t.Fatal(err)
	}
	prof.Spec.Partitions[0].CPUsPerGPU = ptrTo(int32(32))
	if err := rg.c.Update(rg.ctx, &prof); err != nil {
		t.Fatal(err)
	}
	rg.create(extraMD("md-e"))
	tick10(rg) // 08:01:20: md-e's primary, and its extra if admitted
	tick10(rg)
	as := rg.allocations("md-e")
	if len(as) != 1 || as[0].Spec.ForProvider.Batch.CPUsPerTask != 32 {
		t.Fatalf("md-e's Allocations %+v, want its primary alone, on 32 CPUs: po's replacement needs 32 now (80 + 32 = 112 > 100)", as)
	}
	noSlotReasonOn(t, rg, "md-e")
}

// phaseLag is a Reader whose FIRST list of Allocations by label, which is the pass's own read at its start, shows one
// Allocation Pending, and whose later reads show it as it is, Running: the Allocation turned Running between the pass's read
// and the locked one.
type phaseLag struct {
	client.Reader
	alloc string
	mu    sync.Mutex
	n     int
}

func (l *phaseLag) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	err := l.Reader.List(ctx, list, opts...)
	if al, ok := list.(*v1alpha1.AllocationList); ok {
		lo := &client.ListOptions{}
		for _, o := range opts {
			o.ApplyToList(lo)
		}
		if lo.LabelSelector != nil {
			l.mu.Lock()
			first := l.n == 0
			l.n++
			l.mu.Unlock()
			if first {
				for i := range al.Items {
					if al.Items[i].Name == l.alloc {
						al.Items[i].Status.AtProvider.Phase = v1alpha1.AllocationPending
					}
				}
			}
		}
	}
	return err
}

// TestAnOwnPrimaryTurningRunningBeforeTheLockedReadIsCountedOnce (A3): a GPU cap of 3, md (k = 1, no end time). At 08:00:10 its
// primary P and its extra X are made (1 + 1 + P's replacement 1 = 3 <= 3, admitted), and X's Allocation's create is cut short.
// P is Running at 08:00:20. X's retry runs in a pass whose own read still shows P Pending, as a cache that has not caught up
// does, while the locked read shows it Running: P's replacement is the ONE handover left room for, so 1 + 1 + 1 = 3 <= 3 and
// X's Allocation is made. The old reader counted it twice, once from the pass and once from the lock: 4 > 3, X refused.
func TestAnOwnPrimaryTurningRunningBeforeTheLockedReadIsCountedOnce(t *testing.T) {
	log := newCauseLog()
	cut := &allocCutter{}
	rg := newRig(t, append(gpuCapOpts(3), withInterceptors(cut.funcs(log.funcs())))...)
	cut.arm(hoMD, 1) // P's Allocation is made, X's is cut short
	rg.create(withK(mdSpec(hoMD), ptrTo(int32(1))))
	rg.clock.Advance(10 * time.Second)
	stepTolerantAll(rg) // 08:00:10
	if cut.failures() != 1 || len(rg.instances(hoMD)) != 2 || len(rg.allocations(hoMD)) != 1 {
		t.Fatalf("%d cut creates, %d Instances, %d Allocations, want 1, 2 and 1", cut.failures(), len(rg.instances(hoMD)), len(rg.allocations(hoMD)))
	}
	pAlloc := rg.allocations(hoMD)[0].Name
	var xAlloc string
	for _, in := range rg.instances(hoMD) {
		if in.Spec.AllocationRef.Name != pAlloc {
			xAlloc = in.Spec.AllocationRef.Name
		}
	}
	for range 3 { // 08:00:20 to 08:00:40, X's retries cut short, until P's job runs
		rg.clock.Advance(10 * time.Second)
		stepTolerantAll(rg)
	}
	if a, ok := rg.allocation(pAlloc); !ok || a.Status.AtProvider.Phase != v1alpha1.AllocationRunning {
		t.Fatalf("P's Allocation %+v, want Running", a)
	}
	cut.disarm()
	rg.restartController()
	rg.r.Reader = &phaseLag{Reader: rg.c, alloc: pAlloc}
	rg.clock.Advance(10 * time.Second)
	rg.Step()
	if _, made := rg.allocation(xAlloc); !made {
		t.Fatal("X's Allocation was not made: P's replacement was counted twice (once from the pass's read, once from the lock's)")
	}
}
