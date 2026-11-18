package lifecycle

// The reservation's one ordered snapshot (slot_snapshot.go; #751's delta review, D1: points 1 and 4). Every view a reservation
// reads under a Provider's lock comes from ONE list of the Allocations of every namespace, then ONE of the Instances, so an
// Allocation the first shows has its Instance in the second and an Instance without an Allocation in the first is in flight.
// Each row's hook sits at a boundary of that snapshot and its count is worked out by hand: provider-a, one GPU per Allocation,
// a 4 h time limit and a drain window of 2 h 10 min, so an Instance created at 08:00:10 hands over from 09:50:10 and a
// deployment that ends at 09:30 never does.

import (
	"context"
	"sync"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	pfake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// runningAfterFirstAllocationList is a Reader whose first list of Allocations is true to the store and whose later ones show
// the named Allocation Running: it turns Running right after the first Allocation list.
type runningAfterFirstAllocationList struct {
	client.Reader
	name string
	mu   sync.Mutex
	n    int
}

func (l *runningAfterFirstAllocationList) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	err := l.Reader.List(ctx, list, opts...)
	if al, ok := list.(*v1alpha1.AllocationList); ok && err == nil {
		l.mu.Lock()
		l.n++
		later := l.n > 1
		l.mu.Unlock()
		if later {
			for i := range al.Items {
				if al.Items[i].Name == l.name {
					s, e := metav1.NewTime(hoT(8, 0, 20)), metav1.NewTime(hoT(12, 0, 20))
					al.Items[i].Status.AtProvider.Phase = v1alpha1.AllocationRunning
					al.Items[i].Status.AtProvider.StartTime, al.Items[i].Status.AtProvider.HardEnd = &s, &e
				}
			}
		}
	}
	return err
}

// TestAPrimaryTurningRunningAfterTheAllocationListIsCountedOnce (point 1): limit 3 on provider-a. md-e (ends at 09:30: no handover
// of its own) has its primary pe, Pending and listed. md-o (no end time) has its primary po, Pending and listed, which turns
// Running right after the first Allocation list. By hand, from that one list: listed pe 1 + po 1, po's handover counted once, as
// the future slot of a primary that has not started, 1, and the extra 1 = 4 > 3: refused; admitted at 4. The two lists the
// reserved slots once made (the Provider's, which showed po Pending and so reserved nothing, and the deployment's by label,
// which showed it Running and so skipped it) left po's handover in neither: 3 <= 3, admitted.
func TestAPrimaryTurningRunningAfterTheAllocationListIsCountedOnce(t *testing.T) {
	end := hoT(9, 30, 0)
	f := &unitFx{}
	f.md("md-e", &end)
	f.md("md-o", nil)
	f.inst("md-e", "pe", provA, "uid-pe", hoT(8, 0, 10), v1alpha1.InstancePending, nil)
	f.alloc("md-e", "pe", provA, false)
	f.inst("md-o", "po", provA, "uid-po", hoT(8, 0, 10), v1alpha1.InstancePending, nil)
	f.alloc("md-o", "po", provA, false)
	for _, c := range []struct {
		limit int32
		ok    bool
	}{{3, false}, {4, true}} {
		r := &runningAfterFirstAllocationList{Reader: f.reader(t), name: "alloc-po"}
		if a := ask(t, newSlotTable(), r, extraSlotAsk(provA, "alloc-x", i32(c.limit), nil, rigNS+"/md-e", "")); a.ok != c.ok {
			t.Fatalf("limit %d: extra ok %v with a count of %d, want ok %v: po's handover is counted once, from the snapshot", c.limit, a.ok, a.count, c.ok)
		}
	}
}

// afterFirstList is a Reader that runs fire once, right after its first List of any kind returns.
type afterFirstList struct {
	client.Reader
	once sync.Once
	fire func()
}

func (a *afterFirstList) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	err := a.Reader.List(ctx, list, opts...)
	a.once.Do(a.fire)
	return err
}

// TestAnInstanceAndItsAllocationMadeAfterTheFirstListAreCountedInFlight (point 1, the order): limit 3 on provider-a. md-e (ends at
// 09:30) has its primary pe, Pending and listed. md-o (no end time) has no Instance when the reads begin: its primary po and po's
// Allocation are made right after the FIRST list the reservation makes, which the snapshot's order makes the Allocations'.
// po's reservation is in the table (an expectation, 1). The Instance list then shows po without an Allocation in the first
// list: in flight, so its handover counts. By hand: expected po 1 + listed pe 1 + po's handover 1 + the extra 1 = 4 > 3, refused;
// admitted at 4. With the Instances read first it is the Allocation list that shows po's Allocation (listed, the expectation
// is spent) and the Instance list that does not show po: 1 + 1 + the extra 1 = 3 <= 3, admitted, po's handover in neither.
func TestAnInstanceAndItsAllocationMadeAfterTheFirstListAreCountedInFlight(t *testing.T) {
	end := hoT(9, 30, 0)
	for _, c := range []struct {
		limit int32
		ok    bool
	}{{3, false}, {4, true}} {
		f := &unitFx{}
		f.md("md-e", &end)
		f.md("md-o", nil)
		f.inst("md-e", "pe", provA, "uid-pe", hoT(8, 0, 10), v1alpha1.InstancePending, nil)
		f.alloc("md-e", "pe", provA, false)
		c0 := f.client(t)
		later := &unitFx{}
		later.inst("md-o", "po", provA, "uid-po", hoT(8, 0, 10), v1alpha1.InstancePending, nil)
		later.alloc("md-o", "po", provA, false)
		tab := newSlotTable()
		// po's reservation, made before its Allocation: the expectation the extra's count reads.
		if a := ask(t, tab, c0, slotAsk{provider: provA, key: rigNS + "/alloc-po", limit: i32(c.limit)}); !a.ok {
			t.Fatalf("po's reservation refused with a count of %d", a.count)
		}
		r := &afterFirstList{Reader: c0, fire: func() {
			for _, o := range later.objs {
				if err := c0.Create(context.Background(), o); err != nil {
					t.Error(err)
				}
			}
		}}
		if a := ask(t, tab, r, extraSlotAsk(provA, "alloc-x", i32(c.limit), nil, rigNS+"/md-e", "")); a.ok != c.ok {
			t.Fatalf("limit %d: extra ok %v with a count of %d, want ok %v", c.limit, a.ok, a.count, c.ok)
		}
	}
}

// TestTheHeadroomReadsTheSnapshotNotAnotherListOfTheDeploymentsInstances (point 4): limit 3 on provider-a. md-e (ends at 09:30) has
// its primary pe, listed. md-o allows provider-a, wants one Instance and has none when the snapshot is read; its first primary
// po and po's Allocation are made right after (as the headroom's own reads begin), and po's reservation is in the table (an
// expectation, 1). The headroom counts md-o's missing primary from the snapshot, where it has no Instance: H = 1, so the
// extra's count is expected po 1 + listed pe 1 + H 1 + the extra 1 = 4 > 3: refused, and admitted at 4. A headroom that
// listed md-o's Instances and Allocations again saw po with its Allocation, reported no missing primary and counted
// 1 + 1 + 0 + 1 = 3 <= 3: admitted, po's handover in neither.
func TestTheHeadroomReadsTheSnapshotNotAnotherListOfTheDeploymentsInstances(t *testing.T) {
	end := hoT(9, 30, 0)
	for _, c := range []struct {
		limit int32
		ok    bool
	}{{3, false}, {4, true}} {
		f := &unitFx{}
		f.md("md-e", &end)
		mo := f.md("md-o", nil)
		mo.Spec.Providers.Names = []string{provA}
		f.inst("md-e", "pe", provA, "uid-pe", hoT(8, 0, 10), v1alpha1.InstancePending, nil)
		f.alloc("md-e", "pe", provA, false)
		c0 := f.client(t)
		later := &unitFx{}
		later.inst("md-o", "po", provA, "uid-po", hoT(8, 0, 10), v1alpha1.InstancePending, nil)
		later.alloc("md-o", "po", provA, false)
		tab := newSlotTable()
		if a := ask(t, tab, c0, slotAsk{provider: provA, key: rigNS + "/alloc-po", limit: i32(c.limit)}); !a.ok {
			t.Fatalf("po's reservation refused with a count of %d", a.count)
		}
		// The hook fires after the snapshot's two lists: the third list of the reservation is the headroom's first read.
		var mu sync.Mutex
		n := 0
		r := &listsThen{Reader: c0, after: 2, fire: func() {
			mu.Lock()
			defer mu.Unlock()
			n++
			for _, o := range later.objs {
				if err := c0.Create(context.Background(), o); err != nil {
					t.Error(err)
				}
			}
		}}
		pas := &pass{r: &Reconciler{Client: c0, Reader: r, Clock: pfake.NewClock(hoT(8, 0, 30))}, key: types.NamespacedName{Namespace: rigNS, Name: "md-e"}, now: hoT(8, 0, 30)}
		q := extraSlotAsk(provA, "alloc-x", i32(c.limit), nil, rigNS+"/md-e", "")
		q.headroom = pas.headroomOf(&providerView{name: provA})
		if a := ask(t, tab, r, q); a.ok != c.ok {
			t.Fatalf("limit %d: extra ok %v with a count of %d, want ok %v", c.limit, a.ok, a.count, c.ok)
		}
		if n != 1 {
			t.Fatalf("the hook fired %d times, want once", n)
		}
	}
}

// listsThen is a Reader that runs fire once, right after its after-th List of any kind returns.
type listsThen struct {
	client.Reader
	mu    sync.Mutex
	n     int
	after int
	fire  func()
}

func (l *listsThen) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	err := l.Reader.List(ctx, list, opts...)
	l.mu.Lock()
	l.n++
	hit := l.n == l.after
	l.mu.Unlock()
	if hit {
		l.fire()
	}
	return err
}
