package lifecycle

// Forced interleavings for the slot table (HBEP-7, Provisioning, Slots are
// reserved atomically): a reservation is stamped when it is raised, the
// Provider's lock is held across the list, and that list is bounded. Each test
// forces its interleaving with state, never with a window: a hook runs at the
// exact point (the identity resolution, inside the List), and the assertions
// follow it.

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
)

// slowSource is a CheckpointSource whose first Files call runs a hook before it
// answers: a slow identity resolution, which a pass makes before it reserves a
// slot.
type slowSource struct {
	CheckpointSource
	once sync.Once
	hook func()
}

func (s *slowSource) Files(ctx context.Context, c v1alpha1.CheckpointSpec) ([]deploymentid.File, error) {
	s.once.Do(s.hook)
	return s.CheckpointSource.Files(ctx, c)
}

// TestSlotsReservationIsStampedWhenRaised: md-a's pass starts at 08:00:00 and
// its identity resolution takes 6 minutes (the clock moves by 6 minutes inside
// the Hub call), longer than the 5 minutes an expectation lives. It reserves
// the Provider's only slot at 08:06:00 and creates its Allocation. The cache
// does not show the creation. md-b's pass at 08:06:00 must find the slot held:
// the expectation is 0 minutes old. Stamped with the pass's start, 08:00:00,
// the expectation would be 6 minutes old, lapse at md-b's count and give the
// same slot to md-b (review of PR 171, Astra B1).
func TestSlotsReservationIsStampedWhenRaised(t *testing.T) {
	rg := newRig(t, oneSlotOpts()...)
	rg.Step() // the Provider's first status: Ready
	rg.create(mdSpec("md-a"))
	rg.create(mdSpec("md-b"))
	rg.r.Reader = newStaleReader(t, rg.c)
	hooked := false
	rg.r.Checkpoints = &slowSource{CheckpointSource: rg.source, hook: func() {
		hooked = true
		rg.clock.Advance(6 * time.Minute)
	}}
	reconcile := func(md string) {
		t.Helper()
		if _, err := rg.r.Reconcile(rg.ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rigNS, Name: md}}); err != nil {
			t.Fatalf("reconcile %s: %v", md, err)
		}
	}
	reconcile("md-a")
	if !hooked {
		t.Fatal("md-a's identity resolution never ran the slow hook; the test proves nothing")
	}
	if got, want := rg.clock.Now(), rigStart.Add(6*time.Minute); !got.Equal(want) {
		t.Fatalf("the clock reads %s after md-a's pass, want %s", got, want)
	}
	if n := len(rg.allocations("md-a")); n != 1 {
		t.Fatalf("md-a has %d Allocations, want 1", n)
	}
	if err := rg.prov.Sync(rg.ctx); err != nil { // the Provider is still Ready at 08:06:00
		t.Fatal(err)
	}
	reconcile("md-b")
	if n := len(rg.allAllocations()); n != 1 {
		t.Fatalf("%d Allocations on a Provider with a submit limit of 1, want 1: md-a's reservation lapsed at once", n)
	}
	c := rg.condition("md-b", v1alpha1.ConditionDegraded)
	if c.Status != metav1.ConditionTrue || c.Reason != v1alpha1.ReasonSubmitLimitReached || !strings.Contains(c.Message, "maxSubmitPerUser 1 of QOS example-now") {
		t.Fatalf("md-b Degraded = %s/%s %q, want SubmitLimitReached naming the limit", c.Status, c.Reason, c.Message)
	}
}

// listProbe is a client.Reader that runs a hook inside every Allocation List,
// where a reservation holds its Provider's lock.
type listProbe struct {
	client.Reader
	inList func(ctx context.Context) error
}

func (p *listProbe) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*v1alpha1.AllocationList); ok && p.inList != nil {
		if err := p.inList(ctx); err != nil {
			return err
		}
	}
	return p.Reader.List(ctx, list, opts...)
}

// TestSlotsProviderLockIsHeldAcrossTheList: the lock a worker takes to reserve
// is held while it lists, so a list taken before another worker's creation is
// never applied after a newer list lowered that creation's expectation (HBEP-7,
// "under a lock per Provider, which every worker takes"). Inside the List, the
// Provider's lock cannot be taken, and another Provider's can. Without the lock
// a worker whose list lacks a just-created Allocation counts it as free and
// double-books the slot; the interleaving is: A's list is taken without X, X is
// created, B lists, sees X and lowers X's expectation, then A applies its old
// list.
func TestSlotsProviderLockIsHeldAcrossTheList(t *testing.T) {
	rg := newRig(t)
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		call func(tab *slotTable, r client.Reader) error
	}{
		{"reserve", func(tab *slotTable, r client.Reader) error {
			_, _, err := tab.reserve(ctx, r, rigProvider, rigNS+"/a", i32(1), fixedAt(rigStart))
			return err
		}},
		{"count", func(tab *slotTable, r client.Reader) error {
			_, err := tab.count(ctx, r, rigProvider, rigStart)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tab := newSlotTable()
			ran, ownHeld, otherFree := false, false, false
			probe := &listProbe{Reader: rg.c, inList: func(context.Context) error {
				ran = true
				if l := tab.slotLock(rigProvider); l.TryLock() {
					l.Unlock()
				} else {
					ownHeld = true
				}
				if l := tab.slotLock("example-other-provider"); l.TryLock() {
					l.Unlock()
					otherFree = true
				}
				return nil
			}}
			if err := tc.call(tab, probe); err != nil {
				t.Fatal(err)
			}
			if !ran {
				t.Fatal("the probe never ran inside a List; the test proves nothing")
			}
			if !ownHeld {
				t.Fatal("the Provider's lock was free inside the List: a worker's list can be applied after a newer one")
			}
			if !otherFree {
				t.Fatal("another Provider's lock was held inside the List: the locks are per Provider")
			}
			if l := tab.slotLock(rigProvider); !l.TryLock() {
				t.Fatal("the Provider's lock is still held after the call returned")
			} else {
				l.Unlock()
			}
		})
	}
}

// TestSlotsListUnderTheLockIsBounded: a List that never answers gives up at the
// table's timeout, with an error naming the Provider, and the lock goes with it,
// so one hung API call does not hold every worker that reserves on that
// Provider (review of PR 171, Opus n8). The test waits on the call's return; the
// table's timeout is shortened for it.
func TestSlotsListUnderTheLockIsBounded(t *testing.T) {
	rg := newRig(t)
	tab := newSlotTable()
	tab.listTimeout = 20 * time.Millisecond
	hung := &listProbe{Reader: rg.c, inList: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	done := make(chan error, 1)
	go func() {
		_, _, err := tab.reserve(context.Background(), hung, rigProvider, rigNS+"/a", i32(1), fixedAt(rigStart))
		done <- err
	}()
	var err error
	select {
	case err = <-done:
	case <-time.After(time.Minute): // a safety net for a hang, not a window the test races
		t.Fatal("reserve never returned from a List that never answers")
	}
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), rigProvider) {
		t.Fatalf("reserve error = %v, want the deadline, naming Provider %s", err, rigProvider)
	}
	l := tab.slotLock(rigProvider)
	if !l.TryLock() {
		t.Fatal("the Provider's lock is still held after the List gave up")
	}
	l.Unlock()
}
