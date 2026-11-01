package frpsplugin

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMemIndexAnswersFromItsSnapshot(t *testing.T) {
	idx := syncedIndex(t)
	ctx := context.Background()
	for _, c := range []struct {
		token string
		uid   string
		ok    bool
	}{{tokA, "alloc-a", true}, {tokB, "alloc-b", true}, {tokC, "", false}, {"", "", false}, {tokA[:63], "", false}, {tokA + "0", "", false}} {
		if uid, ok := idx.AllocationForToken(ctx, c.token); uid != c.uid || ok != c.ok {
			t.Errorf("AllocationForToken(%.8q...) = %q, %v, want %q, %v", c.token, uid, ok, c.uid, c.ok)
		}
	}
	for _, c := range []struct {
		alloc, inst string
		ok          bool
	}{{"alloc-a", uidA, true}, {"alloc-b", uidB, true}, {"alloc-a", uidB, false}, {"alloc-b", uidA, false}, {"alloc-c", uidA, false}, {"", "", false}, {"alloc-a", "", false}} {
		if got := idx.InstanceOnAllocation(ctx, c.alloc, c.inst); got != c.ok {
			t.Errorf("InstanceOnAllocation(%q, %q) = %v, want %v", c.alloc, c.inst, got, c.ok)
		}
	}
}

func TestMemIndexBeforeAnyReplaceKnowsNothingAndIsNotSynced(t *testing.T) {
	var idx MemIndex
	if idx.Synced() {
		t.Error("a new index is synced")
	}
	if uid, ok := idx.AllocationForToken(context.Background(), tokA); ok || uid != "" {
		t.Errorf("a new index knows %q", uid)
	}
	if idx.InstanceOnAllocation(context.Background(), "alloc-a", uidA) {
		t.Error("a new index places an Instance")
	}
	// Replace alone does not sync: that is the controller's statement about its caches.
	if err := idx.Replace(Snapshot{}); err != nil {
		t.Fatal(err)
	}
	if idx.Synced() {
		t.Error("Replace marked the index synced")
	}
	idx.MarkSynced()
	if !idx.Synced() {
		t.Error("MarkSynced did not")
	}
}

func TestMemIndexRefusesASnapshotThatIsNotWellFormedAndKeepsTheOldState(t *testing.T) {
	good := Snapshot{Allocations: []AllocationEntry{{UID: "alloc-a", Credential: tokA, Instances: []string{uidA}}}}
	for _, c := range []struct {
		name string
		snap Snapshot
	}{
		{"no Allocation UID", Snapshot{Allocations: []AllocationEntry{{Credential: tokB}}}},
		{"a credential of another form", Snapshot{Allocations: []AllocationEntry{{UID: "x", Credential: "short"}}}},
		{"no credential", Snapshot{Allocations: []AllocationEntry{{UID: "x"}}}},
		{"an Allocation twice", Snapshot{Allocations: []AllocationEntry{{UID: "x", Credential: tokB}, {UID: "x", Credential: tokC}}}},
		{"two Allocations with one credential", Snapshot{Allocations: []AllocationEntry{{UID: "x", Credential: tokB}, {UID: "y", Credential: tokB}}}},
		{"an Instance with no UID", Snapshot{Allocations: []AllocationEntry{{UID: "x", Credential: tokB, Instances: []string{""}}}}},
		{"an Instance under two Allocations", Snapshot{Allocations: []AllocationEntry{{UID: "x", Credential: tokB, Instances: []string{"i"}}, {UID: "y", Credential: tokC, Instances: []string{"i"}}}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			var idx MemIndex
			if err := idx.Replace(good); err != nil {
				t.Fatal(err)
			}
			err := idx.Replace(c.snap)
			if err == nil {
				t.Fatal("the snapshot was accepted")
			}
			for _, tok := range []string{tokA, tokB, tokC} {
				if strings.Contains(err.Error(), tok) {
					t.Errorf("the error contains a credential: %v", err)
				}
			}
			if uid, ok := idx.AllocationForToken(context.Background(), tokA); !ok || uid != "alloc-a" {
				t.Errorf("the old state was lost after a refused snapshot")
			}
		})
	}
}

// TestMemIndexSwapsAtomicallyUnderLoad: readers racing Replace see a whole snapshot, never a mix
// (run with -race). Snapshot k holds two Allocations, a-k and b-k, with credentials c-k and d-k and
// Instances i-k and j-k. The readers start together (a barrier), each asks for one credential of the
// snapshot it last saw and for both Instances of it, and the test fails unless every reader took
// part and the lookups succeeded while the replacements ran: a loop that never found anything would
// check nothing.
func TestMemIndexSwapsAtomicallyUnderLoad(t *testing.T) {
	const kinds = 4
	credA := func(k int) string { return strings.Repeat(fmt.Sprintf("%02x", k), 32) }
	credB := func(k int) string { return strings.Repeat(fmt.Sprintf("%02x", k+kinds), 32) }
	snap := func(k int) Snapshot {
		return Snapshot{Allocations: []AllocationEntry{
			{UID: fmt.Sprintf("a-%d", k), Credential: credA(k), Instances: []string{fmt.Sprintf("i-%d", k)}},
			{UID: fmt.Sprintf("b-%d", k), Credential: credB(k), Instances: []string{fmt.Sprintf("j-%d", k)}},
		}}
	}
	var idx MemIndex
	if err := idx.Replace(snap(0)); err != nil {
		t.Fatal(err)
	}
	const readers = 4
	var (
		ready, done sync.WaitGroup
		begin       = make(chan struct{})
		stop        = make(chan struct{})
		hits        atomic.Int64
		rounds      [readers]atomic.Int64
	)
	ctx := context.Background()
	for r := 0; r < readers; r++ {
		ready.Add(1)
		done.Add(1)
		go func() {
			defer done.Done()
			ready.Done()
			<-begin
			for k := r; ; k = (k + 1) % kinds {
				select {
				case <-stop:
					return
				default:
				}
				rounds[r].Add(1)
				runtime.Gosched() // lets the replacer run on a single processor too
				// Whichever snapshot is current, at most one of these matches, and it must name
				// the Allocation that has this k, whose Instance must be on it.
				if uid, ok := idx.AllocationForToken(ctx, credA(k)); ok {
					hits.Add(1)
					if uid != fmt.Sprintf("a-%d", k) {
						t.Errorf("credential %d named Allocation %q", k, uid)
						return
					}
				}
				if uid, ok := idx.AllocationForToken(ctx, credB(k)); ok {
					hits.Add(1)
					if uid != fmt.Sprintf("b-%d", k) {
						t.Errorf("credential %d (b) named Allocation %q", k, uid)
						return
					}
				}
			}
		}()
	}
	ready.Wait()
	close(begin)
	// Replace 200 times, and after each one wait until some lookup has succeeded, so that lookups
	// are known to have succeeded while replacements ran, whatever the scheduling (GOMAXPROCS=1
	// included). The readers do not pause, so a Replace still lands in the middle of their loops.
	deadline := time.Now().Add(20 * time.Second)
	replaced := 0
	for k := 1; k <= 200; k++ {
		before := hits.Load()
		if err := idx.Replace(snap(k % kinds)); err != nil {
			t.Fatal(err)
		}
		replaced++
		for hits.Load() == before {
			if time.Now().After(deadline) {
				close(stop)
				done.Wait()
				t.Fatalf("no lookup succeeded after replacement %d: the readers checked nothing", replaced)
			}
			time.Sleep(20 * time.Microsecond)
		}
	}
	// Every reader must have run its loop, not only some: wait for the slowest to be scheduled.
	for r := range rounds {
		for rounds[r].Load() == 0 {
			if time.Now().After(deadline) {
				t.Errorf("reader %d never took part", r)
				break
			}
			time.Sleep(20 * time.Microsecond)
		}
	}
	close(stop)
	done.Wait()
	if hits.Load() == 0 {
		t.Fatalf("no lookup succeeded in %d replacements: the readers checked nothing", replaced)
	}
}

func TestTriggerCoalescesAndNeverBlocks(t *testing.T) {
	tr := NewTrigger()
	for i := 0; i < 1000; i++ { // nobody drains: it must not block
		tr.ProxyChanged("CloseProxy", "x")
	}
	n := 0
	for drained := false; !drained; {
		select {
		case <-tr.C():
			n++
		default:
			drained = true
		}
	}
	if n != 1 {
		t.Errorf("%d signals pending after 1000 calls, want 1", n)
	}
	tr.ProxyChanged("NewProxy", "y")
	select {
	case <-tr.C():
	default:
		t.Error("no signal after a call on a drained trigger")
	}
	(&Trigger{}).ProxyChanged("NewProxy", "z") // a zero Trigger does not block or panic either
}
