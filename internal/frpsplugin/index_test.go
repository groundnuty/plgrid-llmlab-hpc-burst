package frpsplugin

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
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
// (run with -race). Each snapshot k has Allocation a-k with credential c-k and Instance i-k.
func TestMemIndexSwapsAtomicallyUnderLoad(t *testing.T) {
	cred := func(k int) string { return strings.Repeat(fmt.Sprintf("%02x", k), 32) }
	var idx MemIndex
	snap := func(k int) Snapshot {
		return Snapshot{Allocations: []AllocationEntry{{UID: fmt.Sprintf("a-%d", k), Credential: cred(k), Instances: []string{fmt.Sprintf("i-%d", k)}}}}
	}
	if err := idx.Replace(snap(0)); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; ; k = (k + 1) % 50 {
				select {
				case <-stop:
					return
				default:
				}
				if uid, ok := idx.AllocationForToken(context.Background(), cred(k)); ok {
					if uid != fmt.Sprintf("a-%d", k) {
						t.Errorf("credential %d named Allocation %q", k, uid)
						return
					}
				}
			}
		}()
	}
	for k := 1; k < 300; k++ {
		if err := idx.Replace(snap(k % 50)); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
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
