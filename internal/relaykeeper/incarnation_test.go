package relaykeeper

// New for the review of PR 356: entry equality is not an identity. Each Want
// carries an incarnation the keeper never issues twice, kept while the same
// entry, Provider UID and host stay wanted; the gate allows a start only of
// the current incarnation, and the Runner only of the tag's current instance.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The gate refuses an old incarnation's start though its entry equals the
// current Want's: with credentials A, then B, then A again (A→B→A), and with
// the Allocation dropped and the same entry wanted again. The attempt's
// listing passed before either change, as an attempt paused before it took
// its N would have. A re-sent Want of the same entry keeps its incarnation,
// so its starts still pass (the positive control).
func TestAnOldIncarnationsStartIsRefusedThoughItsEntryIsCurrent(t *testing.T) {
	for _, c := range []struct {
		name   string
		change func(t *testing.T, k *Keeper, e wire.Entry)
		allow  bool
	}{
		{"the same Want re-sent", func(t *testing.T, k *Keeper, e wire.Entry) {
			accepted(t, k, set("example-a", 1, true, "login.example", e))
		}, true},
		{"A, then B, then A again", func(t *testing.T, k *Keeper, e wire.Entry) {
			accepted(t, k, set("example-a", 1, true, "login.example", newGen(e)))
			accepted(t, k, set("example-a", 1, true, "login.example", e))
		}, false},
		{"dropped, then the same entry wanted again", func(t *testing.T, k *Keeper, e wire.Entry) {
			accepted(t, k, set("example-a", 1, true, "login.example"))
			accepted(t, k, set("example-a", 1, true, "login.example", e))
		}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newRig(t)
			k, fr := r.open()
			e := entry(1)
			accepted(t, k, set("example-a", 1, true, "login.example", e))
			old := fr.current("example-a", e.Tag)
			if !fr.lists(old) {
				t.Fatal("the listing refused the first start")
			}
			c.change(t, k, e)
			cur, _ := fr.want("example-a", e.Tag)
			if cur.Entry != e {
				t.Fatalf("the current Want's entry %+v, want the first's", cur.Entry)
			}
			if same := cur.Incarnation == old.w.Incarnation; same != c.allow {
				t.Errorf("the current Want's incarnation %d, the first's %d: same %v, want %v", cur.Incarnation, old.w.Incarnation, same, c.allow)
			}
			if got := fr.gate(old, agentapi.RelayControl); got != c.allow {
				t.Fatalf("the first Want's gate answered %v, want %v", got, c.allow)
			}
			if !c.allow {
				noHolds(t, k)
				if ends := r.disk().Ends; len(ends) != 0 {
					t.Errorf("ends recorded: %+v", ends)
				}
				return
			}
			fr.launchFails(old, agentapi.RelayControl)
		})
	}
}

// pausedJob is a listing that shows e's job running, after waiting on gate
// the first time it is read for each channel's relay, so an attempt can be
// paused after its listing and before it takes its N.
type pausedJob struct {
	e      wire.Entry
	gate   chan struct{}
	called *atomic.Int64
}

func (j pausedJob) ListByTag(ctx context.Context, tag string) ([]provider.Job, error) {
	j.called.Add(1)
	select {
	case <-j.gate:
	case <-ctx.Done():
	}
	return []provider.Job{{ID: j.e.JobID, Name: tag, Phase: v1alpha1.AllocationRunning}}, nil
}

// The Runner starts only the tag's current instance: an attempt of an
// instance the Runner has replaced since its listing is refused before the
// keeper's gate is asked, though that gate would allow it. Here the keeper's
// gate is a stub that allows every start, so the Runner's own check is the
// only fence: the first instance's attempts, paused after their listing,
// are let go after a Want of a new incarnation of the same entry replaced
// it, and neither starts; the new instance's do.
func TestTheRunnerStartsOnlyTheTagsCurrentInstance(t *testing.T) {
	var ops atomic.Int64
	r := identityRunner(t, &ops)
	ssh := wire.SSH{HostName: "login.example", User: "example-user", IdentityFile: "/keys/example"}
	e := entry(1)
	var oldStarts, newStarts atomic.Int64
	gate := make(chan struct{})
	var called atomic.Int64
	first := identityWant(e)
	first.Incarnation = 1
	first.Jobs = pausedJob{e: e, gate: gate, called: &called}
	first.OnStart = func(agentapi.RelayChannel, int64, int64) { oldStarts.Add(1) }
	r.Want("example-a", ssh, first)
	eventuallyQ(t, 30*time.Second, "both channels' first listings paused", func() bool { return called.Load() >= 2 })
	second := identityWant(e)
	second.Incarnation = 2
	second.OnStart = func(agentapi.RelayChannel, int64, int64) { newStarts.Add(1) }
	r.Want("example-a", ssh, second)
	// The worker counts an op before it runs it, so wait until the second
	// Want is installed, not only taken, before the paused attempts go: until
	// then they are the current instance's, and may start rightly.
	p := r.prov("example-a")
	eventuallyQ(t, 30*time.Second, "the second Want installed as the tag's current instance", func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		in := p.cur[e.Tag]
		return in != nil && in.want.Incarnation == 2
	})
	close(gate)
	eventuallyQ(t, 30*time.Second, "the new instance's relays up", func() bool {
		up, n := upRelays(r)
		return len(up) == 2 && n == 2 && newStarts.Load() >= 2
	})
	if n := oldStarts.Load(); n != 0 {
		t.Errorf("the replaced instance's paused attempts started %d processes, want none", n)
	}
}

// A, then B, then A again, coalesced in the Runner's queue: the worker,
// stalled, holds only the latest Want of the tag, the restored A, whose
// entry, Provider UID and host equal the running first A's. Its incarnation
// differs, so it is a new Want, and its relays replace the first A's: new
// processes, the restored A's instance current. Were the Runner's identity
// the entry alone, it would keep the first A's relays under an incarnation
// the keeper no longer wants, whose listing refuses every restart (#359's
// mutant run, review of PR 356).
func TestACoalescedAToBToAReplacesTheFirstAsRelays(t *testing.T) {
	stall := make(chan struct{})
	stalling := make(chan struct{}, 1)
	var ops atomic.Int64
	r := identityRunner(t, &ops)
	r.o.beforeOp = func(string, string) {
		if ops.Add(1) > 1 {
			select {
			case stalling <- struct{}{}:
			default:
			}
			<-stall
		}
	}
	ssh := wire.SSH{HostName: "login.example", User: "example-user", IdentityFile: "/keys/example"}
	e := entry(1)
	a := identityWant(e)
	a.Incarnation = 1
	r.Want("example-a", ssh, a)
	var first map[agentapi.RelayChannel]wire.RelayState
	eventuallyQ(t, 30*time.Second, "the first A's relays up", func() bool {
		up, _ := upRelays(r)
		first = up
		return len(up) == 2
	})
	// A re-sent A stalls the worker; B and the restored A then coalesce.
	r.Want("example-a", ssh, a)
	<-stalling
	b := identityWant(newGen(e))
	b.Incarnation = 2
	r.Want("example-a", ssh, b)
	again := identityWant(e)
	again.Incarnation = 3
	r.Want("example-a", ssh, again)
	p := r.prov("example-a")
	if n := p.queue.pending(); n != 1 {
		t.Fatalf("pending Wants of the tag with the worker stalled: %d, want 1, the restored A", n)
	}
	close(stall)
	eventuallyQ(t, 30*time.Second, "the restored A's relays replacing the first A's", func() bool {
		p.mu.Lock()
		in := p.cur[e.Tag]
		p.mu.Unlock()
		if in == nil || in.want.Incarnation != 3 {
			return false
		}
		up, n := upRelays(r)
		if len(up) != 2 || n != 2 {
			return false
		}
		for ch, s := range up {
			if s.PID == first[ch].PID {
				return false
			}
		}
		return true
	})
}
