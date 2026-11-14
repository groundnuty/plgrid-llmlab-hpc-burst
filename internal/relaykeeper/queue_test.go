package relaykeeper

// New for PR 3: the real Runner's per-Provider queue coalesces, keeping only
// the latest pending Want or Drop per tag, so a worker stalled in a blocking
// Drop while sets keep arriving holds at most one entry per Allocation, and
// the keeper, which enqueues under its lock, never blocks on it.

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The queue keeps one entry per tag, the latest put, in the order the tags
// first came.
func TestTheQueueKeepsTheLatestOpPerTag(t *testing.T) {
	q := newOpQueue()
	var got []string
	for i := range 100 {
		for _, tag := range []string{"a", "b", "c"} {
			n := i
			q.put(tag, func() { got = append(got, tag+":"+string(rune('0'+n%10))) })
		}
	}
	if n := q.pending(); n != 3 {
		t.Fatalf("pending after 300 puts on 3 tags: %d, want 3", n)
	}
	for _, op := range q.take() {
		op.do()
	}
	if len(got) != 3 || got[0] != "a:9" || got[1] != "b:9" || got[2] != "c:9" {
		t.Errorf("the work done: %q, want the latest of each tag, in the order the tags came", got)
	}
	if q.pending() != 0 || len(q.take()) != 0 {
		t.Error("the queue is not empty after its work was taken")
	}
}

// noJobs is a listing that shows no job, so no relay starts.
type noJobs struct{}

func (noJobs) ListByTag(context.Context, string) ([]provider.Job, error) { return nil, nil }

// A worker stalled while the keeper's sets keep coming holds at most one
// pending entry per tag, the keeper's calls never block, and once the worker
// runs again the latest Want of each tag wins: the Runner's current instance
// of each tag is the last one wanted.
func TestAStalledWorkerHoldsOneEntryPerTagAndTheLatestWins(t *testing.T) {
	stall := make(chan struct{})
	var once sync.Once
	stalled := make(chan struct{})
	r, err := NewOwnerRunner(context.Background(), OwnerRunnerOptions{Epoch: EpochBase + 1,
		Ports:  map[agentapi.RelayChannel]int{agentapi.RelayControl: 28000, agentapi.RelayServing: 28001},
		SSHDir: filepath.Join(t.TempDir(), "ssh"), SSHBinary: filepath.Join(t.TempDir(), "no-such-ssh"),
		beforeOp: func(string, string) {
			once.Do(func() { close(stalled) })
			<-stall
		}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Stop)
	ssh := wire.SSH{HostName: "login.example", User: "example-user", IdentityFile: "/keys/example"}
	want := func(e wire.Entry) Want {
		return Want{Entry: e, ProviderUID: "0a1b2c3d-0000-4000-8000-000000000001", Jobs: noJobs{},
			Finals:  func() map[agentapi.RelayChannel]string { return nil },
			OnStart: func(agentapi.RelayChannel, int64, int64) {}, StartFailed: func(agentapi.RelayChannel) {},
			BeforeStart: func(agentapi.RelayChannel, int64, int64) bool { return false }}
	}
	r.Want("example-a", ssh, want(entry(1))) // the worker takes it, and stalls
	select {
	case <-stalled:
	case <-time.After(30 * time.Second):
		t.Fatal("the worker did not take the first Want")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 200 {
			for _, n := range []int{1, 2, 3} {
				e := entry(n)
				e.Gen = string(rune('a'+i%26)) + "1234567890abcde"
				if i%7 == 6 {
					r.Drop("example-a", e.Tag)
					continue
				}
				r.Want("example-a", ssh, want(e))
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the keeper's calls blocked on a stalled worker")
	}
	p := r.prov("example-a")
	if n := p.queue.pending(); n != 3 {
		t.Errorf("pending with the worker stalled after 600 calls on 3 tags: %d, want 3, one per tag", n)
	}
	close(stall)
	eventuallyQ(t, 30*time.Second, "the latest Want of each tag current", func() bool {
		if p.queue.pending() != 0 {
			return false
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, n := range []int{1, 2, 3} {
			in := p.cur[entry(n).Tag]
			// i = 199 is the last: 199%7 == 3, a Want, of Gen 'r' (199%26 = 17).
			if in == nil || in.want.Entry.Gen != "r1234567890abcde" {
				return false
			}
		}
		return true
	})
}

// eventuallyQ waits until cond holds, polling observable state, or fails the
// test after d.
func eventuallyQ(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	end := time.Now().Add(d)
	for !cond() {
		if time.Now().After(end) {
			t.Fatalf("gave up waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
