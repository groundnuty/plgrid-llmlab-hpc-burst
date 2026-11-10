package providerctl

// The relays' fence on the Provider's Lease (fence.go; the delta review of
// PR 301, D1 and D2), against client-go's own elector where its order counts.

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/hostdial"
)

// memLock is a Lease in memory that several copies' locks share.
type memLock struct {
	id    string
	store *memLease
	// failRenew, once set, fails this copy's renewals (not its release).
	failRenew bool
}

type memLease struct {
	mu  sync.Mutex
	rec *resourcelock.LeaderElectionRecord
	// log is every write, in order, with the holder it named.
	log []string
	// onRelease, when set, is told of a release write as it lands.
	onRelease func()
}

func (l *memLock) Get(context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	l.store.mu.Lock()
	defer l.store.mu.Unlock()
	if l.store.rec == nil {
		return nil, nil, kerrors.NewNotFound(schema.GroupResource{Resource: "leases"}, "provider-example")
	}
	r := *l.store.rec
	b, _ := json.Marshal(r)
	return &r, b, nil
}

func (l *memLock) Create(_ context.Context, ler resourcelock.LeaderElectionRecord) error {
	l.store.mu.Lock()
	defer l.store.mu.Unlock()
	if l.store.rec != nil {
		return errors.New("the Lease exists")
	}
	l.store.rec = &ler
	l.store.log = append(l.store.log, "write:"+ler.HolderIdentity)
	return nil
}

func (l *memLock) Update(_ context.Context, ler resourcelock.LeaderElectionRecord) error {
	l.store.mu.Lock()
	defer l.store.mu.Unlock()
	if l.failRenew && ler.HolderIdentity != "" {
		return errors.New("the API server does not answer")
	}
	if ler.HolderIdentity == "" && l.store.onRelease != nil {
		l.store.onRelease()
	}
	l.store.rec = &ler
	l.store.log = append(l.store.log, "write:"+ler.HolderIdentity)
	return nil
}

func (l *memLock) RecordEvent(string) {}
func (l *memLock) Identity() string   { return l.id }
func (l *memLock) Describe() string   { return "provider-example" }

func (s *memLease) entries() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.log...)
}

// The epoch is this copy's own acquisition's (D1): copy A acquires with
// leaseTransitions 4; while A is paused, copy B takes the Lease (5); A's epoch
// is still 4, never B's 5. Before its own acquisition a copy has none.
func TestTheEpochIsTheCopysOwnAcquisition(t *testing.T) {
	lease := &memLease{}
	a := NewRelayFence(&memLock{id: "copy-a", store: lease})
	if _, err := a.Epoch(context.Background()); err == nil {
		t.Error("a copy that has not acquired has an epoch")
	}
	now := time.Now()
	if err := a.Create(context.Background(), resourcelock.LeaderElectionRecord{HolderIdentity: "copy-a", LeaderTransitions: 4, LeaseDurationSeconds: 15, AcquireTime: metaTime(now), RenewTime: metaTime(now)}); err != nil {
		t.Fatal(err)
	}
	b := NewRelayFence(&memLock{id: "copy-b", store: lease})
	if err := b.Update(context.Background(), resourcelock.LeaderElectionRecord{HolderIdentity: "copy-b", LeaderTransitions: 5, LeaseDurationSeconds: 15}); err != nil {
		t.Fatal(err)
	}
	if e, err := a.Epoch(context.Background()); err != nil || e != 4 {
		t.Errorf("A's epoch after B took the Lease: %d, %v; want its own 4", e, err)
	}
	if e, err := b.Epoch(context.Background()); err != nil || e != 5 {
		t.Errorf("B's epoch: %d, %v; want 5", e, err)
	}
	// A write that names another holder gives no epoch.
	c := NewRelayFence(&memLock{id: "copy-c", store: lease})
	_ = c.Update(context.Background(), resourcelock.LeaderElectionRecord{HolderIdentity: "copy-b", LeaderTransitions: 5})
	if _, err := c.Epoch(context.Background()); err == nil {
		t.Error("a write naming another holder gave copy C an epoch")
	}
}

// The relays stop before the Lease is released on a renewal that fails (D2):
// client-go's elector, with ReleaseOnCancel as the Provider manager sets it,
// leads, then its renewals fail; renew() releases the Lease before
// OnStoppedLeading, and the fence's hook, standing for the relays' stop, has
// finished before the release record is written.
func TestTheRelaysStopBeforeTheLeaseIsReleasedOnARenewalFailure(t *testing.T) {
	lease := &memLease{}
	ml := &memLock{id: "copy-a", store: lease}
	fence := NewRelayFence(ml)
	var mu sync.Mutex
	var order []string
	note := func(s string) { mu.Lock(); order = append(order, s); mu.Unlock() }
	lease.onRelease = func() { note("lease released") }
	fence.BeforeRelease(func() {
		time.Sleep(200 * time.Millisecond) // a stop that takes a while: the release must wait for it
		note("relays stopped")
	})
	leading, stopped := make(chan struct{}), make(chan struct{})
	le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock: fence, LeaseDuration: 3 * time.Second, RenewDeadline: 2 * time.Second, RetryPeriod: 200 * time.Millisecond,
		ReleaseOnCancel: true, Name: "provider-example",
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) { close(leading); <-ctx.Done() },
			OnStoppedLeading: func() { note("stopped leading"); close(stopped) },
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	go le.Run(context.Background())
	select {
	case <-leading:
	case <-time.After(30 * time.Second):
		t.Fatal("the elector did not lead")
	}
	if e, err := fence.Epoch(context.Background()); err != nil || e != 0 {
		t.Errorf("the epoch of the first acquisition: %d, %v; want 0", e, err)
	}
	lease.mu.Lock()
	ml.failRenew = true
	lease.mu.Unlock()
	select {
	case <-stopped:
	case <-time.After(30 * time.Second):
		t.Fatal("the elector did not stop leading after its renewals failed")
	}
	log := lease.entries()
	if len(log) == 0 || log[len(log)-1] != "write:" {
		t.Fatalf("the Lease's writes %q: the last is not the release (no holder)", log)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 3 || order[0] != "relays stopped" || order[1] != "lease released" || order[2] != "stopped leading" {
		t.Errorf("the order was %q; want the relays stopped, then the release, then OnStoppedLeading", order)
	}
}

func metaTime(t time.Time) metav1.Time { return metav1.NewTime(t) }

// The wiring of the fence (D2): the Provider manager's fence gets the
// controller's relay stop as its release hook and gives the controller its
// epoch, so a release of the Lease finds the controller's relays stopped.
func TestTheFenceStopsTheControllersRelaysBeforeARelease(t *testing.T) {
	e := newEnv(t, func(e *env) { hostDialedWith(e, hostdial.NewHosts(), 1000); e.opts.Relays.Epoch = nil })
	lease := &memLease{}
	fence := NewRelayFence(&memLock{id: "copy-a", store: lease})
	if err := wireRelayFence(fencedManager{fence: fence}, e.ctl); err != nil {
		t.Fatal(err)
	}
	if err := fence.Create(context.Background(), resourcelock.LeaderElectionRecord{HolderIdentity: "copy-a", LeaderTransitions: 2, LeaseDurationSeconds: 15}); err != nil {
		t.Fatal(err)
	}
	runRelays(e)
	e.newAlloc("alloc-a")
	e.run("alloc-a", 4)
	tag := tagOf(e.get("alloc-a"))
	waitRelays(t, "both relays kept, with the fence's epoch", func() bool {
		rs := relaysOf(e, tag)
		return len(rs) == 2 && rs[0].Epoch == 2 && rs[1].Epoch == 2
	})
	if err := fence.Update(context.Background(), resourcelock.LeaderElectionRecord{LeaderTransitions: 2, LeaseDurationSeconds: 1}); err != nil {
		t.Fatal(err)
	}
	if rs := relaysOf(e, tag); len(rs) != 0 {
		t.Errorf("the controller keeps %d relays after the Lease's release", len(rs))
	}
	e.run("alloc-a", 1)
	if rs := relaysOf(e, tag); len(rs) != 0 {
		t.Errorf("a relay started after the Lease's release: %+v", rs)
	}
}
