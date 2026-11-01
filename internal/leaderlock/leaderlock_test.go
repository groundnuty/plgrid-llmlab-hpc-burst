package leaderlock

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

const me = "host_1111"

// fakeLease is a Lease lock over a Lease held in memory: Update fails with a
// conflict while failing is above zero, as a write after a newer write does.
type fakeLease struct {
	mu      sync.Mutex
	holder  string
	failing int
	// afterFail runs after a failed Update, under mu: the Lease changing
	// between the release's tries.
	afterFail func(*fakeLease)
	failGets  int // the next reads that fail
	updates   int
	gets      int
}

func (f *fakeLease) Get(context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	if f.failGets > 0 {
		f.failGets--
		return nil, nil, errors.New("injected: the read timed out")
	}
	return &resourcelock.LeaderElectionRecord{HolderIdentity: f.holder}, nil, nil
}

func (f *fakeLease) Create(context.Context, resourcelock.LeaderElectionRecord) error {
	return errors.New("not used")
}

func (f *fakeLease) Update(_ context.Context, ler resourcelock.LeaderElectionRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updates++
	if f.failing > 0 {
		f.failing--
		if f.afterFail != nil {
			f.afterFail(f)
		}
		return apierrors.NewConflict(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, "lease", errors.New("the object has been modified"))
	}
	f.holder = ler.HolderIdentity
	return nil
}

func (f *fakeLease) RecordEvent(string) {}
func (f *fakeLease) Describe() string   { return "hpcburst-system/lease" }
func (f *fakeLease) Identity() string   { return me }
func (f *fakeLease) state() (string, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.holder, f.updates
}

// logs collects what the lock logs.
type logs struct {
	mu    sync.Mutex
	lines []string
}

func (l *logs) add(prefix, args string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, args)
}

func (l *logs) has(s string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.lines {
		if strings.Contains(line, s) {
			return true
		}
	}
	return false
}

// release is the record client-go's LeaderElector.release writes
// (leaderelection.go:328-333): no holder, a 1 s duration.
var release = resourcelock.LeaderElectionRecord{LeaseDurationSeconds: 1}

func newLock(f *fakeLease) (resourcelock.Interface, *logs) {
	l := &logs{}
	return WithRetriedRelease(f, funcr.New(l.add, funcr.Options{})), l
}

// A release whose write lost to a late renew is read again and written again,
// and lands.
func TestAReleaseThatConflictsIsRetriedAndLands(t *testing.T) {
	f := &fakeLease{holder: me, failing: 1}
	lock, l := newLock(f)
	if err := lock.Update(context.Background(), release); err != nil {
		t.Fatalf("the release returned %v", err)
	}
	if holder, updates := f.state(); holder != "" || updates != 2 {
		t.Fatalf("after the release the holder is %q after %d writes; want none after 2", holder, updates)
	}
	if !l.has("released the leader Lease on a retry") {
		t.Errorf("the retry was not logged: %v", l.lines)
	}
}

// A release whose write reported an error but whose Lease names no holder on
// the next read was committed after all: nothing is written again.
func TestAReleaseIsNotRepeatedOnceTheLeaseIsFree(t *testing.T) {
	f := &fakeLease{holder: me, failing: 1, afterFail: func(f *fakeLease) { f.holder = "" }}
	lock, _ := newLock(f)
	if err := lock.Update(context.Background(), release); err != nil {
		t.Fatalf("the release returned %v", err)
	}
	if _, updates := f.state(); updates != 1 {
		t.Fatalf("the release wrote %d times; a free Lease needs no second write", updates)
	}
}

// A Lease that another holder took meanwhile is left alone.
func TestAReleaseNeverOverwritesAnotherHolder(t *testing.T) {
	f := &fakeLease{holder: me, failing: 1, afterFail: func(f *fakeLease) { f.holder = "host_2222" }}
	lock, l := newLock(f)
	if err := lock.Update(context.Background(), release); err != nil {
		t.Fatalf("the release returned %v", err)
	}
	if holder, updates := f.state(); holder != "host_2222" || updates != 1 {
		t.Fatalf("the Lease is held by %q after %d writes; want the other holder's, untouched", holder, updates)
	}
	if !l.has("no longer names this holder") {
		t.Errorf("leaving the Lease to its new holder was not logged: %v", l.lines)
	}
}

// A release that keeps failing gives up when ReleaseBudget runs out, before
// the release's own context ends, logs it, and returns the error.
func TestAReleaseThatKeepsFailingGivesUpWithinItsBudget(t *testing.T) {
	f := &fakeLease{holder: me, failing: 1 << 20}
	lock, l := newLock(f)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	err := lock.Update(ctx, release)
	if !apierrors.IsConflict(err) {
		t.Fatalf("the release returned %v; want the last conflict", err)
	}
	if ctx.Err() != nil {
		t.Fatal("the release ran until its caller's context ended; want ReleaseBudget to end it")
	}
	if _, updates := f.state(); updates < 2 {
		t.Fatalf("the release was written %d times; want it retried", updates)
	}
	if !l.has("stays held until it expires") {
		t.Errorf("the give-up was not logged: %v", l.lines)
	}
}

// The release's own context, shorter than the budget, ends the retries.
func TestAReleaseStopsAtItsCallersContext(t *testing.T) {
	f := &fakeLease{holder: me, failing: 1 << 20}
	lock, _ := newLock(f)
	ctx, cancel := context.WithTimeout(context.Background(), 3*releaseRetryEvery)
	defer cancel()
	if err := lock.Update(ctx, release); err == nil {
		t.Fatal("the release succeeded against a Lease that refuses every write")
	}
	if ctx.Err() == nil {
		t.Fatal("the release returned before its caller's context ended; it should retry until then")
	}
}

// A read that fails between two tries is tried again.
func TestAReleaseRetriesAFailedRead(t *testing.T) {
	f := &fakeLease{holder: me, failing: 1, afterFail: func(f *fakeLease) { f.failGets = 2 }}
	lock, _ := newLock(f)
	if err := lock.Update(context.Background(), release); err != nil {
		t.Fatalf("the release returned %v", err)
	}
	if holder, updates := f.state(); holder != "" || updates != 2 || f.gets != 3 {
		t.Fatalf("the Lease is held by %q after %d writes and %d reads; want it released after 2 writes and 3 reads", holder, updates, f.gets)
	}
}

// Only the release is retried: a renew that fails returns at once, for the
// elector's own retry loop.
func TestOnlyTheReleaseIsRetried(t *testing.T) {
	f := &fakeLease{holder: me, failing: 1}
	lock, _ := newLock(f)
	renew := resourcelock.LeaderElectionRecord{HolderIdentity: me, LeaseDurationSeconds: 15}
	if err := lock.Update(context.Background(), renew); !apierrors.IsConflict(err) {
		t.Fatalf("the renew returned %v; want its conflict", err)
	}
	if _, updates := f.state(); updates != 1 {
		t.Fatalf("the renew was written %d times; want once", updates)
	}
}
