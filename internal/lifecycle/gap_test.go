package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// The tests in this file cover HBEP-8, Gaps after a restart, with client-go's
// real leader elector over an in-memory Lease: the lifecycle controller reads
// its Lease's previous renewTime before it takes the Lease, and once it holds
// it emits one degraded/ObservationGap regarding it, whose message names that
// renewTime; a first start emits none. The Lease's reads and writes and the
// recorder's calls are numbered on one shared sequence, since the order is
// the point (HBEP-8, What the recorder merges and drops).

// lockOp is one call of the in-memory Lease, on the shared sequence.
type lockOp struct {
	seq    int
	op     string // get, get-notfound, create, update
	holder string
	failed bool
}

// memLock is a resourcelock.Interface over one in-memory record, standing in
// for the API's Lease.
type memLock struct {
	id  string
	seq *Sequence

	mu  sync.Mutex
	rec *resourcelock.LeaderElectionRecord
	raw []byte
	ops []lockOp
	// commitThenFail makes the next Update commit and then report an error,
	// as a write whose answer is lost does.
	commitThenFail bool
}

func (l *memLock) setLocked(r resourcelock.LeaderElectionRecord) {
	l.rec = &r
	l.raw, _ = json.Marshal(r)
}

// write sets the record as another process would, with a lease of 1 s: the
// elector judges a holder's expiry by the record's own duration.
func (l *memLock) write(holder string, renew time.Time) { l.writeFor(holder, renew, 1) }

// writeFor sets the record with a lease of the given seconds.
func (l *memLock) writeFor(holder string, renew time.Time, seconds int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	t := metav1.NewTime(renew)
	l.setLocked(resourcelock.LeaderElectionRecord{HolderIdentity: holder, LeaseDurationSeconds: seconds, AcquireTime: t, RenewTime: t})
}

// readBy reports whether the elector has read the record held by holder.
func (l *memLock) readBy(holder string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, o := range l.ops {
		if o.op == "get" && o.holder == holder {
			return true
		}
	}
	return false
}

func (l *memLock) Get(context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.rec == nil {
		l.ops = append(l.ops, lockOp{seq: l.seq.Next(), op: "get-notfound"})
		return nil, nil, apierrors.NewNotFound(coordinationv1.Resource("leases"), "lifecycle")
	}
	r := *l.rec
	l.ops = append(l.ops, lockOp{seq: l.seq.Next(), op: "get", holder: r.HolderIdentity})
	return &r, append([]byte(nil), l.raw...), nil
}

func (l *memLock) Create(_ context.Context, r resourcelock.LeaderElectionRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.rec != nil {
		return apierrors.NewAlreadyExists(coordinationv1.Resource("leases"), "lifecycle")
	}
	l.setLocked(r)
	l.ops = append(l.ops, lockOp{seq: l.seq.Next(), op: "create", holder: r.HolderIdentity})
	return nil
}

func (l *memLock) Update(_ context.Context, r resourcelock.LeaderElectionRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.rec == nil {
		return apierrors.NewNotFound(coordinationv1.Resource("leases"), "lifecycle")
	}
	l.setLocked(r)
	if l.commitThenFail {
		l.commitThenFail = false
		l.ops = append(l.ops, lockOp{seq: l.seq.Next(), op: "update", holder: r.HolderIdentity, failed: true})
		return errors.New("the connection broke after the write committed")
	}
	l.ops = append(l.ops, lockOp{seq: l.seq.Next(), op: "update", holder: r.HolderIdentity})
	return nil
}

func (l *memLock) RecordEvent(string) {}
func (l *memLock) Identity() string   { return l.id }
func (l *memLock) Describe() string   { return "hpcburst-system/lifecycle" }
func (l *memLock) opsCopy() []lockOp {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]lockOp(nil), l.ops...)
}
func (l *memLock) renewTime() time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rec.RenewTime.Time
}

// lead runs client-go's leader elector over the lock until it leads, running
// the gap emitter in OnStartedLeading, as controller-runtime starts its
// leader-election runnables.
func lead(t *testing.T, mem *memLock, rec *MemoryRecorder) {
	t.Helper()
	select {
	case <-startLead(t, mem, rec):
	case <-time.After(time.Minute):
		t.Fatalf("did not lead within a minute; lock calls %+v", mem.opsCopy())
	}
}

// startLead starts the elector and returns a channel closed once it leads and
// the gap emitter has run.
func startLead(t *testing.T, mem *memLock, rec *MemoryRecorder) <-chan struct{} {
	t.Helper()
	gl := &gapLock{Interface: mem}
	em := &gapEmitter{lock: gl, rec: rec}
	led := make(chan struct{})
	le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock: gl, LeaseDuration: 600 * time.Millisecond, RenewDeadline: 400 * time.Millisecond, RetryPeriod: 100 * time.Millisecond,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				if err := em.Start(ctx); err != nil {
					t.Errorf("gap emitter: %v", err)
				}
				if err := em.Start(ctx); err != nil { // a second start emits nothing more
					t.Errorf("gap emitter: %v", err)
				}
				close(led)
			},
			OnStoppedLeading: func() {},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { le.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return led
}

// gapEvents returns the degraded/ObservationGap events, checking that each
// regards the lifecycle Lease.
func gapEvents(t *testing.T, rec *MemoryRecorder) []RecordedEvent {
	t.Helper()
	got := rec.Find("degraded", "ObservationGap")
	for _, e := range got {
		if e.Kind != "*v1.Lease" || e.Namespace != "hpcburst-system" || e.Name != "lifecycle" || !e.Warning {
			t.Fatalf("ObservationGap %+v, want a Warning regarding the Lease hpcburst-system/lifecycle", e)
		}
	}
	return got
}

// micro is a renewTime as the API stores it, to the microsecond.
func micro(t time.Time) time.Time { return t.UTC().Truncate(time.Microsecond) }

// checkGapOrder asserts the order on the shared sequence: a read of the Lease
// by another holder, or none found, then this process's first successful write
// (the take), then the event; and that the read just before the take saw
// another holder.
func checkGapOrder(t *testing.T, ops []lockOp, ev RecordedEvent, self string) {
	t.Helper()
	take, lastRead := -1, -1
	for i, o := range ops {
		if (o.op == "create" || o.op == "update") && !o.failed && o.holder == self {
			take = i
			break
		}
		if (o.op == "get" && o.holder != self) || o.op == "get-notfound" {
			lastRead = i
		}
	}
	if take < 0 || lastRead < 0 {
		t.Fatalf("lock calls %+v: no read before a take", ops)
	}
	if !(ops[lastRead].seq < ops[take].seq && ops[take].seq < ev.Seq) {
		t.Fatalf("read at %d, take at %d, ObservationGap at %d: want read < take < event (calls %+v)", ops[lastRead].seq, ops[take].seq, ev.Seq, ops)
	}
}

func TestObservationGapFirstStartEmitsNone(t *testing.T) {
	seq := &Sequence{}
	mem := &memLock{id: "lifecycle-new", seq: seq}
	rec := &MemoryRecorder{Seq: seq}
	lead(t, mem, rec)
	if got := gapEvents(t, rec); len(got) != 0 {
		t.Fatalf("a first start, with no previous Lease, emitted %+v", got)
	}
	if ops := mem.opsCopy(); len(ops) < 2 || ops[0].op != "get-notfound" || ops[1].op != "create" {
		t.Fatalf("lock calls %+v, want a read that found none, then the create", ops)
	}
}

// TestObservationGapAfterARelease: the previous run stopped cleanly and
// released the Lease at T0 (holder empty, renewTime T0, as client-go's release
// writes it). The new run takes it at once and emits exactly one
// ObservationGap naming T0 to the microsecond, after the take.
func TestObservationGapAfterARelease(t *testing.T) {
	seq := &Sequence{}
	mem := &memLock{id: "lifecycle-new", seq: seq}
	t0 := micro(time.Date(2026, 11, 1, 7, 59, 58, 123456000, time.UTC))
	mem.write("", t0)
	rec := &MemoryRecorder{Seq: seq}
	lead(t, mem, rec)
	got := gapEvents(t, rec)
	if len(got) != 1 {
		t.Fatalf("%d ObservationGap events, want exactly one", len(got))
	}
	if !strings.Contains(got[0].Message, "2026-11-01T07:59:58.123456Z") {
		t.Fatalf("message %q does not name the previous renewTime 2026-11-01T07:59:58.123456Z", got[0].Message)
	}
	checkGapOrder(t, mem.opsCopy(), got[0], mem.id)
}

// TestObservationGapAfterALiveHolder: a stale second copy holds the Lease,
// with a duration of an hour, so the new run cannot take it. Once the new run
// has read it (observed on the lock), the second copy renews it a last time at
// T1 with a lease of 1 s and stops. The new run takes the Lease once that
// expires; the gap starts at T1, the renewTime read just before the take, not
// at the first renewTime it read.
func TestObservationGapAfterALiveHolder(t *testing.T) {
	seq := &Sequence{}
	mem := &memLock{id: "lifecycle-new", seq: seq}
	first := micro(time.Now().Add(-time.Second))
	mem.writeFor("lifecycle-old", first, 3600)
	rec := &MemoryRecorder{Seq: seq}
	led := startLead(t, mem, rec)
	for deadline := time.Now().Add(time.Minute); !mem.readBy("lifecycle-old"); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("the elector did not read the held Lease within a minute; lock calls %+v", mem.opsCopy())
		}
	}
	select {
	case <-led:
		t.Fatal("the new run took a Lease another copy holds for an hour")
	default:
	}
	last := micro(time.Now())
	mem.write("lifecycle-old", last)
	select {
	case <-led:
	case <-time.After(time.Minute):
		t.Fatalf("did not lead within a minute of the last renewal; lock calls %+v", mem.opsCopy())
	}
	got := gapEvents(t, rec)
	if len(got) != 1 {
		t.Fatalf("%d ObservationGap events, want exactly one", len(got))
	}
	want, firstS := gapTime(last), gapTime(first)
	if !strings.Contains(got[0].Message, want) || strings.Contains(got[0].Message, firstS) {
		t.Fatalf("message %q: want the last renewTime before the take %s, not the first one read %s", got[0].Message, want, firstS)
	}
	checkGapOrder(t, mem.opsCopy(), got[0], mem.id)
}

// TestObservationGapIgnoresItsOwnCommittedWrite: the take's write commits but
// reports an error, so the elector reads again and finds this run as the
// holder. That read is this run's own heartbeat: the gap still starts at the
// previous run's release, T0.
func TestObservationGapIgnoresItsOwnCommittedWrite(t *testing.T) {
	seq := &Sequence{}
	mem := &memLock{id: "lifecycle-new", seq: seq, commitThenFail: true}
	t0 := micro(time.Date(2026, 11, 1, 7, 59, 58, 123456000, time.UTC))
	mem.write("", t0)
	rec := &MemoryRecorder{Seq: seq}
	lead(t, mem, rec)
	ops := mem.opsCopy()
	sawSelf := false
	for _, o := range ops {
		if o.op == "get" && o.holder == mem.id {
			sawSelf = true
		}
	}
	if !sawSelf {
		t.Fatalf("lock calls %+v: the elector never read its own committed write; the case proves nothing", ops)
	}
	got := gapEvents(t, rec)
	if len(got) != 1 || !strings.Contains(got[0].Message, "2026-11-01T07:59:58.123456Z") {
		t.Fatalf("ObservationGap events %+v, want one naming the release at 2026-11-01T07:59:58.123456Z", got)
	}
	if own := gapTime(mem.renewTime()); strings.Contains(got[0].Message, own) {
		t.Fatalf("message %q names this run's own renewTime %s", got[0].Message, own)
	}
}
