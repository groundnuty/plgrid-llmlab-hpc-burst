package eventcount

// The rig of the publisher's tests (HBEP-8, Test plan, Beta; issue #625, PR E2). The durable log runs as
// a REAL child process (internal/eventlog/node/nodetest, E3's rig: built once, started in a group of
// its own, killed or stopped through a pidfd opened at its start, with the wait status read), and what
// the log holds is read through a FRESH client, from its first sequence: never through the publisher,
// whose acknowledgements prove only what it saw, and never from the counter.
//
// Ports: 28516 and 28517, this package's pair of #625's block (IMPL-COMMON). The tests start one log at
// a time and do not run in parallel.

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog/node/nodetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

var logPorts = nodetest.Ports{Client: 28516, Monitor: 28517}

func TestMain(m *testing.M) {
	code := m.Run()
	nodetest.Cleanup()
	os.Exit(code)
}

// waitFor bounds every wait on observable state; each wait returns as soon as the state holds.
const waitFor = 60 * time.Second

func eventually(t testing.TB, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitFor)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", waitFor, what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// bigCaps leave both streams room for every test event, unless a test sets its own.
var bigCaps = eventlog.Caps{Events: 64 << 20, Usage: 1 << 20}

func testScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(s); err != nil {
		panic(err)
	}
	if err := coordinationv1.AddToScheme(s); err != nil {
		panic(err)
	}
	return s
}

// alloc is a synthetic Allocation with a UID of its own: the object an event is about.
func alloc(uid string) *v1alpha1.Allocation {
	return &v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Name: "alloc-" + uid, Namespace: "ns-a", UID: types.UID(uid)}}
}

// lease is a synthetic Lease with a UID of its own: the regarding object of EventsNotLogged.
func lease(name string) *coordinationv1.Lease {
	return &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: v1alpha1.ControllerNamespace, UID: types.UID("uid-of-" + name)}}
}

// recorded is one Eventf call a recInner received.
type recorded struct {
	regarding runtime.Object
	reason    string
	action    string
	eventtype string
	note      string
	clockSeen int64 // how many times the publisher's clock had been read when the call arrived
}

// recInner stands for client-go's events.k8s.io recorder: it records what the wrapper hands over,
// which is what the gate's decisions are read from. Real delivery to the API, and to the log, is read
// from the API server and from the stream, never from this.
type recInner struct {
	mu    sync.Mutex
	calls []recorded
	clock *countingClock
}

func (r *recInner) Eventf(regarding runtime.Object, related runtime.Object, eventtype, reason, action, note string, args ...interface{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var seen int64
	if r.clock != nil {
		seen = r.clock.reads.Load()
	}
	r.calls = append(r.calls, recorded{regarding: regarding, reason: reason, action: action, eventtype: eventtype, note: fmt.Sprintf(note, args...), clockSeen: seen})
}

func (r *recInner) byReason(reason string) []recorded {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []recorded
	for _, c := range r.calls {
		if c.reason == reason {
			out = append(out, c)
		}
	}
	return out
}

// fakeClock is a time the test sets, read by the publisher.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// countingClock is a fixed time that counts its reads.
type countingClock struct {
	t     time.Time
	reads atomic.Int64
}

func (c *countingClock) now() time.Time {
	c.reads.Add(1)
	return c.t
}

// base is the fake clock's start: a UTC date, never the real clock.
var base = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)

// logBuf is a log sink a test can read while the publisher writes to it.
type logBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// rig is one durable log with its directory, and the publisher, counter and wrapper that talk to it.
type rig struct {
	t       *testing.T
	d       *nodetest.Dir
	proc    *nodetest.Proc
	pub     *Publisher
	counter *Counter
	inner   *recInner
	rec     IDRecorder
	logs    *logBuf
}

// startLog makes a log directory with the caps and starts the log on it.
func startLog(t *testing.T, caps eventlog.Caps) (*nodetest.Dir, *nodetest.Proc) {
	t.Helper()
	d := nodetest.NewDir(t, logPorts, caps)
	p := d.Start(t)
	p.Ready()
	return d, p
}

// newRig starts a log (unless the test says none: down), a publisher toward it and a wrapper
// over a recording inner recorder, for the controller, with the Lease source leaseOf (nil for none).
// opts change the publisher's options before it starts.
func newRig(t *testing.T, controller string, down bool, caps eventlog.Caps, opts func(*PublisherOptions), leaseOf LeaseFunc) *rig {
	t.Helper()
	r := &rig{t: t, counter: New(), inner: &recInner{}, logs: &logBuf{}}
	if down {
		r.d = nodetest.NewDir(t, logPorts, caps)
	} else {
		r.d, r.proc = startLog(t, caps)
	}
	po := PublisherOptions{Addr: r.d.Ports.Addr(), Token: r.d.Token, Logger: slog.New(slog.NewTextHandler(r.logs, nil)),
		MinBackoff: 5 * time.Millisecond, MaxBackoff: 50 * time.Millisecond}
	if opts != nil {
		opts(&po)
	}
	r.pub = NewPublisher(po)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		r.pub.Shutdown(ctx)
	})
	r.rec = Events(r.inner, r.counter, controller, WithLog(Log{Publisher: r.pub, Scheme: testScheme(), ReportingController: "hpcburst-test", Lease: leaseOf}))
	return r
}

// route is the controller's route, whose loss barrier the tests read.
func (r *rig) route(controller string) *route { return r.pub.routeFor(controller, r.counter) }

func (r *rig) unlogged(controller string) float64 {
	v, _ := r.counter.Unlogged(controller)
	return v
}

// read is every message of HB_EVENTS as a fresh client sees it.
func (r *rig) read() []nodetest.Message { r.t.Helper(); return r.d.ReadAll(r.t, eventlog.StreamEvents) }

// ids are the Nats-Msg-Ids of the messages, in sequence order.
func ids(ms []nodetest.Message) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = m.ID
	}
	return out
}

// settled waits until nothing is queued or in flight.
func (r *rig) settled() {
	r.t.Helper()
	eventually(r.t, "the publisher's queue to empty", func() bool { return r.pub.Pending() == 0 })
}

// streamMsgs is the stream's own count of its messages, read through a fresh client.
func (r *rig) streamMsgs() uint64 {
	r.t.Helper()
	_, js := r.d.Connect(r.t)
	return nodetest.StreamInfo(r.t, js, eventlog.StreamEvents).State.Msgs
}

// streamBytes is the stream's own count of its bytes, read through a fresh client.
func (r *rig) streamBytes() uint64 {
	r.t.Helper()
	_, js := r.d.Connect(r.t)
	return nodetest.StreamInfo(r.t, js, eventlog.StreamEvents).State.Bytes
}

// scrape is the counter's /metrics text, one line each, as a scrape would read it.
func scrape(t testing.TB, c *Counter) []string {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(c)
	fams, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	enc := expfmt.NewEncoder(&buf, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, f := range fams {
		if err := enc.Encode(f); err != nil {
			t.Fatal(err)
		}
	}
	return strings.Split(buf.String(), "\n")
}

// leaseFor is a Lease source that answers with the Lease named name.
func leaseFor(name string) LeaseFunc {
	return func(context.Context) (*coordinationv1.Lease, error) { return lease(name), nil }
}

// raise is one typed event with a known id, about an Allocation.
func (r *rig) raise(uid, id, reason string) {
	r.rec.EventfID(id, alloc(uid), nil, provider.EventKindOrdinary, reason, "started", "%s", "a note of a fixed length")
}
