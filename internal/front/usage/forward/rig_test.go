package forward

// The rig of the forwarder's tests: the durable log as a REAL child process (HBEP-8, Test plan, Beta: "Usage
// while the log is down", "A lost tail", "Full streams", "The caps fit the disk"; issue #625). The
// log is stopped or SIGKILLed through a pidfd opened at its start (internal/eventlog/node/nodetest),
// and the stream is read back through a FRESH client. The expectation is always the FILES: each
// counter once, each message's bytes equal to its lines in the files, never what the forwarder held
// in memory and never its acknowledgements.
//
// Ports: 28510 and 28511, the forwarder's pair of #625's block (IMPL-COMMON). The tests start one
// log at a time and do not run in parallel.

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog/node/nodetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
)

var logPorts = nodetest.Ports{Client: 28510, Monitor: 28511}

func TestMain(m *testing.M) {
	code := m.Run()
	nodetest.Cleanup()
	os.Exit(code)
}

// waitFor bounds every wait on observable state; it returns as soon as the state holds.
const waitFor = 60 * time.Second

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitFor)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", waitFor, what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// clock is a time the test sets.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

// testWriter writes the forwarder's logs to the test's own log.
type testWriter struct{ t *testing.T }

func (w testWriter) Write(b []byte) (int, error) {
	w.t.Log(strings.TrimRight(string(b), "\n"))
	return len(b), nil
}

// bigCaps leave the usage stream room for every test record, unless a test sets its own.
var bigCaps = eventlog.Caps{Events: 1 << 20, Usage: 64 << 20}

// fixedDay is the sink's clock: every record goes to the file of one day.
var fixedDay = time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)

// A forwarder run by a test, and what stops it.
type runner struct {
	t      *testing.T
	fwd    *Forwarder
	cancel context.CancelFunc
	done   chan struct{}
}

// startForwarder runs a forwarder toward the log in d, reading the files in usageDir. lastSynced is
// what the sink would report at the forwarder's start. opts changes its options and seam, before it
// runs, its seams.
func startForwarder(t *testing.T, d *nodetest.Dir, usageDir string, lastSynced func() uint64, opts func(*Options), seam func(*Forwarder)) *runner {
	t.Helper()
	return startForwarderRaw(t, d, usageDir, func() (uint64, bool) { return lastSynced(), true }, opts, seam)
}

// startForwarderRaw is startForwarder with the sink's own answer, which may be "not known".
func startForwarderRaw(t *testing.T, d *nodetest.Dir, usageDir string, lastSynced func() (uint64, bool), opts func(*Options), seam func(*Forwarder)) *runner {
	t.Helper()
	o := Options{Addr: d.Ports.Addr(), Token: d.Token, Dir: usageDir, Logger: slog.New(slog.NewTextHandler(testWriter{t}, nil))}
	if opts != nil {
		opts(&o)
	}
	f := New(o)
	if seam != nil {
		seam(f)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &runner{t: t, fwd: f, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(r.done)
		f.Run(ctx, lastSynced)
	}()
	t.Cleanup(r.stop)
	return r
}

func (r *runner) stop() {
	r.cancel()
	<-r.done
}

// host is a sink and a forwarder wired as the front wires them: the forwarder's Offer is the sink's
// Synced hook, and the forwarder reads the sink's LastSynced at its start.
type host struct {
	*runner
	usageDir string
	sink     *usage.Sink
}

func startHost(t *testing.T, d *nodetest.Dir, usageDir string, opts func(*Options), seam func(*Forwarder)) *host {
	t.Helper()
	// The sink needs the forwarder's Offer and the forwarder the sink's LastSynced: the forwarder is
	// made first, the sink with its hook, and Run is told where to read.
	o := Options{Addr: d.Ports.Addr(), Token: d.Token, Dir: usageDir, Logger: slog.New(slog.NewTextHandler(testWriter{t}, nil))}
	if opts != nil {
		opts(&o)
	}
	f := New(o)
	if seam != nil {
		seam(f)
	}
	sink, err := usage.OpenOptions(usageDir, usage.Options{Now: func() time.Time { return fixedDay }, Synced: f.Offer})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &runner{t: t, fwd: f, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(r.done)
		f.Run(ctx, func() (uint64, bool) {
			st := sink.Stats()
			return st.LastSynced, st.LastSyncedKnown
		})
	}()
	h := &host{runner: r, usageDir: usageDir, sink: sink}
	t.Cleanup(func() { r.stop(); _ = sink.Close() })
	return h
}

// write makes n records through the sink, from `workers` goroutines so that records that arrive
// together share a batch, and returns the first and last counter, which must be contiguous. A Write
// that returns an error fails the test: a Write never depends on the log.
func (h *host) write(n, workers int) (first, last uint64) {
	h.t.Helper()
	var next atomic.Int64
	var mu sync.Mutex
	var got []uint64
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1))
				if i > n {
					return
				}
				c, err := h.sink.Write(usage.Record{RequestID: fmt.Sprintf("req-%06d", i), Path: "/v1/chat/completions", Status: 200, ClientKeyID: "k1"})
				if err != nil {
					errs <- err
					return
				}
				mu.Lock()
				got = append(got, c)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		h.t.Fatalf("Write returned %v: a write never depends on the log", err)
	}
	lo, hi := got[0], got[0]
	for _, c := range got {
		lo, hi = min(lo, c), max(hi, c)
	}
	if uint64(len(got)) != hi-lo+1 {
		h.t.Fatalf("the counters of %d records are not contiguous: %d to %d", len(got), lo, hi)
	}
	return lo, hi
}

// awaitCaughtUp waits for the state in which the forwarder holds the log's whole copy: connected,
// nothing unlogged, and `last` both the sink's last synced counter and the stream's last.
func (r *runner) awaitCaughtUp(last uint64) {
	r.t.Helper()
	eventually(r.t, fmt.Sprintf("the forwarder to hold counter %d, caught up", last), func() bool {
		s := r.fwd.Stats()
		return s.Connected && s.LoggedKnown && s.Logged == last && s.Synced == last && s.Unlogged == 0
	})
}

// awaitLogged waits for the stream to hold counter `last` as the sink's last: connected, and the
// forwarder's logged and synced counters both `last`. It does not wait for the gauge to read 0, which
// a record passed over keeps above it.
func (r *runner) awaitLogged(last uint64) {
	r.t.Helper()
	eventually(r.t, fmt.Sprintf("the forwarder to hold counter %d", last), func() bool {
		s := r.fwd.Stats()
		return s.Connected && s.LoggedKnown && s.Logged == last && s.Synced == last
	})
}

func (r *runner) awaitDown() {
	r.t.Helper()
	eventually(r.t, "the forwarder to see the log down", func() bool { return !r.fwd.Stats().Connected })
}

func (r *runner) awaitConnected() {
	r.t.Helper()
	eventually(r.t, "the forwarder to connect and resume", func() bool { return r.fwd.Stats().Connected })
}

// awaitServing waits until a session has finished its first catch-up: the forwarder then takes each
// batch the sink offers, as its own message.
func (r *runner) awaitServing(sessions uint64) {
	r.t.Helper()
	eventually(r.t, "the forwarder to finish its first catch-up", func() bool { return r.fwd.caughtUp.Load() >= sessions })
}

// logBuf is a log sink a test can read while the forwarder writes to it.
type logBuf struct {
	mu sync.Mutex
	b  strings.Builder
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
