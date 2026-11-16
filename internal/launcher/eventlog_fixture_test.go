package launcher

// The durable log under the supervisor, in tests (eventlog.go; HBEP-8; issue #625, PR E1). The log
// these tests supervise is the real one: the fake controller's eventlog role runs node.Run, with its
// streams, its token and its caps. Nothing publishes to it but the test's own client (the event
// publisher and the usage forwarder are PR E2's and E3's).
//
// Ports: #625's block is 28480-28519, and the launcher's tests with the log use base 28480, so
// 28480-28497 (IMPL-COMMON). The controller's /healthz is base+6, the log's clients base+16 and its
// monitoring port base+17. The tests run one at a time.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
)

const eventLogTestBase = 28480

// fakeDisk is the seam's test double: the free space and the sizes are set by exact path, and a path
// that was not set is an error, so a test that reads another path's free space fails instead of
// passing (HBEP-8, Test plan: that "proves nothing"). It records every path asked about.
type fakeDisk struct {
	mu    sync.Mutex
	free  map[string]int64
	size  map[string]int64
	asked []string
}

func newFakeDisk() *fakeDisk { return &fakeDisk{free: map[string]int64{}, size: map[string]int64{}} }

func (d *fakeDisk) set(free map[string]int64, size map[string]int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for k, v := range free {
		d.free[k] = v
	}
	for k, v := range size {
		d.size[k] = v
	}
}

func (d *fakeDisk) Free(path string) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.asked = append(d.asked, "Free "+path)
	v, ok := d.free[path]
	if !ok {
		return 0, fmt.Errorf("fakeDisk: no free space was set for %s: the floor asked about a path that is not the store's", path)
	}
	return v, nil
}

func (d *fakeDisk) Size(path string) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.asked = append(d.asked, "Size "+path)
	v, ok := d.size[path]
	if !ok {
		return 0, fmt.Errorf("fakeDisk: no size was set for %s: the floor asked about a path that is neither the store nor its copy", path)
	}
	return v, nil
}

func (d *fakeDisk) askedPaths() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.asked...)
}

// diskFor sets a fake disk for a host in dir: free on the store's file system, the store's size on
// disk and the previous copy's size.
func (f *ctlFixture) diskFor(free, store, prevCopy int64) *fakeDisk {
	d := newFakeDisk()
	d.set(map[string]int64{f.l.eventLogDir(): free}, map[string]int64{f.l.eventLogDir(): store, f.l.eventLogCopyDir(): prevCopy})
	return d
}

// eventLogFixture is a ctlFixture whose record runs the durable log: the launcher's block at base
// 28480 with the log's two ports, the files up writes, and the caps and the floor named. The
// fixture's port block must be free: a busy one fails the test at once, naming the port.
func eventLogFixture(t *testing.T, cfg EventLogConfig) *ctlFixture {
	t.Helper()
	l, err := newLayout(filepath.Join(shortDir(t), "st"))
	if err != nil {
		t.Fatal(err)
	}
	self, err := newRec("k3s", os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	base := eventLogTestBase
	ports := Ports{API: base, HTTPS: base + 1, KCM: base + 2, Agent: base + 3, LB: base + 5, Controller: base + 6}
	if ports, err = ports.WithEventLog(); err != nil {
		t.Fatal(err)
	}
	if err := checkPortsFree(ports.All()); err != nil {
		t.Fatalf("the log tests' block, 28480-28497, is not free: %v", err)
	}
	f := &ctlFixture{t: t, l: l, ctl: t.TempDir(), base: base, ports: ports, releasePort: func() {}}
	f.st = &State{Version: "v1.36.5+k3s1", Ports: ports, Procs: []ProcRec{self}, StartedAt: time.Now(),
		Controllers: &ControllerConfig{Binary: fakeControllerBin(t)}, EventLog: &cfg}
	for _, d := range []string{l.dir, l.logDir(), l.controllerDir()} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := prepareEventLogFiles(l.host(ports), cfg); err != nil {
		t.Fatal(err)
	}
	if err := l.saveState(f.st); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FAKECTRL_DIR", f.ctl)
	t.Cleanup(func() { noLeftovers(t, l.dir) })
	return f
}

// plenty is a config whose caps and floor fit any disk the tests run on: the disk the fixture's
// supervisor reads is a fakeDisk, so no real free space is involved.
func plentyConfig() EventLogConfig {
	return EventLogConfig{Caps: eventlog.Caps{Events: 64 << 20, Usage: 64 << 20}, Floor: 1_000_000_000}
}

func (f *ctlFixture) logUp() EntryRecord {
	f.t.Helper()
	return f.awaitEntry(eventLogEntryName, "the event log to run", func(e EntryRecord) bool { return e.State == entryRunning && e.Proc != nil })
}

// awaitFileText waits for a file the fake controller writes in its control directory and returns its
// text, trimmed.
func (f *ctlFixture) awaitFileText(name string) string {
	f.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(filepath.Join(f.ctl, name)); err == nil && len(b) > 0 {
			return strings.TrimSpace(string(b))
		}
		time.Sleep(10 * time.Millisecond)
	}
	f.t.Fatal(f.withChildLogs("the fake controller wrote no " + name))
	return ""
}

func (f *ctlFixture) ctlUp() EntryRecord {
	f.t.Helper()
	return f.awaitEntry(controllerEntryName, "the controller to run", func(e EntryRecord) bool { return e.State == entryRunning && e.Proc != nil })
}

// logReady waits until the log's monitoring port lists both streams: they are made and verified,
// which /healthz alone does not say (it answers once JetStream is up). A log that was killed and
// started again is asked afresh, since the old process's port is closed until the new one binds it.
func (f *ctlFixture) logReady() {
	f.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if eventLogStreamsExist(f.ports.EventLogMonitorAddr()) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	f.t.Fatal(f.withChildLogs("the event log's streams were not listed within 30 s"))
}

// logToken reads the token file the way a client does.
func (f *ctlFixture) logToken() string {
	f.t.Helper()
	b, err := os.ReadFile(f.l.host(f.ports).EventLogTokenFile())
	if err != nil {
		f.t.Fatal(err)
	}
	return strings.TrimSpace(string(b))
}

// logClient connects to the running log with its token.
func (f *ctlFixture) logClient() (*nats.Conn, jetstream.JetStream) {
	f.t.Helper()
	f.logReady()
	nc, err := nats.Connect("nats://"+f.ports.EventLogAddr(), nats.Token(f.logToken()), nats.MaxReconnects(0))
	if err != nil {
		f.t.Fatal(f.withChildLogs(fmt.Sprintf("connect to the event log: %v", err)))
	}
	f.t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		f.t.Fatal(err)
	}
	return nc, js
}

func (f *ctlFixture) publishEvents(js jetstream.JetStream, n int) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for i := 1; i <= n; i++ {
		msg := &nats.Msg{Subject: "hb.ev.instance.u1", Data: make([]byte, 100)}
		if _, err := js.PublishMsg(ctx, msg, jetstream.WithMsgID(fmt.Sprintf("e%04d", i))); err != nil {
			f.t.Fatalf("publish %d: %v", i, err)
		}
	}
}

// streamBytes is a stream's own report of its messages and bytes, from a client.
func (f *ctlFixture) streamState(js jetstream.JetStream, name string) jetstream.StreamState {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s, err := js.Stream(ctx, name)
	if err != nil {
		f.t.Fatal(err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	return info.State
}
