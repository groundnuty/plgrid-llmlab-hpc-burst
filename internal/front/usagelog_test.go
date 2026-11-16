package front

// The front's usage forwarder through Run (HBEP-8, The durable log, When it is down; Test plan, Beta:
// "Usage while the log is down" and "The caps fit the disk"; issue #625). The log is a REAL child
// process, stopped through a pidfd opened at its start (internal/eventlog/node/nodetest), and the
// stream is read back through a fresh client and compared with the usage FILES, never with what the
// front held in memory. /healthz and /metrics are read over HTTP from the front's own health
// listener.
//
// Ports: #625's block, 28510-28519 (IMPL-COMMON). The forwarder package's tests own 28510 and
// 28511; these use 28512 and 28513 for the log, and 28514 for a port with nothing on it, so that
// `go test ./...` can run both packages at once.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog/node/nodetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage/usagetest"
)

var (
	usageLogPorts = nodetest.Ports{Client: 28512, Monitor: 28513}
	// emptyPort is where the front looks for a log that is not running.
	emptyPort = nodetest.Ports{Client: 28514, Monitor: 28515}
)

func TestMain(m *testing.M) {
	code := m.Run()
	nodetest.Cleanup()
	os.Exit(code)
}

// usageFront is a front started with Run, with the health listener on a kernel port.
type usageFront struct {
	t      *testing.T
	f      *Front
	o      Options
	health string // host:port of /healthz and /metrics
	dir    string // the usage directory
	cancel context.CancelFunc
	done   chan error
}

// startUsageFront runs a front. edit changes its Options first; a nil edit runs it without the log.
func startUsageFront(t *testing.T, edit func(*Options)) *usageFront {
	t.Helper()
	dir := t.TempDir()
	keys := filepath.Join(dir, "keys.json")
	writeMode600(t, keys, `{"keys":[{"id":"k","key":"client-key"}]}`)
	hl := kernelListener(t)
	o := Options{
		Config:           Config{ClientKeysFile: keys, UsageDir: filepath.Join(dir, "usage")}.WithDefaults(),
		Clock:            clock.Real{},
		Dynamic:          fakeWatchClient(),
		Recorder:         &MemoryRecorder{},
		Log:              discardLog(),
		ClientListener:   kernelListener(t),
		DialHomeListener: kernelListener(t),
		HealthListener:   hl,
	}
	if edit != nil {
		edit(&o)
	}
	ready := make(chan *Front, 1)
	o.Ready = func(f *Front) { ready <- f }
	ctx, cancel := context.WithCancel(context.Background())
	u := &usageFront{t: t, o: o, health: hl.Addr().String(), dir: o.Config.UsageDir, cancel: cancel, done: make(chan error, 1)}
	go func() { u.done <- Run(ctx, o) }()
	select {
	case u.f = <-ready:
	case err := <-u.done:
		cancel()
		t.Fatalf("Run returned early: %v", err)
	case <-time.After(30 * time.Second):
		cancel()
		t.Fatal("the front did not start")
	}
	t.Cleanup(func() { u.stop() })
	return u
}

// stop stops the front and requires a clean return.
func (u *usageFront) stop() {
	u.cancel()
	select {
	case err := <-u.done:
		if err != nil {
			u.t.Errorf("Run returned %v", err)
		}
		u.done <- nil
	case <-time.After(30 * time.Second):
		u.t.Error("the front did not stop")
	}
}

// withLog is the Options edit that gives a front the durable log at d.
func withLog(d *nodetest.Dir) func(*Options) {
	return func(o *Options) { o.EventLogTokenFile, o.EventLogAddr = d.TokenFile(), d.Ports.Addr() }
}

// usageBlock is the /healthz body's usage member as the front writes it.
type usageBlock struct {
	State         string  `json:"state"`
	LostRecords   uint64  `json:"lostRecords"`
	Log           string  `json:"log"`
	LoggedCounter *uint64 `json:"loggedCounter"`
	BehindSince   string  `json:"behindSince"`
	Raw           map[string]json.RawMessage
}

func (u *usageFront) healthz() usageBlock {
	u.t.Helper()
	resp, err := http.Get("http://" + u.health + "/healthz")
	if err != nil {
		u.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var body struct {
		Usage json.RawMessage `json:"usage"`
	}
	if err := json.Unmarshal(b, &body); err != nil || len(body.Usage) == 0 {
		u.t.Fatalf("GET /healthz: %v; body %s", err, b)
	}
	var blk usageBlock
	if err := json.Unmarshal(body.Usage, &blk); err != nil {
		u.t.Fatalf("the usage member %s: %v", body.Usage, err)
	}
	if err := json.Unmarshal(body.Usage, &blk.Raw); err != nil {
		u.t.Fatal(err)
	}
	return blk
}

// gauge reads hpcburst_usage_unlogged_records from GET /metrics, and whether the series is there.
func (u *usageFront) gauge() (float64, bool) {
	u.t.Helper()
	resp, err := http.Get("http://" + u.health + "/metrics")
	if err != nil {
		u.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return find(u.t, parseSamples(u.t, string(b)), "hpcburst_usage_unlogged_records")
}

func (u *usageFront) await(what string, cond func() bool) {
	u.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			u.t.Fatalf("timed out after 60s waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// write makes n records through the front's own sink, from eight goroutines so that records that
// arrive together share a batch. A Write that returns an error fails the test: a Write never
// depends on the log.
func (u *usageFront) write(n int) {
	u.t.Helper()
	var next atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1))
				if i > n {
					return
				}
				if _, err := u.f.Sink.Write(usage.Record{RequestID: fmt.Sprintf("req-%06d", i), Path: "/v1/chat/completions", Status: 200, ClientKeyID: "k1"}); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		u.t.Fatalf("Write returned %v: a write never depends on the log", err)
	}
}

// skipped reads hpcburst_usage_skipped_total from GET /metrics, and whether the series is there.
func (u *usageFront) skipped() (float64, bool) {
	u.t.Helper()
	resp, err := http.Get("http://" + u.health + "/metrics")
	if err != nil {
		u.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return find(u.t, parseSamples(u.t, string(b)), "hpcburst_usage_skipped_total")
}

// loggedIs is the state in which /healthz and /metrics both say the log holds counter n and the
// front has nothing left to copy.
func (u *usageFront) loggedIs(n uint64) bool {
	blk := u.healthz()
	g, ok := u.gauge()
	return blk.Log == "connected" && blk.LoggedCounter != nil && *blk.LoggedCounter == n && ok && g == 0
}

// HBEP-8, Test plan, Beta, "Usage while the log is down", through the front. Records 1 to 1,000 are
// written and logged, and the gauge reads 0. The log is stopped, and records 1,001 to 1,500 are
// written: every Write returns nil, /healthz says `state` `ok` and `log` `down`, and the gauge reads
// 500. The log restarts; the forwarder reads the stream's last counter, 1,000, and publishes 1,001
// to 1,500. The stream then holds 1 to 1,500, each counter once, each record's bytes equal to its
// line in the files, and the gauge reads 0.
func TestFrontUsageWhileTheLogIsDown(t *testing.T) {
	d := nodetest.NewDir(t, usageLogPorts, eventlog.Caps{Events: 1 << 20, Usage: 64 << 20})
	p := d.Start(t)
	p.Ready()
	u := startUsageFront(t, withLog(d))
	u.await("the forwarder to connect", func() bool { return u.healthz().Log == "connected" })

	u.write(1000)
	u.await("1,000 records logged and the gauge at 0", func() bool { return u.loggedIs(1000) })
	if blk := u.healthz(); blk.State != "ok" || blk.BehindSince != "" {
		t.Errorf("1,000 records logged: state %q, behindSince %q; want ok and none", blk.State, blk.BehindSince)
	}

	p.Stop()
	u.await("/healthz to say the log is down", func() bool { return u.healthz().Log == "down" })
	u.write(500)
	blk := u.healthz()
	if blk.State != "ok" || blk.Log != "down" || blk.LoggedCounter == nil || *blk.LoggedCounter != 1000 || blk.LostRecords != 0 || blk.BehindSince == "" {
		t.Errorf("log down: /healthz usage = %+v; want state ok, log down, loggedCounter 1000, no lost records and a behindSince", blk)
	}
	if g, ok := u.gauge(); !ok || g != 500 {
		t.Errorf("hpcburst_usage_unlogged_records = %v (present %v) with the log down, want 500", g, ok)
	}

	d.Start(t).Ready()
	u.await("the log to hold 1,500 and the gauge to read 0", func() bool { return u.loggedIs(1500) })
	usagetest.CheckStream(t, d, u.dir, usagetest.Range(1, 1500))
	if blk := u.healthz(); blk.BehindSince != "" || blk.State != "ok" {
		t.Errorf("caught up: state %q, behindSince %q; want ok and none", blk.State, blk.BehindSince)
	}
}

// A front started with the log not running, as when the launcher refused it for space (HBEP-8, Test
// plan, "The caps fit the disk": "the front's usage.log says down"): /healthz says `log` `down`, its
// `loggedCounter` is null, and the gauge has no series, since a counter the front has never read is
// unknown and unknown is never 0. The Writes succeed. When the log starts, the front connects by
// itself, copies what was written and reports `connected`.
func TestAFrontStartedBeforeTheLogSaysDownAndConnectsLater(t *testing.T) {
	d := nodetest.NewDir(t, emptyPort, eventlog.Caps{Events: 1 << 20, Usage: 64 << 20})
	nodetest.NothingListens(t, emptyPort.Client)
	u := startUsageFront(t, withLog(d))
	u.write(5)

	blk := u.healthz()
	if blk.State != "ok" || blk.Log != "down" || blk.LoggedCounter != nil {
		t.Errorf("no log: usage = %+v; want state ok, log down, loggedCounter null", blk)
	}
	if raw := string(blk.Raw["loggedCounter"]); raw != "null" {
		t.Errorf("the usage member's loggedCounter is %q, want an explicit null: it is unknown, not 0", raw)
	}
	if g, ok := u.gauge(); ok {
		t.Errorf("hpcburst_usage_unlogged_records = %v while the logged counter is unknown, want no series", g)
	}

	d.Start(t).Ready()
	u.await("the front to connect and copy its 5 records", func() bool { return u.loggedIs(5) })
	usagetest.CheckStream(t, d, u.dir, usagetest.Range(1, 5))
}

// A front started without the durable log (no --event-log-token-file) keeps the usage files alone, as
// in alpha: /healthz says the log is down with no logged counter, and /metrics has no
// hpcburst_usage_unlogged_records series, since the gauge exists only while the front runs with the
// log (the launcher's status tells this front from one whose log is down by its own record).
func TestAFrontWithoutTheLogHasNoGaugeAndSaysDown(t *testing.T) {
	u := startUsageFront(t, nil)
	u.write(3)
	blk := u.healthz()
	if blk.State != "ok" || blk.Log != "down" || blk.LoggedCounter != nil || blk.BehindSince != "" {
		t.Errorf("no log configured: usage = %+v; want state ok, log down, loggedCounter null, no behindSince", blk)
	}
	if g, ok := u.gauge(); ok {
		t.Errorf("hpcburst_usage_unlogged_records = %v on a front without the log, want no series", g)
	}
	if u.f.UsageLog != nil {
		t.Error("a front without the log runs a forwarder")
	}
}

// A token file that cannot be read, or holds no token, stops the start with an error that names it:
// the launcher writes the token before it starts the front, so it is a fault in its setup, never
// something to serve without noticing.
func TestATokenFileThatIsNotATokenStopsTheStart(t *testing.T) {
	dir := t.TempDir()
	notAToken := filepath.Join(dir, "token")
	if err := os.WriteFile(notAToken, []byte("hello\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, path := range map[string]string{"missing": filepath.Join(dir, "absent"), "not a token": notAToken} {
		t.Run(name, func(t *testing.T) {
			keys := filepath.Join(dir, "keys.json")
			writeMode600(t, keys, `{"keys":[{"id":"k","key":"client-key"}]}`)
			o := Options{
				Config: Config{ClientKeysFile: keys, UsageDir: filepath.Join(t.TempDir(), "usage")}.WithDefaults(),
				Clock:  clock.Real{}, Dynamic: fakeWatchClient(), Recorder: &MemoryRecorder{}, Log: discardLog(),
				ClientListener: kernelListener(t), DialHomeListener: kernelListener(t),
				EventLogTokenFile: path,
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			err := Run(ctx, o)
			if err == nil || !strings.Contains(err.Error(), "token") {
				t.Errorf("Run = %v, want an error about the token file", err)
			}
		})
	}
}

// seedLongLastLine puts a usage directory in the state a client can leave it in (review of PR #644):
// the front records a request's model name before it checks it, and a request may be 64 MiB, so a
// record's line can be megabytes long. The newest file ends in such a line, counter 4, of 2 MiB;
// before it are three ordinary records, and the counter file says a block ended at 1,000.
func seedLongLastLine(t *testing.T, usageDir string) {
	t.Helper()
	if err := os.MkdirAll(usageDir, 0o700); err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for n := 1; n <= 3; n++ {
		fmt.Fprintf(&b, `{"counter":%d,"requestID":"r%d"}`+"\n", n, n)
	}
	b.WriteString(`{"counter":4,"requestID":"` + strings.Repeat("m", 2<<20) + `"}` + "\n")
	if err := os.WriteFile(filepath.Join(usageDir, "usage-2026-11-10.jsonl"), []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(usageDir, "counter"), []byte("1000\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

// A usage line over 1 MiB as the last line of the newest file must not stop the front from starting,
// with the log on or off: the sink reads the files' last counter at Open for the forwarder, and that
// read used to fail on such a line. Off, the front starts as before and serves; its records follow the
// counter file's block. On, it starts, serves, copies what it can and says what it could not: the
// stream holds 1-3 and the two new records and not the long one, each byte-equal to the files, the
// long one is counted once as skipped, and the gauge reads 0: a client-written line is not lag.
func TestAFrontStartsOverAUsageFileWhoseLastLineIsLongerThanAMiB(t *testing.T) {
	t.Run("log off", func(t *testing.T) {
		u := startUsageFront(t, func(o *Options) { seedLongLastLine(t, o.Config.UsageDir) })
		u.write(2)
		if blk := u.healthz(); blk.State != "ok" || blk.Log != "down" {
			t.Errorf("usage = %+v; want state ok and, with no log, log down", blk)
		}
		files := usagetest.FileLines(t, u.dir)
		for _, n := range []uint64{1, 2, 3, 4, 1001, 1002} {
			if _, ok := files[n]; !ok {
				t.Errorf("the files hold no counter %d: the front must serve and write after the long line", n)
			}
		}
		if len(files[4]) < 2<<20 {
			t.Errorf("the long record is %d bytes in the files, want it untouched at over 2 MiB", len(files[4]))
		}
	})
	t.Run("log on", func(t *testing.T) {
		d := nodetest.NewDir(t, usageLogPorts, eventlog.Caps{Events: 1 << 20, Usage: 64 << 20})
		d.Start(t).Ready()
		edit := withLog(d)
		u := startUsageFront(t, func(o *Options) { seedLongLastLine(t, o.Config.UsageDir); edit(o) })
		u.write(2)
		u.await("the log to hold 1,002 and the gauge to read 0", func() bool { return u.loggedIs(1002) })
		usagetest.CheckStream(t, d, u.dir, []uint64{1, 2, 3, 1001, 1002})
		if g, ok := u.skipped(); !ok || g != 1 {
			t.Errorf("hpcburst_usage_skipped_total = %v (present %v), want 1: the long record, once", g, ok)
		}
		if blk := u.healthz(); blk.State != "ok" || blk.LostRecords != 0 || blk.BehindSince != "" {
			t.Errorf("usage = %+v; want state ok, no lost records and no behindSince: a long record is neither lost nor lag", blk)
		}
		if got := u.f.UsageLog.Stats().Skipped; got != 1 {
			t.Errorf("the forwarder skipped %d records, want the one long one, once", got)
		}
	})
}

// While the sink's position is unknown (its Open could not read the files' last counter, which is
// logged and never stops the front), the forwarder holds back and the exported series is ABSENT,
// not 0: unknown is never 0 (HBEP-8, Unknown samples; review of PR #644's delta). Here the files hold
// records 1 to 3 that the log lacks, so a 0 would say the opposite of the truth. The series is read
// from the front's own /metrics, not from the forwarder. An older file is made unreadable for the
// start, so that the read fails, and readable again once the front runs, so that the forwarder can
// read the stream and the files; the first record the front then writes makes the position known,
// the forwarder copies 1 to 3, and the series appears, at 0. The test cannot make a file unreadable
// for root.
func TestTheGaugeSeriesIsAbsentWhileTheSinksPositionIsUnknown(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("a file mode does not stop root from reading")
	}
	d := nodetest.NewDir(t, usageLogPorts, eventlog.Caps{Events: 1 << 20, Usage: 64 << 20})
	d.Start(t).Ready()
	var old string
	u := startUsageFront(t, func(o *Options) {
		dir := o.Config.UsageDir
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		old = filepath.Join(dir, "usage-2026-11-10.jsonl")
		text := `{"counter":1,"requestID":"r1"}` + "\n" + `{"counter":2,"requestID":"r2"}` + "\n" + `{"counter":3,"requestID":"r3"}` + "\n"
		for name, content := range map[string]string{old: text, filepath.Join(dir, "usage-2026-11-11.jsonl"): "", filepath.Join(dir, "counter"): "1000\n"} {
			if err := os.WriteFile(name, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.Chmod(old, 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(old, 0o600) })
		withLog(d)(o)
	})
	if err := os.Chmod(old, 0o600); err != nil {
		t.Fatal(err)
	}
	u.await("the forwarder to read the log's last counter", func() bool {
		blk := u.healthz()
		return blk.Log == "connected" && blk.LoggedCounter != nil
	})
	if st := u.f.UsageLog.Stats(); st.SyncedKnown {
		t.Fatalf("the sink's position is known (%+v): the test did not start from the state it is about", st)
	}
	if g, ok := u.gauge(); ok {
		t.Errorf("hpcburst_usage_unlogged_records = %v while the sink's position is unknown, want no series", g)
	}
	if blk := u.healthz(); blk.BehindSince != "" {
		t.Errorf("behindSince = %q while the position is unknown, want none", blk.BehindSince)
	}

	u.write(1)
	u.await("the series to appear, at 0, once 1 to 3 and the new record are in the log", func() bool { return u.loggedIs(1001) })
	usagetest.CheckStream(t, d, u.dir, []uint64{1, 2, 3, 1001})
}

// HBEP-8, The durable log, When it is down (the science agent's ruling on #644, 16 Nov 2026): a record
// passed over for its length is counted ONCE, as a skip, in hpcburst_usage_skipped_total, which starts
// at 0 on a front with the log; it is never lag, so the gauge and behindSince stay as they were, and
// one client-written line does not keep `status` saying the copy is behind. The files hold the record.
// Here the long record is written through the sink while the front runs: its batch reaches the
// forwarder by the channel, the path a request takes.
func TestAnOverLongRecordIsCountedOnceAsASkipAndIsNotLag(t *testing.T) {
	d := nodetest.NewDir(t, usageLogPorts, eventlog.Caps{Events: 1 << 20, Usage: 64 << 20})
	d.Start(t).Ready()
	u := startUsageFront(t, withLog(d))
	u.await("the forwarder to connect", func() bool { return u.healthz().Log == "connected" })
	u.write(2)
	u.await("records 1 and 2 in the log", func() bool { return u.loggedIs(2) })
	if g, ok := u.skipped(); !ok || g != 0 {
		t.Fatalf("hpcburst_usage_skipped_total = %v (present %v) before any skip, want a series at 0", g, ok)
	}

	long := usage.Record{RequestID: "long", RequestedModel: strings.Repeat("m", 2<<20), Path: "/v1/chat/completions", Status: 400}
	if c, err := u.f.Sink.Write(long); err != nil || c != 3 {
		t.Fatalf("Write of the long record = %d, %v; want counter 3 and no error", c, err)
	}
	u.await("the long record to be counted as skipped", func() bool { g, ok := u.skipped(); return ok && g == 1 })
	if _, err := u.f.Sink.Write(usage.Record{RequestID: "after", Path: "/v1/chat/completions", Status: 200}); err != nil {
		t.Fatal(err)
	}
	u.await("record 4 in the log and the gauge at 0", func() bool { return u.loggedIs(4) })
	usagetest.CheckStream(t, d, u.dir, []uint64{1, 2, 4})
	if g, ok := u.skipped(); !ok || g != 1 {
		t.Errorf("hpcburst_usage_skipped_total = %v (present %v) after one long record and one more, want 1", g, ok)
	}
	if blk := u.healthz(); blk.BehindSince != "" || blk.State != "ok" {
		t.Errorf("usage = %+v; want no behindSince and state ok: a skip is not lag", blk)
	}
	if rec := usagetest.FileLines(t, u.dir)[3]; len(rec) < 2<<20 {
		t.Errorf("the files hold counter 3 at %d bytes, want the whole long record", len(rec))
	}
}
