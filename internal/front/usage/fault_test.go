package usage_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
)

// Faults of the disk: a write that puts part of its bytes on the disk and
// fails (a full disk), and a truncate that fails.

var errDisk = errors.New("injected: no space left on device")

// flaky is a log file for the failed-batch scenario. Its first write passes.
// Its second write waits for the gate (the test queues a batch behind it). Its
// third puts all but the last 5 bytes of its buffer on the disk and then
// fails, so it leaves a record cut short, as a full disk does. Later writes
// pass. With failTruncate set, its first truncate fails. Only the sink's
// writer goroutine calls it after Open.
type flaky struct {
	usage.LogHandle
	gate         chan struct{}
	entered      chan struct{}
	writes       int
	failTruncate bool
}

func (f *flaky) Write(p []byte) (int, error) {
	f.writes++
	switch f.writes {
	case 2:
		close(f.entered)
		<-f.gate
	case 3:
		n, _ := f.LogHandle.Write(p[:len(p)-5])
		return n, errDisk
	}
	return f.LogHandle.Write(p)
}

func (f *flaky) Truncate(size int64) error {
	if f.failTruncate {
		f.failTruncate = false
		return errDisk
	}
	return f.LogHandle.Truncate(size)
}

// eventually waits until cond holds.
func eventually(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// receive waits for a value on ch.
func receive[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(20 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

// outcome is what failedBatch saw.
type outcome struct {
	pErr, bErr   error   // what Write returned for P and for B, which were written
	batchErrs    []error // ... for X, Y and Z, the batch that failed
	errAfterFail error   // Err() once X, Y and Z had returned
	failsAfter   uint64  // Failures() then
	qCounter     uint64
	qErr         error  // what Write returned for Q, after the failure
	errAfterQ    error  // Err() once Q had returned
	failsAfterQ  uint64 // Failures() then
	closeErr     error
	logs         *lockedBuf // the sink's log, as JSON lines
}

// lockedBuf is a buffer the sink's writer goroutine and a test can both use.
type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

// records decodes the JSON log lines written so far.
func (l *lockedBuf) records(t *testing.T) []map[string]any {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(l.b.Bytes()), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal(line, &m); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

// failedBatch runs one failed batch on a sink over dir. Record P is written
// and synced. Record B is written behind a blocked write; X, Y and Z, written
// at the same time, queue behind it and are one batch; that batch's write
// fails part way (and, with failTruncate, so does the first truncate that
// follows). Then Q is written. The sink is closed, so the log holds all there
// is to find. The counters are P 1, B 2, X, Y and Z 3 to 5, and Q 6.
func failedBatch(t *testing.T, dir string, failTruncate bool) outcome {
	t.Helper()
	f := &flaky{gate: make(chan struct{}), entered: make(chan struct{}), failTruncate: failTruncate}
	logs := &lockedBuf{}
	s, err := usage.OpenWith(dir, usage.Options{Logger: slog.New(slog.NewJSONHandler(logs, nil)), Now: fixedClock}, func(file *os.File) usage.LogHandle { f.LogHandle = file; return f }, nil)
	if err != nil {
		t.Fatalf("OpenWith: %v", err)
	}
	var o outcome
	o.logs = logs
	_, o.pErr = s.Write(rec("P"))
	bDone := make(chan error, 1)
	go func() { _, err := s.Write(rec("B")); bDone <- err }()
	receive(t, f.entered, "the second write of the log")
	batch := make(chan error, 3)
	for _, id := range []string{"X", "Y", "Z"} {
		go func() { _, err := s.Write(rec(id)); batch <- err }()
	}
	eventually(t, func() bool { return s.Queued() == 3 }, "X, Y and Z to queue behind the blocked write")
	close(f.gate)
	o.bErr = receive(t, bDone, "the write of B")
	for range 3 {
		o.batchErrs = append(o.batchErrs, receive(t, batch, "a write of the failed batch"))
	}
	o.errAfterFail, o.failsAfter = s.Err(), s.Failures()
	o.qCounter, o.qErr = s.Write(rec("Q"))
	o.errAfterQ, o.failsAfterQ = s.Err(), s.Failures()
	o.closeErr = s.Close()
	return o
}

// jsonLines returns the log's records and fails the test unless the log is
// valid JSON Lines: it ends in a newline and each line is one JSON object of
// its own. A line that holds two objects, or a fragment and an object, is
// invalid; readLog would accept the first.
func jsonLines(t *testing.T, dir string) []usage.Record {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, logName))
	if err != nil {
		t.Fatalf("usage log: %v", err)
	}
	if len(b) == 0 {
		return nil
	}
	if b[len(b)-1] != '\n' {
		t.Errorf("the log ends in the middle of a line: %q", tail(b))
	}
	var out []usage.Record
	for i, line := range bytes.Split(bytes.TrimSuffix(b, []byte("\n")), []byte("\n")) {
		var r usage.Record
		if err := json.Unmarshal(line, &r); err != nil {
			t.Errorf("log line %d is not a JSON object: %v: %q", i+1, err, tail(line))
			continue
		}
		out = append(out, r)
	}
	return out
}

// tail is the last 80 bytes of b, for a message.
func tail(b []byte) []byte { return b[max(len(b)-80, 0):] }

func ids(recs []usage.Record) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = r.RequestID
	}
	return out
}

// TestEachWriteOfAFailedBatchReturnsItsError: X, Y and Z are one batch whose
// write failed, so each of their Writes returns the error; the Writes before
// and after it, in batches of their own, return nil.
func TestEachWriteOfAFailedBatchReturnsItsError(t *testing.T) {
	o := failedBatch(t, t.TempDir(), false)
	for i, err := range o.batchErrs {
		if !errors.Is(err, errDisk) {
			t.Errorf("Write %d of the failed batch returned %v, want an error that wraps %v", i, err, errDisk)
		}
	}
	if o.pErr != nil || o.bErr != nil {
		t.Errorf("the Writes of P and B returned %v and %v, want nil", o.pErr, o.bErr)
	}
	if !errors.Is(o.errAfterFail, errDisk) {
		t.Errorf("Err() after the failure = %v, want an error that wraps %v", o.errAfterFail, errDisk)
	}
}

// TestAFailureIsCountedAndLogged: the failed batch counts once, and is not
// counted again by a later success; the logger gets one Error record that names
// the cause and the batch, and nothing else is logged.
func TestAFailureIsCountedAndLogged(t *testing.T) {
	o := failedBatch(t, t.TempDir(), false)
	if o.failsAfter != 1 || o.failsAfterQ != 1 {
		t.Errorf("Failures() = %d after the failed batch and %d after a later success, want 1 and 1", o.failsAfter, o.failsAfterQ)
	}
	logs := o.logs.records(t)
	if len(logs) != 1 {
		t.Fatalf("%d log records, want 1: %v", len(logs), logs)
	}
	l := logs[0]
	if l["level"] != "ERROR" {
		t.Errorf("level = %v, want ERROR", l["level"])
	}
	if cause, _ := l["error"].(string); !strings.Contains(cause, errDisk.Error()) {
		t.Errorf("error = %v, want the cause %q", l["error"], errDisk)
	}
	if l["records"] != float64(3) || l["firstCounter"] != float64(3) || l["lastCounter"] != float64(5) {
		t.Errorf("the record names records=%v firstCounter=%v lastCounter=%v, want 3, 3 and 5", l["records"], l["firstCounter"], l["lastCounter"])
	}
}

// failing is a log file whose writes all fail.
type failing struct{ usage.LogHandle }

func (failing) Write([]byte) (int, error) { return 0, errDisk }

// TestNoLoggerIsFine: a sink opened without a logger (Open) reports a failure
// to the Write and counts it, and does not log or panic.
func TestNoLoggerIsFine(t *testing.T) {
	s, err := usage.OpenWith(t.TempDir(), usage.Options{Now: fixedClock}, func(file *os.File) usage.LogHandle { return failing{file} }, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(rec("a")); !errors.Is(err, errDisk) {
		t.Errorf("Write returned %v, want an error that wraps %v", err, errDisk)
	}
	if got := s.Failures(); got != 1 {
		t.Errorf("Failures() = %d, want 1", got)
	}
	if err := s.Close(); !errors.Is(err, errDisk) {
		t.Errorf("Close returned %v, want the latest batch's error", err)
	}
}

// TestAFailureIsNotSticky: the Write after the failed batch writes its record
// and returns nil; Err is nil again, and Close, which returns the latest
// batch's error, returns nil.
func TestAFailureIsNotSticky(t *testing.T) {
	dir := t.TempDir()
	o := failedBatch(t, dir, false)
	if o.qErr != nil {
		t.Errorf("Write after the failure returned %v, want nil: the disk is fine again", o.qErr)
	}
	if o.qCounter != 6 {
		t.Errorf("Write after the failure returned counter %d, want 6", o.qCounter)
	}
	if o.errAfterQ != nil {
		t.Errorf("Err() after a later success = %v, want nil", o.errAfterQ)
	}
	if o.closeErr != nil {
		t.Errorf("Close returned %v, want nil: the latest batch succeeded", o.closeErr)
	}
	if recs := jsonLines(t, dir); !reflect.DeepEqual(ids(recs), []string{"P", "B", "Q"}) {
		t.Errorf("requests in the log = %v, want [P B Q]: Q is in the log", ids(recs))
	}
}

// TestACounterBlockFailureIsCountedAndLogged: a counter block that cannot be
// persisted refuses the Write, as a failed batch does: Err reports it,
// Failures counts it, and the logger gets an Error record. Nothing is queued,
// no counter is used, and once the disk is fine again the next Write succeeds.
// The block fails here because the counter file's name is a directory, so the
// rename onto it fails (it fails for root too, which a mode bit does not).
func TestACounterBlockFailureIsCountedAndLogged(t *testing.T) {
	dir := t.TempDir()
	logs := &lockedBuf{}
	s, err := usage.OpenOptions(dir, usage.Options{Logger: slog.New(slog.NewJSONHandler(logs, nil)), Now: fixedClock})
	if err != nil {
		t.Fatal(err)
	}
	blocker := filepath.Join(dir, counterName)
	if err := os.Mkdir(blocker, 0o700); err != nil {
		t.Fatal(err)
	}
	if c, err := s.Write(rec("a")); err == nil || c != 0 {
		t.Errorf("Write returned %d, %v, want 0 and an error", c, err)
	}
	if s.Err() == nil || s.Failures() != 1 {
		t.Errorf("Err() = %v and Failures() = %d after the failed block, want an error and 1", s.Err(), s.Failures())
	}
	got := logs.records(t)
	if len(got) != 1 || got[0]["level"] != "ERROR" || got[0]["error"] == nil {
		t.Errorf("log = %v, want one Error record that names the cause", got)
	}
	// HBEP-8: a failed reservation drops its record before it has a counter,
	// and the failure counter is its only trace.
	if st := s.Stats(); st.Lost != 1 || st.Records != 0 {
		t.Errorf("Stats() = %+v after the failed block, want 1 record lost and none written", st)
	}
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	if c, err := s.Write(rec("b")); err != nil || c != 1 {
		t.Errorf("Write after the disk recovered returned %d, %v, want 1 and nil", c, err)
	}
	if s.Err() != nil || s.Failures() != 1 {
		t.Errorf("Err() = %v and Failures() = %d after it, want nil and 1", s.Err(), s.Failures())
	}
	if st := s.Stats(); st.Lost != 1 || st.Records != 1 {
		t.Errorf("Stats() = %+v after the recovery, want the lost record kept and 1 written", st)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if recs := jsonLines(t, dir); !reflect.DeepEqual(ids(recs), []string{"b"}) {
		t.Errorf("requests in the log = %v, want [b]", ids(recs))
	}
}

// TestAFailedWriteLeavesNoPartialLine: the write of the batch X, Y, Z puts
// part of it on the disk and fails. The log is cut back to its size before the
// batch, so Q, written after the failure, starts a line of its own. The
// batch's counters 3 to 5 show as a gap.
func TestAFailedWriteLeavesNoPartialLine(t *testing.T) {
	dir := t.TempDir()
	failedBatch(t, dir, false)
	recs := jsonLines(t, dir)
	if want := []string{"P", "B", "Q"}; !reflect.DeepEqual(ids(recs), want) {
		t.Errorf("requests in the log = %v, want %v", ids(recs), want)
	}
	if want := []uint64{1, 2, 6}; !reflect.DeepEqual(counters(recs), want) {
		t.Errorf("counters in the log = %v, want %v: the failed batch shows as a gap", counters(recs), want)
	}
}

// TestAFailedCutBackIsRetried: the truncate that follows the failed write
// fails too, so the partial line stays and the batch's Writes return both
// errors. The next flush cuts the log back before it writes Q.
func TestAFailedCutBackIsRetried(t *testing.T) {
	dir := t.TempDir()
	o := failedBatch(t, dir, true)
	for i, err := range o.batchErrs {
		if !errors.Is(err, errDisk) {
			t.Errorf("Write %d of the failed batch returned %v, want an error that wraps %v", i, err, errDisk)
		}
	}
	if o.qErr != nil {
		t.Errorf("Write after the failure returned %v, want nil", o.qErr)
	}
	recs := jsonLines(t, dir)
	if want := []string{"P", "B", "Q"}; !reflect.DeepEqual(ids(recs), want) {
		t.Errorf("requests in the log = %v, want %v", ids(recs), want)
	}
}

// syncGate is a log file whose Sync blocks until released.
type syncGate struct {
	usage.LogHandle
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *syncGate) Sync() error {
	f.entered <- struct{}{}
	<-f.release
	return f.LogHandle.Sync()
}

func (f *syncGate) unblock() { f.once.Do(func() { close(f.release) }) }

// TestWriteReturnsOnlyAfterTheSync: with the log's sync blocked, the record is
// in the log but its Write has not returned; once the sync is released, Write
// returns nil.
func TestWriteReturnsOnlyAfterTheSync(t *testing.T) {
	dir := t.TempDir()
	f := &syncGate{entered: make(chan struct{}, 1), release: make(chan struct{})}
	s, err := usage.OpenWith(dir, usage.Options{Now: fixedClock}, func(file *os.File) usage.LogHandle { f.LogHandle = file; return f }, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.unblock(); _ = s.Close() })
	done := make(chan error, 1)
	go func() { _, err := s.Write(rec("a")); done <- err }()
	receive(t, f.entered, "the sync of the log")
	if n := logLines(dir); n != 1 {
		t.Errorf("%d lines in the log while the sync is blocked, want 1: the write comes before the sync", n)
	}
	select {
	case err := <-done:
		t.Fatalf("Write returned %v while the sync of its record was blocked", err)
	case <-time.After(200 * time.Millisecond):
	}
	f.unblock()
	if err := receive(t, done, "Write to return once the sync is released"); err != nil {
		t.Errorf("Write returned %v after the sync, want nil", err)
	}
}

// TestOpenCutsATornTail: a crash in the middle of a write leaves a log that
// ends in a partial line. Open cuts it back to the last whole line, so the
// first record of the next run is a line of its own and the older records
// survive. The log is written here by hand, as the crash would leave it; the
// counter file holds the first block's end, as it does after a crash.
func TestOpenCutsATornTail(t *testing.T) {
	const (
		line1 = `{"counter":1,"requestID":"a","path":"/v1/chat/completions","status":200}` + "\n"
		line2 = `{"counter":2,"requestID":"b","path":"/v1/chat/completions","status":200}` + "\n"
		line3 = `{"counter":3,"requestID":"c","path":"/v1/chat/completions","status":200}`
	)
	torn := line3[:40] // a real prefix of a record, with no newline
	cases := []struct {
		name    string
		before  string
		kept    string // the log must start with this after Open
		records int    // the whole lines in kept
	}{
		{"a partial line after whole ones", line1 + line2 + torn, line1 + line2, 2},
		{"only a partial line", torn, "", 0},
		{"a partial line longer than a read", line1 + `{"requestID":"` + strings.Repeat("x", 10000), line1, 1},
		{"a record cut just before its newline", line1 + line3, line1, 1},
		{"whole lines are left alone", line1 + line2, line1 + line2, 2},
		{"an empty log", "", "", 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, logName), []byte(c.before), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, counterName), []byte("1000\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			s := sink(t, dir)
			if got := write(t, s, rec("new")); got != 1001 {
				t.Errorf("counter = %d, want 1001", got)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(filepath.Join(dir, logName))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(string(got), c.kept) {
				t.Errorf("the log starts with %q, want it to start with %q", tail(got[:min(len(got), len(c.kept)+40)]), c.kept)
			}
			recs := jsonLines(t, dir)
			if len(recs) != c.records+1 {
				t.Fatalf("%d records in the log, want %d: the older ones and the new one", len(recs), c.records+1)
			}
			if last := recs[len(recs)-1]; last.RequestID != "new" || last.Counter != 1001 {
				t.Errorf("last record = %q counter %d, want new, 1001", last.RequestID, last.Counter)
			}
		})
	}
}

// TestCloseAfterAFailureKeepsTheBlockEnd: the batch X, Y, Z was lost, so the
// run was not clean, though a later batch was written. Close leaves the
// counter file at the block end, as a crash does, and the next run starts at
// 1001.
func TestCloseAfterAFailureKeepsTheBlockEnd(t *testing.T) {
	dir := t.TempDir()
	failedBatch(t, dir, false)
	if got := counterFile(t, dir); got != "1000" {
		t.Errorf("counter file after a run that had a failed batch = %q, want 1000", got)
	}
	s := sink(t, dir)
	if got := write(t, s, rec("next")); got != 1001 {
		t.Errorf("the first counter after it = %d, want 1001", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}
