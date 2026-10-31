package usage_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
)

// Faults of the disk: a write that puts part of its bytes on the disk and
// fails (a full disk), and a truncate that fails.

var errDisk = errors.New("injected: no space left on device")

// flaky is a log file whose second write puts all but the last 5 bytes of its
// buffer on the disk and then fails, so it leaves a record cut short, as a
// full disk does. Its first write waits for the gate, so that a test can queue
// a batch behind it. With failTruncate set, its first truncate fails. Only the
// sink's writer goroutine calls it after Open.
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
	case 1:
		close(f.entered)
		<-f.gate
	case 2:
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

// outcome is what failedBatch saw.
type outcome struct {
	qCounter uint64
	qErr     error // what Write(Q) returned
	closeErr error
}

// failedBatch runs one failed write on a sink over dir. Record P is written
// and synced. Then X, Y and Z are queued behind a blocked write, so that they
// are one batch, and that batch's write fails part way (and, with
// failTruncate, so does the first truncate that follows). Then Q is written.
// The sink is closed, so the log holds all there is to find.
func failedBatch(t *testing.T, dir string, failTruncate bool) outcome {
	t.Helper()
	f := &flaky{gate: make(chan struct{}), entered: make(chan struct{}), failTruncate: failTruncate}
	s, err := usage.OpenWith(dir, func(file *os.File) usage.LogHandle { f.LogHandle = file; return f })
	if err != nil {
		t.Fatalf("OpenWith: %v", err)
	}
	write(t, s, rec("P"))
	select {
	case <-f.entered:
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for the first write of the log")
	}
	for _, id := range []string{"X", "Y", "Z"} {
		write(t, s, rec(id))
	}
	close(f.gate)
	eventually(t, func() bool { return s.Err() != nil }, "the failed write to be recorded")
	var o outcome
	o.qCounter, o.qErr = s.Write(rec("Q"))
	o.closeErr = s.Close()
	return o
}

// TestWriteReturnsAnEarlierWriteFailure: once a write of the log has failed,
// Write returns that error instead of nil (with the counter, for the record is
// still queued), and Close reports it too.
func TestWriteReturnsAnEarlierWriteFailure(t *testing.T) {
	o := failedBatch(t, t.TempDir(), false)
	if !errors.Is(o.qErr, errDisk) {
		t.Errorf("Write after the failure returned %v, want an error that wraps %v", o.qErr, errDisk)
	}
	if o.qCounter != 5 {
		t.Errorf("Write after the failure returned counter %d, want 5: the record is still queued", o.qCounter)
	}
	if !errors.Is(o.closeErr, errDisk) {
		t.Errorf("Close returned %v, want an error that wraps %v", o.closeErr, errDisk)
	}
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

// TestAFailedWriteLeavesNoPartialLine: the write of the batch X, Y, Z puts
// part of it on the disk and fails. The log is cut back to its size before the
// batch, so Q, written after the failure, starts a line of its own. The
// batch's counters 2 to 4 show as a gap.
func TestAFailedWriteLeavesNoPartialLine(t *testing.T) {
	dir := t.TempDir()
	failedBatch(t, dir, false)
	recs := jsonLines(t, dir)
	if want := []string{"P", "Q"}; !reflect.DeepEqual(ids(recs), want) {
		t.Errorf("requests in the log = %v, want %v", ids(recs), want)
	}
	if want := []uint64{1, 5}; !reflect.DeepEqual(counters(recs), want) {
		t.Errorf("counters in the log = %v, want %v: the failed batch shows as a gap", counters(recs), want)
	}
}

// TestAFailedCutBackIsRetried: the truncate that follows the failed write
// fails too, so the partial line stays. The next flush cuts the log back
// before it writes Q.
func TestAFailedCutBackIsRetried(t *testing.T) {
	dir := t.TempDir()
	failedBatch(t, dir, true)
	recs := jsonLines(t, dir)
	if want := []string{"P", "Q"}; !reflect.DeepEqual(ids(recs), want) {
		t.Errorf("requests in the log = %v, want %v", ids(recs), want)
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
// run was not clean. Close leaves the counter file at the block end, as a
// crash does, and the next run starts at 1001.
func TestCloseAfterAFailureKeepsTheBlockEnd(t *testing.T) {
	dir := t.TempDir()
	failedBatch(t, dir, false)
	if got := counterFile(t, dir); got != "1000" {
		t.Errorf("counter file after a Close that reported an error = %q, want 1000", got)
	}
	s := sink(t, dir)
	if got := write(t, s, rec("next")); got != 1001 {
		t.Errorf("the first counter after it = %d, want 1001", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}
