package usage_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
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
