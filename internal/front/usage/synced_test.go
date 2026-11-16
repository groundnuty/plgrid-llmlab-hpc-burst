package usage_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
)

// The sink's post-sync hook (HBEP-8, The durable log, Who writes: "the front's forwarder takes only
// what the sink has synced"). The expectation is always the FILE: the bytes the hook was given must
// be the bytes on disk, never what the sink encoded in memory.

// syncedCall is one call of Options.Synced.
type syncedCall struct {
	first, last uint64
	lines       []byte
}

// recorder collects the hook's calls; the hook runs on the sink's writer goroutine.
type recorder struct {
	mu    sync.Mutex
	calls []syncedCall
}

func (r *recorder) hook(first, last uint64, lines []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, syncedCall{first, last, lines})
}

func (r *recorder) snapshot() []syncedCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]syncedCall(nil), r.calls...)
}

// A Write that returned has been offered: the hook ran before it returned. Each call's bytes are
// the batch's lines, each with its newline, and all the calls together are the file, byte for byte.
func TestSyncedGetsTheBytesOfEveryBatchAsTheFileHoldsThem(t *testing.T) {
	dir := t.TempDir()
	r := &recorder{}
	s, err := usage.OpenOptions(dir, usage.Options{Now: fixedClock, Synced: r.hook})
	if err != nil {
		t.Fatal(err)
	}
	// Records with characters encoding/json escapes (<, >, &) and non-ASCII text, which it writes
	// as \u003c and the like and leaves raw: the bytes the hook gets must be those.
	for i := 1; i <= 20; i++ {
		rc := rec(fmt.Sprintf("r%d<a>&b-é", i))
		if got := write(t, s, rc); got != uint64(i) {
			t.Fatalf("record %d got counter %d", i, got)
		}
		calls := r.snapshot()
		if len(calls) == 0 || calls[len(calls)-1].last != uint64(i) {
			t.Fatalf("after Write returned for counter %d, the hook has seen %+v: it must have run before Write returned", i, calls)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	onDisk, err := os.ReadFile(filepath.Join(dir, logName))
	if err != nil {
		t.Fatal(err)
	}
	var offered []byte
	next := uint64(1)
	for _, c := range r.snapshot() {
		if c.first != next {
			t.Errorf("a batch begins at counter %d, want %d: the batches must follow each other", c.first, next)
		}
		if n := uint64(bytes.Count(c.lines, []byte("\n"))); n != c.last-c.first+1 || !bytes.HasSuffix(c.lines, []byte("\n")) {
			t.Errorf("batch %d to %d holds %d lines, ending in a newline: %v", c.first, c.last, n, bytes.HasSuffix(c.lines, []byte("\n")))
		}
		next = c.last + 1
		offered = append(offered, c.lines...)
	}
	if next != 21 {
		t.Errorf("the batches end at counter %d, want 20", next-1)
	}
	if !bytes.Equal(offered, onDisk) {
		t.Errorf("the bytes the hook was given are not the file's bytes:\nhook: %q\nfile: %q", offered, onDisk)
	}
	if !bytes.Contains(onDisk, []byte(`r1\u003ca\u003e\u0026b-é"`)) {
		t.Errorf("the fixture's own check: the file does not hold record 1's escaped text; its first line: %q", bytes.SplitN(onDisk, []byte("\n"), 2)[0])
	}
}

// failingFile fails every write while failing is set, after putting nothing on disk.
type failingFile struct {
	usage.LogHandle
	failing *bool
}

func (f failingFile) Write(p []byte) (int, error) {
	if *f.failing {
		return 0, errors.New("injected: write failed")
	}
	return f.LogHandle.Write(p)
}

// A batch that failed is not offered, and Stats.LastSynced does not move for it; the next batch
// that succeeds is offered, and LastSynced moves to its last counter. The failed batch's counters
// are the gap between them.
func TestSyncedIsNotCalledForABatchThatFailed(t *testing.T) {
	dir := t.TempDir()
	r := &recorder{}
	failing := false
	s, err := usage.OpenWith(dir, usage.Options{Now: fixedClock, Synced: r.hook},
		func(f *os.File) usage.LogHandle { return failingFile{LogHandle: f, failing: &failing} }, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	write(t, s, rec("a")) // counter 1
	if got := s.Stats().LastSynced; got != 1 {
		t.Fatalf("LastSynced = %d after counter 1, want 1", got)
	}
	failing = true
	if _, err := s.Write(rec("b")); err == nil { // counter 2, lost
		t.Fatal("the write that was made to fail returned nil")
	}
	if got := s.Stats().LastSynced; got != 1 {
		t.Errorf("LastSynced = %d after a failed batch, want 1", got)
	}
	failing = false
	write(t, s, rec("c")) // counter 3
	calls := r.snapshot()
	if len(calls) != 2 || calls[0].first != 1 || calls[0].last != 1 || calls[1].first != 3 || calls[1].last != 3 {
		t.Fatalf("the hook's calls are %+v, want counters 1 and 3 only", calls)
	}
	if got := s.Stats().LastSynced; got != 3 {
		t.Errorf("LastSynced = %d after counter 3, want 3", got)
	}
}

// LastSynced starts from the files, not from the first write of the run: what the files hold at
// Open, after the repair of a partial last line, was synced by an earlier run (HBEP-8: the gauge is
// the last synced counter minus the last logged one, and must not read from 0 after a restart).
func TestLastSyncedStartsFromTheFilesAtOpen(t *testing.T) {
	t.Run("no file", func(t *testing.T) {
		s := sink(t, t.TempDir())
		defer s.Close()
		if got := s.Stats().LastSynced; got != 0 {
			t.Errorf("LastSynced = %d on an empty directory, want 0", got)
		}
	})
	t.Run("a partial last line is not counted", func(t *testing.T) {
		dir := t.TempDir()
		// Hand-written: records 7 and 8 whole, record 9 cut short, in the newest file; and an older
		// file holding 3. The partial line carries no newline, so it is repaired away at Open.
		old := `{"counter":3,"requestID":"x"}` + "\n"
		newest := `{"counter":7,"requestID":"x"}` + "\n" + `{"counter":8,"requestID":"x"}` + "\n" + `{"counter":9,"reques`
		writeFile(t, dir, "usage-2026-10-30.jsonl", old)
		writeFile(t, dir, "usage-2026-10-31.jsonl", newest)
		s := sink(t, dir)
		defer s.Close()
		if got := s.Stats().LastSynced; got != 8 {
			t.Errorf("LastSynced = %d, want 8 (the last whole line)", got)
		}
	})
	t.Run("an empty newest file is passed over", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, dir, "usage-2026-10-30.jsonl", `{"counter":41,"requestID":"x"}`+"\n")
		writeFile(t, dir, "usage-2026-10-31.jsonl", "")
		s := sink(t, dir)
		defer s.Close()
		if got := s.Stats().LastSynced; got != 41 {
			t.Errorf("LastSynced = %d, want 41", got)
		}
	})
	t.Run("a record written moves it", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, dir, "usage-2026-10-31.jsonl", `{"counter":50,"requestID":"x"}`+"\n")
		if err := os.WriteFile(filepath.Join(dir, "counter"), []byte("1000\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		s := sink(t, dir)
		defer s.Close()
		if got := write(t, s, rec("r")); got != 1001 {
			t.Fatalf("the next counter is %d, want 1001 (the block end plus one: a gap)", got)
		}
		if got := s.Stats().LastSynced; got != 1001 {
			t.Errorf("LastSynced = %d, want 1001", got)
		}
	})
}

func writeFile(t *testing.T, dir, name, text string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
}

// LineCounter reads the counter of a line the sink wrote, and of a line another encoder wrote.
// Every case is hand-written from the file format: one JSON object per line, "counter" first.
func TestLineCounter(t *testing.T) {
	for _, c := range []struct {
		name string
		line string
		want uint64
		ok   bool
	}{
		{"the sink's own line", `{"counter":12,"requestID":"a"}` + "\n", 12, true},
		{"without its newline", `{"counter":7}`, 7, true},
		{"the largest counter", `{"counter":18446744073709551615,"requestID":"a"}`, 18446744073709551615, true},
		{"a space after the colon", `{"counter": 5, "requestID":"a"}`, 5, true},
		{"counter not first", `{"requestID":"a","counter":9}`, 9, true},
		{"too large for 64 bits", `{"counter":18446744073709551616}`, 0, false},
		{"no counter", `{"requestID":"a"}`, 0, false},
		{"not JSON", `counter 5`, 0, false},
		{"a negative counter", `{"counter":-1}`, 0, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, ok := usage.LineCounter([]byte(c.line))
			if got != c.want || ok != c.ok {
				t.Errorf("LineCounter(%q) = %d, %v; want %d, %v", c.line, got, ok, c.want, c.ok)
			}
		})
	}
}

// LastCounter reads the end of the newest file that holds a whole line.
func TestLastCounter(t *testing.T) {
	line := func(n int) string { return fmt.Sprintf(`{"counter":%d,"requestID":"x"}`+"\n", n) }
	for _, c := range []struct {
		name  string
		files map[string]string
		want  uint64
	}{
		{"no files", nil, 0},
		{"one file", map[string]string{"usage-2026-10-31.jsonl": line(1) + line(2)}, 2},
		{"the legacy file only", map[string]string{"usage.jsonl": line(4) + line(5) + line(6)}, 6},
		{"the newest is empty", map[string]string{"usage-2026-10-30.jsonl": line(3), "usage-2026-10-31.jsonl": ""}, 3},
		{"the newest holds a cut line only", map[string]string{"usage-2026-10-30.jsonl": line(3), "usage-2026-10-31.jsonl": `{"counter":4,"re`}, 3},
		{"a gap", map[string]string{"usage-2026-10-30.jsonl": line(3), "usage-2026-10-31.jsonl": line(2001)}, 2001},
		{"a file larger than the read chunk", map[string]string{"usage-2026-10-31.jsonl": bigFile(5000)}, 5000},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, text := range c.files {
				writeFile(t, dir, name, text)
			}
			got, err := usage.LastCounter(dir)
			if err != nil || got != c.want {
				t.Errorf("LastCounter = %d, %v; want %d", got, err, c.want)
			}
		})
	}
}

// bigFile is a file of n lines of about 100 bytes, counters 1 to n: a few chunks of the reader.
func bigFile(n int) string {
	var b bytes.Buffer
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, `{"counter":%d,"requestID":"%080d"}`+"\n", i, i)
	}
	return b.String()
}

// longLine is a usage line of `size` bytes with its newline whose counter is n, in the form the sink
// writes it: a record whose requested model's name is as long as the client made it. The front
// records the name before it checks it, and a request may be 64 MiB, so such a line is real.
func longLine(n, size int) string {
	prefix := fmt.Sprintf(`{"counter":%d,"requestID":"`, n)
	return prefix + strings.Repeat("m", size-len(prefix)-3) + "\"}\n"
}

// FileLastCounter and LastCounter never fail on a line of any length, and read its counter: it is
// the record's first member. A last line over 1 MiB used to be an error, which stopped the sink from
// opening and so the front from starting (review of PR #644).
func TestLastCounterOfALineLongerThanAMiB(t *testing.T) {
	for _, c := range []struct {
		name  string
		files map[string]string
		want  uint64
	}{
		{"the last line is 3 MiB", map[string]string{"usage-2026-10-31.jsonl": bigFile(3) + longLine(77, 3<<20)}, 77},
		{"the only line is 3 MiB", map[string]string{"usage-2026-10-31.jsonl": longLine(5, 3<<20)}, 5},
		{"just over the bound", map[string]string{"usage-2026-10-31.jsonl": bigFile(2) + longLine(9, 1<<20+1)}, 9},
		{"a long line that is not the last is not read at all", map[string]string{"usage-2026-10-31.jsonl": longLine(8, 3<<20) + bigFile(1)}, 1},
		{"the newest file's long last line wins over an older file", map[string]string{"usage-2026-10-30.jsonl": bigFile(40), "usage-2026-10-31.jsonl": longLine(2001, 2<<20)}, 2001},
		{"a long last line with no counter in its first bytes falls back to the older file, with no error",
			map[string]string{"usage-2026-10-30.jsonl": bigFile(40), "usage-2026-10-31.jsonl": `{"requestID":"` + strings.Repeat("m", 3<<20) + "\"}\n"}, 40},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, text := range c.files {
				writeFile(t, dir, name, text)
			}
			got, err := usage.LastCounter(dir)
			if err != nil || got != c.want {
				t.Errorf("LastCounter = %d, %v; want %d, nil", got, err, c.want)
			}
		})
	}
	t.Run("FileLastCounter says so itself", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, dir, "usage-2026-10-31.jsonl", bigFile(3)+longLine(77, 3<<20))
		n, ok, err := usage.FileLastCounter(filepath.Join(dir, "usage-2026-10-31.jsonl"))
		if err != nil || !ok || n != 77 {
			t.Errorf("FileLastCounter = %d, %v, %v; want 77, true, nil", n, ok, err)
		}
	})
}

// The sink opens over a file whose last line is over 1 MiB, and serves: Open reads the files' last
// counter for the durable log's forwarder, whether the log is on or not, and a line of that length
// must not stop it (review of PR #644). The counter is the line's, 77, and the next record follows
// the counter file's block as always.
func TestOpenOverALastLineLongerThanAMiB(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, logName, bigFile(3)+longLine(77, 3<<20))
	if err := os.WriteFile(filepath.Join(dir, "counter"), []byte("1000\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := sink(t, dir)
	defer s.Close()
	if st := s.Stats(); st.LastSynced != 77 || !st.LastSyncedKnown {
		t.Errorf("LastSynced = %d (known %v), want 77, known", st.LastSynced, st.LastSyncedKnown)
	}
	if got := write(t, s, rec("after")); got != 1001 {
		t.Errorf("the next counter is %d, want 1001", got)
	}
}

// Open never fails on the forwarder's bookkeeping: when the files' last counter cannot be read at
// all (here an older file the sink may not open, with the newest one empty so that the sink's own
// repair has nothing to read), the sink opens and serves, LastSynced is unknown instead of 0, and the
// first batch that is synced makes it known. The test cannot make a file unreadable for root.
func TestOpenDoesNotFailWhenTheLastCounterCannotBeRead(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("a file mode does not stop root from reading")
	}
	dir := t.TempDir()
	old := filepath.Join(dir, "usage-2026-10-30.jsonl")
	writeFile(t, dir, "usage-2026-10-30.jsonl", `{"counter":3,"requestID":"x"}`+"\n")
	writeFile(t, dir, logName, "")
	if err := os.Chmod(old, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(old, 0o600) })
	s := sink(t, dir)
	defer s.Close()
	if st := s.Stats(); st.LastSyncedKnown || st.LastSynced != 0 {
		t.Errorf("LastSynced = %d, known %v; want it unknown (0, not known) when the files could not be read", st.LastSynced, st.LastSyncedKnown)
	}
	got := write(t, s, rec("r"))
	if st := s.Stats(); !st.LastSyncedKnown || st.LastSynced != got {
		t.Errorf("after counter %d was synced: LastSynced = %d, known %v; want %d, known", got, st.LastSynced, st.LastSyncedKnown, got)
	}
}
