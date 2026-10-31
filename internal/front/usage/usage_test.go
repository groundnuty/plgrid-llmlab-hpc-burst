package usage_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
)

// HBEP-5, Usage records, Counter and Sink in alpha. The expected values are
// HBEP-5's text: counters start at 1; the front reserves them in blocks of
// 1,000 and syncs the block's end to a counter file before it uses the first
// one; after a restart it starts at the persisted end plus one, so lost
// records show as a gap. File and member names are written as literals.

const (
	logName     = "usage.jsonl"
	counterName = "counter"
)

// sink opens a Sink in dir and fails the test when it cannot.
func sink(t *testing.T, dir string) *usage.Sink {
	t.Helper()
	s, err := usage.Open(dir)
	if err != nil {
		t.Fatalf("Open(%s): %v", dir, err)
	}
	return s
}

// write queues r and returns its counter.
func write(t *testing.T, s *usage.Sink, r usage.Record) uint64 {
	t.Helper()
	c, err := s.Write(r)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	return c
}

// counterFile returns the counter file's text, trimmed.
func counterFile(t *testing.T, dir string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, counterName))
	if err != nil {
		t.Fatalf("counter file: %v", err)
	}
	return strings.TrimSpace(string(b))
}

// readLog decodes the usage log's lines.
func readLog(t *testing.T, dir string) []usage.Record {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, logName))
	if err != nil {
		t.Fatalf("usage log: %v", err)
	}
	var out []usage.Record
	dec := json.NewDecoder(bytes.NewReader(b))
	for dec.More() {
		var r usage.Record
		if err := dec.Decode(&r); err != nil {
			t.Fatalf("usage log line %d: %v", len(out)+1, err)
		}
		out = append(out, r)
	}
	return out
}

// logLines counts the complete lines of the usage log so far.
func logLines(dir string) int {
	b, err := os.ReadFile(filepath.Join(dir, logName))
	if err != nil {
		return 0
	}
	return bytes.Count(b, []byte("\n"))
}

func counters(recs []usage.Record) []uint64 {
	out := make([]uint64, len(recs))
	for i, r := range recs {
		out[i] = r.Counter
	}
	return out
}

func rec(id string) usage.Record {
	return usage.Record{RequestID: id, Path: "/v1/chat/completions", Status: 200}
}

// TestFirstCounterOnAnEmptyDirIsOne: Open on an empty directory starts at 1,
// and the counter file holds 1000 (the first block's end) by the time the
// first record is in the log.
func TestFirstCounterOnAnEmptyDirIsOne(t *testing.T) {
	dir := t.TempDir()
	s := sink(t, dir)
	if got := write(t, s, rec("r1")); got != 1 {
		t.Errorf("first counter = %d, want 1", got)
	}
	// Watch the log; the moment its first line is there, the counter file
	// must already hold the block end.
	deadline := time.Now().Add(5 * time.Second)
	for logLines(dir) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the first record never reached the log")
		}
		time.Sleep(time.Millisecond)
	}
	if got := counterFile(t, dir); got != "1000" {
		t.Errorf("counter file = %q once the first record is in the log, want 1000", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestCountersAreReservedInBlocksOf1000: counters 1 to 1000 share the first
// block; the 1001st counter takes a second block, whose end (2000) is synced
// to the counter file first.
func TestCountersAreReservedInBlocksOf1000(t *testing.T) {
	dir := t.TempDir()
	s := sink(t, dir)
	for i := 1; i <= 1000; i++ {
		if got := write(t, s, rec(fmt.Sprint("r", i))); got != uint64(i) {
			t.Fatalf("record %d got counter %d", i, got)
		}
	}
	if got := counterFile(t, dir); got != "1000" {
		t.Errorf("after 1000 records the counter file = %q, want 1000", got)
	}
	if got := write(t, s, rec("r1001")); got != 1001 {
		t.Errorf("the 1001st counter = %d, want 1001", got)
	}
	if got := counterFile(t, dir); got != "2000" {
		t.Errorf("after the 1001st record the counter file = %q, want 2000", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if recs := readLog(t, dir); len(recs) != 1001 {
		t.Errorf("%d records in the log, want 1001", len(recs))
	}
}

// TestRestartStartsAtTheNextBlock: a second Open on the same directory (a
// restart) starts at 1001, so the records the first run did not write show as
// a gap.
func TestRestartStartsAtTheNextBlock(t *testing.T) {
	dir := t.TempDir()
	s := sink(t, dir)
	for _, id := range []string{"a", "b", "c"} {
		write(t, s, rec(id))
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := sink(t, dir)
	if got := write(t, s2, rec("d")); got != 1001 {
		t.Errorf("the first counter after a restart = %d, want 1001", got)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	recs := readLog(t, dir)
	if want := []uint64{1, 2, 3, 1001}; !reflect.DeepEqual(counters(recs), want) {
		t.Errorf("counters in the log = %v, want %v: the gap must show", counters(recs), want)
	}
	if got := counterFile(t, dir); got != "2000" {
		t.Errorf("counter file = %q, want 2000", got)
	}
}

// TestOpenReadsAnExistingCounterFile: the persisted end plus one is the first
// counter, whatever the block alignment; a file that holds no number is an
// error that names it.
func TestOpenReadsAnExistingCounterFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, counterName), []byte("5000\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := sink(t, dir)
	if got := write(t, s, rec("a")); got != 5001 {
		t.Errorf("first counter = %d, want 5001", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	bad := t.TempDir()
	if err := os.WriteFile(filepath.Join(bad, counterName), []byte("not a number\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if s, err := usage.Open(bad); err == nil {
		_ = s.Close()
		t.Errorf("Open accepted a counter file that holds no number")
	} else if !strings.Contains(err.Error(), counterName) {
		t.Errorf("the error does not name the counter file: %v", err)
	}
}

// full is a record with every field set to a distinct value.
func full(i int) usage.Record {
	n := int64(i)
	s := func(v string) *string { return &v }
	return usage.Record{
		RequestID: fmt.Sprintf("req-%d", i), StartTime: "2026-10-31T12:00:00.000Z", EndTime: "2026-10-31T12:00:01.250Z",
		ClientKeyID: "client-a", Path: "/v1/chat/completions", RequestedModel: "example-27b@helios-a/0123456789ab",
		Level: "pinned", Provider: "helios-a", ModelDeployment: "ns/md", DeploymentID: strings.Repeat("ab", 32),
		Instance: "ns/md-0", InstanceUID: "uid-1", Emitter: "instance", Status: 200, Stream: true,
		Finish: s("stop"), CutBy: s("lease_end"), PromptTokens: &n, CachedTokens: ptr(n + 1), CompletionTokens: ptr(n + 2),
		CountsSource: s("engine"), FirstTokenMs: ptr(n + 3), DurationMs: n + 4, ContentEvents: ptr(n + 5),
	}
}

func ptr(v int64) *int64 { return &v }

// TestRecordsAreInTheLogAfterClose: after Write and Close, a re-read of the
// log holds every record with all its fields, one JSON line each. Writers
// racing each other are batched into shared syncs and still all arrive.
func TestRecordsAreInTheLogAfterClose(t *testing.T) {
	dir := t.TempDir()
	s := sink(t, dir)
	const n = 200
	var mu sync.Mutex
	want := map[string]usage.Record{}
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := full(i)
			c, err := s.Write(r)
			if err != nil {
				t.Errorf("Write: %v", err)
				return
			}
			r.Counter = c
			mu.Lock()
			want[r.RequestID] = r
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	got := readLog(t, dir)
	if len(got) != n {
		t.Fatalf("%d records in the log, want %d", len(got), n)
	}
	seen := map[uint64]bool{}
	for _, r := range got {
		if seen[r.Counter] || r.Counter < 1 || r.Counter > n {
			t.Errorf("counter %d is repeated or outside 1..%d", r.Counter, n)
		}
		seen[r.Counter] = true
		if w, ok := want[r.RequestID]; !ok || !reflect.DeepEqual(r, w) {
			t.Errorf("record %s differs:\n got %+v\nwant %+v", r.RequestID, r, w)
		}
	}
	if lines := logLines(dir); lines != n {
		t.Errorf("%d lines in the log, want %d: one JSON line per record", lines, n)
	}
}

// TestRecordMembers: the JSON members are HBEP-5's field list, and the members
// that can be unknown are written as null, not left out.
func TestRecordMembers(t *testing.T) {
	dir := t.TempDir()
	s := sink(t, dir)
	write(t, s, full(1))
	write(t, s, rec("unknown")) // every pointer member unset
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, logName))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 2 {
		t.Fatalf("%d lines, want 2", len(lines))
	}
	hbep := []string{"counter", "requestID", "startTime", "endTime", "clientKeyID", "path", "requestedModel", "level", "provider",
		"modelDeployment", "deploymentID", "instance", "instanceUID", "emitter", "status", "stream", "finish", "cutBy",
		"promptTokens", "cachedTokens", "completionTokens", "countsSource", "firstTokenMs", "durationMs"}
	var members [2]map[string]any
	for i, line := range lines {
		if err := json.Unmarshal([]byte(line), &members[i]); err != nil {
			t.Fatalf("line %d: %v", i+1, err)
		}
		for _, k := range hbep {
			if _, ok := members[i][k]; !ok {
				t.Errorf("line %d has no %q member: %s", i+1, k, line)
			}
		}
	}
	for _, k := range []string{"finish", "cutBy", "promptTokens", "cachedTokens", "completionTokens", "countsSource", "firstTokenMs"} {
		if v := members[1][k]; v != nil {
			t.Errorf("an unknown %s = %v, want null", k, v)
		}
	}
	if members[0]["finish"] != "stop" || members[0]["cutBy"] != "lease_end" || members[0]["countsSource"] != "engine" || members[0]["contentEvents"] != float64(6) {
		t.Errorf("a full record lost members: %s", lines[0])
	}
}

// TestTimeFormat: a record's times are UTC, millisecond RFC 3339.
func TestTimeFormat(t *testing.T) {
	zone := time.FixedZone("plus1", 3600)
	in := time.Date(2026, 10, 31, 13, 0, 0, 123456789, zone)
	if got, want := usage.Time(in), "2026-10-31T12:00:00.123Z"; got != want {
		t.Errorf("Time(%v) = %q, want %q", in, got, want)
	}
}

// TestWriteAfterCloseFails: a closed sink refuses records with an error rather
// than dropping them silently.
func TestWriteAfterCloseFails(t *testing.T) {
	s := sink(t, t.TempDir())
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(rec("late")); err == nil {
		t.Errorf("Write after Close succeeded, want an error")
	}
}
