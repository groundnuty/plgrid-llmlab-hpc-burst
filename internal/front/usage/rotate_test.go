package usage_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
)

// HBEP-8, Usage records, Rotation keeps every file: records go to
// usage-YYYY-MM-DD.jsonl, named by the UTC day of the write; a closed file is
// never written again, nothing deletes one, the counter runs across them, and
// the only rewrite is the repair at open of the newest file's partial last
// line. A legacy usage.jsonl is the newest file while no daily file exists.
// Every file name below is a literal from the HBEP, not built by the code
// under test.

// movingClock is the sink's clock, moved by the test.
type movingClock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock(t time.Time) *movingClock { return &movingClock{t: t} }

func (c *movingClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *movingClock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

func openAt(t *testing.T, dir string, c *movingClock) *usage.Sink {
	t.Helper()
	s, err := usage.OpenOptions(dir, usage.Options{Now: c.now})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

// fileRecords reads one usage file as strict JSON Lines.
func fileRecords(t *testing.T, dir, name string) []usage.Record {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if len(b) == 0 {
		return nil
	}
	if b[len(b)-1] != '\n' {
		t.Errorf("%s ends in the middle of a line: %q", name, tail(b))
	}
	var out []usage.Record
	for i, line := range bytes.Split(bytes.TrimSuffix(b, []byte("\n")), []byte("\n")) {
		var r usage.Record
		if err := json.Unmarshal(line, &r); err != nil {
			t.Errorf("%s line %d is not a JSON object: %v: %q", name, i+1, err, tail(line))
			continue
		}
		out = append(out, r)
	}
	return out
}

func readBytes(t *testing.T, dir, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func listFiles(t *testing.T, dir string) []string {
	t.Helper()
	files, err := usage.Files(dir)
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// TestRotationCrossesAUTCMidnight: three records before midnight UTC and two
// after go to two files, each record in exactly one; the counter runs
// across them without a gap or a repeat; the closed file's bytes are the
// same after the later writes. The clock is injected; a test that stayed in
// one day would prove nothing about rotation (issue #112).
func TestRotationCrossesAUTCMidnight(t *testing.T) {
	dir := t.TempDir()
	c := newClock(time.Date(2026, 10, 31, 23, 59, 58, 0, time.UTC))
	s := openAt(t, dir, c)
	defer s.Close()
	for _, id := range []string{"a", "b", "c"} {
		write(t, s, rec(id))
	}
	closedBefore := readBytes(t, dir, "usage-2026-10-31.jsonl")

	c.set(time.Date(2026, 11, 1, 0, 0, 1, 0, time.UTC))
	for _, id := range []string{"d", "e"} {
		write(t, s, rec(id))
	}

	if got, want := listFiles(t, dir), []string{"usage-2026-10-31.jsonl", "usage-2026-11-01.jsonl"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("files = %v, want %v", got, want)
	}
	first, second := fileRecords(t, dir, "usage-2026-10-31.jsonl"), fileRecords(t, dir, "usage-2026-11-01.jsonl")
	if got, want := ids(first), []string{"a", "b", "c"}; !reflect.DeepEqual(got, want) {
		t.Errorf("the first day's file holds %v, want %v", got, want)
	}
	if got, want := ids(second), []string{"d", "e"}; !reflect.DeepEqual(got, want) {
		t.Errorf("the second day's file holds %v, want %v", got, want)
	}
	if got, want := counters(append(first, second...)), []uint64{1, 2, 3, 4, 5}; !reflect.DeepEqual(got, want) {
		t.Errorf("counters across the files = %v, want %v: no gap, no repeat", got, want)
	}
	if after := readBytes(t, dir, "usage-2026-10-31.jsonl"); !bytes.Equal(after, closedBefore) {
		t.Errorf("the closed file changed after midnight:\nbefore %q\nafter  %q", closedBefore, after)
	}
}

// TestADayWithNoRecordsMakesNoFile: the first record after a quiet day opens
// the file of its own day; the quiet day has none, and the new file is the
// only one added.
func TestADayWithNoRecordsMakesNoFile(t *testing.T) {
	dir := t.TempDir()
	c := newClock(time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC))
	s := openAt(t, dir, c)
	defer s.Close()
	write(t, s, rec("a"))
	c.set(time.Date(2026, 11, 3, 8, 0, 0, 0, time.UTC))
	write(t, s, rec("b"))
	if got, want := listFiles(t, dir), []string{"usage-2026-10-31.jsonl", "usage-2026-11-03.jsonl"}; !reflect.DeepEqual(got, want) {
		t.Errorf("files = %v, want %v", got, want)
	}
}

// TestADayIsTheUTCDay: a clock in another zone names the file by the UTC day
// of the instant, not by its own wall date.
func TestADayIsTheUTCDay(t *testing.T) {
	dir := t.TempDir()
	zone := time.FixedZone("UTC+3", 3*3600)
	// 01:30 on 1 November in UTC+3 is 22:30 on 31 October UTC.
	c := newClock(time.Date(2026, 11, 1, 1, 30, 0, 0, zone))
	s := openAt(t, dir, c)
	defer s.Close()
	write(t, s, rec("a"))
	if got, want := listFiles(t, dir), []string{"usage-2026-10-31.jsonl"}; !reflect.DeepEqual(got, want) {
		t.Errorf("files = %v, want %v", got, want)
	}
}

// TestACounterRunsAcrossARestartInTheNextDay: a clean stop in one day and a
// start in the next continue the counter with no gap, in the next day's file.
func TestACounterRunsAcrossARestartInTheNextDay(t *testing.T) {
	dir := t.TempDir()
	c := newClock(time.Date(2026, 10, 31, 20, 0, 0, 0, time.UTC))
	s := openAt(t, dir, c)
	write(t, s, rec("a"))
	write(t, s, rec("b"))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	c.set(time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC))
	s = openAt(t, dir, c)
	defer s.Close()
	if got := write(t, s, rec("c")); got != 3 {
		t.Errorf("the first counter after a clean restart = %d, want 3", got)
	}
	if got, want := ids(fileRecords(t, dir, "usage-2026-11-01.jsonl")), []string{"c"}; !reflect.DeepEqual(got, want) {
		t.Errorf("the second day's file holds %v, want %v", got, want)
	}
}

// partial is a record cut short, with no newline, as a crash leaves it.
func partial(counter string) string {
	return `{"counter":` + counter + `,"requestID":"cut","path":"/v1/chat/completions","sta`
}

func whole(counter, id string) string {
	return `{"counter":` + counter + `,"requestID":"` + id + `","path":"/v1/chat/completions","status":200}` + "\n"
}

func plant(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestTheRepairAtOpenTouchesTheNewestFileOnly: yesterday's file and the one
// before it both end in a partial line. Open cuts yesterday's (the newest),
// leaves the older one exactly as it was, and the first record of today goes
// to a file of its own, so yesterday's is not written again.
func TestTheRepairAtOpenTouchesTheNewestFileOnly(t *testing.T) {
	dir := t.TempDir()
	older := whole("1", "x") + partial("2")
	yesterday := whole("3", "y") + whole("4", "z") + partial("5")
	plant(t, dir, "usage-2026-10-29.jsonl", older)
	plant(t, dir, "usage-2026-10-30.jsonl", yesterday)
	plant(t, dir, "counter", "1000\n")

	s := openAt(t, dir, newClock(time.Date(2026, 10, 31, 8, 0, 0, 0, time.UTC)))
	if got := write(t, s, rec("today")); got != 1001 {
		t.Errorf("counter = %d, want 1001", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	if got := string(readBytes(t, dir, "usage-2026-10-29.jsonl")); got != older {
		t.Errorf("the older file was touched: %q, want %q", got, older)
	}
	if got, want := string(readBytes(t, dir, "usage-2026-10-30.jsonl")), whole("3", "y")+whole("4", "z"); got != want {
		t.Errorf("yesterday's file = %q, want its partial line cut: %q", got, want)
	}
	if got, want := ids(fileRecords(t, dir, "usage-2026-10-31.jsonl")), []string{"today"}; !reflect.DeepEqual(got, want) {
		t.Errorf("today's file holds %v, want %v", got, want)
	}
}

// TestTheRepairOfTodaysFileKeepsAppendingToIt: the newest file is today's:
// its partial line is cut and the next record continues it, with no new file.
func TestTheRepairOfTodaysFileKeepsAppendingToIt(t *testing.T) {
	dir := t.TempDir()
	plant(t, dir, "usage-2026-10-31.jsonl", whole("1", "a")+partial("2"))
	plant(t, dir, "counter", "1000\n")
	s := openAt(t, dir, newClock(time.Date(2026, 10, 31, 8, 0, 0, 0, time.UTC)))
	write(t, s, rec("b"))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if got, want := listFiles(t, dir), []string{"usage-2026-10-31.jsonl"}; !reflect.DeepEqual(got, want) {
		t.Errorf("files = %v, want %v", got, want)
	}
	if got, want := ids(fileRecords(t, dir, "usage-2026-10-31.jsonl")), []string{"a", "b"}; !reflect.DeepEqual(got, want) {
		t.Errorf("today's file holds %v, want %v", got, want)
	}
}

// TestALegacyLogIsRepairedAndThenNeverWritten: a usage.jsonl from before
// rotation, with no daily file beside it, is the newest file: Open cuts its
// partial line. The next record goes to a daily file, and the legacy file is
// not written again.
func TestALegacyLogIsRepairedAndThenNeverWritten(t *testing.T) {
	dir := t.TempDir()
	plant(t, dir, "usage.jsonl", whole("1", "old1")+whole("2", "old2")+partial("3"))
	plant(t, dir, "counter", "1000\n")

	s := openAt(t, dir, newClock(time.Date(2026, 10, 31, 8, 0, 0, 0, time.UTC)))
	write(t, s, rec("new1"))
	write(t, s, rec("new2"))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	if got, want := string(readBytes(t, dir, "usage.jsonl")), whole("1", "old1")+whole("2", "old2"); got != want {
		t.Errorf("the legacy file = %q, want it cut to its last whole line and not written to: %q", got, want)
	}
	if got, want := listFiles(t, dir), []string{"usage.jsonl", "usage-2026-10-31.jsonl"}; !reflect.DeepEqual(got, want) {
		t.Errorf("files = %v, want the legacy file first, then the daily one: %v", got, want)
	}
	recs := fileRecords(t, dir, "usage-2026-10-31.jsonl")
	if got, want := ids(recs), []string{"new1", "new2"}; !reflect.DeepEqual(got, want) {
		t.Errorf("the daily file holds %v, want %v", got, want)
	}
	if got, want := counters(recs), []uint64{1001, 1002}; !reflect.DeepEqual(got, want) {
		t.Errorf("counters = %v, want %v: the counter file continues", got, want)
	}
}

// TestALegacyLogBesideADailyFileIsLeftAlone: once a daily file exists, the
// legacy one is no longer the newest, so a partial line in it stays as it is.
func TestALegacyLogBesideADailyFileIsLeftAlone(t *testing.T) {
	dir := t.TempDir()
	legacy := whole("1", "old") + partial("2")
	plant(t, dir, "usage.jsonl", legacy)
	plant(t, dir, "usage-2026-10-30.jsonl", whole("3", "mid"))
	plant(t, dir, "counter", "1000\n")
	s := openAt(t, dir, newClock(time.Date(2026, 10, 31, 8, 0, 0, 0, time.UTC)))
	write(t, s, rec("new"))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if got := string(readBytes(t, dir, "usage.jsonl")); got != legacy {
		t.Errorf("the legacy file was touched: %q, want %q", got, legacy)
	}
}

// TestFilesIgnoresOtherNames: only usage.jsonl and real dates count.
func TestFilesIgnoresOtherNames(t *testing.T) {
	dir := t.TempDir()
	for _, n := range []string{"usage-2026-11-02.jsonl", "usage-2026-10-31.jsonl", "usage.jsonl", "usage-2026-13-45.jsonl", "usage-2026-10-31.jsonl.bak", "usage-latest.jsonl", "counter", ".counter.tmp123"} {
		plant(t, dir, n, "")
	}
	got := listFiles(t, dir)
	if want := []string{"usage.jsonl", "usage-2026-10-31.jsonl", "usage-2026-11-02.jsonl"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Files = %v, want %v", got, want)
	}
}

// TestAClockThatGoesBackKeepsTheNewerFile: the sink rolls forward only. It
// rotates from 31 October to 1 November, then the clock steps back to 31
// October: the closed 31 October file is not written again (its bytes are the
// same), and the record goes to the active 1 November file, which then holds
// exactly the two records written in it.
func TestAClockThatGoesBackKeepsTheNewerFile(t *testing.T) {
	dir := t.TempDir()
	c := newClock(time.Date(2026, 10, 31, 23, 0, 0, 0, time.UTC))
	s := openAt(t, dir, c)
	defer s.Close()
	write(t, s, rec("a"))
	c.set(time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC))
	write(t, s, rec("b"))
	closed := readBytes(t, dir, "usage-2026-10-31.jsonl")

	c.set(time.Date(2026, 10, 31, 23, 30, 0, 0, time.UTC))
	write(t, s, rec("c"))
	if got, want := listFiles(t, dir), []string{"usage-2026-10-31.jsonl", "usage-2026-11-01.jsonl"}; !reflect.DeepEqual(got, want) {
		t.Errorf("files = %v, want %v", got, want)
	}
	if after := readBytes(t, dir, "usage-2026-10-31.jsonl"); !bytes.Equal(after, closed) {
		t.Errorf("the closed file changed when the clock stepped back:\nbefore %q\nafter  %q", closed, after)
	}
	if got, want := ids(fileRecords(t, dir, "usage-2026-11-01.jsonl")), []string{"b", "c"}; !reflect.DeepEqual(got, want) {
		t.Errorf("the active file holds %v, want %v: exactly the one new record after b", got, want)
	}
}

// TestAClockSteppedBackBeforeTheFirstWriteTouchesNoClosedFile (PR 117 review,
// Astra B2): the sink opens on 1 November over the files of 30 and 31
// October, both ending in a partial line. Open repairs the newest (31
// October) only. The clock then steps back to 30 October before the first
// write. The 30 October file must stay exactly as it was, torn tail and all:
// it is a closed file, never repaired or appended to. The record goes to the
// newest file, the one still open, and the next day's record rotates away from
// it and leaves it as it was.
func TestAClockSteppedBackBeforeTheFirstWriteTouchesNoClosedFile(t *testing.T) {
	dir := t.TempDir()
	older := whole("1", "x") + partial("2")
	newest := whole("3", "y") + partial("4")
	plant(t, dir, "usage-2026-10-30.jsonl", older)
	plant(t, dir, "usage-2026-10-31.jsonl", newest)
	plant(t, dir, "counter", "1000\n")

	c := newClock(time.Date(2026, 11, 1, 8, 0, 0, 0, time.UTC))
	s := openAt(t, dir, c)
	defer s.Close()
	c.set(time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC))
	write(t, s, rec("stepped-back"))

	if got := string(readBytes(t, dir, "usage-2026-10-30.jsonl")); got != older {
		t.Errorf("the 30 October file was repaired or written: %q, want it as it was: %q", got, older)
	}
	if got, want := listFiles(t, dir), []string{"usage-2026-10-30.jsonl", "usage-2026-10-31.jsonl"}; !reflect.DeepEqual(got, want) {
		t.Errorf("files = %v, want %v: no file for the day the clock stepped back to", got, want)
	}
	newestNow := readBytes(t, dir, "usage-2026-10-31.jsonl")
	if got, want := ids(fileRecords(t, dir, "usage-2026-10-31.jsonl")), []string{"y", "stepped-back"}; !reflect.DeepEqual(got, want) {
		t.Errorf("the newest file holds %v, want its repaired record then the new one: %v", got, want)
	}

	// The clock reaches 1 November: the newest file is closed, untouched.
	c.set(time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC))
	write(t, s, rec("next-day"))
	if after := readBytes(t, dir, "usage-2026-10-31.jsonl"); !bytes.Equal(after, newestNow) {
		t.Errorf("the 31 October file changed after the rotation: %q, want %q", after, newestNow)
	}
	if got, want := ids(fileRecords(t, dir, "usage-2026-11-01.jsonl")), []string{"next-day"}; !reflect.DeepEqual(got, want) {
		t.Errorf("the 1 November file holds %v, want %v", got, want)
	}
}

// TestANewFilesDirectoryIsSynced: creating a file syncs its directory, once
// for each new file and not for a record written to an open one.
func TestANewFilesDirectoryIsSynced(t *testing.T) {
	dir := t.TempDir()
	c := newClock(time.Date(2026, 10, 31, 23, 59, 0, 0, time.UTC))
	var synced []string
	s, err := usage.OpenWith(dir, usage.Options{Now: c.now}, nil, func(d string) error { synced = append(synced, d); return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	write(t, s, rec("a"))
	write(t, s, rec("b"))
	if want := []string{dir}; !reflect.DeepEqual(synced, want) {
		t.Errorf("directory syncs after two records in a new file = %v, want %v", synced, want)
	}
	c.set(time.Date(2026, 11, 1, 0, 0, 5, 0, time.UTC))
	write(t, s, rec("c"))
	if want := []string{dir, dir}; !reflect.DeepEqual(synced, want) {
		t.Errorf("directory syncs after the next day's file = %v, want %v", synced, want)
	}
}

// TestAFailedDirectorySyncFailsTheBatchAndIsRetried: a directory sync that
// fails after a new file was created fails that batch (the record is lost and
// counted), writes nothing to the new file, leaves the sink to try again, and
// the next batch succeeds and syncs.
func TestAFailedDirectorySyncFailsTheBatchAndIsRetried(t *testing.T) {
	dir := t.TempDir()
	c := newClock(time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC))
	errSync := errors.New("injected: directory sync failed")
	fail := true
	s, err := usage.OpenWith(dir, usage.Options{Now: c.now}, nil, func(string) error {
		if fail {
			return errSync
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Write(rec("lost")); !errors.Is(err, errSync) {
		t.Fatalf("Write = %v, want the directory sync's error", err)
	}
	if st := s.Stats(); st.Lost != 1 || st.Failures != 1 || st.Records != 0 || st.Err == nil {
		t.Errorf("Stats after the failed batch = %+v, want 1 lost, 1 failure, 0 written and an error", st)
	}
	fail = false
	write(t, s, rec("kept"))
	if got, want := ids(fileRecords(t, dir, "usage-2026-10-31.jsonl")), []string{"kept"}; !reflect.DeepEqual(got, want) {
		t.Errorf("the file holds %v, want %v: the lost record is not in it", got, want)
	}
	if st := s.Stats(); st.Err != nil || st.Records != 1 {
		t.Errorf("Stats after the next batch = %+v, want no error and 1 written", st)
	}
}

// TestAFileThatCannotBeOpenedFailsTheBatchAndKeepsTheOldFile: at midnight the
// next day's file cannot be created (its name is a directory here). The batch
// fails and is counted; the old file is untouched; once the obstacle is gone
// the next batch opens the new file.
func TestAFileThatCannotBeOpenedFailsTheBatchAndKeepsTheOldFile(t *testing.T) {
	dir := t.TempDir()
	c := newClock(time.Date(2026, 10, 31, 23, 59, 0, 0, time.UTC))
	s := openAt(t, dir, c)
	defer s.Close()
	write(t, s, rec("a"))
	before := readBytes(t, dir, "usage-2026-10-31.jsonl")

	blocker := filepath.Join(dir, "usage-2026-11-01.jsonl")
	if err := os.Mkdir(blocker, 0o700); err != nil {
		t.Fatal(err)
	}
	c.set(time.Date(2026, 11, 1, 0, 0, 1, 0, time.UTC))
	if _, err := s.Write(rec("lost")); err == nil || !strings.Contains(err.Error(), "usage-2026-11-01.jsonl") {
		t.Fatalf("Write = %v, want an error naming the file", err)
	}
	if st := s.Stats(); st.Lost != 1 || st.Err == nil {
		t.Errorf("Stats = %+v, want one lost record and an error", st)
	}
	if after := readBytes(t, dir, "usage-2026-10-31.jsonl"); !bytes.Equal(after, before) {
		t.Errorf("the old file changed while the new one could not be opened: %q", after)
	}
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	if got := write(t, s, rec("b")); got != 3 {
		t.Errorf("counter after the failed batch = %d, want 3 (the lost record used 2)", got)
	}
	if got, want := ids(fileRecords(t, dir, "usage-2026-11-01.jsonl")), []string{"b"}; !reflect.DeepEqual(got, want) {
		t.Errorf("the new day's file holds %v, want %v", got, want)
	}
}

// TestStatsCountRecordsAndTimeTheState: Records counts what was written and
// synced, Lost what was not, Since moves at each change of state.
func TestStatsCountRecordsAndTimeTheState(t *testing.T) {
	dir := t.TempDir()
	t0 := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	c := newClock(t0)
	failNow := false
	s, err := usage.OpenWith(dir, usage.Options{Now: c.now}, func(f *os.File) usage.LogHandle { return &toggled{LogHandle: f, fail: &failNow} }, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if st := s.Stats(); st.Since != t0 || st.Err != nil || st.Records != 0 {
		t.Fatalf("Stats at open = %+v, want since %v, no error, no records", st, t0)
	}
	write(t, s, rec("a"))
	write(t, s, rec("b"))
	if st := s.Stats(); st.Records != 2 || st.Lost != 0 || st.Since != t0 {
		t.Errorf("Stats after two records = %+v, want 2 written, 0 lost, since unchanged", st)
	}

	failNow = true
	t1 := t0.Add(5 * time.Minute)
	c.set(t1)
	for _, id := range []string{"x", "y"} {
		if _, err := s.Write(rec(id)); err == nil {
			t.Fatal("Write succeeded on a failing disk")
		}
	}
	t2 := t1.Add(5 * time.Minute)
	c.set(t2)
	if _, err := s.Write(rec("z")); err == nil {
		t.Fatal("Write succeeded on a failing disk")
	}
	st := s.Stats()
	if st.Records != 2 || st.Lost != 3 || st.Failures != 3 || st.Err == nil {
		t.Errorf("Stats while failing = %+v, want 2 written, 3 lost in 3 failures, an error", st)
	}
	if st.Since != t1 {
		t.Errorf("Since while failing = %v, want the first failure's time %v, not the latest", st.Since, t1)
	}

	failNow = false
	t3 := t2.Add(time.Hour)
	c.set(t3)
	write(t, s, rec("ok"))
	st = s.Stats()
	if st.Err != nil || st.Records != 3 || st.Lost != 3 || st.Since != t3 {
		t.Errorf("Stats after recovery = %+v, want no error, 3 written, 3 lost kept, since %v", st, t3)
	}
}

// toggled is a log file whose writes fail while *fail is set.
type toggled struct {
	usage.LogHandle
	fail *bool
}

func (l *toggled) Write(p []byte) (int, error) {
	if *l.fail {
		return 0, errDisk
	}
	return l.LogHandle.Write(p)
}
