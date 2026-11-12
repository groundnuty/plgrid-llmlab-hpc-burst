package livechecks

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Issue #422: TestEveryStackRowsTimeoutIsAboveItsWorstCase failed once in CI, in one subtest, just after UTC
// midnight, with "setting LIVE_OWNER_GO_HELIOS_NOW must hold today's UTC date (2026-11-12) ... it is missing
// or from another day". The cause was two readings of the real clock. The fixture wrote the owner's-go
// settings from Go's time.Now, and the script, a fraction of a second later, compared them with its own
// `date -u +%F`. A subtest whose fixture came before 00:00:00Z and whose check came after it was refused, as
// the script is meant to refuse a go of another day. The script reads one clock (today_utc: LIVE_FAKE_DATE_FILE
// when a test sets it, else `date -u`), so the fix is in the harness: a world takes its day from its clock once,
// writes it into the settings, and hands the same day to the script through the seam it already has.

// fileText is a file's text, or a failure of the test.
func fileText(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// stepClock is a clock that reads the given times in turn and the last one from then on.
type stepClock struct {
	mu    sync.Mutex
	times []time.Time
	reads int
}

func (c *stepClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.times[min(c.reads, len(c.times)-1)]
	c.reads++
	return t
}

func (c *stepClock) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reads
}

// A world whose clock reads 23:59:59Z the first time and 00:00:01Z, the next day, every time after: the fixture
// is written just before midnight and the script runs just after it. The row passes, because both read the world's
// day, the first reading's; and the clock is read once, since a second reading is the second clock. The date is in
// the past, so a script that read the real date, or a fixture that did, would not agree with it.
func TestTheFixtureAndTheScriptReadOneClockAcrossMidnight(t *testing.T) {
	clock := &stepClock{times: []time.Time{
		time.Date(2026, 1, 1, 23, 59, 59, 0, time.UTC),
		time.Date(2026, 1, 2, 0, 0, 1, 0, time.UTC),
	}}
	w := newAlphaWorldAt(t, clock.now)
	if w.day != "2026-01-01" {
		t.Fatalf("the world's day is %q, want the first reading's, 2026-01-01", w.day)
	}
	if got := fileText(t, w.settings); !strings.Contains(got, "LIVE_OWNER_GO_HELIOS_NOW=2026-01-01\n") || !strings.Contains(got, "LIVE_OWNER_GO_ATHENA_NOW=2026-01-01\n") {
		t.Errorf("the owner's-go settings do not hold the world's day, 2026-01-01:\n%s", got)
	}
	if got := fileText(t, w.dayFile()); got != "2026-01-01\n" {
		t.Errorf("the script's date file holds %q, want the world's day", got)
	}
	res := w.run(nil, "--run", "alpha-helios-nooverlap", "--yes")
	if res.code != 0 {
		t.Fatalf("exit %d, want 0: the fixture's go and the script's today are one day:\n%s", res.code, res.out)
	}
	if n := clock.count(); n != 1 {
		t.Errorf("the clock was read %d times, want once: every reading after the first is a second clock", n)
	}
}

// The script's own clock still works as it does without the seam: its today is what `date -u +%F` prints. A stub of
// date, first on the script's PATH, prints the world's day for exactly that call and records it, so the test shows
// the call is made and its answer is the one compared, with no real clock in it.
func TestTheScriptsTodayIsWhatDateUPrintsWhenNoSeamIsSet(t *testing.T) {
	clock := &stepClock{times: []time.Time{time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}}
	w := newAlphaWorldAt(t, clock.now)
	write(t, filepath.Join(w.bin, "date"), `#!/bin/bash
if [ "$*" = "-u +%F" ]; then
  echo "$STUB_DATE"
  echo "-u +%F" >> "$STUB_STATE/date-calls"
else
  exec /bin/date "$@"
fi
`, 0o755)
	// An empty LIVE_FAKE_DATE_FILE is no seam: the script's test is -n.
	res := w.run([]string{"LIVE_FAKE_DATE_FILE=", "STUB_DATE=" + w.day}, "--run", "alpha-helios-nooverlap", "--yes")
	if res.code != 0 {
		t.Fatalf("exit %d, want 0:\n%s", res.code, res.out)
	}
	if got := w.stateFile("date-calls"); got == "" {
		t.Errorf("the script never called date -u +%%F, so its today was not read from the date command")
	}
}

// The control: with the day of the world on the script's side and a go of another day in the settings, the row is
// refused (exit 3), and the message names the day the script read. The script is still meant to refuse a go that is
// not today's, and the fix leaves that as it was.
func TestAGoOfAnotherDayIsStillRefusedWithTheWorldsDay(t *testing.T) {
	clock := &stepClock{times: []time.Time{time.Date(2026, 1, 1, 23, 59, 59, 0, time.UTC)}}
	w := newAlphaWorldAt(t, clock.now)
	w.alphaSettings(func(m map[string]string) { m["LIVE_OWNER_GO_HELIOS_NOW"] = w.dayPlus(-1) })
	res := w.run(nil, "--run", "alpha-helios-nooverlap", "--yes")
	if res.code != 3 {
		t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
	}
	w.contains(res, "setting LIVE_OWNER_GO_HELIOS_NOW must hold today's UTC date (2026-01-01)")
}
