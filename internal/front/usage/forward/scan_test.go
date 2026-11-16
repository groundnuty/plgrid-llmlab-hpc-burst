package forward

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// The files, read the way the forwarder reads them (HBEP-8, The durable log, Delivery). Every
// fixture is hand-written, and every expectation is a literal worked from the HBEP's rules, not
// read from the code under test.

// line is a usage line of exactly size bytes with its newline, counter n: {"counter":N,"requestID":"
// ...pad..."}. The framing is 11 + digits + 14 + 2 + 1 bytes, so the pad makes up the rest.
func line(t *testing.T, n, size int) string {
	t.Helper()
	d := len(fmt.Sprint(n))
	pad := size - (11 + d + 14 + 2 + 1)
	if pad < 0 {
		t.Fatalf("a line of %d bytes cannot hold counter %d", size, n)
	}
	l := fmt.Sprintf(`{"counter":%d,"requestID":"%s"}`+"\n", n, strings.Repeat("p", pad))
	if len(l) != size {
		t.Fatalf("fixture error: line for counter %d is %d bytes, want %d", n, len(l), size)
	}
	return l
}

// lines is the lines of counters from to to (inclusive) of size bytes each.
func lines(t *testing.T, from, to, size int) string {
	t.Helper()
	var b strings.Builder
	for n := from; n <= to; n++ {
		b.WriteString(line(t, n, size))
	}
	return b.String()
}

func put(t *testing.T, dir, name, text string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
}

// scanned runs scanFiles and returns the counters it gave out, and the last it returned.
func scanned(t *testing.T, dir string, after, upTo uint64) (got []uint64, last uint64, warned int) {
	t.Helper()
	last, err := scanFiles(dir, after, upTo, 1<<20, func(string, int) { warned++ }, func(uint64, int) { t.Error("a line was long") }, func(c uint64, l []byte) error {
		got = append(got, c)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return got, last, warned
}

func TestScanFilesReadsFromTheNextCounterPresent(t *testing.T) {
	for _, c := range []struct {
		name         string
		files        map[string]string
		after, upTo  uint64
		want         []uint64
		wantLast     uint64
		wantWarnings int
	}{
		{"everything", map[string]string{"usage-2026-11-10.jsonl": lines(t, 1, 5, 100)}, 0, 99, []uint64{1, 2, 3, 4, 5}, 5, 0},
		{"after the first three", map[string]string{"usage-2026-11-10.jsonl": lines(t, 1, 5, 100)}, 3, 99, []uint64{4, 5}, 5, 0},
		{"the file's last counter is the position: nothing new", map[string]string{"usage-2026-11-10.jsonl": lines(t, 1, 5, 100)}, 5, 99, nil, 5, 0},
		{"a gap after the position is crossed, not waited for (a crash's reserved block)",
			map[string]string{"usage-2026-11-10.jsonl": lines(t, 1, 5, 100), "usage-2026-11-11.jsonl": lines(t, 1001, 1003, 100)},
			5, 9999, []uint64{1001, 1002, 1003}, 1003, 0},
		{"a gap inside a file",
			map[string]string{"usage-2026-11-10.jsonl": lines(t, 1, 2, 100) + lines(t, 1001, 1002, 100)},
			0, 9999, []uint64{1, 2, 1001, 1002}, 1002, 0},
		{"the position is in a gap",
			map[string]string{"usage-2026-11-10.jsonl": lines(t, 1, 2, 100) + lines(t, 1001, 1002, 100)},
			500, 9999, []uint64{1001, 1002}, 1002, 0},
		{"the legacy file comes first",
			map[string]string{"usage.jsonl": lines(t, 1, 3, 100), "usage-2026-11-10.jsonl": lines(t, 4, 5, 100)},
			0, 99, []uint64{1, 2, 3, 4, 5}, 5, 0},
		{"not past what the sink synced: upTo ends the scan, in the middle of a file",
			map[string]string{"usage-2026-11-10.jsonl": lines(t, 1, 10, 100)}, 0, 6, []uint64{1, 2, 3, 4, 5, 6}, 6, 0},
		{"not past what the sink synced: upTo ends the scan before the next file",
			map[string]string{"usage-2026-11-10.jsonl": lines(t, 1, 3, 100), "usage-2026-11-11.jsonl": lines(t, 4, 6, 100)}, 0, 3, []uint64{1, 2, 3}, 3, 0},
		{"a last line with no newline is not whole and is left",
			map[string]string{"usage-2026-11-10.jsonl": lines(t, 1, 3, 100) + strings.TrimSuffix(line(t, 4, 100), "\n")}, 0, 99, []uint64{1, 2, 3}, 3, 0},
		{"a line with no counter is passed over, with a warning",
			map[string]string{"usage-2026-11-10.jsonl": line(t, 1, 100) + "not a record\n" + line(t, 2, 100)}, 0, 99, []uint64{1, 2}, 2, 1},
		{"a counter that does not rise is passed over",
			map[string]string{"usage-2026-11-10.jsonl": line(t, 5, 100) + line(t, 3, 100) + line(t, 6, 100)}, 0, 99, []uint64{5, 6}, 6, 0},
		{"no files", nil, 0, 99, nil, 0, 0},
		{"an empty file", map[string]string{"usage-2026-11-10.jsonl": ""}, 0, 99, nil, 0, 0},
		{"files that are not usage files are not read",
			map[string]string{"usage-2026-11-10.jsonl": lines(t, 1, 2, 100), "counter": "1000\n", "notes.txt": line(t, 7, 100)}, 0, 99, []uint64{1, 2}, 2, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, text := range c.files {
				put(t, dir, name, text)
			}
			got, last, warned := scanned(t, dir, c.after, c.upTo)
			if !reflect.DeepEqual(got, c.want) || last != c.wantLast || warned != c.wantWarnings {
				t.Errorf("scanFiles(after %d, upTo %d) gave %v, last %d, %d warnings; want %v, last %d, %d warnings",
					c.after, c.upTo, got, last, warned, c.want, c.wantLast, c.wantWarnings)
			}
		})
	}
}

// The lines come out exactly as the file holds them: not decoded and encoded again. A line that has
// members the sink's Record lacks, in an order it does not write and with a raw "<", would change
// in any re-encoding.
func TestScanFilesGivesTheFilesBytes(t *testing.T) {
	dir := t.TempDir()
	want := `{"counter":1,"zzz":"<raw & unescaped>","requestID":"é","extra":{"a":[1,2]}}` + "\n"
	put(t, dir, "usage-2026-11-10.jsonl", want)
	var got string
	if _, err := scanFiles(dir, 0, 9, 1<<20, func(string, int) {}, func(uint64, int) {}, func(c uint64, l []byte) error { got += string(l); return nil }); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("the line came out as %q, want %q", got, want)
	}
}

// startAfter: the oldest daily file of the newest set whose sizes fit within the cap (R4). Files of
// 600, 500, 400 and 300 bytes, 100 bytes a line: counters 1-6, 7-11, 12-15 and 16-18. Each expected
// start is worked by hand: the newest file always belongs to the set, and files join it, newest
// first, while the running total stays at or under the cap.
func TestStartAfterTakesTheOldestFileOfTheNewestSetThatFits(t *testing.T) {
	files := map[string]string{
		"usage-2026-11-10.jsonl": lines(t, 1, 6, 100),   // 600
		"usage-2026-11-11.jsonl": lines(t, 7, 11, 100),  // 500
		"usage-2026-11-12.jsonl": lines(t, 12, 15, 100), // 400
		"usage-2026-11-13.jsonl": lines(t, 16, 18, 100), // 300
	}
	for _, c := range []struct {
		cap       int64
		wantFirst uint64 // the first counter the forwarder publishes
		why       string
	}{
		{3000, 1, "all four files, 1800 bytes, fit"},
		{1800, 1, "all four fit exactly"},
		{1799, 7, "the three newest, 1200 bytes, fit; the oldest would make 1800"},
		{1200, 7, "the three newest fit exactly"},
		{1199, 12, "the two newest, 700, fit; the third would make 1200"},
		{700, 12, "the two newest fit exactly"},
		{699, 16, "300 fits, 400 more would make 700"},
		{300, 16, "the newest alone fits exactly"},
		{299, 16, "the newest file alone is over the cap and is the set anyway"},
		{1, 16, "the newest file alone is over the cap and is the set anyway"},
		{0, 1, "no cap is no limit: every file"},
		{-1, 1, "no cap is no limit: every file"},
	} {
		t.Run(fmt.Sprintf("cap %d", c.cap), func(t *testing.T) {
			dir := t.TempDir()
			for name, text := range files {
				put(t, dir, name, text)
			}
			after, err := startAfter(dir, c.cap)
			if err != nil {
				t.Fatal(err)
			}
			if after+1 != c.wantFirst {
				t.Errorf("cap %d: starts at counter %d, want %d (%s)", c.cap, after+1, c.wantFirst, c.why)
			}
		})
	}
}

func TestStartAfterEdges(t *testing.T) {
	t.Run("no files", func(t *testing.T) {
		if after, err := startAfter(t.TempDir(), 1000); err != nil || after != 0 {
			t.Errorf("startAfter on an empty directory = %d, %v; want 0", after, err)
		}
	})
	t.Run("the legacy file is the oldest of the set", func(t *testing.T) {
		dir := t.TempDir()
		put(t, dir, "usage.jsonl", lines(t, 1, 3, 100))
		put(t, dir, "usage-2026-11-10.jsonl", lines(t, 4, 5, 100))
		if after, err := startAfter(dir, 1000); err != nil || after != 0 {
			t.Errorf("startAfter = %d, %v; want 0 (the legacy file's first counter, 1, less 1)", after, err)
		}
		if after, err := startAfter(dir, 200); err != nil || after != 3 {
			t.Errorf("startAfter at cap 200 = %d, %v; want 3 (only the newest file fits)", after, err)
		}
	})
	t.Run("an empty file at the start of the set is passed over to the first line", func(t *testing.T) {
		dir := t.TempDir()
		put(t, dir, "usage-2026-11-10.jsonl", "")
		put(t, dir, "usage-2026-11-11.jsonl", lines(t, 40, 41, 100))
		if after, err := startAfter(dir, 1000); err != nil || after != 39 {
			t.Errorf("startAfter = %d, %v; want 39", after, err)
		}
	})
	t.Run("a counter gap before the set does not matter", func(t *testing.T) {
		dir := t.TempDir()
		put(t, dir, "usage-2026-11-10.jsonl", lines(t, 1, 2, 100))
		put(t, dir, "usage-2026-11-11.jsonl", lines(t, 1001, 1002, 100))
		if after, err := startAfter(dir, 250); err != nil || after != 1000 {
			t.Errorf("startAfter = %d, %v; want 1000", after, err)
		}
	})
}

func TestMessageIDs(t *testing.T) {
	if got := messageID(1001, 1256); got != "usage-1001-1256" {
		t.Errorf("messageID = %q, want usage-1001-1256", got)
	}
	for _, c := range []struct {
		id          string
		first, last uint64
		ok          bool
	}{
		{"usage-1001-1256", 1001, 1256, true},
		{"usage-7-7", 7, 7, true},
		{"usage-18446744073709551615-18446744073709551615", 18446744073709551615, 18446744073709551615, true},
		{"usage-9-8", 0, 0, false},
		{"usage-1", 0, 0, false},
		{"usage-1-", 0, 0, false},
		{"usage--1", 0, 0, false},
		{"usage-1-2-3", 0, 0, false},
		{"event-1-2", 0, 0, false},
		{"", 0, 0, false},
		{"usage-a-b", 0, 0, false},
	} {
		first, last, ok := parseID(c.id)
		if first != c.first || last != c.last || ok != c.ok {
			t.Errorf("parseID(%q) = %d, %d, %v; want %d, %d, %v", c.id, first, last, ok, c.first, c.last, c.ok)
		}
	}
}

// A line longer than the scan's bound is never read whole and never stops the scan (review of PR
// #644: a client's model name can make a record as long as a request body, and such a line can be
// the last one of a file). Its counter is read from its first bytes and handed to long with its size,
// the lines before and after it come out as always, and a file of such lines is read in the memory of
// its buffer. Every fixture is hand-written: lines of 100 bytes, and one of 2 MiB or of 300 KB.
func TestScanFilesPassesALongLineAndGoesOn(t *testing.T) {
	const twoMiB = 2 << 20
	for _, c := range []struct {
		name      string
		files     map[string]string
		maxLine   int
		upTo      uint64
		want      []uint64
		wantLongs map[uint64]int
		wantLast  uint64
	}{
		{"in the middle of a file", map[string]string{"usage-2026-11-10.jsonl": lines(t, 1, 2, 100) + line(t, 3, twoMiB) + lines(t, 4, 5, 100)},
			1 << 20, 99, []uint64{1, 2, 4, 5}, map[uint64]int{3: twoMiB}, 5},
		{"as the last line of a file", map[string]string{"usage-2026-11-10.jsonl": lines(t, 1, 2, 100) + line(t, 3, twoMiB)},
			1 << 20, 99, []uint64{1, 2}, map[uint64]int{3: twoMiB}, 3},
		{"as the first line of a file", map[string]string{"usage-2026-11-10.jsonl": line(t, 1, twoMiB) + lines(t, 2, 3, 100)},
			1 << 20, 99, []uint64{2, 3}, map[uint64]int{1: twoMiB}, 3},
		{"as the last line of a file, with the next file after it", map[string]string{"usage-2026-11-10.jsonl": lines(t, 1, 2, 100) + line(t, 3, twoMiB), "usage-2026-11-11.jsonl": lines(t, 4, 5, 100)},
			1 << 20, 99, []uint64{1, 2, 4, 5}, map[uint64]int{3: twoMiB}, 5},
		{"two of them in a row", map[string]string{"usage-2026-11-10.jsonl": line(t, 1, twoMiB) + line(t, 2, twoMiB) + lines(t, 3, 3, 100)},
			1 << 20, 99, []uint64{3}, map[uint64]int{1: twoMiB, 2: twoMiB}, 3},
		{"one over the bound by a byte, and the bound itself is a line", map[string]string{"usage-2026-11-10.jsonl": line(t, 1, 1001) + line(t, 2, 1000)},
			1000, 99, []uint64{2}, map[uint64]int{1: 1001}, 2},
		{"longer than the buffer but within the bound, which is whole", map[string]string{"usage-2026-11-10.jsonl": line(t, 1, 300<<10) + lines(t, 2, 2, 100)},
			1 << 20, 99, []uint64{1, 2}, nil, 2},
		{"longer than the buffer and the bound, small bound", map[string]string{"usage-2026-11-10.jsonl": lines(t, 1, 1, 100) + line(t, 2, 300<<10) + lines(t, 3, 3, 100)},
			1000, 99, []uint64{1, 3}, map[uint64]int{2: 300 << 10}, 3},
		{"past what the sink synced it is not told", map[string]string{"usage-2026-11-10.jsonl": lines(t, 1, 2, 100) + line(t, 3, twoMiB)},
			1 << 20, 2, []uint64{1, 2}, nil, 2},
		{"with no newline it is not whole", map[string]string{"usage-2026-11-10.jsonl": lines(t, 1, 2, 100) + strings.TrimSuffix(line(t, 3, twoMiB), "\n")},
			1 << 20, 99, []uint64{1, 2}, nil, 2},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, text := range c.files {
				put(t, dir, name, text)
			}
			var got []uint64
			longs := map[uint64]int{}
			last, err := scanFiles(dir, 0, c.upTo, c.maxLine, func(string, int) { t.Error("a line had no counter") },
				func(counter uint64, size int) { longs[counter] = size },
				func(counter uint64, l []byte) error {
					if len(l) > c.maxLine {
						t.Errorf("counter %d was emitted whole at %d bytes, over the bound %d", counter, len(l), c.maxLine)
					}
					got = append(got, counter)
					return nil
				})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, c.want) || last != c.wantLast || (len(longs) != len(c.wantLongs)) {
				t.Fatalf("scanFiles gave %v, long lines %v, last %d; want %v, %v, %d", got, longs, last, c.want, c.wantLongs, c.wantLast)
			}
			for n, size := range c.wantLongs {
				if longs[n] != size {
					t.Errorf("long line %d has size %d, want %d", n, longs[n], size)
				}
			}
		})
	}
}
