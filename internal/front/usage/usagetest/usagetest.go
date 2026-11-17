// Package usagetest holds what the tests of the usage forwarder and of the front read the durable
// log's HB_USAGE stream with: the files' lines, and a check that the stream holds exactly the
// records the files hold for a list of counters, each once and with the files' own bytes. The
// expectation is the FILES (HBEP-8, Test plan, Beta: "A test that compares the stream with what the
// forwarder held in memory rather than with the files proves nothing"), so nothing here reads the
// forwarder: even an id is parsed by this package's own code, not the forwarder's.
package usagetest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog/node/nodetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
)

// FileLines reads every usage file in dir the plain way: each line, with its newline, by its counter.
// A file that ends in a partial line fails the test.
func FileLines(t testing.TB, dir string) map[uint64]string {
	t.Helper()
	names, err := usage.Files(dir)
	if err != nil {
		t.Fatal(err)
	}
	byCounter := map[uint64]string{}
	for _, name := range names {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		for _, l := range strings.SplitAfter(string(b), "\n") {
			if l == "" {
				continue
			}
			if !strings.HasSuffix(l, "\n") {
				t.Fatalf("%s ends in a partial line %q", name, l)
			}
			var r struct{ Counter uint64 }
			if err := json.Unmarshal([]byte(l), &r); err != nil {
				t.Fatalf("%s: line %q: %v", name, l, err)
			}
			byCounter[r.Counter] = l
		}
	}
	return byCounter
}

// Range is the counters from to to, inclusive: a literal expectation.
func Range(from, to uint64) []uint64 {
	var out []uint64
	for c := from; c <= to; c++ {
		out = append(out, c)
	}
	return out
}

// ParseID reads the Nats-Msg-Id of a usage message, usage-<first>-<last>.
func ParseID(id string) (first, last uint64, ok bool) {
	parts := strings.Split(id, "-")
	if len(parts) != 3 || parts[0] != "usage" {
		return 0, 0, false
	}
	first, err1 := strconv.ParseUint(parts[1], 10, 64)
	last, err2 := strconv.ParseUint(parts[2], 10, 64)
	if err1 != nil || err2 != nil || last < first {
		return 0, 0, false
	}
	return first, last, true
}

// CheckStream reads HB_USAGE through a fresh client and requires that it holds exactly the counters
// want, each once and in order, and that each message is what its id says, holds at most 256 lines
// and under 1 MiB, and holds the files' own bytes for its lines. It returns the messages.
func CheckStream(t testing.TB, d *nodetest.Dir, usageDir string, want []uint64) []nodetest.Message {
	t.Helper()
	files := FileLines(t, usageDir)
	msgs := d.ReadAll(t, eventlog.StreamUsage)
	var got []uint64
	for _, m := range msgs {
		first, last, ok := ParseID(m.ID)
		if !ok {
			t.Fatalf("message %d has the id %q, not usage-<first>-<last>", m.Seq, m.ID)
		}
		ls := strings.SplitAfter(string(m.Payload), "\n")
		if ls[len(ls)-1] != "" {
			t.Fatalf("message %d (%s) does not end in a newline", m.Seq, m.ID)
		}
		ls = ls[:len(ls)-1]
		if len(ls) > 256 {
			t.Errorf("message %d (%s) holds %d lines, over HBEP-8's 256", m.Seq, m.ID, len(ls))
		}
		if len(m.Payload) >= 1<<20 {
			t.Errorf("message %d (%s) is %d bytes, not under 1 MiB", m.Seq, m.ID, len(m.Payload))
		}
		for i, l := range ls {
			var r struct{ Counter uint64 }
			if err := json.Unmarshal([]byte(l), &r); err != nil {
				t.Fatalf("message %d (%s) line %d: %v", m.Seq, m.ID, i+1, err)
			}
			if f, ok := files[r.Counter]; !ok {
				t.Errorf("message %d (%s) holds counter %d, which no file holds", m.Seq, m.ID, r.Counter)
			} else if f != l {
				t.Errorf("counter %d in message %d is not the file's line:\n log: %q\nfile: %q", r.Counter, m.Seq, l, f)
			}
			if i == 0 && r.Counter != first {
				t.Errorf("message %d is id %s but its first line is counter %d", m.Seq, m.ID, r.Counter)
			}
			if i == len(ls)-1 && r.Counter != last {
				t.Errorf("message %d is id %s but its last line is counter %d", m.Seq, m.ID, r.Counter)
			}
			got = append(got, r.Counter)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("HB_USAGE holds counters %s, want %s", Summary(got), Summary(want))
	}
	return msgs
}

// Summary prints a list of counters as runs, so a failure is readable.
func Summary(c []uint64) string {
	if len(c) == 0 {
		return "none"
	}
	var parts []string
	start, prev := c[0], c[0]
	flush := func() {
		if start == prev {
			parts = append(parts, strconv.FormatUint(start, 10))
		} else {
			parts = append(parts, fmt.Sprintf("%d-%d", start, prev))
		}
	}
	for _, x := range c[1:] {
		if x != prev+1 {
			flush()
			start = x
		}
		prev = x
	}
	flush()
	return fmt.Sprintf("%s (%d in all)", strings.Join(parts, ","), len(c))
}
