package alpha

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The agent's rehash line as slog's text handler writes it (internal/agent, converge.go), with synthetic numbers: RehashOf
// reads its four numbers, the first line only, an INFO line only, and none from a line that lacks one or from a log
// without the line. The warm start's parse reads the line as an other line, so it never counts as a file or an image.
func TestRehashOfReadsTheAgentsRehashLine(t *testing.T) {
	line := `time=2026-11-14T15:00:04.123Z level=INFO msg="checkpoint rehashed" uid=11111111-2222-3333-4444-555555555555 generation=1 files=4 listed=5 bytes=15400000000 ms=11000`
	log := "time=2026-11-14T15:00:01Z level=INFO msg=\"allocation agent starting\" restartCount=0\n" + line + "\n" +
		`time=2026-11-14T15:20:00Z level=INFO msg="checkpoint rehashed" uid=u generation=2 files=1 listed=5 bytes=1 ms=1` + "\n"
	if got, want := RehashOf([]byte(log)), (RehashObs{Read: true, Files: 4, Listed: 5, Bytes: 15400000000, MS: 11000}); got != want {
		t.Errorf("RehashOf = %+v, want %+v (the first line)", got, want)
	}
	for name, l := range map[string]string{
		"no line":         "time=2026-11-14T15:00:01Z level=INFO msg=\"worker started\" worker=worker-0\n",
		"a number absent": strings.Replace(line, " ms=11000", "", 1),
		"a WARN line":     strings.Replace(line, "level=INFO", "level=WARN", 1),
		"another msg":     strings.Replace(line, `msg="checkpoint rehashed"`, `msg="checkpoint rehashed later"`, 1),
		"empty":           "",
	} {
		if got := RehashOf([]byte(l)); got.Read {
			t.Errorf("%s: RehashOf = %+v, want not read", name, got)
		}
	}
	if k := ParseAgentLog([]byte(line))[0].Kind; k != LineOther {
		t.Errorf("the warm start's parse reads the rehash line as %q, want %q", k, LineOther)
	}
}

// The post's line: the numbers and their rate; a cold start and a line not read in words, never a zero; a log not read.
func TestRehashLineSaysTheNumbersOrWhyNot(t *testing.T) {
	read := WarmObs{LogRead: true, Rehash: RehashObs{Read: true, Files: 4, Listed: 5, Bytes: 15400000000, MS: 11000}}
	head := "- the checkpoint's rehash of the job, from its agent's own log (HBEP-9's rehash time; information, not a check): "
	for _, c := range []struct {
		name string
		w    WarmObs
		want string
	}{
		{"read", read, head + "4 of 5 listed files, 15400000000 bytes (15.40 GB) in 11.0 s, 1400 MB/s\n"},
		{"too short for a rate", WarmObs{LogRead: true, Rehash: RehashObs{Read: true, Files: 1, Listed: 1, Bytes: 120}}, head + "1 of 1 listed files, 120 bytes (0.00 GB) in 0.0 s, too short for a rate\n"},
		{"a cold start", WarmObs{LogRead: true, Rehash: RehashObs{Read: true, Listed: 5}}, head + "none: a cold start, no listed file of 5 was present to rehash\n"},
		{"no line", WarmObs{LogRead: true}, head + "not read: the agent's log holds no rehash line\n"},
		{"no log", WarmObs{}, head + "not read: the agent's log was not read\n"},
	} {
		if got := rehashLine("the job", c.w); got != c.want {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
	}
	for _, w := range []WarmObs{{LogRead: true, Rehash: RehashObs{Read: true, Listed: 5}}, {LogRead: true}} {
		if l := rehashLine("the job", w); strings.Contains(l, " 0 bytes") || strings.Contains(l, " 0 of ") {
			t.Errorf("a line without a rehash carries a zero: %q", l)
		}
	}
}

// Through Run, the text to post of row A (one job) and row B (two jobs) carries each job's rehash line, the numbers on a
// healthy run and the words on a cold one or one with no line; the run passes either way.
func TestTheWarmStartRowsPostTheirRehash(t *testing.T) {
	for _, c := range []struct {
		row, family string
		jobs        []string
	}{
		{"alpha-athena-now-serve", "nowserve", []string{"the job"}},
		{"alpha-athena-now-nooverlap", "nownooverlap", []string{"the predecessor's job", "the successor's job"}},
	} {
		for fault, want := range map[string]string{
			"":              "3 of 4 listed files, 15400000000 bytes (15.40 GB) in 11.0 s, 1400 MB/s\n",
			"rehash-cold":   "none: a cold start, no listed file of 4 was present to rehash\n",
			"rehash-unread": "not read: the agent's log holds no rehash line\n",
		} {
			t.Run(c.row+"/"+fault, func(t *testing.T) {
				row := rowOf(c.row)
				cfg := Config{Row: row, Dry: true, DryFault: fault, Out: filepath.Join(t.TempDir(), "out"), PendingMax: time.Minute, FrontLabel: testFrontLabel}
				rec, err := NewRecorder(cfg.Out)
				if err != nil {
					t.Fatal(err)
				}
				if f, err := Run(context.Background(), cfg, newDrySteps(t, cfg, c.family), rec, t.Logf); err != nil || len(f) != 0 {
					t.Fatalf("failures %v, err %v; the rehash is no check", f, err)
				}
				post, err := os.ReadFile(filepath.Join(cfg.Out, PostsFile))
				if err != nil {
					t.Fatal(err)
				}
				for _, job := range c.jobs {
					line := "- the checkpoint's rehash of " + job + ", from its agent's own log (HBEP-9's rehash time; information, not a check): " + want
					if !strings.Contains(string(post), line) {
						t.Errorf("the text to post lacks %q:\n%s", line, post)
					}
				}
			})
		}
	}
}
