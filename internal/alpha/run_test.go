package alpha

// The dry runs of the three stack rows: the stand-in steps over the fake Slurm
// in run mode, judged by the live path (Run). Each row runs healthy and passes,
// then once per fault, and the fault must fail it with its own label. A row
// that passes with a check removed is the verification that proves nothing; the
// verdict mutants in verdict_test.go and the table below are what rule it out.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type dryCase struct {
	fault string
	want  string // a substring of a failed check; "" for the healthy run
}

var dryCases = map[string][]dryCase{
	"alpha-athena-handover": {
		{"", ""},
		{"cut", "a stream was cut"},
		{"no-stream", "no stream was in flight at the drain start"},
		{"two-restarts", "2 restarts at the worker rung, want 1"},
		{"no-restart", "0 restarts at the worker rung, want 1"},
		{"no-handover", "0 handovers completed, want 1"},
		{"late-successor", "the successor was not ready before the predecessor ended"},
		{"wrong-partition", "a job ran on another partition than the row's"},
		{"untagged", "a job tag was not written down before its submission"},
		{"unattributed", "a response lacked its attribution headers"},
	},
	"alpha-helios-nooverlap": {
		{"", ""},
		{"nocap", "the successor started before the predecessor ended"},
		{"nogap", "the gap was not measured"},
		{"noslot", "the deployment never showed NoOverlapSlot"},
		{"cut", "a stream was cut"},
		{"no-stream", "no stream was in flight at the drain start"},
		{"late-submit", "the successor was not queued before the predecessor ended"},
		{"untagged", "a job tag was not written down before its submission"},
		{"wrong-partition", "a job ran on another partition than the row's"},
	},
	"tunnel-hour-helios": tunnelCases,
	"tunnel-hour-athena": tunnelCases,
}

var tunnelCases = []dryCase{
	{"", ""},
	{"short", "a stream lasted less than an hour"},
	{"cut", "a stream was cut during the hour"},
	{"drops", "the tunnel dropped during the hour"},
	{"no-rate", "the path rate was not measured"},
	{"short-rate", "the path rate was asked for under 30 s, timed under 29 s, or moved no bytes"},
	{"wrong-direction", "the path rate is not in the token direction"},
	{"untagged", "a job tag was not written down before its submission"},
	{"wrong-partition", "a job ran on another partition than the row's"},
}

func familyOf(row string) string {
	switch {
	case strings.HasPrefix(row, "alpha-athena"):
		return "handover"
	case strings.HasPrefix(row, "alpha-helios"):
		return "nooverlap"
	}
	return "tunnel"
}

func TestDryRunsOfEveryRowAndEveryFault(t *testing.T) {
	for _, row := range Rows() {
		cases, ok := dryCases[row.Name]
		if !ok {
			t.Fatalf("row %s has no dry cases", row.Name)
		}
		for _, c := range cases {
			name := row.Name + "/" + c.fault
			if c.fault == "" {
				name = row.Name + "/healthy"
			}
			t.Run(name, func(t *testing.T) {
				cfg := Config{Row: row, Dry: true, DryFault: c.fault, Out: filepath.Join(t.TempDir(), "out"), PendingMax: time.Minute}
				rec, err := NewRecorder(cfg.Out)
				if err != nil {
					t.Fatal(err)
				}
				st := newDrySteps(t, cfg, familyOf(row.Name))
				failures, err := Run(context.Background(), cfg, st, rec, t.Logf)
				if err != nil {
					t.Fatalf("the dry run broke: %v", err)
				}
				joined := Summary(failures)
				if c.want == "" {
					if len(failures) != 0 {
						t.Fatalf("the healthy dry run failed: %s", joined)
					}
				} else if !strings.Contains(joined, c.want) {
					t.Fatalf("fault %q: failed checks %q lack %q", c.fault, joined, c.want)
				}
				// The record and the text to post exist either way, private.
				for _, f := range []string{RecordFile, PostsFile} {
					fi, err := os.Stat(filepath.Join(cfg.Out, f))
					if err != nil || fi.Mode().Perm() != 0o600 {
						t.Errorf("%s: %v mode %v, want a file of mode 600", f, err, fi)
					}
				}
				posts, _ := os.ReadFile(filepath.Join(cfg.Out, PostsFile))
				verdict := "PASS"
				if c.want != "" {
					verdict = "FAIL"
				}
				if !strings.Contains(string(posts), row.Name+": "+verdict) {
					t.Errorf("the text to post lacks %q:\n%s", row.Name+": "+verdict, posts)
				}
			})
		}
	}
}

// What a healthy run posts carries its measurements, from the cluster's times
// and the scenario's: expected values worked out from the stand-in's clock
// (the no-overlap gap is the 30 s stream tail, the 10 s before the release and
// the 20 s start-to-ready: 60 s).
func TestHealthyRunsPostTheirMeasurements(t *testing.T) {
	cases := map[string][]string{
		"alpha-athena-handover":  {"handovers completed: 1", "restarts at the worker rung: 1", "responses with attribution: 20 of 20"},
		"alpha-helios-nooverlap": {"gap from the drain start to the successor ready: 1m0s", "NoOverlapSlot seen: true", "queue wait once the GPU freed: 0s"},
		"tunnel-hour-athena":     {"path rate, node to front:", "lanes held: 2, the shortest 1h1m0s", "tunnel drops: 0"},
	}
	for name, wants := range cases {
		t.Run(name, func(t *testing.T) {
			row, _ := RowByName(name)
			cfg := Config{Row: row, Dry: true, Out: filepath.Join(t.TempDir(), "out"), PendingMax: time.Minute}
			rec, err := NewRecorder(cfg.Out)
			if err != nil {
				t.Fatal(err)
			}
			if f, err := Run(context.Background(), cfg, newDrySteps(t, cfg, familyOf(name)), rec, t.Logf); err != nil || len(f) != 0 {
				t.Fatalf("failures %v, err %v", f, err)
			}
			posts, _ := os.ReadFile(filepath.Join(cfg.Out, PostsFile))
			for _, w := range wants {
				if !strings.Contains(string(posts), w) {
					t.Errorf("the text to post lacks %q:\n%s", w, posts)
				}
			}
		})
	}
}

// A scenario that returns an error is a failed run, not a pass with no verdict,
// and the steps are closed either way.
func TestAScenarioErrorFailsTheRunAndClosesTheSteps(t *testing.T) {
	row, _ := RowByName("alpha-athena-handover")
	cfg := Config{Row: row, Out: filepath.Join(t.TempDir(), "out")}
	rec, err := NewRecorder(cfg.Out)
	if err != nil {
		t.Fatal(err)
	}
	st := &closing{notWired: notWired{}}
	_, err = Run(context.Background(), cfg, st, rec, t.Logf)
	if err == nil || !strings.Contains(err.Error(), "wait for #161") {
		t.Fatalf("err = %v, want the not-wired error", err)
	}
	if !st.closed {
		t.Error("the steps were not closed after a failed scenario")
	}
}

type closing struct {
	notWired
	closed bool
}

func (c *closing) Close(context.Context) error { c.closed = true; return nil }
