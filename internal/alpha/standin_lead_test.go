package alpha

// The stand-in proof of the handover lead (issue #447): the LIVE steps of the three rows that open a stream across a drain start,
// over the real stack on this host and the fake Slurm (standin_test.go), with the engine double or the row's deadline changed so
// that the deployment's max-model-len binds the lead as it did on the A100. Both run only with HPCBURST_ALPHA_STANDIN=1, under the
// stand-in's rules (launcher.lock, a memory cap; heavy runs on a hosted runner).
//
// The stand-in's deployment has the context 4096 (stack.ServedDeployment) and its engine double streams 50 tokens a second (a 20 ms
// token delay), so (4096 - 512) / 50 / 2 = 35.8 s fits, and the stand-in's deadline of 90 s has a third of 30 s: the stand-in's own
// lead is the deadline's third, as before the lead was sized. The two tests move one side each, to the A100's situation:
//   - a fast engine (a 5 ms token delay, about 200 tokens a second, the A100's 199): 3584 / 200 / 2 = 9 s fits, under the 30 s
//     floor, and the row is NOT RUN before any stream opens;
//   - a deadline of 150 s, whose third, 50 s, is longer than the 35 s that fits: the lead is what fits, between the floor and the
//     deadline's third, where the deadline's third would make a stream of 4,500 tokens, more than the context holds (the old code
//     ended these runs as "did not finish").

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
)

// standInFastEngine is the engine double's setting that makes it stream about as fast as the A100 (199 tokens a second).
const standInFastEngine = "token-delay=5ms"

// handoverLeadRows are the rows that open a stream across a drain start, and so size a lead (streamLead).
var handoverLeadRows = []string{"alpha-athena-handover", "alpha-helios-nooverlap", "alpha-athena-now-nooverlap"}

// standInLeadRun is one run of a row's LIVE steps over the stand-in, through Run, with the engine double's setting changed to engine
// when it is not empty and the row's clock changed by clock when it is not nil. The run's Close keeps the stack's own stop apart from
// the run's error, as the second-held stand-ins do.
func standInLeadRun(t *testing.T, name, engine string, clock func(Row) Row) secondHeldRun {
	t.Helper()
	if os.Getenv(envStandIn) != "1" {
		t.Skipf("the stand-in proof of the live steps runs only with %s=1 (internal/alpha, standin_test.go)", envStandIn)
	}
	row := standInRow(t, name)
	if clock != nil {
		row = clock(row)
	}
	id, err := NewRunID()
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Row: row, StandIn: true, Out: filepath.Join(t.TempDir(), "out"), PendingMax: 2 * time.Minute, RunID: id, FrontLabel: standInFrontLabel}
	t.Logf("%s: %s, engine %q, deadline %s, lease %s", row.Name, cfg.Mode(), engine, row.Clock().MaxRequestDuration, row.Lease)
	rec, err := NewRecorder(cfg.Out)
	if err != nil {
		t.Fatal(err)
	}
	w, cl := standInWorldShaped(t, row, nil)
	if engine != "" {
		set := false
		env := w.md.Spec.Source.Served.Env
		for i := range env {
			if env[i].Name == "HB_ENGINE_DOUBLE" {
				env[i].Value, set = engine, true
			}
		}
		if !set {
			t.Fatal("the stand-in's deployment has no engine double setting to change")
		}
	}
	if name == "alpha-athena-now-nooverlap" {
		// Row B starts both jobs warm: the pre-staging row's staging is done, and the agent's own log is read from its tail, as
		// standInNowNoOverlap does. Without it the run ends NOT RUN for the warm start its log did not show (run 43200139547,
		// fit-row-b: ten of ten), after the lead and the streams have worked.
		preStage(t, cl, w)
	}
	lr := newLiveRun(t, cfg, w)
	lr.stop = &stackStop{}
	if name == "alpha-athena-now-nooverlap" {
		lr.agentLogFn = func(path string) string { return "tail -c 65536 " + path }
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.PendingMax+20*time.Minute)
	defer cancel()
	failures, err := Run(ctx, cfg, lr, rec, t.Logf)
	return secondHeldRun{row: row, cfg: cfg, cl: cl, prefix: TagPrefix(id), failures: failures, err: err, stop: *lr.stop}
}

// TestStandInAFastEngineEndsNotRunBeforeAnyStream is the incident of 12 Nov 2026 at the stand-in's scale: an engine as fast as the
// A100's on a context too small for a stream that can tell a cut from an end. The row stops after its rate stream: NOT RUN (the
// script's exit 3), no failure, no record and no text to post, nothing left running, and the marker names the context and a rate
// above 59 tokens a second, the rate over which (4096 - 512) tokens give a lead under 30 s.
//
//livecheck:none a stand-in on this host, no cluster: run by hand with HPCBURST_ALPHA_STANDIN=1
func TestStandInAFastEngineEndsNotRunBeforeAnyStream(t *testing.T) {
	for _, name := range handoverLeadRows {
		t.Run(name, func(t *testing.T) {
			s := standInLeadRun(t, name, standInFastEngine, nil)
			if err := s.stop.problem(); err != nil {
				t.Fatalf("the stack did not stop cleanly, whatever the run ended with: %v", err)
			}
			if len(s.failures) != 0 || !errors.Is(s.err, ErrLeadTooShort) || !NotRun(s.row, s.err) {
				t.Fatalf("failures %v, err %v; want the NOT RUN of a lead that does not fit", s.failures, s.err)
			}
			var lt *LeadTooShortError
			if !errors.As(s.err, &lt) || lt.MaxModelLen != 4096 || lt.Rate <= 59 || lt.Fit >= LeadFloor {
				t.Errorf("the error is %+v, want the stand-in's context 4096, a rate over 59 tokens a second and a fit under the floor", lt)
			}
			note := NotRunNote(s.row, s.cfg.PendingMax, s.err)
			t.Logf("the marker would say: %s", strings.TrimSpace(note))
			if !strings.Contains(note, "with max-model-len 4096 the lead that fits is ") {
				t.Errorf("the marker %q does not name the context", note)
			}
			s.wroteNothing(t, "a NOT RUN")
			s.nothingLeftRunning(t)
		})
	}
}

// TestStandInTheLeadIsWhatFitsWhenTheDeadlinesThirdDoesNot is the other half: a deadline of 150 s, whose third, 50 s, is longer than the
// 35 s the stand-in's context fits at its 50 tokens a second. The row runs, and passes its own verdict, with a stream that opens
// between the floor (30 s) and the deadline's third (50 s) before the drain start, is in flight at it and ends after it. Without the
// minimum the lead is 50 s and the stream of 4,500 tokens does not fit: the run ends as "did not finish".
//
//livecheck:none a stand-in on this host, no cluster: run by hand with HPCBURST_ALPHA_STANDIN=1
func TestStandInTheLeadIsWhatFitsWhenTheDeadlinesThirdDoesNot(t *testing.T) {
	for _, name := range handoverLeadRows {
		t.Run(name, func(t *testing.T) {
			// A deadline of 150 s makes a drain window of 3 minutes here, so the lease that holds the Instance's lead (the drain
			// window, the 30 s handover margin and the 4-minute start timeout: 7.5 minutes) is 8.
			s := standInLeadRun(t, name, "", func(r Row) Row {
				r.Timing.MaxRequestDuration, r.Lease = 150*time.Second, 8*time.Minute
				return r
			})
			if err := s.stop.problem(); err != nil {
				t.Fatalf("the stack did not stop cleanly, whatever the run ended with: %v", err)
			}
			if s.err != nil || len(s.failures) > 0 {
				t.Fatalf("%s over the stand-in: failures %v, err %v; want a PASS", s.row.Name, s.failures, s.err)
			}
			record, err := os.ReadFile(filepath.Join(s.cfg.Out, RecordFile))
			if err != nil {
				t.Fatal(err)
			}
			var rec struct {
				Observation struct {
					DrainStart time.Time `json:"drainStart"`
					Streams    []Stream  `json:"streams"`
				} `json:"observation"`
			}
			if err := json.Unmarshal(record, &rec); err != nil || rec.Observation.DrainStart.IsZero() || len(rec.Observation.Streams) == 0 {
				t.Fatalf("the record holds no drain start and streams (%v): %s", err, record)
			}
			ds := rec.Observation.DrainStart
			// The first stream opens a lead before the drain start (the handover's second follows its first token). The lead is
			// the stand-in's own to the second: the stream opens when the wait for it ends, a poll after the lead's time.
			first := rec.Observation.Streams[0]
			lead := ds.Sub(first.Opened)
			t.Logf("%s: the first stream opened %s before the drain start and ended %s after it, %d tokens", s.row.Name, lead.Round(time.Second), first.Ended.Sub(ds).Round(time.Second), first.Tokens)
			if lead < 30*time.Second || lead >= 50*time.Second {
				t.Errorf("the first stream opened %s before the drain start, want at least the floor's 30 s and under the deadline's third, 50 s", lead)
			}
			for i, st := range rec.Observation.Streams {
				if !st.Opened.Before(ds) || !st.Ended.After(ds) || !st.Complete || st.CutBy != "" {
					t.Errorf("stream %d opened %s, ended %s, complete %t, cut by %q; want it in flight at the drain start %s and ended after it, uncut", i, st.Opened, st.Ended, st.Complete, st.CutBy, ds)
				}
			}
		})
	}
}

// nothingLeftRunning checks that no job of the run is pending or running on the fake cluster: the run's cleanup cancelled them by
// their tags.
func (s secondHeldRun) nothingLeftRunning(t *testing.T) {
	t.Helper()
	for _, j := range s.cl.Plat.Jobs() {
		if strings.HasPrefix(j.Name, s.prefix) && (j.State == fakeslurm.Pending || j.State == fakeslurm.Running) {
			t.Errorf("a job of the run is %s after the run", j.State)
		}
	}
}
