package alpha

// The dry runs of the three stack rows: the stand-in steps over the fake Slurm
// in run mode, judged by the live path (Run). Each row runs healthy and passes,
// then once per fault, and the fault must fail it with its own label. A row
// that passes with a check removed is the verification that proves nothing; the
// verdict mutants in verdict_test.go and the table below are what rule it out.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
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
		{"no-responses", "no response, so attribution is unchecked"},
		{"duplicate-tag", "a tag names more than one job"},
		{"extra-allocation", "exactly two Allocations"},
		{"adds-helios", "the deployment does not allow exactly one Provider"},
		{"default-lease", "the deployment's leaseLimit is not the row's shortest lease"},
		{"short-lease", "not longer than the lead an Instance needs (PartitionTooShort)"},
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
		// The Helios Provider alone: a deployment that also allowed Athena breaks alpha's fence (HBEP-7, #205).
		{"adds-athena", "the deployment does not allow exactly one Provider"},
		{"default-lease", "the deployment's leaseLimit is not the row's shortest lease"},
		{"short-lease", "not longer than the lead an Instance needs (PartitionTooShort)"},
		{"no-release-when-idle", "lacks ReleaseWhenIdle with a short idleLimit"},
		{"two-restarts", "2 restarts at the worker rung, want 1"},
		{"no-restart", "0 restarts at the worker rung, want 1"},
		{"duplicate-tag", "a tag names more than one job"},
		{"extra-allocation", "exactly two Allocations"},
	},
	"tunnel-hour-helios": tunnelCases,
	"tunnel-hour-athena": tunnelCases,
}

var tunnelCases = []dryCase{
	{"", ""},
	{"short", "the requests lasted less than an hour"},
	{"cut", "a stream was cut during the hour"},
	{"stream-cut-error", "a stream was cut during the hour"},
	{"no-rate", "the upload path rate was not measured"},
	{"short-rate", "the download path rate was asked for under 30 s, timed under 29 s, or moved no bytes"},
	// Two rates that say the same direction: one per direction is the rule, and the upload is then missing.
	{"wrong-direction", "the path rates are not one per direction"},
	{"no-load-rate", "the download path rate was not measured under load"},
	// The upload direction (issue #224): a rate for each, request bodies of 0.1 MB or more, sent among the streams.
	{"no-upload-rate", "the upload path rate was not posted: both directions are needed"},
	{"no-download-rate", "the download path rate was not posted: both directions are needed"},
	{"upload-not-under-load", "the upload path rate was not measured under load"},
	{"no-large", "no request body reached 0.1 MB, so the upload direction was not exercised"},
	{"large-apart", "no request of 0.1 MB or more was sent while the 16 streams were open"},
	{"response-overlap", "no request of 0.1 MB or more was sent while the 16 streams were open"},
	{"untagged", "a job tag was not written down before its submission"},
	{"wrong-partition", "a job ran on another partition than the row's"},
	{"adds-other-cluster", "the deployment does not allow exactly one Provider"},
	{"default-lease", "the deployment's leaseLimit is not the row's shortest lease"},
	{"short-lease", "not longer than the lead an Instance needs (PartitionTooShort)"},
	{"duplicate-tag", "a tag names more than one job"},
	{"extra-allocation", "exactly one Allocation"},
	{"past-deadline", "outlasted the deployment's request deadline"},
	{"past-drain", "ended after the Instance's drain start"},
	{"no-drain-start", "drain start was not observed"},
	// frpc logged in again mid-hour and every request passed: only the session check can fail it.
	{"reconnect", "lastStartTime for the proxy changed"},
	{"disconnected", "TunnelConnected was not True"},
	{"sparse-samples", "more than 2 minutes without a look"},
	{"late-first-sample", "not looked at before the first request"},
	{"no-concurrency", "no 16 concurrent streams"},
	{"slow-streams", "no 16 concurrent streams"},
	{"no-idle-gap", "no idle stretch"},
	{"no-long-stream", "no request of about 4 minutes"},
	{"no-back-to-back", "no run of requests back to back"},
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
				cfg := Config{Row: row, Dry: true, DryFault: c.fault, Out: filepath.Join(t.TempDir(), "out"), PendingMax: time.Minute, FrontLabel: testFrontLabel}
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
		"tunnel-hour-athena": {
			// The stand-in's rates worked out by hand: upload 24 MiB over 30.1 s, download 31 MiB over 30.2 s and 16 streams at
			// 20 tokens a second over 30 s (9,600 tokens). The stand-in sends four large bodies of 100,000 to 400,000 bytes
			// inside the burst's streams.
			"path rate, upload (front to node): 836074 B/s under load (25165824 bytes timed over 30.1 s, asked 30 s)",
			"path rate, download (node to front): 1076353 B/s and 318 tokens/s under load (32505856 bytes, 9600 tokens timed over 30.2 s, asked 30 s)",
			"large requests (a body of 0.1 MB or more): 4, bodies of 100000 to 400000 bytes; 4 of them sent while the 16 streams were open", "1 distinct frps lastStartTime across 63 looks (one is the pass)", "peak streams at 20 tokens/s or more: 16", "longest idle stretch: 3m0s"},
	}
	for name, wants := range cases {
		t.Run(name, func(t *testing.T) {
			row, _ := RowByName(name)
			cfg := Config{Row: row, Dry: true, Out: filepath.Join(t.TempDir(), "out"), PendingMax: time.Minute, FrontLabel: testFrontLabel}
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

// Every row reports the count of its watch resets as a measured fact (the science
// agent's A2 on PR 292), in the text to post and the record, and the verdict does not
// judge it: the watch's own rule fails a test that had a reset, at its end.
func TestEveryRowReportsItsWatchResets(t *testing.T) {
	for _, name := range []string{"alpha-athena-handover", "alpha-helios-nooverlap", "tunnel-hour-athena"} {
		for _, resets := range []int{0, 2} {
			t.Run(fmt.Sprintf("%s/%d", name, resets), func(t *testing.T) {
				row, _ := RowByName(name)
				cfg := Config{Row: row, Dry: true, Out: filepath.Join(t.TempDir(), "out"), PendingMax: time.Minute, FrontLabel: testFrontLabel}
				rec, err := NewRecorder(cfg.Out)
				if err != nil {
					t.Fatal(err)
				}
				st := newDrySteps(t, cfg, familyOf(name))
				st.watchResets = resets
				if f, err := Run(context.Background(), cfg, st, rec, t.Logf); err != nil || len(f) != 0 {
					t.Fatalf("failures %v, err %v; the count is not judged", f, err)
				}
				posts, _ := os.ReadFile(filepath.Join(cfg.Out, PostsFile))
				if want := fmt.Sprintf("- watch resets over the run: %d\n", resets); !strings.Contains(string(posts), want) {
					t.Errorf("the text to post lacks %q:\n%s", want, posts)
				}
				record, _ := os.ReadFile(filepath.Join(cfg.Out, RecordFile))
				if want := fmt.Sprintf(`"watchResets": %d`, resets); !strings.Contains(string(record), want) {
					t.Errorf("the record lacks %q:\n%s", want, record)
				}
			})
		}
	}
}

// A scenario that returns an error is a failed run, not a pass with no verdict,
// and the steps are closed either way.
func TestAScenarioErrorFailsTheRunAndClosesTheSteps(t *testing.T) {
	row, _ := RowByName("alpha-athena-handover")
	cfg := Config{Row: row, Out: filepath.Join(t.TempDir(), "out"), FrontLabel: testFrontLabel}
	rec, err := NewRecorder(cfg.Out)
	if err != nil {
		t.Fatal(err)
	}
	st := &closing{failedSteps: failedSteps{ErrNoDialHomeListen}}
	_, err = Run(context.Background(), cfg, st, rec, t.Logf)
	if !errors.Is(err, ErrNoDialHomeListen) {
		t.Fatalf("err = %v, want the steps' named error", err)
	}
	if !st.closed {
		t.Error("the steps were not closed after a failed scenario")
	}
}

type closing struct {
	failedSteps
	closed bool
}

// A live run in direct mode with no local dial-home listener cannot start: its
// steps return the named error, before anything is started or submitted.
func TestALiveRunWithoutItsListenerNamesWhatIsMissing(t *testing.T) {
	m := liveSettings("alpha-athena-handover")
	delete(m, envListen)
	cfg, skip, err := ConfigFromEnv(env(m), "alpha-athena-handover")
	if skip != "" || err != nil {
		t.Fatalf("skip %q, err %v", skip, err)
	}
	st, ok := liveSteps(t, cfg).(failedSteps)
	if !ok || !errors.Is(st.err, ErrNoDialHomeListen) {
		t.Fatalf("liveSteps = %#v, want the steps that return ErrNoDialHomeListen", st)
	}
	if _, err := st.Handover(context.Background(), Plan{}); !errors.Is(err, ErrNoDialHomeListen) {
		t.Errorf("Handover returned %v", err)
	}
	if _, err := st.Ledger().Jobs(context.Background(), []string{"hb-x-1"}); !errors.Is(err, ErrNoLedger) {
		t.Errorf("the ledger of a run that never started returned %v, want ErrNoLedger", err)
	}
}

func (c *closing) Close(context.Context) error { c.closed = true; return nil }

// The lease the owner chooses is the lease the run uses, judged and posted. With the short lease the hour (61 min)
// must end before the drain start, 91 minutes in: an Instance ready at 20 minutes passes, one ready at 35 minutes
// does not (81 + 15 = 96 minutes after the job starts is past it); with the default lease both pass.
func TestTheChosenLeaseDecidesWhetherASlowStartCanPass(t *testing.T) {
	base := rowOf("tunnel-hour-athena")
	short, err := WithTunnelLease(base, MinLease())
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name  string
		row   Row
		fault string
		want  string // "" for a pass
	}{
		{"default lease, ready at 20 min", base, "", ""},
		{"default lease, ready at 35 min", base, "slow-start", ""},
		{"short lease, ready at 20 min", short, "", ""},
		{"short lease, ready at 35 min", short, "slow-start", "the hour ended after the Instance's drain start"},
	} {
		t.Run(c.name, func(t *testing.T) {
			cfg := Config{Row: c.row, Dry: true, DryFault: c.fault, Out: filepath.Join(t.TempDir(), "out"), PendingMax: time.Minute, FrontLabel: testFrontLabel}
			rec, err := NewRecorder(cfg.Out)
			if err != nil {
				t.Fatal(err)
			}
			failures, err := Run(context.Background(), cfg, newDrySteps(t, cfg, "tunnel"), rec, t.Logf)
			if err != nil {
				t.Fatal(err)
			}
			if c.want == "" && len(failures) != 0 || c.want != "" && !strings.Contains(Summary(failures), c.want) {
				t.Errorf("failed checks %q, want %q", Summary(failures), c.want)
			}
			posts, _ := os.ReadFile(filepath.Join(cfg.Out, PostsFile))
			if want := "leaseLimit " + c.row.Lease.String(); !strings.Contains(string(posts), want) {
				t.Errorf("the text to post lacks %q:\n%s", want, posts)
			}
		})
	}
}

// A job that pends past the first clock is cancelled by its tag and the run ends without a verdict: NOT RUN on
// every row (the science agent's ruling of 8 Nov 2026, #272: a row with no slot, such as tunnel-hour-athena, used
// to fail here, and a queue wait tests nothing). No record and no text to post are written, the run's own job
// was cancelled and never started, and the owner's job under the same QOS is left running. The expected outcome
// is the ruling's, not read from the code under test. The bound is the one the case names, written out in its own
// units, and the states are read at the times the issue's clock gives: the job still pending one second before
// the bound, cancelled by its tag after it, not a minute either side.
func TestAJobPendingPastTheBoundIsCancelledByItsTag(t *testing.T) {
	for _, c := range []struct {
		row                    string
		bound                  time.Duration
		wantStill, wantCancels string // the stand-in's two readings, as the issue's clock gives them
		wantNotRun, ownerJob   bool
	}{
		{"alpha-helios-nooverlap", 10 * time.Minute, "9m59s", "10m1s", true, true},
		{"alpha-athena-handover", 10 * time.Minute, "9m59s", "10m1s", true, false},
		{"tunnel-hour-helios", 30 * time.Minute, "29m59s", "30m1s", true, true},
		{"tunnel-hour-athena", time.Minute, "59s", "1m1s", true, false},
	} {
		t.Run(c.row, func(t *testing.T) {
			row := rowOf(c.row)
			cfg := Config{Row: row, Dry: true, DryFault: "pend-past-bound", Out: filepath.Join(t.TempDir(), "out"), PendingMax: c.bound, FrontLabel: testFrontLabel}
			rec, err := NewRecorder(cfg.Out)
			if err != nil {
				t.Fatal(err)
			}
			st := newDrySteps(t, cfg, familyOf(c.row))
			began := st.now()
			var logged []string
			logf := func(format string, a ...any) {
				logged = append(logged, fmt.Sprintf(format, a...))
				t.Logf(format, a...)
			}
			failures, err := Run(context.Background(), cfg, st, rec, logf)
			if !errors.Is(err, ErrNotStarted) || len(failures) != 0 {
				t.Fatalf("failures %v, err %v, want ErrNotStarted and no verdict", failures, err)
			}
			if got := NotRun(row, err); got != c.wantNotRun {
				t.Errorf("NotRun = %t, want %t", got, c.wantNotRun)
			}
			// Still pending just before the bound, cancelled just after it: the readings of the manual clock.
			for _, want := range []string{
				"stand-in: the job is still pending " + c.wantStill + " after its submission",
				"stand-in: the run's jobs are cancelled by their tags " + c.wantCancels + " after the job's submission",
			} {
				if !containsLine(logged, want) {
					t.Errorf("the run did not log %q; it logged:\n%s", want, strings.Join(logged, "\n"))
				}
			}
			// The cluster's own record agrees: the first clock ran its length on the manual clock, and the
			// cancel took a few seconds at most.
			if waited := st.now().Sub(began); waited < cfg.PendingMax || waited > cfg.PendingMax+10*time.Second {
				t.Errorf("the wait lasted %s, want the bound of %s", waited, cfg.PendingMax)
			}
			for _, f := range []string{RecordFile, PostsFile} {
				if _, err := os.Stat(filepath.Join(cfg.Out, f)); err == nil {
					t.Errorf("a run that ended on the first clock wrote %s", f)
				}
			}
			var ours, owners int
			for _, j := range st.plat.Jobs() {
				if strings.HasPrefix(j.Name, "hb-") {
					ours++
					if j.State != fakeslurm.Cancelled || !j.Start.IsZero() {
						t.Errorf("the run's job is %s (started %t), want cancelled and never started", j.State, !j.Start.IsZero())
					}
					if pended := j.End.Sub(j.Submit); pended < c.bound || pended > c.bound+10*time.Second {
						t.Errorf("the run's job was cancelled %s after its submission, want the bound of %s", pended, c.bound)
					}
					continue
				}
				owners++
				if j.State != fakeslurm.Running {
					t.Errorf("the owner's job is %s, want it left running", j.State)
				}
				// It is held by a release file the cleanup writes, not by a stretch of wall-clock time (PR 230, B2): a
				// stalled run must not find the owner's job gone and the slot free.
				if !strings.Contains(j.Wrap, "while [ ! -e ") || !strings.Contains(j.Wrap, "release-") || strings.Contains(j.Wrap, "sleep 600") {
					t.Errorf("the owner's job is not held on a release gate: %q", j.Wrap)
				}
			}
			if ours != 1 || owners != map[bool]int{true: 1, false: 0}[c.ownerJob] {
				t.Errorf("%d jobs of the run and %d of the owner's", ours, owners)
			}
		})
	}
}

func containsLine(lines []string, want string) bool {
	for _, l := range lines {
		if l == want {
			return true
		}
	}
	return false
}

// notStarted is a scenario whose first job never started, with the error it ends on; its Close can fail.
type notStarted struct {
	failedSteps
	closeErr error
}

func (n notStarted) Close(context.Context) error { return n.closeErr }

// neverStartedErrors are the three ways a job of a run never starts within a bound, written out here: the wait in
// the queue (the first clock), the deployment's first end (both the bare sentinel and the error that carries the
// bound), as the ruling of 8 Nov 2026 on #272 lists them.
func neverStartedErrors() map[string]error {
	return map[string]error{
		"the wait in the queue":   &NotStartedError{Bound: 2 * time.Hour},
		"the bare sentinel":       ErrNotStarted,
		"the deployment's end":    &NotStartedError{Bound: 121 * time.Minute, PastEnd: true},
		"the bare end sentinel":   ErrQueueWaitPastEnd,
		"a wrapped end":           fmt.Errorf("the scenario: %w", &NotStartedError{Bound: time.Hour, PastEnd: true}),
		"a wrapped pending bound": fmt.Errorf("the scenario: %w", &NotStartedError{Bound: time.Hour}),
	}
}

// stackRows are the rows this package runs through Run; the count is written out so that a row added later is
// added to the table below, not left out of it.
func stackRows(t *testing.T) []Row {
	t.Helper()
	out := Rows()
	if len(out) != 4 {
		t.Fatalf("the package runs %d stack rows, the table below was written for 4: add the new row's cases", len(out))
	}
	return out
}

// allRows are the rows of every kind (stack, accounting, reach); the count is written out too.
func allRows(t *testing.T) []Row {
	t.Helper()
	out := append(append(Rows(), AccountingRows()...), ReachRows()...)
	if len(out) != 8 {
		t.Fatalf("there are %d rows, the table below was written for 8: add the new row's cases", len(out))
	}
	return out
}

// A job that never started within its bound is NOT RUN on EVERY row, whichever bound ran out (the science agent's
// ruling of 8 Nov 2026, #272): the rule reads no property of the row, so a rule by row (a slot, a job count) added
// later fails here. Rows with no test of this package (the accounting and reach rows) are in the table too, since
// the rule is the same function; an error that is not a never-started one is a failure on every row.
func TestEveryRowEndsNotRunWhenItsJobNeverStarts(t *testing.T) {
	for _, r := range allRows(t) {
		for name, err := range neverStartedErrors() {
			if !NotRun(r, err) {
				t.Errorf("%s: NotRun(%s) = false, want true on every row", r.Name, name)
			}
		}
		for _, err := range []error{errors.New("another"), fmt.Errorf("the transport: %w", slurm.ErrNotStarted), context.DeadlineExceeded, nil} {
			if NotRun(r, err) {
				t.Errorf("%s: NotRun(%v) = true, want a failure for an error that is not a job that never started", r.Name, err)
			}
		}
	}
}

// Through Run, on every stack row and for each way a job never starts: a clean close ends NOT RUN, a close that
// failed may have left a job behind and is a failure, never a NOT RUN.
func TestEveryStackRowRunsToNotRunAndAFailedCloseIsAFailure(t *testing.T) {
	for _, row := range stackRows(t) {
		for name, nerr := range neverStartedErrors() {
			for _, c := range []struct {
				close      string
				closeErr   error
				wantNotRun bool
			}{
				{"closes cleanly", nil, true},
				{"fails to close", errors.New("scancel did not answer"), false},
			} {
				t.Run(row.Name+"/"+name+"/"+c.close, func(t *testing.T) {
					cfg := Config{Row: row, Out: filepath.Join(t.TempDir(), "out"), FrontLabel: testFrontLabel}
					rec, err := NewRecorder(cfg.Out)
					if err != nil {
						t.Fatal(err)
					}
					_, err = Run(context.Background(), cfg, notStarted{failedSteps: failedSteps{err: nerr}, closeErr: c.closeErr}, rec, t.Logf)
					if err == nil {
						t.Fatal("the run returned no error")
					}
					if got := NotRun(row, err); got != c.wantNotRun {
						t.Errorf("NotRun(%v) = %t, want %t", err, got, c.wantNotRun)
					}
				})
			}
		}
	}
}

// The two-job rows through Run, with the stand-in's real fake cluster (review of PR 284, B3): the first job ran and the
// second never started is NOT RUN, with no record and no text to post. An earlier failure of the first job's part still
// wins: the row fails with that failure's words and says so in its record and its text. Each row's checks of the
// second job (the handover itself, the gap) are not among them, since nothing tested them.
func TestASecondJobThatNeverStartedIsNotRunUnlessAnEarlierFailureStands(t *testing.T) {
	for _, row := range []string{"alpha-athena-handover", "alpha-helios-nooverlap"} {
		for _, c := range []struct {
			fault, wantFailure string
		}{
			{"second-never-started", ""},
			// What was read of ReplacementLate is information: each reading ends the same way, either way.
			{"second-never-started-late", ""},
			{"second-never-started-unknown", ""},
			{"second-never-started-cut", "a stream was cut"},
			{"second-never-started-late-cut", "a stream was cut"},
			{"second-never-started-no-restart", "0 restarts at the worker rung, want 1"},
		} {
			t.Run(row+"/"+c.fault, func(t *testing.T) {
				cfg := Config{Row: rowOf(row), Dry: true, DryFault: c.fault, Out: filepath.Join(t.TempDir(), "out"), PendingMax: 10 * time.Minute, FrontLabel: testFrontLabel}
				rec, err := NewRecorder(cfg.Out)
				if err != nil {
					t.Fatal(err)
				}
				st := newDrySteps(t, cfg, familyOf(row))
				failures, err := Run(context.Background(), cfg, st, rec, t.Logf)
				_, postErr := os.Stat(filepath.Join(cfg.Out, PostsFile))
				_, recErr := os.Stat(filepath.Join(cfg.Out, RecordFile))
				if c.wantFailure == "" {
					if !errors.Is(err, ErrSecondNotStarted) || !NotRun(cfg.Row, err) || len(failures) != 0 {
						t.Fatalf("failures %v, err %v; want the NOT RUN of a second job that never started", failures, err)
					}
					if postErr == nil || recErr == nil {
						t.Errorf("a NOT RUN wrote a text to post (%v) or a record (%v)", postErr, recErr)
					}
				} else {
					if err != nil || NotRun(cfg.Row, err) {
						t.Fatalf("err %v: an earlier failure must end the run as that failure", err)
					}
					if len(failures) != 1 || failures[0] != c.wantFailure {
						t.Fatalf("failures %v, want only %q: nothing about the second job, which tested nothing", failures, c.wantFailure)
					}
					if postErr != nil || recErr != nil {
						t.Errorf("a failed run wrote no text to post (%v) or no record (%v)", postErr, recErr)
					}
					post, _ := os.ReadFile(filepath.Join(cfg.Out, PostsFile))
					if !strings.Contains(string(post), "the first job ran and the second never started") {
						t.Errorf("the text to post does not say the second job never started:\n%s", post)
					}
					// What was read of ReplacementLate stays on a failure too: information, whichever way the run ends.
					wantLate := LateNotRaised
					if strings.Contains(c.fault, "-late") {
						wantLate = LateRaised
					}
					if want := "ReplacementLate for its deployment: " + wantLate + " (information, not a check)"; !strings.Contains(string(post), want) {
						t.Errorf("the text to post lacks %q:\n%s", want, post)
					}
				}
				// The cluster agrees: the first job ran, and the second was cancelled and never started.
				var ran, never int
				for _, j := range st.plat.Jobs() {
					switch {
					case !j.Start.IsZero():
						ran++
					case j.State == fakeslurm.Cancelled:
						never++
					}
				}
				if ran != 1 || never != 1 {
					t.Errorf("%d jobs ran and %d were cancelled unstarted, want one of each", ran, never)
				}
			})
		}
	}
}

// A close that fails after a second job that never started may have left a job behind: a failure, never a NOT RUN.
func TestAFailedCloseAfterASecondJobThatNeverStartedIsAFailure(t *testing.T) {
	cfg := Config{Row: rowOf("alpha-athena-handover"), Dry: true, DryFault: "second-never-started", Out: filepath.Join(t.TempDir(), "out"), PendingMax: 10 * time.Minute, FrontLabel: testFrontLabel}
	rec, err := NewRecorder(cfg.Out)
	if err != nil {
		t.Fatal(err)
	}
	st := failingClose{newDrySteps(t, cfg, "handover")}
	_, err = Run(context.Background(), cfg, st, rec, t.Logf)
	if err == nil || NotRun(cfg.Row, err) || !strings.Contains(err.Error(), "scancel did not answer") {
		t.Errorf("err = %v, want a failure that names the close", err)
	}
}

// failingClose is the stand-in's steps with a Close that fails.
type failingClose struct{ *drySteps }

func (failingClose) Close(context.Context) error { return errors.New("scancel did not answer") }

// The marker names the row and the wait, and no job: the script prints and posts around it.
func TestTheNotRunNoteNamesNoJob(t *testing.T) {
	second := NotRunNote(rowOf("alpha-helios-nooverlap"), 30*time.Minute, fmt.Errorf("the scenario: %w", ErrSecondNotStarted))
	if want := "alpha-helios-nooverlap: the first job ran and the second never started; ReplacementLate for its deployment: unknown; this run's jobs were cancelled by tag\n"; second != want {
		t.Errorf("note %q, want %q", second, want)
	}
	note := NotRunNote(rowOf("tunnel-hour-helios"), 30*time.Minute, ErrNotStarted)
	if want := "tunnel-hour-helios: a job was still pending after 30m0s;"; !strings.Contains(note, want) {
		t.Errorf("note %q lacks %q", note, want)
	}
	if strings.Contains(note, "hb-") {
		t.Errorf("the note names a tag: %q", note)
	}
}
