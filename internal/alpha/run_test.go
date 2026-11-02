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

// A scenario that returns an error is a failed run, not a pass with no verdict,
// and the steps are closed either way.
func TestAScenarioErrorFailsTheRunAndClosesTheSteps(t *testing.T) {
	row, _ := RowByName("alpha-athena-handover")
	cfg := Config{Row: row, Out: filepath.Join(t.TempDir(), "out"), FrontLabel: testFrontLabel}
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
