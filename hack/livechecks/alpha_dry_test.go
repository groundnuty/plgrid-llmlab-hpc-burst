package livechecks

// The dry runs of the alpha rows through the script (issue #194). The row's own
// test is compiled and run for real, by the script, as it runs it live: the
// script builds, takes the slot, runs the binary, reads its verbose log for its
// verdict and cleans up. Two things differ from a live run, and both are taken by
// the wrapper below, which stands where the cluster would: the binary gets the
// dry switch and loses the live settings, so its steps are the stand-ins over a
// fake Slurm in run mode (internal/alpha/dry_test.go, and gpuacct_test.go for
// the accounting rows). The script's side keeps the bash stubs.
//
// A dry run is the verification that proves nothing if the test skips its own
// checks, so the healthy run passes and every fault fails the row with exit 1:
// the checks are reached and run. The faults are the stand-ins' (the same
// names as in internal/alpha/run_test.go and gpuacct_test.go).
//
// The compiled binaries come from HPCBURST_LIVECHECKS_ALPHA_TEST and
// HPCBURST_LIVECHECKS_CONF_TEST when set (a developer builds them under the
// shared build lock); CI leaves them unset and the test builds them.

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// dryWrapper stands for the compiled test of a row. It is run by the script from the fixture package's
// directory, with the live settings; it removes them (the cluster's side), turns the dry switch on, names the
// stand-in's fault, and runs the real binary from its real package directory.
const dryWrapper = `#!/bin/bash
echo "testbin dry" >> "$STUB_STATE/events"
fault=${STUB_DRY_FAULT:-}
if [ -n "${HPCBURST_CONFORMANCE:-}" ]; then
  # the conformance package's rows: the row is named by the platform, the record directory is the run's. The reach
  # rows (issue #225) keep the gateway's settings, which are what the test points its probe at (the stand-in the
  # caller started); they go under the dry names, and every live setting goes.
  case " $* " in
    *" -test.run ^TestReach$ "*)
      export HPCBURST_ALPHA_ROW="reach-$HPCBURST_CONF_PROFILE" HPCBURST_ALPHA_PROBE_HOST="$HPCBURST_CONF_PROBE_HOST" \
        HPCBURST_ALPHA_PROBE_PORT="$HPCBURST_CONF_PROBE_PORT" HPCBURST_ALPHA_PROBE_LABEL="$HPCBURST_CONF_PROBE_LABEL" ;;
    *" -test.run ^TestReachHostDialed$ "*)
      # The host-dialed relay rows (issue #305): the agent the fake node runs is the one the caller built (the stub go
      # builds nothing real), the relays' ssh is the real one (the stubs come off PATH), and the run's directories are
      # a short one of the caller's, since a unix socket's path has at most 107 bytes.
      export HPCBURST_ALPHA_ROW="reach-host-dialed-$HPCBURST_CONF_PROFILE" HPCBURST_ALPHA_AGENT_BIN="${STUB_AGENT_BIN:-}" TMPDIR="${STUB_SHORT_TMP:-$TMPDIR}"
      export PATH="${PATH#"${STUB_STATE%/state}/bin:"}" ;;
    *) export HPCBURST_ALPHA_ROW="gpu-accounting-$HPCBURST_CONF_PROFILE" ;;
  esac
  export HPCBURST_ALPHA_OUT="$HPCBURST_CONF_OUT"
  unset $(env | sed -n 's/^\(HPCBURST_CONF[A-Z_]*\)=.*/\1/p')
else
  unset HPCBURST_ALPHA_ACCOUNT HPCBURST_ALPHA_CREDENTIALS HPCBURST_ALPHA_HOST_LOCK HPCBURST_ALPHA_CONTROL_DIR HPCBURST_ALPHA_BIN_DIR \
    HPCBURST_ALPHA_BUILD_DIR HPCBURST_ALPHA_DIALHOME_MODE HPCBURST_ALPHA_DIALHOME_ADDR HPCBURST_ALPHA_DIALHOME_KEY
fi
export HPCBURST_ALPHA_DRY=1 HPCBURST_ALPHA_DRY_FAULT="$fault"
cd "@REALDIR@" || exit 3
exec "@REAL@" "$@"
`

// dryWrapperKeepsLive is the wrapper that forgets to take the live settings away.
const dryWrapperKeepsLive = `#!/bin/bash
echo "testbin dry" >> "$STUB_STATE/events"
export HPCBURST_ALPHA_DRY=1
cd "@REALDIR@" || exit 3
exec "@REAL@" "$@"
`

// liveWrapper is the wrapper that does not turn the dry switch on, whatever the caller's environment says.
const liveWrapper = `#!/bin/bash
echo "testbin live" >> "$STUB_STATE/events"
cd "@REALDIR@" || exit 3
exec "@REAL@" "$@"
`

// realTestBinary returns the compiled test of a package: the prebuilt one when its variable is set, else one build per
// package for the whole process, into a directory of the process's own that TestMain removes (the review of PR 306: every
// dry case built the same package again). No test changes the binary.
func realTestBinary(t *testing.T, envName, pkg string) string {
	t.Helper()
	if p := os.Getenv(envName); p != "" {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("%s names no file: %v", envName, err)
		}
		return p
	}
	testBuilds.mu.Lock()
	defer testBuilds.mu.Unlock()
	if b, ok := testBuilds.done[pkg]; ok {
		if b.err != nil {
			t.Fatal(b.err)
		}
		return b.path
	}
	var b testBuild
	if testBuilds.dir == "" {
		testBuilds.dir, b.err = os.MkdirTemp("", "lct")
	}
	if b.err == nil {
		b.path = filepath.Join(testBuilds.dir, strings.ReplaceAll(pkg, "/", "_")+".test")
		cmd := exec.Command("go", "test", "-c", "-o", b.path, "./"+pkg)
		cmd.Dir = repoRoot(t)
		if out, err := cmd.CombinedOutput(); err != nil {
			b.err = fmt.Errorf("go test -c ./%s: %v\n%s", pkg, err, out)
		}
	}
	if testBuilds.done == nil {
		testBuilds.done = map[string]testBuild{}
	}
	testBuilds.done[pkg] = b
	if b.err != nil {
		t.Fatal(b.err)
	}
	return b.path
}

// testBuilds are the packages' test binaries this process built, one each.
var testBuilds struct {
	mu   sync.Mutex
	dir  string
	done map[string]testBuild
}

type testBuild struct {
	path string
	err  error
}

func (w *world) installDry(template, real, realDir string) {
	w.t.Helper()
	body := strings.NewReplacer("@REAL@", real, "@REALDIR@", realDir).Replace(template)
	write(w.t, filepath.Join(w.state, "testbin.sh"), body, 0o755)
}

type dryRow struct {
	row, pkg, env string
	faults        []string
	// passing are variants of the stand-in that must still pass the row.
	passing []string
	// notRun are variants that must end NOT RUN: exit 3, the line, the marker, no text to post.
	notRun []string
}

var handoverFaults = []string{"cut", "no-stream", "two-restarts", "no-restart", "no-handover", "late-successor", "wrong-partition", "untagged", "unattributed", "adds-helios", "default-lease", "short-lease", "duplicate-tag", "extra-allocation", "no-responses", "second-never-started-cut", "second-never-started-late-cut", "second-never-started-no-restart"}
var noOverlapFaults = []string{"nocap", "nogap", "noslot", "cut", "no-stream", "late-submit", "untagged", "wrong-partition", "adds-athena", "default-lease", "short-lease", "no-release-when-idle", "duplicate-tag", "extra-allocation", "two-restarts", "no-restart", "second-never-started-cut", "second-never-started-late-cut", "second-never-started-no-restart"}
var tunnelFaults = []string{"short", "cut", "no-rate", "short-rate", "wrong-direction", "untagged", "wrong-partition", "adds-other-cluster", "default-lease", "short-lease", "duplicate-tag", "extra-allocation", "past-deadline", "past-drain", "no-drain-start", "stream-cut-error", "reconnect", "disconnected", "sparse-samples", "late-first-sample", "no-concurrency", "slow-streams", "no-idle-gap", "no-long-stream", "no-back-to-back", "no-load-rate", "no-large", "large-apart", "response-overlap", "no-upload-rate", "no-download-rate", "upload-not-under-load"}
var accountingFaults = []string{"node-two", "node-none"}

// nowServeFaults are row A's failing faults (issue #376; internal/alpha, run_test.go's nowServeCases); its NOT RUNs are
// TestRowAsNotRunsThroughTheScript's.
var nowServeFaults = []string{"cut", "short-stream", "past-deadline", "unattributed", "no-responses", "no-restart", "two-restarts", "ladder-instance-rung", "ladder-climb", "wrong-identity",
	"events-missing", "events-unchecked", "gauge-problem", "not-ready", "untagged", "wrong-partition", "default-lease", "short-lease", "duplicate-tag",
	"extra-allocation"}

// nowNoOverlapFaults are row B's failing faults (issue #376; internal/alpha, run_test.go's nowNoOverlapCases); its NOT RUNs
// are TestRowBsNotRunsThroughTheScript's.
var nowNoOverlapFaults = []string{"cut", "no-stream", "noslot", "nogap", "early-submit", "created-early", "order-unseen", "marked", "not-ended",
	"not-ready", "successor-not-ready", "refused-no-owner", "no-pred-tag", "untagged", "wrong-partition", "adds-helios", "default-lease", "short-lease",
	"no-release-when-idle", "duplicate-tag", "extra-allocation", "second-never-started-cut", "slot-taken-cut"}

// accountingPassing: a cluster that does not confine devices lists the whole node to nvidia-smi, and one that
// does not set CUDA_VISIBLE_DEVICES leaves nvidia-smi the witness; neither is a false failure.
var accountingPassing = []string{"smi-whole-node", "cvd-unset"}

const (
	alphaPkg = "internal/alpha"
	confPkg  = "internal/providerctl/conformance"
)

var dryRows = []dryRow{
	{"alpha-athena-handover", alphaPkg, "HPCBURST_LIVECHECKS_ALPHA_TEST", handoverFaults, nil, nil},
	{"alpha-helios-nooverlap", alphaPkg, "HPCBURST_LIVECHECKS_ALPHA_TEST", noOverlapFaults, nil, nil},
	{"tunnel-hour-helios", alphaPkg, "HPCBURST_LIVECHECKS_ALPHA_TEST", tunnelFaults, nil, nil},
	{"tunnel-hour-athena", alphaPkg, "HPCBURST_LIVECHECKS_ALPHA_TEST", tunnelFaults, nil, nil},
	{"gpu-accounting-helios", confPkg, "HPCBURST_LIVECHECKS_CONF_TEST", accountingFaults, accountingPassing, []string{"slot-held"}},
	{"gpu-accounting-athena", confPkg, "HPCBURST_LIVECHECKS_CONF_TEST", accountingFaults, accountingPassing, nil},
	// plgrid-now (issue #145): the Athena twin's job under the partition's own one-job slot, which the owner's job can hold.
	{"gpu-accounting-athena-now", confPkg, "HPCBURST_LIVECHECKS_CONF_TEST", accountingFaults, accountingPassing, []string{"slot-held"}},
	{"alpha-athena-now-serve", alphaPkg, "HPCBURST_LIVECHECKS_ALPHA_TEST", nowServeFaults, nil, nil},
	{"alpha-athena-now-nooverlap", alphaPkg, "HPCBURST_LIVECHECKS_ALPHA_TEST", nowNoOverlapFaults, nil, nil},
}

func containsString(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// postsOf returns the text the row left to post, from the run's record directory.
func (w *world) postsOf(row string) string {
	w.t.Helper()
	m, _ := filepath.Glob(filepath.Join(w.out(), row+"-*", "posts.md"))
	if len(m) != 1 {
		w.t.Fatalf("want one posts.md for %s, found %v", row, m)
	}
	b, err := os.ReadFile(m[0])
	if err != nil {
		w.t.Fatal(err)
	}
	return string(b)
}

func TestEveryAlphaRowPassesItsDryRunAndEveryFaultFailsIt(t *testing.T) {
	for _, d := range dryRows {
		real := realTestBinary(t, d.env, d.pkg)
		realDir := filepath.Join(repoRoot(t), d.pkg)
		var shape alphaShape
		for _, s := range alphaShapes {
			if s.row == d.row {
				shape = s
			}
		}
		for _, fault := range append(append(append([]string{""}, d.passing...), d.notRun...), d.faults...) {
			name := d.row + "/healthy"
			if fault != "" {
				name = d.row + "/" + fault
			}
			passes := fault == "" || containsString(d.passing, fault)
			notRun := containsString(d.notRun, fault)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				w := newAlphaWorld(t)
				w.installDry(dryWrapper, real, realDir)
				res := w.run([]string{"STUB_DRY_FAULT=" + fault}, "--run", d.row, "--yes")
				if notRun {
					// NOT RUN, not a failure and not a pass: the owner's job holds the slot, this run's job was
					// cancelled by its tag at the start-wait bound, and nothing was judged or posted.
					if res.code != 3 {
						t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
					}
					w.contains(res, d.row+": tests: 0 passed, 0 failed, 1 skipped", d.row+": NOT RUN (a job did not start within 10 min of its acceptance and was cancelled by its tag; not a failure)", "cleanup: left behind 0")
					if strings.Contains(res.out, d.row+": FAIL") || strings.Contains(res.out, d.row+": PASS") {
						t.Errorf("a NOT RUN run was also reported as a pass or a failure:\n%s", res.out)
					}
					if m, _ := filepath.Glob(filepath.Join(w.out(), d.row+"-*", "posts.md")); len(m) != 0 {
						t.Errorf("a NOT RUN run left a text to post: %v", m)
					}
				} else if passes {
					if res.code != 0 {
						t.Fatalf("exit %d:\n%s", res.code, res.out)
					}
					w.contains(res, d.row+": tests: 1 passed, 0 failed, 0 skipped", d.row+": PASS")
					if p := w.postsOf(d.row); !strings.Contains(p, d.row+": PASS") {
						t.Errorf("the text to post does not say PASS:\n%s", p)
					}
				} else {
					if res.code != 1 {
						t.Fatalf("exit %d, want 1: the fault %q must fail the row:\n%s", res.code, fault, res.out)
					}
					w.contains(res, d.row+": tests: 0 passed, 1 failed, 0 skipped", d.row+": FAIL")
					if p := w.postsOf(d.row); d.pkg == alphaPkg && !strings.Contains(p, d.row+": FAIL") {
						t.Errorf("the text to post does not say FAIL:\n%s", p)
					}
				}
				// The binary ran once and it is the dry one: its log says so, in the private log.
				log, _ := os.ReadFile(filepath.Join(w.out(), d.row+".log"))
				if !strings.Contains(string(log), "DRY RUN") {
					t.Errorf("the test's log does not say it was a dry run:\n%s", log)
				}
				_ = shape
			})
		}
	}
}

// A job that is still pending when the wait in the queue runs out is cancelled by its tag and the row is NOT RUN,
// exit 3, on EVERY row (the science agent's ruling of 8 Nov 2026, #272, replacing #230's failure for a row with no
// slot; a queue wait tests nothing). The row's own test is the real one, in dry mode, with the owner's job holding the
// one GPU of the fake QOS on the Helios rows (the owner's slot was busy: try again later), which is left alone, and the
// job simply held on the Athena rows, which have no slot. The default bounds are written out here, not read from the
// script: 30 minutes in the slot (issue #222) and 2 hours without; a wait the operator asks for is the one named. The
// dry run keeps the bound the script gives it (PR 230, B1), and the test judges it by the states on the dry run's
// manual clock: the job still pending one second before the bound, cancelled one second after it (the stand-in's two
// logged readings, written out per case), so a bound a minute out in either direction fails the case, whatever the
// script announced. Rows on both sides of the slot are in the table: one with only slot rows would prove nothing, since
// those were NOT RUN before the ruling.
func TestAJobPendingPastTheBoundIsNotRunOnEveryRow(t *testing.T) {
	real := realTestBinary(t, "HPCBURST_LIVECHECKS_ALPHA_TEST", alphaPkg)
	realDir := filepath.Join(repoRoot(t), alphaPkg)
	const (
		pending30 = "NOT RUN (a job was still pending after 30 min in the queue and was cancelled by its tag; not a failure)"
		pending2h = "NOT RUN (a job was still pending after 120 min in the queue and was cancelled by its tag; not a failure)"
		firstEnd  = "NOT RUN (a job was still pending a minute before its deployment's first end and was cancelled by its tag; not a failure)"
	)
	for _, c := range []struct {
		row, setting string
		wantLine     string
		still, gone  string // the stand-in's readings of the manual clock, since the job's submission
		marker       string // a fragment of the marker's text, which names the bound for the script
	}{
		// The wait in the queue, in the one-job slot.
		{"alpha-helios-nooverlap", "", pending30, "29m59s", "30m1s", "still pending after 30m0s;"},
		{"tunnel-hour-helios", "", pending30, "29m59s", "30m1s", "still pending after 30m0s;"},
		{"tunnel-hour-helios", "LIVE_PENDING_MAX_SECONDS=600", "NOT RUN (a job was still pending after 10 min in the queue and was cancelled by its tag; not a failure)", "9m59s", "10m1s", "still pending after 10m0s;"},
		{"alpha-helios-nooverlap", "LIVE_PENDING_MAX_SECONDS=90", "NOT RUN (a job was still pending after 1 min 30 s in the queue and was cancelled by its tag; not a failure)", "1m29s", "1m31s", "still pending after 1m30s;"},
		// The wait in the queue, with no slot: the 2 hours of the default, and what the operator asks for.
		{"tunnel-hour-athena", "", pending2h, "1h59m59s", "2h0m1s", "still pending after 2h0m0s;"},
		{"alpha-athena-handover", "", pending2h, "1h59m59s", "2h0m1s", "still pending after 2h0m0s;"},
		{"alpha-athena-handover", "LIVE_PENDING_MAX_SECONDS=600", "NOT RUN (a job was still pending after 10 min in the queue and was cancelled by its tag; not a failure)", "9m59s", "10m1s", "still pending after 10m0s;"},
		{"tunnel-hour-athena", "LIVE_PENDING_MAX_SECONDS=90", "NOT RUN (a job was still pending after 1 min 30 s in the queue and was cancelled by its tag; not a failure)", "1m29s", "1m31s", "still pending after 1m30s;"},
		// A tunnel row's first clock stops a minute before its deployment's first end (the science agent's ruling on
		// PR 255): at the default lease, 2 h 17 min, less the 15 min drain window and the minute, 2 h 1 min; at the
		// longest, 4 h, 3 h 44 min. A job still pending then is NOT RUN on both clusters, whatever wait was asked.
		{"tunnel-hour-athena", "LIVE_PENDING_MAX_SECONDS=10800", firstEnd, "2h0m59s", "2h1m1s", "a minute before the deployment's first end"},
		{"tunnel-hour-helios", "LIVE_PENDING_MAX_SECONDS=10800", firstEnd, "2h0m59s", "2h1m1s", "a minute before the deployment's first end"},
		{"tunnel-hour-athena", "LIVE_PENDING_MAX_SECONDS=86400 LIVE_TUNNEL_LEASE_MINUTES=240", firstEnd, "3h43m59s", "3h44m1s", "a minute before the deployment's first end"},
		{"tunnel-hour-helios", "LIVE_PENDING_MAX_SECONDS=86400 LIVE_TUNNEL_LEASE_MINUTES=240", firstEnd, "3h43m59s", "3h44m1s", "a minute before the deployment's first end"},
	} {
		t.Run(c.row+"/"+c.setting, func(t *testing.T) {
			t.Parallel()
			w := newAlphaWorld(t)
			w.installDry(dryWrapper, real, realDir)
			env := []string{"STUB_DRY_FAULT=pend-past-bound"}
			for _, kv := range strings.Fields(c.setting) {
				// The tunnel lease is a setting of the settings file; the wait in the queue is the environment's.
				if k, v, _ := strings.Cut(kv, "="); k == "LIVE_TUNNEL_LEASE_MINUTES" {
					w.alphaSettings(func(m map[string]string) { m[k] = v })
					continue
				}
				env = append(env, kv)
			}
			res := w.run(env, "--run", c.row, "--yes")
			if res.code != 3 {
				t.Fatalf("exit %d, want 3 (a job that never started is NOT RUN on every row):\n%s", res.code, res.out)
			}
			log, _ := os.ReadFile(filepath.Join(w.out(), c.row+".log"))
			for _, want := range []string{
				"stand-in: the job is still pending " + c.still + " after its submission",
				"stand-in: the run's jobs are cancelled by their tags " + c.gone + " after the job's submission",
			} {
				if !strings.Contains(string(log), want) {
					t.Errorf("the dry run's log does not hold %q:\n%s", want, log)
				}
			}
			markers, _ := filepath.Glob(filepath.Join(w.out(), c.row+"-*", "not-run.txt"))
			if posts, _ := filepath.Glob(filepath.Join(w.out(), c.row+"-*", "posts.md")); len(posts) != 0 {
				t.Errorf("a run that ended on the first clock left a text to post: %v", posts)
			}
			w.contains(res, c.row+": tests: 0 passed, 0 failed, 1 skipped", c.row+": "+c.wantLine, "cleanup: left behind 0")
			if strings.Contains(res.out, c.row+": FAIL") || strings.Contains(res.out, c.row+": PASS") {
				t.Errorf("a NOT RUN run was also reported as a pass or a failure:\n%s", res.out)
			}
			if len(markers) != 1 {
				t.Fatalf("want one not-run marker, found %v", markers)
			}
			if fi, err := os.Stat(markers[0]); err != nil || fi.Mode().Perm() != 0o600 {
				t.Errorf("the marker: %v mode %v, want a file of mode 600", err, fi)
			}
			b, _ := os.ReadFile(markers[0])
			if strings.Contains(string(b), "hb-") {
				t.Errorf("the marker names a tag: %s", b)
			}
			// The marker names the bound that ran out, which the script's line then names.
			if !strings.Contains(string(b), c.marker) {
				t.Errorf("the marker %q does not say %q", b, c.marker)
			}
		})
	}
}

// The accounting row with no slot: its wait in the queue is its first clock, and a job still pending past it is
// cancelled by its tag and the row is NOT RUN too (#272), with no kept record and nothing to post. The real
// conformance test, in dry mode: another job of the owner's fills a cap on the fake partition (queue-held), on a
// clock that moves only when the wait sleeps. The dry world sets the row's wait to a minute, so the test's own text
// says 1m0s; the script's line is the wait it announced, 2 h by default, written out here.
func TestAnAccountingRowWithNoSlotThatPendedPastItsWaitIsNotRun(t *testing.T) {
	real := realTestBinary(t, "HPCBURST_LIVECHECKS_CONF_TEST", confPkg)
	w := newAlphaWorld(t)
	w.installDry(dryWrapper, real, filepath.Join(repoRoot(t), confPkg))
	res := w.run([]string{"STUB_DRY_FAULT=queue-held"}, "--run", "gpu-accounting-athena", "--yes")
	if res.code != 3 {
		t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
	}
	w.contains(res, "gpu-accounting-athena: tests: 0 passed, 0 failed, 1 skipped",
		"gpu-accounting-athena: NOT RUN (a job was still pending after 120 min in the queue and was cancelled by its tag; not a failure)", "cleanup: left behind 0")
	log, _ := os.ReadFile(filepath.Join(w.out(), "gpu-accounting-athena.log"))
	if !strings.Contains(string(log), "NOT RUN: the job: a job was still pending after 1m0s in the queue") {
		t.Errorf("the test's own text does not name the wait in the queue:\n%s", log)
	}
	markers, _ := filepath.Glob(filepath.Join(w.out(), "gpu-accounting-athena-*", "not-run.txt"))
	if len(markers) != 1 {
		t.Fatalf("want one not-run marker, found %v", markers)
	}
	for _, f := range []string{"posts.md", "record.json"} {
		if m, _ := filepath.Glob(filepath.Join(w.out(), "gpu-accounting-athena-*", f)); len(m) != 0 {
			t.Errorf("a NOT RUN run wrote %v", m)
		}
	}
}

// The dry switch is guarded from both sides. A dry binary refuses live settings, so a dry pass cannot
// stand for a live one; and the script never turns the switch on, so a live run is never a dry one.
func TestADryBinaryRefusesLiveSettings(t *testing.T) {
	w := newAlphaWorld(t)
	w.installDry(dryWrapperKeepsLive, realTestBinary(t, "HPCBURST_LIVECHECKS_ALPHA_TEST", alphaPkg), filepath.Join(repoRoot(t), alphaPkg))
	res := w.run(nil, "--run", "alpha-athena-handover", "--yes")
	if res.code != 1 {
		t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
	}
	log, _ := os.ReadFile(filepath.Join(w.out(), "alpha-athena-handover.log"))
	if !strings.Contains(string(log), "a dry run refuses to start with live settings present") {
		t.Errorf("the refusal is not in the log:\n%s", log)
	}
	if strings.Contains(string(log), "DRY RUN:") {
		t.Errorf("a refused dry run went on:\n%s", log)
	}
}

// The harness's settings are direct mode with no LIVE_DIALHOME_LISTEN: the live
// steps stop at their first step with the named error, before any stack or job.
func TestALiveRunWithNoListenerFailsLoudlyAndUsesNoCluster(t *testing.T) {
	w := newAlphaWorld(t)
	w.installDry(liveWrapper, realTestBinary(t, "HPCBURST_LIVECHECKS_ALPHA_TEST", alphaPkg), filepath.Join(repoRoot(t), alphaPkg))
	// The caller asks for a dry run: the script must not pass it on.
	res := w.run([]string{"HPCBURST_ALPHA_DRY=1", "HPCBURST_ALPHA_DRY_FAULT=cut"}, "--run", "alpha-athena-handover", "--yes")
	if res.code != 1 {
		t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
	}
	log, _ := os.ReadFile(filepath.Join(w.out(), "alpha-athena-handover.log"))
	for _, want := range []string{"LIVE RUN", "set LIVE_DIALHOME_LISTEN"} {
		if !strings.Contains(string(log), want) {
			t.Errorf("the log lacks %q:\n%s", want, log)
		}
	}
	if strings.Contains(string(log), "DRY RUN") {
		t.Errorf("the caller's dry switch reached the run:\n%s", log)
	}
	// No job was submitted: the only sbatch is the estimate.
	if argv := lines(w.stateFile("sbatch-argv")); len(argv) != 1 || !strings.Contains(argv[0], "--test-only") {
		t.Errorf("sbatch calls %v, want only the estimate", argv)
	}
}
