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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// dryWrapper stands for the compiled test of a row. It is run by the script from the fixture package's
// directory, with the live settings; it removes them (the cluster's side), turns the dry switch on, names the
// stand-in's fault, and runs the real binary from its real package directory.
const dryWrapper = `#!/bin/bash
echo "testbin dry" >> "$STUB_STATE/events"
fault=${STUB_DRY_FAULT:-}
if [ -n "${HPCBURST_CONFORMANCE:-}" ]; then
  # the accounting rows: the row is named by the platform, the record directory is the run's
  export HPCBURST_ALPHA_ROW="gpu-accounting-$HPCBURST_CONF_PROFILE" HPCBURST_ALPHA_OUT="$HPCBURST_CONF_OUT"
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

var builtTests = map[string]string{}

// TestMain removes the directories this package built test binaries into.
func TestMain(m *testing.M) {
	code := m.Run()
	for _, p := range builtTests {
		_ = os.RemoveAll(filepath.Dir(p))
	}
	os.Exit(code)
}

// realTestBinary returns the compiled test of a package: the prebuilt one when its variable is set, else a
// build in a directory of this test run (removed by TestMain).
func realTestBinary(t *testing.T, envName, pkg string) string {
	t.Helper()
	if p := os.Getenv(envName); p != "" {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("%s names no file: %v", envName, err)
		}
		return p
	}
	if p, ok := builtTests[pkg]; ok {
		return p
	}
	dir, err := os.MkdirTemp("", "hbdry")
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, filepath.Base(pkg)+".test")
	cmd := exec.Command("go", "test", "-c", "-o", out, "./"+pkg)
	cmd.Dir = repoRoot(t)
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go test -c ./%s: %v\n%s", pkg, err, b)
	}
	builtTests[pkg] = out
	return out
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
}

var handoverFaults = []string{"cut", "no-stream", "two-restarts", "no-restart", "no-handover", "late-successor", "wrong-partition", "untagged", "unattributed"}
var noOverlapFaults = []string{"nocap", "nogap", "noslot", "cut", "no-stream", "late-submit", "untagged", "wrong-partition"}
var tunnelFaults = []string{"short", "cut", "drops", "no-rate", "short-rate", "wrong-direction", "untagged", "wrong-partition"}
var accountingFaults = []string{"node-two", "node-none"}

// accountingPassing: a cluster that does not confine devices lists the whole node to nvidia-smi, and one that
// does not set CUDA_VISIBLE_DEVICES leaves nvidia-smi the witness; neither is a false failure.
var accountingPassing = []string{"smi-whole-node", "cvd-unset"}

const (
	alphaPkg = "internal/alpha"
	confPkg  = "internal/providerctl/conformance"
)

var dryRows = []dryRow{
	{"alpha-athena-handover", alphaPkg, "HPCBURST_LIVECHECKS_ALPHA_TEST", handoverFaults, nil},
	{"alpha-helios-nooverlap", alphaPkg, "HPCBURST_LIVECHECKS_ALPHA_TEST", noOverlapFaults, nil},
	{"tunnel-hour-helios", alphaPkg, "HPCBURST_LIVECHECKS_ALPHA_TEST", tunnelFaults, nil},
	{"tunnel-hour-athena", alphaPkg, "HPCBURST_LIVECHECKS_ALPHA_TEST", tunnelFaults, nil},
	{"gpu-accounting-helios", confPkg, "HPCBURST_LIVECHECKS_CONF_TEST", accountingFaults, accountingPassing},
	{"gpu-accounting-athena", confPkg, "HPCBURST_LIVECHECKS_CONF_TEST", accountingFaults, accountingPassing},
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
		for _, fault := range append(append([]string{""}, d.passing...), d.faults...) {
			name := d.row + "/healthy"
			if fault != "" {
				name = d.row + "/" + fault
			}
			passes := fault == "" || containsString(d.passing, fault)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				w := newAlphaWorld(t)
				w.installDry(dryWrapper, real, realDir)
				res := w.run([]string{"STUB_DRY_FAULT=" + fault}, "--run", d.row, "--yes")
				if passes {
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

func TestALiveRunWithNoScenarioFailsLoudlyAndUsesNoCluster(t *testing.T) {
	w := newAlphaWorld(t)
	w.installDry(liveWrapper, realTestBinary(t, "HPCBURST_LIVECHECKS_ALPHA_TEST", alphaPkg), filepath.Join(repoRoot(t), alphaPkg))
	// The caller asks for a dry run: the script must not pass it on.
	res := w.run([]string{"HPCBURST_ALPHA_DRY=1", "HPCBURST_ALPHA_DRY_FAULT=cut"}, "--run", "alpha-athena-handover", "--yes")
	if res.code != 1 {
		t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
	}
	log, _ := os.ReadFile(filepath.Join(w.out(), "alpha-athena-handover.log"))
	for _, want := range []string{"LIVE RUN", "wait for #161"} {
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
