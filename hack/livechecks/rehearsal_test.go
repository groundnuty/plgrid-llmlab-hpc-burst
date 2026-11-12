package livechecks

// The rehearsal row through the script (issue #193). The row runs the one-host rehearsal: the script builds the
// stack's binaries and the test binary under the build lock, then runs hack/rehearsal.sh, never the test binary on its
// own, because only that script stops the scope the stack runs in, whatever way the run ends. In these tests
// hack/rehearsal.sh is a stand-in that keeps the parts of its contract the row relies on (REHEARSAL_PREBUILT needs the
// five files in REHEARSAL_OUT; it runs REHEARSAL_OUT/rehearsal.test with the test's gate set and exits with its
// status, or 1 when a process is left in the scope), and records what it was called with. The stand-in test binary
// is the stubs' one, which skips when its gate (HPCBURST_REHEARSAL=1, set only by rehearsal.sh) is not met, so a run
// that bypassed rehearsal.sh would be a skip, and a skip is a failure.

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// rehearsalFixtureFiles is the rehearsal's package in the fixture module: the one test file the row names. The
// stack's binaries it runs are the fixture's cmd packages (alphaFixtureFiles).
func rehearsalFixtureFiles() map[string]string {
	return map[string]string{
		"internal/rehearsal/scenario/part3_test.go": "package scenario_test\n\nimport \"testing\"\n\nfunc TestRehearsalPart3(t *testing.T) {}\n",
	}
}

// rehearsalSh stands for hack/rehearsal.sh. STUB_REHEARSAL_EXIT makes it fail before the test (no user manager);
// STUB_REHEARSAL_LEFT makes it fail after a test that returned, as a process left in the scope does; STUB_REHEARSAL_HOLD
// makes it wait for a signal and spend STUB_REHEARSAL_TEARDOWN seconds on its teardown, which the run must wait for.
const rehearsalSh = `#!/bin/bash
echo "rehearsal.sh" >> "$STUB_STATE/events"
{
  echo "run=${REHEARSAL_RUN-unset}"; echo "out=${REHEARSAL_OUT-unset}"; echo "prebuilt=${REHEARSAL_PREBUILT-unset}"
  echo "timeout=${REHEARSAL_TIMEOUT-unset}"; echo "bin_dir=${HPCBURST_BIN_DIR-unset}"; echo "frp_dir=${HPCBURST_FRP_DIR-unset}"
  echo "parent=$(tr '\0' ' ' < /proc/$PPID/cmdline)"; echo "args=$*"
} > "$STUB_STATE/rehearsal-env"
if [ -n "${STUB_BUILD_LOCK:-}" ]; then
  if flock -n "$STUB_BUILD_LOCK" true; then echo free >> "$STUB_STATE/rehearsal-lock"; else echo held >> "$STUB_STATE/rehearsal-lock"; fi
fi
: "${HPCBURST_BIN_DIR:?}" "${HPCBURST_FRP_DIR:?}"
[ -z "${STUB_REHEARSAL_EXIT:-}" ] || { echo "rehearsal.sh: systemd-run --user does not work here (seedhost01)" >&2; exit "$STUB_REHEARSAL_EXIT"; }
if [ "${REHEARSAL_PREBUILT:-0}" != 1 ]; then echo "rehearsal.sh stand-in: the row must run it prebuilt" >&2; exit 2; fi
for f in hpcburst hpcburst-controller hpcburst-front hb-agent rehearsal.test; do
  [ -x "$REHEARSAL_OUT/$f" ] || { echo "rehearsal.sh: REHEARSAL_PREBUILT=1, but $REHEARSAL_OUT/$f is missing" >&2; exit 2; }
done
if [ -n "${STUB_REHEARSAL_HOLD:-}" ]; then
  trap 'sleep "${STUB_REHEARSAL_TEARDOWN:-1}"; echo done > "$STUB_STATE/torn-down"; exit 143' TERM
  touch "$STUB_STATE/started"
  while :; do sleep 0.1; done
fi
echo "running the binaries in $REHEARSAL_OUT"
HPCBURST_REHEARSAL=1 "$REHEARSAL_OUT/rehearsal.test" -test.v -test.timeout "${REHEARSAL_TIMEOUT:-30m}" -test.run "${REHEARSAL_RUN:-^TestRehearsal(Direct|SSH)\$}"
rc=$?
if [ -n "${STUB_REHEARSAL_LEFT:-}" ]; then
  echo "rehearsal.sh: processes left in the scope after the test returned (exit $rc):" >&2
  [ "$rc" -ne 0 ] || rc=1
fi
exit "$rc"
`

// rehearsalGo stands for the go command of the rehearsal's builds. Like go, build -o DIR/ writes one executable
// per package, named after its directory, and refuses several packages written to one file. STUB_GO_FAIL names
// the subcommand that fails.
const rehearsalGo = `#!/bin/bash
printf '%s\n' "$*" >> "$STUB_STATE/go-argv"
echo "go $1" >> "$STUB_STATE/events"
if [ -n "${STUB_BUILD_LOCK:-}" ]; then
  if flock -n "$STUB_BUILD_LOCK" true; then echo free >> "$STUB_STATE/go-lock"; else echo held >> "$STUB_STATE/go-lock"; fi
fi
[ "${STUB_GO_FAIL:-}" != "$1" ] || { echo "go: $1 failed in seedhost01 (SEEDKEY)" >&2; exit 1; }
out=""; prev=""; pkgs=()
for a in "$@"; do
  [ "$prev" = -o ] && out=$a
  prev=$a
  case $a in ./*) pkgs+=("$a") ;; esac
done
case "$1" in
  build)
    case $out in */) ;; *) echo "go: cannot write multiple packages to non-directory $out" >&2; exit 1 ;; esac
    mkdir -p "$out"
    for p in "${pkgs[@]}"; do echo binary > "$out$(basename "$p")"; chmod +x "$out$(basename "$p")"; done ;;
  test) mkdir -p "$(dirname "$out")"; cp "$STUB_STATE/testbin.sh" "$out"; chmod +x "$out" ;;
esac
`

// useRehearsal puts the stand-ins in place and gives the settings a directory of pinned binaries, as
// hack/fetch-k3s.sh and hack/fetch-frp.sh lay it out. It returns that directory.
func (w *world) useRehearsal(mutate func(m map[string]string)) string {
	w.t.Helper()
	write(w.t, filepath.Join(w.bin, "go"), rehearsalGo, 0o755)
	write(w.t, filepath.Join(w.repo, "hack", "rehearsal.sh"), rehearsalSh, 0o755)
	bin := filepath.Join(w.dir, "pinned")
	pinnedBinaries(w.t, bin)
	w.writeSettings(func(m map[string]string) {
		m["LIVE_BIN_DIR"] = bin
		if mutate != nil {
			mutate(m)
		}
	})
	return bin
}

// recorded reads the stand-in rehearsal.sh's record of its call, as key=value lines.
func (w *world) recorded() map[string]string {
	w.t.Helper()
	got := map[string]string{}
	for _, l := range lines(w.stateFile("rehearsal-env")) {
		if k, v, ok := strings.Cut(l, "="); ok {
			got[k] = v
		}
	}
	if len(got) == 0 {
		w.t.Fatal("hack/rehearsal.sh was never run")
	}
	return got
}

func TestRehearsalRowRunsThroughRehearsalShAndNeedsNoYes(t *testing.T) {
	w := newWorld(t)
	bin := w.useRehearsal(nil)
	res := w.run(nil, "--run", "rehearsal-part3") // no --yes
	if res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	w.contains(res, "rehearsal-part3: tests: 1 passed, 0 failed, 0 skipped", "rehearsal-part3: PASS")

	got := w.recorded()
	// The pattern is the row's, whole: a looser one would run the other rehearsal tests too.
	for k, want := range map[string]string{"run": "^TestRehearsalPart3$", "prebuilt": "1", "bin_dir": bin, "frp_dir": bin,
		"out": filepath.Join(w.out(), "rehearsal-part3-bin")} {
		if got[k] != want {
			t.Errorf("rehearsal.sh got %s=%q, want %q", k, got[k], want)
		}
	}
	// What ran was the test binary the builds left, with the gate rehearsal.sh sets and the pattern it was given.
	if env := w.stateFile("test-env-TestRehearsalPart3"); !strings.Contains(env, "HPCBURST_REHEARSAL=1") {
		t.Errorf("the test did not get the gate that rehearsal.sh sets:\n%s", env)
	}
	if argv := w.stateFile("testbin-argv-TestRehearsalPart3"); !strings.Contains(argv, "-test.run ^TestRehearsalPart3$") {
		t.Errorf("the test binary was called with %q", argv)
	}
	// The order: both builds, then rehearsal.sh, which runs the test; nothing started the test binary itself.
	ev := w.events()
	build, test, run, bintest := index(ev, "go build", 1), index(ev, "go test", 1), index(ev, "rehearsal.sh", 1), index(ev, "testbin TestRehearsalPart3", 1)
	if !(build >= 0 && build < test && test < run && run < bintest) || index(ev, "testbin", 2) >= 0 || index(ev, "rehearsal.sh", 2) >= 0 {
		t.Errorf("events:\n%s", strings.Join(ev, "\n"))
	}
	// No cluster: nothing reached ssh, sbatch or squeue.
	for _, f := range []string{"ssh-argv", "sbatch-argv"} {
		if got := w.stateFile(f); got != "" {
			t.Errorf("a local row called %s: %s", f, got)
		}
	}
	if index(ev, "squeue", 1) >= 0 {
		t.Errorf("a local row read the queue:\n%s", strings.Join(ev, "\n"))
	}
	// The log is the private one under the output directory, and holds what the run printed.
	if log, _ := os.ReadFile(filepath.Join(w.out(), "rehearsal-part3.log")); !strings.Contains(string(log), "SEEDKEY") || !strings.Contains(string(log), "--- PASS: TestRehearsalPart3") {
		t.Errorf("the run's log is not under the output directory, or lacks the test's output:\n%s", log)
	}
}

// The run is judged from the log and rehearsal.sh's exit status together: each way it can fail is a failure.
func TestRehearsalRowFailsWhenTheRunFails(t *testing.T) {
	cases := []struct {
		name  string
		extra []string
		want  string
	}{
		{"the test fails", []string{"STUB_TEST_MODE=fail"}, "rehearsal-part3: FAIL (exit status 1)"},
		{"rehearsal.sh fails after a passing test, as a process left in the scope does", []string{"STUB_REHEARSAL_LEFT=1"}, "rehearsal-part3: FAIL (exit status 1)"},
		{"rehearsal.sh stops before the test, for want of a user manager", []string{"STUB_REHEARSAL_EXIT=2"}, "rehearsal-part3: FAIL (exit status 2)"},
		{"the test skips", []string{"STUB_TEST_MODE=skip"}, "rehearsal-part3: FAIL (skipped: its gate was not met, so nothing ran)"},
		{"a subtest skips", []string{"STUB_TEST_MODE=skip-sub"}, "rehearsal-part3: FAIL (a subtest was skipped, so part of the run did not happen)"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := newWorld(t)
			w.useRehearsal(nil)
			res := w.run(c.extra, "--run", "rehearsal-part3")
			if res.code != 1 {
				t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
			}
			w.contains(res, c.want)
			if strings.Contains(res.out, "rehearsal-part3: PASS") {
				t.Errorf("a failed run is reported as a pass:\n%s", res.out)
			}
			w.recorded() // rehearsal.sh was reached: the failure is the run's, not a refusal before it
		})
	}
}

// A build that fails ends the row before the run, and the tool's own text stays in the private build log.
func TestRehearsalRowBuildFailureRunsNothing(t *testing.T) {
	for _, sub := range []string{"build", "test"} {
		t.Run("go "+sub, func(t *testing.T) {
			w := newWorld(t)
			w.useRehearsal(nil)
			res := w.run([]string{"STUB_GO_FAIL=" + sub}, "--run", "rehearsal-part3")
			if res.code != 1 {
				t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
			}
			w.contains(res, "rehearsal-part3: FAIL (a build step failed; see the build log in the output directory)")
			if index(w.events(), "rehearsal.sh", 1) >= 0 {
				t.Errorf("rehearsal.sh ran after a failed build:\n%s", strings.Join(w.events(), "\n"))
			}
			if log, _ := os.ReadFile(filepath.Join(w.out(), "rehearsal-part3-build.log")); !strings.Contains(string(log), "SEEDKEY") {
				t.Errorf("the build's diagnostic is not in the private build log")
			}
		})
	}
}

// The builds take the shared build lock and the run, which lasts half an hour, does not: a lock held that long would
// stop every build on the host (REHEARSAL_PREBUILT exists for this).
func TestRehearsalRowTakesTheBuildLockOnlyForTheBuilds(t *testing.T) {
	w := newWorld(t)
	w.useRehearsal(func(m map[string]string) { m["LIVE_BUILD_LOCK"] = filepath.Join(w.dir, "locks", "gobuild.lock") })
	res := w.run([]string{"STUB_BUILD_LOCK=" + filepath.Join(w.dir, "locks", "gobuild.lock")}, "--run", "rehearsal-part3")
	if res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	if got := lines(w.stateFile("go-lock")); len(got) != 2 || got[0] != "held" || got[1] != "held" {
		t.Errorf("the two builds ran with the build lock %v, want held both times", got)
	}
	for _, f := range []string{"rehearsal-lock", "testbin-lock"} {
		if got := lines(w.stateFile(f)); len(got) != 1 || got[0] != "free" {
			t.Errorf("%s: the run saw the build lock %v, want free", f, got)
		}
	}
}

// The caller's shell cannot redirect the run: every variable the run depends on is the row's.
func TestRehearsalRowIgnoresTheCallersRehearsalVariables(t *testing.T) {
	w := newWorld(t)
	bin := w.useRehearsal(nil)
	res := w.run([]string{"REHEARSAL_RUN=^TestRehearsalDirect$", "REHEARSAL_OUT=" + filepath.Join(w.dir, "elsewhere"), "REHEARSAL_PREBUILT=0",
		"REHEARSAL_TIMEOUT=1m", "HPCBURST_BIN_DIR=" + filepath.Join(w.dir, "other"), "HPCBURST_FRP_DIR=" + filepath.Join(w.dir, "other")}, "--run", "rehearsal-part3")
	if res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	got := w.recorded()
	for k, want := range map[string]string{"run": "^TestRehearsalPart3$", "prebuilt": "1", "timeout": "30m", "bin_dir": bin, "frp_dir": bin,
		"out": filepath.Join(w.out(), "rehearsal-part3-bin")} {
		if got[k] != want {
			t.Errorf("rehearsal.sh got %s=%q, want %q", k, got[k], want)
		}
	}
}

// minutes reads "35m" as 35.
func minutes(t *testing.T, s string) int {
	t.Helper()
	n, err := strconv.Atoi(strings.TrimSuffix(s, "m"))
	if err != nil || !strings.HasSuffix(s, "m") {
		t.Fatalf("%q is not a whole number of minutes", s)
	}
	return n
}

// The rehearsal runs about 15 minutes (issue #146) and rehearsal.sh gives the test 30 by default: the row's own
// timeout must outlast the test's by the scope's teardown (a minute of SIGKILLs, rehearsal.sh's header) and a margin,
// or a slow run that cleans up properly would be killed from outside. The local rows' 15 minutes would end a normal run.
func TestRehearsalRowTimeoutOutlastsTheRehearsal(t *testing.T) {
	w := newWorld(t)
	w.useRehearsal(nil)
	if res := w.run(nil, "--run", "rehearsal-part3"); res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	got := w.recorded()
	script, err := os.ReadFile(filepath.Join(repoRoot(t), "hack", "rehearsal.sh"))
	if err != nil {
		t.Fatal(err)
	}
	def := regexp.MustCompile(`REHEARSAL_TIMEOUT:-(\d+m)`).FindSubmatch(script)
	if def == nil {
		t.Fatal("hack/rehearsal.sh no longer has a default for REHEARSAL_TIMEOUT: update this test")
	}
	test := minutes(t, got["timeout"])
	if test < minutes(t, string(def[1])) {
		t.Errorf("the test's timeout %dm is under rehearsal.sh's own default %s", test, def[1])
	}
	// The stand-in's parent is the row's timeout: "timeout 35m /path/hack/rehearsal.sh".
	f := strings.Fields(got["parent"])
	if len(f) < 3 || f[0] != "timeout" {
		t.Fatalf("rehearsal.sh was not started under timeout: %q", got["parent"])
	}
	if outer := minutes(t, f[1]); outer < test+2 {
		t.Errorf("the row's timeout %dm leaves under two minutes after the test's %dm for the scope's teardown", outer, test)
	}
}

// What the row builds is what hack/rehearsal.sh builds and what its PREBUILT mode looks for: read from the real
// script, so that a change to either side fails here instead of at the start of a long run.
func TestRehearsalRowBuildsWhatRehearsalShBuilds(t *testing.T) {
	script, err := os.ReadFile(filepath.Join(repoRoot(t), "hack", "rehearsal.sh"))
	if err != nil {
		t.Fatal(err)
	}
	build := regexp.MustCompile(`(?m)^\s*go build -o "\$out/" (.+)$`).FindSubmatch(script)
	testc := regexp.MustCompile(`(?m)^\s*go test -c -o "\$out/rehearsal\.test" (\S+)$`).FindSubmatch(script)
	files := regexp.MustCompile(`(?m)for f in ([a-z. -]+); do\n\s*if \[ ! -x "\$out/\$f" \]`).FindSubmatch(script)
	if build == nil || testc == nil || files == nil {
		t.Fatal("hack/rehearsal.sh's build lines or its PREBUILT file list changed shape: update this test")
	}
	w := newWorld(t)
	w.useRehearsal(nil)
	if res := w.run(nil, "--run", "rehearsal-part3"); res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	argv := lines(w.stateFile("go-argv"))
	if len(argv) != 2 {
		t.Fatalf("go ran %d times, want 2:\n%s", len(argv), strings.Join(argv, "\n"))
	}
	pkgs := func(line string) []string {
		var p []string
		for _, f := range strings.Fields(line) {
			if strings.HasPrefix(f, "./") {
				p = append(p, f)
			}
		}
		sort.Strings(p)
		return p
	}
	wantBuild, wantTest := pkgs(string(build[1])), pkgs(string(testc[1]))
	if got := pkgs(argv[0]); strings.Join(got, " ") != strings.Join(wantBuild, " ") || !strings.HasPrefix(argv[0], "build ") {
		t.Errorf("the row ran `go %s`, rehearsal.sh builds %v", argv[0], wantBuild)
	}
	if got := pkgs(argv[1]); strings.Join(got, " ") != strings.Join(wantTest, " ") || !strings.HasPrefix(argv[1], "test -c ") {
		t.Errorf("the row ran `go %s`, rehearsal.sh compiles the test of %v", argv[1], wantTest)
	}
	// Every file PREBUILT mode requires is there after the row's builds (the stand-in refused otherwise, as the real one does).
	for _, f := range strings.Fields(string(files[1])) {
		if _, err := os.Stat(filepath.Join(w.out(), "rehearsal-part3-bin", f)); err != nil {
			t.Errorf("rehearsal.sh (PREBUILT) needs %s, which the row's builds left no: %v", f, err)
		}
	}
}

func TestRehearsalRowRefusesWithoutWhatItNeeds(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(w *world, bin string, m map[string]string)
		expect []string
	}{
		{"no binary directory", func(_ *world, _ string, m map[string]string) { delete(m, "LIVE_BIN_DIR") }, []string{"setting LIVE_BIN_DIR is missing"}},
		{"a relative one", func(_ *world, _ string, m map[string]string) { m["LIVE_BIN_DIR"] = "pinned" }, []string{"LIVE_BIN_DIR must be an absolute path"}},
		{"one that is not a directory", func(w *world, _ string, m map[string]string) { m["LIVE_BIN_DIR"] = filepath.Join(w.dir, "nothing") }, []string{"LIVE_BIN_DIR is not a directory"}},
		{"no k3s", func(_ *world, bin string, _ map[string]string) {
			_ = os.Remove(filepath.Join(bin, "k3s-1.36.5", "k3s"))
		}, []string{"LIVE_BIN_DIR has no executable k3s-1.36.5/k3s"}},
		{"a k3s that does not run", func(_ *world, bin string, _ map[string]string) {
			_ = os.Chmod(filepath.Join(bin, "k3s-1.36.5", "k3s"), 0o644)
		}, []string{"LIVE_BIN_DIR has no executable k3s-1.36.5/k3s"}},
		{"no frp at all, every problem at once", func(_ *world, bin string, _ map[string]string) {
			_ = os.Remove(filepath.Join(bin, "frps"))
			_ = os.Remove(filepath.Join(bin, "frpc"))
			_ = os.Remove(filepath.Join(bin, "k3s-1.36.5", "k3s"))
		}, []string{"LIVE_BIN_DIR has no executable k3s-1.36.5/k3s", "LIVE_BIN_DIR has no executable frps", "LIVE_BIN_DIR has no executable frpc"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := newWorld(t)
			bin := w.useRehearsal(nil)
			w.writeSettings(func(m map[string]string) {
				m["LIVE_BIN_DIR"] = bin
				c.mutate(w, bin, m)
			})
			res := w.run(nil, "--run", "rehearsal-part3")
			if res.code != 3 {
				t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
			}
			w.contains(res, c.expect...)
			if w.stateFile("go-argv") != "" || index(w.events(), "rehearsal.sh", 1) >= 0 {
				t.Errorf("a refused run built or ran something:\n%s", strings.Join(w.events(), "\n"))
			}
		})
	}
}

// A signal stops the run only after rehearsal.sh has torn its scope down: the script's own grace before it kills would
// otherwise leave the stack's processes behind. The stand-in spends 11 seconds on its teardown (past the 10 of a
// plain local row), and the test reads the screen as it grows: "interrupted" must appear after the teardown finished,
// never before.
func TestRehearsalRowInterruptWaitsForRehearsalShsTeardown(t *testing.T) {
	w := newWorld(t)
	w.useRehearsal(nil)
	screen, err := os.Create(filepath.Join(w.dir, "screen.txt"))
	if err != nil {
		t.Fatal(err)
	}
	defer screen.Close()
	cmd := w.command([]string{"STUB_REHEARSAL_HOLD=1", "STUB_REHEARSAL_TEARDOWN=11"}, "--run", "rehearsal-part3")
	cmd.Stdout, cmd.Stderr = screen, screen
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	w.waitFor("started")
	w.signal(cmd, syscall.SIGTERM)
	deadline := time.Now().Add(2 * time.Minute)
	tornDownWhenReported := false
	for time.Now().Before(deadline) {
		b, _ := os.ReadFile(screen.Name())
		if strings.Contains(string(b), "rehearsal-part3: FAIL (interrupted)") {
			_, statErr := os.Stat(filepath.Join(w.state, "torn-down"))
			tornDownWhenReported = statErr == nil
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	err = cmd.Wait()
	w.noLeftoverProcess(cmd)
	b, _ := os.ReadFile(screen.Name())
	w.noLeak(string(b))
	if exitCode(err) == 0 {
		t.Errorf("an interrupted run exited 0:\n%s", b)
	}
	if !strings.Contains(string(b), "rehearsal-part3: FAIL (interrupted)") {
		t.Fatalf("no interrupt report:\n%s", b)
	}
	if !tornDownWhenReported {
		t.Errorf("the run reported the interrupt before rehearsal.sh had finished its teardown:\n%s", b)
	}
}
