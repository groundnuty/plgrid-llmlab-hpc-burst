package livechecks

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
)

var slotUse = regexp.MustCompile(`: slot use \(([a-z-]+)\): start \d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ end \d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ duration \d\d:\d\d:\d\d`)

func (w *world) seedQueue(rows ...string) {
	w.t.Helper()
	write(w.t, filepath.Join(w.state, "queue"), strings.Join(rows, "\n")+"\n", 0o644)
}

func (w *world) contains(res result, want ...string) {
	w.t.Helper()
	for _, s := range want {
		if !strings.Contains(res.out, s) {
			w.t.Errorf("the output lacks %q:\n%s", s, res.out)
		}
	}
}

func (w *world) noSubmit() {
	w.t.Helper()
	for _, l := range lines(w.stateFile("sbatch-argv")) {
		if !strings.Contains(l, "--test-only") {
			w.t.Errorf("a job was submitted: %s", l)
		}
	}
	if _, err := os.Stat(filepath.Join(w.state, "test-env-TestConformance")); err == nil {
		w.t.Errorf("the test binary ran")
	}
}

func TestClusterTestWithoutYesPrintsTheEstimateAndRunsNothing(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	res := w.run(nil, "--run", "conformance-athena")
	if res.code != 4 {
		t.Fatalf("exit %d, want 4:\n%s", res.code, res.out)
	}
	w.contains(res, "conformance-athena: shape: partition plgrid-now, 1 GPU, 4 CPU, 32Gi, time limit 00:05:00, one job, no requeue",
		"queue estimate: a start in about", "not run: pass --yes")
	w.noSubmit()
	if w.stateFile("go-argv") != "" {
		t.Errorf("the test was built before --yes: %s", w.stateFile("go-argv"))
	}
	// The estimate asks for the shape the test would use.
	argv := lines(w.stateFile("sbatch-argv"))
	if len(argv) != 1 {
		t.Fatalf("want exactly one sbatch call (the estimate), got %v", argv)
	}
	for _, f := range []string{"--test-only", "-p plgrid-now", "-N 1", "-c 4", "--mem=32768M", "--gres=gpu:1", "-t 00:05:00", "--no-requeue", "-A SEEDACCT-a100"} {
		if !strings.Contains(argv[0], f) {
			t.Errorf("the estimate lacks %q: %s", f, argv[0])
		}
	}
	if strings.Contains(w.stateFile("ssh-argv"), "SEEDACCT") {
		t.Errorf("the account went into the ssh arguments, where ps shows it")
	}
}

func TestSlotHeldByAnotherJobIsNotQueuedBehind(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	w.seedQueue("5000|someone-elses-job|plgrid-now|plgrid-now|RUNNING")
	res := w.run(nil, "--run", "conformance-athena", "--yes")
	if res.code != 3 {
		t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
	}
	w.contains(res, "already holds 1 job(s) of yours; not queuing behind it")
	if strings.Contains(res.out, "queue estimate") {
		t.Errorf("an estimate was printed for a slot that is busy:\n%s", res.out)
	}
	w.noSubmit()
	if w.stateFile("cancelled") != "" {
		t.Errorf("a job that was not this run's was cancelled: %s", w.stateFile("cancelled"))
	}
}

func TestClusterTestPassesAndReportsTheSlotUse(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	res := w.run([]string{"HPCBURST_CONF_GPU_MODEL=A100", "HPCBURST_CONF_DATA_PARSER=v0.0.40"}, "--run", "conformance-athena", "--yes")
	if res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	w.contains(res, "conformance-athena: tests: 1 passed, 0 failed, 0 skipped", "conformance-athena: PASS", "cleanup: left behind 0 (cancelled 0)")
	if m := slotUse.FindStringSubmatch(res.out); m == nil || m[1] != "plgrid-now" {
		t.Errorf("no slot-use line with UTC start, end and duration:\n%s", res.out)
	}
	env := w.stateFile("test-env-TestConformance")
	for _, want := range []string{"HPCBURST_CONFORMANCE=1", "HPCBURST_CONF_PROFILE=athena", "HPCBURST_CONF_PARTITION=plgrid-now",
		"HPCBURST_CONF_GPUS=1", "HPCBURST_CONF_CPUS=4", "HPCBURST_CONF_MEMORY=32Gi", "HPCBURST_CONF_ACCOUNT=SEEDACCT-a100",
		"HPCBURST_CONF_CREDENTIALS=" + filepath.Join(w.dir, "athena.ssh"), "HPCBURST_CONF_HOST_LOCK=" + filepath.Join(w.dir, "locks", "athena.lock")} {
		if !strings.Contains(env, want) {
			t.Errorf("the test did not get %s:\n%s", want, env)
		}
	}
	for _, retired := range []string{"HPCBURST_CONF_GPU_MODEL", "HPCBURST_CONF_CPU_ARCH", "HPCBURST_CONF_DATA_PARSER"} {
		if strings.Contains(env, retired) {
			t.Errorf("the retired variable %s reached the test, which stops a run that sets it", retired)
		}
	}
	if strings.Contains(w.stateFile("ssh-argv"), "SEEDACCT") {
		t.Errorf("the account went into the ssh arguments")
	}
	// The seeds did flow into the run's own log, so the clean screen is not for lack of them.
	if log, _ := os.ReadFile(filepath.Join(w.out(), "conformance-athena.log")); !strings.Contains(string(log), "SEEDKEY") {
		t.Errorf("the seeded key never reached the private log: the leak check would prove nothing")
	}
}

func TestClusterTestWithoutASlotSaysSo(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	res := w.run(nil, "--run", "conformance-helios", "--yes")
	if res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	w.contains(res, "cluster run (no one-job slot): start ")
}

func TestFailedRunCancelsTheJobItLeftAndOnlyThat(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	w.seedQueue("5001|hb-other-bbbbbbbbbbbb|normal|plgrid|RUNNING")
	res := w.run([]string{"STUB_TEST_MODE=leave-fail"}, "--run", "conformance-helios", "--yes")
	if res.code != 1 {
		t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
	}
	w.contains(res, "conformance-helios: FAIL", "cleanup: left behind 0 (cancelled 1)")
	if got := lines(w.stateFile("cancelled")); len(got) != 1 || got[0] != "9001" {
		t.Errorf("cancelled %v, want only the job this run left", got)
	}
	if q := w.stateFile("queue"); !strings.Contains(q, "5001|") || strings.Contains(q, "9001|") {
		t.Errorf("queue after the run:\n%s", q)
	}
}

func TestPassingRunThatLeftAJobStillCancelsIt(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	res := w.run([]string{"STUB_TEST_MODE=leave-pass"}, "--run", "conformance-helios", "--yes")
	w.contains(res, "cleanup: left behind 0 (cancelled 1)")
	if res.code != 0 {
		t.Errorf("exit %d: a job already cancelled is a cleanup, not a failure of the test:\n%s", res.code, res.out)
	}
}

func TestSkippedTestIsAFailure(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	res := w.run([]string{"STUB_TEST_MODE=skip"}, "--run", "conformance-helios", "--yes")
	if res.code != 1 {
		t.Fatalf("exit %d, want 1 (the stub exits 0 on a skip, as go test does):\n%s", res.code, res.out)
	}
	w.contains(res, "0 passed, 0 failed, 1 skipped", "FAIL (skipped: its gate was not met, so nothing ran)")
}

func TestSignalStopsTheTestAndRunsTheCleanup(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	cmd, out := w.start([]string{"STUB_TEST_MODE=sleep"}, "--run", "conformance-helios", "--yes")
	w.waitFor("started")
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	err := cmd.Wait()
	w.noLeak(out.String())
	w.noLeftoverProcess(cmd)
	if exitCode(err) == 0 {
		t.Errorf("a killed run exited 0:\n%s", out)
	}
	if !strings.Contains(out.String(), "FAIL (interrupted)") || !strings.Contains(out.String(), "cleanup: left behind 0 (cancelled 1)") {
		t.Errorf("no interrupt or cleanup report:\n%s", out)
	}
	if got := lines(w.stateFile("cancelled")); len(got) != 1 || got[0] != "9001" {
		t.Errorf("cancelled %v, want the job the test left", got)
	}
}

func TestAnUnreadableQueueIsNeverTakenForAnEmptyOne(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	res := w.run([]string{"STUB_SQUEUE_FAIL=1"}, "--run", "conformance-athena", "--yes")
	if res.code != 3 {
		t.Errorf("slot: exit %d, want 3:\n%s", res.code, res.out)
	}
	w.contains(res, "the slot could not be read, so it is taken as busy")
	res = w.run([]string{"STUB_SQUEUE_FAIL=1"}, "--run", "conformance-helios", "--yes")
	if res.code != 1 {
		t.Errorf("baseline: exit %d, want 1:\n%s", res.code, res.out)
	}
	w.contains(res, "the queue could not be read before the run")
	w.noSubmit()
	res = w.run([]string{"STUB_SSH_FAIL=1"}, "--run", "conformance-athena", "--yes")
	if res.code != 3 {
		t.Errorf("unreachable cluster: exit %d, want 3:\n%s", res.code, res.out)
	}
}

func TestLocalTestsNeedNoClusterAndNoYes(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	w.writeSettings(func(m map[string]string) { m["LIVE_LLMLAB_MODEL"] = "Example/Model-1B" })
	res := w.run(nil, "--run", "llmlab-smoke", "hub-golden")
	if res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	w.contains(res, "llmlab-smoke: PASS", "hub-golden: PASS")
	if env := w.stateFile("test-env-TestLLMLabThroughTheFront"); !strings.Contains(env, "HPCBURST_LLMLAB_CREDENTIALS="+filepath.Join(w.dir, "llmlab.json")) || !strings.Contains(env, "HPCBURST_LLMLAB_MODEL=Example/Model-1B") {
		t.Errorf("llmlab-smoke got the wrong environment:\n%s", env)
	}
	if env := w.stateFile("test-env-TestGoldenHub"); !strings.Contains(env, "HPCBURST_TEST_HUB=1") {
		t.Errorf("hub-golden got the wrong environment:\n%s", env)
	}
	if w.stateFile("ssh-argv") != "" {
		t.Errorf("a local test called ssh")
	}
}

func TestRefusals(t *testing.T) {
	cases := []struct {
		name   string
		prep   func(w *world)
		args   []string
		code   int
		expect []string
	}{
		{"settings file readable by others", func(w *world) { _ = os.Chmod(w.settings, 0o644) }, []string{"--run", "hub-golden"}, 3,
			[]string{"HPCBURST_LIVE_SETTINGS names a file that is not mode 600"}},
		{"credentials file readable by others", func(w *world) { _ = os.Chmod(filepath.Join(w.dir, "llmlab.json"), 0o644) }, []string{"--run", "llmlab-smoke"}, 3,
			[]string{"LIVE_LLMLAB_CREDENTIALS names a file that is not mode 600"}},
		{"every missing setting at once", func(w *world) {
			w.writeSettings(func(m map[string]string) {
				delete(m, "LIVE_LOCK_DIR")
				delete(m, "ACCOUNT_ATHENA_A100")
				delete(m, "LIVE_CONF_CREDENTIALS_ATHENA")
			})
		}, []string{"--run", "conformance-athena", "--yes"}, 3,
			[]string{"setting LIVE_LOCK_DIR is missing", "setting ACCOUNT_ATHENA_A100 is missing", "setting LIVE_CONF_CREDENTIALS_ATHENA is missing"}},
		{"an account with odd characters", func(w *world) {
			w.writeSettings(func(m map[string]string) { m["ACCOUNT_ATHENA_A100"] = "a;b" })
		}, []string{"--run", "conformance-athena", "--yes"}, 3, []string{"setting ACCOUNT_ATHENA_A100 holds characters an account does not"}},
		{"an unknown test", nil, []string{"--run", "no-such-test"}, 2, []string{"unknown test"}},
		{"a pending test", nil, []string{"--run", "rehearsal"}, 2, []string{"rehearsal is pending and cannot run yet"}},
		{"no test named", nil, []string{"--run"}, 2, []string{"--run needs at least one test name"}},
		{"nothing asked", nil, nil, 2, []string{"usage:"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := newWorld(t)
			w.useStubGo()
			if c.prep != nil {
				c.prep(w)
			}
			res := w.run(nil, c.args...)
			if res.code != c.code {
				t.Errorf("exit %d, want %d:\n%s", res.code, c.code, res.out)
			}
			w.contains(res, c.expect...)
			if w.stateFile("ssh-argv") != "" || w.stateFile("go-argv") != "" {
				t.Errorf("a refused run reached the cluster or the compiler")
			}
		})
	}
}

func TestMissingSettingsFileIsRefused(t *testing.T) {
	w := newWorld(t)
	cmd := w.command([]string{"HPCBURST_LIVE_SETTINGS="}, "--run", "hub-golden")
	out, err := cmd.CombinedOutput()
	if exitCode(err) != 3 || !strings.Contains(string(out), "HPCBURST_LIVE_SETTINGS is not set") {
		t.Errorf("exit %d:\n%s", exitCode(err), out)
	}
}

func (w *world) events() []string { return lines(w.stateFile("events")) }

// index returns the position of the nth (from 1) event that starts with prefix, or -1.
func index(ev []string, prefix string, nth int) int {
	for i, e := range ev {
		if strings.HasPrefix(e, prefix) {
			if nth--; nth == 0 {
				return i
			}
		}
	}
	return -1
}

func TestCleanupSparesJobsOutsideTheTestsPartition(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	res := w.run([]string{"STUB_TEST_MODE=leave-fail", "STUB_FOREIGN=1"}, "--run", "conformance-helios", "--yes")
	if res.code != 1 {
		t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
	}
	if got := lines(w.stateFile("cancelled")); len(got) != 1 || got[0] != "9001" {
		t.Errorf("cancelled %v: only the job in the test's partition is this run's", got)
	}
	if q := w.stateFile("queue"); !strings.Contains(q, "9002|") || strings.Contains(q, "9001|") {
		t.Errorf("another hb- job (a controller's, in another partition) must stay:\n%s", q)
	}
}

// Another live run on the same host would make the cleanup's diff unsafe, slot or not.
func TestOneLiveRunPerHost(t *testing.T) {
	for _, name := range []string{"conformance-helios", "conformance-athena"} {
		t.Run(name, func(t *testing.T) {
			w := newWorld(t)
			w.useStubGo()
			host := strings.TrimPrefix(name, "conformance-")
			f, err := os.OpenFile(filepath.Join(w.dir, "locks", host+"-live.lock"), os.O_CREATE|os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
				t.Fatal(err)
			}
			res := w.run(nil, "--run", name, "--yes")
			if res.code != 3 {
				t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
			}
			w.contains(res, "another live run holds this host's run lock")
			w.noSubmit()
		})
	}
}

// The slot's use starts after the builds: the compile (and, for the real-engine run, the plan and the
// staging) come before the second look at the slot, the baseline and the submit.
func TestBuildsFinishBeforeTheSlotIsTaken(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	if res := w.run(nil, "--run", "conformance-athena", "--yes"); res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	ev := w.events()
	est, build, check2, base, run := index(ev, "sbatch test-only", 1), index(ev, "go test", 1), index(ev, "squeue %q|%P", 2), index(ev, "squeue %i|%j|%P", 1), index(ev, "testbin TestConformance", 1)
	if !(index(ev, "squeue %q|%P", 1) < est && est < build && build < check2 && check2 < base && base < run) {
		t.Errorf("conformance order wrong, events:\n%s", strings.Join(ev, "\n"))
	}

	w = newWorld(t)
	w.useStubGo()
	if res := w.run(nil, "--run", "real-engine", "--yes"); res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	ev = w.events()
	lastBuild, plan, check2, base, submit := index(ev, "go test", 2), index(ev, "testbin TestRealEnginePlan", 1), index(ev, "squeue %q|%P", 2), index(ev, "squeue %i|%j|%P", 1), index(ev, "sbatch submit", 1)
	if !(index(ev, "go build", 1) < lastBuild && lastBuild < plan && plan < check2 && check2 < base && base < submit) {
		t.Errorf("real-engine order wrong, events:\n%s", strings.Join(ev, "\n"))
	}
}

func TestBuildsTakeTheBuildLockAndTestRunsDoNot(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	lock := filepath.Join(w.dir, "locks", "gobuild.lock")
	w.writeSettings(func(m map[string]string) { m["LIVE_BUILD_LOCK"] = lock })
	for _, args := range [][]string{{"--run", "conformance-helios", "--yes"}, {"--run", "hub-golden"}, {"--run", "real-engine", "--yes"}} {
		res := w.run([]string{"STUB_BUILD_LOCK=" + lock}, args...)
		if res.code != 0 {
			t.Fatalf("%v: exit %d:\n%s", args, res.code, res.out)
		}
	}
	builds := lines(w.stateFile("go-lock"))
	if len(builds) != 5 {
		t.Fatalf("go ran %d times, want 5 (one build each for conformance and the hub test, three for real-engine)", len(builds))
	}
	for _, b := range builds {
		if b != "held" {
			t.Errorf("a build ran without the build lock: %v", builds)
			break
		}
	}
	for _, b := range lines(w.stateFile("testbin-lock")) {
		if b != "free" {
			t.Errorf("a test binary ran while the build lock was held")
		}
	}
}

// A signal while the builds run must stop the run before it takes the slot and starts the test.
func TestSignalDuringTheBuildStopsBeforeTheSlotIsTaken(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	cmd, out := w.start([]string{"STUB_GO_HOLD=1"}, "--run", "conformance-athena", "--yes")
	w.waitFor("building")
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(w.state, "release"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	err := cmd.Wait()
	w.noLeak(out.String())
	w.noLeftoverProcess(cmd)
	if exitCode(err) == 0 {
		t.Errorf("an interrupted run exited 0:\n%s", out)
	}
	if !strings.Contains(out.String(), "FAIL (interrupted before the run started)") {
		t.Errorf("no interrupt report:\n%s", out)
	}
	w.noSubmit()
	if _, err := os.Stat(filepath.Join(w.dir, "locks", "athena-live.lock")); err == nil {
		// The lock file may exist from a stale run; what matters is that no baseline was read.
		if index(w.events(), "squeue %i|%j|%P", 1) >= 0 {
			t.Errorf("the run went on to read the queue for a baseline after the signal:\n%s", strings.Join(w.events(), "\n"))
		}
	}
}
