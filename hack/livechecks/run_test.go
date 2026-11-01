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
