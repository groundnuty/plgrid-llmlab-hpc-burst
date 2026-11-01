package livechecks

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestRealEngineRunBuildsStagesSubmitsPollsAndFetches(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	res := w.run(nil, "--run", "real-engine", "--yes")
	if res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	w.contains(res, "real-engine: shape: partition plgrid-gpu-gh200, qos now, 1 GPU, 72 CPU, 120000M, time limit 00:57:00, one job, no requeue",
		"queue estimate: a start in about", "submitted one job", "job state: PENDING", "job state: RUNNING", "job state: gone",
		"job ended COMPLETED 0:0; TestRealEngine passed 1; processes left in the job 0", "record: 1 file(s) fetched",
		"staged files stay on the cluster", "cleanup: left behind 0 (cancelled 0)")
	if m := slotUse.FindStringSubmatch(res.out); m == nil || m[1] != "now" {
		t.Errorf("no slot-use line for QOS now:\n%s", res.out)
	}

	// The estimate comes first and asks for the submitted shape.
	argv := lines(w.stateFile("sbatch-argv"))
	if len(argv) != 2 || !strings.Contains(argv[0], "--test-only") || strings.Contains(argv[1], "--test-only") {
		t.Fatalf("want the estimate and then one submit, got %v", argv)
	}
	for i, a := range argv {
		for _, f := range []string{"-p plgrid-gpu-gh200", "-q now", "-N 1", "-c 72", "--mem=120000M", "--gres=gpu:1", "-C memfs", "-t 00:57:00", "--no-requeue", "-A SEEDACCT-gh"} {
			if !strings.Contains(a, f) {
				t.Errorf("sbatch call %d lacks %q: %s", i, f, a)
			}
		}
	}
	if !strings.Contains(argv[1], "-J hb-live-live-") || !strings.Contains(argv[1], "--export=ALL,H=") {
		t.Errorf("the submit does not name its job hb-live-... or export H: %s", argv[1])
	}
	// The builds are the documented ones.
	goArgv := w.stateFile("go-argv")
	for _, f := range []string{"build -trimpath -ldflags -X main.version=live-", "-o " + filepath.Join(w.out(), "real-engine/bin/hb-agent") + " ./cmd/hb-agent",
		"test -c -trimpath -o " + filepath.Join(w.out(), "real-engine/bin/hbagent.test") + " ./cmd/hb-agent", "test -count=1 -run ^TestRealEnginePlan$ ./cmd/hb-agent"} {
		if !strings.Contains(goArgv, f) {
			t.Errorf("go was not run as %q:\n%s", f, goArgv)
		}
	}
	// What the job needs reached the cluster, under the user's own directory.
	staged, _ := filepath.Glob(filepath.Join(w.state, "scratch-helios", "hpc-burst-live", "live-*"))
	if len(staged) != 1 {
		t.Fatalf("staged directories: %v", staged)
	}
	for _, f := range []string{"bin/hb-agent", "bin/hbagent.test", "plan.json", "job.sh", "logs"} {
		if _, err := os.Stat(filepath.Join(staged[0], f)); err != nil {
			t.Errorf("%s was not staged: %v", f, err)
		}
	}
	if strings.Contains(w.stateFile("ssh-argv"), "SEEDACCT") {
		t.Errorf("the account went into the ssh arguments")
	}
	if _, err := os.Stat(filepath.Join(w.out(), "real-engine", "record", "summary.json")); err != nil {
		t.Errorf("the run's record was not fetched: %v", err)
	}
}

func TestRealEngineRunFailsWhenTheJobLeftProcessesBehind(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	res := w.run([]string{"STUB_PROCS_LEFT=2"}, "--run", "real-engine", "--yes")
	if res.code != 1 {
		t.Errorf("exit %d, want 1:\n%s", res.code, res.out)
	}
	w.contains(res, "processes left in the job 2")
}

func TestRealEngineRunCancelsItsJobWhenInterrupted(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	cmd, out := w.start([]string{"STUB_JOB_FOREVER=1", "LIVE_POLL_SECONDS=1"}, "--run", "real-engine", "--yes")
	w.waitFor("polled")
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	err := cmd.Wait()
	w.noLeak(out.String())
	w.noLeftoverProcess(cmd)
	if exitCode(err) == 0 {
		t.Errorf("an interrupted run exited 0:\n%s", out)
	}
	if got := lines(w.stateFile("cancelled")); len(got) != 1 || got[0] != "7001" {
		t.Errorf("cancelled %v, want the submitted job", got)
	}
	if !strings.Contains(out.String(), "cleanup: left behind 0 (cancelled 1)") {
		t.Errorf("no cleanup report:\n%s", out)
	}
	if strings.Contains(w.stateFile("queue"), "7001|") {
		t.Errorf("the job is still queued")
	}
}
