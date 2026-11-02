package livechecks

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
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
	run := w.realEngineDir()
	// The builds are the documented ones.
	goArgv := w.stateFile("go-argv")
	for _, f := range []string{"build -trimpath -ldflags -X main.version=live-", "-o " + filepath.Join(run, "bin/hb-agent") + " ./cmd/hb-agent",
		"test -c -trimpath -o " + filepath.Join(run, "bin/hbagent.test") + " ./cmd/hb-agent",
		"test -c -o " + filepath.Join(run, "plan.test") + " ./cmd/hb-agent"} {
		if !strings.Contains(goArgv, f) {
			t.Errorf("go was not run as %q:\n%s", f, goArgv)
		}
	}
	if !strings.Contains(w.stateFile("test-env-TestRealEnginePlan"), "HB_REAL_PLAN_OUT="+filepath.Join(run, "plan.json")) {
		t.Errorf("the plan test did not run with HB_REAL_PLAN_OUT set:\n%s", w.stateFile("test-env-TestRealEnginePlan"))
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
	if _, err := os.Stat(filepath.Join(run, "record", "summary.json")); err != nil {
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
	cmd, out := w.start([]string{"STUB_JOB_FOREVER=1", "STUB_FOREIGN=1", "LIVE_POLL_SECONDS=1"}, "--run", "real-engine", "--yes")
	w.waitFor("polled")
	w.signal(cmd, syscall.SIGTERM)
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
	if q := w.stateFile("queue"); strings.Contains(q, "7001|") || !strings.Contains(q, "9002|") {
		t.Errorf("the run's job must be gone and another hb- job (a controller's, say) must stay:\n%s", q)
	}
}

func (w *world) fakeClock() []string {
	write(w.t, filepath.Join(w.state, "clock"), "1000000", 0o644)
	return []string{"LIVE_FAKE_CLOCK=" + filepath.Join(w.state, "clock")}
}

// The job waited 90 minutes in the queue, longer than its whole run budget (57 min limit plus 15). It
// must still pass: the run's clock starts at the first state that is not PENDING.
func TestRealEnginePendingTimeIsNotRunTime(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	res := w.run(append(w.fakeClock(), "STUB_CLOCK_STEP=1800", "STUB_JOB_STATES=PENDING PENDING PENDING RUNNING"), "--run", "real-engine", "--yes")
	if res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	w.contains(res, "job state: PENDING", "job state: RUNNING", "processes left in the job 0")
	if w.stateFile("cancelled") != "" {
		t.Errorf("a job that was only waiting was cancelled")
	}
}

func TestRealEngineJobPendingForTooLongIsCancelled(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	res := w.run(append(w.fakeClock(), "STUB_CLOCK_STEP=3600", "STUB_JOB_FOREVER=PENDING"), "--run", "real-engine", "--yes")
	if res.code != 1 {
		t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
	}
	w.contains(res, "FAIL (the job was still pending after 120 minutes; cancelling it)", "cleanup: left behind 0 (cancelled 1)")
	if got := lines(w.stateFile("cancelled")); len(got) != 1 || got[0] != "7001" {
		t.Errorf("cancelled %v, want the submitted job", got)
	}
}

func TestRealEngineJobRunningPastItsLimitIsCancelled(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	res := w.run(append(w.fakeClock(), "STUB_CLOCK_STEP=1800", "STUB_JOB_FOREVER=RUNNING"), "--run", "real-engine", "--yes")
	if res.code != 1 {
		t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
	}
	w.contains(res, "FAIL (the job ran past its time limit plus 15 minutes; cancelling it)", "cleanup: left behind 0 (cancelled 1)")
}

// sacct can answer nothing for a moment after the job leaves squeue.
func TestRealEngineAsksSacctAgainBeforeCallingTheStateUnknown(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	res := w.run([]string{"STUB_SACCT_EMPTY=2", "LIVE_SACCT_GAP=0"}, "--run", "real-engine", "--yes")
	if res.code != 0 {
		t.Errorf("a lagging sacct: exit %d, want 0:\n%s", res.code, res.out)
	}
	w.contains(res, "job ended COMPLETED 0:0")
	w2 := newWorld(t)
	w2.useStubGo()
	res = w2.run([]string{"STUB_SACCT_EMPTY=99", "LIVE_SACCT_GAP=0"}, "--run", "real-engine", "--yes")
	if res.code != 1 {
		t.Errorf("a sacct that never answers: exit %d, want 1:\n%s", res.code, res.out)
	}
	w2.contains(res, "job ended unknown")
}

// realEngineDir is the directory of the run's own below the output directory (one per run).
func (w *world) realEngineDir() string {
	w.t.Helper()
	m, _ := filepath.Glob(filepath.Join(w.out(), "real-engine-live-*"))
	if len(m) != 1 {
		w.t.Fatalf("real-engine run directories: %v", m)
	}
	return m[0]
}

// The reply to the submit is lost after Slurm accepted it. The tag was written before the submit, so the
// cleanup finds the job by it and cancels it; it does not report "left behind 0" for a job it never saw.
func TestRealEngineLostSubmitReplyIsReconciledByTag(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	res := w.run([]string{"STUB_LOSE_SUBMIT_REPLY=1"}, "--run", "real-engine", "--yes")
	if res.code != 1 {
		t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
	}
	w.contains(res, "FAIL (the submit did not complete)", "cleanup: left behind 0 (cancelled 1)")
	if got := lines(w.stateFile("cancelled")); len(got) != 1 || got[0] != "7001" {
		t.Errorf("cancelled %v, want the job Slurm accepted", got)
	}
	if strings.Contains(w.stateFile("queue"), "7001|") {
		t.Errorf("the accepted job is still queued")
	}
	tag, _ := os.ReadFile(filepath.Join(w.realEngineDir(), "submission-tag"))
	if !strings.HasPrefix(string(tag), "hb-live-live-") {
		t.Errorf("no submission tag was written before the submit: %q", tag)
	}
}

// The queue cannot be read once the job exists. The known job is cancelled all the same, and the failed
// verification is reported on its own.
func TestRealEngineJobIsCancelledEvenWhenSqueueFails(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	res := w.run([]string{"STUB_BREAK_SQUEUE_AFTER_SUBMIT=1"}, "--run", "real-engine", "--yes")
	if res.code != 1 {
		t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
	}
	w.contains(res, "FAIL (the queue could not be read three times in a row)", "cleanup: the queue could not be read, so the cleanup could not look for this run's jobs by tag or confirm a cancel (cancelled 1 known job(s)); check the queue by hand")
	if strings.Contains(res.out, "left behind 0") {
		t.Errorf("an unverified cleanup reported 'left behind 0':\n%s", res.out)
	}
	got := lines(w.stateFile("cancelled"))
	if len(got) < 1 || got[0] != "7001" {
		t.Errorf("cancelled %v, want the known job", got)
	}
	if n := strings.Count(res.out, "the queue could not be read, so the cleanup"); n != 2 {
		t.Errorf("the cleanup was tried %d times, want the run's try and the exit trap's retry", n)
	}
}

func TestRealEngineFailedRecordFetchFailsTheRun(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	res := w.run([]string{"STUB_FETCH_FAIL=1"}, "--run", "real-engine", "--yes")
	if res.code != 1 {
		t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
	}
	w.contains(res, "FAIL (the run's record could not be fetched)")
}

// Each run fetches into a directory of its own: a second run does not count the first run's files.
func TestRealEngineRecordsAreFreshPerRun(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	for i := 0; i < 2; i++ {
		res := w.run(nil, "--run", "real-engine", "--yes")
		if res.code != 0 {
			t.Fatalf("run %d: exit %d:\n%s", i, res.code, res.out)
		}
		w.contains(res, "record: 1 file(s) fetched")
		time.Sleep(1100 * time.Millisecond) // the run directory is named by the second
	}
	if m, _ := filepath.Glob(filepath.Join(w.out(), "real-engine-live-*")); len(m) != 2 {
		t.Errorf("run directories %v, want two", m)
	}
}
