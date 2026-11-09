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

// B4 of the review of PR 284. The wait in the queue applies only when the latest read positively says PENDING: a job can
// start after the last good read, so a failed read at the bound tells nothing about a start, and is not NOT RUN. These go
// through the real poll loop, on a clock the stub moves 15 minutes at each read, so the third read is past the row's 30.
func TestRealEngineNotRunOnlyOnAReadThatSaysPending(t *testing.T) {
	const notRun = "real-engine: NOT RUN (a job was still pending after 30 min in the queue and was cancelled by its tag; not a failure)"
	const unreadable = "real-engine: FAIL (the queue could not be read three times in a row)"
	for _, c := range []struct {
		name, states string
		code         int
		want, wantNo string
	}{
		{"a read fails past the bound and the job then shows running: it started", "STUB_JOB_STATES=PENDING PENDING UNREADABLE RUNNING", 0, "job state: RUNNING", "NOT RUN"},
		{"three reads fail past the bound", "STUB_JOB_STATES=PENDING PENDING UNREADABLE UNREADABLE UNREADABLE", 1, unreadable, "NOT RUN"},
		{"a read fails, and the next past the bound says pending", "STUB_JOB_STATES=PENDING UNREADABLE PENDING PENDING", 3, notRun, "real-engine: FAIL"},
		{"a read that says something unparsable tells nothing either", "STUB_JOB_FOREVER=not-a-state", 1, unreadable, "NOT RUN"},
		{"a job that ran and is pending again was not waiting to start", "STUB_JOB_STATES=RUNNING PENDING PENDING PENDING", 0, "job state: RUNNING", "NOT RUN"},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			w := newWorld(t)
			w.useStubGo()
			res := w.run(append(w.fakeClock(), "STUB_CLOCK_STEP=900", c.states), "--run", "real-engine", "--yes")
			if res.code != c.code {
				t.Fatalf("exit %d, want %d:\n%s", res.code, c.code, res.out)
			}
			w.contains(res, c.want, "cleanup: left behind 0")
			if strings.Contains(res.out, c.wantNo) {
				t.Errorf("the run says %q:\n%s", c.wantNo, res.out)
			}
		})
	}
}

func (w *world) fakeClock() []string {
	write(w.t, filepath.Join(w.state, "clock"), "1000000", 0o644)
	return []string{"LIVE_FAKE_CLOCK=" + filepath.Join(w.state, "clock")}
}

// The job waited 90 minutes in the queue (the operator allowed 2 hours, LIVE_PENDING_MAX_SECONDS: the default for this
// row, in QOS now, is 30 minutes), longer than its whole run budget (57 min limit plus 15). It must still pass: the
// run's clock starts at the first state that is not PENDING.
func TestRealEnginePendingTimeIsNotRunTime(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	res := w.run(append(w.fakeClock(), "STUB_CLOCK_STEP=1800", "STUB_JOB_STATES=PENDING PENDING PENDING RUNNING", "LIVE_PENDING_MAX_SECONDS=7200"), "--run", "real-engine", "--yes")
	if res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	w.contains(res, "job state: PENDING", "job state: RUNNING", "processes left in the job 0")
	if w.stateFile("cancelled") != "" {
		t.Errorf("a job that was only waiting was cancelled")
	}
}

// The wait in the queue of real-engine, a row in QOS now, is 30 minutes (the science agent's ruling, 2 Nov 2026), written
// out here, and a job still pending past it ends the run as NOT RUN with exit 3, as issue #222 defines the consequence of a
// pend past the bound: the job never started, so it is not a failure. The script's cleanup cancels it (here the one job the
// stub submitted), nothing is posted, and the row is neither a pass nor a failure. The operator's own wait overrides the
// bound, exactly, even a part of a minute. The fake clock moves one step per poll of the job, and the run gives up at the
// first poll that finds the clock more than the bound past its submission, so the polls the run made are the bound read off
// the clock: 30 min in 60 s steps is 31 polls (30 min is not past 30 min, 31 are), 10 min is 11, 90 s in 60 s steps is 2,
// 2 h in 1 h steps is 3. A bound a minute either way changes the count.
func TestRealEngineJobPendingPastTheBoundIsNotRunNotFailed(t *testing.T) {
	for _, c := range []struct {
		name, setting, step, want string
		polls                     int
	}{
		{"the default", "", "60", "real-engine: NOT RUN (a job was still pending after 30 min in the queue and was cancelled by its tag; not a failure)", 31},
		{"a shorter wait asked for", "LIVE_PENDING_MAX_SECONDS=600", "60", "real-engine: NOT RUN (a job was still pending after 10 min in the queue and was cancelled by its tag; not a failure)", 11},
		{"a longer wait asked for", "LIVE_PENDING_MAX_SECONDS=7200", "3600", "real-engine: NOT RUN (a job was still pending after 120 min in the queue and was cancelled by its tag; not a failure)", 3},
		{"a part of a minute", "LIVE_PENDING_MAX_SECONDS=90", "60", "real-engine: NOT RUN (a job was still pending after 1 min 30 s in the queue and was cancelled by its tag; not a failure)", 2},
	} {
		t.Run(c.name, func(t *testing.T) {
			w := newWorld(t)
			w.useStubGo()
			env := append(w.fakeClock(), "STUB_CLOCK_STEP="+c.step, "STUB_JOB_FOREVER=PENDING")
			if c.setting != "" {
				env = append(env, c.setting)
			}
			res := w.run(env, "--run", "real-engine", "--yes")
			if res.code != 3 {
				t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
			}
			w.contains(res, c.want, "cleanup: left behind 0 (cancelled 1)")
			if strings.Contains(res.out, "real-engine: FAIL") || strings.Contains(res.out, "real-engine: PASS") || strings.Contains(res.out, "job ended") {
				t.Errorf("a NOT RUN was also reported as a failure or a pass, or the run went on:\n%s", res.out)
			}
			if got := strings.Count(w.stateFile("events"), "squeue %T job\n"); got != c.polls {
				t.Errorf("the run polled the job %d times before giving up, want %d", got, c.polls)
			}
			if got := lines(w.stateFile("cancelled")); len(got) != 1 || got[0] != "7001" {
				t.Errorf("cancelled %v, want the submitted job", got)
			}
			if m := slotUse.FindStringSubmatch(res.out); m == nil || m[1] != "now" {
				t.Errorf("no slot-use line for QOS now:\n%s", res.out)
			}
		})
	}
}

// A NOT RUN whose cleanup leaves the job behind is a failure, as for every row: the job was still pending past the bound and
// the cancel did nothing, so the slot is still held.
func TestRealEngineNotRunStaysAFailureWhenTheCleanupLeavesTheJob(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	res := w.run(append(w.fakeClock(), "STUB_CLOCK_STEP=3600", "STUB_JOB_FOREVER=PENDING", "STUB_SCANCEL_KEEP=1"), "--run", "real-engine", "--yes")
	if res.code != 1 {
		t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
	}
	w.contains(res, "real-engine: NOT RUN", "cleanup: left behind 1", "a job was left behind")
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
