package livechecks

// The cleanup after a run (PR 210's review, B1) and the dry run's estimate
// (the same review's nit). A submission whose reply was lost can register
// after the cleanup's first look at the queue, and a held job does not run out
// its time limit, so the cleanup matches the run's owned tags on every poll
// and reports "left behind 0" only for a queue that stayed clear. The stub
// squeue registers a late job (STUB_LATE_JOB at read STUB_LATE_AFTER, or a job
// in the queue that is hidden from the first STUB_HIDE_READS reads), counted
// in reads after the test binary ran. The expected strings are written out
// from the issue, not read from the script.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

var clusterRows = []string{"conformance-helios", "conformance-helios-gh200", "conformance-athena"}

const lateJob = "9003|hb-late-bbbbbbbbbbbb|normal|plgrid|PENDING"

func quick(extra ...string) []string {
	return append([]string{"LIVE_CLEAN_QUIET_POLLS=3", "LIVE_CLEAN_MAX_POLLS=12", "LIVE_CLEAN_UNRESOLVED_POLLS=4"}, extra...)
}

// A job that is in the queue but not yet shown at the first snapshot is found
// by a later poll and cancelled, on every conformance row: the cleanup does
// not take the first snapshot's IDs for all there is.
func TestCleanupCancelsAJobThatRegistersAfterTheFirstSnapshot(t *testing.T) {
	for _, row := range clusterRows {
		t.Run(row, func(t *testing.T) {
			w := newWorld(t)
			w.useStubGo()
			res := w.run(quick("STUB_TEST_MODE=late", "STUB_HIDE_READS=1"), "--run", row, "--yes")
			if res.code != 0 {
				t.Fatalf("exit %d, want 0 (the test passed; the cleanup cancelled a job it owns):\n%s", res.code, res.out)
			}
			w.contains(res, "cleanup: left behind 0 (cancelled 1)")
			if q := w.stateFile("queue"); strings.Contains(q, "9003|") {
				t.Errorf("the late job is still in the queue after a cleanup that said it left 0 behind:\n%s", q)
			}
			if c := w.stateFile("cancelled"); !strings.Contains(c, "9003") {
				t.Errorf("the late job was never cancelled: %q", c)
			}
		})
	}
}

// The same for a job that registers while the cleanup is polling (it was not
// in the queue at all at the first snapshot).
func TestCleanupCancelsAJobThatAppearsDuringItsPolls(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	res := w.run(quick("STUB_TEST_MODE=late-tag", "STUB_LATE_JOB="+lateJob, "STUB_LATE_AFTER=2"), "--run", "conformance-helios-gh200", "--yes")
	if res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	w.contains(res, "cleanup: left behind 0 (cancelled 1)")
	if q := w.stateFile("queue"); strings.Contains(q, "9003|") {
		t.Errorf("the job that registered during the polls was left in the queue:\n%s", q)
	}
}

// The row FAILS when an owned job is still there at the end: "left behind"
// counts the owned jobs in the last listing, so it is never 0 while one is
// listed. A cancel that did nothing is the case. A job listed again after its
// cancel is cancelled again and counted once.
func TestCleanupFailsTheRowWhileAnOwnedJobIsStillListed(t *testing.T) {
	for _, row := range clusterRows {
		t.Run(row, func(t *testing.T) {
			w := newWorld(t)
			w.useStubGo()
			res := w.run(quick("STUB_TEST_MODE=late", "STUB_SCANCEL_KEEP=1"), "--run", row, "--yes")
			if res.code != 1 {
				t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
			}
			w.contains(res, "cleanup: left behind 1 (cancelled 1)", "a job was left behind")
			if strings.Contains(res.out, "left behind 0") {
				t.Errorf("the cleanup said 0 while the job was listed:\n%s", res.out)
			}
			if n := strings.Count(w.stateFile("cancelled"), "9003"); n < 2 {
				t.Errorf("the job still listed was cancelled %d time(s), want it sent again", n)
			}
		})
	}
}

// A submission the test could not settle (unresolved.txt) makes the cleanup
// wait longer: a job that registers after the plain quiet window is still
// found and cancelled. Without the file the window is the plain one, and a
// job that registers after it is beyond the cleanup's reach, which is why the
// test says when it cannot settle a submission.
func TestCleanupWaitsLongerForAnUnresolvedSubmission(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	// Quiet polls 3, unresolved 4: the job appears at read 5, after a plain window of 3 would have ended.
	res := w.run(quick("STUB_TEST_MODE=late-unresolved", "STUB_LATE_JOB="+lateJob, "STUB_LATE_AFTER=5"), "--run", "conformance-helios-gh200", "--yes")
	if res.code != 0 {
		t.Fatalf("exit %d:\n%s", res.code, res.out)
	}
	w.contains(res, "cleanup: waited for 1 unresolved submission(s) the test could not settle", "cleanup: left behind 0 (cancelled 1)")
	if q := w.stateFile("queue"); strings.Contains(q, "9003|") {
		t.Errorf("the late job was left in the queue:\n%s", q)
	}

	w = newWorld(t)
	w.useStubGo()
	res = w.run(quick("STUB_TEST_MODE=late-tag", "STUB_LATE_JOB="+lateJob, "STUB_LATE_AFTER=5"), "--run", "conformance-helios-gh200", "--yes")
	if strings.Contains(res.out, "unresolved submission") {
		t.Errorf("an unresolved submission was reported with no unresolved.txt:\n%s", res.out)
	}
	if reads := strings.TrimSpace(w.stateFile("late-reads")); reads != "3" {
		t.Errorf("without unresolved.txt the cleanup read the queue %s times, want the plain window of 3", reads)
	}
}

// The unresolved file is read only as a regular file of tags in the run's own record directory: a symlink, or
// lines that are not tags, add no wait. (The quiet polls are 3 here, and the extra wait would be 4 more reads.)
func TestCleanupIgnoresAnUnresolvedFileThatIsNotAFileOfTags(t *testing.T) {
	for _, mode := range []string{"unresolved-symlink", "unresolved-junk"} {
		t.Run(mode, func(t *testing.T) {
			w := newWorld(t)
			w.useStubGo()
			res := w.run(quick("STUB_TEST_MODE="+mode), "--run", "conformance-helios", "--yes")
			if res.code != 0 {
				t.Fatalf("exit %d:\n%s", res.code, res.out)
			}
			if strings.Contains(res.out, "unresolved") {
				t.Errorf("an unusable unresolved.txt added a wait:\n%s", res.out)
			}
		})
	}
}

// A shape the scheduler REJECTS (sbatch --test-only exits non-zero: a site
// minimum, say) is told from an estimate that is merely unavailable, and stops
// the run, with or without --yes. sbatch's own text goes to the private log in
// the output directory and never to the screen.
func TestARejectedShapeStopsTheRunAndItsTextStaysPrivate(t *testing.T) {
	for _, args := range [][]string{{"--run", "conformance-helios-gh200"}, {"--run", "conformance-helios-gh200", "--yes"}} {
		t.Run(strings.Join(args[2:], ""), func(t *testing.T) {
			w := newWorld(t)
			w.useStubGo()
			res := w.run([]string{"STUB_TESTONLY_REJECT=1"}, args...)
			if res.code != 3 {
				t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
			}
			w.contains(res, "queue estimate: the scheduler REJECTED the shape (sbatch --test-only exit 1;", "not run: the shape is rejected by the scheduler")
			if strings.Contains(res.out, "Requested node configuration") || strings.Contains(res.out, "pass --yes") {
				t.Errorf("sbatch's text reached the screen, or the run offered --yes:\n%s", res.out)
			}
			if g := w.stateFile("go-argv"); g != "" {
				t.Errorf("a build ran after a rejection: %s", g)
			}
			if w.stateFile("test-env-TestConformance") != "" {
				t.Errorf("the test ran after a rejection")
			}
			log, err := os.ReadFile(filepath.Join(w.out(), "conformance-helios-gh200-estimate.log"))
			if err != nil || !strings.Contains(string(log), "Requested node configuration is not available") {
				t.Errorf("the private estimate log lacks sbatch's text: %q, %v", log, err)
			}
			if info, _ := os.Stat(filepath.Join(w.out(), "conformance-helios-gh200-estimate.log")); info == nil || info.Mode().Perm() != 0o600 {
				t.Errorf("the estimate log is not mode 600")
			}
		})
	}
}

// An accepted shape with no start time is "none", not a rejection, and the run goes on.
func TestAnEstimateThatIsMerelyUnavailableDoesNotStopTheRun(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	res := w.run([]string{"STUB_SSH_FAIL=1"}, "--run", "conformance-helios-gh200")
	if strings.Contains(res.out, "REJECTED") {
		t.Errorf("an unreachable cluster was reported as a rejection:\n%s", res.out)
	}
}

// The row's timeout reaches the test: the binary gets -test.timeout one minute above it (the script
// wraps the binary in timeout for the row's minutes as well).
func TestTheRowsTimeoutReachesTheTestBinary(t *testing.T) {
	for row, want := range map[string]string{"conformance-helios-gh200": "-test.timeout 58m", "conformance-helios": "-test.timeout 98m", "conformance-athena": "-test.timeout 58m"} {
		w := newWorld(t)
		w.useStubGo()
		if res := w.run(nil, "--run", row, "--yes"); res.code != 0 {
			t.Fatalf("%s: exit %d:\n%s", row, res.code, res.out)
		}
		if argv := w.stateFile("testbin-argv-TestConformance"); !strings.Contains(argv, want) {
			t.Errorf("%s: the test binary got %q, want it to contain %q", row, argv, want)
		}
	}
}

// A test that was stopped (the timeout, a signal) never got to write unresolved.txt, though its sbatch may be in
// flight: the cleanup treats every owned tag as unresolved and waits for a late registration (PR 210's review,
// B1: "a submission registering after a timeout or signal"). A test that failed by its own verdict, whose files
// were written, gets the plain window.
func TestCleanupWaitsForALateJobWhenTheTestWasStopped(t *testing.T) {
	const wait = "cleanup: the test was stopped before it finished, so a submission in flight may still register; waited for it"
	// The job appears at read 5: after the plain window of 3 quiet polls, inside the extended one.
	late := quick("STUB_LATE_JOB="+lateJob, "STUB_LATE_AFTER=5")

	t.Run("timed out", func(t *testing.T) {
		w := newWorld(t)
		w.useStubGo()
		res := w.run(append(late, "STUB_TEST_MODE=timed-out"), "--run", "conformance-helios-gh200", "--yes")
		w.contains(res, "FAIL (timed out)", wait, "cleanup: left behind 0 (cancelled 1)")
		if reads := strings.TrimSpace(w.stateFile("late-reads")); reads == "3" {
			t.Errorf("a stopped test got the plain window of 3 reads")
		}
		if q := w.stateFile("queue"); strings.Contains(q, "9003|") {
			t.Errorf("the late job was left in the queue:\n%s", q)
		}
	})
	t.Run("interrupted", func(t *testing.T) {
		w := newWorld(t)
		w.useStubGo()
		cmd, out := w.start(append(late, "STUB_TEST_MODE=sleep-late"), "--run", "conformance-helios-gh200", "--yes")
		w.waitFor("started")
		w.signal(cmd, syscall.SIGTERM)
		_ = cmd.Wait()
		w.noLeak(out.String())
		w.noLeftoverProcess(cmd)
		for _, want := range []string{"FAIL (interrupted)", wait, "cleanup: left behind 0 (cancelled 1)"} {
			if !strings.Contains(out.String(), want) {
				t.Errorf("the output lacks %q:\n%s", want, out)
			}
		}
		if q := w.stateFile("queue"); strings.Contains(q, "9003|") {
			t.Errorf("the late job was left in the queue:\n%s", q)
		}
	})
	t.Run("a test that failed by its own verdict gets the plain window", func(t *testing.T) {
		w := newWorld(t)
		w.useStubGo()
		res := w.run(append(late, "STUB_TEST_MODE=late-fail"), "--run", "conformance-helios-gh200", "--yes")
		if res.code != 1 || strings.Contains(res.out, "was stopped before it finished") {
			t.Fatalf("exit %d, or a stop reported for a test that failed by its own verdict:\n%s", res.code, res.out)
		}
		// The plain window: three quiet reads and no more, so the job due at read 5 never registered.
		if reads := strings.TrimSpace(w.stateFile("late-reads")); reads != "3" {
			t.Errorf("the cleanup read the queue %s times, want the plain window of 3", reads)
		}
	})
}

// The real-engine run is not a test binary the script waits on: it polls its own job. A signal that stops it
// is still a stop, so its cleanup waits for a late registration of the submission too.
func TestAnInterruptedRealEngineRunWaitsForALateRegistrationToo(t *testing.T) {
	w := newWorld(t)
	w.useStubGo()
	cmd, out := w.start(quick("STUB_JOB_FOREVER=1", "LIVE_POLL_SECONDS=1"), "--run", "real-engine", "--yes")
	w.waitFor("polled")
	w.signal(cmd, syscall.SIGTERM)
	_ = cmd.Wait()
	w.noLeak(out.String())
	w.noLeftoverProcess(cmd)
	for _, want := range []string{"cleanup: the test was stopped before it finished, so a submission in flight may still register; waited for it", "cleanup: left behind 0 (cancelled 1)"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("the output lacks %q:\n%s", want, out)
		}
	}
}

// The listing seam of #213. Under pipefail, printf ... | grep -q failed whenever grep stopped at a match while
// printf was still writing (bash writes its standard output a line at a time), and the failure read as "no
// match". A text over twice a pipe's buffer (64 KiB on Linux), with the line sought near its top, makes that
// certain: printf may fill the pipe once while grep reads it, and still has more to write when grep exits.
const pipeSeam = 2 * (64 << 10)

// An owned job still queued is never reported as gone, however many tags the run owns. The cleanup matches
// the listing against the owned tags on every poll, and "left behind" counts the owned jobs in the LAST
// listing. Here the run owns 3,001 tags, the queued job's tag sorts first, and scancel leaves the job in the
// queue, so it is listed on every poll, the last one included. Matched as not owned, it would be reported
// as "left behind 0" with the job still queued.
func TestCleanupCountsAnOwnedJobAmongManyTags(t *testing.T) {
	const tags = 3000
	if size := tags * (len("hb-"+strings.Repeat("z", 55)+"-0000") + 1); size <= pipeSeam {
		t.Fatalf("the owned tags list %d bytes, not over twice a pipe's buffer: the test would prove nothing", size)
	}
	w := newWorld(t)
	w.useStubGo()
	res := w.run(quick("STUB_TEST_MODE=leave-many-tags", fmt.Sprintf("STUB_MANY_TAGS=%d", tags), "STUB_SCANCEL_KEEP=1"), "--run", "conformance-helios", "--yes")
	if res.code != 1 {
		t.Fatalf("exit %d, want 1 (an owned job is still queued):\n%s", res.code, res.out)
	}
	w.contains(res, "cleanup: left behind 1 (cancelled 1)", "a job was left behind")
	if strings.Contains(res.out, "left behind 0") {
		t.Errorf("the cleanup said 0 while the owned job was listed:\n%s", res.out)
	}
	// Listed on every poll, the job keeps the cleanup polling to its bound (LIVE_CLEAN_MAX_POLLS in quick).
	if reads := strings.TrimSpace(w.stateFile("late-reads")); reads != "12" {
		t.Errorf("the cleanup read the queue %s times after the test, want all 12 polls", reads)
	}
	if !strings.Contains(w.stateFile("queue"), "9001|hb-leftover-") {
		t.Errorf("the job left the queue, so the last listing did not hold it and the test proves nothing")
	}
}

// A long queue is read, not taken for an unreadable one. queue_jobs looks for its marker line, squeue=ok, at
// the top of the reply; the listing after it (other runs' and the owner's hb- jobs) must not make the queue
// read as unreadable, which fails the run before it starts.
func TestALongQueueIsReadNotTakenForUnreadable(t *testing.T) {
	const foreign = 3000
	var queue strings.Builder
	reply := 0 // the reply lists each job as id|name|partition
	for i := 0; i < foreign; i++ {
		id, name := 20000+i, fmt.Sprintf("hb-controller-%s%04d", strings.Repeat("y", 40), i)
		fmt.Fprintf(&queue, "%d|%s|normal|plgrid|RUNNING\n", id, name)
		reply += len(fmt.Sprintf("%d|%s|plgrid\n", id, name))
	}
	if reply <= pipeSeam {
		t.Fatalf("the queue's reply is %d bytes, not over twice a pipe's buffer: the test would prove nothing", reply)
	}
	w := newWorld(t)
	w.useStubGo()
	write(t, filepath.Join(w.state, "queue"), queue.String(), 0o644)
	res := w.run(quick(), "--run", "conformance-helios", "--yes")
	if res.code != 0 {
		t.Fatalf("exit %d, want 0 (the queue was readable):\n%s", res.code, res.out)
	}
	w.contains(res, "conformance-helios: PASS", "cleanup: left behind 0 (cancelled 0)")
	if strings.Contains(res.out, "could not be read") {
		t.Errorf("a readable queue was reported as unreadable:\n%s", res.out)
	}
	if c := w.stateFile("cancelled"); c != "" {
		t.Errorf("jobs that are not this run's were cancelled: %q", c)
	}
}
