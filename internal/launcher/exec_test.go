package launcher

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The exec-loop helper: the test binary, started with execLoopEnv=N, re-executes itself N times,
// each time from a thread other than the thread-group leader, as k3s does when it re-executes
// itself after unpacking. Each image appends one line to the report file before its exec, with
// its thread ID and PID, so the test can show the non-leader path ran. The last image writes
// "done" and waits to be killed.
const (
	execLoopEnv   = "HPCBURST_TEST_EXEC_LOOP"
	execReportEnv = "HPCBURST_TEST_EXEC_REPORT"
)

func init() {
	if os.Getenv(execLoopEnv) != "" {
		// Keep the main goroutine on the leader thread, so the exec below runs on another one.
		runtime.LockOSThread()
	}
}

func TestMain(m *testing.M) {
	if os.Getenv(execLoopEnv) != "" {
		execLoopHelper()
		return
	}
	if d := os.Getenv(abortLauncherEnv); d != "" { // before superviseEnv: its process sets both
		runAbortedLauncherHelper(d)
		return
	}
	if d := os.Getenv(superviseEnv); d != "" {
		runSupervisorHelper(d)
		return
	}
	code := m.Run()
	removeFakeController()
	os.Exit(code)
}

func appendReport(line string) {
	f, err := os.OpenFile(os.Getenv(execReportEnv), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		os.Exit(4)
	}
	fmt.Fprintln(f, line)
	f.Close()
}

func execLoopHelper() {
	n, _ := strconv.Atoi(os.Getenv(execLoopEnv))
	if n <= 0 {
		appendReport("done")
		time.Sleep(time.Hour)
		os.Exit(0)
	}
	go func() {
		runtime.LockOSThread()
		tid, pid := syscall.Gettid(), os.Getpid()
		appendReport(fmt.Sprintf("exec %d from tid %d of pid %d", n, tid, pid))
		env := []string{execLoopEnv + "=" + strconv.Itoa(n-1), execReportEnv + "=" + os.Getenv(execReportEnv)}
		err := syscall.Exec("/proc/self/exe", []string{os.Args[0]}, env)
		appendReport("exec failed: " + err.Error())
		os.Exit(3)
	}()
	time.Sleep(time.Hour)
}

var execLineRe = regexp.MustCompile(`^exec \d+ from tid (\d+) of pid (\d+)$`)

// TestLivenessAcrossConcurrentExec is the regression test for the old rule "the leader reads Z and
// the task scan finds no live thread, so the process is dead". A real process re-executes itself
// many times from a non-leader thread while the test asks identify, in a tight loop, whether it is
// alive. It is alive throughout (it only ends when the test kills it), so every answer other than
// "ours" is a misreport. As a control, the same loop applies the old task-scan rule to the
// same process and counts its false deaths; it is logged, not asserted, because that race is
// timing-dependent.
func TestLivenessAcrossConcurrentExec(t *testing.T) {
	const execs = 300
	report := filepath.Join(t.TempDir(), "report")
	cmd := exec.Command("/proc/self/exe")
	cmd.Env = []string{execLoopEnv + "=" + strconv.Itoa(execs), execReportEnv + "=" + report}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	pid := cmd.Process.Pid
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	t.Cleanup(func() { _ = cmd.Process.Kill(); <-done })

	rec, err := newRec("helper", pid)
	if err != nil {
		t.Fatal(err)
	}
	var trials, misreports, windowSeen, leaderZ, oldFalseDeaths int
	var firstMis []string
	deadline := time.Now().Add(3 * time.Minute)
	for finished := false; !finished; {
		for i := 0; i < 200; i++ {
			v, d := identify(rec)
			trials++
			if v != Ours {
				misreports++
				if len(firstMis) < 5 {
					firstMis = append(firstMis, v.String()+": "+d)
				}
			} else if strings.Contains(d, "execve in progress") {
				windowSeen++
			}
			// The control: the old rule, with this file's independent /proc reader.
			if c := procState(pid); c == 'Z' || c == 'X' {
				leaderZ++
				if !runningNow(pid) {
					oldFalseDeaths++
				}
			}
		}
		b, _ := os.ReadFile(report)
		finished = bytes.Contains(b, []byte("done\n"))
		select {
		case <-done:
			t.Fatalf("the helper exited during the test; report:\n%s", b)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("the helper did not finish its %d execs in time; report:\n%s", execs, b)
		}
	}
	// The report: every exec came from a thread other than the leader, under the same PID.
	b, _ := os.ReadFile(report)
	nonLeader := 0
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		m := execLineRe.FindStringSubmatch(l)
		if m == nil {
			continue
		}
		if m[1] != m[2] && m[2] == strconv.Itoa(pid) {
			nonLeader++
		}
	}
	t.Logf("NUMBERS concurrent exec: %d execs from a non-leader thread of pid %d; %d liveness checks; misreported %d; "+
		"identify saw the execve window %d times; the leader read Z %d times; the old task-scan rule said dead %d times",
		nonLeader, pid, trials, misreports, windowSeen, leaderZ, oldFalseDeaths)
	if nonLeader != execs {
		t.Fatalf("%d of %d execs came from a non-leader thread; report:\n%s", nonLeader, execs, b)
	}
	if windowSeen == 0 {
		t.Fatalf("in %d checks identify never caught the process in the middle of an execve, so this run proves nothing", trials)
	}
	if misreports != 0 {
		t.Fatalf("a live process was misreported %d times in %d checks: %v", misreports, trials, firstMis)
	}
	if v, d := identify(rec); v != Ours {
		t.Fatalf("after the execs: %v %s", v, d)
	}
}
