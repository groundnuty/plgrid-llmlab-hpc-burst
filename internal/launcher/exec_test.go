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

	"golang.org/x/sys/unix"
)

// The exec-loop helper: the test binary, started with execLoopEnv=N, re-executes itself N times,
// each time from a thread other than the thread-group leader, as k3s does when it re-executes
// itself after unpacking. Each image appends one line to the report file before its exec, with
// its thread ID and PID, so the test can show the non-leader path ran. The last image writes
// "done" and waits to be killed.
//
// A helper can also be a reparented grandchild, which is not the test's child, as a process an
// earlier invocation of the launcher started is not the next invocation's: the test starts an
// intermediate (execSpawnEnv) that starts the helper and exits. The helper then holds the read end
// of a pipe from the test (execPipeEnv) and, once its execs are done, waits for that pipe to
// close, so it ends by itself if the test binary dies.
const (
	execLoopEnv   = "HPCBURST_TEST_EXEC_LOOP"
	execReportEnv = "HPCBURST_TEST_EXEC_REPORT"
	execSpawnEnv  = "HPCBURST_TEST_EXEC_SPAWN"   // the intermediate: the exec count to start its helper with
	execPidEnv    = "HPCBURST_TEST_EXEC_PIDFILE" // where the intermediate writes its helper's PID
	execPipeEnv   = "HPCBURST_TEST_EXEC_PIPE"    // the helper's descriptor number for the pipe from the test
	// execCountEnv sets how many execs TestLivenessAcrossConcurrentExec runs (default 300). A
	// hosted run on a 6.15 or later kernel raises it to see the pidfd's transient (#690).
	execCountEnv     = "HPCBURST_TEST_EXEC_COUNT"
	defaultExecCount = 300
)

func init() {
	if os.Getenv(execLoopEnv) != "" {
		// Keep the main goroutine on the leader thread, so the exec below runs on another one.
		runtime.LockOSThread()
	}
}

func TestMain(m *testing.M) {
	if os.Getenv(execSpawnEnv) != "" {
		spawnReparentedHelper()
		return
	}
	if os.Getenv(execLoopEnv) != "" {
		execLoopHelper()
		return
	}
	if os.Getenv(listenEnv) != "" || os.Getenv(listenChildEnv) != "" {
		runListenHelper()
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

// spawnReparentedHelper is the intermediate: it starts the exec-loop helper with the pipe the test
// gave it (descriptor 3), records the helper's PID, and exits, so that the helper is reparented.
func spawnReparentedHelper() {
	cmd := exec.Command("/proc/self/exe")
	cmd.Env = []string{
		execLoopEnv + "=" + os.Getenv(execSpawnEnv),
		execReportEnv + "=" + os.Getenv(execReportEnv),
		execPipeEnv + "=3",
	}
	cmd.ExtraFiles = []*os.File{os.NewFile(3, "pipe-from-the-test")}
	if err := cmd.Start(); err != nil {
		os.Exit(5)
	}
	if err := os.WriteFile(os.Getenv(execPidEnv), []byte(strconv.Itoa(cmd.Process.Pid)), 0o600); err != nil {
		os.Exit(6)
	}
	os.Exit(0)
}

// waitToBeKilled is where the helper ends up: it waits for the pipe from the test to close (or
// for the test to kill it), or, when it has no pipe, for an hour.
func waitToBeKilled() {
	if fd, err := strconv.Atoi(os.Getenv(execPipeEnv)); err == nil {
		var b [1]byte
		_, _ = os.NewFile(uintptr(fd), "pipe-from-the-test").Read(b[:]) // returns at end of file
		os.Exit(0)
	}
	time.Sleep(time.Hour)
	os.Exit(0)
}

func execLoopHelper() {
	n, _ := strconv.Atoi(os.Getenv(execLoopEnv))
	if n <= 0 {
		appendReport("done")
		waitToBeKilled()
	}
	go func() {
		runtime.LockOSThread()
		tid, pid := syscall.Gettid(), os.Getpid()
		appendReport(fmt.Sprintf("exec %d from tid %d of pid %d", n, tid, pid))
		env := []string{execLoopEnv + "=" + strconv.Itoa(n-1), execReportEnv + "=" + os.Getenv(execReportEnv)}
		if p := os.Getenv(execPipeEnv); p != "" {
			env = append(env, execPipeEnv+"="+p)
		}
		err := syscall.Exec("/proc/self/exe", []string{os.Args[0]}, env)
		appendReport("exec failed: " + err.Error())
		os.Exit(3)
	}()
	time.Sleep(time.Hour)
}

// startReparented starts an exec-loop helper of n execs that is not the test's child: the test
// starts an intermediate, which starts the helper and exits, and the helper is reparented. It
// returns the helper's PID and its report file. The helper ends when the test's pipe closes, and
// the cleanup also kills it through a pidfd opened here, never by a bare PID.
func startReparented(t *testing.T, n int) (pid int, report string) {
	t.Helper()
	dir := t.TempDir()
	report, pidFile := filepath.Join(dir, "report"), filepath.Join(dir, "pid")
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() }) // the helper's end of file
	cmd := exec.Command("/proc/self/exe")
	cmd.Env = []string{execSpawnEnv + "=" + strconv.Itoa(n), execReportEnv + "=" + report, execPidEnv + "=" + pidFile}
	cmd.ExtraFiles = []*os.File{r}
	err = cmd.Run() // returns once the intermediate has exited, so the helper has been reparented
	r.Close()
	if err != nil {
		t.Fatalf("the intermediate that starts the reparented helper: %v", err)
	}
	b, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	if pid, err = strconv.Atoi(string(b)); err != nil {
		t.Fatalf("the helper's PID file holds %q", b)
	}
	own, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		t.Fatalf("the reparented helper (pid %d) is not there: %v", pid, err)
	}
	t.Cleanup(func() {
		_ = unix.PidfdSendSignal(own, unix.SIGKILL, nil, 0)
		_ = unix.Close(own)
	})
	return pid, report
}

var execLineRe = regexp.MustCompile(`^exec \d+ from tid (\d+) of pid (\d+)$`)

// execCount reads the number of execs for the liveness test from execCountEnv. A value that is not
// a positive integer fails the test, naming the variable, rather than running with a default.
func execCount(t *testing.T) int {
	t.Helper()
	v, set := os.LookupEnv(execCountEnv)
	if !set || v == "" {
		return defaultExecCount
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		t.Fatalf("%s=%q is not a positive integer", execCountEnv, v)
	}
	return n
}

// kernelRelease is the kernel's release string (what uname -r prints), or says why it is unknown.
func kernelRelease() string {
	b, err := os.ReadFile("/proc/sys/kernel/osrelease")
	if err != nil {
		return "unknown (" + err.Error() + ")"
	}
	return strings.TrimSpace(string(b))
}

// TestLivenessAcrossConcurrentExec is the regression test for two races between a process's
// execve from a non-leader thread and a liveness check. The first is the old rule "the leader
// reads Z and the task scan finds no live thread, so the process is dead". The second, on Linux
// 6.15 and later, is the pidfd itself (#690): while the PID is handed to the exec'ing thread, one
// lockless pidfd_poll can read readable for a process that lives (fs/pidfs.c tests
// `exit_state && !delay_group_leader(task)` on a leader whose exit_signal is already -1), which
// sysPidfd.exited confirms with waitid. A real process re-executes itself many times from a
// non-leader thread while the test asks identify, in a tight loop, whether it is alive. It is
// alive throughout (it only ends when the test kills it), so every answer other than "ours" is
// a misreport. As a control, the same loop applies the old task-scan rule to the same process
// and counts its false deaths; it is logged, not asserted, because that race is timing-dependent.
//
// It runs twice: with the helper as the test's child (waitid answers for it, so the "alive by
// waitid" counter can rise) and as a reparented grandchild, which is not the test's child (waitid
// answers ECHILD, and the re-poll confirms, so the re-poll counter can rise). The second race does
// not exist before Linux 6.15, so a pass on an older kernel shows nothing about it. The NUMBERS
// lines name the kernel and the two overturn counts (zero on an older kernel), which are logged,
// not asserted, since they depend on timing. HPCBURST_TEST_EXEC_COUNT raises the exec count.
func TestLivenessAcrossConcurrentExec(t *testing.T) {
	execs := execCount(t)
	t.Run("child", func(t *testing.T) { runExecLiveness(t, execs, false) })
	t.Run("grandchild", func(t *testing.T) { runExecLiveness(t, execs, true) })
}

func runExecLiveness(t *testing.T, execs int, grandchild bool) {
	t.Helper()
	overChild, overRepoll := pidfdOverturnedChild.Load(), pidfdOverturnedRepoll.Load()
	var pid int
	var report string
	var done <-chan struct{} // closed when the helper, as the test's child, has been reaped
	label := "as a child"
	if grandchild {
		label = "as a reparented grandchild"
		pid, report = startReparented(t, execs)
	} else {
		report = filepath.Join(t.TempDir(), "report")
		cmd := exec.Command("/proc/self/exe")
		cmd.Env = []string{execLoopEnv + "=" + strconv.Itoa(execs), execReportEnv + "=" + report}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		pid = cmd.Process.Pid
		d := make(chan struct{})
		go func() { _ = cmd.Wait(); close(d) }()
		done = d
		t.Cleanup(func() { _ = cmd.Process.Kill(); <-d })
	}

	rec, err := newRec("helper", pid)
	if err != nil {
		t.Fatal(err)
	}
	var trials, misreports, windowSeen, leaderZ, oldFalseDeaths int
	var firstMis []string
	// 3 minutes for every 300 execs: 300 took 4 s on a hosted runner and 13 s on a loaded VM.
	deadline := time.Now().Add(3 * time.Minute * time.Duration((execs+defaultExecCount-1)/defaultExecCount))
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
		case <-done: // never ready for a grandchild, which nobody waits for
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
	t.Logf("NUMBERS concurrent exec (%s): %d execs from a non-leader thread of pid %d; %d liveness checks; misreported %d; "+
		"identify saw the execve window %d times; the leader read Z %d times; the old task-scan rule said dead %d times; "+
		"kernel %s; pidfd readable polls overturned: by waitid on a live child %d, by the re-poll of a non-child %d",
		label, nonLeader, pid, trials, misreports, windowSeen, leaderZ, oldFalseDeaths,
		kernelRelease(), pidfdOverturnedChild.Load()-overChild, pidfdOverturnedRepoll.Load()-overRepoll)
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
