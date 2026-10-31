package launcher

// Tests of the supervisor's start-up handoff (Astra 12 on PR #88): an interrupted up must not leave
// a supervisor that state.json does not name. The processes are judged from /proc and from the
// files the fake controller writes (argv-<pid>.json), not from the launcher's own records.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The aborted-launcher helper: the test binary, started with these two variables, runs
// startSupervisor with a hook that stops it between the supervisor's creation and the save of its
// record, so that the test can kill it there.
const (
	abortLauncherEnv = "HPCBURST_TEST_ABORT_LAUNCHER" // the state directory
	abortMarkerEnv   = "HPCBURST_TEST_ABORT_MARKER"   // the file that receives the supervisor's ProcRec
)

func runAbortedLauncherHelper(dir string) {
	marker := os.Getenv(abortMarkerEnv)
	// The supervisor started below inherits this environment, and must act as a supervisor.
	os.Unsetenv(abortLauncherEnv)
	os.Unsetenv(abortMarkerEnv)
	die := func(err error) {
		fmt.Fprintln(os.Stderr, "aborted-launcher helper:", err)
		os.Exit(2)
	}
	l, err := newLayout(dir)
	if err != nil {
		die(err)
	}
	st, err := l.loadState()
	if err != nil || st == nil {
		die(fmt.Errorf("state: %v, %v", st, err))
	}
	self, err := os.Executable()
	if err != nil {
		die(err)
	}
	afterSupervisorStart = func(rec ProcRec) {
		b, err := json.Marshal(rec)
		if err == nil {
			err = writeFileAtomic(marker, b, 0o600)
		}
		if err != nil {
			die(err)
		}
		time.Sleep(time.Hour) // the test kills this process here
	}
	err = startSupervisor(context.Background(), l, st, []string{self}, 30*time.Second, os.Stderr)
	die(fmt.Errorf("startSupervisor returned without being interrupted: %v", err))
}

// ticksOf reads a process's start time from /proc with a parser of its own; false when it is gone.
func ticksOf(pid int) (uint64, bool) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, false
	}
	s := string(b)
	fields := strings.Fields(s[strings.LastIndexByte(s, ')')+1:])
	n, err := strconv.ParseUint(fields[19], 10, 64)
	return n, err == nil
}

// isProc is true while the process recorded as rec runs: its PID is alive and has rec's start time.
func isProc(rec ProcRec) bool {
	n, ok := ticksOf(rec.PID)
	return ok && n == rec.StartTicks && runningNow(rec.PID)
}

func waitGone(rec ProcRec, d time.Duration) bool {
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if !isProc(rec) {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return !isProc(rec)
}

// killOnExit kills rec, and everything below it or in its group or session, at the end of the
// test if it still runs, so that a failed assertion (or a mutated launcher) never leaves a
// supervisor behind.
func killOnExit(t *testing.T, rec ProcRec) {
	t.Helper()
	t.Cleanup(func() {
		if isProc(rec) {
			killTree([]int{rec.PID}, "")
		}
	})
}

// fakeStarts lists the controller starts the fake recorded in its control directory.
func (f *ctlFixture) fakeStarts() []string {
	m, _ := filepath.Glob(filepath.Join(f.ctl, "argv-*.json"))
	return m
}

// TestInterruptedStartLeavesNoSupervisor: the launcher is killed (SIGKILL, so nothing runs in it)
// after it created the supervisor and before it saved the supervisor's record. The supervisor
// must have started nothing meanwhile, must exit by itself when the launcher is gone, and a later
// start must work.
func TestInterruptedStartLeavesNoSupervisor(t *testing.T) {
	f := newCtlFixture(t, Front{})
	k3s := startBystander(t, "sleep", "300")
	k3sRec, err := newRec("k3s", k3s.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	f.st.Procs = []ProcRec{k3sRec}
	if err := f.l.saveState(f.st); err != nil {
		t.Fatal(err)
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(superviseEnv, f.l.dir) // for every supervisor started below
	marker := filepath.Join(t.TempDir(), "supervisor.json")
	var helperOut bytes.Buffer
	helper := exec.Command(self)
	helper.Env = append(os.Environ(), abortLauncherEnv+"="+f.l.dir, abortMarkerEnv+"="+marker)
	helper.Stdout, helper.Stderr = &helperOut, &helperOut
	if err := helper.Start(); err != nil {
		t.Fatal(err)
	}
	helperDone := make(chan struct{})
	go func() { _ = helper.Wait(); close(helperDone) }()
	t.Cleanup(func() { _ = helper.Process.Kill(); <-helperDone })

	var sup ProcRec
	deadline := time.Now().Add(30 * time.Second)
	for {
		if b, err := os.ReadFile(marker); err == nil && json.Unmarshal(b, &sup) == nil {
			break
		}
		select {
		case <-helperDone:
			t.Fatalf("the launcher helper ended before it created the supervisor:\n%s", helperOut.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("the launcher helper did not create the supervisor:\n%s", helperOut.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	killOnExit(t, sup)

	// The launcher is alive but has not confirmed: the supervisor exists and starts nothing.
	time.Sleep(time.Second)
	if !isProc(sup) {
		t.Fatalf("the supervisor (pid %d) is gone while its launcher is alive:\n%s", sup.PID, fileOf(f.l.supervisorLog()))
	}
	if starts := f.fakeStarts(); len(starts) != 0 {
		t.Fatalf("the supervisor started a controller before its launcher confirmed the record: %v", starts)
	}
	if _, err := os.Stat(f.l.supervisorFile()); !os.IsNotExist(err) {
		t.Fatalf("supervisor.json exists before the launcher confirmed the record: %v", err)
	}

	// The launcher dies before saving the record.
	if err := helper.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-helperDone
	if !waitGone(sup, 10*time.Second) {
		t.Fatalf("the supervisor (pid %d) still runs after its launcher died without recording it:\n%s", sup.PID, fileOf(f.l.supervisorLog()))
	}
	if starts := f.fakeStarts(); len(starts) != 0 {
		t.Errorf("a controller was started: %v", starts)
	}
	if _, err := os.Stat(f.l.supervisorFile()); !os.IsNotExist(err) {
		t.Errorf("supervisor.json exists: %v", err)
	}
	if got := fileOf(f.l.supervisorLog()); !strings.Contains(got, "ended without confirming") {
		t.Errorf("supervisor.log does not say why the supervisor ended:\n%s", got)
	}
	st, err := f.l.loadState()
	if err != nil || len(st.Procs) != 1 || st.Procs[0].Name != "k3s" {
		t.Fatalf("state.json after the interrupted start: %+v, %v; want only k3s recorded", st, err)
	}

	// A later start works, and its supervisor is a new process.
	f.st = st
	stopOnExit(t, f)
	if err := startSupervisor(context.Background(), f.l, f.st, []string{self}, 30*time.Second, testLog{t}); err != nil {
		t.Fatalf("the start after the interrupted one: %v", err)
	}
	if len(f.st.Procs) != 2 || f.st.Procs[1] == sup || f.record().Supervisor != f.st.Procs[1] {
		t.Errorf("procs %+v, supervisor.json names %+v; the first supervisor was %+v", f.st.Procs, f.record().Supervisor, sup)
	}
	awaitHealthy(t, f.ports.ControllerAddr())
}

// TestStartSupervisorSaveFailureStopsTheSupervisor: the record cannot be saved (the state
// directory is read-only from the hook on). startSupervisor fails, nothing is left of the
// supervisor, and the in-memory record is rolled back.
func TestStartSupervisorSaveFailureStopsTheSupervisor(t *testing.T) {
	if os.Geteuid() == 0 {
		skipOrFail(t, "a read-only directory does not stop root, so the failing save cannot be provoked")
	}
	f := newCtlFixture(t, Front{})
	k3s := startBystander(t, "sleep", "300")
	k3sRec, err := newRec("k3s", k3s.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	f.st.Procs = []ProcRec{k3sRec}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(superviseEnv, f.l.dir)
	var sup ProcRec
	afterSupervisorStart = func(rec ProcRec) {
		sup = rec
		if err := os.Chmod(f.l.dir, 0o500); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(func() { afterSupervisorStart = nil; _ = os.Chmod(f.l.dir, 0o700) })
	err = startSupervisor(context.Background(), f.l, f.st, []string{self}, 30*time.Second, testLog{t})
	_ = os.Chmod(f.l.dir, 0o700)
	if sup.PID == 0 {
		t.Fatalf("the hook did not run: startSupervisor = %v", err)
	}
	killOnExit(t, sup)
	if err == nil {
		t.Fatal("startSupervisor succeeded though the record could not be saved")
	}
	if !waitGone(sup, 10*time.Second) {
		t.Errorf("the supervisor (pid %d) still runs though its record was not saved", sup.PID)
	}
	if len(f.st.Procs) != 1 {
		t.Errorf("procs %+v: the unsaved supervisor is still in the record", f.st.Procs)
	}
	if starts := f.fakeStarts(); len(starts) != 0 {
		t.Errorf("a controller was started: %v", starts)
	}
}

// TestSupervisorKeepsTheHandoffPipeToItself: the supervisor receives the pipe at descriptor 3, and
// after the handoff neither it nor the controller holds it: the controller must not inherit it,
// and the supervisor has no use for it once it has the confirmation.
func TestSupervisorKeepsTheHandoffPipeToItself(t *testing.T) {
	f := newCtlFixture(t, Front{})
	k3s := startBystander(t, "sleep", "300")
	k3sRec, err := newRec("k3s", k3s.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	f.st.Procs = []ProcRec{k3sRec}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(superviseEnv, f.l.dir)
	stopOnExit(t, f)
	var handed string
	afterSupervisorStart = func(rec ProcRec) {
		handed, _ = os.Readlink(fmt.Sprintf("/proc/%d/fd/%d", rec.PID, HandoffFD))
	}
	t.Cleanup(func() { afterSupervisorStart = nil })
	if err := startSupervisor(context.Background(), f.l, f.st, []string{self}, 30*time.Second, testLog{t}); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(handed, "pipe:[") {
		t.Fatalf("the supervisor holds %q at descriptor %d, not a pipe", handed, HandoffFD)
	}
	child := f.record().Entries[0].Proc
	if child == nil {
		t.Fatal("no controller is running")
	}
	for name, pid := range map[string]int{"supervisor": f.st.Procs[1].PID, "controller": child.PID} {
		fds, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
		if err != nil {
			t.Fatal(err)
		}
		for _, fd := range fds {
			if link, _ := os.Readlink(fmt.Sprintf("/proc/%d/fd/%s", pid, fd.Name())); link == handed {
				t.Errorf("the %s (pid %d) holds the handoff pipe %s as descriptor %s", name, pid, handed, fd.Name())
			}
		}
	}
}

// TestSuperviseStartsNothingBeforeTheHandoff: with a pipe that is open and silent, Supervise waits
// and starts no controller; the confirmation then starts it.
func TestSuperviseStartsNothingBeforeTheHandoff(t *testing.T) {
	f := newCtlFixture(t, Front{})
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	f.supervise(SuperviseOptions{Handoff: r})
	time.Sleep(700 * time.Millisecond)
	if starts := f.fakeStarts(); len(starts) != 0 {
		t.Fatalf("a controller started before the confirmation: %v", starts)
	}
	if _, err := os.Stat(f.l.supervisorFile()); !os.IsNotExist(err) {
		t.Fatalf("supervisor.json exists before the confirmation: %v", err)
	}
	if _, err := w.Write([]byte{handoffGo}); err != nil {
		t.Fatal(err)
	}
	f.awaitRunning()
}

// TestSuperviseRefusesWithoutConfirmation: each way the confirmation can fail to come makes
// Supervise return an error that says so, having started and written nothing.
func TestSuperviseRefusesWithoutConfirmation(t *testing.T) {
	cases := []struct {
		name    string
		handoff func(t *testing.T) io.ReadCloser
		timeout time.Duration
		want    string
	}{
		{"the launcher closes the pipe without the byte", func(t *testing.T) io.ReadCloser {
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			w.Close()
			return r
		}, 0, "ended without confirming"},
		{"the launcher sends another byte", func(t *testing.T) io.ReadCloser {
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			_, _ = w.Write([]byte{'x'})
			w.Close()
			return r
		}, 0, "not its confirmation"},
		{"the launcher is alive and silent", func(t *testing.T) io.ReadCloser {
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { w.Close() }) // after the test: it ends the read the timeout abandoned
			return r
		}, 300 * time.Millisecond, "did not confirm"},
		{"there is no handoff", func(*testing.T) io.ReadCloser { return nil }, 0, "no handoff"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newCtlFixture(t, Front{})
			o := SuperviseOptions{StateDir: f.l.dir, HandoffTimeout: c.timeout, Log: testLog{t}}
			if h := c.handoff(t); h != nil {
				o.Handoff = h
			}
			// Bounded, so a supervisor that wrongly runs on makes the test fail instead of hang.
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			start := time.Now()
			err := Supervise(ctx, o)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("Supervise = %v, want an error containing %q", err, c.want)
			}
			if c.timeout > 0 && time.Since(start) < c.timeout {
				t.Errorf("returned after %s, before the %s timeout", time.Since(start), c.timeout)
			}
			if starts := f.fakeStarts(); len(starts) != 0 {
				t.Errorf("a controller was started: %v", starts)
			}
			if _, err := os.Stat(f.l.supervisorFile()); !os.IsNotExist(err) {
				t.Errorf("supervisor.json was written: %v", err)
			}
		})
	}
}

// TestSuperviseStopsWaitingWhenToldToStop: SIGTERM (a cancelled context) during the wait ends it.
func TestSuperviseStopsWaitingWhenToldToStop(t *testing.T) {
	f := newCtlFixture(t, Front{})
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	ctx, cancel := context.WithCancel(t.Context())
	res := make(chan error, 1)
	go func() { res <- Supervise(ctx, SuperviseOptions{StateDir: f.l.dir, Handoff: r}) }()
	time.Sleep(200 * time.Millisecond)
	cancel()
	select {
	case err := <-res:
		if err == nil || !strings.Contains(err.Error(), "context canceled") {
			t.Errorf("Supervise = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Supervise did not return after its context was cancelled")
	}
}

// TestOpenHandoff: only an open pipe at a descriptor the launcher could have passed is accepted,
// and it is marked close-on-exec.
func TestOpenHandoff(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	defer r.Close()
	reg, err := os.Open("/dev/null")
	if err != nil {
		t.Fatal(err)
	}
	defer reg.Close()
	closedFD, err := syscall.Dup(int(reg.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	syscall.Close(closedFD)
	for name, fd := range map[string]int{"stdin": 0, "a character device": int(reg.Fd()), "a closed descriptor": closedFD, "a negative one": -1} {
		if h, err := OpenHandoff(fd); err == nil {
			h.Close()
			t.Errorf("%s (fd %d) accepted", name, fd)
		}
	}
	// dup clears close-on-exec, as exec does for the descriptors it passes.
	fd, err := syscall.Dup(int(r.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	h, err := OpenHandoff(fd)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	flags, _, errno := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_GETFD, 0)
	if errno != 0 || flags&syscall.FD_CLOEXEC == 0 {
		t.Errorf("descriptor %d is not close-on-exec after OpenHandoff (flags %d, %v)", fd, flags, errno)
	}
	if _, err := w.Write([]byte{handoffGo}); err != nil {
		t.Fatal(err)
	}
	if err := awaitHandoff(t.Context(), h, time.Second); err != nil {
		t.Errorf("a confirmation written to the pipe was not read through the opened descriptor: %v", err)
	}
}
