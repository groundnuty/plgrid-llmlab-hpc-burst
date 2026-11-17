package launcher

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

// waitExecOfSleep waits until the shell with this PID has exec'd sleep, which is when its ignored
// SIGTERM is final: before that, bash could still handle it.
func waitExecOfSleep(t *testing.T, pid int) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if exe, err := os.Readlink("/proc/" + strconv.Itoa(pid) + "/exe"); err == nil && strings.HasSuffix(exe, "/sleep") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("bash never exec'd sleep")
		}
	}
}

// The tests here use ordinary processes as stand-ins for frps, so they run wherever there is a
// shell and sleep(1). The same rules are shown on a real frps in frps_real_test.go.

// TestStopFrpsNeverSignalsAReusedPID gives StopFrps records that name a live process, with the
// start time or the boot ID of a process that once had the PID. The live process (a sleep that
// knows nothing of the launcher) must be neither signalled nor stopped, and the stop must fail,
// for each kind of mismatch.
func TestStopFrpsNeverSignalsAReusedPID(t *testing.T) {
	cmd := startBystander(t, "sleep", "300")
	pid := cmd.Process.Pid
	own, err := newRec(FrpsProcName, pid)
	if err != nil {
		t.Fatal(err)
	}

	rec := func(pid int, start uint64, boot string) ProcRec {
		return ProcRec{Name: FrpsProcName, PID: pid, StartTicks: start, BootID: boot}
	}
	for _, c := range []struct {
		name    string
		rec     ProcRec
		foreign bool
	}{
		{"another start time (+1 tick)", rec(pid, own.StartTicks+1, own.BootID), true},
		{"another start time (-1 tick)", rec(pid, own.StartTicks-1, own.BootID), true},
		{"recorded in another boot", rec(pid, own.StartTicks, "00000000-0000-0000-0000-000000000000"), true},
		{"no boot ID: identity unknown", rec(pid, own.StartTicks, ""), false},
		{"no start time: identity unknown", rec(pid, 0, own.BootID), false},
		{"pid 1", rec(1, 1, own.BootID), true},
	} {
		t.Run(c.name, func(t *testing.T) {
			rep, err := StopFrps(context.Background(), c.rec, 200*time.Millisecond)
			if err == nil {
				t.Fatalf("StopFrps = %+v, want a refusal", rep)
			}
			if got := errors.Is(err, ErrFrpsForeign); got != c.foreign {
				t.Errorf("errors.Is(err, ErrFrpsForeign) = %v, want %v (%v)", got, c.foreign, err)
			}
			if rep.Signals != "" {
				t.Errorf("signals sent: %q", rep.Signals)
			}
			if rep.Detail == "" {
				t.Error("the report does not say why")
			}
			time.Sleep(100 * time.Millisecond)
			if !runningNow(pid) {
				t.Fatal("the process holding the PID was signalled")
			}
			if st, err := readStat(pid); err != nil || st.StartTicks != own.StartTicks {
				t.Fatalf("the process changed: %+v %v", st, err)
			}
		})
	}

	// With the true record the same call does stop it: the refusals above are about identity, not
	// about this process being unstoppable.
	rep, err := StopFrps(context.Background(), own, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Signals != "TERM" || rep.Before != "ours" {
		t.Errorf("report = %+v", rep)
	}
	// Safe as a post-condition: StopFrps returns nil only after its pidfd wait saw the exit.
	if !waitNotRunning(pid, 5*time.Second) {
		t.Fatal("the process is still running after SIGTERM")
	}
	// A process that has gone is a stop that succeeded, with nothing signalled.
	rep, err = StopFrps(context.Background(), own, time.Second)
	if err != nil || rep.Before != "gone" || rep.Signals != "" {
		t.Errorf("after the exit: %+v %v", rep, err)
	}
}

// TestStopFrpsEscalatesToKill: a process that ignores SIGTERM gets SIGKILL once the grace period
// is over, and not before. SIGTERM ignored is inherited across exec, so sleep ignores it.
func TestStopFrpsEscalatesToKill(t *testing.T) {
	cmd := startBystander(t, "bash", "-c", `trap "" TERM; exec sleep 300`)
	pid := cmd.Process.Pid
	waitExecOfSleep(t, pid)
	rec, err := newRec(FrpsProcName, pid)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	rep, err := StopFrps(context.Background(), rec, 400*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Signals != "TERM+KILL" {
		t.Errorf("signals = %q, want TERM+KILL", rep.Signals)
	}
	if d := time.Since(start); d < 400*time.Millisecond {
		t.Errorf("SIGKILL came after %s, before the grace period was over", d)
	}
	// Safe as a post-condition: StopFrps returns nil only after its pidfd wait saw the exit after SIGKILL.
	if !waitNotRunning(pid, 5*time.Second) {
		t.Fatal("still running after SIGKILL")
	}
}

// TestStopFrpsCancelledContextCutsTheGraceShort: ctx ends the wait for SIGTERM to work, never the
// wait after SIGKILL.
func TestStopFrpsCancelledContextCutsTheGraceShort(t *testing.T) {
	cmd := startBystander(t, "bash", "-c", `trap "" TERM; exec sleep 300`)
	pid := cmd.Process.Pid
	waitExecOfSleep(t, pid)
	rec, err := newRec(FrpsProcName, pid)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	rep, err := StopFrps(ctx, rec, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 5*time.Second || rep.Signals != "TERM+KILL" {
		t.Errorf("took %s, signals %q: the cancelled context did not cut the hour-long grace short", d, rep.Signals)
	}
	// Safe as a post-condition: StopFrps returns nil only after its pidfd wait saw the exit after SIGKILL.
	if !waitNotRunning(pid, 5*time.Second) {
		t.Fatal("still running")
	}
}

// standInProc is an FrpsProc around an ordinary child (a sleep): the pidfd and the reaping are the
// ones StartFrps sets up, the program is not frps.
func standInProc(t *testing.T) (*FrpsProc, *exec.Cmd) {
	t.Helper()
	cmd := exec.Command("sleep", "300")
	pidfd := -1
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, PidFD: &pidfd}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &FrpsProc{cmd: cmd, done: make(chan struct{})}
	go func() { _ = cmd.Wait(); close(p.done) }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
		}
	})
	p.h = &procHandle{rec: ProcRec{Name: FrpsProcName, PID: cmd.Process.Pid}, fd: pidfd}
	if _, err := p.h.record(); err != nil {
		t.Fatal(err)
	}
	return p, cmd
}

// takeDescriptorNumber opens pidfds for pid until one gets the number n, as the kernel hands out
// the lowest free number, and closes the others. It stands for "something opened a file after n
// was freed": here another process's pidfd. It fails the test when n was taken by something
// else, so a run can never close a descriptor that is not its own. That makes it reliable only in
// a process where nothing else opens a file between the close of n and this call, which a test
// binary running other tests is not (#718): the closed-handle regression below runs it in a child
// test binary of its own. TestSignalHandleRefusesAClosedHandle still calls it in the test binary.
func takeDescriptorNumber(t *testing.T, n, pid int) {
	t.Helper()
	var spare []int
	defer func() {
		for _, fd := range spare {
			_ = unix.Close(fd)
		}
	}()
	for i := 0; i < 4096; i++ {
		fd, err := unix.PidfdOpen(pid, 0)
		if err != nil {
			t.Fatal(err)
		}
		if fd == n {
			t.Cleanup(func() { _ = unix.Close(n) })
			t.Logf("descriptor %d: the bystander's pidfd took the number the closed handle held", n)
			return
		}
		spare = append(spare, fd)
		if fd > n {
			t.Fatalf("descriptor %d is in use by something else, so its reuse cannot be reproduced", n)
		}
	}
	t.Fatalf("no pidfd got the number %d", n)
}

// The closed-handle regression needs a process in which nothing else opens a file between the
// close of the handle's pidfd and the reopening of its number. The test binary is not one: the
// package's t.Parallel tests, a goroutine that a finished test left draining and the runtime all
// open files, and when one of them took the number first the test failed by design (#718). So each
// case runs in a child, this test binary on one test alone (testenv.StartChild, as frps_leak_test.go
// does), where only the case's own goroutines exist. The child makes every check the test makes,
// and a precondition it cannot reproduce is a failure there, never a skip. It also writes what it
// saw, and the parent checks that again: a child that ran no test at all, or returned early, exits 0
// and would otherwise pass.

const (
	// closedHandleChildTest is the test the children run; closedHandleCaseEnv names the case.
	closedHandleChildTest = "TestFrpsProcClosedHandleChild"
	closedHandleCaseEnv   = "HPCBURST_CLOSED_HANDLE_CHILD_CASE"
)

// closedHandleCases are the ways an FrpsProc's pidfd gets closed. key is what the parent tells the
// child; the parent's subtests keep their names.
var closedHandleCases = []struct {
	name  string
	key   string
	close func(t *testing.T, p *FrpsProc)
}{
	{"after Release", "release", func(t *testing.T, p *FrpsProc) { p.Release() }},
	{"after a Stop", "stop", func(t *testing.T, p *FrpsProc) {
		if _, err := p.Stop(context.Background(), 5*time.Second); err != nil {
			t.Fatal(err)
		}
	}},
}

// closedHandleRecord is what a child saw, written after its checks have passed.
type closedHandleRecord struct {
	Case           string            `json:"case"`
	Descriptor     int               `json:"descriptor"`      // the closed handle's number, then held by the bystander's pidfd
	BystanderAlive bool              `json:"bystander_alive"` // after the later Stop
	Released       bool              `json:"released"`        // the later Stop failed with ErrFrpsReleased
	Signals        string            `json:"signals"`         // what the later Stop sent
	StopMS         int64             `json:"stop_ms"`
	RecordStop     *closedHandleStop `json:"record_stop,omitempty"` // after Release: StopFrps with the record
}

// closedHandleStop is the result of StopFrps(record).
type closedHandleStop struct {
	OK      bool   `json:"ok"`
	Signals string `json:"signals"`
}

// TestFrpsProcClosedHandleChild is run only as a child of the test below, and returns at once
// otherwise. It runs one case, named by closedHandleCaseEnv.
func TestFrpsProcClosedHandleChild(t *testing.T) {
	if _, ok := testenv.ChildRecordPath(); !ok {
		return
	}
	key := os.Getenv(closedHandleCaseEnv)
	for _, c := range closedHandleCases {
		if c.key == key {
			runClosedHandleCase(t, c.key, c.close)
			return
		}
	}
	t.Fatalf("%s=%q is not a case", closedHandleCaseEnv, key)
}

// runClosedHandleCase closes an frps stand-in's handle, makes the bystander's pidfd take the
// number it held, and checks that a later Stop touches nothing.
func runClosedHandleCase(t *testing.T, key string, closeHandle func(t *testing.T, p *FrpsProc)) {
	frps, _ := standInProc(t)
	bystander := startBystander(t, "sleep", "300")
	n := frps.h.fd
	closeHandle(t, frps)
	takeDescriptorNumber(t, n, bystander.Process.Pid)

	start := time.Now()
	rep, err := frps.Stop(context.Background(), 3*time.Second)
	time.Sleep(100 * time.Millisecond)
	if !runningNow(bystander.Process.Pid) {
		t.Fatalf("the process that owns the reused descriptor number was signalled (Stop = %+v, %v)", rep, err)
	}
	if !errors.Is(err, ErrFrpsReleased) {
		t.Fatalf("Stop = %+v, %v, want ErrFrpsReleased", rep, err)
	}
	if rep.Signals != "" {
		t.Errorf("signals sent: %q", rep.Signals)
	}
	d := time.Since(start)
	if d > 2*time.Second {
		t.Errorf("Stop took %s: it waited on a closed handle", d)
	}
	rec := closedHandleRecord{Case: key, Descriptor: n, BystanderAlive: true, Released: true, Signals: rep.Signals, StopMS: d.Milliseconds()}
	// The record still works: it reopens the process by PID and checks it.
	if key == "release" {
		rs, err := StopFrps(context.Background(), frps.Rec(), 5*time.Second)
		if err != nil || rs.Signals != "TERM" {
			t.Errorf("StopFrps(record) = %+v, %v", rs, err)
		}
		if !runningNow(bystander.Process.Pid) {
			t.Fatal("StopFrps stopped the wrong process")
		}
		rec.RecordStop = &closedHandleStop{OK: err == nil, Signals: rs.Signals}
	}
	testenv.WriteChildRecord(t, rec)
}

// TestFrpsProcClosedHandleNeverTouchesAReusedDescriptor is the regression for the closed-handle
// bug: Release (or a Stop) closes the pidfd, the kernel gives its number to another process's
// pidfd, and a later Stop used to verify frps by /proc but poll and signal through that number,
// killing the other process and leaving frps running. Now the later Stop fails and sends nothing.
// Each case runs in a child (see above); here the child's exit, its PASS line, the reuse it
// logged and its record are checked.
func TestFrpsProcClosedHandleNeverTouchesAReusedDescriptor(t *testing.T) {
	for _, c := range closedHandleCases {
		t.Run(c.name, func(t *testing.T) {
			child := testenv.StartChild(t, closedHandleChildTest, closedHandleCaseEnv+"="+c.key)
			// d only bounds a child that never ends: a run that touches the closed handle takes
			// 10 to 13 s, and the child's own test timeout is 3 minutes.
			code, out := child.Wait(2 * time.Minute)
			if code != 0 {
				t.Fatalf("the child exited %d:\n%s", code, out)
			}
			if !strings.Contains(out, "--- PASS: "+closedHandleChildTest+" ") {
				t.Fatalf("the child has no PASS line for %s, so it ran no test, or skipped it:\n%s", closedHandleChildTest, out)
			}
			var rec closedHandleRecord
			child.WaitRecord(10*time.Second, &rec)
			if rec.Case != c.key {
				t.Fatalf("the child ran the case %q, want %q: %+v", rec.Case, c.key, rec)
			}
			if rec.Descriptor < 3 {
				t.Fatalf("the child's descriptor number is %d: %+v", rec.Descriptor, rec)
			}
			if want := "descriptor " + strconv.Itoa(rec.Descriptor) + ": the bystander's pidfd took the number"; !strings.Contains(out, want) {
				t.Errorf("the child did not log %q, so the reuse was not reproduced there:\n%s", want, out)
			}
			if !rec.BystanderAlive {
				t.Error("the bystander that owned the reused number was signalled")
			}
			if !rec.Released {
				t.Errorf("the later Stop did not fail with ErrFrpsReleased: %+v", rec)
			}
			if rec.Signals != "" {
				t.Errorf("signals sent: %q", rec.Signals)
			}
			if rec.StopMS > 2000 {
				t.Errorf("Stop took %d ms: it waited on a closed handle", rec.StopMS)
			}
			if c.key == "release" {
				if rec.RecordStop == nil || !rec.RecordStop.OK || rec.RecordStop.Signals != "TERM" {
					t.Errorf("StopFrps(record) after Release: %+v", rec.RecordStop)
				}
			} else if rec.RecordStop != nil {
				t.Errorf("the %q case recorded a StopFrps(record): %+v", c.key, rec.RecordStop)
			}
		})
	}
}

// TestSignalHandleRefusesAClosedHandle: the shared rule under the test above, on the plain handle.
func TestSignalHandleRefusesAClosedHandle(t *testing.T) {
	p, _ := standInProc(t)
	bystander := startBystander(t, "sleep", "300")
	n := p.h.fd
	p.h.Close()
	takeDescriptorNumber(t, n, bystander.Process.Pid)
	if sent, err := signalHandle(p.h, syscall.SIGTERM); sent || !errors.Is(err, errHandleClosed) {
		t.Fatalf("signalHandle = %v, %v, want an errHandleClosed and nothing sent", sent, err)
	}
	if _, err := p.h.exited(); !errors.Is(err, errHandleClosed) {
		t.Errorf("exited on a closed handle: %v", err)
	}
	p.h.Close() // idempotent: it must not close the number a second time
	time.Sleep(100 * time.Millisecond)
	if !runningNow(bystander.Process.Pid) {
		t.Fatal("the other process was signalled")
	}
	var st unix.Stat_t
	if err := unix.Fstat(n, &st); err != nil {
		t.Errorf("a second Close closed descriptor %d, which now belongs to the other process's pidfd: %v", n, err)
	}
}

// TestFrpsProcReleaseAndStopRace runs Release, Stop and Release together on one process. Run it
// with -race: the closed flag and the descriptor are guarded by one mutex.
func TestFrpsProcReleaseAndStopRace(t *testing.T) {
	for i := 0; i < 20; i++ {
		p, cmd := standInProc(t)
		var wg sync.WaitGroup
		for _, f := range []func(){
			p.Release,
			func() { _, _ = p.Stop(context.Background(), 200*time.Millisecond) },
			p.Release,
		} {
			wg.Add(1)
			go func() { defer wg.Done(); f() }()
		}
		wg.Wait()
		_ = cmd.Process.Kill()
	}
}
