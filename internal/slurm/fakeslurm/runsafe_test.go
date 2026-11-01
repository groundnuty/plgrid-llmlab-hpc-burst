package fakeslurm

// New. B2, N1, N2 and N3 of the PR 153 review, on a whole run-mode platform:
// the batch shell stays a zombie, and so keeps its PID (the session ID the
// sweep selects by), until the sweep has finished; no scan of /proc happens
// once it is reaped; a job whose processes cannot be listed is not Finished;
// and a job that exited is finished in the state its exit gave it before the
// sweep, so a late scancel cannot change it. The tests stop the goroutines of
// the fake at the point they need through the knobs (hooks) of the Platform,
// so every interleaving is forced, not hoped for.

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// procState is the state letter of /proc/<pid>/stat ("Z" for a zombie, "S" or
// "R" for a live process), and "" for a PID that names nothing.
func procState(pid int) string {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return ""
	}
	f := strings.Fields(string(b[strings.LastIndexByte(string(b), ')')+1:]))
	if len(f) == 0 {
		return ""
	}
	return f[0]
}

// The batch shell stays a zombie while the sweep runs, and is reaped only
// after it. The sweep is held at its first scan that sees the shell as a
// zombie; at that moment the job has already ended COMPLETED (N3), a scancel
// finds it finished, and the child the shell left is alive.
func TestRunModeShellStaysAZombieUntilTheSweepEnds(t *testing.T) {
	r := newRunRig(t, RunMode{})
	var mu sync.Mutex
	var states []string // the shell's state at each scan
	inSweep, release := make(chan struct{}), make(chan struct{})
	var held atomic.Bool
	r.p.knobs.proc.scanned = func(leader int) {
		st := procState(leader)
		mu.Lock()
		states = append(states, st)
		mu.Unlock()
		if st == "Z" && held.CompareAndSwap(false, true) {
			close(inSweep)
			<-release
		}
	}
	childFile := r.home + "/child"
	id := r.submit("sleep 300 & echo $! > " + childFile + "; exit 0")
	shell := r.job(id).PID
	waitFor(t, "the sweep to scan with the shell a zombie", func() bool {
		select {
		case <-inSweep:
			return true
		default:
			return r.job(id).Finished // the sweep ended without ever seeing a zombie
		}
	})
	select {
	case <-inSweep:
	default:
		t.Fatalf("the job finished without a scan that found its shell a zombie; the shell's states at the scans: %v", states)
	}
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	child, err := strconv.Atoi(strings.TrimSpace(r.waitFile(childFile)))
	if err != nil {
		t.Fatal(err)
	}
	// N3: the exit is recorded before the sweep.
	j := r.job(id)
	if j.State != Completed || j.ExitCode != 0 || j.Finished {
		t.Errorf("during the sweep: state %s, exit code %d, finished %v; want COMPLETED, 0, not finished", j.State, j.ExitCode, j.Finished)
	}
	if out, errOut, code := r.run("scancel " + itoa(id)); code != 1 || !strings.Contains(errOut, "already completing or completed") {
		t.Errorf("scancel of a job that exited: exit %d, stdout %q, stderr %q; want 1, already completing or completed", code, out, errOut)
	}
	if j := r.job(id); j.State != Completed || !j.TermAt.IsZero() {
		t.Errorf("after the scancel: state %s, TermAt %v; want COMPLETED and no TERM", j.State, j.TermAt)
	}
	if st := procState(shell); st != "Z" {
		t.Errorf("the shell %d is %q during the sweep, want a zombie (Z): its PID must stay allocated", shell, st)
	}
	if st := procState(child); st == "" || st == "Z" {
		t.Errorf("the child %d is %q before the sweep reached it, want alive", child, st)
	}
	if pids, err := r.p.Processes(id); err != nil || len(pids) != 1 || pids[0] != child {
		t.Errorf("Processes during the sweep: %v, %v; want only the child %d", pids, err, child)
	}
	close(release)
	j = r.waitFinished(id)
	if !j.TermAt.IsZero() || !j.KillAt.IsZero() {
		t.Errorf("TermAt %v, KillAt %v for a job that exited by itself; the sweep's KILL is not counted", j.TermAt, j.KillAt)
	}
	if st := procState(shell); st != "" {
		t.Errorf("the shell %d is %q after the job finished, want it reaped", shell, st)
	}
	if !procGone(child) {
		t.Errorf("the child %d outlived the job", child)
	}
	mu.Lock()
	defer mu.Unlock()
	for i, st := range states {
		if st == "" {
			t.Errorf("scan %d found the shell's PID gone: a scan ran after the shell was reaped (states %v)", i, states)
		}
	}
}

// No terminator scans /proc after the run is over, however late it runs. The
// terminator that scancel starts is held before its first scan; the job then
// ends by itself and is finished; the terminator is released and must find
// nothing to do. Processes of a finished job lists nothing and does not scan.
func TestRunModeNoScanAfterTheShellIsReaped(t *testing.T) {
	r := newRunRig(t, RunMode{KillWait: time.Hour})
	var scans atomic.Int64
	r.p.knobs.proc.scanned = func(int) { scans.Add(1) }
	entered, release, ended := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	var releaseOnce, endOnce sync.Once
	letGo := func() { releaseOnce.Do(func() { close(release) }) }
	defer letGo() // a failed test must not leave a terminator blocked for Close
	r.p.knobs.terminateEnter = func(*jobRun) {
		if first.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
	}
	r.p.knobs.terminateDone = func(*jobRun) { endOnce.Do(func() { close(ended) }) }
	goFile := r.home + "/go"
	id := r.submit(waitLoop(goFile))
	r.must("scancel " + itoa(id))
	waitFor(t, "the terminator to start", func() bool {
		select {
		case <-entered:
			return true
		default:
			return false
		}
	})
	if err := os.WriteFile(goFile, nil, 0o644); err != nil { // the script ends by itself
		t.Fatal(err)
	}
	j := r.waitFinished(id)
	before := scans.Load()
	if before == 0 {
		t.Fatalf("the sweep made no scan; the test has nothing to compare")
	}
	letGo()
	waitFor(t, "the late terminator to end", func() bool {
		select {
		case <-ended:
			return true
		default:
			return false
		}
	})
	if pids, err := r.p.Processes(id); err != nil || len(pids) != 0 {
		t.Errorf("Processes of a finished job: %v, %v; want none", pids, err)
	}
	if after := scans.Load(); after != before {
		t.Errorf("%d scans of /proc after the shell was reaped, want none", after-before)
	}
	if j2 := r.job(id); !j2.TermAt.IsZero() || !j2.KillAt.IsZero() || j2.State != Cancelled || j.State != Cancelled {
		t.Errorf("TermAt %v, KillAt %v, state %s; want no signal sent after the end, and CANCELLED", j2.TermAt, j2.KillAt, j2.State)
	}
}

// A /proc that cannot be read leaves the job not Finished, with the reason in
// its snapshot and in Close: unknown is not "none left" (N1). Its state still
// follows its exit.
func TestRunModeUnreadableProcTableIsNotFinished(t *testing.T) {
	r := newRunRigCfg(t, Config{Run: &RunMode{}, NotListed: NotListedAthena}, false)
	r.p.knobs.proc.list = func() ([]procRow, error) { return nil, errors.New("proc table unreadable (injected)") }
	id := r.submit("exit 0")
	waitFor(t, "the sweep to give up", func() bool { return r.job(id).CleanupErr != nil })
	j := r.job(id)
	if j.Finished || j.State != Completed || j.ExitCode != 0 || !strings.Contains(j.CleanupErr.Error(), "proc table unreadable") {
		t.Errorf("finished %v, state %s, exit code %d, CleanupErr %v; want not finished, COMPLETED, 0 and the reason", j.Finished, j.State, j.ExitCode, j.CleanupErr)
	}
	if pids, err := r.p.Processes(id); err == nil || pids != nil {
		t.Errorf("Processes with an unreadable table: %v, %v; want an error", pids, err)
	}
	err := r.p.Close()
	if err == nil || !strings.Contains(err.Error(), "job "+itoa(id)) || !strings.Contains(err.Error(), "proc table unreadable") {
		t.Errorf("Close: %v, want an error naming job %d and the reason", err, id)
	}
}

// Processes that survive the sweep's KILL for its limit leave the job not
// Finished, with the error reported by the snapshot and by Close (N2). The
// survivor is a row the scripted table keeps listing: a real process that
// survives SIGKILL cannot be made on demand.
func TestRunModeSurvivorsOfKillLeaveTheJobNotFinished(t *testing.T) {
	r := newRunRigCfg(t, Config{Run: &RunMode{}, NotListed: NotListedAthena}, false)
	r.p.knobs.sweepLimit, r.p.knobs.sweepInterval = 50*time.Millisecond, time.Millisecond
	var leader atomic.Int64
	host := r.p.knobs.proc.list
	r.p.knobs.proc.scanned = func(l int) { leader.Store(int64(l)) }
	r.p.knobs.proc.list = func() ([]procRow, error) {
		rows, err := host()
		// A member of the job's session that no signal reaches: a PID that no
		// process has (pidfd_open says ESRCH), listed again at every scan.
		return append(rows, procRow{pid: 1<<22 - 1, ppid: 1, sid: int(leader.Load()), ticks: 1}), err
	}
	id := r.submit("exit 0")
	waitFor(t, "the sweep to give up", func() bool { return r.job(id).CleanupErr != nil })
	j := r.job(id)
	if j.Finished || j.State != Completed || !strings.Contains(j.CleanupErr.Error(), "survive SIGKILL") {
		t.Errorf("finished %v, state %s, CleanupErr %v; want not finished, COMPLETED, and the survivors reported", j.Finished, j.State, j.CleanupErr)
	}
	if err := r.p.Close(); err == nil || !strings.Contains(err.Error(), "survive SIGKILL") {
		t.Errorf("Close: %v, want an error naming the survivors", err)
	}
}
