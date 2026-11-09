package fakeslurm

// New. Issue #288: a background child of a job's batch shell must end with a
// host that dies without Close. The parent-death signal (procstart.go) reaches
// the shell and not what it started; the keeper (keeper.go) ends the session.
//
// Two verifications here would prove nothing, and neither is used: a job script
// whose background child ends on its own (it is gone whatever the host does),
// and a host that exits normally (Close ends every job with no keeper at all).
// The child is a loop that never ends, and the host is killed by SIGKILL,
// which runs no code in the host.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// newlineName is a command name with a newline in it: /proc/<pid>/stat then
// spans two lines, and a reader that takes the first line sees half a record.
const newlineName = "line\nbreak"

// dieWithLoop is the helper host: a run-mode platform with one batch job whose
// script starts a loop in the background and waits for it. It writes
// "<shell pid> <loop pid>" to readyFile once both run, and is killed by
// SIGKILL when the file readyFile.go exists, with no Close and no cleanup. With
// named set the loop runs under a command name with a newline in it: the shell
// itself, started through a symlink of that name, so the kernel's name for the
// process is the link's.
func dieWithLoop(root, readyFile string, named bool) int {
	home := filepath.Join(root, "home", "example-user")
	if err := os.MkdirAll(home, 0o755); err != nil {
		return 2
	}
	p := New(fake.NewClock(t0), Config{Run: &RunMode{Root: root}})
	s := p.Login(Login{User: "example-user", Env: map[string]string{"HOME": home}})
	loopFile := filepath.Join(root, "loop-pid")
	wrap := foreverLoop + " & echo $! > " + loopFile + "; wait"
	if named {
		link := filepath.Join(root, newlineName)
		if err := os.Symlink(jobShell, link); err != nil {
			return 2
		}
		// printf makes the newline, so the wrap holds none and no single quote.
		wrap = `n=$(printf "line\nbreak"); "` + root + `/$n" -c "` + foreverLoop + `" & echo $! > ` + loopFile + "; wait"
	}
	res, err := s.Run(context.Background(), slurm.Command{Line: "sbatch --parsable '--wrap=" + wrap + "'"})
	if err != nil || res.ExitCode != 0 {
		return 2
	}
	id, err := strconv.Atoi(strings.TrimSpace(string(res.Stdout)))
	if err != nil {
		return 2
	}
	deadline := time.Now().Add(waitDeadline)
	var loop string
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(loopFile); err == nil && strings.HasSuffix(string(b), "\n") {
			loop = strings.TrimSpace(string(b))
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	j, _ := p.Job(id)
	if loop == "" || j.PID <= 0 || os.WriteFile(readyFile+".tmp", []byte(fmt.Sprintf("%d %s\n", j.PID, loop)), 0o644) != nil || os.Rename(readyFile+".tmp", readyFile) != nil {
		return 3
	}
	for {
		if _, err := os.Stat(readyFile + ".go"); err == nil {
			break
		}
		if time.Now().After(deadline) {
			return 3
		}
		time.Sleep(5 * time.Millisecond)
	}
	_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
	time.Sleep(time.Minute) // not reached: the host is gone
	return 0
}

// procsUnder lists the live processes whose working directory is root or under
// it: every process of a run whose files are in root, found by that and never
// by name, since other agents run the same programs on this machine. A
// zombie has no working directory to read and is not listed.
func procsUnder(root string) []int {
	real, err := filepath.EvalSymlinks(root)
	if err != nil {
		real = root
	}
	ents, _ := os.ReadDir("/proc")
	var out []int
	for _, e := range ents {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		cwd, err := os.Readlink("/proc/" + e.Name() + "/cwd")
		if err != nil {
			continue
		}
		if (cwd == real || strings.HasPrefix(cwd, real+"/")) && !procGone(pid) {
			out = append(out, pid)
		}
	}
	sort.Ints(out)
	return out
}

// A background loop that the job's script started ends with a host that is
// killed by SIGKILL, and so does the keeper itself: nothing of the run is
// left, found by its directory. The loop, the shell and the loop's child were
// all alive before the death.
func TestABackgroundChildOfAJobEndsWhenItsHostIsKilled(t *testing.T) {
	hostKilledLeavesNothing(t, "die-with-loop", "")
}

// The same with a loop whose command name holds a newline, which a keeper that
// reads one line of /proc/<pid>/stat does not recognise as the job's: such a
// process is an ordinary member of the session and must end with it.
func TestABackgroundChildNamedWithANewlineEndsWhenItsHostIsKilled(t *testing.T) {
	hostKilledLeavesNothing(t, "die-with-named-loop", newlineName+"\n")
}

// hostKilledLeavesNothing runs the helper host, kills it by SIGKILL once its
// job's shell and loop run, and waits for nothing of the run to be left. A
// non-empty comm is the kernel's name for the loop (with its newline), which
// it checks before the death.
func hostKilledLeavesNothing(t *testing.T, mode, comm string) {
	t.Helper()
	root := t.TempDir()
	ready := filepath.Join(root, "ready")
	cmd := exec.Command(os.Args[0], root, ready)
	cmd.Env = append(os.Environ(), helperEnv+"="+mode)
	var log strings.Builder
	cmd.Stdout, cmd.Stderr = &log, &log
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	reaped := make(chan struct{})
	go func() { exited <- cmd.Wait(); close(reaped) }()
	t.Cleanup(func() { // a leak found below must not outlive this test either
		_ = cmd.Process.Kill() // the host itself, if it stalled before its job ran
		<-reaped
		for _, pid := range procsUnder(root) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})
	waitFor(t, "the helper host to have a job with a background loop running", func() bool {
		select {
		case err := <-exited:
			t.Fatalf("the helper host ended before its job ran: %v\n%s", err, log.String())
		default:
		}
		b, err := os.ReadFile(ready)
		return err == nil && len(b) > 0
	})
	var shell, loop int
	b, _ := os.ReadFile(ready)
	if n, err := fmt.Sscanf(string(b), "%d %d", &shell, &loop); err != nil || n != 2 {
		t.Fatalf("ready file %q, want the shell's and the loop's pid", b)
	}
	// The loop is a background child of the batch shell, in its session: the
	// case the parent-death signal does not reach.
	if shell == loop || statField(t, loop, 4) != shell || statField(t, loop, 6) != shell {
		t.Fatalf("loop %d is not a background child of the batch shell %d in its session", loop, shell)
	}
	if comm != "" {
		if got, err := os.ReadFile("/proc/" + strconv.Itoa(loop) + "/comm"); err != nil || string(got) != comm {
			t.Fatalf("the loop %d is named %q (%v), want %q: the test would not cover a newline in the name", loop, got, err, comm)
		}
	}
	// Both were alive, and the scan by directory finds them: a scan that found
	// nothing would also report "none left" after the death.
	before := procsUnder(root)
	for _, pid := range []int{shell, loop} {
		if procGone(pid) {
			t.Fatalf("process %d was gone before the host died; the test would pass for the wrong reason", pid)
		}
		if i := sort.SearchInts(before, pid); i == len(before) || before[i] != pid {
			t.Fatalf("process %d is alive but not found under %s (found %v); the scan by directory sees nothing", pid, root, before)
		}
	}
	if err := os.WriteFile(ready+".go", nil, 0o644); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-exited:
		var ee *exec.ExitError
		if !errors.As(err, &ee) {
			t.Fatalf("the helper host: %v, want death by a signal\n%s", err, log.String())
		}
		if ws, ok := ee.Sys().(syscall.WaitStatus); !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
			t.Fatalf("the helper host ended with %v, want death by SIGKILL\n%s", ee, log.String())
		}
	case <-time.After(waitDeadline):
		t.Fatalf("the helper host did not die")
	}
	// The kernel ends the shell at once; the keeper ends the rest within a few
	// passes of its scan. The deadline is only generous.
	deadline := time.Now().Add(20 * time.Second)
	for {
		left := procsUnder(root)
		if len(left) == 0 {
			break
		}
		if time.Now().After(deadline) {
			var what []string
			for _, pid := range left {
				what = append(what, fmt.Sprintf("%d (%s)", pid, procState(pid)))
			}
			t.Fatalf("after the host was killed, processes of the run are left under %s: %s; the loop was %d, the shell %d", root, strings.Join(what, ", "), loop, shell)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A job that ends in order, by itself or by Close, leaves no keeper: it is told
// to stop and reaped before the execution is done, so Close returns with no
// process of the run left. A keeper that was not stopped would wait on its
// pipe for as long as the host lives.
func TestAJobsKeeperEndsWithTheJob(t *testing.T) {
	r := newRunRig(t, RunMode{})
	id := r.submit("exit 0")
	r.waitFinished(id)
	if left := procsUnder(r.root); len(left) != 0 {
		t.Fatalf("job %d is finished, and processes of the run are left under %s: %v", id, r.root, left)
	}

	// A job that is still running when Close ends it.
	release := filepath.Join(r.home, "release")
	id = r.submit(waitLoop(release) + " & " + waitLoop(release) + "; wait")
	waitFor(t, "job "+itoa(id)+" to run", func() bool { return r.job(id).PID > 0 && len(procsUnder(r.root)) >= 3 })
	if err := r.p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if left := procsUnder(r.root); len(left) != 0 {
		t.Fatalf("Close returned, and processes of the run are left under %s: %v", r.root, left)
	}
}

// reapSeen is what the beforeReap hook saw of one job: the state letters of
// /proc/<pid>/stat for its keeper and its batch shell, "" for a PID that names
// nothing (a keeper that was stopped is reaped, so it names nothing).
type reapSeen struct {
	keeper, shell string
}

// watchReaps sets the hook of the moment before a batch shell is reaped. It
// records the keeper's and the shell's state there, and the run, so a test can
// wait for the run to settle. The keeper's session-ID kills stay safe only if
// it is already gone at that moment: the reap frees the shell's PID, which is
// the session ID the keeper kills by.
func watchReaps(r *runRig) (seen func() []reapSeen, run func() *jobRun) {
	var mu sync.Mutex
	var got []reapSeen
	var jr *jobRun
	r.p.knobs.beforeReap = func(x *jobRun) {
		s := reapSeen{keeper: procState(x.keeper.cmd.Process.Pid), shell: procState(x.pid)}
		mu.Lock()
		got, jr = append(got, s), x
		mu.Unlock()
	}
	return func() []reapSeen { mu.Lock(); defer mu.Unlock(); return append([]reapSeen(nil), got...) },
		func() *jobRun { mu.Lock(); defer mu.Unlock(); return jr }
}

// A job that ends in order has its keeper stopped, and reaped, before its
// batch shell is reaped. While the shell is a zombie it holds its PID, the
// session ID; after the reap the ID is free for an unrelated session, and a
// keeper that still ran then would kill that session if the host died at that
// moment. The test does not reuse an ID: it looks at the moment before the
// reap, where the keeper must already be gone and the shell must still be there.
func TestAJobsKeeperIsStoppedBeforeItsShellIsReaped(t *testing.T) {
	r := newRunRig(t, RunMode{})
	seen, _ := watchReaps(r)
	id := r.submit("exit 0")
	r.waitFinished(id)
	got := seen()
	if len(got) != 1 {
		t.Fatalf("the shell was reaped %d times, want 1: %v", len(got), got)
	}
	if got[0].keeper != "" || got[0].shell != "Z" {
		t.Fatalf("just before the shell was reaped, its keeper was in state %q (want %q: stopped and reaped) and the shell in state %q (want %q: a zombie, still holding its session ID)",
			got[0].keeper, "", got[0].shell, "Z")
	}
}

// The same on the path of a job whose exit status cannot be read (waitFailed):
// the shell is killed, the keeper stopped, and only then is the shell reaped.
// The real waitid does not fail on a child, so the test makes the read of the
// status fail through the exitStatus seam. Close reports that error and ends
// the keeper all the same.
func TestAFailedWaitStopsTheKeeperBeforeItReapsTheShell(t *testing.T) {
	mode := RunMode{}
	r := newRunRigCfg(t, Config{Run: &mode, NotListed: NotListedAthena}, false)
	proc := *r.p.knobs.proc
	proc.exitStatus = func(int) (syscall.WaitStatus, error) { return 0, errors.New("injected: status unreadable") }
	r.p.knobs.proc = &proc
	seen, run := watchReaps(r)
	r.submit("exit 0")
	var jr *jobRun
	waitFor(t, "the failed wait to reach the reap", func() bool { jr = run(); return jr != nil })
	select {
	case <-jr.settled:
	case <-time.After(waitDeadline):
		t.Fatalf("the run did not settle")
	}
	got := seen()
	if len(got) != 1 {
		t.Fatalf("the shell was reaped %d times, want 1: %v", len(got), got)
	}
	if got[0].keeper != "" || got[0].shell == "" {
		t.Fatalf("just before the shell was reaped, its keeper was in state %q (want %q: stopped and reaped) and the shell in state %q (want a process that is not yet reaped)",
			got[0].keeper, "", got[0].shell)
	}
	if err := r.p.Close(); err == nil || !strings.Contains(err.Error(), "injected: status unreadable") {
		t.Fatalf("Close: %v, want the injected error", err)
	}
	if left := procsUnder(r.root); len(left) != 0 {
		t.Fatalf("processes of the run are left under %s: %v", r.root, left)
	}
}
