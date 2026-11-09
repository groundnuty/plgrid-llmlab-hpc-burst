package fakeslurm

// New. Issue #274: a host that dies without running its cleanups (SIGKILL,
// SIGTERM from timeout(1), a -test.timeout panic, os.Exit) must not leave its
// jobs' processes running. The verification that proves nothing is a test that
// ends normally and then looks for processes: Close ends them without any
// parent-death signal, so that passes on the code without the fix. The host
// here is a helper process (TestMain), killed the way a builder's run was.

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// foreverLoop is the shell loop of the jobs and steps here: it never ends on
// its own, so a process found alive later is a leak and not a slow exit.
const foreverLoop = "while :; do sleep 0.05; done"

// dieWithJobs is the helper host: a run-mode platform with a batch job and a
// step running in it, then a death of the kind how names, with no Close and no
// cleanup of any kind. It writes "<shell pid> <step pid>" to readyFile once
// both run, and dies when the file readyFile.go exists: the test looks at both
// processes first, so that they were alive before the death is known, not hoped.
func dieWithJobs(root, readyFile, how string) int {
	home, scratch := filepath.Join(root, "home", "example-user"), filepath.Join(root, "scratch", "example-user")
	for _, d := range []string{home, scratch} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return 2
		}
	}
	p := New(fake.NewClock(t0), Config{Run: &RunMode{Root: root}})
	s := p.Login(Login{User: "example-user", Env: map[string]string{"HOME": home, "SCRATCH": scratch}})
	res, err := s.Run(context.Background(), slurm.Command{Line: "sbatch --parsable '--wrap=" + foreverLoop + "'"})
	if err != nil || res.ExitCode != 0 {
		return 2
	}
	id, err := strconv.Atoi(strings.TrimSpace(string(res.Stdout)))
	if err != nil {
		return 2
	}
	deadline := time.Now().Add(waitDeadline)
	for {
		if j, ok := p.Job(id); ok && j.PID > 0 {
			break
		}
		if time.Now().After(deadline) {
			return 3
		}
		time.Sleep(5 * time.Millisecond)
	}
	stepPids := filepath.Join(root, "step-pid")
	go func() { // the step never returns: the host dies under it
		_, _ = s.Run(context.Background(), slurm.Command{Line: srunLine(id), Stdin: []byte("echo $$ > " + stepPids + "\n" + foreverLoop + "\n")})
	}()
	var step string
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(stepPids); err == nil && len(b) > 0 {
			step = strings.TrimSpace(string(b))
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	j, _ := p.Job(id)
	if step == "" || os.WriteFile(readyFile+".tmp", []byte(fmt.Sprintf("%d %s\n", j.PID, step)), 0o644) != nil || os.Rename(readyFile+".tmp", readyFile) != nil {
		return 3
	}
	goFile := readyFile + ".go" // the test looks at both processes, then lets the host die
	for {
		if _, err := os.Stat(goFile); err == nil {
			break
		}
		if time.Now().After(deadline) {
			return 3
		}
		time.Sleep(5 * time.Millisecond)
	}
	switch how {
	case "kill":
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
	case "term":
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
	case "panic": // what the -test.timeout alarm does: a panic on a goroutine, which runs no cleanup
		go func() { panic("the host dies") }()
	case "exit":
		os.Exit(3)
	}
	time.Sleep(time.Minute) // not reached: the host is gone
	return 0
}

// ownedBy reports whether the process is one this test's helper started: its
// working directory is under root (the batch shell and the step both start in
// the login's HOME, which is under it).
func ownedBy(pid int, root string) bool {
	cwd, err := os.Readlink("/proc/" + strconv.Itoa(pid) + "/cwd")
	return err == nil && strings.HasPrefix(cwd, root+"/")
}

// A job's batch shell and a step's program end with the process that hosts the
// fake, however it dies: the kernel ends them, since no Close, no cleanup and
// no sweep runs after SIGKILL. Both were alive before the death.
func TestProcessesEndWhenTheirHostDiesWithoutClose(t *testing.T) {
	for _, how := range []string{"kill", "term", "panic", "exit"} {
		t.Run(how, func(t *testing.T) {
			root := t.TempDir()
			ready := filepath.Join(root, "ready")
			cmd := exec.Command(os.Args[0], root, ready, how)
			cmd.Env = append(os.Environ(), helperEnv+"=die-with-jobs")
			var log strings.Builder
			cmd.Stdout, cmd.Stderr = &log, &log
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			exited := make(chan error, 1)
			reaped := make(chan struct{})
			go func() { exited <- cmd.Wait(); close(reaped) }()
			var pids []int
			t.Cleanup(func() { // a leak found below must not outlive this test either
				_ = cmd.Process.Kill() // the host itself, if it stalled before its job and step ran
				<-reaped
				for _, pid := range pids {
					if !procGone(pid) && ownedBy(pid, root) {
						_ = syscall.Kill(pid, syscall.SIGKILL)
					}
				}
			})
			waitFor(t, "the helper host to have a job and a step running", func() bool {
				select {
				case err := <-exited:
					t.Fatalf("the helper host ended before its job and step ran: %v\n%s", err, log.String())
				default:
				}
				b, err := os.ReadFile(ready)
				return err == nil && len(b) > 0
			})
			b, _ := os.ReadFile(ready)
			for _, f := range strings.Fields(string(b)) {
				n, err := strconv.Atoi(f)
				if err != nil {
					t.Fatalf("ready file %q: %v", b, err)
				}
				pids = append(pids, n)
			}
			if len(pids) != 2 {
				t.Fatalf("ready file %q, want the shell's and the step's pid", b)
			}
			for _, pid := range pids {
				if procGone(pid) {
					t.Fatalf("process %d was gone before the host died; the test would pass for the wrong reason", pid)
				}
			}
			if err := os.WriteFile(ready+".go", nil, 0o644); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-exited:
				if err == nil {
					t.Fatalf("the helper host exited 0 (%s)", how)
				}
			case <-time.After(waitDeadline):
				t.Fatalf("the helper host did not die (%s)", how)
			}
			// The kernel ends them when the host's last thread does: milliseconds. The deadline is only generous.
			deadline := time.Now().Add(20 * time.Second)
			for !(procGone(pids[0]) && procGone(pids[1])) {
				if time.Now().After(deadline) {
					t.Fatalf("after the host died (%s): shell %d is %s, step program %d is %s", how, pids[0], procState(pids[0]), pids[1], procState(pids[1]))
				}
				time.Sleep(5 * time.Millisecond)
			}
		})
	}
}

// The parent-death signal fires when the thread that created the process
// exits, so a job must not be created on the caller's thread: a goroutine that
// locks its thread and returns, with an sbatch among its calls, ends that
// thread. The job lives on, and answers: a ping file is met with a pong, which
// a killed shell never writes.
func TestAJobSurvivesTheThreadThatSubmittedIt(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("threads are read from /proc")
	}
	r := newRunRig(t, RunMode{})
	ping, pong := filepath.Join(r.root, "ping"), filepath.Join(r.root, "pong")
	answer := "while :; do if [ -e " + ping + " ]; then : > " + pong + "; rm -f " + ping + "; fi; sleep 0.02; done"
	type sent struct {
		tid  int
		main bool // the goroutine ran on the process's first thread
		res  slurm.Result
		err  error
	}
	got := make(chan sent, 1)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	var s sent
	for { // until a goroutine runs on a thread that can end
		go func() {
			runtime.LockOSThread() // never unlocked, but for the first thread: this thread ends when the goroutine returns
			tid := syscall.Gettid()
			if tid == os.Getpid() {
				// The runtime never ends the process's first thread (it parks it), so a goroutine that returns
				// from it kills no thread. Hold this one here, still on it, so the next goroutine runs elsewhere.
				got <- sent{tid: tid, main: true}
				<-release
				runtime.UnlockOSThread()
				return
			}
			res, err := r.s.Run(context.Background(), slurm.Command{Line: "sbatch --parsable '--wrap=" + answer + "'"})
			got <- sent{tid: tid, res: res, err: err}
		}()
		if s = <-got; !s.main {
			break
		}
		t.Log("a goroutine ran on the process's first thread; trying another")
	}
	if s.err != nil || s.res.ExitCode != 0 {
		t.Fatalf("sbatch: %v exit %d %s", s.err, s.res.ExitCode, s.res.Stderr)
	}
	id, err := strconv.Atoi(strings.TrimSpace(string(s.res.Stdout)))
	if err != nil {
		t.Fatalf("sbatch printed %q", s.res.Stdout)
	}
	waitFor(t, "the submitting goroutine's thread to end", func() bool {
		_, err := os.Stat("/proc/self/task/" + strconv.Itoa(s.tid))
		return err != nil
	})
	if err := os.WriteFile(ping, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the job to answer, or to end", func() bool {
		_, err := os.Stat(pong)
		return err == nil || r.job(id).State != Running
	})
	if _, err := os.Stat(pong); err != nil {
		t.Fatalf("job %d did not answer after the thread that submitted it ended; it is %s", id, r.job(id).State)
	}
}
