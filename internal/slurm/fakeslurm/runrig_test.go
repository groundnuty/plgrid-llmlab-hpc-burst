package fakeslurm

// New. The rig of the run-mode tests (run_test.go and its neighbours): a
// platform on a fresh directory, and the waits on observable state.

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

// waitDeadline bounds every wait: generous, since the VM may be loaded.
const waitDeadline = 60 * time.Second

// helperEnv makes the test binary a helper process instead of running tests:
// "setpgid-sleep <file>" moves itself into a process group of its own, as
// hb-agent starts its engine, writes "<pid> <pgid>" to the file and sleeps
// for at most two minutes.
const helperEnv = "FAKESLURM_TEST_HELPER"

func TestMain(m *testing.M) {
	switch os.Getenv(helperEnv) {
	case "setpgid-sleep":
		os.Exit(setpgidSleep(os.Args[1]))
	case "setpgid-term":
		os.Exit(setpgidTerm(os.Args[1], os.Args[2]))
	}
	os.Exit(m.Run())
}

func setpgidSleep(file string) int {
	if err := syscall.Setpgid(0, 0); err != nil {
		return 2
	}
	line := strconv.Itoa(os.Getpid()) + " " + strconv.Itoa(syscall.Getpgrp()) + "\n"
	if os.WriteFile(file+".tmp", []byte(line), 0o644) != nil || os.Rename(file+".tmp", file) != nil {
		return 2
	}
	time.Sleep(2 * time.Minute)
	return 0
}

type runRig struct {
	*rig
	root, home string
}

// newRunRig is a run-mode platform on a fresh directory, with example-user's
// HOME and SCRATCH under it. It is closed in the test's cleanup, which fails
// the test if a job's processes survive.
func newRunRig(t *testing.T, mode RunMode) *runRig {
	t.Helper()
	// The Athena shape lists an ended job in its end state until MinJobAge,
	// which the manual clock never reaches, so squeue shows the end state.
	return newRunRigCfg(t, Config{Run: &mode, NotListed: NotListedAthena}, true)
}

// newRunRigCfg is newRunRig for any Config, whose Run is set. With
// checkClose false the cleanup closes the platform without judging it, for a
// test that expects Close to fail and checks that itself.
func newRunRigCfg(t *testing.T, cfg Config, checkClose bool) *runRig {
	t.Helper()
	root := t.TempDir()
	mode := *cfg.Run
	mode.Root = root
	cfg.Run = &mode
	home, scratch := filepath.Join(root, "home", "example-user"), filepath.Join(root, "scratch", "example-user")
	for _, d := range []string{home, scratch} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	clock := fake.NewClock(t0)
	p := New(clock, cfg)
	t.Cleanup(func() {
		if err := p.Close(); err != nil && checkClose {
			t.Errorf("Close: %v", err)
		}
	})
	s := p.Login(Login{User: "example-user", Env: map[string]string{"HOME": home, "SCRATCH": scratch}})
	return &runRig{rig: &rig{t: t, clock: clock, p: p, s: s}, root: root, home: home}
}

// submit runs sbatch --parsable with the wrap as one quoted word and returns
// the job ID.
func (r *runRig) submit(wrap string, args ...string) int {
	r.t.Helper()
	if strings.Contains(wrap, "'") {
		r.t.Fatalf("a wrap cannot hold a single quote: %q", wrap)
	}
	line := strings.Join(append([]string{"sbatch", "--parsable"}, args...), " ") + " '--wrap=" + wrap + "'"
	out, errOut, code := r.run(line)
	if code != 0 {
		r.t.Fatalf("%q: exit %d, stderr %q", line, code, errOut)
	}
	id, err := strconv.Atoi(strings.TrimSpace(out))
	if err != nil {
		r.t.Fatalf("%q printed %q, want a job ID", line, out)
	}
	return id
}

func (r *runRig) job(id int) Job {
	r.t.Helper()
	j, ok := r.p.Job(id)
	if !ok {
		r.t.Fatalf("no job %d", id)
	}
	return j
}

// waitFor polls cond until it holds, failing the test at the deadline.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitDeadline)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", waitDeadline, what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitFinished waits until every process of the job is gone and returns it.
func (r *runRig) waitFinished(id int) Job {
	r.t.Helper()
	waitFor(r.t, "job "+itoa(id)+"'s processes to end", func() bool { return r.job(id).Finished })
	return r.job(id)
}

// waitFile waits until the file exists and returns its content.
func (r *runRig) waitFile(name string) string {
	r.t.Helper()
	var b []byte
	waitFor(r.t, name+" to appear", func() bool {
		var err error
		b, err = os.ReadFile(name)
		return err == nil && len(b) > 0
	})
	return string(b)
}

// groupGone asks the kernel whether any process is left in the process group:
// kill(-pgid, 0) fails with ESRCH when there is none.
func groupGone(pgid int) bool {
	return errors.Is(syscall.Kill(-pgid, 0), syscall.ESRCH)
}

// statField returns field n (1-based, as proc(5) numbers them) of
// /proc/<pid>/stat, for a field after the command name.
func statField(t *testing.T, pid, n int) int {
	t.Helper()
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		t.Fatalf("read /proc/%d/stat: %v", pid, err)
	}
	f := strings.Fields(string(b[strings.LastIndexByte(string(b), ')')+1:]))
	v, err := strconv.Atoi(f[n-3])
	if err != nil {
		t.Fatalf("/proc/%d/stat field %d: %v", pid, n, err)
	}
	return v
}

// waitLoop is a shell loop that waits until the file exists, for at most
// about 60 s, so a job that nobody releases still ends on its own.
func waitLoop(file string) string {
	return "i=0; while [ ! -e " + file + " ] && [ $i -lt 1200 ]; do sleep 0.05; i=$((i+1)); done"
}
