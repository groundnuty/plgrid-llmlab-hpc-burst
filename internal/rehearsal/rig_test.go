package rehearsal

// New. The test rig: a run-mode fake Slurm platform, a login node over a real
// sshd, and the controller's own transport (internal/slurm/sshrun) in front of
// it, with a check at the end that no process of the test is left.

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/sshrun"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/fake"
)

func TestMain(m *testing.M) {
	Dispatch()
	os.Exit(m.Run())
}

// waitDeadline bounds every wait: generous, since the VM may be loaded.
const waitDeadline = 60 * time.Second

// clusterUser is the fake cluster's user; the OS account of the test is
// another thing, the one sshd logs in.
const clusterUser = "example-user"

type rig struct {
	t     *testing.T
	base  string // a short directory of this test
	root  string // the run-mode root
	home  string // the cluster user's HOME, under root
	plat  *fakeslurm.Platform
	node  *LoginNode
	ctl   string // the ssh master's control directory
	creds slurm.SSHConfig
	watch *watch
}

// newRig builds the rig and registers its cleanup. The cleanups run in the
// reverse order: the transport closes first, then the login node, then the
// platform (which fails the test if a job's process survives), and last the
// check that nothing of the test is left.
func newRig(t *testing.T) (*rig, *sshrun.SSH) {
	t.Helper()
	RequireTools(t)
	base, err := os.MkdirTemp("", "rl")
	if err != nil {
		t.Fatal(err)
	}
	r := &rig{t: t, base: base, root: filepath.Join(base, "r"), ctl: filepath.Join(base, "c")}
	r.home = filepath.Join(r.root, "home", clusterUser)
	for _, d := range []string{r.home, r.ctl, filepath.Join(base, "n")} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	w := startWatch(t, base)
	r.watch = w
	t.Cleanup(func() {
		w.finish()
		_ = os.RemoveAll(base) // this test's own directory
	})
	r.plat = fakeslurm.New(fake.NewClock(time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)), fakeslurm.Config{Run: &fakeslurm.RunMode{Root: r.root}})
	t.Cleanup(func() {
		if err := r.plat.Close(); err != nil {
			t.Errorf("Platform.Close: %v", err)
		}
	})
	r.node, err = StartLoginNode(LoginConfig{
		Dir: filepath.Join(base, "n"), Platform: r.plat,
		Login: fakeslurm.Login{User: clusterUser, Env: map[string]string{"HOME": r.home}},
	})
	if err != nil {
		t.Fatalf("StartLoginNode: %v", err)
	}
	t.Cleanup(func() {
		if err := r.node.Close(); err != nil {
			t.Errorf("LoginNode.Close: %v", err)
		}
	})
	t.Cleanup(func() { // a diagnostic for a failed test; no test asserts on it
		if b, err := os.ReadFile(r.node.files.log); t.Failed() && err == nil {
			t.Logf("sshd's log:\n%s", b)
		}
	})
	r.creds = credentials(t, r.node, base)
	return r, r.transport()
}

// transport is a new controller transport to the login node. Two of them are
// two controller runs against one cluster.
func (r *rig) transport() *sshrun.SSH {
	r.t.Helper()
	s, err := sshrun.New(sshrun.Options{Name: "rehearsal", Config: r.creds, ControlDir: r.ctl})
	if err != nil {
		r.t.Fatalf("sshrun.New: %v", err)
	}
	r.t.Cleanup(func() { _ = s.Close() })
	return s
}

// credentials is what the controller reads: the credentials file the node
// writes, parsed by slurm.ParseSSHConfig.
func credentials(t *testing.T, n *LoginNode, dir string) slurm.SSHConfig {
	t.Helper()
	path := filepath.Join(dir, "creds")
	if err := n.WriteCredentials(path); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	c, err := slurm.ParseSSHConfig(b, "")
	if err != nil {
		t.Fatalf("ParseSSHConfig refused the login node's credentials file: %v", err)
	}
	return c
}

// run runs one line through the transport and fails the test on a runner
// error.
func run(t *testing.T, s *sshrun.SSH, line string, stdin []byte) slurm.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitDeadline)
	defer cancel()
	res, err := s.Run(ctx, slurm.Command{Line: line, Stdin: stdin})
	if err != nil {
		t.Fatalf("%q: runner error: %v", line, err)
	}
	return res
}

// must is run with exit 0 required; it returns standard output.
func must(t *testing.T, s *sshrun.SSH, line string) string {
	t.Helper()
	res := run(t, s, line, nil)
	if res.ExitCode != 0 {
		t.Fatalf("%q: exit %d, stderr %q", line, res.ExitCode, res.Stderr)
	}
	return string(res.Stdout)
}

func atoi(t *testing.T, s string) int {
	t.Helper()
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		t.Fatalf("%q is not a number", s)
	}
	return n
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

// ---- the end-of-test check ----

// watch records every process below the test process while the test runs, and
// at the end checks two independent ways that none is left: the recorded ones
// by PID and start time, and any process whose command line or environment
// names the test's directory (sshd, the ssh master, the relay client, a job).
type watch struct {
	t    *testing.T
	base string
	stop chan struct{}
	done chan struct{}
	// wantSSH is true for a test that runs a command: the watcher must then
	// have seen sshd and ssh.
	wantSSH bool

	mu   sync.Mutex
	seen map[int]seenProc
}

type seenProc struct {
	row  procRow
	comm string
}

func startWatch(t *testing.T, base string) *watch {
	w := &watch{t: t, base: base, wantSSH: true, stop: make(chan struct{}), done: make(chan struct{}), seen: map[int]seenProc{}}
	go func() {
		defer close(w.done)
		for {
			w.sample()
			select {
			case <-w.stop:
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}()
	return w
}

func (w *watch) sample() {
	rows := below(os.Getpid())
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, r := range rows {
		if _, ok := w.seen[r.pid]; !ok {
			b, _ := os.ReadFile("/proc/" + strconv.Itoa(r.pid) + "/comm")
			w.seen[r.pid] = seenProc{row: r, comm: strings.TrimSpace(string(b))}
		}
	}
}

// finish runs after every other cleanup of the test.
func (w *watch) finish() {
	close(w.stop)
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	comms := map[string]bool{}
	for _, p := range w.seen {
		comms[p.comm] = true
	}
	// The watcher must have seen the processes it is there to find; a watcher
	// that saw nothing would pass for any outcome.
	if w.wantSSH && (!comms["sshd"] || !comms["ssh"]) {
		w.t.Errorf("the process watcher saw %v: it never saw sshd and ssh, so its \"nothing left\" proves nothing", keys(comms))
	}
	deadline := time.Now().Add(waitDeadline)
	for {
		left := w.survivors()
		if len(left) == 0 {
			return
		}
		if time.Now().After(deadline) {
			w.t.Errorf("processes left after the test: %s", strings.Join(left, ", "))
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// survivors lists the recorded processes still alive and the live processes
// that name the test's directory.
func (w *watch) survivors() []string {
	var left []string
	for pid, p := range w.seen {
		if anyLive([]procRow{p.row}) {
			left = append(left, strconv.Itoa(pid)+" "+p.comm)
		}
	}
	for _, r := range liveProcs() {
		if r.pid == os.Getpid() {
			continue
		}
		if namesDir(r.pid, w.base) {
			left = append(left, strconv.Itoa(r.pid)+" (names "+filepath.Base(w.base)+")")
		}
	}
	return left
}

func namesDir(pid int, dir string) bool {
	for _, f := range []string{"cmdline", "environ"} {
		if b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/" + f); err == nil && strings.Contains(string(b), dir) {
			return true
		}
	}
	return false
}

func keys(m map[string]bool) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}
