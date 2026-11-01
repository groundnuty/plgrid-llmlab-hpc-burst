package livechecks

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The reaper. Every run of hack/live-checks.sh in these tests goes through a helper process, the
// reaper: this test binary started again, with reaperEnv set, which TestMain turns into the
// helper below. The reaper makes itself a child subreaper (PR_SET_CHILD_SUBREAPER), runs the
// script as its child, and relays the script's exit status. Every process the script leaves
// behind, in any process group or session, ends up as a child of the reaper when its parent
// exits, so the reaper sees them all (a group probe sees only the script's own group, and
// misses a `setsid` child and anything run under `timeout`, which makes a group of its own).
// After the script exits the reaper reaps its descendants itself, so it does not depend on init
// to collect zombies, and gives the live ones a grace to exit. What is still alive then is
// killed and written to the report. The report is the only channel from the helper: the helper
// never writes to its own standard output or error, because the tests assert on the script's
// output exactly.
//
// The script sends no signal to its process group (it only signals the pids it started), so the
// reaper, which shares the script's group, is never reached by a signal meant for the script.
const (
	reaperEnv      = "HB_LC_REAPER"       // "1" in the helper's environment; stripped from the script's
	reaperTouchEnv = "HB_LC_REAPER_TOUCH" // a test hook, see reapAll; stripped from the script's
)

// TestMain turns the test binary into the reaper when it is started as one.
func TestMain(m *testing.M) {
	if os.Getenv(reaperEnv) == "1" {
		os.Exit(reaperMain(os.Args[1:]))
	}
	os.Exit(m.Run())
}

// procInfo is what the reaper reads about a process from /proc.
type procInfo struct {
	ppid int
	comm string
}

// reaperCommand builds the command that runs bash with argv under the reaper. The report is the
// file the reaper writes when it is done; grace is how long a descendant may outlive the script.
// touch, when not empty, is a file the reaper creates right after the first probe that finds a
// descendant alive (see reapAll). The caller sets Stdout and Stderr. bash is looked up here, in
// the test's PATH, as exec.Command("bash") did, so a script run with a limited PATH still starts.
func reaperCommand(t testing.TB, report string, grace time.Duration, touch, dir string, env []string, argv ...string) *exec.Cmd {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	bash, err := exec.LookPath("bash")
	if err != nil {
		bash = "bash"
	}
	cmd := exec.Command(self, append([]string{report, grace.String(), bash}, argv...)...)
	cmd.Dir = dir
	cmd.Env = append(append([]string{}, env...), reaperEnv+"=1")
	if touch != "" {
		cmd.Env = append(cmd.Env, reaperTouchEnv+"="+touch)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return cmd
}

// readReport reads the reaper's report: its first line (clean, leftover or error) and the lines
// after it. A missing report is an error, never "clean": the reaper may have died before it
// wrote one.
func readReport(path string) (kind string, details []string, err error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", nil, fmt.Errorf("the reaper never finished (no report): %w", err)
	}
	ls := lines(string(b))
	if len(ls) == 0 {
		return "", nil, errors.New("the reaper's report is empty")
	}
	return ls[0], ls[1:], nil
}

// reaperMain is the helper. args: report path, grace (a Go duration), bash, then bash's arguments.
func reaperMain(args []string) (code int) {
	if len(args) < 3 {
		return 125 // no report path to write to
	}
	report := args[0]
	fail := func(msg string) int {
		if err := writeReport(report, "error\n"+msg+"\n"); err != nil {
			return 126
		}
		return 125
	}
	defer func() {
		if r := recover(); r != nil {
			code = fail(fmt.Sprintf("the reaper panicked: %v", r))
		}
	}()
	grace, err := time.ParseDuration(args[1])
	if err != nil {
		return fail("the grace is not a duration: " + err.Error())
	}
	if err := setSubreaper(); err != nil {
		return fail("the reaper cannot become a subreaper: " + err.Error())
	}
	// Signals are caught before the script starts, so one that arrives early is not lost.
	sigs := make(chan os.Signal, 8)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	script := exec.Command(args[2], args[3:]...)
	script.Env = withoutReaperEnv(os.Environ())
	script.Stdin, script.Stdout, script.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := script.Start(); err != nil {
		return fail("the script cannot start: " + err.Error())
	}
	finished := make(chan struct{})
	go func() {
		for {
			select {
			case s := <-sigs:
				_ = script.Process.Signal(s)
			case <-finished:
				return
			}
		}
	}()
	code = exitStatus(script.Wait())
	close(finished)

	res := reapAll(os.Getpid(), grace, os.Getenv(reaperTouchEnv))
	if err := writeReport(report, res.String()); err != nil {
		return 126
	}
	return code
}

// withoutReaperEnv drops the reaper's own variables, so the script sees the environment it
// would have seen without a reaper.
func withoutReaperEnv(env []string) []string {
	var out []string
	for _, e := range env {
		if !strings.HasPrefix(e, reaperEnv) {
			out = append(out, e)
		}
	}
	return out
}

// exitStatus is the script's exit code, or 128 plus the signal that killed it.
func exitStatus(err error) int {
	var ee *exec.ExitError
	switch {
	case err == nil:
		return 0
	case errors.As(err, &ee):
		if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			return 128 + int(ws.Signal())
		}
		return ee.ExitCode()
	}
	return 127
}

func writeReport(path, body string) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(body), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// reapResult is what reapAll found.
type reapResult struct {
	leftover []string // the names of the descendants still alive after the grace
	problems []string
}

// String is the report: "clean", or "leftover" or "error" and the details, one per line.
func (r reapResult) String() string {
	switch {
	case len(r.leftover) == 0 && len(r.problems) == 0:
		return "clean\n"
	case len(r.leftover) > 0:
		return "leftover\n" + strings.Join(r.leftover, "\n") + "\n" + strings.Join(r.problems, "\n") + "\n"
	}
	return "error\n" + strings.Join(r.problems, "\n") + "\n"
}

const (
	reapedOne = iota
	stillAlive
	noChildren
)

// reapOne collects one exited child without waiting. stillAlive means there are children and
// none has exited.
func reapOne() (int, error) {
	for {
		var ws syscall.WaitStatus
		pid, err := syscall.Wait4(-1, &ws, syscall.WNOHANG, nil)
		switch {
		case errors.Is(err, syscall.EINTR):
			continue
		case errors.Is(err, syscall.ECHILD):
			return noChildren, nil
		case err != nil:
			return 0, err
		case pid == 0:
			return stillAlive, nil
		}
		return reapedOne, nil
	}
}

// reapAll reaps the descendants of process self (this process is their subreaper) until none is
// left, and returns clean if they all exited within grace. Those still alive at the grace are
// named, then killed and reaped. It waits on the children's state, never on a fixed window: with
// no child left it returns at once. touch, when not empty, is created right after the first probe
// that finds a descendant alive, on this goroutine and before the next probe: a test uses it to
// let a descendant exit only after it has been seen alive, so a detector that probes only once
// reports it, whatever the scheduler does.
func reapAll(self int, grace time.Duration, touch string) (res reapResult) {
	end := time.Now().Add(grace)
	seen := false
	for {
		st, err := reapOne()
		if err != nil {
			res.problems = append(res.problems, "wait4: "+err.Error())
			return res
		}
		switch st {
		case reapedOne:
			continue
		case noChildren:
			return res
		}
		if !seen {
			seen = true
			if touch != "" {
				if err := os.WriteFile(touch, nil, 0o600); err != nil {
					res.problems = append(res.problems, "the touch file cannot be written: "+err.Error())
					return res
				}
			}
		}
		if time.Now().After(end) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Descendants are still alive after the grace: name them, kill them, reap them. A killed
	// process's children become the reaper's children, so the pass repeats until none is left.
	killEnd := time.Now().Add(5 * time.Second)
	named := false
	for time.Now().Before(killEnd) {
		st, err := reapOne()
		if err != nil {
			res.problems = append(res.problems, "wait4: "+err.Error())
			return res
		}
		if st == reapedOne {
			continue
		}
		if st == noChildren {
			return res
		}
		table, err := readProcTable()
		if err != nil {
			res.problems = append(res.problems, "the process table cannot be read: "+err.Error())
		}
		pids := descendantsOf(self, table)
		if !named {
			named = true
			for _, p := range pids {
				res.leftover = append(res.leftover, table[p].comm)
			}
			if len(res.leftover) == 0 {
				res.leftover = []string{"(a process that could not be named)"}
			}
		}
		for _, p := range pids {
			_ = syscall.Kill(p, syscall.SIGKILL)
		}
		time.Sleep(5 * time.Millisecond)
	}
	res.problems = append(res.problems, "a descendant could not be killed and reaped")
	return res
}

// descendantsOf lists the processes below root in table, in pid order.
func descendantsOf(root int, table map[int]procInfo) []int {
	kids := map[int][]int{}
	for pid, p := range table {
		kids[p.ppid] = append(kids[p.ppid], pid)
	}
	var out []int
	for frontier := []int{root}; len(frontier) > 0; {
		var next []int
		for _, f := range frontier {
			next = append(next, kids[f]...)
		}
		out = append(out, next...)
		frontier = next
	}
	sort.Ints(out)
	return out
}
