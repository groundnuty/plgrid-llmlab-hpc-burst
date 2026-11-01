package livechecks

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"sort"
	"strings"
	"sync"
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
// to collect zombies, and gives the live ones a grace to exit. What is still alive when the
// grace ends is named in the report first (the verdict is latched there), then killed. The
// report is the only channel from the helper: the helper never writes to its own standard output
// or error, because the tests assert on the script's output exactly.
//
// Three rules keep the reaper from harming anything else:
//   - It signals only its own unreaped direct children. A child it has not reaped holds its pid,
//     so the pid cannot have been reused, and the reaper is the only thing that reaps them.
//     A grandchild is never signalled by pid: its parent may reap it at any moment. When a
//     parent dies, its children become the reaper's, and the next pass kills them.
//   - It dies with its parent. The test binary holds the write end of a pipe whose read end is
//     the reaper's file descriptor 3; when the test binary exits for any reason (a timeout, a
//     panic, a kill) the pipe reaches end of file, and the reaper kills and reaps everything it
//     has collected before it exits. Nothing is left running by a test that did not finish.
//   - It acknowledges every signal it forwards (a file next to the report), so a test that
//     signals the script can wait until the script has the signal.
//
// The script sends no signal to its process group (it only signals the pids it started), so the
// reaper, which shares the script's group, is never reached by a signal meant for the script.
const (
	reaperEnv      = "HB_LC_REAPER"             // "1" in the helper's environment; stripped from the script's
	reaperTouchEnv = "HB_LC_REAPER_TOUCH"       // a test hook, see reapAll; stripped from the script's
	reaperLatchEnv = "HB_LC_REAPER_LATCH_TOUCH" // a test hook, see reapAll; stripped from the script's
	runnerEnv      = "HB_LC_RUNNER"             // "1" starts the runner instead of the tests, see runnerMain
)

// parentPipeFD is the reaper's end of the pipe that the test binary holds the other end of.
const parentPipeFD = 3

// TestMain turns the test binary into the reaper, or into the runner of a test of the reaper,
// when it is started as one.
func TestMain(m *testing.M) {
	switch {
	case os.Getenv(reaperEnv) == "1":
		os.Exit(reaperMain(os.Args[1:]))
	case os.Getenv(runnerEnv) == "1":
		os.Exit(runnerMain(os.Args[1:]))
	}
	os.Exit(m.Run())
}

// procInfo is what the reaper reads about a process from /proc.
type procInfo struct {
	ppid  int
	comm  string
	state byte // R, S, D, Z (a zombie), ...
}

// reaperOpts are the reaper's settings besides what the script is.
type reaperOpts struct {
	// grace is how long a descendant may outlive the script.
	grace time.Duration
	// touch, when not empty, is a file the reaper creates right after the first probe that
	// finds a descendant alive (see reapAll).
	touch string
	// latch, when not empty, is a file the reaper creates the moment the grace ends with a
	// descendant alive, after it has named it; it then waits until every child is a zombie.
	latch string
}

// buildReaperCmd builds the command that runs bash with argv under the reaper, and the pipe to
// it: release closes the test's end, which tells the reaper the test is over. The report is the
// file the reaper writes when it is done. bash is looked up here, in the caller's PATH, as
// exec.Command("bash") did, so a script run with a limited PATH still starts. The caller sets
// Stdout and Stderr.
func buildReaperCmd(report string, o reaperOpts, dir string, env []string, argv ...string) (cmd *exec.Cmd, release func(), err error) {
	self, err := os.Executable()
	if err != nil {
		return nil, nil, err
	}
	bash, err := exec.LookPath("bash")
	if err != nil {
		bash = "bash"
	}
	pr, pw, err := os.Pipe()
	if err != nil {
		return nil, nil, err
	}
	cmd = exec.Command(self, append([]string{report, o.grace.String(), bash}, argv...)...)
	cmd.Dir = dir
	cmd.Env = append(append([]string{}, env...), reaperEnv+"=1")
	if o.touch != "" {
		cmd.Env = append(cmd.Env, reaperTouchEnv+"="+o.touch)
	}
	if o.latch != "" {
		cmd.Env = append(cmd.Env, reaperLatchEnv+"="+o.latch)
	}
	cmd.ExtraFiles = []*os.File{pr} // file descriptor 3 in the reaper
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var once sync.Once
	return cmd, func() { once.Do(func() { _ = pw.Close(); _ = pr.Close() }) }, nil
}

// reaperCommand is buildReaperCmd for a test. When the test ends, however it ends, the pipe is
// closed, so the reaper kills and reaps what is left, and the test waits for the reaper to exit
// (a reaper that does not exit in time is killed). A reaper whose Wait has been called is gone
// by then and the wait returns at once.
func reaperCommand(t testing.TB, report string, o reaperOpts, dir string, env []string, argv ...string) *exec.Cmd {
	t.Helper()
	cmd, release, err := buildReaperCmd(report, o, dir, env, argv...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		release()
		if cmd.Process == nil {
			return
		}
		done := make(chan struct{})
		go func() { _, _ = cmd.Process.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	})
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

// ackPath is where the reaper says it has forwarded a signal.
func ackPath(report string) string { return report + ".ack" }

// reaperMain is the helper. args: report path, grace (a Go duration), bash, then bash's arguments.
func reaperMain(args []string) (code int) {
	if len(args) < 3 {
		return 125 // no report path to write to
	}
	report := args[0]
	self := os.Getpid()
	fail := func(msg string) int {
		if err := writeReport(report, "error\n"+msg+"\n"); err != nil {
			return 126
		}
		return 125
	}
	defer func() {
		if r := recover(); r != nil {
			killChildren(self, 5*time.Second) // a panic must not leave the script or its descendants
			code = fail(fmt.Sprintf("the reaper panicked: %v", r))
		}
	}()
	grace, err := time.ParseDuration(args[1])
	if err != nil {
		return fail("the grace is not a duration: " + err.Error())
	}
	testEnd := os.NewFile(parentPipeFD, "test")
	if _, err := testEnd.Stat(); err != nil {
		return fail("the pipe from the test is not on file descriptor 3: " + err.Error())
	}
	syscall.CloseOnExec(parentPipeFD) // the script and what it starts do not hold it
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
				_ = os.WriteFile(ackPath(report), []byte(s.String()+"\n"), 0o600)
			case <-finished:
				return
			}
		}
	}()
	// The test binary is gone when the pipe reaches end of file: stop the script, and let the
	// reaping below take everything else without a grace.
	died := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, testEnd)
		close(died)
		_ = script.Process.Kill()
	}()
	code = exitStatus(script.Wait())
	close(finished)

	res := reapAll(reapConfig{self: self, grace: grace, touch: os.Getenv(reaperTouchEnv), latch: os.Getenv(reaperLatchEnv), died: died})
	select {
	case <-died:
		res.problems = append(res.problems, "the test ended before the run did")
	default:
	}
	if err := writeReport(report, res.String()); err != nil {
		return 126
	}
	return code
}

// runnerMain is a second helper mode, for the test of a test binary that is killed (see
// TestAKilledTestBinaryLeavesNothing): it plays the test binary. args: report path, ready file,
// directory. It starts the reaper over a script that leaves a process in a new session, a
// process of its own session's group and a running foreground command, creates the ready file,
// and then waits to be killed.
func runnerMain(args []string) int {
	if len(args) != 3 {
		return 2
	}
	report, ready, dir := args[0], args[1], args[2]
	var env []string
	for _, e := range os.Environ() {
		if !strings.HasPrefix(e, runnerEnv) {
			env = append(env, e)
		}
	}
	env = append(env, "READY="+ready)
	script := `setsid sleep 600 >/dev/null 2>&1 </dev/null &
sleep 600 >/dev/null 2>&1 </dev/null &
: > "$READY"
sleep 600`
	cmd, release, err := buildReaperCmd(report, reaperOpts{grace: 5 * time.Second}, dir, env, "-c", script)
	if err != nil {
		return 3
	}
	if err := cmd.Start(); err != nil {
		return 4
	}
	// The pipe's write end must stay open for as long as this process lives: a *os.File that is
	// no longer referenced is closed by the garbage collector, and the reaper would take that
	// for the death of the test. It closes with this process, when the test kills it.
	runnerPipe = release
	for {
		time.Sleep(time.Hour) // not a bare select: the runtime would call that a deadlock
	}
}

// runnerPipe keeps the runner's pipe to the reaper reachable (see runnerMain).
var runnerPipe func()

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
	leftover []string // the names of the descendants still alive when the grace ended
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

// reapConfig is what reapAll needs.
type reapConfig struct {
	self         int
	grace        time.Duration
	touch, latch string
	died         <-chan struct{} // closed when the test binary is gone: no grace
}

// reapAll reaps the descendants of process self (this process is their subreaper) until none is
// left, and returns clean if they all exited within the grace.
//
// The verdict is latched where the grace ends: a child still alive at that moment is a leftover,
// named from a read of /proc (names only, nothing is signalled by it), and nothing the cleanup
// does afterwards can take that back, not a child that exits a moment later nor a failure to
// kill. Then the children are killed and reaped.
//
// It waits on the children's state, never on a fixed window: with no child left it returns at
// once. The two hooks are for tests. touch is created right after the first probe that finds a
// descendant alive, on this goroutine and before the next probe, so a descendant that exits on
// that file is certainly alive at the first probe and certainly gone only after it: a detector
// that gives a verdict at its first probe reports it, whatever the scheduler does. latch is
// created right after the verdict is latched, and reapAll then waits until every child is a
// zombie, so the children are certainly all gone by the first probe of the cleanup, which would
// find nothing to kill and read the run as clean if the verdict were not already latched.
func reapAll(c reapConfig) (res reapResult) {
	end := time.Now().Add(c.grace)
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
			if c.touch != "" {
				if err := os.WriteFile(c.touch, nil, 0o600); err != nil {
					res.problems = append(res.problems, "the touch file cannot be written: "+err.Error())
					return res
				}
			}
		}
		expired := time.Now().After(end)
		select {
		case <-c.died:
			expired = true
		default:
		}
		if expired {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	// A child is alive at the end of the grace.
	res.leftover = []string{"(a process that could not be named)"}
	if table, err := readProcTable(); err == nil {
		var names []string
		for _, p := range descendantsOf(c.self, table) {
			names = append(names, table[p].comm)
		}
		if len(names) > 0 {
			res.leftover = names
		}
	}
	if c.latch != "" {
		if err := os.WriteFile(c.latch, nil, 0o600); err != nil {
			res.problems = append(res.problems, "the latch file cannot be written: "+err.Error())
		} else {
			awaitZombies(c.self)
		}
	}
	if left := killChildren(c.self, 5*time.Second); left != "" {
		res.problems = append(res.problems, left)
	}
	return res
}

// killChildren kills and reaps the children of process self until none is left or within has
// passed, and returns a problem text if some is. It signals only unreaped direct children
// (directChildrenOf): see the rules at the top of this file. A killed child's children become
// self's, and a later pass kills them.
func killChildren(self int, within time.Duration) string {
	end := time.Now().Add(within)
	for time.Now().Before(end) {
		st, err := reapOne()
		switch {
		case err != nil:
			return "wait4: " + err.Error()
		case st == reapedOne:
			continue
		case st == noChildren:
			return ""
		}
		if table, err := readProcTable(); err == nil {
			for _, p := range directChildrenOf(self, table) {
				_ = syscall.Kill(p, syscall.SIGKILL)
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	return "a descendant could not be killed and reaped"
}

// awaitZombies waits, at most 30 s, until every child of process self is a zombie. A hook.
func awaitZombies(self int) {
	end := time.Now().Add(30 * time.Second)
	for time.Now().Before(end) {
		table, err := readProcTable()
		if err != nil {
			return
		}
		all := true
		for _, p := range directChildrenOf(self, table) {
			if table[p].state != 'Z' {
				all = false
			}
		}
		if all {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// directChildrenOf lists the processes whose parent is root in table, in pid order. These are
// the only processes the reaper signals.
func directChildrenOf(root int, table map[int]procInfo) []int {
	var out []int
	for pid, p := range table {
		if p.ppid == root {
			out = append(out, pid)
		}
	}
	sort.Ints(out)
	return out
}

// descendantsOf lists the processes below root in table, in pid order. It names them; it is
// never used to signal them.
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
