package fakeslurm

// New. Tests of srun --overlap --jobid=<id> (srun.go, srunstep.go). The
// expected values come from the brief of issue #245 (what the command line
// is, which jobs it may reach, what a refusal says and that nothing runs in
// one), from Slurm's error table (slurm_errno.c, 24.11) and from the shell's
// conventions for exit codes; none is read back from the fake. Every wait is
// on observable state (a file a step script wrote, a PID gone) with a
// generous deadline. Every step script records its own PID, and each
// background child's, in a file, and a test checks right after Run returns
// that none of them is alive: waiting there would hide a leak.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

// srunLine is the command line the stack sends to restart a worker.
func srunLine(id int) string {
	return "srun --overlap --jobid=" + itoa(id) + " --ntasks=1 --time=1 bash -s"
}

// quote wraps a path, which may hold characters outside the plain set, as one
// word of a command line.
func quote(s string) string { return "'" + s + "'" }

// login is a Session of another user, with HOME and SCRATCH under the root.
func (r *runRig) login(user string) *Session {
	r.t.Helper()
	env := map[string]string{
		"HOME":    filepath.Join(r.root, "home", user),
		"SCRATCH": filepath.Join(r.root, "scratch", user),
	}
	for _, d := range env {
		if err := os.MkdirAll(d, 0o755); err != nil {
			r.t.Fatal(err)
		}
	}
	return r.p.Login(Login{User: user, Env: env})
}

// submitAs is submit for another Session.
func (r *runRig) submitAs(s *Session, wrap string, args ...string) int {
	r.t.Helper()
	line := strings.Join(append([]string{"sbatch", "--parsable"}, args...), " ") + " '--wrap=" + wrap + "'"
	out, errOut, code := r.runAs(s, line, nil)
	if code != 0 {
		r.t.Fatalf("%q: exit %d, stderr %q", line, code, errOut)
	}
	id, err := strconv.Atoi(strings.TrimSpace(out))
	if err != nil {
		r.t.Fatalf("%q printed %q, want a job ID", line, out)
	}
	return id
}

// holdJob submits a job that waits until release exists, and waits until its
// shell runs.
func (r *runRig) holdJob(s *Session, release string) int {
	r.t.Helper()
	id := r.submitAs(s, waitLoop(release))
	waitFor(r.t, "job "+itoa(id)+" to run", func() bool { return r.job(id).PID > 0 })
	return id
}

// readPids reads the PIDs a script recorded, one per line.
func readPids(file string) []int {
	b, _ := os.ReadFile(file)
	var out []int
	for _, f := range strings.Fields(string(b)) {
		if n, err := strconv.Atoi(f); err == nil {
			out = append(out, n)
		}
	}
	return out
}

// waitPids waits until the file holds n PIDs and returns them.
func waitPids(t *testing.T, file string, n int) []int {
	t.Helper()
	var pids []int
	waitFor(t, itoa(n)+" PIDs in "+filepath.Base(file), func() bool { pids = readPids(file); return len(pids) >= n })
	return pids
}

// wantNoneLeft checks, at once, that the file holds at least n PIDs and that
// none of them is alive. A file with fewer would pass for the wrong reason.
func wantNoneLeft(t *testing.T, file string, n int) {
	t.Helper()
	pids := readPids(file)
	if len(pids) < n {
		t.Fatalf("%s holds %d PIDs, want at least %d: the step did not get as far as the test needs", filepath.Base(file), len(pids), n)
	}
	for _, pid := range pids {
		if !procGone(pid) {
			t.Errorf("process %d of the step is alive after Run returned (state %s)", pid, procState(pid))
		}
	}
}

type runMsg struct {
	res slurm.Result
	err error
}

// runAsync runs the command in a goroutine and returns the channel of its
// result.
func srunAsync(ctx context.Context, s *Session, cmd slurm.Command) <-chan runMsg {
	ch := make(chan runMsg, 1)
	go func() {
		res, err := s.Run(ctx, cmd)
		ch <- runMsg{res, err}
	}()
	return ch
}

func recvRun(t *testing.T, ch <-chan runMsg) runMsg {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(waitDeadline):
		t.Fatalf("Run did not return within %s", waitDeadline)
		return runMsg{}
	}
}

// jobView is what a step must leave as it found it: the job's snapshot, its
// live processes, and what squeue and sacct say of it.
type jobView struct {
	job           Job
	procs         []int
	sacct, squeue map[string]any
}

func (r *runRig) view(id int) jobView {
	r.t.Helper()
	procs, err := r.p.Processes(id)
	if err != nil {
		r.t.Fatal(err)
	}
	return jobView{job: r.job(id), procs: procs, sacct: r.sacct(), squeue: r.squeue()}
}

// The brief's command, on the user's own running job: the script on standard
// input runs under bash with the job's environment, and prints the job's ID.
// The step leaves the job as it found it, and the call is logged.
func TestSrunRunsTheScriptInTheOwnJob(t *testing.T) {
	r := newRunRig(t, RunMode{})
	// The job is one process that never changes (exec), so that its process
	// list is the same before and after unless the step changed it; a shell
	// loop's own sleeps come and go.
	id := r.submit("exec sleep 300")
	waitFor(t, "the job to run", func() bool { return r.job(id).PID > 0 })
	pids := r.root + "/pids"
	before := r.view(id)
	stdin := "echo $$ >> " + pids + "\necho \"$SLURM_JOB_ID\"\n"
	out, errOut, code := r.runAs(r.s, srunLine(id), []byte(stdin))
	if out != itoa(id)+"\n" || errOut != "" || code != 0 {
		t.Errorf("srun gave stdout %q, stderr %q, exit %d; want %q, none, 0", out, errOut, code, itoa(id)+"\n")
	}
	wantNoneLeft(t, pids, 1)
	if after := r.view(id); !reflect.DeepEqual(before, after) {
		t.Errorf("the step changed the job\n before %+v\n after  %+v", before, after)
	}
	if got := r.p.CountFor("example-user", "srun"); got != 1 {
		t.Errorf("%d srun calls in the log, want 1", got)
	}
	var last Call
	for _, c := range r.p.Calls() {
		if c.Words[0] == "srun" {
			last = c
		}
	}
	if want := strings.Fields(srunLine(id)); !reflect.DeepEqual(last.Words, want) || last.StdinLen != len(stdin) || last.User != "example-user" {
		t.Errorf("logged call %+v, want words %v, stdin length %d, user example-user", last, want, len(stdin))
	}
}

// The step has the job's environment (the login's, RunMode.Env and the Slurm
// variables, TMPDIR in the execution's directory) and starts in the submit
// directory; its output and error come back apart.
func TestSrunHasTheJobsEnvironmentAndDirectory(t *testing.T) {
	r := newRunRig(t, RunMode{Env: map[string]string{"EXAMPLE_SITE": "example-value"}})
	id := r.holdJob(r.s, r.root+"/release")
	pids := r.root + "/pids"
	script := "echo $$ >> " + pids + "\npwd\necho \"$SLURM_SUBMIT_DIR\"\necho \"$TMPDIR\"\necho \"$EXAMPLE_SITE\"\necho \"$HOME\"\necho \"$SLURM_JOB_USER\"\necho oops >&2\n"
	out, errOut, code := r.runAs(r.s, srunLine(id), []byte(script))
	want := strings.Join([]string{r.home, r.home, filepath.Join(r.root, itoa(id), "tmp"), "example-value", r.home, "example-user"}, "\n") + "\n"
	if out != want || errOut != "oops\n" || code != 0 {
		t.Errorf("srun gave stdout %q, stderr %q, exit %d\n want stdout %q, stderr %q, exit 0", out, errOut, code, want, "oops\n")
	}
	wantNoneLeft(t, pids, 1)
}

// The step kills exactly the process it is told to, by PID, with SIGKILL, and
// nothing else: the job's batch shell and its other child live on and the job
// is still RUNNING. The job starts two children, so that the shell's wait
// does not end with the one that is killed (with one, the shell would exit
// and the job end, which is the job's doing, not the step's).
func TestSrunKillsOnlyTheProcessItIsTold(t *testing.T) {
	r := newRunRig(t, RunMode{})
	victimF, siblingF, release := r.root+"/victim", r.root+"/sibling", r.root+"/release"
	id := r.submit("sleep 300 & echo $! > " + victimF + "; sleep 600 & echo $! > " + siblingF + "; " + waitLoop(release))
	victim, _ := strconv.Atoi(strings.TrimSpace(r.waitFile(victimF)))
	sibling, _ := strconv.Atoi(strings.TrimSpace(r.waitFile(siblingF)))
	batch := r.job(id).PID
	for _, pid := range []int{victim, sibling, batch} {
		if pid <= 0 || procGone(pid) {
			t.Fatalf("process %d is not alive before the step", pid)
		}
	}
	pids := r.root + "/pids"
	script := "echo $$ >> " + pids + "\nkill -KILL " + itoa(victim) + "\n"
	out, errOut, code := r.runAs(r.s, srunLine(id), []byte(script))
	if out != "" || errOut != "" || code != 0 {
		t.Fatalf("srun gave stdout %q, stderr %q, exit %d; want nothing and 0", out, errOut, code)
	}
	wantNoneLeft(t, pids, 1)
	waitFor(t, "the killed process to be gone", func() bool { return procGone(victim) })
	if procGone(sibling) || procGone(batch) {
		t.Errorf("the step touched more than it was told: sibling gone %v, batch shell gone %v", procGone(sibling), procGone(batch))
	}
	j := r.job(id)
	if j.State != Running || j.PID != batch || j.Finished {
		t.Errorf("after the step: state %s, PID %d (was %d), finished %v; want RUNNING, same PID", j.State, j.PID, batch, j.Finished)
	}
	procs, err := r.p.Processes(id)
	if err != nil || !reflect.DeepEqual(inList(procs, batch, sibling), []bool{true, true}) || inList(procs, victim)[0] {
		t.Errorf("Processes = %v (%v), want the batch shell %d and the sibling %d, not the victim %d", procs, err, batch, sibling, victim)
	}
	if err := os.WriteFile(release, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if j := r.waitFinished(id); j.State != Completed || j.ExitCode != 0 {
		t.Errorf("after release: state %s, exit %d; want COMPLETED, 0", j.State, j.ExitCode)
	}
}

func inList(list []int, want ...int) []bool {
	out := make([]bool, len(want))
	for i, w := range want {
		for _, x := range list {
			out[i] = out[i] || x == w
		}
	}
	return out
}

// A step does not hold the platform's lock: while one waits, other commands
// are answered, the step is not among the job's processes, and the job is
// unchanged. The step ends when its script does.
func TestSrunDoesNotBlockThePlatformWhileItRuns(t *testing.T) {
	r := newRunRig(t, RunMode{})
	id := r.holdJob(r.s, r.root+"/release")
	pids, hold := r.root+"/pids", r.root+"/hold"
	script := "echo $$ >> " + pids + "\n" + waitLoop(hold) + "\necho done\n"
	done := srunAsync(context.Background(), r.s, slurm.Command{Line: srunLine(id), Stdin: []byte(script)})
	step := waitPids(t, pids, 1)[0]
	answered := make(chan jobView, 1)
	go func() { answered <- r.view(id) }() // blocks for good if the step holds the lock
	var v jobView
	select {
	case v = <-answered:
	case <-time.After(waitDeadline):
		t.Fatalf("squeue, sacct and Processes did not answer in %s while a step ran", waitDeadline)
	}
	if v.job.State != Running || inList(v.procs, step)[0] {
		t.Errorf("while the step runs: state %s, processes %v; want RUNNING and not the step %d", v.job.State, v.procs, step)
	}
	if err := os.WriteFile(hold, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	m := recvRun(t, done)
	if m.err != nil || string(m.res.Stdout) != "done\n" || m.res.ExitCode != 0 {
		t.Errorf("step result %+v, %v; want stdout %q, exit 0", m.res, m.err, "done\n")
	}
	wantNoneLeft(t, pids, 1)
}

// Another user's running job is refused with Slurm's error, exit 1, and the
// script does not run: the same script against the caller's own job creates
// its marker file (the control), so an absent marker is not a wrong path.
func TestSrunRefusesAnotherUsersJob(t *testing.T) {
	r := newRunRig(t, RunMode{})
	other := r.login("other-user")
	release := r.root + "/release"
	own := r.holdJob(r.s, release)
	// The other user's job is one process that never changes (exec), so that
	// its process list is the same before and after unless the step changed
	// it; a shell loop's own sleeps come and go.
	theirs := r.submitAs(other, "exec sleep 300")
	waitFor(t, "the other user's job to run", func() bool { return r.job(theirs).PID > 0 })
	marker := r.root + "/marker"
	script := []byte("echo ran > " + marker + "\n")

	if out, errOut, code := r.runAs(r.s, srunLine(own), script); code != 0 {
		t.Fatalf("control: the own job's step exited %d (%q, %q)", code, out, errOut)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("control: the script left no marker (%v), so the marker proves nothing", err)
	}
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}

	viewBefore := r.view(theirs)
	out, errOut, code := r.runAs(r.s, srunLine(theirs), script)
	want := "srun: error: Unable to confirm allocation for job " + itoa(theirs) + ": Access/permission denied\n"
	if out != "" || errOut != want || code != 1 {
		t.Errorf("srun on another user's job gave stdout %q, stderr %q, exit %d; want nothing, %q, 1", out, errOut, code, want)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the script ran in another user's job: marker stat = %v", err)
	}
	if after := r.view(theirs); !reflect.DeepEqual(viewBefore, after) {
		t.Errorf("the refused step changed the other user's job")
	}
	// The other user reaches their own job; the first user is the one refused.
	if _, errOut, code := r.runAs(other, srunLine(theirs), script); code != 0 {
		t.Errorf("the owner's own step exited %d (%q)", code, errOut)
	}
	if got := r.p.CountFor("example-user", "srun"); got != 2 {
		t.Errorf("%d srun calls of the first user in the log, want 2 (the control and the refused one)", got)
	}
}

// A job that is pending or held, ended, or unknown is refused, exit 1, with
// the reason in Slurm's words, and nothing runs.
func TestSrunRefusesAJobThatIsNotRunning(t *testing.T) {
	r := newRunRig(t, RunMode{})
	marker := r.root + "/marker"
	script := []byte("echo ran > " + marker + "\n")
	ended := r.submit("true")
	r.waitFinished(ended)
	held := r.submit("true", "--hold")
	const unknown = 9999999
	for _, c := range []struct {
		name   string
		id     int
		reason string
	}{
		{"held", held, "Job is pending execution"},
		{"ended", ended, "Job/step already completing or completed"},
		{"unknown", unknown, "Invalid job id specified"},
	} {
		t.Run(c.name, func(t *testing.T) {
			out, errOut, code := r.runAs(r.s, srunLine(c.id), script)
			want := "srun: error: Unable to confirm allocation for job " + itoa(c.id) + ": " + c.reason + "\n"
			if out != "" || errOut != want || code != 1 {
				t.Errorf("srun gave stdout %q, stderr %q, exit %d; want nothing, %q, 1", out, errOut, code, want)
			}
			if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("the script ran: marker stat = %v", err)
			}
		})
	}
	if j := r.job(held); j.State != Pending || !j.Held {
		t.Errorf("the refused step changed the held job: state %s, held %v", j.State, j.Held)
	}
}

// A line that is not exactly the accepted form is refused, exit 1, and
// nothing runs. The valid line with the same script is the control: it makes
// the marker, so the refusals do not pass by a wrong marker path.
func TestSrunRefusesWhatItDoesNotKnow(t *testing.T) {
	r := newRunRig(t, RunMode{})
	id := r.holdJob(r.s, r.root+"/release")
	marker := r.root + "/marker"
	script := []byte("echo ran > " + marker + "\n")
	if _, errOut, code := r.runAs(r.s, srunLine(id), script); code != 0 {
		t.Fatalf("control: exit %d (%q)", code, errOut)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("control: no marker (%v)", err)
	}
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	job := "--jobid=" + itoa(id)
	for _, c := range []struct {
		name, line, want string
	}{
		{"no overlap", "srun " + job + " --ntasks=1 bash -s", "--overlap is required"},
		{"no jobid", "srun --overlap --ntasks=1 bash -s", "--jobid=<id> is required"},
		{"unknown flag", "srun --overlap " + job + " --exclusive bash -s", `not "--exclusive"`},
		{"short flag", "srun --overlap " + job + " -n1 bash -s", `not "-n1"`},
		{"overlap with a value", "srun --overlap=yes " + job + " bash -s", `not "--overlap=yes"`},
		{"jobid in two words", "srun --overlap --jobid " + itoa(id) + " bash -s", `not "--jobid"`},
		{"jobid not digits", "srun --overlap --jobid=abc bash -s", "invalid job id"},
		{"jobid with a sign", "srun --overlap --jobid=+" + itoa(id) + " bash -s", "invalid job id"},
		{"two tasks", "srun --overlap " + job + " --ntasks=2 bash -s", "the fake runs one task"},
		{"bad time", "srun --overlap " + job + " --time=abc bash -s", "invalid --time"},
		{"no program", "srun --overlap " + job, "no program to run"},
	} {
		t.Run(c.name, func(t *testing.T) {
			out, errOut, code := r.runAs(r.s, c.line, script)
			if out != "" || code != 1 || !strings.HasPrefix(errOut, "srun: error: ") || !strings.Contains(errOut, c.want) {
				t.Errorf("%q gave stdout %q, stderr %q, exit %d; want nothing, an srun error holding %q, 1", c.line, out, errOut, code, c.want)
			}
			if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("the script ran: marker stat = %v", err)
			}
		})
	}
}

// Outside run mode there is no process to run in: refused, exit 1, nothing
// runs, and the job is not changed.
func TestSrunIsRefusedOutsideRunMode(t *testing.T) {
	r := newRig(t, Config{})
	id := r.sbatch()
	r.start(id)
	marker := filepath.Join(t.TempDir(), "marker")
	out, errOut, code := r.runAs(r.s, srunLine(id), []byte("echo ran > "+marker+"\n"))
	if out != "" || code != 1 || !strings.HasPrefix(errOut, "srun: error: ") || !strings.Contains(errOut, "run mode") {
		t.Errorf("srun gave stdout %q, stderr %q, exit %d; want nothing, an srun error that names run mode, 1", out, errOut, code)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the script ran: marker stat = %v", err)
	}
	if j, _ := r.p.Job(id); j.State != Running {
		t.Errorf("the refused step changed the job: state %s", j.State)
	}
}

// Command.Timeout cuts a step that runs past it: Run returns ErrTimeout, not
// before the deadline, and the step's shell and its background child are
// both gone. The job is untouched.
func TestSrunIsKilledAtTheCommandTimeout(t *testing.T) {
	r := newRunRig(t, RunMode{})
	id := r.holdJob(r.s, r.root+"/release")
	pids := r.root + "/pids"
	script := "echo $$ >> " + pids + "\nsleep 300 &\necho $! >> " + pids + "\nwait\n"
	const timeout = 3 * time.Second
	start := time.Now()
	_, err := r.s.Run(context.Background(), slurm.Command{Line: srunLine(id), Stdin: []byte(script), Timeout: timeout})
	took := time.Since(start)
	if !errors.Is(err, slurm.ErrTimeout) {
		t.Fatalf("Run returned %v after %s; want slurm.ErrTimeout", err, took)
	}
	if took < timeout || took > waitDeadline {
		t.Errorf("Run returned after %s; want the timeout of %s, not earlier and not the step's 300 s", took, timeout)
	}
	wantNoneLeft(t, pids, 2)
	if j := r.job(id); j.State != Running {
		t.Errorf("the job is %s after its step timed out, want RUNNING", j.State)
	}
}

// The caller's context ends a step as well: Run returns a transport error (a
// cancelled connection), and nothing of the step is left.
func TestSrunIsKilledWhenTheContextEnds(t *testing.T) {
	r := newRunRig(t, RunMode{})
	id := r.holdJob(r.s, r.root+"/release")
	pids := r.root + "/pids"
	script := "echo $$ >> " + pids + "\nsleep 300 &\necho $! >> " + pids + "\nwait\n"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := srunAsync(ctx, r.s, slurm.Command{Line: srunLine(id), Stdin: []byte(script)})
	waitPids(t, pids, 2)
	cancel()
	if m := recvRun(t, done); !errors.Is(m.err, slurm.ErrTransport) {
		t.Errorf("Run returned %v, want slurm.ErrTransport", m.err)
	}
	wantNoneLeft(t, pids, 2)
}

// --time bounds the step: past it the step is killed with its children, and
// srun exits 1 with the time-limit line. RunMode.Minute scales the minute,
// as it does for a job's --time.
func TestSrunHonoursItsTimeLimit(t *testing.T) {
	const minute = 2 * time.Second
	r := newRunRig(t, RunMode{Minute: minute})
	id := r.holdJob(r.s, r.root+"/release")
	pids := r.root + "/pids"
	script := "echo $$ >> " + pids + "\nsleep 300 &\necho $! >> " + pids + "\nwait\n"
	start := time.Now()
	out, errOut, code := r.runAs(r.s, srunLine(id), []byte(script)) // --time=1
	if took := time.Since(start); took < minute {
		t.Errorf("the step ended after %s, before its --time of %s", took, minute)
	}
	if out != "" || code != 1 || !strings.Contains(errOut, "TIME LIMIT") {
		t.Errorf("srun gave stdout %q, stderr %q, exit %d; want nothing, a TIME LIMIT line, 1", out, errOut, code)
	}
	wantNoneLeft(t, pids, 2)
}

// A step does not outlive its job: when the job's batch shell is gone the
// step is killed, with its children, and srun says so.
func TestSrunEndsWithItsJob(t *testing.T) {
	r := newRunRig(t, RunMode{})
	id := r.holdJob(r.s, r.root+"/release")
	pids := r.root + "/pids"
	script := "echo $$ >> " + pids + "\nsleep 300 &\necho $! >> " + pids + "\nwait\n"
	done := srunAsync(context.Background(), r.s, slurm.Command{Line: srunLine(id), Stdin: []byte(script)})
	waitPids(t, pids, 2)
	if err := r.p.End(id, NodeFail); err != nil { // KILL at once, as a failed node does
		t.Fatal(err)
	}
	m := recvRun(t, done)
	if m.err != nil || m.res.ExitCode != 1 || !strings.Contains(string(m.res.Stderr), "Job step aborted") {
		t.Errorf("step result %+v, %v; want exit 1 and a Job step aborted line", m.res, m.err)
	}
	wantNoneLeft(t, pids, 2)
}

// What the program leaves behind in its session is killed when it exits: a
// background child is not left running after Run returns, and the exit status
// is the program's own.
func TestSrunKillsWhatTheProgramLeaves(t *testing.T) {
	r := newRunRig(t, RunMode{})
	id := r.holdJob(r.s, r.root+"/release")
	pids := r.root + "/pids"
	script := "echo $$ >> " + pids + "\nsleep 300 &\necho $! >> " + pids + "\nexit 5\n"
	if _, _, code := r.runAs(r.s, srunLine(id), []byte(script)); code != 5 {
		t.Errorf("exit %d, want the script's 5", code)
	}
	wantNoneLeft(t, pids, 2)
}

// A step that writes without end is killed at 16 MiB of output, with what it
// wrote kept up to the cap, instead of filling the memory.
func TestSrunOutputIsCapped(t *testing.T) {
	r := newRunRig(t, RunMode{})
	id := r.holdJob(r.s, r.root+"/release")
	pids := r.root + "/pids"
	out, errOut, code := r.runAs(r.s, srunLine(id), []byte("echo $$ >> "+pids+"\nexec yes\n"))
	if code != 1 || !strings.Contains(errOut, "more than 16 MiB") {
		t.Errorf("exit %d, stderr %q; want 1 and a line naming the 16 MiB cap", code, errOut)
	}
	if len(out) != 16<<20 {
		t.Errorf("stdout holds %d bytes, want the cap of %d", len(out), 16<<20)
	}
	wantNoneLeft(t, pids, 1)
}

// A program that never reads its standard input does not hold the step: a
// megabyte of input it ignores neither blocks nor fails it.
func TestSrunProgramThatIgnoresItsInput(t *testing.T) {
	r := newRunRig(t, RunMode{})
	id := r.holdJob(r.s, r.root+"/release")
	line := "srun --overlap --jobid=" + itoa(id) + " true"
	m := recvRun(t, srunAsync(context.Background(), r.s, slurm.Command{Line: line, Stdin: make([]byte, 1<<20)}))
	if m.err != nil || m.res.ExitCode != 0 {
		t.Errorf("result %+v, %v; want exit 0", m.res, m.err)
	}
}

// A process that escaped the step's session (setsid, and its parent gone)
// can hold the step's output open for ever; srun returns anyway, once the
// processes it can find are gone and the streams have had a moment to run
// dry, with what was written, and the escapee is left running (the limit
// doc.go names for jobs too). The test kills it by its PID.
func TestSrunReturnsWhileAnEscapeeHoldsItsOutput(t *testing.T) {
	r := newRunRig(t, RunMode{})
	id := r.holdJob(r.s, r.root+"/release")
	esc := r.root + "/escapee"
	script := "echo before\nsetsid sh -c 'echo $$ > " + esc + "; exec sleep 300' &\nexit 0\n"
	out, errOut, code := r.runAs(r.s, srunLine(id), []byte(script))
	if out != "before\n" || errOut != "" || code != 0 {
		t.Errorf("srun gave stdout %q, stderr %q, exit %d; want %q, none, 0", out, errOut, code, "before\n")
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(r.waitFile(esc)))
	if pid <= 0 || procGone(pid) {
		t.Fatalf("the escapee %d is not running; the test would prove nothing", pid)
	}
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the escapee to be gone", func() bool { return procGone(pid) })
}

// The exit status of the program is srun's: its own code, or 128 plus the
// signal that ended it, as a shell reports it.
func TestSrunExitStatus(t *testing.T) {
	r := newRunRig(t, RunMode{})
	id := r.holdJob(r.s, r.root+"/release")
	for _, c := range []struct {
		prog string
		want int
	}{
		{"sh -c 'exit 7'", 7},
		{"sh -c 'kill -TERM $$'", 128 + int(syscall.SIGTERM)},
		{"true", 0},
		{"false", 1},
	} {
		line := "srun --overlap --jobid=" + itoa(id) + " " + c.prog
		if out, errOut, code := r.runAs(r.s, line, nil); code != c.want || out != "" || errOut != "" {
			t.Errorf("%q gave stdout %q, stderr %q, exit %d; want exit %d", line, out, errOut, code, c.want)
		}
	}
}

// The program is looked up on the job's PATH, not the test process's: a tool
// in a directory of the login's PATH is found, one only the test process's
// PATH holds is not (exit 127, as a shell says "not found"), a file that is
// not executable is refused with 126, and a name with a slash is used as it
// is.
func TestSrunLooksTheProgramUpOnTheJobsPath(t *testing.T) {
	r := newRunRig(t, RunMode{})
	jobBin, hostBin := filepath.Join(r.root, "jobbin"), filepath.Join(r.root, "hostbin")
	for _, d := range []string{jobBin, hostBin} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	tool := func(dir, name string, mode os.FileMode) string {
		f := filepath.Join(dir, name)
		if err := os.WriteFile(f, []byte("#!/bin/sh\necho "+name+" ran\n"), mode); err != nil {
			t.Fatal(err)
		}
		return f
	}
	jobTool := tool(jobBin, "example-jobtool", 0o755)
	tool(jobBin, "example-noexec", 0o644)
	tool(hostBin, "example-hosttool", 0o755)
	t.Setenv("PATH", hostBin+":"+os.Getenv("PATH"))
	s := r.p.Login(Login{User: "example-user", Env: map[string]string{
		"HOME": r.home, "SCRATCH": filepath.Join(r.root, "scratch", "example-user"), "PATH": jobBin + ":/usr/bin:/bin",
	}})
	id := r.holdJob(s, r.root+"/release")
	for _, c := range []struct {
		prog         string
		out, errText string
		code         int
	}{
		{"example-jobtool", "example-jobtool ran\n", "", 0},
		{quote(jobTool), "example-jobtool ran\n", "", 0},
		{"example-hosttool", "", "srun: error: execve(): example-hosttool: No such file or directory\n", 127},
		{"example-noexec", "", "srun: error: execve(): example-noexec: Permission denied\n", 126},
		{"no-such-program", "", "srun: error: execve(): no-such-program: No such file or directory\n", 127},
	} {
		line := "srun --overlap --jobid=" + itoa(id) + " " + c.prog
		out, errOut, code := r.runAs(s, line, nil)
		if out != c.out || errOut != c.errText || code != c.code {
			t.Errorf("%q gave stdout %q, stderr %q, exit %d; want %q, %q, %d", line, out, errOut, code, c.out, c.errText, c.code)
		}
	}
}
