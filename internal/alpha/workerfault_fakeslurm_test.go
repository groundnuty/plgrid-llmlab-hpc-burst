package alpha

// The worker fault in each candidate shape of issue #445, over the fake Slurm in run mode: one job per shape, whose
// batch script builds the shape's process tree on this host out of real processes (shells and Python doubles of
// Apptainer's master, its shim and the engine, each named as Apptainer names it), then the live step's own code over
// the fake's srun --overlap: FindEngine lists the job's processes and picks the engine, KillEngine kills it through
// its pidfd. The engine double writes its own PID to a file once it runs: that, not the code under test, is the
// expected pick, and the expected shapes are literals. Before #445 the pick found no engine in any of them.
//
// Every process of a job is the fake's to end (KILL through pidfds, at Close); the one process this test starts
// outside every job is ended through its own pidfd (os.Process), and its child by its parent-death signal. Nothing
// signals a process group.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
)

// faultLaunch is the launch's command in these jobs.
var faultLaunch = []string{"vllm", "serve", "/hpcburst/model", "--port=8000", "--uds=/hpcburst/run/engine.sock"}

// engineDouble is the engine: a Python console script run by its shebang, as vLLM's is, so that its command line is
// the interpreter, the script's path and the launch's arguments. The words in the file mode beside it give it
// children as vLLM v1's API server has: fork, a copy with its very command line; spawn, an interpreter running code.
// Once they run, it writes its own PID to engine.pid beside it.
const engineDouble = `#!{py}
import os, subprocess, sys, time
here = os.path.dirname(os.path.abspath(sys.argv[0]))
mode = open(os.path.join(here, "mode")).read().split()
if "fork" in mode:
    if os.fork() == 0:
        time.sleep(300)
        os._exit(0)
if "spawn" in mode:
    subprocess.Popen([sys.executable, "-c", "import time; time.sleep(300)"])
with open(os.path.join(here, "engine.pid.tmp"), "w") as f:
    f.write(str(os.getpid()))
os.rename(os.path.join(here, "engine.pid.tmp"), os.path.join(here, "engine.pid"))
time.sleep(300)
`

// masterDouble is setuid Apptainer's master as the listing sees it once it has changed its user IDs: not dumpable,
// so that its environment is root's to read (Apptainer 1.4.5, starter.c, set_master_privileges; proc(5)), run as
// starter-suid. It starts the engine with a clean environment, as --cleanenv does, and waits for it.
const masterDouble = `import ctypes, os, subprocess, sys
here = os.path.dirname(os.path.abspath(sys.argv[0]))
libc = ctypes.CDLL(None, use_errno=True)
if libc.prctl(4, ctypes.c_ulong(0), ctypes.c_ulong(0), ctypes.c_ulong(0), ctypes.c_ulong(0)) != 0:
    sys.exit("prctl(PR_SET_DUMPABLE, 0) failed")
sys.exit(subprocess.call([os.path.join(here, "vllm")] + sys.argv[1:], env={"PATH": "/usr/bin:/bin"}))
`

// outsideDouble is a runtime parent of no job, started by the test: it starts the engine with a parent-death signal
// of KILL, so that the engine ends with it.
const outsideDouble = `import ctypes, os, subprocess, sys
here = os.path.dirname(os.path.abspath(sys.argv[0]))
me = os.getpid()
libc = ctypes.CDLL(None, use_errno=True)
def die_with_parent():
    libc.prctl(1, ctypes.c_ulong(9), ctypes.c_ulong(0), ctypes.c_ulong(0), ctypes.c_ulong(0))
    if os.getppid() != me:
        os._exit(1)
sys.exit(subprocess.call([os.path.join(here, "vllm")] + sys.argv[1:], preexec_fn=die_with_parent))
`

// faultShape is one candidate shape: the job's scripts ({dir} is the job's directory, {py} the interpreter, {args}
// the launch's arguments), the engine double's mode, and the job tree's shape, its counts line aside.
type faultShape struct {
	name    string
	scripts map[string]string
	mode    string
	shape   []string
}

// The job's batch shell, which the wrap makes bash running tree.sh, with the job's environment: the agent's
// stand-in. Like the agent, it outlives the engine (it sleeps once its children are gone), so the job runs on.
const faultRoot = "d0 shell in-job:env env:job cgroup:other argv:program meets:job parent:outside-the-job"

var faultShapes = []faultShape{
	{
		name: "(a) the parent is the starter shim, appinit",
		scripts: map[string]string{
			"tree.sh": "(exec -a 'Apptainer runtime parent' bash {dir}/rp.sh) &\nwait\nexec sleep 300\n",
			"rp.sh":   "(exec -a appinit bash {dir}/shim.sh) &\nwait\n",
			"shim.sh": "{dir}/vllm {args} &\nwait\n",
		},
		shape: []string{faultRoot,
			"d1 runtime-parent in-job:env env:job cgroup:other argv:program meets:job",
			"d2 starter in-job:env env:job cgroup:other argv:program meets:job+apptainer",
			"d3 python in-job:env env:job cgroup:other argv:python-script meets:job+apptainer+command"},
	},
	{
		name: "(b) a shell between, and the engine children",
		scripts: map[string]string{
			"tree.sh":  "(exec -a 'Apptainer runtime parent' bash {dir}/rp.sh) &\nwait\nexec sleep 300\n",
			"rp.sh":    "bash {dir}/entry.sh &\nwait\n",
			"entry.sh": "{dir}/vllm {args} &\nwait\n",
		},
		mode: "fork spawn",
		shape: []string{faultRoot,
			"d1 runtime-parent in-job:env env:job cgroup:other argv:program meets:job",
			"d2 shell in-job:env env:job cgroup:other argv:program meets:job+apptainer",
			"d3 python in-job:env env:job cgroup:other argv:python-script meets:job+apptainer+command",
			"d4 python in-job:env env:job cgroup:other argv:python-code meets:job",
			"d4 python in-job:env env:job cgroup:other argv:python-script meets:job+command"},
	},
	{
		name: "(c) the interpreter with an option that takes a value",
		scripts: map[string]string{
			"tree.sh": "(exec -a 'Apptainer runtime parent' bash {dir}/rp.sh) &\nwait\nexec sleep 300\n",
			"rp.sh":   "{py} -X faulthandler {dir}/vllm {args} &\nwait\n",
		},
		shape: []string{faultRoot,
			"d1 runtime-parent in-job:env env:job cgroup:other argv:program meets:job",
			"d2 python in-job:env env:job cgroup:other argv:python-options-script meets:job+apptainer+command"},
	},
	{
		// (d) No cgroup names the job on this host; the master's environment cannot be read and the engine's
		// carries no SLURM_JOB_ID. Beside them, the launch's command run by the job's own shell, not under
		// Apptainer: a near miss in the job.
		name: "(d) of the job by descent alone",
		scripts: map[string]string{
			"tree.sh": "env -i PATH=/usr/bin:/bin {dir}/starter-suid {dir}/master.py {args} &\n{dir}/near/vllm {args} &\nwait\nexec sleep 300\n",
		},
		shape: []string{faultRoot,
			"d1 python in-job:env env:job cgroup:other argv:python-script meets:job+command",
			"d1 starter in-job:descent env:unreadable cgroup:other argv:program meets:job",
			"d2 python in-job:descent env:no cgroup:other argv:python-script meets:job+apptainer+command"},
	},
}

// faultRig is a fake Slurm in run mode on this host, and a login to it.
type faultRig struct {
	t    *testing.T
	root string
	sess *fakeslurm.Session
	py   string
}

// newFaultRig starts the fake. Its job IDs are this process's own (index i of 16), so that no other fake on this
// host, in this binary or another, has a job of the same ID while it runs: the listing finds a job's processes by
// its SLURM_JOB_ID on the whole host. The worker fault's other tests use 900,000,000 and 910,000,000 plus the PID,
// the stand-in rows 10,000,000 plus 8 times it.
func newFaultRig(t *testing.T, i int) *faultRig {
	t.Helper()
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Fatalf("the worker fault's doubles and its kill need python3: %v", err)
	}
	if py, err = filepath.Abs(py); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if strings.ContainsAny(root+py, "'\"$`\\") {
		t.Fatalf("the paths %q and %q must hold no quote, dollar or backslash: the job's scripts quote them", root, py)
	}
	home := filepath.Join(root, "home", "example-user")
	if err := os.MkdirAll(home, 0o755); err != nil {
		t.Fatal(err)
	}
	plat := fakeslurm.New(slurm.RealClock, fakeslurm.Config{FirstJobID: 920_000_000 + 16*os.Getpid() + 4*i, Run: &fakeslurm.RunMode{Root: root}})
	t.Cleanup(func() {
		if err := plat.Close(); err != nil {
			t.Errorf("the fake Slurm's Close: %v", err)
		}
	})
	return &faultRig{t: t, root: root, py: py, sess: plat.Login(fakeslurm.Login{User: "example-user", Env: map[string]string{"HOME": home}})}
}

// expand fills a script's placeholders; expandShell fills a shell script's, its paths in double quotes (the
// temporary directory's name holds the subtest's, parentheses and all; newFaultRig refuses one that a double quote
// would not hold).
func (r *faultRig) expand(s, dir string) string {
	return strings.NewReplacer("{dir}", dir, "{py}", r.py, "{args}", strings.Join(faultLaunch[1:], " ")).Replace(s)
}

func (r *faultRig) expandShell(s, dir string) string {
	return r.expand(strings.NewReplacer("{dir}", `"{dir}"`, "{py}", `"{py}"`).Replace(s), dir)
}

// engineDir writes an engine double and its mode into dir.
func (r *faultRig) engineDir(dir, mode string) {
	r.t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		r.t.Fatal(err)
	}
	r.write(filepath.Join(dir, "vllm"), r.expand(engineDouble, dir), 0o755)
	r.write(filepath.Join(dir, "mode"), mode+"\n", 0o644)
}

func (r *faultRig) write(path, content string, mode os.FileMode) {
	r.t.Helper()
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		r.t.Fatal(err)
	}
}

// job writes a shape's scripts into a directory of its own and submits the job that runs them; it returns the
// job's ID and directory.
func (r *faultRig) job(name string, c faultShape) (string, string) {
	r.t.Helper()
	dir := filepath.Join(r.root, name)
	r.engineDir(dir, c.mode)
	for f, s := range c.scripts {
		r.write(filepath.Join(dir, f), r.expandShell(s, dir), 0o644)
	}
	if strings.Contains(c.scripts["tree.sh"], "starter-suid") {
		r.write(filepath.Join(dir, "master.py"), masterDouble, 0o644)
		if err := os.Symlink(r.py, filepath.Join(dir, "starter-suid")); err != nil {
			r.t.Fatal(err)
		}
		r.engineDir(filepath.Join(dir, "near"), "")
	}
	line := "sbatch --parsable '--wrap=exec bash \"" + filepath.Join(dir, "tree.sh") + "\"'"
	res, err := r.sess.Run(context.Background(), slurm.Command{Line: line})
	if err != nil || res.ExitCode != 0 {
		r.t.Fatalf("%s: exit %d, %v, stderr %q", line, res.ExitCode, err, res.Stderr)
	}
	id := strings.TrimSpace(string(res.Stdout))
	if _, err := strconv.Atoi(id); err != nil {
		r.t.Fatalf("sbatch printed %q, want a job ID", res.Stdout)
	}
	return id, dir
}

// enginePID waits for the engine double in dir to write its PID. A double that does not fails the test with what
// the jobs wrote (their output files are in the login's home, the submit directory).
func (r *faultRig) enginePID(dir string) int {
	r.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		b, err := os.ReadFile(filepath.Join(dir, "engine.pid"))
		if n, perr := strconv.Atoi(string(b)); err == nil && perr == nil && n > 0 {
			return n
		}
		if time.Now().After(deadline) {
			outs, _ := filepath.Glob(filepath.Join(r.root, "home", "example-user", "slurm-*.out"))
			var said []string
			for _, o := range outs {
				b, _ := os.ReadFile(o)
				said = append(said, fmt.Sprintf("%s: %q", filepath.Base(o), b))
			}
			r.t.Fatalf("the engine double in %s wrote no PID within 60 s; the jobs wrote %s", filepath.Base(dir), strings.Join(said, "; "))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// alive reports whether the process pid lives and is not a zombie.
func alive(pid int) bool {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false
	}
	s := string(b)
	f := strings.Fields(s[strings.LastIndex(s, ")")+1:])
	return len(f) > 0 && f[0] != "Z" && f[0] != "X"
}

// waitGone waits for the process pid to be gone or a zombie.
func waitGone(t *testing.T, pid int, what string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for alive(pid) {
		if time.Now().After(deadline) {
			t.Fatalf("%s is alive 30 s after its kill", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// outsideEngine starts the launch's command under a runtime parent of no job, from the test itself: a near miss
// outside every job. It returns the engine's PID.
func (r *faultRig) outsideEngine() int {
	r.t.Helper()
	dir := filepath.Join(r.root, "outside")
	r.engineDir(dir, "")
	r.write(filepath.Join(dir, "parent.py"), outsideDouble, 0o644)
	cmd := exec.Command(r.py, append([]string{filepath.Join(dir, "parent.py")}, faultLaunch[1:]...)...)
	cmd.Args[0] = "Apptainer runtime parent"
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "SLURM_") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	if err := cmd.Start(); err != nil {
		r.t.Fatal(err)
	}
	engine := 0
	r.t.Cleanup(func() {
		// os.Process signals through its pidfd: this one process, never a group. Its engine ends by its parent-death
		// signal.
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if engine > 0 {
			waitGone(r.t, engine, "the engine outside every job")
		}
	})
	engine = r.enginePID(dir)
	return engine
}

func TestTheWorkerFaultFindsTheEngineInEachShape(t *testing.T) {
	me, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	for i, c := range faultShapes {
		t.Run(c.name, func(t *testing.T) {
			r := newFaultRig(t, i)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			jobID, dir := r.job("job", c)
			engine := r.enginePID(dir)
			var spared []int
			if strings.HasPrefix(c.name, "(d)") {
				// Near misses of the widest membership rule: the launch's command run by the job's own shell, under
				// the same tree in another job, and under a runtime parent of no job.
				_, other := r.job("other", faultShape{scripts: c.scripts})
				spared = append(spared, r.enginePID(filepath.Join(dir, "near")), r.enginePID(other), r.outsideEngine())
			}

			eng, shape, err := FindEngine(ctx, r.sess, jobID, faultLaunch, MatchesCommand, time.Minute)
			if err != nil || eng.PID != engine {
				t.Fatalf("FindEngine = PID %d, %v; want the engine double's PID %d\n%s", eng.PID, err, engine, shape)
			}
			lines := strings.Split(shape, "\n  ")
			head := regexp.MustCompile(`^the job's process tree on its node, redacted \(kinds, depths and counts only\): ([0-9]+) processes listed, ([0-9]+) in the job's tree, 1 meeting all three conditions$`).FindStringSubmatch(lines[0])
			if head == nil {
				t.Fatalf("the shape's first line is %q", lines[0])
			}
			if head[2] != strconv.Itoa(len(c.shape)) || strings.Join(lines[1:], "\n") != strings.Join(c.shape, "\n") {
				t.Errorf("the shape is\n%s\nwant %d in the job's tree, 1 meeting all three conditions, and\n  %s", shape, len(c.shape), strings.Join(c.shape, "\n  "))
			}
			listed, _ := strconv.Atoi(head[1])
			forbidden := []string{me.Username, jobID, strconv.Itoa(engine), strconv.Itoa(os.Getpid())}
			for _, part := range strings.Split(r.root, "/") {
				if len(part) >= 4 { // the temporary directory's own words, not a short one such as t
					forbidden = append(forbidden, part)
				}
			}
			if leaks := shapeLeaks(shape, forbidden, listed); len(leaks) != 0 {
				t.Errorf("the shape leaks %q:\n%s", leaks, shape)
			}

			// A process that is no longer the one listed is left alone, and the step says it did not kill.
			stale := eng
			stale.Start = "1"
			if err := KillEngine(ctx, r.sess, jobID, stale, time.Minute); err == nil || !alive(engine) {
				t.Fatalf("KillEngine of a stale listing: %v, the engine alive %v; want a refusal and the engine alive", err, alive(engine))
			}
			if err := KillEngine(ctx, r.sess, jobID, eng, time.Minute); err != nil {
				t.Fatal(err)
			}
			waitGone(t, engine, "the engine")
			for _, pid := range spared {
				if !alive(pid) {
					t.Errorf("a near miss of the engine's command (PID %d) died: the kill reached more than the engine", pid)
				}
			}
		})
	}
}

// The step refuses a job ID that is not digits before anything runs, and a failed listing step says so without its
// output.
func TestFindEngineRefusesBeforeItRuns(t *testing.T) {
	r := newFaultRig(t, 0) // it starts no job
	ctx := context.Background()
	if _, _, err := FindEngine(ctx, r.sess, "12;id", faultLaunch, MatchesCommand, time.Minute); err == nil {
		t.Error("FindEngine ran with a job ID that is not digits")
	}
	if err := KillEngine(ctx, r.sess, "12;id", NodeProc{PID: 1, Start: "1"}, time.Minute); err == nil {
		t.Error("KillEngine ran with a job ID that is not digits")
	}
	// No job 999 on this fake: srun refuses, exit 1.
	_, _, err := FindEngine(ctx, r.sess, "999", faultLaunch, MatchesCommand, time.Minute)
	var ec ErrEngineCount
	if err == nil || errors.As(err, &ec) || !strings.Contains(err.Error(), "the srun step that lists the job's processes failed (exit 1") {
		t.Errorf("FindEngine on no job: %v, want the listing step's failure", err)
	}
}
