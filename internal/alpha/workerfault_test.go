package alpha

// The worker-rung fault's pure parts, and its two scripts run for real with
// bash against a process tree of this host shaped like a job's: a process
// named apptainer whose environment carries the job's SLURM_JOB_ID, and its
// child whose arguments are the launch's. Expected values come from the
// science agent's ruling (exactly one engine, by PID, SIGKILL, never by name).

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestSrunLine(t *testing.T) {
	if l, err := SrunLine("123456"); err != nil || l != "srun --overlap --jobid=123456 --ntasks=1 --time=1 bash -s" {
		t.Errorf("SrunLine(123456) = %q %v", l, err)
	}
	for _, bad := range []string{"", "12 3", "12;id", "-1", "1e3", "abc", "12\n3"} {
		if _, err := SrunLine(bad); err == nil {
			t.Errorf("SrunLine(%q) accepted", bad)
		}
	}
}

func b64(argv ...string) string {
	return base64.StdEncoding.EncodeToString([]byte(strings.Join(argv, "\x00") + "\x00"))
}

func TestParseListing(t *testing.T) {
	out := fmt.Sprintf("proc 10 1 500 1 %s\nproc 11 10 501 0 %s\nlisted\n", b64("/x/apptainer", "exec"), b64("python3", "vllm", "serve"))
	procs, err := ParseListing([]byte(out))
	if err != nil {
		t.Fatal(err)
	}
	if len(procs) != 2 || procs[0].PID != 10 || procs[0].PPID != 1 || procs[0].Start != "500" || !procs[0].InJob ||
		procs[1].InJob || strings.Join(procs[1].Argv, " ") != "python3 vllm serve" {
		t.Errorf("ParseListing = %+v", procs)
	}
	for name, bad := range map[string]string{
		"no end":           "proc 10 1 500 1 " + b64("a") + "\n",
		"a line after":     "listed\nproc 10 1 500 1 " + b64("a") + "\n",
		"a field short":    "proc 10 1 500 " + b64("a") + "\nlisted\n",
		"an injob of 2":    "proc 10 1 500 2 " + b64("a") + "\nlisted\n",
		"a PID not number": "proc x 1 500 1 " + b64("a") + "\nlisted\n",
		"a start not num":  "proc 10 1 s 1 " + b64("a") + "\nlisted\n",
		"bad base64":       "proc 10 1 500 1 !!!\nlisted\n",
		"another word":     "pid 10 1 500 1 " + b64("a") + "\nlisted\n",
		"PID 0":            "proc 0 1 500 1 " + b64("a") + "\nlisted\n",
	} {
		if _, err := ParseListing([]byte(bad)); !errors.Is(err, ErrListing) {
			t.Errorf("%s: ParseListing returned %v, want ErrListing", name, err)
		}
	}
}

// The launch's command, program included (review of PR 255, B2): the program
// directly, by its name or any absolute path to it, or as the script of a
// Python interpreter; then exactly the launch's arguments.
func TestMatchesCommand(t *testing.T) {
	cmd := []string{"vllm", "serve", "/hpcburst/model", "--port=8000", "--uds=/hpcburst/run/engine.sock"}
	args := cmd[1:]
	with := func(head ...string) []string { return append(head, args...) }
	for name, c := range map[string]struct {
		argv []string
		want bool
	}{
		"the program by its name":                {with("vllm"), true},
		"the program by its path":                {with("/usr/local/bin/vllm"), true},
		"through an interpreter":                 {with("/usr/bin/python3", "/usr/local/bin/vllm"), true},
		"through python3.12":                     {with("python3.12", "/usr/local/bin/vllm"), true},
		"through python with a shebang's flags":  {with("/usr/bin/python3", "-s", "-u", "/usr/local/bin/vllm"), true},
		"another program, the same arguments":    {with("/usr/local/bin/other-engine"), false},
		"another program through an interpreter": {with("python3", "/usr/local/bin/other-engine"), false},
		"a script by a relative path":            {with("python3", "vllm"), false},
		"the program by a relative path":         {with("bin/vllm"), false},
		"an interpreter that is not Python":      {with("/bin/bash", "/usr/local/bin/vllm"), false},
		"an interpreter flag with a value":       {with("python3", "-X", "dev", "/usr/local/bin/vllm"), false},
		"an option whose value is the path":      {with("python3", "-X", "/usr/local/bin/vllm"), false},
		"a module run with -m":                   {with("python3", "-m", "vllm"), false},
		"a name that only ends with it":          {with("/usr/local/bin/xvllm"), false},
		"bound paths rewritten (a stand-in's)":   {[]string{"/t/alpha.test", "serve", "/tmp/c/model", "--port=8000", "--uds=/tmp/r/engine.sock"}, false},
		"another model":                          {[]string{"vllm", "serve", "/hpcburst/model2", "--port=8000", "--uds=/hpcburst/run/engine.sock"}, false},
		"another port":                           {[]string{"python3", "/usr/local/bin/vllm", "serve", "/hpcburst/model", "--port=8001", "--uds=/hpcburst/run/engine.sock"}, false},
		"another socket":                         {[]string{"vllm", "serve", "/hpcburst/model", "--port=8000", "--uds=/hpcburst/run/other.sock"}, false},
		"an argument missing":                    {[]string{"python3", "/usr/local/bin/vllm", "serve", "/hpcburst/model", "--port=8000"}, false},
		"one more at the end":                    {append([]string{"vllm"}, append(append([]string(nil), args...), "--x")...), false},
		"no program before":                      {args, false},
		"another subcommand":                     {[]string{"python3", "/usr/local/bin/vllm", "bench", "/hpcburst/model", "--port=8000", "--uds=/hpcburst/run/engine.sock"}, false},
	} {
		if got := MatchesCommand(c.argv, cmd); got != c.want {
			t.Errorf("%s: MatchesCommand(%q) = %v, want %v", name, c.argv, got, c.want)
		}
	}
	if MatchesCommand([]string{"vllm"}, []string{"vllm"}) {
		t.Error("a command with no argument after its program matched")
	}
	// A launch whose program is a path matches that path alone.
	abs := []string{"/opt/engine/bin/vllm", "serve"}
	if !MatchesCommand([]string{"/opt/engine/bin/vllm", "serve"}, abs) || MatchesCommand([]string{"/usr/local/bin/vllm", "serve"}, abs) {
		t.Error("a launch by its absolute path")
	}
}

func TestPickEngine(t *testing.T) {
	cmd := []string{"vllm", "serve", "/hpcburst/model"}
	leader := NodeProc{PID: 10, PPID: 5, InJob: true, Argv: []string{"/usr/bin/apptainer", "exec", "img.sif", "vllm", "serve", "/hpcburst/model"}}
	engine := NodeProc{PID: 11, PPID: 10, Argv: []string{"python3", "/usr/local/bin/vllm", "serve", "/hpcburst/model"}}
	fuse := NodeProc{PID: 12, PPID: 10, Argv: []string{"squashfuse_ll", "img.sif", "/var/mnt"}}
	// Another child of the job's Apptainer with the engine's arguments, but another program (review of PR 255, B2).
	lookalike := NodeProc{PID: 14, PPID: 10, Argv: []string{"/usr/local/bin/other-tool", "serve", "/hpcburst/model"}}
	agent := NodeProc{PID: 5, PPID: 1, InJob: true, Argv: []string{"/scratch/stage/hb-agent"}}
	if p, err := PickEngine([]NodeProc{agent, leader, fuse, lookalike, engine}, cmd, MatchesCommand); err != nil || p.PID != 11 {
		t.Errorf("one engine beside a fuse mount and a lookalike: %+v %v, want PID 11", p, err)
	}
	for name, c := range map[string]struct {
		procs []NodeProc
		want  int
	}{
		"no engine":                 {[]NodeProc{agent, leader, fuse}, 0},
		"only the lookalike":        {[]NodeProc{agent, leader, lookalike}, 0},
		"two engines":               {[]NodeProc{agent, leader, engine, {PID: 13, PPID: 10, Argv: engine.Argv}}, 2},
		"the leader not in the job": {[]NodeProc{agent, {PID: 10, PPID: 5, Argv: leader.Argv}, engine}, 0},
		"the leader not Apptainer":  {[]NodeProc{agent, {PID: 10, PPID: 5, InJob: true, Argv: []string{"/bin/bash", "run.sh"}}, engine}, 0},
		"no parent listed":          {[]NodeProc{agent, engine}, 0},
		"an engine of another job":  {[]NodeProc{{PID: 20, PPID: 1, Argv: leader.Argv}, {PID: 21, PPID: 20, Argv: engine.Argv}}, 0},
	} {
		_, err := PickEngine(c.procs, cmd, MatchesCommand)
		var ec ErrEngineCount
		if !errors.As(err, &ec) || ec.N != c.want {
			t.Errorf("%s: PickEngine returned %v, want ErrEngineCount{%d}", name, err, c.want)
		}
	}
	for _, title := range [][]string{{"Apptainer runtime parent"}, {"/usr/libexec/apptainer/bin/starter-suid", "x"}, {"/opt/apptainer-1.3/bin/apptainer"}} {
		l := leader
		l.Argv = title
		if p, err := PickEngine([]NodeProc{l, engine}, cmd, MatchesCommand); err != nil || p.PID != 11 {
			t.Errorf("a leader %q: %+v %v, want PID 11", title, p, err)
		}
	}
}

// The listing's job membership over a process tree of the test's own, so that the cgroup is the test's to
// write: a process whose environment does not carry the job's SLURM_JOB_ID is still of the job when its cgroup
// holds the segment job_<id>, under v2 (one line) and v1 (a line per controller); job_<id> as the prefix of a
// longer ID is not the job.
func TestTheListingFindsTheJobByItsCgroupToo(t *testing.T) {
	root := t.TempDir()
	stat := func(pid, ppid int) string {
		return fmt.Sprintf("%d (x) S %d %s 4242\n", pid, ppid, strings.TrimSpace(strings.Repeat("0 ", 17)))
	}
	write := func(pid, ppid int, env, cgroup string, argv ...string) {
		d := filepath.Join(root, strconv.Itoa(pid))
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatal(err)
		}
		for f, b := range map[string]string{"stat": stat(pid, ppid), "environ": env, "cgroup": cgroup, "cmdline": strings.Join(argv, "\x00") + "\x00"} {
			if err := os.WriteFile(filepath.Join(d, f), []byte(b), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	write(101, 1, "SLURM_JOB_ID=4321\x00", "0::/user.slice\n", "/scratch/stage/hb-agent")
	write(102, 101, "PATH=/usr/bin\x00", "0::/system.slice/slurmstepd.scope/job_4321/step_0/user/task_0\n", "Apptainer runtime parent")
	write(103, 101, "", "12:memory:/slurm/uid_1000/job_4321/step_0\n4:cpu:/slurm/uid_1000/job_4321/step_0\n", "/usr/bin/apptainer", "exec")
	write(104, 1, "", "0::/system.slice/slurmstepd.scope/job_43210/step_0\n", "/usr/bin/apptainer", "exec")
	write(105, 1, "SLURM_JOB_ID=43210\x00", "0::/user.slice\n", "/usr/bin/apptainer", "exec")
	procs, err := ParseListing(runScript(t, listScript(root, "4321")))
	if err != nil {
		t.Fatal(err)
	}
	got := map[int]bool{}
	for _, p := range procs {
		got[p.PID] = p.InJob
	}
	want := map[int]bool{101: true, 102: true, 103: true, 104: false, 105: false}
	for pid, in := range want {
		if v, ok := got[pid]; !ok || v != in {
			t.Errorf("process %d: listed %v, of the job %v; want of the job %v", pid, ok, v, in)
		}
	}
}

// jobTree starts a process tree shaped like a job's on this host: a process
// run as "apptainer" with SLURM_JOB_ID=job in its environment, and its child,
// the engine: a Python console script named vllm, run through its shebang as
// a real engine is (python3 <dir>/vllm args...). With lookalike, the
// apptainer has a second child with the same arguments, another program
// (review of PR 255, B2). It returns the engine's PID and stops the tree at
// the end of the test.
func jobTree(t *testing.T, job string, lookalike bool, args ...string) (leader *exec.Cmd, engine int) {
	t.Helper()
	if _, err := exec.LookPath("python3"); err != nil {
		t.Fatalf("the worker fault's kill needs python3 (its pidfd): %v", err)
	}
	dir := t.TempDir()
	app := filepath.Join(dir, "apptainer")
	if err := os.Symlink("/bin/bash", app); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"vllm", "other-tool"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/usr/bin/env python3\nimport time\ntime.sleep(300)\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	pidFile := filepath.Join(dir, "engine.pid")
	quoted := make([]string, len(args))
	for i, a := range args {
		quoted[i] = "'" + a + "'"
	}
	script := "'" + filepath.Join(dir, "vllm") + "' " + strings.Join(quoted, " ") + " & echo $! > " + pidFile + "; "
	if lookalike {
		script += "'" + filepath.Join(dir, "other-tool") + "' " + strings.Join(quoted, " ") + " & "
	}
	script += "wait; exec sleep 300"
	leader = exec.Command(app, "-c", script)
	leader.Env = append(os.Environ(), "SLURM_JOB_ID="+job)
	leader.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := leader.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = syscall.Kill(-leader.Process.Pid, syscall.SIGKILL)
		_ = leader.Wait()
	})
	deadline := time.Now().Add(30 * time.Second)
	for {
		b, err := os.ReadFile(pidFile)
		if n, perr := strconv.Atoi(strings.TrimSpace(string(b))); err == nil && perr == nil && n > 0 {
			engine = n
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the engine's PID was not written within 30 s")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// The child is listed once its arguments are its own: the interpreter has run, not only forked.
	want := "/vllm\x00" + strings.Join(args, "\x00")
	for {
		b, _ := os.ReadFile("/proc/" + strconv.Itoa(engine) + "/cmdline")
		if strings.Contains(string(b), want) {
			return leader, engine
		}
		if time.Now().After(deadline) {
			t.Fatal("the engine did not start within 30 s")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func runScript(t *testing.T, script string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash", "-s")
	cmd.Stdin = strings.NewReader(script)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("bash -s: %v", err)
	}
	return out
}

func TestTheScriptsKillExactlyTheEngineOfTheJob(t *testing.T) {
	job := strconv.Itoa(900000000 + os.Getpid())
	launch := []string{"vllm", "serve", "/hpcburst/model", "--port=8000"}
	leader, engine := jobTree(t, job, true, "serve", "/hpcburst/model", "--port=8000")
	// A second tree with the same command in ANOTHER job: never chosen, never killed.
	_, other := jobTree(t, job+"1", false, "serve", "/hpcburst/model", "--port=8000")

	procs, err := ParseListing(runScript(t, ListScript(job)))
	if err != nil {
		t.Fatal(err)
	}
	p, err := PickEngine(procs, launch, MatchesCommand)
	if err != nil || p.PID != engine {
		t.Fatalf("PickEngine = %+v %v, want the engine %d", p, err, engine)
	}
	// A process that is no longer the one listed is left alone.
	stale := p
	stale.Start = "1"
	if out := runScript(t, KillScript(stale)); strings.TrimSpace(string(out)) != "changed" || Killed(out, stale) {
		t.Errorf("a stale start time: %q, want changed", out)
	}
	if syscall.Kill(engine, 0) != nil {
		t.Fatal("the engine died from a kill script that should have refused")
	}
	out := runScript(t, KillScript(p))
	if !Killed(out, p) {
		t.Fatalf("KillScript printed %q, want killed %d", out, engine)
	}
	deadline := time.Now().Add(30 * time.Second)
	for syscall.Kill(engine, 0) == nil {
		b, _ := os.ReadFile(fmt.Sprintf("/proc/%d/stat", engine))
		if f := strings.Fields(string(b)); len(f) > 2 && f[2] == "Z" {
			break // killed, not yet reaped by its parent
		}
		if time.Now().After(deadline) {
			t.Fatal("the engine is alive 30 s after its kill")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if syscall.Kill(other, 0) != nil {
		t.Error("the engine of another job died")
	}
	// The job's apptainer reaped its engine and lives on (its script sleeps after the wait).
	b, _ := os.ReadFile(fmt.Sprintf("/proc/%d/stat", leader.Process.Pid))
	if f := strings.Fields(string(b)); len(f) < 3 || f[2] == "Z" {
		t.Error("the job's apptainer died: the kill touched more than the engine")
	}
	if out := runScript(t, KillScript(p)); strings.TrimSpace(string(out)) == "killed "+strconv.Itoa(engine) {
		t.Errorf("a second kill of the reaped engine printed %q", out)
	}
}

// raceHarness runs KillHelper's main as a module, in python3, with the seams of the race tests: after its checks
// and before its signal, the engine (argv[2]) exits and its parent reaps it, so its PID is free; from then on a
// signal sent by that NUMBER reaches the victim (argv[5]) instead, as it would once the kernel gave the free PID to
// the victim. This host cannot make the kernel reuse a given PID (unprivileged user namespaces, and with them
// ns_last_pid, are closed here, and pid_max is 4,194,304), so the reuse stands at the one place a kill by number
// would meet it, os.kill. A kill through the pidfd never goes there. With "pidfd-of-victim" the helper's pidfd is
// opened on the victim instead: a pidfd that does not name the process at the PID. With "no-pidfd" opening one fails
// as on a kernel without pidfd_open.
const raceHarness = `import importlib.util, os, signal, sys, time
spec = importlib.util.spec_from_file_location("killhelper", sys.argv[1])
kh = importlib.util.module_from_spec(spec)
spec.loader.exec_module(kh)
pid, start, cmd, victim, mode = int(sys.argv[2]), sys.argv[3], sys.argv[4], int(sys.argv[5]), sys.argv[6]
real_kill, real_open = os.kill, kh.pidfd_open
def after_check():
    real_kill(pid, signal.SIGKILL)
    deadline = time.time() + 30
    while os.path.exists("/proc/%d" % pid):
        if time.time() > deadline:
            print("the engine was not reaped")
            sys.exit(1)
        time.sleep(0.01)
    os.kill = lambda p, s: real_kill(victim if p == pid else p, s)
if mode == "pidfd-of-victim":
    kh.pidfd_open = lambda p: real_open(victim)
if mode == "no-pidfd":
    def nopidfd(p):
        raise OSError(38, "Function not implemented")
    kh.pidfd_open = nopidfd
print(kh.main([str(pid), start, cmd], after_check if mode == "exit-then-reuse" else None))
`

// The kill goes through a pidfd, never by the PID's number (review of PR 255, B1). Deterministically: the
// engine passes the checks, then exits and is reaped, and its PID's number names another process of this user
// (the victim, raceHarness). The kill must say "gone" and leave the victim alive: the pidfd still names the
// engine, which is gone. A pidfd that names another process than the one at the PID is refused, and a node that
// cannot open a pidfd, or has no python3, kills nothing.
func TestTheKillGoesThroughAPidfdNotThePIDsNumber(t *testing.T) {
	job := strconv.Itoa(910000000 + os.Getpid())
	launch := []string{"vllm", "serve", "/hpcburst/model", "--port=8000"}
	dir := t.TempDir()
	helper, harness := filepath.Join(dir, "killhelper.py"), filepath.Join(dir, "harness.py")
	if err := os.WriteFile(helper, []byte(KillHelper), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(harness, []byte(raceHarness), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		mode, want   string
		engineKilled bool
	}{
		{"exit-then-reuse", "gone", true},
		{"pidfd-of-victim", "gone", false},
		{"no-pidfd", "no-pidfd", false},
	} {
		t.Run(c.mode, func(t *testing.T) {
			_, engine := jobTree(t, job, false, "serve", "/hpcburst/model", "--port=8000")
			procs, err := ParseListing(runScript(t, ListScript(job)))
			if err != nil {
				t.Fatal(err)
			}
			p, err := PickEngine(procs, launch, MatchesCommand)
			if err != nil || p.PID != engine {
				t.Fatalf("PickEngine = %+v %v, want the engine %d", p, err, engine)
			}
			victim := exec.Command("sleep", "300")
			if err := victim.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = victim.Process.Kill(); _ = victim.Wait() })
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			out, err := exec.CommandContext(ctx, "python3", "-I", harness, helper, strconv.Itoa(p.PID), p.Start, p.cmdline, strconv.Itoa(victim.Process.Pid), c.mode).CombinedOutput()
			if err != nil || strings.TrimSpace(string(out)) != c.want {
				t.Fatalf("the helper printed %q (%v), want %q", out, err, c.want)
			}
			if syscall.Kill(victim.Process.Pid, 0) != nil {
				t.Fatal("the victim died: a signal reached a process that is not the engine")
			}
			if alive := syscall.Kill(engine, 0) == nil; alive == c.engineKilled {
				t.Errorf("the engine alive %v, want killed %v", alive, c.engineKilled)
			}
		})
	}
	// No python3 on the node: nothing is killed.
	_, engine := jobTree(t, job+"2", false, "serve", "/hpcburst/model", "--port=8000")
	procs, err := ParseListing(runScript(t, ListScript(job+"2")))
	if err != nil {
		t.Fatal(err)
	}
	p, err := PickEngine(procs, launch, MatchesCommand)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("/bin/bash", "-s")
	cmd.Env = []string{"PATH=" + filepath.Join(dir, "empty")}
	cmd.Stdin = strings.NewReader(KillScript(p))
	if out, err := cmd.Output(); err != nil || strings.TrimSpace(string(out)) != "no-pidfd" || Killed(out, p) {
		t.Errorf("with no python3: %q %v, want no-pidfd", out, err)
	}
	if syscall.Kill(engine, 0) != nil {
		t.Error("the engine died on a node with no python3")
	}
}

func TestRestartTimesLine(t *testing.T) {
	k := time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC)
	r := RestartTimes{Killed: k, AgentNoticed: k.Add(4 * time.Second), Restarted: k.Add(5 * time.Second), ReadyAgain: k.Add(70 * time.Second)}
	want := "worker-rung restart, from the engine's kill: the exit shown 4s, the restart decided 5s, ready again 1m10s"
	if r.Line() != want {
		t.Errorf("Line() = %q, want %q", r.Line(), want)
	}
	if l := (RestartTimes{Killed: k}).Line(); !strings.Contains(l, "the exit shown not seen") || !strings.Contains(l, "ready again not seen") {
		t.Errorf("unseen steps: %q", l)
	}
}
