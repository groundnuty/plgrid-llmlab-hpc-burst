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

func TestMatchesCommand(t *testing.T) {
	cmd := []string{"vllm", "serve", "/hpcburst/model", "--port=8000", "--uds=/hpcburst/run/engine.sock"}
	for name, c := range map[string]struct {
		argv []string
		want bool
	}{
		"through an interpreter":   {[]string{"/usr/bin/python3", "/usr/local/bin/vllm", "serve", "/hpcburst/model", "--port=8000", "--uds=/hpcburst/run/engine.sock"}, true},
		"bound paths rewritten":    {[]string{"/t/alpha.test", "serve", "/tmp/c/model", "--port=8000", "--uds=/tmp/r/engine.sock"}, true},
		"another port":             {[]string{"python3", "vllm", "serve", "/hpcburst/model", "--port=8001", "--uds=/hpcburst/run/engine.sock"}, false},
		"an argument missing":      {[]string{"python3", "vllm", "serve", "/hpcburst/model", "--port=8000"}, false},
		"one more at the end":      {[]string{"python3", "vllm", "serve", "/hpcburst/model", "--port=8000", "--uds=/hpcburst/run/engine.sock", "--x"}, false},
		"no program before":        {[]string{"serve", "/hpcburst/model", "--port=8000", "--uds=/hpcburst/run/engine.sock"}, false},
		"a path flag renamed":      {[]string{"python3", "vllm", "serve", "/hpcburst/model", "--port=8000", "--socket=/hpcburst/run/engine.sock"}, false},
		"a path for a plain value": {[]string{"python3", "vllm", "serve", "/hpcburst/model", "--port=/8000", "--uds=/hpcburst/run/engine.sock"}, false},
		"another subcommand":       {[]string{"python3", "vllm", "bench", "/hpcburst/model", "--port=8000", "--uds=/hpcburst/run/engine.sock"}, false},
	} {
		if got := MatchesCommand(c.argv, cmd); got != c.want {
			t.Errorf("%s: MatchesCommand = %v, want %v", name, got, c.want)
		}
	}
	if MatchesCommand([]string{"vllm"}, []string{"vllm"}) {
		t.Error("a command with no argument after its program matched")
	}
}

func TestPickEngine(t *testing.T) {
	cmd := []string{"vllm", "serve", "/hpcburst/model"}
	leader := NodeProc{PID: 10, PPID: 5, InJob: true, Argv: []string{"/usr/bin/apptainer", "exec", "img.sif", "vllm", "serve", "/hpcburst/model"}}
	engine := NodeProc{PID: 11, PPID: 10, Argv: []string{"python3", "/usr/local/bin/vllm", "serve", "/hpcburst/model"}}
	fuse := NodeProc{PID: 12, PPID: 10, Argv: []string{"squashfuse_ll", "img.sif", "/var/mnt"}}
	agent := NodeProc{PID: 5, PPID: 1, InJob: true, Argv: []string{"/scratch/stage/hb-agent"}}
	if p, err := PickEngine([]NodeProc{agent, leader, fuse, engine}, cmd); err != nil || p.PID != 11 {
		t.Errorf("one engine beside a fuse mount: %+v %v, want PID 11", p, err)
	}
	for name, c := range map[string]struct {
		procs []NodeProc
		want  int
	}{
		"no engine":                 {[]NodeProc{agent, leader, fuse}, 0},
		"two engines":               {[]NodeProc{agent, leader, engine, {PID: 13, PPID: 10, Argv: engine.Argv}}, 2},
		"the leader not in the job": {[]NodeProc{agent, {PID: 10, PPID: 5, Argv: leader.Argv}, engine}, 0},
		"the leader not Apptainer":  {[]NodeProc{agent, {PID: 10, PPID: 5, InJob: true, Argv: []string{"/bin/bash", "run.sh"}}, engine}, 0},
		"no parent listed":          {[]NodeProc{agent, engine}, 0},
		"an engine of another job":  {[]NodeProc{{PID: 20, PPID: 1, Argv: leader.Argv}, {PID: 21, PPID: 20, Argv: engine.Argv}}, 0},
	} {
		_, err := PickEngine(c.procs, cmd)
		var ec ErrEngineCount
		if !errors.As(err, &ec) || ec.N != c.want {
			t.Errorf("%s: PickEngine returned %v, want ErrEngineCount{%d}", name, err, c.want)
		}
	}
	for _, title := range [][]string{{"Apptainer runtime parent"}, {"/usr/libexec/apptainer/bin/starter-suid", "x"}, {"/opt/apptainer-1.3/bin/apptainer"}} {
		l := leader
		l.Argv = title
		if p, err := PickEngine([]NodeProc{l, engine}, cmd); err != nil || p.PID != 11 {
			t.Errorf("a leader %q: %+v %v, want PID 11", title, p, err)
		}
	}
}

// jobTree starts a process tree shaped like a job's on this host: a process
// run as "apptainer" with SLURM_JOB_ID=job in its environment, whose child's
// arguments end with args. It returns the child's PID and stops the tree at
// the end of the test.
func jobTree(t *testing.T, job string, args ...string) (leader *exec.Cmd, engine int) {
	t.Helper()
	dir := t.TempDir()
	app := filepath.Join(dir, "apptainer")
	if err := os.Symlink("/bin/bash", app); err != nil {
		t.Fatal(err)
	}
	pidFile := filepath.Join(dir, "engine.pid")
	quoted := make([]string, len(args))
	for i, a := range args {
		quoted[i] = "'" + a + "'"
	}
	script := "bash -c 'sleep 300; true' engine " + strings.Join(quoted, " ") + " & echo $! > " + pidFile + "; wait; exec sleep 300"
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
	// The child is listed once its arguments are its own: bash has run, not only forked.
	for {
		b, _ := os.ReadFile("/proc/" + strconv.Itoa(engine) + "/cmdline")
		if strings.Contains(string(b), "sleep 300; true") {
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
	leader, engine := jobTree(t, job, "serve", "/hpcburst/model", "--port=8000")
	// A second tree with the same command in ANOTHER job: never chosen, never killed.
	_, other := jobTree(t, job+"1", "serve", "/hpcburst/model", "--port=8000")

	procs, err := ParseListing(runScript(t, ListScript(job)))
	if err != nil {
		t.Fatal(err)
	}
	p, err := PickEngine(procs, launch)
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
