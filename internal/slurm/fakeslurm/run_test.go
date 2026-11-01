package fakeslurm

// New. Tests of run mode (doc.go, Run mode). The expected values come from
// issue #146's criteria (the states of real Slurm), from Slurm's documented
// behaviour (KillWait, --hold, the job's environment) and from RunMode's
// documented layout; none is read back from the fake. Every wait is on
// observable state (a file the job writes, the job's snapshot, the kernel's
// answer for a process group) with a generous deadline, never a fixed sleep.

import (
	"crypto/sha256"
	"encoding/hex"
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

// The batch script runs under /bin/sh, leading its own process group (and
// session), apart from the test's; the job is RUNNING while it runs and
// COMPLETED once it exits 0, after which the group is gone.
func TestRunModeRunsWrapUnderShInItsOwnGroup(t *testing.T) {
	r := newRunRig(t, RunMode{})
	pidFile, goFile := r.home+"/pid", r.home+"/go"
	id := r.submit("echo $$ > "+pidFile+"; "+waitLoop(goFile), "--job-name=example-run")
	j := r.job(id)
	if j.State != Running || j.PID <= 0 {
		t.Fatalf("after sbatch: state %s, PID %d; want RUNNING with a PID (a job that is not held starts at once)", j.State, j.PID)
	}
	if got := strings.TrimSpace(r.waitFile(pidFile)); got != itoa(j.PID) {
		t.Fatalf("the job's $$ is %s, the recorded PID %d", got, j.PID)
	}
	if pgrp, sid := statField(t, j.PID, 5), statField(t, j.PID, 6); pgrp != j.PID || sid != j.PID {
		t.Errorf("the batch shell's process group %d and session %d, want both %d (its own)", pgrp, sid, j.PID)
	}
	if j.PID == syscall.Getpgrp() {
		t.Errorf("the job shares the test's process group")
	}
	exe, err := os.Readlink("/proc/" + itoa(j.PID) + "/exe")
	sh, serr := filepath.EvalSymlinks("/bin/sh")
	if err != nil || serr != nil || exe != sh {
		t.Errorf("the batch shell runs %q (%v), want /bin/sh, which is %q (%v)", exe, err, sh, serr)
	}
	if got := r.squeueJob(id)["job_state"]; !equalStrings(got, Running) {
		t.Errorf("squeue job_state %v while it runs, want [RUNNING]", got)
	}
	if err := os.WriteFile(goFile, []byte("go"), 0o644); err != nil {
		t.Fatal(err)
	}
	j = r.waitFinished(id)
	if j.State != Completed || j.ExitCode != 0 {
		t.Errorf("after exit 0: state %s, exit code %d; want COMPLETED, 0", j.State, j.ExitCode)
	}
	waitGroupGone(t, j.PID)
}

func equalStrings(v any, want ...string) bool {
	l, ok := v.([]any)
	if !ok || len(l) != len(want) {
		return false
	}
	for i, x := range l {
		if s, _ := x.(string); s != want[i] {
			return false
		}
	}
	return true
}

// envOf parses the output of env into a map.
func envOf(s string) map[string]string {
	m := map[string]string{}
	for _, l := range strings.Split(s, "\n") {
		if k, v, ok := strings.Cut(l, "="); ok {
			m[k] = v
		}
	}
	return m
}

// The job's environment: the login's own (sbatch exports it), RunMode.Env,
// SLURM_JOB_ID and the other Slurm variables, a fresh TMPDIR, and each job
// variable: MEMFS only for a job that requests -C memfs, a variable with no
// feature in every job. The directories exist while the job runs, are gone
// when it ends (Helios frees $MEMFS), and the job starts in HOME.
func TestRunModeJobEnvironment(t *testing.T) {
	jobDir := filepath.Join(t.TempDir(), "j")
	r := newRunRig(t, RunMode{
		JobDir:  jobDir,
		Env:     map[string]string{"EXAMPLE_SITE": "example-value"},
		JobVars: []JobVar{{Name: "MEMFS", Feature: "memfs"}, {Name: "EXAMPLE_LOCAL"}},
	})
	wrap := func(out string) string {
		return "env > " + out + ".tmp; pwd >> " + out + ".tmp; " +
			"[ -d \"$TMPDIR\" ] && [ -d \"$EXAMPLE_LOCAL\" ] && { [ -z \"$MEMFS\" ] || [ -d \"$MEMFS\" ]; } && echo dirs-ok >> " + out + ".tmp; " +
			"mv " + out + ".tmp " + out
	}
	withMemfs := r.submit(wrap(r.home+"/env-memfs"), "--job-name=example-env", "--partition=example-part",
		"--account=example-account", "--cpus-per-task=4", "--mem=2048M", "--constraint=memfs")
	without := r.submit(wrap(r.home + "/env-plain"))
	for _, c := range []struct {
		id    int
		file  string
		memfs bool
	}{{withMemfs, "env-memfs", true}, {without, "env-plain", false}} {
		out := r.waitFile(r.home + "/" + c.file)
		env, lines := envOf(out), strings.Split(strings.TrimSpace(out), "\n")
		dir := filepath.Join(jobDir, itoa(c.id))
		want := map[string]string{
			"SLURM_JOB_ID": itoa(c.id), "SLURM_JOBID": itoa(c.id), "TMPDIR": dir + "/tmp",
			"EXAMPLE_LOCAL": dir + "/example_local", "EXAMPLE_SITE": "example-value",
			"HOME": r.home, "SCRATCH": filepath.Join(r.root, "scratch", "example-user"),
			"SLURM_SUBMIT_DIR": r.home, "SLURM_JOB_USER": "example-user", "PATH": "/usr/local/bin:/usr/bin:/bin",
		}
		if c.memfs {
			want["MEMFS"] = dir + "/memfs"
			want["SLURM_JOB_NAME"], want["SLURM_JOB_PARTITION"], want["SLURM_JOB_ACCOUNT"] = "example-env", "example-part", "example-account"
			want["SLURM_CPUS_PER_TASK"], want["SLURM_MEM_PER_NODE"] = "4", "2048"
		}
		for k, v := range want {
			if env[k] != v {
				t.Errorf("job %d: %s=%q, want %q", c.id, k, env[k], v)
			}
		}
		if v, ok := env["MEMFS"]; !c.memfs && ok {
			t.Errorf("job %d without -C memfs has MEMFS=%q, want it unset", c.id, v)
		}
		if _, ok := env["SLURM_RESTART_COUNT"]; ok {
			t.Errorf("job %d: SLURM_RESTART_COUNT is set for a job never restarted", c.id)
		}
		if n := len(lines); n < 2 || lines[n-1] != "dirs-ok" || lines[n-2] != r.home {
			t.Errorf("job %d: last lines %q, want the working directory %s then dirs-ok", c.id, lines[max(0, n-2):], r.home)
		}
		r.waitFinished(c.id)
		if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("job %d's directory %s after the job: %v, want it removed", c.id, dir, err)
		}
	}
}

// The exit code gives the end state: COMPLETED for 0, FAILED otherwise,
// including a batch shell that a signal from elsewhere ended. The JSON
// replies carry the state as they do outside run mode.
func TestRunModeExitCodeGivesTheEndState(t *testing.T) {
	for _, c := range []struct {
		name, wrap string
		state      string
		code       int
		sig        syscall.Signal
		exitStatus string
	}{
		{"exit 0", "exit 0", Completed, 0, 0, "SUCCESS"},
		{"exit 3", "exit 3", Failed, 3, 0, "ERROR"},
		{"killed", "kill -KILL $$", Failed, -1, syscall.SIGKILL, "ERROR"},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newRunRig(t, RunMode{})
			id := r.submit(c.wrap)
			j := r.waitFinished(id)
			if j.State != c.state || j.ExitCode != c.code || j.ExitSignal != c.sig {
				t.Errorf("state %s, exit code %d, signal %v; want %s, %d, %v", j.State, j.ExitCode, j.ExitSignal, c.state, c.code, c.sig)
			}
			sq := r.squeueJob(id)
			if !equalStrings(sq["job_state"], c.state) {
				t.Errorf("squeue job_state %v, want [%s]", sq["job_state"], c.state)
			}
			if st := sq["exit_code"].(map[string]any)["status"]; !equalStrings(st, c.exitStatus) {
				t.Errorf("squeue exit_code.status %v, want [%s]", st, c.exitStatus)
			}
			js := jobsOf(t, r.sacct("-j", itoa(id)))
			if len(js) != 1 || !equalStrings(js[0]["state"].(map[string]any)["current"], c.state) {
				t.Errorf("sacct -j %d: %v, want one record in state %s", id, js, c.state)
			}
		})
	}
}

// A held job has no process until scontrol release; an admin hold is not
// lifted by the user, so its job stays without one.
func TestRunModeHeldJobRunsOnlyWhenReleased(t *testing.T) {
	r := newRunRig(t, RunMode{})
	ran := r.home + "/ran"
	id := r.submit("echo ran > "+ran, "--hold")
	admin := r.submit("echo ran > "+ran+"-admin", "--hold")
	if err := r.p.HoldAdmin(admin); err != nil {
		t.Fatal(err)
	}
	for _, x := range []int{id, admin} {
		if j := r.job(x); j.State != Pending || !j.Held || j.PID != 0 {
			t.Fatalf("job %d after sbatch --hold: state %s, held %v, PID %d; want PENDING, held, no process", x, j.State, j.Held, j.PID)
		}
	}
	if _, err := os.Stat(ran); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the held job's script ran: %v", err)
	}
	if _, _, code := r.run("scontrol release " + itoa(admin)); code != 1 {
		t.Errorf("scontrol release of an admin hold: exit %d, want 1", code)
	}
	if j := r.job(admin); j.PID != 0 || j.State != Pending {
		t.Errorf("the admin-held job after the user's release: state %s, PID %d; want PENDING, no process", j.State, j.PID)
	}
	r.must("scontrol release " + itoa(id))
	if j := r.job(id); j.PID <= 0 {
		t.Fatalf("after scontrol release: PID %d, want the job started", j.PID)
	}
	if got := r.waitFile(ran); got != "ran\n" {
		t.Errorf("the released job wrote %q", got)
	}
	if j := r.waitFinished(id); j.State != Completed {
		t.Errorf("the released job ended %s, want COMPLETED", j.State)
	}
}

// Start is refused in run mode: the fake starts a job itself.
func TestRunModeRefusesStart(t *testing.T) {
	r := newRunRig(t, RunMode{})
	id := r.submit("true", "--hold")
	if err := r.p.Start(id); err == nil {
		t.Errorf("Start in run mode: no error, want a refusal")
	}
}

// The login node's file commands work on the real directory, so the job sees
// what they wrote: modes as the umask gives them, the real SHA-256, a rename,
// a removal. Paths outside the root read as missing and refuse writes.
func TestRunModeFileCommandsUseTheDisk(t *testing.T) {
	r := newRunRig(t, RunMode{})
	dir := r.home + "/stage"
	content := []byte("#!/bin/sh\necho staged\n")
	if _, errOut, code := r.runAs(r.s, "umask 077 && mkdir -p "+dir+" && cat > "+dir+"/f.tmp", content); code != 0 {
		t.Fatalf("write: exit %d, %s", code, errOut)
	}
	if fi, err := os.Stat(dir + "/f.tmp"); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("the written file: %v, %v; want mode 600", fi, err)
	}
	if fi, err := os.Stat(dir); err != nil || fi.Mode().Perm() != 0o700 {
		t.Fatalf("the made directory: %v, %v; want mode 700", fi, err)
	}
	r.must("chmod 755 " + dir + "/f.tmp && mv -f " + dir + "/f.tmp " + dir + "/f")
	if b, err := os.ReadFile(dir + "/f"); err != nil || string(b) != string(content) {
		t.Fatalf("after mv: %q, %v", b, err)
	}
	if fi, _ := os.Stat(dir + "/f"); fi.Mode().Perm() != 0o755 {
		t.Errorf("mode after chmod 755: %o", fi.Mode().Perm())
	}
	sum := sha256.Sum256(content)
	if out := r.must("sha256sum " + dir + "/f"); out != hex.EncodeToString(sum[:])+"  "+dir+"/f\n" {
		t.Errorf("sha256sum: %q", out)
	}
	r.must("test -f " + dir + "/f")
	id := r.submit(dir + "/f > " + r.home + "/out")
	r.waitFinished(id)
	if b, _ := os.ReadFile(r.home + "/out"); string(b) != "staged\n" {
		t.Errorf("the job ran the staged file and wrote %q", b)
	}
	if _, errOut, code := r.run("cat > " + r.home + "/missing/f"); code != 1 || !strings.Contains(errOut, "No such file or directory") {
		t.Errorf("cat into a missing directory: exit %d, %q", code, errOut)
	}
	if _, errOut, code := r.run("mkdir -p " + dir + "/f/sub"); code != 1 || !strings.Contains(errOut, "File exists") {
		t.Errorf("mkdir -p through a file: exit %d, %q", code, errOut)
	}
	r.must("rm -rf " + dir)
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("rm -rf left %s: %v", dir, err)
	}

	outside := t.TempDir() // a sibling of the root, outside it
	if err := os.WriteFile(outside+"/keep", []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{"rm -rf " + outside, "mkdir -p " + outside + "/new", "cat > " + outside + "/new-file", "chmod 600 " + outside + "/keep"} {
		if _, _, code := r.run(line); code != 1 {
			t.Errorf("%q outside the root: exit %d, want 1", line, code)
		}
	}
	if _, _, code := r.run("test -f " + outside + "/keep"); code != 1 {
		t.Errorf("test -f outside the root: exit %d, want 1 (missing)", code)
	}
	if fi, err := os.Stat(outside + "/keep"); err != nil || fi.Mode().Perm() != 0o644 {
		t.Errorf("the file outside the root after the commands: %v, %v; want it untouched", fi, err)
	}
	for _, p := range []string{outside + "/new", outside + "/new-file"} {
		if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s was made outside the root: %v", p, err)
		}
	}
}

// A job whose --output Slurm cannot open never runs and ends FAILED; the
// snapshot says why.
func TestRunModeUnopenableOutputFails(t *testing.T) {
	r := newRunRig(t, RunMode{})
	id := r.submit("echo ran > "+r.home+"/ran", "--output="+r.home+"/missing/job.out")
	j := r.job(id)
	if j.State != Failed || !j.Finished || j.StartErr == nil || j.PID != 0 {
		t.Errorf("state %s, finished %v, StartErr %v, PID %d; want FAILED, finished, an error, no process", j.State, j.Finished, j.StartErr, j.PID)
	}
	if _, err := os.Stat(r.home + "/ran"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the script ran: %v", err)
	}
}

// RunMode values the fake cannot use panic in New, as a test bug.
func TestRunModeRefusesABadConfig(t *testing.T) {
	root := t.TempDir()
	for name, mode := range map[string]RunMode{
		"relative root":        {Root: "relative"},
		"missing root":         {Root: root + "/missing"},
		"a job var for TMPDIR": {Root: root, JobVars: []JobVar{{Name: "TMPDIR"}}},
		"a job var on tmp":     {Root: root, JobVars: []JobVar{{Name: "TMP"}}},
		"a duplicate job var":  {Root: root, JobVars: []JobVar{{Name: "MEMFS"}, {Name: "memfs"}}},
		"a SLURM_ job var":     {Root: root, JobVars: []JobVar{{Name: "SLURM_JOB_ID"}}},
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("New accepted %+v", mode)
				}
			}()
			New(fake.NewClock(t0), Config{Run: &mode})
		})
	}
}
