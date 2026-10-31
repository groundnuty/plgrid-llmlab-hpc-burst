package sshrun

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

func TestMain(m *testing.M) {
	if os.Getenv(fakeEnv) == "1" {
		os.Exit(fakeSSHMain(os.Args[1:]))
	}
	os.Exit(m.Run())
}

var exampleConfig = slurm.SSHConfig{
	HostName: "login.example.org", User: "example-user", IdentityFile: "/nonexistent/example-key",
}

type rig struct {
	t   *testing.T
	dir string // the fake's state directory
	ctl string // the control directory
}

func newRig(t *testing.T) *rig {
	t.Helper()
	dir := t.TempDir()
	ctl, err := os.MkdirTemp("/tmp", "hbssh")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(ctl) })
	if err := os.Chmod(ctl, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "bin"), 0o700); err != nil {
		t.Fatal(err)
	}
	return &rig{t: t, dir: dir, ctl: ctl}
}

func (r *rig) transport(name string, cfg slurm.SSHConfig, mod func(*Options)) *SSH {
	r.t.Helper()
	exe, err := os.Executable()
	if err != nil {
		r.t.Fatal(err)
	}
	o := Options{
		Name: name, Config: cfg, ControlDir: r.ctl, Binary: exe,
		Env:        []string{fakeEnv + "=1", "FAKE_SSH_DIR=" + r.dir, "PATH=/usr/bin:/bin"},
		MinBackoff: 20 * time.Millisecond, MaxBackoff: 200 * time.Millisecond,
		ConnectTimeout: 5 * time.Second, MinLifetime: time.Millisecond,
	}
	if mod != nil {
		mod(&o)
	}
	s, err := New(o)
	if err != nil {
		r.t.Fatal(err)
	}
	r.t.Cleanup(func() { s.Close() })
	return s
}

func (r *rig) write(name, content string) {
	r.t.Helper()
	if err := os.WriteFile(filepath.Join(r.dir, name), []byte(content), 0o600); err != nil {
		r.t.Fatal(err)
	}
}

func run(t *testing.T, s *SSH, line string) slurm.Result {
	t.Helper()
	res, err := s.Run(context.Background(), slurm.Command{Line: line, Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("run %q: %v", line, err)
	}
	return res
}

// One master serves every command, and at most four run at once.
func TestOneMasterServesConcurrentCommands(t *testing.T) {
	r := newRig(t)
	s := r.transport("prov-a", exampleConfig, nil)
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := s.Run(context.Background(), slurm.Command{Line: "sleep 0.2; echo hi", Timeout: 20 * time.Second})
			switch {
			case err != nil:
				errs <- err
			case strings.TrimSpace(string(res.Stdout)) != "hi" || res.ExitCode != 0:
				errs <- fmt.Errorf("got %q exit %d", res.Stdout, res.ExitCode)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if n := len(fakeLogLines(r.dir, "master")); n != 1 {
		t.Errorf("%d masters were started, want 1", n)
	}
	if n := len(fakeLogLines(r.dir, "client")); n != 20 {
		t.Errorf("%d client sessions ran, want 20", n)
	}
	b, _ := os.ReadFile(filepath.Join(r.dir, "max"))
	max := strings.TrimSpace(string(b))
	if max != "4" && max != "3" && max != "2" {
		t.Errorf("at most %s commands ran at once; want between 2 and 4 (four allowed, and 20 commands of 0.2 s overlap)", max)
	}
	if g := s.Generation(); g != 1 {
		t.Errorf("generation %d, want 1", g)
	}
}

// The argument templates: -F a private file, no agent, no forwards, strict
// host keys; the master runs -N; a client cannot fall back to its own
// connection.
func TestArgumentsAndEnvironment(t *testing.T) {
	r := newRig(t)
	t.Setenv("SSH_AUTH_SOCK", "/tmp/example-agent.sock")
	s := r.transport("prov-a", slurm.SSHConfig{
		HostName: "login.example.org", User: "example-user", Port: 2222,
		IdentityFile: "/nonexistent/example-key", UserKnownHostsFile: "/nonexistent/known_hosts",
	}, nil)
	run(t, s, "true")
	masters, clients := fakeLogLines(r.dir, "master"), fakeLogLines(r.dir, "client")
	if len(masters) != 1 || len(clients) != 1 {
		t.Fatalf("log: %d masters, %d clients", len(masters), len(clients))
	}
	common := []string{"-F " + r.ctl + "/", "BatchMode=yes", "IdentitiesOnly=yes", "IdentityAgent=none",
		"ClearAllForwardings=yes", "ForwardAgent=no", "StrictHostKeyChecking=yes",
		"ControlPath=" + r.ctl + "/"}
	for _, line := range append(masters, clients...) {
		f := strings.Split(line, "\t")
		args, env := f[1], f[2]
		for _, want := range common {
			if !strings.Contains(args, want) {
				t.Errorf("%s invocation lacks %q", f[0], want)
			}
		}
		for _, bad := range []string{" -L ", " -R ", " -D ", " -A ", "ControlPersist=yes", "ProxyJump", "-F /dev/null"} {
			if strings.Contains(" "+args+" ", bad) {
				t.Errorf("%s invocation has %q", f[0], bad)
			}
		}
		if strings.Contains(env, "SSH_AUTH_SOCK") {
			t.Errorf("%s invocation inherited SSH_AUTH_SOCK", f[0])
		}
	}
	// Port and the known-hosts file reach ssh through the configuration file.
	resolved, err := os.ReadFile(filepath.Join(r.dir, "resolved"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"HostName=login.example.org", "User=example-user", "Port=2222",
		"IdentityFile=/nonexistent/example-key", "UserKnownHostsFile=/nonexistent/known_hosts"} {
		if !strings.Contains(string(resolved), want) {
			t.Errorf("the connection did not use %q; resolved:\n%s", want, resolved)
		}
	}
	m := strings.Split(masters[0], "\t")[1]
	for _, want := range []string{"ControlMaster=yes", "ControlPersist=no", " -N"} {
		if !strings.Contains(m, want) {
			t.Errorf("master lacks %q", want)
		}
	}
	c := strings.Split(clients[0], "\t")[1]
	for _, want := range []string{"ControlMaster=no", "ProxyCommand=/bin/false", " -- true"} {
		if !strings.Contains(c, want) {
			t.Errorf("client lacks %q", want)
		}
	}
	// The default environment carries PATH and HOME only.
	d, err := New(Options{Name: "x", Config: exampleConfig, ControlDir: r.ctl})
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range d.o.Env {
		if !strings.HasPrefix(kv, "PATH=") && !strings.HasPrefix(kv, "HOME=") {
			t.Errorf("default environment holds %q", kv)
		}
	}
	if _, err := New(Options{Name: "x", Config: exampleConfig, ControlDir: r.ctl, Env: []string{"SSH_AUTH_SOCK=/x"}}); err == nil {
		t.Error("an environment with SSH_AUTH_SOCK was accepted")
	}
}

// File content travels on standard input and never on the command line.
func TestStdinNeverOnTheCommandLine(t *testing.T) {
	r := newRig(t)
	s := r.transport("prov-a", exampleConfig, nil)
	secret := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	res, err := s.Run(context.Background(), slurm.Command{Line: "cat > got", Stdin: []byte(secret)})
	if err != nil || res.ExitCode != 0 {
		t.Fatalf("run: %v exit %d", err, res.ExitCode)
	}
	b, _ := os.ReadFile(filepath.Join(r.dir, "got"))
	if string(b) != secret {
		t.Errorf("the remote side read %q", b)
	}
	for _, l := range fakeLogLines(r.dir, "") {
		if strings.Contains(l, secret) {
			t.Fatal("the content reached an ssh command line")
		}
	}
}

func TestExitCodesStderrAndTransportFailure(t *testing.T) {
	r := newRig(t)
	s := r.transport("prov-a", exampleConfig, nil)
	res := run(t, s, "echo oops >&2; exit 3")
	if res.ExitCode != 3 || string(res.Stderr) != "oops\n" {
		t.Errorf("got exit %d stderr %q", res.ExitCode, res.Stderr)
	}
	_, err := s.Run(context.Background(), slurm.Command{Line: "exit 255"})
	if !errors.Is(err, slurm.ErrTransport) {
		t.Errorf("exit 255: got %v, want ErrTransport", err)
	}
}

func TestTimeout(t *testing.T) {
	r := newRig(t)
	s := r.transport("prov-a", exampleConfig, nil)
	run(t, s, "true")
	start := time.Now()
	_, err := s.Run(context.Background(), slurm.Command{Line: "sleep 5", Timeout: 300 * time.Millisecond})
	if !errors.Is(err, slurm.ErrTimeout) {
		t.Fatalf("got %v, want ErrTimeout", err)
	}
	if d := time.Since(start); d > 4*time.Second {
		t.Errorf("the time-out took %s", d)
	}
}

// A dropped master is replaced on the next command, and the generation rises,
// so login-node paths are resolved again.
func TestDroppedMasterReconnects(t *testing.T) {
	r := newRig(t)
	s := r.transport("prov-a", exampleConfig, nil)
	run(t, s, "true")
	r.write("drop", "")
	deadline := time.Now().Add(5 * time.Second)
	for len(fakeLogLines(r.dir, "master-dropped")) == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	var err error
	for time.Now().Before(deadline) {
		if _, err = s.Run(context.Background(), slurm.Command{Line: "true"}); err == nil {
			break
		}
		if !errors.Is(err, slurm.ErrNotStarted) {
			t.Fatalf("got %v", err)
		}
		time.Sleep(30 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	if n := len(fakeLogLines(r.dir, "master")); n != 2 {
		t.Errorf("%d masters, want 2", n)
	}
	if g := s.Generation(); g != 2 {
		t.Errorf("generation %d, want 2", g)
	}
}

// A login node that refuses connections (a throttle) gets exponential
// backoff: commands during the backoff fail at once without starting ssh.
func TestRefusedLoginBacksOff(t *testing.T) {
	r := newRig(t)
	r.write("refuse", "3")
	s := r.transport("prov-a", exampleConfig, func(o *Options) {
		o.MinBackoff, o.MaxBackoff = 100*time.Millisecond, 400*time.Millisecond
	})
	_, err := s.Run(context.Background(), slurm.Command{Line: "true"})
	if !errors.Is(err, slurm.ErrNotStarted) {
		t.Fatalf("first command: %v, want ErrNotStarted", err)
	}
	// Inside the backoff: no new ssh invocation.
	before := len(fakeLogLines(r.dir, ""))
	if _, err := s.Run(context.Background(), slurm.Command{Line: "true"}); !errors.Is(err, slurm.ErrNotStarted) {
		t.Fatalf("command inside the backoff: %v", err)
	}
	if after := len(fakeLogLines(r.dir, "")); after != before {
		t.Errorf("a command inside the backoff started ssh (%d log lines, was %d)", after, before)
	}
	start := time.Now()
	deadline := start.Add(10 * time.Second)
	for {
		_, err = s.Run(context.Background(), slurm.Command{Line: "true"})
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	if n := len(fakeLogLines(r.dir, "master-refused")); n != 3 {
		t.Errorf("%d refused masters, want 3", n)
	}
	if st := s.Starts(); st != 4 {
		t.Errorf("%d master attempts, want 4: three refusals, then success", st)
	}
	// Backoffs 100 ms, 200 ms and 400 ms after the three refusals: the
	// success cannot come before 600 ms of waiting after the first.
	if d := time.Since(start); d < 500*time.Millisecond {
		t.Errorf("reconnected after %s; the backoff should hold it back about 600 ms", d)
	}
}

// Two Providers never share a master, even with the same credentials; nor do
// two credentials of one Provider.
func TestSeparateMasters(t *testing.T) {
	r := newRig(t)
	a := r.transport("prov-a", exampleConfig, nil)
	b := r.transport("prov-b", exampleConfig, nil)
	other := exampleConfig
	other.User = "other-user"
	c := r.transport("prov-a", other, nil)
	for _, s := range []*SSH{a, b, c} {
		run(t, s, "true")
	}
	if n := len(fakeLogLines(r.dir, "master")); n != 3 {
		t.Errorf("%d masters, want 3", n)
	}
	if a.socket == b.socket || a.socket == c.socket || b.socket == c.socket {
		t.Error("two transports share a control socket")
	}
}

func TestControlDirChecks(t *testing.T) {
	r := newRig(t)
	long := filepath.Join(r.ctl, strings.Repeat("d", 80))
	if err := os.MkdirAll(long, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Options{Name: "x", Config: exampleConfig, ControlDir: long}); err == nil || !strings.Contains(err.Error(), "control socket path") {
		t.Errorf("a socket path above the limit: got %v", err)
	}
	open := filepath.Join(r.ctl, "open")
	if err := os.Mkdir(open, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(open, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Options{Name: "x", Config: exampleConfig, ControlDir: open}); err == nil {
		t.Error("a control directory of mode 755 was accepted")
	}
}

func TestCloseStopsTheMaster(t *testing.T) {
	r := newRig(t)
	s := r.transport("prov-a", exampleConfig, nil)
	run(t, s, "true")
	if _, err := os.Stat(s.socket); err != nil {
		t.Fatalf("no control socket while the master runs: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(s.socket); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the control socket is left after Close: %v", err)
	}
	if _, err := s.Run(context.Background(), slurm.Command{Line: "true"}); !errors.Is(err, slurm.ErrNotStarted) {
		t.Errorf("a command after Close: %v", err)
	}
}

// With a host lock, no command runs while another process holds the lock.
func TestHostLock(t *testing.T) {
	r := newRig(t)
	lock := filepath.Join(r.dir, "host.lock")
	f, err := os.OpenFile(lock, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	s := r.transport("prov-a", exampleConfig, func(o *Options) { o.HostLock = lock })
	done := make(chan error, 1)
	go func() {
		_, err := s.Run(context.Background(), slurm.Command{Line: "true"})
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("the command ran while the lock was held: %v", err)
	case <-time.After(400 * time.Millisecond):
	}
	if n := len(fakeLogLines(r.dir, "")); n != 0 {
		t.Errorf("%d ssh invocations while the lock was held", n)
	}
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the command did not run after the lock was released")
	}
}

// The Batch end to end over this transport: the remote shell resolves
// $SCRATCH once per connection, files are staged private and only once, and
// the --wrap bootstrap reaches sbatch as one argument, intact through the
// login node's shell, and runs the staged agent.
func TestBatchOverSSH(t *testing.T) {
	r := newRig(t)
	scratch := filepath.Join(r.dir, "scratch-dir")
	home := filepath.Join(r.dir, "home-dir")
	for _, d := range []string{scratch, home} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	r.write("scratch", scratch)
	r.write("home", home)
	// A fake sbatch: one argument per line into sbatch.args, then a job ID.
	sbatch := "#!/bin/sh\nfor a in \"$@\"; do printf '%s\\n' \"$a\"; done > \"$HOME/sbatch.args\"\necho 1001\n"
	if err := os.WriteFile(filepath.Join(r.dir, "bin", "sbatch"), []byte(sbatch), 0o700); err != nil {
		t.Fatal(err)
	}
	s := r.transport("prov-a", exampleConfig, nil)
	gpus := int32(4)
	max := v1alpha1.Duration("48h")
	profile := provider.Profile{
		Name: "example",
		Spec: v1alpha1.PlatformProfileSpec{ProviderKind: v1alpha1.PlatformBatch, StagingDir: "$SCRATCH/stage", JSONDataParser: "v0.0.42"},
		Partition: v1alpha1.PartitionProfile{Name: "example-gpu", AcceleratorClass: v1alpha1.AcceleratorClass{GPUModel: "A100", CPUArch: "x86_64"},
			GPUsPerNode: &gpus, MaxTime: &max},
	}
	b := slurm.NewBatch(s, profile, provider.Settings{Provider: "prov-a", Partition: "example-gpu", Account: "example-account"}, slurm.Options{})
	ctx := context.Background()

	paths, err := b.Paths(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if paths.Staging != scratch+"/stage" || paths.Home != home {
		t.Fatalf("paths %+v", paths)
	}

	agent := []byte("#!/bin/sh\necho \"agent ran with $#: $1\" > \"$1/ran\"\n")
	sum := sha256.Sum256(agent)
	digest := hex.EncodeToString(sum[:])
	tag := v1alpha1.JobTag("ns", "alloc-a", "uid-a")
	files := []provider.File{
		{Path: "agent/" + digest + "/hb-agent", SHA256: digest, Mode: 0o700, Content: agent},
		cred(tag),
	}
	for _, f := range files {
		if err := b.EnsureFile(ctx, f); err != nil {
			t.Fatal(err)
		}
	}
	writes := len(grepLog(r.dir, "cat > "))
	for _, f := range files {
		if err := b.EnsureFile(ctx, f); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(grepLog(r.dir, "cat > ")); n != writes {
		t.Errorf("a second EnsureFile wrote again (%d writes, was %d)", n, writes)
	}
	credPath := filepath.Join(scratch, "stage", "alloc", tag, "credential")
	fi, err := os.Stat(credPath)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("credential mode %o, want 600", fi.Mode().Perm())
	}
	di, _ := os.Stat(filepath.Dir(credPath))
	if di.Mode().Perm() != 0o700 {
		t.Errorf("Allocation directory mode %o, want 700", di.Mode().Perm())
	}
	if n := len(grepLog(r.dir, "printenv")); n != 2 {
		t.Errorf("%d printenv calls, want 2 (HOME and SCRATCH, once per connection)", n)
	}

	id, outcome, err := b.Submit(ctx, provider.SubmitRequest{Tag: tag, Hold: true, Account: "example-account", Params: v1alpha1.BatchParameters{
		Partition: "example-gpu", TimeLimit: "90m", GPUs: 1, CPUsPerTask: 8, Memory: resource.MustParse("16Gi"), AgentDigest: digest,
	}})
	if err != nil || outcome != provider.Accepted || id != "1001" {
		t.Fatalf("submit: %q %s %v", id, outcome, err)
	}
	argsFile, _ := os.ReadFile(filepath.Join(home, "sbatch.args"))
	args := strings.Split(strings.TrimRight(string(argsFile), "\n"), "\n")
	allocDir := filepath.Join(scratch, "stage", "alloc", tag)
	agentPath := filepath.Join(scratch, "stage", "agent", digest, "hb-agent")
	want := []string{"--parsable", "--job-name=" + tag, "--no-requeue", "--hold", "--partition=example-gpu",
		"--account=example-account", "--time=90", "--nodes=1", "--gres=gpu:1", "--cpus-per-task=8", "--mem=16384M",
		"--output=" + allocDir + "/job.out", "--error=" + allocDir + "/job.err",
		"--wrap=[ $(sha256sum " + agentPath + " | cut -c1-64) = " + digest + " ] && exec " + agentPath + " " + allocDir}
	if strings.Join(args, "\n") != strings.Join(want, "\n") {
		t.Fatalf("sbatch received\n%s\nwant\n%s", strings.Join(args, "\n"), strings.Join(want, "\n"))
	}
	// The batch script runs the agent with the Allocation's directory.
	wrap := strings.TrimPrefix(args[len(args)-1], "--wrap=")
	if out, err := exec.Command("/bin/sh", "-c", wrap).CombinedOutput(); err != nil {
		t.Fatalf("bootstrap: %v: %s", err, out)
	}
	ran, err := os.ReadFile(filepath.Join(allocDir, "ran"))
	if err != nil || string(ran) != "agent ran with 1: "+allocDir+"\n" {
		t.Errorf("the agent did not run as expected: %q %v", ran, err)
	}

	if err := b.RemoveAllocationFiles(ctx, tag); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(allocDir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the Allocation's directory is left: %v", err)
	}
	// After a reconnect, the paths are resolved again.
	r.write("drop", "")
	deadline := time.Now().Add(5 * time.Second)
	for len(fakeLogLines(r.dir, "master-dropped")) == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, err = b.Paths(ctx); err == nil {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	if n := len(grepLog(r.dir, "printenv")); n != 4 {
		t.Errorf("%d printenv calls after a reconnect, want 4", n)
	}
}

func cred(tag string) provider.File {
	c := []byte(strings.Repeat("ab", 32))
	sum := sha256.Sum256(c)
	return provider.File{Path: "alloc/" + tag + "/credential", SHA256: hex.EncodeToString(sum[:]), Mode: 0o600, Content: c}
}

func grepLog(dir, needle string) []string {
	var out []string
	for _, l := range fakeLogLines(dir, "client") {
		if strings.Contains(l, needle) {
			out = append(out, l)
		}
	}
	return out
}
