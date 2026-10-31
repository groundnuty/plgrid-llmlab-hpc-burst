package sshrun

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

// hangChecks makes every "ssh -O check" stall (see fakessh_test.go) until the
// test ends. Call it after r.transport: cleanups run last in, first out, so
// the release then runs before the transport's Close, which a stalled connect
// would otherwise block, and no fake client outlives the test.
func (r *rig) hangChecks() {
	r.t.Helper()
	r.write("check-hang", "")
	r.t.Cleanup(func() { os.Remove(filepath.Join(r.dir, "check-hang")) })
}

func (r *rig) waitForLog(mode string, d time.Duration) {
	r.t.Helper()
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
		if len(fakeLogLines(r.dir, mode)) > 0 {
			return
		}
	}
	r.t.Fatalf("no %q line in the fake's log within %s", mode, d)
}

// runAsync runs a command in a goroutine; the result arrives on the channel,
// so a test can bound the wait and fail instead of hanging on a regression.
func runAsync(s *SSH, ctx context.Context, line string) <-chan error {
	done := make(chan error, 1)
	go func() {
		_, err := s.Run(ctx, slurm.Command{Line: line, Timeout: 10 * time.Second})
		done <- err
	}()
	return done
}

func within(t *testing.T, what string, ch <-chan error, d time.Duration) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(d):
		t.Fatalf("%s did not return within %s", what, d)
		return nil
	}
}

// A master that accepts "ssh -O check" and never answers must not block the
// transport: the connect gives up at ConnectTimeout instead of holding the
// mutex forever.
func TestStalledCheckIsBounded(t *testing.T) {
	r := newRig(t)
	s := r.transport("prov-a", exampleConfig, func(o *Options) {
		o.ConnectTimeout, o.CheckTimeout = 2*time.Second, 10*time.Second
	})
	r.hangChecks()
	start := time.Now()
	err := within(t, "Run against a stalled master", runAsync(s, context.Background(), "true"), 8*time.Second)
	if !errors.Is(err, slurm.ErrNotStarted) {
		t.Errorf("got %v, want ErrNotStarted", err)
	}
	if n := len(fakeLogLines(r.dir, "check-hang")); n == 0 {
		t.Error("no check stalled; the test proves nothing")
	}
	if d := time.Since(start); d < 1800*time.Millisecond {
		t.Errorf("returned after %s, before the connect bound of 2 s: the stall was not what ended it", d)
	}
	// Another command is not stuck behind the first one's mutex either.
	within(t, "a second Run", runAsync(s, context.Background(), "true"), 8*time.Second)
}

// Each stalled check is cut at CheckTimeout and asked again, long before
// ConnectTimeout: a second stall can only start after the first was killed.
// The loop waits for that instead of sleeping, so a slow machine only
// lengthens the test.
func TestStalledCheckIsCutAtCheckTimeout(t *testing.T) {
	r := newRig(t)
	s := r.transport("prov-a", exampleConfig, func(o *Options) {
		o.ConnectTimeout, o.CheckTimeout = time.Minute, 1500*time.Millisecond
	})
	r.hangChecks()
	pending := runAsync(s, context.Background(), "true")
	for end := time.Now().Add(20 * time.Second); len(fakeLogLines(r.dir, "check-hang")) < 2; time.Sleep(20 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("%d checks stalled in 20 s; the first was not cut at CheckTimeout (1.5 s)", len(fakeLogLines(r.dir, "check-hang")))
		}
	}
	select {
	case err := <-pending:
		t.Fatalf("the Run ended before ConnectTimeout: %v", err)
	default:
	}
}

// Close does not wait for a connect that is stuck on a master that never
// answers, although ConnectTimeout and CheckTimeout are both far away.
func TestCloseDuringStalledConnect(t *testing.T) {
	r := newRig(t)
	s := r.transport("prov-a", exampleConfig, func(o *Options) {
		o.ConnectTimeout, o.CheckTimeout = 30*time.Second, 30*time.Second
	})
	r.hangChecks()
	pending := runAsync(s, context.Background(), "true")
	r.waitForLog("check-hang", 5*time.Second)
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	if err := within(t, "Close during a stalled connect", closed, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := within(t, "the stuck Run after Close", pending, 2*time.Second); !errors.Is(err, slurm.ErrNotStarted) {
		t.Errorf("the stuck Run: got %v, want ErrNotStarted", err)
	}
	if n := len(fakeLogLines(r.dir, "master-stopped")); n != 1 {
		t.Errorf("%d masters were stopped, want the one that was half started", n)
	}
	if _, err := os.Stat(s.config); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the configuration file is left after Close: %v", err)
	}
}

// A caller whose context ends while the master is stuck gets its call back at
// once, and the login node is not blamed: no backoff follows.
func TestCallerContextEndsStalledConnect(t *testing.T) {
	r := newRig(t)
	s := r.transport("prov-a", exampleConfig, func(o *Options) {
		o.ConnectTimeout, o.CheckTimeout = 30*time.Second, 30*time.Second
		o.MinBackoff, o.MaxBackoff = time.Minute, time.Minute
	})
	r.hangChecks()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err := within(t, "Run with a context that ends", runAsync(s, ctx, "true"), 3*time.Second)
	if !errors.Is(err, slurm.ErrNotStarted) {
		t.Fatalf("got %v, want ErrNotStarted", err)
	}
	os.Remove(filepath.Join(r.dir, "check-hang"))
	if err := within(t, "the next Run", runAsync(s, context.Background(), "true"), 5*time.Second); err != nil {
		t.Errorf("the next command failed (a backoff after the caller gave up?): %v", err)
	}
}

// The credentials reach ssh through a private file, not the command line,
// where any local user could read them with ps. The log is what the fake
// binary was given, not what the transport meant to pass.
func TestCredentialsStayOffTheCommandLine(t *testing.T) {
	r := newRig(t)
	cfg := slurm.SSHConfig{
		HostName: "login-n5.example.org", User: "n5user", Port: 2299,
		IdentityFile: "/nonexistent/n5key", UserKnownHostsFile: "/nonexistent/n5hosts",
	}
	s := r.transport("prov-a", cfg, nil)
	run(t, s, "true")
	seen := map[string]bool{}
	var files []string
	for _, line := range fakeLogLines(r.dir, "") {
		f := strings.Split(line, "\t")
		seen[f[0]] = true
		for _, v := range []string{cfg.HostName, cfg.User, cfg.IdentityFile, cfg.UserKnownHostsFile, "HostName", "IdentityFile", "UserKnownHostsFile", "Port=", "User="} {
			if strings.Contains(f[1], v) {
				t.Errorf("a %s invocation has %q on its command line: %s", f[0], v, f[1])
			}
		}
		args := strings.Fields(f[1])
		for i, a := range args {
			if a == "-F" && i+1 < len(args) {
				files = append(files, args[i+1])
			}
		}
	}
	for _, mode := range []string{"master", "check", "client"} {
		if !seen[mode] {
			t.Errorf("no %s invocation was logged; the test proves nothing about it", mode)
		}
	}
	if len(files) == 0 {
		t.Fatal("no invocation passed -F")
	}
	file := files[0]
	for _, f := range files {
		if f != file {
			t.Errorf("-F names %s and %s", file, f)
		}
	}
	if filepath.Dir(file) != r.ctl {
		t.Errorf("-F names %s, not a file in the control directory %s", file, r.ctl)
	}
	fi, err := os.Stat(file)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode() != 0o600 {
		t.Errorf("the configuration file has mode %v, want exactly -rw-------", fi.Mode())
	}
	b, _ := os.ReadFile(file)
	for _, want := range []string{"Host hpcburst-login", cfg.HostName, cfg.User, cfg.IdentityFile, cfg.UserKnownHostsFile, "2299"} {
		if !strings.Contains(string(b), want) {
			t.Errorf("the configuration file lacks %q:\n%s", want, b)
		}
	}
	// The fake connected with the file's values, so ssh would have too.
	resolved, _ := os.ReadFile(filepath.Join(r.dir, "resolved"))
	for _, want := range []string{"HostName=" + cfg.HostName, "User=" + cfg.User, "IdentityFile=" + cfg.IdentityFile, "Port=2299"} {
		if !strings.Contains(string(resolved), want) {
			t.Errorf("the connection did not use %q:\n%s", want, resolved)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if left, _ := os.ReadDir(r.ctl); len(left) != 0 {
		t.Errorf("files are left in the control directory after Close: %v", left)
	}
}

// A deleted configuration file is written again by the next master start, via
// the same path that replaces a master whose socket is gone.
func TestDeletedConfigFileIsRestored(t *testing.T) {
	r := newRig(t)
	s := r.transport("prov-a", exampleConfig, nil)
	run(t, s, "true")
	if err := os.Remove(s.config); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Run(context.Background(), slurm.Command{Line: "true", Timeout: 10 * time.Second}); !errors.Is(err, slurm.ErrTransport) {
		t.Fatalf("a command without its configuration file: %v, want ErrTransport", err)
	}
	if _, err := s.Run(context.Background(), slurm.Command{Line: "true", Timeout: 10 * time.Second}); err != nil {
		t.Errorf("the transport did not recover from a deleted configuration file: %v", err)
	}
	if _, err := os.Stat(s.config); err != nil {
		t.Errorf("the configuration file was not written again: %v", err)
	}
}

// A value that could end its line and add a directive (ProxyCommand) never
// reaches the file, and the error does not repeat it.
func TestConfigValuesAreValidated(t *testing.T) {
	r := newRig(t)
	bad := map[string]func(*slurm.SSHConfig, string){
		"HostName":           func(c *slurm.SSHConfig, v string) { c.HostName = v },
		"User":               func(c *slurm.SSHConfig, v string) { c.User = v },
		"IdentityFile":       func(c *slurm.SSHConfig, v string) { c.IdentityFile = v },
		"UserKnownHostsFile": func(c *slurm.SSHConfig, v string) { c.UserKnownHostsFile = v },
	}
	for field, set := range bad {
		for _, v := range []string{"a\nProxyCommand /bin/evil", "a b", "a\"b", "a#b", "a\\b", "a\tb", "a\x7fb"} {
			c := exampleConfig
			set(&c, v)
			_, err := New(Options{Name: "x", Config: c, ControlDir: r.ctl})
			if err == nil {
				t.Errorf("%s %q was accepted", field, v)
			} else if !strings.Contains(err.Error(), field) || strings.Contains(err.Error(), "evil") {
				t.Errorf("%s %q: error %q should name the field and not the value", field, v, err)
			}
		}
	}
	for _, c := range []slurm.SSHConfig{
		{User: "u", IdentityFile: "/k"}, {HostName: "h", IdentityFile: "/k"}, {HostName: "h", User: "u"},
	} {
		if _, err := New(Options{Name: "x", Config: c, ControlDir: r.ctl}); err == nil {
			t.Errorf("%+v was accepted", c)
		}
	}
}

// The master is alive but its control socket is gone (tmpfiles cleaning the
// directory): the command that finds out fails, the next one runs on a new
// master, and the old one is stopped, not left behind.
func TestMasterWithDeletedSocketIsReplaced(t *testing.T) {
	r := newRig(t)
	s := r.transport("prov-a", exampleConfig, nil)
	run(t, s, "true")
	if err := os.Remove(s.socket); err != nil {
		t.Fatal(err)
	}
	if len(fakeLogLines(r.dir, "master-dropped")) != 0 || len(fakeLogLines(r.dir, "master-stopped")) != 0 {
		t.Fatal("the master is not running, so the test is not about a live master")
	}
	if _, err := s.Run(context.Background(), slurm.Command{Line: "true", Timeout: 10 * time.Second}); err != nil && !errors.Is(err, slurm.ErrTransport) {
		t.Fatalf("the command that finds the socket gone: %v, want nil or ErrTransport", err)
	}
	run(t, s, "true")
	if g := s.Generation(); g != 2 {
		t.Errorf("generation %d, want 2", g)
	}
	if n := len(fakeLogLines(r.dir, "master")); n != 2 {
		t.Errorf("%d masters were started, want 2", n)
	}
	if n := len(fakeLogLines(r.dir, "master-stopped")); n != 1 {
		t.Errorf("%d masters were stopped, want 1: the one whose socket was deleted", n)
	}
}

// A remote command that exits 255 is not a lost master: the check passes and
// the master stays.
func TestRemoteExit255KeepsTheMaster(t *testing.T) {
	r := newRig(t)
	s := r.transport("prov-a", exampleConfig, nil)
	if _, err := s.Run(context.Background(), slurm.Command{Line: "exit 255"}); !errors.Is(err, slurm.ErrTransport) {
		t.Fatalf("got %v, want ErrTransport", err)
	}
	run(t, s, "true")
	if g, n := s.Generation(), len(fakeLogLines(r.dir, "master")); g != 1 || n != 1 {
		t.Errorf("generation %d with %d masters started, want 1 and 1", g, n)
	}
	if n := len(fakeLogLines(r.dir, "master-stopped")); n != 0 {
		t.Errorf("%d masters were stopped", n)
	}
}

// Two instances with the same options do not share a socket, so one stopping
// cannot delete the path the other's master listens on.
func TestInstancesDoNotShareSockets(t *testing.T) {
	r := newRig(t)
	a := r.transport("prov-a", exampleConfig, nil)
	b := r.transport("prov-a", exampleConfig, nil)
	if a.socket == b.socket || a.config == b.config {
		t.Fatalf("two instances share a path: %s %s", a.socket, b.socket)
	}
	if len(a.socket) > maxSocketPath {
		t.Errorf("the socket path is %d bytes, above %d", len(a.socket), maxSocketPath)
	}
	var wg sync.WaitGroup
	for _, s := range []*SSH{a, b} {
		wg.Add(1)
		go func() { defer wg.Done(); run(t, s, "true") }()
	}
	wg.Wait()
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	run(t, b, "true")
	if g := b.Generation(); g != 1 {
		t.Errorf("the second instance reconnected (generation %d) after the first closed", g)
	}
	if n := len(fakeLogLines(r.dir, "master")); n != 2 {
		t.Errorf("%d masters were started, want 2", n)
	}
}
