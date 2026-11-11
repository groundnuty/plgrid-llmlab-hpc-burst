package sshrun

// Tests for issue 403: Relay.Login's ssh -G is bounded. ssh -G only reads the
// configuration, but a Match exec command in it can hang, and Login used to
// wait for it for ever, holding the Relay's lock. Now it runs under a bound,
// ends the process group with the Match exec command in it, and reports a
// named error.
//
// The fake ssh is a /bin/sh script that logs its arguments and, unless the
// file "answer" is in its directory, hangs as a stuck Match exec would: it
// starts a child of its own and waits for it. Every process of a test carries a
// marker in its environment (the Relay's Env), so "no process is left" is a
// scan of /proc for the marker, never a search by name, and a process that
// outlives a failing test is ended through a pidfd by its cleanup.

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

// loginTestBound is the bound a test injects. The fake needs a few
// milliseconds to start its child, so the margin is large; a test that finds
// the child missing says so rather than pass without having tested anything.
const loginTestBound = 2 * time.Second

// loginReturnDeadline is how long a test waits for Login to return: far over
// the bound, so only a Login that has no bound misses it.
const loginReturnDeadline = 20 * time.Second

const loginMarkVar = "HB_LOGIN_TEST"

// markedProcs are the live processes whose environment holds the marker as a
// whole NAME=value entry (a token, not a substring). A zombie's environment is
// gone, so an ended process the reaper has not collected yet is not counted.
func markedProcs(mark string) []int {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	want := loginMarkVar + "=" + mark
	var out []int
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		b, err := os.ReadFile("/proc/" + e.Name() + "/environ")
		if err != nil {
			continue
		}
		for _, kv := range bytes.Split(b, []byte{0}) {
			if string(kv) == want {
				out = append(out, pid)
				break
			}
		}
	}
	return out
}

// endMarked ends every process that still carries the marker, through a pidfd
// opened on it (the marker is read again once the pidfd names it, so a PID that
// was handed on is not signalled). It is the cleanup of a test that failed.
func endMarked(mark string) {
	for _, pid := range markedProcs(mark) {
		fd, err := unix.PidfdOpen(pid, 0)
		if err != nil {
			continue
		}
		still := false
		for _, p := range markedProcs(mark) {
			still = still || p == pid
		}
		if still {
			_ = unix.PidfdSendSignal(fd, syscall.SIGKILL, nil, 0)
		}
		_ = unix.Close(fd)
	}
}

// waitNoMarked waits, on the observable state and with a generous deadline,
// until no process carries the marker: the leader is reaped when Login returns,
// but the rest of its group may still be leaving.
func waitNoMarked(t *testing.T, mark string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		left := markedProcs(mark)
		if len(left) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d processes of the fake ssh are still alive after Login returned: %v", len(left), left)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// syncBuf is a log's destination that the Login goroutine and exec's watcher
// can both write to.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// loginFake is a Relay whose ssh is the fake, the directory the fake keeps its
// records in, and the Relay's log.
type loginFake struct {
	r    *Relay
	dir  string
	mark string
	log  *syncBuf
}

const loginFakeScript = `#!/bin/sh
dir='@DIR@'
printf '%s\n' "$@" > "$dir/argv"
echo $$ > "$dir/ssh.pid"
n=$(cat "$dir/count" 2>/dev/null || echo 0)
echo $((n+1)) > "$dir/count"
if [ -e "$dir/answer" ]; then
  printf 'hostname login01.example.org\nport 22\n'
  exit 0
fi
if [ ! -e "$dir/quiet" ]; then
  printf 'debug1: Reading configuration data /private-dir/config-marker\n' >&2
  printf "debug1: Executing command: 'hang-forever --flag'\n" >&2
fi
sleep 300 &
echo $! > "$dir/child.pid"
wait
`

// newLoginFake makes the fake and a Relay on it with the test's bound.
func newLoginFake(t *testing.T, bound time.Duration) *loginFake {
	t.Helper()
	dir := t.TempDir()
	if strings.ContainsAny(dir, `'\`) {
		t.Fatalf("the test's directory holds a quote")
	}
	ssh := filepath.Join(dir, "ssh")
	if err := os.WriteFile(ssh, []byte(strings.Replace(loginFakeScript, "@DIR@", dir, 1)), 0o700); err != nil {
		t.Fatal(err)
	}
	mark := strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + strconv.Itoa(os.Getpid())
	t.Cleanup(func() { endMarked(mark) })
	conf, err := os.MkdirTemp("", "rl")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(conf) })
	if err := os.Chmod(conf, 0o700); err != nil {
		t.Fatal(err)
	}
	logBuf := &syncBuf{}
	r, err := NewRelay(RelayOptions{Name: "login-timeout", Binary: ssh, ControlDir: conf, LoginTimeout: bound,
		Logger: slog.New(slog.NewTextHandler(logBuf, nil)),
		Env:    []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), loginMarkVar + "=" + mark},
		Config: slurm.SSHConfig{HostName: "LOGIN01.Example.org", Port: 2222, User: "example-user",
			IdentityFile: "/nonexistent/key", UserKnownHostsFile: "/nonexistent/known_hosts"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return &loginFake{r: r, dir: dir, mark: mark, log: logBuf}
}

func (f *loginFake) touch(t *testing.T, name string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(f.dir, name), nil, 0o600); err != nil {
		t.Fatal(err)
	}
}

func (f *loginFake) read(name string) string {
	b, _ := os.ReadFile(filepath.Join(f.dir, name))
	return strings.TrimSpace(string(b))
}

// login runs Login in a goroutine and waits for it with the generous
// deadline, so a Login without a bound fails the test instead of hanging it
// (the cleanup then ends the fake, which frees the goroutine).
func (f *loginFake) login(t *testing.T) (string, error) {
	t.Helper()
	type result struct {
		host string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		host, err := f.r.Login()
		done <- result{host, err}
	}()
	select {
	case res := <-done:
		return res.host, res.err
	case <-time.After(loginReturnDeadline):
		endMarked(f.mark)
		t.Fatalf("Login did not return within %s of a %s bound", loginReturnDeadline, f.r.o.LoginTimeout)
		return "", nil
	}
}

// requireStarted is the precondition of the tests that end the fake: it ran,
// and its child was started, before the bound ended it.
func (f *loginFake) requireStarted(t *testing.T) {
	t.Helper()
	if f.read("child.pid") == "" {
		t.Fatalf("the fake never started its child before the %s bound ended it: nothing was tested", f.r.o.LoginTimeout)
	}
}

// A stalled ssh -G ends at the bound, with an error that wraps ErrLoginTimeout
// and says only that ssh -G timed out after the bound. The last Match exec
// command ssh's standard error shows is in the log and not in the error, which
// reaches a Provider's status (PR 409's ruling), and no other line of ssh's
// output is in either. The whole group is ended: the child the fake started is
// not left running.
func TestLoginTimesOutAndEndsTheWholeGroup(t *testing.T) {
	f := newLoginFake(t, loginTestBound)
	host, err := f.login(t)
	if !errors.Is(err, ErrLoginTimeout) {
		t.Fatalf("Login = %q, %v, want an error that wraps ErrLoginTimeout", host, err)
	}
	if host != "LOGIN01.Example.org" {
		t.Errorf("the host with the error is %q, want the configured HostName", host)
	}
	f.requireStarted(t)
	if want := "sshrun: ssh -G timed out after " + loginTestBound.String(); err.Error() != want {
		t.Errorf("the error is %q, want exactly %q", err, want)
	}
	for _, leaked := range []string{"hang-forever", "Match exec", "config-marker"} {
		if strings.Contains(err.Error(), leaked) {
			t.Errorf("the error %q carries %q: it reaches a Provider's status", err, leaked)
		}
	}
	if log := f.log.String(); !strings.Contains(log, `match_exec="hang-forever --flag"`) {
		t.Errorf("the log lacks the Match exec command:\n%s", log)
	} else if strings.Contains(log, "config-marker") {
		t.Errorf("the log carries ssh's other output:\n%s", log)
	}
	waitNoMarked(t, f.mark)
}

// When ssh's standard error names no Match exec command, the error does not
// claim one.
func TestLoginTimeoutWithoutAMatchExecLine(t *testing.T) {
	f := newLoginFake(t, loginTestBound)
	f.touch(t, "quiet")
	_, err := f.login(t)
	if !errors.Is(err, ErrLoginTimeout) {
		t.Fatalf("Login: %v, want an error that wraps ErrLoginTimeout", err)
	}
	f.requireStarted(t)
	if strings.Contains(err.Error(), "Match exec") || strings.Contains(err.Error(), "hang-forever") {
		t.Errorf("the error names a command ssh never reported: %q", err)
	}
	if log := f.log.String(); !strings.Contains(log, "timed out") || strings.Contains(log, "match_exec") {
		t.Errorf("the log of a timeout with no Match exec line is not the timeout alone:\n%s", log)
	}
	waitNoMarked(t, f.mark)
}

// A timeout is not kept: the next Login runs ssh -G again, and when it answers,
// that answer is the host and is kept. No later call reports a quiet fallback
// for a timeout.
func TestLoginTimeoutIsNotKept(t *testing.T) {
	f := newLoginFake(t, loginTestBound)
	if _, err := f.login(t); !errors.Is(err, ErrLoginTimeout) {
		t.Fatalf("the first Login: %v, want ErrLoginTimeout", err)
	}
	waitNoMarked(t, f.mark)
	f.touch(t, "answer")
	for i := 1; i <= 2; i++ {
		host, err := f.login(t)
		if err != nil || host != "login01.example.org" {
			t.Fatalf("Login %d after the fake answers = %q, %v, want login01.example.org", i+1, host, err)
		}
	}
	if got := f.read("count"); got != "2" {
		t.Errorf("ssh -G ran %s times, want 2: one timeout, one answer, then the kept host", got)
	}
}

// The normal path: ssh -G answers at once, the host is its hostname, the
// arguments start with -G (the test doubles of other packages read the first)
// and are otherwise the private file and the alias, with -v, and nothing is
// left running.
func TestLoginAnswersThroughTheBound(t *testing.T) {
	f := newLoginFake(t, loginTestBound)
	f.touch(t, "answer")
	host, err := f.login(t)
	if err != nil || host != "login01.example.org" {
		t.Fatalf("Login = %q, %v, want login01.example.org", host, err)
	}
	want := fmt.Sprintf("-G\n-v\n-F\n%s\n%s", f.r.Config(), alias)
	if got := f.read("argv"); got != want {
		t.Errorf("ssh -G's arguments:\n%s\nwant:\n%s", got, want)
	}
	if _, err := f.r.Login(); err != nil || f.read("count") != "1" {
		t.Errorf("the host is kept: a second Login = %v and ssh -G ran %s times, want nil and 1", err, f.read("count"))
	}
	waitNoMarked(t, f.mark)
}

// An ssh -G that fails by itself is not a timeout: Login is the configured
// HostName with no error, as before, and the failure is kept.
func TestLoginFailureOtherThanATimeoutIsTheConfiguredHostName(t *testing.T) {
	f := newLoginFake(t, loginTestBound)
	f.r.o.Binary = "/bin/false"
	host, err := f.login(t)
	if err != nil || host != "LOGIN01.Example.org" {
		t.Errorf("Login = %q, %v, want the configured HostName and nil", host, err)
	}
}

// The real ssh, with a Match exec that hangs in the relays' file: Login ends at
// the bound, the log (not the error) names the command, and the command's own
// process is gone, not only ssh.
func TestLoginTimeoutOfARealSSHWithAHangingMatchExec(t *testing.T) {
	if _, err := exec.LookPath("ssh"); err != nil {
		t.Skip("no ssh client on this host")
	}
	f := newLoginFake(t, loginTestBound)
	f.r.o.Binary = "ssh"
	started := filepath.Join(f.dir, "started")
	conf, err := os.OpenFile(f.r.Config(), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(conf, "Match exec \"touch %s; sleep 600\"\n  Port 2222\n", started); err != nil {
		t.Fatal(err)
	}
	if err := conf.Close(); err != nil {
		t.Fatal(err)
	}
	_, lerr := f.login(t)
	if !errors.Is(lerr, ErrLoginTimeout) {
		t.Fatalf("Login: %v, want an error that wraps ErrLoginTimeout", lerr)
	}
	if _, err := os.Stat(started); err != nil {
		t.Fatalf("ssh never ran the Match exec command before the bound ended it: nothing was tested")
	}
	if strings.Contains(lerr.Error(), "sleep 600") {
		t.Errorf("the error names the Match exec command, which belongs in the log: %q", lerr)
	}
	if log := f.log.String(); !strings.Contains(log, "sleep 600") {
		t.Errorf("the log does not name the Match exec command:\n%s", log)
	}
	waitNoMarked(t, f.mark)
}
