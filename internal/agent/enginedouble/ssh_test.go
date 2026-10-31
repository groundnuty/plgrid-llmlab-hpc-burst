package enginedouble

import (
	"bufio"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// sshArgsFor is the argument vector the agent builds (internal/agent/ssh.go),
// with the key and known_hosts files in dir.
func sshArgsFor(dir, socket string, port int) []string {
	return []string{
		"-N",
		"-o", "BatchMode=yes",
		"-o", "IdentitiesOnly=yes",
		"-o", "StrictHostKeyChecking=yes",
		"-o", "UserKnownHostsFile=" + filepath.Join(dir, "known_hosts"),
		"-o", "ExitOnForwardFailure=yes",
		"-o", "StreamLocalBindUnlink=yes",
		"-o", "ServerAliveInterval=15",
		"-o", "ServerAliveCountMax=3",
		"-o", "ConnectTimeout=20",
		"-F", "/dev/null",
		"-o", "ControlMaster=no",
		"-o", "ControlPath=none",
		"-i", filepath.Join(dir, "key"),
		"-p", "22",
		"-L", socket + ":127.0.0.1:" + itoaPort(port),
		"hpcburst@control.example",
	}
}

func itoaPort(p int) string { return strconv.Itoa(p) }

// sshMainWithin runs SSHMain and returns its exit code. An ssh that accepts
// its arguments forwards until it is told to stop; after d the call sends TERM
// to this process, which SSHMain catches, and returns -1.
func sshMainWithin(args []string, d time.Duration) int {
	done := make(chan int, 1)
	go func() { done <- SSHMain(args) }()
	select {
	case code := <-done:
		return code
	case <-time.After(d):
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
		<-done
		return -1
	}
}

func sshFiles(t *testing.T) string {
	t.Helper()
	dir := shortDir(t)
	must(t, os.WriteFile(filepath.Join(dir, "key"), []byte("key"), 0o600))
	must(t, os.WriteFile(filepath.Join(dir, "known_hosts"), []byte("host"), 0o600))
	return dir
}

// without returns args minus the "-o <prefix>..." option.
func without(args []string, prefix string) []string {
	var out []string
	for i := 0; i < len(args); i++ {
		if args[i] == "-o" && strings.HasPrefix(args[i+1], prefix) {
			i++
			continue
		}
		out = append(out, args[i])
	}
	return out
}

func replace(args []string, old, new string) []string {
	out := append([]string(nil), args...)
	for i, a := range out {
		if a == old {
			out[i] = new
		}
	}
	return out
}

// Every option the dial-home forward must carry is required, and the files it
// names must exist; a refusal exits 255 before anything is listened on.
func TestSSHRefusesWhatTheForwardMustCarry(t *testing.T) {
	dir := sshFiles(t)
	socket := sockPath(t, dir, "fwd.sock")
	good := sshArgsFor(dir, socket, 29290)
	t.Setenv(EnvSSHLog, filepath.Join(dir, "sshlog"))

	cases := map[string][]string{
		"no BatchMode":                 without(good, "BatchMode="),
		"BatchMode=no":                 replace(good, "BatchMode=yes", "BatchMode=no"),
		"no ExitOnForwardFailure":      without(good, "ExitOnForwardFailure="),
		"no StrictHostKeyChecking":     without(good, "StrictHostKeyChecking="),
		"StrictHostKeyChecking=no":     replace(good, "StrictHostKeyChecking=yes", "StrictHostKeyChecking=no"),
		"no IdentitiesOnly":            without(good, "IdentitiesOnly="),
		"no UserKnownHostsFile":        without(good, "UserKnownHostsFile="),
		"a missing known_hosts file":   replace(good, "UserKnownHostsFile="+filepath.Join(dir, "known_hosts"), "UserKnownHostsFile="+filepath.Join(dir, "absent")),
		"a missing identity file":      replace(good, filepath.Join(dir, "key"), filepath.Join(dir, "absent")),
		"no -N":                        replace(good, "-N", "-T"),
		"no destination":               good[:len(good)-1],
		"a destination without a user": replace(good, "hpcburst@control.example", "control.example"),
		"no -L":                        append(append([]string(nil), good[:len(good)-3]...), "hpcburst@control.example"),
		"an unknown option":            append([]string{"-X"}, good...),
		"a malformed -L":               replace(good, socket+":127.0.0.1:29290", "justasocket"),
		"a missing -F file":            replace(good, "/dev/null", filepath.Join(dir, "absent")),
		"-F without its file":          append(append([]string(nil), good[:len(good)-1]...), "-F"),
	}
	for name, args := range cases {
		if code := sshMainWithin(args, 2*time.Second); code != 255 {
			t.Errorf("%s: exit %d, want 255 (-1: it accepted the arguments and forwarded)", name, code)
		}
	}
	if _, err := os.Stat(socket); err == nil {
		t.Error("a refused ssh left a socket")
	}
	if lines := strings.Split(strings.TrimSpace(string(mustRead(t, filepath.Join(dir, "sshlog")))), "\n"); len(lines) != len(cases) {
		t.Errorf("%d log lines, want one per attempt (%d), refused ones included", len(lines), len(cases))
	}
}

// The options that keep the user's ssh configuration out of the forward are
// accepted, and -F is optional until the agent passes it: the log carries the
// arguments, so a test of the agent asserts them from there.
func TestSSHAcceptsTheConfigurationIsolation(t *testing.T) {
	dir := sshFiles(t)
	socket := sockPath(t, dir, "fwd.sock")
	good := sshArgsFor(dir, socket, 29290)
	has := func(args []string, want ...string) bool {
		for i := range args {
			if i+len(want) <= len(args) && strings.Join(args[i:i+len(want)], "\x00") == strings.Join(want, "\x00") {
				return true
			}
		}
		return false
	}
	if !has(good, "-F", "/dev/null") || !has(good, "-o", "ControlMaster=no") || !has(good, "-o", "ControlPath=none") {
		t.Fatalf("the reference arguments lack the isolation options: %q", good)
	}
	if code := sshMainWithin(good, time.Second); code != -1 {
		t.Errorf("with -F /dev/null, ControlMaster=no and ControlPath=none: exit %d, want it to accept and forward (-1)", code)
	}
	// an agent that does not pass -F yet is accepted
	var legacy []string
	for i := 0; i < len(good); i++ {
		if good[i] == "-F" {
			i++ // and its file
			continue
		}
		legacy = append(legacy, good[i])
	}
	if has(legacy, "-F") {
		t.Fatal("the legacy arguments still carry -F")
	}
	if code := sshMainWithin(legacy, time.Second); code != -1 {
		t.Errorf("without -F: exit %d, want it accepted (-1) until the agent passes it", code)
	}
}

// echoServer answers every line with "pong:" and the line.
func echoServer(t *testing.T) (net.Listener, int) {
	t.Helper()
	ln := listenTestPort(t)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				rd := bufio.NewReader(c)
				for {
					line, err := rd.ReadString('\n')
					if err != nil {
						return
					}
					_, _ = c.Write([]byte("pong:" + line))
				}
			}()
		}
	}()
	return ln, ln.Addr().(*net.TCPAddr).Port
}

func runSSH(t *testing.T, env []string, args []string) *proc {
	t.Helper()
	cmd := exec.Command(filepath.Join(installed(t), "ssh"), args...)
	cmd.Env = env
	return start(t, cmd)
}

func roundTrip(t *testing.T, socket, msg string) string {
	t.Helper()
	c, err := net.DialTimeout("unix", socket, 2*time.Second)
	must(t, err)
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := c.Write([]byte(msg + "\n")); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(c).ReadString('\n')
	must(t, err)
	return strings.TrimSuffix(line, "\n")
}

func TestSSHForwardsTheSocketToTCP(t *testing.T) {
	dir := sshFiles(t)
	_, port := echoServer(t)
	socket := sockPath(t, dir, "fwd.sock")
	must(t, os.WriteFile(socket, []byte("stale"), 0o600)) // a stale file at the socket path is replaced
	logPath := filepath.Join(dir, "sshlog")
	args := sshArgsFor(dir, socket, port)
	p := runSSH(t, []string{EnvSSHLog + "=" + logPath}, args)
	waitSocket(t, socket)

	if got := roundTrip(t, socket, "ping"); got != "pong:ping" {
		t.Errorf("through the forward: %q, want pong:ping", got)
	}
	if got := roundTrip(t, socket, "again"); got != "pong:again" {
		t.Errorf("a second connection: %q, want pong:again", got)
	}
	if lines := decodeLines(t, strings.Split(strings.TrimSpace(string(mustRead(t, logPath))), "\n")); strings.Join(toStrings(lines[0]["argv"]), "\x00") != strings.Join(args, "\x00") {
		t.Errorf("the logged argv is %v, want %v", lines[0]["argv"], args)
	}

	p.signalGroup(syscall.SIGTERM)
	if code := p.exitCode(t, 2*time.Second); code != 0 {
		t.Errorf("exit %d on TERM, want 0", code)
	}
	if _, err := os.Stat(socket); !os.IsNotExist(err) {
		t.Errorf("the socket remains after TERM: %v", err)
	}
}

// HB_FAKE_SSH_DIE_AFTER: the forward works, then ssh exits 255 and takes its
// socket with it, as when the connection is lost.
func TestSSHDiesAfterTheGivenTime(t *testing.T) {
	dir := sshFiles(t)
	_, port := echoServer(t)
	socket := sockPath(t, dir, "fwd.sock")
	t0 := time.Now()
	p := runSSH(t, []string{EnvSSHDieAfter + "=2s"}, sshArgsFor(dir, socket, port))
	waitSocket(t, socket)
	if got := roundTrip(t, socket, "x"); got != "pong:x" {
		t.Errorf("before it dies: %q", got)
	}
	if code := p.exitCode(t, 15*time.Second); code != 255 || time.Since(t0) < 2*time.Second {
		t.Errorf("exit %d after %v, want 255 after at least 2s", code, time.Since(t0))
	}
	if _, err := os.Stat(socket); !os.IsNotExist(err) {
		t.Errorf("the socket remains after the death: %v", err)
	}
}
