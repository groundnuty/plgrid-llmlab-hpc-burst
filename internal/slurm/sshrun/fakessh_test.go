package sshrun

// A fake ssh client for the transport's tests. The test binary runs as it when
// HPCBURST_FAKE_SSH is set (TestMain). It models what the transport relies on
// in OpenSSH, and records every invocation in a log so tests count
// connections from what happened, not from the transport's return values:
//
//   - master mode (ControlMaster=yes with -N): one "connection" to the login
//     node; it listens on the ControlPath until SIGTERM, or until the file
//     "drop" appears in the state directory (a dropped connection). The file
//     "refuse" holds a count of master attempts to refuse (a login
//     throttle), each refused one exiting 255 at once.
//   - -O check: exit 0 when the ControlPath answers, 255 otherwise. While
//     the file "check-hang" exists in the state directory it connects, logs
//     "check-hang" and then waits (up to an hour) for the file to go away, as
//     a client does that waits on a master that never answers.
//   - client mode (ControlMaster=no): connects to the ControlPath; without a
//     master it exits 255, as ProxyCommand=/bin/false makes real ssh do.
//     With one it runs the remote line with /bin/sh -c in the state
//     directory's "remote" environment: HOME and SCRATCH from the files
//     "home" and "scratch", PATH from "bin" first.
//
// Like ssh, the fake takes the login node, user, key, port and known-hosts
// file from -o options first and from the "Host hpcburst-login" block of the
// -F file second, and it exits 255 without them (no host name to connect to)
// or with an unreadable -F file. The values it resolved go to the file
// "resolved" in the state directory, one Key=value per line, not to the log,
// so a test reads the logged arguments for what ssh's command line exposed
// and "resolved" for what the connection used.
//
// The log is one line per invocation: a mode, then the arguments, then the
// environment's variable names, tab-separated.

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const fakeEnv = "HPCBURST_FAKE_SSH"

type fakeArgs struct {
	opts    map[string]string // from -o
	conn    map[string]string // from the -F file's Host block
	config  string            // -F
	control string            // -O
	noCmd   bool              // -N
	command string
	all     []string
}

// connKeys are the options the transport keeps in the configuration file.
var connKeys = []string{"HostName", "User", "IdentityFile", "Port", "UserKnownHostsFile"}

// get returns the value ssh would use: the command line's first, then the
// configuration file's.
func (a fakeArgs) get(k string) string {
	if v, ok := a.opts[k]; ok {
		return v
	}
	return a.conn[k]
}

// loadConfig reads the Host block for the alias from the -F file, the first
// value of a keyword winning.
func loadConfig(path string) (map[string]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	conn := map[string]string{}
	in := false
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, _ := strings.Cut(line, " ")
		v = strings.Trim(strings.TrimSpace(v), `"`)
		if strings.EqualFold(k, "Host") {
			in = v == alias
			continue
		}
		if _, dup := conn[k]; in && !dup {
			conn[k] = v
		}
	}
	return conn, nil
}

// resolve fills a.conn from the -F file and reports what ssh would fail on.
func resolve(dir string, a *fakeArgs) string {
	if a.config != "" {
		conn, err := loadConfig(a.config)
		if err != nil {
			return "Can't open user config file: No such file or directory"
		}
		a.conn = conn
	}
	if a.get("HostName") == "" || a.get("User") == "" || a.get("IdentityFile") == "" {
		return "ssh: Could not resolve hostname " + alias + ": Name or service not known"
	}
	var lines []string
	for _, k := range connKeys {
		lines = append(lines, k+"="+a.get(k))
	}
	_ = os.WriteFile(filepath.Join(dir, "resolved"), []byte(strings.Join(lines, "\n")+"\n"), 0o600)
	return ""
}

func parseFake(args []string) fakeArgs {
	a := fakeArgs{opts: map[string]string{}, all: args}
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-F":
			i++
			a.config = args[i]
		case "-o":
			i++
			k, v, _ := strings.Cut(args[i], "=")
			if _, dup := a.opts[k]; !dup {
				a.opts[k] = v
			}
		case "-O":
			i++
			a.control = args[i]
		case "-N":
			a.noCmd = true
		case "-T":
		case "--":
			a.command = strings.Join(args[i+1:], " ")
			return a
		default:
			// the host argument
		}
	}
	return a
}

func fakeLog(dir, mode string, a fakeArgs) {
	var names []string
	for _, kv := range os.Environ() {
		k, _, _ := strings.Cut(kv, "=")
		names = append(names, k)
	}
	f, err := os.OpenFile(filepath.Join(dir, "log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer f.Close()
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
	fmt.Fprintf(f, "%s\t%s\t%s\n", mode, strings.Join(a.all, " "), strings.Join(names, ","))
}

func fakeSSHMain(args []string) int {
	dir := os.Getenv("FAKE_SSH_DIR")
	a := parseFake(args)
	sock := a.opts["ControlPath"]
	if msg := resolve(dir, &a); msg != "" {
		fakeLog(dir, "unresolved", a)
		fmt.Fprintln(os.Stderr, msg)
		return 255
	}
	switch {
	case a.control == "check":
		c, err := net.Dial("unix", sock)
		if err != nil {
			return 255
		}
		defer c.Close()
		fakeLog(dir, "check", a)
		return stallCheck(dir, a)
	case a.control != "":
		return 255
	case a.opts["ControlMaster"] == "yes" && a.noCmd:
		return fakeMaster(dir, sock, a)
	case a.opts["ControlMaster"] == "no":
		return fakeClient(dir, sock, a)
	default:
		fakeLog(dir, "unexpected", a)
		fmt.Fprintln(os.Stderr, "fake ssh: unexpected invocation")
		return 255
	}
}

// stallCheck holds a connected -O check while "check-hang" exists, then
// reports success. It models a master that accepted the connection and does
// not answer. It polls instead of blocking on a read so that removing the file
// (a test's cleanup) always frees it; the state directory going away does too.
func stallCheck(dir string, a fakeArgs) int {
	hang := filepath.Join(dir, "check-hang")
	if _, err := os.Stat(hang); err != nil {
		return 0
	}
	fakeLog(dir, "check-hang", a)
	for deadline := time.Now().Add(time.Hour); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if _, err := os.Stat(hang); err != nil {
			return 255
		}
	}
	return 255
}

func takeRefusal(dir string) bool {
	p := filepath.Join(dir, "refuse")
	b, err := os.ReadFile(p)
	if err != nil {
		return false
	}
	n, _ := strconv.Atoi(strings.TrimSpace(string(b)))
	if n <= 0 {
		return false
	}
	_ = os.WriteFile(p, []byte(strconv.Itoa(n-1)), 0o600)
	return true
}

func fakeMaster(dir, sock string, a fakeArgs) int {
	if takeRefusal(dir) {
		fakeLog(dir, "master-refused", a)
		fmt.Fprintln(os.Stderr, "Connection closed by example-host port 22")
		return 255
	}
	l, err := net.Listen("unix", sock)
	if err != nil {
		fakeLog(dir, "master-bind-failed", a)
		return 255
	}
	fakeLog(dir, "master", a)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	for {
		select {
		case <-stop:
			l.Close()
			os.Remove(sock)
			fakeLog(dir, "master-stopped", a)
			return 0
		case <-time.After(20 * time.Millisecond):
			if _, err := os.Stat(filepath.Join(dir, "drop")); err == nil {
				os.Remove(filepath.Join(dir, "drop"))
				l.Close()
				os.Remove(sock)
				fakeLog(dir, "master-dropped", a)
				fmt.Fprintln(os.Stderr, "Connection to example-host closed by remote host.")
				return 255
			}
		}
	}
}

// fakeClient runs the remote line through /bin/sh -c, counting concurrent
// clients in the file "active" and the highest count in "max".
func fakeClient(dir, sock string, a fakeArgs) int {
	c, err := net.Dial("unix", sock)
	if err != nil {
		fakeLog(dir, "client-no-master", a)
		fmt.Fprintln(os.Stderr, "kex_exchange_identification: Connection closed by remote host")
		return 255
	}
	c.Close()
	fakeLog(dir, "client", a)
	bump(dir, +1)
	defer bump(dir, -1)
	env := []string{"PATH=" + filepath.Join(dir, "bin") + ":/usr/bin:/bin"}
	for _, v := range []string{"home", "scratch"} {
		if b, err := os.ReadFile(filepath.Join(dir, v)); err == nil {
			env = append(env, strings.ToUpper(v)+"="+strings.TrimSpace(string(b)))
		}
	}
	cmd := exec.Command("/bin/sh", "-c", a.command)
	cmd.Env = env
	cmd.Dir = dir
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		if x, ok := err.(*exec.ExitError); ok {
			return x.ExitCode()
		}
		return 1
	}
	return 0
}

func bump(dir string, d int) {
	f, err := os.OpenFile(filepath.Join(dir, "active.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return
	}
	defer f.Close()
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
	read := func(name string) int {
		b, _ := os.ReadFile(filepath.Join(dir, name))
		n, _ := strconv.Atoi(strings.TrimSpace(string(b)))
		return n
	}
	n := read("active") + d
	_ = os.WriteFile(filepath.Join(dir, "active"), []byte(strconv.Itoa(n)), 0o600)
	if n > read("max") {
		_ = os.WriteFile(filepath.Join(dir, "max"), []byte(strconv.Itoa(n)), 0o600)
	}
}

// fakeLogLines returns the log's lines whose mode is mode ("" for all).
func fakeLogLines(dir, mode string) []string {
	f, err := os.Open(filepath.Join(dir, "log"))
	if err != nil {
		return nil
	}
	defer f.Close()
	var out []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		if m, _, _ := strings.Cut(sc.Text(), "\t"); mode == "" || m == mode {
			out = append(out, sc.Text())
		}
	}
	return out
}
