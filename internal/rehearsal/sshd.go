package rehearsal

// New, after internal/agent/tunnelrig_test.go (startSSHServer): a user-level
// OpenSSH sshd in inetd mode (sshd -i), one process per connection, as the
// current user and behind a listener the node holds, so the port is
// kernel-chosen and cannot be lost. The login node and the dial-home gateway
// both run on it.

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
)

// safeText is what a path may hold to go into authorized_keys' command="..."
// and sshd's configuration, where the user's login shell reads it: letters,
// digits and ._:=,/+@-.
var safeText = regexp.MustCompile(`^[A-Za-z0-9._:=,/+@-]+$`)

// sshdFiles are the files the login node writes in its directory.
type sshdFiles struct {
	dir                                  string
	hostKey, clientKey, authorized, conf string
	knownHosts, log                      string
}

func filesIn(dir string) sshdFiles {
	j := func(n string) string { return filepath.Join(dir, n) }
	return sshdFiles{
		dir: dir, hostKey: j("host_key"), clientKey: j("id"), authorized: j("authorized_keys"),
		conf: j("sshd_config"), knownHosts: j("known_hosts"), log: j("sshd.log"),
	}
}

// keygen makes an ed25519 key pair without a passphrase and returns the
// public key's two fields (type and base64 body).
func keygen(path string) (typ, body string, err error) {
	if out, err := exec.Command("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", "hb-rehearsal", "-f", path).CombinedOutput(); err != nil {
		return "", "", fmt.Errorf("ssh-keygen: %v: %s", err, strings.TrimSpace(string(out)))
	}
	b, err := os.ReadFile(path + ".pub")
	if err != nil {
		return "", "", err
	}
	f := strings.Fields(string(b))
	if len(f) < 2 {
		return "", "", fmt.Errorf("ssh-keygen wrote an unreadable public key")
	}
	return f[0], f[1], nil
}

// sshdSpec is what differs between the nodes that run on an sshd: the key's
// options and the configuration lines after the common ones.
type sshdSpec struct {
	// user is the OS account sshd logs in; a non-root sshd can log in no
	// other.
	user string
	// options is what precedes the client key in authorized_keys.
	options string
	// conf are sshd_config lines.
	conf []string
}

// sshdUser is the OS account that runs the test, read at run time, checked
// for the characters sshd's configuration allows.
func sshdUser() (string, error) {
	u, err := user.Current()
	if err != nil {
		return "", fmt.Errorf("rehearsal: the current user: %w", err)
	}
	if !safeText.MatchString(u.Username) {
		return "", errors.New("rehearsal: the current user name holds a character that is not allowed in sshd's configuration")
	}
	return u.Username, nil
}

// checkDir refuses a directory that would break sshd's files or leak them: it
// must be absolute, existing, private and made of safe characters. why says
// what a loose mode would expose.
func checkDir(dir, why string) error {
	fi, err := os.Stat(dir)
	switch {
	case dir == "" || !filepath.IsAbs(dir):
		return errors.New("rehearsal: Dir must be an absolute path")
	case err != nil || !fi.IsDir():
		return errors.New("rehearsal: Dir must be an existing directory")
	case fi.Mode().Perm()&0o077 != 0:
		return errors.New("rehearsal: Dir must have mode 700: " + why)
	case !safeText.MatchString(dir):
		return errors.New("rehearsal: Dir must hold only letters, digits and ._:=,/+@-: sshd's configuration reads it")
	}
	return nil
}

// writeSSHDFiles writes the host key, the client key, authorized_keys with
// spec.options, sshd_config and known_hosts for the address.
func writeSSHDFiles(f sshdFiles, spec sshdSpec, port int) error {
	hostType, hostBody, err := keygen(f.hostKey)
	if err != nil {
		return err
	}
	clientType, clientBody, err := keygen(f.clientKey)
	if err != nil {
		return err
	}
	auth := fmt.Sprintf("%s %s %s\n", spec.options, clientType, clientBody)
	conf := strings.Join(slices.Concat([]string{
		"HostKey " + f.hostKey,
		"PidFile none",
		"AuthorizedKeysFile " + f.authorized,
		"UsePAM no", "StrictModes no",
		"PasswordAuthentication no", "KbdInteractiveAuthentication no", "PubkeyAuthentication yes",
		"AllowUsers " + spec.user,
		"LogLevel VERBOSE",
	}, spec.conf, []string{""}), "\n")
	known := fmt.Sprintf("[127.0.0.1]:%d %s %s\n", port, hostType, hostBody)
	for _, w := range []struct{ path, content string }{
		{f.authorized, auth}, {f.conf, conf}, {f.knownHosts, known},
	} {
		if err := os.WriteFile(w.path, []byte(w.content), 0o600); err != nil {
			return err
		}
	}
	return nil
}

// writeAtomic writes data at path with mode 600, by a temporary file in the
// same directory and a rename, so a reader never sees a partial file.
func writeAtomic(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".tmp-*") // mode 600
	if err != nil {
		return err
	}
	_, werr := f.Write(data)
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr == nil {
		werr = os.Rename(f.Name(), path)
	}
	if werr != nil {
		_ = os.Remove(f.Name())
	}
	return werr
}

// sshdServer is a listener on a kernel-chosen 127.0.0.1 port with an sshd per
// accepted connection behind it, and the log they write.
type sshdServer struct {
	ln   net.Listener
	pool *sshdPool
	log  *os.File
}

// startSSHD writes the files and starts serving. A client can connect as
// soon as it returns.
func startSSHD(bin string, f sshdFiles, spec sshdSpec) (*sshdServer, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("rehearsal: listen: %w", err)
	}
	if err := writeSSHDFiles(f, spec, ln.Addr().(*net.TCPAddr).Port); err != nil {
		ln.Close()
		return nil, fmt.Errorf("rehearsal: write sshd's files: %w", err)
	}
	log, err := os.Create(f.log)
	if err != nil {
		ln.Close()
		return nil, err
	}
	s := &sshdServer{ln: ln, log: log, pool: &sshdPool{bin: bin, conf: f.conf, log: log}}
	s.pool.wg.Add(1)
	go s.pool.serve(ln)
	return s, nil
}

func (s *sshdServer) port() int { return s.ln.Addr().(*net.TCPAddr).Port }

// stopListening makes no new connection reach an sshd.
func (s *sshdServer) stopListening() { s.ln.Close() }

// stop ends every sshd and everything seen under it, with extra, and waits
// until they are gone (sshdPool.stop). The listener must be closed first.
func (s *sshdServer) stop(extra []procRow) {
	s.pool.stop(extra)
	s.log.Close()
}

// sshdProc is one sshd the node started.
type sshdProc struct {
	cmd  *exec.Cmd
	self procRow
	done chan struct{}
	// below are the processes seen under it while it lived; mu guards them.
	mu    sync.Mutex
	below map[int]procRow
}

func (p *sshdProc) record() {
	rows := below(p.self.pid)
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range rows {
		p.below[r.pid] = r
	}
}

func (p *sshdProc) seen() []procRow {
	p.mu.Lock()
	defer p.mu.Unlock()
	rows := make([]procRow, 0, len(p.below))
	for _, r := range p.below {
		rows = append(rows, r)
	}
	return rows
}

// sshdPool starts an sshd per accepted connection and stops them all.
type sshdPool struct {
	bin, conf string
	log       *os.File

	mu     sync.Mutex
	procs  []*sshdProc
	closed bool
	wg     sync.WaitGroup
}

// serve hands each connection the listener accepts to a new sshd, until the
// listener closes.
func (s *sshdPool) serve(ln net.Listener) {
	defer s.wg.Done()
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		s.spawn(c.(*net.TCPConn))
	}
}

// recordEvery is how often a live sshd's descendants are recorded. It is a
// sampling interval, not a wait: Close also records them once more.
const recordEvery = 50 * time.Millisecond

func (s *sshdPool) spawn(c *net.TCPConn) {
	defer c.Close()
	f, err := c.File()
	if err != nil {
		return
	}
	defer f.Close()
	cmd := exec.Command(s.bin, "-i", "-e", "-f", s.conf)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = f, f, s.log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	if err := cmd.Start(); err != nil {
		s.mu.Unlock()
		return
	}
	self, _ := readProcRow(cmd.Process.Pid)
	p := &sshdProc{cmd: cmd, self: self, done: make(chan struct{}), below: map[int]procRow{}}
	s.procs = append(s.procs, p)
	s.wg.Add(2)
	s.mu.Unlock()
	go func() { defer s.wg.Done(); _ = cmd.Wait(); close(p.done) }()
	go func() {
		defer s.wg.Done()
		tick := time.NewTicker(recordEvery)
		defer tick.Stop()
		for {
			p.record()
			select {
			case <-p.done:
				return
			case <-tick.C:
			}
		}
	}()
}

// stopWait is how long the TERM phase lasts before the KILL.
const stopWait = 5 * time.Second

// stop ends every sshd and everything seen under it. The caller has stopped
// the listener and the relay, so the forced commands end on their own; TERM
// then KILL, by PID and start time, take what does not.
func (s *sshdPool) stop(extra []procRow) {
	s.mu.Lock()
	s.closed = true
	procs := append([]*sshdProc(nil), s.procs...)
	s.mu.Unlock()
	var all []procRow
	for _, p := range procs {
		p.record()
		if p.self.pid != 0 {
			all = append(all, p.self)
		}
		all = append(all, p.seen()...)
	}
	all = append(all, extra...)
	signalProcs(all, syscall.SIGTERM)
	deadline := time.Now().Add(stopWait)
	for anyLive(all) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	signalProcs(all, syscall.SIGKILL)
	s.wg.Wait()
	for time.Now().Before(deadline.Add(stopWait)) && anyLive(all) {
		time.Sleep(20 * time.Millisecond)
	}
}
