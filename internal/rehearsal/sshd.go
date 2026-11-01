package rehearsal

// New, after internal/agent/tunnelrig_test.go (startSSHServer): a user-level
// OpenSSH sshd in inetd mode (sshd -i), one process per connection, as the
// current user and behind a listener the login node holds, so the port is
// kernel-chosen and cannot be lost.

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
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

// writeSSHDFiles writes the host key, the client key, authorized_keys with the
// forced command, sshd_config and known_hosts for the address.
func writeSSHDFiles(f sshdFiles, user, forced string, port int) error {
	hostType, hostBody, err := keygen(f.hostKey)
	if err != nil {
		return err
	}
	clientType, clientBody, err := keygen(f.clientKey)
	if err != nil {
		return err
	}
	// restrict switches off forwarding, a terminal, agent and X11 for the key.
	auth := fmt.Sprintf("restrict,command=%q %s %s\n", forced, clientType, clientBody)
	conf := strings.Join([]string{
		"HostKey " + f.hostKey,
		"PidFile none",
		"AuthorizedKeysFile " + f.authorized,
		"UsePAM no", "StrictModes no",
		"PasswordAuthentication no", "KbdInteractiveAuthentication no", "PubkeyAuthentication yes",
		"AllowUsers " + user,
		"AllowTcpForwarding no", "AllowStreamLocalForwarding no", "GatewayPorts no",
		"X11Forwarding no", "PermitTTY no", "PrintMotd no", "PermitUserEnvironment no",
		"LogLevel VERBOSE", "",
	}, "\n")
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
