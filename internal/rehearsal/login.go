// Package rehearsal is test support for the one-host rehearsal (issue #146):
// a fake Slurm login node that the controller's real ssh client reaches over a
// real sshd. Only _test.go files import it, as they do internal/agent/
// enginedouble.
//
// # How it works
//
// StartLoginNode runs a user-level OpenSSH sshd in inetd mode (sshd -i, one
// process per connection, as the current user) behind a 127.0.0.1 listener on
// a kernel-chosen port. The client key's line in authorized_keys forces a
// command: this test binary again, with HB_REHEARSAL_ROLE=slurm-relay, which
// TestMain's Dispatch turns into the relay client. The relay client sends
// SSH_ORIGINAL_COMMAND and all of standard input over a unix socket to the
// test process, where one run-mode (or in-memory) fakeslurm Platform answers
// as a Session, and writes back stdout and stderr and the exit code. The
// Platform therefore belongs to the test: it outlives a restart of the
// controller, and the test reads its jobs and files directly.
//
// ssh's ControlMaster (ssh -N) opens no session, so no command runs for it;
// each mux client opens one session, so one relayed command.
//
// # Choices
//
//   - A runner error of the Session (slurm.ErrNotStarted, ErrTimeout,
//     ErrTransport) becomes exit 255 and one line of standard error. sshrun
//     reads 255 as ssh's own failure, slurm.ErrTransport, so a fake
//     ErrNotStarted reaches the controller as ErrTransport.
//   - The command line goes to the fake verbatim, never through a shell: the
//     fake parses its own quotes. (sshd itself runs the forced command, not the
//     requested one, through the user's login shell, so the paths in the forced
//     command are restricted to a safe character set.)
//   - Close never closes the Platform; a run-mode Platform is the caller's to
//     close, after Close.
//   - The credentials file names the OS account that runs the test, because a
//     non-root sshd can only log in as the account it runs as.
//
// Provenance: new package. The sshd harness follows internal/agent/
// tunnelrig_test.go (startSSHServer); the process helpers follow
// internal/slurm/fakeslurm/procfs.go.
package rehearsal

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
)

// maxSocketPath is the longest unix socket path accepted: Linux limits it to
// 108 bytes including the NUL.
const maxSocketPath = 100

// alias is the Host line of the credentials file; the transport ignores it.
const alias = "hpcburst-rehearsal"

// LoginConfig configures a login node.
type LoginConfig struct {
	// Dir is an existing, private (mode 700) scratch directory for sshd's
	// files, the keys and the relay socket. Keep it short: the socket path
	// must fit in 100 bytes.
	Dir string
	// Platform is the fake cluster, made by the caller, usually in run mode.
	Platform *fakeslurm.Platform
	// Login is the cluster user's session: its name and its environment (a
	// HOME under the run-mode root).
	Login fakeslurm.Login
	// SSHD is the sshd binary; "" means $HPCBURST_SSHD, then /usr/sbin/sshd.
	SSHD string
}

// LoginNode is a running fake login node.
type LoginNode struct {
	cfg   LoginConfig
	files sshdFiles
	user  string
	sock  string
	sess  *fakeslurm.Session

	ln    net.Listener
	pool  *sshdPool
	relay *relayServer
	log   *os.File

	closeOnce sync.Once
}

// StartLoginNode starts the sshd listener and the relay. It fails, rather
// than guesses, when a path is unsafe, the directory is not private or a tool
// is missing.
func StartLoginNode(cfg LoginConfig) (*LoginNode, error) {
	if cfg.Platform == nil {
		return nil, errors.New("rehearsal: no Platform")
	}
	if cfg.SSHD == "" {
		cfg.SSHD = SSHDPath()
	}
	if err := CheckTools(cfg.SSHD); err != nil {
		return nil, err
	}
	exe, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("rehearsal: the running binary: %w", err)
	}
	n := &LoginNode{cfg: cfg, files: filesIn(cfg.Dir), sock: filepath.Join(cfg.Dir, "relay.sock")}
	if err := n.check(exe); err != nil {
		return nil, err
	}
	u, err := user.Current()
	if err != nil {
		return nil, fmt.Errorf("rehearsal: the current user: %w", err)
	}
	n.user = u.Username
	if !safeText.MatchString(n.user) {
		return nil, errors.New("rehearsal: the current user name holds a character that is not allowed in sshd's configuration")
	}
	n.sess = cfg.Platform.Login(cfg.Login)
	if n.ln, err = net.Listen("tcp", "127.0.0.1:0"); err != nil {
		return nil, fmt.Errorf("rehearsal: listen: %w", err)
	}
	if _, err := slurm.ParseSSHConfig([]byte(n.SSHConfig()), ""); err != nil {
		n.ln.Close()
		return nil, fmt.Errorf("rehearsal: the controller's parser refuses the login node's credentials: %w", err)
	}
	forced := fmt.Sprintf("%s=%s %s=%s %s", EnvRole, RoleRelay, EnvRelay, n.sock, exe)
	if err := writeSSHDFiles(n.files, n.user, forced, n.port()); err != nil {
		n.ln.Close()
		return nil, fmt.Errorf("rehearsal: write sshd's files: %w", err)
	}
	if n.log, err = os.Create(n.files.log); err != nil {
		n.ln.Close()
		return nil, err
	}
	if n.relay, err = startRelay(n.sock, n.sess); err != nil {
		n.ln.Close()
		n.log.Close()
		return nil, err
	}
	n.pool = &sshdPool{bin: cfg.SSHD, conf: n.files.conf, log: n.log}
	n.pool.wg.Add(1)
	go n.pool.serve(n.ln)
	return n, nil
}

// check refuses what would break the forced command or the socket.
func (n *LoginNode) check(exe string) error {
	fi, err := os.Stat(n.cfg.Dir)
	switch {
	case n.cfg.Dir == "" || !filepath.IsAbs(n.cfg.Dir):
		return errors.New("rehearsal: Dir must be an absolute path")
	case err != nil || !fi.IsDir():
		return errors.New("rehearsal: Dir must be an existing directory")
	case fi.Mode().Perm()&0o077 != 0:
		return errors.New("rehearsal: Dir must have mode 700: the relay socket in it runs commands on the fake cluster")
	case !safeText.MatchString(n.cfg.Dir) || !safeText.MatchString(exe):
		return errors.New("rehearsal: Dir and the test binary's path must hold only letters, digits and ._:=,/+@-: sshd's login shell reads them")
	case len(n.sock) > maxSocketPath:
		return fmt.Errorf("rehearsal: the relay socket path is %d bytes, above %d: use a shorter Dir", len(n.sock), maxSocketPath)
	}
	return nil
}

func (n *LoginNode) port() int { return n.ln.Addr().(*net.TCPAddr).Port }

// Addr is the sshd listener's address, 127.0.0.1:<kernel port>.
func (n *LoginNode) Addr() string { return n.ln.Addr().String() }

// Session is the cluster user's session that the relay runs commands as. The
// test can use it too, for example to read what the controller did.
func (n *LoginNode) Session() *fakeslurm.Session { return n.sess }

// SSHConfig is a credentials file in ssh_config form, for the Provider's
// credentialsRef: the user is the OS account that runs the test, read at run
// time, and the key and known-hosts file are in Dir.
func (n *LoginNode) SSHConfig() string {
	return "Host " + alias + "\n" +
		"  HostName 127.0.0.1\n" +
		"  Port " + strconv.Itoa(n.port()) + "\n" +
		"  User " + n.user + "\n" +
		"  IdentityFile " + n.files.clientKey + "\n" +
		"  UserKnownHostsFile " + n.files.knownHosts + "\n" +
		"  StrictHostKeyChecking yes\n" +
		"  IdentitiesOnly yes\n"
}

// WriteCredentials writes SSHConfig at path with mode 600, by a temporary
// file in the same directory and a rename, so a reader never sees a partial
// file.
func (n *LoginNode) WriteCredentials(path string) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".creds-*") // mode 600
	if err != nil {
		return err
	}
	_, werr := f.WriteString(n.SSHConfig())
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

// Close stops the listener, the relay and every sshd child with the
// processes below them, and waits until they are gone. It never closes the
// Platform. It is safe to call twice.
func (n *LoginNode) Close() error {
	n.closeOnce.Do(func() {
		n.ln.Close()
		n.relay.close()
		n.pool.stop(withRelayEnv(n.sock))
		n.log.Close()
		_ = os.Remove(n.sock)
	})
	return nil
}
