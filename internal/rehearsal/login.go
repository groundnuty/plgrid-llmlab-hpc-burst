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
// # The dial-home gateway
//
// StartGateway runs the same kind of sshd (sshd.go) as the control-plane
// host's ssh server for the agent's ssh mode (HBEP-6, Dial-home modes): the
// client key's line is HBEP-6's restricted line for two loopback ports, the
// agent endpoint's and the front's dial-home listener's, and WriteDialHome
// puts the key and known_hosts where the agent looks for them. See gateway.go.
//
// Provenance: new package. The sshd harness follows internal/agent/
// tunnelrig_test.go (startSSHServer); the process helpers follow
// internal/slurm/fakeslurm/procfs.go.
package rehearsal

import (
	"errors"
	"fmt"
	"os"
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

	srv   *sshdServer
	relay *relayServer

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
	if n.user, err = sshdUser(); err != nil {
		return nil, err
	}
	n.sess = cfg.Platform.Login(cfg.Login)
	if n.relay, err = startRelay(n.sock, n.sess); err != nil {
		return nil, err
	}
	forced := fmt.Sprintf("%s=%s %s=%s %s", EnvRole, RoleRelay, EnvRelay, n.sock, exe)
	spec := sshdSpec{user: n.user, options: fmt.Sprintf("restrict,command=%q", forced), conf: loginConf}
	if n.srv, err = startSSHD(cfg.SSHD, n.files, spec); err != nil {
		n.relay.close()
		return nil, err
	}
	if _, err := slurm.ParseSSHConfig([]byte(n.SSHConfig()), ""); err != nil {
		_ = n.Close()
		return nil, fmt.Errorf("rehearsal: the controller's parser refuses the login node's credentials: %w", err)
	}
	return n, nil
}

// loginConf is sshd_config for the login node: commands only (the key's
// restrict switches off forwarding, a terminal, agent and X11 as well).
var loginConf = []string{
	"AllowTcpForwarding no", "AllowStreamLocalForwarding no", "GatewayPorts no",
	"X11Forwarding no", "PermitTTY no", "PrintMotd no", "PermitUserEnvironment no",
}

// check refuses what would break the forced command or the socket.
func (n *LoginNode) check(exe string) error {
	if err := checkDir(n.cfg.Dir, "the relay socket in it runs commands on the fake cluster"); err != nil {
		return err
	}
	switch {
	case !safeText.MatchString(exe):
		return errors.New("rehearsal: the test binary's path must hold only letters, digits and ._:=,/+@-: sshd's login shell reads it")
	case len(n.sock) > maxSocketPath:
		return fmt.Errorf("rehearsal: the relay socket path is %d bytes, above %d: use a shorter Dir", len(n.sock), maxSocketPath)
	}
	return nil
}

func (n *LoginNode) port() int { return n.srv.port() }

// Addr is the sshd listener's address, 127.0.0.1:<kernel port>.
func (n *LoginNode) Addr() string { return n.srv.ln.Addr().String() }

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
	return writeAtomic(path, []byte(n.SSHConfig()))
}

// Close stops the listener, the relay and every sshd child with the
// processes below them, and waits until they are gone. It never closes the
// Platform. It is safe to call twice.
func (n *LoginNode) Close() error {
	n.closeOnce.Do(func() {
		n.srv.stopListening()
		n.relay.close()
		n.srv.stop(withRelayEnv(n.sock))
		_ = os.Remove(n.sock)
	})
	return nil
}
