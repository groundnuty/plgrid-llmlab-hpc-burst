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
// # Streaming an srun line
//
// A line whose first word is srun does not go as one request and one reply: it
// streams (issue #298). The relay of HBEP-6's host-dialed mode runs "srun -u
// --overlap --jobid=<id> --ntasks=1 <agent> relay <channel>" through this
// login node and carries one multiplexed session over the step's standard
// input and output for the job's whole life. The forced command copies
// standard input to the server as it arrives, and the step's standard output
// and error back as they arrive, in length-prefixed frames of at most 64 KiB
// (wire.go, Streaming), and exits with the step's exit code. The end of
// standard input is sent as an end frame and the output goes on (half-close).
// The server runs fakeslurm.Session.Stream, through LoginConfig.Around when it
// is set, with a run function whose Result carries only the exit code. When
// the ssh session goes away (the forced command finds its output pipes
// without a reader, or the connection closes) the step's context ends and the
// fake kills the step, as real srun cancels its step on SIGHUP; Close ends
// every live stream the same way. The server watches the connection for the
// client's hang-up apart from reading it (hangup.go), so the step ends even
// while its input is backed up and the frame reader waits to queue a frame.
// Bytes cross exact, in order and unchanged: no line translation, none added
// or dropped. Every other line keeps the
// request and reply format, byte for byte, and Command.Timeout is not part of a
// stream: the controller's own ssh client ends a command that runs past it,
// which ends the session.
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
	// Around, when set, runs each relayed command: it gets the command line
	// and a function that runs it on the Platform, which it must call once,
	// and returns that function's result. It runs in the test process, so a
	// test can read the Platform's state just before and just after a
	// command, at the command itself rather than at a sample (issue #193). For
	// an srun line, which streams, the function runs the whole stream and the
	// Result carries only the exit code; it returns when the step has ended.
	Around func(line string, run func() (slurm.Result, error)) (slurm.Result, error)
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
	closeErr  error
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
	if n.relay, err = startRelay(n.sock, n.sess, cfg.Around); err != nil {
		return nil, err
	}
	spec := sshdSpec{user: n.user, options: fmt.Sprintf("restrict,command=%q", forcedCommand(n.sock, exe)), conf: loginConf}
	if n.srv, err = startSSHD("login node", cfg.SSHD, n.files, spec); err != nil {
		n.relay.close()
		return nil, err
	}
	if _, err := slurm.ParseSSHConfig([]byte(n.SSHConfig()), ""); err != nil {
		return nil, errors.Join(fmt.Errorf("rehearsal: the controller's parser refuses the login node's credentials: %w", err), n.Close())
	}
	return n, nil
}

// forcedCommand is the login node's forced command: this binary as the relay
// for sock. The shell execs env, and env execs the binary, so the binary's
// parent is sshd's session process whatever the login shell's startup files
// do (an EXIT trap keeps bash from replacing itself with a last command): the
// relay guard (relayguard.go) requires that parent.
func forcedCommand(sock, exe string) string {
	return fmt.Sprintf("exec env %s=%s %s=%s %s", EnvRole, RoleRelay, EnvRelay, sock, exe)
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
// Platform. It is safe to call twice, and each call returns the first one's
// error: a process the stop refused to signal, since it was not the test's
// (issue #308).
func (n *LoginNode) Close() error {
	n.closeOnce.Do(func() {
		n.srv.stopListening()
		n.relay.close()
		n.closeErr = n.srv.stop(withRelayEnv(n.sock))
		_ = os.Remove(n.sock)
	})
	return n.closeErr
}
