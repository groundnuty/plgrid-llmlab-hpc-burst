package sshrun

// New for HBEP-6's host-dialed mode (PR 298): the ssh clients of the relays.

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
)

// RelayOptions configure a Relay.
type RelayOptions struct {
	// Name identifies the owner, the Provider; it enters the configuration
	// file's name.
	Name string
	// Config is the Provider's parsed credentials.
	Config slurm.SSHConfig
	// ControlDir holds the relays' configuration file: a directory of this
	// user's, mode 700, as for the transport.
	ControlDir string
	// Binary is the ssh client; "" means "ssh" on PATH.
	Binary string
	// Env is the child's environment; nil means PATH and HOME of this
	// process only. It must not carry SSH_AUTH_SOCK.
	Env []string
}

// Relay makes the ssh clients of host-dialed mode's relays (HBEP-6,
// Dial-home modes, host-dialed mode): one process per relay, each on a
// connection of its own to the Provider's login node, never the transport's
// shared master (HBEP-4; L-7.3), with BatchMode, ServerAliveInterval=10 and
// ServerAliveCountMax=3. The credentials are in a private -F file, as for the
// transport, so no login node, user or key path is on a command line.
type Relay struct {
	o      RelayOptions
	config string

	mu     sync.Mutex
	closed bool
}

// NewRelay writes the relays' configuration file and returns the maker. Close
// removes the file.
func NewRelay(o RelayOptions) (*Relay, error) {
	if o.Name == "" {
		return nil, errors.New("sshrun: no name")
	}
	if o.Binary == "" {
		o.Binary = "ssh"
	}
	if o.Env == nil {
		o.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME")}
	}
	for _, kv := range o.Env {
		if strings.HasPrefix(kv, "SSH_AUTH_SOCK=") {
			return nil, errors.New("sshrun: the environment must not carry SSH_AUTH_SOCK")
		}
	}
	if err := checkConfigValues(o.Config); err != nil {
		return nil, err
	}
	if err := privateDir(o.ControlDir); err != nil {
		return nil, err
	}
	_, conf, err := controlPaths(Options{Name: o.Name + "-relay", Config: o.Config, ControlDir: o.ControlDir})
	if err != nil {
		return nil, err
	}
	r := &Relay{o: o, config: conf}
	if err := writeConfigFile(o.ControlDir, r.config, o.Config); err != nil {
		return nil, err
	}
	return r, nil
}

// RelayArgs are a relay's ssh arguments for line, with the configuration file
// config. ControlMaster=no and ControlPath=none keep it off every master, the
// transport's included, so it opens a connection of its own. It carries no
// forward and no terminal, and no escape character, since the session's bytes
// are binary.
func RelayArgs(config, line string) []string {
	return []string{"-F", config,
		"-o", "BatchMode=yes",
		"-o", "IdentitiesOnly=yes",
		"-o", "IdentityAgent=none",
		"-o", "ClearAllForwardings=yes",
		"-o", "ForwardAgent=no",
		"-o", "ForwardX11=no",
		"-o", "StrictHostKeyChecking=yes",
		"-o", "UpdateHostKeys=no",
		"-o", "ServerAliveInterval=10",
		"-o", "ServerAliveCountMax=3",
		"-o", "ConnectTimeout=20",
		"-o", "LogLevel=ERROR",
		"-o", "ControlMaster=no",
		"-o", "ControlPath=none",
		"-o", "EscapeChar=none",
		"-T", alias, "--", line,
	}
}

// Command returns the ssh client that runs line on the login node. Its
// standard error is kept, up to 4 KiB, for Classify; the caller sets the
// other streams and starts it. After Close it fails: a relay of credentials
// that were replaced never starts again on them, and never writes their file
// again (review of PR 301, F1).
func (r *Relay) Command(line string) (*exec.Cmd, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errors.New("sshrun: the relay's credentials were replaced")
	}
	if _, err := os.Stat(r.config); err != nil {
		// Something removed it: write it again rather than fail every start.
		if err := writeConfigFile(r.o.ControlDir, r.config, r.o.Config); err != nil {
			return nil, err
		}
	}
	return r.command(line)
}

func (r *Relay) command(line string) (*exec.Cmd, error) {
	cmd := exec.Command(r.o.Binary, RelayArgs(r.config, line)...)
	cmd.Env = r.o.Env
	cmd.Stderr = &limited{w: &bytes.Buffer{}, n: 4096}
	return cmd, nil
}

// Config is the relays' configuration file, for a check that a relay runs on
// these credentials.
func (r *Relay) Config() string { return r.config }

// Classify names the kind of a relay's failure from its kept standard error.
// A line that the relay step wrote itself, starting "hb-relay: " (its fixed
// messages, which name no path, host or user: hostdial.RelayMessage), is
// returned as it is, cut to 200 bytes; otherwise the kind of the ssh failure
// is named without repeating any of it.
func Classify(cmd *exec.Cmd) string {
	l, ok := cmd.Stderr.(*limited)
	if !ok {
		return "ssh failed"
	}
	for _, line := range strings.Split(l.w.String(), "\n") {
		if i := strings.Index(line, relayLinePrefix); i >= 0 {
			line = line[i:]
			if len(line) > 200 {
				line = line[:200]
			}
			return line
		}
	}
	return classify(l.w.String())
}

// relayLinePrefix starts the relay step's own messages on its standard error.
const relayLinePrefix = "hb-relay: "

// Login is the login node as the per-login-node cap names it: host:port, the
// port 22 when the credentials name none.
func (r *Relay) Login() string { return LoginOf(r.o.Config) }

// LoginOf is a Provider's login node as host:port.
func LoginOf(c slurm.SSHConfig) string {
	port := c.Port
	if port == 0 {
		port = 22
	}
	return net.JoinHostPort(c.HostName, strconv.Itoa(port))
}

// Close removes the configuration file; later Commands fail.
func (r *Relay) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	if err := os.Remove(r.config); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("sshrun: remove the relays' configuration file: %w", err)
	}
	return nil
}
