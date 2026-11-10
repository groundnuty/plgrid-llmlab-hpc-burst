package sshrun

// New for HBEP-6's host-dialed mode (PR 298): the ssh clients of the relays.

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

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
	login  string
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
		"-o", "ConnectTimeout=" + strconv.Itoa(int(RelayConnectTimeout/time.Second)),
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

// RelayConnectTimeout is the relay's ssh ConnectTimeout: how long a relay may
// take to reach the login node before its ssh gives up.
const RelayConnectTimeout = 20 * time.Second

// Stderr is what an ended relay's command kept of its standard error, up to
// 4 KiB. It may name the host and the user, so it is for classifying the end
// (hostdial.ClassifyEnd), never for a log.
func Stderr(cmd *exec.Cmd) string {
	if l, ok := cmd.Stderr.(*limited); ok {
		return l.w.String()
	}
	return ""
}

// Login is the login node as the slots and the one-Provider-per-host rule
// name it: the host name as ssh -G resolves it through the relays' own
// configuration file (HBEP-6, Capacity), so an alias and a full name of one
// node are one host. When ssh -G fails, it is the configured HostName.
func (r *Relay) Login() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.login != "" {
		return r.login
	}
	r.login = r.o.Config.HostName
	cmd := exec.Command(r.o.Binary, "-G", "-F", r.config, alias)
	cmd.Env = r.o.Env
	if out, err := cmd.Output(); err == nil {
		for _, line := range strings.Split(string(out), "\n") {
			if h, ok := strings.CutPrefix(line, "hostname "); ok && strings.TrimSpace(h) != "" {
				r.login = strings.TrimSpace(h)
				break
			}
		}
	}
	return r.login
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
