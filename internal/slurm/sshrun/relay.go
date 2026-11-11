package sshrun

// New for HBEP-6's host-dialed mode (PR 298): the ssh clients of the relays.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
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
	// ConfigFile, when set, is the configuration file's path: a stable name
	// directly in ControlDir, ending ".conf", for the relay keeper's own file
	// per Provider and credentials generation (HBEP-6, The relay keeper: "its
	// own private -F file for each Provider, under a stable name"; issue
	// #343), so a keeper that restarts writes the same file again. Close
	// removes it, as it removes a derived one. "" is alpha's: a name derived
	// from the credentials with a random part.
	ConfigFile string
	// LoginTimeout bounds Login's ssh -G; 0 means DefaultLoginTimeout.
	LoginTimeout time.Duration
	// Logger is for the guarded stop of a timed-out ssh -G (group.go); nil
	// means slog's default.
	Logger *slog.Logger
}

// DefaultLoginTimeout is how long Login lets ssh -G run (issue 403,
// `@hpc-burst-science-agent`'s ruling, 11 Nov 2026). ssh -G only reads the
// configuration, so it answers at once; it can stall only when something it
// runs on the way hangs, a Match exec command in the file.
const DefaultLoginTimeout = 10 * time.Second

// ErrLoginTimeout is what Login returns when ssh -G did not answer within its
// bound; the error wraps it, so errors.Is finds it.
var ErrLoginTimeout = errors.New("sshrun: ssh -G timed out")

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

	// signalGroup is killGroup; a field so the leader of ssh -G is made as the
	// transport's are (group.go), and the package scan sees one use of it.
	signalGroup func(pgid int, sig syscall.Signal) error
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
	if o.ConfigFile != "" {
		// A stable name is a regular file of its own: controlPaths' derived
		// names, and the bound on the control socket's path they check, are
		// not used.
		if err := checkConfigFile(o.ControlDir, o.ConfigFile); err != nil {
			return nil, err
		}
		r := newRelay(o, o.ConfigFile)
		if err := writeConfigFile(o.ControlDir, r.config, o.Config); err != nil {
			return nil, err
		}
		return r, nil
	}
	_, conf, err := controlPaths(Options{Name: o.Name + "-relay", Config: o.Config, ControlDir: o.ControlDir})
	if err != nil {
		return nil, err
	}
	r := newRelay(o, conf)
	if err := writeConfigFile(o.ControlDir, r.config, o.Config); err != nil {
		return nil, err
	}
	return r, nil
}

// newRelay is a Relay with its group signal in place (see Relay.signalGroup).
func newRelay(o RelayOptions, config string) *Relay {
	return &Relay{o: o, config: config, signalGroup: killGroup}
}

// configFileName is a stable configuration file's name (RelayOptions.ConfigFile):
// letters, digits, '.', '_' and '-', not starting with '.', as the temporary
// files of writeConfigFile do, and ending ".conf".
var configFileName = regexp.MustCompile(`^[A-Za-z0-9_-][A-Za-z0-9._-]*\.conf$`)

// checkConfigFile refuses a stable configuration file's path that is not a
// clean, absolute name directly in the control directory dir, so the file is
// as private as the directory, whose mode NewRelay checks. Errors name no
// path.
func checkConfigFile(dir, file string) error {
	if !filepath.IsAbs(file) || filepath.Clean(file) != file || filepath.Dir(file) != filepath.Clean(dir) || !configFileName.MatchString(filepath.Base(file)) {
		return errors.New("sshrun: the relays' configuration file must be a clean name ending .conf directly in the control directory")
	}
	return nil
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
// node are one host. When ssh -G fails, it is the configured HostName, with a
// nil error, and that answer is kept for the Relay's life.
//
// ssh -G runs for at most the bound (RelayOptions.LoginTimeout, default
// DefaultLoginTimeout) and is then ended with everything it started, a Match
// exec command included (issue 403). That is the one failure Login reports: an
// error wrapping ErrLoginTimeout, naming the bound and, when ssh's standard
// error shows one, the last Match exec command it started (which is the
// operator's own configuration text, so it is for logs, as Stderr's is), with
// the configured HostName as the host. A timeout is never kept, and never
// turns into a quiet fallback later: the next Login runs ssh -G again, bounded
// again.
//
// Login holds the Relay's lock while ssh -G runs, so a Command or a Close
// waits up to the bound behind a stalled one.
func (r *Relay) Login() (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.login != "" {
		return r.login, nil
	}
	host, err := r.resolveLogin()
	if err != nil {
		return r.o.Config.HostName, err
	}
	r.login = host
	return host, nil
}

// loginStderrKept is how much of ssh -G's standard error is kept, for the
// Match exec command a timeout names.
const loginStderrKept = 16 << 10

// resolveLogin runs ssh -G as the leader of a process group of its own (so
// the Match exec commands it starts are in it), under the bound. It returns
// the host ssh names, the configured HostName when ssh could not answer, and
// an error only for a timeout. The caller holds r.mu.
//
// -v is after -G: the doubles in the tests read the first argument, and
// without -v ssh writes no word about the commands it runs; with it, stdout is
// the same and standard error says "Executing command: '...'" before each.
func (r *Relay) resolveLogin() (string, error) {
	fallback := r.o.Config.HostName
	bound := r.o.LoginTimeout
	if bound <= 0 {
		bound = DefaultLoginTimeout
	}
	log := r.o.Logger
	if log == nil {
		log = slog.Default()
	}
	ctx, cancel := context.WithTimeout(context.Background(), bound)
	defer cancel()
	var stdout bytes.Buffer
	stderr := &limited{w: &bytes.Buffer{}, n: loginStderrKept}
	cmd := exec.CommandContext(ctx, r.o.Binary, "-G", "-v", "-F", r.config, alias)
	cmd.Env = r.o.Env
	cmd.Stdout, cmd.Stderr = &stdout, stderr
	leader := newGroupLeader(log, r.signalGroup)
	cmd.Cancel = func() error { return leader.signal(syscall.SIGKILL) }
	cmd.WaitDelay = time.Second
	if err := leader.start(cmd); err != nil { // its own group, pinned (group.go)
		return fallback, nil
	}
	runErr := leader.reap(cmd) // behind the barrier; releases the pidfd
	if runErr != nil && ctx.Err() != nil {
		return "", loginTimeoutError(bound, stderr.w.String())
	}
	if runErr != nil {
		return fallback, nil
	}
	for _, line := range strings.Split(stdout.String(), "\n") {
		if h, ok := strings.CutPrefix(line, "hostname "); ok && strings.TrimSpace(h) != "" {
			return strings.TrimSpace(h), nil
		}
	}
	return fallback, nil
}

// execLinePrefix starts ssh's verbose line for a Match exec command.
const execLinePrefix = "Executing command: '"

// loginTimeoutError is the timeout's error: the bound, and the last Match exec
// command ssh's standard error shows, quoted and cut to 200 bytes, never any
// other line of it (they name the configuration file and the host).
func loginTimeoutError(bound time.Duration, stderr string) error {
	last := ""
	for _, line := range strings.Split(stderr, "\n") {
		if i := strings.Index(line, execLinePrefix); i >= 0 {
			last = strings.TrimSuffix(strings.TrimSpace(line[i+len(execLinePrefix):]), "'")
		}
	}
	if last == "" {
		return fmt.Errorf("%w after %s", ErrLoginTimeout, bound)
	}
	if len(last) > 200 {
		last = last[:200]
	}
	return fmt.Errorf("%w after %s (the last Match exec command it started: %q)", ErrLoginTimeout, bound, last)
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
