package launcher

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

const stateSchema = 1

// maxStateDirLen keeps the longest socket path k3s creates inside the state directory below
// the 108-byte limit of a unix socket address (kine's socket is <data>/server/kine.sock).
const maxStateDirLen = 80

// layout names every path inside a state directory. The state directory is the launcher's
// whole footprint: k3s's data, its home, its logs and the record of what was started.
type layout struct{ dir string }

func (l layout) stateFile() string     { return filepath.Join(l.dir, "state.json") }
func (l layout) lockFile() string      { return filepath.Join(l.dir, "lock") }
func (l layout) home() string          { return filepath.Join(l.dir, "home") }
func (l layout) runDir() string        { return filepath.Join(l.dir, "run") }
func (l layout) dataDir() string       { return filepath.Join(l.dir, "data") }
func (l layout) logDir() string        { return filepath.Join(l.dir, "logs") }
func (l layout) logFile() string       { return filepath.Join(l.logDir(), "k3s.log") }
func (l layout) emptyConfig() string   { return filepath.Join(l.dir, "empty-config.yaml") }
func (l layout) emptyRegistry() string { return filepath.Join(l.dir, "empty-registries.yaml") }

// adminKubeconfig is where k3s writes its own admin kubeconfig, early in its start. It is the
// source only: the file users get is kubeconfig(), which exists only once the host is ready.
func (l layout) adminKubeconfig() string { return filepath.Join(l.dir, "k3s-admin.yaml") }
func (l layout) kubeconfig() string      { return filepath.Join(l.dir, "kubeconfig") }
func (l layout) backupsDir() string      { return filepath.Join(l.dir, "backups") }
func (l layout) sqliteDir() string       { return filepath.Join(l.dataDir(), "server", "db") }
func (l layout) storeDB() string         { return filepath.Join(l.sqliteDir(), "state.db") }
func (l layout) tokenFile() string       { return filepath.Join(l.dataDir(), "server", "token") }
func (l layout) tmpDir() string          { return filepath.Join(l.dir, "tmp") }
func (l layout) auditPolicy() string     { return filepath.Join(l.dir, "audit-policy.yaml") }
func (l layout) auditLog() string        { return filepath.Join(l.logDir(), "audit.log") }

// The controllers' files: the supervisor's own record and the logs of the supervisor and of each
// supervised process. The credentials and agent-builds directories are the shared host layout's.
func (l layout) controllerDir() string       { return filepath.Join(l.dir, "controller") }
func (l layout) supervisorFile() string      { return filepath.Join(l.controllerDir(), "supervisor.json") }
func (l layout) supervisorLog() string       { return filepath.Join(l.logDir(), "supervisor.log") }
func (l layout) childLog(name string) string { return filepath.Join(l.logDir(), name+".log") }

// host is the layout the launcher shares with the controller (agentapi.Host): the state directory
// and the port base of the block recorded in ports.
func (l layout) host(ports Ports) agentapi.Host {
	return agentapi.Host{StateDir: l.dir, PortBase: ports.Base()}
}

// storeVersionFile records which k3s release last wrote the store. down never removes it.
func (l layout) storeVersionFile() string { return filepath.Join(l.dir, "store-version.json") }

// newLayout makes the state directory path absolute and rejects one that is too long for k3s's sockets.
func newLayout(dir string) (layout, error) {
	if dir == "" {
		return layout{}, errors.New("no state directory given")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return layout{}, err
	}
	if len(abs) > maxStateDirLen {
		return layout{}, fmt.Errorf("state directory path %q is %d bytes long; k3s's unix sockets need it under %d, so choose a shorter --state-dir", abs, len(abs), maxStateDirLen)
	}
	return layout{abs}, nil
}

// State is the launcher's record of one running (or just stopped) host. It is written right
// after the process is started, before readiness, so a crashed or interrupted start can still
// be cleaned up by down.
type State struct {
	Schema       int       `json:"schema"`
	Version      string    `json:"version"`
	Binary       string    `json:"binary"`
	BinarySHA256 string    `json:"binary_sha256"`
	Nice         int       `json:"nice,omitempty"`
	Ports        Ports     `json:"ports"`
	Procs        []ProcRec `json:"processes"`
	StartedAt    time.Time `json:"started_at"`
	Front        Front     `json:"front"`
	// EventTTL is the event-ttl the launcher started k3s with (HBEP-8, Retention), written with the
	// first record of a start. status reports it. Empty in the record of a host that an earlier
	// release started: that k3s ran with the API server's own default of one hour.
	EventTTL string `json:"event_ttl,omitempty"`
	// Controllers is what the supervisor runs. up records it before it starts the supervisor,
	// which reads it back from here, and upgrade starts the supervisor again from it. Nil when no
	// controller binary was found: no controllers run.
	Controllers *ControllerConfig `json:"controllers,omitempty"`
	// FrontRun is the front the supervisor runs beside the controllers (issue #64). Nil when up was
	// given no --front-config.
	FrontRun *FrontRunConfig `json:"front_run,omitempty"`
	// Frps is the frps the supervisor runs (HBEP-6). Nil when no frps was found at up, or no
	// controllers run.
	Frps *FrpsRunConfig `json:"frps,omitempty"`
}

// ControllerConfig is the launcher's choice of what to run beside k3s.
type ControllerConfig struct {
	Binary string `json:"binary"` // absolute path of hpcburst-controller
}

// loadState reads the record. A missing file is (nil, nil): nothing was started.
func (l layout) loadState() (*State, error) {
	b, err := os.ReadFile(l.stateFile())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var s State
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("%s is not a valid launcher record (%w); stop whatever it names by hand, then remove it", l.stateFile(), err)
	}
	if s.Schema != stateSchema {
		return nil, fmt.Errorf("%s has schema %d, this launcher reads schema %d", l.stateFile(), s.Schema, stateSchema)
	}
	if len(s.Procs) == 0 {
		return nil, fmt.Errorf("%s records no process", l.stateFile())
	}
	for _, p := range s.Procs {
		// Without all three, no identity can be confirmed, and an unconfirmed process is never
		// signalled; a record that lacks one is damaged, not a reason to skip a check.
		if p.PID <= 1 || p.StartTicks == 0 || p.BootID == "" {
			return nil, fmt.Errorf("%s is damaged: process %q has pid %d, start time %d and boot ID %q; stop whatever it names by hand, then remove it", l.stateFile(), p.Name, p.PID, p.StartTicks, p.BootID)
		}
	}
	return &s, nil
}

// saveState writes the record atomically with mode 0600: a reader never sees half a file.
func (l layout) saveState(s *State) error {
	s.Schema = stateSchema
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(l.stateFile(), append(b, '\n'), 0o600)
}

func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	fail := func(err error) error { tmp.Close(); os.Remove(name); return err }
	if err := tmp.Chmod(mode); err != nil {
		return fail(err)
	}
	if _, err := tmp.Write(data); err != nil {
		return fail(err)
	}
	if err := tmp.Sync(); err != nil {
		return fail(err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(name)
		return err
	}
	if err := os.Rename(name, path); err != nil {
		os.Remove(name)
		return err
	}
	return nil
}

// lockState takes an exclusive lock on the state directory, so two commands cannot start or
// stop the same host at once. The lock goes with the process, so a crash never leaves it held.
func (l layout) lockState() (unlock func(), err error) {
	if err := os.MkdirAll(l.dir, 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(l.lockFile(), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("another hpcburst command is using the state directory %s", l.dir)
		}
		return nil, err
	}
	return func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); f.Close() }, nil
}

// DefaultStateDir is $XDG_STATE_HOME/hpcburst, or ~/.local/state/hpcburst.
func DefaultStateDir() (string, error) {
	if x := os.Getenv("XDG_STATE_HOME"); x != "" {
		return filepath.Join(x, "hpcburst"), nil
	}
	h, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("no state directory: set --state-dir (%w)", err)
	}
	return filepath.Join(h, ".local", "state", "hpcburst"), nil
}

// DefaultBinDir is where hack/fetch-k3s.sh puts the pinned binaries: $XDG_CACHE_HOME/hpcburst/k3s
// or ~/.cache/hpcburst/k3s.
func DefaultBinDir() (string, error) {
	if x := os.Getenv("XDG_CACHE_HOME"); x != "" {
		return filepath.Join(x, "hpcburst", "k3s"), nil
	}
	h, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("no binary directory: set --bin-dir (%w)", err)
	}
	return filepath.Join(h, ".cache", "hpcburst", "k3s"), nil
}
