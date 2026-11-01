package launcher

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// FrontStopBudget is the front's longest normal stop (HBEP-5, stop order), the sum of its parts
// in internal/front:
//   - the requests in flight run for its shutdown grace, front.DefaultShutdownGrace: 10 s;
//   - the rest are cut, and their handlers are waited for up to that long again: 10 s;
//   - a cut stream may spend its write grace (internal/front's defaultWriteGrace) writing its
//     last event: 5 s;
//   - then the usage sink is flushed and closed: 1 s.
//
// A SIGKILL before the sink is closed loses the cut requests' front_shutdown records and leaves a
// counter gap. A front configured with a longer shutdown grace needs more.
const FrontStopBudget = 2*frontShutdownGrace + frontWriteGrace + frontSinkClose

// The parts of FrontStopBudget. TestFrontStopBudget checks frontShutdownGrace against
// front.DefaultShutdownGrace.
const (
	frontShutdownGrace = 10 * time.Second
	frontWriteGrace    = 5 * time.Second
	frontSinkClose     = time.Second
)

// FrontTermGrace is how long the supervisor lets the front stop, from SIGTERM to SIGKILL:
// FrontStopBudget and a margin of 4 s.
const FrontTermGrace = FrontStopBudget + 4*time.Second

// SupervisorGrace is the least grace the supervisor needs to stop: the longest kill grace it gives
// a child (the controller's ControllerTermGrace; the front's FrontTermGrace when it runs one), and
// 5 s for the supervisor to see the exits, write its record and exit. down kills the supervisor
// sooner only when given a shorter --grace, and its children die with it (Pdeathsig).
const SupervisorGrace = max(ControllerTermGrace, FrontTermGrace) + 5*time.Second

// SupervisorGraceWithFront is the least grace down and upgrade accept for a host whose supervisor
// runs a front: SupervisorGrace. A shorter --grace is refused (ErrGraceTooShort), since a killed
// front loses usage records; DefaultGrace is longer. A host without a front accepts a shorter
// grace: a controller killed before its stop ends leaves its Leases held until they expire.
const SupervisorGraceWithFront = SupervisorGrace

// FrontBinName is the front's binary (issue #64, HBEP-5), looked for beside the running hpcburst
// unless --front-bin names another.
const FrontBinName = "hpcburst-front"

// FrontRunConfig is the front the supervisor runs: its binary and its configuration file (HBEP-5,
// Parameters: "the front's configuration file, which the launcher passes"). Both paths are
// absolute. The configuration names the client keys file; no key is on the front's command line.
type FrontRunConfig struct {
	Binary string `json:"binary"`
	Config string `json:"config"`
}

// resolveFrontRun checks the front's files before anything starts. No configuration means no
// front: it returns nil. A configuration with no binary found is an error, since the operator asked
// for a front.
func resolveFrontRun(config, explicitBin string) (*FrontRunConfig, error) {
	if config == "" {
		if explicitBin != "" {
			return nil, errors.New("--front-bin needs --front-config, the front's configuration file")
		}
		return nil, nil
	}
	cfg, _, err := regularFile(config, "front configuration")
	if err != nil {
		return nil, err
	}
	bin := explicitBin
	if bin == "" {
		self, err := os.Executable()
		if err != nil {
			return nil, fmt.Errorf("find %s beside hpcburst: %w", FrontBinName, err)
		}
		bin = filepath.Join(filepath.Dir(self), FrontBinName)
	}
	bin, fi, err := regularFile(bin, "front binary")
	if err != nil {
		return nil, err
	}
	if fi.Mode().Perm()&0o111 == 0 {
		return nil, fmt.Errorf("front binary %s is not executable", bin)
	}
	return &FrontRunConfig{Binary: bin, Config: cfg}, nil
}

// regularFile returns the absolute path of a regular file and the FileInfo it was judged by, so
// the caller checks the same file and never stats it a second time.
func regularFile(path, what string) (string, os.FileInfo, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", nil, err
	}
	fi, err := os.Stat(abs)
	if err != nil {
		return "", nil, fmt.Errorf("%s: %w", what, err)
	}
	if !fi.Mode().IsRegular() {
		return "", nil, fmt.Errorf("%s %s is not a regular file", what, abs)
	}
	return abs, fi, nil
}

// frontEntryName is the front's entry in the supervised set and in supervisor.json.
const frontEntryName = "front"

// frontEntry is the front's supervised entry, or nil when no front was configured. Its command
// line is the contract with cmd/hpcburst-front: it fills an agentapi.Host from --state-dir and
// --port-base (the Instance keys and the agent endpoint's certificate live there), reads the
// kubeconfig in the state directory, and takes the dial-home mode the launcher recorded.
func (s *State) frontEntry(l layout) (*supervised, error) {
	if s.FrontRun == nil {
		return nil, nil
	}
	if s.Ports.FrontHealth == 0 {
		return nil, errors.New("the launcher record has no front health port")
	}
	health := s.Ports.FrontHealthAddr()
	args := []string{"--config", s.FrontRun.Config, "--state-dir", l.dir, "--port-base", strconv.Itoa(s.Ports.Base()), "--health-addr", health}
	if s.Front.Mode != "" {
		args = append(args, "--mode", s.Front.Mode)
	}
	return &supervised{Name: frontEntryName, Path: s.FrontRun.Binary, Args: args, Health: health, TermGrace: FrontTermGrace, WaitReady: true}, nil
}
