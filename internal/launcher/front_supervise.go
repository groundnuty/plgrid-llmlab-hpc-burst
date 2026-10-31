package launcher

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

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
	cfg, err := regularFile(config, "front configuration")
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
	bin, err = regularFile(bin, "front binary")
	if err != nil {
		return nil, err
	}
	if fi, _ := os.Stat(bin); fi.Mode().Perm()&0o111 == 0 {
		return nil, fmt.Errorf("front binary %s is not executable", bin)
	}
	return &FrontRunConfig{Binary: bin, Config: cfg}, nil
}

func regularFile(path, what string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	fi, err := os.Stat(abs)
	if err != nil {
		return "", fmt.Errorf("%s: %w", what, err)
	}
	if !fi.Mode().IsRegular() {
		return "", fmt.Errorf("%s %s is not a regular file", what, abs)
	}
	return abs, nil
}

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
	return &supervised{Name: "front", Path: s.FrontRun.Binary, Args: args, Health: health}, nil
}
