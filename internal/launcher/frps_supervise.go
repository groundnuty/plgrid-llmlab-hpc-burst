package launcher

// frps under the supervisor (HBEP-6, frps on the control-plane host; HBEP-11, Single-user host):
// up verifies the pinned binary and writes frps's configuration, and the supervisor runs frps as
// one more entry and restarts it when it exits. Before every start it copies the binary into a
// sealed memory file again, checks the copy against the pin and executes that descriptor
// (/proc/self/fd/3). The kernel names such a process after the file it ran, "3" (argv[0] is the
// recorded binary's path, for ps), so nothing may find frps by its command name; the launcher finds
// it by its recorded identity (supervisor.json) and its ports.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// frpsEntryName is frps's entry in the supervised set and in supervisor.json.
const frpsEntryName = FrpsProcName

// FrpsRunConfig is the frps the supervisor runs: the pinned binary, checked against the pin by
// version at every start. The configuration is FrpsConfigFile of the host, which up writes.
type FrpsRunConfig struct {
	Binary  string `json:"binary"` // absolute
	Version string `json:"version"`
}

// resolveFrpsRun finds and verifies frps before anything starts. explicit is --frps-bin; empty
// means the default, FrpDirName(version)/frps under binDir. A missing default is not an error:
// it returns nil, and up warns that no frps runs. A given path that is missing or does not match
// the pin is an error.
func resolveFrpsRun(binDir, explicit, version string) (*FrpsRunConfig, error) {
	var bin FrpsBinary
	var err error
	if explicit == "" {
		def := filepath.Join(binDir, FrpDirName(version), "frps")
		if _, serr := os.Stat(def); errors.Is(serr, os.ErrNotExist) {
			return nil, nil
		}
		bin, err = ResolveFrps(binDir, version)
	} else {
		bin, err = ResolveFrpsFile(explicit, version)
	}
	if err != nil {
		return nil, fmt.Errorf("frps: %w", err)
	}
	defer bin.Close()
	return &FrpsRunConfig{Binary: bin.Path, Version: bin.Version}, nil
}

// noFrpsWarning is printed when the controllers run but no frps was found.
const noFrpsWarning = "warning: no frps found (--frps-bin, or frp-" + DefaultFrpVersion + "/frps in --bin-dir): Instances cannot open their dial-home tunnels (HBEP-6)"

// frpsSpecFor is the start of the recorded frps for the host, without its binary.
func frpsSpecFor(h agentapi.Host) FrpsSpec {
	return FrpsSpec{Config: FrpsConfigFile(h), Log: FrpsLogFile(h), Dir: h.StateDir}
}

// frpsEntry is frps's supervised entry, or nil when no frps was recorded. Its health is the
// three loopback ports of HBEP-6 (bind, tcpmux and the web server), exactly, and GET /healthz on
// the web server, which answers before the server's password check.
func (s *State) frpsEntry(l layout) *supervised {
	if s.Frps == nil {
		return nil
	}
	h := l.host(s.Ports)
	spec := frpsSpecFor(h)
	run := *s.Frps
	return &supervised{
		Name:   frpsEntryName,
		Path:   execPath(), // the verified binary, passed at descriptor 3 by Open
		Argv0:  run.Binary, // what ps shows; the file executed is still the descriptor
		Args:   spec.Argv()[1:],
		Health: h.FrpsWebAddr(),
		Ports:  []int{h.FrpsBindPort(), h.FrpsTCPMuxPort(), h.FrpsWebPort()},
		Env:    spec.Env(),
		// Copied, sealed and checked against the pin again before every start: a binary replaced or
		// rewritten since up fails its start, and the supervisor reports it, instead of running
		// unchecked bytes.
		Open: func() ([]*os.File, error) {
			bin, err := ResolveFrpsFile(run.Binary, run.Version)
			if err != nil {
				return nil, err
			}
			return []*os.File{bin.file}, nil
		},
		WaitReady: true,
	}
}

// writeFrpsFiles makes sure frps's web password exists (kept when it does, so the lifecycle
// controller's copy stays valid) and writes frps's configuration with it, both mode 600 (HBEP-6).
func writeFrpsFiles(h agentapi.Host) (string, error) {
	key, _, err := EnsureFrpsWebPassword(h)
	if err != nil {
		return "", err
	}
	return WriteFrpsConfig(h, key)
}
