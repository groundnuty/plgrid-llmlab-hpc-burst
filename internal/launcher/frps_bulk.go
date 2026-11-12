package launcher

// The bulk path's second frps (beta; HBEP-6, The bulk path): its configuration, web password and
// paths. It is the first frps's configuration with its own three loopback ports (base+13 to
// base+15), its own log and password, the plugins' bulk paths, and transport.tcpMux = false.

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// checkBulkPathFront refuses up --bulk-path without a front to serve it: no front mode, no front
// process, or ssh mode, where the agent starts no bulk frpc (HBEP-6, The bulk path; the science
// agent's ruling S2 on #448). Each is refused with its reason, as --relay-keeper is outside
// host-dialed mode, so the flag never quietly does nothing.
func checkBulkPathFront(front Front, frontRun *FrontRunConfig) error {
	switch {
	case !front.Set():
		return errors.New("--bulk-path needs a front (--front-mode direct or host-dialed): the bulk frpc reaches its frps through the front's dial-home listener")
	case front.Mode == string(agentapi.DialHomeSSH):
		return fmt.Errorf("--bulk-path is for --front-mode %s or %s only: in ssh mode the agent starts no bulk frpc (HBEP-6, The bulk path)", agentapi.DialHomeDirect, agentapi.DialHomeHostDialed)
	case frontRun == nil:
		return errors.New("--bulk-path needs the front to run (--front-config): the front's dial-home listener sends the bulk frpc to its frps by the TLS server name, and its bulk transport carries the requests")
	}
	return nil
}

// FrpsBulkDir is the second frps's directory, <state>/frps-bulk, which holds its web password file
// and its configuration.
func FrpsBulkDir(h agentapi.Host) string { return filepath.Join(h.StateDir, agentapi.FrpsBulkDirName) }

// FrpsBulkConfigFile is <state>/frps-bulk/frps.toml, mode 600: it holds the second frps's web
// password.
func FrpsBulkConfigFile(h agentapi.Host) string { return filepath.Join(FrpsBulkDir(h), "frps.toml") }

// FrpsBulkLogFile is <state>/logs/frps-bulk.log, beside the first frps's log.
func FrpsBulkLogFile(h agentapi.Host) string {
	return filepath.Join(layout{dir: h.StateDir}.logDir(), "frps-bulk.log")
}

// RenderFrpsBulkConfig returns the second frps's configuration for a host without a front: the
// first's (RenderFrpsConfig) with the bulk path's shape (bulkFrpsShape).
func RenderFrpsBulkConfig(h agentapi.Host, webPassword string) (string, error) {
	return renderFrps(h, bulkFrpsShape(h), webPassword, "")
}

// RenderFrpsBulkConfigWithFront is RenderFrpsBulkConfig for a host whose launcher runs a front: the
// front's plugin comes second, at frontAddr, on agentapi.FrontFrpsBulkPluginPath, so a bulk
// NewProxy or CloseProxy moves only the UID's bulk epoch (HBEP-6, The bulk path).
func RenderFrpsBulkConfigWithFront(h agentapi.Host, webPassword, frontAddr string) (string, error) {
	if err := checkFrontPluginAddr(frontAddr); err != nil {
		return "", err
	}
	return renderFrps(h, bulkFrpsShape(h), webPassword, frontAddr)
}

// WriteFrpsBulkConfig renders the second frps's configuration and writes it atomically, mode 600,
// at FrpsBulkConfigFile, and returns its path. It does not create the password: pass
// EnsureFrpsBulkWebPassword's key.
func WriteFrpsBulkConfig(h agentapi.Host, webPassword string) (string, error) {
	text, err := RenderFrpsBulkConfig(h, webPassword)
	if err != nil {
		return "", err
	}
	return writeFrpsConfigTextAt(FrpsBulkConfigFile(h), FrpsBulkLogFile(h), text)
}

// WriteFrpsBulkConfigWithFront is WriteFrpsBulkConfig with the front's plugin.
func WriteFrpsBulkConfigWithFront(h agentapi.Host, webPassword, frontAddr string) (string, error) {
	text, err := RenderFrpsBulkConfigWithFront(h, webPassword, frontAddr)
	if err != nil {
		return "", err
	}
	return writeFrpsConfigTextAt(FrpsBulkConfigFile(h), FrpsBulkLogFile(h), text)
}

// writeFrpsBulkFiles is writeFrpsFiles for the second frps: its own password, kept when it exists,
// and its configuration, with the front's plugin when frontAddr is set.
func writeFrpsBulkFiles(h agentapi.Host, frontAddr string) (string, error) {
	key, _, err := EnsureFrpsBulkWebPassword(h)
	if err != nil {
		return "", err
	}
	if frontAddr == "" {
		return WriteFrpsBulkConfig(h, key)
	}
	return WriteFrpsBulkConfigWithFront(h, key, frontAddr)
}
