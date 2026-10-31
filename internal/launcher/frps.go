package launcher

// The frp server of HBEP-6 ("frps on the control-plane host"): its configuration, the web
// server's password, and the paths of both. The process itself is started and stopped in
// frps_proc.go; the binary is pinned in frps_pins.go.

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Heartbeat settings of frps: S1's set (HBEP-6, Heartbeats). frpc's side is the agent's.
const (
	frpsTCPMuxKeepaliveInterval = 10
	frpsHeartbeatTimeout        = 25
	frpsTCPKeepalive            = 30
)

// frpsPluginOps are the operations frps posts to the plugin. Ping is left out on purpose
// (`@hpc-burst-science-agent`, 31 Oct 2026, HBEP-6): a failed Ping call makes frpc close its
// session, so subscribing would drop every tunnel whenever the plugin is down.
var frpsPluginOps = []string{"Login", "NewProxy", "CloseProxy"}

// FrpsDir is the launcher's frps directory, <state>/frps, which holds the web password file
// (agentapi.FrpsDirName) and the configuration.
func FrpsDir(h agentapi.Host) string { return filepath.Join(h.StateDir, agentapi.FrpsDirName) }

// FrpsConfigFile is <state>/frps/frps.toml, mode 600: it holds the web server's password.
func FrpsConfigFile(h agentapi.Host) string { return filepath.Join(FrpsDir(h), "frps.toml") }

// FrpsLogFile is <state>/logs/frps.log, next to k3s's log. frps writes it through log.to, and its
// standard output and error go to the same file.
func FrpsLogFile(h agentapi.Host) string {
	return filepath.Join(layout{dir: h.StateDir}.logDir(), "frps.log")
}

// checkTOMLPath refuses a path that cannot be written between double quotes as it is: the
// renderer does no escaping, so nothing it writes can end the string early or start a new line.
func checkTOMLPath(what, p string) error {
	if !filepath.IsAbs(p) {
		return fmt.Errorf("%s %q is not an absolute path", what, p)
	}
	if !utf8.ValidString(p) || strings.ContainsAny(p, `"\`) || strings.IndexFunc(p, unicode.IsControl) >= 0 {
		return fmt.Errorf("%s %q holds a double quote, a backslash, a control character or invalid UTF-8", what, p)
	}
	return nil
}

// RenderFrpsConfig returns frps's configuration file for the host (HBEP-6). Everything the HBEP
// fixes is written out, including the settings that are only frp's defaults (the listeners that
// stay off, token authentication with no token), so that a change of frp's defaults changes
// nothing here. webPassword is the key from the web password file. The rendering refuses a
// password that is not a key's form, a port block that does not fit below 65536, and a state
// directory that cannot sit inside a quoted TOML string.
func RenderFrpsConfig(h agentapi.Host, webPassword string) (string, error) {
	if err := agentapi.CheckKey([]byte(webPassword)); err != nil {
		return "", fmt.Errorf("frps web password: %w", err)
	}
	if err := checkTOMLPath("state directory", h.StateDir); err != nil {
		return "", err
	}
	for _, p := range []int{h.FrpsPluginPort(), h.FrpsBindPort(), h.FrpsTCPMuxPort(), h.FrpsWebPort()} {
		if p < 1 || p > 65535 {
			return "", fmt.Errorf("the port block at %d puts an frps port at %d, outside 1-65535", h.PortBase, p)
		}
	}
	ops := make([]string, len(frpsPluginOps))
	for i, op := range frpsPluginOps {
		ops[i] = fmt.Sprintf("%q", op)
	}
	var b strings.Builder
	w := func(format string, a ...any) { fmt.Fprintf(&b, format+"\n", a...) }
	w("# Written by hpcburst at every start (HBEP-6). It holds the web password: mode 600, never edited by hand.")
	w("# Loopback only: bindAddr defaults to every interface, and proxyBindAddr follows it.")
	w(`bindAddr = "127.0.0.1"`)
	w("bindPort = %d", h.FrpsBindPort())
	w(`proxyBindAddr = "127.0.0.1"`)
	w("tcpmuxHTTPConnectPort = %d", h.FrpsTCPMuxPort())
	w("# Every other listener stays off. Port 0 is frp's default; it is written so a new default cannot turn one on.")
	w("kcpBindPort = 0")
	w("quicBindPort = 0")
	w("vhostHTTPPort = 0")
	w("vhostHTTPSPort = 0")
	w("sshTunnelGateway.bindPort = 0")
	w("# Token authentication with no token: frps's own check passes every login, and the plugin is the gate.")
	w(`auth.method = "token"`)
	w(`webServer.addr = "127.0.0.1"`)
	w("webServer.port = %d", h.FrpsWebPort())
	w("webServer.user = %q", agentapi.FrpsWebUser)
	w("webServer.password = %q", webPassword)
	w("log.to = %q", FrpsLogFile(h))
	w("transport.tcpMuxKeepaliveInterval = %d", frpsTCPMuxKeepaliveInterval)
	w("transport.heartbeatTimeout = %d", frpsHeartbeatTimeout)
	w("transport.tcpKeepalive = %d", frpsTCPKeepalive)
	w("")
	w("[[httpPlugins]]")
	w(`name = "hpcburst"`)
	w("addr = %q", h.FrpsPluginAddr())
	w("path = %q", agentapi.FrpsPluginPath)
	w("ops = [%s]", strings.Join(ops, ", "))
	return b.String(), nil
}

// EnsureFrpsWebPassword returns the web server's password from agentapi.Host.FrpsWebPasswordFile,
// and creates the file with a new key (agentapi.NewKey, WriteKeyFile: mode 600, in a 700
// directory) when there is none. An existing file is kept as it is. One that ReadKeyFile
// refuses (a wrong mode or owner, a symbolic link, a wrong form) is an error, and nothing is
// written over it: the launcher does not guess whether a password someone else touched is
// still secret. The state directory's lock serialises the launcher's commands, so two of them
// never create the file at once.
func EnsureFrpsWebPassword(h agentapi.Host) (key string, created bool, err error) {
	path := h.FrpsWebPasswordFile()
	key, err = agentapi.ReadKeyFile(path)
	switch {
	case err == nil:
		return key, false, nil
	case !errors.Is(err, fs.ErrNotExist):
		return "", false, fmt.Errorf("frps web password file: %w", err)
	}
	if key, err = agentapi.NewKey(); err != nil {
		return "", false, err
	}
	if err = agentapi.WriteKeyFile(path, key); err != nil {
		return "", false, fmt.Errorf("frps web password file: %w", err)
	}
	return key, true, nil
}

// WriteFrpsConfig renders the configuration for the host and writes it atomically, mode 600, at
// FrpsConfigFile. It creates the directories above it and the log's directory (mode 700), and
// returns the file's path. It does not create the password: pass EnsureFrpsWebPassword's key.
func WriteFrpsConfig(h agentapi.Host, webPassword string) (string, error) {
	text, err := RenderFrpsConfig(h, webPassword)
	if err != nil {
		return "", err
	}
	for _, d := range []string{FrpsDir(h), filepath.Dir(FrpsLogFile(h))} {
		if err := os.MkdirAll(d, agentapi.KeyDirMode); err != nil {
			return "", err
		}
	}
	path := FrpsConfigFile(h)
	if err := writeFileAtomic(path, []byte(text), 0o600); err != nil {
		return "", fmt.Errorf("write %s: %w", path, err)
	}
	return path, nil
}
