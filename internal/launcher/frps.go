package launcher

// The frp server of HBEP-6 ("frps on the control-plane host"): its configuration, the web
// server's password, and the paths of both. The process itself is started and stopped in
// frps_proc.go; the binary is pinned in frps_pins.go.

import (
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strconv"
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

// frontPluginOps are the operations frps posts to the front's plugin, the second (#371; HBEP-6,
// Admission): each admitted registration and each close of a proxy, so that the front stops
// routing to the Instance's UID before frps registers it again.
var frontPluginOps = []string{"NewProxy", "CloseProxy"}

// frontPluginName is the front's plugin entry's name in frps's configuration.
const frontPluginName = "hpcburst-front"

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
	return renderFrpsConfig(h, webPassword, "")
}

// RenderFrpsConfigWithFront is RenderFrpsConfig for a host whose launcher runs a front: frps gets
// a second server plugin, the front's, at frontAddr (the front's health address, base+4), for
// NewProxy and CloseProxy, after the lifecycle controller's (HBEP-6, Admission; #371). frps calls
// its plugins in that order and stops at the first that refuses or does not answer (frp v0.71.1,
// pkg/plugin/server/manager.go:70-98), so the front hears only proxies the controller admitted,
// and frps refuses new proxies while the front does not answer. frontAddr must be 127.0.0.1:<port>.
func RenderFrpsConfigWithFront(h agentapi.Host, webPassword, frontAddr string) (string, error) {
	if err := checkFrontPluginAddr(frontAddr); err != nil {
		return "", err
	}
	return renderFrpsConfig(h, webPassword, frontAddr)
}

// checkFrontPluginAddr refuses a front's plugin address that is not 127.0.0.1:<port>.
func checkFrontPluginAddr(frontAddr string) error {
	host, port, err := net.SplitHostPort(frontAddr)
	if p, perr := strconv.Atoi(port); err != nil || perr != nil || host != "127.0.0.1" || p < 1 || p > 65535 {
		return fmt.Errorf("the front's plugin address %q is not 127.0.0.1:<port>", frontAddr)
	}
	return nil
}

// frpsShape is what sets the bulk path's second frps apart from the first in its configuration
// (beta; HBEP-6, The bulk path): its three loopback ports, its log, the paths it posts to on the
// two plugins, and tcpMux off. Everything else is the first's, byte for byte.
type frpsShape struct {
	bind, mux, web        int
	log                   string
	pluginPath, frontPath string
	muxOff                bool
}

// tokenFrpsShape is the first frps's: base+9 to base+11, frps.log, the plugins' first paths, and
// frp's default tcpMux.
func tokenFrpsShape(h agentapi.Host) frpsShape {
	return frpsShape{bind: h.FrpsBindPort(), mux: h.FrpsTCPMuxPort(), web: h.FrpsWebPort(), log: FrpsLogFile(h),
		pluginPath: agentapi.FrpsPluginPath, frontPath: agentapi.FrontFrpsPluginPath}
}

// bulkFrpsShape is the bulk path's frps: base+13 to base+15, frps-bulk.log, the plugins' bulk
// paths, and transport.tcpMux = false (HBEP-6, The bulk path).
func bulkFrpsShape(h agentapi.Host) frpsShape {
	return frpsShape{bind: h.FrpsBulkBindPort(), mux: h.FrpsBulkTCPMuxPort(), web: h.FrpsBulkWebPort(), log: FrpsBulkLogFile(h),
		pluginPath: agentapi.FrpsBulkPluginPath, frontPath: agentapi.FrontFrpsBulkPluginPath, muxOff: true}
}

func renderFrpsConfig(h agentapi.Host, webPassword, frontAddr string) (string, error) {
	return renderFrps(h, tokenFrpsShape(h), webPassword, frontAddr)
}

func renderFrps(h agentapi.Host, s frpsShape, webPassword, frontAddr string) (string, error) {
	if err := agentapi.CheckKey([]byte(webPassword)); err != nil {
		return "", fmt.Errorf("frps web password: %w", err)
	}
	if err := checkTOMLPath("state directory", h.StateDir); err != nil {
		return "", err
	}
	for _, p := range []int{h.FrpsPluginPort(), s.bind, s.mux, s.web} {
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
	w("bindPort = %d", s.bind)
	w(`proxyBindAddr = "127.0.0.1"`)
	w("tcpmuxHTTPConnectPort = %d", s.mux)
	w("# Every other listener stays off. Port 0 is frp's default; it is written so a new default cannot turn one on.")
	w("kcpBindPort = 0")
	w("quicBindPort = 0")
	w("vhostHTTPPort = 0")
	w("vhostHTTPSPort = 0")
	w("sshTunnelGateway.bindPort = 0")
	w("# Token authentication with no token: frps's own check passes every login, and the plugin is the gate.")
	w(`auth.method = "token"`)
	w(`webServer.addr = "127.0.0.1"`)
	w("webServer.port = %d", s.web)
	w("webServer.user = %q", agentapi.FrpsWebUser)
	w("webServer.password = %q", webPassword)
	w("log.to = %q", s.log)
	if s.muxOff {
		w("# The bulk path's frps (beta): every connection on its own, with no multiplexed session, so a large")
		w("# reply holds no other connection back. S1's heartbeat set is written all the same, since with")
		w("# tcpMux off frp's defaults are 90 s (HBEP-6, The bulk path).")
		w("transport.tcpMux = false")
	}
	w("transport.tcpMuxKeepaliveInterval = %d", frpsTCPMuxKeepaliveInterval)
	w("transport.heartbeatTimeout = %d", frpsHeartbeatTimeout)
	w("transport.tcpKeepalive = %d", frpsTCPKeepalive)
	w("")
	w("[[httpPlugins]]")
	w(`name = "hpcburst"`)
	w("addr = %q", h.FrpsPluginAddr())
	w("path = %q", s.pluginPath)
	w("ops = [%s]", strings.Join(ops, ", "))
	if frontAddr != "" {
		w("")
		w("# The front's plugin, after the controller's: frps stops at the first refusal, so the front hears only admitted")
		w("# proxies, and registers a proxy only once the front has stopped routing to its UID (HBEP-6, Admission).")
		b.WriteString(frontPluginBlockAt(frontAddr, s.frontPath))
	}
	return b.String(), nil
}

// frontPluginBlock is the front's plugin entry exactly as RenderFrpsConfigWithFront writes it, one
// line per key; status looks for it in frps's file (checkFrpsNamesTheFront).
func frontPluginBlock(frontAddr string) string {
	return frontPluginBlockAt(frontAddr, agentapi.FrontFrpsPluginPath)
}

// frontPluginBlockAt is frontPluginBlock for the front's plugin at path: agentapi.FrontFrpsPluginPath
// for the first frps, agentapi.FrontFrpsBulkPluginPath for the bulk path's (HBEP-6, The bulk path).
func frontPluginBlockAt(frontAddr, path string) string {
	fops := make([]string, len(frontPluginOps))
	for i, op := range frontPluginOps {
		fops[i] = fmt.Sprintf("%q", op)
	}
	return fmt.Sprintf("[[httpPlugins]]\nname = %q\naddr = %q\npath = %q\nops = [%s]\n",
		frontPluginName, frontAddr, path, strings.Join(fops, ", "))
}

// EnsureFrpsWebPassword returns the web server's password from agentapi.Host.FrpsWebPasswordFile,
// and creates the file with a new key (agentapi.NewKey, WriteKeyFile: mode 600, in a 700
// directory) when there is none. An existing file is kept as it is. One that ReadKeyFile
// refuses (a wrong owner, a symbolic link, a wrong form), or whose mode is not exactly 0600
// (ReadKeyFile also takes 0400, 0500 and 0700), is an error, and nothing is
// written over it: the launcher does not guess whether a password someone else touched is
// still secret. The state directory's lock serialises the launcher's commands, so two of them
// never create the file at once.
func EnsureFrpsWebPassword(h agentapi.Host) (key string, created bool, err error) {
	return ensureWebPasswordFile(h.FrpsWebPasswordFile())
}

// EnsureFrpsBulkWebPassword is EnsureFrpsWebPassword for the bulk path's second frps, whose
// password is a file of its own, agentapi.Host.FrpsBulkWebPasswordFile (HBEP-6, The bulk path).
func EnsureFrpsBulkWebPassword(h agentapi.Host) (key string, created bool, err error) {
	return ensureWebPasswordFile(h.FrpsBulkWebPasswordFile())
}

// ensureWebPasswordFile is EnsureFrpsWebPassword for the password file at path.
func ensureWebPasswordFile(path string) (key string, created bool, err error) {
	key, err = agentapi.ReadKeyFile(path)
	switch {
	case err == nil:
		// ReadKeyFile accepts any owner-only mode (0400, 0500, 0700). The launcher wrote this
		// file as 0600 (HBEP-6: mode 600), so another mode means someone changed it, and the
		// launcher does not guess whether that was harmless: it refuses, as for a group bit.
		fi, serr := os.Lstat(path)
		if serr != nil {
			return "", false, fmt.Errorf("frps web password file: %w", serr)
		}
		if perm := fi.Mode().Perm(); perm != agentapi.KeyFileMode {
			return "", false, fmt.Errorf("frps web password file %s has mode %04o, and the launcher requires exactly %04o: check who changed it, then chmod 600 it or remove it", path, perm, agentapi.KeyFileMode)
		}
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
	return writeFrpsConfigText(h, text)
}

// WriteFrpsConfigWithFront is WriteFrpsConfig with the front's plugin (RenderFrpsConfigWithFront).
func WriteFrpsConfigWithFront(h agentapi.Host, webPassword, frontAddr string) (string, error) {
	text, err := RenderFrpsConfigWithFront(h, webPassword, frontAddr)
	if err != nil {
		return "", err
	}
	return writeFrpsConfigText(h, text)
}

func writeFrpsConfigText(h agentapi.Host, text string) (string, error) {
	return writeFrpsConfigTextAt(FrpsConfigFile(h), FrpsLogFile(h), text)
}

// writeFrpsConfigTextAt writes text at path, mode 600, after creating path's directory and log's
// (mode 700).
func writeFrpsConfigTextAt(path, log, text string) (string, error) {
	for _, d := range []string{filepath.Dir(path), filepath.Dir(log)} {
		if err := os.MkdirAll(d, agentapi.KeyDirMode); err != nil {
			return "", err
		}
	}
	if err := writeFileAtomic(path, []byte(text), 0o600); err != nil {
		return "", fmt.Errorf("write %s: %w", path, err)
	}
	return path, nil
}
