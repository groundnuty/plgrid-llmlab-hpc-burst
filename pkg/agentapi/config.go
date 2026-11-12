package agentapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// DialHomeMode is how the agent reaches the control-plane host (HBEP-7,
// Registration; HBEP-11, open question 8). In both modes frpc, the serving
// path's client, runs in the job (HBEP-6, Dial-home modes), so AgentConfig
// carries frpc's digest in both.
type DialHomeMode string

// Dial-home modes.
const (
	// DialHomeDirect: the agent connects to the front's dial-home listener,
	// which passes PathPrefix through to the endpoint.
	DialHomeDirect DialHomeMode = "direct"
	// DialHomeSSH: the agent keeps two forwards of its own, each ssh -N -L in
	// its own reconnect loop, with the restricted key and the pinned host key
	// from DialHomeDir: connection 1 to the agent endpoint's loopback port for
	// its control traffic, and connection 2 to the dial-home listener's
	// loopback port for frpc (HBEP-6, Dial-home modes). The endpoint is the
	// control-plane host's ssh server, on port 22 unless the owner routes the
	// cluster through a port forward on the gateway at another port
	// (`@hpc-burst-science-agent`, 31 Oct 2026).
	DialHomeSSH DialHomeMode = "ssh"
	// DialHomeHostDialed: the control-plane host dials the job, through the
	// login node, for a host that compute nodes cannot reach (HBEP-6,
	// Dial-home modes, host-dialed mode; science-agent decision, 9 Nov 2026).
	// The agent binds two loopback listeners, control and serving, and runs
	// each channel's multiplexed session over the relay that the batch
	// Provider's controller starts in an srun step (RelayLine): the control
	// client dials the control listener, and frpc's serverPort is the serving
	// listener's port. agent.json names no endpoint and no ssh settings in
	// this mode: nothing in the job dials the control-plane host's address.
	DialHomeHostDialed DialHomeMode = "host-dialed"
)

// DialHomeModes are the dial-home modes, in the order the HBEP names them.
var DialHomeModes = []DialHomeMode{DialHomeDirect, DialHomeSSH, DialHomeHostDialed}

// ValidDialHomeMode reports whether m is one of DialHomeModes.
func ValidDialHomeMode(m DialHomeMode) bool { return slices.Contains(DialHomeModes, m) }

// AgentConfig is agent.json, which the provider writes into the Allocation's
// directory, mode 600, and the agent reads at start (HBEP-4, Submission). It
// names a private address and user, so it is never in the repository. It has
// no maps, so Encode gives the same bytes for the same values, and the
// provider's EnsureFile skips an unchanged file.
type AgentConfig struct {
	// ProtocolVersion is the agent protocol's version, ProtocolVersion.
	ProtocolVersion string `json:"protocolVersion"`

	// Tag is the Allocation's job tag. The agent sends it as HeaderTag.
	Tag string `json:"tag"`

	// Mode is the dial-home mode.
	Mode DialHomeMode `json:"mode"`

	// Endpoint is host:port: in direct mode the front's dial-home listener;
	// in ssh mode the control-plane host's ssh server, which listens on port
	// 22 by default and may be reached on any other valid port when the owner
	// routes a cluster through a port forward on the gateway
	// (`@hpc-burst-science-agent`, 31 Oct 2026). Validate takes any port in
	// both modes; the default 22 is applied where an address is recorded.
	Endpoint string `json:"endpoint"`

	// SSH is set exactly in ssh mode.
	SSH *SSHForward `json:"ssh,omitempty"`

	// Allocation is the Allocation's UID, set exactly in host-dialed mode:
	// the agent's greeting on its relay socket names it, and a relay checks
	// it against its --allocation (HBEP-6, The rendezvous).
	Allocation string `json:"allocation,omitempty"`

	// CertSPKISHA256 is the pin of the agent endpoint's certificate (PinOf).
	CertSPKISHA256 string `json:"certSPKISHA256"`

	// FrpcDigest is the SHA-256 of the staged frpc binary, 64 lowercase hex
	// characters, which the agent checks before it runs frpc. frpc is staged
	// at StagedFrpcPath(FrpcDigest) under StagingDir (HBEP-4, Submission:
	// Files first). It is required in every dial-home mode, with no default:
	// frpc runs in the job in both (HBEP-6, Dial-home modes; HBEP-7, the
	// allocation agent), and an agent.json without it would let a job skip
	// the check (`@hpc-burst-science-agent`, 31 Oct 2026).
	FrpcDigest string `json:"frpcDigest"`

	// StagingDir is the resolved absolute staging directory (HBEP-4,
	// Transport): never a variable or "~".
	StagingDir string `json:"stagingDir"`

	// DialHomeDir is the resolved absolute dial-home directory,
	// <home>/DialHomeDir.
	DialHomeDir string `json:"dialHomeDir"`

	// HubEndpoint is the origin of the Hugging Face Hub the agent stages the
	// checkpoint's files from: the controller's HF_ENDPOINT, which the
	// Provider controller writes here because a job, reached over ssh, does
	// not inherit the control-plane host's environment (ruling of 1 Nov 2026
	// on #156). Empty, the agent uses its own HF_ENDPOINT, then the public
	// Hub. Set, it must pass CheckHubEndpoint.
	HubEndpoint string `json:"hubEndpoint,omitempty"`

	// Bulk is the bulk path's settings (beta; HBEP-6, The bulk path): set,
	// the agent starts a second frpc for the Instance, the bulk frpc, with
	// tcpMux off, to the second frps. The Provider's controller writes it only
	// while the launcher runs the bulk path, and never in ssh mode, so an
	// Allocation staged without it runs one frpc, as before. Its certificate is
	// StagedBulkDialHomeCertPath, pinned by CertSPKISHA256, since it is
	// issued for the same key (`@hpc-burst-science-agent`, 12 Nov 2026, #448).
	Bulk *BulkPath `json:"bulk,omitempty"`
}

// BulkPath is agent.json's bulk block.
type BulkPath struct {
	// ServerName is the bulk frpc's transport.tls.serverName, which the
	// dial-home listener routes the bulk frpc's websocket by. Validate
	// requires exactly BulkDialHomeServerName.
	ServerName string `json:"serverName"`
}

// SSHForward is what the agent's ssh forward needs besides the endpoint and the
// files in DialHomeDir.
type SSHForward struct {
	// User is the user on the control-plane host whose authorized_keys holds
	// the restricted key's line.
	User string `json:"user"`
	// AgentPort is the agent endpoint's loopback port on the control-plane
	// host, the target of connection 1's forward.
	AgentPort int `json:"agentPort"`
	// DialHomePort is the dial-home listener's loopback port on the
	// control-plane host, the target of connection 2's forward, which carries
	// frpc's websocket (HBEP-6, Dial-home modes). The launcher fills it with
	// Host.DialHomePort(). It is an explicit value, never derived from the
	// layout (HBEP-4, Submission: agent.json carries the port), and must
	// differ from AgentPort, because the restricted key permits forwards to
	// exactly the two ports.
	DialHomePort int `json:"dialHomePort"`
}

// FrpcPath is the absolute path of the staged frpc: <StagingDir>/frpc/
// <FrpcDigest>/frpc (StagedFrpcPath under StagingDir). It is only as good as
// Validate has made the two fields.
func (c AgentConfig) FrpcPath() string {
	return filepath.Join(c.StagingDir, StagedFrpcPath(c.FrpcDigest))
}

// Encode returns agent.json's bytes: the config as indented JSON with a final
// newline. The same config always gives the same bytes.
func (c AgentConfig) Encode() ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// DecodeAgentConfig reads agent.json and validates it. Unknown fields are
// ignored, as everywhere in the protocol.
func DecodeAgentConfig(b []byte) (AgentConfig, error) {
	var c AgentConfig
	if err := json.NewDecoder(bytes.NewReader(b)).Decode(&c); err != nil {
		return c, fmt.Errorf("agent.json: %w", err)
	}
	return c, c.Validate()
}

var (
	// safePath is HBEP-4's safe pattern for values placed in commands, for an
	// absolute path.
	safePath = regexp.MustCompile(`^/[A-Za-z0-9._:=,/-]*$`)
	// tagForm is v1alpha1.JobTag's output: hb-<n>-<12 hex>, where <n> is the
	// Allocation's name cut to 40 characters. The cut can leave <n> ending in
	// "-" or "." (a 39-character Provider name followed by its "-<5 random>"
	// suffix ends at the hyphen), so <n> is required to start with a letter or
	// digit and nothing more.
	tagForm = regexp.MustCompile(`^hb-[a-z0-9][-a-z0-9.]{0,39}-[0-9a-f]{12}$`)
	// pinForm is 64 lowercase hex characters: a certificate pin, and the
	// SHA-256 digest of a staged binary (FrpcDigest).
	pinForm = regexp.MustCompile(`^[0-9a-f]{64}$`)
	// userForm is ValidUserName's form.
	userForm = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.-]{0,31}$`)
)

// ValidUserName reports whether name is a portable user name, as POSIX allows
// them: letters of either case, digits, '.', '_' and '-', with no leading '-',
// and here also no leading digit and at most 32 characters, the limit of
// Linux's useradd. Every such name is safe in an ssh argument and a shell
// word. It is the one rule for every user name the framework puts into ssh:
// agent.json's dial-home user, who runs the controller (user.Current) and may
// have capitals, as on the development VM (decision of
// `@hpc-burst-code-agent`, confirmed by `@hpc-burst-science-agent`, 1 Nov
// 2026; review of PR 132: B2), and a batch Provider's cluster login in its
// credentials file (internal/slurm, ParseSSHConfig; the same ruling extended
// to it for issue #146, 1 Nov 2026).
func ValidUserName(name string) bool { return userForm.MatchString(name) }

// Validate returns every problem with c, joined.
func (c AgentConfig) Validate() error {
	var errs []error
	bad := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }
	if c.ProtocolVersion != ProtocolVersion {
		bad("protocolVersion %q: want %q", c.ProtocolVersion, ProtocolVersion)
	}
	if len(c.Tag) > 56 || !tagForm.MatchString(c.Tag) {
		bad("tag %q is not a job tag, hb-<name>-<12 hex>, at most 56 characters", c.Tag)
	}
	if c.Mode == DialHomeHostDialed {
		// Nothing in the job dials the control-plane host in this mode: the
		// relays reach the job (HBEP-6, Dial-home modes, host-dialed mode).
		if c.Endpoint != "" {
			bad("endpoint %q is set in host-dialed mode: the agent dials only its own loopback listeners, so agent.json names no endpoint", c.Endpoint)
		}
	} else {
		host, port, err := net.SplitHostPort(c.Endpoint)
		if err != nil || host == "" {
			bad("endpoint %q: want host:port", c.Endpoint)
		} else if p, err := strconv.Atoi(port); err != nil || p < 1 || p > 65535 {
			bad("endpoint %q: %q is not a port", c.Endpoint, port)
		}
	}
	switch c.Mode {
	case DialHomeDirect:
		if c.SSH != nil {
			bad("ssh is set in direct mode")
		}
	case DialHomeSSH:
		if c.SSH == nil {
			bad("ssh mode needs ssh.user, ssh.agentPort and ssh.dialHomePort")
		} else {
			if !ValidUserName(c.SSH.User) {
				bad("ssh.user %q is not a user name", c.SSH.User)
			}
			if c.SSH.AgentPort < 1 || c.SSH.AgentPort > 65535 {
				bad("ssh.agentPort %d is not a port", c.SSH.AgentPort)
			}
			switch {
			case c.SSH.DialHomePort < 1 || c.SSH.DialHomePort > 65535:
				bad("ssh.dialHomePort %d: want the dial-home listener's loopback port, 1 to 65535; ssh mode requires it (HBEP-4, Submission)", c.SSH.DialHomePort)
			case c.SSH.DialHomePort == c.SSH.AgentPort:
				bad("ssh.dialHomePort %d is also ssh.agentPort: connection 2 must forward to the dial-home listener, not the agent endpoint", c.SSH.DialHomePort)
			}
		}
	case DialHomeHostDialed:
		if c.SSH != nil {
			bad("ssh is set in host-dialed mode")
		}
		if !uidForm.MatchString(c.Allocation) {
			bad("allocation %q: host-dialed mode needs the Allocation's UID, which the agent's greeting names", c.Allocation)
		}
	default:
		bad("mode %q: want %q, %q or %q", c.Mode, DialHomeDirect, DialHomeSSH, DialHomeHostDialed)
	}
	if c.Mode != DialHomeHostDialed && c.Allocation != "" {
		bad("allocation is set outside host-dialed mode")
	}
	if !pinForm.MatchString(c.CertSPKISHA256) {
		bad("certSPKISHA256: want 64 lowercase hex characters")
	}
	// frpc runs in every mode (HBEP-6, Dial-home modes), so its digest is
	// always required.
	if !pinForm.MatchString(c.FrpcDigest) {
		bad("frpcDigest %q: want the SHA-256 of the staged frpc, 64 lowercase hex characters; it is required in every dial-home mode", c.FrpcDigest)
	}
	for name, p := range map[string]string{"stagingDir": c.StagingDir, "dialHomeDir": c.DialHomeDir} {
		if !safePath.MatchString(p) || strings.Contains(p, "/../") || strings.HasSuffix(p, "/..") {
			bad("%s %q: want an absolute path of letters, digits and ._:=,/- with no ..", name, p)
		}
	}
	if c.HubEndpoint != "" {
		if err := CheckHubEndpoint(c.HubEndpoint); err != nil {
			bad("hubEndpoint: %v", err)
		}
	}
	if c.Bulk != nil {
		if c.Mode == DialHomeSSH {
			bad("bulk is set in ssh mode: the bulk path is built for direct and host-dialed mode only (HBEP-6, The bulk path)")
		}
		if c.Bulk.ServerName != BulkDialHomeServerName {
			bad("bulk.serverName %q: want exactly %q", c.Bulk.ServerName, BulkDialHomeServerName)
		}
	}
	return errors.Join(errs...)
}

// CheckHubEndpoint refuses a Hub origin the agent and the controller would not
// use: it must be a scheme and a host (and port) only, https, or http for a
// loopback host (a local Hub double), as pkg/hubfiles requires.
func CheckHubEndpoint(origin string) error {
	u, err := url.Parse(origin)
	switch {
	case err != nil:
		return fmt.Errorf("%q is not a URL: %v", origin, err)
	case u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.Trim(u.Path, "/") != "":
		return fmt.Errorf("%q must be a scheme and a host only", origin)
	case u.Scheme == "https":
		return nil
	case u.Scheme == "http" && loopbackHost(u.Hostname()):
		return nil
	}
	return fmt.Errorf("%q must be https (http only for a loopback host)", origin)
}

func loopbackHost(h string) bool {
	if h == "localhost" {
		return true
	}
	ip := net.ParseIP(h)
	return ip != nil && ip.IsLoopback()
}
