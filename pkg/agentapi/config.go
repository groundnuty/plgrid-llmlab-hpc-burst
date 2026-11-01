package agentapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"regexp"
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
)

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

	// CertSPKISHA256 is the pin of the agent endpoint's certificate (PinOf).
	CertSPKISHA256 string `json:"certSPKISHA256"`

	// FrpcDigest is the SHA-256 of the staged frpc binary, 64 lowercase hex
	// characters, which the agent checks before it runs frpc. frpc is staged
	// at StagedFrpcPath(FrpcDigest) under StagingDir (HBEP-4, Submission:
	// Files first). It is required in both dial-home modes, with no default:
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
	// userForm is a portable user name, as POSIX allows them: letters of
	// either case, digits, '.', '_' and '-', with no leading '-', and here
	// also no leading digit and at most 32 characters, the limit of Linux's
	// useradd. The ssh user is the one who runs the controller
	// (user.Current), whose name may have capitals, as on the development
	// VM; every such name is still safe in an ssh argument and a shell word
	// (decision of `@hpc-burst-code-agent`, confirmed by
	// `@hpc-burst-science-agent`, 1 Nov 2026; review of PR 132: B2).
	userForm = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.-]{0,31}$`)
)

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
	host, port, err := net.SplitHostPort(c.Endpoint)
	if err != nil || host == "" {
		bad("endpoint %q: want host:port", c.Endpoint)
	} else if p, err := strconv.Atoi(port); err != nil || p < 1 || p > 65535 {
		bad("endpoint %q: %q is not a port", c.Endpoint, port)
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
			if !userForm.MatchString(c.SSH.User) {
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
	default:
		bad("mode %q: want %q or %q", c.Mode, DialHomeDirect, DialHomeSSH)
	}
	if !pinForm.MatchString(c.CertSPKISHA256) {
		bad("certSPKISHA256: want 64 lowercase hex characters")
	}
	// frpc runs in both modes (HBEP-6, Dial-home modes), so its digest is
	// always required.
	if !pinForm.MatchString(c.FrpcDigest) {
		bad("frpcDigest %q: want the SHA-256 of the staged frpc, 64 lowercase hex characters; it is required in both dial-home modes", c.FrpcDigest)
	}
	for name, p := range map[string]string{"stagingDir": c.StagingDir, "dialHomeDir": c.DialHomeDir} {
		if !safePath.MatchString(p) || strings.Contains(p, "/../") || strings.HasSuffix(p, "/..") {
			bad("%s %q: want an absolute path of letters, digits and ._:=,/- with no ..", name, p)
		}
	}
	return errors.Join(errs...)
}
