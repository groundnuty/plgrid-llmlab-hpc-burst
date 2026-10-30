package agentapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
)

// DialHomeMode is how the agent reaches the control-plane host (HBEP-7,
// Registration; HBEP-11, open question 8).
type DialHomeMode string

// Dial-home modes.
const (
	// DialHomeDirect: the agent connects to the front's dial-home listener,
	// which passes PathPrefix through to the endpoint.
	DialHomeDirect DialHomeMode = "direct"
	// DialHomeSSH: the agent keeps its own forward, ssh -N -L over port 22 in
	// a reconnect loop, with the restricted key and the pinned host key from
	// DialHomeDir, and connects to the endpoint through it.
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
	// in ssh mode the control-plane host's ssh server, port 22.
	Endpoint string `json:"endpoint"`

	// SSH is set exactly in ssh mode.
	SSH *SSHForward `json:"ssh,omitempty"`

	// CertSPKISHA256 is the pin of the agent endpoint's certificate (PinOf).
	CertSPKISHA256 string `json:"certSPKISHA256"`

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
	// host, the target of the forward.
	AgentPort int `json:"agentPort"`
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
	tagForm  = regexp.MustCompile(`^hb-[a-z0-9]([-a-z0-9.]*[a-z0-9])?-[0-9a-f]{12}$`)
	pinForm  = regexp.MustCompile(`^[0-9a-f]{64}$`)
	userForm = regexp.MustCompile(`^[a-z_][a-z0-9_.-]*$`)
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
		if port != "22" {
			bad("endpoint %q: the ssh dial-home mode uses port 22 (HBEP-11, open question 8)", c.Endpoint)
		}
		if c.SSH == nil {
			bad("ssh mode needs ssh.user and ssh.agentPort")
		} else {
			if !userForm.MatchString(c.SSH.User) {
				bad("ssh.user %q is not a user name", c.SSH.User)
			}
			if c.SSH.AgentPort < 1 || c.SSH.AgentPort > 65535 {
				bad("ssh.agentPort %d is not a port", c.SSH.AgentPort)
			}
		}
	default:
		bad("mode %q: want %q or %q", c.Mode, DialHomeDirect, DialHomeSSH)
	}
	if !pinForm.MatchString(c.CertSPKISHA256) {
		bad("certSPKISHA256: want 64 lowercase hex characters")
	}
	for name, p := range map[string]string{"stagingDir": c.StagingDir, "dialHomeDir": c.DialHomeDir} {
		if !safePath.MatchString(p) || strings.Contains(p, "/../") || strings.HasSuffix(p, "/..") {
			bad("%s %q: want an absolute path of letters, digits and ._:=,/- with no ..", name, p)
		}
	}
	return errors.Join(errs...)
}
