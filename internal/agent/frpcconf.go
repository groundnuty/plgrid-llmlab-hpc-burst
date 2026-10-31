package agent

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// FrpcConfig is frpc's configuration for one Instance: HBEP-6's block (frpc
// in the job), with S1's heartbeat settings (HBEP-6, Heartbeats).
type FrpcConfig struct {
	// ServerAddr and ServerPort are where frpc dials: in direct mode the
	// front's dial-home listener; in ssh mode 127.0.0.1 and connection 2's
	// local port.
	ServerAddr string
	ServerPort int
	// LogFile is frpc's log, in the job's private temporary directory.
	LogFile string
	// Token is the Allocation's credential, sent as metadatas.token.
	Token string
	// CAFile is the staged launcher certificate, frpc's only trusted
	// certificate.
	CAFile string
	// ServerName is the name frpc checks the certificate against.
	ServerName string
	// UID is the Instance's UID: the proxy's name and its one custom domain.
	UID string
	// Socket is the Unix socket the proxy serves.
	Socket string
}

var (
	// uidForm is a Kubernetes UID's alphabet, which frps matches as the
	// tcpmux domain of CONNECT <UID>:80.
	uidForm = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
	// hostForm is a DNS name or an IPv4 address.
	hostForm = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9.-]{0,252})$`)
)

// Validate returns every problem with c, joined. No message holds the token.
func (c FrpcConfig) Validate() error {
	var errs []error
	bad := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }
	if !hostForm.MatchString(c.ServerAddr) {
		bad("frpc server address %q is not a host name or an IPv4 address", c.ServerAddr)
	}
	if c.ServerPort < 1 || c.ServerPort > 65535 {
		bad("frpc server port %d is not a port", c.ServerPort)
	}
	if err := agentapi.CheckKey([]byte(c.Token)); err != nil {
		errs = append(errs, errors.New("frpc token: the Allocation's credential is not a key (64 lowercase hex characters)"))
	}
	if !uidForm.MatchString(c.UID) {
		bad("Instance UID %q is not a Kubernetes UID", c.UID)
	}
	for name, p := range map[string]string{"log file": c.LogFile, "certificate": c.CAFile, "socket": c.Socket} {
		if !strings.HasPrefix(p, "/") || strings.ContainsAny(p, "\x00\n\r") {
			bad("frpc %s %q is not an absolute path", name, p)
		}
	}
	if c.ServerName == "" {
		bad("frpc has no TLS server name")
	}
	return errors.Join(errs...)
}

// Render returns frpc's TOML. Each string is written as a JSON string, which
// is a valid TOML basic string (Go's %q is not: TOML has no \x escape).
func (c FrpcConfig) Render() ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	q := func(s string) string {
		b, _ := json.Marshal(s)
		return string(b)
	}
	var b bytes.Buffer
	line := func(k, v string) { b.WriteString(k + " = " + v + "\n") }
	line("serverAddr", q(c.ServerAddr))
	line("serverPort", strconv.Itoa(c.ServerPort))
	// frpc's default exits when its first login fails; false keeps it
	// retrying, like the ssh forwards (HBEP-6).
	line("loginFailExit", "false")
	line("log.to", q(c.LogFile))
	line("metadatas.token", q(c.Token))
	line("transport.protocol", q("wss"))
	line("transport.tls.trustedCaFile", q(c.CAFile))
	line("transport.tls.serverName", q(c.ServerName))
	line("transport.poolCount", "0")
	line("transport.tcpMuxKeepaliveInterval", "10")
	line("transport.heartbeatInterval", "10")
	line("transport.heartbeatTimeout", "25")
	line("transport.dialServerKeepalive", "30")
	b.WriteString("\n[[proxies]]\n")
	line("name", q(c.UID))
	line("type", q("tcpmux"))
	line("multiplexer", q("httpconnect"))
	line("customDomains", "["+q(c.UID)+"]")
	b.WriteString("[proxies.plugin]\n")
	line("type", q("unix_domain_socket"))
	line("unixPath", q(c.Socket))
	return b.Bytes(), nil
}

// proxyVariables are the environment variables a program may take as its
// proxy. frp v0.71.1 takes http_proxy as its proxy to frps when none is
// configured (HBEP-6, frp facts); the others are dropped as well, so no
// version of frpc reaches frps through a proxy of the job's environment.
var proxyVariables = []string{"http_proxy", "https_proxy", "all_proxy", "no_proxy", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"}

// frpcEnv returns env without the proxy variables. It is never nil: a nil
// environment would make exec pass the agent's whole environment on.
func frpcEnv(env []string) []string {
	out := make([]string, 0, len(env))
	for _, kv := range env {
		name, _, _ := strings.Cut(kv, "=")
		drop := false
		for _, p := range proxyVariables {
			if name == p {
				drop = true
				break
			}
		}
		if !drop {
			out = append(out, kv)
		}
	}
	return out
}

// CheckDialHomeCert checks the staged launcher certificate before frpc may
// trust it: the file holds exactly one PEM certificate and nothing else, and
// its public-key pin equals agent.json's pin (HBEP-6, Certificate checks;
// HBEP-7). frpc trusts every certificate in its trustedCaFile, and the pin
// covers one, so a second certificate is refused. frpc checks the server
// name itself.
func CheckDialHomeCert(path, pin string) error {
	if path == "" {
		return errors.New("no dial-home certificate: frpc would verify nothing without one")
	}
	if pin == "" {
		return errors.New("no certificate pin (agent.json's certSPKISHA256) to check the dial-home certificate against")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("the dial-home certificate: %w", err)
	}
	var certs []*x509.Certificate
	for rest := b; ; {
		var blk *pem.Block
		blk, rest = pem.Decode(rest)
		if blk == nil {
			if len(bytes.TrimSpace(rest)) != 0 {
				return fmt.Errorf("the dial-home certificate %s holds data that is not PEM", path)
			}
			break
		}
		if blk.Type != "CERTIFICATE" {
			return fmt.Errorf("the dial-home certificate %s holds a %q block; want one certificate only", path, blk.Type)
		}
		c, err := x509.ParseCertificate(blk.Bytes)
		if err != nil {
			return fmt.Errorf("the dial-home certificate %s: %w", path, err)
		}
		certs = append(certs, c)
	}
	if len(certs) != 1 {
		return fmt.Errorf("the dial-home certificate %s holds %d certificates; want exactly one, the pinned one", path, len(certs))
	}
	if got := agentapi.PinOf(certs[0]); subtle.ConstantTimeCompare([]byte(got), []byte(pin)) != 1 {
		return fmt.Errorf("the dial-home certificate %s has pin %s, not agent.json's %s", path, got, pin)
	}
	return nil
}

// digestForm is agent.json's frpcDigest: 64 lowercase hex characters.
var digestForm = regexp.MustCompile(`^[0-9a-f]{64}$`)

// CheckFrpcDigest checks that the file at path has the SHA-256 digest (64
// lowercase hex characters, agent.json's frpcDigest) before the agent runs it
// (HBEP-6, frpc in the job).
func CheckFrpcDigest(path, digest string) error {
	if !digestForm.MatchString(digest) {
		return fmt.Errorf("frpc digest %q: want 64 lowercase hex characters", digest)
	}
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("frpc: %w", err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return fmt.Errorf("frpc: %w", err)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != digest {
		return fmt.Errorf("frpc %s has SHA-256 %s, not agent.json's %s", path, got, digest)
	}
	return nil
}
