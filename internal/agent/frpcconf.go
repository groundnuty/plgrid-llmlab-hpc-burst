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
	"syscall"
	"time"

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

// CheckDialHomeCert reads the staged launcher certificate and checks it
// (VerifyDialHomeCert). A read error is returned as it is; a content error
// wraps errRefused.
func CheckDialHomeCert(path, pin string) error {
	if path == "" {
		return fmt.Errorf("%w: no dial-home certificate: frpc would verify nothing without one", errRefused)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("the dial-home certificate: %w", err)
	}
	return VerifyDialHomeCert(b, pin, time.Now())
}

// VerifyDialHomeCert checks the launcher certificate's bytes before frpc may
// trust them (HBEP-6, Certificate checks; HBEP-7): exactly one PEM
// certificate, with nothing before, between or after it and no PEM headers;
// its public-key pin equals agent.json's pin; it names
// agentapi.DialHomeServerName, which frpc checks the front against; and it is
// valid at now. frpc trusts every certificate in its trustedCaFile, and the
// pin covers one, so a second one is refused. Every error wraps errRefused.
func VerifyDialHomeCert(b []byte, pin string, now time.Time) error {
	refuse := func(format string, args ...any) error {
		return fmt.Errorf("%w: the dial-home certificate: %s", errRefused, fmt.Sprintf(format, args...))
	}
	if pin == "" {
		return refuse("no pin (agent.json's certSPKISHA256) to check it against")
	}
	var certs []*x509.Certificate
	rest := b
	for len(bytes.TrimSpace(rest)) != 0 {
		trimmed := bytes.TrimLeft(rest, " \t\r\n")
		if !bytes.HasPrefix(trimmed, []byte("-----BEGIN ")) {
			return refuse("it holds data that is not a PEM block")
		}
		blk, next := pem.Decode(trimmed)
		if blk == nil {
			return refuse("it holds a malformed PEM block")
		}
		rest = next
		if blk.Type != "CERTIFICATE" {
			return refuse("it holds a %q block; want one certificate only", blk.Type)
		}
		if len(blk.Headers) != 0 {
			// frpc's AppendCertsFromPEM skips a block with headers, which
			// would leave it trusting nothing.
			return refuse("its certificate block has PEM headers")
		}
		c, err := x509.ParseCertificate(blk.Bytes)
		if err != nil {
			return refuse("%v", err)
		}
		certs = append(certs, c)
	}
	if len(certs) != 1 {
		return refuse("it holds %d certificates; want exactly one, the pinned one", len(certs))
	}
	c := certs[0]
	if got := agentapi.PinOf(c); subtle.ConstantTimeCompare([]byte(got), []byte(pin)) != 1 {
		return refuse("its pin %s is not agent.json's %s", got, pin)
	}
	if err := c.VerifyHostname(agentapi.DialHomeServerName); err != nil {
		return refuse("it does not name %s, which frpc checks: %v", agentapi.DialHomeServerName, err)
	}
	if now.Before(c.NotBefore) || now.After(c.NotAfter) {
		return refuse("it is valid from %s to %s, not now (%s)", c.NotBefore.UTC().Format(time.RFC3339), c.NotAfter.UTC().Format(time.RFC3339), now.UTC().Format(time.RFC3339))
	}
	return nil
}

// digestForm is agent.json's frpcDigest: 64 lowercase hex characters.
var digestForm = regexp.MustCompile(`^[0-9a-f]{64}$`)

// verifiedBinary is frpc opened once and hashed through that descriptor
// (HBEP-6, frpc in the job). It is run as /proc/self/fd/3 with the
// descriptor passed on, so what runs is the file that was hashed, even
// after its path is replaced or purged. The file's size and change times
// are kept: a write into the same file shows there, and it is hashed again
// before the next start.
type verifiedBinary struct {
	f      *os.File
	path   string
	digest string
	stat   fileStamp
}

// fileStamp is what an in-place write changes.
type fileStamp struct {
	size        int64
	mtime, ctim syscall.Timespec
}

func stampOf(f *os.File) (fileStamp, error) {
	var st syscall.Stat_t
	if err := syscall.Fstat(int(f.Fd()), &st); err != nil {
		return fileStamp{}, err
	}
	return fileStamp{size: st.Size, mtime: st.Mtim, ctim: st.Ctim}, nil
}

// openVerifiedBinary opens the file at path and checks its SHA-256 against
// digest through the same descriptor. A malformed digest and a mismatch wrap
// errRefused; an I/O error does not, and can be retried.
func openVerifiedBinary(path, digest string) (*verifiedBinary, error) {
	if !digestForm.MatchString(digest) {
		return nil, fmt.Errorf("%w: frpc digest %q: want 64 lowercase hex characters", errRefused, digest)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("frpc: %w", err)
	}
	v := &verifiedBinary{f: f, path: path, digest: digest}
	if err := v.rehash(); err != nil {
		f.Close()
		return nil, err
	}
	return v, nil
}

// rehash hashes the open file from its start and checks the digest.
func (v *verifiedBinary) rehash() error {
	st, err := stampOf(v.f)
	if err != nil {
		return fmt.Errorf("frpc: %w", err)
	}
	h := sha256.New()
	if _, err := io.Copy(h, io.NewSectionReader(v.f, 0, st.size)); err != nil {
		return fmt.Errorf("frpc: %w", err)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != v.digest {
		return fmt.Errorf("%w: frpc %s has SHA-256 %s, not agent.json's %s", errRefused, v.path, got, v.digest)
	}
	v.stat = st
	return nil
}

// recheck hashes the file again if it was written to since it was hashed.
func (v *verifiedBinary) recheck() error {
	st, err := stampOf(v.f)
	if err != nil {
		return fmt.Errorf("frpc: %w", err)
	}
	if st == v.stat {
		return nil
	}
	return v.rehash()
}

// CheckFrpcDigest checks that the file at path has the SHA-256 digest (64
// lowercase hex characters, agent.json's frpcDigest).
func CheckFrpcDigest(path, digest string) error {
	v, err := openVerifiedBinary(path, digest)
	if err != nil {
		return err
	}
	return v.f.Close()
}
