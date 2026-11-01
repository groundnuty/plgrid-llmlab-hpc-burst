package launcher

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// agentCertValidity is how long a certificate the launcher issues is valid. Nothing renews it, and
// agents trust its pin rather than a chain, so it is made long enough to outlast any host.
const agentCertValidity = 100 * 365 * 24 * time.Hour

// agentKeyReadLimit bounds what is read from a key file, so a wrong file cannot fill memory.
const agentKeyReadLimit = 64 << 10

// ensureAgentEndpointPair makes sure the agent endpoint's certificate and key (Host.AgentCertFile
// and Host.AgentKeyFile) exist and are usable, before anything that loads them starts:
//
//   - neither exists: it creates both, a self-signed ECDSA P-256 certificate for 127.0.0.1,
//     localhost and agentapi.DialHomeServerName (the name frpc verifies, HBEP-6), the key first;
//   - the key exists without the certificate (a start that was killed between the two writes): it
//     issues a certificate for that key, so the pin, which follows the key, does not change;
//   - the certificate exists without the key: an error naming the key. The key is never invented
//     for a certificate that agents may already pin;
//   - both exist: they are kept, after checking that they load as a pair (as the server loads them,
//     with tls.X509KeyPair) and that the key is a regular file owned by the user with no
//     permission bit for the group or others. A certificate that lacks agentapi.DialHomeServerName
//     (one made before the name was added) is issued again for the same key, so the pin does not
//     change; the key is never replaced.
//
// The directory is made 0700 and the key 0600. An error names the file and never holds key bytes.
func ensureAgentEndpointPair(host agentapi.Host, log io.Writer) error {
	certPath, keyPath := host.AgentCertFile(), host.AgentKeyFile()
	dir := filepath.Dir(certPath)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("agent endpoint directory: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("agent endpoint directory: %w", err)
	}
	certPEM, certErr := os.ReadFile(certPath)
	if certErr != nil && !errors.Is(certErr, os.ErrNotExist) {
		return fmt.Errorf("agent endpoint certificate %s: %w", certPath, certErr)
	}
	keyPEM, keyErr := readAgentKey(keyPath)
	if keyErr != nil && !errors.Is(keyErr, os.ErrNotExist) {
		return keyErr
	}
	haveCert, haveKey := certErr == nil, keyErr == nil
	switch {
	case haveCert && haveKey:
		if err := checkAgentPair(certPath, keyPath, certPEM, keyPEM); err != nil {
			return err
		}
		return reissueWithoutName(certPath, keyPath, certPEM, keyPEM, log)
	case haveCert:
		return fmt.Errorf("agent endpoint key %s is missing, but its certificate %s exists: restore the key, or remove the certificate to have up create a new pair (agents that pin the old certificate then need the new pin)", keyPath, certPath)
	case haveKey:
		signer, err := signerOf(keyPEM)
		if err != nil {
			return fmt.Errorf("agent endpoint key %s: %w", keyPath, err)
		}
		return issueAgentCert(certPath, keyPath, signer, "created", log)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("agent endpoint key: %w", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return fmt.Errorf("agent endpoint key: %w", err)
	}
	if err := writeFileAtomic(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		return fmt.Errorf("agent endpoint key %s: %w", keyPath, err)
	}
	if err := issueAgentCert(certPath, keyPath, key, "created", log); err != nil {
		_ = os.Remove(keyPath) // a half-made pair would be refused at the next start
		return err
	}
	return nil
}

// reissueWithoutName issues the certificate again when the kept one (which checkAgentPair has
// accepted with its key) is not valid for agentapi.DialHomeServerName, and leaves it alone
// otherwise. The new certificate is for the same key, so the pin the agents hold stays the same;
// a new key is never made here. The key file is not written.
func reissueWithoutName(certPath, keyPath string, certPEM, keyPEM []byte, log io.Writer) error {
	cert, err := parseCertPEM(certPEM)
	if err != nil {
		return fmt.Errorf("agent endpoint certificate %s is not usable: %w", certPath, err)
	}
	if cert.VerifyHostname(agentapi.DialHomeServerName) == nil {
		return nil
	}
	signer, err := signerOf(keyPEM)
	if err != nil {
		return fmt.Errorf("agent endpoint key %s: %w", keyPath, err)
	}
	// The pin is the hash of the public key. The pair loaded, so the keys agree; this refuses to
	// write if they somehow do not, since a changed pin would break every agent that holds it.
	spki, err := x509.MarshalPKIXPublicKey(signer.Public())
	if err != nil {
		return fmt.Errorf("agent endpoint key %s: %w", keyPath, err)
	}
	if sum := sha256.Sum256(spki); hex.EncodeToString(sum[:]) != agentapi.PinOf(cert) {
		return fmt.Errorf("agent endpoint certificate %s: not re-issued for %s, because the new certificate would have another pin than the one agents hold", certPath, agentapi.DialHomeServerName)
	}
	logf(log, "the agent endpoint certificate %s is not valid for %s; issuing it again for the same key", certPath, agentapi.DialHomeServerName)
	return issueAgentCert(certPath, keyPath, signer, "re-issued", log)
}

// readAgentKey reads the key file after checking what it is: a regular file (not a link) owned by
// the user, readable by its owner and by no one else. A missing file wraps os.ErrNotExist.
func readAgentKey(path string) ([]byte, error) {
	li, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("agent endpoint key %s: %w", path, err)
	}
	if err := checkAgentKeyInfo(path, li); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("agent endpoint key %s: %w", path, err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("agent endpoint key %s: %w", path, err)
	}
	if !os.SameFile(li, fi) {
		return nil, fmt.Errorf("agent endpoint key %s changed while it was opened", path)
	}
	b, err := io.ReadAll(io.LimitReader(f, agentKeyReadLimit+1))
	if err != nil {
		return nil, fmt.Errorf("agent endpoint key %s: read: %w", path, err)
	}
	if len(b) > agentKeyReadLimit {
		return nil, fmt.Errorf("agent endpoint key %s is longer than %d bytes: it is not a key", path, agentKeyReadLimit)
	}
	return b, nil
}

func checkAgentKeyInfo(path string, fi os.FileInfo) error {
	if !fi.Mode().IsRegular() {
		return fmt.Errorf("agent endpoint key %s is not a regular file (%s)", path, fi.Mode().Type())
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("agent endpoint key %s: its owner cannot be read on this platform", path)
	}
	if st.Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("agent endpoint key %s is owned by user %d, not by the user running up (%d)", path, st.Uid, os.Geteuid())
	}
	if perm := fi.Mode().Perm(); perm&0o077 != 0 || perm&0o400 == 0 {
		return fmt.Errorf("agent endpoint key %s has mode %04o: it must be readable by its owner only (chmod 600 it, or remove it and its certificate to have up create a new pair)", path, perm)
	}
	return nil
}

// checkAgentPair returns nil when the certificate and the key load as a pair.
func checkAgentPair(certPath, keyPath string, certPEM, keyPEM []byte) error {
	_, err := tls.X509KeyPair(certPEM, keyPEM)
	if err == nil {
		return nil
	}
	// X509KeyPair does not say which of the two is at fault; the certificate alone can be parsed.
	if _, cerr := parseCertPEM(certPEM); cerr != nil {
		return fmt.Errorf("agent endpoint certificate %s is not usable: %w", certPath, cerr)
	}
	return fmt.Errorf("agent endpoint key %s is not a usable key for the certificate %s: %w", keyPath, certPath, err)
}

func parseCertPEM(b []byte) (*x509.Certificate, error) {
	for {
		var blk *pem.Block
		if blk, b = pem.Decode(b); blk == nil {
			return nil, errors.New("no CERTIFICATE PEM block")
		} else if blk.Type == "CERTIFICATE" {
			return x509.ParseCertificate(blk.Bytes)
		}
	}
}

// signerOf parses a PEM private key in any of the forms tls.X509KeyPair accepts.
func signerOf(keyPEM []byte) (crypto.Signer, error) {
	blk, _ := pem.Decode(keyPEM)
	if blk == nil {
		return nil, errors.New("no PEM block")
	}
	if k, err := x509.ParsePKCS8PrivateKey(blk.Bytes); err == nil {
		if s, ok := k.(crypto.Signer); ok {
			return s, nil
		}
	}
	if k, err := x509.ParseECPrivateKey(blk.Bytes); err == nil {
		return k, nil
	}
	if k, err := x509.ParsePKCS1PrivateKey(blk.Bytes); err == nil {
		return k, nil
	}
	return nil, errors.New("not a private key that can sign")
}

// issueAgentCert writes a self-signed certificate for signer's key at certPath (mode 0644: it is
// public) and checks that it loads with the key at keyPath. verb is how the log says what was done.
func issueAgentCert(certPath, keyPath string, signer crypto.Signer, verb string, log io.Writer) error {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		return fmt.Errorf("agent endpoint certificate: %w", err)
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial.Add(serial, big.NewInt(1)), // positive
		Subject:               pkix.Name{CommonName: "hpcburst agent endpoint"},
		NotBefore:             now.Add(-time.Hour), // a little clock skew between hosts
		NotAfter:              now.Add(agentCertValidity),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost", agentapi.DialHomeServerName},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, signer.Public(), signer)
	if err != nil {
		return fmt.Errorf("agent endpoint certificate: %w", err)
	}
	if err := writeFileAtomic(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		return fmt.Errorf("agent endpoint certificate %s: %w", certPath, err)
	}
	if _, err := tls.LoadX509KeyPair(certPath, keyPath); err != nil {
		return fmt.Errorf("the agent endpoint certificate %s just written does not load with %s: %w", certPath, keyPath, err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return fmt.Errorf("agent endpoint certificate: %w", err)
	}
	logf(log, "%s the agent endpoint certificate %s (pin %s)", verb, certPath, agentapi.PinOf(cert))
	return nil
}
