package launcher

import (
	"crypto"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/big"
	"os"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// ensureBulkCert makes sure the bulk path's second certificate (agentapi.Host.BulkCertFile) exists
// and is usable, for up and upgrade with the bulk path on (beta; HBEP-6, The bulk path). It runs
// after ensureAgentEndpointPair, which owns the first certificate and the key.
//
// The first certificate and the key are only read, never written: re-issuing the first would cut
// every running Allocation's token tunnel, since frpc verifies by chain against its staged copy and
// a new leaf for the same key does not chain to the old one (HBEP-6, The bulk path). The second is
// a certificate of its own, for the same key, so it has the same pin, naming
// agentapi.BulkDialHomeServerName only.
//
//   - It is issued when it does not exist, and issued again when the kept one is unusable: it is
//     not exactly one certificate, its pin is not the first's, it does not name the bulk server,
//     it names anything else, it is not valid now, or it does not load with the key. The check is
//     agentapi.VerifyDialHomeCertFor, the one the agent runs on the staged copy, so a certificate
//     the launcher keeps is one the agent accepts.
//   - A new certificate is compared with the first's pin before it is written, so a mismatch
//     writes nothing.
//
// An error names the file and never holds key bytes.
func ensureBulkCert(host agentapi.Host, log io.Writer) error {
	certPath, keyPath, bulkPath := host.AgentCertFile(), host.AgentKeyFile(), host.BulkCertFile()
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return fmt.Errorf("the bulk path's certificate is issued for the agent endpoint's key, whose certificate %s cannot be read: %w", certPath, err)
	}
	keyPEM, err := readAgentKey(keyPath)
	if err != nil {
		return err
	}
	if err := checkAgentPair(certPath, keyPath, certPEM, keyPEM); err != nil {
		return err
	}
	first, err := parseCertPEM(certPEM)
	if err != nil {
		return fmt.Errorf("agent endpoint certificate %s is not usable: %w", certPath, err)
	}
	pin := agentapi.PinOf(first)
	bulkPEM, err := os.ReadFile(bulkPath)
	switch {
	case err == nil:
		why := bulkCertProblem(bulkPEM, keyPEM, pin, time.Now())
		if why == nil {
			return nil
		}
		logf(log, "the bulk path's certificate %s is not usable (%v); issuing it again for the agent endpoint's key", bulkPath, why)
	case !errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("the bulk path's certificate %s: %w", bulkPath, err)
	}
	signer, err := signerOf(keyPEM)
	if err != nil {
		return fmt.Errorf("agent endpoint key %s: %w", keyPath, err)
	}
	return issueBulkCert(bulkPath, keyPath, signer, pin, log)
}

// bulkCertProblem is why a kept bulk certificate is unusable, or nil: the agent's own check
// against the bulk name and the first's pin, then exactly the bulk name and nothing else, then
// the pair with the key.
func bulkCertProblem(bulkPEM, keyPEM []byte, pin string, now time.Time) error {
	if err := agentapi.VerifyDialHomeCertFor(bulkPEM, pin, agentapi.BulkDialHomeServerName, now); err != nil {
		return err
	}
	c, err := parseCertPEM(bulkPEM)
	if err != nil {
		return err
	}
	if len(c.DNSNames) != 1 || c.DNSNames[0] != agentapi.BulkDialHomeServerName || len(c.IPAddresses) != 0 || len(c.URIs) != 0 || len(c.EmailAddresses) != 0 {
		return fmt.Errorf("it names %v %v, not %s alone", c.DNSNames, c.IPAddresses, agentapi.BulkDialHomeServerName)
	}
	if _, err := tls.X509KeyPair(bulkPEM, keyPEM); err != nil {
		return fmt.Errorf("it does not load with the agent endpoint's key: %w", err)
	}
	return nil
}

// issueBulkCert writes a self-signed certificate for signer's key, naming
// agentapi.BulkDialHomeServerName alone, at path (mode 0644: it is public), and checks that it
// loads with the key at keyPath. pin is the first certificate's: a new certificate with another
// pin is refused before anything is written.
func issueBulkCert(path, keyPath string, signer crypto.Signer, pin string, log io.Writer) error {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		return fmt.Errorf("the bulk path's certificate: %w", err)
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial.Add(serial, big.NewInt(1)), // positive
		Subject:               pkix.Name{CommonName: "hpcburst bulk path"},
		NotBefore:             now.Add(-time.Hour), // a little clock skew between hosts
		NotAfter:              now.Add(agentCertValidity),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{agentapi.BulkDialHomeServerName},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, signer.Public(), signer)
	if err != nil {
		return fmt.Errorf("the bulk path's certificate: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return fmt.Errorf("the bulk path's certificate: %w", err)
	}
	if got := agentapi.PinOf(cert); got != pin {
		return fmt.Errorf("the bulk path's certificate %s: not issued, because it would have pin %s, not the first certificate's %s", path, got, pin)
	}
	if err := writeFileAtomic(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		return fmt.Errorf("the bulk path's certificate %s: %w", path, err)
	}
	if _, err := tls.LoadX509KeyPair(path, keyPath); err != nil {
		return fmt.Errorf("the bulk path's certificate %s just written does not load with %s: %w", path, keyPath, err)
	}
	logf(log, "issued the bulk path's certificate %s for %s (pin %s, the agent endpoint's)", path, agentapi.BulkDialHomeServerName, pin)
	return nil
}
