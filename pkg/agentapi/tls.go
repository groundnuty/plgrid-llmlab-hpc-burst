package agentapi

import (
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
)

// DialHomeServerName is the fixed synthetic DNS name of the launcher's dial-home
// certificate. It is both the certificate's subject alternative name and the
// value of frpc's transport.tls.serverName, in both dial-home modes (in ssh
// mode frpc dials loopback, so the name is never resolved). The .invalid top-level
// domain never resolves (HBEP-6, frpc in the job; HBEP-11, the launcher's
// certificate; `@hpc-burst-science-agent`, 31 Oct 2026).
const DialHomeServerName = "dial-home.hpcburst.invalid"

// BulkDialHomeServerName is the bulk path's server name (beta; HBEP-6, The bulk
// path, `@hpc-burst-science-agent`, 12 Nov 2026): the bulk frpc's
// transport.tls.serverName, and the only DNS name of the launcher's second
// certificate, which is issued for the same key as the first and so has the
// same pin. The dial-home listener serves that certificate, and passes
// GET /~!frp to the second frps, only to a client that names this server. The
// first certificate never carries it: re-issuing the first would cut every
// running Allocation's token tunnel (HBEP-6, The bulk path).
const BulkDialHomeServerName = "bulk.dial-home.hpcburst.invalid"

// PinOf returns a certificate's pin: the SHA-256 of its DER-encoded
// SubjectPublicKeyInfo, as 64 lowercase hex characters. The pin follows the
// key, so a certificate re-issued for the same key keeps it.
func PinOf(cert *x509.Certificate) string {
	sum := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
	return hex.EncodeToString(sum[:])
}

// VerifyPin returns nil when the leaf of a presented chain has the pin.
func VerifyPin(rawCerts [][]byte, pin string) error {
	if len(rawCerts) == 0 {
		return errors.New("agent endpoint presented no certificate")
	}
	leaf, err := x509.ParseCertificate(rawCerts[0])
	if err != nil {
		return fmt.Errorf("agent endpoint certificate: %w", err)
	}
	if got := PinOf(leaf); subtle.ConstantTimeCompare([]byte(got), []byte(pin)) != 1 {
		return fmt.Errorf("agent endpoint certificate pin %s is not the pinned %s", got, pin)
	}
	return nil
}

// ClientTLSConfig returns the agent's TLS configuration: it trusts exactly a
// server whose leaf certificate has the pin, and checks no CA and no host
// name, because the pin is the trust (HBEP-7: agent.json carries the pin).
// TLS 1.3 only.
func ClientTLSConfig(pin string) *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		// The default verification is replaced, not skipped: VerifyConnection
		// below runs on every handshake and refuses any other key.
		InsecureSkipVerify: true, //nolint:gosec // pinned in VerifyConnection
		VerifyConnection: func(cs tls.ConnectionState) error {
			raw := make([][]byte, len(cs.PeerCertificates))
			for i, c := range cs.PeerCertificates {
				raw[i] = c.Raw
			}
			return VerifyPin(raw, pin)
		},
	}
}
