package agentapi

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"
)

type testCert struct {
	pem []byte
	der []byte
	pin string
}

// newTestCert makes a self-signed certificate; its pin is computed here from the
// DER SubjectPublicKeyInfo, not by PinOf.
func newTestCert(t *testing.T, names []string, notBefore, notAfter time.Time) testCert {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "example launcher"},
		DNSNames: names, NotBefore: notBefore, NotAfter: notAfter}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	c, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(c.RawSubjectPublicKeyInfo)
	return testCert{pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), der: der, pin: hex.EncodeToString(sum[:])}
}

// VerifyDialHomeCert is the one check the agent and the Provider's controller
// share (HBEP-6, Certificate checks): exactly one PEM certificate and nothing
// else, the pin, the dial-home name, and validity now. The cases are the agent's
// (internal/agent, TestCheckDialHomeCert) as bytes, plus the Provider's reason to
// share it: text before a PEM that has the right pin, which pem.Decode alone skips.
func TestVerifyDialHomeCert(t *testing.T) {
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	valid := func(names ...string) testCert {
		return newTestCert(t, names, now.Add(-time.Hour), now.Add(24*time.Hour))
	}
	a, b := valid(DialHomeServerName), valid(DialHomeServerName)
	noName := valid("other.hpcburst.invalid")
	expired := newTestCert(t, []string{DialHomeServerName}, now.Add(-48*time.Hour), now.Add(-time.Hour))
	early := newTestCert(t, []string{DialHomeServerName}, now.Add(time.Hour), now.Add(48*time.Hour))
	key := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: []byte{1, 2, 3}})
	withHeaders := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Headers: map[string]string{"Proc-Type": "4,ENCRYPTED"}, Bytes: a.der})
	cat := func(parts ...[]byte) []byte {
		var out []byte
		for _, p := range parts {
			out = append(out, p...)
		}
		return out
	}
	for _, tc := range []struct {
		name string
		b    []byte
		pin  string
		want string // "" accepts
	}{
		{"the pinned certificate", a.pem, a.pin, ""},
		{"white space around it", cat([]byte("\n  "), a.pem, []byte("\n\n")), a.pin, ""},
		{"text before the PEM, the right pin", cat([]byte("Certificate:\n    Data: ...\n"), a.pem), a.pin, "not a PEM block"},
		{"text after the PEM", cat(a.pem, []byte("junk")), a.pin, "not a PEM block"},
		{"no dial-home name", noName.pem, noName.pin, "does not name " + DialHomeServerName},
		{"expired", expired.pem, expired.pin, "not now (2026-10-30T12:00:00Z)"},
		{"not yet valid", early.pem, early.pin, "not now (2026-10-30T12:00:00Z)"},
		{"PEM headers", withHeaders, a.pin, "PEM headers"},
		{"no pin", a.pem, "", "no pin"},
		{"another pin", a.pem, b.pin, "is not agent.json's " + b.pin},
		{"two certificates", cat(a.pem, b.pem), a.pin, "2 certificates"},
		{"a key next to it", cat(a.pem, key), a.pin, "want one certificate only"},
		{"DER, not PEM", a.der, a.pin, "not a PEM block"},
		{"nothing", nil, a.pin, "0 certificates"},
		{"a block that is no certificate", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("junk")}), a.pin, "the dial-home certificate:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := VerifyDialHomeCert(tc.b, tc.pin, now)
			switch {
			case tc.want == "" && err != nil:
				t.Errorf("refused: %v", err)
			case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want) || !strings.HasPrefix(err.Error(), "the dial-home certificate: ")):
				t.Errorf("error %v, want one that starts with the dial-home certificate and contains %q", err, tc.want)
			}
		})
	}
}
