// Package ctltest holds the fixtures that the Provider controller's tests
// share (internal/providerctl, its e2e and conformance packages): an
// example launcher certificate and example frpc builds. The tests that judge
// what is staged write their own fixtures and hash them by hand instead, so an
// expected value never comes from here.
package ctltest

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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// CertOptions are what a test sets on an example launcher certificate; the zero
// value is a good one.
type CertOptions struct {
	// Names are its DNS names; nil means agentapi.DialHomeServerName.
	Names []string
	// NotBefore and NotAfter bound its validity; zero values mean 2020-01-01 and
	// 2120-01-01, wide enough for the tests' manual clocks and the wall clock.
	NotBefore, NotAfter time.Time
}

// Cert returns a new good self-signed certificate for
// agentapi.DialHomeServerName, as PEM, and its pin (agentapi.PinOf). Every call
// has its own key.
func Cert(t testing.TB) (certPEM []byte, pin string) { return CertWith(t, CertOptions{}) }

// CertWith is Cert with the options set; a certificate with another name, or
// outside its validity, still has a pin of its own key.
func CertWith(t testing.TB, o CertOptions) (certPEM []byte, pin string) {
	t.Helper()
	if o.Names == nil {
		o.Names = []string{agentapi.DialHomeServerName}
	}
	if o.NotBefore.IsZero() {
		o.NotBefore = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	if o.NotAfter.IsZero() {
		o.NotAfter = time.Date(2120, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "example launcher"},
		DNSNames: o.Names, NotBefore: o.NotBefore, NotAfter: o.NotAfter,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	c, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), agentapi.PinOf(c)
}

// FrpcContent is the example frpc build for a cpuArch: another one for each,
// so a build staged for the wrong architecture has the wrong digest.
func FrpcContent(cpuArch string) []byte {
	return []byte("#!/bin/sh\n# an example frpc build for " + cpuArch + "\n")
}

// Frpc writes the example frpc build for cpuArch into the agent-builds
// directory dir, under agentapi.FrpcBuildName, and returns its SHA-256.
func Frpc(t testing.TB, dir, cpuArch string) string {
	t.Helper()
	b := FrpcContent(cpuArch)
	if err := os.WriteFile(filepath.Join(dir, agentapi.FrpcBuildName(cpuArch)), b, 0o700); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
