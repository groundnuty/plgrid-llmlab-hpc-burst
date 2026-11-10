package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

// DialHomeServerName is the dial-home listener's synthetic server name, which
// frpc verifies (HBEP-6, frpc in the job: "transport.tls.serverName").
const DialHomeServerName = "dial-home.hpcburst.invalid"

// runCerts writes a throwaway self-signed certificate and key for the
// dial-home listener: cert.pem (which frpc also trusts as its CA, as the
// launcher's certificate is) and key.pem, mode 600.
func runCerts(args []string) error {
	fs := flag.NewFlagSet("certs", flag.ContinueOnError)
	dir := fs.String("dir", "", "directory for cert.pem and key.pem")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" {
		return fmt.Errorf("-dir is required")
	}
	certPEM, keyPEM, err := selfSigned(DialHomeServerName, time.Now())
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(*dir, "cert.pem"), certPEM, 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(*dir, "key.pem"), keyPEM, 0o600)
}

// selfSigned returns a P-256 certificate for name, valid for a week from an
// hour before now, and its key, both PEM.
func selfSigned(name string, now time.Time) (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	if err != nil {
		return nil, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: name},
		DNSNames:              []string{name},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(7 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}
	kder, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kder}), nil
}
