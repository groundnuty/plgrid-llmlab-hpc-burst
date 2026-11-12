package launcher

// Tests of the bulk path's second certificate (HBEP-6, The bulk path; issue #448). Each certificate
// is judged with crypto/x509 and crypto/tls directly, and its pin is computed here from the key
// file, never with the launcher's helpers. The first certificate and the key are judged by their
// bytes, inode and modification time: the bulk path must never re-issue the first, since that
// would cut every running Allocation's token tunnel.

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

const bulkName = "bulk.dial-home.hpcburst.invalid" // HBEP-6, The bulk path

// fileMark is what a test compares to tell that a file was not written: its bytes' hash, its
// inode and its modification time.
type fileMark struct {
	sum   [32]byte
	ino   uint64
	mtime time.Time
}

func markOf(t *testing.T, path string) fileMark {
	t.Helper()
	b := mustRead(t, path)
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fileMark{sum: sha256.Sum256(b), ino: fi.Sys().(*syscall.Stat_t).Ino, mtime: fi.ModTime()}
}

// keyOfHost parses the key file at h's path, without the launcher's code.
func keyOfHost(t *testing.T, h agentapi.Host) *ecdsa.PrivateKey {
	t.Helper()
	blk, _ := pem.Decode(mustRead(t, h.AgentKeyFile()))
	if blk == nil {
		t.Fatal("the key file holds no PEM block")
	}
	k, err := x509.ParsePKCS8PrivateKey(blk.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	ek, ok := k.(*ecdsa.PrivateKey)
	if !ok {
		t.Fatalf("the key is a %T", k)
	}
	return ek
}

// certFile parses the one certificate in a PEM file.
func certFile(t *testing.T, path string) *x509.Certificate {
	t.Helper()
	blk, rest := pem.Decode(mustRead(t, path))
	if blk == nil || blk.Type != "CERTIFICATE" || len(bytes.TrimSpace(rest)) != 0 {
		t.Fatalf("%s is not exactly one PEM certificate", path)
	}
	c, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// handshakeWith makes a TLS 1.3 handshake with a server that presents certPath with keyPath, from a
// client that trusts only the certificate in trustPath and checks serverName, as frpc does with its
// trustedCaFile and serverName. It returns the client's error.
func handshakeWith(t *testing.T, certPath, keyPath, trustPath, serverName string) error {
	t.Helper()
	kp, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatalf("%s does not load with %s: %v", certPath, keyPath, err)
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{kp}, MinVersion: tls.VersionTLS13})
	if err != nil {
		skipOrFail(t, "cannot listen on loopback: "+err.Error())
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _ = c.(*tls.Conn).Handshake(); c.Close() }()
		}
	}()
	pool := x509.NewCertPool()
	pool.AddCert(certFile(t, trustPath))
	c, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", ln.Addr().String(),
		&tls.Config{RootCAs: pool, ServerName: serverName, MinVersion: tls.VersionTLS13})
	if err == nil {
		c.Close()
	}
	return err
}

// TestBulkCertSameKeyBulkNameOnly: the second certificate is for the agent endpoint's key (its pin
// is the key's own, so an agent's pin holds for both), names the bulk server and nothing else, and
// is a certificate frpc verifies as the bulk frpc does: trusted alone, for the bulk name. Controls:
// the same handshake fails for the token's name, and a client that trusts the first certificate
// instead fails for the bulk name, as M1 measured with frpc v0.71.1.
func TestBulkCertSameKeyBulkNameOnly(t *testing.T) {
	h := testHost(t)
	if err := ensureAgentEndpointPair(h, nil); err != nil {
		t.Fatal(err)
	}
	if err := ensureBulkCert(h, nil); err != nil {
		t.Fatal(err)
	}
	bulk := certFile(t, h.BulkCertFile())
	sum := sha256.Sum256(bulk.RawSubjectPublicKeyInfo)
	if got, want := sum, pinOfKeyBytes(t, keyOfHost(t, h)); got != want {
		t.Errorf("the second certificate's key is not the agent endpoint's")
	}
	if len(bulk.DNSNames) != 1 || bulk.DNSNames[0] != bulkName || len(bulk.IPAddresses) != 0 {
		t.Errorf("the second certificate names %v %v, want %s alone", bulk.DNSNames, bulk.IPAddresses, bulkName)
	}
	if bulk.IsCA {
		t.Error("the second certificate is a CA")
	}
	if fi, err := os.Stat(h.BulkCertFile()); err != nil || fi.Mode().Perm() != 0o644 {
		t.Errorf("the second certificate's mode: %v %v, want 0644 (it is public)", fi, err)
	}
	if err := handshakeWith(t, h.BulkCertFile(), h.AgentKeyFile(), h.BulkCertFile(), bulkName); err != nil {
		t.Errorf("the bulk frpc's handshake fails: %v", err)
	}
	if err := handshakeWith(t, h.BulkCertFile(), h.AgentKeyFile(), h.BulkCertFile(), agentapi.DialHomeServerName); !isHostnameError(err) {
		t.Errorf("the second certificate checked for the token's name gave %v, want a host name error", err)
	}
	if err := handshakeWith(t, h.BulkCertFile(), h.AgentKeyFile(), h.AgentCertFile(), bulkName); err == nil {
		t.Error("a client that trusts the first certificate accepted the second: the certificates are not apart")
	}
	if err := handshakeWith(t, h.AgentCertFile(), h.AgentKeyFile(), h.AgentCertFile(), agentapi.DialHomeServerName); err != nil {
		t.Errorf("the first certificate's own handshake fails: %v", err)
	}
}

// pinOfKeyBytes is the SHA-256 of the key's DER SubjectPublicKeyInfo, as the pin is defined.
func pinOfKeyBytes(t *testing.T, key *ecdsa.PrivateKey) [32]byte {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	return sha256.Sum256(der)
}

// TestBulkPathLeavesTheFirstCertificateByteForByte (issue #448: never re-issue the first): the
// first certificate and the key keep their bytes, inode and modification time through every way
// the second is made: a first issue, a call that keeps it, an issue after it was deleted, and an
// issue over an unusable one. TestFileMarkSeesAReissuedFirstCertificate is the control that the
// mark can see a re-issue.
func TestBulkPathLeavesTheFirstCertificateByteForByte(t *testing.T) {
	h := testHost(t)
	if err := ensureAgentEndpointPair(h, nil); err != nil {
		t.Fatal(err)
	}
	certBefore, keyBefore := markOf(t, h.AgentCertFile()), markOf(t, h.AgentKeyFile())
	check := func(step string) {
		t.Helper()
		if markOf(t, h.AgentCertFile()) != certBefore {
			t.Errorf("%s: the first certificate was written", step)
		}
		if markOf(t, h.AgentKeyFile()) != keyBefore {
			t.Errorf("%s: the key was written", step)
		}
	}
	// The file system's clock may not move between two writes in one test, so every step sleeps
	// first: a write made in the step then shows in the modification time.
	step := func(name string, before func()) {
		t.Helper()
		time.Sleep(20 * time.Millisecond)
		if before != nil {
			before()
		}
		if err := ensureBulkCert(h, nil); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		check(name)
	}
	step("the first issue", nil)
	step("a call that keeps it", nil)
	step("an issue after it was deleted", func() {
		if err := os.Remove(h.BulkCertFile()); err != nil {
			t.Fatal(err)
		}
	})
	step("an issue over another key's certificate", func() {
		writeCertFor(t, h.BulkCertFile(), newKey(t), []string{bulkName}, time.Now().Add(time.Hour))
	})
}

// TestFileMarkSeesAReissuedFirstCertificate: the control for the test above. The launcher's own
// re-issue of a first certificate that lacks the dial-home name (reissueWithoutName, for the same
// key) changes the mark, so a re-issue by the bulk path could not hide from it.
func TestFileMarkSeesAReissuedFirstCertificate(t *testing.T) {
	h := testHost(t)
	writeOldStylePair(t, h, newKey(t))
	before := markOf(t, h.AgentCertFile())
	time.Sleep(20 * time.Millisecond)
	if err := ensureAgentEndpointPair(h, nil); err != nil {
		t.Fatal(err)
	}
	if markOf(t, h.AgentCertFile()) == before {
		t.Fatal("the first certificate was re-issued, and its mark did not change: the mark proves nothing")
	}
}

// writeCertFor writes a self-signed certificate for key, naming names, valid until notAfter.
func writeCertFor(t *testing.T, path string, key *ecdsa.PrivateKey, names []string, notAfter time.Time) {
	t.Helper()
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(9), Subject: pkix.Name{CommonName: "example"},
		NotBefore: notAfter.Add(-48 * time.Hour), NotAfter: notAfter, DNSNames: names,
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestBulkCertKeptAcrossCalls: a usable second certificate is kept as it is.
func TestBulkCertKeptAcrossCalls(t *testing.T) {
	h := testHost(t)
	if err := ensureAgentEndpointPair(h, nil); err != nil {
		t.Fatal(err)
	}
	if err := ensureBulkCert(h, nil); err != nil {
		t.Fatal(err)
	}
	before := markOf(t, h.BulkCertFile())
	time.Sleep(20 * time.Millisecond)
	var log bytes.Buffer
	if err := ensureBulkCert(h, &log); err != nil {
		t.Fatal(err)
	}
	if markOf(t, h.BulkCertFile()) != before {
		t.Error("a usable second certificate was written again")
	}
	if log.Len() != 0 {
		t.Errorf("a call that keeps the certificate logged %q", log.String())
	}
}

// TestUnusableBulkCertIsIssuedAgain: a kept second certificate that the agent would refuse, or
// that names more than the bulk server, is replaced by one for the agent endpoint's key and the
// bulk name alone, and the log says why.
func TestUnusableBulkCertIsIssuedAgain(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write func(t *testing.T, h agentapi.Host)
	}{
		{"another key's certificate", func(t *testing.T, h agentapi.Host) {
			writeCertFor(t, h.BulkCertFile(), newKey(t), []string{bulkName}, time.Now().Add(time.Hour))
		}},
		{"the first certificate copied", func(t *testing.T, h agentapi.Host) {
			if err := os.WriteFile(h.BulkCertFile(), mustRead(t, h.AgentCertFile()), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{"both names", func(t *testing.T, h agentapi.Host) {
			writeCertFor(t, h.BulkCertFile(), keyOfHost(t, h), []string{agentapi.DialHomeServerName, bulkName}, time.Now().Add(time.Hour))
		}},
		{"expired", func(t *testing.T, h agentapi.Host) {
			writeCertFor(t, h.BulkCertFile(), keyOfHost(t, h), []string{bulkName}, time.Now().Add(-time.Minute))
		}},
		{"two certificates", func(t *testing.T, h agentapi.Host) {
			if err := ensureBulkCert(h, nil); err != nil {
				t.Fatal(err)
			}
			b := mustRead(t, h.BulkCertFile())
			if err := os.WriteFile(h.BulkCertFile(), append(b, b...), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{"no PEM", func(t *testing.T, h agentapi.Host) {
			if err := os.WriteFile(h.BulkCertFile(), []byte("junk\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := testHost(t)
			if err := ensureAgentEndpointPair(h, nil); err != nil {
				t.Fatal(err)
			}
			tc.write(t, h)
			first := markOf(t, h.AgentCertFile())
			var log bytes.Buffer
			if err := ensureBulkCert(h, &log); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(log.String(), "is not usable") || !strings.Contains(log.String(), "issued the bulk path's certificate") {
				t.Errorf("the log %q does not say the kept certificate was unusable and a new one issued", log.String())
			}
			bulk := certFile(t, h.BulkCertFile())
			if sha256.Sum256(bulk.RawSubjectPublicKeyInfo) != pinOfKeyBytes(t, keyOfHost(t, h)) {
				t.Error("the new certificate is not for the agent endpoint's key")
			}
			if len(bulk.DNSNames) != 1 || bulk.DNSNames[0] != bulkName {
				t.Errorf("the new certificate names %v", bulk.DNSNames)
			}
			if err := handshakeWith(t, h.BulkCertFile(), h.AgentKeyFile(), h.BulkCertFile(), bulkName); err != nil {
				t.Errorf("the new certificate fails the bulk frpc's handshake: %v", err)
			}
			if markOf(t, h.AgentCertFile()) != first {
				t.Error("the first certificate was written")
			}
		})
	}
}

// TestEnsureBulkCertNeedsTheFirstPair: with no first certificate there is no key to issue for
// (ensureAgentEndpointPair makes the pair first), so nothing is written and the error names the
// first certificate.
func TestEnsureBulkCertNeedsTheFirstPair(t *testing.T) {
	h := testHost(t)
	err := ensureBulkCert(h, nil)
	if err == nil || !strings.Contains(err.Error(), h.AgentCertFile()) {
		t.Fatalf("ensureBulkCert with no pair: %v, want an error naming %s", err, h.AgentCertFile())
	}
	if _, serr := os.Stat(h.BulkCertFile()); !os.IsNotExist(serr) {
		t.Errorf("a second certificate was written with no pair: %v", serr)
	}
}

// TestIssueBulkCertRefusesAnotherPin: the pin guard writes nothing when the new certificate would
// not have the first's pin.
func TestIssueBulkCertRefusesAnotherPin(t *testing.T) {
	h := testHost(t)
	if err := ensureAgentEndpointPair(h, nil); err != nil {
		t.Fatal(err)
	}
	err := issueBulkCert(h.BulkCertFile(), h.AgentKeyFile(), keyOfHost(t, h), strings.Repeat("0", 64), nil)
	if err == nil || !strings.Contains(err.Error(), "not issued") {
		t.Fatalf("issue with another pin: %v, want a refusal", err)
	}
	if _, serr := os.Stat(h.BulkCertFile()); !os.IsNotExist(serr) {
		t.Errorf("the refused certificate was written: %v", serr)
	}
}
