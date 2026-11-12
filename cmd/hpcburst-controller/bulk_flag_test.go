package main

// --bulk-path on hpcburst-controller (beta; HBEP-6, The bulk path; the science agent's rulings S2 and
// S3 on #448; #448 part 3): the flag the launcher passes only when its state records the bulk path; the
// controller refuses it where the bulk path has no frpc to serve (ssh mode, no front) or a block that
// doesn't fit, and with it stages the second certificate and encodes agent.json's block.

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl/ctltest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// TestParseBulkPath: the flag is a bool, off by default, and means nothing without a front that runs
// the bulk frpc. The block reaches base+15 with it (frps-bulk's web server), so the top base is 65520,
// which the launcher's `up --bulk-path` refuses above too. The control: without the flag the same
// command lines parse as before, the top base 65523 among them.
func TestParseBulkPath(t *testing.T) {
	base := []string{"--state-dir", "/state", "--port-base", "29220", "--health-addr", "127.0.0.1:29226"}
	with := func(extra ...string) []string { return append(append([]string{}, base...), extra...) }
	top := func(port string, extra ...string) []string {
		return append([]string{"--state-dir", "/s", "--port-base", port, "--health-addr", "127.0.0.1:29226"}, extra...)
	}
	for _, c := range []struct {
		name    string
		args    []string
		want    bool
		wantErr string
	}{
		{"off by default", with("--front-mode", "direct", "--front-addr", "front.example:29443"), false, ""},
		{"direct mode", with("--front-mode", "direct", "--front-addr", "front.example:29443", "--bulk-path"), true, ""},
		{"host-dialed mode", with("--front-mode", "host-dialed", "--bulk-path"), true, ""},
		{"with the relay keeper", with("--front-mode", "host-dialed", "--relay-keeper-socket", "/run/k.sock", "--bulk-path"), true, ""},
		{"explicitly off", with("--front-mode", "direct", "--front-addr", "front.example:29443", "--bulk-path=false"), false, ""},
		{"no front", with("--bulk-path"), false, "--bulk-path needs --front-mode"},
		{"ssh mode", with("--front-mode", "ssh", "--front-addr", "front.example:22", "--bulk-path"), false, "in ssh mode the agent starts no bulk frpc"},
		{"the top base of the block", top("65520", "--front-mode", "host-dialed", "--bulk-path"), true, ""},
		{"a base whose block does not fit", top("65521", "--front-mode", "host-dialed", "--bulk-path"), false, "out of range with --bulk-path"},
		{"the control: the same base without the flag", top("65521", "--front-mode", "host-dialed"), false, ""},
		{"the control: the alpha top base", top("65523", "--front-mode", "host-dialed"), false, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			f, err := parse(c.args, io.Discard)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("parse error %v, want one containing %q", err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if f.bulkPath != c.want {
				t.Errorf("bulkPath = %t, want %t", f.bulkPath, c.want)
			}
		})
	}
}

// bulkEnv writes the first certificate and its key as the launcher keeps them, and returns the host, the
// first certificate's PEM and its pin.
func bulkEnv(t *testing.T) (host agentapi.Host, first []byte, pin string) {
	t.Helper()
	host = agentapi.Host{StateDir: t.TempDir(), PortBase: 29300}
	cert := writeEndpointCert(t, host)
	return host, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}), agentapi.PinOf(cert)
}

func bulkDeps() deps {
	return deps{
		frpcDigests: func() (map[string]string, error) { return map[string]string{"x86_64": strings.Repeat("a", 64)}, nil },
		username:    func() (string, error) { return "ExampleUser", nil },
	}
}

func parseFlags(t *testing.T, host agentapi.Host, extra ...string) flags {
	t.Helper()
	f, err := parse(append([]string{"--state-dir", host.StateDir, "--port-base", "29300", "--health-addr", "127.0.0.1:29306"}, extra...), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// writeBulkCert puts a bulk certificate where the launcher keeps it.
func writeBulkCert(t *testing.T, host agentapi.Host, b []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(host.BulkCertFile()), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(host.BulkCertFile(), b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// issueBulk makes the bulk certificate of the key behind the first one writeEndpointCert wrote: the same
// key (the one in host.AgentKeyFile), the bulk name only. It reads the key file, as the launcher does.
func issueBulk(t *testing.T, host agentapi.Host) []byte {
	t.Helper()
	kb, err := os.ReadFile(host.AgentKeyFile())
	if err != nil {
		t.Fatal(err)
	}
	blk, _ := pem.Decode(kb)
	if blk == nil {
		t.Fatal("the agent endpoint's key file holds no PEM block")
	}
	k, err := x509.ParsePKCS8PrivateKey(blk.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	key := k.(*ecdsa.PrivateKey)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "example launcher"},
		DNSNames:  []string{"bulk.dial-home.hpcburst.invalid"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// TestFrontSettingsBulkPath: with --bulk-path, frontSettings reads Host.BulkCertFile() into
// BulkCertPEM, unchanged (Create stages these bytes), and checkFront verifies it against the bulk name
// and the SAME pin as the first. Without the flag BulkCertPEM stays empty though the file is there (the
// control, with the file present in both runs). A missing file, the first certificate in the bulk file, a
// bulk certificate of another key and the bulk certificate in the first's file are each refused before
// anything listens, with the agent's own words.
func TestFrontSettingsBulkPath(t *testing.T) {
	host, first, pin := bulkEnv(t)
	bulk := issueBulk(t, host)
	writeBulkCert(t, host, bulk)
	direct := []string{"--front-mode", "direct", "--front-addr", "front.example.org:29443"}

	fs, err := frontSettings(parseFlags(t, host, append(direct, "--bulk-path")...), bulkDeps())
	if err != nil {
		t.Fatalf("frontSettings with --bulk-path: %v", err)
	}
	if !bytes.Equal(fs.BulkCertPEM, bulk) || !bytes.Equal(fs.CertPEM, first) || fs.CertPin != pin {
		t.Errorf("the settings keep BulkCertPEM %d bytes, CertPEM %d bytes, pin %q; want the two files' bytes and the first's pin %q", len(fs.BulkCertPEM), len(fs.CertPEM), fs.CertPin, pin)
	}
	// Judged here with crypto/x509: what was read is a certificate of the bulk name, for the first's key.
	blk, _ := pem.Decode(fs.BulkCertPEM)
	c, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if c.VerifyHostname("bulk.dial-home.hpcburst.invalid") != nil || c.VerifyHostname("dial-home.hpcburst.invalid") == nil || agentapi.PinOf(c) != pin {
		t.Errorf("the bulk certificate read: names %v, pin %s; want the bulk name only and the first's pin %s", c.DNSNames, agentapi.PinOf(c), pin)
	}

	// The control: the same state directory, the file there, no flag.
	fs, err = frontSettings(parseFlags(t, host, direct...), bulkDeps())
	if err != nil || len(fs.BulkCertPEM) != 0 {
		t.Fatalf("frontSettings without --bulk-path: %d bytes of BulkCertPEM, %v; want none read", len(fs.BulkCertPEM), err)
	}
	// Host-dialed mode keeps it too.
	fs, err = frontSettings(parseFlags(t, host, "--front-mode", "host-dialed", "--bulk-path"), bulkDeps())
	if err != nil || !bytes.Equal(fs.BulkCertPEM, bulk) {
		t.Fatalf("host-dialed with --bulk-path: %v, BulkCertPEM %d bytes", err, len(fs.BulkCertPEM))
	}

	for _, rc := range []struct {
		name  string
		setup func()
		want  string
	}{
		{"the first certificate in the bulk file", func() { writeBulkCert(t, host, first) }, "does not name bulk.dial-home.hpcburst.invalid"},
		{"a bulk certificate of another key", func() { _, other, _ := ctltest.CertPair(t); writeBulkCert(t, host, other) }, "its pin"},
		{"the bulk file holds nothing", func() { writeBulkCert(t, host, []byte("not a certificate\n")) }, "the dial-home certificate"},
		{"no bulk file", func() { _ = os.Remove(host.BulkCertFile()) }, "the bulk dial-home certificate"},
		{"an empty bulk file, which would read as no bulk path", func() { writeBulkCert(t, host, nil) }, "is empty"},
	} {
		t.Run(rc.name, func(t *testing.T) {
			rc.setup()
			fs, err := frontSettings(parseFlags(t, host, append(direct, "--bulk-path")...), bulkDeps())
			if err == nil || !strings.Contains(err.Error(), rc.want) {
				t.Fatalf("frontSettings = %+v, %v; want a refusal containing %q", fs, err, rc.want)
			}
		})
	}

	// The reverse: the bulk certificate in the FIRST certificate's file is refused (as it was before
	// the bulk path: the first is judged against the token name), with or without the flag.
	writeBulkCert(t, host, bulk)
	if err := os.WriteFile(host.AgentCertFile(), bulk, 0o644); err != nil {
		t.Fatal(err)
	}
	for _, flag := range [][]string{direct, append(append([]string{}, direct...), "--bulk-path")} {
		if fs, err := frontSettings(parseFlags(t, host, flag...), bulkDeps()); err == nil || !strings.Contains(err.Error(), "does not name dial-home.hpcburst.invalid") {
			t.Errorf("the bulk certificate as the first file (flags %v): %+v, %v; want the agent's refusal by name", flag, fs, err)
		}
	}
}
