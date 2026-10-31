package launcher

// Tests of the agent endpoint's certificate pair (Astra 13 on PR #88). A pair is judged by loading
// it as the agent API's server does (tls.LoadX509KeyPair), by a TLS handshake that a client pinned
// with agentapi.PinOf must accept, and by the pin the test computes from the key itself.

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

func testHost(t *testing.T) agentapi.Host {
	t.Helper()
	return agentapi.Host{StateDir: filepath.Join(shortDir(t), "st"), PortBase: 29220}
}

// loadPair loads the pair at host's paths as the server does, and returns the certificate's pin.
func loadPair(t *testing.T, h agentapi.Host) (cert *x509.Certificate, pin string) {
	t.Helper()
	kp, err := tls.LoadX509KeyPair(h.AgentCertFile(), h.AgentKeyFile())
	if err != nil {
		t.Fatalf("the pair does not load: %v", err)
	}
	cert, err = x509.ParseCertificate(kp.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	return cert, agentapi.PinOf(cert)
}

// pinOfKey is the pin a certificate for key must have, computed from the key alone (the SHA-256 of
// its DER SubjectPublicKeyInfo), without the launcher's or agentapi's code.
func pinOfKey(t *testing.T, key *ecdsa.PrivateKey) string {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(der)
	return hex.EncodeToString(sum[:])
}

func mode(t *testing.T, path string) os.FileMode {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Mode().Perm()
}

// writePair writes key and a self-signed certificate for certKey to h's paths, key mode keyMode.
func writePair(t *testing.T, h agentapi.Host, key, certKey *ecdsa.PrivateKey, keyMode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(h.AgentKeyFile()), 0o700); err != nil {
		t.Fatal(err)
	}
	kder, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(h.AgentKeyFile(), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kder}), keyMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(h.AgentKeyFile(), keyMode); err != nil { // the umask must not decide
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	cder, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &certKey.PublicKey, certKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(h.AgentCertFile(), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cder}), 0o644); err != nil {
		t.Fatal(err)
	}
}

func newKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// TestAgentEndpointPairIsCreated: with neither file present, both are created with the modes
// the layout names, the certificate is self-signed for loopback and long-lived, the pair loads,
// and a client pinned with PinOf completes a TLS 1.3 handshake with a server that uses it.
func TestAgentEndpointPairIsCreated(t *testing.T) {
	h := testHost(t)
	var log bytes.Buffer
	if err := ensureAgentEndpointPair(h, &log); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Dir(h.AgentCertFile())
	if m := mode(t, dir); m != 0o700 {
		t.Errorf("%s has mode %04o, want 0700", dir, m)
	}
	if m := mode(t, h.AgentKeyFile()); m != 0o600 {
		t.Errorf("the key has mode %04o, want 0600", m)
	}
	cert, pin := loadPair(t, h)
	if cert.PublicKeyAlgorithm != x509.ECDSA || cert.PublicKey.(*ecdsa.PublicKey).Curve != elliptic.P256() {
		t.Errorf("certificate key %v %T, want ECDSA P-256", cert.PublicKeyAlgorithm, cert.PublicKey)
	}
	// Self-signed: its own key made the signature, and it names itself as issuer. (CheckSignatureFrom
	// would refuse it, since a leaf is no CA.)
	if err := cert.CheckSignature(cert.SignatureAlgorithm, cert.RawTBSCertificate, cert.Signature); err != nil || !bytes.Equal(cert.RawIssuer, cert.RawSubject) {
		t.Errorf("not self-signed: %v", err)
	}
	if err := cert.VerifyHostname("127.0.0.1"); err != nil {
		t.Errorf("127.0.0.1: %v", err)
	}
	if err := cert.VerifyHostname("localhost"); err != nil {
		t.Errorf("localhost: %v", err)
	}
	if len(cert.ExtKeyUsage) != 1 || cert.ExtKeyUsage[0] != x509.ExtKeyUsageServerAuth {
		t.Errorf("extended key usage %v, want server auth", cert.ExtKeyUsage)
	}
	if cert.NotBefore.After(time.Now()) || cert.NotAfter.Before(time.Now().AddDate(10, 0, 0)) {
		t.Errorf("valid %s to %s: not valid now, or for under ten years", cert.NotBefore, cert.NotAfter)
	}
	if !strings.Contains(log.String(), pin) {
		t.Errorf("the log %q does not name the pin %s", log.String(), pin)
	}

	// The key in the file is the one the certificate was issued for, and the pin follows it.
	kb, err := os.ReadFile(h.AgentKeyFile())
	if err != nil {
		t.Fatal(err)
	}
	blk, _ := pem.Decode(kb)
	parsed, err := x509.ParsePKCS8PrivateKey(blk.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if want := pinOfKey(t, parsed.(*ecdsa.PrivateKey)); pin != want {
		t.Errorf("pin %s, but the key's own pin is %s", pin, want)
	}

	// A server with the pair, a client with only the pin.
	kp, err := tls.LoadX509KeyPair(h.AgentCertFile(), h.AgentKeyFile())
	if err != nil {
		t.Fatal(err)
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
	dial := func(pin string) error {
		c, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", ln.Addr().String(), agentapi.ClientTLSConfig(pin))
		if err == nil {
			c.Close()
		}
		return err
	}
	if err := dial(pin); err != nil {
		t.Errorf("a client pinned to the pair's pin cannot connect: %v", err)
	}
	if err := dial(pinOfKey(t, newKey(t))); err == nil {
		t.Error("a client pinned to another key connected: the handshake test does not discriminate")
	}
}

// TestAgentEndpointPairIsKept: a second call, as a second up or an upgrade makes it, leaves both
// files byte for byte as they were, and the pin unchanged. A directory with a looser mode is
// tightened.
func TestAgentEndpointPairIsKept(t *testing.T) {
	h := testHost(t)
	if err := ensureAgentEndpointPair(h, nil); err != nil {
		t.Fatal(err)
	}
	_, pin := loadPair(t, h)
	certBefore, keyBefore := mustRead(t, h.AgentCertFile()), mustRead(t, h.AgentKeyFile())
	dir := filepath.Dir(h.AgentCertFile())
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	var log bytes.Buffer
	if err := ensureAgentEndpointPair(h, &log); err != nil {
		t.Fatal(err)
	}
	if got := mustRead(t, h.AgentCertFile()); !bytes.Equal(got, certBefore) {
		t.Error("the certificate was replaced")
	}
	if got := mustRead(t, h.AgentKeyFile()); !bytes.Equal(got, keyBefore) {
		t.Error("the key was replaced")
	}
	if _, again := loadPair(t, h); again != pin {
		t.Errorf("pin %s, was %s", again, pin)
	}
	if m := mode(t, dir); m != 0o700 {
		t.Errorf("the directory has mode %04o, want 0700", m)
	}
	if log.Len() != 0 {
		t.Errorf("a kept pair was logged as created: %q", log.String())
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestAgentEndpointPairIsRefused: a pair that is not usable makes the call fail with an error that
// names the file, never holds key bytes, and changes nothing on disk.
func TestAgentEndpointPairIsRefused(t *testing.T) {
	cases := []struct {
		name  string
		setup func(t *testing.T, h agentapi.Host)
		want  []string // substrings of the error
	}{
		{"the key does not belong to the certificate", func(t *testing.T, h agentapi.Host) {
			writePair(t, h, newKey(t), newKey(t), 0o600)
		}, []string{"tls.key", "tls.crt"}},
		{"the key is readable by others", func(t *testing.T, h agentapi.Host) {
			k := newKey(t)
			writePair(t, h, k, k, 0o644)
		}, []string{"tls.key", "0644"}},
		{"the key is readable by its group", func(t *testing.T, h agentapi.Host) {
			k := newKey(t)
			writePair(t, h, k, k, 0o640)
		}, []string{"tls.key", "0640"}},
		{"the key is a symbolic link", func(t *testing.T, h agentapi.Host) {
			k := newKey(t)
			writePair(t, h, k, k, 0o600)
			real := h.AgentKeyFile() + ".real"
			if err := os.Rename(h.AgentKeyFile(), real); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(real, h.AgentKeyFile()); err != nil {
				t.Fatal(err)
			}
		}, []string{"tls.key", "not a regular file"}},
		{"the key is not a key", func(t *testing.T, h agentapi.Host) {
			k := newKey(t)
			writePair(t, h, k, k, 0o600)
			if err := os.WriteFile(h.AgentKeyFile(), []byte("not a key\n"), 0o600); err != nil {
				t.Fatal(err)
			}
		}, []string{"tls.key"}},
		{"the certificate is not a certificate", func(t *testing.T, h agentapi.Host) {
			k := newKey(t)
			writePair(t, h, k, k, 0o600)
			if err := os.WriteFile(h.AgentCertFile(), []byte("not a certificate\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}, []string{"tls.crt"}},
		{"the certificate exists and the key does not", func(t *testing.T, h agentapi.Host) {
			k := newKey(t)
			writePair(t, h, k, k, 0o600)
			if err := os.Remove(h.AgentKeyFile()); err != nil {
				t.Fatal(err)
			}
		}, []string{"tls.key", "missing"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := testHost(t)
			c.setup(t, h)
			before := snapshotDir(t, filepath.Dir(h.AgentCertFile()))
			err := ensureAgentEndpointPair(h, nil)
			if err == nil {
				t.Fatal("the pair was accepted")
			}
			for _, w := range c.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q lacks %q", err, w)
				}
			}
			// Nothing of the key may reach the message: not its PEM, nor any 16 characters of its body.
			for _, body := range pemBodies(before) {
				for i := 0; i+16 <= len(body); i += 8 {
					if strings.Contains(err.Error(), body[i:i+16]) {
						t.Fatalf("the error holds key or certificate bytes: %q", err)
					}
				}
			}
			if after := snapshotDir(t, filepath.Dir(h.AgentCertFile())); !sameSnapshot(before, after) {
				t.Errorf("the files changed:\nbefore %v\nafter  %v", names(before), names(after))
			}
		})
	}
}

// TestAgentEndpointCertificateIsIssuedForAKeyAlone: a key without its certificate (a start killed
// between the two writes) gets a certificate, and the pin is the one the key itself gives.
func TestAgentEndpointCertificateIsIssuedForAKeyAlone(t *testing.T) {
	h := testHost(t)
	k := newKey(t)
	writePair(t, h, k, k, 0o600)
	if err := os.Remove(h.AgentCertFile()); err != nil {
		t.Fatal(err)
	}
	keyBefore := mustRead(t, h.AgentKeyFile())
	if err := ensureAgentEndpointPair(h, nil); err != nil {
		t.Fatal(err)
	}
	if _, pin := loadPair(t, h); pin != pinOfKey(t, k) {
		t.Errorf("pin %s, want the key's %s", pin, pinOfKey(t, k))
	}
	if !bytes.Equal(mustRead(t, h.AgentKeyFile()), keyBefore) {
		t.Error("the existing key was replaced")
	}
	if m := mode(t, h.AgentKeyFile()); m != 0o600 {
		t.Errorf("key mode %04o", m)
	}
}

// snapshot is the content and mode of every file in a directory.
type snapshot map[string]string

func snapshotDir(t *testing.T, dir string) snapshot {
	t.Helper()
	s := snapshot{}
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		p := filepath.Join(dir, e.Name())
		fi, err := os.Lstat(p)
		if err != nil {
			t.Fatal(err)
		}
		var content []byte
		if fi.Mode().IsRegular() {
			content = mustRead(t, p)
		}
		s[e.Name()] = fi.Mode().String() + "|" + string(content)
	}
	return s
}

func sameSnapshot(a, b snapshot) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func names(s snapshot) []string {
	var n []string
	for k, v := range s {
		n = append(n, k+" "+v[:strings.IndexByte(v, '|')])
	}
	return n
}

// pemBodies returns the base64 body of every PEM block in the snapshot, with the line breaks
// removed.
func pemBodies(s snapshot) []string {
	var out []string
	for _, v := range s {
		content := v[strings.IndexByte(v, '|')+1:]
		for b := []byte(content); ; {
			var blk *pem.Block
			if blk, b = pem.Decode(b); blk == nil {
				break
			}
			out = append(out, base64.StdEncoding.EncodeToString(blk.Bytes))
		}
	}
	return out
}

// TestStartSupervisorMakesTheEndpointPair: startSupervisor makes the pair before it starts the
// supervisor, so that the controller finds it. A second start keeps it.
func TestStartSupervisorMakesTheEndpointPair(t *testing.T) {
	f := newCtlFixture(t, Front{})
	k3s := startBystander(t, "sleep", "300")
	k3sRec, err := newRec("k3s", k3s.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	f.st.Procs = []ProcRec{k3sRec}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(superviseEnv, f.l.dir)
	stopOnExit(t, f)
	host := f.l.host(f.ports)
	var pairAtStart bool
	afterSupervisorStart = func(ProcRec) {
		_, err := tls.LoadX509KeyPair(host.AgentCertFile(), host.AgentKeyFile())
		pairAtStart = err == nil
	}
	t.Cleanup(func() { afterSupervisorStart = nil })
	if err := startSupervisor(context.Background(), f.l, f.st, []string{self}, 30*time.Second, testLog{t}); err != nil {
		t.Fatal(err)
	}
	if !pairAtStart {
		t.Error("the pair did not load when the supervisor was created")
	}
	_, pin := loadPair(t, host)
	if m := mode(t, host.AgentKeyFile()); m != 0o600 {
		t.Errorf("key mode %04o", m)
	}
	// The same state directory, started again: the same pair.
	if _, err := stopRecorded(context.Background(), f.st, 10*time.Second, nil); err != nil {
		t.Fatal(err)
	}
	f.st.Procs = f.st.Procs[:1]
	if err := startSupervisor(context.Background(), f.l, f.st, []string{self}, 30*time.Second, testLog{t}); err != nil {
		t.Fatal(err)
	}
	if _, again := loadPair(t, host); again != pin {
		t.Errorf("pin %s after the second start, was %s", again, pin)
	}
}

// TestStartSupervisorRefusesABrokenEndpointPair: with a key that others can read, nothing is
// started, and the error names the key.
func TestStartSupervisorRefusesABrokenEndpointPair(t *testing.T) {
	f := newCtlFixture(t, Front{})
	k := newKey(t)
	writePair(t, f.l.host(f.ports), k, k, 0o644)
	t.Setenv(superviseEnv, f.l.dir)
	// If the check fails to stop the start, the detached stand-in must not outlive the test. (Not
	// stopOnExit: the fixture's record of k3s is the test process itself.)
	t.Cleanup(func() {
		if len(f.st.Procs) > 1 && isProc(f.st.Procs[1]) {
			_ = syscall.Kill(f.st.Procs[1].PID, syscall.SIGKILL)
		}
	})
	err := startSupervisor(context.Background(), f.l, f.st, []string{"/bin/sleep", "300"}, 5*time.Second, nil)
	if err == nil || !strings.Contains(err.Error(), "tls.key") || !strings.Contains(err.Error(), "0644") {
		t.Fatalf("startSupervisor = %v, want an error naming tls.key and its mode", err)
	}
	if len(f.st.Procs) != 1 {
		t.Errorf("procs %+v: a supervisor was started", f.st.Procs)
	}
	if _, err := os.Stat(f.l.supervisorLog()); !os.IsNotExist(err) {
		t.Errorf("supervisor.log exists: a supervisor was created (%v)", err)
	}
}

// TestUpRefusesABrokenEndpointPairBeforeStartingAnything: Up checks the pair before it looks for
// a k3s binary, let alone starts one (BinDir is empty here: reaching it would be another error).
func TestUpRefusesABrokenEndpointPairBeforeStartingAnything(t *testing.T) {
	dir := filepath.Join(shortDir(t), "st")
	k := newKey(t)
	writePair(t, agentapi.Host{StateDir: dir}, k, k, 0o644)
	_, err := Up(context.Background(), UpOptions{StateDir: dir, BinDir: t.TempDir(), ControllerBin: fakeControllerBin(t)})
	if err == nil || !strings.Contains(err.Error(), "tls.key") || !strings.Contains(err.Error(), "0644") {
		t.Fatalf("Up = %v, want an error naming tls.key and its mode", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "state.json")); !os.IsNotExist(err) {
		t.Errorf("state.json exists: something was started (%v)", err)
	}
}
