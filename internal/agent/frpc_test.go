package agent_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// testCert is a self-signed certificate, as the launcher makes its own.
type testCert struct {
	pem []byte
	pin string
	tls tls.Certificate
}

func makeCert(t *testing.T, dnsNames ...string) testCert {
	t.Helper()
	return makeCertWith(t, certOpts{names: dnsNames})
}

// certOpts vary makeCertWith's certificate: its names, its key (a new one
// when nil) and its validity (an hour ago to a day ahead when zero).
type certOpts struct {
	names               []string
	key                 *ecdsa.PrivateKey
	notBefore, notAfter time.Time
}

func makeCertWith(t *testing.T, o certOpts) testCert {
	t.Helper()
	key := o.key
	if key == nil {
		var err error
		if key, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader); err != nil {
			t.Fatal(err)
		}
	}
	if o.notBefore.IsZero() {
		o.notBefore = time.Now().Add(-time.Hour)
	}
	if o.notAfter.IsZero() {
		o.notAfter = time.Now().Add(24 * time.Hour)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: "hpcburst launcher (test)"},
		DNSNames:              o.names,
		NotBefore:             o.notBefore,
		NotAfter:              o.notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	// The pin is computed here, independently of agentapi.PinOf: the SHA-256
	// of the DER SubjectPublicKeyInfo (HBEP-7).
	sum := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
	return testCert{
		pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pin: hex.EncodeToString(sum[:]),
		tls: tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: cert},
	}
}

func writeFile(t *testing.T, path string, b []byte) string {
	t.Helper()
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// frpc's configuration is HBEP-6's block (frpc in the job), line for line,
// with S1's heartbeat settings (HBEP-6, Heartbeats). The expected text is
// copied from the HBEP, with its placeholders filled.
func TestFrpcConfigIsHBEP6sBlock(t *testing.T) {
	cfg := agent.FrpcConfig{
		ServerAddr: "127.0.0.1", ServerPort: 41001,
		LogFile: "/tmp/hb-1/frpc.log", Token: hCredential,
		CAFile: "/staging/alloc/hb-example-0123456789ab/dial-home.crt", ServerName: "dial-home.hpcburst.invalid",
		UID: hInstUID, Socket: "/tmp/hb-1/serve.sock",
	}
	got, err := cfg.Render()
	if err != nil {
		t.Fatal(err)
	}
	want := `serverAddr = "127.0.0.1"
serverPort = 41001
loginFailExit = false
log.to = "/tmp/hb-1/frpc.log"
metadatas.token = "` + hCredential + `"
transport.protocol = "wss"
transport.tls.trustedCaFile = "/staging/alloc/hb-example-0123456789ab/dial-home.crt"
transport.tls.serverName = "dial-home.hpcburst.invalid"
transport.poolCount = 0
transport.tcpMuxKeepaliveInterval = 10
transport.heartbeatInterval = 10
transport.heartbeatTimeout = 25
transport.dialServerKeepalive = 30

[[proxies]]
name = "` + hInstUID + `"
type = "tcpmux"
multiplexer = "httpconnect"
customDomains = ["` + hInstUID + `"]
[proxies.plugin]
type = "unix_domain_socket"
unixPath = "/tmp/hb-1/serve.sock"
`
	if string(got) != want {
		t.Fatalf("frpc's configuration:\n%s\nwant HBEP-6's:\n%s", got, want)
	}
	// Each refusal names the field, never the token.
	for name, mut := range map[string]func(*agent.FrpcConfig){
		"no server port":    func(c *agent.FrpcConfig) { c.ServerPort = 0 },
		"a token not a key": func(c *agent.FrpcConfig) { c.Token = "short" },
		"a UID with quotes": func(c *agent.FrpcConfig) { c.UID = `a"b` },
		"a relative socket": func(c *agent.FrpcConfig) { c.Socket = "serve.sock" },
		"no server name":    func(c *agent.FrpcConfig) { c.ServerName = "" },
		"no certificate":    func(c *agent.FrpcConfig) { c.CAFile = "" },
	} {
		c := cfg
		mut(&c)
		_, err := c.Render()
		if err == nil {
			t.Errorf("%s: rendered", name)
		} else if strings.Contains(err.Error(), hCredential) {
			t.Errorf("%s: the refusal holds the token: %v", name, err)
		}
	}
}

// The certificate check: exactly one PEM certificate, whose pin is
// agent.json's. frpc trusts every certificate in its trustedCaFile, so a
// second one, even with the pinned one first, is refused (HBEP-6,
// Certificate checks).
func TestCheckDialHomeCert(t *testing.T) {
	dir := t.TempDir()
	a, b := makeCert(t, "dial-home.hpcburst.invalid"), makeCert(t, "dial-home.hpcburst.invalid")
	good := writeFile(t, filepath.Join(dir, "good.crt"), a.pem)
	if err := agent.CheckDialHomeCert(good, a.pin); err != nil {
		t.Fatalf("the pinned certificate is refused: %v", err)
	}
	keyBlock := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: []byte{1, 2, 3}})
	noName := makeCert(t, "other.hpcburst.invalid")
	expired := makeCertWith(t, certOpts{names: []string{launcherName}, notBefore: time.Now().Add(-48 * time.Hour), notAfter: time.Now().Add(-time.Hour)})
	early := makeCertWith(t, certOpts{names: []string{launcherName}, notBefore: time.Now().Add(time.Hour), notAfter: time.Now().Add(48 * time.Hour)})
	withHeaders := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Headers: map[string]string{"Proc-Type": "4,ENCRYPTED"}, Bytes: a.tls.Certificate[0]})
	for name, tc := range map[string]struct{ path, pin string }{
		// frpc checks the front against agentapi.DialHomeServerName; a
		// certificate without it would leave every frpc retrying forever.
		"no dial-home name":   {writeFile(t, filepath.Join(dir, "noname.crt"), noName.pem), noName.pin},
		"expired":             {writeFile(t, filepath.Join(dir, "expired.crt"), expired.pem), expired.pin},
		"not yet valid":       {writeFile(t, filepath.Join(dir, "early.crt"), early.pem), early.pin},
		"text before the PEM": {writeFile(t, filepath.Join(dir, "pre.crt"), append([]byte("Certificate:\n    Data: ...\n"), a.pem...)), a.pin},
		// frpc's AppendCertsFromPEM skips a block with headers.
		"PEM headers":             {writeFile(t, filepath.Join(dir, "hdr.crt"), withHeaders), a.pin},
		"no file named":           {"", a.pin},
		"a missing file":          {filepath.Join(dir, "absent.crt"), a.pin},
		"no pin":                  {good, ""},
		"another pin":             {good, b.pin},
		"two certificates":        {writeFile(t, filepath.Join(dir, "two.crt"), append(append([]byte{}, a.pem...), b.pem...)), a.pin},
		"a key next to it":        {writeFile(t, filepath.Join(dir, "key.crt"), append(append([]byte{}, a.pem...), keyBlock...)), a.pin},
		"not PEM":                 {writeFile(t, filepath.Join(dir, "der.crt"), a.tls.Certificate[0]), a.pin},
		"PEM with trailing bytes": {writeFile(t, filepath.Join(dir, "tail.crt"), append(append([]byte{}, a.pem...), "junk"...)), a.pin},
	} {
		if err := agent.CheckDialHomeCert(tc.path, tc.pin); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// The proxy variables never reach frpc, in either case; the rest of the
// environment does.
func TestFrpcEnvironmentHasNoProxyVariables(t *testing.T) {
	env := []string{"PATH=/usr/bin", "http_proxy=http://p:1", "HTTP_PROXY=http://p:1", "https_proxy=http://p:1",
		"HTTPS_PROXY=http://p:1", "all_proxy=socks5://p:1", "ALL_PROXY=socks5://p:1", "HOME=/home/u", "http_proxy_note=kept"}
	got := agent.FrpcEnvForTest(env)
	want := []string{"PATH=/usr/bin", "HOME=/home/u", "http_proxy_note=kept"}
	if !slices.Equal(got, want) {
		t.Fatalf("frpc's environment %q, want %q", got, want)
	}
	if got := agent.FrpcEnvForTest(nil); got == nil {
		t.Fatal("a nil environment: exec would pass the agent's whole environment on")
	}
}

// stubFrpc is an frpc that only runs, and its digest.
func stubFrpc(t *testing.T, dir string) (string, string) {
	t.Helper()
	b := []byte("#!/bin/sh\nexec sleep 300\n")
	p := writeFile(t, filepath.Join(dir, "frpc"), b)
	if err := os.Chmod(p, 0o700); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	return p, hex.EncodeToString(sum[:])
}

// The agent refuses to start frpc without the staged certificate, with a
// certificate whose pin is not agent.json's, and with an frpc whose digest is
// not agent.json's (HBEP-6, frpc in the job; Test plan, Certificate). Run
// returns the reason and starts no process. The positive control is the
// same setup with every check passing, which starts frpc.
func TestFrpcRefusesWithoutTheCertificateOrItsPin(t *testing.T) {
	dir := shortTemp(t)
	bin, digest := stubFrpc(t, dir)
	a, b := makeCert(t, "dial-home.hpcburst.invalid"), makeCert(t, "dial-home.hpcburst.invalid")
	certA := writeFile(t, filepath.Join(dir, "dial-home.crt"), a.pem)
	server, err := agent.DirectServer("dial-home.example:443")
	if err != nil {
		t.Fatal(err)
	}
	base := func() *agent.Frpc {
		return &agent.Frpc{
			Binary: bin, FrpcDigest: digest, CertFile: certA, CertSPKISHA256: a.pin,
			Credential: hCredential, RunDir: dir, Socket: filepath.Join(dir, "serve.sock"),
			Server: server, StopGrace: 200 * time.Millisecond,
		}
	}
	for name, mut := range map[string]func(*agent.Frpc){
		"no certificate":      func(f *agent.Frpc) { f.CertFile = "" },
		"another pin":         func(f *agent.Frpc) { f.CertSPKISHA256 = b.pin },
		"no pin":              func(f *agent.Frpc) { f.CertSPKISHA256 = "" },
		"another frpc digest": func(f *agent.Frpc) { f.FrpcDigest = strings.Repeat("0", 64) },
		"no frpc digest":      func(f *agent.Frpc) { f.FrpcDigest = "" },
	} {
		t.Run(name, func(t *testing.T) {
			f := base()
			mut(f)
			var started []agent.ProcessID
			var mu sync.Mutex
			f.OnStart = func(id agent.ProcessID) { mu.Lock(); started = append(started, id); mu.Unlock() }
			f.Serve(hInstUID)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			err := f.Run(ctx)
			mu.Lock()
			defer mu.Unlock()
			for _, id := range started {
				if id.Alive() {
					_ = killID(id)
				}
			}
			if len(started) != 0 {
				t.Fatalf("frpc started %d times", len(started))
			}
			if err == nil || ctx.Err() != nil {
				t.Fatalf("Run returned %v after %v; want a refusal at once", err, ctx.Err())
			}
			if strings.Contains(err.Error(), hCredential) {
				t.Fatalf("the refusal holds the credential: %v", err)
			}
		})
	}
	t.Run("every check passes", func(t *testing.T) {
		f := base()
		startedCh := make(chan agent.ProcessID, 4)
		f.OnStart = func(id agent.ProcessID) { startedCh <- id }
		f.Serve(hInstUID)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- f.Run(ctx) }()
		var id agent.ProcessID
		select {
		case id = <-startedCh:
		case err := <-done:
			t.Fatalf("Run returned %v without starting frpc", err)
		case <-time.After(5 * time.Second):
			t.Fatal("frpc did not start")
		}
		cfg, err := os.Stat(f.ConfigPath())
		if err != nil || cfg.Mode().Perm() != 0o600 {
			t.Errorf("frpc's configuration: %v, mode %v; want mode 600", err, cfg.Mode().Perm())
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("Run: %v", err)
		}
		if id.Alive() {
			t.Fatalf("frpc %+v runs after Run returned", id)
		}
	})
}

// The agent tells its tunnel which Instance to serve: the desired Instance
// while the desired state runs it, none once it idles, and none at the
// agent's end. An unknown action changes nothing.
func TestAgentTellsTheTunnelItsInstance(t *testing.T) {
	rec := &serveRecorder{}
	h := newHarness(t, harnessOptions{tunnel: rec})
	h.set(h.runState("", 1, 0, nil))
	h.waitReport("worker-0 running", 20*time.Second, running)
	rec.wait(t, []string{hInstUID})
	h.set(agentapi.DesiredState{Action: "drain-later"})
	time.Sleep(600 * time.Millisecond)
	rec.wait(t, []string{hInstUID})
	h.set(agentapi.DesiredState{Action: agentapi.ActionIdle})
	rec.wait(t, []string{hInstUID, ""})
}

type serveRecorder struct {
	mu   sync.Mutex
	uids []string
}

func (r *serveRecorder) Serve(uid string) {
	r.mu.Lock()
	r.uids = append(r.uids, uid)
	r.mu.Unlock()
}

func (r *serveRecorder) wait(t *testing.T, want []string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		r.mu.Lock()
		got := append([]string(nil), r.uids...)
		r.mu.Unlock()
		if slices.Equal(got, want) {
			return
		}
		if time.Now().After(deadline) || len(got) > len(want) {
			t.Fatalf("the tunnel was told %q, want %q", got, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// shortTemp is a short temporary directory, so Unix socket paths in it fit.
func shortTemp(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("", "hbt")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return d
}

// killID kills a process recorded by PID and start time, if it still runs.
func killID(id agent.ProcessID) error {
	if !id.Alive() {
		return nil
	}
	return syscall.Kill(id.PID, syscall.SIGKILL)
}
