package launcher

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// writeAdmin overwrites the fake host's admin kubeconfig the way k3s writes it: in place, not
// atomically.
func writeAdmin(t *testing.T, h *fakeHost, content string) {
	t.Helper()
	if err := os.WriteFile(h.l.adminKubeconfig(), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// useWaitReadyHook installs hook as waitReadyHook for the test.
func useWaitReadyHook(t *testing.T, hook func(last error)) {
	t.Helper()
	waitReadyHook = hook
	t.Cleanup(func() { waitReadyHook = nil })
}

// testClientCert returns a self-signed client certificate and its key as PEM, the form k3s writes
// into the admin kubeconfig.
func testClientCert(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "system:admin", Organization: []string{"system:masters"}},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

// certKubeconfig is fakeKubeconfig with the user's credential a client certificate and key, as
// k3s writes it, in place of the token.
func certKubeconfig(t *testing.T, server string) *clientcmdapi.Config {
	t.Helper()
	cfg := fakeKubeconfig(server)
	cfg.AuthInfos["default"].Token = ""
	cfg.AuthInfos["default"].ClientCertificateData, cfg.AuthInfos["default"].ClientKeyData = testClientCert(t)
	return cfg
}

// authenticates reports whether cfg can present a credential: a token, or a client certificate
// and key that load as a pair.
func authenticates(cfg *rest.Config) bool {
	if cfg.BearerToken != "" {
		return true
	}
	_, err := tls.X509KeyPair(cfg.CertData, cfg.KeyData)
	return err == nil
}

// requireClientcmdOrder fails unless text has the field order that makes a cut kubeconfig
// dangerous: a cluster's server before its name, all of that before the contexts and the
// current-context, and the users after them. That is the order of clientcmd.Write, which k3s
// uses, and a fixture that put "name" first would hide the cut file that parses as an unnamed
// cluster.
func requireClientcmdOrder(t *testing.T, text string) {
	t.Helper()
	server, name := strings.Index(text, "server:"), strings.Index(text, "name:")
	contexts, current := strings.Index(text, "contexts:"), strings.Index(text, "current-context:")
	users := strings.Index(text, "\nusers:")
	if server < 0 || name < 0 || contexts < 0 || current < 0 || users < 0 || server >= name || name >= contexts || contexts >= current || current >= users {
		t.Fatalf("the fixture is not in clientcmd's field order (server %d, name %d, contexts %d, current-context %d, users %d):\n%s", server, name, contexts, current, users, text)
	}
}

// TestWaitReadyWaitsForAWholeAdminKubeconfig: k3s writes its admin kubeconfig early in its start
// and not atomically, so waitReady can find the file empty or cut short. That is "not ready yet",
// not a failure. The values that must hold come from the file k3s would write once done, which
// the fake host holds as the launcher's own kubeconfig (the same text, written by newFakeHost).
//
// The file changes only inside waitReadyHook, which runs after each round that failed: so the test
// publishes the whole file only after two rounds have failed on the incomplete one, and nothing in
// it depends on how long a round takes.
func TestWaitReadyWaitsForAWholeAdminKubeconfig(t *testing.T) {
	validOf := func(t *testing.T, h *fakeHost) string {
		t.Helper()
		b, err := os.ReadFile(h.l.kubeconfig())
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}

	// Each incomplete state must make the launcher wait. Five are refused by restConfig before any
	// request: empty, cut before the contexts, cut before the current-context, cut inside the
	// server address (the case where clientcmd's field order matters: the file then holds a cluster
	// with no name yet, which client-go uses although no context selects it, and the address reads
	// as another server's), and cut inside the users, where the user has no credential yet (the
	// fake apiserver, like a real one, answers /readyz to a request with none, so a config
	// without credentials would pass it). The last parses and resolves: the user's certificate and
	// key are PEM blocks, but not a certificate and key. restConfig accepts it and only the first
	// use of the config fails, so the file must be read again after /readyz failed, not kept from
	// the first parse.
	for _, tc := range []struct {
		name    string
		partial func(t *testing.T, h *fakeHost, valid string) string
	}{
		{"empty", func(*testing.T, *fakeHost, string) string { return "" }},
		{"cut before the contexts", func(_ *testing.T, _ *fakeHost, v string) string { return v[:strings.Index(v, "contexts:")] }},
		{"cut before the current-context", func(_ *testing.T, _ *fakeHost, v string) string { return v[:strings.Index(v, "current-context:")] }},
		{"cut inside the server address, before the cluster has a name", func(t *testing.T, _ *fakeHost, v string) string {
			requireClientcmdOrder(t, v)
			cut := v[:strings.Index(v, "127.0.0.1:")+len("127.0.0.1:")+1]
			if !strings.Contains(cut, "server: https://127.0.0.1:") || strings.Contains(cut, "name:") {
				t.Fatalf("the cut is not inside the first cluster's server address:\n%s", cut)
			}
			return cut
		}},
		{"cut inside the users, before the user has a credential", func(t *testing.T, _ *fakeHost, v string) string {
			requireClientcmdOrder(t, v)
			cut := v[:strings.Index(v, "token:")]
			if !strings.Contains(cut, "\nusers:") || !strings.Contains(cut, "name: default") || strings.Contains(cut, "token") {
				t.Fatalf("the cut is not inside the user, before its credential:\n%s", cut)
			}
			return cut
		}},
		{"parses, but its certificate and key are not a pair", func(t *testing.T, h *fakeHost, _ string) string {
			cfg := fakeKubeconfig("https://127.0.0.1:" + strconv.Itoa(h.st.Ports.HTTPS))
			// PEM blocks around "notpem" in base64: they pass for PEM and fail when loaded as a pair.
			cfg.AuthInfos["default"].Token = ""
			cfg.AuthInfos["default"].ClientCertificateData = []byte("-----BEGIN CERTIFICATE-----\nbm90cGVt\n-----END CERTIFICATE-----\n")
			cfg.AuthInfos["default"].ClientKeyData = []byte("-----BEGIN EC PRIVATE KEY-----\nbm90cGVt\n-----END EC PRIVATE KEY-----\n")
			return kubeconfigText(t, cfg)
		}},
	} {
		t.Run("fills in after two failed rounds, "+tc.name, func(t *testing.T) {
			h := newFakeHost(t)
			valid := validOf(t, h)
			partial := tc.partial(t, h, valid)
			writeAdmin(t, h, partial)
			var failed []error
			published := false
			useWaitReadyHook(t, func(last error) {
				if last == nil {
					return // a round that got a step further (the whole file's /readyz), not a failure
				}
				failed = append(failed, last)
				switch len(failed) {
				case 1:
					writeAdmin(t, h, partial) // k3s writes the file again, still incomplete
				case 2:
					writeAdmin(t, h, valid)
					published = true
				}
			})
			_, _, err := waitReady(context.Background(), h.l, h.st, h.handle(), 10*time.Second)
			if err != nil {
				t.Fatalf("a kubeconfig that was only briefly incomplete: %v", err)
			}
			if !published {
				t.Fatalf("ready before the whole file was published (%d failed rounds): the launcher read a partial file as good", len(failed))
			}
		})
	}

	t.Run("never becomes valid, the wait ends at the deadline with the reason", func(t *testing.T) {
		h := newFakeHost(t)
		writeAdmin(t, h, "")
		rounds := 0
		useWaitReadyHook(t, func(error) { rounds++ })
		const timeout = 600 * time.Millisecond
		t0 := time.Now()
		_, _, err := waitReady(context.Background(), h.l, h.st, h.handle(), timeout)
		d := time.Since(t0)
		if err == nil {
			t.Fatal("an empty kubeconfig counted as ready")
		}
		for _, want := range []string{"not ready after", h.l.adminKubeconfig(), "no current-context"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q lacks %q", err, want)
			}
		}
		// The loop cannot end before its deadline, which is set after t0; this guards against an
		// early return that skips the wait.
		if d < timeout {
			t.Errorf("gave up after %s, before the deadline %s", d, timeout)
		}
		if rounds == 0 {
			t.Error("the file was never read again")
		}
	})

	t.Run("a whole kubeconfig naming another server fails at once", func(t *testing.T) {
		h := newFakeHost(t)
		other := kubeconfigText(t, fakeKubeconfig("https://127.0.0.1:6443"))
		writeAdmin(t, h, other)
		rounds := 0
		useWaitReadyHook(t, func(error) { rounds++ })
		// The timeout is long only so that a wait would show: this file will not change, so the
		// refusal must come out of the first round, with no round failed before it.
		_, _, err := waitReady(context.Background(), h.l, h.st, h.handle(), 5*time.Second)
		var wrong *wrongServerError
		if !errors.As(err, &wrong) || !strings.Contains(err.Error(), "127.0.0.1:6443") || !strings.Contains(err.Error(), "not at this launcher's") {
			t.Fatalf("got %v; want a wrongServerError naming the other server", err)
		}
		if rounds != 0 {
			t.Errorf("%d round(s) failed before the refusal; want it at once", rounds)
		}
	})
}

// TestRestConfigNeverCallsACutKubeconfigAWrongServer cuts the serialised kubeconfig at every byte.
// A file cut inside the first cluster's server address names an address that is not this
// launcher's, and client-go builds a config from it all the same (the cluster has no name yet).
// That is not a refusal to make: the address is not final until the file resolves, which is
// when current-context is whole, and until then restConfig must give an ordinary error. A file cut
// inside the users resolves to a config with no credential, or a cut one; restConfig must give
// no config at all for it, because /readyz answers a request that has none.
func TestRestConfigNeverCallsACutKubeconfigAWrongServer(t *testing.T) {
	ports := Ports{API: 29220, HTTPS: 29221, KCM: 29222, LB: 29225}
	own := "https://127.0.0.1:" + strconv.Itoa(ports.HTTPS)
	other := "https://127.0.0.1:6443"
	fixtures := map[string]func(t *testing.T, server string) *clientcmdapi.Config{
		"token":       func(_ *testing.T, server string) *clientcmdapi.Config { return fakeKubeconfig(server) },
		"certificate": certKubeconfig,
	}
	for _, tc := range []struct {
		name, server string
		wantWrong    bool // for the whole file
	}{
		{"this launcher's server", own, false},
		{"another server", other, true},
	} {
		for credName, mk := range fixtures {
			t.Run(tc.name+", user with a "+credName, func(t *testing.T) {
				full := kubeconfigText(t, mk(t, tc.server))
				requireClientcmdOrder(t, full)
				// Resolved once the current-context is whole: the clusters are then final, since
				// they come before it. The users, last in the file, are cut further on.
				resolvedAt := strings.Index(full, "current-context: default") + len("current-context: default")
				path := filepath.Join(t.TempDir(), "k3s-admin.yaml")
				cutUsers, rejectedInUsers := 0, 0
				for n := 0; n < len(full); n++ {
					if err := os.WriteFile(path, []byte(full[:n]), 0o600); err != nil {
						t.Fatal(err)
					}
					cfg, err := restConfig(path, ports, time.Second)
					var wrong *wrongServerError
					// Only a file that resolves can be refused for its address, and only when it names
					// another server.
					if errors.As(err, &wrong) && (n < resolvedAt || !tc.wantWrong) {
						t.Fatalf("the first %d bytes were refused as a wrong server (%v); a cut file is not ready yet:\n%s", n, err, full[:n])
					}
					if n < resolvedAt && err == nil {
						t.Fatalf("the first %d bytes were accepted, before the current-context is whole:\n%s", n, full[:n])
					}
					if err == nil && !authenticates(cfg) {
						t.Fatalf("the first %d bytes gave a config that cannot authenticate:\n%s", n, full[:n])
					}
					if n >= resolvedAt {
						cutUsers++
						if err != nil {
							rejectedInUsers++
						}
					}
				}
				// The cuts inside the users must include some that are refused, or the check
				// above passed because nothing was cut there.
				if cutUsers == 0 || rejectedInUsers == 0 {
					t.Errorf("%d of %d cuts inside the users were refused; the sweep did not reach an unfinished user", rejectedInUsers, cutUsers)
				}
				if err := os.WriteFile(path, []byte(full), 0o600); err != nil {
					t.Fatal(err)
				}
				cfg, err := restConfig(path, ports, time.Second)
				var wrong *wrongServerError
				switch {
				case tc.wantWrong && !errors.As(err, &wrong):
					t.Errorf("the whole file naming %s: got %v; want a wrongServerError", tc.server, err)
				case !tc.wantWrong && err != nil:
					t.Errorf("the whole file naming this launcher's server: %v", err)
				case !tc.wantWrong && !authenticates(cfg):
					t.Error("the whole file gave a config that cannot authenticate")
				}
			})
		}
	}
}

// TestResolvedNeedsAUserWithACredential: a kubeconfig resolves only when the current context's
// user is in the file and carries a complete credential: a token, or a certificate and key that
// both parse as PEM.
func TestResolvedNeedsAUserWithACredential(t *testing.T) {
	certPEM, keyPEM := testClientCert(t)
	with := func(edit func(u *clientcmdapi.AuthInfo)) *clientcmdapi.Config {
		cfg := fakeKubeconfig("https://127.0.0.1:1")
		cfg.AuthInfos["default"].Token = ""
		edit(cfg.AuthInfos["default"])
		return cfg
	}
	for _, tc := range []struct {
		name string
		cfg  *clientcmdapi.Config
		ok   bool
	}{
		{"a token", with(func(u *clientcmdapi.AuthInfo) { u.Token = "t" }), true},
		{"a certificate and key", with(func(u *clientcmdapi.AuthInfo) { u.ClientCertificateData, u.ClientKeyData = certPEM, keyPEM }), true},
		{"no credential", with(func(*clientcmdapi.AuthInfo) {}), false},
		{"a certificate without its key", with(func(u *clientcmdapi.AuthInfo) { u.ClientCertificateData = certPEM }), false},
		{"a key without its certificate", with(func(u *clientcmdapi.AuthInfo) { u.ClientKeyData = keyPEM }), false},
		{"a certificate and key that are not PEM", with(func(u *clientcmdapi.AuthInfo) {
			u.ClientCertificateData, u.ClientKeyData = []byte("notpem"), []byte("notpem")
		}), false},
		{"a certificate cut short", with(func(u *clientcmdapi.AuthInfo) {
			u.ClientCertificateData, u.ClientKeyData = certPEM[:len(certPEM)/2], keyPEM
		}), false},
		{"a user the file lacks", func() *clientcmdapi.Config {
			cfg := fakeKubeconfig("https://127.0.0.1:1")
			delete(cfg.AuthInfos, "default")
			return cfg
		}(), false},
		{"a context with no user", func() *clientcmdapi.Config {
			cfg := fakeKubeconfig("https://127.0.0.1:1")
			cfg.Contexts["default"].AuthInfo = ""
			return cfg
		}(), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := resolved(tc.cfg); (err == nil) != tc.ok {
				t.Errorf("resolved = %v, want ok=%v", err, tc.ok)
			}
		})
	}
}

// TestWaitReadyDoesNotAnswerAfterItsDeadline: the timeout is a deadline for the whole wait. The
// controller manager answers 200, but only after the deadline; a probe that starts shortly before
// the deadline used to get its own fresh 1.5 s, and so reported ready late. The fake answers a
// fixed 1.2 s after it is asked, which is after the 1 s deadline whenever the probe began, and
// within a fresh 1.5 s whenever the probe began.
func TestWaitReadyDoesNotAnswerAfterItsDeadline(t *testing.T) {
	h := newFakeHost(t)
	var asked atomic.Bool
	h.onKCM = func() {
		asked.Store(true)
		time.Sleep(1200 * time.Millisecond)
	}
	_, _, err := waitReady(context.Background(), h.l, h.st, h.handle(), time.Second)
	if !asked.Load() {
		t.Fatal("the controller manager was never asked: the probe this test is about did not run")
	}
	if err == nil {
		t.Fatal("ready on an answer that came after the deadline")
	}
	if !strings.Contains(err.Error(), "controller manager /healthz not ready after 1s") {
		t.Errorf("got %v; want a timeout in the controller manager stage", err)
	}
}

// TestWaitReadyDeadlineHoldsWhenARoundIsSlowBeforeItsRequest: a round fixes its probe timeout when
// it begins, but sends its request only after it has checked who holds the port, and a slow check
// moves the request later, past where a timeout counted from the round's start would end. The
// fake slows that check by 0.8 s in the controller manager's round, then answers 0.3 s after it
// is asked: the answer comes after the 1 s deadline whenever the round began, and well within
// the probe's own timeout (about 0.95 s from the request).
func TestWaitReadyDeadlineHoldsWhenARoundIsSlowBeforeItsRequest(t *testing.T) {
	h := newFakeHost(t)
	var asked atomic.Bool
	h.onKCM = func() {
		asked.Store(true)
		time.Sleep(300 * time.Millisecond)
	}
	// The hook runs after /readyz has answered, so the next round is the controller manager's. Its
	// first exited check is the round's own verify, before the probe timeout is fixed; the second is
	// the port check, after it.
	slow := false
	checks := 0
	useWaitReadyHook(t, func(error) { slow = true })
	h.f.onExited = func() {
		if !slow {
			return
		}
		if checks++; checks == 2 {
			time.Sleep(800 * time.Millisecond)
		}
	}
	_, _, err := waitReady(context.Background(), h.l, h.st, h.handle(), time.Second)
	if !asked.Load() {
		t.Fatal("the controller manager was never asked: the round this test is about did not run")
	}
	if err == nil {
		t.Fatal("ready on an answer that came after the deadline")
	}
	if !strings.Contains(err.Error(), "controller manager /healthz not ready after 1s") {
		t.Errorf("got %v; want a timeout in the controller manager stage", err)
	}
}
