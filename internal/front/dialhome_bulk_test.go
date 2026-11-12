package front

// Tests of the dial-home listener's split by TLS server name, for the bulk path (HBEP-6, The bulk
// path; HBEP-5, Listeners; #448, part 5). Each test serves the production handler (NewDialHome,
// dialHomeTLS, withBulkCertificate) or runs the whole front, and judges the outcome by what a TLS
// client sees (the certificate's bytes, the status) and what each frps stand-in received.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

const bulkName = "bulk.dial-home.hpcburst.invalid" // HBEP-6, The bulk path

// bulkPair writes the launcher's two certificates for one key at the host's paths, as part 2's
// launcher keeps them: the first for localhost and the token's name, the second for the bulk name
// alone. It returns their DER bytes.
func bulkPair(t *testing.T, host agentapi.Host) (first, bulk []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	issue := func(serial int64, names []string) []byte {
		tmpl := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "example launcher"},
			NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), DNSNames: names,
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, BasicConstraintsValid: true}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		return der
	}
	first, bulk = issue(1, []string{"localhost", agentapi.DialHomeServerName}), issue(2, []string{bulkName})
	kb, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(host.AgentCertFile()), 0o700); err != nil {
		t.Fatal(err)
	}
	writeMode600(t, host.AgentCertFile(), string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: first})))
	writeMode600(t, host.BulkCertFile(), string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: bulk})))
	writeMode600(t, host.AgentKeyFile(), string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb})))
	return first, bulk
}

// serveSplit serves the dial-home handler as listen builds it, in mode, with frpsFirst and
// frpsBulk as the two frps's bind ports, and the bulk path on or off. It returns the address.
func serveSplit(t *testing.T, mode string, host agentapi.Host, frpsFirst, frpsBulk string, bulkOn bool) string {
	t.Helper()
	first := newFrpPassthrough(frpsFirst, 5*time.Second, discardLog())
	dh, err := NewDialHome(mode, host, first)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := dialHomeTLS(mode, host)
	if err != nil || cfg == nil {
		t.Fatalf("dialHomeTLS: %v, %v", cfg, err)
	}
	var bulk *frpPassthrough
	if bulkOn {
		bulk = newFrpPassthrough(frpsBulk, 5*time.Second, discardLog())
		dh.withBulk(bulk)
		if cfg, err = withBulkCertificate(cfg, host); err != nil {
			t.Fatal(err)
		}
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: dh, ReadHeaderTimeout: time.Minute}
	go func() { _ = srv.Serve(tls.NewListener(ln, cfg)) }()
	t.Cleanup(func() {
		_ = srv.Close()
		first.Close()
		if bulk != nil {
			bulk.Close()
		}
	})
	return ln.Addr().String()
}

// peerCert dials addr naming serverName ("" sends no server name), verifying nothing, and returns
// the certificate the listener presented.
func peerCert(t *testing.T, addr, serverName string) []byte {
	t.Helper()
	c, err := tls.Dial("tcp", addr, &tls.Config{ServerName: serverName, InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}) //nolint:gosec // the test reads the certificate the server chose
	if err != nil {
		t.Fatalf("TLS to %s as %q: %v", addr, serverName, err)
	}
	defer c.Close()
	return c.ConnectionState().PeerCertificates[0].Raw
}

// sendAs sends raw, a request, over TLS to addr naming serverName, and returns the status line's
// code. The client trusts nothing: the listener's choice of certificate is TestBulkName*'s subject.
func sendAs(t *testing.T, addr, serverName, raw string) int {
	t.Helper()
	c, err := tls.Dial("tcp", addr, &tls.Config{ServerName: serverName, InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}) //nolint:gosec // as above
	if err != nil {
		t.Fatalf("TLS to %s as %q: %v", addr, serverName, err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(15 * time.Second))
	if _, err := io.WriteString(c, raw); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatalf("reading the answer as %q: %v", serverName, err)
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

// switching is the frps stand-in's answer to the upgrade. The stand-in (startRawUpstream) answers
// once it has read the five bytes the tests send after the headers, "EARLY".
const switching = "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n"

// TestBulkNameGetsTheSecondCertificate: with the bulk path on, the listener presents the second
// certificate to a client that names the bulk server, and the first to a client that names the
// token's server or none (HBEP-6, The bulk path: GetCertificate). A client that trusts only the
// second, as the bulk frpc does, verifies it for the bulk name.
func TestBulkNameGetsTheSecondCertificate(t *testing.T) {
	host := agentapi.Host{StateDir: t.TempDir()}
	first, bulk := bulkPair(t, host)
	addr := serveSplit(t, ModeHostDialed, host, "127.0.0.1:1", "127.0.0.1:1", true)
	for name, want := range map[string][]byte{bulkName: bulk, agentapi.DialHomeServerName: first, "": first, "other.example.test": first} {
		if got := peerCert(t, addr, name); !bytes.Equal(got, want) {
			t.Errorf("as %q the listener presented the %s certificate", name, map[bool]string{true: "first", false: "second"}[bytes.Equal(got, first)])
		}
	}
	pool := x509.NewCertPool()
	c, err := x509.ParseCertificate(bulk)
	if err != nil {
		t.Fatal(err)
	}
	pool.AddCert(c)
	conn, err := tls.Dial("tcp", addr, &tls.Config{RootCAs: pool, ServerName: bulkName, MinVersion: tls.VersionTLS12})
	if err != nil {
		t.Fatalf("a client trusting only the second certificate, as the bulk frpc does: %v", err)
	}
	conn.Close()
}

// TestFrpUpgradeReachesItsOwnFrps: frpc's websocket upgrade under the bulk name reaches the second
// frps's bind port, and under the token's name, or none, the first's; neither stand-in hears the
// other's (HBEP-6, The bulk path: "/~!frp under each name reaches its own frps"). In both modes the
// bulk path is built for.
func TestFrpUpgradeReachesItsOwnFrps(t *testing.T) {
	for _, mode := range []string{ModeDirect, ModeHostDialed} {
		t.Run(mode, func(t *testing.T) {
			host := agentapi.Host{StateDir: t.TempDir(), PortBase: kernelPortBase(t)}
			bulkPair(t, host)
			upFirst, upBulk := startRawUpstream(t, switching), startRawUpstream(t, switching)
			addr := serveSplit(t, mode, host, upFirst.ln.Addr().String(), upBulk.ln.Addr().String(), true)
			for _, c := range []struct {
				name      string
				wantFirst bool
			}{{bulkName, false}, {agentapi.DialHomeServerName, true}, {"", true}} {
				nFirst, nBulk := len(upFirst.received()), len(upBulk.received())
				if code := sendAs(t, addr, c.name, frpUpgrade+"EARLY"); code != http.StatusSwitchingProtocols {
					t.Fatalf("the upgrade as %q answered %d", c.name, code)
				}
				waitUntil(t, "the upgrade to reach an frps", func() bool {
					return len(upFirst.received())+len(upBulk.received()) == nFirst+nBulk+1
				})
				gotFirst := len(upFirst.received()) == nFirst+1
				if gotFirst != c.wantFirst {
					t.Errorf("as %q the upgrade reached the %s frps", c.name, map[bool]string{true: "first", false: "second"}[gotFirst])
				}
			}
			for _, r := range append(upFirst.received(), upBulk.received()...) {
				if !strings.HasPrefix(r, "GET /~!frp HTTP/1.1\r\n") {
					t.Errorf("an frps got %q, not the upgrade's request line unchanged", r)
				}
			}
		})
	}
}

// waitUntil waits on observable state, for at most 10 s.
func waitUntil(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for end := time.Now().Add(10 * time.Second); !ok(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// agentRegister is an /agent/v1/ request, as the agent's control client sends it in direct mode.
const agentRegister = "POST /agent/v1/register HTTP/1.1\r\nHost: front.example.test\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}"

// directHost is a host whose agent endpoint, base+3, is a port the kernel just gave out, with the
// two certificates and a stand-in endpoint there (ptStandIn), for direct mode's /agent/v1/ passthrough.
func directHost(t *testing.T) (agentapi.Host, *ptEndpoint) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	base := ln.Addr().(*net.TCPAddr).Port - agentapi.AgentEndpointPortOffset
	ln.Close()
	host := agentapi.Host{StateDir: t.TempDir(), PortBase: base}
	bulkPair(t, host)
	return host, ptStandIn(t, host)
}

// TestAgentPathRefusedUnderTheBulkName (#448's trap: /agent/v1/ refused only under the bulk name):
// in direct mode, where the dial-home listener passes /agent/v1/ to the agent endpoint, a request
// under the bulk name gets 404 and the endpoint never sees it. TestAgentPathServedUnderTheTokenNameAndNone
// is its control: the same request, under the token's name or none, reaches the endpoint. In ssh and
// host-dialed mode /agent/v1/ is 404 under every name already, so only direct mode can tell.
func TestAgentPathRefusedUnderTheBulkName(t *testing.T) {
	host, ep := directHost(t)
	addr := serveSplit(t, ModeDirect, host, "127.0.0.1:1", "127.0.0.1:1", true)
	if code := sendAs(t, addr, bulkName, agentRegister); code != http.StatusNotFound {
		t.Errorf("/agent/v1/register under the bulk name answered %d, want 404", code)
	}
	n := len(ep.requests())
	if n != 0 {
		t.Errorf("the agent endpoint received %d requests made under the bulk name", n)
	}
}

func TestAgentPathServedUnderTheTokenNameAndNone(t *testing.T) {
	host, ep := directHost(t)
	addr := serveSplit(t, ModeDirect, host, "127.0.0.1:1", "127.0.0.1:1", true)
	for _, name := range []string{agentapi.DialHomeServerName, ""} {
		if code := sendAs(t, addr, name, agentRegister); code != http.StatusCreated {
			t.Errorf("/agent/v1/register as %q answered %d, want the endpoint's 201", name, code)
		}
	}
	n := len(ep.requests())
	if n != 2 {
		t.Errorf("the agent endpoint received %d requests, want 2", n)
	}
}

// TestOtherPathsUnderTheBulkName404: under the bulk name everything but frpc's upgrade gets 404,
// and the second frps hears none of it (HBEP-5, Listeners: every other path gets 404).
func TestOtherPathsUnderTheBulkName404(t *testing.T) {
	host := agentapi.Host{StateDir: t.TempDir()}
	bulkPair(t, host)
	upBulk := startRawUpstream(t, switching)
	addr := serveSplit(t, ModeHostDialed, host, "127.0.0.1:1", upBulk.ln.Addr().String(), true)
	for _, raw := range []string{
		"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
		"POST /v1/chat/completions HTTP/1.1\r\nHost: x\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
		"GET /agent/v1/desired HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
		"GET /~!frp HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n", // no Upgrade
		"GET /~!frp/x HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n",
	} {
		if code := sendAs(t, addr, bulkName, raw); code != http.StatusNotFound {
			t.Errorf("%q under the bulk name answered %d, want 404", strings.SplitN(raw, "\r\n", 2)[0], code)
		}
	}
	if got := upBulk.received(); len(got) != 0 {
		t.Errorf("the second frps heard %q", got)
	}
}

// TestBulkOffDialHomeIsUnchanged: without the bulk path the listener's TLS has no GetCertificate,
// and a client naming the bulk server is served as today: it gets the first certificate, its
// upgrade reaches the first frps, and in direct mode its /agent/v1/ reaches the agent endpoint. The
// control: the same requests with the bulk path on get the second certificate, reach the second
// frps, and get 404.
func TestBulkOffDialHomeIsUnchanged(t *testing.T) {
	host, ep := directHost(t)
	cfg, err := dialHomeTLS(ModeDirect, host)
	if err != nil || cfg.GetCertificate != nil {
		t.Fatalf("dialHomeTLS: GetCertificate set %v, %v", cfg != nil && cfg.GetCertificate != nil, err)
	}
	first, bulk := func() ([]byte, []byte) {
		f, _ := tls.LoadX509KeyPair(host.AgentCertFile(), host.AgentKeyFile())
		b, _ := tls.LoadX509KeyPair(host.BulkCertFile(), host.AgentKeyFile())
		return f.Certificate[0], b.Certificate[0]
	}()
	for _, on := range []bool{false, true} {
		upFirst, upBulk := startRawUpstream(t, switching), startRawUpstream(t, switching)
		addr := serveSplit(t, ModeDirect, host, upFirst.ln.Addr().String(), upBulk.ln.Addr().String(), on)
		wantCert, wantAgent := first, http.StatusCreated
		if on {
			wantCert, wantAgent = bulk, http.StatusNotFound
		}
		if got := peerCert(t, addr, bulkName); !bytes.Equal(got, wantCert) {
			t.Errorf("bulk path %v: the certificate under the bulk name is not the expected one", on)
		}
		if code := sendAs(t, addr, bulkName, frpUpgrade+"EARLY"); code != http.StatusSwitchingProtocols {
			t.Fatalf("bulk path %v: the upgrade answered %d", on, code)
		}
		waitUntil(t, "the upgrade to reach an frps", func() bool { return len(upFirst.received())+len(upBulk.received()) == 1 })
		if gotFirst := len(upFirst.received()) == 1; gotFirst == on {
			t.Errorf("bulk path %v: the upgrade under the bulk name reached the %s frps", on, map[bool]string{true: "first", false: "second"}[gotFirst])
		}
		if code := sendAs(t, addr, bulkName, agentRegister); code != wantAgent {
			t.Errorf("bulk path %v: /agent/v1/ under the bulk name answered %d, want %d", on, code, wantAgent)
		}
	}
	n := len(ep.requests())
	if n != 1 {
		t.Errorf("the agent endpoint received %d requests, want the bulk-off one only", n)
	}
}

// TestWithBulkCertificateRefusesWhatItCannotServe: the second certificate must exist, load with
// the agent endpoint's key and name the bulk server, or the front does not start; and the bulk path
// needs TLS (a recorded mode).
func TestWithBulkCertificateRefusesWhatItCannotServe(t *testing.T) {
	host := agentapi.Host{StateDir: t.TempDir()}
	first, _ := bulkPair(t, host)
	cfg, err := dialHomeTLS(ModeHostDialed, host)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := withBulkCertificate(nil, host); err == nil {
		t.Error("no TLS configuration was accepted")
	}
	writeMode600(t, host.BulkCertFile(), string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: first})))
	if _, err := withBulkCertificate(cfg, host); err == nil || !strings.Contains(err.Error(), "does not name "+bulkName) {
		t.Errorf("the first certificate as the second: %v, want a refusal naming the bulk server", err)
	}
	other := agentapi.Host{StateDir: t.TempDir()}
	bulkPair(t, other)
	if err := os.Rename(other.BulkCertFile(), host.BulkCertFile()); err != nil {
		t.Fatal(err)
	}
	if _, err := withBulkCertificate(cfg, host); err == nil {
		t.Error("a second certificate for another key was accepted")
	}
	if err := os.Remove(host.BulkCertFile()); err != nil {
		t.Fatal(err)
	}
	if _, err := withBulkCertificate(cfg, host); err == nil {
		t.Error("a missing second certificate was accepted")
	}
}

// TestRunServesTheBulkSplit: the whole front, with Options.BulkPath, splits its dial-home listener
// by server name (listen's wiring), and refuses the bulk path in ssh mode, with no mode, and
// without the second certificate.
func TestRunServesTheBulkSplit(t *testing.T) {
	host := agentapi.Host{StateDir: t.TempDir(), PortBase: kernelPortBase(t)}
	bulkPair(t, host)
	upFirst, upBulk := startRawUpstream(t, switching), startRawUpstream(t, switching)
	f, _ := runProbeFront(t, startMuxDouble(t), func(o *Options) {
		o.Mode, o.Host, o.BulkPath = ModeHostDialed, host, true
		o.Config.FrpsBindAddr, o.Config.FrpsBulkBindAddr = upFirst.ln.Addr().String(), upBulk.ln.Addr().String()
	})
	for _, c := range []struct {
		name      string
		wantFirst bool
	}{{bulkName, false}, {agentapi.DialHomeServerName, true}} {
		nFirst, nBulk := len(upFirst.received()), len(upBulk.received())
		if code := sendAs(t, f.Dial.String(), c.name, frpUpgrade+"EARLY"); code != http.StatusSwitchingProtocols {
			t.Fatalf("the upgrade as %q answered %d", c.name, code)
		}
		waitUntil(t, "the upgrade to reach an frps", func() bool {
			return len(upFirst.received())+len(upBulk.received()) == nFirst+nBulk+1
		})
		if gotFirst := len(upFirst.received()) == nFirst+1; gotFirst != c.wantFirst {
			t.Errorf("as %q the front's upgrade reached the %s frps", c.name, map[bool]string{true: "first", false: "second"}[gotFirst])
		}
	}

	for _, c := range []struct {
		name, mode string
		edit       func(agentapi.Host)
		want       string
	}{
		{"ssh mode", ModeSSH, nil, "direct and host-dialed mode only"},
		{"no mode", "", nil, "needs a dial-home mode"},
		{"no second certificate", ModeHostDialed, func(h agentapi.Host) { _ = os.Remove(h.BulkCertFile()) }, "second certificate"},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := agentapi.Host{StateDir: t.TempDir(), PortBase: kernelPortBase(t)}
			bulkPair(t, h)
			if c.edit != nil {
				c.edit(h)
			}
			o := runOptions(t, c.mode, h, "")
			o.BulkPath = true
			o.DialHomeListener = kernelListener(t)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := Run(ctx, o); err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("Run with the bulk path, %s: %v, want an error naming %q", c.name, err, c.want)
			}
		})
	}
}
