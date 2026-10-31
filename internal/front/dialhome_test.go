package front

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynfake "k8s.io/client-go/dynamic/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/inventory"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// writeAgentCert puts a self-signed certificate and key where the launcher
// keeps the agent endpoint's (Host.AgentCertFile, Host.AgentKeyFile) and
// returns the certificate.
func writeAgentCert(t *testing.T, host agentapi.Host) *x509.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "hpcburst-agent-endpoint"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), DNSNames: []string{"localhost"}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	kb, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(host.AgentCertFile()), 0o700); err != nil {
		t.Fatal(err)
	}
	writeMode600(t, host.AgentCertFile(), string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})))
	writeMode600(t, host.AgentKeyFile(), string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb})))
	c, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestDialHomeAddrDefaultsInSSHModeOnly(t *testing.T) {
	c := Config{}
	host := agentapi.Host{PortBase: 29220}
	// The ruling: 127.0.0.1:<base+7>, 29227 at the launcher's default base 29220.
	if got, err := c.DialHomeAddr(ModeSSH, host); err != nil || got != "127.0.0.1:29227" {
		t.Errorf("ssh default = %q, %v; want 127.0.0.1:29227", got, err)
	}
	if _, err := c.DialHomeAddr(ModeDirect, host); err == nil || !strings.Contains(err.Error(), "dialHomeListen") {
		t.Errorf("direct mode with no dialHomeListen must be refused, got %v", err)
	}
	c.DialHomeListen = "0.0.0.0:7443"
	for _, mode := range []string{ModeDirect, ModeSSH, ""} {
		if got, _ := c.DialHomeAddr(mode, host); got != "0.0.0.0:7443" {
			t.Errorf("mode %q: an explicit dialHomeListen is used as given, got %q", mode, got)
		}
	}
}

func fakeWatchClient() *dynfake.FakeDynamicClient {
	return dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		inventory.GVRModelDeployments: "ModelDeploymentList", inventory.GVRInstances: "InstanceList",
		inventory.GVRProviders: "ProviderList", inventory.GVRPlatformProfiles: "PlatformProfileList",
	})
}

// runOptions are a Run's inputs over a fake control plane, with the client
// listener in the test block.
func runOptions(t *testing.T, mode string, host agentapi.Host, dialHome string) Options {
	t.Helper()
	dir := t.TempDir()
	keys := filepath.Join(dir, "keys.json")
	writeMode600(t, keys, `{"keys":[{"id":"k","key":"client-key"}]}`)
	cfg := Config{ClientKeysFile: keys, UsageDir: filepath.Join(dir, "usage"), DialHomeListen: dialHome}.WithDefaults()
	cl, err := listenFree()
	if err != nil {
		t.Fatal(err)
	}
	return Options{Config: cfg, Host: host, Mode: mode, Dynamic: fakeWatchClient(), Recorder: &MemoryRecorder{}, Log: discardLog(), ClientListener: cl}
}

func TestRunRefusesDirectModeWithoutDialHomeListen(t *testing.T) {
	host := agentapi.Host{StateDir: t.TempDir(), PortBase: 29220}
	writeAgentCert(t, host)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := Run(ctx, runOptions(t, ModeDirect, host, ""))
	if err == nil || !strings.Contains(err.Error(), "dialHomeListen is required in direct mode") {
		t.Fatalf("Run = %v, want the direct-mode refusal", err)
	}
}

// TestRunListensOnTheSSHDefault: in ssh mode with no dialHomeListen, the
// dial-home listener is 127.0.0.1:<base+7> and terminates TLS with the agent
// endpoint's certificate. Base 29325 puts it on 29332, a spare port of this
// package's block.
func TestRunListensOnTheSSHDefault(t *testing.T) {
	host := agentapi.Host{StateDir: t.TempDir(), PortBase: 29325}
	cert := writeAgentCert(t, host)
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan *Front, 1)
	o := runOptions(t, ModeSSH, host, "")
	o.Ready = func(f *Front) { ready <- f }
	done := make(chan error, 1)
	go func() { done <- Run(ctx, o) }()
	var f *Front
	select {
	case f = <-ready:
	case err := <-done:
		t.Fatalf("Run returned early: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the front did not start")
	}
	defer func() { cancel(); <-done }()
	if got := f.Dial.String(); got != "127.0.0.1:29332" {
		t.Fatalf("dial-home listener on %s, want 127.0.0.1:29332", got)
	}
	conn, err := tls.Dial("tcp", "127.0.0.1:29332", agentapi.ClientTLSConfig(agentapi.PinOf(cert)))
	if err != nil {
		t.Fatalf("a client pinning the agent endpoint's certificate cannot connect: %v", err)
	}
	conn.Close()
}

func TestDialHomeTLSIsRequiredInBothModes(t *testing.T) {
	host := agentapi.Host{StateDir: t.TempDir(), PortBase: 29220}
	for _, mode := range []string{ModeDirect, ModeSSH} {
		if _, err := dialHomeTLS(mode, host); err == nil {
			t.Errorf("mode %s with no certificate: accepted", mode)
		}
	}
	if c, err := dialHomeTLS("", host); err != nil || c != nil {
		t.Errorf("no mode, no certificate: %v, %v; want a plain listener", c, err)
	}
}
