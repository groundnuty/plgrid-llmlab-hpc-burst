package front

import (
	"context"
	"net"
	"net/http"
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

// TestRunTunnelInBothDialHomeModes is HBEP-6's wiring test for the front, in
// direct and in ssh mode: Run builds its own Tunnel and probe. An Instance's
// frpc dials the front's dial-home listener over wss, which terminates TLS
// with the listener's certificate and passes GET /~!frp to frps, and frps
// lists the proxy online. The probe registers the transport, and a client's
// request through the front reaches the engine with the Instance's key. When
// the front stops it cuts the tunnels it passed through, and frps drops the
// proxy.
func TestRunTunnelInBothDialHomeModes(t *testing.T) {
	for _, mode := range []string{ModeDirect, ModeSSH} {
		t.Run(mode, func(t *testing.T) {
			rig := newFrpRig(t)
			h := newHarnessReal(t, nil)
			h.host = rig.host
			f := &tunFront{harness: h}
			f.tunDeployment(1)
			key := newKey(t)
			e, sock := rig.engine(key)
			a := f.tunInstance("md-a", key)
			client := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
				inventory.GVRModelDeployments: "ModelDeploymentList", inventory.GVRInstances: "InstanceList",
				inventory.GVRProviders: "ProviderList", inventory.GVRPlatformProfiles: "PlatformProfileList",
			}, kySeed(t, h)...)

			cfg := Config{ClientKeysFile: h.cfg.ClientKeysFile, UsageDir: filepath.Join(h.dir, "usage-run"),
				ProbeInterval: Duration(100 * time.Millisecond)}.WithDefaults()
			cl, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			dl, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			ready := make(chan *Front, 1)
			o := Options{Config: cfg, Host: rig.host, Mode: mode, Dynamic: client, Recorder: &MemoryRecorder{}, Log: discardLog(),
				ClientListener: cl, DialHomeListener: dl, Ready: func(f *Front) { ready <- f }, ShutdownGrace: time.Second}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- Run(ctx, o) }()
			stopped := false
			stop := func() {
				if !stopped {
					stopped = true
					cancel()
					if err := <-done; err != nil {
						t.Errorf("Run: %v", err)
					}
				}
			}
			defer stop()
			var front *Front
			select {
			case front = <-ready:
			case err := <-done:
				t.Fatalf("Run returned early: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("the front did not start")
			}
			if front.Tunnel == nil {
				t.Fatal("Run built no Tunnel")
			}

			p := rig.startFrpc(a, sock, front.Dial.String())
			rig.waitOnline(a, p)
			var status int
			var body string
			for end := time.Now().Add(10 * time.Second); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
				req, _ := http.NewRequest(http.MethodPost, "http://"+front.Client.String()+"/v1/chat/completions", strings.NewReader(tunChat))
				req.Header.Set("Authorization", "Bearer "+h.clientKey)
				resp, err := ctHTTP().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				if status, body = resp.StatusCode, readAll(t, resp); status == http.StatusOK {
					break
				}
			}
			if status != http.StatusOK {
				t.Fatalf("no request reached the engine through the tunnel: %d %s", status, body)
			}
			if !front.Tunnel.Registered(a) {
				t.Error("a request was routed, but the Tunnel does not count the transport as registered")
			}
			reqs := e.Requests()
			if len(reqs) != 1 || reqs[0].Header.Get("Authorization") != "Bearer "+key {
				t.Errorf("the engine saw %d requests, want one with the Instance's key", len(reqs))
			}
			if e.HealthChecks() == 0 {
				t.Error("the engine saw no probe")
			}

			stop()
			for end := time.Now().Add(10 * time.Second); rig.proxyStatus(string(a)) == "online"; time.Sleep(50 * time.Millisecond) {
				if time.Now().After(end) {
					t.Fatal("frps still lists the proxy online after the front that carried its tunnel stopped")
				}
			}
		})
	}
}

// TestRunNeedsFrpsAddressesWhenJobsDialHome: in direct and ssh mode the
// front must know frps's addresses, from the configuration or the launcher's
// port base, or it does not start. With no mode recorded no job dials home,
// and it starts without them (TestRunNamesTheUsageFileByItsClock runs so).
func TestRunNeedsFrpsAddressesWhenJobsDialHome(t *testing.T) {
	for _, mode := range []string{ModeDirect, ModeSSH} {
		host := agentapi.Host{StateDir: t.TempDir()}
		writeDialHomeCert(t, host)
		o := runOptions(t, mode, host, "127.0.0.1:0")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := Run(ctx, o)
		cancel()
		if err == nil || !strings.Contains(err.Error(), "frpsBindAddr or frpsMuxAddr") {
			t.Errorf("%s mode with no port base and no frps addresses: Run = %v, want a refusal naming them", mode, err)
		}
	}
}
