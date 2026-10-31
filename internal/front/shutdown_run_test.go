package front

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynfake "k8s.io/client-go/dynamic/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/inventory"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
)

// TestRunStopCutsAStreamAndWritesItsRecord (PR #95 review, Opus B5's
// recipe): cancel Run's context with a stream open. After ShutdownGrace the
// stream gets one stream_cut, detail shutdown, and when Run returns its usage
// record is in the log with cutBy front_shutdown. This checks Run's own
// wiring: the client server's base context and the stop sequence.
func TestRunStopCutsAStreamAndWritesItsRecord(t *testing.T) {
	h := newHarness(t, nil)
	instanceKey := newKey(t)
	e := enginetest.Start(t, enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: instanceKey})
	e.Set(enginetest.Behaviour{Tokens: 100000, PerToken: 20 * time.Millisecond})
	h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
	h.batchProvider("helios-a", "helios-gh200")
	h.servedMD("md", scModel, 1, "helios-a")
	h.instance("md", "md-0", "helios-a", e, instanceKey, instanceOpts{leaseEnd: time.Now().Add(48 * time.Hour)})
	client := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		inventory.GVRModelDeployments: "ModelDeploymentList", inventory.GVRInstances: "InstanceList",
		inventory.GVRProviders: "ProviderList", inventory.GVRPlatformProfiles: "PlatformProfileList",
	}, kySeed(t, h)...)

	o := runOptions(t, "", h.host, "")
	o.Config.ClientKeysFile = h.cfg.ClientKeysFile
	o.Config.UsageDir = filepath.Join(h.dir, "usage-run")
	o.Dynamic, o.Transports, o.ShutdownGrace = client, h.trans, 300*time.Millisecond
	dial, err := listenFree()
	if err != nil {
		t.Fatal(err)
	}
	o.DialHomeListener = dial
	ready := make(chan *Front, 1)
	o.Ready = func(f *Front) { ready <- f }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, o) }()
	defer func() { cancel(); <-done }()
	var f *Front
	select {
	case f = <-ready:
	case err := <-done:
		t.Fatalf("Run returned early: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the front did not start")
	}

	// The stream must be routed from what the watch read; retry until it is.
	var resp *http.Response
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest(http.MethodPost, "http://"+f.Client.String()+scChat, strings.NewReader(prChatStream))
		req.Header.Set("Authorization", "Bearer "+h.clientKey)
		if resp, err = errClient.Do(req); err == nil && resp.StatusCode == 200 {
			break
		}
		if resp != nil {
			readAll(t, resp)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if resp == nil || resp.StatusCode != 200 {
		t.Fatal("no stream was routed from the watched objects")
	}
	id := resp.Header.Get("hpcburst-request-id")
	br := bufio.NewReader(resp.Body)
	if _, err := br.ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	cancel()
	rest, _ := io.ReadAll(br)
	resp.Body.Close()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	done <- nil // for the deferred receive
	if n := errorEvents(string(rest)); n != 1 || !strings.Contains(string(rest), `"detail":"shutdown"`) {
		t.Errorf("the stream got %d error events, want one stream_cut with detail shutdown; its end: %q", n, string(rest)[max(0, len(rest)-300):])
	}
	b, err := os.ReadFile(filepath.Join(o.Config.UsageDir, usage.LogFile))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var r usage.Record
		if json.Unmarshal([]byte(line), &r) == nil && r.RequestID == id {
			found = true
			if r.CutBy == nil || *r.CutBy != cutFrontShutdown {
				t.Errorf("cutBy %v, want front_shutdown", r.CutBy)
			}
		}
	}
	if !found {
		t.Errorf("the stream's usage record is not in the log after Run returned:\n%s", b)
	}
}
