package main

// The front's stop under hpcburst down, against a real k3s and the real front (PR 134 review, B1):
// a request in flight when the stop begins is cut with front_shutdown, its usage record is
// written, and the sink closes cleanly, with no counter gap. Judged from the front's own usage
// files and counter file, never from the launcher's report.

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/inventory"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/managed/llmlabtest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The LLMLab double's port: the last free port of issue #64's block 29320-29339 (front_test.go's
// frontClientPort is 29334).
const stopTestLLMLabPort = 29335

type usageLine struct {
	Counter uint64  `json:"counter"`
	CutBy   *string `json:"cutBy"`
}

// usageLines reads every usage record in dir, in file and line order.
func usageLines(t *testing.T, dir string) []usageLine {
	t.Helper()
	var out []usageLine
	for _, f := range usageFiles(t, dir) {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(bytes.NewReader(b))
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			var r usageLine
			if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
				t.Fatalf("%s: a line that is not a usage record: %v", f, err)
			}
			out = append(out, r)
		}
	}
	return out
}

// TestFrontStopFlushesARequestInFlight: a chat request to a managed entry whose upstream (the LLMLab
// double) stalls is in flight when hpcburst down stops the host. The front lets it run for its
// 10 s shutdown grace, cuts it with front_shutdown, writes its record and closes the sink, all
// within the grace the supervisor gives it. The record is there, the counters run without a gap,
// and the counter file holds the last one, as only a clean close leaves it.
func TestFrontStopFlushesARequestInFlight(t *testing.T) {
	const base = 29220
	h := newHost(t)
	ctl, _ := fakeController(t)
	fb := frontBinary(t)
	work := shortTmp(t, false)
	clientKey := "front-stop-client-key-" + strings.Repeat("5", 20)
	keys := filepath.Join(work, "client-keys.json")
	if err := os.WriteFile(keys, []byte(fmt.Sprintf(`{"keys":[{"id":"e2e","key":%q}]}`, clientKey)), 0o600); err != nil {
		t.Fatal(err)
	}
	usageDir := filepath.Join(work, "usage")
	cfg := filepath.Join(work, "front.json")
	if err := os.WriteFile(cfg, []byte(fmt.Sprintf(`{"clientListen":"127.0.0.1:%d","clientKeysFile":%q,"usageDir":%q}`, frontClientPort, keys, usageDir)), 0o600); err != nil {
		t.Fatal(err)
	}

	const upstreamKey, catalog, model = "llmlab-upstream-key-front-stop-test-77", "llmlab-example-27b", "example/stop-27b"
	llm := llmlabtest.Start(t, llmlabtest.Options{PortLo: stopTestLLMLabPort, PortHi: stopTestLLMLabPort, Keys: []string{upstreamKey},
		Models: []llmlabtest.Model{{ID: catalog}}})
	creds := filepath.Join(work, "llm-creds.json")
	b, err := provider.ManagedCredentials{Key: upstreamKey, BaseURL: llm.BaseURL()}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(creds, b, 0o600); err != nil {
		t.Fatal(err)
	}

	r := cli(t, h.upArgs(base, append(frpsArgs(t), "--json", "--controller-bin", ctl, "--manifests", "../../config/crd", "--front-config", cfg, "--front-bin", fb)...)...)
	if r.code != exitOK {
		t.Fatalf("up exited %d: %s", r.code, r.err)
	}
	healthAddr := fmt.Sprintf("127.0.0.1:%d", base+4)

	// ---- a managed entry on the LLMLab double ----
	c := h.clients()
	ctx := t.Context()
	prof := &v1alpha1.PlatformProfile{ObjectMeta: metav1.ObjectMeta{Name: "llmlab"}, Spec: v1alpha1.PlatformProfileSpec{ProviderKind: v1alpha1.PlatformManaged}}
	prov := &v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: "llm-a", Labels: map[string]string{v1alpha1.LabelPlatform: "llmlab"}},
		Spec: v1alpha1.ProviderSpec{PlatformProfileRef: v1alpha1.ObjectReference{Name: "llmlab"}, CredentialsRef: &v1alpha1.CredentialsReference{File: creds}}}
	md := &v1alpha1.ModelDeployment{ObjectMeta: metav1.ObjectMeta{Name: "stop-e2e", Namespace: "default"},
		Spec: v1alpha1.ModelDeploymentSpec{Model: model, Providers: v1alpha1.ProviderSelection{Names: []string{"llm-a"}},
			Source: v1alpha1.ModelSource{Managed: &v1alpha1.ManagedCatalogEntry{Platform: "llmlab", CatalogName: catalog}}}}
	for _, o := range []struct {
		obj  runtime.Object
		kind string
		ns   string
	}{{prof, "PlatformProfile", ""}, {prov, "Provider", ""}, {md, "ModelDeployment", "default"}} {
		gvr := map[string]schema.GroupVersionResource{"PlatformProfile": inventory.GVRPlatformProfiles, "Provider": inventory.GVRProviders, "ModelDeployment": inventory.GVRModelDeployments}[o.kind]
		ri := c.dyn.Resource(gvr)
		var err error
		if o.ns != "" {
			_, err = ri.Namespace(o.ns).Create(ctx, toU(t, o.obj, o.kind), metav1.CreateOptions{})
		} else {
			_, err = ri.Create(ctx, toU(t, o.obj, o.kind), metav1.CreateOptions{})
		}
		if err != nil {
			t.Fatalf("create %s: %v", o.kind, err)
		}
	}
	// The catalog entry is the lifecycle controller's to write; the fake controller writes none,
	// so the test sets it through the status subresource.
	pu, err := c.dyn.Resource(inventory.GVRProviders).Get(ctx, "llm-a", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	entry := map[string]any{"name": catalog, "listed": true, "managedID": string(v1alpha1.FullDeploymentID(fmt.Sprintf("%064x", 0xabc001)))}
	if err := unstructured.SetNestedSlice(pu.Object, []any{entry}, "status", "catalog"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.dyn.Resource(inventory.GVRProviders).UpdateStatus(ctx, pu, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("write the Provider's catalog: %v", err)
	}
	waitFor(t, 60*time.Second, "the front to route the managed entry", func() bool {
		code, body := listModels(t, clientKey)
		return code == http.StatusOK && strings.Contains(body, `"id":"`+model+`"`) && strings.Contains(body, `"routable":true`)
	})

	// ---- a request in flight: the double holds it before its first byte ----
	llm.Stall(true)
	done := make(chan error, 1)
	go func() {
		body := fmt.Sprintf(`{"model":%q,"stream":true,"messages":[{"role":"user","content":"hello"}]}`, model)
		req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:%d/v1/chat/completions", frontClientPort), strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+clientKey)
		req.Header.Set("Content-Type", "application/json")
		resp, err := (&http.Client{Timeout: 90 * time.Second}).Do(req)
		if err == nil {
			_, _ = bytes.NewBuffer(nil).ReadFrom(resp.Body)
			resp.Body.Close()
		}
		done <- err
	}()
	waitFor(t, 30*time.Second, "the request to wait at the stalled upstream", func() bool { return llm.Waiting() == 1 })
	before := len(usageLines(t, usageDir))

	// ---- down: the front gets its own grace, cuts the request, and closes the sink ----
	start := time.Now()
	if r := cli(t, h.args("down")...); r.code != exitOK {
		t.Fatalf("down exited %d: %s", r.code, r.err)
	}
	took := time.Since(start)
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Error("the client's request did not end after down")
	}
	lines := usageLines(t, usageDir)
	var cut []usageLine
	for _, l := range lines {
		if l.CutBy != nil && *l.CutBy == "front_shutdown" {
			cut = append(cut, l)
		}
	}
	if len(lines) != before+1 || len(cut) != 1 {
		t.Fatalf("after down: %d records (%d before), %d cut by front_shutdown; want the request in flight's record, cut by front_shutdown (down took %s)", len(lines), before, len(cut), took)
	}
	for i, l := range lines {
		if l.Counter != lines[0].Counter+uint64(i) {
			t.Fatalf("record %d has counter %d after %d: a gap", i, l.Counter, lines[i-1].Counter)
		}
	}
	// A clean close persists the last counter used; a killed front leaves the reserved block's end.
	cb, err := os.ReadFile(filepath.Join(usageDir, "counter"))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(cb)); got != strconv.FormatUint(lines[len(lines)-1].Counter, 10) {
		t.Errorf("the counter file holds %q, the last record's counter is %d: the sink was not closed", got, lines[len(lines)-1].Counter)
	}
	if _, ok := getFrontHealth(healthAddr); ok {
		t.Error("the front still answers after down")
	}
	t.Logf("down took %s with a request in flight", took)
}
