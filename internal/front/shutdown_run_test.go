package front

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/inventory"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// TestRunStopCutsAStreamAndWritesItsRecord (PR #95 review, Opus B5's
// recipe): cancel Run's context with a stream open. After ShutdownGrace the
// stream gets one stream_cut, detail shutdown, and when Run returns its usage
// record is in the log with cutBy front_shutdown. This checks Run's own
// wiring: the client server's base context and the stop sequence.
//
// Like the shutdown tests (#432) it waits on state, not on a window: the stop
// goes through Options.stop with the Shutdown grace Run passes, ShutdownGrace
// 300 ms, and the generous handler wait; and the stream's handler is held 1 s
// between the cut and its record's write, so a handler wait of the grace would
// lose the record. TestRunPassesOneGraceToBothWaitsOfAStop reads what Run
// passes for the handler wait.
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
	o.Dynamic, o.Transports, o.ShutdownGrace = client, h.trans, testShutdownGrace
	dial, err := listenFree()
	if err != nil {
		t.Fatal(err)
	}
	o.DialHomeListener = dial
	var cut, releasedAtStop atomic.Bool
	var held *heldRecord
	o.stop = func(servers []*http.Server, cancelBase context.CancelCauseFunc, p *Proxy, shutdownGrace, _ time.Duration, log *slog.Logger) {
		noting := func(cause error) { cut.Store(true); cancelBase(cause) }
		stopServing(servers, noting, p, shutdownGrace, testHandlerWait, log)
		releasedAtStop.Store(held.released.Load())
	}
	ready := make(chan *Front, 1)
	o.Ready = func(f *Front) {
		held = holdFirstRecord(f.Proxy, cut.Load, time.Second)
		ready <- f
	}
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
	if !held.entered.Load() || !held.afterCut.Load() {
		t.Fatalf("the stream's handler was not held after the shutdown cut (reached its record's write %v, after the cut %v), so the stop's wait for it was never at stake", held.entered.Load(), held.afterCut.Load())
	}
	if !releasedAtStop.Load() {
		t.Error("the stop returned while the stream's handler was still held before its record's write: it waited for a window, not for the handler")
	}
	if n := errorEvents(string(rest)); n != 1 || !strings.Contains(string(rest), `"detail":"shutdown"`) {
		t.Errorf("the stream got %d error events, want one stream_cut with detail shutdown; its end: %q", n, string(rest)[max(0, len(rest)-300):])
	}
	b := readUsageFiles(t, o.Config.UsageDir)
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

// TestHealthAndEventOnAFailingUsageSink (@hpc-burst-science-agent, 31 Oct
// 2026; HBEP-8, Usage failure): the usage log is /dev/full, so every write
// fails with ENOSPC. The front keeps serving; /healthz stays 200 and its
// usage member says failing, with the records lost, the time the failure
// began and the cause, and usageError repeats the cause; /metrics counts the
// records lost in hpcburst_usage_write_failures_total; and a UsageWriteFailed
// event, whose reason is that code alone, regards the front's Lease
// (@hpc-burst-science-agent, 31 Oct 2026: no fallback to a ModelDeployment).
func TestHealthAndEventOnAFailingUsageSink(t *testing.T) {
	h := newHarness(t, nil)
	instanceKey := newKey(t)
	e := enginetest.Start(t, enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: instanceKey})
	h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
	h.batchProvider("helios-a", "helios-gh200")
	h.servedMD("md", scModel, 1, "helios-a")
	h.instance("md", "md-0", "helios-a", e, instanceKey, instanceOpts{leaseEnd: time.Now().Add(48 * time.Hour)})
	client := dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		inventory.GVRModelDeployments: "ModelDeploymentList", inventory.GVRInstances: "InstanceList",
		inventory.GVRProviders: "ProviderList", inventory.GVRPlatformProfiles: "PlatformProfileList",
	}, kySeed(t, h)...)

	dir := filepath.Join(h.dir, "usage-full")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	// Today's file and tomorrow's, so that a run across UTC midnight fails too.
	for _, day := range []time.Time{time.Now(), time.Now().Add(24 * time.Hour)} {
		if err := os.Symlink("/dev/full", filepath.Join(dir, usage.DayFile(day))); err != nil {
			t.Fatal(err)
		}
	}
	rec := &MemoryRecorder{}
	o := runOptions(t, "", h.host, "")
	o.Config.ClientKeysFile = h.cfg.ClientKeysFile
	o.Config.UsageDir = dir
	o.Dynamic, o.Transports, o.Recorder = client, h.trans, rec
	o.Kube = kubefake.NewSimpleClientset()
	dial, err := listenFree()
	if err != nil {
		t.Fatal(err)
	}
	health, err := listenFree()
	if err != nil {
		t.Fatal(err)
	}
	o.DialHomeListener, o.HealthListener = dial, health
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

	deadline := time.Now().Add(5 * time.Second)
	status := 0
	for time.Now().Before(deadline) && status != 200 {
		req, _ := http.NewRequest(http.MethodPost, "http://"+f.Client.String()+scChat, strings.NewReader(errChatBody(scModel, false, "hi")))
		req.Header.Set("Authorization", "Bearer "+h.clientKey)
		if resp, err := errClient.Do(req); err == nil {
			readAll(t, resp)
			status = resp.StatusCode
		}
		if status != 200 {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if status != 200 {
		t.Fatalf("the front did not serve with a failing sink: status %d", status)
	}
	errWaitFor(t, "a UsageWriteFailed event", func() bool { return len(rec.Find(eventFailing, reasonUsageWriteFailed)) > 0 })
	ev := rec.Find(eventFailing, reasonUsageWriteFailed)[0]
	if !ev.Warning || ev.Object != leaseDesc || !strings.Contains(ev.Message, "no space left") {
		t.Errorf("event %+v: want a warning regarding %s naming the cause", ev, leaseDesc)
	}
	resp, err := http.Get("http://" + f.Health.String() + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		UsageError string `json:"usageError"`
		Usage      struct {
			State       string `json:"state"`
			LostRecords uint64 `json:"lostRecords"`
			Since       string `json:"since"`
			Error       string `json:"error"`
		} `json:"usage"`
	}
	if err := json.Unmarshal([]byte(readAll(t, resp)), &body); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || body.Usage.State != "failing" || body.Usage.LostRecords == 0 || !strings.Contains(body.Usage.Error, "no space left") || body.Usage.Since == "" {
		t.Errorf("/healthz: status %d usage %+v; want 200, failing, a count, a time and the cause", resp.StatusCode, body.Usage)
	}
	if !strings.Contains(body.UsageError, "no space left") {
		t.Errorf("/healthz usageError = %q, want the cause", body.UsageError)
	}

	// The counter, over the health listener: the records lost, and the
	// records written still 0.
	mr, err := http.Get("http://" + f.Health.String() + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	ss := parseSamples(t, readAll(t, mr))
	if v, ok := find(t, ss, "hpcburst_usage_write_failures_total"); !ok || v < 1 || v != float64(body.Usage.LostRecords) {
		t.Errorf("hpcburst_usage_write_failures_total = %v (present %v), want the %d records /healthz counts", v, ok, body.Usage.LostRecords)
	}
	if v, ok := find(t, ss, "hpcburst_usage_records_total"); !ok || v != 0 {
		t.Errorf("hpcburst_usage_records_total = %v (present %v), want 0: nothing reached the disk", v, ok)
	}
}

// TestRunPassesOneGraceToBothWaitsOfAStop (#432): stopServing takes the
// Shutdown grace and the handler wait apart, so that a test can wait on state;
// Run still gives both the one configured grace, and DefaultShutdownGrace when
// none is set, so production's stop is unchanged. The bounds are read at Run's
// own call, through Options.stop; nothing is in flight, so the stop is
// instant whatever the grace.
func TestRunPassesOneGraceToBothWaitsOfAStop(t *testing.T) {
	for _, tc := range []struct {
		name  string
		grace time.Duration
		want  time.Duration
	}{
		{"configured", 7 * time.Second, 7 * time.Second},
		{"default", 0, DefaultShutdownGrace},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := runOptions(t, "", agentapi.Host{StateDir: t.TempDir()}, "")
			dial, err := listenFree()
			if err != nil {
				t.Fatal(err)
			}
			o.DialHomeListener = dial
			o.ShutdownGrace = tc.grace
			var calls int
			var shutdown, handlers time.Duration
			o.stop = func(servers []*http.Server, cancelBase context.CancelCauseFunc, p *Proxy, shutdownGrace, handlerWait time.Duration, log *slog.Logger) {
				calls++
				shutdown, handlers = shutdownGrace, handlerWait
				stopServing(servers, cancelBase, p, shutdownGrace, handlerWait, log)
			}
			ready := make(chan *Front, 1)
			o.Ready = func(f *Front) { ready <- f }
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- Run(ctx, o) }()
			select {
			case <-ready:
			case err := <-done:
				t.Fatalf("Run returned early: %v", err)
			case <-time.After(30 * time.Second):
				cancel()
				<-done
				t.Fatal("the front did not start")
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatalf("Run: %v", err)
			}
			if calls != 1 {
				t.Fatalf("Run stopped %d times, want once", calls)
			}
			if shutdown != tc.want || handlers != tc.want {
				t.Errorf("Run passed a Shutdown grace of %v and a handler wait of %v, want %v for both", shutdown, handlers, tc.want)
			}
		})
	}
}
