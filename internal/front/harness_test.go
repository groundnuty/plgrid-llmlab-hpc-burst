package front

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/inventory"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The front package's tests bind 127.0.0.1 ports in 29320-29331, the first
// free ones (issue #64's block is 29320-29339; 29333 is the passthrough test's
// agent endpoint, at port base 29330; cmd/hpcburst-front's tests take
// 29336-29339).
const (
	testPortLo = 29320
	testPortHi = 29331
)

// t0 is the tests' starting time on the fake clock.
var t0 = time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)

// harness is a front built from its parts, fed by a hand-built snapshot
// instead of a watch, with engine doubles behind StaticTransports.
type harness struct {
	t         *testing.T
	clk       *clock.Fake
	cfg       Config
	dir       string
	host      agentapi.Host
	trans     *StaticTransports
	router    *Router
	proxy     *Proxy
	sink      *usage.Sink
	metrics   *Metrics
	free      *freeSpace
	rec       *MemoryRecorder
	logs      *syncBuffer
	url       string
	clientKey string
	snap      inventory.Snapshot
	uid       int
	// The client server and its base context, for a test that stops it.
	srv        *http.Server
	base       context.Context
	cancelBase context.CancelCauseFunc
}

// newHarness starts a front on a fake clock at t0. edit, when set, changes
// the configuration before it starts.
func newHarness(t *testing.T, edit func(*Config)) *harness {
	t.Helper()
	return newHarnessOn(t, nil, edit)
}

// newHarnessReal starts a front on the wall clock; h.clk is nil then, and an
// engine started with Clock: nil sleeps for real.
func newHarnessReal(t *testing.T, edit func(*Config)) *harness {
	t.Helper()
	return newHarnessOn(t, clock.Real{}, edit)
}

func newHarnessOn(t *testing.T, real clock.Clock, edit func(*Config)) *harness {
	t.Helper()
	dir := t.TempDir()
	var clk clock.Clock = real
	var fake *clock.Fake
	if real == nil {
		fake = clock.NewFake(t0)
		clk = fake
	}
	h := &harness{t: t, clk: fake, dir: dir, host: agentapi.Host{StateDir: filepath.Join(dir, "state"), PortBase: 29220},
		trans: NewStaticTransports(), rec: &MemoryRecorder{}, logs: &syncBuffer{}, clientKey: "client-key-for-tests-0001"}
	keysPath := filepath.Join(dir, "client-keys.json")
	writeMode600(t, keysPath, fmt.Sprintf(`{"keys":[{"id":"tester","key":%q}]}`, h.clientKey))
	h.cfg = Config{DialHomeListen: "127.0.0.1:0", ClientKeysFile: keysPath, UsageDir: filepath.Join(dir, "usage")}.WithDefaults()
	// An engine double on the fake clock jumps it at once after flushing its
	// headers, before the front may have read them; a 30 s headers deadline
	// would then fire in tests that are not about it. The tests that are set
	// it (TestStreamHeadersDeadline); the default is checked by
	// TestStreamHeadersTimeoutDefault.
	h.cfg.StreamHeadersTimeout = Duration(24 * time.Hour)
	if edit != nil {
		edit(&h.cfg)
	}
	keys, err := ReadClientKeys(keysPath)
	if err != nil {
		t.Fatal(err)
	}
	// The sink names its files by the harness's clock: the fake one in
	// the default harness, so a test that moves it past midnight rotates.
	if h.sink, err = usage.OpenOptions(h.cfg.UsageDir, usage.Options{Now: clk.Now}); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	h.free = &freeSpace{n: 1 << 40}
	h.metrics = NewMetrics(h.sink, h.cfg.UsageDir, h.free.get)
	h.router = NewRouter(h.cfg, clk, h.trans, h.rec, log, h.metrics)
	h.proxy = NewProxy(ProxyOptions{Config: h.cfg, Host: h.host, Clock: clk, Router: h.router, Transports: h.trans, Keys: keys, Sink: h.sink, Metrics: h.metrics, Log: log})
	ln, err := enginetest.Listen(testPortLo, testPortHi)
	if err != nil {
		t.Fatal(err)
	}
	h.base, h.cancelBase = context.WithCancelCause(context.Background())
	srv := clientServer(h.cfg, h.proxy, h.base)
	h.srv = srv
	go func() { _ = srv.Serve(ln) }()
	h.url = "http://" + ln.Addr().String()
	t.Cleanup(func() {
		_ = srv.Close()
		_ = h.sink.Close()
	})
	return h
}

func writeMode600(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
}

func (h *harness) newUID() types.UID {
	h.uid++
	return types.UID(fmt.Sprintf("00000000-0000-4000-8000-%012d", h.uid))
}

// profile adds a PlatformProfile; window and margin are optional ("" keeps
// the schema default).
func (h *harness) profile(name string, kind v1alpha1.PlatformKind, window, margin string) {
	p := v1alpha1.PlatformProfile{ObjectMeta: metav1.ObjectMeta{Name: name, ResourceVersion: "1"}, Spec: v1alpha1.PlatformProfileSpec{ProviderKind: kind}}
	if window != "" {
		w := v1alpha1.Duration(window)
		p.Spec.DrainWindow = &w
	}
	if margin != "" {
		m := v1alpha1.Duration(margin)
		p.Spec.DrainMargin = &m
	}
	h.snap.Profiles = append(h.snap.Profiles, p)
}

// batchProvider adds a batch Provider on the given profile.
func (h *harness) batchProvider(name, profile string) {
	h.snap.Providers = append(h.snap.Providers, v1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{v1alpha1.LabelPlatform: "helios", v1alpha1.LabelOrganization: "cyfronet"}},
		Spec:       v1alpha1.ProviderSpec{PlatformProfileRef: v1alpha1.ObjectReference{Name: profile}},
	})
}

// managedProvider adds a managed Provider on platform whose credentials file
// points at baseURL with key, and whose catalog lists entries.
func (h *harness) managedProvider(name, profile, platform, baseURL, key string, entries ...string) {
	creds := filepath.Join(h.dir, "creds-"+name+".json")
	b, err := provider.ManagedCredentials{Key: key, BaseURL: baseURL}.Encode()
	if err != nil {
		h.t.Fatal(err)
	}
	writeMode600(h.t, creds, string(b))
	p := v1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{v1alpha1.LabelPlatform: platform, v1alpha1.LabelOrganization: "cyfronet"}},
		Spec:       v1alpha1.ProviderSpec{PlatformProfileRef: v1alpha1.ObjectReference{Name: profile}, CredentialsRef: &v1alpha1.CredentialsReference{File: creds}},
	}
	for i, e := range entries {
		p.Status.Catalog = append(p.Status.Catalog, v1alpha1.CatalogEntryStatus{Name: e, Listed: true, ManagedID: v1alpha1.FullDeploymentID(fmt.Sprintf("%064x", 0xabc000+i))})
	}
	h.snap.Providers = append(h.snap.Providers, p)
}

// servedMD adds a served ModelDeployment of model on the named providers.
func (h *harness) servedMD(name, model string, instances int32, providers ...string) *v1alpha1.ModelDeployment {
	h.snap.Deployments = append(h.snap.Deployments, v1alpha1.ModelDeployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name, UID: h.newUID()},
		Spec: v1alpha1.ModelDeploymentSpec{Model: model, Instances: instances, Providers: v1alpha1.ProviderSelection{Names: providers},
			Source: v1alpha1.ModelSource{Served: &v1alpha1.ServingSpec{}}},
	})
	return &h.snap.Deployments[len(h.snap.Deployments)-1]
}

// managedMD adds a managed-source ModelDeployment of model naming a catalog
// entry on platform, allowing the named providers.
func (h *harness) managedMD(name, model, platform, catalog string, providers ...string) *v1alpha1.ModelDeployment {
	h.snap.Deployments = append(h.snap.Deployments, v1alpha1.ModelDeployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name, UID: h.newUID()},
		Spec: v1alpha1.ModelDeploymentSpec{Model: model, Providers: v1alpha1.ProviderSelection{Names: providers},
			Source: v1alpha1.ModelSource{Managed: &v1alpha1.ManagedCatalogEntry{Platform: platform, CatalogName: catalog}}},
	})
	return &h.snap.Deployments[len(h.snap.Deployments)-1]
}

// md returns the ModelDeployment of that name in the snapshot.
func (h *harness) md(name string) *v1alpha1.ModelDeployment {
	for i := range h.snap.Deployments {
		if h.snap.Deployments[i].Name == name {
			return &h.snap.Deployments[i]
		}
	}
	h.t.Fatalf("no ModelDeployment %s", name)
	return nil
}

// instanceOpts are an Instance's optional settings.
type instanceOpts struct {
	phase      v1alpha1.InstancePhase // default ready
	leaseEnd   time.Time              // default t0 + 48 h
	noLeaseEnd bool
	maxNumSeqs int32 // default 256
	deployID   string
}

// instance adds a routable-to-be Instance of md on provider, served by e: its
// key file is written and its transport registered. It returns its UID.
func (h *harness) instance(mdName, name, providerName string, e *enginetest.Engine, key string, o instanceOpts) types.UID {
	md := h.md(mdName)
	uid := h.newUID()
	if o.phase == "" {
		o.phase = v1alpha1.InstanceReady
	}
	if o.maxNumSeqs == 0 {
		o.maxNumSeqs = 256
	}
	if o.deployID == "" {
		o.deployID = fmt.Sprintf("%064x", 0x1234500+h.uid)
	}
	inst := v1alpha1.Instance{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name, UID: uid,
			Labels:          map[string]string{v1alpha1.LabelProvider: providerName, v1alpha1.LabelModelDeployment: md.Name},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: v1alpha1.GroupVersion.String(), Kind: "ModelDeployment", Name: md.Name, UID: md.UID, Controller: boolp(true)}}},
		Spec:   v1alpha1.InstanceSpec{DeploymentID: v1alpha1.FullDeploymentID(o.deployID), Launch: v1alpha1.Launch{MaxNumSeqs: o.maxNumSeqs}},
		Status: v1alpha1.InstanceStatus{Phase: o.phase},
	}
	if !o.noLeaseEnd {
		le := o.leaseEnd
		if le.IsZero() {
			le = t0.Add(48 * time.Hour)
		}
		inst.Status.LeaseEnd = &metav1.Time{Time: le}
	}
	h.snap.Instances = append(h.snap.Instances, inst)
	if key != "" {
		if err := agentapi.WriteKeyFile(agentapi.InstanceKeyPath(h.host.CredentialsDir(), string(uid)), key); err != nil {
			h.t.Fatal(err)
		}
	}
	if e != nil {
		h.trans.Set(uid, e.Addr())
	}
	return uid
}

// inst returns the snapshot's Instance with that UID, to edit it.
func (h *harness) inst(uid types.UID) *v1alpha1.Instance {
	for i := range h.snap.Instances {
		if h.snap.Instances[i].UID == uid {
			return &h.snap.Instances[i]
		}
	}
	h.t.Fatalf("no Instance %s", uid)
	return nil
}

// apply hands the snapshot to the router, as the watch does.
func (h *harness) apply() { h.router.Update(h.snap) }

// newKey returns a fresh Instance key.
func newKey(t *testing.T) string {
	t.Helper()
	k, err := agentapi.NewKey()
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// post sends a request with the client key; extra headers are added.
func (h *harness) post(path, body string, hdr ...string) *http.Response {
	h.t.Helper()
	req, err := http.NewRequest(http.MethodPost, h.url+path, strings.NewReader(body))
	if err != nil {
		h.t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+h.clientKey)
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	return resp
}

// readAll reads and closes a body.
func readAll(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil && err != io.ErrUnexpectedEOF {
		t.Logf("reading the body: %v", err)
	}
	return string(b)
}

// readUsageFiles returns the text of every usage file in dir, oldest first:
// the records written so far, whichever day's file each is in. A directory
// with none gives nothing.
func readUsageFiles(t *testing.T, dir string) []byte {
	t.Helper()
	files, err := usage.Files(dir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	var all []byte
	for _, name := range files {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, b...)
	}
	return all
}

// records closes nothing and returns the usage records written so far, after
// waiting for the sink to sync what is queued.
func (h *harness) records() []usage.Record {
	h.t.Helper()
	var out []usage.Record
	deadline := time.Now().Add(5 * time.Second)
	for {
		out = out[:0]
		b := readUsageFiles(h.t, h.cfg.UsageDir)
		dec := json.NewDecoder(bytes.NewReader(b))
		for dec.More() {
			var r usage.Record
			if err := dec.Decode(&r); err != nil {
				h.t.Fatal(err)
			}
			out = append(out, r)
		}
		if time.Now().After(deadline) || len(out) > 0 {
			return out
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// record waits for the usage record of a request ID.
func (h *harness) record(id string) usage.Record {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, r := range h.records() {
			if r.RequestID == id {
				return r
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.t.Fatalf("no usage record for request %s", id)
	return usage.Record{}
}

func boolp(b bool) *bool { return &b }

// syncBuffer is a goroutine-safe log buffer.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// listenFree binds the first free port of the package's test block.
func listenFree() (net.Listener, error) { return enginetest.Listen(testPortLo, testPortHi) }

// discardLog is a logger that writes nowhere.
func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// freeSpace is the free-space reading behind hpcburst_usage_dir_free_bytes, a
// double that a test moves or makes fail.
type freeSpace struct {
	mu  sync.Mutex
	n   uint64
	err error
}

func (f *freeSpace) get(string) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.n, f.err
}

func (f *freeSpace) set(n uint64, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n, f.err = n, err
}
