package managed

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/managed/llmlabtest"
)

// Shared helpers of the managed controller's tests. Every expectation is
// written out by hand: times, reasons, and the managed IDs, which are the
// sha256 of HBEP-3's canonical form written here or the committed golden
// vectors, never deploymentid.ComputeManaged and never a helper of the
// controller.

const (
	testProvider = "example-managed"
	// testPlatform is the platform of HBEP-3's golden vectors 02 and 03.
	testPlatform = "llmlab"
	testKey      = "example-key-0001"
)

// Issue #69's loopback port block (IMPL-COMMON.md, Other listeners).
const portLo, portHi = 29340, 29359

// at is a moment of 31 Oct 2026, UTC.
func at(h, m, s int) time.Time { return time.Date(2026, 10, 31, h, m, s, 0, time.UTC) }

// manualClock is the controller's clock in tests that move time by hand.
type manualClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *manualClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

func ptr[T any](v T) *T { return &v }

// mtOrNil is t as a metav1.Time, or nil.
func mtOrNil(t *time.Time) *metav1.Time {
	if t == nil {
		return nil
	}
	m := metav1.NewTime(*t)
	return &m
}

func model(id string) llmlabtest.Model { return llmlabtest.Model{ID: id} }

func modelRev(id, revision string) llmlabtest.Model {
	return llmlabtest.Model{ID: id, Revision: revision}
}

// envConfig is what newEnv builds from.
type envConfig struct {
	models   []llmlabtest.Model
	key      string
	moreKeys []string
	noLabel  bool
	credsRef func(path string) *v1alpha1.CredentialsReference
	builders []func(*clientfake.ClientBuilder) *clientfake.ClientBuilder
	clock    Clock
	recorder events.EventRecorder
	log      logr.Logger
}

type envOption func(*envConfig)

func withModels(ms ...llmlabtest.Model) envOption { return func(c *envConfig) { c.models = ms } }
func withKey(k string) envOption                  { return func(c *envConfig) { c.key = k } }
func withExtraKeys(ks ...string) envOption        { return func(c *envConfig) { c.moreKeys = ks } }
func withNoLabel() envOption                      { return func(c *envConfig) { c.noLabel = true } }
func withClock(clk Clock) envOption               { return func(c *envConfig) { c.clock = clk } }
func withLog(l logr.Logger) envOption             { return func(c *envConfig) { c.log = l } }
func withRecorder(r events.EventRecorder) envOption {
	return func(c *envConfig) { c.recorder = r }
}
func withBuilder(f func(*clientfake.ClientBuilder) *clientfake.ClientBuilder) envOption {
	return func(c *envConfig) { c.builders = append(c.builders, f) }
}

// withCredentialsRef sets the Provider's spec.credentialsRef from the path of
// its credentials file; the default names that file.
func withCredentialsRef(f func(path string) *v1alpha1.CredentialsReference) envOption {
	return func(c *envConfig) { c.credsRef = f }
}

// withManagedFields makes the fake client record managedFields, so a test sees
// which field manager wrote what.
func withManagedFields(b *clientfake.ClientBuilder) *clientfake.ClientBuilder {
	return b.WithReturnManagedFields()
}

// env is one managed Provider, its credentials file, a double of the gateway,
// and the Controller under test, on a fake client.
type env struct {
	t     *testing.T
	clock *manualClock // nil when the test passed its own clock
	gw    *llmlabtest.Server
	c     client.Client
	ctl   *Controller
	path  string // the credentials file
	key   string
}

func newEnv(t *testing.T, opts ...envOption) *env {
	t.Helper()
	cfg := envConfig{key: testKey, credsRef: func(path string) *v1alpha1.CredentialsReference {
		return &v1alpha1.CredentialsReference{File: path}
	}}
	for _, o := range opts {
		o(&cfg)
	}
	// The double keeps real time: its stall and cut windows need a clock that
	// moves, and only the controller's clock is the test's.
	gw := llmlabtest.Start(t, llmlabtest.Options{
		PortLo: portLo, PortHi: portHi, Keys: append([]string{cfg.key}, cfg.moreKeys...), Models: cfg.models,
	})
	path := filepath.Join(t.TempDir(), "credentials.json")
	writeCreds(t, path, cfg.key, gw.BaseURL(), "")

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	var labels map[string]string
	if !cfg.noLabel {
		labels = map[string]string{v1alpha1.LabelPlatform: testPlatform}
	}
	prov := &v1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: testProvider, Generation: 1, Labels: labels},
		Spec: v1alpha1.ProviderSpec{
			PlatformProfileRef: v1alpha1.ObjectReference{Name: "example-profile"},
			CredentialsRef:     cfg.credsRef(path),
		},
	}
	builder := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(prov).WithStatusSubresource(&v1alpha1.Provider{})
	for _, b := range cfg.builders {
		builder = b(builder)
	}
	c := builder.Build()

	e := &env{t: t, gw: gw, c: c, path: path, key: cfg.key}
	clk := cfg.clock
	if clk == nil {
		e.clock = &manualClock{t: at(12, 0, 0)}
		clk = e.clock
	}
	ctl, err := New(Options{Client: c, Provider: testProvider, Clock: clk, Recorder: cfg.recorder, Log: cfg.log})
	if err != nil {
		t.Fatal(err)
	}
	e.ctl = ctl
	return e
}

// writeFile writes a mode-600 file, whatever the file's mode was.
func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
}

// writeCreds writes a credentials file by hand: strict JSON, with an expiry
// (an RFC 3339 string) when one is given.
func writeCreds(t *testing.T, path, key, baseURL, expiry string) {
	t.Helper()
	body := fmt.Sprintf(`{"key":%q,"baseURL":%q`, key, baseURL)
	if expiry != "" {
		body += fmt.Sprintf(`,"expiry":%q`, expiry)
	}
	writeFile(t, path, body+"}")
}

// pointAt rewrites the credentials file to name baseURL, keeping the env's key.
func (e *env) pointAt(baseURL, expiry string) {
	e.t.Helper()
	writeCreds(e.t, e.path, e.key, baseURL, expiry)
}

// trySyncAt sets the manual clock to when and runs one pass.
func (e *env) trySyncAt(when time.Time) (time.Duration, error) {
	e.t.Helper()
	e.clock.Set(when)
	return e.ctl.Sync(context.Background())
}

// syncAt runs a pass at when and fails the test when it returns an error.
func (e *env) syncAt(when time.Time) time.Duration {
	e.t.Helper()
	wait, err := e.trySyncAt(when)
	if err != nil {
		e.t.Fatalf("Sync at %s: %v", when.Format("15:04:05.000"), err)
	}
	return wait
}

// gets is how many GET /v1/models the double has logged: the reads, counted
// at the gateway and nowhere in the controller.
func (e *env) gets() int { return e.gw.Count(http.MethodGet, "/v1/models") }

func (e *env) provider() *v1alpha1.Provider {
	e.t.Helper()
	var p v1alpha1.Provider
	if err := e.c.Get(context.Background(), client.ObjectKey{Name: testProvider}, &p); err != nil {
		e.t.Fatal(err)
	}
	return &p
}

// storeStatus changes the Provider's stored status, as an earlier process
// would have left it.
func (e *env) storeStatus(mutate func(*v1alpha1.ProviderStatus)) {
	e.t.Helper()
	p := e.provider()
	mutate(&p.Status)
	if err := e.c.Status().Update(context.Background(), p); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) setCredentialsRef(ref *v1alpha1.CredentialsReference) {
	e.t.Helper()
	p := e.provider()
	p.Spec.CredentialsRef = ref
	if err := e.c.Update(context.Background(), p); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) entry(name string) v1alpha1.CatalogEntryStatus {
	e.t.Helper()
	for _, ce := range e.provider().Status.Catalog {
		if ce.Name == name {
			return ce
		}
	}
	e.t.Fatalf("status.catalog has no entry %q: %+v", name, e.provider().Status.Catalog)
	return v1alpha1.CatalogEntryStatus{}
}

func (e *env) names() []string {
	var out []string
	for _, ce := range e.provider().Status.Catalog {
		out = append(out, ce.Name)
	}
	return out
}

func (e *env) condition(typ string) *metav1.Condition {
	return apimeta.FindStatusCondition(e.provider().Status.Conditions, typ)
}

func (e *env) wantCondition(typ string, status metav1.ConditionStatus, reason string) metav1.Condition {
	e.t.Helper()
	c := e.condition(typ)
	if c == nil || c.Status != status || c.Reason != reason {
		e.t.Fatalf("condition %s = %+v, want %s %s", typ, c, status, reason)
	}
	return *c
}

func (e *env) wantNoCondition(typ string) {
	e.t.Helper()
	if c := e.condition(typ); c != nil {
		e.t.Fatalf("condition %s = %+v, want none", typ, c)
	}
}

func wantMT(t *testing.T, name string, got *metav1.Time, want time.Time) {
	t.Helper()
	if got == nil || !got.Time.Equal(want) {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
}

// wantManagedID is HBEP-3's canonical form of a managed entry, written out by
// hand (members sorted, no spaces) and hashed with the standard library. Names
// and revisions are ASCII here; the Unicode vector is checked against its
// committed file.
func wantManagedID(platform, name, revision string) string {
	canonical := fmt.Sprintf(`{"catalog_name":%q,"kind":"managed","opaque":"true","platform":%q,`, name, platform)
	if revision != "" {
		canonical += fmt.Sprintf(`"revision":%q,`, revision)
	}
	canonical += `"scheme":"v1"}`
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}

// wantEntry checks one catalog entry of the status, its managed ID included.
func wantEntry(t *testing.T, got v1alpha1.CatalogEntryStatus, listed bool, revision string, lastListed time.Time) {
	t.Helper()
	switch {
	case got.Listed != listed:
		t.Fatalf("entry %s: listed = %v, want %v", got.Name, got.Listed, listed)
	case revision == "" && got.Revision != nil, revision != "" && (got.Revision == nil || *got.Revision != revision):
		t.Fatalf("entry %s: revision = %v, want %q", got.Name, got.Revision, revision)
	case string(got.ManagedID) != wantManagedID(testPlatform, got.Name, revision):
		t.Fatalf("entry %s: managedID = %s, want the sha256 of its canonical form %s", got.Name, got.ManagedID, wantManagedID(testPlatform, got.Name, revision))
	}
	wantMT(t, "entry "+got.Name+" lastListedTime", got.LastListedTime, lastListed)
}

// waitFor polls cond until it holds, for a few seconds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// failReads makes the double's reads end with 504 after a short real wait: a
// manual stall that outlasts a 100 ms no-byte cut. It returns the function
// that undoes it.
func failReads(gw *llmlabtest.Server) (undo func()) {
	gw.SetNoByteCut(100 * time.Millisecond)
	gw.Stall(true)
	return func() {
		gw.Stall(false)
		gw.SetNoByteCut(0)
	}
}

// closedPort returns a port of the test block with nothing listening on it:
// the highest free one, taken and released at once, because the double and
// other packages' doubles take theirs from the bottom of the block.
func closedPort(t *testing.T) int {
	t.Helper()
	for p := portHi; p >= portLo; p-- {
		ln, err := llmlabtest.Listen(p, p)
		if err != nil {
			continue
		}
		if err := ln.Close(); err != nil {
			t.Fatal(err)
		}
		return p
	}
	t.Fatalf("no free port in %d-%d", portLo, portHi)
	return 0
}

// stub is a tiny gateway whose answer a test sets, for the answers the double
// does not give: a redirect, a body that is not JSON, and so on. It counts the
// requests by path.
type stub struct {
	srv  *http.Server
	url  string
	mu   sync.Mutex
	h    http.HandlerFunc
	hits map[string]int
	auth []string // the Authorization header of each request
}

func startStub(t *testing.T, h http.HandlerFunc) *stub {
	t.Helper()
	ln, err := llmlabtest.Listen(portLo, portHi)
	if err != nil {
		t.Fatal(err)
	}
	s := &stub{url: "http://" + ln.Addr().String(), h: h, hits: map[string]int{}}
	s.srv = &http.Server{Handler: http.HandlerFunc(s.serve), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = s.srv.Serve(ln) }()
	t.Cleanup(func() { _ = s.srv.Close() })
	return s
}

func (s *stub) serve(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.hits[r.URL.Path]++
	s.auth = append(s.auth, r.Header.Get("Authorization"))
	h := s.h
	s.mu.Unlock()
	h(w, r)
}

func (s *stub) set(h http.HandlerFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.h = h
}

func (s *stub) count(path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hits[path]
}

func (s *stub) authHeaders() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.auth...)
}

// baseURL is the stub's base URL as a credentials file names it.
func (s *stub) baseURL() string { return s.url + "/v1" }

// okListing is a well-formed listing of one model.
const okListing = `{"object":"list","data":[{"id":"example-a","object":"model"}]}`

// answer is a handler that replies with status and body.
func answer(status int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}
}

// applyDialHome plays the lifecycle controller: it sets DialHomeUnreachable on
// the named Provider through the contract's own server-side apply object,
// under its field manager. Copied from pkg/provider/fake's tests.
func applyDialHome(t *testing.T, c client.Client, providerName string, cond metav1.Condition) {
	t.Helper()
	u, err := v1alpha1.DialHomeUnreachableApply(providerName, cond)
	if err != nil {
		t.Fatal(err)
	}
	var prov v1alpha1.Provider
	if err := c.Get(context.Background(), client.ObjectKey{Name: providerName}, &prov); err != nil {
		t.Fatal(err)
	}
	// The fake client, unlike the API server, wants a resourceVersion on an
	// apply to the status subresource.
	u.SetResourceVersion(prov.ResourceVersion)
	if err := c.Status().Apply(context.Background(), client.ApplyConfigurationFromUnstructured(u),
		client.FieldOwner(v1alpha1.FieldManagerLifecycle), client.ForceOwnership); err != nil {
		t.Fatalf("apply DialHomeUnreachable: %v", err)
	}
}

// findDialHome returns the Provider's DialHomeUnreachable condition, and fails
// the test when it is gone.
func findDialHome(t *testing.T, prov *v1alpha1.Provider) metav1.Condition {
	t.Helper()
	c := apimeta.FindStatusCondition(prov.Status.Conditions, v1alpha1.ConditionDialHomeUnreachable)
	if c == nil {
		t.Fatalf("the Provider's DialHomeUnreachable condition is gone: %+v", prov.Status.Conditions)
	}
	return *c
}
