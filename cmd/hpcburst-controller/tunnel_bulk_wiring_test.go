package main

// addTunnel with and without --bulk-path (beta; HBEP-6, The bulk path; #448 part 3), through the real
// wiring: the plugin component on its kernel-chosen port, the tunnel-state component, both frps lists
// read over HTTP from two web servers of the test's (one per frps, each with its own password in its
// own file at the Host's own path, each with its own list), and a manager that is a stand-in for
// controller-runtime's, with the cache, the client and the event recorder the components use.
//
// What this adds to the unit tests of tunnel_bulk_test.go is the wiring itself: that the flag reaches the
// handler, the two Notifiers and the reader, that the second list is read at base+15 with frps-bulk's
// password and never without the flag, and that the two admissions are kept apart in the code that
// builds them. Ports: the kernel's (holdBlock), 127.0.0.1 only.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/frpsplugin"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The two web passwords, each in its own file: a read with the other's is answered 401.
const (
	tokenWebPassword = "c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1"
	bulkWebPassword  = "d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2d2"
)

// fakeFrpsWeb is one frps's web server, as far as the Reader uses it: GET /api/proxy/tcpmux with Basic
// auth. Its list and status are the test's, per frps.
type fakeFrpsWeb struct {
	password string
	ln       *acceptCounter
	mu       sync.Mutex
	list     []frpsplugin.Proxy
	status   int // when not 0, every authenticated list request is answered with it
	listed   atomic.Int64
	refused  atomic.Int64 // requests with another password
}

// acceptCounter counts the connections accepted on a listener, whatever is then said on them.
type acceptCounter struct {
	net.Listener
	accepted atomic.Int64
}

func (a *acceptCounter) Accept() (net.Conn, error) {
	c, err := a.Listener.Accept()
	if err == nil {
		a.accepted.Add(1)
	}
	return c, err
}

func serveFakeFrps(t *testing.T, ln net.Listener, password string) *fakeFrpsWeb {
	t.Helper()
	f := &fakeFrpsWeb{password: password, ln: &acceptCounter{Listener: ln}}
	srv := &http.Server{Handler: http.HandlerFunc(f.serve), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(f.ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return f
}

func (f *fakeFrpsWeb) serve(w http.ResponseWriter, r *http.Request) {
	user, pw, _ := r.BasicAuth()
	if user != agentapi.FrpsWebUser || pw != f.password || r.URL.Path != "/api/proxy/tcpmux" {
		f.refused.Add(1)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	f.listed.Add(1)
	f.mu.Lock()
	list, status := append([]frpsplugin.Proxy(nil), f.list...), f.status
	f.mu.Unlock()
	if status != 0 {
		http.Error(w, "frps failed", status)
		return
	}
	body, _ := json.Marshal(map[string]any{"proxies": append([]frpsplugin.Proxy{}, list...)})
	_, _ = w.Write(body)
}

func (f *fakeFrpsWeb) set(list []frpsplugin.Proxy, status int) {
	f.mu.Lock()
	f.list, f.status = list, status
	f.mu.Unlock()
}

// fakeCache is the manager's cache for the components: the stub's sync and informers.
type fakeCache struct {
	cache.Cache
	stub *stubCache
}

func (c fakeCache) WaitForCacheSync(ctx context.Context) bool { return c.stub.WaitForCacheSync(ctx) }
func (c fakeCache) GetInformer(ctx context.Context, obj client.Object, opts ...cache.InformerGetOption) (cache.Informer, error) {
	return c.stub.GetInformer(ctx, obj, opts...)
}

// capturedEvent is one event a recorder was given.
type capturedEvent struct {
	kind, namespace, name, eventType, reason, action, note string
}

type captureEvents struct {
	mu   sync.Mutex
	list []capturedEvent
}

func (c *captureEvents) Eventf(regarding, _ runtime.Object, eventtype, reason, action, note string, args ...any) {
	o := regarding.(client.Object)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.list = append(c.list, capturedEvent{kind: fmt.Sprintf("%T", regarding), namespace: o.GetNamespace(), name: o.GetName(),
		eventType: eventtype, reason: reason, action: action, note: fmt.Sprintf(note, args...)})
}

func (c *captureEvents) all() []capturedEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]capturedEvent(nil), c.list...)
}

// orphans returns the TunnelOrphaned events as (regarding kind, name, bulk?) triples, bulk when the
// note names frps-bulk.
func (c *captureEvents) orphans() []string {
	var out []string
	for _, e := range c.all() {
		if e.reason != v1alpha1.EventReasonTunnelOrphaned {
			continue
		}
		which := "first"
		if strings.Contains(e.note, "frps-bulk") {
			which = "bulk"
		}
		out = append(out, fmt.Sprintf("%s %s %s %s", which, orphanProxyOf(e.note), e.kind, e.name))
	}
	return out
}

// orphanProxyOf reads the proxy name out of a TunnelOrphaned note ("... lists the proxy NAME online ...").
func orphanProxyOf(note string) string {
	_, rest, _ := strings.Cut(note, "lists the proxy ")
	name, _, _ := strings.Cut(rest, " online")
	return name
}

// fakeMgr is the part of controller-runtime's manager that addTunnel uses. Any other method panics:
// the embedded interface is nil.
type fakeMgr struct {
	manager.Manager
	cache  cache.Cache
	cl     client.Client
	rec    events.EventRecorder
	runnbl []manager.Runnable
}

func (m *fakeMgr) GetCache() cache.Cache                        { return m.cache }
func (m *fakeMgr) GetClient() client.Client                     { return m.cl }
func (m *fakeMgr) GetAPIReader() client.Reader                  { return m.cl }
func (m *fakeMgr) GetEventRecorder(string) events.EventRecorder { return m.rec }
func (m *fakeMgr) Add(r manager.Runnable) error                 { m.runnbl = append(m.runnbl, r); return nil }

// tunnelWiring is addTunnel running against two fake frps web servers and a fake manager.
type tunnelWiring struct {
	t        *testing.T
	host     agentapi.Host
	cl       client.Client
	token    *fakeFrpsWeb
	bulk     *fakeFrpsWeb
	events   *captureEvents
	pluginAt string
}

// startWiring starts addTunnel's components for the host block the kernel gave: bulk says whether
// --bulk-path is on. The cache is open, the credentials are in place, and the timings are shortened.
func startWiring(t *testing.T, bulkPath bool, objs ...client.Object) *tunnelWiring {
	t.Helper()
	return startWiringEvery(t, bulkPath, 40*time.Millisecond, objs...)
}

// startWiringEvery is startWiring with the read period the test sets: a long one leaves the plugin's
// calls as the only thing that wakes a read.
func startWiringEvery(t *testing.T, bulkPath bool, period time.Duration, objs ...client.Object) *tunnelWiring {
	t.Helper()
	setVars(t, &feedPeriod, 20*time.Millisecond)
	setVars(t, &feedMinGap, time.Millisecond)
	setVars(t, &tunnelReadPeriod, period)
	setVars(t, &tunnelReadSettle, 2*time.Millisecond)
	setVars(t, &tunnelOrphanGrace, 300*time.Millisecond)

	base := holdBlock(t, []int{agentapi.FrpsPluginPortOffset}, []int{agentapi.FrpsWebPortOffset, agentapi.FrpsBulkWebPortOffset})
	host := agentapi.Host{StateDir: t.TempDir(), PortBase: base}
	for dir, pw := range map[string]string{agentapi.FrpsDirName: tokenWebPassword, agentapi.FrpsBulkDirName: bulkWebPassword} {
		if err := os.MkdirAll(filepath.Join(host.StateDir, dir), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := agentapi.WriteKeyFile(filepath.Join(host.StateDir, dir, agentapi.FrpsWebPasswordFileName), pw); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(host.CredentialsDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := checkCredentialsDir(host.CredentialsDir()); err != nil {
		t.Fatal(err)
	}
	writeUnitCredential(t, host.CredentialsDir(), "alloc-uid-1", credA)

	take := func(addr string) net.Listener {
		ln, err := heldAddrs.take(addr)
		if err != nil {
			t.Fatal(err)
		}
		return ln
	}
	w := &tunnelWiring{t: t, host: host, events: &captureEvents{}, pluginAt: host.FrpsPluginAddr()}
	w.token = serveFakeFrps(t, take(host.FrpsWebAddr()), tokenWebPassword)
	w.bulk = serveFakeFrps(t, take(host.FrpsBulkWebAddr()), bulkWebPassword)
	w.cl = tunnelScheme(t).WithObjects(objs...).Build()

	sc := newStubCache()
	sc.open()
	mgr := &fakeMgr{cache: fakeCache{stub: sc}, cl: w.cl, rec: w.events}
	plugin, err := addTunnel(mgr, host, nil, heldAddrs.take, eventcount.New(), bulkPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(mgr.runnbl) != 1 {
		t.Fatalf("addTunnel added %d components to the manager, want tunnel-state alone", len(mgr.runnbl))
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for _, r := range []manager.Runnable{plugin, mgr.runnbl[0]} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.Start(ctx); err != nil {
				t.Errorf("a tunnel component returned %v after its context ended, want nil", err)
			}
		}()
	}
	t.Cleanup(func() {
		cancel()
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Error("the tunnel components did not stop within 30 s of their context's end")
		}
	})
	// The plugin admits a login once its index has loaded.
	waitUntil(t, 30*time.Second, "the plugin to admit Allocation al-a's credential", func() bool {
		reject, _ := w.call(agentapi.FrpsPluginPath, "Login", map[string]any{"metas": map[string]string{"token": credA}})
		return !reject
	})
	return w
}

// call posts a plugin call to path as frps does. A 404 (a path nobody serves) is reported as a refusal
// with the reason "HTTP 404".
func (w *tunnelWiring) call(path, op string, content any) (reject bool, reason string) {
	w.t.Helper()
	body, err := json.Marshal(map[string]any{"version": "0.1.0", "op": op, "content": content})
	if err != nil {
		w.t.Fatal(err)
	}
	hc := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}}
	resp, err := hc.Post("http://"+w.pluginAt+path+"?op="+op, "application/json", bytes.NewReader(body))
	if err != nil {
		w.t.Fatalf("plugin call %s at %s: %v", op, path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return true, fmt.Sprintf("HTTP %d", resp.StatusCode)
	}
	var a struct {
		Reject       bool   `json:"reject"`
		RejectReason string `json:"reject_reason"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&a); err != nil {
		w.t.Fatalf("plugin call %s at %s: %v", op, path, err)
	}
	return a.Reject, a.RejectReason
}

func (w *tunnelWiring) newProxy(path, name string) (bool, string) {
	return w.call(path, "NewProxy", map[string]any{
		"user": map[string]any{"metas": map[string]string{"token": credA}}, "proxy_name": name, "proxy_type": "tcpmux",
		"custom_domains": []string{name}, "multiplexer": "httpconnect",
	})
}

// condition is the stored TunnelConnected of the named Instance as status/reason, "" when absent.
func (w *tunnelWiring) condition(name string) string {
	w.t.Helper()
	in := &v1alpha1.Instance{}
	if err := w.cl.Get(w.t.Context(), client.ObjectKey{Namespace: "team-a", Name: name}, in); err != nil {
		w.t.Fatal(err)
	}
	c := meta.FindStatusCondition(in.Status.Conditions, "TunnelConnected")
	if c == nil {
		return ""
	}
	return string(c.Status) + "/" + c.Reason
}

func (w *tunnelWiring) endInstance(name string) {
	w.t.Helper()
	// tunnel-state writes the Instance's TunnelConnected condition meanwhile (3 of 50 runs under load
	// met a newer resourceVersion), so each attempt reads the Instance again.
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		in := &v1alpha1.Instance{}
		if err := w.cl.Get(w.t.Context(), client.ObjectKey{Namespace: "team-a", Name: name}, in); err != nil {
			return err
		}
		in.Status.Phase = v1alpha1.InstanceEnded
		in.Status.EndReason = v1alpha1.ReasonDrained
		return w.cl.Status().Update(w.t.Context(), in)
	})
	if err != nil {
		w.t.Fatal(err)
	}
}

// bulkWiringObjects: Allocation al-a (credential credA) with three Instances placed on it.
func bulkWiringObjects() []client.Object {
	live := unitAllocation("al-a", "alloc-uid-1", v1alpha1.AllocationRunning)
	var objs = []client.Object{live}
	for _, in := range []struct{ name, uid string }{{"in-live", "uid-live"}, {"in-bulk", "uid-bulk-only"}, {"in-tok", "uid-tok-only"}} {
		i := unitInstance(in.name, in.uid, 1)
		i.Status.Phase = v1alpha1.InstanceReady
		objs = append(objs, i)
	}
	return objs
}

// TestNoBulkReadWithoutBulkPath (HBEP-6, Path state: "With the bulk path on, the second frps's list at
// base+15 is read"; without it nothing of that runs): a listener stands on base+15, where the
// controller would read, and counts the connections made to it. Without --bulk-path the controller
// reads the first list again and again and the listener sees no connection, and the bulk plugin path
// is 404. The control, with the flag: the same listener sees connections, answered with frps-bulk's
// own password, and the bulk plugin path is served.
func TestNoBulkReadWithoutBulkPath(t *testing.T) {
	for _, c := range []struct {
		name string
		bulk bool
	}{{"without --bulk-path", false}, {"the control: with --bulk-path", true}} {
		t.Run(c.name, func(t *testing.T) {
			w := startWiring(t, c.bulk, bulkWiringObjects()...)
			w.token.set(proxies(online, "uid-live"), 0)
			from := tunnelReads.Load()
			waitUntil(t, 30*time.Second, "several reads of the first list", func() bool { return tunnelReads.Load() >= from+5 && w.token.listed.Load() >= 5 })
			if c.bulk {
				waitUntil(t, 30*time.Second, "the second list to be read", func() bool { return w.bulk.listed.Load() >= 3 })
				if got := w.bulk.ln.accepted.Load(); got == 0 {
					t.Error("the listener at base+15 saw no connection with --bulk-path")
				}
				if got := w.bulk.refused.Load(); got != 0 || w.token.refused.Load() != 0 {
					t.Errorf("a read was refused: frps-bulk's web %d, the first's %d; each is read with its own password file", got, w.token.refused.Load())
				}
			} else if got := w.bulk.ln.accepted.Load(); got != 0 {
				t.Errorf("the listener at base+15 saw %d connections without --bulk-path, want none", got)
			}
			if got := w.token.ln.accepted.Load(); got == 0 {
				t.Error("the first list was never read")
			}
			// The bulk plugin path: served with the flag, 404 without.
			reject, reason := w.newProxy(agentapi.FrpsBulkPluginPath, "uid-live")
			if c.bulk && reject {
				t.Errorf("the bulk plugin path refused a valid NewProxy: %q", reason)
			}
			if !c.bulk && (!reject || reason != "HTTP 404") {
				t.Errorf("the bulk plugin path without the flag answered reject=%t %q, want HTTP 404", reject, reason)
			}
			// The first frps's path is served either way.
			if reject, reason := w.newProxy(agentapi.FrpsPluginPath, "uid-live"); reject {
				t.Errorf("the first plugin path refused a valid NewProxy: %q", reason)
			}
		})
	}
}

// TestTunnelConnectedFollowsTheTokenListOverTheWire is TestTunnelConnectedFollowsTheTokenListOnly through
// the wiring: two web servers with a list each. The stored condition of an Instance follows the
// first frps's list when the second one disagrees, is absent from the first and is unreadable (HTTP 500).
func TestTunnelConnectedFollowsTheTokenListOverTheWire(t *testing.T) {
	w := startWiring(t, true, bulkWiringObjects()...)
	steps := []struct {
		name         string
		tok, blk     []frpsplugin.Proxy
		tokSt, blkSt int
		want         string
	}{
		{"token offline, bulk online", proxies(offline, "uid-live"), proxies(online, "uid-live"), 0, 0, "False/ProxyOffline"},
		{"token online, bulk absent", proxies(online, "uid-live"), nil, 0, 0, "True/ProxyOnline"},
		{"token online, the bulk list unreadable", proxies(online, "uid-live"), nil, 0, http.StatusInternalServerError, "True/ProxyOnline"},
		{"token offline again, bulk online", proxies(offline, "uid-live"), proxies(online, "uid-live"), 0, 0, "False/ProxyOffline"},
		{"the control: token unreadable, bulk online", nil, proxies(online, "uid-live"), http.StatusInternalServerError, 0, "Unknown/ProxyListUnreadable"},
	}
	for _, s := range steps {
		w.token.set(s.tok, s.tokSt)
		w.bulk.set(s.blk, s.blkSt)
		// Reads of both lists after the change, so the answer is the one for these lists.
		tFrom, bFrom := w.token.listed.Load()+w.token.refused.Load(), w.bulk.listed.Load()+w.bulk.refused.Load()
		waitUntil(t, 30*time.Second, s.name+": the stored condition", func() bool { return w.condition("in-live") == s.want })
		waitUntil(t, 30*time.Second, s.name+": two more reads of each list", func() bool {
			return w.token.listed.Load()+w.token.refused.Load() >= tFrom+2 && w.bulk.listed.Load()+w.bulk.refused.Load() >= bFrom+2
		})
		if got := w.condition("in-live"); got != s.want {
			t.Errorf("%s: TunnelConnected = %q after more reads, want %q", s.name, got, s.want)
		}
	}
}

// TestBulkAndTokenAdmissionsAreSeparate (HBEP-6: "a plugin call carries nothing that names its frps, so
// the path is how a plugin keeps its state per frps"): a NewProxy at one path changes what that
// path's admissions record and nothing of the other's. Two proxies are admitted, one at each path;
// both Instances then end and both frps still list both proxies online, so each frps reports an orphan
// for each. An orphan's event regards the Allocation whose credential admitted the tunnel, as its OWN
// frps's plugin recorded it: the proxy admitted at the bulk path regards Allocation al-a on the bulk
// frps and the lifecycle Lease on the first, and the reverse for the other. With one record shared by
// the two, the first frps would name al-a for both.
func TestBulkAndTokenAdmissionsAreSeparate(t *testing.T) {
	w := startWiring(t, true, bulkWiringObjects()...)
	for path, uid := range map[string]string{agentapi.FrpsBulkPluginPath: "uid-bulk-only", agentapi.FrpsPluginPath: "uid-tok-only"} {
		if reject, reason := w.newProxy(path, uid); reject {
			t.Fatalf("NewProxy of %s at %s refused: %q", uid, path, reason)
		}
	}
	w.token.set(proxies(online, "uid-live", "uid-bulk-only", "uid-tok-only"), 0)
	w.bulk.set(proxies(online, "uid-live", "uid-bulk-only", "uid-tok-only"), 0)
	w.endInstance("in-bulk")
	w.endInstance("in-tok")

	lease := "*v1.Lease lifecycle"
	alloc := "*v1alpha1.Allocation al-a"
	want := []string{
		"first uid-bulk-only " + lease, // admitted at the bulk path: the first frps's plugin never heard of it
		"first uid-tok-only " + alloc,
		"bulk uid-bulk-only " + alloc,
		"bulk uid-tok-only " + lease,
	}
	waitUntil(t, 30*time.Second, "the four TunnelOrphaned events", func() bool { return len(w.events.orphans()) >= 4 })
	got := w.events.orphans()
	if !sameSet(got, want) {
		t.Errorf("TunnelOrphaned events (frps, proxy, regarding)\n got %q\nwant %q", got, want)
	}
	for _, e := range w.events.all() {
		if e.reason == v1alpha1.EventReasonTunnelOrphaned && (e.action != "degraded" || e.eventType != "Warning") {
			t.Errorf("event %+v: want HBEP-8's action degraded, a Warning", e)
		}
	}
}

func sameSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	seen := map[string]int{}
	for _, g := range got {
		seen[g]++
	}
	for _, w := range want {
		seen[w]--
	}
	for _, n := range seen {
		if n != 0 {
			return false
		}
	}
	return true
}

// TestBulkPluginPathRefusesWhatTheFirstRefuses, through the server: the same body is refused at both paths
// with the same reason, and an unknown path is 404.
func TestBulkPluginPathRefusesWhatTheFirstRefuses(t *testing.T) {
	w := startWiring(t, true, bulkWiringObjects()...)
	for _, path := range []string{agentapi.FrpsPluginPath, agentapi.FrpsBulkPluginPath} {
		if reject, reason := w.newProxy(path, "uid-nobody"); !reject || !strings.Contains(reason, "is not the UID of an Instance placed on this Allocation") {
			t.Errorf("%s: a proxy that is no Instance's UID: reject=%t %q, want the placement refusal", path, reject, reason)
		}
		if reject, reason := w.call(path, "Login", map[string]any{"metas": map[string]string{"token": credB}}); !reject || !strings.Contains(reason, "is not the credential of a live Allocation") {
			t.Errorf("%s: a login with an unknown token: reject=%t %q, want the credential refusal", path, reject, reason)
		}
	}
	if reject, reason := w.call("/frp/v1/handler/other", "Login", map[string]any{"metas": map[string]string{"token": credA}}); !reject || reason != "HTTP 404" {
		t.Errorf("an unknown plugin path: reject=%t %q, want HTTP 404", reject, reason)
	}
}

// TestBulkPluginCallWakesOneReadOfBothLists (HBEP-6, Path state: the list is read "about a second after each
// NewProxy and CloseProxy", and the bulk path's second list "the same way"): with the read period an hour, the
// only thing that reads is a plugin call. A CloseProxy posted to the BULK path, and then an accepted bulk
// NewProxy, each wake exactly one read, of the first list and of frps-bulk's, at the same settle time. The
// test waits on the two web servers' own counts of lists served, never on a time. A bulk Notifier that
// woke a trigger nobody reads, or none, leaves the counts where they were.
func TestBulkPluginCallWakesOneReadOfBothLists(t *testing.T) {
	w := startWiringEvery(t, true, time.Hour, bulkWiringObjects()...)
	// The read at the start, of both lists. The period is an hour, so no other read comes by itself.
	waitUntil(t, 30*time.Second, "the read at the start of both lists", func() bool { return w.token.listed.Load() >= 1 && w.bulk.listed.Load() >= 1 })
	for _, step := range []struct {
		name string
		call func() (bool, string)
	}{
		{"a CloseProxy at the bulk path", func() (bool, string) {
			return w.call(agentapi.FrpsBulkPluginPath, "CloseProxy", map[string]any{"user": map[string]any{"metas": map[string]string{"token": credA}}, "proxy_name": "uid-live"})
		}},
		{"an accepted NewProxy at the bulk path", func() (bool, string) { return w.newProxy(agentapi.FrpsBulkPluginPath, "uid-live") }},
	} {
		tFrom, bFrom, reads := w.token.listed.Load(), w.bulk.listed.Load(), tunnelReads.Load()
		if reject, reason := step.call(); reject {
			t.Fatalf("%s was refused: %q", step.name, reason)
		}
		waitUntil(t, 30*time.Second, step.name+": a read of both lists", func() bool { return w.token.listed.Load() > tFrom && w.bulk.listed.Load() > bFrom })
		if dt, db, dr := w.token.listed.Load()-tFrom, w.bulk.listed.Load()-bFrom, tunnelReads.Load()-reads; dt != 1 || db != 1 || dr != 1 {
			t.Errorf("%s woke %d reads of the first list, %d of frps-bulk's and %d passes; want one of each", step.name, dt, db, dr)
		}
	}
}
