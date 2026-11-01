package frpsplugin

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// fixture reads one of the request bodies frps v0.71.1 posts (testdata/, see frp_real_test.go), as
// a generic JSON object. Their token is tokA, the proxy is uid-a-1111.
func fixture(t *testing.T, name string) map[string]any {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// with returns a copy of m with the dotted path set to v; a nil v removes the key.
func with(t *testing.T, m map[string]any, path string, v any) map[string]any {
	t.Helper()
	b, _ := json.Marshal(m)
	var c map[string]any
	_ = json.Unmarshal(b, &c)
	keys := strings.Split(path, ".")
	cur := c
	for _, k := range keys[:len(keys)-1] {
		next, ok := cur[k].(map[string]any)
		if !ok {
			next = map[string]any{}
			cur[k] = next
		}
		cur = next
	}
	if v == nil {
		delete(cur, keys[len(keys)-1])
	} else {
		cur[keys[len(keys)-1]] = v
	}
	return c
}

// answer is what frps reads back.
type answer struct {
	Reject       bool   `json:"reject"`
	RejectReason string `json:"reject_reason"`
	Unchange     bool   `json:"unchange"`
}

// post sends frps's call: the body, and the op in the query as frps puts it.
func post(t *testing.T, h http.Handler, queryOp string, body []byte) (int, answer, string) {
	t.Helper()
	target := agentapi.FrpsPluginPath + "?version=0.1.0"
	if queryOp != "" {
		target += "&op=" + url.QueryEscape(queryOp)
	}
	req := httptest.NewRequest(http.MethodPost, target, bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	var a answer
	if w.Code == http.StatusOK {
		if err := json.Unmarshal(w.Body.Bytes(), &a); err != nil {
			t.Fatalf("the answer %q is not JSON: %v", w.Body.String(), err)
		}
	}
	return w.Code, a, w.Body.String()
}

func call(t *testing.T, h http.Handler, op string, m map[string]any) answer {
	t.Helper()
	b, _ := json.Marshal(m)
	code, a, raw := post(t, h, op, b)
	if code != http.StatusOK {
		t.Fatalf("%s: HTTP %d %s", op, code, raw)
	}
	if !a.Unchange {
		t.Errorf("%s: unchange is false in %s; the plugin never edits content", op, raw)
	}
	return a
}

const (
	uidA = "uid-a-1111" // the fixtures' proxy, placed on alloc-a
	uidB = "uid-b-2222" // placed on alloc-b
)

// syncedIndex has two Allocations, a (tokA, uidA) and b (tokB, uidB).
func syncedIndex(t *testing.T) *MemIndex {
	t.Helper()
	idx := &MemIndex{}
	if err := idx.Replace(Snapshot{Allocations: []AllocationEntry{
		{UID: "alloc-a", Credential: tokA, Instances: []string{uidA}},
		{UID: "alloc-b", Credential: tokB, Instances: []string{uidB}},
	}}); err != nil {
		t.Fatal(err)
	}
	idx.MarkSynced()
	return idx
}

func newTestHandler(t *testing.T, idx Index, ev Notifier) *Handler {
	t.Helper()
	h, err := NewHandler(idx, Options{Notifier: ev})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestLoginAcceptsALiveAllocationsCredential(t *testing.T) {
	h := newTestHandler(t, syncedIndex(t), nil)
	if a := call(t, h, OpLogin, fixture(t, "login")); a.Reject {
		t.Fatalf("a login with Allocation a's credential was refused: %q", a.RejectReason)
	}
	if a := call(t, h, OpLogin, with(t, fixture(t, "login"), "content.metas.token", tokB)); a.Reject {
		t.Fatalf("a login with Allocation b's credential was refused: %q", a.RejectReason)
	}
}

func TestLoginRefusals(t *testing.T) {
	for _, c := range []struct {
		name string
		body map[string]any
	}{
		{"an unknown credential of a key's form", with(t, fixture(t, "login"), "content.metas.token", tokC)},
		{"no token", with(t, fixture(t, "login"), "content.metas.token", nil)},
		{"no metas", with(t, fixture(t, "login"), "content.metas", nil)},
		{"an empty token", with(t, fixture(t, "login"), "content.metas.token", "")},
		{"a token of another form", with(t, fixture(t, "login"), "content.metas.token", "tok-test")},
		{"the credential in capitals", with(t, fixture(t, "login"), "content.metas.token", strings.ToUpper(tokA))},
		{"the credential with a newline", with(t, fixture(t, "login"), "content.metas.token", tokA+"\n")},
		{"the credential in another key", with(t, with(t, fixture(t, "login"), "content.metas.token", nil), "content.metas.password", tokA)},
		{"a token that is a number", with(t, fixture(t, "login"), "content.metas.token", 7)},
		{"no content", with(t, fixture(t, "login"), "content", nil)},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newTestHandler(t, syncedIndex(t), nil)
			a := call(t, h, OpLogin, c.body)
			if !a.Reject || a.RejectReason == "" {
				t.Fatalf("answer %+v, want a refusal with a reason", a)
			}
		})
	}
}

func TestLoginOfAnEndedAllocationIsRefused(t *testing.T) {
	idx := syncedIndex(t)
	h := newTestHandler(t, idx, nil)
	if a := call(t, h, OpLogin, fixture(t, "login")); a.Reject {
		t.Fatalf("before it ended: %q", a.RejectReason)
	}
	// The feeder lists live Allocations only: ended means absent.
	if err := idx.Replace(Snapshot{Allocations: []AllocationEntry{{UID: "alloc-b", Credential: tokB, Instances: []string{uidB}}}}); err != nil {
		t.Fatal(err)
	}
	if a := call(t, h, OpLogin, fixture(t, "login")); !a.Reject {
		t.Fatal("a login with the credential of an Allocation that ended was accepted")
	}
}

func TestLoginAndNewProxyAreRefusedUntilTheIndexHasSynced(t *testing.T) {
	idx := &MemIndex{}
	if err := idx.Replace(Snapshot{Allocations: []AllocationEntry{{UID: "alloc-a", Credential: tokA, Instances: []string{uidA}}}}); err != nil {
		t.Fatal(err)
	}
	h := newTestHandler(t, idx, nil)
	// The state is loaded, but the controller's caches have not synced: nothing is decided.
	if a := call(t, h, OpLogin, fixture(t, "login")); !a.Reject || a.RejectReason != reasonNotSynced {
		t.Fatalf("Login before the sync: %+v, want a refusal with %q", a, reasonNotSynced)
	}
	if a := call(t, h, OpNewProxy, fixture(t, "newproxy")); !a.Reject || a.RejectReason != reasonNotSynced {
		t.Fatalf("NewProxy before the sync: %+v, want a refusal with %q", a, reasonNotSynced)
	}
	idx.MarkSynced()
	if a := call(t, h, OpLogin, fixture(t, "login")); a.Reject {
		t.Fatalf("Login after the sync: %q", a.RejectReason)
	}
	if a := call(t, h, OpNewProxy, fixture(t, "newproxy")); a.Reject {
		t.Fatalf("NewProxy after the sync: %q", a.RejectReason)
	}
}

func TestNewProxyAcceptsAnInstancePlacedOnTheTokensAllocation(t *testing.T) {
	ev := &events{}
	h := newTestHandler(t, syncedIndex(t), ev)
	if a := call(t, h, OpNewProxy, fixture(t, "newproxy")); a.Reject {
		t.Fatalf("the proxy of an Instance placed on the Allocation was refused: %q", a.RejectReason)
	}
	if got := ev.all(); len(got) != 1 || got[0] != "NewProxy "+uidA {
		t.Errorf("the Notifier heard %v, want [NewProxy %s]", got, uidA)
	}
}

func TestNewProxyRefusals(t *testing.T) {
	base := fixture(t, "newproxy")
	for _, c := range []struct {
		name string
		body map[string]any
		why  string
	}{
		{"another proxy type", with(t, base, "content.proxy_type", "http"), reasonProxyType},
		{"tcp", with(t, base, "content.proxy_type", "tcp"), reasonProxyType},
		{"no proxy type", with(t, base, "content.proxy_type", nil), reasonProxyType},
		{"another multiplexer", with(t, base, "content.multiplexer", "other"), reasonProxyType},
		{"no multiplexer", with(t, base, "content.multiplexer", nil), reasonProxyType},
		{"no custom domain", with(t, base, "content.custom_domains", nil), reasonDomains},
		{"an empty list of custom domains", with(t, base, "content.custom_domains", []any{}), reasonDomains},
		{"two custom domains", with(t, base, "content.custom_domains", []any{uidA, uidA}), reasonDomains},
		{"a second domain besides the UID", with(t, base, "content.custom_domains", []any{uidA, "other.example"}), reasonDomains},
		{"a custom domain that is not the proxy name", with(t, base, "content.custom_domains", []any{uidB}), reasonDomains},
		{"a UID that no Instance has", with(t, with(t, base, "content.proxy_name", "uid-nobody"), "content.custom_domains", []any{"uid-nobody"}), reasonNotPlaced},
		{"an Instance placed on another Allocation", with(t, with(t, base, "content.proxy_name", uidB), "content.custom_domains", []any{uidB}), reasonNotPlaced},
		{"an empty proxy name", with(t, with(t, base, "content.proxy_name", ""), "content.custom_domains", []any{""}), reasonNotPlaced},
		{"a subdomain", with(t, base, "content.subdomain", "s"), reasonExtraFields},
		{"a group", with(t, base, "content.group", "g"), reasonExtraFields},
		{"an http user", with(t, base, "content.http_user", "u"), reasonExtraFields},
		{"a route by http user", with(t, base, "content.route_by_http_user", "r"), reasonExtraFields},
		{"an unknown credential", with(t, base, "content.user.metas.token", tokC), reasonCredential},
		{"no session token", with(t, base, "content.user.metas.token", nil), reasonCredential},
		{"no session metas", with(t, base, "content.user.metas", nil), reasonCredential},
		{"no user", with(t, base, "content.user", nil), reasonCredential},
		// The proxy's own metas are the proxy's metadata, chosen by frpc per proxy; the session's
		// credential is the login's, in user.metas.
		{"a valid token only in the proxy's own metas", with(t, with(t, base, "content.user.metas.token", tokC), "content.metas.token", tokA), reasonCredential},
	} {
		t.Run(c.name, func(t *testing.T) {
			ev := &events{}
			h := newTestHandler(t, syncedIndex(t), ev)
			a := call(t, h, OpNewProxy, c.body)
			if !a.Reject || a.RejectReason != c.why {
				t.Fatalf("answer %+v, want a refusal with %q", a, c.why)
			}
			if got := ev.all(); len(got) != 0 {
				t.Errorf("the Notifier heard %v for a refused proxy", got)
			}
		})
	}
}

func TestNewProxyOfAnotherAllocationsInstanceIsAcceptedWithThatAllocationsToken(t *testing.T) {
	h := newTestHandler(t, syncedIndex(t), nil)
	b := with(t, with(t, with(t, fixture(t, "newproxy"), "content.proxy_name", uidB), "content.custom_domains", []any{uidB}), "content.user.metas.token", tokB)
	if a := call(t, h, OpNewProxy, b); a.Reject {
		t.Fatalf("Allocation b's own Instance with b's token: %q", a.RejectReason)
	}
}

func TestCloseProxyIsAcceptedAndTriggersARead(t *testing.T) {
	// Even before the sync and with a token nobody has: it is a notification, and the list is the truth.
	for _, idx := range []Index{syncedIndex(t), &MemIndex{}} {
		ev := &events{}
		h := newTestHandler(t, idx, ev)
		if a := call(t, h, OpCloseProxy, fixture(t, "closeproxy")); a.Reject {
			t.Fatalf("CloseProxy was refused: %q", a.RejectReason)
		}
		if got := ev.all(); len(got) != 1 || got[0] != "CloseProxy "+uidA {
			t.Errorf("the Notifier heard %v, want [CloseProxy %s]", got, uidA)
		}
	}
	// With no Notifier at all it is still accepted.
	h := newTestHandler(t, syncedIndex(t), nil)
	if a := call(t, h, OpCloseProxy, fixture(t, "closeproxy")); a.Reject {
		t.Fatalf("CloseProxy with no Notifier: %q", a.RejectReason)
	}
}

func TestOtherOpsAreRefused(t *testing.T) {
	for _, op := range []string{"Ping", "NewWorkConn", "NewUserConn", "login", "LOGIN", "", "Login ", "Logout"} {
		t.Run(op, func(t *testing.T) {
			ev := &events{}
			h := newTestHandler(t, syncedIndex(t), ev)
			// A well-formed call of that op, with a valid session in it.
			a := call(t, h, op, with(t, fixture(t, "login"), "op", op))
			if !a.Reject || !strings.Contains(a.RejectReason, "not served") {
				t.Fatalf("op %q: %+v, want a refusal that says it is not served", op, a)
			}
			if len(ev.all()) != 0 {
				t.Errorf("the Notifier heard %v", ev.all())
			}
		})
	}
}

func TestMalformedCallsAreRefused(t *testing.T) {
	h := newTestHandler(t, syncedIndex(t), &events{})
	huge, _ := json.Marshal(with(t, fixture(t, "login"), "content.metas.pad", strings.Repeat("x", maxBody)))
	login, _ := json.Marshal(fixture(t, "login"))
	for _, c := range []struct {
		name, queryOp string
		body          []byte
	}{
		{"an empty body", "Login", nil},
		{"text", "Login", []byte("hello")},
		{"a truncated body", "Login", login[:len(login)/2]},
		{"an array", "Login", []byte(`[1,2]`)},
		{"a body larger than the limit", "Login", huge},
		{"content that is a string", "Login", []byte(`{"op":"Login","content":"x"}`)},
		{"content that is null", "Login", []byte(`{"op":"Login","content":null}`)},
		{"NewProxy content that is a list", "NewProxy", []byte(`{"op":"NewProxy","content":[1]}`)},
		{"NewProxy with metas of the wrong type", "NewProxy", []byte(`{"op":"NewProxy","content":{"user":{"metas":"x"}}}`)},
		{"CloseProxy content that is a number", "CloseProxy", []byte(`{"op":"CloseProxy","content":7}`)},
		{"an op in the body that differs from the query", "NewProxy", login},
		{"no op at all", "", []byte(`{"content":{}}`)},
	} {
		t.Run(c.name, func(t *testing.T) {
			code, a, raw := post(t, h, c.queryOp, c.body)
			if code != http.StatusOK {
				t.Fatalf("HTTP %d %s, want a 200 with a refusal (frps reads the reason from it)", code, raw)
			}
			if !a.Reject || a.RejectReason == "" || !a.Unchange {
				t.Fatalf("answer %+v, want a refusal with a reason and unchange", a)
			}
		})
	}
}

func TestOnlyPostToThePluginPathIsServed(t *testing.T) {
	h := newTestHandler(t, syncedIndex(t), nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, agentapi.FrpsPluginPath, nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET: %d, want 405", w.Code)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/other", strings.NewReader("{}")))
	if w.Code != http.StatusNotFound {
		t.Errorf("POST /other: %d, want 404", w.Code)
	}
}

// TestRefusalReasonsNeverContainTheToken: the reasons reach frpc and frps's log (HBEP-6), so none
// may repeat a credential, whether it was right or wrong.
func TestRefusalReasonsNeverContainTheToken(t *testing.T) {
	const secret = "d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4"
	idx := &MemIndex{}
	if err := idx.Replace(Snapshot{Allocations: []AllocationEntry{{UID: "alloc-a", Credential: secret, Instances: []string{uidA}}}}); err != nil {
		t.Fatal(err)
	}
	h := newTestHandler(t, idx, nil)
	check := func(what string, b map[string]any) {
		t.Helper()
		_, a, raw := post(t, h, b["op"].(string), mustJSON(b))
		if strings.Contains(raw, secret) || strings.Contains(raw, tokA) {
			t.Errorf("%s: the answer contains a token: %s", what, raw)
		}
		if !a.Reject {
			t.Errorf("%s was accepted", what)
		}
	}
	login := with(t, fixture(t, "login"), "content.metas.token", secret)
	np := with(t, fixture(t, "newproxy"), "content.user.metas.token", secret)
	check("a login before the sync", login) // refused: not synced
	check("a proxy before the sync", np)    // refused: not synced
	idx.MarkSynced()
	check("a login with a wrong token", fixture(t, "login")) // tokA is not secret's Allocation
	check("a proxy with a wrong token", fixture(t, "newproxy"))
	check("a proxy with the right token and a wrong shape", with(t, np, "content.proxy_type", "tcp"))
	check("a proxy of an unplaced Instance", with(t, np, "content.proxy_name", "zzz"))
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// slowIndex wraps an Index so that chosen methods wait, ignoring their context as an Index that
// blocks on something else would, until the test ends.
type slowIndex struct {
	Index
	wait    map[string]bool // "Synced", "AllocationForToken", "InstanceOnAllocation"
	release chan struct{}
	calls   chan string
}

func newSlowIndex(t *testing.T, inner Index, methods ...string) *slowIndex {
	s := &slowIndex{Index: inner, wait: map[string]bool{}, release: make(chan struct{}), calls: make(chan string, 64)}
	for _, m := range methods {
		s.wait[m] = true
	}
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(s.release) }) })
	return s
}

func (s *slowIndex) block(m string) {
	s.calls <- m
	if s.wait[m] {
		select {
		case <-s.release:
		case <-time.After(3 * time.Second):
		}
	}
}

func (s *slowIndex) Synced() bool { s.block("Synced"); return s.Index.Synced() }
func (s *slowIndex) AllocationForToken(ctx context.Context, tok string) (string, bool) {
	s.block("AllocationForToken")
	return s.Index.AllocationForToken(ctx, tok)
}
func (s *slowIndex) InstanceOnAllocation(ctx context.Context, a, i string) bool {
	s.block("InstanceOnAllocation")
	return s.Index.InstanceOnAllocation(ctx, a, i)
}

// TestABlockedIndexIsAnsweredWithARefusalWithinOneSecond: frps calls NewProxy on the session's own
// read loop with no timeout (HBEP-6), so whatever the Index does, the answer comes within 1 s. The
// index here waits for 3 s, ignoring its context; the call goes over real HTTP, so the measure is
// what frps would see.
func TestABlockedIndexIsAnsweredWithARefusalWithinOneSecond(t *testing.T) {
	for _, c := range []struct {
		method string
		op     string
		body   map[string]any
	}{
		{"Synced", OpLogin, fixture(t, "login")},
		{"AllocationForToken", OpLogin, fixture(t, "login")},
		{"Synced", OpNewProxy, fixture(t, "newproxy")},
		{"AllocationForToken", OpNewProxy, fixture(t, "newproxy")},
		{"InstanceOnAllocation", OpNewProxy, fixture(t, "newproxy")},
	} {
		t.Run(c.op+" blocked in "+c.method, func(t *testing.T) {
			idx := newSlowIndex(t, syncedIndex(t), c.method)
			srv := httptest.NewServer(newTestHandler(t, idx, nil))
			defer srv.Close()
			start := time.Now()
			resp, err := (&http.Client{Timeout: 5 * time.Second}).Post(srv.URL+agentapi.FrpsPluginPath+"?op="+c.op, "application/json", bytes.NewReader(mustJSON(c.body)))
			took := time.Since(start)
			if err != nil {
				t.Fatalf("after %s: %v", took, err)
			}
			defer resp.Body.Close()
			var a answer
			if err := json.NewDecoder(resp.Body).Decode(&a); err != nil {
				t.Fatal(err)
			}
			if took >= time.Second {
				t.Errorf("the answer took %s, want under 1 s", took)
			}
			if !a.Reject || !strings.Contains(a.RejectReason, "could not decide") {
				t.Errorf("answer %+v, want a refusal for the missed deadline", a)
			}
			if len(idx.calls) == 0 {
				t.Error("the Index was never called")
			}
		})
	}
}

// blockingNotifier waits, as a Notifier that breaks its contract would.
type blockingNotifier struct{ release chan struct{} }

func (b blockingNotifier) ProxyChanged(string, string) {
	select {
	case <-b.release:
	case <-time.After(3 * time.Second):
	}
}

func TestACloseProxyWhoseNotifierBlocksIsStillAnsweredWithinOneSecond(t *testing.T) {
	n := blockingNotifier{release: make(chan struct{})}
	t.Cleanup(func() { close(n.release) })
	srv := httptest.NewServer(newTestHandler(t, syncedIndex(t), n))
	defer srv.Close()
	start := time.Now()
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Post(srv.URL+agentapi.FrpsPluginPath+"?op=CloseProxy", "application/json", bytes.NewReader(mustJSON(fixture(t, "closeproxy"))))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var a answer
	if err := json.NewDecoder(resp.Body).Decode(&a); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took >= time.Second {
		t.Errorf("the answer took %s, want under 1 s", took)
	}
	if a.Reject {
		t.Errorf("CloseProxy was refused: %q (a notification is accepted whatever the Notifier does)", a.RejectReason)
	}
}

// TestLookupsStuckInTheIndexFillTheSlotsAndThenCallsAreRefusedAtOnce: a lookup that never returns
// keeps a goroutine; the cap keeps them from piling up, and a full house refuses without waiting.
func TestStuckLookupsFillTheSlotsAndThenCallsAreRefusedAtOnce(t *testing.T) {
	idx := newSlowIndex(t, syncedIndex(t), "AllocationForToken")
	h, err := NewHandler(idx, Options{MaxInFlight: 2})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = call(t, h, OpLogin, fixture(t, "login")) // refused at the deadline
		}()
	}
	for i := 0; i < 2; i++ { // both lookups are inside the Index now
		select {
		case m := <-idx.calls:
			if m == "Synced" {
				i--
			}
		case <-time.After(2 * time.Second):
			t.Fatal("the lookups never started")
		}
	}
	start := time.Now()
	a := call(t, h, OpLogin, fixture(t, "login"))
	if !a.Reject || a.RejectReason != reasonBusy {
		t.Errorf("with the slots full: %+v, want a refusal with %q", a, reasonBusy)
	}
	if took := time.Since(start); took > 200*time.Millisecond {
		t.Errorf("the refusal took %s, want it at once", took)
	}
	wg.Wait()
	// Still stuck after the callers were answered, so the slots stay taken until the lookups return.
	if a := call(t, h, OpLogin, fixture(t, "login")); a.RejectReason != reasonBusy {
		t.Errorf("after the callers were answered but the lookups still stuck: %+v", a)
	}
}

// panicOnceIndex panics in its first lookup and answers normally after that.
type panicOnceIndex struct {
	Index
	n atomic.Int32
}

func (p *panicOnceIndex) AllocationForToken(ctx context.Context, tok string) (string, bool) {
	if p.n.Add(1) == 1 {
		panic("boom")
	}
	return p.Index.AllocationForToken(ctx, tok)
}

// TestAPanicInTheIndexIsARefusalAndLeaksNoSlot: with one slot, a second call through the same handler
// is accepted only if the panicking call gave its slot back and the handler still serves.
func TestAPanicInTheIndexIsARefusalAndLeaksNoSlot(t *testing.T) {
	h, err := NewHandler(&panicOnceIndex{Index: syncedIndex(t)}, Options{MaxInFlight: 1})
	if err != nil {
		t.Fatal(err)
	}
	a := call(t, h, OpLogin, fixture(t, "login"))
	if !a.Reject || a.RejectReason != reasonInternal {
		t.Fatalf("answer %+v, want a refusal with %q", a, reasonInternal)
	}
	for i := 0; i < 3; i++ { // the same handler, the same single slot
		if a := call(t, h, OpLogin, fixture(t, "login")); a.Reject {
			t.Fatalf("call %d after the panic: %q", i+2, a.RejectReason)
		}
	}
}

// stalledBody opens a connection to srv, sends a Login's headers announcing a body of 4000 bytes and
// the start of it, and then sends nothing more.
func stalledBody(t *testing.T, srv *httptest.Server) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if _, err := io.WriteString(c, "POST "+agentapi.FrpsPluginPath+"?version=0.1.0&op=Login HTTP/1.1\r\nHost: plugin\r\n"+
		"Content-Type: application/json\r\nContent-Length: 4000\r\n\r\n"+`{"version":"0.1.0","op":"Login","content":{"metas":{`); err != nil {
		t.Fatal(err)
	}
	return c
}

// answeredOrClosed waits for the server's answer on c, or for it to close the connection, and
// returns how long that took and the answer ("" when it closed without one). A server that does
// neither is waited on for 3 s, so that a handler without a deadline fails the test and does not hang it.
func answeredOrClosed(t *testing.T, c net.Conn) (time.Duration, answer, bool) {
	t.Helper()
	start := time.Now()
	_ = c.SetReadDeadline(start.Add(3 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	took := time.Since(start)
	if err != nil {
		return took, answer{}, false
	}
	defer resp.Body.Close()
	var a answer
	if err := json.NewDecoder(resp.Body).Decode(&a); err != nil {
		t.Fatalf("the answer is not JSON: %v", err)
	}
	return took, a, true
}

// TestAStalledBodyIsAnsweredOrClosedWithinOneSecond: the deadline starts when the handler does and
// covers the read of the body. The server here has no timeouts of its own, so only the handler stops
// a client that sends its headers and then goes quiet.
func TestAStalledBodyIsAnsweredOrClosedWithinOneSecond(t *testing.T) {
	srv := httptest.NewServer(newTestHandler(t, syncedIndex(t), nil))
	defer srv.Close()
	c := stalledBody(t, srv)
	took, a, answered := answeredOrClosed(t, c)
	t.Logf("answered=%v after %s", answered, took)
	if took >= time.Second {
		t.Fatalf("after %s the server had neither answered nor closed the connection", took)
	}
	if answered && (!a.Reject || a.RejectReason == "") {
		t.Errorf("answer %+v, want a refusal with a reason", a)
	}
}

// TestStalledBodiesTakeSlotsAndGiveThemBack: a stalled body holds a slot from the start of its read
// (so a flood of them counts against the cap), and gives it back when the deadline ends it (so it
// does not hold a well-formed Login out for good).
func TestStalledBodiesTakeSlotsAndGiveThemBack(t *testing.T) {
	h, err := NewHandler(syncedIndex(t), Options{MaxInFlight: 2})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	defer srv.Close()
	login := mustJSON(fixture(t, "login"))
	client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	postLogin := func() answer {
		t.Helper()
		resp, err := client.Post(srv.URL+agentapi.FrpsPluginPath+"?op=Login", "application/json", bytes.NewReader(login))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var a answer
		if err := json.NewDecoder(resp.Body).Decode(&a); err != nil {
			t.Fatal(err)
		}
		return a
	}
	if a := postLogin(); a.Reject {
		t.Fatalf("a well-formed Login with nothing stalled: %q", a.RejectReason)
	}
	conns := []net.Conn{stalledBody(t, srv), stalledBody(t, srv)}
	// While the two stalled bodies are being read, they hold both slots.
	waitFor(t, time.Second, "the stalled bodies never took the slots", func() bool {
		return postLogin().RejectReason == reasonBusy
	})
	for _, c := range conns {
		if took, _, _ := answeredOrClosed(t, c); took >= time.Second {
			t.Errorf("a stalled body was still held after %s", took)
		}
	}
	// Both have been ended: the slots are free and a well-formed Login is accepted.
	if a := postLogin(); a.Reject {
		t.Fatalf("a well-formed Login after the stalled bodies ended: %q", a.RejectReason)
	}
}

func TestNewHandlerChecksItsOptions(t *testing.T) {
	idx := syncedIndex(t)
	for _, c := range []struct {
		name string
		idx  Index
		o    Options
	}{
		{"no index", nil, Options{}},
		{"a deadline of one second", idx, Options{Deadline: time.Second}},
		{"a deadline above one second", idx, Options{Deadline: 2 * time.Second}},
		{"a negative deadline", idx, Options{Deadline: -time.Millisecond}},
		{"a negative cap", idx, Options{MaxInFlight: -1}},
	} {
		t.Run(c.name, func(t *testing.T) {
			if h, err := NewHandler(c.idx, c.o); err == nil {
				t.Fatalf("NewHandler accepted %+v: %v", c.o, h)
			}
		})
	}
	if _, err := NewHandler(idx, Options{Deadline: 999 * time.Millisecond}); err != nil {
		t.Errorf("a deadline of 999 ms: %v", err)
	}
}
