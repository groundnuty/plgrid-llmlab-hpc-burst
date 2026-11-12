package frpsplugin

// The plugin's second path, for the bulk path's second frps (beta; HBEP-6, The bulk path: "Admission
// and the front's plugin", #448 part 3). A plugin call carries nothing that names its frps, so each
// frps posts to a path of its own and a Handler for each keeps its state per frps: the same
// admission rules, an Index shared, a Notifier of its own.
//
// The two paths are typed from HBEP-6, not read from agentapi's constants: the first frps keeps
// /frp/v1/handler, the second posts to /frp/v1/handler/bulk.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

const (
	tokenPluginPath = "/frp/v1/handler"
	bulkPluginPath  = "/frp/v1/handler/bulk"
)

// postTo is post for a plugin served at any path.
func postTo(t *testing.T, h http.Handler, path, queryOp string, body []byte) (int, answer) {
	t.Helper()
	target := path + "?version=0.1.0"
	if queryOp != "" {
		target += "&op=" + url.QueryEscape(queryOp)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, target, bytes.NewReader(body)))
	var a answer
	if w.Code == http.StatusOK {
		if err := json.Unmarshal(w.Body.Bytes(), &a); err != nil {
			t.Fatalf("the answer %q is not JSON: %v", w.Body.String(), err)
		}
	}
	return w.Code, a
}

func callAt(t *testing.T, h http.Handler, path, op string, m map[string]any) answer {
	t.Helper()
	b, _ := json.Marshal(m)
	code, a := postTo(t, h, path, op, b)
	if code != http.StatusOK {
		t.Fatalf("%s at %s: HTTP %d", op, path, code)
	}
	return a
}

// bulkHandler is a Handler for the second frps's path over the same Index, with a Notifier of its
// own.
func bulkHandler(t *testing.T, idx Index, ev Notifier) *Handler {
	t.Helper()
	h, err := NewHandler(idx, Options{Notifier: ev, Path: bulkPluginPath})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// TestHandlerServesOnlyItsPath: each handler answers 404 at the other's path (HBEP-6: each frps
// posts to its own path), and a call at its own path is decided. The control is the own path: a
// handler that served every path, or none, would pass half of this.
func TestHandlerServesOnlyItsPath(t *testing.T) {
	token := newTestHandler(t, syncedIndex(t), nil)
	bulk := bulkHandler(t, syncedIndex(t), nil)
	login := fixture(t, "login")
	body, _ := json.Marshal(login)
	for _, c := range []struct {
		name     string
		h        http.Handler
		own, not string
	}{
		{"the token handler", token, tokenPluginPath, bulkPluginPath},
		{"the bulk handler", bulk, bulkPluginPath, tokenPluginPath},
	} {
		t.Run(c.name, func(t *testing.T) {
			if code, a := postTo(t, c.h, c.own, OpLogin, body); code != http.StatusOK || a.Reject {
				t.Fatalf("at its own path %s: HTTP %d, %+v; want 200 and an accepted login", c.own, code, a)
			}
			if code, _ := postTo(t, c.h, c.not, OpLogin, body); code != http.StatusNotFound {
				t.Errorf("at the other's path %s: HTTP %d, want 404", c.not, code)
			}
			// The bulk path's name is a prefix of nothing and an extension of the token's: neither
			// reaches the other by a trailing slash or a longer name.
			for _, p := range []string{c.own + "/", c.own + "x", "/frp/v1", "/"} {
				if code, _ := postTo(t, c.h, p, OpLogin, body); code != http.StatusNotFound {
					t.Errorf("at %s: HTTP %d, want 404 (the path is matched whole)", p, code)
				}
			}
		})
	}
}

// TestDefaultPathIsTheFirstFrpsPath: a Handler with no Path serves what it served before the bulk
// path, /frp/v1/handler (HBEP-6, Admission), so the first frps's configuration is untouched.
func TestDefaultPathIsTheFirstFrpsPath(t *testing.T) {
	h, err := NewHandler(syncedIndex(t), Options{})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(fixture(t, "login"))
	if code, a := postTo(t, h, tokenPluginPath, OpLogin, b); code != http.StatusOK || a.Reject {
		t.Errorf("the default path %s: HTTP %d, %+v; want an accepted login", tokenPluginPath, code, a)
	}
	if code, _ := postTo(t, h, bulkPluginPath, OpLogin, b); code != http.StatusNotFound {
		t.Errorf("the default Handler answered at the bulk path: HTTP %d, want 404", code)
	}
}

// TestNewHandlerRefusesAPathWithoutASlash: a path that no request line can carry would serve
// nothing, and the plugin would then refuse every login of that frps in silence.
func TestNewHandlerRefusesAPathWithoutASlash(t *testing.T) {
	if h, err := NewHandler(syncedIndex(t), Options{Path: "frp/v1/handler/bulk"}); err == nil {
		t.Fatalf("NewHandler accepted a path with no leading slash: %v", h)
	}
}

// TestBulkHandlerRefusesWhatTheTokenHandlerRefuses: the admission rules are the same on both frps
// (HBEP-6: the token, an Instance of that Allocation, the name equal to the UID). Each refusal of
// HBEP-6's Test plan, "Plugin refusals", is posted to the bulk path, with the answer the token path
// gives the same body beside it (the same Index), and none of them reaches the bulk Notifier. The
// control, last: the valid call is accepted on the bulk path and the Notifier hears it.
func TestBulkHandlerRefusesWhatTheTokenHandlerRefuses(t *testing.T) {
	idx := syncedIndex(t)
	tokenEv, bulkEv := &events{}, &events{}
	token := newTestHandler(t, idx, tokenEv)
	bulk := bulkHandler(t, idx, bulkEv)
	base := fixture(t, "newproxy")
	for _, c := range []struct {
		name string
		body map[string]any
	}{
		{"a proxy of type tcp", with(t, base, "content.proxy_type", "tcp")},
		{"tcpmux with another multiplexer", with(t, base, "content.multiplexer", "other")},
		{"two custom domains", with(t, base, "content.custom_domains", []any{uidA, uidA})},
		{"a domain that is not the proxy name", with(t, base, "content.custom_domains", []any{uidB})},
		{"an Instance placed on another Allocation", with(t, with(t, base, "content.proxy_name", uidB), "content.custom_domains", []any{uidB})},
		{"a UID that no Instance has", with(t, with(t, base, "content.proxy_name", "uid-nobody"), "content.custom_domains", []any{"uid-nobody"})},
		{"a subdomain", with(t, base, "content.subdomain", "s")},
		{"a group", with(t, base, "content.group", "g")},
		{"an http user", with(t, base, "content.http_user", "u")},
		{"a route by http user", with(t, base, "content.route_by_http_user", "r")},
		{"an unknown credential", with(t, base, "content.user.metas.token", tokC)},
		{"no session token", with(t, base, "content.user.metas.token", nil)},
	} {
		t.Run(c.name, func(t *testing.T) {
			want := callAt(t, token, tokenPluginPath, OpNewProxy, c.body)
			if !want.Reject || want.RejectReason == "" {
				t.Fatalf("the token path accepts this body, so it is no refusal to compare: %+v", want)
			}
			got := callAt(t, bulk, bulkPluginPath, OpNewProxy, c.body)
			if !got.Reject || got.RejectReason != want.RejectReason {
				t.Errorf("the bulk path answered %+v, want the token path's refusal %q", got, want.RejectReason)
			}
		})
	}
	// Logins: a wrong token, no token, and an Allocation that has ended (not in the Index).
	login := fixture(t, "login")
	for name, body := range map[string]map[string]any{
		"a login with a wrong token":  with(t, login, "content.metas.token", tokC),
		"a login with no token":       with(t, login, "content.metas.token", nil),
		"a login with no metas":       with(t, login, "content.metas", nil),
		"a login with a short token":  with(t, login, "content.metas.token", tokA[:10]),
		"a login with a longer token": with(t, login, "content.metas.token", tokA+"0"),
	} {
		if a := callAt(t, bulk, bulkPluginPath, OpLogin, body); !a.Reject {
			t.Errorf("%s: accepted on the bulk path", name)
		}
	}
	if got := bulkEv.all(); len(got) != 0 {
		t.Fatalf("the bulk Notifier heard %v for refused proxies", got)
	}
	if got := tokenEv.all(); len(got) != 0 {
		t.Fatalf("the token Notifier heard %v for refused proxies", got)
	}

	// The control: the valid proxy is accepted on the bulk path, and only the bulk Notifier hears it.
	if a := callAt(t, bulk, bulkPluginPath, OpNewProxy, base); a.Reject {
		t.Fatalf("the valid NewProxy was refused on the bulk path: %q", a.RejectReason)
	}
	if got := bulkEv.all(); len(got) != 1 || got[0] != "NewProxy "+uidA {
		t.Errorf("the bulk Notifier heard %v, want [NewProxy %s]", got, uidA)
	}
	if got := tokenEv.all(); len(got) != 0 {
		t.Errorf("the token Notifier heard %v for a bulk NewProxy, want nothing", got)
	}
}

// TestBulkEndedAllocationIsRefusedOnTheBulkPath: a credential whose Allocation has ended is refused
// on the bulk path too. The Index is shared, so the one snapshot swap that drops the Allocation
// ends it on both frps at once.
func TestBulkEndedAllocationIsRefusedOnTheBulkPath(t *testing.T) {
	idx := syncedIndex(t)
	bulk := bulkHandler(t, idx, nil)
	b := fixture(t, "newproxy")
	if a := callAt(t, bulk, bulkPluginPath, OpNewProxy, b); a.Reject {
		t.Fatalf("before the end: %q", a.RejectReason)
	}
	if err := idx.Replace(Snapshot{Allocations: []AllocationEntry{{UID: "alloc-b", Credential: tokB, Instances: []string{uidB}}}}); err != nil {
		t.Fatal(err)
	}
	if a := callAt(t, bulk, bulkPluginPath, OpNewProxy, b); !a.Reject {
		t.Fatal("a proxy of an ended Allocation was accepted on the bulk path")
	}
}

// TestBulkCloseProxyIsHeardByTheBulkNotifierOnly: a CloseProxy on one path moves that path's
// Notifier alone (the reverse of the bulk Notifier test above), even before the sync, as on the
// token path.
func TestBulkCloseProxyIsHeardByTheBulkNotifierOnly(t *testing.T) {
	idx := syncedIndex(t)
	tokenEv, bulkEv := &events{}, &events{}
	token := newTestHandler(t, idx, tokenEv)
	bulk := bulkHandler(t, idx, bulkEv)
	cp := fixture(t, "closeproxy")
	if a := callAt(t, bulk, bulkPluginPath, OpCloseProxy, cp); a.Reject {
		t.Fatalf("a bulk CloseProxy was refused: %q", a.RejectReason)
	}
	if got := bulkEv.all(); len(got) != 1 || got[0] != "CloseProxy "+uidA {
		t.Errorf("the bulk Notifier heard %v, want [CloseProxy %s]", got, uidA)
	}
	if got := tokenEv.all(); len(got) != 0 {
		t.Errorf("the token Notifier heard %v for a bulk CloseProxy, want nothing", got)
	}
	if a := callAt(t, token, tokenPluginPath, OpCloseProxy, cp); a.Reject {
		t.Fatalf("a token CloseProxy was refused: %q", a.RejectReason)
	}
	if got := bulkEv.all(); len(got) != 1 {
		t.Errorf("the bulk Notifier heard %v after a token CloseProxy, want it unchanged", got)
	}
	if got := tokenEv.all(); len(got) != 1 || !strings.HasPrefix(got[0], "CloseProxy ") {
		t.Errorf("the token Notifier heard %v, want one CloseProxy", got)
	}
}
