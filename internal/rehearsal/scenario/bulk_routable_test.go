package scenario_test

// The front's own answer to "would you place a request for this model now?", for startServing (#671).
//
// Registered is the tunnel's state: frps's plugin accepted the Instance's proxy. Routing needs more,
// and reads another source: the router's view, built from the control plane's watch, holds the
// Instance as Eligible (ready, a transport, its Provider attached, not AgentUnreachable;
// internal/front/inventory, build.go). The test reads "Ready" straight from the API server, so the
// front's view can still lag it after the registration came. A request that arrives in that gap gets
// 503 model_inactive no_ready_target (internal/front/route.go, errNoReadyTarget), which the key
// search's first request met on main (issue #671, the same family as #637).
//
// GET /v1/models is the front's interface for this: each entry has `routable`, true when an eligible
// Instance of ours or an unpaused managed entry exists in the view (Router.Models, internal/front/
// models.go). It takes the same fresh view a routed request does (lockFresh), and #642's waitRoutable
// (internal/front/bulk_test.go) reads the same predicate, Eligible and not paused, from inside. Each
// call is a client request: the front writes a usage record for it and counts it in
// hpcburst_front_requests_total (endpoint models), but not in the bulk counter.

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// frontRoutes asks the front at addr, with the client key, whether GET /v1/models lists model as
// routable. It never fails the test: every way of not seeing it (no answer, a non-200 status, a body
// that does not parse, the model not listed, listed as not routable) returns false with a reason that
// says which, for a wait's timeout message.
//
// routable is a property of the model, not of one Instance: true for any eligible Instance, or any
// managed entry that is not paused. startServing's deployment has no managed entry, but a replacement
// can stand beside the primary (the controller creates it at the primary's start; TestRehearsalDirect's
// comment). So a true answer rules out no_ready_target, which is what the key search's first request
// met; it does not say which Instance the ring places a key on. KeyFor settles that, by the Instance
// each reply's usage record names.
func frontRoutes(addr, key, model string) (routes bool, why string) {
	req, err := http.NewRequest(http.MethodGet, "http://"+addr+"/v1/models", nil)
	if err != nil {
		return false, "building the request for GET /v1/models: " + err.Error()
	}
	req.Header.Set("Authorization", "Bearer "+key)
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return false, "GET /v1/models: " + err.Error()
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return false, "GET /v1/models: reading the body: " + err.Error()
	}
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Sprintf("GET /v1/models answered %d: %.200s", resp.StatusCode, body)
	}
	var list struct {
		Data []struct {
			ID       string `json:"id"`
			Routable bool   `json:"routable"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		return false, fmt.Sprintf("GET /v1/models: a body that does not parse (%v): %.200s", err, body)
	}
	for _, m := range list.Data {
		if m.ID != model {
			continue
		}
		if !m.Routable {
			return false, "the front lists the model with routable false: its view holds no eligible Instance for it yet"
		}
		return true, ""
	}
	return false, "the front does not list the model yet"
}

// modelsDouble serves GET /v1/models as the front does, with one entry per (id, routable) pair, for
// the client key only; anything else gets the status the front answers it with.
func modelsDouble(t *testing.T, key string, entries ...modelsEntry) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+key {
			http.Error(w, `{"error":{"code":"invalid_api_key"}}`, http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodGet || r.URL.Path != "/v1/models" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		data := []map[string]any{}
		for _, e := range entries {
			data = append(data, map[string]any{"id": e.id, "object": "model", "routable": e.routable})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": data})
	}))
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://")
}

type modelsEntry struct {
	id       string
	routable bool
}

// frontRoutes is true only for the model's own entry with routable true, and says why when it is
// false. Each false case is the state the wait must hold through: the model absent (the view has no
// deployment), listed not routable (the gap of #671: a registration the router has not caught up
// with), another model routable, and the front not answering or refusing the key. The model's name has
// a "/", as every LLMLab name does, and matches whole.
//
//livecheck:none a test of the rehearsal's readiness helper on a double; nothing live runs
func TestFrontRoutesIsTrueOnlyForTheModelsOwnRoutableEntry(t *testing.T) {
	const key = "client-key-0123456789"
	model := stack.Model
	cases := []struct {
		name    string
		entries []modelsEntry
		key     string
		want    bool
		why     string // a substring of the reason when false
	}{
		{"the model is routable", []modelsEntry{{model, true}}, key, true, ""},
		{"the model is routable beside another", []modelsEntry{{"example-org/other", false}, {model, true}}, key, true, ""},
		{"no model is listed", nil, key, false, "does not list the model"},
		{"the model is listed and not routable: the router's view lacks an eligible Instance", []modelsEntry{{model, false}}, key, false, "routable false"},
		{"another model is routable, this one is not listed", []modelsEntry{{"example-org/other", true}}, key, false, "does not list the model"},
		{"another model is routable, this one is not", []modelsEntry{{"example-org/other", true}, {model, false}}, key, false, "routable false"},
		{"a model whose name only begins with ours", []modelsEntry{{model + "-b", true}}, key, false, "does not list the model"},
		{"the key is wrong", []modelsEntry{{model, true}}, "another-key", false, "answered 401"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			addr := modelsDouble(t, key, c.entries...)
			got, why := frontRoutes(addr, c.key, model)
			if got != c.want {
				t.Fatalf("frontRoutes = %t (%q), want %t", got, why, c.want)
			}
			if c.want && why != "" {
				t.Errorf("a true answer carries the reason %q", why)
			}
			if !c.want && !strings.Contains(why, c.why) {
				t.Errorf("the reason %q does not say %q", why, c.why)
			}
		})
	}
}

// A front that answers nothing, a path it does not serve and a body that is not a model list each
// give false with a reason, and none panics or fails the test: the wait around the helper polls
// again, and its timeout names the last reason.
//
//livecheck:none a test of the rehearsal's readiness helper on a double; nothing live runs
func TestFrontRoutesNamesWhyItCouldNotSeeTheModel(t *testing.T) {
	const key = "client-key-0123456789"
	closed := httptest.NewServer(http.NotFoundHandler())
	addrClosed := strings.TrimPrefix(closed.URL, "http://")
	closed.Close()
	notAList := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("<html>not json</html>"))
	}))
	t.Cleanup(notAList.Close)
	cases := []struct {
		name, addr, why string
	}{
		{"nothing listens", addrClosed, "GET /v1/models:"},
		{"the body does not parse", strings.TrimPrefix(notAList.URL, "http://"), "does not parse"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, why := frontRoutes(c.addr, key, stack.Model)
			if got {
				t.Fatalf("frontRoutes = true for a front that gave no model list")
			}
			if !strings.Contains(why, c.why) {
				t.Errorf("the reason %q does not say %q", why, c.why)
			}
		})
	}
}
