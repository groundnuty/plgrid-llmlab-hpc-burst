package front

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/managed/llmlabtest"
)

// HBEP-5, Workloads, Naming it: "Neither the suffix nor the header is sent on: the model field is rewritten as step 7 says, and the header is
// dropped." Toward an Instance the rewrite sends the deployment's served name, toward a managed provider the catalog name, so no "#" reaches
// either. The doubles record the headers and the body as they arrived, and each test has a control that proves the double logs what a client sent:
// a header of the client's own reaches the same double, so an absent Workload header is the front's doing.

const upControl = "X-Study-Trace" // a client header that is passed on, as a control

// bodyModel is the "model" of a recorded JSON body.
func bodyModel(t *testing.T, body []byte) string {
	t.Helper()
	var m struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("a recorded body is not JSON: %v: %q", err, body)
	}
	return m.Model
}

func hasHeader(h http.Header, name string) bool {
	_, ok := h[http.CanonicalHeaderKey(name)]
	return ok
}

// Toward an Instance: the suffix and the header both reach the front, and neither the header nor a "#" reaches the engine; the engine does see
// the client's control header and the headers the front adds.
func TestTheWorkloadHeaderAndSuffixAreNotSentToAnInstance(t *testing.T) {
	for name, c := range map[string]struct {
		model string
		hdr   []string
	}{
		"the suffix":             {scModel + "#study-a", nil},
		"the header":             {scModel, []string{wlHeader, "study-a"}},
		"both":                   {scModel + "#study-a", []string{wlHeader, "study-a"}},
		"the provider level":     {scModel + "@helios-a#study-a", []string{wlHeader, "study-a"}},
		"the header in capitals": {scModel, []string{"X-HPCBURST-WORKLOAD", "study-a"}},
	} {
		t.Run(name, func(t *testing.T) {
			f := wlFront(t)
			f.workload("ns", "study-a", scModel)
			f.apply()
			resp, body := f.say(c.model, append([]string{upControl, "kept"}, c.hdr...)...)
			if resp.StatusCode != 200 {
				t.Fatalf("status %d, want 200: %s", resp.StatusCode, body)
			}
			reqs := f.engines["ns/a-0"].Requests()
			if len(reqs) != 1 {
				t.Fatalf("the engine saw %d requests, want 1", len(reqs))
			}
			r := reqs[0]
			// The control: the double logs headers, and the client's own and the front's reach it.
			if r.Header.Get(upControl) != "kept" || r.Header.Get("Authorization") == "" || r.Header.Get("X-Session-Id") == "" {
				t.Fatalf("the engine's record of the headers is %v: it must hold the client's %s and the front's Authorization and X-Session-Id, or it proves nothing", r.Header, upControl)
			}
			if hasHeader(r.Header, wlHeader) {
				t.Errorf("the engine received the workload header: %v", r.Header)
			}
			if got := bodyModel(t, r.Body); got != scModel {
				t.Errorf("the engine received model %q, want the served name %q", got, scModel)
			}
		})
	}
}

// A front configured with another header name drops that one, and the default name is then an ordinary header: passed on, naming nothing.
func TestOnlyTheConfiguredWorkloadHeaderIsDropped(t *testing.T) {
	f := newPwFixWith(t, func(c *Config) { c.WorkloadHeader = "X-Study-Workload" }, pwDep{name: "md-a", instances: []string{"a-0"}})
	f.workload("ns", "study-a", scModel)
	f.apply()
	resp, body := f.say(scModel, "X-Study-Workload", "study-a", wlHeader, "not-a-workload")
	if resp.StatusCode != 200 {
		t.Fatalf("status %d, want 200 (the default header names nothing here): %s", resp.StatusCode, body)
	}
	wantWorkload(t, "the configured header", f.record(resp.Header.Get("hpcburst-request-id")), str("study-a"))
	r := f.engines["ns/a-0"].Requests()[0]
	if hasHeader(r.Header, "X-Study-Workload") {
		t.Errorf("the engine received the configured workload header: %v", r.Header)
	}
	if r.Header.Get(wlHeader) != "not-a-workload" {
		t.Errorf("the default header, which this front does not use, was not passed on: %v", r.Header)
	}
}

// Toward a managed provider: the same, with the catalog name in the body. The LLMLab double records what it was sent.
func TestTheWorkloadHeaderAndSuffixAreNotSentToAManagedProvider(t *testing.T) {
	const upstreamKey = "llmlab-upstream-key-for-workload-tests-5e2d"
	for name, c := range map[string]struct {
		model string
		hdr   []string
	}{
		"the suffix": {scModel + "#study-a", nil},
		"the header": {scModel, []string{wlHeader, "study-a"}},
		"both":       {scModel + "#study-a", []string{wlHeader, "study-a"}},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, nil)
			llm := llmlabtest.Start(t, llmlabtest.Options{PortLo: testPortLo, PortHi: testPortHi, Keys: []string{upstreamKey}, Models: []llmlabtest.Model{{ID: "catalog-1"}}})
			h.profile("llmlab", v1alpha1.PlatformManaged, "", "")
			h.managedProvider("llmlab-a", "llmlab", "llmlab", llm.BaseURL(), upstreamKey, "catalog-1")
			h.managedMD("md-llm", scModel, "llmlab", "catalog-1", "llmlab-a").Spec.Workloads = []string{"study-a"}
			h.workload("ns", "study-a", scModel)
			h.apply()
			f := &pwFix{harness: h}
			resp, body := f.say(scModel, append([]string{upControl, "kept"}, c.hdr...)...)
			if resp.StatusCode != 200 || scEmitterOf(resp) != "provider" {
				t.Fatalf("status %d emitter %q, want 200 from the provider: %s", resp.StatusCode, scEmitterOf(resp), body)
			}
			reqs := llm.Requests()
			if len(reqs) != 1 {
				t.Fatalf("LLMLab double logged %d requests, want 1", len(reqs))
			}
			r := reqs[0]
			if r.Header.Get(upControl) != "kept" || r.Header.Get("Authorization") == "" {
				t.Fatalf("the LLMLab double's record of the headers is %v: it must hold the client's %s, or it proves nothing", r.Header, upControl)
			}
			if hasHeader(r.Header, wlHeader) {
				t.Errorf("LLMLab received the workload header: %v", r.Header)
			}
			if r.Model != "catalog-1" || bodyModel(t, r.Body) != "catalog-1" {
				t.Errorf("LLMLab received model %q, want the catalog name catalog-1", r.Model)
			}
		})
	}
}

// newPwFixWith is newPwFix on a front whose configuration edit changes.
func newPwFixWith(t *testing.T, edit func(*Config), deps ...pwDep) *pwFix {
	t.Helper()
	return newPwFixOn(t, edit, deps...)
}
