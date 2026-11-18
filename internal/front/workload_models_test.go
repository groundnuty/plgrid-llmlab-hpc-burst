package front

import (
	"strings"
	"testing"
)

// HBEP-5, Workloads; the science agent's ruling on #764, Q14: GET /v1/models/<model>%23<workload> returns the model's card, unchanged, when the
// Workload is a known one of that model, and is a 404 as for an unknown model when it is not ("%23" is "#", which a URL cannot carry raw).

func TestModelsRetrieveWithAWorkloadSuffix(t *testing.T) {
	f := wlFront(t)
	f.workload("ns", "study-a", scModel)
	f.workload("ns", "dup", scModel)
	f.workload("ns-two", "dup", scModel)
	f.workload("ns", "other-model-wl", "example-other-7b")
	f.apply()

	plain := readAll(t, f.errGet("/v1/models/"+scModel))
	if !strings.Contains(plain, `"id":"`+scModel+`"`) {
		t.Fatalf("the plain card is %s", plain)
	}
	for name, path := range map[string]string{
		"a known Workload":                           "/v1/models/" + scModel + "%23study-a",
		"a known Workload, a hyphen percent-encoded": "/v1/models/" + strings.ReplaceAll(scModel, "-", "%2D") + "%23study-a",
		"a Workload in two namespaces":               "/v1/models/" + scModel + "%23dup", // known; a chat request for it is the ambiguous 400
	} {
		resp := f.errGet(path)
		body := readAll(t, resp)
		if resp.StatusCode != 200 || body != plain {
			t.Errorf("%s: GET %s: status %d, body %s; want 200 and the model's card unchanged, %s", name, path, resp.StatusCode, body, plain)
		}
	}
	for name, path := range map[string]string{
		"an unknown Workload":                       "/v1/models/" + scModel + "%23nope",
		"a Workload of another model":               "/v1/models/" + scModel + "%23other-model-wl",
		"a Workload of the model, an unknown model": "/v1/models/example-other-7b%23other-model-wl",
		"two hashes":                                "/v1/models/" + scModel + "%23study-a%23study-a",
		"an empty Workload":                         "/v1/models/" + scModel + "%23",
		"an empty model":                            "/v1/models/%23study-a",
		"a name that is no label":                   "/v1/models/" + scModel + "%23Study_A",
		"the provider level and a Workload":         "/v1/models/" + scModel + "@helios-a%23study-a",
	} {
		resp := f.errGet(path)
		body := readAll(t, resp)
		checkError(t, resp, body, wantError{status: 404, code: "model_not_found"})
		if t.Failed() {
			t.Fatalf("%s: GET %s: %s", name, path, body)
		}
	}
	// The retrieve names no Workload in the record: it is not a request that resolved one.
	resp := f.errGet("/v1/models/" + scModel + "%23study-a")
	readAll(t, resp)
	if rec := f.record(resp.Header.Get("hpcburst-request-id")); rec.Path != "/v1/models/"+scModel+"#study-a" || rec.Status != 200 {
		t.Errorf("the record of the retrieve: path %q status %d, want the decoded path with the hash and 200", rec.Path, rec.Status)
	}
}

// The longest name a request can be routed by now ends with "#" and a Workload's name of 63 characters: 1,355 bytes. It is served whole and
// recorded whole, as the 1,291-byte one before it was (recorded_text_test.go): a model of 256 four-byte characters, "@", a Provider of 253
// characters, "/", the 12-character deployment ID, "#" and the Workload. A name one byte longer is a grammar error and is recorded cut.
func TestTheLongestRoutableNameWithAWorkloadIsServedAndRecordedWhole(t *testing.T) {
	h, model, provider, name := longestRoutableFix(t)
	workload := strings.Repeat("w", 63)
	h.workload("ns", workload, model)
	h.apply()
	full := name + "#" + workload
	if len(full) != 1355 {
		t.Fatalf("the name built is %d bytes, want 1,355", len(full))
	}
	if len(full) != maxRoutableNameBytes {
		t.Fatalf("the name built is %d bytes, but the record's name bound is %d", len(full), maxRoutableNameBytes)
	}
	for level, sent := range map[string]string{"model": model + "#" + workload, "provider": model + "@" + provider + "#" + workload, "pinned": full} {
		resp := h.errPost("/v1/chat/completions", errChatBody(sent, false, "hello there"))
		body := readAll(t, resp)
		if resp.StatusCode != 200 {
			t.Fatalf("%s level: status %d, want 200, a served request: %s", level, resp.StatusCode, body)
		}
		rec := h.record(resp.Header.Get("hpcburst-request-id"))
		if rec.RequestedModel != sent || rec.Workload == nil || *rec.Workload != workload {
			t.Errorf("%s level: requestedModel of %d bytes and workload %v, want the whole name, %d bytes, and the Workload", level, len(rec.RequestedModel), rec.Workload, len(sent))
		}
	}
	// One more character in the Workload's name breaks the grammar; the record keeps the first 1,355 bytes and the marker.
	long := full + "w"
	resp := h.errPost("/v1/chat/completions", errChatBody(long, false, "hello there"))
	body := readAll(t, resp)
	h.checkFrontError(t, resp, body, wantError{status: 400, code: "invalid_model_name", detail: "grammar"})
	if rec := h.record(resp.Header.Get("hpcburst-request-id")); rec.RequestedModel != full+cutMarker {
		t.Errorf("a name of 1,356 bytes: requestedModel of %d bytes, want its first 1,355 and the marker", len(rec.RequestedModel))
	}
}
