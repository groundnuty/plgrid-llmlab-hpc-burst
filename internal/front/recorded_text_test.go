package front

// The bound on the client's text in a usage record (recorded_text.go; issue #645). What is checked: the cut itself (where, and that it leaves valid
// UTF-8), that the bound is the API's own limit and not a guess, that the numbers are the ones HBEP-5's amendment words (1,291 bytes of a name kept,
// then a marker; the decoded path at 1,302), that a name or a path over the bound is recorded within it while every answer
// stays what it was, and that a name at the longest that can be routed is recorded whole.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// A text at the bound is kept whole. A longer one keeps its first max bytes, and the marker follows: for ASCII, exactly max bytes kept. The cut never
// splits a character: where byte max is inside one, that character is left out, so the kept part is up to 3 bytes short of max, valid UTF-8 that begins
// as the text does; where byte max starts a character, exactly max bytes are kept, whatever the characters' width.
func TestBoundedTextKeepsTheBoundThenTheMarker(t *testing.T) {
	const max = 64
	for _, s := range []string{"", "m", strings.Repeat("a", max)} {
		if got := boundedText(s, max); got != s {
			t.Errorf("a text of %d bytes, at or under %d: cut to %q", len(s), max, got)
		}
	}
	over := strings.Repeat("a", max+1)
	if got, want := boundedText(over, max), strings.Repeat("a", max)+cutMarker; got != want {
		t.Errorf("one byte over the bound: %q, want the first %d bytes and the marker, %q", got, max, want)
	}
	for _, wide := range []string{"é", "€", "𝔸"} { // 2, 3 and 4 bytes
		for pad := 0; pad < utf8.UTFMax; pad++ { // the place in the character where the cut falls
			s := strings.Repeat("a", pad) + strings.Repeat(wide, 100)
			got := boundedText(s, max)
			kept := strings.TrimSuffix(got, cutMarker)
			if !utf8.ValidString(got) || !strings.HasSuffix(got, cutMarker) || !strings.HasPrefix(s, kept) || len(kept) > max || len(kept) < max-(utf8.UTFMax-1) {
				t.Errorf("%q characters, padded by %d: %q (valid %t, %d bytes kept), want valid UTF-8, the text's start, between %d and %d bytes kept, and the marker", wide, pad, got, utf8.ValidString(got), len(kept), max-(utf8.UTFMax-1), max)
			}
			if utf8.RuneStart(s[max]) && len(kept) != max {
				t.Errorf("%q characters, padded by %d: byte %d starts a character, and %d bytes are kept, want exactly %d", wide, pad, max, len(kept), max)
			}
		}
	}
	// Bytes that are not UTF-8 at all cost the marker and a few bytes of back-off, not the prefix.
	if got := boundedText(strings.Repeat("\x80", 1000), max); len(got) < max-utf8.UTFMax+len(cutMarker) {
		t.Errorf("1000 continuation bytes: %d bytes recorded, want about %d: the back-off to a character boundary is bounded", len(got), max+len(cutMarker))
	}
}

// The numbers are those HBEP-5's amendment states, typed here: a model name is kept to 1,291 bytes, the longest name the front can serve (a model's 256
// characters of up to 4 bytes, "@", a 253-byte provider, "/" and a 12-character ID), and the decoded path to 1,302, the prefix of GET /v1/models/<name>
// and that name. A text of exactly the bound is whole, one byte more keeps the bound and the marker.
func TestTheBoundsAreTheAmendedNumbers(t *testing.T) {
	if maxRoutableNameBytes != 1291 {
		t.Errorf("the name bound is %d bytes, want 1,291", maxRoutableNameBytes)
	}
	if maxRecordedPathBytes != 1302 {
		t.Errorf("the path bound is %d bytes, want 1,302: %q and 1,291", maxRecordedPathBytes, modelsPathPrefix)
	}
	name := strings.Repeat("n", 1291)
	if got := boundedText(name, maxRoutableNameBytes); got != name {
		t.Errorf("a name of 1,291 bytes: %d bytes recorded, want it whole", len(got))
	}
	if got, want := boundedText(name+"n", maxRoutableNameBytes), name+cutMarker; got != want {
		t.Errorf("a name of 1,292 bytes: %d bytes recorded, want its first 1,291 and the marker", len(got))
	}
	path := strings.Repeat("p", 1302)
	if got := boundedText(path, maxRecordedPathBytes); got != path {
		t.Errorf("a path of 1,302 bytes: %d bytes recorded, want it whole", len(got))
	}
	if got, want := boundedText(path+"p", maxRecordedPathBytes), path+cutMarker; got != want {
		t.Errorf("a path of 1,303 bytes: %d bytes recorded, want its first 1,302 and the marker", len(got))
	}
}

// The bound is the API's limit on a model name and not a number chosen here: spec.model of a ModelDeployment is limited in the generated CRD, and every
// name a request can be routed by has a model part from one. If the API allows more, the bound must grow with it, or a served name would be recorded cut.
func TestTheNameBoundCoversTheLongestRoutableName(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "config", "crd", "hpcburst.groundnuty.github.io_modeldeployments.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var crd map[string]any
	if err := yaml.Unmarshal(b, &crd); err != nil {
		t.Fatal(err)
	}
	at := func(v any, path ...string) any {
		for _, k := range path {
			m, ok := v.(map[string]any)
			if !ok {
				return nil
			}
			v = m[k]
		}
		return v
	}
	versions, _ := at(crd, "spec", "versions").([]any)
	if len(versions) == 0 {
		t.Fatal("the CRD lists no version")
	}
	for _, v := range versions {
		got, ok := at(v, "schema", "openAPIV3Schema", "properties", "spec", "properties", "model", "maxLength").(float64)
		if !ok {
			t.Fatalf("spec.model has no maxLength in a version of the CRD: %v", at(v, "schema", "openAPIV3Schema", "properties", "spec", "properties", "model"))
		}
		if int(got) != apiModelChars {
			t.Errorf("the CRD limits spec.model to %d characters and recorded_text.go assumes %d: the bound on a recorded name (%d bytes) must follow the API", int(got), apiModelChars, maxRoutableNameBytes)
		}
	}
	if want := 4*apiModelChars + 1 + apiProviderNameBytes + 1 + 12; maxRoutableNameBytes != want {
		t.Errorf("maxRoutableNameBytes is %d, want the model's 4-byte characters, %q, a provider, %q and a 12-character ID: %d", maxRoutableNameBytes, "@", "/", want)
	}
}

// jsonEscaped is s as a JSON string holds it, without the quotes.
func jsonEscaped(t *testing.T, s string) string {
	t.Helper()
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	return string(b[1 : len(b)-1])
}

// usageLines returns the lines of the harness's usage files.
func (h *harness) usageLines() []string {
	h.t.Helper()
	return strings.Split(strings.TrimSpace(string(readUsageFiles(h.t, h.cfg.UsageDir))), "\n")
}

// maxUsageLine is far above what one record can be once its client text is bound (each of the two fields is JSON-escaped to at most 6 bytes a byte,
// for a control character or "<") and far below what a client could make it.
const maxUsageLine = 6*(maxRoutableNameBytes+maxRecordedPathBytes+2*len(cutMarker)) + 4096

// checkBoundedRecord fails unless the record of the request holds the first bytes of the text that its bound keeps, then the marker, and no usage line
// is longer than maxUsageLine.
func (h *harness) checkBoundedRecord(t *testing.T, id, sentModel, sentPath string) {
	t.Helper()
	rec := h.record(id)
	if sentModel != "" {
		// The names these tests send are ASCII, so exactly the bound's bytes are kept, then the marker.
		if len(rec.RequestedModel) != maxRoutableNameBytes+len(cutMarker) || !strings.HasSuffix(rec.RequestedModel, cutMarker) || !strings.HasPrefix(sentModel, strings.TrimSuffix(rec.RequestedModel, cutMarker)) || !utf8.ValidString(rec.RequestedModel) {
			t.Errorf("requestedModel of %d bytes (a name of %d was sent): want its first %d bytes, valid UTF-8, and the marker", len(rec.RequestedModel), len(sentModel), maxRoutableNameBytes)
		}
	}
	if sentPath != "" {
		if len(rec.Path) != maxRecordedPathBytes+len(cutMarker) || !strings.HasSuffix(rec.Path, cutMarker) || !strings.HasPrefix(sentPath, strings.TrimSuffix(rec.Path, cutMarker)) {
			t.Errorf("path of %d bytes (a path of %d was sent): want its first %d bytes and the marker", len(rec.Path), len(sentPath), maxRecordedPathBytes)
		}
	}
	for _, l := range h.usageLines() {
		if len(l) > maxUsageLine {
			t.Errorf("a usage line of %d bytes, want at most %d", len(l), maxUsageLine)
		}
	}
}

// A model name of several MiB that breaks the grammar, holding a newline and characters JSON escapes at length, is answered 400 invalid_model_name as
// a short one is, in the same body, and its record's requestedModel is within the bound, on one line.
func TestAnOverLongModelNameThatBreaksTheGrammarIsRecordedWithinTheBound(t *testing.T) {
	s := newErrFix(t, nil, instanceOpts{}, "")
	short := s.post("/v1/chat/completions", errChatBody("x@", false, "hello there"))
	shortBody := readAll(t, short)
	s.checkFrontError(t, short, shortBody, wantError{status: 400, code: "invalid_model_name", detail: "grammar"})

	for name, model := range map[string]string{
		"a long run of letters":             strings.Repeat("a", 5<<20) + "\nb@",
		"characters JSON escapes at length": strings.Repeat("<\n\x01", 2<<20) + "@",
	} {
		t.Run(name, func(t *testing.T) {
			resp := s.post("/v1/chat/completions", errChatBody(model, false, "hello there"))
			body := readAll(t, resp)
			s.checkFrontError(t, resp, body, wantError{status: 400, code: "invalid_model_name", detail: "grammar"})
			if body != shortBody {
				t.Errorf("the answer to an over-long name differs from the short one's: %q against %q", body, shortBody)
			}
			s.checkBoundedRecord(t, resp.Header.Get("hpcburst-request-id"), model, "")
		})
	}
	if n := len(s.e.Requests()); n != 0 {
		t.Errorf("the engine saw %d requests for names that break the grammar, want 0", n)
	}
}

// A model name that is a valid name by the grammar, which sets no length, and that no deployment has, is answered as an unknown one is, 503
// model_inactive not_attached, and recorded within the bound. The answer still carries the name, as before: this bounds the record only.
func TestAnOverLongModelNameThatNoDeploymentHasIsRecordedWithinTheBound(t *testing.T) {
	s := newErrFix(t, nil, instanceOpts{}, "")
	model := strings.Repeat("m", 5<<20) + "\nn"
	resp := s.post("/v1/chat/completions", errChatBody(model, false, "hello there"))
	body := readAll(t, resp)
	s.checkFrontError(t, resp, body, wantError{status: 503, code: "model_inactive", detail: "not_attached", retryAfter: "30"})
	if !strings.Contains(body, jsonEscaped(t, model)) {
		t.Errorf("the answer does not carry the name whole (%d bytes of answer for a name of %d): the cut name was used for more than the record", len(body), len(model))
	}
	s.checkBoundedRecord(t, resp.Header.Get("hpcburst-request-id"), model, "")
	if n := len(s.e.Requests()); n != 0 {
		t.Errorf("the engine saw %d requests for a model no deployment has, want 0", n)
	}
}

// longestRoutableFix is a front that serves the longest name a request can be routed by: a model of the API's 256 characters of 4 bytes each, and a
// provider of a Kubernetes object name's longest. name is the pinned form of it, model@provider/<12 hex>, maxRoutableNameBytes bytes.
func longestRoutableFix(t *testing.T) (h *harness, model, provider, name string) {
	t.Helper()
	model = strings.Repeat("𝔸", apiModelChars)
	provider = strings.Repeat("p", apiProviderNameBytes)
	name = model + "@" + provider + "/" + efDeployDisplay
	if len(name) != maxRoutableNameBytes {
		t.Fatalf("the name built is %d bytes, want maxRoutableNameBytes, %d", len(name), maxRoutableNameBytes)
	}
	h = newHarness(t, nil)
	key := newKey(t)
	e := enginetest.Start(t, enginetest.Options{PortLo: testPortLo, PortHi: testPortHi, Key: key, Clock: h.clk})
	h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
	h.batchProvider(provider, "helios-gh200")
	h.servedMD("md", model, 1, provider)
	h.instance("md", "md-0", provider, e, key, instanceOpts{deployID: efDeployID})
	h.apply()
	return h, model, provider, name
}

// A name at the longest that can be routed by, a model of the API's 256 characters of 4 bytes each, a provider of a Kubernetes object name's
// longest and a 12-character deployment ID, is served and recorded WHOLE: the bound cuts nothing a request can be served under.
func TestTheLongestRoutableNameIsServedAndRecordedWhole(t *testing.T) {
	h, model, provider, name := longestRoutableFix(t)
	for level, sent := range map[string]string{"model": model, "provider": model + "@" + provider, "pinned": name} {
		resp := h.errPost("/v1/chat/completions", errChatBody(sent, false, "hello there"))
		body := readAll(t, resp)
		id := resp.Header.Get("hpcburst-request-id")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s level: status %d, want 200, a served request: %s", level, resp.StatusCode, body)
		}
		if rec := h.record(id); rec.RequestedModel != sent || rec.Status != http.StatusOK {
			t.Errorf("%s level: requestedModel of %d bytes and status %d, want the whole name, %d bytes, and 200", level, len(rec.RequestedModel), rec.Status, len(sent))
		}
	}
}

// percentEncoded writes every byte of s as %XX, as a client may.
func percentEncoded(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		fmt.Fprintf(&b, "%%%02X", s[i])
	}
	return b.String()
}

// The front takes the name of GET /v1/models/<name> from the path decoded once, so a client may percent-encode every byte of it, 3 bytes for each, and
// the request line is then up to three times as long as the bound. What the record holds is the path DECODED (r.URL.Path), so the bound is enough:
// the longest model that is listed, 256 characters of 4 bytes each, fully percent-encoded, is SERVED, and recorded whole, decoded; a name of the name
// bound's 1,291 bytes, fully encoded, is answered 404 as an unlisted name and recorded whole, decoded, at the path bound; and an encoded path whose
// decoded form is over the bound is recorded cut, at the decoded form.
func TestTheLongestPathsAnsweredByANameAreRecordedWholeWhateverTheirEncoding(t *testing.T) {
	h, model, _, _ := longestRoutableFix(t)
	t.Run("the longest listed model, fully percent-encoded, is served", func(t *testing.T) {
		path := modelsPathPrefix + percentEncoded(model)
		resp := h.errGet(path)
		body := readAll(t, resp)
		if resp.StatusCode != http.StatusOK || !strings.Contains(body, jsonEscaped(t, model)) {
			t.Fatalf("status %d for a path of %d bytes, want 200 and the model's entry: %.200s", resp.StatusCode, len(path), body)
		}
		if rec := h.record(resp.Header.Get("hpcburst-request-id")); rec.Path != modelsPathPrefix+model {
			t.Errorf("path of %d bytes (a path of %d bytes was sent, encoded), want the decoded path whole, %d bytes", len(rec.Path), len(path), len(modelsPathPrefix+model))
		}
	})
	t.Run("a name of the name bound, fully encoded", func(t *testing.T) {
		name := strings.Repeat("n", maxRoutableNameBytes)
		path := modelsPathPrefix + percentEncoded(name)
		if want := len(modelsPathPrefix) + 3*maxRoutableNameBytes; len(path) != want || len(modelsPathPrefix+name) != maxRecordedPathBytes {
			t.Fatalf("the path built is %d bytes (want %d on the wire) and its decoded form %d (want the path bound, %d)", len(path), want, len(modelsPathPrefix+name), maxRecordedPathBytes)
		}
		resp := h.errGet(path)
		readAll(t, resp)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status %d, want 404 model_not_found for a name no deployment has", resp.StatusCode)
		}
		if rec := h.record(resp.Header.Get("hpcburst-request-id")); rec.Path != modelsPathPrefix+name {
			t.Errorf("path of %d bytes, want the decoded path whole, %d bytes", len(rec.Path), len(modelsPathPrefix+name))
		}
	})
	t.Run("an encoded path that decodes to more than the bound is cut at the decoded form", func(t *testing.T) {
		name := strings.Repeat("n", maxRecordedPathBytes+100)
		resp := h.errGet(modelsPathPrefix + percentEncoded(name))
		readAll(t, resp)
		h.checkBoundedRecord(t, resp.Header.Get("hpcburst-request-id"), "", modelsPathPrefix+name)
	})
}

// The path is the client's too, and it is recorded for a request with no key at all (HBEP-5 keeps one record per request): a path of nearly the
// header limit is answered as a short one is and recorded within the bound.
func TestAnOverLongPathIsRecordedWithinTheBound(t *testing.T) {
	s := newErrFix(t, nil, instanceOpts{}, "")
	longPath := strings.Repeat("p", 900<<10)
	t.Run("no key, off /v1/", func(t *testing.T) {
		get := func(path string) (*http.Response, string) {
			req, err := http.NewRequest(http.MethodGet, s.url+path, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := errClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			return resp, readAll(t, resp)
		}
		shortResp, shortBody := get("/q")
		resp, body := get("/" + longPath)
		if resp.StatusCode != shortResp.StatusCode || body != shortBody {
			t.Errorf("a long path: status %d body %q, a short one: status %d body %q, want the same", resp.StatusCode, body, shortResp.StatusCode, shortBody)
		}
		s.checkBoundedRecord(t, resp.Header.Get("hpcburst-request-id"), "", "/"+longPath)
	})
	t.Run("a key, under /v1/", func(t *testing.T) {
		post := func(path string) (*http.Response, string) {
			resp := s.post(path, errChatBody(efModel, false, "hello there"))
			return resp, readAll(t, resp)
		}
		shortResp, shortBody := post("/v1/pp")
		resp, body := post("/v1/" + longPath)
		if resp.StatusCode != shortResp.StatusCode || len(body) != len(shortBody) {
			t.Errorf("a long path: status %d, %d bytes, a short one: status %d, %d bytes, want the same", resp.StatusCode, len(body), shortResp.StatusCode, len(shortBody))
		}
		s.checkBoundedRecord(t, resp.Header.Get("hpcburst-request-id"), "", "/v1/"+longPath)
	})
	if !bytes.Contains(readUsageFiles(t, s.cfg.UsageDir), []byte(cutMarker)) {
		t.Error("no record carries the marker of a cut text")
	}
}
