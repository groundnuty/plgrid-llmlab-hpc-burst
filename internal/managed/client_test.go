package managed

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/managed/llmlabtest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Client against the double and against a stub that gives the answers the
// double does not.

func newTestClient(t *testing.T, baseURL string) *Client {
	t.Helper()
	c, err := NewClient(provider.ManagedCredentials{Key: testKey, BaseURL: baseURL}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func startDouble(t *testing.T, models ...llmlabtest.Model) *llmlabtest.Server {
	t.Helper()
	return llmlabtest.Start(t, llmlabtest.Options{PortLo: portLo, PortHi: portHi, Keys: []string{testKey}, Models: models})
}

func TestListCatalogReadsNamesAndStringRevisionsInListingOrder(t *testing.T) {
	gw := startDouble(t, modelRev("example-27b", "2026-09"), model("example-9b"))
	entries, err := newTestClient(t, gw.BaseURL()).ListCatalog(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].Name != "example-27b" || entries[1].Name != "example-9b" {
		t.Fatalf("entries = %+v, want example-27b then example-9b, as listed", entries)
	}
	if entries[0].Revision == nil || *entries[0].Revision != "2026-09" {
		t.Fatalf("example-27b revision = %v, want 2026-09", entries[0].Revision)
	}
	if entries[1].Revision != nil {
		t.Fatalf("example-9b revision = %q, want none: the double lists no revision member", *entries[1].Revision)
	}
}

func TestListCatalogRevisionIsAbsentUnlessAString(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		want       string // "" is absent
	}{
		{"no revision member", `{"data":[{"id":"example-a"}]}`, ""},
		{"a null revision", `{"data":[{"id":"example-a","revision":null}]}`, ""},
		{"a string revision", `{"data":[{"id":"example-a","revision":"r7"}]}`, "r7"},
		{"unknown members are ignored", `{"object":"list","data":[{"id":"example-a","revision":"r7","owned_by":"x","extra":{"k":[1,2]}}],"more":true}`, "r7"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := startStub(t, answer(200, tc.body))
			entries, err := newTestClient(t, st.baseURL()).ListCatalog(context.Background())
			if err != nil || len(entries) != 1 || entries[0].Name != "example-a" {
				t.Fatalf("ListCatalog = %+v, %v, want example-a", entries, err)
			}
			switch got := entries[0].Revision; {
			case tc.want == "" && got != nil:
				t.Fatalf("revision = %q, want absent", *got)
			case tc.want != "" && (got == nil || *got != tc.want):
				t.Fatalf("revision = %v, want %q", got, tc.want)
			}
		})
	}
}

// Issue #69, criterion 12: a revision is read from "revision" when it is a
// string, and is absent otherwise. A member of any other type is therefore an
// absent revision, not a failed read: the member name is an assumption, and a
// gateway whose "revision" means something else must not stop the catalog.
func TestListCatalogNonStringRevisionIsAbsent(t *testing.T) {
	for _, body := range []string{
		`{"data":[{"id":"example-a","revision":7}]}`,
		`{"data":[{"id":"example-a","revision":{"v":1}}]}`,
		`{"data":[{"id":"example-a","revision":["r1"]}]}`,
		`{"data":[{"id":"example-a","revision":true}]}`,
	} {
		st := startStub(t, answer(200, body))
		entries, err := newTestClient(t, st.baseURL()).ListCatalog(context.Background())
		if err != nil || len(entries) != 1 || entries[0].Name != "example-a" || entries[0].Revision != nil {
			t.Fatalf("ListCatalog(%s) = %+v, %v, want one entry example-a with no revision", body, entries, err)
		}
	}
}

func TestListCatalogKeyRejectionWrapsErrKeyRejected(t *testing.T) {
	gw := startDouble(t, model("example-a"))
	c := newTestClient(t, gw.BaseURL())

	gw.RejectKey(testKey, 401)
	if _, err := c.ListCatalog(context.Background()); !errors.Is(err, provider.ErrKeyRejected) || !strings.Contains(err.Error(), "401") {
		t.Fatalf("a 401: error = %v, want it to wrap provider.ErrKeyRejected and name 401", err)
	}
	gw.RejectKey(testKey, 403)
	if _, err := c.ListCatalog(context.Background()); !errors.Is(err, provider.ErrKeyRejected) || !strings.Contains(err.Error(), "403") {
		t.Fatalf("a 403: error = %v, want it to wrap provider.ErrKeyRejected and name 403", err)
	}
	gw.RejectKey(testKey, 0)
	gw.RemoveKey(testKey) // an unknown key is a 401 too
	if _, err := c.ListCatalog(context.Background()); !errors.Is(err, provider.ErrKeyRejected) {
		t.Fatalf("an unknown key: error = %v, want it to wrap provider.ErrKeyRejected", err)
	}
	gw.AddKey(testKey)
	if entries, err := c.ListCatalog(context.Background()); err != nil || len(entries) != 1 {
		t.Fatalf("an accepted key: %+v, %v, want one entry", entries, err)
	}
}

func TestListCatalogOtherStatusesAreNotKeyRejections(t *testing.T) {
	for _, status := range []int{400, 404, 405, 429, 500, 502, 503, 504} {
		st := startStub(t, answer(status, `{"error":{"message":"example"}}`))
		_, err := newTestClient(t, st.baseURL()).ListCatalog(context.Background())
		if err == nil || errors.Is(err, provider.ErrKeyRejected) {
			t.Fatalf("a %d: error = %v, want an error that is not a key rejection", status, err)
		}
	}
}

func TestListCatalogBodyLimit(t *testing.T) {
	const prefix = `{"data":[{"id":"example-a"}]}`
	atLimit := prefix + strings.Repeat(" ", MaxCatalogBytes-len(prefix))
	if len(atLimit) != MaxCatalogBytes {
		t.Fatalf("the test body has %d bytes, want %d", len(atLimit), MaxCatalogBytes)
	}

	st := startStub(t, answer(200, atLimit))
	if entries, err := newTestClient(t, st.baseURL()).ListCatalog(context.Background()); err != nil || len(entries) != 1 {
		t.Fatalf("a body of exactly MaxCatalogBytes: %+v, %v, want one entry", entries, err)
	}
	st.set(answer(200, atLimit+" "))
	if entries, err := newTestClient(t, st.baseURL()).ListCatalog(context.Background()); err == nil {
		t.Fatalf("a body of MaxCatalogBytes+1: %+v, nil, want an error", entries)
	}
}

func TestListCatalogSendsTheKeyAsABearerToken(t *testing.T) {
	gw := startDouble(t, model("example-a"))
	if _, err := newTestClient(t, gw.BaseURL()).ListCatalog(context.Background()); err != nil {
		t.Fatal(err)
	}
	reqs := gw.Requests()
	if len(reqs) != 1 {
		t.Fatalf("the gateway logged %d requests, want 1", len(reqs))
	}
	r := reqs[0]
	if r.Method != http.MethodGet || r.Path != "/v1/models" {
		t.Fatalf("request = %s %s, want GET /v1/models", r.Method, r.Path)
	}
	if got := r.Header.Get("Authorization"); got != "Bearer "+testKey {
		t.Fatalf("Authorization = %q, want exactly %q", got, "Bearer "+testKey)
	}
	if got := r.Header["Authorization"]; len(got) != 1 {
		t.Fatalf("Authorization sent %d times, want once", len(got))
	}
	if len(r.Body) != 0 {
		t.Fatalf("the read sent a body of %d bytes, want none", len(r.Body))
	}
}

func TestListCatalogFollowsNoRedirect(t *testing.T) {
	st := startStub(t, nil)
	st.set(http.RedirectHandler(st.url+"/elsewhere", http.StatusFound).ServeHTTP)
	_, err := newTestClient(t, st.baseURL()).ListCatalog(context.Background())
	if err == nil || errors.Is(err, provider.ErrKeyRejected) {
		t.Fatalf("a 302: error = %v, want an unknown error", err)
	}
	if n := st.count("/elsewhere"); n != 0 {
		t.Fatalf("the redirect target received %d requests, want 0: the key must not follow a redirect", n)
	}
	for _, a := range st.authHeaders() {
		if a != "Bearer "+testKey {
			t.Fatalf("the stub saw Authorization %q", a)
		}
	}
}

// An error holds neither the key nor any byte of the response body.
func TestListCatalogErrorsHoldNeitherTheKeyNorTheBody(t *testing.T) {
	const marker = "example-body-marker-5c1d"
	for _, tc := range []struct {
		name         string
		status       int
		body         string
		wantRejected bool
	}{
		{"a 500", 500, `{"error":"` + marker + `"}`, false},
		{"a body that is not JSON", 200, "not json " + marker, false},
		{"an entry of the wrong type", 200, `{"data":[{"id":5,"x":"` + marker + `"}]}`, false},
		{"a 401 that echoes", 401, `{"error":"` + marker + ` ` + testKey + `"}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := startStub(t, answer(tc.status, tc.body))
			_, err := newTestClient(t, st.baseURL()).ListCatalog(context.Background())
			if err == nil {
				t.Fatal("ListCatalog = nil error")
			}
			if errors.Is(err, provider.ErrKeyRejected) != tc.wantRejected {
				t.Fatalf("error = %v, rejected = %v, want %v", err, !tc.wantRejected, tc.wantRejected)
			}
			if strings.Contains(err.Error(), marker) || strings.Contains(err.Error(), testKey) {
				t.Fatalf("error = %q holds the key or a byte of the body", err)
			}
		})
	}

	// And the double's own echo of the key, on a rejection.
	gw := startDouble(t, model("example-a"))
	gw.EchoKey(true)
	gw.RejectKey(testKey, 401)
	if status, body := rawGet(t, gw.URL()+"/v1/models", testKey); status != 401 || !strings.Contains(body, testKey) {
		t.Fatalf("control: the double answered %d with %q, want a body that holds the key", status, body)
	}
	_, err := newTestClient(t, gw.BaseURL()).ListCatalog(context.Background())
	if !errors.Is(err, provider.ErrKeyRejected) || strings.Contains(err.Error(), testKey) {
		t.Fatalf("error = %v, want a key rejection without the key", err)
	}
}

func TestNewClientRefusesInvalidCredentials(t *testing.T) {
	for _, creds := range []provider.ManagedCredentials{
		{Key: testKey, BaseURL: "http://127.0.0.1:29340"}, // no /v1
		{Key: "", BaseURL: "http://127.0.0.1:29340/v1"},
		{Key: testKey, BaseURL: "http://gateway.example/v1"}, // http away from loopback
	} {
		c, err := NewClient(creds, nil)
		if err == nil || !errors.Is(err, provider.ErrCredentials) || c != nil {
			t.Fatalf("NewClient(%+v) = %v, %v, want a nil Client and an error wrapping provider.ErrCredentials", creds, c, err)
		}
		if strings.Contains(err.Error(), testKey) {
			t.Fatalf("error = %q holds the key", err)
		}
	}
}

func TestListCatalogEndsWithItsContext(t *testing.T) {
	gw := startDouble(t, model("example-a"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := newTestClient(t, gw.BaseURL()).ListCatalog(ctx); err == nil {
		t.Fatal("ListCatalog with a cancelled context = nil error")
	}
	if n := len(gw.Requests()); n != 0 {
		t.Fatalf("the gateway logged %d requests for a cancelled read, want 0", n)
	}
}
