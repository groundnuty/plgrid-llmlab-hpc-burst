package hubfiles

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
)

// twoPageServer serves page 1 (file a) with the given Link, and page 2 (file
// b) for any request that carries a cursor. A client that follows a bad Link
// would return both files.
func twoPageServer(t *testing.T, link func(self *httptest.Server) string) *treeServer {
	t.Helper()
	var srv *treeServer
	srv = newTreeServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("cursor") != "" {
			_, _ = w.Write([]byte(listOf(lfsJSON("b.safetensors"))))
			return
		}
		w.Header().Set("Link", "<"+link(srv.Server)+`>; rel="next"`)
		_, _ = w.Write([]byte(listOf(lfsJSON("a.safetensors"))))
	})
	return srv
}

func TestFilesRefusesALinkThatLeavesTheSnapshot(t *testing.T) {
	const otherRev = "ffffffffffffffffffffffffffffffffffffffff"
	other := newTreeServer(t, serveBody(listOf(lfsJSON("evil.safetensors"))))
	for name, tc := range map[string]struct {
		link func(self *httptest.Server) string
		want error
	}{
		"changed revision": {func(s *httptest.Server) string {
			return s.URL + "/api/models/" + testRepo + "/tree/" + otherRev + "?cursor=2&recursive=true"
		}, ErrInvalidListing},
		"changed repository": {func(s *httptest.Server) string {
			return s.URL + "/api/models/other-org/other/tree/" + testRev + "?cursor=2&recursive=true"
		}, ErrInvalidListing},
		"relative link, changed revision": {func(*httptest.Server) string {
			return "/api/models/" + testRepo + "/tree/" + otherRev + "?cursor=2&recursive=true"
		}, ErrInvalidListing},
		"recursive=false":             {func(*httptest.Server) string { return "?cursor=2&recursive=false" }, ErrInvalidListing},
		"recursive missing":           {func(*httptest.Server) string { return "?cursor=2" }, ErrInvalidListing},
		"recursive given twice":       {func(*httptest.Server) string { return "?cursor=2&recursive=true&recursive=false" }, ErrInvalidListing},
		"recursive=TRUE":              {func(*httptest.Server) string { return "?cursor=2&recursive=TRUE" }, ErrInvalidListing},
		"a query that does not parse": {func(*httptest.Server) string { return "?cursor=2;recursive=true" }, ErrInvalidListing},
		"credentials in the link": {func(s *httptest.Server) string {
			return strings.Replace(s.URL, "://", "://user:pw@", 1) + treePath + "?cursor=2&recursive=true"
		}, ErrInvalidListing},
		"another origin": {func(*httptest.Server) string { return other.URL + treePath + "?cursor=2&recursive=true" }, ErrCrossHostLink},
		"the same host on another port": {func(s *httptest.Server) string {
			return strings.TrimSuffix(s.URL, s.URL[strings.LastIndex(s.URL, ":"):]) + ":1" + treePath + "?cursor=2&recursive=true"
		}, ErrCrossHostLink},
		"protocol-relative link, other host": {func(*httptest.Server) string {
			return "//" + strings.TrimPrefix(other.URL, "http://") + treePath + "?cursor=2&recursive=true"
		}, ErrCrossHostLink},
	} {
		srv := twoPageServer(t, tc.link)
		before := other.requests()
		entries, err := srv.client(t, Config{}).Files(context.Background(), testRepo, testRev)
		if !errors.Is(err, tc.want) {
			t.Errorf("%s: err = %v, want %v", name, err, tc.want)
		}
		if entries != nil {
			t.Errorf("%s: returned a list %v; a partial or mixed listing would be a wrong deployment ID", name, paths(entries))
		}
		if srv.requests() != 1 {
			t.Errorf("%s: %d requests, want 1 (the link must be refused before it is followed)", name, srv.requests())
		}
		if other.requests() != before {
			t.Errorf("%s: the other origin received a request", name)
		}
	}
}

func TestFilesFollowsALinkInsideTheSnapshotInAnySpelling(t *testing.T) {
	for name, link := range map[string]func(self *httptest.Server) string{
		"absolute":                    func(s *httptest.Server) string { return s.URL + treePath + "?cursor=2&recursive=true" },
		"relative to the host":        func(*httptest.Server) string { return treePath + "?cursor=2&recursive=true" },
		"relative to the page":        func(*httptest.Server) string { return "?cursor=2&recursive=true" },
		"recursive first, other keys": func(*httptest.Server) string { return "?recursive=true&expand=false&cursor=2&limit=50" },
	} {
		srv := twoPageServer(t, link)
		entries, err := srv.client(t, Config{}).Files(context.Background(), testRepo, testRev)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if got, want := paths(entries), []string{"a.safetensors", "b.safetensors"}; !slices.Equal(got, want) {
			t.Errorf("%s: files = %q, want %q", name, got, want)
		}
	}
}

func TestNextPageRefusesAnotherSchemeOfTheSameHost(t *testing.T) {
	base, _ := url.Parse("https://hub.example.org")
	c := &Client{base: base}
	pageURL, _ := url.Parse("https://hub.example.org" + treePath + "?recursive=true")
	for link, want := range map[string]error{
		"http://hub.example.org" + treePath + "?cursor=2&recursive=true":               ErrCrossHostLink, // downgrade
		"https://hub.example.org" + treePath + "?cursor=2&recursive=true":              nil,
		"https://hub.example.org:443" + treePath + "?cursor=2&recursive=true":          nil,
		"https://hub.example.org:8443" + treePath + "?cursor=2&recursive=true":         ErrCrossHostLink,
		"https://hub.example.org.evil.example" + treePath + "?cursor=2&recursive=true": ErrCrossHostLink,
	} {
		_, err := c.nextPage(page{url: pageURL, links: []string{"<" + link + `>; rel="next"`}})
		if want == nil && err != nil || want != nil && !errors.Is(err, want) {
			t.Errorf("link %s: err = %v, want %v", link, err, want)
		}
	}
}

// TestFilesAcceptsARenameRedirectOfTheListing: the Hub answers the tree of a
// renamed repository with a redirect to the new name, and its Links use the
// new name. The next page is compared with the page that carried the link
// (which is the redirect's target), not with the first request.
func TestFilesAcceptsARenameRedirectOfTheListing(t *testing.T) {
	const newName = "/api/models/example-org/renamed/tree/" + testRev
	srv := newTreeServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == treePath:
			http.Redirect(w, r, newName+"?"+r.URL.RawQuery, http.StatusMovedPermanently)
		case r.URL.Query().Get("cursor") != "":
			_, _ = w.Write([]byte(listOf(lfsJSON("b.safetensors"))))
		default:
			w.Header().Set("Link", `<?cursor=2&recursive=true>; rel="next"`) // relative: resolved against the new name
			_, _ = w.Write([]byte(listOf(lfsJSON("a.safetensors"))))
		}
	})
	entries, err := srv.client(t, Config{}).Files(context.Background(), testRepo, testRev)
	if err != nil {
		t.Fatalf("Files: %v", err)
	}
	if got, want := paths(entries), []string{"a.safetensors", "b.safetensors"}; !slices.Equal(got, want) {
		t.Errorf("files = %q, want %q", got, want)
	}
}

func TestFilesRefusesAListingRedirectedAwayFromTheSnapshot(t *testing.T) {
	const otherRev = "ffffffffffffffffffffffffffffffffffffffff"
	for name, target := range map[string]string{
		"to another commit":       "/api/models/" + testRepo + "/tree/" + otherRev + "?recursive=true",
		"to another endpoint":     "/api/models/" + testRepo + "?recursive=true",
		"to a non-API path":       "/" + testRepo + "/tree/" + testRev + "?recursive=true",
		"dropping recursive":      treePath,
		"making it not recursive": treePath + "?recursive=false",
	} {
		srv := newTreeServer(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.RequestURI() == target {
				_, _ = w.Write([]byte(listOf(lfsJSON("moved.safetensors"))))
				return
			}
			http.Redirect(w, r, target, http.StatusFound)
		})
		entries, err := srv.client(t, Config{}).Files(context.Background(), testRepo, testRev)
		if !errors.Is(err, ErrInvalidListing) || entries != nil {
			t.Errorf("%s: Files = %v, %v; want ErrInvalidListing", name, entries, err)
		}
	}
}

// TestListingRedirectsStayOnTheHubsOrigin: the listing may be redirected only
// within the Hub's origin. Downloads may go to a CDN (see redirect_test.go),
// because their bytes are verified; the listing's are not.
func TestListingRedirectsStayOnTheHubsOrigin(t *testing.T) {
	other := newTreeServer(t, serveBody(listOf(lfsJSON("evil.safetensors"))))
	h := newFakeHub(t, allFiles())
	h.treeRedirectTo = other.URL
	path := writeToken(t, testToken, 0o600)
	h.token = testToken
	entries, err := h.client(t, Config{TokenFile: path}).Files(context.Background(), testRepo, testRev)
	if !errors.Is(err, ErrCrossHostLink) || entries != nil {
		t.Fatalf("Files = %v, %v; want ErrCrossHostLink", entries, err)
	}
	if other.requests() != 0 {
		t.Errorf("the other origin received %d requests, %q", other.requests(), other.authHeaders())
	}
	leakCheck(t, "listing redirect", err, testToken)
}

func TestListingRedirectPolicyRefusesPlainHTTPAndOtherHosts(t *testing.T) {
	base, _ := url.Parse("https://hub.example.org")
	policy := listingRedirectPolicy(base)
	for target, refused := range map[string]bool{
		"https://hub.example.org/x":      false,
		"https://hub.example.org:443/x":  false,
		"http://hub.example.org/x":       true, // never to http
		"https://hub.example.org:8443/x": true,
		"https://cdn.example.org/x":      true,
	} {
		u, _ := url.Parse(target)
		req := &http.Request{URL: u, Header: http.Header{"Authorization": {"Bearer x"}}}
		err := policy(req)
		if refused != errors.Is(err, ErrCrossHostLink) {
			t.Errorf("redirect to %s: err = %v, refused should be %v", target, err, refused)
		}
		if refused && req.Header.Get("Authorization") != "" {
			t.Errorf("redirect to %s kept the token", target)
		}
	}
}

// TestListingRedirectToPlainHTTPIsRefusedEndToEnd: an https Hub (a TLS test
// server) that redirects the listing to http.
func TestListingRedirectToPlainHTTPIsRefusedEndToEnd(t *testing.T) {
	plain := newTreeServer(t, serveBody(listOf(lfsJSON("evil.safetensors"))))
	tls := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, plain.URL+r.URL.RequestURI(), http.StatusFound)
	}))
	defer tls.Close()
	c, err := New(Config{BaseURL: tls.URL, HTTPClient: tls.Client()})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := c.Files(context.Background(), testRepo, testRev)
	if !errors.Is(err, ErrCrossHostLink) || entries != nil {
		t.Errorf("Files = %v, %v; want ErrCrossHostLink", entries, err)
	}
	if plain.requests() != 0 {
		t.Errorf("the http server received %d requests", plain.requests())
	}
}
