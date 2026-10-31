package hubfiles

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

const testToken = "hf_AbCdEfGhIjKlMnOpQrStUvWxYz0123456789"

// writeToken writes content to a new file with the given mode and returns its
// path. The mode is set with Chmod, so the umask does not change it.
func writeToken(t *testing.T, content string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "hub-token")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestTokenFileModes(t *testing.T) {
	for _, mode := range []os.FileMode{0o600, 0o400, 0o700} {
		tok, err := readToken(writeToken(t, testToken+"\n", mode))
		if err != nil || tok.reveal() != testToken {
			t.Errorf("mode %04o: token %q, err %v; want the token with its newline trimmed", mode, tok.reveal(), err)
		}
	}
	for _, mode := range []os.FileMode{0o640, 0o644, 0o604, 0o660, 0o666, 0o620, 0o602, 0o750, 0o755} {
		path := writeToken(t, testToken, mode)
		_, err := readToken(path)
		if !errors.Is(err, ErrTokenFile) {
			t.Errorf("mode %04o: err = %v, want ErrTokenFile", mode, err)
			continue
		}
		if strings.Contains(err.Error(), testToken) {
			t.Errorf("mode %04o: the error holds the token: %v", mode, err)
		}
		if !strings.Contains(err.Error(), fmt.Sprintf("%04o", mode)) {
			t.Errorf("mode %04o: the error does not state the mode: %v", mode, err)
		}
	}
}

func TestTokenFileRefusesWhatIsNotAToken(t *testing.T) {
	dir := t.TempDir()
	cases := map[string]string{
		"missing":       filepath.Join(dir, "absent"),
		"directory":     dir,
		"empty":         writeToken(t, "", 0o600),
		"blank":         writeToken(t, " \n\t\n", 0o600),
		"two lines":     writeToken(t, "hf_first\nhf_second\n", 0o600),
		"inner space":   writeToken(t, "hf_secretpart1 hf_secretpart2", 0o600),
		"control char":  writeToken(t, "hf_secret\x01tail", 0o600),
		"non-ASCII":     writeToken(t, "hf_secretéé", 0o600),
		"over the size": writeToken(t, strings.Repeat("x", 5000), 0o600),
	}
	for name, path := range cases {
		_, err := readToken(path)
		if !errors.Is(err, ErrTokenFile) {
			t.Errorf("%s: err = %v, want ErrTokenFile", name, err)
			continue
		}
		for _, secret := range []string{"hf_secret", "hf_first", "hf_second"} {
			if strings.Contains(err.Error(), secret) {
				t.Errorf("%s: the error echoes the file content: %v", name, err)
			}
		}
	}
}

func TestTokenIsRefusedBeforeAnyRequestWhenTheFileIsOpen(t *testing.T) {
	h := newFakeHub(t, allFiles())
	path := writeToken(t, testToken, 0o644)
	_, err := h.client(t, Config{TokenFile: path}).Files(context.Background(), testRepo, testRev)
	if !errors.Is(err, ErrTokenFile) {
		t.Fatalf("err = %v, want ErrTokenFile", err)
	}
	if got := h.authHeaders(); len(got) != 0 {
		t.Errorf("%d requests were made with a token file that is too open", len(got))
	}
}

func TestTokenIsSentToTheHubOnListingAndDownloads(t *testing.T) {
	h := newFakeHub(t, allFiles())
	h.token = testToken
	h.pageSize = 8
	path := writeToken(t, testToken+"\n", 0o600)
	entries, err := h.client(t, Config{TokenFile: path}).Files(context.Background(), testRepo, testRev)
	if err != nil {
		t.Fatalf("Files with the right token: %v", err)
	}
	if len(entries) != len(allFiles()) {
		t.Errorf("%d entries, want %d", len(entries), len(allFiles()))
	}
	got := h.authHeaders()
	tree, resolves := h.requests()
	if len(got) != len(tree)+len(resolves) || len(got) < 3 {
		t.Fatalf("%d requests with %d recorded, want every request recorded", len(got), len(tree)+len(resolves))
	}
	for i, a := range got {
		if a != "Bearer "+testToken {
			t.Errorf("request %d carried %q, want the bearer token", i, a)
		}
	}
}

func TestTokenFileIsReadAgainOnEveryCall(t *testing.T) {
	h := newFakeHub(t, allFiles())
	h.token = testToken
	path := writeToken(t, "hf_stale_token_value", 0o600)
	c := h.client(t, Config{TokenFile: path})
	if _, err := c.Files(context.Background(), testRepo, testRev); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("with the stale token: err = %v, want ErrUnauthorized", err)
	}
	if err := os.WriteFile(path, []byte(testToken), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Files(context.Background(), testRepo, testRev); err != nil {
		t.Errorf("after the token file was rotated: %v", err)
	}
}

// leakCheck fails when text holds the token, in any of the usual renderings of
// an error.
func leakCheck(t *testing.T, what string, err error, secrets ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: no error", what)
	}
	var he *HTTPError
	renderings := []string{err.Error(), fmt.Sprintf("%v", err), fmt.Sprintf("%+v", err), fmt.Sprintf("%#v", err), fmt.Sprintf("%q", err)}
	if errors.As(err, &he) {
		renderings = append(renderings, fmt.Sprintf("%+v", *he), fmt.Sprintf("%#v", *he))
	}
	for _, r := range renderings {
		for _, s := range secrets {
			if strings.Contains(r, s) {
				t.Errorf("%s: the token appears in %q", what, r)
			}
		}
	}
}

func TestTokenNeverReachesAnErrorAfterAnAuthFailure(t *testing.T) {
	wrong := "hf_WrongTokenValue_0123456789abcdef"

	h := newFakeHub(t, allFiles())
	h.token = testToken
	// Prove the echo is real: a plain request with the wrong token gets it
	// back in a header and in the body. Without this the leak check below
	// could pass because the server never repeated anything.
	req, _ := http.NewRequest(http.MethodGet, h.srv.URL+"/api/models/"+testRepo+"/tree/"+testRev, nil)
	req.Header.Set("Authorization", "Bearer "+wrong)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), wrong) || !strings.Contains(resp.Header.Get("X-Error-Message"), wrong) {
		t.Fatalf("the fake does not echo the Authorization header; the leak check would be vacuous (body %q)", body)
	}

	// A rejected token on the listing.
	path := writeToken(t, wrong, 0o600)
	_, err = h.client(t, Config{TokenFile: path}).Files(context.Background(), testRepo, testRev)
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("err = %v, want ErrUnauthorized", err)
	}
	var he *HTTPError
	if !errors.As(err, &he) || !he.TokenSent {
		t.Errorf("HTTPError = %+v, want TokenSent", he)
	}
	leakCheck(t, "rejected on the listing", err, wrong, "Bearer")

	// A 401 on a download, after the listing succeeded.
	h2 := newFakeHub(t, allFiles())
	h2.denyDownloads = true
	path2 := writeToken(t, testToken, 0o600)
	_, err = h2.client(t, Config{TokenFile: path2}).Files(context.Background(), testRepo, testRev)
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("download: err = %v, want ErrUnauthorized", err)
	}
	leakCheck(t, "401 on a download", err, testToken, "Bearer")
}

func TestTokenIsNotSentToAnotherHostByALink(t *testing.T) {
	var (
		mu   sync.Mutex
		hits []string
	)
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits = append(hits, r.URL.RequestURI()+" auth="+r.Header.Get("Authorization"))
		mu.Unlock()
		_, _ = w.Write([]byte(`[]`))
	}))
	defer other.Close()

	h := newFakeHub(t, allFiles())
	h.token = testToken
	h.pageSize = 5
	h.linkTo = other.URL
	path := writeToken(t, testToken, 0o600)
	_, err := h.client(t, Config{TokenFile: path}).Files(context.Background(), testRepo, testRev)
	if !errors.Is(err, ErrCrossHostLink) {
		t.Fatalf("err = %v, want ErrCrossHostLink", err)
	}
	leakCheck(t, "cross-host link", err, testToken)
	mu.Lock()
	defer mu.Unlock()
	if len(hits) != 0 {
		t.Errorf("the other host received %v", hits)
	}
}

func TestTokenIsDroppedOnARedirectToAnotherHost(t *testing.T) {
	var (
		mu   sync.Mutex
		auth = map[string]string{} // path -> Authorization the other host saw
	)
	content := map[string]string{}
	for _, f := range fixtures {
		content["/"+f.path] = f.content
	}
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		auth[r.URL.Path] = r.Header.Get("Authorization")
		mu.Unlock()
		_, _ = w.Write([]byte(content[r.URL.Path]))
	}))
	defer other.Close()

	h := newFakeHub(t, allFiles())
	h.token = testToken
	h.redirectTo = other.URL
	path := writeToken(t, testToken, 0o600)
	if _, err := h.client(t, Config{TokenFile: path}).Files(context.Background(), testRepo, testRev); err != nil {
		t.Fatalf("Files through redirects: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(auth) != len(fixtures) {
		t.Fatalf("the other host served %d downloads, want %d", len(auth), len(fixtures))
	}
	for p, a := range auth {
		if a != "" {
			t.Errorf("the other host received an Authorization header for %s", p)
		}
	}
}

func TestTokenPrintsRedactedUnderEveryVerb(t *testing.T) {
	tok := token(testToken)
	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%d"} {
		for _, v := range []any{tok, &tok, []token{tok}, struct{ T token }{tok}, map[string]token{"k": tok}} {
			if got := fmt.Sprintf(format, v); strings.Contains(got, testToken) || strings.Contains(strings.ToLower(got), strings.ToLower(testToken[:12])) {
				t.Errorf("Sprintf(%q) of %T = %q holds the token", format, v, got)
			}
		}
	}
	if got := fmt.Sprint(tok); got != "[redacted]" {
		t.Errorf("Sprint = %q, want [redacted]", got)
	}
	c, err := New(Config{TokenFile: "/path/to/token"})
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprintf("%+v", *c); strings.Contains(got, testToken) {
		t.Errorf("client prints the token: %s", got)
	}
}

func TestScrubBlanksATokenInAnyErrorText(t *testing.T) {
	err := scrub(fmt.Errorf("lower layer said: Authorization: Bearer %s was refused", testToken), token(testToken))
	if strings.Contains(err.Error(), testToken) || !strings.Contains(err.Error(), "[redacted]") {
		t.Errorf("scrub = %q", err)
	}
	var cause error = io.EOF
	untouched := scrub(cause, token(testToken))
	if untouched != cause {
		t.Errorf("an error without the token was replaced: %v", untouched)
	}
	if got := scrub(cause, ""); got != cause {
		t.Errorf("with no token, scrub changed the error: %v", got)
	}
}
