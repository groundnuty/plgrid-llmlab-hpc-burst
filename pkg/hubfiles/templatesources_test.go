package hubfiles

import (
	"context"
	"errors"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
)

// The template files below were hashed once with git hash-object and
// sha256sum, outside this package, and pasted here. The tests build their
// entries from these literals (never from Files, except where a test says it
// feeds Files' result on purpose), so an entry is right or wrong by
// construction.
var (
	jinjaFx = fixture{"chat_template.jinja", "{{ bos_token }}{% for m in messages %}{{ m.content }}{% endfor %}\n",
		"3a6728e7d122cf3853e7cb00579a554cc2bce966", "2a471ae0defffc3d84c52a5a5e09f0c1e5a7f60ed0c5d9f6eb2f3b5728bc1f5c"}
	jsonFx = fixture{"chat_template.json", "{\"chat_template\": \"JSON TEMPLATE {{ x }}\"}\n",
		"271db30d1a057d1371f0842025a0f2d2bd086d56", "6dcb82a735b70599444584bcc7be6984a2f2addc883b69b2281d27a0efe90345"}
	configFx = fixture{"tokenizer_config.json", "{\"model_max_length\": 8192, \"chat_template\": \"CONFIG TEMPLATE {{ y }}\"}\n",
		"180002d16fad7e0de407cfaed7aad139ee025a68", "61dd3e5723dc3cb4ba87e4456f6c35be81f8cc23d20ed5f898d43dd221b96bc9"}
	// Not valid UTF-8: the bytes ff fe, then "{{ bad }}" and a newline.
	badJinjaFx = fixture{"chat_template.jinja", "\xff\xfe{{ bad }}\n",
		"fcf7108908e32c3758b35b04b0a35c719cae1179", "81462064977fd466b69a524f87c5475637281fb726d4973a2b01f2f0aeeffb56"}
	allTemplateFx = []fixture{jinjaFx, jsonFx, configFx}
)

// entryOf is the Entry of a file that is not in LFS, from a fixture's pasted
// values, as Files would return it.
func entryOf(f fixture) Entry {
	e := Entry{BlobID: f.blob, Size: int64(len(f.content))}
	e.Path = f.path
	e.SHA256 = f.sha256
	return e
}

// listingOf is the entries for the fixtures, with the files Files returns for
// the rest of a checkpoint.
func listingOf(fs ...fixture) []Entry {
	entries := []Entry{entryOf(fixtureNamed("config.json")), entryOf(fixtureNamed("a/b.json"))}
	for _, f := range fs {
		entries = append(entries, entryOf(f))
	}
	return entries
}

// servedFiles is what the serving Hub holds: honest copies of the fixtures,
// and of the ordinary files.
func servedFiles(fs ...fixture) []fakeFile {
	files := []fakeFile{plainFile(fixtureNamed("config.json")), plainFile(fixtureNamed("a/b.json"))}
	for _, f := range fs {
		files = append(files, plainFile(f))
	}
	return files
}

// tampered serves other bytes of the same length for the fixture's path, under
// the fixture's real blob id.
func tampered(f fixture) fakeFile {
	ff := plainFile(f)
	ff.content = sameSize(f.content)
	return ff
}

func replaceServed(files []fakeFile, with fakeFile) []fakeFile {
	out := slices.Clone(files)
	i := slices.IndexFunc(out, func(f fakeFile) bool { return f.path == with.path })
	if i < 0 {
		panic("no served file " + with.path)
	}
	out[i] = with
	return out
}

func assertNoSources(t *testing.T, what string, src deploymentid.TemplateSources) {
	t.Helper()
	if src.Spec != nil || src.Jinja != nil || src.ChatTemplateJSON != nil || src.TokenizerConfig != nil {
		t.Errorf("%s: a failed call returned sources: %+v", what, src)
	}
}

func TestTemplateSourcesReadsTheThreeFilesFromTheGivenEntries(t *testing.T) {
	h := newFakeHub(t, servedFiles(allTemplateFx...))
	src, err := h.client(t, Config{}).TemplateSources(context.Background(), testRepo, testRev, listingOf(allTemplateFx...))
	if err != nil {
		t.Fatal(err)
	}
	if src.Jinja == nil || *src.Jinja != jinjaFx.content {
		t.Errorf("Jinja = %v, want %q", src.Jinja, jinjaFx.content)
	}
	if string(src.ChatTemplateJSON) != jsonFx.content {
		t.Errorf("ChatTemplateJSON = %q, want %q", src.ChatTemplateJSON, jsonFx.content)
	}
	if string(src.TokenizerConfig) != configFx.content {
		t.Errorf("TokenizerConfig = %q, want %q", src.TokenizerConfig, configFx.content)
	}
	if src.Spec != nil {
		t.Errorf("Spec = %q, want nil: the caller sets the serving spec's template", *src.Spec)
	}
	// The listing is not read again, and only the three files are downloaded.
	tree, resolves := h.requests()
	if len(tree) != 0 {
		t.Errorf("%d tree requests, want none: TemplateSources takes the entries it is given", len(tree))
	}
	slices.Sort(resolves)
	if want := []string{"chat_template.jinja", "chat_template.json", "tokenizer_config.json"}; !slices.Equal(resolves, want) {
		t.Errorf("downloads = %q, want %q", resolves, want)
	}
}

func TestTemplateSourcesWithNoTemplateFileReadsNothing(t *testing.T) {
	h := newFakeHub(t, servedFiles(allTemplateFx...))
	// The Hub holds the template files, but the entries do not list them.
	src, err := h.client(t, Config{}).TemplateSources(context.Background(), testRepo, testRev, listingOf())
	if err != nil {
		t.Fatal(err)
	}
	assertNoSources(t, "no template file", src)
	if got := h.authHeaders(); len(got) != 0 {
		t.Errorf("%d requests, want none", len(got))
	}
}

func TestTemplateSourcesWithOnlyTokenizerConfig(t *testing.T) {
	h := newFakeHub(t, servedFiles(allTemplateFx...))
	src, err := h.client(t, Config{}).TemplateSources(context.Background(), testRepo, testRev, listingOf(configFx))
	if err != nil {
		t.Fatal(err)
	}
	if src.Jinja != nil || src.ChatTemplateJSON != nil || src.Spec != nil {
		t.Errorf("fields of absent files are set: %+v", src)
	}
	if string(src.TokenizerConfig) != configFx.content {
		t.Errorf("TokenizerConfig = %q, want %q", src.TokenizerConfig, configFx.content)
	}
	if _, resolves := h.requests(); !slices.Equal(resolves, []string{"tokenizer_config.json"}) {
		t.Errorf("downloads = %q, want only tokenizer_config.json", resolves)
	}
}

func TestTemplateSourcesMatchesTheRootFilesExactly(t *testing.T) {
	h := newFakeHub(t, servedFiles(allTemplateFx...))
	var entries []Entry
	for _, path := range []string{"Chat_Template.jinja", "CHAT_TEMPLATE.JSON", "sub/chat_template.jinja", "sub/tokenizer_config.json", "chat_template.jinja.bak", "tokenizer_config.json.orig"} {
		e := entryOf(jinjaFx)
		e.Path = path
		entries = append(entries, e)
	}
	src, err := h.client(t, Config{}).TemplateSources(context.Background(), testRepo, testRev, entries)
	if err != nil {
		t.Fatal(err)
	}
	assertNoSources(t, "files of another case, another directory or another name", src)
	if got := h.authHeaders(); len(got) != 0 {
		t.Errorf("%d requests, want none", len(got))
	}
}

func TestTemplateSourcesGivesAPresentEmptyFileAsEmptyNotNil(t *testing.T) {
	empty := func(path string) fixture {
		return fixture{path, "", "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391", sha256Empty}
	}
	files := []fixture{empty("chat_template.jinja"), empty("chat_template.json"), empty("tokenizer_config.json")}
	h := newFakeHub(t, servedFiles(files...))
	src, err := h.client(t, Config{}).TemplateSources(context.Background(), testRepo, testRev, listingOf(files...))
	if err != nil {
		t.Fatal(err)
	}
	if src.Jinja == nil || *src.Jinja != "" {
		t.Errorf("Jinja = %v, want a pointer to the empty string (present, empty)", src.Jinja)
	}
	if src.ChatTemplateJSON == nil || len(src.ChatTemplateJSON) != 0 {
		t.Errorf("ChatTemplateJSON = %#v, want non-nil and empty", src.ChatTemplateJSON)
	}
	if src.TokenizerConfig == nil || len(src.TokenizerConfig) != 0 {
		t.Errorf("TokenizerConfig = %#v, want non-nil and empty", src.TokenizerConfig)
	}
}

func TestTemplateSourcesRefusesAJinjaThatIsNotUTF8(t *testing.T) {
	h := newFakeHub(t, servedFiles(badJinjaFx, jsonFx, configFx))
	src, err := h.client(t, Config{}).TemplateSources(context.Background(), testRepo, testRev, listingOf(badJinjaFx, jsonFx, configFx))
	if !errors.Is(err, deploymentid.ErrInvalid) {
		t.Fatalf("err = %v, want deploymentid.ErrInvalid", err)
	}
	if !strings.Contains(err.Error(), "chat_template.jinja") {
		t.Errorf("the error does not name the file: %v", err)
	}
	assertNoSources(t, "invalid UTF-8", src)
}

func TestTemplateSourcesFailsAsAWholeWhenOneFileDiffersFromItsEntry(t *testing.T) {
	// For each template file in turn: the Hub serves other bytes of the same
	// length, and the entry is right by its blob id and its SHA-256. The files
	// that come before and after it are fine and must not be returned.
	for _, bad := range allTemplateFx {
		files := replaceServed(servedFiles(allTemplateFx...), tampered(bad))
		h := newFakeHub(t, files)
		src, err := h.client(t, Config{}).TemplateSources(context.Background(), testRepo, testRev, listingOf(allTemplateFx...))
		if !errors.Is(err, ErrBlobMismatch) {
			t.Errorf("%s: err = %v, want ErrBlobMismatch", bad.path, err)
		}
		if err != nil && !strings.Contains(err.Error(), bad.path) {
			t.Errorf("%s: the error does not name the file: %v", bad.path, err)
		}
		assertNoSources(t, bad.path, src)
	}
}

func TestTemplateSourcesChecksTheBlobIDAndTheSHA256OfAnEntryOnItsOwn(t *testing.T) {
	// The Hub serves the real bytes in all three cases; the entry is wrong in
	// one of its two hashes. Each hash is checked by itself.
	for name, mutate := range map[string]func(*Entry){
		"the blob id differs": func(e *Entry) { e.BlobID = fixtureNamed("config.json").blob },
		"the SHA-256 differs": func(e *Entry) { e.SHA256 = fixtureNamed("config.json").sha256 },
	} {
		entries := listingOf(allTemplateFx...)
		i := slices.IndexFunc(entries, func(e Entry) bool { return e.Path == "chat_template.json" })
		mutate(&entries[i])
		h := newFakeHub(t, servedFiles(allTemplateFx...))
		src, err := h.client(t, Config{}).TemplateSources(context.Background(), testRepo, testRev, entries)
		if !errors.Is(err, ErrBlobMismatch) {
			t.Errorf("%s: err = %v, want ErrBlobMismatch", name, err)
		}
		assertNoSources(t, name, src)
	}

	// An entry with no SHA-256 (hand-built) is checked by its blob id alone.
	entries := listingOf(allTemplateFx...)
	for i := range entries {
		entries[i].SHA256 = ""
	}
	h := newFakeHub(t, replaceServed(servedFiles(allTemplateFx...), tampered(jsonFx)))
	if _, err := h.client(t, Config{}).TemplateSources(context.Background(), testRepo, testRev, entries); !errors.Is(err, ErrBlobMismatch) {
		t.Errorf("no SHA-256 in the entries, altered bytes: err = %v, want ErrBlobMismatch", err)
	}
}

func TestTemplateSourcesRefusesAContentThatDiffersFromTheEntrySize(t *testing.T) {
	for name, size := range map[string]int64{"the entry says longer": 500, "the entry says shorter": 3} {
		entries := listingOf(allTemplateFx...)
		i := slices.IndexFunc(entries, func(e Entry) bool { return e.Path == "tokenizer_config.json" })
		entries[i].Size = size
		h := newFakeHub(t, servedFiles(allTemplateFx...))
		src, err := h.client(t, Config{}).TemplateSources(context.Background(), testRepo, testRev, entries)
		if !errors.Is(err, ErrSizeMismatch) {
			t.Errorf("%s: err = %v, want ErrSizeMismatch", name, err)
		}
		assertNoSources(t, name, src)
	}
}

func TestTemplateSourcesTakesTheLFSFlagFromTheEntries(t *testing.T) {
	lfsEntry := func(f fixture) Entry {
		e := entryOf(f)
		e.LFS, e.BlobID = true, pointerOID // the blob id of the pointer file, which no content matches
		return e
	}
	entries := []Entry{lfsEntry(configFx)}

	h := newFakeHub(t, []fakeFile{lfsContentFile(configFx.path, configFx.content, configFx.sha256)})
	src, err := h.client(t, Config{}).TemplateSources(context.Background(), testRepo, testRev, entries)
	if err != nil {
		t.Fatalf("an LFS file with its right SHA-256: %v", err)
	}
	if string(src.TokenizerConfig) != configFx.content {
		t.Errorf("TokenizerConfig = %q, want %q", src.TokenizerConfig, configFx.content)
	}

	h = newFakeHub(t, []fakeFile{lfsContentFile(configFx.path, sameSize(configFx.content), configFx.sha256)})
	src, err = h.client(t, Config{}).TemplateSources(context.Background(), testRepo, testRev, entries)
	if !errors.Is(err, ErrBlobMismatch) {
		t.Errorf("an LFS file with altered bytes: err = %v, want ErrBlobMismatch", err)
	}
	assertNoSources(t, "LFS SHA-256 mismatch", src)
}

func TestTemplateSourcesRefusesAFileAboveTheLimitBeforeAnyDownload(t *testing.T) {
	for name, lfs := range map[string]bool{"not in LFS": false, "in LFS": true} {
		entries := listingOf(allTemplateFx...)
		i := slices.IndexFunc(entries, func(e Entry) bool { return e.Path == "tokenizer_config.json" })
		entries[i].Size, entries[i].LFS = 10_000, lfs
		h := newFakeHub(t, servedFiles(allTemplateFx...))
		src, err := h.client(t, Config{MaxFileBytes: 1000}).TemplateSources(context.Background(), testRepo, testRev, entries)
		if !errors.Is(err, ErrFileTooLarge) {
			t.Errorf("%s: err = %v, want ErrFileTooLarge", name, err)
		}
		assertNoSources(t, name, src)
		if got := h.authHeaders(); len(got) != 0 {
			t.Errorf("%s: %d requests, want none: the entries settle it", name, len(got))
		}
	}
}

func TestTemplateSourcesRefusesTwoEntriesWithOnePathBeforeAnyRequest(t *testing.T) {
	h := newFakeHub(t, servedFiles(allTemplateFx...))
	entries := append(listingOf(allTemplateFx...), entryOf(jsonFx))
	src, err := h.client(t, Config{}).TemplateSources(context.Background(), testRepo, testRev, entries)
	if !errors.Is(err, ErrInvalidListing) {
		t.Errorf("err = %v, want ErrInvalidListing", err)
	}
	assertNoSources(t, "duplicate entries", src)
	if got := h.authHeaders(); len(got) != 0 {
		t.Errorf("%d requests, want none: the entries settle it", len(got))
	}
}

func TestTemplateSourcesRefusesABadRevisionOrRepositoryBeforeAnyRequest(t *testing.T) {
	h := newFakeHub(t, servedFiles(allTemplateFx...))
	c := h.client(t, Config{})
	entries := listingOf(allTemplateFx...)
	for _, rev := range []string{"", "main", testRev[:7], strings.ToUpper(testRev)} {
		if _, err := c.TemplateSources(context.Background(), testRepo, rev, entries); !errors.Is(err, ErrInvalidRevision) {
			t.Errorf("revision %q: err = %v, want ErrInvalidRevision", rev, err)
		}
	}
	for _, repo := range []string{"", "../x", "a/b/c"} {
		if _, err := c.TemplateSources(context.Background(), repo, testRev, entries); !errors.Is(err, ErrInvalidRepository) {
			t.Errorf("repository %q: err = %v, want ErrInvalidRepository", repo, err)
		}
	}
	if got := h.authHeaders(); len(got) != 0 {
		t.Errorf("%d requests were made for refused arguments", len(got))
	}
}

func TestTemplateSourcesSendsTheTokenOnlyInTheAuthorizationHeader(t *testing.T) {
	h := newFakeHub(t, servedFiles(allTemplateFx...))
	h.token = testToken
	path := writeToken(t, testToken+"\n", 0o600)
	c := h.client(t, Config{TokenFile: path})
	if _, err := c.TemplateSources(context.Background(), testRepo, testRev, listingOf(allTemplateFx...)); err != nil {
		t.Fatal(err)
	}
	auths := h.authHeaders()
	tree, resolves := h.requests()
	if len(auths) != 3 || len(resolves) != 3 || len(tree) != 0 {
		t.Fatalf("%d requests recorded for %d tree and %d download requests, want the three downloads", len(auths), len(tree), len(resolves))
	}
	for i, a := range auths {
		if a != "Bearer "+testToken {
			t.Errorf("request %d carried %q, want the bearer token", i, a)
		}
	}
	for _, u := range resolves {
		if strings.Contains(u, testToken) {
			t.Errorf("the token is in a URL: %s", u)
		}
	}

	// The token file is read again on every call.
	if err := os.WriteFile(path, []byte("hf_stale_token_value"), 0o600); err != nil {
		t.Fatal(err)
	}
	src, err := c.TemplateSources(context.Background(), testRepo, testRev, listingOf(allTemplateFx...))
	if !errors.Is(err, ErrUnauthorized) {
		t.Errorf("with the stale token: err = %v, want ErrUnauthorized", err)
	}
	assertNoSources(t, "stale token", src)
	if err := os.WriteFile(path, []byte(testToken), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := c.TemplateSources(context.Background(), testRepo, testRev, listingOf(allTemplateFx...)); err != nil {
		t.Errorf("after the token file was rotated: %v", err)
	}
}

func TestTemplateSourcesRefusesATokenFileThatIsTooOpenBeforeAnyRequest(t *testing.T) {
	h := newFakeHub(t, servedFiles(allTemplateFx...))
	c := h.client(t, Config{TokenFile: writeToken(t, testToken, 0o644)})
	if _, err := c.TemplateSources(context.Background(), testRepo, testRev, listingOf(allTemplateFx...)); !errors.Is(err, ErrTokenFile) {
		t.Fatalf("err = %v, want ErrTokenFile", err)
	}
	if got := h.authHeaders(); len(got) != 0 {
		t.Errorf("%d requests were made with a token file that is too open", len(got))
	}
}

func TestTemplateSourcesErrorsNeverHoldTheToken(t *testing.T) {
	h := newFakeHub(t, servedFiles(allTemplateFx...))
	h.denyDownloads = true
	src, err := h.client(t, Config{TokenFile: writeToken(t, testToken, 0o600)}).TemplateSources(context.Background(), testRepo, testRev, listingOf(allTemplateFx...))
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("err = %v, want ErrUnauthorized", err)
	}
	leakCheck(t, "401 on a download", err, testToken, "Bearer")
	assertNoSources(t, "401", src)

	h2 := newFakeHub(t, servedFiles(allTemplateFx...))
	c := h2.client(t, Config{TokenFile: writeToken(t, testToken, 0o600), HTTPClient: &http.Client{Transport: leakyTransport()}})
	src, err = c.TemplateSources(context.Background(), testRepo, testRev, listingOf(allTemplateFx...))
	if err == nil {
		t.Fatal("no error from a transport that fails every download")
	}
	if !strings.Contains(err.Error(), redacted) {
		t.Errorf("the error does not show the token was blanked: %v", err)
	}
	leakCheck(t, "transport error", err, testToken, "Bearer "+testToken)
	if !Retryable(err) {
		t.Errorf("the scrubbed error lost its classification (a cut connection is retryable): %v", err)
	}
	assertNoSources(t, "transport error", src)
}

// TestTemplateSourcesFeedsResolveChatTemplate runs the chain the lifecycle
// controller runs: Files, then TemplateSources on its entries, then the
// resolver. The expected template of each case is written out here by hand.
func TestTemplateSourcesFeedsResolveChatTemplate(t *testing.T) {
	for name, tc := range map[string]struct {
		files []fixture
		want  string
	}{
		"chat_template.jinja alone":        {[]fixture{jinjaFx}, "{{ bos_token }}{% for m in messages %}{{ m.content }}{% endfor %}\n"},
		"chat_template.json alone":         {[]fixture{jsonFx}, "JSON TEMPLATE {{ x }}"},
		"tokenizer_config.json alone":      {[]fixture{configFx}, "CONFIG TEMPLATE {{ y }}"},
		"all three: the jinja file wins":   {allTemplateFx, "{{ bos_token }}{% for m in messages %}{{ m.content }}{% endfor %}\n"},
		"json and config: the json wins":   {[]fixture{jsonFx, configFx}, "JSON TEMPLATE {{ x }}"},
		"jinja and config: the jinja wins": {[]fixture{jinjaFx, configFx}, "{{ bos_token }}{% for m in messages %}{{ m.content }}{% endfor %}\n"},
		"none: the empty template":         {nil, ""},
	} {
		files := allFiles()
		for _, f := range tc.files {
			files = append(files, plainFile(f))
		}
		h := newFakeHub(t, files)
		c := h.client(t, Config{})
		entries, err := c.Files(context.Background(), testRepo, testRev)
		if err != nil {
			t.Fatalf("%s: Files: %v", name, err)
		}
		treeBefore, _ := h.requests()
		src, err := c.TemplateSources(context.Background(), testRepo, testRev, entries)
		if err != nil {
			t.Fatalf("%s: TemplateSources: %v", name, err)
		}
		if treeAfter, _ := h.requests(); len(treeAfter) != len(treeBefore) {
			t.Errorf("%s: TemplateSources read the listing again", name)
		}
		got, err := deploymentid.ResolveChatTemplate(src)
		if err != nil {
			t.Fatalf("%s: ResolveChatTemplate: %v", name, err)
		}
		if got != tc.want {
			t.Errorf("%s: template = %q, want %q", name, got, tc.want)
		}
	}

	// The caller's own template still wins over every file.
	files := allFiles()
	for _, f := range allTemplateFx {
		files = append(files, plainFile(f))
	}
	h := newFakeHub(t, files)
	c := h.client(t, Config{})
	entries, err := c.Files(context.Background(), testRepo, testRev)
	if err != nil {
		t.Fatal(err)
	}
	src, err := c.TemplateSources(context.Background(), testRepo, testRev, entries)
	if err != nil {
		t.Fatal(err)
	}
	spec := "SPEC TEMPLATE"
	src.Spec = &spec
	if got, err := deploymentid.ResolveChatTemplate(src); err != nil || got != "SPEC TEMPLATE" {
		t.Errorf("with the spec's template: %q, %v; want %q", got, err, "SPEC TEMPLATE")
	}
}
