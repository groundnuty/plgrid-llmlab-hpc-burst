package hubfiles

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"
)

// Members of a valid LFS entry, to be replaced one at a time.
const (
	mType = `"type":"file"`
	mOID  = `"oid":"` + pointerOID + `"`
	mSize = `"size":3`
	mPath = `"path":"m.safetensors"`
	mLFS  = `"lfs":{"oid":"` + sha256ABC + `","size":3,"pointerSize":135}`
)

func obj(members ...string) string { return "{" + strings.Join(members, ",") + "}" }

// good is the entry that every case below damages in one place.
func good(replace ...string) string {
	m := map[string]string{"type": mType, "oid": mOID, "size": mSize, "path": mPath, "lfs": mLFS}
	for i := 0; i+1 < len(replace); i += 2 {
		m[replace[i]] = replace[i+1]
	}
	var parts []string
	for _, k := range []string{"type", "oid", "size", "path", "lfs"} {
		if m[k] != "" {
			parts = append(parts, m[k])
		}
	}
	return obj(parts...)
}

func dirJSON(path string) string {
	return `{"type":"directory","oid":"` + pointerOID + `","size":0,"path":"` + path + `"}`
}

// TestFilesRefusesMalformedListingBodies feeds raw response bodies, because
// the fake Hub builds its JSON with encoding/json and so can never send these.
// Each is a body that a lenient decoder turns into a successful list.
func TestFilesRefusesMalformedListingBodies(t *testing.T) {
	two := listOf(good()) + " " + listOf(good(`path`, `"path":"n.safetensors"`))
	for name, body := range map[string]string{
		// Text that the decoder would repair into another path.
		"invalid UTF-8 in a path":                 listOf(good("path", "\"path\":\"a\xff.json\"")),
		"unpaired high surrogate escape":          listOf(good("path", `"path":"\ud800"`)),
		"unpaired low surrogate escape":           listOf(good("path", `"path":"x\udc00y"`)),
		"high surrogate followed by a plain char": listOf(good("path", `"path":"\ud800\u0041"`)),
		"high surrogate then the closing quote":   listOf(good("path", `"path":"a\ud83d"`)),
		"surrogate in a key":                      listOf(good("path", `"pa\ud800th":"a"`)),

		// One complete array per page.
		"null page":                 `null`,
		"object page":               good(),
		"string page":               `"[]"`,
		"number page":               `3`,
		"empty body":                ``,
		"blank body":                " \n ",
		"a second array after":      two,
		"garbage after the array":   listOf(good()) + ` x`,
		"a truncated array":         `[` + good(),
		"null entry":                `[null]`,
		"number entry":              `[1]`,
		"array entry":               `[[]]`,
		"entry is a string":         `["file"]`,
		"a null among good entries": `[` + good() + `,null]`,

		// Keys are matched exactly.
		"PATH for path":          listOf(good("path", `"PATH":"m.safetensors"`)),
		"OID for oid":            listOf(good("oid", `"OID":"`+pointerOID+`"`)),
		"Type for type":          listOf(good("type", `"Type":"file"`)),
		"Size for size":          listOf(good("size", `"Size":3`)),
		"LFS for lfs":            listOf(good("lfs", `"LFS":{"oid":"`+sha256ABC+`","size":3}`)),
		"both path and PATH":     listOf(good("lfs", mLFS+`,"PATH":"other"`)),
		"OID inside lfs":         listOf(good("lfs", `"lfs":{"OID":"`+sha256ABC+`","size":3}`)),
		"Size inside lfs":        listOf(good("lfs", `"lfs":{"oid":"`+sha256ABC+`","Size":3}`)),
		"path given twice":       listOf(good("lfs", mLFS+`,"path":"n.safetensors"`)),
		"lfs.oid given twice":    listOf(good("lfs", `"lfs":{"oid":"`+sha256ABC+`","oid":"`+sha256Empty+`","size":3}`)),
		"missing type":           listOf(good("type", "")),
		"missing path":           listOf(good("path", "")),
		"missing oid":            listOf(good("oid", "")),
		"path is a number":       listOf(good("path", `"path":5`)),
		"path is null":           listOf(good("path", `"path":null`)),
		"oid is a number":        listOf(good("oid", `"oid":5`)),
		"type is an object":      listOf(good("type", `"type":{}`)),
		"lfs is a number":        listOf(good("lfs", `"lfs":5`)),
		"lfs is a string":        listOf(good("lfs", `"lfs":"x"`)),
		"lfs is an array":        listOf(good("lfs", `"lfs":[]`)),
		"lfs.oid is a number":    listOf(good("lfs", `"lfs":{"oid":5,"size":3}`)),
		"lfs.oid is missing":     listOf(good("lfs", `"lfs":{"size":3}`)),
		"unknown entry type":     listOf(good("type", `"type":"symlink"`)),
		"directory without path": listOf(good(), `{"type":"directory","oid":"`+pointerOID+`","size":0}`),
		"directory path unclean": listOf(good(), dirJSON("../x")),
		"file and its sub-file":  listOf(lfsJSON("a"), lfsJSON("a/b")),
		"file in a deeper path":  listOf(lfsJSON("a/b"), lfsJSON("a/b/c/d"), dirJSON("a/b/c")),
		"file and a directory":   listOf(lfsJSON("a"), dirJSON("a")),
		"directory and a file":   listOf(dirJSON("a"), lfsJSON("a")),
		"directory listed twice": listOf(lfsJSON("a/b"), dirJSON("a"), dirJSON("a")),
		"file listed twice":      listOf(lfsJSON("a"), lfsJSON("a")),

		// Sizes: each is validated alone, and they must agree.
		"size missing":                         listOf(good("size", "")),
		"size is a string":                     listOf(good("size", `"size":"3"`)),
		"size is a float":                      listOf(good("size", `"size":3.5`)),
		"size in exponent form":                listOf(good("size", `"size":3e0`)),
		"size is null":                         listOf(good("size", `"size":null`)),
		"size negative, lfs.size 3":            listOf(good("size", `"size":-1`)),
		"size above int64":                     listOf(good("size", `"size":9223372036854775808`)),
		"lfs.size missing":                     listOf(good("lfs", `"lfs":{"oid":"`+sha256ABC+`"}`)),
		"lfs.size null":                        listOf(good("lfs", `"lfs":{"oid":"`+sha256ABC+`","size":null}`)),
		"lfs.size negative, size fine":         listOf(good("lfs", `"lfs":{"oid":"`+sha256ABC+`","size":-3}`)),
		"lfs.size is a string":                 listOf(good("lfs", `"lfs":{"oid":"`+sha256ABC+`","size":"3"}`)),
		"size and lfs.size disagree":           listOf(good("lfs", `"lfs":{"oid":"`+sha256ABC+`","size":4}`)),
		"size 0, lfs.size 3 (a zeroed size)":   listOf(good("size", `"size":0`)),
		"top-level size is the pointer's size": listOf(good("size", `"size":135`)),
	} {
		srv := newTreeServer(t, serveBody(body))
		entries, err := srv.client(t, Config{}).Files(context.Background(), testRepo, testRev)
		if !errors.Is(err, ErrInvalidListing) {
			t.Errorf("%s: err = %v, want ErrInvalidListing", name, err)
		}
		if entries != nil {
			t.Errorf("%s: a failed call returned a list: %+v", name, entries)
		}
	}
}

func TestFilesRefusesAFileThatLacksItsLFSBlock(t *testing.T) {
	// Without "lfs" the file is an ordinary one: it is downloaded, and the
	// content of an LFS file does not hash to the pointer's blob id.
	h := newFakeHub(t, []fakeFile{{path: "m.safetensors", content: "real weights", oid: pointerOID, size: -1}})
	if _, err := h.client(t, Config{}).Files(context.Background(), testRepo, testRev); !errors.Is(err, ErrBlobMismatch) {
		t.Errorf("err = %v, want ErrBlobMismatch", err)
	}
}

func TestFilesAcceptsWhatTheHubSendsInAnyLegalSpelling(t *testing.T) {
	for name, tc := range map[string]struct {
		body string
		want []string
	}{
		"extra keys are ignored": {listOf(good(), dirJSON("d")), []string{"m.safetensors"}},
		"a surrogate pair is one character": {
			listOf(good("path", `"path":"\ud83d\ude00.safetensors"`)),
			[]string{"\U0001F600.safetensors"},
		},
		"a literal U+FFFD is kept": {
			listOf(good("path", "\"path\":\"a\uFFFDb.safetensors\"")),
			[]string{"a\uFFFDb.safetensors"},
		},
		"white space between tokens": {" [\n " + good() + " ,\n" + lfsJSON("n.safetensors") + "\n]\n", []string{"m.safetensors", "n.safetensors"}},
		"a file next to a directory of a similar name": {
			listOf(lfsJSON("a.bin"), lfsJSON("a/b.bin"), dirJSON("a")),
			[]string{"a.bin", "a/b.bin"},
		},
	} {
		srv := newTreeServer(t, serveBody(tc.body))
		entries, err := srv.client(t, Config{}).Files(context.Background(), testRepo, testRev)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if got := paths(entries); !slices.Equal(got, tc.want) {
			t.Errorf("%s: paths = %q, want %q", name, got, tc.want)
		}
	}
}

func TestFilesRefusesABackslashPathEvenWhenEscapedInJSON(t *testing.T) {
	srv := newTreeServer(t, serveBody(listOf(good("path", `"path":"a\\ud800.safetensors"`))))
	_, err := srv.client(t, Config{}).Files(context.Background(), testRepo, testRev)
	if !errors.Is(err, ErrInvalidListing) {
		t.Errorf("err = %v, want ErrInvalidListing for a path with a backslash", err)
	}
}

func TestFilesEnforcesThePageSizeLimitWithOverflowDetection(t *testing.T) {
	const limit = 1024
	valid := listOf(lfsJSON("a.safetensors"))
	pad := func(n int) string { return valid + strings.Repeat(" ", n-len(valid)) }

	// Exactly at the limit is a page.
	atLimit := newTreeServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(pad(limit)))
	})
	c := atLimit.client(t, Config{})
	c.maxPageBytes = limit
	if _, err := c.Files(context.Background(), testRepo, testRev); err != nil {
		t.Fatalf("a page of exactly the limit: %v", err)
	}

	// One byte over is refused, not cut: a complete listing array within the
	// first limit bytes followed by padding would otherwise be accepted. The
	// chunked variant has no Content-Length, so the read itself must notice.
	for name, chunked := range map[string]bool{"content-length": false, "chunked": true} {
		over := newTreeServer(t, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(pad(limit + 1)))
			if chunked {
				w.(http.Flusher).Flush()
			}
		})
		c := over.client(t, Config{})
		c.maxPageBytes = limit
		_, err := c.Files(context.Background(), testRepo, testRev)
		if !errors.Is(err, ErrInvalidListing) || !strings.Contains(err.Error(), "limit") && !strings.Contains(err.Error(), "longer") {
			t.Errorf("%s: a page of limit+1 bytes: err = %v, want ErrInvalidListing about the size", name, err)
		}
	}
}

// TestFilesRefusesALaterPageThatIsNotAnArray: page 1 is fine and page 2 is
// not. A null that decodes as zero entries would leave page 1's files as a
// successful, shorter list.
func TestFilesRefusesALaterPageThatIsNotAnArray(t *testing.T) {
	for name, second := range map[string]string{
		"null":                "null",
		"empty body":          "",
		"an object":           "{}",
		"a string":            `"x"`,
		"two documents":       "[] []",
		"an array and a null": "[] null",
	} {
		srv := newTreeServer(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("cursor") != "" {
				_, _ = w.Write([]byte(second))
				return
			}
			w.Header().Set("Link", `<?cursor=2&recursive=true>; rel="next"`)
			_, _ = w.Write([]byte(listOf(lfsJSON("a.safetensors"))))
		})
		entries, err := srv.client(t, Config{}).Files(context.Background(), testRepo, testRev)
		if !errors.Is(err, ErrInvalidListing) || entries != nil {
			t.Errorf("%s: Files = %v, %v; want ErrInvalidListing and no list", name, entries, err)
		}
		if srv.requests() != 2 {
			t.Errorf("%s: %d requests, want 2", name, srv.requests())
		}
	}
}
