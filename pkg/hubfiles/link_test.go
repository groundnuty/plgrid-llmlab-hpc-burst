package hubfiles

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"testing"
)

func TestNextLinkTarget(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []string
		want   string
	}{
		{"plain", []string{`<https://h/p?c=1%3D%3D>; rel="next"`}, "https://h/p?c=1%3D%3D"},
		{"unquoted rel", []string{`<https://h/p>; rel=next`}, "https://h/p"},
		{"other relation only", []string{`<https://h/p>; rel="prev"`}, ""},
		{"two links in one value", []string{`<https://h/a>; rel="prev", <https://h/b>; rel="next"`}, "https://h/b"},
		{"two header lines", []string{`<https://h/a>; rel="prev"`, `<https://h/b>; rel="next"`}, "https://h/b"},
		{"rel with several values", []string{`<https://h/a>; rel="next last"`}, "https://h/a"},
		{"extra parameter first", []string{`<https://h/a>; title="x"; rel="next"`}, "https://h/a"},
		{"none", nil, ""},
		{"empty value", []string{``}, ""},
		{"names and values in other case", []string{`<https://h/a>; REL="NEXT"`}, "https://h/a"},
		{"blanks around the equals sign", []string{`<https://h/a> ;rel = "next"`}, "https://h/a"},
		{"empty list elements and parameters", []string{`, ,<https://h/a>;; rel="next",,`}, "https://h/a"},
		{"a comma inside the target", []string{`<https://h/p?a=1,2>; rel="next"`}, "https://h/p?a=1,2"},
		{"the same next link twice", []string{`<https://h/a>; rel="next"`, `<https://h/a>; rel="next"`}, "https://h/a"},

		// A quoted comma or semicolon is text, not a delimiter. The first row is
		// the Link that a regular expression on [^;,] reads as "no next page".
		{"quoted comma before rel", []string{`<?cursor=2&recursive=true>; title="page 2, continued"; rel="next"`}, "?cursor=2&recursive=true"},
		{"quoted semicolon and rel text, real rel is prev", []string{`<https://h/a>; title="x; rel=next"; rel="prev"`}, ""},
		{"escaped quote and comma", []string{`<https://h/a>; title="say \"hi\", ok"; rel="next"`}, "https://h/a"},
		{"a quoted link-looking text", []string{`<https://h/a>; title="x, <https://h/evil>; rel=next"; rel="prev"`}, ""},
		{"a quoted backslash before the closing quote", []string{`<https://h/a>; title="c:\\"; rel="next"`}, "https://h/a"},
		{"extended parameter name", []string{`<https://h/a>; title*=UTF-8''p%C3%A9; rel="next"`}, "https://h/a"},
	} {
		got, err := nextLinkTarget(tc.values)
		if err != nil || got != tc.want {
			t.Errorf("%s: nextLinkTarget(%q) = %q, %v; want %q", tc.name, tc.values, got, err, tc.want)
		}
	}
}

func TestNextLinkTargetRefusesWhatItCannotReadUnambiguously(t *testing.T) {
	for name, values := range map[string][]string{
		"not a link":                   {`not a link`},
		"no closing angle bracket":     {`<https://h/a; rel="next"`},
		"space in the target":          {`<https://h/a b>; rel=next`},
		"unterminated quoted string":   {`<https://h/a>; title="abc; rel=next`},
		"quote closes early":           {`<https://h/a>; title="oops; rel="next"`},
		"ends in a backslash":          {`<https://h/a>; title="abc\`},
		"parameter without a name":     {`<https://h/a>; ="x"`},
		"empty parameter value":        {`<https://h/a>; rel=`},
		"junk after a link":            {`<https://h/a>; rel="next" junk`},
		"junk in a second header line": {`<https://h/a>; rel="prev"`, `garbage`},
		"two different next links":     {`<https://h/a>; rel="next", <https://h/b>; rel="next"`},
		"two next links on two lines":  {`<https://h/a>; rel="next"`, `<https://h/b>; rel="next"`},
		"two rel parameters":           {`<https://h/a>; rel="prev"; rel="next"`},
		"next with an anchor":          {`<https://h/a>; rel="next"; anchor="#x"`},
		"next with an empty target":    {`<>; rel="next"`},
	} {
		got, err := nextLinkTarget(values)
		if !errors.Is(err, ErrInvalidListing) {
			t.Errorf("%s: nextLinkTarget(%q) = %q, %v; want ErrInvalidListing, never a silent end of the list", name, values, got, err)
		}
	}
}

func TestFilesFollowsALinkWithAQuotedCommaBeforeRel(t *testing.T) {
	// Page 1 holds a, page 2 holds b. The Link to page 2 has a title with a
	// comma in it, before rel: a parser that stops at the comma loses b and
	// returns a successful, shorter list.
	srv := newTreeServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("cursor") == "" {
			w.Header().Set("Link", `<?cursor=2&recursive=true>; title="page 2, continued"; rel="next"`)
			_, _ = w.Write([]byte(listOf(lfsJSON("a.safetensors"))))
			return
		}
		_, _ = w.Write([]byte(listOf(lfsJSON("b.safetensors"))))
	})
	entries, err := srv.client(t, Config{}).Files(context.Background(), testRepo, testRev)
	if err != nil {
		t.Fatalf("Files: %v", err)
	}
	if got, want := paths(entries), []string{"a.safetensors", "b.safetensors"}; !slices.Equal(got, want) {
		t.Errorf("files = %q, want %q (page 2 lost?)", got, want)
	}
	if srv.requests() != 2 {
		t.Errorf("%d requests, want 2", srv.requests())
	}
}

func TestFilesRefusesAMalformedLinkHeaderInsteadOfEndingTheList(t *testing.T) {
	srv := newTreeServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Link", `<?cursor=2&recursive=true>; title="never closed; rel="next"`)
		_, _ = w.Write([]byte(listOf(lfsJSON("a.safetensors"))))
	})
	entries, err := srv.client(t, Config{}).Files(context.Background(), testRepo, testRev)
	if !errors.Is(err, ErrInvalidListing) || entries != nil {
		t.Errorf("Files = %v, %v; want ErrInvalidListing and no list", entries, err)
	}
}
