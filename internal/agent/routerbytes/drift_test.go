package routerbytes

// The copies in this directory are S13's files (README.md). Each Go file is a provenance block ending in
// the line "// End of provenance." and a blank line, then S13's file byte for byte, with S13's own import
// path mapped to this directory; the field list is S13's byte for byte. A change to either side fails.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	s13Dir         = "../../../spikes/s13"
	provenanceEnd  = "// End of provenance.\n\n"
	s13ImportPath  = "github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s13/"
	copyImportPath = "github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/routerbytes/"
)

var copied = []string{
	"cases/cases.go", "cases/cases_test.go",
	"rawjson/parse.go", "rawjson/compare.go", "rawjson/compare_test.go",
	"recorder/recorder.go", "recorder/recorder_test.go",
	"fields/vllm-v0.32.0.tsv",
}

// drift says how the copy cp of S13's file f, whose bytes are orig, departs from it; "" when it doesn't.
func drift(f string, cp, orig []byte) string {
	got, want := string(cp), string(orig)
	if strings.HasSuffix(f, ".go") {
		i := strings.Index(got, provenanceEnd)
		if i < 0 || !strings.HasPrefix(got, "// Copied from spikes/s13/"+f+" ") {
			return "no provenance block naming spikes/s13/" + f + " and ending in the end line"
		}
		got = got[i+len(provenanceEnd):]
		want = strings.ReplaceAll(want, s13ImportPath, copyImportPath)
	}
	if got != want {
		return "differs from spikes/s13/" + f + " beyond the provenance block and the import path"
	}
	return ""
}

func read(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestTheCopiesAreS13s(t *testing.T) {
	for _, f := range copied {
		if d := drift(f, read(t, f), read(t, filepath.Join(s13Dir, f))); d != "" {
			t.Errorf("%s: %s", f, d)
		}
	}
	// Every Go file of S13's three packages is copied, so none is left behind.
	for _, dir := range []string{"cases", "rawjson", "recorder"} {
		names, err := filepath.Glob(filepath.Join(s13Dir, dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range names {
			rel := dir + "/" + filepath.Base(n)
			found := false
			for _, f := range copied {
				found = found || f == rel
			}
			if !found {
				t.Errorf("S13's %s is not copied", rel)
			}
		}
	}
}

// The control: the check sees a one-byte change in a copy's body, a missing provenance block, and a
// one-byte change in the field list.
func TestTheDriftCheckSeesAChange(t *testing.T) {
	const f = "rawjson/parse.go"
	cp, orig := read(t, f), read(t, filepath.Join(s13Dir, f))
	if d := drift(f, cp, orig); d != "" {
		t.Fatalf("the unchanged copy: %s; the control needs a matching pair to start from", d)
	}
	if drift(f, []byte(strings.Replace(string(cp), "package rawjson", "package rawjsom", 1)), orig) == "" {
		t.Error("a one-byte change below the provenance block passed")
	}
	if drift(f, []byte(strings.Replace(string(cp), provenanceEnd, "", 1)), orig) == "" {
		t.Error("a copy without its provenance end line passed")
	}
	const tsv = "fields/vllm-v0.32.0.tsv"
	ft, ot := read(t, tsv), read(t, filepath.Join(s13Dir, tsv))
	if drift(tsv, append(append([]byte(nil), ft...), '\n'), ot) == "" {
		t.Error("a field list with one more byte passed")
	}
}
