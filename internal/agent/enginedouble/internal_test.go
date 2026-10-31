package enginedouble

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestVocabularyIsFixedAndDistinct(t *testing.T) {
	if len(vocab) < 200 {
		t.Fatalf("%d words, want at least 200", len(vocab))
	}
	words, ids := map[string]bool{}, map[int32]string{}
	for _, w := range vocab {
		if strings.ContainsAny(w, " \t\n") || w == "" {
			t.Errorf("%q is not a single word", w)
		}
		if words[w] {
			t.Errorf("word %q twice", w)
		}
		words[w] = true
		id := refID(w) // hash/fnv directly
		if other, dup := ids[id]; dup {
			t.Errorf("%q and %q share token id %d", w, other, id)
		}
		ids[id] = w
		if id < 1 || id > 100000 {
			t.Errorf("%q has id %d, outside 1..100000", w, id)
		}
	}
	if vocab[0] != "bab" || vocab[1] != "bad" {
		t.Errorf("the vocabulary starts %v: it is meant to be fixed", vocab[:2])
	}
}

func TestRewriteArgOnPathBoundaries(t *testing.T) {
	binds := []bind{{src: "/h/m", dst: "/hpcburst/model"}, {src: "/h/r", dst: "/hpcburst/run"}, {src: "/h/deep", dst: "/hpcburst/run/deep"}}
	for in, want := range map[string]string{
		"/hpcburst/model":                     "/h/m",
		"/hpcburst/model/sub/f":               "/h/m/sub/f",
		"/hpcburst/modelx":                    "/hpcburst/modelx",
		"/hpcburst":                           "/hpcburst",
		"--uds=/hpcburst/run/worker-0.sock":   "--uds=/h/r/worker-0.sock",
		"--chat-template=/hpcburst/run/c.j2":  "--chat-template=/h/r/c.j2",
		"--served-model-name=/hpcburst/model": "--served-model-name=/h/m",
		"--other=/elsewhere/x":                "--other=/elsewhere/x",
		"/hpcburst/run/deep/x":                "/h/deep/x", // the longest destination wins
		"serve":                               "serve",
		"x=/hpcburst/model":                   "x=/hpcburst/model", // only --flag=value is rewritten
	} {
		if got := rewriteArg(in, binds); got != want {
			t.Errorf("rewriteArg(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestChildEnv(t *testing.T) {
	own := []string{"A=1", "APPTAINERENV_B=2", "SINGULARITYENV_B=old", "SINGULARITYENV_C=3", "APPTAINERENV_A=over"}
	clean, hidden := childEnv(own, true)
	if got, want := strings.Join(clean, " "), "A=over B=2 C=3"; got != want {
		t.Errorf("clean env = %q, want %q", got, want)
	}
	if !hidden["A"] || !hidden["B"] || !hidden["C"] {
		t.Errorf("hidden = %v, want the three prefixed names", hidden)
	}
	inherit, _ := childEnv(own, false)
	if got, want := strings.Join(inherit, " "), "A=over B=2 C=3"; got != want { // A is overridden, the prefixed ones are gone
		t.Errorf("inherited env = %q, want %q", got, want)
	}
	plain, _ := childEnv([]string{"P=1", "APPTAINERENV_Q=2"}, false)
	if got, want := strings.Join(plain, " "), "P=1 Q=2"; got != want {
		t.Errorf("env = %q, want %q", got, want)
	}
}

// The package is test support: its source may import only the standard
// library and package agentapi, and no source of the module except its own
// tests may import it. Both are read from the sources, so neither needs the
// go tool or the network.
func TestPackageImportsAndImporters(t *testing.T) {
	const agentapi = "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	const self = "github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	files, err := filepath.Glob("*.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("no source files found: %v", err)
	}
	checked := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		checked++
		for _, path := range importsOf(t, f) {
			first, _, _ := strings.Cut(path, "/")
			if path != agentapi && strings.Contains(first, ".") {
				t.Errorf("%s imports %s: only the standard library and %s are allowed", f, path, agentapi)
			}
		}
	}
	if checked < 10 {
		t.Errorf("checked %d source files, expected the package's whole source", checked)
	}

	root := "../../.."
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("the module root is not at %s: %v", root, err)
	}
	scanned := 0
	err = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == ".git" || d.Name() == "node_modules") {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		if dir, _ := filepath.Abs(filepath.Dir(p)); strings.HasSuffix(filepath.ToSlash(dir), "internal/agent/enginedouble") {
			return nil
		}
		scanned++
		for _, path := range importsOf(t, p) {
			if path == self {
				t.Errorf("%s imports the engine double: only _test.go files may", p)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if scanned < 20 {
		t.Errorf("scanned %d non-test sources of the module, expected many more", scanned)
	}
}

func importsOf(t *testing.T, file string) []string {
	t.Helper()
	src, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parser.ParseFile(token.NewFileSet(), file, src, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("%s: %v", file, err)
	}
	var out []string
	for _, imp := range parsed.Imports {
		p, _ := strconv.Unquote(imp.Path.Value)
		out = append(out, p)
	}
	return out
}
