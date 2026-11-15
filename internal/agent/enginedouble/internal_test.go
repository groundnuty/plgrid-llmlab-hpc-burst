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
// tests may import it, with one exception: the bulk row's test shim, the main
// package internal/agent/enginedouble/shim, which is the double's program on a
// node (issue #521, option (A)). Both are read from the sources, so neither
// needs the go tool or the network. That no shipped binary reaches the double
// or the shim is the shim's own test (its closure guard).
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
	scanned, shimFiles, bad := scanImporters(t, root, self)
	for _, b := range bad {
		t.Error(b)
	}
	if shimFiles == 0 {
		t.Error("found no source of the shim, internal/agent/enginedouble/shim: the allowance would name nothing")
	}
	if scanned < 20 {
		t.Errorf("scanned %d non-test sources of the module, expected many more", scanned)
	}
}

// scanImporters reads every non-test source under root, but the double's own, and returns how many it read, how many are the shim's, and what is
// wrong: a source that imports the double and is not the shim's main, or a source of the shim's that is not package main.
func scanImporters(t *testing.T, root, self string) (scanned, shimFiles int, bad []string) {
	t.Helper()
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == ".git" || d.Name() == "node_modules") {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		dir, _ := filepath.Abs(filepath.Dir(p))
		if strings.HasSuffix(filepath.ToSlash(dir), "internal/agent/enginedouble") {
			return nil
		}
		scanned++
		allowed := strings.HasSuffix(filepath.ToSlash(dir), "internal/agent/enginedouble/shim")
		if allowed {
			shimFiles++
			if pkg := packageOf(t, p); pkg != "main" {
				bad = append(bad, p+" is package "+pkg+": the one importer allowed is the shim's main")
			}
		}
		for _, path := range importsOf(t, p) {
			if path == self && !allowed {
				bad = append(bad, p+" imports the engine double: only _test.go files and the bulk row's shim may")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return scanned, shimFiles, bad
}

// The scan itself, over a tree of its own: the shim's main may import the double, a test file may, and any other source may not, a second main
// included; a source of the shim's that is not package main is wrong too.
func TestTheImporterScanAllowsTheShimAlone(t *testing.T) {
	const self = "example.test/m/internal/agent/enginedouble"
	root := t.TempDir()
	write := func(rel, body string) {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	imp := "import _ \"" + self + "\"\n"
	write("internal/agent/enginedouble/double.go", "package enginedouble\n")
	write("internal/agent/enginedouble/shim/main.go", "package main\n\n"+imp)
	write("internal/x/x_test.go", "package x\n\n"+imp)
	write("internal/y/y.go", "package y\n")
	_, shimFiles, bad := scanImporters(t, root, self)
	if shimFiles != 1 || len(bad) != 0 {
		t.Fatalf("the allowed tree: %d shim files, %q; want 1 and nothing wrong", shimFiles, bad)
	}
	write("cmd/other/main.go", "package main\n\n"+imp)
	write("internal/agent/enginedouble/shim/lib.go", "package shimlib\n")
	_, _, bad = scanImporters(t, root, self)
	if len(bad) != 2 || !strings.Contains(strings.Join(bad, "\n"), "cmd/other/main.go imports the engine double") ||
		!strings.Contains(strings.Join(bad, "\n"), "is package shimlib") {
		t.Errorf("a second main importing the double and a shim source not main: %q; want both named", bad)
	}
}

// packageOf is a source file's package name.
func packageOf(t *testing.T, file string) string {
	t.Helper()
	src, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parser.ParseFile(token.NewFileSet(), file, src, parser.PackageClauseOnly)
	if err != nil {
		t.Fatal(err)
	}
	return parsed.Name.Name
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
