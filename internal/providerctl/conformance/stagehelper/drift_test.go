package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"sort"
	"strings"
	"testing"
)

// The helper copies what cmd/hb-agent does to stage: its Stager's
// configuration, two small functions and two calls. cmd/hb-agent is package
// main, which cannot be imported, so the copies are checked here against the
// agent's source, parsed with go/parser: the agent gains a field or changes a
// function, and these tests fail until the helper follows.

// sourceDir is this package's directory: the directory of this file as the
// compiler recorded it, else the working directory, which `go test` sets to the
// package's. A test binary built with -trimpath records no usable path (the
// live checks build the agent's test binary with it), and one run from another
// directory has no usable working directory, so each covers the other.
func sourceDir(t *testing.T) string {
	t.Helper()
	var candidates []string
	if _, file, _, ok := runtime.Caller(0); ok {
		candidates = append(candidates, filepath.Dir(file))
	}
	candidates = append(candidates, ".")
	for _, d := range candidates {
		if _, err := os.Stat(filepath.Join(d, "stage.go")); err == nil {
			return d
		}
	}
	t.Fatalf("this package's sources are in none of %q: run the test from its directory", candidates)
	return ""
}

func parseFile(t *testing.T, path string, src any) (*token.FileSet, *ast.File) {
	t.Helper()
	fset := token.NewFileSet()
	// Without comments, so a function's text is its code alone.
	f, err := parser.ParseFile(fset, path, src, 0)
	if err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return fset, f
}

func text(fset *token.FileSet, n ast.Node) string {
	var b bytes.Buffer
	if err := printer.Fprint(&b, fset, n); err != nil {
		panic(err)
	}
	return b.String()
}

func funcDecl(f *ast.File, name string) *ast.FuncDecl {
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Recv == nil && fd.Name.Name == name {
			return fd
		}
	}
	return nil
}

// stageConfigFields is the field names and expressions of the stage.Config
// literal inside the function fn.
func stageConfigFields(t *testing.T, fset *token.FileSet, f *ast.File, fn string) map[string]string {
	t.Helper()
	fd := funcDecl(f, fn)
	if fd == nil {
		t.Fatalf("no function %s", fn)
	}
	out := map[string]string{}
	ast.Inspect(fd, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok || text(fset, lit.Type) != "stage.Config" {
			return true
		}
		for _, e := range lit.Elts {
			kv, ok := e.(*ast.KeyValueExpr)
			if !ok {
				t.Fatalf("%s: a positional field in %s", fn, text(fset, lit))
			}
			out[text(fset, kv.Key)] = text(fset, kv.Value)
		}
		return false
	})
	return out
}

// callsTo is the text of every call in fn whose function is named callee.
func callsTo(t *testing.T, fset *token.FileSet, f *ast.File, fn, callee string) []string {
	t.Helper()
	fd := funcDecl(f, fn)
	if fd == nil {
		t.Fatalf("no function %s", fn)
	}
	var out []string
	ast.Inspect(fd, func(n ast.Node) bool {
		if c, ok := n.(*ast.CallExpr); ok && text(fset, c.Fun) == callee {
			out = append(out, text(fset, c))
		}
		return true
	})
	return out
}

// differences lists how two field maps differ.
func differences(agentFields, helperFields map[string]string) []string {
	var out []string
	for k, v := range agentFields {
		switch w, ok := helperFields[k]; {
		case !ok:
			out = append(out, fmt.Sprintf("the agent sets %s: %s, the helper does not", k, v))
		case w != v:
			out = append(out, fmt.Sprintf("%s: the agent has %s, the helper %s", k, v, w))
		}
	}
	for k, w := range helperFields {
		if _, ok := agentFields[k]; !ok {
			out = append(out, fmt.Sprintf("the helper sets %s: %s, the agent does not", k, w))
		}
	}
	sort.Strings(out)
	return out
}

func agentMain(t *testing.T) (*token.FileSet, *ast.File) {
	return parseFile(t, filepath.Join(sourceDir(t), "..", "..", "..", "..", "cmd", "hb-agent", "main.go"), nil)
}

func helperStage(t *testing.T) (*token.FileSet, *ast.File) {
	return parseFile(t, filepath.Join(sourceDir(t), "stage.go"), nil)
}

// The helper's Stager is configured with the agent's fields and the agent's
// expressions for them, so that what it stages is found by the agent.
func TestStagerConfigMirrorsTheAgents(t *testing.T) {
	af, amain := agentMain(t)
	hf, hstage := helperStage(t)
	want := stageConfigFields(t, af, amain, "run")
	got := stageConfigFields(t, hf, hstage, "stageAll")
	for _, k := range []string{"StagingDir", "Apptainer", "TmpDir", "HubEndpoint", "BaseEnv"} {
		if _, ok := want[k]; !ok {
			t.Fatalf("the agent's stage.Config has no %s: this test no longer reads the agent's literal (%v)", k, want)
		}
	}
	if d := differences(want, got); len(d) > 0 {
		t.Errorf("the helper's stage.Config has drifted from cmd/hb-agent's:\n%s", strings.Join(d, "\n"))
	}
}

// The helper finds Apptainer and makes its run directory as the agent does,
// and its two copied functions are the agent's.
func TestCopiedFunctionsMatchTheAgents(t *testing.T) {
	af, amain := agentMain(t)
	hf, hstage := helperStage(t)
	for _, name := range []string{"lookTool", "hubEndpoint"} {
		a, h := funcDecl(amain, name), funcDecl(hstage, name)
		if a == nil || h == nil {
			t.Fatalf("%s: in the agent %v, in the helper %v", name, a != nil, h != nil)
		}
		if at, ht := text(af, a), text(hf, h); at != ht {
			t.Errorf("%s has drifted.\nagent:\n%s\nhelper:\n%s", name, at, ht)
		}
	}
	for _, callee := range []string{"lookTool", "agent.MakeRunDir"} {
		want := callsTo(t, af, amain, "run", callee)
		got := callsTo(t, hf, hstage, "stageAll", callee)
		if len(want) == 0 {
			t.Fatalf("the agent's run makes no call to %s: this test no longer reads the agent", callee)
		}
		if !slices.Equal(want, got) {
			t.Errorf("calls to %s: the agent makes %q, the helper %q", callee, want, got)
		}
	}
}

// The comparison sees what it is for: on two sources it reports an added field,
// a missing one and a changed expression, and nothing on equal ones.
func TestComparisonSeesADrift(t *testing.T) {
	const base = `package main
func run() {
	_, _ = stage.New(stage.Config{StagingDir: job.Config.StagingDir, Apptainer: apptainer, TmpDir: runDir})
}`
	fields := func(src string) map[string]string {
		fset, f := parseFile(t, "snippet.go", src)
		return stageConfigFields(t, fset, f, "run")
	}
	b := fields(base)
	if len(b) != 3 {
		t.Fatalf("the snippet's fields: %v", b)
	}
	for name, c := range map[string]struct {
		src  string
		diff int
	}{
		"equal, spaced differently":   {"package main\nfunc run() { stage.New(stage.Config{\n StagingDir: job.Config.StagingDir,\n Apptainer:apptainer,\n TmpDir: runDir,\n}) }", 0},
		"a field the agent gains":     {strings.Replace(base, "TmpDir: runDir", "TmpDir: runDir, Readers: 8", 1), 1},
		"a field the helper lacks":    {strings.Replace(base, ", TmpDir: runDir", "", 1), 1},
		"an expression that changed":  {strings.Replace(base, "TmpDir: runDir", "TmpDir: os.TempDir()", 1), 1},
		"every field changed or lost": {"package main\nfunc run() { stage.New(stage.Config{Log: stderr}) }", 4},
	} {
		if got := differences(b, fields(c.src)); len(got) != c.diff {
			t.Errorf("%s: %d differences %q, want %d", name, len(got), got, c.diff)
		}
	}
}

// The build line in the package comment sets exactly the variables the helper
// reads, and each is a string variable with no initializer, which is all that
// -ldflags -X sets: a name that is misspelt there is left empty by the linker,
// and the helper then says "config".
func TestBuildLineSetsTheCompiledInVariables(t *testing.T) {
	dir := sourceDir(t)
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filepath.Join(dir, "main.go"), nil, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	var named []string
	for _, m := range regexp.MustCompile(`-X main\.([A-Za-z0-9_]+)=`).FindAllStringSubmatch(f.Doc.Text(), -1) {
		if !slices.Contains(named, m[1]) { // the comment names loadFormat twice
			named = append(named, m[1])
		}
	}
	var declared []string
	for _, d := range f.Decls {
		gd, ok := d.(*ast.GenDecl)
		if !ok || gd.Tok != token.VAR {
			continue
		}
		for _, s := range gd.Specs {
			vs := s.(*ast.ValueSpec)
			if len(vs.Values) != 0 {
				continue
			}
			if id, ok := vs.Type.(*ast.Ident); !ok || id.Name != "string" {
				continue
			}
			for _, n := range vs.Names {
				declared = append(declared, n.Name)
			}
		}
	}
	slices.Sort(named)
	slices.Sort(declared)
	want := []string{"checkpointRepo", "checkpointRev", "image", "imageDigest", "loadFormat"}
	if !slices.Equal(named, want) || !slices.Equal(declared, want) {
		t.Errorf("the build line names %v, the package declares uninitialized string variables %v, want %v", named, declared, want)
	}
	// compiled() hands every one of them on.
	in := compiled()
	if len(in.missing()) != len(want) {
		t.Errorf("with nothing compiled in, missing() names %v, want all of %v", in.missing(), want)
	}
}
