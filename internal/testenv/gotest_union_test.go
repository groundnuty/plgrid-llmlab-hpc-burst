package testenv

// hack/go-test.sh splits the root module's tests between the packages' run, which leaves the groups'
// tests out by -skip, and the groups, each run by -run in its package (issues #282 and #295). A name in
// two groups runs twice; a name a group lists for the wrong package is skipped everywhere and run
// nowhere; a -skip or -run wider than its names drops or doubles a test; and a group with no step in CI's
// go job runs only in the crd job. So these tests read every test function of the root module from its
// source, take each part's -skip or -run and package from what the script really passes to go (through
// the double of gotest_script_test.go), and count, for each test, the parts that run it: each must be
// exactly one. The module's tests are found here with go/build and go/parser, independently of the
// script, so the count is not the script checking itself.

import (
	"errors"
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	"sigs.k8s.io/yaml"
)

// part is one go test run of the script: the packages' run (pkg "./...", with skip) or a group's (one
// package, with run).
type part struct {
	name, pkg string
	run, skip *regexp.Regexp
}

// runs reports whether the part runs the top-level test name of package pkg. go test matches a
// pattern with no slash against a top-level test's name alone (testing's matcher), and the script's
// patterns have none (partFromCall refuses one).
func (p part) runs(pkg, name string) bool {
	if p.pkg != "./..." && p.pkg != pkg {
		return false
	}
	if p.run != nil && !p.run.MatchString(name) {
		return false
	}
	return p.skip == nil || !p.skip.MatchString(name)
}

// unionProblems returns one line for each test that no part runs, or that more than one part runs.
func unionProblems(parts []part, tests map[string][]string) []string {
	var problems []string
	for _, pkg := range sortedKeys(tests) {
		for _, name := range tests[pkg] {
			var by []string
			for _, p := range parts {
				if p.runs(pkg, name) {
					by = append(by, p.name)
				}
			}
			switch {
			case len(by) == 0:
				problems = append(problems, fmt.Sprintf("%s %s runs in no part: it is dropped", pkg, name))
			case len(by) > 1:
				problems = append(problems, fmt.Sprintf("%s %s runs in %d parts (%s): it runs twice", pkg, name, len(by), strings.Join(by, ", ")))
			}
		}
	}
	return problems
}

func sortedKeys(m map[string][]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// moduleTests returns the top-level test functions of every package of the module at root, by package
// ("./internal/lifecycle"), as go test would build them on this machine: the test and external test
// files that build.Default's constraints keep. It walks what ./... walks: no directory whose name starts
// with . or _, no testdata, and no nested module (spikes/ and third_party/forked/ have their own).
func moduleTests(root string) (map[string][]string, error) {
	tests := map[string][]string{}
	err := filepath.WalkDir(root, func(path string, e fs.DirEntry, err error) error {
		if err != nil || !e.IsDir() {
			return err
		}
		if path != root {
			if n := e.Name(); strings.HasPrefix(n, ".") || strings.HasPrefix(n, "_") || n == "testdata" {
				return fs.SkipDir
			}
			if _, err := os.Stat(filepath.Join(path, "go.mod")); err == nil {
				return fs.SkipDir
			}
		}
		bp, err := build.Default.ImportDir(path, 0)
		var noGo *build.NoGoError
		if errors.As(err, &noGo) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read the package in %s: %w", path, err)
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		pkg := "./" + filepath.ToSlash(rel)
		if rel == "." {
			pkg = "."
		}
		for _, f := range append(append([]string{}, bp.TestGoFiles...), bp.XTestGoFiles...) {
			names, err := testFuncs(filepath.Join(path, f))
			if err != nil {
				return err
			}
			tests[pkg] = append(tests[pkg], names...)
		}
		return nil
	})
	return tests, err
}

// testFuncs returns the file's top-level test functions, by cmd/go's rule (load/test.go): a function with
// no receiver named Test, or Test then a rune that is not lower case, taking one *testing.T. TestMain
// taking a *testing.M is the package's main, not a test.
func testFuncs(path string) ([]string, error) {
	f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || !isTestName(fn.Name.Name) || !takesOnePointerTo(fn, "T") {
			continue
		}
		names = append(names, fn.Name.Name)
	}
	return names, nil
}

func isTestName(name string) bool {
	if !strings.HasPrefix(name, "Test") {
		return false
	}
	if len(name) == len("Test") {
		return true
	}
	r, _ := utf8.DecodeRuneInString(name[len("Test"):])
	return !unicode.IsLower(r)
}

func takesOnePointerTo(fn *ast.FuncDecl, typ string) bool {
	ps := fn.Type.Params.List
	if len(ps) != 1 || len(ps[0].Names) > 1 || (fn.Type.Results != nil && len(fn.Type.Results.List) > 0) {
		return false
	}
	star, ok := ps[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	switch x := star.X.(type) {
	case *ast.Ident:
		return x.Name == typ
	case *ast.SelectorExpr:
		return x.Sel.Name == typ
	}
	return false
}

// groupRow is a row of the script's groups table, "name|package|tests|timeout".
var groupRow = regexp.MustCompile(`(?m)^\s*"([a-z0-9-]+)\|(\./[^|"]*)\|[^|"]*\|[0-9]+s"\s*$`)

// scriptGroupNames returns the names of the groups in the script's table, in order.
func scriptGroupNames(t *testing.T, root string) []string {
	t.Helper()
	src, err := os.ReadFile(filepath.Join(root, "hack", "go-test.sh"))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, m := range groupRow.FindAllStringSubmatch(string(src), -1) {
		names = append(names, m[1])
	}
	return names
}

// scriptParts runs the script in root with the double, once for the packages and once for each group in
// its table, and returns the parts from the calls it made. "own" with no group must make one call per
// group read from the table, so that a row this reading missed is not missed by the count.
func scriptParts(t *testing.T, root string) []part {
	t.Helper()
	names := scriptGroupNames(t, root)
	if len(names) == 0 {
		t.Fatal("found no group in hack/go-test.sh's table")
	}
	if _, _, calls := runGoTestScript(t, root, allPassed(), "own"); len(calls) != len(names) {
		t.Fatalf("own ran %d groups, but the table reads as %d (%q): a row is not in the form this test reads", len(calls), len(names), names)
	}
	_, _, calls := runGoTestScript(t, root, allPassed(), "packages")
	if len(calls) != 1 {
		t.Fatalf("packages: go called %q, want once", calls)
	}
	parts := []part{partFromCall(t, "packages", calls[0])}
	for _, n := range names {
		_, _, calls := runGoTestScript(t, root, allPassed(), "own", n)
		if len(calls) != 1 {
			t.Fatalf("own %s: go called %q, want once", n, calls)
		}
		parts = append(parts, partFromCall(t, n, calls[0]))
	}
	return parts
}

// partFromCall reads a recorded go call, "test ... -skip RE ./..." or "test ... -run RE ./pkg".
func partFromCall(t *testing.T, name, call string) part {
	t.Helper()
	f := strings.Fields(call)
	if len(f) < 3 || f[0] != "test" {
		t.Fatalf("%s: go was called as %q, not go test", name, call)
	}
	p := part{name: name, pkg: f[len(f)-1]}
	for i := 0; i+1 < len(f)-1; i++ {
		var re **regexp.Regexp
		switch f[i] {
		case "-run":
			re = &p.run
		case "-skip":
			re = &p.skip
		default:
			continue
		}
		if strings.Contains(f[i+1], "/") {
			t.Fatalf("%s: the pattern %q has a slash, which matches subtests and is not counted here", name, f[i+1])
		}
		r, err := regexp.Compile(f[i+1])
		if err != nil {
			t.Fatalf("%s: %s %q: %v", name, f[i], f[i+1], err)
		}
		*re = r
	}
	if (p.pkg == "./...") != (p.skip != nil) || (p.pkg == "./...") == (p.run != nil) {
		t.Fatalf("%s: go was called as %q: want either -skip over ./... or -run in one package", name, call)
	}
	return p
}

// Every test function of the root module runs in exactly one part of the script, and CI's go job runs
// every part, as the crd job does through hack/check.sh's "all".
func TestGoTestScriptRunsEveryTestExactlyOnce(t *testing.T) {
	root := realRoot(t)
	tests, err := moduleTests(root)
	if err != nil {
		t.Fatal(err)
	}
	parts := scriptParts(t, root)
	for _, p := range unionProblems(parts, tests) {
		t.Error(p)
	}
	// A group that runs none of its package's tests would pass the count above with nothing to show.
	for _, p := range parts[1:] {
		n := 0
		for _, name := range tests[p.pkg] {
			if p.runs(p.pkg, name) {
				n++
			}
		}
		if n == 0 {
			t.Errorf("group %s runs no test of %s", p.name, p.pkg)
		}
	}
	// The count is only as good as the reading of the module: the packages the groups name must be
	// found, with tests.
	for _, pkg := range []string{"./internal/lifecycle", "./cmd/hpcburst-controller", "./hack/livechecks", "./internal/testenv"} {
		if len(tests[pkg]) == 0 {
			t.Errorf("found no test in %s", pkg)
		}
	}
	var groups []string
	for _, p := range parts[1:] {
		groups = append(groups, p.name)
	}
	for _, p := range ciProblems(goJobRuns(t, root), groups) {
		t.Error(p)
	}
}

// ciProblems returns one line for each part that CI's go job does not run in exactly one step, and for a
// crd job that does not run hack/check.sh, whose "all" runs every part (TestGoTestScriptIsWhatCIAndCheckRun
// checks that it does).
func ciProblems(runs map[string][]string, groups []string) []string {
	var problems []string
	want := []string{"bash hack/go-test.sh packages"}
	for _, g := range groups {
		want = append(want, "bash hack/go-test.sh own "+g)
	}
	// "own" alone or "all" in the go job would run the groups a second time.
	for _, extra := range []string{"bash hack/go-test.sh own", "bash hack/go-test.sh all"} {
		for _, r := range runs["go"] {
			for _, line := range strings.Split(r, "\n") {
				if strings.TrimSpace(line) == extra {
					problems = append(problems, fmt.Sprintf("CI's go job runs %q, which runs the groups again", extra))
				}
			}
		}
	}
	for _, w := range want {
		if n := countRuns(runs["go"], w); n != 1 {
			problems = append(problems, fmt.Sprintf("CI's go job runs %q in %d steps, want 1", w, n))
		}
	}
	if n := countRuns(runs["crd"], "bash hack/check.sh"); n != 1 {
		problems = append(problems, fmt.Sprintf("CI's crd job runs hack/check.sh in %d steps, want 1 (it runs every part through all)", n))
	}
	return problems
}

// goJobRuns returns each job's steps' run scripts, by job id, from ci.yml read as YAML.
func goJobRuns(t *testing.T, root string) map[string][]string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Jobs map[string]struct {
			Steps []struct {
				Run string `json:"run"`
			} `json:"steps"`
		} `json:"jobs"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("ci.yml: %v", err)
	}
	runs := map[string][]string{}
	for id, j := range doc.Jobs {
		for _, s := range j.Steps {
			runs[id] = append(runs[id], s.Run)
		}
	}
	return runs
}

// countRuns counts the steps whose run script has a line that is exactly cmd, or that runs cmd and
// pipes or redirects it (bash hack/go-test.sh packages 2>&1 | tee ...).
func countRuns(runs []string, cmd string) int {
	n := 0
	for _, r := range runs {
		for _, line := range strings.Split(r, "\n") {
			line = strings.TrimSpace(line)
			if line == cmd || strings.HasPrefix(line, cmd+" ") {
				n++
				break
			}
		}
	}
	return n
}
