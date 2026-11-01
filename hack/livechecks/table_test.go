// Package livechecks tests hack/live-checks.sh (issue #163): that its one table
// names every package that holds a live test, and how the script plans, gates,
// runs and cleans up. It has no code of its own; CI's "go" job runs it with
// the rest of the module. It starts no live test and needs no credential.
package livechecks

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
)

// row is one line of the script's table of live tests.
type row struct{ name, kind, pkg, test, gates, inputs, status string }

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the test's directory")
		}
		dir = parent
	}
}

func scriptPath(t *testing.T) string { return filepath.Join(repoRoot(t), "hack", "live-checks.sh") }

// block returns the lines between the BEGIN and END markers, without comments and blank lines.
func block(t *testing.T, script, begin, end string) []string {
	t.Helper()
	var out []string
	in := false
	for _, l := range strings.Split(script, "\n") {
		switch {
		case l == "# BEGIN "+begin:
			in = true
		case l == "# END "+end:
			in = false
		case in && l != "" && !strings.HasPrefix(l, "#"):
			out = append(out, l)
		}
	}
	if len(out) == 0 {
		t.Fatalf("the script has no %s block", begin)
	}
	return out
}

func readRows(t *testing.T) []row {
	t.Helper()
	b, err := os.ReadFile(scriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	var rows []row
	for _, l := range block(t, string(b), "LIVE TESTS", "LIVE TESTS") {
		f := strings.Split(l, "|")
		if len(f) != 7 {
			t.Fatalf("table row %q has %d fields, want 7", l, len(f))
		}
		rows = append(rows, row{f[0], f[1], f[2], f[3], f[4], f[5], f[6]})
	}
	return rows
}

func readShapeNames(t *testing.T) []string {
	t.Helper()
	b, err := os.ReadFile(scriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, l := range block(t, string(b), "CLUSTER SHAPES", "CLUSTER SHAPES") {
		f := strings.Split(l, "|")
		if len(f) != 13 {
			t.Fatalf("shape row %q has %d fields, want 13", l, len(f))
		}
		names = append(names, f[0])
	}
	return names
}

// A live test announces itself in at least one of three ways, none of which
// is read from the table: its Skip message says it needs a real system and the
// file reads an environment variable; its file name has a "live" word; or it
// mentions a gate variable by name (the gates the table lists).
var (
	liveWord = regexp.MustCompile(`(?i)\b(real|live)\b`)
	liveFile = regexp.MustCompile(`(^|_)live(_|\.)`)
	skipDirs = map[string]bool{".git": true, "spikes": true, "third_party": true, "vendor": true, "testdata": true}
)

// findLive returns the package directories of fsys that hold a live test, each with why.
func findLive(fsys fs.FS, gates map[string]bool) (map[string][]string, error) {
	found := map[string][]string{}
	err := fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if skipDirs[d.Name()] || p == "hack/livechecks" {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, "_test.go") {
			return nil
		}
		src, err := fs.ReadFile(fsys, p)
		if err != nil {
			return err
		}
		f, err := parser.ParseFile(token.NewFileSet(), p, src, 0)
		if err != nil {
			return err
		}
		var reads, skips, gated bool
		ast.Inspect(f, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.CallExpr:
				sel, ok := x.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == "os" && (sel.Sel.Name == "Getenv" || sel.Sel.Name == "LookupEnv") {
					reads = true
				}
				if sel.Sel.Name == "Skip" || sel.Sel.Name == "Skipf" {
					ast.Inspect(x, func(m ast.Node) bool {
						if lit, ok := m.(*ast.BasicLit); ok && lit.Kind == token.STRING {
							if s, err := strconv.Unquote(lit.Value); err == nil && liveWord.MatchString(s) {
								skips = true
							}
						}
						return true
					})
				}
			case *ast.BasicLit:
				if x.Kind == token.STRING {
					if s, err := strconv.Unquote(x.Value); err == nil && gates[s] {
						gated = true
					}
				}
			}
			return true
		})
		dir := path.Dir(p)
		switch {
		case skips && reads:
			found[dir] = append(found[dir], p+": a Skip that says it needs a real system, and an environment read")
		case gated:
			found[dir] = append(found[dir], p+": names a gate variable of the table")
		case liveFile.MatchString(path.Base(p)):
			found[dir] = append(found[dir], p+": a live file name")
		}
		return nil
	})
	return found, err
}

func gateSet(rows []row) map[string]bool {
	g := map[string]bool{}
	for _, r := range rows {
		for _, v := range strings.Split(r.gates, ",") {
			if v != "-" && v != "" {
				g[v] = true
			}
		}
	}
	return g
}

// unlisted returns the live-test packages that no ready row names.
func unlisted(found map[string][]string, rows []row) []string {
	listed := map[string]bool{}
	for _, r := range rows {
		if r.status == "ready" {
			listed[r.pkg] = true
		}
	}
	var out []string
	for dir := range found {
		if !listed[dir] {
			out = append(out, dir)
		}
	}
	sort.Strings(out)
	return out
}

func TestEveryLiveTestPackageIsInTheTable(t *testing.T) {
	rows := readRows(t)
	found, err := findLive(os.DirFS(repoRoot(t)), gateSet(rows))
	if err != nil {
		t.Fatal(err)
	}
	var dirs []string
	for d := range found {
		dirs = append(dirs, d)
	}
	sort.Strings(dirs)
	t.Logf("packages that hold a live test: %s", strings.Join(dirs, ", "))
	if missing := unlisted(found, rows); len(missing) > 0 {
		for _, m := range missing {
			t.Errorf("package %s holds a live test (%s) that hack/live-checks.sh's table does not name; add a row", m, strings.Join(found[m], "; "))
		}
	}
}

// The scanner must see the live tests that issue #163 names, or a scanner that
// found nothing would pass the check above. This is the floor from the issue's
// text, not the mechanism: the mechanism is findLive.
func TestScannerSeesTheLiveTestsTheIssueNames(t *testing.T) {
	found, err := findLive(os.DirFS(repoRoot(t)), gateSet(readRows(t)))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"internal/front", "internal/providerctl/conformance", "cmd/hb-agent"} {
		if _, ok := found[want]; !ok {
			t.Errorf("the scanner did not find the live test in %s", want)
		}
	}
}

// Positive control: a tree with a new live test that the table does not name
// must be reported, one that reads a variable for another reason must not.
func TestScannerFlagsAnUnlistedLivePackage(t *testing.T) {
	src := func(skip, extra string) *fstest.MapFile {
		return &fstest.MapFile{Data: []byte("package p\nimport (\"os\"; \"testing\")\nfunc TestX(t *testing.T) {\n" +
			"\tif os.Getenv(\"SOME_GATE\") == \"\" {\n\t\tt.Skip(" + strconv.Quote(skip) + ")\n\t}\n}\n" + extra)}
	}
	fsys := fstest.MapFS{
		"internal/newlive/x_test.go":      src("needs a real cluster", ""),
		"internal/quiet/x_test.go":        src("root reads every file", ""),
		"internal/gated/x_test.go":        {Data: []byte("package p\nconst g = \"HPCBURST_CONFORMANCE\"\n")},
		"internal/named/foo_live_test.go": {Data: []byte("package p\n")},
		"internal/alive/x_test.go":        src("an alive and kicking cache", ""), // "alive" is not the word "live"
		"spikes/s1/x_test.go":             src("needs a real cluster", ""),
	}
	rows := []row{{"a", "local", "internal/other", "^TestA$", "HPCBURST_CONFORMANCE", "-", "ready"}}
	found, err := findLive(fsys, gateSet(rows))
	if err != nil {
		t.Fatal(err)
	}
	got := unlisted(found, rows)
	want := []string{"internal/gated", "internal/named", "internal/newlive"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("unlisted live packages %v, want %v", got, want)
	}
	rows = append(rows, row{"b", "local", "internal/newlive", "^TestX$", "-", "-", "ready"})
	if got := unlisted(found, rows); len(got) != 2 {
		t.Fatalf("after listing internal/newlive, unlisted = %v, want the other two", got)
	}
}

// Mutation: on the real tree, removing any ready row must make the check fail
// for its package. A check that passed with the row gone would be vacuous.
func TestDroppingARowFailsTheCheck(t *testing.T) {
	rows := readRows(t)
	found, err := findLive(os.DirFS(repoRoot(t)), gateSet(rows))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if r.status != "ready" {
			continue
		}
		var rest []row
		for _, o := range rows {
			if o.pkg != r.pkg {
				rest = append(rest, o)
			}
		}
		if got := unlisted(found, rest); len(got) != 1 || got[0] != r.pkg {
			t.Errorf("without the rows of %s, unlisted = %v, want exactly that package", r.pkg, got)
		}
	}
}

func TestTableRowsAreRealAndWellFormed(t *testing.T) {
	root := repoRoot(t)
	rows := readRows(t)
	shapes := map[string]bool{}
	for _, n := range readShapeNames(t) {
		shapes[n] = true
	}
	seen := map[string]bool{}
	for _, r := range rows {
		if seen[r.name] {
			t.Errorf("row name %s appears twice", r.name)
		}
		seen[r.name] = true
		if r.kind != "local" && r.kind != "cluster" {
			t.Errorf("%s: kind %q is neither local nor cluster", r.name, r.kind)
		}
		switch r.status {
		case "pending":
			if r.pkg != "-" {
				t.Errorf("%s: a pending row names no package yet", r.name)
			}
			continue
		case "ready":
		default:
			t.Errorf("%s: status %q is neither ready nor pending", r.name, r.status)
			continue
		}
		if (r.kind == "cluster") != shapes[r.name] {
			t.Errorf("%s: a cluster row needs exactly one shape row, and a local row none", r.name)
		}
		var tests strings.Builder
		entries, err := os.ReadDir(filepath.Join(root, r.pkg))
		if err != nil {
			t.Errorf("%s: package %s: %v", r.name, r.pkg, err)
			continue
		}
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), "_test.go") {
				b, _ := os.ReadFile(filepath.Join(root, r.pkg, e.Name()))
				tests.Write(b)
			}
		}
		fn := strings.TrimSuffix(strings.TrimPrefix(r.test, "^"), "$")
		if !regexp.MustCompile(`(?m)^func ` + regexp.QuoteMeta(fn) + `\(`).MatchString(tests.String()) {
			t.Errorf("%s: package %s has no test function %s", r.name, r.pkg, fn)
		}
		for _, g := range strings.Split(r.gates, ",") {
			if !strings.Contains(tests.String(), `"`+g+`"`) {
				t.Errorf("%s: package %s never mentions its gate %s as a string", r.name, r.pkg, g)
			}
		}
		if r.inputs != "-" {
			for _, in := range strings.Split(r.inputs, ",") {
				if _, err := os.Stat(filepath.Join(root, in)); err != nil {
					t.Errorf("%s: input %s does not exist", r.name, in)
				}
			}
		}
	}
	for n := range shapes {
		if !seen[n] {
			t.Errorf("shape row %s has no table row", n)
		}
	}
}
