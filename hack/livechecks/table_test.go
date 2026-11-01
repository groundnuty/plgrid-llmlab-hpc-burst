// Package livechecks tests hack/live-checks.sh (issue #163): that its one table
// names every package that holds a live test and every test function that
// runs, and how the script plans, gates, runs and cleans up. It has no code of
// its own; CI's "go" job runs it with the rest of the module. It starts no live
// test and needs no credential.
package livechecks

import (
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"testing/fstest"
)

// row is one line of the script's table of live tests.
type row struct{ name, kind, pkg, test, gates, inputs, contexts, status string }

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
func block(t *testing.T, script, name string) []string {
	t.Helper()
	var out []string
	in := false
	for _, l := range strings.Split(script, "\n") {
		switch {
		case l == "# BEGIN "+name:
			in = true
		case l == "# END "+name:
			in = false
		case in && l != "" && !strings.HasPrefix(l, "#"):
			out = append(out, l)
		}
	}
	if len(out) == 0 {
		t.Fatalf("the script has no %s block", name)
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
	for _, l := range block(t, string(b), "LIVE TESTS") {
		f := strings.Split(l, "|")
		if len(f) != 8 {
			t.Fatalf("table row %q has %d fields, want 8", l, len(f))
		}
		rows = append(rows, row{f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7]})
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
	for _, l := range block(t, string(b), "CLUSTER SHAPES") {
		f := strings.Split(l, "|")
		if len(f) != 13 {
			t.Fatalf("shape row %q has %d fields, want 13", l, len(f))
		}
		names = append(names, f[0])
	}
	return names
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

// overlay is a file system with some files replaced, for the mutations of the real tree.
type overlay struct {
	base fs.FS
	over map[string][]byte
}

func (o overlay) Open(name string) (fs.File, error) {
	if b, ok := o.over[name]; ok {
		m := fstest.MapFS{name: {Data: b}}
		return m.Open(name)
	}
	return o.base.Open(name)
}

func (o overlay) ReadFile(name string) ([]byte, error) {
	if b, ok := o.over[name]; ok {
		return b, nil
	}
	return fs.ReadFile(o.base, name)
}

func realTree(t *testing.T) fs.FS { t.Helper(); return os.DirFS(repoRoot(t)) }

func TestEveryLiveTestIsRegistered(t *testing.T) {
	rows := readRows(t)
	problems, s, err := checkRegistry(realTree(t), rows)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("packages that hold a live test: %s", strings.Join(s.liveDirs(), ", "))
	for _, p := range problems {
		t.Error(p)
	}
}

// The scanner must see the live tests that issue #163 names, or a scanner that
// found nothing would pass the check above. This is the floor from the issue's
// text, not the mechanism: the mechanism is scan.
func TestScannerSeesTheLiveTestsTheIssueNames(t *testing.T) {
	_, s, err := checkRegistry(realTree(t), readRows(t))
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{}
	for _, d := range s.liveDirs() {
		have[d] = true
	}
	for _, want := range []string{"internal/front", "internal/providerctl/conformance", "cmd/hb-agent", "pkg/hubfiles"} {
		if !have[want] {
			t.Errorf("the scanner did not find the live test in %s", want)
		}
	}
}

// mapFile is a Go file of a fixture tree.
func mapFile(src string) *fstest.MapFile { return &fstest.MapFile{Data: []byte(src)} }

const liveTestSrc = `package p

import (
	"os"
	"testing"
)

//livecheck:%s
func %s(t *testing.T) {
	if os.Getenv("SOME_GATE") == "" {
		t.Skip("needs a real cluster")
	}
}
`

// Positive controls: each way a live test can slip past the registration is
// planted in a fixture tree, and the check must name it.
func TestRegistryCatchesWhatSlipsPast(t *testing.T) {
	good := []row{{"a", "local", "internal/a", "^TestA$", "HPCBURST_CONFORMANCE", "-", "host", "ready"}}
	cases := []struct {
		name  string
		files map[string]string
		rows  []row
		want  string // a substring of a problem; "" means the tree must pass
	}{
		{"a registered test passes", map[string]string{"internal/a/a_test.go": strings.Replace(strings.Replace(liveTestSrc, "%s", "a", 1), "%s", "TestA", 1)}, good, ""},
		{"an unlisted live package", map[string]string{"internal/newlive/x_test.go": strings.Replace(strings.Replace(liveTestSrc, "%s", "none a reason", 1), "%s", "TestX", 1)}, good, "package internal/newlive holds a live test"},
		{"a test that reads an environment variable for another reason is not live",
			map[string]string{"internal/quiet/x_test.go": "package p\nimport (\"os\"; \"testing\")\nfunc TestQ(t *testing.T) { if os.Getenv(\"X\") == \"\" { t.Skip(\"root reads every file\") } }\n"}, good, ""},
		{"a gate helper in a non-test file", map[string]string{
			"internal/hid/helper.go": "package p\nimport (\"os\"; \"testing\")\nfunc requireReal(t *testing.T) { if os.Getenv(\"X\") == \"\" { t.Skip(\"needs a real cluster\") } }\n",
			"internal/hid/x_test.go": "package p\nimport \"testing\"\nfunc TestHidden(t *testing.T) { requireReal(t) }\n"}, good, "package internal/hid holds a live test (internal/hid/x_test.go: calls the gate helper requireReal)"},
		{"a gate helper in another package", map[string]string{
			"internal/livegate/gate.go": "package livegate\nimport (\"os\"; \"testing\")\nfunc Require(t *testing.T) { if os.Getenv(\"X\") == \"\" { t.Skip(\"needs a live system\") } }\n",
			"internal/hid/x_test.go":    "package p\nimport (\"testing\"; \"x/internal/livegate\")\nfunc TestHidden(t *testing.T) { livegate.Require(t) }\n"}, good, "calls the gate helper Require"},
		{"a live file named for its gate", map[string]string{"internal/g/x_test.go": "package p\nconst g = \"HPCBURST_CONFORMANCE\"\nfunc TestG(t *testing.T) {}\n"}, good, "package internal/g holds a live test"},
		{"a test of a live file with no directive", map[string]string{
			"internal/a/a_test.go": strings.Replace(strings.Replace(liveTestSrc, "%s", "a", 1), "%s", "TestA", 1),
			"internal/a/b_test.go": "package p\nimport (\"os\"; \"testing\")\nfunc TestB(t *testing.T) { if os.Getenv(\"X\") == \"\" { t.Skip(\"needs a real cluster\") } }\n"}, good, "TestB is in a live file and has no //livecheck: directive"},
		{"none without a reason", map[string]string{"internal/a/a_test.go": strings.Replace(strings.Replace(liveTestSrc, "%s", "none", 1), "%s", "TestA", 1)}, good, "none without a reason"},
		{"a directive naming a row that does not exist", map[string]string{"internal/a/a_test.go": strings.Replace(strings.Replace(liveTestSrc, "%s", "nope", 1), "%s", "TestA", 1)}, good, "names row nope"},
		{"a directive naming a row of another package", map[string]string{"internal/b/a_test.go": strings.Replace(strings.Replace(liveTestSrc, "%s", "a", 1), "%s", "TestA", 1)}, good, "whose package is internal/a"},
		{"a live test that the row's -run pattern leaves out", map[string]string{
			"internal/a/a_test.go": strings.Replace(strings.Replace(liveTestSrc, "%s", "a", 1), "%s", "TestA", 1),
			"internal/a/b_test.go": strings.Replace(strings.Replace(liveTestSrc, "%s", "a", 1), "%s", "TestB", 1)}, good, "does not match it, so the script would never run it"},
		{"a pattern that also runs a function not naming the row", map[string]string{
			"internal/a/a_test.go": strings.Replace(strings.Replace(liveTestSrc, "%s", "a", 1), "%s", "TestA", 1),
			"internal/a/b_test.go": "package p\nimport \"testing\"\nfunc TestAB(t *testing.T) {}\n"},
			[]row{{"a", "local", "internal/a", "^TestA", "HPCBURST_CONFORMANCE", "-", "host", "ready"}}, "also runs TestAB"},
		{"a row no function names", map[string]string{"internal/a/a_test.go": "package p\nimport \"testing\"\nfunc TestOther(t *testing.T) {}\n"}, good, "no test function of internal/a matches"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Every tree starts with a registered test of the row "a"; a case replaces or adds files.
			fsys := fstest.MapFS{"internal/a/a_test.go": mapFile(strings.Replace(strings.Replace(liveTestSrc, "%s", "a", 1), "%s", "TestA", 1))}
			for p, src := range c.files {
				fsys[p] = mapFile(src)
			}
			problems, _, err := checkRegistry(fsys, c.rows)
			if err != nil {
				t.Fatal(err)
			}
			joined := strings.Join(problems, "\n")
			switch {
			case c.want == "" && len(problems) > 0:
				t.Errorf("unexpected problems:\n%s", joined)
			case c.want != "" && !strings.Contains(joined, c.want):
				t.Errorf("no problem containing %q in:\n%s", c.want, joined)
			}
		})
	}
}

// Mutations of the real tree: each must be reported, or the check would pass with its parts gone.
func TestRegistryFailsOnTheRealTreeWhenAPartIsRemoved(t *testing.T) {
	rows := readRows(t)
	root := repoRoot(t)

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
		problems, _, err := checkRegistry(realTree(t), rest)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(strings.Join(problems, "\n"), "package "+r.pkg+" holds a live test") {
			t.Errorf("without the rows of %s the check reports %v", r.pkg, problems)
		}
	}

	golden, err := os.ReadFile(filepath.Join(root, "pkg/hubfiles/golden_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	stripped := regexp.MustCompile(`(?m)^//livecheck:.*\n`).ReplaceAll(golden, nil)
	if len(stripped) == len(golden) {
		t.Fatal("golden_test.go has no directive to strip")
	}
	problems, _, _ := checkRegistry(overlay{realTree(t), map[string][]byte{"pkg/hubfiles/golden_test.go": stripped}}, rows)
	if !strings.Contains(strings.Join(problems, "\n"), "TestGoldenHub is in a live file and has no //livecheck: directive") {
		t.Errorf("a stripped directive is not reported: %v", problems)
	}

	var narrowed []row
	for _, r := range rows {
		if r.name == "real-engine" {
			r.test = "^TestRealEngineXX$"
		}
		narrowed = append(narrowed, r)
	}
	problems, _, _ = checkRegistry(realTree(t), narrowed)
	if !strings.Contains(strings.Join(problems, "\n"), "does not match it, so the script would never run it") {
		t.Errorf("a -run pattern that excludes the test is not reported: %v", problems)
	}
}

// The scanner lists test functions from the syntax of the files. This checks
// that against the compiler's own list for one real package, so the two do not drift.
func TestTestListAgreesWithGoTestList(t *testing.T) {
	root := repoRoot(t)
	s, err := scan(realTree(t), gateSet(readRows(t)))
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	for _, fn := range s.tests["pkg/hubfiles"] {
		want = append(want, fn.name)
	}
	cmd := exec.Command("go", "test", "-list", ".*", "./pkg/hubfiles")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go test -list: %v", err)
	}
	var got []string
	for _, l := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(l, "Test") {
			got = append(got, strings.TrimSpace(l))
		}
	}
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("the scanner lists %d tests of pkg/hubfiles, go test -list %d", len(want), len(got))
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
		for _, c := range strings.Split(r.contexts, ",") {
			if c != "host" && c != "linux-arm64" {
				t.Errorf("%s: build context %q is neither host nor linux-arm64", r.name, c)
			}
		}
		if _, err := os.Stat(filepath.Join(root, r.pkg)); err != nil {
			t.Errorf("%s: package %s: %v", r.name, r.pkg, err)
			continue
		}
		var tests strings.Builder
		entries, _ := os.ReadDir(filepath.Join(root, r.pkg))
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), "_test.go") {
				b, _ := os.ReadFile(filepath.Join(root, r.pkg, e.Name()))
				tests.Write(b)
			}
		}
		for _, g := range strings.Split(r.gates, ",") {
			if !strings.Contains(tests.String(), `"`+g+`"`) {
				t.Errorf("%s: package %s never mentions its gate %s as a string", r.name, r.pkg, g)
			}
		}
		if r.inputs != "-" {
			for _, in := range strings.Split(r.inputs, ",") {
				if m, _ := filepath.Glob(filepath.Join(root, in)); len(m) == 0 {
					t.Errorf("%s: input %s matches nothing", r.name, in)
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

// TestGoldenHub reads these files of spike S7; a change to one must select the test (issue #163, review of PR 164).
func TestHubGoldenInputsAreTheFilesTheTestReads(t *testing.T) {
	src, err := os.ReadFile(filepath.Join(repoRoot(t), "pkg/hubfiles/golden_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	var hub row
	for _, r := range readRows(t) {
		if r.name == "hub-golden" {
			hub = r
		}
	}
	// The test builds its paths from s7Dir = ../../spikes/s7; the file names it joins are these.
	for _, want := range []string{`"checkpoints.env"`, `"results", "run1", "hash-"`, `"results", "run1", "hub"`} {
		if !strings.Contains(string(src), want) {
			t.Fatalf("golden_test.go no longer joins %s: update this test and the row's inputs together", want)
		}
	}
	for _, want := range []string{"spikes/s7/checkpoints.env", "spikes/s7/results/run1/hub", "spikes/s7/results/run1/hash-*.jsonl"} {
		if !strings.Contains(","+hub.inputs+",", ","+want+",") {
			t.Errorf("hub-golden's inputs lack %s: %s", want, hub.inputs)
		}
	}
}
