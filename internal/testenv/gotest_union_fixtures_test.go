package testenv

// The union count of gotest_union_test.go, shown to fail: on fixtures of each mistake it exists for, and
// through the real script with a table that makes one, in a stand-in tree so the real tree is never
// changed.

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

var fixtureTests = map[string][]string{
	"./a": {"TestA1", "TestA2", "TestA3"},
	"./b": {"TestB1", "TestB2"},
}

func TestUnionProblemsFindsADroppedOrDuplicatedTest(t *testing.T) {
	re := regexp.MustCompile
	pkgs := func(skip string) part { return part{name: "packages", pkg: "./...", skip: re(skip)} }
	grp := func(name, pkg, run string) part { return part{name: name, pkg: pkg, run: re(run)} }
	for _, c := range []struct {
		name  string
		parts []part
		want  []string
	}{
		{"every test once", []part{pkgs("^(TestA1|TestB2)$"), grp("g1", "./a", "^(TestA1)$"), grp("g2", "./b", "^(TestB2)$")}, nil},
		{"a name in two groups", []part{pkgs("^(TestA1|TestA1|TestA2)$"), grp("g1", "./a", "^(TestA1)$"), grp("g2", "./a", "^(TestA1|TestA2)$")},
			[]string{"./a TestA1 runs in 2 parts (g1, g2): it runs twice"}},
		{"a group lists a test of another package", []part{pkgs("^(TestB1)$"), grp("g1", "./a", "^(TestB1)$")},
			[]string{"./b TestB1 runs in no part: it is dropped"}},
		{"the packages' skip is wider than the groups", []part{pkgs("^(TestA1|TestA2)$"), grp("g1", "./a", "^(TestA1)$")},
			[]string{"./a TestA2 runs in no part: it is dropped"}},
		{"a group's run is wider than its names", []part{pkgs("^(TestA1)$"), grp("g1", "./a", "^TestA")},
			[]string{"./a TestA2 runs in 2 parts (packages, g1): it runs twice", "./a TestA3 runs in 2 parts (packages, g1): it runs twice"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := unionProblems(c.parts, fixtureTests); !reflect.DeepEqual(got, c.want) {
				t.Errorf("problems %q, want %q", got, c.want)
			}
		})
	}
}

func TestCIProblemsFindsAPartTheGoJobDoesNotRunOnce(t *testing.T) {
	good := map[string][]string{
		"go":  {"go vet ./...", "log=x\nbash hack/go-test.sh packages 2>&1 | tee \"$log\" || rc=$?", "bash hack/go-test.sh own g1", "bash hack/go-test.sh own g2"},
		"crd": {"bash hack/check.sh 2>&1 | tee \"$log\" || rc=$?"},
	}
	edit := func(job string, steps ...string) map[string][]string {
		m := map[string][]string{"go": good["go"], "crd": good["crd"]}
		m[job] = steps
		return m
	}
	for _, c := range []struct {
		name string
		runs map[string][]string
		want []string
	}{
		{"every part once", good, nil},
		{"a group has no step", edit("go", good["go"][1], good["go"][2]), []string{`CI's go job runs "bash hack/go-test.sh own g2" in 0 steps, want 1`}},
		{"a group has two", edit("go", append(append([]string{}, good["go"]...), "bash hack/go-test.sh own g1")...),
			[]string{`CI's go job runs "bash hack/go-test.sh own g1" in 2 steps, want 1`}},
		// A step named for another group, "own g10", is not g1's.
		{"only a longer name", edit("go", good["go"][1], "bash hack/go-test.sh own g10", good["go"][3]), []string{`CI's go job runs "bash hack/go-test.sh own g1" in 0 steps, want 1`}},
		{"the go job runs every group again", edit("go", append(append([]string{}, good["go"]...), "bash hack/go-test.sh own")...),
			[]string{`CI's go job runs "bash hack/go-test.sh own", which runs the groups again`}},
		{"no packages' run", edit("go", good["go"][2], good["go"][3]), []string{`CI's go job runs "bash hack/go-test.sh packages" in 0 steps, want 1`}},
		{"the crd job does not run check.sh", edit("crd", "go test ./..."), []string{"CI's crd job runs hack/check.sh in 0 steps, want 1 (it runs every part through all)"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := ciProblems(c.runs, []string{"g1", "g2"}); !reflect.DeepEqual(got, c.want) {
				t.Errorf("problems %q, want %q", got, c.want)
			}
		})
	}
}

// The real script's code with a table that makes each mistake, in a tree of two packages: the count
// finds it, through the calls the script makes.
func TestGoTestScriptUnionCatchesAMistakeInTheScriptsTable(t *testing.T) {
	real, err := os.ReadFile(filepath.Join(realRoot(t), "hack", "go-test.sh"))
	if err != nil {
		t.Fatal(err)
	}
	table := regexp.MustCompile(`(?s)\ngroups=\(\n.*?\n\)\n`)
	if len(table.FindAllIndex(real, -1)) != 1 {
		t.Fatal("hack/go-test.sh has no single groups=( ... ) table to replace")
	}
	for _, c := range []struct {
		name string
		rows []string
		want []string
	}{
		{"every test once", []string{"g1|./a|TestA1|60s", "g2|./b|TestB2|60s"}, nil},
		{"a name in two groups", []string{"g1|./a|TestA1|60s", "g2|./a|TestA1 TestA2|60s"}, []string{"./a TestA1 runs in 2 parts (g1, g2): it runs twice"}},
		{"a group lists a test of another package", []string{"g1|./a|TestA1 TestB1|60s"}, []string{"./b TestB1 runs in no part: it is dropped"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			stand := t.TempDir()
			var rows strings.Builder
			for _, r := range c.rows {
				rows.WriteString("  \"" + r + "\"\n")
			}
			script := table.ReplaceAll(real, []byte("\ngroups=(\n"+rows.String()+")\n"))
			for rel, content := range map[string]string{
				"hack/go-test.sh": string(script),
				"a/a_test.go":     "package a\n\nimport \"testing\"\n\nfunc TestA1(t *testing.T) {}\nfunc TestA2(t *testing.T) {}\nfunc TestA3(t *testing.T) {}\n",
				"b/b_test.go":     "package b\n\nimport \"testing\"\n\nfunc TestB1(t *testing.T) {}\nfunc TestB2(t *testing.T) {}\n\n// Not tests: a lower-case rune after Test, and the package's main.\nfunc Testable(t *testing.T) {}\nfunc TestMain(m *testing.M) {}\n",
			} {
				p := filepath.Join(stand, rel)
				if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(p, []byte(content), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			tests, err := moduleTests(stand)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(tests, fixtureTests) {
				t.Fatalf("the stand-in's tests read as %v, want %v", tests, fixtureTests)
			}
			if got := unionProblems(scriptParts(t, stand), tests); !reflect.DeepEqual(got, c.want) {
				t.Errorf("problems %q, want %q", got, c.want)
			}
		})
	}
}
