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

func TestCIProblemsFindsAPartThatCIDoesNotRunOnce(t *testing.T) {
	gated := func(run string) ciStep { return ciStep{Run: run, If: verifiedGate} }
	goSteps := []ciStep{{Run: "go vet ./..."}, {Run: "log=x\nbash hack/go-test.sh packages 2>&1 | tee \"$log\" || rc=$?"}}
	ownSteps := []ciStep{gated("bash hack/go-test.sh own g1"), gated("bash hack/go-test.sh own g2")}
	crdSteps := []ciStep{{Run: "bash hack/check.sh 2>&1 | tee \"$log\" || rc=$?"}}
	jobs := func(goS, ownS, crdS []ciStep) map[string]ciJob {
		return map[string]ciJob{"go": {Steps: goS}, "go-own": {Steps: ownS}, "crd": {Steps: crdS}}
	}
	// own returns the go-own job's steps with step i changed by f; packages the go job's step i.
	own := func(i int, f func(*ciStep)) []ciStep {
		s := append([]ciStep{}, ownSteps...)
		f(&s[i])
		return s
	}
	packages := func(i int, f func(*ciStep)) []ciStep {
		s := append([]ciStep{}, goSteps...)
		f(&s[i])
		return s
	}
	more := func(steps []ciStep, extra ...ciStep) []ciStep { return append(append([]ciStep{}, steps...), extra...) }
	for _, c := range []struct {
		name string
		jobs map[string]ciJob
		want []string
	}{
		{"every part once", jobs(goSteps, ownSteps, crdSteps), nil},
		{"a group has no step", jobs(goSteps, ownSteps[:1], crdSteps), []string{`CI's go-own job runs "bash hack/go-test.sh own g2" in 0 steps, want 1`}},
		{"a group has two", jobs(goSteps, more(ownSteps, gated("bash hack/go-test.sh own g1")), crdSteps),
			[]string{`CI's go-own job runs "bash hack/go-test.sh own g1" in 2 steps, want 1`}},
		// A step named for another group, "own g10", is not g1's.
		{"only a longer name", jobs(goSteps, own(0, func(s *ciStep) { s.Run = "bash hack/go-test.sh own g10" }), crdSteps),
			[]string{`CI's go-own job runs "bash hack/go-test.sh own g1" in 0 steps, want 1`}},
		{"the go-own job runs every group again", jobs(goSteps, more(ownSteps, gated("bash hack/go-test.sh own")), crdSteps),
			[]string{`CI's go-own job runs "bash hack/go-test.sh own", which runs the groups again`}},
		{"the go job runs every group", jobs(more(goSteps, gated("bash hack/go-test.sh own")), ownSteps, crdSteps),
			[]string{`CI's go job runs "bash hack/go-test.sh own", which runs the groups again`}},
		{"no packages' run", jobs(goSteps[:1], ownSteps, crdSteps), []string{`CI's go job runs "bash hack/go-test.sh packages" in 0 steps, want 1`}},
		{"the crd job does not run check.sh", jobs(goSteps, ownSteps, []ciStep{{Run: "go test ./..."}}), []string{`CI's crd job runs "bash hack/check.sh" in 0 steps, want 1`}},
		// A part in the other job runs twice.
		{"a group also runs in the go job", jobs(more(goSteps, gated("bash hack/go-test.sh own g1")), ownSteps, crdSteps),
			[]string{`CI's go job runs "bash hack/go-test.sh own g1" in 1 steps, which belong to the go-own job: it would run twice`}},
		{"the packages also run in the go-own job", jobs(goSteps, more(ownSteps, ciStep{Run: "bash hack/go-test.sh packages"}), crdSteps),
			[]string{`CI's go-own job runs "bash hack/go-test.sh packages" in 1 steps, which belongs to the go job: it would run twice`}},
		{"a group moved back to the go job, with no step in go-own", jobs(more(goSteps, gated("bash hack/go-test.sh own g2")), ownSteps[:1], crdSteps),
			[]string{`CI's go-own job runs "bash hack/go-test.sh own g2" in 0 steps, want 1`, `CI's go job runs "bash hack/go-test.sh own g2" in 1 steps, which belong to the go-own job: it would run twice`}},
		// Steps and jobs that can be skipped, or pass when they fail.
		{"a group step under if: false", jobs(goSteps, own(1, func(s *ciStep) { s.If = false }), crdSteps),
			[]string{`CI's go-own job runs "bash hack/go-test.sh own g2" under if: false, which can skip it; want no if: or ` + verifiedGate}},
		{"a group step only on pushes", jobs(goSteps, own(0, func(s *ciStep) { s.If = "github.event_name == 'push'" }), crdSteps),
			[]string{`CI's go-own job runs "bash hack/go-test.sh own g1" under if: github.event_name == 'push', which can skip it; want no if: or ` + verifiedGate}},
		{"a group step that continues on error", jobs(goSteps, own(0, func(s *ciStep) { s.ContinueOnError = true }), crdSteps),
			[]string{`CI's go-own job runs "bash hack/go-test.sh own g1" with continue-on-error: true, which passes the job when it fails`}},
		{"a group step that continues on an expression", jobs(goSteps, own(1, func(s *ciStep) { s.ContinueOnError = "${{ github.event_name == 'pull_request' }}" }), crdSteps),
			[]string{`CI's go-own job runs "bash hack/go-test.sh own g2" with continue-on-error: ${{ github.event_name == 'pull_request' }}, which passes the job when it fails`}},
		{"the packages' step continues on error", jobs(packages(1, func(s *ciStep) { s.ContinueOnError = true }), ownSteps, crdSteps),
			[]string{`CI's go job runs "bash hack/go-test.sh packages" with continue-on-error: true, which passes the job when it fails`}},
		{"check.sh under an if:", jobs(goSteps, ownSteps, []ciStep{{Run: crdSteps[0].Run, If: "github.ref == 'refs/heads/main'"}}),
			[]string{`CI's crd job runs "bash hack/check.sh" under if: github.ref == 'refs/heads/main', which can skip it; want no if: or ` + verifiedGate}},
		{"the go job under an if:", map[string]ciJob{"go": {If: "false", Steps: goSteps}, "go-own": {Steps: ownSteps}, "crd": {Steps: crdSteps}},
			[]string{"CI's go job has if: false, which can skip it, and a skipped job passes its required check"}},
		{"the go-own job under an if:", map[string]ciJob{"go": {Steps: goSteps}, "go-own": {If: "github.event_name == 'push'", Steps: ownSteps}, "crd": {Steps: crdSteps}},
			[]string{"CI's go-own job has if: github.event_name == 'push', which can skip it, and a skipped job passes its required check"}},
		{"the go-own job continues on error", map[string]ciJob{"go": {Steps: goSteps}, "go-own": {ContinueOnError: true, Steps: ownSteps}, "crd": {Steps: crdSteps}},
			[]string{"CI's go-own job has continue-on-error: true, which passes it when a part fails"}},
		{"the crd job continues on error", map[string]ciJob{"go": {Steps: goSteps}, "go-own": {Steps: ownSteps}, "crd": {ContinueOnError: true, Steps: crdSteps}},
			[]string{"CI's crd job has continue-on-error: true, which passes it when a part fails"}},
		// An explicit continue-on-error: false is the default, and allowed.
		{"continue-on-error: false", jobs(goSteps, own(0, func(s *ciStep) { s.ContinueOnError = false }), crdSteps), nil},
		// No go-own job at all: every group is missing.
		{"no go-own job", map[string]ciJob{"go": {Steps: goSteps}, "crd": {Steps: crdSteps}},
			[]string{`CI's go-own job runs "bash hack/go-test.sh own g1" in 0 steps, want 1`, `CI's go-own job runs "bash hack/go-test.sh own g2" in 0 steps, want 1`}},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := ciProblems(c.jobs, []string{"g1", "g2"}); !reflect.DeepEqual(got, c.want) {
				t.Errorf("problems %q, want %q", got, c.want)
			}
		})
	}
}

// GOFLAGS' build tags reach the reading of the module, as they reach go test's builds: a test behind a
// tag counts only when the tag is set.
func TestModuleTestsReadsTheActiveBuildTags(t *testing.T) {
	for _, c := range []struct {
		goflags string
		want    []string
	}{
		{"", nil},
		{"-p=2", nil},
		{"-p=2 -tags=realtime", []string{"realtime"}},
		{"-tags=a,b -count=1", []string{"a", "b"}},
		{"--tags=x", []string{"x"}},
		{"-tags=a -tags=b", []string{"b"}},
	} {
		if got := tagsFromGOFLAGS(c.goflags); !reflect.DeepEqual(got, c.want) {
			t.Errorf("tagsFromGOFLAGS(%q) = %q, want %q", c.goflags, got, c.want)
		}
	}
	stand := t.TempDir()
	for rel, content := range map[string]string{
		"a/a_test.go":      "package a\n\nimport \"testing\"\n\nfunc TestA1(t *testing.T) {}\n",
		"a/tagged_test.go": "//go:build realtime\n\npackage a\n\nimport \"testing\"\n\nfunc TestTagged(t *testing.T) {}\n",
	} {
		p := filepath.Join(stand, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []struct {
		tags []string
		want map[string][]string
	}{
		{nil, map[string][]string{"./a": {"TestA1"}}},
		{[]string{"realtime"}, map[string][]string{"./a": {"TestA1", "TestTagged"}}},
	} {
		got, err := moduleTests(stand, c.tags)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("tags %q: read %v, want %v", c.tags, got, c.want)
		}
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
			tests, err := moduleTests(stand, nil)
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
