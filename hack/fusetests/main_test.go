package main

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// writeTree writes files (path relative to a new directory -> content) and returns the directory.
func writeTree(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for name, content := range files {
		p := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// sinks is a test file that defines every mount function, as cmd/hpcburst's capfs files do.
const sinks = `package x

import "testing"

func newCapVolume(t *testing.T, n int64) {}
func mountCapVolume()                    {}
func launchCapServer()                   {}
func fusermountCapfs()                   {}
`

// The expected sets below are written out by hand from the shape of each fixture, never computed by the code under test.
func TestAFixtureTreeGivesTheTestsThatReachAMount(t *testing.T) {
	root := writeTree(t, map[string]string{
		"sinks_test.go": sinks,
		"uses_test.go": `package x

import "testing"

// A direct call, a call through a helper, a call through a helper of a helper.
func TestDirect(t *testing.T)         { newCapVolume(t, 1) }
func TestViaHelper(t *testing.T)      { helper(t) }
func TestViaTwoHelpers(t *testing.T)  { outer() }
func TestUsesTheMountStep(t *testing.T) { fusermountCapfs() }
func TestStartsTheServer(t *testing.T) { launchCapServer() }

func helper(t *testing.T) { newCapVolume(t, 2) }
func outer()              { inner() }
func inner()              { mountCapVolume() }

// A method, reached by its name, and a function value that is only named.
type rig struct{}

func (r *rig) up()                  { launchCapServer() }
func TestViaAMethod(t *testing.T)   { r := &rig{}; r.up() }
func TestViaAValue(t *testing.T)    { t.Cleanup(release) }
func release()                      { mountCapVolume() }

// A cycle between two functions, one of which mounts.
func ping(n int) { if n > 0 { pong(n - 1) } }
func pong(n int) { ping(n); mountCapVolume() }
func TestInACycle(t *testing.T) { ping(3) }

// Tests that mount nothing, a test-shaped name that is not a test, and a function of another shape.
func TestPlain(t *testing.T)        { plain() }
func plain()                        {}
func TestMain(m *testing.M)         { mountCapVolume() }
func Testlower(t *testing.T)        { mountCapVolume() }
func TestWrongType(b *testing.B)    { mountCapVolume() }
func TestTwoParams(t *testing.T, x int) { mountCapVolume() }
func (r *rig) TestOnAMethod(t *testing.T) { mountCapVolume() }
`,
		"nested/deeper_test.go": `package y

import "testing"

func newCapVolume(t *testing.T, n int64) {}
func TestInANestedDirectory(t *testing.T) { newCapVolume(t, 3) }
`,
		"testdata/ignored_test.go": `package z

import "testing"

func TestInTestdata(t *testing.T) { newCapVolume(t, 4) }
`,
	})
	// nested/ defines only one mount function of its own; the others are defined in the root directory, and the tool asks that each
	// exists somewhere under the roots.
	got, err := fuseTests([]string{root})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"TestDirect", "TestInACycle", "TestInANestedDirectory", "TestStartsTheServer", "TestUsesTheMountStep", "TestViaAMethod",
		"TestViaAValue", "TestViaHelper", "TestViaTwoHelpers"}
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("tests that reach a mount:\n got  %v\n want %v", got, want)
	}
}

func TestAMountFunctionThatNoDirectoryDefinesFailsTheTool(t *testing.T) {
	for _, missing := range mountSinks {
		other := "mountCapVolume" // a test reaches one that is still defined, so that the only fault is the missing one
		if missing == other {
			other = "launchCapServer"
		}
		src := strings.ReplaceAll(sinks, "func "+missing+"(", "func renamed"+missing+"(")
		root := writeTree(t, map[string]string{
			"sinks_test.go": src,
			"uses_test.go":  "package x\n\nimport \"testing\"\n\nfunc TestUses(t *testing.T) { " + other + "() }\n",
		})
		_, err := fuseTests([]string{root})
		if err == nil || !strings.Contains(err.Error(), missing+" is not defined") {
			t.Errorf("with %s renamed the tool gave %v, want an error that says it is not defined", missing, err)
		}
	}
}

func TestATreeWithNoTestThatMountsFailsTheTool(t *testing.T) {
	root := writeTree(t, map[string]string{
		"sinks_test.go": sinks,
		"plain_test.go": "package x\n\nimport \"testing\"\n\nfunc TestPlain(t *testing.T) {}\n",
	})
	if _, err := fuseTests([]string{root}); err == nil || !strings.Contains(err.Error(), "no test") {
		t.Errorf("a tree where no test mounts: %v, want an error", err)
	}
}

func TestAFileThatDoesNotParseFailsTheTool(t *testing.T) {
	root := writeTree(t, map[string]string{"broken_test.go": "package x\n\nfunc TestBroken( {\n"})
	if _, err := fuseTests([]string{root}); err == nil {
		t.Error("a file that does not parse gave no error")
	}
}

// The real trees. The names below are the ones issue #657 gives, and #656's real-mount tests, and the tests this change adds: the list
// is a lower bound read from the issue, so that the tool is held to what a person says mounts a volume, never to its own output. A test
// added later is found by the tool and needs no edit here.
func TestTheRealTreesGiveTheTestsThatMountAVolume(t *testing.T) {
	got, err := fuseTests([]string{"../../internal/launcher", "../../cmd/hpcburst"})
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]bool{}
	for _, n := range got {
		have[n] = true
	}
	for _, name := range []string{
		// #657: the three the job named, and the two it missed on main.
		"TestCapVolume", "TestStoreFull", "TestStoreFullWholeDataDir", "TestCapVolumeFrozen", "TestStoreFullHintOnACapVolume",
		// #656's eight real-mount tests.
		"TestAKilledServerLeavesADeadMountAndNoFusermount3", "TestAnUnmountedVolumeLeavesNoFusermount3", "TestThePollHackLeavesTheKernelNoPOLLToSend",
		"TestTheNextCapVolumeSweepsAMountLeftByAKilledServer", "TestTheNextCapVolumeLeavesAnotherProcessesLiveMountAlone",
		"TestTheSweepTakesTheDeadMountAndLeavesTheLiveOneOfAnotherChild", "TestTheSweepLeavesAWedgedMountAloneAndReturnsPromptly",
		"TestTheMountsDescriptorAndSocketAreCloseOnExec",
		// #658: the kill test and its victim.
		"TestAKilledTestProcessMidWriteLeavesADeadMountNotAWedgedOne", "TestCapVictimWritesToItsVolume",
	} {
		if !have[name] {
			t.Errorf("%s mounts a volume, and the tool does not list it", name)
		}
	}
	// Tests of the same files that mount nothing: the mount's helper runs ls, the sweep's are over fixed text.
	for _, name := range []string{
		"TestTheMountHelperGetsOnlyItsOwnEndOfTheSocket", "TestSweepTakesOnlyThisUsersDeadCapMounts", "TestUnescapeMountinfo",
		"TestTheProbeOfAnOrdinaryPath", "TestMountedUnderFindsAMountInTheDirectory",
	} {
		if have[name] {
			t.Errorf("%s mounts nothing, and the tool lists it", name)
		}
	}
	if !sort.StringsAreSorted(got) {
		t.Errorf("the names are not sorted: %v", got)
	}
}

// The job reads its list from this tool and from nowhere else, and scans the packages that hack/test-launcher.sh runs: a hand-kept list in
// the job, or a package that the script runs and the tool does not scan, would bring back what #657 removed.
func TestLauncherK3sDerivesItsNoFuseListFromThisTool(t *testing.T) {
	workflow, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(workflow), "\n  launcher-k3s:\n")
	if start < 0 {
		t.Fatal("ci.yml has no launcher-k3s job")
	}
	job := string(workflow)[start+1:]
	if next := regexp.MustCompile(`\n  [A-Za-z0-9_-]+:\n`).FindStringIndex(job[len("  launcher-k3s:\n"):]); next != nil {
		job = job[:len("  launcher-k3s:\n")+next[0]]
	}
	if hand := regexp.MustCompile(`excluded=\(\s*Test`).FindString(job); hand != "" {
		t.Errorf("launcher-k3s keeps a hand-written list of tests (%q): derive it with hack/fusetests", hand)
	}
	m := regexp.MustCompile(`go run \./hack/fusetests((?: \./[A-Za-z0-9_/.-]+)+)`).FindStringSubmatch(job)
	if m == nil {
		t.Fatal("launcher-k3s does not run go run ./hack/fusetests with the packages to scan")
	}
	scanned := strings.Fields(m[1])

	script, err := os.ReadFile("../test-launcher.sh")
	if err != nil {
		t.Fatal(err)
	}
	line := regexp.MustCompile(`(?m)^go test .*$`).FindString(string(script))
	var run []string
	for _, f := range strings.Fields(line) {
		if strings.HasPrefix(f, "./") && strings.HasSuffix(f, "/...") {
			run = append(run, strings.TrimSuffix(f, "/..."))
		}
	}
	if len(run) == 0 {
		t.Fatalf("hack/test-launcher.sh names no package to test in %q", line)
	}
	sort.Strings(scanned)
	sort.Strings(run)
	if !reflect.DeepEqual(scanned, run) {
		t.Errorf("hack/test-launcher.sh runs %v, and launcher-k3s asks hack/fusetests to scan %v: they must be the same packages", run, scanned)
	}
}
