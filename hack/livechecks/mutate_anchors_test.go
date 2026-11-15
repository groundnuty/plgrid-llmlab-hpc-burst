package livechecks

import (
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// Every mutant of hack/mutate-alpha.sh names a target file and a text that must occur exactly once in it (issue #592). When a later
// change moves the code under a text, the mutant no longer applies and so tests nothing; CI never runs the full set, so on main about 36
// such mutants went unseen until a builder checked every text by hand. `hack/mutate-alpha.sh --check-anchors` reads the list as the run
// does (the same parse_mutant) and judges each text by the same count the run applies a mutant by (anchor_edit), and it runs no mutant
// and no test, so it takes seconds. These tests are in the `go` job, a required check.

// runCheckAnchors runs the harness's --check-anchors, over the real tree when root is "" and over a throwaway tree otherwise, with
// extra environment (a PATH in front, for the tests of what the mode must not run), and returns its exit status and its output.
func runCheckAnchors(t *testing.T, root, mutants string, env ...string) (int, string) {
	t.Helper()
	cmd := exec.Command("bash", filepath.Join(repoRoot(t), "hack", "mutate-alpha.sh"), "--check-anchors")
	cmd.Env = os.Environ()
	if root != "" {
		cmd.Env = append(cmd.Env, "MUTATE_ALPHA_ROOT="+root, "MUTATE_ALPHA_MUTANTS="+mutants)
	}
	cmd.Env = append(cmd.Env, env...)
	out, err := cmd.CombinedOutput()
	if ee, ok := err.(*exec.ExitError); ok {
		return ee.ExitCode(), string(out)
	} else if err != nil {
		t.Fatal(err)
	}
	return 0, string(out)
}

var anchorsSummary = regexp.MustCompile(`(?m)^anchors: (\d+) mutants, (\d+) not exactly once$`)

// mutantStartLines counts the lines of the script's list that start a mutant (two spaces, a quote, and the @@ of its first field). It is a
// second reading of the list that shares nothing with the harness's, and a lower bound of what the harness reads: a line may hold two.
func mutantStartLines(t *testing.T) int {
	t.Helper()
	f, err := os.Open(filepath.Join(repoRoot(t), "hack", "mutate-alpha.sh"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	in, n := false, 0
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
	for sc.Scan() {
		switch line := sc.Text(); {
		case line == "mutants=(":
			in = true
		case in && line == ")":
			return n
		case in && strings.HasPrefix(line, "  '") && strings.Contains(line, "@@"):
			n++
		}
	}
	t.Fatalf("no list of mutants found in hack/mutate-alpha.sh (scan error: %v)", sc.Err())
	return 0
}

// The real list, over the real tree: every mutant's text occurs exactly once in its file. A failure names the mutant by its place in the
// list and its file, and says how many times the text occurs. Once the texts are all fixed this passes whether or not the check works, so
// the next tests, on a fixture, are what show that it does.
func TestMutateAlphaEveryMutantsTextOccursExactlyOnce(t *testing.T) {
	code, out := runCheckAnchors(t, "", "")
	m := anchorsSummary.FindStringSubmatch(out)
	if m == nil {
		t.Fatalf("--check-anchors (exit %d) printed no summary line:\n%s", code, out)
	}
	read, _ := strconv.Atoi(m[1])
	if starts := mutantStartLines(t); read < starts {
		t.Fatalf("--check-anchors read %d mutants, but the list has %d lines that each start one: the harness read the list short", read, starts)
	}
	if code != 0 {
		t.Fatalf("a mutant's text does not occur exactly once in its file, so it tests nothing (re-anchor it, replace it, or remove it with a reason):\n%s", out)
	}
}

// anchorsFixture is a throwaway tree with one target file in the package's directory and one in hack/, and the list of mutants over it.
func anchorsFixture(t *testing.T, mutants ...string) (dir, list string) {
	t.Helper()
	dir = t.TempDir()
	write(t, filepath.Join(dir, "internal", "alpha", "good.go"),
		"package alpha\n\nvar a = 1 // the-one-text\nvar b = 2 // the-doubled-text\nvar c = 3 // the-doubled-text\n", 0o644)
	write(t, filepath.Join(dir, "hack", "live-checks.sh"), "#!/bin/bash\necho a-text-in-the-script\n", 0o644)
	list = filepath.Join(dir, "mutants.txt")
	write(t, list, strings.Join(mutants, "\n")+"\n", 0o644)
	return dir, list
}

// The check must tell a mutant whose text occurs once from one whose text is missing, one whose text occurs twice, and one whose file is
// gone, and name each of the bad three, and only them: a check that passes everything, or fails everything, fails here. The good ones
// are a bare file name (the package's directory) and a path.
func TestMutateAlphaCheckAnchorsNamesEachMutantWhoseTextIsNotThereExactlyOnce(t *testing.T) {
	dir, list := anchorsFixture(t,
		"good.go@@the-one-text@@x",
		"good.go@@no-such-text@@x",
		"good.go@@the-doubled-text@@x",
		"gone.go@@anything@@x",
		"hack/live-checks.sh@@a-text-in-the-script@@x",
	)
	code, out := runCheckAnchors(t, dir, list)
	if code != 1 {
		t.Fatalf("exit %d, want 1 for a list with three bad texts:\n%s", code, out)
	}
	for _, want := range []string{
		"BROKEN   mutant 2, good.go: the text 'no-such-text' occurs 0 times, not exactly once\n",
		"BROKEN   mutant 3, good.go: the text 'the-doubled-text' occurs 2 times, not exactly once\n",
		"BROKEN   mutant 4, gone.go: the file internal/alpha/gone.go cannot be read, so its text 'anything' occurs nowhere\n",
		"anchors: 5 mutants, 3 not exactly once\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the output lacks %q:\n%s", want, out)
		}
	}
	for _, good := range []string{"the-one-text", "a-text-in-the-script", "mutant 1,", "mutant 5,"} {
		if strings.Contains(out, good) {
			t.Errorf("the output names a mutant whose text occurs exactly once (%q):\n%s", good, out)
		}
	}
	// The same list with only the good mutants is a pass.
	dir, list = anchorsFixture(t, "good.go@@the-one-text@@x", "hack/live-checks.sh@@a-text-in-the-script@@x")
	if code, out = runCheckAnchors(t, dir, list); code != 0 || out != "anchors: 2 mutants, 0 not exactly once\n" {
		t.Errorf("a list of good mutants: exit %d, output %q; want exit 0 and the summary alone", code, out)
	}
}

// A list that reads as empty is a check of nothing, so it must not pass (exit 2, not 0); and neither may a checker that ends without
// having read every mutant, which a stand-in for python3 that prints nothing and exits 0 plays.
func TestMutateAlphaCheckAnchorsRefusesToPassWhenItCheckedNothing(t *testing.T) {
	dir, list := anchorsFixture(t)
	if err := os.WriteFile(list, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if code, out := runCheckAnchors(t, dir, list); code != 2 || !strings.Contains(out, "read no mutant") {
		t.Errorf("an empty list: exit %d, output %q; want exit 2 saying it read no mutant", code, out)
	}
	dir, list = anchorsFixture(t, "good.go@@no-such-text@@x")
	stubs := t.TempDir()
	write(t, filepath.Join(stubs, "python3"), "#!/bin/sh\nexit 0\n", 0o755)
	code, out := runCheckAnchors(t, dir, list, "PATH="+stubs+":"+os.Getenv("PATH"))
	if code != 2 || !strings.Contains(out, "checked none of 1 mutants") || strings.Contains(out, "anchors:") {
		t.Errorf("a checker that said nothing: exit %d, output %q; want exit 2 saying it checked none of 1, and no summary", code, out)
	}
}

// --check-anchors runs no mutant and no test: it must not compile or run a Go test (a stand-in for go that records its use), and it leaves
// every file of the tree as it was.
func TestMutateAlphaCheckAnchorsRunsNothingAndChangesNothing(t *testing.T) {
	dir, list := anchorsFixture(t, "good.go@@the-one-text@@x", "good.go@@no-such-text@@x")
	before, err := os.ReadFile(filepath.Join(dir, "internal", "alpha", "good.go"))
	if err != nil {
		t.Fatal(err)
	}
	stubs, used := t.TempDir(), filepath.Join(t.TempDir(), "go-was-run")
	write(t, filepath.Join(stubs, "go"), "#!/bin/sh\necho \"$@\" >> "+used+"\nexit 0\n", 0o755)
	if code, out := runCheckAnchors(t, dir, list, "PATH="+stubs+":"+os.Getenv("PATH")); code != 1 {
		t.Fatalf("exit %d, want 1 for the bad mutant:\n%s", code, out)
	}
	if b, err := os.ReadFile(used); err == nil {
		t.Errorf("--check-anchors ran go: %s", b)
	}
	if after, err := os.ReadFile(filepath.Join(dir, "internal", "alpha", "good.go")); err != nil || string(after) != string(before) {
		t.Errorf("--check-anchors changed the file of a mutant (%v):\n%s", err, after)
	}
}
