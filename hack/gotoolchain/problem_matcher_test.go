package gotoolchain

// .github/go-problem-matcher.json tells GitHub which lines of a job's log are Go compiler or vet
// errors, to annotate them (issue #340). Its first regexp read any line that cited a source file as an
// error, so passing rehearsal jobs showed error annotations. These tests apply the committed regexp to
// real lines and say by hand which of them it must annotate.
//
// How GitHub applies the matcher (actions/runner, src/Runner.Worker/IssueMatcher.cs, read 10 Nov
// 2026): every line of the step's output goes to Regex.Match, an unanchored search, with the options
// CultureInvariant and ECMAScript. A line that matches becomes an annotation whose file, line, column
// and message are the groups the JSON numbers, and a group that did not take part reads as "". With
// no severity group the annotation is an error (actions/toolkit, docs/problem-matchers.md).
//
// Go's regexp stands in for that engine. The pattern may use only what both read the same way:
// literals, ^, ., classes, \s and \d (ASCII in ECMAScript mode and in Go; Go's \s has no \v, which a
// log line does not hold), groups, and the * + ? operators, all with leftmost-first matching.
// TestProblemMatcherUsesOnlyWhatBothEnginesShare holds the pattern to that list. The old pattern, run
// here on the lines of a real CI log, gives the annotations GitHub recorded for that log
// (TestTheOldMatcherReproducesTheAnnotationsGitHubRecorded), so the stand-in is checked against the
// real engine on the one pattern whose behaviour GitHub's own record shows.
//
// Expected values are typed here from the text of the lines, or from GitHub's record, and never come
// from running a regexp. Two files in testdata hold the lines. go-tool-output.txt is what the go
// command of CI's pinned release printed for broken packages (testdata/capture-go-output.sh makes it).
// ci-log-main-42830956936.txt is lines of the log of main's passing run 42830956936, nine lines that
// logged ABSENCE checks, of which GitHub annotated six as errors.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"regexp/syntax"
	"strings"
	"testing"
)

// oldRegexp is the pattern main carried until this issue's fix (setup-go's, kept by #335).
const oldRegexp = `^\s*(.+\.go):(?:(\d+):(\d+):)? (.*)`

type issue struct{ file, line, column, message string }

type matcher struct {
	pattern                     string
	re                          *regexp.Regexp
	file, line, column, message int
}

// match is the runner's step: an unanchored search of one line, then the numbered groups.
func (m matcher) match(line string) (issue, bool) {
	sub := m.re.FindStringSubmatch(line)
	if sub == nil {
		return issue{}, false
	}
	return issue{sub[m.file], sub[m.line], sub[m.column], sub[m.message]}, true
}

func newMatcher(t *testing.T, pattern string, file, line, column, message int) matcher {
	t.Helper()
	re, err := regexp.Compile(pattern)
	if err != nil {
		t.Fatalf("the regexp %q does not compile: %v", pattern, err)
	}
	return matcher{pattern, re, file, line, column, message}
}

// committedMatcher reads the file CI registers and checks what the runner's own validation checks:
// one pattern, a message group, and every group number inside the pattern's groups.
func committedMatcher(t *testing.T) matcher {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", ".github", "go-problem-matcher.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		ProblemMatcher []struct {
			Owner   string `json:"owner"`
			Pattern []struct {
				Regexp  string `json:"regexp"`
				File    *int   `json:"file"`
				Line    *int   `json:"line"`
				Column  *int   `json:"column"`
				Message *int   `json:"message"`
			} `json:"pattern"`
		} `json:"problemMatcher"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("the matcher is not JSON: %v", err)
	}
	if len(doc.ProblemMatcher) != 1 || doc.ProblemMatcher[0].Owner != "go" || len(doc.ProblemMatcher[0].Pattern) != 1 {
		t.Fatalf("want one matcher owned by go with one pattern, got %+v", doc)
	}
	p := doc.ProblemMatcher[0].Pattern[0]
	if p.File == nil || p.Line == nil || p.Column == nil || p.Message == nil {
		t.Fatalf("the pattern must number its file, line, column and message groups: %+v", p)
	}
	m := newMatcher(t, p.Regexp, *p.File, *p.Line, *p.Column, *p.Message)
	// Fatal, as the runner's own validation is: a group number past the pattern's groups is a config it
	// refuses to load, and match would index past the end of the submatches.
	if m.file != 1 || m.line != 2 || m.column != 3 || m.message != 4 || m.re.NumSubexp() != 4 {
		t.Fatalf("want groups file=1 line=2 column=3 message=4 of 4 groups, got %d %d %d %d of %d",
			m.file, m.line, m.column, m.message, m.re.NumSubexp())
	}
	return m
}

func oldMatcher(t *testing.T) matcher { return newMatcher(t, oldRegexp, 1, 2, 3, 4) }

func fixtureLines(t *testing.T, name string) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
}

// A diagnostic is a line of a compiler or vet error that CI must annotate, with the annotation it
// must make, typed from the line.
type diagnostic struct {
	text                    string
	file, line, column, msg string
}

// The lines of testdata/go-tool-output.txt that are errors, and nothing else in that file is.
var toolDiagnostics = []diagnostic{
	{`compile/a.go:3:8: "os" imported and not used`, "compile/a.go", "3", "8", `"os" imported and not used`},
	{`compile/a.go:6:2: declared and not used: unused`, "compile/a.go", "6", "2", "declared and not used: unused"},
	{`compile/a.go:7:9: undefined: Foo`, "compile/a.go", "7", "9", "undefined: Foo"},
	{`./a.go:3:8: "os" imported and not used`, "./a.go", "3", "8", `"os" imported and not used`},
	{`./a.go:6:2: declared and not used: unused`, "./a.go", "6", "2", "declared and not used: unused"},
	{`./a.go:7:9: undefined: Foo`, "./a.go", "7", "9", "undefined: Foo"},
	{`syntax/b.go:3:9: syntax error: unexpected {, expected )`, "syntax/b.go", "3", "9", "syntax error: unexpected {, expected )"},
	{`vetp/c.go:10:8: assignment copies lock value to cp: sync.Mutex`, "vetp/c.go", "10", "8", "assignment copies lock value to cp: sync.Mutex"},
	{`vetp/c.go:11:6: assignment copies lock value to _: sync.Mutex`, "vetp/c.go", "11", "6", "assignment copies lock value to _: sync.Mutex"},
	{`vetp/c.go:12:25: fmt.Sprintf format %s reads arg #2, but call has 1 arg`, "vetp/c.go", "12", "25", "fmt.Sprintf format %s reads arg #2, but call has 1 arg"},
	{`tbuild/d_test.go:6:12: undefined: Missing`, "tbuild/d_test.go", "6", "12", "undefined: Missing"},
	// go vet prints a type-checking error with a "vet: " prefix. The old pattern took the prefix into
	// the file name; the file is the path after it.
	{`vet: tbuild/d_test.go:6:12: undefined: Missing`, "tbuild/d_test.go", "6", "12", "undefined: Missing"},
	{`vet: compile/a.go:7:9: undefined: Foo`, "compile/a.go", "7", "9", "undefined: Foo"},
	// A package-loading error names the file and no line or column, and go build, go vet and go test all
	// print it (found in the review of PR #345). The old pattern annotated these, so the line and
	// column stay optional. What keeps such a line apart from a log line is that it begins with the
	// file name: an ABSENCE log line begins with the test's own file and line.
	{`x.go: multiple //go:build comments`, "x.go", "", "", "multiple //go:build comments"},
	{`y.go: parsing //go:build line: unexpected end of expression`, "y.go", "", "", "parsing //go:build line: unexpected end of expression"},
	{`mixbuild/z.go:2:1: +build lines do not match //go:build condition`, "mixbuild/z.go", "2", "1", "+build lines do not match //go:build condition"},
}

// Lines of the same shape that no fixture happens to hold: the forms the issue and the compiler's
// documentation give, a test file, and an indented line.
var shapeDiagnostics = []diagnostic{
	{`./internal/x/y.go:12:3: undefined: Foo`, "./internal/x/y.go", "12", "3", "undefined: Foo"},
	{`internal/x/y.go:12:3: printf: non-constant format string in call to fmt.Printf`, "internal/x/y.go", "12", "3", "printf: non-constant format string in call to fmt.Printf"},
	{`y_test.go:45:2: undefined: z`, "y_test.go", "45", "2", "undefined: z"},
	{`vet: internal/x/y_test.go:12:3: undefined: Foo`, "internal/x/y_test.go", "12", "3", "undefined: Foo"},
	{`    internal/x/y.go:12:3: undefined: Foo`, "internal/x/y.go", "12", "3", "undefined: Foo"},
}

// Lines that are not errors but mention a Go file: a test's log line, which has a line number and no
// column; the ABSENCE line of the issue and a shorter one of the same shape; a log line with a file
// and a message in its text; a diagnostic quoted inside a log line; and the stack frame of a panic.
var notDiagnostics = []string{
	`    file_test.go:110: message`,
	`    hostdialed_test.go:110: ABSENCE not raised (first read after the controller restart): hpcburst_events_raised_total{...}: not raised, tunnel_state.go: readOnce, then reportOrphans`,
	`    x_test.go:9: see tunnel_state.go: readOnce`,
	`    x_test.go:9: x.go: multiple //go:build comments`,
	`    x_test.go:5: y.go:12:3: undefined: Foo`,
	`see hack/x.go:12:3: undefined: Foo for the error`,
	"\t/home/runner/work/repo/repo/panics/p_test.go:6 +0x25",
	`--- FAIL: TestPanics (0.00s)`,
	`# example.com/fx/compile`,
	``,
}

func TestProblemMatcherAnnotatesTheErrorsOfTheGoTools(t *testing.T) {
	m := committedMatcher(t)
	want := map[string]diagnostic{}
	for _, d := range toolDiagnostics {
		want[d.text] = d
	}
	seen := map[string]bool{}
	for i, line := range fixtureLines(t, "go-tool-output.txt") {
		got, ok := m.match(line)
		d, isDiag := want[line]
		switch {
		case isDiag && !ok:
			t.Errorf("go-tool-output.txt:%d: %q is an error and is not annotated", i+1, line)
		case isDiag && got != (issue{d.file, d.line, d.column, d.msg}):
			t.Errorf("go-tool-output.txt:%d: %q is annotated as %+v, want file %q line %s column %s message %q", i+1, line, got, d.file, d.line, d.column, d.msg)
		case !isDiag && ok:
			t.Errorf("go-tool-output.txt:%d: %q is not an error and is annotated as %+v", i+1, line, got)
		}
		seen[line] = true
	}
	for _, d := range toolDiagnostics {
		if !seen[d.text] {
			t.Errorf("go-tool-output.txt holds no line %q, so the table lists a line the fixture lacks", d.text)
		}
	}
}

func TestProblemMatcherAnnotatesTheShapesOfCompilerAndVetErrors(t *testing.T) {
	m := committedMatcher(t)
	for _, d := range shapeDiagnostics {
		got, ok := m.match(d.text)
		if !ok || got != (issue{d.file, d.line, d.column, d.msg}) {
			t.Errorf("%q is annotated as %+v (matched %v), want file %q line %s column %s message %q", d.text, got, ok, d.file, d.line, d.column, d.msg)
		}
	}
}

func TestProblemMatcherDoesNotAnnotateLogLines(t *testing.T) {
	m := committedMatcher(t)
	lines := append([]string{}, notDiagnostics...)
	// The nine real lines of main's passing run, six of which GitHub annotated.
	lines = append(lines, fixtureLines(t, "ci-log-main-42830956936.txt")...)
	for _, line := range lines {
		if got, ok := m.match(line); ok {
			t.Errorf("%q is annotated as %+v, and is a log line", line, got)
		}
	}
}

// Every error the old pattern annotated, the new one still annotates, with the same line, column and
// message. The file is the same too, except for the "vet: " lines, where the old pattern's file name
// began with the prefix. The old pattern's reading is checked against the table first, so that a
// table line the old pattern never matched cannot pass for a regression test.
func TestProblemMatcherKeepsEveryErrorTheOldOneAnnotated(t *testing.T) {
	m, old := committedMatcher(t), oldMatcher(t)
	for _, d := range append(append([]diagnostic{}, toolDiagnostics...), shapeDiagnostics...) {
		o, ok := old.match(d.text)
		if !ok {
			t.Errorf("the old pattern does not annotate %q, so it says nothing about a regression", d.text)
			continue
		}
		if o.line != d.line || o.column != d.column || o.message != d.msg || (o.file != d.file && o.file != "vet: "+d.file) {
			t.Errorf("the old pattern reads %q as %+v, not what the table says", d.text, o)
		}
		if n, ok := m.match(d.text); !ok || n.line != o.line || n.column != o.column || n.message != o.message {
			t.Errorf("the old pattern annotated %q as %+v; the new one gives %+v (matched %v)", d.text, o, n, ok)
		}
	}
}

// The old pattern's mistake, on the go command's own output: of the lines in go-tool-output.txt it
// annotates every error and one line that is not one, the t.Log line that cites tunnel_state.go.
func TestTheOldMatcherAnnotatesALogLineThatCitesAFile(t *testing.T) {
	old := oldMatcher(t)
	isDiag := map[string]bool{}
	for _, d := range toolDiagnostics {
		isDiag[d.text] = true
	}
	var wrong []string
	for _, line := range fixtureLines(t, "go-tool-output.txt") {
		if _, ok := old.match(line); ok && !isDiag[line] {
			wrong = append(wrong, line)
		}
	}
	if len(wrong) != 1 || !strings.HasPrefix(strings.TrimSpace(wrong[0]), "l_test.go:6: ABSENCE not raised") {
		t.Errorf("want the old pattern to annotate exactly the ABSENCE log line, got %q", wrong)
	}
}

// GitHub's annotations for run 42830956936 (check-run annotations API, 10 Nov 2026) are six errors,
// each with a message that begins where the old pattern's message group does, after "tunnel_state.go: ".
// The run's other ABSENCE lines, which do not cite a file in their text, have no annotation. This
// applies the old pattern to those nine lines and expects exactly that.
func TestTheOldMatcherReproducesTheAnnotationsGitHubRecorded(t *testing.T) {
	old := oldMatcher(t)
	const recorded = "readOnce, then reportOrphans), which raises it for any proxy frps lists online with no current Instance"
	annotated := 0
	for _, line := range fixtureLines(t, "ci-log-main-42830956936.txt") {
		got, ok := old.match(line)
		cites := strings.Contains(line, "tunnel_state.go: ")
		if ok != cites {
			t.Errorf("%q: the old pattern matched %v, and GitHub annotated %v", line, ok, cites)
		}
		if ok {
			annotated++
			if !strings.HasPrefix(got.message, recorded) || !strings.HasSuffix(got.file, "tunnel_state.go") || got.line != "" {
				t.Errorf("%q: the old pattern reads file %q line %q message %q, which is not GitHub's annotation", line, got.file, got.line, got.message)
			}
		}
	}
	if annotated != 6 {
		t.Errorf("want the six lines GitHub annotated, got %d", annotated)
	}
}

// Go's regexp stands for the runner's engine only while the pattern uses what both read the same way.
func TestProblemMatcherUsesOnlyWhatBothEnginesShare(t *testing.T) {
	m := committedMatcher(t)
	if !strings.HasPrefix(m.pattern, "^") {
		t.Errorf("the pattern %q is not anchored: the runner searches the whole line, so it would match mid-text", m.pattern)
	}
	for _, bad := range []string{`\p`, `\P`, `\Q`, `\A`, `\z`, `\Z`, `\b`, `\B`, `\C`, `(?i`, `(?s`, `(?m`, `(?U`} {
		if strings.Contains(m.pattern, bad) {
			t.Errorf("the pattern %q uses %q, which the two engines read differently", m.pattern, bad)
		}
	}
	tree, err := syntax.Parse(m.pattern, syntax.Perl)
	if err != nil {
		t.Fatal(err)
	}
	var walk func(*syntax.Regexp)
	walk = func(r *syntax.Regexp) {
		switch r.Op {
		case syntax.OpLiteral, syntax.OpCharClass, syntax.OpAnyCharNotNL, syntax.OpBeginText, syntax.OpCapture,
			syntax.OpConcat, syntax.OpAlternate, syntax.OpStar, syntax.OpPlus, syntax.OpQuest, syntax.OpRepeat:
		default:
			t.Errorf("the pattern %q uses %v, which the runner's engine may read differently", m.pattern, r.Op)
		}
		if r.Flags&syntax.FoldCase != 0 {
			t.Errorf("the pattern %q is case-insensitive, and the runner's is not", m.pattern)
		}
		for _, sub := range r.Sub {
			walk(sub)
		}
	}
	walk(tree)
}
