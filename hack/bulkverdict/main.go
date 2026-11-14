// Command bulkverdict is CI's rehearsal-bulk job's verdict on TestBulk64MiBReplyBesideEightStreams (internal/rehearsal/scenario,
// bulk_test.go), read from the test's verbose output in the log hack/rehearsal.sh wrote. The test judges each mode, direct and host-dialed, by
// S4 as revised (judgeS4), and fails a mode whose control is INVALID; the job maps those outcomes as the science agent ruled on #563, (a) with
// the both-INVALID failure. The test is not changed: only how the job reads it.
//
// The job fails on:
//  1. an S4 FAIL in either mode;
//  2. any functional witness failing in either mode, whatever its control: the control's or the bulk path's run failed (its 64 MiB on the
//     link, the bulk counter rising by one, no fallback, no cut stream are its checks), or did not run;
//  3. a bulk-path gap p99 over 75 ms, even beside an INVALID control;
//  4. both modes INVALID.
//
// One mode INVALID, with everything else clean, warns with its figures and the link's rate, and passes. Anything the test's output does not
// explain fails too: a test that skipped (#558's quiet pass), a mode with no verdict line, an error the parent test reported itself, a test
// that failed while both its modes passed, a panic or timeout, processes hack/rehearsal.sh found left, or a test that never ran (a build that
// failed, a selection that matched nothing). Each mode's verdict goes to the job's step summary too ($GITHUB_STEP_SUMMARY, when set).
//
//	go run ./hack/bulkverdict <log>
//
// Exit 0 for a pass (with warnings), 1 for a failure, 2 for a usage error. It prints GitHub annotations (::error::, ::warning::).
package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// What the test writes, as bulk_test.go writes it: TestTheMapperReadsWhatTheTestWrites holds each to the test's source.
const (
	testName     = "TestBulk64MiBReplyBesideEightStreams"
	controlRun   = "control without the bulk path"
	bulkRun      = "with the bulk path"
	invalidLine  = "INVALID run, neither a pass nor a fail: "
	ratesFormat  = "(%.1f and %.1f MB/s through the link)" // the end of the mode's figures line, bulk_test.go's t.Logf
	s4FailLine   = "S4 fails: "
	s4PassLine   = "S4 passes: "
	bulkPassMax  = 75 * time.Millisecond // S4's pass bar for the bulk path's gap p99 (s4PassMaxP99)
	leftoverLine = "rehearsal.sh: processes left"
)

// modes are the test's two modes, in its order.
var modes = []string{"direct", string(agentapi.DialHomeHostDialed)}

var (
	marker  = regexp.MustCompile(`^=== (?:RUN|NAME|CONT)\s+(\S+)`)
	result  = regexp.MustCompile(`^\s*--- (PASS|FAIL|SKIP): (\S+) \(`)
	message = regexp.MustCompile(`^\s{4,}\S+\.go:\d+: (.*)$`)
	// figures is judgeS4's "gap p99 <control> without the bulk path, <bulk> with it".
	figures = regexp.MustCompile(`gap p99 (\S+) without the bulk path, (\S+) with it`)
	// rates is the end of the mode's figures line: the link's rate without and with the bulk path.
	rates = regexp.MustCompile(`\(([0-9.]+) and ([0-9.]+) MB/s through the link\)$`)
)

// log is what the output says: each test's result, each test's own messages, and what went wrong outside any test.
type log struct {
	results  map[string]string
	messages map[string][]string
	leftover bool
	crashed  bool
}

func read(r io.Reader) (log, error) {
	l := log{results: map[string]string{}, messages: map[string][]string{}}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	current := ""
	for sc.Scan() {
		line := sc.Text()
		switch {
		case marker.MatchString(line):
			current = marker.FindStringSubmatch(line)[1]
		case result.MatchString(line):
			m := result.FindStringSubmatch(line)
			l.results[m[2]] = m[1]
		case message.MatchString(line) && current != "":
			l.messages[current] = append(l.messages[current], message.FindStringSubmatch(line)[1])
		case strings.HasPrefix(line, leftoverLine):
			l.leftover = true
		case strings.HasPrefix(line, "panic:"), strings.Contains(line, "test timed out after"):
			l.crashed = true
		}
	}
	return l, sc.Err()
}

// subtest is a subtest's name as go test prints it: spaces become underscores.
func subtest(parts ...string) string {
	return strings.ReplaceAll(strings.Join(parts, "/"), " ", "_")
}

// verdict is the job's: the failures, the warnings, and what each mode came to.
type verdict struct {
	failures, warnings, modes []string
	rows                      []row
}

// row is one mode's line of the step summary: its verdict, its figures when the test judged it, and the link's rate.
type row struct {
	mode, verdict, control, bulk, gap, rate string
}

func (v *verdict) fail(format string, a ...any) {
	v.failures = append(v.failures, fmt.Sprintf(format, a...))
}
func (v *verdict) warn(format string, a ...any) {
	v.warnings = append(v.warnings, fmt.Sprintf(format, a...))
}

// judge maps the test's outcomes to the job's verdict (#563's ruling).
func judge(l log) verdict {
	var v verdict
	if l.crashed {
		v.fail("the test binary panicked or timed out")
	}
	if l.leftover {
		v.fail("hack/rehearsal.sh found processes left after the test")
	}
	switch l.results[testName] {
	case "":
		v.fail("%s did not run: a build failed, or the selection matched nothing", testName)
		return v
	case "SKIP":
		// The quiet pass #558 is there to stop: go test exits 0 for a skip.
		v.fail("%s skipped, so the bulk path was not tested: %s", testName, strings.Join(l.messages[testName], " | "))
		v.rows = append(v.rows, row{mode: "all", verdict: "SKIPPED: the job fails"})
		return v
	}
	if own := l.messages[testName]; len(own) > 0 {
		v.fail("%s reported %d message(s) of its own, outside any mode: %s", testName, len(own), strings.Join(own, " | "))
	}
	invalid := 0
	allPassed := true // every mode's subtest passed
	for _, m := range modes {
		mode := subtest(testName, m)
		if l.results[mode] != "PASS" {
			allPassed = false
		}
		ctl, on := l.results[subtest(testName, m, controlRun)], l.results[subtest(testName, m, bulkRun)]
		if ctl != "PASS" || on != "PASS" {
			v.fail("%s: a functional witness failed, or a run did not happen: the control without the bulk path %s, the run with it %s",
				m, orNotRun(ctl), orNotRun(on))
			v.modes = append(v.modes, m+": FAIL (a witness)")
			v.rows = append(v.rows, row{mode: m, verdict: "FAIL: a functional witness"})
			continue
		}
		var lines []string
		for _, msg := range l.messages[mode] {
			if strings.HasPrefix(msg, invalidLine) || strings.HasPrefix(msg, s4FailLine) || strings.HasPrefix(msg, s4PassLine) {
				lines = append(lines, msg)
			}
		}
		if len(lines) != 1 {
			v.fail("%s: %d S4 verdict lines, want exactly 1 (the test stopped before judging, or judged twice)", m, len(lines))
			v.modes = append(v.modes, m+": FAIL (no verdict)")
			v.rows = append(v.rows, row{mode: m, verdict: "FAIL: no S4 verdict"})
			continue
		}
		line := lines[0]
		r := figuresRow(m, line, l.messages[mode])
		switch {
		case strings.HasPrefix(line, s4FailLine):
			v.fail("%s: %s", m, line)
			v.modes = append(v.modes, m+": FAIL (S4)")
			r.verdict = "FAIL: S4"
		case strings.HasPrefix(line, s4PassLine):
			if l.results[mode] != "PASS" {
				v.fail("%s: S4 passed, but the mode's subtest %s", m, orNotRun(l.results[mode]))
			}
			v.modes = append(v.modes, m+": PASS")
			r.verdict = "PASS"
		default:
			invalid++
			bulk, ok := bulkP99(line)
			switch {
			case !ok:
				v.fail("%s: the INVALID line's bulk p99 cannot be read: %s", m, line)
				r.verdict = "FAIL: INVALID, unreadable"
			case bulk > bulkPassMax:
				v.fail("%s: INVALID, and the bulk path's gap p99 %s is over %s: %s", m, bulk, bulkPassMax, line)
				r.verdict = "FAIL: INVALID, bulk over " + bulkPassMax.String()
			default:
				v.warn("%s: %s; the link's rate %s", m, line, r.rate)
				r.verdict = "INVALID: warns"
			}
			v.modes = append(v.modes, m+": INVALID")
		}
		v.rows = append(v.rows, r)
	}
	if invalid == len(modes) {
		v.fail("both modes are INVALID: no mode shows a stall for the bulk path to remove")
	}
	// The belt: a test that failed while both its modes passed failed for a reason none of the above reads (a cleanup of the parent's own
	// that fails it without a message, say); never a pass (#563's delta review).
	if l.results[testName] == "FAIL" && allPassed && len(v.failures) == 0 {
		v.fail("%s failed while both its modes passed, for a reason the mapper cannot read", testName)
	}
	return v
}

// figuresRow is a judged mode's summary row: control and bulk p99 and their gap from the S4 line, the link's rate from the mode's figures line.
func figuresRow(m, line string, msgs []string) row {
	r := row{mode: m, rate: "not read"}
	if f := figures.FindStringSubmatch(line); f != nil {
		r.control, r.bulk = f[1], f[2]
		c, errC := time.ParseDuration(f[1])
		b, errB := time.ParseDuration(f[2])
		if errC == nil && errB == nil {
			r.gap = (c - b).Round(10 * time.Microsecond).String()
		}
	}
	for _, msg := range msgs {
		if strings.HasPrefix(msg, m+": gap p99 without the bulk path ") {
			if x := rates.FindStringSubmatch(msg); x != nil {
				r.rate = x[1] + " MB/s without the bulk path, " + x[2] + " MB/s with it"
			}
		}
	}
	return r
}

// summary is the step summary's markdown: each mode's verdict, figures and the link's rate, and the job's verdict.
func summary(v verdict) string {
	var b strings.Builder
	b.WriteString("### rehearsal-bulk: TestBulk64MiBReplyBesideEightStreams, read as ruled on #563\n\n")
	b.WriteString("| mode | verdict | control p99 | bulk p99 | apart | the link's rate |\n|---|---|---|---|---|---|\n")
	for _, r := range v.rows {
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s |\n", r.mode, r.verdict, dash(r.control), dash(r.bulk), dash(r.gap), dash(r.rate))
	}
	if len(v.rows) == 0 {
		b.WriteString("| - | the test did not run | - | - | - | - |\n")
	}
	if len(v.failures) > 0 {
		fmt.Fprintf(&b, "\n**The job fails:**\n")
		for _, e := range v.failures {
			fmt.Fprintf(&b, "- %s\n", e)
		}
	} else {
		b.WriteString("\n**The job passes.**\n")
	}
	return b.String()
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func orNotRun(r string) string {
	if r == "" {
		return "did not run"
	}
	return r
}

// bulkP99 is the bulk path's gap p99 on an S4 line.
func bulkP99(line string) (time.Duration, bool) {
	m := figures.FindStringSubmatch(line)
	if m == nil {
		return 0, false
	}
	d, err := time.ParseDuration(m[2])
	return d, err == nil
}

// appendFile appends text to the file at path, creating it.
func appendFile(path, text string) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(text); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: bulkverdict <log of hack/rehearsal.sh running TestBulk64MiBReplyBesideEightStreams>")
		os.Exit(2)
	}
	f, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, "bulkverdict:", err)
		os.Exit(2)
	}
	defer f.Close()
	l, err := read(f)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bulkverdict:", err)
		os.Exit(2)
	}
	v := judge(l)
	if p := os.Getenv("GITHUB_STEP_SUMMARY"); p != "" {
		if err := appendFile(p, summary(v)); err != nil {
			// The summary is a view of the verdict, not the verdict: the job's exit stays the mapping's.
			fmt.Printf("::warning::the step summary could not be written: %v\n", err)
		}
	}
	fmt.Printf("bulkverdict: %s\n", strings.Join(v.modes, "; "))
	for _, w := range v.warnings {
		fmt.Printf("::warning::%s (an INVALID mode warns; the science agent's ruling on #563)\n", w)
	}
	for _, e := range v.failures {
		fmt.Printf("::error::%s\n", e)
	}
	if len(v.failures) > 0 {
		os.Exit(1)
	}
	fmt.Println("bulkverdict: the job passes")
}
