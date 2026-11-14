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
// One mode INVALID, with everything else clean, warns with its figures and passes. Anything the test's output does not explain fails too: a
// mode with no verdict line, an error the parent test reported itself, a panic or timeout, processes hack/rehearsal.sh found left, or a test
// that never ran (a build that failed, a selection that matched nothing).
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
	if _, ran := l.results[testName]; !ran {
		v.fail("%s did not run: a build failed, or the selection matched nothing", testName)
		return v
	}
	if own := l.messages[testName]; len(own) > 0 {
		v.fail("%s reported %d message(s) of its own, outside any mode: %s", testName, len(own), strings.Join(own, " | "))
	}
	invalid := 0
	for _, m := range modes {
		mode := subtest(testName, m)
		ctl, on := l.results[subtest(testName, m, controlRun)], l.results[subtest(testName, m, bulkRun)]
		if ctl != "PASS" || on != "PASS" {
			v.fail("%s: a functional witness failed, or a run did not happen: the control without the bulk path %s, the run with it %s",
				m, orNotRun(ctl), orNotRun(on))
			v.modes = append(v.modes, m+": FAIL (a witness)")
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
			continue
		}
		line := lines[0]
		switch {
		case strings.HasPrefix(line, s4FailLine):
			v.fail("%s: %s", m, line)
			v.modes = append(v.modes, m+": FAIL (S4)")
		case strings.HasPrefix(line, s4PassLine):
			if l.results[mode] != "PASS" {
				v.fail("%s: S4 passed, but the mode's subtest %s", m, orNotRun(l.results[mode]))
			}
			v.modes = append(v.modes, m+": PASS")
		default:
			invalid++
			bulk, ok := bulkP99(line)
			switch {
			case !ok:
				v.fail("%s: the INVALID line's bulk p99 cannot be read: %s", m, line)
			case bulk > bulkPassMax:
				v.fail("%s: INVALID, and the bulk path's gap p99 %s is over %s: %s", m, bulk, bulkPassMax, line)
			default:
				v.warn("%s: %s", m, line)
			}
			v.modes = append(v.modes, m+": INVALID")
		}
	}
	if invalid == len(modes) {
		v.fail("both modes are INVALID: no mode shows a stall for the bulk path to remove")
	}
	return v
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
