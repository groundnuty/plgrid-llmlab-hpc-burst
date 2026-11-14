package main

// The job's mapping of the bulk test's outcomes (#563's ruling, (a) with the both-INVALID failure), checked against logs of the test's own shape:
// testdata/one-invalid.log is one as hack/rehearsal.sh writes it (a hosted run's structure, with invented figures), and mode builds the others,
// line for line in the same form. Each of the ruling's four failures, and its one warning, has a case; so does each way the output can fail to
// explain itself.

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// modeOut is one mode as the test prints it: each run's subtest result (and a witness's message when it failed), and the mode's S4 line ("" for
// none, as when a run failed and the mode stopped).
type modeOut struct {
	control, bulk string // PASS, FAIL or "" for a run that never started
	witness       string // the failed run's message
	verdict       string // the mode's S4 line
}

func invalidAt(control, bulk string) string {
	return invalidLine + "gap p99 " + control + " without the bulk path, " + bulk + " with it, 30ms apart: the control is under 85ms, so this run can't show a stall to remove"
}

func passAt(control, bulk string) string {
	return s4PassLine + "gap p99 " + control + " without the bulk path, " + bulk + " with it, 45ms apart"
}

// vlog is the verbose output of the test with the two modes' outcomes, after hack/rehearsal.sh's own lines.
func vlog(direct, hostDialed modeOut, extra ...string) string {
	var b strings.Builder
	b.WriteString("hack/rehearsal.sh: building the four binaries and the rehearsal test\nthe test runs in hpcburst-rehearsal-1234.scope\n")
	b.WriteString("=== RUN   " + testName + "\n")
	results := []string{}
	parentFail := false
	for i, o := range []modeOut{direct, hostDialed} {
		m := modes[i]
		b.WriteString("=== RUN   " + subtest(testName, m) + "\n")
		modeFail := false
		for _, r := range []struct{ name, res string }{{controlRun, o.control}, {bulkRun, o.bulk}} {
			if r.res == "" {
				continue
			}
			b.WriteString("=== RUN   " + subtest(testName, m, r.name) + "\n")
			b.WriteString("    bulk_test.go:222: " + m + ": gap p99 60ms over 300 gaps in the reply's window\n")
			if r.res == "FAIL" {
				b.WriteString("    bulk_test.go:190: " + o.witness + "\n")
				modeFail = true
			}
		}
		if o.verdict != "" {
			b.WriteString("=== NAME  " + subtest(testName, m) + "\n")
			b.WriteString("    bulk_test.go:101: " + m + ": gap p99 without the bulk path 90ms (300 gaps), with it 55ms (300 gaps); the reply took 2s and 2s (32.0 and 32.5 MB/s through the link)\n")
			b.WriteString("    bulk_test.go:106: " + o.verdict + "\n")
			if !strings.HasPrefix(o.verdict, s4PassLine) {
				modeFail = true
			}
		}
		res := "PASS"
		if modeFail {
			res, parentFail = "FAIL", true
		}
		results = append(results, "    --- "+res+": "+subtest(testName, m)+" (230.00s)")
		for _, r := range []struct{ name, res string }{{controlRun, o.control}, {bulkRun, o.bulk}} {
			if r.res != "" {
				results = append(results, "        --- "+r.res+": "+subtest(testName, m, r.name)+" (115.00s)")
			}
		}
	}
	for _, e := range extra {
		b.WriteString(e + "\n")
	}
	top := "PASS"
	if parentFail {
		top = "FAIL"
	}
	b.WriteString("--- " + top + ": " + testName + " (470.00s)\n" + strings.Join(results, "\n") + "\n" + top + "\n")
	return b.String()
}

func judgeText(t *testing.T, text string) verdict {
	t.Helper()
	l, err := read(strings.NewReader(text))
	if err != nil {
		t.Fatal(err)
	}
	return judge(l)
}

func TestTheJobMapsTheBulkTestsOutcomesAsRuled(t *testing.T) {
	clean := modeOut{control: "PASS", bulk: "PASS", verdict: passAt("101.5ms", "56.5ms")}
	for _, c := range []struct {
		name               string
		log                string
		fails              bool
		failure, warning   string // substrings of a failure and of a warning, "" for none expected
		warnings, failures int
	}{
		{"both modes pass: the job passes, no warning",
			vlog(clean, clean), false, "", "", 0, 0},
		{"one mode INVALID, everything else clean: a warning with its figures, and the job passes",
			vlog(modeOut{control: "PASS", bulk: "PASS", verdict: invalidAt("81.5ms", "51.5ms")}, clean), false, "", "direct: INVALID run", 1, 0},
		// The ruling's four failures.
		{"1. an S4 FAIL",
			vlog(clean, modeOut{control: "PASS", bulk: "PASS", verdict: s4FailLine + "gap p99 100ms without the bulk path, 80ms with it, 20ms apart: less than 30ms apart"}),
			true, "host-dialed: S4 fails:", "", 0, 1},
		{"2. a functional witness failing, beside an INVALID control's mode: the bulk counter",
			vlog(modeOut{control: "PASS", bulk: "PASS", verdict: invalidAt("81ms", "50ms")},
				modeOut{control: "PASS", bulk: "FAIL", witness: "the front's bulk counter rose by 0 across the reply, want exactly 1"}),
			true, "host-dialed: a functional witness failed", "direct: INVALID run", 1, 1},
		{"2. a functional witness failing in the control: 64 MiB not on the link",
			vlog(modeOut{control: "FAIL", witness: "the link carried 1048576 bytes across the reply, below its 67108864"}, clean),
			true, "direct: a functional witness failed, or a run did not happen: the control without the bulk path FAIL, the run with it did not run", "", 0, 1},
		{"3. a bulk p99 over 75 ms beside an INVALID control",
			vlog(modeOut{control: "PASS", bulk: "PASS", verdict: invalidAt("84ms", "76ms")}, clean),
			true, "direct: INVALID, and the bulk path's gap p99 76ms is over 75ms", "", 0, 1},
		{"3's boundary: a bulk p99 of exactly 75 ms beside an INVALID control is not over the bar, and warns",
			vlog(modeOut{control: "PASS", bulk: "PASS", verdict: invalidAt("84ms", "75ms")}, clean), false, "", "direct: INVALID run", 1, 0},
		{"4. both modes INVALID",
			vlog(modeOut{control: "PASS", bulk: "PASS", verdict: invalidAt("80ms", "50ms")}, modeOut{control: "PASS", bulk: "PASS", verdict: invalidAt("82ms", "55ms")}),
			true, "both modes are INVALID", "INVALID run", 2, 1},
		// What the output does not explain.
		{"a mode that stopped before its verdict (a run measured nothing)",
			vlog(clean, modeOut{control: "PASS", bulk: "PASS"}, "    bulk_test.go:98: a run measured nothing (0 and 300 gaps): nothing to judge"),
			true, "host-dialed: 0 S4 verdict lines", "", 0, 1},
		{"an error the parent test reported itself, beside two clean modes (a cleanup of its own)",
			vlog(clean, clean, "=== NAME  "+testName, "    harness_test.go:80: a process of the test was left after it: frps"),
			true, "reported 1 message(s) of its own", "", 0, 1},
		{"the belt: the test failed while both its modes passed, with no message (a parent cleanup that fails it)",
			strings.Replace(strings.Replace(vlog(clean, clean), "--- PASS: "+testName+" (", "--- FAIL: "+testName+" (", 1), "\nPASS\n", "\nFAIL\n", 1),
			true, testName + " failed while both its modes passed", "", 0, 1},
		{"processes left after the test",
			vlog(clean, clean, "rehearsal.sh: processes left in hpcburst-rehearsal-1234.scope after the test returned (exit 0):"),
			true, "processes left", "", 0, 1},
		{"a panic", vlog(clean, clean, "panic: test timed out after 30m0s"), true, "panicked or timed out", "", 0, 1},
		{"the test skipped (no HPCBURST_REHEARSAL=1): named as a skip, which go test passes",
			"hack/rehearsal.sh: building the four binaries and the rehearsal test\n=== RUN   " + testName + "\n" +
				"    harness_test.go:61: the one-host rehearsal runs the real stack, and only with HPCBURST_REHEARSAL=1 (hack/rehearsal.sh)\n" +
				"--- SKIP: " + testName + " (0.00s)\nPASS\n",
			true, testName + " skipped, so the bulk path was not tested: the one-host rehearsal runs the real stack", "", 0, 1},
		{"the test never ran", "hack/rehearsal.sh: building the four binaries and the rehearsal test\ntesting: warning: no tests to run\nPASS\n",
			true, "did not run", "", 0, 1},
		{"a build that failed", "hack/rehearsal.sh: building the four binaries and the rehearsal test\n# github.com/x\n./a.go:1: syntax error\n",
			true, "did not run", "", 0, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			v := judgeText(t, c.log)
			if (len(v.failures) > 0) != c.fails || len(v.failures) != c.failures || len(v.warnings) != c.warnings {
				t.Fatalf("failures %q, warnings %q; want %d failure(s) and %d warning(s)", v.failures, v.warnings, c.failures, c.warnings)
			}
			if c.failure != "" && !slices.ContainsFunc(v.failures, func(f string) bool { return strings.Contains(f, c.failure) }) {
				t.Errorf("failures %q lack %q", v.failures, c.failure)
			}
			if c.warning != "" && !slices.ContainsFunc(v.warnings, func(w string) bool { return strings.Contains(w, c.warning) }) {
				t.Errorf("warnings %q lack %q", v.warnings, c.warning)
			}
		})
	}
}

// A log as hack/rehearsal.sh writes it, of a hosted run's shape: one mode INVALID with a bulk p99 under the bar, the other a pass. It warns and
// passes, the warning naming the mode and its figures.
func TestAHostedRunsLogWithOneInvalidModeWarnsAndPasses(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("testdata", "one-invalid.log"))
	if err != nil {
		t.Fatal(err)
	}
	v := judgeText(t, string(b))
	if len(v.failures) != 0 || len(v.warnings) != 1 || !strings.Contains(v.warnings[0], "direct: INVALID run, neither a pass nor a fail: gap p99 81.5ms without the bulk path, 51.5ms with it") {
		t.Fatalf("failures %q, warnings %q; want one warning for direct's INVALID run and no failure", v.failures, v.warnings)
	}
	if !slices.Equal(v.modes, []string{"direct: INVALID", "host-dialed: PASS"}) {
		t.Errorf("modes %q", v.modes)
	}
}

// The mapper reads what the test writes: each message, subtest name, mode and bar it keys on is in bulk_test.go as it is written there, and the
// host-dialed mode's name is agentapi's. A change to the test that the mapper would no longer read fails here, before a CI run reads it wrong.
func TestTheMapperReadsWhatTheTestWrites(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "internal", "rehearsal", "scenario", "bulk_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	for _, want := range []string{
		"func " + testName + "(",
		`t.Run("` + controlRun + `"`,
		`t.Run("` + bulkRun + `"`,
		`t.Fatalf("` + invalidLine + `%s"`,
		`t.Errorf("` + s4FailLine + `%s"`,
		`t.Logf("` + s4PassLine + `%s"`,
		"gap p99 %s without the bulk path, %s with it",
		`"%s: gap p99 without the bulk path %s (%d gaps)`,
		ratesFormat + `",`,
		"s4PassMaxP99    = 75 * time.Millisecond",
		`{"direct", portsBulkDirect}`,
		"{string(agentapi.DialHomeHostDialed), portsBulkHostDialed}",
	} {
		if strings.Count(src, want) != 1 {
			t.Errorf("bulk_test.go has %q %d times, want once: the mapper reads it", want, strings.Count(src, want))
		}
	}
	if bulkPassMax != 75*time.Millisecond || !slices.Equal(modes, []string{"direct", "host-dialed"}) {
		t.Errorf("the mapper's bar %s and modes %q", bulkPassMax, modes)
	}
	r, err := os.ReadFile(filepath.Join("..", "rehearsal.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(r), `echo "`+leftoverLine+` in $scope`) {
		t.Errorf("hack/rehearsal.sh no longer reports processes left as %q", leftoverLine)
	}
}

// The warning carries the link's rate, read from the mode's figures line, and the step summary has a row per mode with its verdict, figures
// and rate, and the job's verdict (#563's ruling, as its delta review read it).
func TestTheWarningCarriesTheRateAndTheSummaryEachModesVerdict(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("testdata", "one-invalid.log"))
	if err != nil {
		t.Fatal(err)
	}
	v := judgeText(t, string(b))
	if len(v.warnings) != 1 || !strings.HasSuffix(v.warnings[0], "; the link's rate 32.0 MB/s without the bulk path, 32.5 MB/s with it") {
		t.Errorf("warnings %q: want direct's, ending with the link's rate", v.warnings)
	}
	sum := summary(v)
	for _, want := range []string{
		"| direct | INVALID: warns | 81.5ms | 51.5ms | 30ms | 32.0 MB/s without the bulk path, 32.5 MB/s with it |",
		"| host-dialed | PASS | 101.5ms | 56.5ms | 45ms | 25.0 MB/s without the bulk path, 25.5 MB/s with it |",
		"**The job passes.**",
	} {
		if !strings.Contains(sum, want) {
			t.Errorf("the summary lacks %q:\n%s", want, sum)
		}
	}
	// A failing job's summary names why, and a skip has its own row.
	skip := judgeText(t, "=== RUN   "+testName+"\n    harness_test.go:61: only with HPCBURST_REHEARSAL=1\n--- SKIP: "+testName+" (0.00s)\nPASS\n")
	if sum := summary(skip); !strings.Contains(sum, "| all | SKIPPED: the job fails |") || !strings.Contains(sum, "**The job fails:**\n- "+testName+" skipped") {
		t.Errorf("a skip's summary:\n%s", sum)
	}
	// appendFile adds to what the job's summary already holds.
	p := filepath.Join(t.TempDir(), "summary.md")
	if err := os.WriteFile(p, []byte("earlier\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := appendFile(p, sum); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(p); string(got) != "earlier\n"+sum {
		t.Errorf("the summary file holds %q", got)
	}
}
