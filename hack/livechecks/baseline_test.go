package livechecks

// The baseline row's INVALID (issue #764; internal/alpha, ErrBaselineInvalid): a run judged, recorded and posted with no failure whose reading measures
// nothing: a cap that never bound, a 503 in the detach phase, an engine whose KV is below the cap's need, and the other INVALID rules. Its
// test writes internal/alpha's BaselineInvalidNote to the marker and skips once, and the script reads that as INVALID on the baseline row alone, exit 3,
// like a NOT RUN and with its own word. The positive case runs the real test in dry mode through the script; the guard is that the marker on any other
// row is no INVALID and no NOT RUN but a failure, and that the bulk row's own INVALID words on the baseline row are none of its.

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
)

// The baseline row's verdicts and their exit codes, through the script with the real test in dry mode: PASS 0, FAIL 1 (LLMLab past its guard of 144 requests
// in a phase, a request the front or an Instance failed), INVALID 3. Each says its own line and none of the others', and the text to post is headed with it.
func TestTheBaselineRowsVerdictsThroughTheScript(t *testing.T) {
	const row = "beta-sano-llmlab-baseline"
	real := realTestBinary(t, "HPCBURST_LIVECHECKS_ALPHA_TEST", alphaPkg)
	realDir := filepath.Join(repoRoot(t), alphaPkg)
	lines := map[string]string{"PASS": row + ": PASS", "FAIL": row + ": FAIL", "INVALID": row + ": INVALID (not a NOT RUN"}
	cases := []struct {
		fault, verdict string
		code           int
	}{{"", "PASS", 0}, {"llmlab-over-its-guard", "FAIL", 1}}
	for _, f := range baselineInvalidFaults {
		cases = append(cases, struct {
			fault, verdict string
			code           int
		}{f, "INVALID", 3})
	}
	for _, c := range cases {
		t.Run(c.verdict+"/"+c.fault, func(t *testing.T) {
			t.Parallel()
			w := newAlphaWorld(t)
			w.installDry(dryWrapper, real, realDir)
			res := w.run([]string{"STUB_DRY_FAULT=" + c.fault}, "--run", row, "--yes")
			if res.code != c.code {
				t.Fatalf("fault %q: exit %d, want %d:\n%s", c.fault, res.code, c.code, res.out)
			}
			w.contains(res, lines[c.verdict])
			for v, l := range lines {
				if v != c.verdict && strings.Contains(res.out, l) {
					t.Errorf("a %s run also says %q:\n%s", c.verdict, l, res.out)
				}
			}
			if strings.Contains(res.out, row+": NOT RUN") {
				t.Errorf("a %s run also says NOT RUN:\n%s", c.verdict, res.out)
			}
			if p := w.postsOf(row); !strings.Contains(p, row+": "+c.verdict+" (posts to #764)") {
				t.Errorf("the text to post is not headed %s:\n%s", c.verdict, p)
			}
		})
	}
}

// The baseline row's INVALID marker is that row's alone, in the words the Go test writes (alpha.BaselineInvalidNote, not a copy): on any other row it is
// a failure, not an INVALID and not a NOT RUN; and the bulk row's INVALID words on the baseline row are a failure too. The control first, on each row: the
// same mode with the plain wait-in-the-queue marker reads NOT RUN, so the failure is the INVALID words'.
func TestTheBaselinesInvalidMarkerIsThatRowsAlone(t *testing.T) {
	const baseline = "beta-sano-llmlab-baseline"
	for _, row := range []string{"alpha-sano-serve", "alpha-athena-now-serve", "beta-athena-now-bulk", "tunnel-hour-athena"} {
		t.Run(row, func(t *testing.T) {
			w := newAlphaWorld(t)
			if hostDialedOnly(row) {
				hostDialed(w)
			}
			res := w.run([]string{"STUB_ALPHA_MODE=marker", "STUB_MARKER=" + alpha.BaselineInvalidNote(alpha.Row{Name: row})}, "--run", row, "--yes")
			if res.code != 1 || strings.Contains(res.out, row+": INVALID") || strings.Contains(res.out, row+": NOT RUN") {
				t.Errorf("the baseline's INVALID marker on %s: exit %d, want 1 and neither INVALID nor NOT RUN:\n%s", row, res.code, res.out)
			}
		})
	}
	t.Run("the bulk row's words on the baseline row", func(t *testing.T) {
		w := newAlphaWorld(t)
		hostDialed(w)
		res := w.run([]string{"STUB_ALPHA_MODE=marker", "STUB_MARKER=" + alpha.InvalidNote(alpha.Row{Name: baseline})}, "--run", baseline, "--yes")
		if res.code != 1 || strings.Contains(res.out, baseline+": INVALID") || strings.Contains(res.out, baseline+": NOT RUN") {
			t.Errorf("the bulk row's INVALID marker on the baseline row: exit %d, want 1 and neither INVALID nor NOT RUN:\n%s", res.code, res.out)
		}
	})
	t.Run("the baseline row with its own words", func(t *testing.T) {
		w := newAlphaWorld(t)
		hostDialed(w)
		res := w.run([]string{"STUB_ALPHA_MODE=marker", "STUB_MARKER=" + alpha.BaselineInvalidNote(alpha.BaselineRow())}, "--run", baseline, "--yes")
		if res.code != 3 || !strings.Contains(res.out, baseline+": INVALID (not a NOT RUN") {
			t.Errorf("the baseline row with its INVALID marker: exit %d, want 3 and INVALID:\n%s", res.code, res.out)
		}
	})
}

// The baseline row's settings: the LLMLab credentials, the generator's binary and its two optional settings are held to their forms, each refusal names
// the setting and never its value, and no other row is given them.
func TestTheBaselineRowsSettings(t *testing.T) {
	const row = "beta-sano-llmlab-baseline"
	for _, c := range []struct {
		name string
		mut  func(m map[string]string)
		want string
	}{
		{"no generator binary", func(m map[string]string) { delete(m, "LIVE_STUDYLOAD_BIN") }, "setting LIVE_STUDYLOAD_BIN is missing"},
		{"a relative generator binary", func(m map[string]string) { m["LIVE_STUDYLOAD_BIN"] = "SEEDBIN/studyload" }, "LIVE_STUDYLOAD_BIN must be an absolute path"},
		{"a generator binary that does not exist", func(m map[string]string) { m["LIVE_STUDYLOAD_BIN"] = "/SEEDBIN/none" }, "LIVE_STUDYLOAD_BIN is not an executable regular file"},
		{"no LLMLab credentials", func(m map[string]string) { delete(m, "LIVE_LLMLAB_CREDENTIALS") }, "setting LIVE_LLMLAB_CREDENTIALS is missing"},
		{"a bad min_tokens switch", func(m map[string]string) { m["LIVE_STUDYLOAD_OMIT_MIN_TOKENS"] = "yes" }, "LIVE_STUDYLOAD_OMIT_MIN_TOKENS must be 1 or 0"},
		{"the account the shape needs", func(m map[string]string) { delete(m, "ACCOUNT_SANO_GPU") }, "setting ACCOUNT_SANO_GPU is missing"},
		{"no staging directory", func(m map[string]string) { delete(m, "LIVE_SANO_STAGING_DIR") }, "setting LIVE_SANO_STAGING_DIR is missing"},
	} {
		t.Run(c.name, func(t *testing.T) {
			w := newAlphaWorld(t)
			w.exactMode = true // the mode as written: the harness gives no host-dialed mode here
			w.alphaSettings(func(m map[string]string) {
				m["LIVE_DIALHOME_MODE"] = "host-dialed"
				delete(m, "LIVE_DIALHOME_ADDR")
				c.mut(m)
			})
			res := w.run(nil, "--run", row, "--yes")
			if res.code != 3 || !strings.Contains(res.out, c.want) {
				t.Errorf("exit %d, want 3 and a refusal naming %q:\n%s", res.code, c.want, res.out)
			}
			for _, seed := range []string{"SEEDBIN", "SEEDACCT", "SEEDSTAGE", "SEEDKEY"} {
				if strings.Contains(res.out, seed) {
					t.Errorf("the refusal leaks %s:\n%s", seed, res.out)
				}
			}
		})
	}
}
