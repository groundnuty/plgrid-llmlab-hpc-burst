package livechecks

// The baseline's Athena twin through the script (issue #764; internal/alpha, AthenaBaselineRow): the same verdicts and exit codes as Row 1's
// (baseline_test.go), on plgrid-now under athena-now.yaml, host-dialed alone, with row A's slot guard, start wait and owner's go. The positive cases run the
// real test in dry mode through the script; the settings and the marker are refused as the script's tables and cases give them.

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
)

const athenaBaseline = "beta-athena-now-llmlab-baseline"

// PASS 0, FAIL 1, INVALID 3 (its own word, like a NOT RUN), each with its own line and the text to post headed with it and with the Athena twin's name.
func TestTheAthenaBaselinesVerdictsThroughTheScript(t *testing.T) {
	real := realTestBinary(t, "HPCBURST_LIVECHECKS_ALPHA_TEST", alphaPkg)
	realDir := filepath.Join(repoRoot(t), alphaPkg)
	lines := map[string]string{"PASS": athenaBaseline + ": PASS", "FAIL": athenaBaseline + ": FAIL", "INVALID": athenaBaseline + ": INVALID (not a NOT RUN"}
	type c struct {
		fault, verdict string
		code           int
	}
	cases := []c{{"", "PASS", 0}, {"llmlab-over-its-guard", "FAIL", 1}}
	for _, f := range athenaBaselineInvalidFaults {
		cases = append(cases, c{f, "INVALID", 3})
	}
	for _, tc := range cases {
		t.Run(tc.verdict+"/"+tc.fault, func(t *testing.T) {
			t.Parallel()
			w := newAlphaWorld(t)
			w.installDry(dryWrapper, real, realDir)
			res := w.run([]string{"STUB_DRY_FAULT=" + tc.fault}, "--run", athenaBaseline, "--yes")
			if res.code != tc.code {
				t.Fatalf("fault %q: exit %d, want %d:\n%s", tc.fault, res.code, tc.code, res.out)
			}
			w.contains(res, lines[tc.verdict])
			for v, l := range lines {
				if v != tc.verdict && strings.Contains(res.out, l) {
					t.Errorf("a %s run also says %q:\n%s", tc.verdict, l, res.out)
				}
			}
			if strings.Contains(res.out, athenaBaseline+": NOT RUN") {
				t.Errorf("a %s run also says NOT RUN:\n%s", tc.verdict, res.out)
			}
			if p := w.postsOf(athenaBaseline); !strings.Contains(p, athenaBaseline+": "+tc.verdict+" (posts to #764)") {
				t.Errorf("the text to post is not headed %s:\n%s", tc.verdict, p)
			}
		})
	}
}

// The baseline's INVALID marker on the Athena twin, in the words the Go test writes: INVALID, exit 3, as on Row 1; and the bulk row's words on it are a failure.
func TestTheAthenaBaselinesInvalidMarkerReadsAsInvalid(t *testing.T) {
	w := newAlphaWorld(t)
	res := w.run([]string{"STUB_ALPHA_MODE=marker", "STUB_MARKER=" + alpha.BaselineInvalidNote(alpha.AthenaBaselineRow())}, "--run", athenaBaseline, "--yes")
	if res.code != 3 || !strings.Contains(res.out, athenaBaseline+": INVALID (not a NOT RUN") {
		t.Errorf("the Athena twin with the baseline's INVALID marker: exit %d, want 3 and INVALID:\n%s", res.code, res.out)
	}
	w = newAlphaWorld(t)
	res = w.run([]string{"STUB_ALPHA_MODE=marker", "STUB_MARKER=" + alpha.InvalidNote(alpha.Row{Name: athenaBaseline})}, "--run", athenaBaseline, "--yes")
	if res.code != 1 || strings.Contains(res.out, athenaBaseline+": INVALID") || strings.Contains(res.out, athenaBaseline+": NOT RUN") {
		t.Errorf("the bulk row's INVALID marker on the Athena twin: exit %d, want 1 and neither INVALID nor NOT RUN:\n%s", res.code, res.out)
	}
}

// The Athena twin's settings: the baseline's (held to their forms as on Row 1), host-dialed mode alone (athena-now.yaml lists no other), the Athena account, and
// the owner's go of plgrid-now; each refusal names the setting and never its value, and no staging directory is asked for.
func TestTheAthenaBaselineRowsSettings(t *testing.T) {
	for _, c := range []struct {
		name string
		mut  func(m map[string]string)
		want string
	}{
		{"no generator binary", func(m map[string]string) { delete(m, "LIVE_STUDYLOAD_BIN") }, "setting LIVE_STUDYLOAD_BIN is missing"},
		{"no LLMLab credentials", func(m map[string]string) { delete(m, "LIVE_LLMLAB_CREDENTIALS") }, "setting LIVE_LLMLAB_CREDENTIALS is missing"},
		{"a bad min_tokens switch", func(m map[string]string) { m["LIVE_STUDYLOAD_OMIT_MIN_TOKENS"] = "yes" }, "LIVE_STUDYLOAD_OMIT_MIN_TOKENS must be 1 or 0"},
		{"the account the shape needs", func(m map[string]string) { delete(m, "ACCOUNT_ATHENA_A100") }, "setting ACCOUNT_ATHENA_A100 is missing"},
		{"direct mode", func(m map[string]string) {
			m["LIVE_DIALHOME_MODE"] = "direct"
			m["LIVE_DIALHOME_ADDR"] = "gateway.example:29227"
		},
			"beta-athena-now-llmlab-baseline runs in host-dialed mode alone (LIVE_DIALHOME_MODE=host-dialed)"},
		{"ssh mode", func(m map[string]string) {
			m["LIVE_DIALHOME_MODE"] = "ssh"
			m["LIVE_DIALHOME_ADDR"] = "gateway.example:29227"
			m["LIVE_DIALHOME_KEY"] = m["LIVE_CONF_CREDENTIALS_ATHENA"]
		}, "beta-athena-now-llmlab-baseline runs in host-dialed mode alone (LIVE_DIALHOME_MODE=host-dialed)"},
		{"no owner's go", func(m map[string]string) { delete(m, "LIVE_OWNER_GO_ATHENA_NOW") }, "setting LIVE_OWNER_GO_ATHENA_NOW must hold today's UTC date"},
	} {
		t.Run(c.name, func(t *testing.T) {
			w := newAlphaWorld(t)
			w.exactMode = true // the mode as written: the harness gives no host-dialed mode here
			w.alphaSettings(func(m map[string]string) {
				m["LIVE_DIALHOME_MODE"] = "host-dialed"
				delete(m, "LIVE_DIALHOME_ADDR")
				c.mut(m)
			})
			res := w.run(nil, "--run", athenaBaseline, "--yes")
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
	// With the settings as the row takes them, and no staging directory, the row passes its settings (a run without --yes ends at the estimate, exit 4).
	w := newAlphaWorld(t)
	w.exactMode = true
	w.alphaSettings(func(m map[string]string) {
		m["LIVE_DIALHOME_MODE"] = "host-dialed"
		delete(m, "LIVE_DIALHOME_ADDR")
		delete(m, "LIVE_SANO_STAGING_DIR")
	})
	if res := w.run(nil, "--run", athenaBaseline); res.code != 4 {
		t.Errorf("the settings as the row takes them: exit %d, want 4 (no --yes):\n%s", res.code, res.out)
	}
}

// A failed LLMLab P0 ends the run INVALID before any job is submitted (PR 809's review, point 6): the script's line, the marker it reads and the text to post say
// that no job was submitted and no slot was spent, on Row 1 and on the Athena twin alike, and never that a job started and ran. The real test runs in dry mode through the
// script; the control, an INVALID run that did run, keeps its words.
func TestAFailedLLMLabP0ReportsNoSubmissionThroughTheScript(t *testing.T) {
	real := realTestBinary(t, "HPCBURST_LIVECHECKS_ALPHA_TEST", alphaPkg)
	realDir := filepath.Join(repoRoot(t), alphaPkg)
	for _, row := range []string{"beta-sano-llmlab-baseline", athenaBaseline} {
		for _, c := range []struct {
			fault string
			noJob bool
		}{{"p0-llmlab-fails", true}, {"p0-empty-200", true}, {"cap-never-binds", false}} {
			t.Run(row+"/"+c.fault, func(t *testing.T) {
				w := newAlphaWorld(t)
				w.installDry(dryWrapper, real, realDir)
				res := w.run([]string{"STUB_DRY_FAULT=" + c.fault}, "--run", row, "--yes")
				if res.code != 3 {
					t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
				}
				post := w.postsOf(row)
				if c.noJob {
					w.contains(res, row+": INVALID (not a NOT RUN: P0 ended the run before any job was submitted, so no job started and no slot was spent")
					if strings.Contains(res.out, "the job started and ran") || strings.Contains(post, "started, ran and spent the slot") || !strings.Contains(post, "no job was submitted") {
						t.Errorf("a run that submitted nothing says a job ran:\n%s\n%s", res.out, post)
					}
					return
				}
				w.contains(res, row+": INVALID (not a NOT RUN: the job started and ran")
				if strings.Contains(res.out, "before any job was submitted") || !strings.Contains(post, "started, ran and spent the slot") {
					t.Errorf("a run that ran lost its words:\n%s\n%s", res.out, post)
				}
			})
		}
	}
}
