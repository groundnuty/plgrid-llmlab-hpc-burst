package livechecks

// The bulk row's INVALID (issue #521; the science agent's ruling on #521): a run judged, recorded and posted with no failure and no saturation
// witness. Its test writes internal/alpha's InvalidNote to the marker and skips once, and the script reads that as INVALID on the bulk row alone.
// The row is ready since its test shim (option (A)): the positive case runs the real test in dry mode through the script, INVALID and exit 3 with its
// text to post; the guard is that the marker on any other row is no INVALID and no NOT RUN but a failure, with the words the Go test writes, not a
// copy of them.

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
)

func TestTheInvalidMarkerOnAnotherRowIsAFailure(t *testing.T) {
	for _, row := range []string{"alpha-athena-now-serve", "beta-athena-now-router", "beta-athena-now-keeper", "tunnel-hour-athena"} {
		t.Run(row, func(t *testing.T) {
			// The control first: the same mode with the plain start-wait marker reads NOT RUN on this row, so the failure below is the INVALID words'.
			for _, c := range []struct {
				name, marker string
				code         int
			}{
				{"a start-wait marker", row + ": a job did not start within the start wait of 10m0s from the deployment's creation; this run's jobs were cancelled by tag\n", 3},
				{"the INVALID marker", alpha.InvalidNote(alpha.Row{Name: row}), 1},
			} {
				w := newAlphaWorld(t)
				if hostDialedOnly(row) {
					hostDialed(w)
				}
				res := w.run([]string{"STUB_ALPHA_MODE=marker", "STUB_MARKER=" + c.marker}, "--run", row, "--yes")
				if res.code != c.code {
					t.Fatalf("%s: exit %d, want %d:\n%s", c.name, res.code, c.code, res.out)
				}
				if strings.Contains(res.out, row+": INVALID") {
					t.Errorf("%s on %s read as INVALID:\n%s", c.name, row, res.out)
				}
				if c.code == 1 && strings.Contains(res.out, row+": NOT RUN") {
					t.Errorf("%s on %s read as NOT RUN:\n%s", c.name, row, res.out)
				}
			}
		})
	}
}

// The positive INVALID, now that the row can run: the real test in dry mode, with a control too short of gaps to measure the transfer, is judged,
// recorded and posted, and the script reads it as INVALID, exit 3, apart from PASS, FAIL and NOT RUN. The control is the same run with its gaps: PASS.
func TestTheBulkRowsInvalidThroughTheScript(t *testing.T) {
	const row = "beta-athena-now-bulk"
	real := realTestBinary(t, "HPCBURST_LIVECHECKS_ALPHA_TEST", alphaPkg)
	realDir := filepath.Join(repoRoot(t), alphaPkg)
	for _, c := range []struct {
		fault string
		code  int
		says  string
	}{
		{"", 0, row + ": PASS"},
		{"control-few-gaps", 3, row + ": INVALID (not a NOT RUN"},
	} {
		w := newAlphaWorld(t)
		w.installDry(dryWrapper, real, realDir)
		res := w.run([]string{"STUB_DRY_FAULT=" + c.fault}, "--run", row, "--yes")
		if res.code != c.code {
			t.Fatalf("fault %q: exit %d, want %d:\n%s", c.fault, res.code, c.code, res.out)
		}
		w.contains(res, c.says)
		if c.code == 3 {
			if strings.Contains(res.out, row+": FAIL") || strings.Contains(res.out, row+": PASS") || strings.Contains(res.out, row+": NOT RUN") {
				t.Errorf("an INVALID run was also reported as a pass, a failure or a NOT RUN:\n%s", res.out)
			}
			if p := w.postsOf(row); !strings.Contains(p, "INVALID") {
				t.Errorf("the text to post does not say INVALID:\n%s", p)
			}
		}
	}
}

// stopBulkRun starts the bulk row with the stub's staging of its shipped agent, signals the script once the test runs, and returns its output and
// exit status: the test never reaches its own removal.
func stopBulkRun(t *testing.T, w *world, extra ...string) (string, int) {
	t.Helper()
	cmd, out := w.start(append([]string{"STUB_ALPHA_MODE=stage-and-sleep"}, extra...), "--run", "beta-athena-now-bulk", "--yes")
	w.waitFor("started")
	if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	_ = w.waitStopped(cmd, out) // bounded at stopBound (issue #494)
	w.noLeftoverProcess(cmd)
	return out.String(), cmd.ProcessState.ExitCode()
}

// A bulk run stopped before its own removal (a signal, a timeout, a panic) leaves the shipped agent's directory: the script's cleanup removes it by
// the exact path the test wrote, and says so by its digest alone (the science agent's condition 2; #579's review).
func TestAStoppedBulkRunsStagedAgentIsRemovedByTheScript(t *testing.T) {
	w := newAlphaWorld(t)
	d := filepath.Join(w.state, "staging", "test-agent", strings.Repeat("ab", 32))
	out, _ := stopBulkRun(t, w)
	if _, err := os.Stat(d); !os.IsNotExist(err) {
		t.Errorf("the staged agent's directory remains (%v):\n%s", err, out)
	}
	if !strings.Contains(out, "cleanup: removed the bulk row's staged shipped agent test-agent/abababababab…, which its stopped test had left") {
		t.Errorf("the cleanup did not say it removed the staged agent:\n%s", out)
	}
	if strings.Contains(out, w.state) {
		t.Errorf("the output names a path:\n%s", out)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(d))); err != nil {
		t.Errorf("the parent of the staged agent's directory went too: %v", err)
	}
}

// The leftover check: a directory the removal could not take is named, and the row fails.
func TestAStagedAgentTheScriptCannotRemoveIsNamed(t *testing.T) {
	w := newAlphaWorld(t)
	d := filepath.Join(w.state, "staging", "test-agent", strings.Repeat("ab", 32))
	t.Cleanup(func() { _ = os.Chmod(d, 0o755) })
	out, code := stopBulkRun(t, w, "STUB_TEST_AGENT_LOCK=1")
	if !strings.Contains(out, "cleanup: the bulk row's staged shipped agent test-agent/abababababab… remains after its removal; remove it by hand") || code == 0 {
		t.Errorf("exit %d; want a failure naming the staged agent that remains:\n%s", code, out)
	}
}

// A path that is not <staging>/test-agent/<64 hex>, plain and under the staging root the run recorded, is never removed, whatever it would reach: the
// staging root itself, a "." or ".." segment, a doubled or trailing slash, another root than the recorded one, a relative path, a short leaf.
func TestTheScriptRemovesNoPathButAStagedAgentsOwn(t *testing.T) {
	leaf := strings.Repeat("ab", 32)
	for _, c := range []struct{ name, staging, dir string }{
		{"the staging root", "", "{root}"},
		{"a .. segment", "", "{root}/test-agent/../test-agent/" + leaf},
		{"a . segment", "", "{root}/./test-agent/" + leaf},
		{"a doubled slash", "", "{root}//test-agent/" + leaf},
		{"a trailing slash", "", "{root}/test-agent/" + leaf + "/"},
		{"another root than the recorded one", "{root}/other", "{root}/test-agent/" + leaf},
		{"a recorded root with a .. segment", "{root}/x/..", "{root}/x/../test-agent/" + leaf},
		{"a relative path", "staging", "staging/test-agent/" + leaf},
		{"a short leaf", "", "{root}/test-agent/" + leaf[:63]},
	} {
		t.Run(c.name, func(t *testing.T) {
			w := newAlphaWorld(t)
			root := filepath.Join(w.state, "staging")
			sub := func(s string) string { return strings.ReplaceAll(s, "{root}", root) }
			extra := []string{"STUB_TEST_AGENT_PATH=" + sub(c.dir)}
			if c.staging != "" {
				extra = append(extra, "STUB_TEST_AGENT_STAGING="+sub(c.staging))
			}
			out, code := stopBulkRun(t, w, extra...)
			if _, err := os.Stat(filepath.Join(root, "test-agent", leaf, "hb-agent")); err != nil {
				t.Errorf("a path of another shape removed what lies under it: %v", err)
			}
			if !strings.Contains(out, "cleanup: the bulk row's staged shipped agent was named by a path that is not <staging>/test-agent/<digest> under the staging root the run recorded; nothing was removed") || code == 0 {
				t.Errorf("exit %d; want the refusal and a failure:\n%s", code, out)
			}
		})
	}
}

// The bulk row's NOT RUN for an image not staged: exit 3 and its line through the script, the test's own marker words; on another row a failure.
func TestTheUnstagedImageNotRunThroughTheScript(t *testing.T) {
	for _, c := range []struct {
		row  string
		code int
	}{{"beta-athena-now-bulk", 3}, {"beta-athena-now-keeper", 1}} {
		w := newAlphaWorld(t)
		hostDialed(w)
		note := alpha.NotRunNote(alpha.Row{Name: c.row}, 0, alpha.ErrNoStagedImage)
		res := w.run([]string{"STUB_ALPHA_MODE=marker", "STUB_MARKER=" + note}, "--run", c.row, "--yes")
		if res.code != c.code {
			t.Fatalf("%s: exit %d, want %d:\n%s", c.row, res.code, c.code, res.out)
		}
		if c.code == 3 {
			w.contains(res, c.row+": NOT RUN (the Provider's image for the deployment is not staged with a whole record, and the bulk row's test shim stages none: stage it first with alpha-athena-now-stage; no job was submitted; not a failure)")
		} else if strings.Contains(res.out, c.row+": NOT RUN") {
			t.Errorf("the marker on %s read as NOT RUN:\n%s", c.row, res.out)
		}
	}
}
