package livechecks

// The dry runs of the reach rows through the script (issue #225). The row's own test is compiled and run for real by
// the script, as it runs it live: the script reads the settings, builds, takes the slot, runs the binary, reads its
// verbose log for the verdict and cleans up. What differs from a live run is what the dry wrapper
// (alpha_dry_test.go) takes: the cluster. The job is the probe script a live run stages, run by a fake Slurm on this
// machine, and the gateway is a socket that this test started and put in the settings file.
//
// The stand-ins are real sockets (internal/providerctl/ctltest, reachstub.go): an SSH banner, a TLS server, a socket
// that says nothing, a port nobody listens on, a listener whose accept queue is full, and a name under .invalid. A
// probe tested only against a stand-in that always connects proves nothing, so each outcome has its own, the
// reachable ones are asked whether they saw the probe, and each of the five outcomes that are not a pass fails the row.

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/providerctl/ctltest"
)

// dryWrapperReachKeepsLive is the wrapper that gives the dry binary the gateway's settings and forgets to take the
// live settings away.
const dryWrapperReachKeepsLive = `#!/bin/bash
echo "testbin dry" >> "$STUB_STATE/events"
export HPCBURST_ALPHA_ROW="reach-$HPCBURST_CONF_PROFILE" HPCBURST_ALPHA_PROBE_HOST="$HPCBURST_CONF_PROBE_HOST" \
  HPCBURST_ALPHA_PROBE_PORT="$HPCBURST_CONF_PROBE_PORT" HPCBURST_ALPHA_PROBE_LABEL="$HPCBURST_CONF_PROBE_LABEL" HPCBURST_ALPHA_OUT="$HPCBURST_CONF_OUT"
export HPCBURST_ALPHA_DRY=1
cd "@REALDIR@" || exit 3
exec "@REAL@" "$@"
`

// reachCase is a stand-in and what the row must make of it, written out from the probe's header.
type reachCase struct {
	kind    ctltest.ReachKind
	outcome string
	passes  bool
}

var reachCases = []reachCase{
	{ctltest.StubSSH, "reachable", true},
	{ctltest.StubTLS, "reachable", true},
	{ctltest.StubSilent, "reachable", true},
	{ctltest.StubRefused, "refused", false},
	{ctltest.StubTimeout, "timeout", false},
	{ctltest.StubUnresolved, "unresolved", false},
}

// reachDryWorld is a world whose settings name the stand-in as the gateway, and whose test binary is the dry one.
func reachDryWorld(t *testing.T, real, realDir string, stub ctltest.ReachStub, label string) *world {
	t.Helper()
	w := newAlphaWorld(t)
	w.reachSettings(func(m map[string]string) {
		m["LIVE_PROBE_HOST"], m["LIVE_PROBE_PORT"], m["LIVE_PROBE_LABEL"] = stub.Host, strconv.Itoa(stub.Port), label
	})
	w.installDry(dryWrapper, real, realDir)
	return w
}

// noTarget fails when the stand-in's host or port is on the screen or in any file the run wrote. The port is a word,
// so a number that holds it by chance (a longer one) is not a hit; the unresolved stand-in's port, 9, is any digit.
func (w *world) noTarget(stub ctltest.ReachStub, kind ctltest.ReachKind, screen string) {
	w.t.Helper()
	var port *regexp.Regexp
	if kind != ctltest.StubUnresolved {
		port = regexp.MustCompile(`\b` + strconv.Itoa(stub.Port) + `\b`)
	}
	hit := func(where, text string) {
		if strings.Contains(text, stub.Host) || (port != nil && port.MatchString(text)) {
			w.t.Errorf("%s holds the gateway's host or port:\n%s", where, text)
		}
	}
	hit("the screen", screen)
	files := 0
	err := filepath.WalkDir(w.out(), func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		files++
		hit(strings.TrimPrefix(p, w.out()+"/"), string(b))
		return err
	})
	if err != nil {
		w.t.Fatal(err)
	}
	if files < 4 {
		// The records of the run are in the directory: a search of an empty one proves nothing.
		w.t.Errorf("the output directory holds %d file(s), too few for the search to mean anything", files)
	}
}

func TestEveryReachRowReportsEachOutcomeThroughTheScript(t *testing.T) {
	real := realTestBinary(t, "HPCBURST_LIVECHECKS_CONF_TEST", confPkg)
	realDir := filepath.Join(repoRoot(t), confPkg)
	for i, s := range reachShapes {
		for j, c := range reachCases {
			t.Run(s.row+"/"+string(c.kind), func(t *testing.T) {
				t.Parallel()
				stub := ctltest.StartReachStub(t, c.kind, "127.77."+strconv.Itoa(i+1)+"."+strconv.Itoa(j+1), 0)
				w := reachDryWorld(t, real, realDir, stub, reachSeedLabel)
				res := w.run(nil, "--run", s.row, "--yes")
				wantCode, verdict := 1, "FAIL"
				counts := s.row + ": tests: 0 passed, 1 failed, 0 skipped"
				if c.passes {
					wantCode, verdict, counts = 0, "PASS", s.row+": tests: 1 passed, 0 failed, 0 skipped"
				}
				if res.code != wantCode {
					t.Fatalf("exit %d, want %d (outcome %s):\n%s", res.code, wantCode, c.outcome, res.out)
				}
				w.contains(res, counts, s.row+": "+verdict, "cleanup: left behind 0")
				// What the stand-in saw: the reachable ones must have seen the probe, the others none.
				if c.passes != (stub.Accepted() > 0) {
					t.Errorf("the stand-in accepted %d connection(s) for outcome %s: the outcome may not be its own", stub.Accepted(), c.outcome)
				}
				// The text to post: the verdict, the label and the outcome, the partition and the QOS used.
				posts := w.postsOf(s.row)
				wantQOS := "qos now (requested"
				wantPart := "partition plgrid-gpu-gh200"
				if s.slot != "now" {
					wantQOS, wantPart = "qos plgrid-now (the partition's own; none requested)", "partition plgrid-now"
				}
				for _, want := range []string{s.row + ": " + verdict, "as " + reachSeedLabel + ": outcome " + c.outcome, wantPart, wantQOS, "1 GPU, 1 CPU and 1Gi"} {
					if !strings.Contains(posts, want) {
						t.Errorf("the text to post lacks %q:\n%s", want, posts)
					}
				}
				// The target is on the screen nowhere, and in no file of the run.
				w.noTarget(stub, c.kind, res.out)
				log, _ := os.ReadFile(filepath.Join(w.out(), s.row+".log"))
				if !strings.Contains(string(log), "DRY RUN") {
					t.Errorf("the test's log does not say it was a dry run:\n%s", log)
				}
			})
		}
	}
}

// The owner's other job holds the slot, so this run's job pends past the start-wait bound: the row is NOT RUN, with
// exit 3, the line, the job cancelled by its tag, and no text to post. Under QOS now on Helios, and under the
// partition's own QOS on Athena.
func TestAReachRowWhoseSlotIsHeldIsNotRun(t *testing.T) {
	real := realTestBinary(t, "HPCBURST_LIVECHECKS_CONF_TEST", confPkg)
	realDir := filepath.Join(repoRoot(t), confPkg)
	for _, s := range reachShapes {
		t.Run(s.row, func(t *testing.T) {
			t.Parallel()
			stub := ctltest.StartReachStub(t, ctltest.StubSSH, "127.77.9.9", 0)
			w := reachDryWorld(t, real, realDir, stub, reachSeedLabel)
			res := w.run([]string{"STUB_DRY_FAULT=slot-held"}, "--run", s.row, "--yes")
			if res.code != 3 {
				t.Fatalf("exit %d, want 3:\n%s", res.code, res.out)
			}
			w.contains(res, s.row+": tests: 0 passed, 0 failed, 1 skipped", s.row+": NOT RUN (a job did not start within 10 min of its acceptance and was cancelled by its tag; not a failure)", "cleanup: left behind 0")
			if strings.Contains(res.out, s.row+": FAIL") || strings.Contains(res.out, s.row+": PASS") {
				t.Errorf("a NOT RUN run was also reported as a pass or a failure:\n%s", res.out)
			}
			if m, _ := filepath.Glob(filepath.Join(w.out(), s.row+"-*", "posts.md")); len(m) != 0 {
				t.Errorf("a NOT RUN run left a text to post: %v", m)
			}
			// The job never started, so it never reached the stand-in.
			if stub.Accepted() != 0 {
				t.Errorf("the stand-in accepted %d connection(s) from a job that never started", stub.Accepted())
			}
		})
	}
}

// The dry switch is guarded from both sides, as for the other rows. A dry binary refuses live settings, so a dry
// pass cannot stand for a live one; and a dry binary refuses a gateway that is not a stand-in, so that a dry run can
// never probe a real one from this machine.
func TestADryReachBinaryRefusesLiveSettingsAndARealGateway(t *testing.T) {
	real := realTestBinary(t, "HPCBURST_LIVECHECKS_CONF_TEST", confPkg)
	realDir := filepath.Join(repoRoot(t), confPkg)
	t.Run("live settings present", func(t *testing.T) {
		stub := ctltest.StartReachStub(t, ctltest.StubSSH, "127.77.9.8", 0)
		w := reachDryWorld(t, real, realDir, stub, reachSeedLabel)
		w.installDry(dryWrapperReachKeepsLive, real, realDir)
		res := w.run(nil, "--run", "reach-athena", "--yes")
		if res.code != 1 {
			t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
		}
		log, _ := os.ReadFile(filepath.Join(w.out(), "reach-athena.log"))
		if !strings.Contains(string(log), "a dry run refuses to start with live settings present") {
			t.Errorf("the refusal is not in the log:\n%s", log)
		}
		if strings.Contains(string(log), "DRY RUN:") || stub.Accepted() != 0 {
			t.Errorf("a refused dry run went on (accepted %d):\n%s", stub.Accepted(), log)
		}
	})
	t.Run("a gateway that is not a stand-in", func(t *testing.T) {
		w := newAlphaWorld(t)
		// 192.0.2.1 (TEST-NET-1) is not loopback: the dry binary must stop before it connects to anything.
		w.reachSettings(func(m map[string]string) { m["LIVE_PROBE_HOST"] = "192.0.2.1" })
		w.installDry(dryWrapper, real, realDir)
		res := w.run(nil, "--run", "reach-athena", "--yes")
		if res.code != 1 {
			t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
		}
		log, _ := os.ReadFile(filepath.Join(w.out(), "reach-athena.log"))
		if !strings.Contains(string(log), "a dry run connects only to a loopback address or a name under .invalid") {
			t.Errorf("the refusal is not in the log:\n%s", log)
		}
	})
}
