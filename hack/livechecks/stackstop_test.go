package livechecks

// The two safety rules of PR 809's review on the shared Athena login node, held through the script with the stubs.
//
// Point 1: the run's own stack is stopped BEFORE its jobs are cancelled. The launcher detaches its supervisor, so a test that a timeout or a signal stopped never ran
// its Go cleanup, and the controller of the run is still running: it can submit another job, and reopen relays, after the cancel by tag. The test writes the stack's
// state directory to its record directory before it starts the stack (internal/alpha, StackStateFile) and the script runs `hpcburst down` on that directory, waits for
// it, and only then cancels by tag; every stack row's path is the same code, so each is run here.
//
// Point 2: a run that was stopped, or left a submission it could not settle, may have a submission still in flight (the Provider's submit and release can land inside
// the profiles' inflightWindow of 5 minutes, profiles/athena-now.yaml:72), and on plgrid-now a late job takes the owner's one-job slot. The cleanup cancels by the run's
// OWN tag and re-reads until none is listed for at least that window, and fails if one still is.

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/alpha"
)

// stackRowsOfEveryKind are the stack rows the cleanup serves, one of each flow: the handover, row A, the tunnel hour, Row 1 and the Athena baseline.
var stackRowsOfEveryKind = []string{"alpha-athena-handover", "alpha-athena-now-serve", "tunnel-hour-athena", "beta-sano-llmlab-baseline", "beta-athena-now-llmlab-baseline"}

const downAnnounced = "cleanup: the run's own stack was stopped (hpcburst down) before its jobs were cancelled"

// A run stopped by a signal, a panic or a timeout leaves its stack named in its record directory: the script stops that stack, then cancels the job by its tag, on every
// stack row. The order is read from the stubs' events: the down comes before the first scancel.
func TestTheRunsOwnStackIsStoppedBeforeItsJobsAreCancelled(t *testing.T) {
	for _, row := range stackRowsOfEveryKind {
		for _, mode := range []string{"stack-die", "stack-timeout", "stack-sleep"} {
			t.Run(row+"/"+mode, func(t *testing.T) {
				w := newAlphaWorld(t)
				var out string
				if mode == "stack-sleep" {
					if hostDialedOnly(row) {
						hostDialed(w)
					}
					cmd, buf := w.start([]string{"STUB_ALPHA_MODE=stack-sleep"}, "--run", row, "--yes")
					w.waitFor("started")
					w.signal(cmd, syscall.SIGTERM)
					_ = w.waitStopped(cmd, buf)
					w.noLeak(buf.String())
					out = buf.String()
				} else {
					if hostDialedOnly(row) {
						hostDialed(w)
					}
					res := w.run([]string{"STUB_ALPHA_MODE=" + mode}, "--run", row, "--yes")
					if res.code != 1 {
						t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
					}
					w.noLeak(res.out)
					out = res.out
				}
				if !strings.Contains(out, downAnnounced) || !strings.Contains(out, "cleanup: left behind 0 (cancelled 1)") {
					t.Errorf("the output lacks the stop of the stack or the cancel:\n%s", out)
				}
				ev := w.events()
				down, cancel := index(ev, "hpcburst down", 1), index(ev, "scancel", 1)
				if down < 0 || cancel < 0 || down > cancel {
					t.Errorf("the stack's down is event %d and the first cancel event %d: the stack must be stopped first:\n%s", down, cancel, strings.Join(ev, "\n"))
				}
				if n := len(regexp.MustCompile(`(?m)^hpcburst down$`).FindAllString(strings.Join(ev, "\n"), -1)); n != 1 {
					t.Errorf("the stack was stopped %d times, want once", n)
				}
				argv := strings.TrimSpace(w.stateFile("down-argv"))
				if want := "down --state-dir " + filepath.Join(w.state, "stack", "st"); argv != want {
					t.Errorf("the down was run as %q, want exactly %q: the run's own state directory alone", argv, want)
				}
			})
		}
	}
}

// What the script will not do: a normal run leaves no stack named (nothing to stop); a stack whose directory is gone (the test removed it itself) is not stopped
// again; a name that is not a plain absolute <directory>/st is refused and nothing is run; and a down that fails fails the row while the jobs are still cancelled.
func TestTheStackIsStoppedOnlyAsTheRunNamedIt(t *testing.T) {
	const row = "alpha-athena-now-serve"
	t.Run("a run that ended well names no stack", func(t *testing.T) {
		w := newAlphaWorld(t)
		res := w.run(nil, "--run", row, "--yes")
		if res.code != 0 || strings.Contains(res.out, "hpcburst down") || index(w.events(), "hpcburst down", 1) >= 0 {
			t.Errorf("exit %d, or a stack was stopped in a run that named none:\n%s", res.code, res.out)
		}
	})
	t.Run("a stack whose directory is gone", func(t *testing.T) {
		w := newAlphaWorld(t)
		res := w.run([]string{"STUB_ALPHA_MODE=stack-die", "STUB_STACK_STATE=state=" + w.state + "/removed/st"}, "--run", row, "--yes")
		if index(w.events(), "hpcburst down", 1) >= 0 || strings.Contains(res.out, downAnnounced) {
			t.Errorf("a down was run for a directory that is not there:\n%s", res.out)
		}
	})
	for name, line := range map[string]string{"a relative path": "state=stack/st", "a path that climbs": "state=/tmp/../stack/st", "a path not ending in st": "state=/tmp/stackdir", "a path with a space": "state=/tmp/a b/st"} {
		t.Run(name, func(t *testing.T) {
			w := newAlphaWorld(t)
			res := w.run([]string{"STUB_ALPHA_MODE=stack-die", "STUB_STACK_STATE=" + line}, "--run", row, "--yes")
			if res.code != 1 || index(w.events(), "hpcburst down", 1) >= 0 {
				t.Errorf("exit %d, or a down was run for the name %q:\n%s", res.code, line, res.out)
			}
			w.contains(res, "cleanup: the run's stack was named by a path that is not a plain absolute <directory>/st; nothing was stopped")
		})
	}
	t.Run("a down that fails", func(t *testing.T) {
		w := newAlphaWorld(t)
		res := w.run([]string{"STUB_ALPHA_MODE=stack-die", "STUB_DOWN_FAIL=1"}, "--run", row, "--yes")
		if res.code != 1 {
			t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
		}
		w.contains(res, "cleanup: hpcburst down could not stop the run's stack", "cleanup: left behind 0 (cancelled 1)")
		if strings.Contains(res.out, downAnnounced) {
			t.Errorf("a failed down was reported as a stop:\n%s", res.out)
		}
	})
}

// Point 2, through the pre-staging row of the Athena baseline (a conformance run on plgrid-now, whose submit can be interrupted): a run that died leaves a job that the
// queue shows only after many reads, as a submission in flight lands late. The cleanup of a stopped run waits for the window and cancels it by its own tag; with the
// window of the world's default (off), or short of the reads, it would have declared the queue clear. A job that is cancelled and still listed fails the row.
func TestAStoppedRunsCleanupReadsForTheSubmissionWindow(t *testing.T) {
	const row = "alpha-athena-now-stage-qwen3-vl"
	t.Run("a late job inside the window is cancelled by its tag", func(t *testing.T) {
		w := newAlphaWorld(t)
		res := w.run([]string{"STUB_ALPHA_MODE=late-hidden-die", "STUB_HIDE_READS=20", "LIVE_CLEAN_QUIET_POLLS=3", "LIVE_CLEAN_UNRESOLVED_POLLS=2", "LIVE_CLEAN_WINDOW_SECONDS=6"}, "--run", row, "--yes")
		w.contains(res, "cleanup: the test was stopped before it finished, so a submission in flight may still register; waited for it", "cleanup: left behind 0 (cancelled 1)")
		if got := strings.TrimSpace(w.stateFile("cancelled")); !strings.Contains(got, "9003") {
			t.Errorf("the late job of the run's tag was not cancelled: %q", got)
		}
		if q := w.stateFile("queue"); strings.Contains(q, "9003|") {
			t.Errorf("the late job is still in the queue after a cleanup that said it left 0 behind:\n%s", q)
		}
		if reads, _ := strconv.Atoi(strings.TrimSpace(w.stateFile("late-reads"))); reads <= 20 {
			t.Errorf("the cleanup read the queue %d times, want more than the 20 that hid the job (a plain window of 5 would have ended long before)", reads)
		}
	})
	t.Run("a job still listed after the window fails the cleanup", func(t *testing.T) {
		w := newAlphaWorld(t)
		res := w.run([]string{"STUB_ALPHA_MODE=late-hidden-die", "STUB_HIDE_READS=2", "STUB_SCANCEL_KEEP=1", "LIVE_CLEAN_WINDOW_SECONDS=2", "LIVE_CLEAN_MAX_POLLS=2"}, "--run", row, "--yes")
		if res.code != 1 {
			t.Fatalf("exit %d, want 1:\n%s", res.code, res.out)
		}
		w.contains(res, "cleanup: left behind 1", "a job was left behind")
		if strings.Contains(res.out, "left behind 0") {
			t.Errorf("the cleanup said 0 while the job was listed:\n%s", res.out)
		}
	})
	t.Run("a run that finished by its own verdict keeps the plain window", func(t *testing.T) {
		w := newAlphaWorld(t)
		res := w.run([]string{"STUB_ALPHA_MODE=late-hidden", "STUB_HIDE_READS=20", "LIVE_CLEAN_QUIET_POLLS=3", "LIVE_CLEAN_UNRESOLVED_POLLS=2", "LIVE_CLEAN_WINDOW_SECONDS=6"}, "--run", row, "--yes")
		if strings.Contains(res.out, "was stopped before it finished") {
			t.Errorf("a run that ended on its own was treated as stopped:\n%s", res.out)
		}
	})
}

// The window the script gives a stopped run's cleanup is at least the submission window of every shipped profile (inflightWindow, whose framework default is 5 minutes and
// which athena-now.yaml sets at line 72): read from the profiles and the script, never written beside the script's number.
func TestTheStoppedCleanupWindowCoversTheProfilesSubmissionWindow(t *testing.T) {
	b, err := os.ReadFile(scriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^CLEAN_WINDOW_SECONDS=\$\{LIVE_CLEAN_WINDOW_SECONDS:-(\d+)\}$`).FindSubmatch(b)
	if m == nil {
		t.Fatal("the script has no CLEAN_WINDOW_SECONDS default")
	}
	window, _ := strconv.Atoi(string(m[1]))
	profiles, err := filepath.Glob(filepath.Join(repoRoot(t), "profiles", "*.yaml"))
	if err != nil || len(profiles) == 0 {
		t.Fatalf("no profiles (%v)", err)
	}
	seen := 0
	for _, f := range profiles {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var pp struct {
			Spec struct {
				InflightWindow string `json:"inflightWindow"`
			} `json:"spec"`
		}
		if err := yaml.Unmarshal(raw, &pp); err != nil || pp.Spec.InflightWindow == "" {
			continue
		}
		d, err := time.ParseDuration(pp.Spec.InflightWindow)
		if err != nil {
			t.Fatalf("%s: %v", filepath.Base(f), err)
		}
		seen++
		if window < int(d.Seconds()) {
			t.Errorf("the stopped cleanup's window is %d s, under %s's inflightWindow of %s", window, filepath.Base(f), pp.Spec.InflightWindow)
		}
	}
	if seen == 0 {
		t.Fatal("no profile names an inflightWindow, so this checked nothing")
	}
	// And the alpha package names the file the stack's state is written to, which the script reads.
	if !strings.Contains(string(b), alpha.StackStateFile) {
		t.Errorf("the script does not read %s", alpha.StackStateFile)
	}
}
