package main

// The guard that the CLI tests put on every down (issue #264, PR #277 round): down must not call a
// listener on a recorded port foreign unless a test put that listener there. downGuardProblem
// judges one run. Two real-k3s tests do put listeners there on purpose (TestUpStatusDown, which
// corrupts k3s's recorded start time so that down must leave k3s running, and
// TestStatusAfterTheProcessIsKilled, whose decoys stand on k3s's ports after k3s is killed); they
// call cliExpectingForeign with the exact listeners, and every other down stays under the guard.
//
// The tests in this file need no k3s: the first judges output strings, the second runs the real
// command against a state directory whose one process is already gone.

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
)

// foreignWarning is the line down prints, once per listener, after every recorded process is
// stopped and the listener has outlasted the release wait. The earlier line, "... but not as a
// launched process: waiting up to ...", is a socket still being released, which down tolerates, so
// it does not match.
var foreignWarning = regexp.MustCompile(`warning: (\S+) listens on one of the recorded ports, but not as a launched process; left alone`)

// foreignWarnings returns the listeners that out names in final warnings, sorted.
func foreignWarnings(out string) []string {
	var got []string
	for _, m := range foreignWarning.FindAllStringSubmatch(out, -1) {
		got = append(got, m[1])
	}
	sort.Strings(got)
	return got
}

// downGuardProblem judges one CLI run and returns what is wrong with it, or "". Only down is
// judged. With no wantForeign, no foreign-listener warning is allowed: no test here leaves
// someone else's listener on a recorded port in a normal shutdown, so a warning names the host's
// own sockets, as k3s's own ports were named in every down of a supervised host until issue #264
// (down judged k3s's listeners while k3s was still to be stopped). With wantForeign, exactly
// those listeners, in down's "[::1]:port" form, must be named: a warning set that differs, or no
// warning at all, fails, so an expectation cannot hide a regression nor hold when nothing happens.
func downGuardProblem(args []string, out string, wantForeign []string) string {
	if len(args) == 0 || args[0] != "down" {
		return ""
	}
	got := foreignWarnings(out)
	if len(wantForeign) == 0 {
		if len(got) > 0 {
			return fmt.Sprintf("down called a listener on a recorded port foreign (%s), but no test leaves one there; the host's own ports were named until issue #264:\n%s", strings.Join(got, ", "), out)
		}
		return ""
	}
	want := slices.Sorted(slices.Values(wantForeign))
	if !slices.Equal(got, want) {
		return fmt.Sprintf("down named the foreign listeners %v, want exactly %v:\n%s", got, want, out)
	}
	return ""
}

// cliExpectingForeign is cli for a down that must leave listeners of the test's own on the
// recorded ports and say so: want lists them as down prints them. Use it only where the test puts
// those listeners there (or leaves them, as k3s's are left when down is made to refuse to stop it).
func cliExpectingForeign(t testing.TB, want []string, args ...string) cliResult {
	t.Helper()
	if len(want) == 0 {
		t.Fatal("cliExpectingForeign needs the listeners the down is expected to name; use cli for a normal shutdown")
	}
	return cliRun(t, "", want, args...)
}

func TestDownGuardJudgesTheForeignListenerWarnings(t *testing.T) {
	warn := func(listeners ...string) string {
		var b strings.Builder
		for _, l := range listeners {
			b.WriteString("warning: " + l + " listens on one of the recorded ports, but not as a launched process; left alone\n")
		}
		return b.String()
	}
	waiting := "127.0.0.1:1 listens on one of the recorded ports, but not as a launched process: waiting up to 5s for it to go\n"
	down := []string{"down", "--state-dir", "/x"}
	cases := []struct {
		name string
		args []string
		out  string
		want []string
		bad  bool
	}{
		{"a quiet down", down, "sent SIGTERM to k3s (pid 5)\n", nil, false},
		{"a down that warns, with nothing expected", down, "sent SIGTERM to k3s (pid 5)\n" + warn("127.0.0.1:29220"), nil, true},
		{"a down that warns about IPv6, with nothing expected", down, warn("[::1]:29221"), nil, true},
		{"only the waiting line: a socket that was being released", down, waiting, nil, false},
		{"a command other than down is not judged", []string{"status"}, warn("127.0.0.1:29220"), nil, false},
		{"no arguments are not judged", nil, warn("127.0.0.1:29220"), nil, false},
		{"the expected listeners, in another order", down, warn("[::1]:29221", "127.0.0.1:29220"), []string{"127.0.0.1:29220", "[::1]:29221"}, false},
		{"the expected listeners, plus the waiting line", down, waiting + warn("127.0.0.1:29220"), []string{"127.0.0.1:29220"}, false},
		{"an expected listener that is not named", down, warn("127.0.0.1:29220"), []string{"127.0.0.1:29220", "127.0.0.1:29221"}, true},
		{"a listener nobody expected", down, warn("127.0.0.1:29220", "127.0.0.1:29221"), []string{"127.0.0.1:29220"}, true},
		{"a different listener", down, warn("127.0.0.1:29222"), []string{"127.0.0.1:29220"}, true},
		{"nothing named, though something was expected", down, "sent SIGTERM to k3s (pid 5)\n" + waiting, []string{"127.0.0.1:29220"}, true},
		{"the same listener named twice", down, warn("127.0.0.1:29220", "127.0.0.1:29220"), []string{"127.0.0.1:29220"}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := downGuardProblem(c.args, c.out, c.want)
			if (got != "") != c.bad {
				t.Errorf("problem %q, want a problem: %v", got, c.bad)
			}
		})
	}
}

// recordingTB is a testing.TB that keeps its Errorf calls instead of failing the test, so a test
// can see that the guard in cliIn fires. Everything else is the real test's.
type recordingTB struct {
	testing.TB
	errs []string
}

func (r *recordingTB) Errorf(format string, a ...any) {
	r.errs = append(r.errs, fmt.Sprintf(format, a...))
}

// guardBase is the top port base (cli_test.go's TestUpAcceptsTheTopPortBase uses it too, binding
// nothing). A down records a block of 13 ports and looks at every listener on them, so the block
// must be one that no other test in the run binds: inside #264's 29740-29759 it would overlap the
// launcher tests' own recorded block (29740-29752), whose tests count the listeners on it, and
// 65523-65535 lies above the kernel's ephemeral ports. This file binds one port, the base.
const guardBase = 65523

// writeGoneHostRecord writes a launcher record in state whose only process is already gone: a
// child that has exited (its PID, a start time and the boot ID are all the record needs). down
// finds the process gone, stops nothing, and then looks for foreign listeners on the block.
func writeGoneHostRecord(t testing.TB, state string) {
	t.Helper()
	ports, err := launcher.PortsFromBase(guardBase)
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command("true")
	if err := child.Run(); err != nil {
		t.Fatal(err)
	}
	rec := launcher.State{
		Schema:  1, // the launcher's record schema; down refuses any other
		Version: "v0.0.0+example",
		Ports:   ports,
		Procs: []launcher.ProcRec{{Name: "k3s", PID: child.ProcessState.Pid(), StartTicks: 1,
			BootID: strings.TrimSpace(fileOf("/proc/sys/kernel/random/boot_id"))}},
		StartedAt: time.Now(),
	}
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(state, "state.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestCLIGuardOnARealDown runs the real command, through cliIn and cliExpectingForeign, against a
// record whose process is gone and a listener of this test on a recorded port: the one situation
// in which down must call a listener foreign. The guard must fail a normal down that warns, an
// expected warning must pass, and an expectation that nothing satisfies must fail.
func TestCLIGuardOnARealDown(t *testing.T) {
	addr := "127.0.0.1:" + itoa(guardBase)
	state := filepath.Join(shortTmp(t, false), "st")
	down := []string{"down", "--state-dir", state}
	listen := func(t *testing.T) net.Listener {
		t.Helper()
		l, err := net.Listen("tcp", addr)
		if err != nil {
			t.Fatalf("cannot hold %s: %v", addr, err)
		}
		t.Cleanup(func() { l.Close() })
		return l
	}

	t.Run("a normal down with nothing on the ports passes", func(t *testing.T) {
		writeGoneHostRecord(t, state)
		rec := &recordingTB{TB: t}
		if r := cliIn(rec, "", down...); r.code != exitOK || len(rec.errs) != 0 {
			t.Errorf("exit %d, guard errors %q", r.code, rec.errs)
		}
	})
	t.Run("a normal down that warns about a foreign listener fails the guard", func(t *testing.T) {
		listen(t)
		writeGoneHostRecord(t, state)
		rec := &recordingTB{TB: t}
		r := cliIn(rec, "", down...)
		if r.code != exitOK {
			t.Errorf("down exited %d", r.code)
		}
		if len(rec.errs) != 1 || !strings.Contains(rec.errs[0], addr) {
			t.Errorf("the guard did not fire exactly once, naming %s: %q", addr, rec.errs)
		}
		if got := foreignWarnings(r.out + r.err); !slices.Equal(got, []string{addr}) {
			t.Errorf("down named %v, want exactly [%s]", got, addr)
		}
	})
	t.Run("a down that is expected to warn about that listener passes", func(t *testing.T) {
		l := listen(t)
		writeGoneHostRecord(t, state)
		rec := &recordingTB{TB: t}
		if r := cliExpectingForeign(rec, []string{addr}, down...); r.code != exitOK || len(rec.errs) != 0 {
			t.Errorf("exit %d, guard errors %q", r.code, rec.errs)
		}
		if c, err := net.DialTimeout("tcp", addr, 2*time.Second); err != nil {
			t.Errorf("down did not leave the listener alone: %v", err)
		} else {
			c.Close()
		}
		l.Close()
	})
	t.Run("an expected warning that does not come fails", func(t *testing.T) {
		writeGoneHostRecord(t, state)
		rec := &recordingTB{TB: t}
		if r := cliExpectingForeign(rec, []string{addr}, down...); r.code != exitOK || len(rec.errs) != 1 {
			t.Errorf("exit %d, guard errors %q; want one", r.code, rec.errs)
		}
	})
}
