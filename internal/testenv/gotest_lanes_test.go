package testenv

// hack/go-test.sh's lanes (issue #295): "own" with no group runs the groups of different packages at the same
// time and the groups of one package one after another. The tests run the real script with a go double, as
// gotest_script_test.go does. What a lane test waits on is observable state, never a window: the double of
// laneDouble holds each group until the lane of EVERY package has started, so a script that ran the lanes
// one at a time would wait for ever, and the double gives up after twenty seconds and says so.

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// lanePackages are the packages of the groups, in table order: one lane each.
func lanePackages() []string {
	var pkgs []string
	for _, g := range groups {
		if !slices.Contains(pkgs, g.pkg) {
			pkgs = append(pkgs, g.pkg)
		}
	}
	return pkgs
}

// groupsIn are the package's groups, in table order.
func groupsIn(pkg string) []group {
	var in []group
	for _, g := range groups {
		if g.pkg == pkg {
			in = append(in, g)
		}
	}
	return in
}

// wantWarmUp is the call the script makes before its lanes: one go test of every group's package with
// -exec true, which compiles and links the test binaries and runs none of them (the lanes then do not each
// compile the same dependencies at the same time).
func wantWarmUp() string {
	return "test -count=1 -exec true " + strings.Join(lanePackages(), " ")
}

// ownCallProblems compares the calls of an "own" run with what lanes make: the warm-up once and first when
// it is wanted and not at all when it is not, every group's call exactly once, and the groups of each
// package in table order. The order between packages is not compared.
func ownCallProblems(calls []string, wantWarm bool) []string {
	var problems []string
	var warm, group []string
	for _, c := range calls {
		if strings.Contains(c, " -exec ") {
			warm = append(warm, c)
		} else {
			group = append(group, c)
		}
	}
	switch {
	case wantWarm && (len(warm) != 1 || warm[0] != wantWarmUp() || calls[0] != warm[0]):
		problems = append(problems, fmt.Sprintf("want the warm-up %q once, first; got %q", wantWarmUp(), warm))
	case !wantWarm && len(warm) != 0:
		problems = append(problems, fmt.Sprintf("want no warm-up; got %q", warm))
	}
	for _, g := range groups {
		if n := countOf(group, g.wantCall()); n != 1 {
			problems = append(problems, fmt.Sprintf("group %s was run %d times, want once", g.name, n))
		}
	}
	if len(group) != len(groups) {
		problems = append(problems, fmt.Sprintf("%d group calls, want %d", len(group), len(groups)))
	}
	for _, pkg := range lanePackages() {
		var got, want []string
		for _, c := range group {
			if strings.HasSuffix(c, " "+pkg) {
				got = append(got, c)
			}
		}
		for _, g := range groupsIn(pkg) {
			want = append(want, g.wantCall())
		}
		if !slices.Equal(got, want) {
			problems = append(problems, fmt.Sprintf("the calls for %s are %q, want %q: a package's groups run in table order", pkg, got, want))
		}
	}
	return problems
}

func countOf(list []string, s string) int {
	n := 0
	for _, x := range list {
		if x == s {
			n++
		}
	}
	return n
}

// laneDouble is go for the lane tests. Every call is logged. A -skip or -exec call exits at once. A group's
// call (-run RE, the package last) marks that its lane has started and waits until the lane of every
// package has (LD_KEYS), and, when LD_HOLD is 1, until the test creates "release". While it runs it holds
// its package's slot, a directory that only one call may make at a time: a second group of the same
// package starting meanwhile finds the slot taken and logs an overlap. It then answers as a passing group.
// A wait that runs out (about 20 s) logs a problem and goes on. The 0.2 s after the wait is no condition of
// the test: it keeps the first call in its slot a moment longer, so that a script which started two groups
// of one package together is caught; a correct script never meets the second call, whatever the time.
const laneDouble = `#!/bin/sh
printf '%s\n' "$*" >> "$LD/calls"
case "$*" in *-skip*|*-exec*) exit 0 ;; esac
run=''
prev=''
for a in "$@"; do
  [ "$prev" = -run ] && run=$a
  prev=$a
  last=$a
done
key=$(printf '%s' "$last" | tr -c 'a-z0-9' '_')
mkdir "$LD/slot-$key" 2>/dev/null || printf 'overlap %s\n' "$last" >> "$LD/problems"
printf 'start %s %s\n' "$last" "$run" >> "$LD/events"
: > "$LD/started-$key"
printf '%s\n' "$$" > "$LD/pid-$$"
i=0
while :; do
  missing=0
  for k in $LD_KEYS; do [ -e "$LD/started-$k" ] || missing=1; done
  if [ "$LD_HOLD" = 1 ] && [ ! -e "$LD/release" ]; then missing=1; fi
  [ "$missing" -eq 0 ] && break
  i=$((i + 1))
  if [ "$i" -gt 1000 ]; then printf 'waited 20 s: %s still not all started\n' "$last" >> "$LD/problems"; break; fi
  sleep 0.02
done
sleep 0.2
printf 'end %s %s\n' "$last" "$run" >> "$LD/events"
rmdir "$LD/slot-$key"
printf '%s\n' "$run" | sed -e 's/^\^(//' -e 's/)\$$//' | tr '|' '\n' | while read -r n; do
  printf '=== RUN   %s\n--- PASS: %s (0.01s)\n' "$n" "$n"
done
printf 'PASS\nok  \texample/%s\t0.1s\n' "$last"
`

// laneKey is how the double names a package's markers.
func laneKey(pkg string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			return r
		}
		return '_'
	}, pkg)
}

// laneScript is a started run of the real script's "own" with the lane double.
type laneScript struct {
	t     *testing.T
	cmd   *exec.Cmd
	out   *bytes.Buffer
	state string // the double's directory: calls, events, problems, started-*, pid-*
	token string // in the environment of the script and every process under it
}

// startLaneScript starts "own" in the real repository with the lane double as go. With hold the double
// ends no group until release.
func startLaneScript(t *testing.T, hold bool) *laneScript {
	t.Helper()
	bin, state := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(laneDouble), 0o700); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, pkg := range lanePackages() {
		keys = append(keys, laneKey(pkg))
	}
	holdValue := "0"
	if hold {
		holdValue = "1"
	}
	l := &laneScript{t: t, out: &bytes.Buffer{}, state: state, token: "lane-test-" + strconv.FormatInt(time.Now().UnixNano(), 36)}
	l.cmd = exec.Command("bash", filepath.Join(realRoot(t), "hack", "go-test.sh"), "own")
	l.cmd.Env = append(os.Environ(), "GITHUB_ACTIONS=", "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"LD="+state, "LD_KEYS="+strings.Join(keys, " "), "LD_HOLD="+holdValue, "LD_TOKEN="+l.token)
	l.cmd.Stdout, l.cmd.Stderr = l.out, l.out
	l.cmd.WaitDelay = outlivedWait
	if err := l.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// Let any double that is still waiting end, whatever the test did.
		_ = os.WriteFile(filepath.Join(state, "release"), nil, 0o600)
		_ = l.cmd.Process.Kill()
		_ = l.cmd.Wait()
	})
	return l
}

// wait waits for the script to exit and returns its exit code.
func (l *laneScript) wait() int {
	l.t.Helper()
	began := time.Now()
	err := l.cmd.Wait()
	if time.Since(began) >= outlivedWait {
		l.t.Errorf("a process the script started outlived it and held its output open for %s", outlivedWait)
	}
	if ee, ok := err.(*exec.ExitError); ok {
		return ee.ExitCode()
	}
	if err != nil {
		l.t.Logf("wait: %v", err)
	}
	return 0
}

// lines are the lines of one of the double's files; none when it is missing.
func (l *laneScript) lines(name string) []string {
	l.t.Helper()
	raw, err := os.ReadFile(filepath.Join(l.state, name))
	if err != nil || len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	return strings.Split(strings.TrimSpace(string(raw)), "\n")
}

// awaitStarted waits until the first group of every lane is running.
func (l *laneScript) awaitStarted() {
	l.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		all := true
		for _, pkg := range lanePackages() {
			if _, err := os.Stat(filepath.Join(l.state, "started-"+laneKey(pkg))); err != nil {
				all = false
			}
		}
		if all {
			return
		}
		if time.Now().After(deadline) {
			l.t.Fatalf("the lanes did not all start within 60 s; started: %q; output so far:\n%s", l.lines("events"), l.out.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Every lane runs at the same time, and the groups of one package one at a time, in table order.
func TestGoTestScriptOwnRunsLanesAtOnceAndEachLaneOneGroupAtATime(t *testing.T) {
	l := startLaneScript(t, false)
	if code := l.wait(); code != 0 {
		t.Fatalf("exit %d, want 0; output:\n%s", code, l.out)
	}
	// The double waits for every lane's first group to have started, so a script that ran the lanes one at
	// a time would leave "waited" in the problems; and it logs an overlap when a package's slot is taken.
	if problems := l.lines("problems"); len(problems) != 0 {
		t.Errorf("the double met: %q (a waited line: the lanes did not run at the same time; an overlap: two groups of one package did)", problems)
	}
	events := l.lines("events")
	if len(events) < len(lanePackages()) {
		t.Fatalf("the double logged %d events, want at least one start for each of %d lanes", len(events), len(lanePackages()))
	}
	// All lanes started before any group ended: the first events are one start for each package.
	var first []string
	for _, e := range events[:len(lanePackages())] {
		f := strings.Fields(e)
		if f[0] != "start" {
			t.Errorf("the first %d events are not all starts: %q", len(lanePackages()), events)
		}
		first = append(first, f[1])
	}
	if !sameSet(first, lanePackages()) {
		t.Errorf("the first starts are of %q, want one for each of %q", first, lanePackages())
	}
	// Per package, the events alternate start, end, in table order of the groups.
	for _, pkg := range lanePackages() {
		var got, want []string
		for _, e := range events {
			if f := strings.Fields(e); f[1] == pkg {
				got = append(got, f[0]+" "+f[2])
			}
		}
		for _, g := range groupsIn(pkg) {
			run := anchored(g.tests)
			want = append(want, "start "+run, "end "+run)
		}
		if !slices.Equal(got, want) {
			t.Errorf("the events of %s are\n%s\nwant\n%s", pkg, strings.Join(got, "\n"), strings.Join(want, "\n"))
		}
	}
	calls := l.lines("calls")
	for _, p := range ownCallProblems(calls, true) {
		t.Errorf("%s; calls:\n%s", p, strings.Join(calls, "\n"))
	}
	if strings.Contains(l.out.String(), "::group::") {
		t.Errorf("the output has ::group:: lines outside GitHub Actions:\n%s", l.out)
	}
	// The table at the end has a row for every group, each ok.
	for _, g := range groups {
		if !regexp.MustCompile(`(?m)^ok +` + regexp.QuoteMeta(g.name) + ` +` + regexp.QuoteMeta(g.pkg) + ` +\+\d+s\.\.\+\d+s, \d+s of ` + g.timeout + `$`).MatchString(l.out.String()) {
			t.Errorf("the table has no ok row for group %s; output:\n%s", g.name, l.out)
		}
	}
}

func sameSet(a, b []string) bool {
	x, y := slices.Clone(a), slices.Clone(b)
	slices.Sort(x)
	slices.Sort(y)
	return slices.Equal(x, y)
}

// A failing group fails the step, and the output names it. It stops neither the rest of its lane nor the
// other lanes, and the exit is the first failing group's in table order.
func TestGoTestScriptOwnAFailingLaneFailsTheStepAndNamesItsGroup(t *testing.T) {
	root := realRoot(t)
	for _, c := range []struct {
		name     string
		fail     map[string]string // group -> go test's exit code
		nothing  string            // a group whose output is empty, as when -run matches nothing
		wantCode int
		named    []string
	}{
		{"the first group of a lane", map[string]string{"lease": "1"}, "", 1, []string{"lease"}},
		{"a middle group of the longest lane", map[string]string{"lifecycle-copies": "5"}, "", 5, []string{"lifecycle-copies"}},
		{"a lane of one group", map[string]string{"envoy-timeouts": "4"}, "", 4, []string{"envoy-timeouts"}},
		{"the last lane", map[string]string{"livechecks-dry": "6"}, "", 6, []string{"livechecks-dry"}},
		{"two lanes: the first in table order sets the exit", map[string]string{"controller-failover": "3", "envoy-timeouts": "4"}, "", 3, []string{"controller-failover", "envoy-timeouts"}},
		{"two groups of one lane", map[string]string{"lease": "2", "lifecycle-control-plane": "7"}, "", 2, []string{"lease", "lifecycle-control-plane"}},
		{"a group that ran nothing", nil, "livechecks-hostdialed", 1, []string{"livechecks-hostdialed"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			d := allPassed()
			for name, code := range c.fail {
				d.outputs[name], d.codes[name] = "--- FAIL: "+groupNamed(t, name).tests[0]+" (1.00s)\nFAIL\n", code
			}
			if c.nothing != "" {
				delete(d.outputs, c.nothing)
			}
			out, code, calls := runGoTestScript(t, root, d, "own")
			if code != c.wantCode {
				t.Errorf("exit %d, want %d; output:\n%s", code, c.wantCode, out)
			}
			// Every group still ran: a failure stops neither its lane nor another.
			for _, p := range ownCallProblems(calls, true) {
				t.Errorf("%s; go called\n%s", p, strings.Join(calls, "\n"))
			}
			// The step's own line names the groups that failed, in table order.
			if want := "FAIL hack/go-test.sh: own: the groups that failed: " + strings.Join(c.named, " "); !strings.Contains(out, want) {
				t.Errorf("the output has no line %q:\n%s", want, out)
			}
			for _, g := range groups {
				tag := "ok"
				if slices.Contains(c.named, g.name) {
					tag = "FAIL"
				}
				if !regexp.MustCompile(`(?m)^` + tag + ` +` + regexp.QuoteMeta(g.name) + ` `).MatchString(out) {
					t.Errorf("the table has no %s row for group %s:\n%s", tag, g.name, out)
				}
			}
		})
	}
}

// In GitHub Actions a lane whose groups all passed is folded, and a lane with a failure is not, so that
// the failure is in view; an annotation names the failed groups.
func TestGoTestScriptOwnFoldsPassingLanesInGitHubActionsAndLeavesAFailedOneOpen(t *testing.T) {
	root := realRoot(t)
	d := allPassed()
	d.outputs["envoy-timeouts"], d.codes["envoy-timeouts"] = "--- FAIL: "+envoyTimeouts[0]+" (1.00s)\nFAIL\n", "1"
	out, code, _ := runGoTestScriptEnv(t, root, d, []string{"GITHUB_ACTIONS=true"}, "own")
	if code != 1 {
		t.Errorf("exit %d, want 1; output:\n%s", code, out)
	}
	for _, pkg := range lanePackages() {
		var names []string
		for _, g := range groupsIn(pkg) {
			names = append(names, g.name)
		}
		header := "::group::own: lane " + pkg + " (" + strings.Join(names, ", ") + ")"
		folded := strings.Contains(out, header)
		if want := pkg != "./internal/agent"; folded != want {
			t.Errorf("lane %s folded: %v, want %v; output:\n%s", pkg, folded, want, out)
		}
	}
	if n, m := strings.Count(out, "::group::"), strings.Count(out, "::endgroup::"); n != 3 || m != 3 {
		t.Errorf("%d ::group:: and %d ::endgroup:: lines, want 3 and 3 (the three lanes that passed)", n, m)
	}
	if want := "::error title=hack/go-test.sh own::the groups that failed: envoy-timeouts"; !strings.Contains(out, want) {
		t.Errorf("no annotation %q:\n%s", want, out)
	}
}

// The warm-up is the script's own build step: its failure fails the step by name, and the lanes still run.
func TestGoTestScriptOwnFailsByNameWhenTheWarmUpFailsAndStillRunsTheLanes(t *testing.T) {
	d := allPassed()
	d.warmUpCode = "1"
	out, code, calls := runGoTestScript(t, realRoot(t), d, "own")
	if code != 1 {
		t.Errorf("exit %d, want 1: every group passed, but the warm-up did not; output:\n%s", code, out)
	}
	if want := "FAIL hack/go-test.sh: the warm-up build of the groups' packages failed with status 1"; !strings.Contains(out, want) {
		t.Errorf("the output has no line %q:\n%s", want, out)
	}
	for _, p := range ownCallProblems(calls, true) {
		t.Errorf("%s; go called\n%s", p, strings.Join(calls, "\n"))
	}
}

// A TERM sent to the script stops every lane: the exit is 128 + 15, no process a lane started is left
// running when the script exits, no group starts after the signal, and the output says which groups did
// not end.
func TestGoTestScriptOwnStopsEveryLaneOnTermAndLeavesNothingRunning(t *testing.T) {
	if _, err := os.Stat("/proc/self/environ"); err != nil {
		t.Skip("reads /proc to find a process the script left behind")
	}
	l := startLaneScript(t, true)
	l.awaitStarted()
	// The test started the script and holds its Process, so this signal reaches that process.
	if err := l.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if code := l.wait(); code != 143 {
		t.Errorf("exit %d, want 143 (128 + TERM); output:\n%s", code, l.out)
	}
	// Every double the lanes ran has ended: a pid file names it, and /proc names the process only while it
	// runs and carries this run's token (a recycled number would not).
	pids, err := filepath.Glob(filepath.Join(l.state, "pid-*"))
	if err != nil || len(pids) != len(lanePackages()) {
		t.Fatalf("pid files %q (%v), want one for each of the %d lanes", pids, err, len(lanePackages()))
	}
	deadline := time.Now().Add(20 * time.Second)
	for _, f := range pids {
		pid := strings.TrimPrefix(filepath.Base(f), "pid-")
		for l.runsWithToken(pid) {
			if time.Now().After(deadline) {
				t.Errorf("process %s of a lane still runs after the script exited", pid)
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	// No group started after the signal: only each lane's first group was ever called.
	var started []string
	for _, c := range l.lines("calls") {
		if !strings.Contains(c, " -exec ") {
			started = append(started, c)
		}
	}
	if len(started) != len(lanePackages()) {
		t.Errorf("go was called for %d groups, want the %d first groups of the lanes alone:\n%s", len(started), len(lanePackages()), strings.Join(started, "\n"))
	}
	// A group that did not end is a failure in the table, whether its lane was running it or had not reached it.
	for _, g := range groups {
		if wantRow := fmt.Sprintf("FAIL  %s ", g.name); !strings.Contains(l.out.String(), wantRow) {
			t.Errorf("no row %q:\n%s", wantRow, l.out)
		}
	}
	if !strings.Contains(l.out.String(), "the group did not end") {
		t.Errorf("the partial output does not say that a group did not end:\n%s", l.out)
	}
}

// runsWithToken reports whether process pid is alive with this run's token in its environment.
func (l *laneScript) runsWithToken(pid string) bool {
	raw, err := os.ReadFile(filepath.Join("/proc", pid, "environ"))
	return err == nil && bytes.Contains(raw, []byte("LD_TOKEN="+l.token))
}
