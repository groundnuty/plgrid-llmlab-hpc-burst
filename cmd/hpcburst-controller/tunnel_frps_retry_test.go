package main

// The start of the real frps when a stranger holds one of its ports (issue #261).
//
// frps is a real process that binds its three ports (bind, tcpmux, web: the port base plus 9, 10
// and 11) itself, so the rig releases them just before it starts frps (releaseForFrps) and a
// stranger can take one in that window. On 3 Nov 2026 a CI run lost one that way: "listen tcp
// 127.0.0.1:<port>: bind: address already in use", frps exited at its start, and the test failed.
// startFrps now repeats a start that fails like that (exit status 1, and frps's own bind-error line
// in its output) on a fresh block, at most maxBindRetries times, each logged, and no start that
// fails any other way. The fresh block shares no port with any block the rig has tried, and the old
// block's cleanup closes only the old block's own addresses (tunnel_frps_block_test.go).
//
// Every test here makes the collision itself: beforeFrps binds the port at the offset under test
// after the rig has released it and before frps starts, so frps's bind finds it taken. A run in
// which no port is taken proves nothing about the retry.

import (
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// frpsStranger is a beforeFrps hook that binds one of frps's ports in the first attempt, and holds
// it until the test ends: a stranger that takes the port and keeps it, as a client socket of
// another process does for as long as its connection lives. It records the attempts it saw.
type frpsStranger struct {
	offset   int
	attempts []int
	addr     string // the address it holds
}

// installStranger sets beforeFrps to a stranger on the port at the offset, and puts the hook back
// at the end of the test. It returns the stranger; wrap beforeFrps afterwards to add to it.
func installStranger(t *testing.T, offset int) *frpsStranger {
	t.Helper()
	s := &frpsStranger{offset: offset}
	old := beforeFrps
	t.Cleanup(func() { beforeFrps = old })
	beforeFrps = func(t *testing.T, attempt int, f flags) {
		s.attempts = append(s.attempts, attempt)
		if attempt != 0 {
			return
		}
		ln, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(f.host.PortBase+offset)))
		if err != nil {
			t.Fatalf("the stranger cannot bind the port at offset %d: %v", offset, err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		s.addr = ln.Addr().String()
	}
	return s
}

// recordedLog is a logf that keeps what it was given, as well as passing it to the test's log.
type recordedLog struct {
	t     *testing.T
	lines []string
}

func (l *recordedLog) logf(format string, args ...any) {
	l.t.Helper()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
	l.t.Logf(format, args...)
}

// startWithRetries repeats an attempt that found a port taken, and only that: any other failure is
// returned at once, whatever came before it, and the repeats stop at maxBindRetries. The attempts
// are fakes, so the counts are exact.
func TestStartWithRetriesRepeatsOnlyAPortTaken(t *testing.T) {
	taken := fmt.Errorf("%w: frps exited at its start", errFrpsPortTaken)
	other := errors.New("frps exited at its start: exit status 1\nunknown field")
	for _, tc := range []struct {
		name        string
		results     []error // what attempt n returns; the last one repeats
		wantAttempt int
		wantRetries int
		wantErr     func(error) bool
	}{
		{"taken once, then it starts", []error{taken, nil}, 2, 1, func(err error) bool { return err == nil }},
		{"first attempt starts", []error{nil}, 1, 0, func(err error) bool { return err == nil }},
		{"another failure at once", []error{other}, 1, 0, func(err error) bool { return err == other }},
		{"taken, then another failure", []error{taken, other}, 2, 1, func(err error) bool { return err == other }},
		{"taken twice, then another failure", []error{taken, taken, other}, 3, 2, func(err error) bool { return err == other }},
		{"always taken", []error{taken}, maxBindRetries + 1, maxBindRetries, func(err error) bool {
			return errors.Is(err, errFrpsPortTaken) && strings.Contains(err.Error(), fmt.Sprintf("after %d retries", maxBindRetries))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var attempts, retried []int
			var logged []string
			err := startWithRetries(
				func(format string, args ...any) { logged = append(logged, fmt.Sprintf(format, args...)) },
				func(n int) error {
					attempts = append(attempts, n)
					return tc.results[min(n, len(tc.results)-1)]
				},
				func(n int) { retried = append(retried, n) })
			if !tc.wantErr(err) {
				t.Errorf("startWithRetries returned %v", err)
			}
			if len(attempts) != tc.wantAttempt {
				t.Errorf("%d attempts (%v), want %d", len(attempts), attempts, tc.wantAttempt)
			}
			if len(retried) != tc.wantRetries || len(logged) != tc.wantRetries {
				t.Errorf("%d fresh blocks (%v) and %d log lines, want %d of each", len(retried), retried, len(logged), tc.wantRetries)
			}
			for i, l := range logged {
				if want := fmt.Sprintf("retry %d of %d", i+1, maxBindRetries); !strings.Contains(l, want) {
					t.Errorf("log line %d is %q, want it to say %q", i+1, l, want)
				}
			}
		})
	}
}

// A stranger on any one of frps's three ports fails its start; the start is repeated on a fresh
// block and the retry is logged. The stranger still holds the old port afterwards, which shows the
// first attempt really did lose it, and frps's own log says why.
func TestFrpsStartIsRepeatedOnAFreshBlockWhenAStrangerHoldsAPort(t *testing.T) {
	for _, tc := range []struct {
		name   string
		offset int
	}{
		{"bind port", agentapi.FrpsBindPortOffset},
		{"tcpmux port", agentapi.FrpsTCPMuxPortOffset},
		{"web port", agentapi.FrpsWebPortOffset},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newTunnelRig(t)
			rec := &recordedLog{t: t}
			r.logf = rec.logf
			before := r.f.host
			st := installStranger(t, tc.offset)

			if b := r.startFrps(nil); b != nil {
				t.Fatalf("startFrps returned a binary though none was running")
			}
			if !slices.Equal(st.attempts, []int{0, 1}) {
				t.Fatalf("frps was started in attempts %v, want [0 1]: the first lost its port, the second did not", st.attempts)
			}
			if len(rec.lines) != 1 || !strings.Contains(rec.lines[0], fmt.Sprintf("retry 1 of %d", maxBindRetries)) || !strings.Contains(rec.lines[0], "address already in use") {
				t.Errorf("the retry was logged as %q, want one line naming retry 1 of %d and the cause", rec.lines, maxBindRetries)
			}
			after := r.f.host
			if after.PortBase == before.PortBase {
				t.Errorf("the port base is still %d: the retry did not move to a fresh block", before.PortBase)
			}
			if after.StateDir != before.StateDir {
				t.Errorf("the state directory changed from %s to %s", before.StateDir, after.StateDir)
			}
			if !listening(st.addr) || st.addr != net.JoinHostPort("127.0.0.1", strconv.Itoa(before.PortBase+tc.offset)) {
				t.Errorf("the stranger does not hold %d (the old port at offset %d), so frps's first start was not what lost it", before.PortBase+tc.offset, tc.offset)
			}
			for _, a := range []string{after.FrpsBindAddr(), after.FrpsTCPMuxAddr(), after.FrpsWebAddr()} {
				if !listening(a) {
					t.Errorf("frps does not listen on %s of the fresh block", a)
				}
			}
			if log := logFrom(launcher.FrpsLogFile(after), 0); !strings.Contains(log, "address already in use") {
				t.Errorf("frps's log has no 'address already in use': the first start failed for another reason\n%s", log)
			}
		})
	}
}

// With the binary running (as TestTunnelWithRealFrpsAndFrpc has it before frps), a fresh block
// means a fresh base for the binary too, since its plugin endpoint and the frps web address it
// reads are the base plus an offset. The test follows the whole path on the new block: the old
// binary has returned and its plugin is gone, the new one reads the new frps's list, and an frpc
// that logs in registers through the new plugin and turns the Instance's condition True.
func TestFrpsRetryRestartsTheRunningBinaryOnTheFreshBlock(t *testing.T) {
	setVars(t, &tunnelReadPeriod, 2*time.Second)
	r := newTunnelRig(t)
	allocA, credA := r.allocation("al-a", v1alpha1.AllocationRunning)
	a1 := r.instance("in-a1", allocA.Name, "")
	b := r.start()
	r.waitCond(b, a1, statusUnknown, reasonUnreadable) // frps is not running
	oldPlugin := r.f.host.FrpsPluginAddr()
	st := installStranger(t, agentapi.FrpsWebPortOffset)

	b2 := r.startFrps(b)
	if b2 == nil || b2 == b {
		t.Fatal("startFrps did not return a new binary after the retry")
	}
	select {
	case <-b.exited:
		if b.err != nil {
			t.Errorf("the old binary returned %v, want nil", b.err)
		}
	default:
		t.Error("the old binary still runs")
	}
	if listening(oldPlugin) {
		t.Errorf("the old plugin endpoint %s still listens", oldPlugin)
	}
	if !slices.Equal(st.attempts, []int{0, 1}) {
		t.Errorf("frps was started in attempts %v, want [0 1]", st.attempts)
	}
	r.waitCond(b2, a1, statusFalse, reasonOffline) // the new binary reads the new frps's list
	r.runFrpc(credA, uid(a1))
	r.waitCond(b2, a1, statusTrue, reasonOnline) // frps posted to the new plugin, which admitted it
	if err := b2.stop(t); err != nil {
		t.Fatalf("run returned %v after its context ended, want nil", err)
	}
}

// A start that fails for any other reason is not repeated, even after a repeat that was right. The
// log of frps is appended to, so a start that read all of it would see the first attempt's line in
// the second's failure and repeat again: one row breaks the second attempt's configuration (frps
// refuses an unknown key), and shows that its failure is returned at once while the file still
// holds the first attempt's "address already in use". Another row breaks the first attempt's
// configuration with a key that is the phrase itself: frps's complaint, "unknown field", quotes it,
// so the attempt's own output mentions "address already in use" and frps exits with status 1, as it
// does when it loses a port; it is not a bind failure, and is not repeated.
func TestFrpsStartIsNotRepeatedForAnyOtherFailure(t *testing.T) {
	for _, tc := range []struct {
		name      string
		stranger  bool   // a port is taken in attempt 0
		breakAt   int    // the attempt whose configuration is broken
		breakLine string // the line added to its configuration
		wantTries []int  // the attempts that ran
		mentions  string // what the failure's output must say, to show the phrase is in it
	}{
		{"the first attempt fails", false, 0, "noSuchSetting = 1", []int{0}, ""},
		{"the second attempt fails after a repeat", true, 1, "noSuchSetting = 1", []int{0, 1}, ""},
		{"the output of the attempt mentions the phrase", false, 0, `"address already in use" = 1`, []int{0}, `unknown field "address already in use"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newTunnelRig(t)
			rec := &recordedLog{t: t}
			r.logf = rec.logf
			orig := beforeFrps
			t.Cleanup(func() { beforeFrps = orig })
			if tc.stranger {
				installStranger(t, agentapi.FrpsWebPortOffset)
			}
			inner := beforeFrps
			var attempts []int
			beforeFrps = func(t *testing.T, attempt int, f flags) {
				attempts = append(attempts, attempt)
				if inner != nil {
					inner(t, attempt, f)
				}
				if attempt == tc.breakAt {
					breakFrpsConfig(t, f, tc.breakLine)
				}
			}
			var fresh []int
			err := startWithRetries(r.logf, r.tryFrps, func(n int) {
				fresh = append(fresh, n)
				r.freshBlock(nil)
			})
			if err == nil {
				t.Fatal("frps started with a broken configuration")
			}
			if errors.Is(err, errFrpsPortTaken) || !strings.Contains(err.Error(), "frps exited at its start") {
				t.Errorf("the failure is %v, want frps's exit at its start, not a taken port", err)
			}
			if strings.Contains(err.Error(), "bind: address already in use") {
				t.Errorf("the failure carries the bind error of an earlier attempt: %v", err)
			}
			if tc.mentions != "" && !strings.Contains(err.Error(), tc.mentions) {
				t.Errorf("the failure does not say %q, so the attempt's output does not mention the phrase and the row shows nothing: %v", tc.mentions, err)
			}
			if !slices.Equal(attempts, tc.wantTries) {
				t.Errorf("attempts %v, want %v", attempts, tc.wantTries)
			}
			if want := len(tc.wantTries) - 1; len(fresh) != want || len(rec.lines) != want {
				t.Errorf("%d fresh blocks and %d retries logged, want %d of each", len(fresh), len(rec.lines), want)
			}
			if tc.stranger {
				if log := logFrom(launcher.FrpsLogFile(r.f.host), 0); !strings.Contains(log, "address already in use") {
					t.Errorf("the log file lost the first attempt's line, so the test does not show what it is for\n%s", log)
				}
			}
		})
	}
}

// breakFrpsConfig adds a line to frps's configuration that frps does not know, which its strict
// parsing refuses: frps exits at its start, for a reason that is not a taken port.
func breakFrpsConfig(t *testing.T, f flags, line string) {
	t.Helper()
	fh, err := os.OpenFile(launcher.FrpsConfigFile(f.host), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()
	if _, err := fh.WriteString(line + "\n"); err != nil {
		t.Fatal(err)
	}
}

// TestFrpsExitErrorIsAPortTakenOnlyForABindLineAndExitStatusOne: an exit of frps is a lost port only for a line that is frps's own bind error
// for one of its three ports, in everything it wrote during the attempt, and the exit status 1 a
// failed bind gives. The lines are the ones frps 0.71.1 writes (checked by hand with a stranger on
// each port): the bind and tcpmux ports say "create server listener error, listen tcp A: bind:
// address already in use", the web server's says "listen tcp A: bind: address already in use".
func TestFrpsExitErrorIsAPortTakenOnlyForABindLineAndExitStatusOne(t *testing.T) {
	frpsPorts := []string{"127.0.0.1:40009", "127.0.0.1:40010", "127.0.0.1:40011"}
	startLine := "2026-11-08 01:06:03.024 [I] [frps/root.go:115] frps uses config file: /state/frps/frps.toml\n"
	filler := strings.Repeat("2026-11-08 01:06:03.024 [I] [server/service.go:210] an unrelated line of frps's log\n", 40) // over 1500 bytes
	for _, tc := range []struct {
		name   string
		out    string
		status string
		want   bool
	}{
		{"the bind port", startLine + "create server listener error, listen tcp 127.0.0.1:40009: bind: address already in use\n", "exit status 1", true},
		{"the tcpmux port", startLine + "create server listener error, listen tcp 127.0.0.1:40010: bind: address already in use\n", "exit status 1", true},
		{"the web port, in its own words", startLine + "listen tcp 127.0.0.1:40011: bind: address already in use\n", "exit status 1", true},
		{"no final newline", "listen tcp 127.0.0.1:40011: bind: address already in use", "exit status 1", true},
		{"the line is the first of more than 1500 bytes", "create server listener error, listen tcp 127.0.0.1:40009: bind: address already in use\n" + filler, "exit status 1", true},
		{"the right line, another exit status", startLine + "listen tcp 127.0.0.1:40011: bind: address already in use\n", "exit status 2", false},
		{"the right line, killed by a signal", startLine + "listen tcp 127.0.0.1:40011: bind: address already in use\n", "signal: killed", false},
		{"the phrase in another complaint", startLine + "json: unknown field \"address already in use\"\n", "exit status 1", false},
		{"the phrase at the end of a log line", startLine + "2026-11-08 01:06:03.024 [W] retrying: listen tcp 127.0.0.1:40009: bind: address already in use\n", "exit status 1", false},
		{"the phrase with text after it", startLine + "listen tcp 127.0.0.1:40009: bind: address already in use (config path)\n", "exit status 1", false},
		{"a bind error of a port that is not one of frps's three", startLine + "listen tcp 127.0.0.1:40008: bind: address already in use\n", "exit status 1", false},
		{"no bind error at all", startLine + "json: unknown field \"noSuchSetting\"\n", "exit status 1", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := frpsExitError(tc.out, tc.status, frpsPorts)
			if err == nil {
				t.Fatal("frpsExitError returned nil for an exit")
			}
			if got := errors.Is(err, errFrpsPortTaken); got != tc.want {
				t.Errorf("a port taken: %v, want %v (status %q, error %v)", got, tc.want, tc.status, err)
			}
			if !strings.Contains(err.Error(), "frps exited at its start") || !strings.Contains(err.Error(), tc.status) {
				t.Errorf("the error %q does not say that frps exited at its start with %q", err, tc.status)
			}
			if tc.want && !strings.Contains(err.Error(), "bind: address already in use") {
				t.Errorf("the error does not carry the bind line, so a retry's log would not name its cause: %v", err)
			}
		})
	}
}
