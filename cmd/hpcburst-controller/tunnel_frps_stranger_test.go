package main

// The strangers that make frps's start fail, and what a test may assert about the starts that
// follow (issues #261 and #311).
//
// A fresh block is no safer than the first. The runner's ports are contended (the tests of other
// packages run in parallel, and the kernel hands out the same ports to client sockets), so a start
// on a fresh block can find a port taken too, and the rig then picks yet another block, as it is
// meant to. On 10 Nov 2026 a CI run failed on "frps was started in attempts [0 1 2], want [0 1]":
// the test had assumed that its fresh block was free. So a test here does not assert how many starts
// there were. It asserts what the retry guarantees (frpsAttemptProblems): the attempts are numbered
// in order, there were as many as the strangers forced and no more than the bound, and no block
// shares a port with an earlier one. The count the test itself makes certain of is the strangers':
// each one takes a port in its own attempt, so that attempt must lose, whatever the runner does.

import (
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// frpsStranger is a beforeFrps hook that binds one of frps's ports in each attempt of a set, and
// holds it until the test ends: a stranger that takes the port and keeps it, as a client socket of
// another process does for as long as its connection lives. It records every attempt it saw, and
// the block that attempt ran on.
type frpsStranger struct {
	offset   int             // the port's offset in a block
	take     []int           // the attempts in which a stranger takes the port
	attempts []int           // every attempt the hook saw, in order
	hosts    []agentapi.Host // the block of each of those attempts
	addrs    []string        // the address each stranger holds, in the order they took it
}

// installStranger sets beforeFrps to a stranger on the port at the offset in the first attempt.
func installStranger(t *testing.T, offset int) *frpsStranger {
	t.Helper()
	return installStrangers(t, offset, 0)
}

// installStrangers sets beforeFrps to a stranger on the port at the offset in each of the attempts
// in take, and puts the hook back at the end of the test. In an attempt after the first, the port is
// the fresh block's. It returns the stranger; wrap beforeFrps afterwards to add to it.
func installStrangers(t *testing.T, offset int, take ...int) *frpsStranger {
	t.Helper()
	s := &frpsStranger{offset: offset, take: take}
	old := beforeFrps
	t.Cleanup(func() { beforeFrps = old })
	beforeFrps = func(t *testing.T, attempt int, f flags) {
		s.attempts = append(s.attempts, attempt)
		s.hosts = append(s.hosts, f.host)
		if !slices.Contains(s.take, attempt) {
			return
		}
		ln, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(f.host.PortBase+offset)))
		if err != nil {
			t.Fatalf("the stranger cannot bind the port at offset %d in attempt %d: %v", offset, attempt, err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		s.addrs = append(s.addrs, ln.Addr().String())
	}
	return s
}

// checkRetries fails the test unless the starts of frps that the strangers forced went as the retry
// is meant to (frpsAttemptProblems), and every stranger took its port and still holds it, which
// shows that its attempt really lost the port. atLeast is the fewest attempts there can have been:
// the strangers' number, plus one when a start is to succeed after them. It stops the test when
// anything is wrong, since what a test goes on to read depends on it.
func (s *frpsStranger) checkRetries(t *testing.T, atLeast int) {
	t.Helper()
	bases := make([]int, len(s.hosts))
	for i, h := range s.hosts {
		bases[i] = h.PortBase
	}
	problems := frpsAttemptProblems(s.attempts, bases, atLeast, maxBindRetries+1)
	if len(s.addrs) != len(s.take) {
		problems = append(problems, fmt.Sprintf("%d strangers took their port, want %d (attempts %v): a stranger that did not take its port forced nothing", len(s.addrs), len(s.take), s.take))
	}
	for _, a := range s.addrs {
		if !listening(a) {
			problems = append(problems, fmt.Sprintf("the stranger no longer holds %s, so the start that lost it was not lost to the stranger", a))
		}
	}
	for _, p := range problems {
		t.Error(p)
	}
	if len(problems) > 0 {
		t.FailNow()
	}
}

// frpsAttemptProblems is what is wrong with the starts of frps a test saw: attempts are the
// numbers the hook was given, bases the port base of each one's block. A retry guarantees that the
// attempts are 0, 1, 2... in order, that there were at least atLeast of them (a retry that stops
// early leaves fewer: atLeast is how many the strangers forced) and at most atMost (the bound,
// maxBindRetries + 1), and that no block shares a port with an earlier one, however many fresh
// blocks the contention made it take. The ports of a block are the literal offsets of HBEP-6
// (blockOffsets), not read from the code under test.
func frpsAttemptProblems(attempts, bases []int, atLeast, atMost int) []string {
	if len(attempts) != len(bases) {
		return []string{fmt.Sprintf("%d attempts but %d blocks recorded: the record is out of step", len(attempts), len(bases))}
	}
	var out []string
	for i, a := range attempts {
		if a != i {
			out = append(out, fmt.Sprintf("the attempts are %v, want 0 to %d in order", attempts, len(attempts)-1))
			break
		}
	}
	if len(attempts) < atLeast {
		out = append(out, fmt.Sprintf("the retry stopped early: %d attempts (%v), want at least %d", len(attempts), attempts, atLeast))
	}
	if len(attempts) > atMost {
		out = append(out, fmt.Sprintf("%d attempts (%v), more than the bound of %d", len(attempts), attempts, atMost))
	}
	for j := range bases {
		for i := range j {
			if sharePort(bases[i], bases[j]) {
				out = append(out, fmt.Sprintf("attempt %d's block at %d shares a port with attempt %d's block at %d", j, bases[j], i, bases[i]))
			}
		}
	}
	return out
}

// checkFrpsLog fails the test unless frps's log holds at least want of "address already in use":
// each start that lost a port wrote its bind-error line, and the log is appended to, so it holds
// the lines of every attempt (the state directory, and so the file, does not change with the block).
func checkFrpsLog(t *testing.T, host agentapi.Host, want int) {
	t.Helper()
	log := logFrom(launcher.FrpsLogFile(host), 0)
	if got := strings.Count(log, "address already in use"); got < want {
		t.Errorf("frps's log has %d 'address already in use', want at least %d: a start that lost its port wrote one each\n%s", got, want, log)
	}
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

// checkRepeats fails the test unless the log holds one line for each of the repeats, line i naming
// "retry i of maxBindRetries" and the cause, frps's bind error.
func (l *recordedLog) checkRepeats(repeats int) {
	l.t.Helper()
	if len(l.lines) != repeats {
		l.t.Errorf("the retries were logged as %q, want %d lines, one for each repeat", l.lines, repeats)
		return
	}
	for i, line := range l.lines {
		if want := fmt.Sprintf("retry %d of %d", i+1, maxBindRetries); !strings.Contains(line, want) || !strings.Contains(line, "address already in use") {
			l.t.Errorf("log line %d is %q, want it to name %q and the cause", i+1, line, want)
		}
	}
}

// The checks of the attempts hold for any number of fresh blocks, and fail for what a retry must not
// do: stop early, go past the bound, skip a number, or return to a block it has left. The blocks are
// the literal ports of HBEP-6: 40000's are 40003, 40008 to 40012.
func TestFrpsAttemptProblems(t *testing.T) {
	for _, tc := range []struct {
		name            string
		attempts, bases []int
		atLeast, atMost int
		want            []string // a substring of each problem, in order; none for a clean record
	}{
		{"one retry", []int{0, 1}, []int{40000, 40100}, 2, 4, nil},
		{"a second block taken as well", []int{0, 1, 2}, []int{40000, 40100, 40200}, 2, 4, nil},
		{"the bound, as many as the strangers forced", []int{0, 1, 2, 3}, []int{40000, 40100, 40200, 40300}, 4, 4, nil},
		{"the nearest block that shares no port with another (13 above)", []int{0, 1}, []int{40000, 40013}, 2, 4, nil},
		{"no retry at all", []int{0}, []int{40000}, 2, 4, []string{"stopped early"}},
		{"a stop one short of the strangers", []int{0, 1}, []int{40000, 40100}, 3, 4, []string{"stopped early"}},
		{"past the bound", []int{0, 1, 2, 3, 4}, []int{40000, 40100, 40200, 40300, 40400}, 2, 4, []string{"more than the bound of 4"}},
		{"a number skipped", []int{0, 2}, []int{40000, 40100}, 2, 4, []string{"want 0 to 1 in order"}},
		{"a start that is not the first", []int{1, 2}, []int{40000, 40100}, 2, 4, []string{"want 0 to 1 in order"}},
		{"the same block again", []int{0, 1}, []int{40000, 40000}, 2, 4, []string{"shares a port"}},
		{"a block that overlaps the last one by a port", []int{0, 1}, []int{40000, 40009}, 2, 4, []string{"shares a port"}},
		{"a block that overlaps an earlier one, not the last", []int{0, 1, 2}, []int{40000, 40100, 40006}, 2, 4, []string{"attempt 2's block at 40006 shares a port with attempt 0's block at 40000"}},
		{"a record out of step", []int{0, 1}, []int{40000}, 2, 4, []string{"out of step"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := frpsAttemptProblems(tc.attempts, tc.bases, tc.atLeast, tc.atMost)
			if len(got) != len(tc.want) {
				t.Fatalf("problems %q, want %d, each naming %q", got, len(tc.want), tc.want)
			}
			for i, w := range tc.want {
				if !strings.Contains(got[i], w) {
					t.Errorf("problem %d is %q, want it to name %q", i, got[i], w)
				}
			}
		})
	}
}
