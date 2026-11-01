package launcher

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// These tests force the interleavings behind issue #154 on a fake /proc tree and fake pidfds, so no
// load, sleep or real process is involved. A process that exits closes its descriptors (exit_files)
// before its pidfd fires, and its leader reads Z for a moment before that too, so the launcher can
// see a process whose pidfd lives, whose /proc entry reads Z and whose sockets are already gone.
// The pidfd is the only authority on death (handle.go; HBEP-11, Operational readiness, "Silent
// failure"), so checkEntry must report the process as not running as soon as any look at its pidfd
// finds it exited, and must never blame the exit on the sockets or the parent it read in between.

// exiting makes the child look as the kernel shows a process in its last moments: its descriptors
// are closed (so it holds no socket) and its leader reads Z, while its pidfd has not fired.
func (s *supervisedFakeHost) exiting() {
	s.t.Helper()
	writeFakeProc(s.t, s.root, s.childRec.PID, "Z", s.supRec.PID, s.childRec.StartTicks)
	if err := os.RemoveAll(filepath.Join(s.root, strconv.Itoa(s.childRec.PID), "fd")); err != nil {
		s.t.Fatal(err)
	}
}

// exitsAtLook makes the child's pidfd say "exited" from its k-th look on, and returns how many
// looks have been made. A look is any exited check on any handle, so look 1 is the supervisor's
// handle being opened in checkChild; the child is then already gone when checkEntry starts. k = 0
// never flips it.
func (s *supervisedFakeHost) exitsAtLook(k int) (looks func() int) {
	n := 0
	s.f.onExited = func() {
		if n++; n == k {
			s.f.setAlive(s.childRec.PID, false)
		}
	}
	s.t.Cleanup(func() { s.f.onExited = nil })
	return func() int { return n }
}

// noUDPSockets gives the fake /proc an empty UDP table. A process with a fixed set of ports is
// checked against every reachable socket, UDP included (treeBound), and a missing table is an
// error, not an empty one.
func (s *supervisedFakeHost) noUDPSockets() {
	s.t.Helper()
	const header = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ref pointer drops\n"
	if err := os.WriteFile(filepath.Join(s.root, "net", "udp"), []byte(header), 0o644); err != nil {
		s.t.Fatal(err)
	}
}

// checkChild runs checkEntry on the supervisor's recorded child, as status does.
func (s *supervisedFakeHost) checkChild() (EntryHealth, []string) {
	s.t.Helper()
	sup, v, d := openRec(s.supRec)
	if v != Ours {
		s.t.Fatalf("the fake supervisor is %v: %s", v, d)
	}
	defer sup.Close()
	e := EntryRecord{Name: "controller", Health: s.health, Ports: s.ports, State: entryRunning, Proc: &s.childRec}
	return checkEntry(sup, s.supRec.PID, e)
}

// TestCheckEntryProcessExitsDuringTheCheck: the pidfd fires at each look in turn, and the answer
// is "not running" every time, with Running false. Before #154's fix, a pidfd that fired between
// openRec and the second identity check gave "not a child of the supervisor ... its parent is pid
// 0", one that fired while the sockets were read gave "does not hold its health address" (or, with
// fixed ports, "expected listener ... is missing"), and one that fired after the answer gave
// "changed while it was being checked", all with Running true.
func TestCheckEntryProcessExitsDuringTheCheck(t *testing.T) {
	// holding is what a process whose pidfd lives at every look is reported as when it holds
	// everything it should.
	holding := func(t *testing.T, eh EntryHealth, problems []string) {
		if len(problems) != 0 || !eh.Running || eh.HealthzOK == nil || !*eh.HealthzOK {
			t.Fatalf("%+v, problems %v", eh, problems)
		}
	}
	cases := []struct {
		name  string
		setup func(*supervisedFakeHost)
		alive func(*testing.T, EntryHealth, []string)
	}{
		{"holding its health address", func(*supervisedFakeHost) {}, holding},
		{"holding its health address and its fixed port", func(s *supervisedFakeHost) {
			// The shape of frps (HBEP-6): the entry names the exact ports the process must hold. Here
			// it is the health port alone, which the fake child holds, so the control finds nothing
			// wrong and the check reaches treeBound and verifyExactListeners. When the pidfd fires
			// while treeBound reads the sockets, they read as none, and the missing listener must
			// not be blamed on a process that is gone.
			port, err := parsePort(s.health)
			if err != nil {
				s.t.Fatal(err)
			}
			s.ports = []int{port}
			s.noUDPSockets()
		}, holding},
		{"exiting: leader reads Z, descriptors closed", (*supervisedFakeHost).exiting, func(t *testing.T, eh EntryHealth, problems []string) {
			// The pidfd says it lives, and /proc is no authority on death: it is reported by
			// what it lacks. This is the answer the original subtest got while its wait read
			// /proc and the pidfd had not fired.
			if !eh.Running || !strings.Contains(strings.Join(problems, "; "), "does not hold its health address") {
				t.Fatalf("%+v, problems %v", eh, problems)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The control: the pidfd lives at every look. It also counts the looks.
			ctl := newSupervisedFakeHost(t)
			tc.setup(ctl)
			looks := ctl.exitsAtLook(0)
			eh, problems := ctl.checkChild()
			tc.alive(t, eh, problems)
			n := looks()
			if n < 3 {
				t.Fatalf("only %d look(s) at the pidfd in a whole check", n)
			}
			for k := 1; k <= n; k++ {
				t.Run(fmt.Sprintf("the pidfd fires at look %d of %d", k, n), func(t *testing.T) {
					s := newSupervisedFakeHost(t)
					tc.setup(s)
					looks := s.exitsAtLook(k)
					eh, problems := s.checkChild()
					if looks() < k {
						t.Fatalf("the check made %d look(s) and never reached look %d, so nothing was forced", looks(), k)
					}
					got := strings.Join(problems, "; ")
					if !strings.Contains(got, "pid 4344 is not running: pid 4344 has exited") || eh.Running {
						t.Errorf("a process whose pidfd fired is not reported as not running, with Running false: %+v, problems %v", eh, problems)
					}
					for _, wrong := range []string{"does not hold", "not a child of the supervisor", "after the answer", "changed while", "expected listener", "which it must not", "other processes listen"} {
						if strings.Contains(got, wrong) {
							t.Errorf("the exit is blamed on %q: %+v, problems %v", wrong, eh, problems)
						}
					}
				})
			}
		})
	}
}

// TestCheckEntryProcEntryGoneWhilePidfdLives is the interleaving the issue first guessed: the
// child's /proc entry reads as gone while its pidfd still lives. The pidfd is not contradicted by
// a failed read (handle.go, statWhileHeld), so the answer is "not running" with the reason, and
// it was before the fix too: this is the control that the guess is not the cause.
func TestCheckEntryProcEntryGoneWhilePidfdLives(t *testing.T) {
	s := newSupervisedFakeHost(t)
	if err := os.RemoveAll(filepath.Join(s.root, strconv.Itoa(s.childRec.PID))); err != nil {
		t.Fatal(err)
	}
	eh, problems := s.checkChild()
	got := strings.Join(problems, "; ")
	if eh.Running || !strings.Contains(got, "pid 4344 is not running") || !strings.Contains(got, "its pidfd says it lives, but /proc/4344/stat cannot be read") {
		t.Fatalf("%+v, problems %v", eh, problems)
	}
}
