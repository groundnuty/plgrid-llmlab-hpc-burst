package launcher

// Leftover-process checks for the tests that start real processes. A test that starts a detached
// supervisor (own session) can leave it, and anything it started, running after a failure: the
// parent is then init, the working directory a deleted state directory, and nothing but /proc says
// the process exists. These helpers find such processes by what /proc shows (their working
// directory, their parents, their process group and session), not by the launcher's records, kill
// them all, and let the test fail if there were any.

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// procRow is one process as /proc/<pid>/stat shows it.
type procRow struct {
	pid, ppid, pgrp, sid int
	ticks                uint64
	state                byte
}

// procRows reads every process from /proc with a parser of its own (proc(5): the fields after the
// last ')' are state, ppid, pgrp, session, ... and, at index 19, the start time).
func procRows() []procRow {
	ents, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	var rows []procRow
	for _, e := range ents {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		b, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		if err != nil {
			continue // gone since the listing
		}
		s := string(b)
		f := strings.Fields(s[strings.LastIndexByte(s, ')')+1:])
		if len(f) < 20 {
			continue
		}
		ppid, _ := strconv.Atoi(f[1])
		pgrp, _ := strconv.Atoi(f[2])
		sid, _ := strconv.Atoi(f[3])
		ticks, _ := strconv.ParseUint(f[19], 10, 64)
		rows = append(rows, procRow{pid, ppid, pgrp, sid, ticks, f[0][0]})
	}
	return rows
}

// cwdUnder is true when the process's working directory is dir or below it, also when dir has been
// removed since (the kernel then appends " (deleted)").
func cwdUnder(pid int, dir string) bool {
	if dir == "" {
		return false
	}
	cwd, err := os.Readlink("/proc/" + strconv.Itoa(pid) + "/cwd")
	if err != nil {
		return false
	}
	cwd = strings.TrimSuffix(cwd, " (deleted)")
	return cwd == dir || strings.HasPrefix(cwd, dir+"/")
}

// killTree finds the live processes that belong to a test's fixture and kills them all with
// SIGKILL: the roots, whatever has dir as its working directory, and, repeatedly, every process
// whose parent, process group or session is one already found. The last two catch a child that was
// placed in its own group or session, and (with the working directory) one whose parent died
// first and left it to init. The set is taken in one snapshot before anything is killed. It waits
// until they are gone, and returns the PIDs it found. The test process and its own group are never
// touched.
func killTree(roots []int, dir string) []int {
	rows := procRows()
	self, selfPgrp := os.Getpid(), syscall.Getpgrp()
	live := func(r procRow) bool { return r.state != 'Z' && r.state != 'X' && r.pid != self && r.pgrp != selfPgrp }
	set := map[int]procRow{}
	for _, r := range rows {
		if !live(r) {
			continue
		}
		for _, root := range roots {
			if r.pid == root {
				set[r.pid] = r
			}
		}
		if cwdUnder(r.pid, dir) {
			set[r.pid] = r
		}
	}
	for changed := true; changed; {
		changed = false
		for _, r := range rows {
			if _, ok := set[r.pid]; ok || !live(r) {
				continue
			}
			_, byParent := set[r.ppid]
			_, byGroup := set[r.pgrp]
			_, bySession := set[r.sid]
			if byParent || byGroup || bySession {
				set[r.pid] = r
				changed = true
			}
		}
	}
	var found []int
	for pid, r := range set {
		found = append(found, pid)
		// The same process still: a recycled PID is left alone.
		if now, ok := ticksOf(pid); ok && now == r.ticks {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	}
	end := time.Now().Add(5 * time.Second)
	for _, pid := range found {
		for time.Now().Before(end) && runningNow(pid) {
			time.Sleep(10 * time.Millisecond)
		}
	}
	return found
}

// noLeftovers fails the test when any process still has dir as its working directory (or belongs
// to the group, session or tree of one that does), and kills them so that they do not outlive it.
func noLeftovers(t *testing.T, dir string) {
	t.Helper()
	if left := killTree(nil, dir); len(left) > 0 {
		t.Errorf("processes left running from %s (killed now): %v", dir, left)
	}
}

// TestKillTreeFindsAChildInItsOwnGroup is the check on the check: a shell starts two sleeps, one of
// them in a session of its own, and the first case kills the tree from the shell's PID while it is
// alive, the second after the shell died and left both to init, where only the working directory
// still links them.
func TestKillTreeFindsAChildInItsOwnGroup(t *testing.T) {
	for name, orphan := range map[string]bool{"killed from the shell": false, "orphaned": true} {
		t.Run(name, func(t *testing.T) {
			dir := shortDir(t)
			cmd := exec.Command("sh", "-c", "sleep 300 & setsid sleep 300 & wait")
			cmd.Dir = dir
			cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			go func() { _ = cmd.Wait(); close(done) }()
			shell := cmd.Process.Pid
			var sleeps []int
			deadline := time.Now().Add(10 * time.Second)
			for len(sleeps) < 2 && time.Now().Before(deadline) {
				sleeps = sleeps[:0]
				for _, r := range procRows() {
					if r.ppid == shell {
						sleeps = append(sleeps, r.pid)
					}
				}
				time.Sleep(10 * time.Millisecond)
			}
			if len(sleeps) != 2 {
				killTree([]int{shell}, dir)
				t.Fatalf("the shell started %d children, want 2", len(sleeps))
			}
			var own int // the sleep that leads a session of its own
			for _, r := range procRows() {
				if (r.pid == sleeps[0] || r.pid == sleeps[1]) && r.sid == r.pid {
					own = r.pid
				}
			}
			if own == 0 {
				killTree([]int{shell}, dir)
				t.Fatalf("no child of the shell has a session of its own (sleeps %v)", sleeps)
			}
			var found []int
			if orphan {
				_ = syscall.Kill(shell, syscall.SIGKILL)
				<-done
				found = killTree(nil, dir)
			} else {
				found = killTree([]int{shell}, "")
				<-done
			}
			for _, pid := range append([]int{shell}, sleeps...) {
				if runningNow(pid) {
					t.Errorf("pid %d still runs", pid)
				}
			}
			for _, want := range sleeps {
				ok := false
				for _, f := range found {
					ok = ok || f == want
				}
				if !ok {
					t.Errorf("killTree did not report pid %d (found %v; the one in its own session is %d)", want, found, own)
				}
			}
		})
	}
}
