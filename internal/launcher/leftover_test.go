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
	"path/filepath"
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

// dirForms returns the spellings in which the kernel may show a working directory that was named
// dir: dir itself and, when a symbolic link lies on its path, dir with the links resolved. The
// kernel shows /proc/<pid>/cwd resolved, so a directory named through a link (a $TMPDIR that is a
// link, which shortDir takes) never matches the way it was named. A directory that has been removed
// since cannot be resolved as a whole; the part that still exists is, and the rest is added back.
func dirForms(dir string) []string {
	if dir == "" {
		return nil
	}
	forms := []string{dir}
	if r := resolveExisting(dir); r != dir {
		forms = append(forms, r)
	}
	return forms
}

// resolveExisting resolves the symbolic links in p, through its nearest ancestor that still exists.
func resolveExisting(p string) string {
	if r, err := filepath.EvalSymlinks(p); err == nil {
		return r
	}
	parent := filepath.Dir(p)
	if parent == p {
		return p
	}
	return filepath.Join(resolveExisting(parent), filepath.Base(p))
}

// cwdUnder is true when the process's working directory is dir or below it, however dir is spelled
// (see dirForms), also when dir has been removed since (the kernel then appends " (deleted)").
func cwdUnder(pid int, dir string) bool { return cwdUnderForms(pid, dirForms(dir)) }

// cwdUnderForms is cwdUnder for the spellings dirForms made, so a caller that asks about many
// processes resolves the directory once.
func cwdUnderForms(pid int, forms []string) bool {
	cwd, err := os.Readlink("/proc/" + strconv.Itoa(pid) + "/cwd")
	if err != nil {
		return false
	}
	cwd = strings.TrimSuffix(cwd, " (deleted)")
	for _, dir := range forms {
		if cwd == dir || strings.HasPrefix(cwd, dir+"/") {
			return true
		}
	}
	return false
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
	forms := dirForms(dir)
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
		if cwdUnderForms(r.pid, forms) {
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
			// Poll until the shell has both children and one of them leads a session of its own.
			// The second child is a forked shell until it has exec'd setsid and called setsid(), so
			// a single look taken as soon as both children exist can find neither leading a
			// session (issue #130).
			var sleeps []int
			var own int // the sleep that leads a session of its own
			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) {
				sleeps, own = sleeps[:0], 0
				for _, r := range procRows() {
					if r.ppid == shell {
						sleeps = append(sleeps, r.pid)
						if r.sid == r.pid {
							own = r.pid
						}
					}
				}
				if len(sleeps) == 2 && own != 0 {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if len(sleeps) != 2 {
				killTree([]int{shell}, dir)
				t.Fatalf("the shell started %d children, want 2", len(sleeps))
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

// symlinkedDir makes a real directory and, at a short path, a symbolic link to it. The link is not
// made in t.TempDir(): shortDir takes $TMPDIR only when it is under 60 bytes, and a link in a long
// directory would be ignored.
func symlinkedDir(t *testing.T) (link, real string) {
	t.Helper()
	parent, err := os.MkdirTemp("/tmp", "hbl")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(parent) })
	real, err = filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	link = filepath.Join(parent, "l")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	return link, real
}

// sleeperIn starts a sleep whose working directory is dir, killed at the end of the test. It leads a
// session of its own: killTree never touches the test's process group.
func sleeperIn(t *testing.T, dir string) int {
	t.Helper()
	cmd := exec.Command("sleep", "300")
	cmd.Dir = dir
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	})
	return cmd.Process.Pid
}

// TestCwdUnderSeesThroughASymbolicLink: the kernel shows a working directory resolved, so a directory
// named through a link must still be recognised, also below the link and after the directory below
// it was removed. The preconditions show the trap is there: the kernel's spelling is not the link's.
func TestCwdUnderSeesThroughASymbolicLink(t *testing.T) {
	link, real := symlinkedDir(t)
	pid := sleeperIn(t, link)
	if cwd, err := os.Readlink("/proc/" + strconv.Itoa(pid) + "/cwd"); err != nil || cwd != real || cwd == link {
		t.Fatalf("the kernel shows the working directory as %q (%v); want the resolved %q, which is not the link %q", cwd, err, real, link)
	}
	for name, c := range map[string]struct {
		dir  string
		want bool
	}{
		"the link":                 {link, true},
		"the real directory":       {real, true},
		"a directory it is not in": {filepath.Join(link, "sub"), false},
		"a sibling of the link":    {filepath.Join(filepath.Dir(link), "other"), false},
	} {
		if got := cwdUnder(pid, c.dir); got != c.want {
			t.Errorf("%s: cwdUnder(%s) = %v, want %v", name, c.dir, got, c.want)
		}
	}

	// A directory below the link, removed while a process is in it: the kernel shows
	// "<real>/sub (deleted)", and the directory named through the link cannot be resolved as a whole.
	if err := os.Mkdir(filepath.Join(real, "sub"), 0o700); err != nil {
		t.Fatal(err)
	}
	below := filepath.Join(link, "sub")
	pid2 := sleeperIn(t, below)
	if err := os.Remove(filepath.Join(real, "sub")); err != nil {
		t.Fatal(err)
	}
	if cwd, err := os.Readlink("/proc/" + strconv.Itoa(pid2) + "/cwd"); err != nil || cwd != filepath.Join(real, "sub")+" (deleted)" {
		t.Fatalf("the kernel shows %q (%v), want %q", cwd, err, filepath.Join(real, "sub")+" (deleted)")
	}
	if !cwdUnder(pid2, below) {
		t.Errorf("cwdUnder(%s) is false for a process in that directory, removed since", below)
	}
	if !cwdUnder(pid2, link) {
		t.Errorf("cwdUnder(%s) is false for a process below that directory", link)
	}
	if cwdUnder(pid2, filepath.Join(link, "other")) {
		t.Error("cwdUnder is true for a directory the process was never in")
	}
}

// TestKillTreeFindsAProcessUnderASymlinkedTMPDIR is the failure that was seen: with $TMPDIR a link,
// shortDir names its directories through it, the kernel shows the resolved spelling, and killTree,
// asked about the directory by working directory alone, found nothing. t.Setenv keeps this test
// serial.
func TestKillTreeFindsAProcessUnderASymlinkedTMPDIR(t *testing.T) {
	link, _ := symlinkedDir(t)
	t.Setenv("TMPDIR", link)
	dir := shortDir(t)
	if !strings.HasPrefix(dir, link+"/") {
		t.Fatalf("shortDir made %s, which is not under the link %s: the test would not go through the link", dir, link)
	}
	if r, err := filepath.EvalSymlinks(dir); err != nil || r == dir {
		t.Fatalf("%s resolves to %s (%v): it has no symbolic link on its path", dir, r, err)
	}
	pid := sleeperIn(t, dir)
	found := killTree(nil, dir)
	ok := false
	for _, f := range found {
		ok = ok || f == pid
	}
	if !ok {
		t.Errorf("killTree did not report pid %d, which has the directory as its working directory (found %v)", pid, found)
	}
	if runningNow(pid) {
		t.Errorf("pid %d still runs", pid)
	}
}
