package fakeslurm

// New. Unit tests of the process table and the sweep (procfs.go,
// procsweep.go), B2, N1 and N2 of the PR 153 review. The kernel cannot be made
// to hand out a recycled PID or an empty listing on request, so most of these
// run the real code against a scripted procAPI or a /proc laid out by the
// test; the one that uses real processes checks that the signal goes to the
// right one. Expected values are written down by the test, not read from the
// code under test.

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// scripted is a procAPI that serves listings in turn (the last repeats) and
// records what it is asked, in order.
type scripted struct {
	listings [][]procRow
	listErr  error
	openErr  error // from open
	rowNow   func(pid int) (procRow, bool, error)
	sendErr  error
	lists    int
	events   []string
}

func (s *scripted) api() *procAPI {
	return &procAPI{
		list: func() ([]procRow, error) {
			i := min(s.lists, len(s.listings)-1)
			s.lists++
			return s.listings[i], s.listErr
		},
		row: func(pid int) (procRow, bool, error) {
			s.events = append(s.events, fmt.Sprintf("row %d", pid))
			return s.rowNow(pid)
		},
		open: func(pid int) (int, error) {
			s.events = append(s.events, fmt.Sprintf("open %d", pid))
			return pid + 1000, s.openErr
		},
		send: func(fd int, sig syscall.Signal) error {
			s.events = append(s.events, fmt.Sprintf("send %d %d", fd, int(sig)))
			return s.sendErr
		},
		closeFd: func(fd int) { s.events = append(s.events, fmt.Sprintf("close %d", fd)) },
	}
}

var (
	member = procRow{pid: 41, ppid: 1, sid: 40, ticks: 700}
	// sameNow answers a row read with the process the listing held.
	sameNow = func(int) (procRow, bool, error) { return member, true, nil }
)

// A listing that finds nothing is not believed on its own: a process forked
// after the last KILL can fork a child and exit between the directory read and
// its own stat read. The sweep ends only when two listings in a row are empty.
func TestSweepSessionConfirmsAnEmptyListing(t *testing.T) {
	s := &scripted{listings: [][]procRow{{member}, nil, {member}, nil, nil}, rowNow: sameNow}
	if err := sweepSession(s.api(), 40, time.Minute, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if s.lists != 5 {
		t.Errorf("the sweep listed %d times, want 5: it must not stop at the first empty listing", s.lists)
	}
	want := []string{"open 41", "row 41", "send 1041 9", "close 1041", "open 41", "row 41", "send 1041 9", "close 1041"}
	if strings.Join(s.events, ",") != strings.Join(want, ",") {
		t.Errorf("events %v, want a KILL through the pidfd for each of the two non-empty listings: %v", s.events, want)
	}
}

// What the sweep cannot see or cannot do is an error, never "none left".
func TestSweepSessionReportsWhatItCannotSee(t *testing.T) {
	unreadable := errors.New("table unreadable (scripted)")
	for _, c := range []struct {
		name    string
		s       *scripted
		limit   time.Duration
		wantErr string
	}{
		{"an unreadable process table", &scripted{listings: [][]procRow{nil}, listErr: unreadable}, time.Minute, "table unreadable"},
		{"a process that cannot be pinned", &scripted{listings: [][]procRow{{member}}, openErr: syscall.EPERM, rowNow: sameNow}, time.Minute, "pidfd_open of process 41"},
		{"a process that cannot be read", &scripted{listings: [][]procRow{{member}}, rowNow: func(int) (procRow, bool, error) { return procRow{}, false, unreadable }}, time.Minute, "read process 41"},
		{"a process that cannot be signalled", &scripted{listings: [][]procRow{{member}}, sendErr: syscall.EPERM, rowNow: sameNow}, time.Minute, "signal killed to process 41"},
		{"processes that survive KILL", &scripted{listings: [][]procRow{{member}}, rowNow: sameNow}, 30 * time.Millisecond, "1 processes of the job survive SIGKILL"},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := sweepSession(c.s.api(), 40, c.limit, time.Millisecond)
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("sweep returned %v, want an error holding %q", err, c.wantErr)
			}
		})
	}
}

// A member is signalled through a pidfd that was opened before its start time
// was read again, and only if it is still the process that was listed. The
// events are in the order the kernel was asked.
func TestSignalProcsPinsBeforeItChecksAndSignalsThroughThePidfd(t *testing.T) {
	recycled := func(int) (procRow, bool, error) {
		r := member
		r.ticks++ // another process has the PID now
		return r, true, nil
	}
	for _, c := range []struct {
		name    string
		s       *scripted
		want    string
		wantErr bool
	}{
		{"the same process", &scripted{rowNow: sameNow}, "open 41,row 41,send 1041 15,close 1041", false},
		{"the PID was recycled", &scripted{rowNow: recycled}, "open 41,row 41,close 1041", false},
		{"it ended after the pidfd", &scripted{rowNow: func(int) (procRow, bool, error) { return procRow{}, false, nil }}, "open 41,row 41,close 1041", false},
		{"it ended before the pidfd", &scripted{openErr: syscall.ESRCH}, "open 41", false},
		{"it ended before the signal", &scripted{rowNow: sameNow, sendErr: syscall.ESRCH}, "open 41,row 41,send 1041 15,close 1041", false},
		{"it cannot be pinned", &scripted{openErr: syscall.EPERM}, "open 41", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := signalProcs(c.s.api(), []procRow{member}, syscall.SIGTERM)
			if (err != nil) != c.wantErr || strings.Join(c.s.events, ",") != c.want {
				t.Errorf("events %v, error %v; want %q, error: %v", c.s.events, err, c.want, c.wantErr)
			}
		})
	}
}

// With real processes: the one listed with its true start time gets the
// signal, one listed with a start time that is not its own (a recycled PID)
// does not.
func TestSignalProcsKillsOnlyTheProcessThatWasListed(t *testing.T) {
	api := hostProc()
	start := func() *exec.Cmd {
		cmd := exec.Command("sleep", "120")
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
		return cmd
	}
	listed, stranger := start(), start()
	row := func(cmd *exec.Cmd) procRow {
		r, live, err := api.row(cmd.Process.Pid)
		if err != nil || !live {
			t.Fatalf("row of %d: live %v, %v", cmd.Process.Pid, live, err)
		}
		return r
	}
	rl, rs := row(listed), row(stranger)
	rs.ticks++ // listed with a start time that is not its own
	if err := signalProcs(api, []procRow{rl, rs}, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	_ = listed.Wait()
	if ws, ok := listed.ProcessState.Sys().(syscall.WaitStatus); !ok || !ws.Signaled() || ws.Signal() != syscall.SIGTERM {
		t.Errorf("the listed process ended %v, want killed by SIGTERM", listed.ProcessState)
	}
	if _, live, err := api.row(stranger.Process.Pid); !live || err != nil {
		t.Errorf("the process listed with a wrong start time: live %v, %v; want it left alone", live, err)
	}
}

// statLine is a line of /proc/<pid>/stat: the fields after the command name
// are zero except the ones the fake reads.
func statLine(pid int, comm, state string, ppid, sid int, ticks uint64, exit int) string {
	f := make([]string, 50)
	for i := range f {
		f[i] = "0"
	}
	f[0], f[1], f[3], f[19], f[49] = state, fmt.Sprint(ppid), fmt.Sprint(sid), fmt.Sprint(ticks), fmt.Sprint(exit)
	return fmt.Sprintf("%d (%s) %s\n", pid, comm, strings.Join(f, " "))
}

// fakeProc lays out a /proc with these entries (pid to stat content).
func fakeProc(t *testing.T, entries map[int]string) string {
	t.Helper()
	dir := t.TempDir()
	for pid, line := range entries {
		d := filepath.Join(dir, fmt.Sprint(pid))
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(d+"/stat", []byte(line), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// The process table is read for what it says, and a table that cannot be read
// is an error: an empty or missing /proc must not look like a host with no
// processes of the job.
func TestListProcs(t *testing.T) {
	self := os.Getpid()
	selfLine := statLine(self, "test", "S", 1, self, 5, 0)
	for _, c := range []struct {
		name    string
		dir     func(t *testing.T) string
		want    []procRow
		wantErr string
	}{
		{"a table with this program and others", func(t *testing.T) string {
			return fakeProc(t, map[int]string{
				self: selfLine,
				7:    statLine(7, "a) b (c d", "R", 1, 7, 12, 0), // a command name with spaces and parentheses
				8:    statLine(8, "zombie", "Z", 1, 7, 13, 0),    // not live
			})
		}, []procRow{{pid: self, ppid: 1, sid: self, ticks: 5}, {pid: 7, ppid: 1, sid: 7, ticks: 12}}, ""},
		{"a missing /proc", func(t *testing.T) string { return filepath.Join(t.TempDir(), "absent") }, nil, "read the process table"},
		{"an empty /proc", func(t *testing.T) string { return t.TempDir() }, nil, "does not list this program"},
		{"a table without this program", func(t *testing.T) string {
			return fakeProc(t, map[int]string{7: statLine(7, "a", "S", 1, 7, 1, 0)})
		}, nil, "does not list this program"},
		{"an entry that cannot be parsed", func(t *testing.T) string {
			return fakeProc(t, map[int]string{self: selfLine, 7: "7 (a) S 1 7\n"})
		}, nil, "fields after the command name"},
	} {
		t.Run(c.name, func(t *testing.T) {
			rows, err := listProcs(c.dir(t))
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Errorf("listProcs: %v, want an error holding %q", err, c.wantErr)
				}
				return
			}
			if err != nil || len(rows) != len(c.want) {
				t.Fatalf("listProcs: %v, %v; want %v", rows, err, c.want)
			}
			got := map[int]procRow{}
			for _, r := range rows {
				got[r.pid] = r
			}
			for _, w := range c.want {
				if got[w.pid] != w {
					t.Errorf("row of %d is %+v, want %+v", w.pid, got[w.pid], w)
				}
			}
		})
	}
}

// A zombie's wait status comes from field 52 of its stat.
func TestReadExitStatus(t *testing.T) {
	parts := strings.Fields(statLine(14, "sh", "Z", 1, 14, 1, 0)) // the pid, the name, then 50 fields
	dir := fakeProc(t, map[int]string{
		11: statLine(11, "sh", "Z", 1, 11, 1, 3<<8), // exit 3
		12: statLine(12, "sh", "Z", 1, 12, 1, 9),    // killed by SIGKILL
		13: statLine(13, "sh", "Z", 1, 13, 1, 0),    // exit 0
		14: strings.Join(parts[:2+45], " ") + "\n",  // 45 fields: no exit code
	})
	for pid, want := range map[int]func(syscall.WaitStatus) bool{
		11: func(w syscall.WaitStatus) bool { return w.Exited() && w.ExitStatus() == 3 },
		12: func(w syscall.WaitStatus) bool { return w.Signaled() && w.Signal() == syscall.SIGKILL },
		13: func(w syscall.WaitStatus) bool { return w.Exited() && w.ExitStatus() == 0 },
	} {
		if w, err := readExitStatus(dir, pid); err != nil || !want(w) {
			t.Errorf("exit status of %d: %v, %v", pid, w, err)
		}
	}
	for _, pid := range []int{14, 99} {
		if w, err := readExitStatus(dir, pid); err == nil {
			t.Errorf("exit status of %d: %v, want an error (too few fields, or no such process)", pid, w)
		}
	}
}
