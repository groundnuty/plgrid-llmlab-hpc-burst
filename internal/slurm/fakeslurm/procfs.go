package fakeslurm

// New, after internal/launcher's leftover test (killTree): the processes of a
// run-mode job, read from /proc. Slurm tracks a job's processes in a cgroup;
// the fake tracks the session the batch shell leads, which every descendant
// stays in unless it calls setsid, plus the descendants of its members, which
// catches one that did while its parent lives.
//
// What /proc says is never taken for "none" when it could not be read: every
// reader returns an error for a listing or a process it cannot read, and the
// caller treats that as unknown (the job is not Finished).

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// procRow is one live process: its ID, parent, session and start time in
// clock ticks since boot, which tells a recycled PID from the same process.
type procRow struct {
	pid, ppid, sid int
	ticks          uint64
}

// procAPI is what the fake asks of the kernel. The Platform holds the host's;
// a test replaces single fields to script a listing, an error or a PID that
// was recycled, which a real kernel cannot be made to do on demand.
type procAPI struct {
	// list returns every live process. An error means the listing is unknown,
	// not that it is empty.
	list func() ([]procRow, error)
	// row reads one process; live is false for one that has exited, a zombie
	// included.
	row func(pid int) (r procRow, live bool, err error)
	// exitStatus is the wait status of a zombie, from /proc.
	exitStatus func(pid int) (syscall.WaitStatus, error)
	// open pins a process with a pidfd; ESRCH when it is gone. send signals
	// the process the pidfd names, whatever the PID has become since.
	open    func(pid int) (fd int, err error)
	send    func(fd int, sig syscall.Signal) error
	closeFd func(fd int)
	// scanned, when set, is called with the batch shell's PID at the start of
	// every scan of a job's session (tests).
	scanned func(leader int)
}

// hostProc is the kernel's own /proc.
func hostProc() *procAPI { return procAt("/proc") }

// procAt reads the process table from dir, which is /proc or, in a test, a
// directory laid out like it.
func procAt(dir string) *procAPI {
	return &procAPI{
		list:       func() ([]procRow, error) { return listProcs(dir) },
		row:        func(pid int) (procRow, bool, error) { return readProcRow(dir, pid) },
		exitStatus: func(pid int) (syscall.WaitStatus, error) { return readExitStatus(dir, pid) },
		open:       func(pid int) (int, error) { return unix.PidfdOpen(pid, 0) },
		send:       func(fd int, sig syscall.Signal) error { return unix.PidfdSendSignal(fd, sig, nil, 0) },
		closeFd:    func(fd int) { _ = unix.Close(fd) },
	}
}

// statFields reads /proc/<pid>/stat and returns the fields after the command
// name (which may hold spaces and parentheses, so they start after the last
// ')'): f[0] is the state (field 3 of proc(5)), f[1] the parent, f[3] the
// session, f[19] the start time and f[49] the exit code (field 52). gone is
// true for a process that has exited; any other failure is an error.
func statFields(dir string, pid int) (f []string, gone bool, err error) {
	b, err := os.ReadFile(dir + "/" + strconv.Itoa(pid) + "/stat")
	switch {
	case errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH):
		return nil, true, nil // exited before or while it was read
	case err != nil:
		return nil, false, err
	}
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return nil, false, fmt.Errorf("%s/%d/stat has no command name: %q", dir, pid, s)
	}
	f = strings.Fields(s[i+1:])
	if len(f) < 20 {
		return nil, false, fmt.Errorf("%s/%d/stat has %d fields after the command name, want at least 20", dir, pid, len(f))
	}
	return f, false, nil
}

// readProcRow parses /proc/<pid>/stat. A zombie or a dead process is not
// live.
func readProcRow(dir string, pid int) (procRow, bool, error) {
	f, gone, err := statFields(dir, pid)
	if gone || err != nil || f[0] == "Z" || f[0] == "X" {
		return procRow{}, false, err
	}
	ppid, err1 := strconv.Atoi(f[1])
	sid, err2 := strconv.Atoi(f[3])
	ticks, err3 := strconv.ParseUint(f[19], 10, 64)
	if err := errors.Join(err1, err2, err3); err != nil {
		return procRow{}, false, fmt.Errorf("%s/%d/stat: %w", dir, pid, err)
	}
	return procRow{pid: pid, ppid: ppid, sid: sid, ticks: ticks}, true, nil
}

// readExitStatus is the wait status of a zombie (field 52 of its stat, since
// Linux 3.5): what waitpid would return, which the kernel keeps until the
// parent reaps it.
func readExitStatus(dir string, pid int) (syscall.WaitStatus, error) {
	f, gone, err := statFields(dir, pid)
	switch {
	case err != nil:
		return 0, err
	case gone:
		return 0, fmt.Errorf("process %d is gone, so it has no exit status to read", pid)
	case len(f) < 50:
		return 0, fmt.Errorf("%s/%d/stat has %d fields after the command name; the exit status (field 52) needs Linux 3.5", dir, pid, len(f))
	}
	code, err := strconv.ParseUint(f[49], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("%s/%d/stat exit code: %w", dir, pid, err)
	}
	return syscall.WaitStatus(code), nil
}

// listProcs lists every live process. It fails when /proc cannot be read, or
// when the listing lacks this program itself: an empty /proc (not mounted, or
// hidden) must not read as "no process of the job is left".
func listProcs(dir string) ([]procRow, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read the process table: %w", err)
	}
	var rows []procRow
	self := false
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		r, live, err := readProcRow(dir, pid)
		if err != nil {
			return nil, err
		}
		if live {
			rows = append(rows, r)
			self = self || pid == os.Getpid()
		}
	}
	if !self {
		return nil, fmt.Errorf("read the process table: %s does not list this program (pid %d); is /proc mounted?", dir, os.Getpid())
	}
	return rows, nil
}
