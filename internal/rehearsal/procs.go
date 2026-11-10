package rehearsal

// New, after internal/slurm/fakeslurm/procfs.go and procsweep.go: the
// processes below a given one, read from /proc, and the signals the nodes send
// them. sshd's session child calls setsid, so it leaves sshd's process group;
// the nodes find what to stop by parentage and verify each process by its
// start time, so a recycled PID is left alone.
//
// Issue #308. An sshd that had exited before its row was read left a row of
// PID 0, the walk from PID 0 returned every process on the host (PID 1 and the
// kernel's threads have parent 0), and the stop sent SIGTERM to all of them:
// the likely cause of the development VM's outage of 10 Nov 2026. Three
// guards now stand between a sweep and a process the test did not start, and
// each is enough alone to stop that walk's signals:
//  1. spawn (sshd.go) stores no sshd whose row it cannot read: it kills that
//     one through its os.Process, which holds a pidfd, and reaps it;
//  2. below walks nothing from a root of PID 1 or less, nor from a root whose
//     start time no longer matches;
//  3. signalProcs refuses PID 1 and below, the process itself and its
//     ancestors, and any process that is neither below it nor in its cgroup,
//     and returns the refusal as an error. It pins each process with a pidfd
//     before it checks it, as fakeslurm's signalOne does, so the signal goes
//     to the process that was checked.

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// procRow is one live process: its ID, parent and start time in clock ticks
// since boot, which tells a recycled PID from the same process.
type procRow struct {
	pid, ppid int
	ticks     uint64
}

// procAPI is what the nodes ask of the kernel. hostProcs is the host's; a test
// replaces single fields to script a row, a cgroup or an exited sshd, and
// records the signals in place of sending them. Every signal the nodes send
// goes through send or kill.
type procAPI struct {
	// self is the PID of the process the nodes run in.
	self int
	// row reads one process: live is false for one that has exited, a zombie
	// included, and err is set for one whose row exists but cannot be read.
	row func(pid int) (r procRow, live bool, err error)
	// cgroup is the content of /proc/<pid>/cgroup.
	cgroup func(pid int) (string, error)
	// open pins a process with a pidfd; ESRCH when it is gone, and EINVAL
	// for one that is mid-exit on some kernels (fakeslurm, issue #211).
	open func(pid int) (fd int, err error)
	// probe is kill(pid, 0), sent only after open answered EINVAL.
	probe func(pid int) error
	// send signals the process the pidfd names.
	send    func(fd int, sig syscall.Signal) error
	closeFd func(fd int)
	// kill ends an sshd the pool started, through its os.Process (a pidfd
	// since go 1.23), never by its number.
	kill func(p *os.Process) error
	// started, when set, is called with each sshd's PID after it starts and
	// before its row is read (tests).
	started func(pid int)
}

// hostProcs is the kernel's own /proc.
func hostProcs() *procAPI {
	return &procAPI{
		self:    os.Getpid(),
		row:     statRow,
		cgroup:  readCgroup,
		open:    func(pid int) (int, error) { return unix.PidfdOpen(pid, 0) },
		probe:   func(pid int) error { return syscall.Kill(pid, 0) },
		send:    func(fd int, sig syscall.Signal) error { return unix.PidfdSendSignal(fd, sig, nil, 0) },
		closeFd: func(fd int) { _ = unix.Close(fd) },
		kill: func(p *os.Process) error {
			if err := p.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				return err
			}
			return nil
		},
	}
}

func readCgroup(pid int) (string, error) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/cgroup")
	return string(b), err
}

// statRow parses /proc/<pid>/stat. The fields after the command name, which
// may hold spaces and parentheses, start after the last ')'. A zombie, a dead
// process and one that is gone are not live; a row that exists but cannot be
// read or parsed is an error.
func statRow(pid int) (procRow, bool, error) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	switch {
	case errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH):
		return procRow{}, false, nil
	case err != nil:
		return procRow{}, false, err
	}
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return procRow{}, false, fmt.Errorf("/proc/%d/stat has no command name", pid)
	}
	// f[0] is the state (field 3), f[1] the parent, f[19] the start time.
	f := strings.Fields(s[i+1:])
	if len(f) < 20 {
		return procRow{}, false, fmt.Errorf("/proc/%d/stat has %d fields after the command name, want at least 20", pid, len(f))
	}
	if f[0] == "Z" || f[0] == "X" {
		return procRow{}, false, nil
	}
	ppid, err1 := strconv.Atoi(f[1])
	ticks, err2 := strconv.ParseUint(f[19], 10, 64)
	if err1 != nil || err2 != nil {
		return procRow{}, false, fmt.Errorf("/proc/%d/stat has an unreadable parent or start time", pid)
	}
	return procRow{pid: pid, ppid: ppid, ticks: ticks}, true, nil
}

// readProcRow is statRow with a row that cannot be read taken as not live, so
// it is never listed or walked.
func readProcRow(pid int) (procRow, bool) {
	r, live, err := statRow(pid)
	return r, live && err == nil
}

// liveProcs lists every live process on the host.
func liveProcs() []procRow {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	var rows []procRow
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		if r, ok := readProcRow(pid); ok {
			rows = append(rows, r)
		}
	}
	return rows
}

// below returns the live processes under root, transitively, root excluded.
func below(root procRow) []procRow { return belowIn(liveProcs(), root) }

// belowIn is below over a listing. It walks nothing from a root of PID 1 or
// less, since every process descends from PID 1 and PID 1 and the kernel's
// threads have parent 0, nor from a root that is not in the listing with its
// recorded start time: that one has exited, or its PID names another process
// now, whose children are not the root's (issue #308).
func belowIn(rows []procRow, root procRow) []procRow {
	if root.pid <= 1 {
		return nil
	}
	found := false
	for _, r := range rows {
		if r.pid == root.pid && r.ticks == root.ticks {
			found = true
			break
		}
	}
	if !found {
		return nil
	}
	in := map[int]bool{root.pid: true}
	var out []procRow
	for changed := true; changed; {
		changed = false
		for _, r := range rows {
			if !in[r.pid] && in[r.ppid] {
				in[r.pid] = true
				out = append(out, r)
				changed = true
			}
		}
	}
	return out
}

// withRelayEnv returns the live processes that hold relayPath in their
// environment: the forced command of the login node, wherever it now sits in
// the process tree. The path is the node's own socket, in a private
// directory; anything but an absolute path selects nothing.
func withRelayEnv(relayPath string) []procRow {
	if !strings.HasPrefix(relayPath, "/") {
		return nil
	}
	want := []byte(EnvRelay + "=" + relayPath)
	var out []procRow
	for _, r := range liveProcs() {
		b, err := os.ReadFile("/proc/" + strconv.Itoa(r.pid) + "/environ")
		if err != nil {
			continue
		}
		for _, kv := range splitNUL(b) {
			if string(kv) == string(want) {
				out = append(out, r)
				break
			}
		}
	}
	return out
}

func splitNUL(b []byte) [][]byte {
	var out [][]byte
	start := 0
	for i, c := range b {
		if c == 0 {
			out = append(out, b[start:i])
			start = i + 1
		}
	}
	if start < len(b) {
		out = append(out, b[start:])
	}
	return out
}

// maxDepth bounds a walk up the parents, which a listing that changes while
// it is read must not make endless.
const maxDepth = 4096

// owner is who the signals are for: the process the nodes run in, its
// cgroup, and its ancestors.
type owner struct {
	api *procAPI
	// cgroup is the process's /proc/<pid>/cgroup, or "" when it cannot be read
	// or names only the root, which holds too much to identify the test.
	cgroup    string
	ancestors map[int]bool
}

// ownerOf reads the process's ancestors, up to and excluding PID 1. An
// ancestry that cannot be read is an error: an ancestor that is not known
// could be signalled, so every signal is refused.
func ownerOf(api *procAPI) (*owner, error) {
	o := &owner{api: api, ancestors: map[int]bool{}}
	if cg, err := api.cgroup(api.self); err == nil && !rootCgroup(cg) {
		o.cgroup = cg
	}
	pid := api.self
	for depth := 0; ; depth++ {
		r, live, err := api.row(pid)
		switch {
		case err != nil:
			return nil, fmt.Errorf("the ancestry of process %d: %w", api.self, err)
		case !live:
			return nil, fmt.Errorf("the ancestry of process %d: process %d is not live", api.self, pid)
		case depth >= maxDepth:
			return nil, fmt.Errorf("the ancestry of process %d is deeper than %d", api.self, maxDepth)
		}
		if r.ppid <= 1 {
			return o, nil
		}
		o.ancestors[r.ppid] = true
		pid = r.ppid
	}
}

// rootCgroup reports whether a /proc/<pid>/cgroup names no cgroup but the
// root ("/") on any line: the root holds the whole host, or a whole
// container, and says nothing about which test a process belongs to.
func rootCgroup(cg string) bool {
	for _, l := range strings.Split(strings.TrimSpace(cg), "\n") {
		if p := strings.SplitN(l, ":", 3); len(p) == 3 && p[2] != "/" {
			return false
		}
	}
	return true
}

// owns reports whether a live process is the test's: below it now, by its
// parents, or in its cgroup, which a process keeps when its parent exits and
// it is reparented away (withRelayEnv's forced commands).
func (o *owner) owns(r procRow) bool {
	pid := r.ppid
	for depth := 0; depth < maxDepth && pid > 1; depth++ {
		if pid == o.api.self {
			return true
		}
		p, live, err := o.api.row(pid)
		if err != nil || !live {
			break
		}
		pid = p.ppid
	}
	if o.cgroup == "" {
		return false
	}
	cg, err := o.api.cgroup(r.pid)
	return err == nil && cg == o.cgroup
}

// refusal is the error for a process signalProcs will not signal.
func refusal(r procRow, sig syscall.Signal, why string) error {
	return fmt.Errorf("rehearsal: refused to send %v to process %d: %s (issue #308)", sig, r.pid, why)
}

// signalProcs sends sig to each listed process that is still the one listed
// and is the test's own. Each is pinned with a pidfd first and checked after
// that: a PID that now names a process with another start time was recycled
// and is left alone, and the signal goes through the pidfd, never to a PID. It
// refuses PID 1 and below, the process itself, its ancestors, and a process
// that is neither below it nor in its cgroup. A refusal, and a process that
// cannot be pinned, read or signalled, is an error, and the others are still
// signalled. It returns the rows it neither refused nor failed on.
func signalProcs(api *procAPI, rows []procRow, sig syscall.Signal) ([]procRow, error) {
	o, err := ownerOf(api)
	if err != nil {
		return nil, fmt.Errorf("rehearsal: refused to send %v to %d processes: %w (issue #308)", sig, len(rows), err)
	}
	var kept []procRow
	var errs []error
	for _, r := range rows {
		if err := o.signalOne(r, sig); err != nil {
			errs = append(errs, err)
			continue
		}
		kept = append(kept, r)
	}
	return kept, errors.Join(errs...)
}

func (o *owner) signalOne(r procRow, sig syscall.Signal) error {
	switch {
	case r.pid <= 1:
		return refusal(r, sig, "PID 1 and below are the host's")
	case r.pid == o.api.self:
		return refusal(r, sig, "it is the process that sends it")
	}
	fd, err := o.api.open(r.pid)
	if errors.Is(err, syscall.ESRCH) {
		return nil // it ended since the listing
	}
	if errors.Is(err, syscall.EINVAL) {
		// A process that exits between the listing and the pidfd can answer EINVAL, not ESRCH (fakeslurm,
		// issue #211). EINVAL is taken as "ended" only when a re-check agrees.
		gone, cerr := o.endedSince(r.pid)
		switch {
		case cerr != nil:
			return fmt.Errorf("rehearsal: pidfd_open of process %d: %w (re-check: %v)", r.pid, err, cerr)
		case gone:
			return nil
		}
	}
	if err != nil {
		return fmt.Errorf("rehearsal: pidfd_open of process %d: %w", r.pid, err)
	}
	defer o.api.closeFd(fd)
	now, live, err := o.api.row(r.pid)
	switch {
	case err != nil:
		return fmt.Errorf("rehearsal: read process %d: %w", r.pid, err)
	case !live || now.ticks != r.ticks:
		return nil // gone, or the PID names another process now
	case o.ancestors[r.pid]:
		return refusal(r, sig, "it is an ancestor of the process that sends it")
	case !o.owns(now):
		return refusal(r, sig, "it is neither below the process that sends it nor in its cgroup")
	}
	if err := o.api.send(fd, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("rehearsal: send %v to process %d: %w", sig, r.pid, err)
	}
	return nil
}

// endedSince re-checks a listed process after pidfd_open answered EINVAL: it
// has ended when kill(pid, 0) says no such process, or when its row shows it
// is not live. One that kill(0) finds and whose row is live has not ended, and
// a re-check that cannot be made is an error.
func (o *owner) endedSince(pid int) (bool, error) {
	switch err := o.api.probe(pid); {
	case errors.Is(err, syscall.ESRCH):
		return true, nil
	case err != nil && !errors.Is(err, syscall.EPERM):
		return false, fmt.Errorf("kill(%d, 0): %w", pid, err)
	}
	_, live, err := o.api.row(pid)
	if err != nil {
		return false, fmt.Errorf("read process %d: %w", pid, err)
	}
	return !live, nil
}

// anyLive reports whether any listed process is still the one listed.
func anyLive(rows []procRow) bool {
	for _, r := range rows {
		if now, ok := readProcRow(r.pid); ok && now.ticks == r.ticks {
			return true
		}
	}
	return false
}
