package launcher

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// Process handles. A PID is only a number: the kernel gives it to the next process once the
// recorded one has exited and been reaped. A pidfd is a handle on one process. It never comes to
// mean another one, a signal sent through it reaches that process or nobody, and it turns
// readable once the whole process (every thread) has exited. So the launcher decides "alive" and
// "ours" only through a pidfd it holds:
//
//  1. take a pidfd for the recorded PID (pidfd_open; at launch, from clone itself);
//  2. read the start time from /proc/<pid>/stat;
//  3. check that the pidfd has not exited, after that read.
//
// If it has not, the process cannot have been reaped, so its PID cannot have been reused, so the
// /proc entry read in step 2 was its own. A matching start time and boot ID then make it the
// recorded process, and the held pidfd pins it for every signal sent afterwards. Death is read
// from the pidfd only, never from a /proc read that failed or came back empty.

// pidfdAPI is the kernel's pidfd interface. Tests replace it to run the identity logic on a fake
// /proc tree, whose PIDs are not real processes.
type pidfdAPI interface {
	open(pid int) (int, error)
	exited(fd int) (bool, error)
	signal(fd int, sig syscall.Signal) error
	close(fd int)
}

type sysPidfd struct{}

func (sysPidfd) open(pid int) (int, error) { return unix.PidfdOpen(pid, 0) }

// exited polls the pidfd without waiting. The kernel makes it readable once every thread of the
// process has exited (a leader that is a zombie while another thread runs, as during execve from
// that thread, does not count), and reports a hang-up once the process has been reaped.
func (sysPidfd) exited(fd int) (bool, error) {
	pfd := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	for {
		_, err := unix.Poll(pfd, 0)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return false, err
		}
		if pfd[0].Revents&(unix.POLLNVAL|unix.POLLERR) != 0 {
			return false, fmt.Errorf("poll of pidfd %d: revents %#x", fd, pfd[0].Revents)
		}
		return pfd[0].Revents&(unix.POLLIN|unix.POLLHUP) != 0, nil
	}
}

func (sysPidfd) signal(fd int, sig syscall.Signal) error {
	return unix.PidfdSendSignal(fd, sig, nil, 0)
}

func (sysPidfd) close(fd int) { _ = unix.Close(fd) }

var pidfds pidfdAPI = sysPidfd{}

// procHandle is a held pidfd and the record it is checked against.
//
// Once Close has run, the descriptor number belongs to the kernel again and may be handed out to
// anything that opens a file next, including another process's pidfd. A closed handle must
// therefore never touch fd: a poll or a signal through it would reach whichever process now owns
// the number, while the identity checks (which read /proc by PID) still look at the recorded
// one. mu makes the closed check and the use of fd one step, so a Close from another goroutine
// cannot come between them, and every method that uses fd goes through it.
type procHandle struct {
	rec    ProcRec
	fd     int
	mu     sync.Mutex
	closed bool
}

// errHandleClosed is what a closed handle answers to every use of its descriptor.
var errHandleClosed = errors.New("the process handle is closed; its descriptor number may belong to another process now")

func (h *procHandle) Close() {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.closed {
		pidfds.close(h.fd)
		h.closed = true
	}
}

// isClosed reports whether Close has run.
func (h *procHandle) isClosed() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.closed
}

// signal sends sig through the descriptor, or fails with errHandleClosed without sending anything.
func (h *procHandle) signal(sig syscall.Signal) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return errHandleClosed
	}
	return pidfds.signal(h.fd, sig)
}

func closeAll(hs []*procHandle) {
	for _, h := range hs {
		h.Close()
	}
}

func (h *procHandle) exited() (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false, errHandleClosed
	}
	return pidfds.exited(h.fd)
}

var errExited = errors.New("the process has exited")

// statWhileHeld reads /proc/<pid>/stat, then confirms through the pidfd that the process had not
// exited after the read, so the entry was its own. A read that fails while the pidfd says the
// process lives is retried, and then reported as an error: never as an exit.
func (h *procHandle) statWhileHeld() (procStat, error) {
	var last error
	for try := 0; try < 5; try++ {
		if try > 0 {
			time.Sleep(time.Millisecond)
		}
		st, err := readStat(h.rec.PID)
		gone, perr := h.exited()
		switch {
		case perr != nil:
			return procStat{}, perr
		case gone:
			return procStat{}, errExited
		case err == nil:
			return st, nil
		}
		last = err
	}
	return procStat{}, fmt.Errorf("its pidfd says it lives, but /proc/%d/stat cannot be read: %w", h.rec.PID, last)
}

// check decides, while the handle is held, whether it is still the recorded process.
func (h *procHandle) check() (procStat, Verdict, string) {
	pid := h.rec.PID
	boot, err := currentBootID()
	if err != nil {
		return procStat{}, Unknown, fmt.Sprintf("pid %d: %v", pid, err)
	}
	st, err := h.statWhileHeld()
	switch {
	case errors.Is(err, errExited):
		return st, Gone, fmt.Sprintf("pid %d has exited", pid)
	case err != nil:
		return st, Unknown, fmt.Sprintf("pid %d: %v", pid, err)
	case boot != h.rec.BootID:
		return st, Foreign, fmt.Sprintf("pid %d was recorded in another boot (%s, now %s), so the process using it now is not the recorded one", pid, h.rec.BootID, boot)
	case st.StartTicks != h.rec.StartTicks:
		return st, Foreign, fmt.Sprintf("pid %d now belongs to another process (start time %d, recorded %d)", pid, st.StartTicks, h.rec.StartTicks)
	}
	if zombieState(st.State) {
		// The pidfd says the process lives while its leader reads Z: execve from another thread is
		// in progress. The task scan is a cross-check for the message only; at this point it may
		// find no live thread at all.
		return st, Ours, fmt.Sprintf("pid %d: the leader reads %c while the process lives (execve in progress; a live thread seen in /proc: %v)", pid, st.State, liveThreads(pid))
	}
	return st, Ours, ""
}

func (h *procHandle) verify() (Verdict, string) {
	_, v, d := h.check()
	return v, d
}

// record fills in the identity of the held process: its start time and the boot ID, read while
// the pidfd is held and confirmed after the read.
func (h *procHandle) record() (ProcRec, error) {
	boot, err := currentBootID()
	if err != nil {
		return ProcRec{}, err
	}
	st, err := h.statWhileHeld()
	if err != nil {
		return ProcRec{}, fmt.Errorf("pid %d: %w", h.rec.PID, err)
	}
	if st.StartTicks == 0 {
		return ProcRec{}, fmt.Errorf("pid %d: no start time in its stat", h.rec.PID)
	}
	h.rec.StartTicks, h.rec.BootID = st.StartTicks, boot
	return h.rec, nil
}

// newRec records the process with the given PID as it is now, through a pidfd.
func newRec(name string, pid int) (ProcRec, error) {
	fd, err := pidfds.open(pid)
	if err != nil {
		return ProcRec{}, fmt.Errorf("take a pidfd for pid %d: %w", pid, err)
	}
	h := &procHandle{rec: ProcRec{Name: name, PID: pid}, fd: fd}
	defer h.Close()
	return h.record()
}

// openRec takes a pidfd for the recorded PID and checks, while holding it, that it is the recorded
// process. It returns the handle only for Ours, and the caller closes it.
func openRec(rec ProcRec) (*procHandle, Verdict, string) {
	if rec.PID <= 1 {
		return nil, Foreign, fmt.Sprintf("recorded pid %d is not a process the launcher could have started", rec.PID)
	}
	if rec.BootID == "" || rec.StartTicks == 0 {
		return nil, Unknown, fmt.Sprintf("the record of pid %d lacks a boot ID or a start time, so its identity cannot be checked", rec.PID)
	}
	var fd int
	var err error
	for try := 0; try < 3; try++ {
		if fd, err = pidfds.open(rec.PID); !errors.Is(err, syscall.EINVAL) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	switch {
	case errors.Is(err, syscall.ESRCH):
		return nil, Gone, fmt.Sprintf("pid %d does not exist", rec.PID)
	case errors.Is(err, syscall.EINVAL):
		return nil, Gone, fmt.Sprintf("pid %d names no process now (no thread group has that ID)", rec.PID)
	case err != nil:
		return nil, Unknown, fmt.Sprintf("pid %d: cannot take a pidfd (%v); without one its identity cannot be pinned, so it is never signalled", rec.PID, err)
	}
	h := &procHandle{rec: rec, fd: fd}
	v, d := h.verify()
	if v != Ours {
		h.Close()
		return nil, v, d
	}
	return h, Ours, d
}

// identify decides whether rec is still alive and still the same process. It never signals.
func identify(rec ProcRec) (Verdict, string) {
	h, v, d := openRec(rec)
	h.Close()
	return v, d
}

// openTree returns the root and every descendant that can be adopted. The descendants come from
// one scan of /proc, which records each one's parent and start time. A descendant is adopted
// only if a pidfd taken for it afterwards shows the same start time and the same parent, and the
// parent's own pidfd shows the parent still alive after that read, so the parent link is real.
// The handles are in tree order, root first; the caller closes them.
func openTree(root *procHandle) []*procHandle {
	table := procTable()
	kids := map[int][]int{}
	for pid, st := range table {
		if pid != st.PPID {
			kids[st.PPID] = append(kids[st.PPID], pid)
		}
	}
	out := []*procHandle{root}
	for i := 0; i < len(out); i++ {
		parent := out[i]
		ks := kids[parent.rec.PID]
		sort.Ints(ks)
		for _, pid := range ks {
			if h := adopt(root.rec.Name, parent, pid, table[pid]); h != nil {
				out = append(out, h)
			}
		}
	}
	return out
}

func adopt(rootName string, parent *procHandle, pid int, seen procStat) *procHandle {
	fd, err := pidfds.open(pid)
	if err != nil {
		return nil
	}
	h := &procHandle{rec: ProcRec{Name: rootName + "-child", PID: pid, StartTicks: seen.StartTicks, BootID: parent.rec.BootID}, fd: fd}
	st, v, _ := h.check()
	if v != Ours || st.PPID != parent.rec.PID {
		h.Close()
		return nil
	}
	if gone, err := parent.exited(); err != nil || gone {
		h.Close()
		return nil
	}
	return h
}

// ownedListeners returns the LISTEN sockets held by the handles' processes, and every other LISTEN
// socket. A process's sockets count only if the process is verified again after its file
// descriptors were read: otherwise they could belong to whatever took over its PID.
func ownedListeners(hs []*procHandle) (owned, others []sock, err error) {
	all, err := listenSockets()
	if err != nil {
		return nil, nil, err
	}
	owned, others = splitOwned(hs, all)
	return owned, others, nil
}

// splitOwned divides all into the sockets held by the handles' processes and the rest, by socket
// inode, with ownedListeners's rule: a process's descriptors count only if it is verified again
// after they were read.
func splitOwned(hs []*procHandle, all []sock) (owned, others []sock) {
	inodes := map[string]bool{}
	for _, h := range hs {
		got := fdSocketInodes(h.rec.PID)
		if v, _ := h.verify(); v != Ours {
			continue
		}
		for i := range got {
			inodes[i] = true
		}
	}
	for _, s := range all {
		if inodes[s.inode] {
			owned = append(owned, s)
		} else {
			others = append(others, s)
		}
	}
	return owned, others
}

// treeListeners is ownedListeners for the root and its adopted descendants.
func treeListeners(root *procHandle) (owned, others []sock, err error) {
	tree := openTree(root)
	defer closeAll(tree[1:])
	return ownedListeners(tree)
}

// treeBound is treeListeners with UDP: every socket of the network namespace that can be reached
// from outside it (boundSockets), divided into those of root's tree and the rest.
func treeBound(root *procHandle) (owned, others []sock, err error) {
	all, err := boundSockets()
	if err != nil {
		return nil, nil, err
	}
	tree := openTree(root)
	defer closeAll(tree[1:])
	owned, others = splitOwned(tree, all)
	return owned, others, nil
}

// addrOwned reports whether root's tree holds a TCP LISTEN socket bound to exactly addr (host and
// port). A socket on the same port at another address does not count: a connection to addr does
// not reach it, and whatever holds addr itself answers instead.
func addrOwned(root *procHandle, addr string) (bool, error) {
	host, p, err := net.SplitHostPort(addr)
	if err != nil {
		return false, err
	}
	ip, port := net.ParseIP(host), 0
	if port, err = strconv.Atoi(p); err != nil || ip == nil {
		return false, fmt.Errorf("%q is not an IP address and port", addr)
	}
	owned, _, err := treeListeners(root)
	if err != nil {
		return false, err
	}
	for _, s := range owned {
		if s.Port == port && ip.Equal(net.ParseIP(s.Addr)) {
			return true, nil
		}
	}
	return false, nil
}

// portOwned reports whether a LISTEN socket on port is held by the process tree of root.
func portOwned(root *procHandle, port int) (bool, error) {
	owned, _, err := treeListeners(root)
	if err != nil {
		return false, err
	}
	for _, s := range owned {
		if s.Port == port {
			return true, nil
		}
	}
	return false, nil
}

// assetRoot is where k3s unpacks its binaries: <data-dir>/data, with symbolic links resolved, as
// the kernel reports executables.
func assetRoot(l layout) (string, error) {
	d, err := filepath.EvalSymlinks(l.dataDir())
	if err != nil {
		return "", fmt.Errorf("resolve the data directory %s: %w", l.dataDir(), err)
	}
	return filepath.Join(d, "data"), nil
}

// checkExe confirms that k3s runs a binary it unpacked into its own data directory. k3s
// re-executes itself from <data-dir>/data/<hash>/bin; when that directory lacks the assets but
// the system k3s's /var/lib/rancher/k3s/data has them, it runs those instead, silently. The link
// is read while the handle is held, and the handle is verified after the read.
func checkExe(l layout, h *procHandle) (string, error) {
	exe, err := exePath(h.rec.PID)
	if v, d := h.verify(); v != Ours {
		return exe, fmt.Errorf("k3s (pid %d) changed while its executable was read: %s", h.rec.PID, d)
	}
	if err != nil {
		return "", fmt.Errorf("cannot read the executable of k3s (pid %d): %w", h.rec.PID, err)
	}
	want, err := assetRoot(l)
	if err != nil {
		return exe, err
	}
	if strings.HasSuffix(exe, " (deleted)") || !strings.HasPrefix(exe, want+string(filepath.Separator)) {
		return exe, fmt.Errorf("k3s (pid %d) runs %s, which is not under its own data directory %s: it fell back to another k3s's unpacked files (such as a system k3s's /var/lib/rancher/k3s/data)", h.rec.PID, exe, want)
	}
	return exe, nil
}

// signalHandle sends sig through the held pidfd, after checking once more that the handle is the
// recorded process. It reports whether the signal was sent.
func signalHandle(h *procHandle, sig syscall.Signal) (bool, error) {
	// A closed handle is an error, never "not ours": a caller that asked to signal a process and
	// was told nothing was sent must be able to tell it apart from a process that is gone.
	if h.isClosed() {
		return false, errHandleClosed
	}
	if v, _ := h.verify(); v != Ours {
		return false, nil
	}
	if err := h.signal(sig); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// signalOurs signals the recorded process only through a pidfd that was taken for it and
// verified. Without a pidfd it fails and sends nothing.
func signalOurs(rec ProcRec, sig syscall.Signal) (bool, error) {
	h, v, d := openRec(rec)
	switch v {
	case Ours:
	case Unknown:
		return false, fmt.Errorf("not signalling %s: %s", rec.Name, d)
	default:
		return false, nil
	}
	defer h.Close()
	return signalHandle(h, sig)
}

// waitExited waits until the pidfd of every handle says its process has exited, or the time is
// up, or ctx is done.
func waitExited(ctx context.Context, hs []*procHandle, d time.Duration) bool {
	end := time.Now().Add(d)
	for {
		all := true
		for _, h := range hs {
			if gone, err := h.exited(); err != nil || !gone {
				all = false
				break
			}
		}
		if all {
			return true
		}
		if time.Now().After(end) || ctx.Err() != nil {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
}
