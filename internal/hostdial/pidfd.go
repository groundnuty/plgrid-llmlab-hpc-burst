package hostdial

// New for HBEP-6's relay keeper (beta; issue #343): a relay's ssh started
// with a pidfd of its own and signalled only through it, after a check of its
// recorded start time, as the rehearsal's guards signal (internal/rehearsal,
// signal.go, PR #309). HBEP-6, The relay keeper, Signals: "the keeper signals
// only its own recorded children, each through a pidfd ... Alpha's signal to
// a relay's process group is dropped, since a relay's ssh starts no
// children." So the keeper's relays get no process group of their own.
// Alpha's relays keep startProcess and process.stop, unchanged.
//
// The pidfd comes from clone itself (SysProcAttr.PidFD), so it names the
// relay's process and nothing else for as long as it is open, and a signal
// through it reaches that process or nobody. The start time is read before
// anything can reap the process, so it is the relay's own. Before each
// signal it is read again: a PID that no longer shows that start time was
// reaped, and its number is free or another process's, so nothing is sent.
// The descriptor is closed only after the process is reaped, under the same
// lock the signals take, so a signal never goes through a closed descriptor's
// number, which the kernel may have given to another open file.
//
// The start time's reading follows internal/rehearsal/procs.go (readStat):
// field 22 of /proc/<pid>/stat, counted after the command name's last ')'.

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// pidfdOpen is the kernel's pidfd_open; a test replaces it to show that an
// Owner refuses a kernel with none.
var pidfdOpen = unix.PidfdOpen

// checkPidfd fails when this kernel gives no pidfd: an Owner whose relays are
// signalled only through one refuses to run rather than signal a PID.
func checkPidfd() error {
	fd, err := pidfdOpen(os.Getpid(), 0)
	if err != nil {
		return fmt.Errorf("hostdial: the relays are signalled only through a pidfd (OwnerOptions.Pidfd), and this kernel gives none: %w", err)
	}
	_ = unix.Close(fd)
	return nil
}

// pidfdHold is a relay process's pidfd and its recorded start time.
type pidfdHold struct {
	ticks uint64 // the start time, in clock ticks since boot
	log   *slog.Logger

	mu     sync.Mutex
	fd     int
	closed bool // after the process was reaped: fd is not ours any more
}

// pidfdAttr is the keeper's relays' process attributes: a pidfd from clone,
// stored at fd, a KILL when this process dies, and no process group of their
// own. A test replaces it to fake a clone that gave no pidfd.
var pidfdAttr = func(fd *int) *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL, PidFD: fd}
}

// unpinnedReaped is told of a process that started without a pidfd, once it
// has ended by itself and been reaped; a test replaces it.
var unpinnedReaped = func(*exec.Cmd) {}

// startPidfdProcess starts cmd with pipes for its standard input and output,
// a pidfd from clone, no process group of its own, and a KILL when this
// process dies, from the thread locked for good (startLockedThread).
//
// A start that gets no pidfd from clone is refused, loudly, and nothing is
// signalled by its number (the science agent's rule for PR 349's round, 10
// Nov 2026: no signal ever goes to a numeric PID). It cannot happen on a
// kernel that passed checkPidfd, since pidfd_open (Linux 5.3) came after
// clone's CLONE_PIDFD (5.2), and Go's start fails when clone refuses the
// flag. Should it happen, the process's pipes are closed, so a relay's ssh
// ends by itself (its relay step ends when its standard input closes,
// HBEP-6); its parent-death signal ends it with this process at the latest;
// and it is reaped in the background.
func startPidfdProcess(cmd *exec.Cmd, log *slog.Logger) (*process, error) {
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	fd := -1
	cmd.SysProcAttr = pidfdAttr(&fd)
	if err := startLockedThread(cmd); err != nil {
		return nil, err
	}
	pid := cmd.Process.Pid
	if fd < 0 {
		_ = in.Close()
		_ = out.Close()
		go func() {
			_ = cmd.Wait()
			unpinnedReaped(cmd)
		}()
		log.Error("a relay's ssh started without a pidfd from clone: refused; it is not signalled, its pipes are closed, and it ends by itself", "pid", pid)
		return nil, fmt.Errorf("hostdial: the relay's ssh %d started without a pidfd from clone: refused, and not signalled", pid)
	}
	// Nothing waits for the process yet, so its PID is its own: read its
	// start time now.
	ticks, err := procStartTicks(pid)
	if err == nil && ticks == 0 {
		err = errors.New("its start time is 0")
	}
	if err != nil {
		_ = unix.PidfdSendSignal(fd, syscall.SIGKILL, nil, 0)
		_ = cmd.Wait()
		_ = unix.Close(fd)
		return nil, fmt.Errorf("hostdial: the relay's ssh %d: no start time: %w", pid, err)
	}
	h := &pidfdHold{ticks: ticks, log: log, fd: fd}
	p := &process{cmd: cmd, pid: pid, in: in, out: out, done: make(chan struct{}), pf: h}
	go func() {
		_ = cmd.Wait()
		h.close()
		close(p.done)
	}()
	return p, nil
}

// stop stops the relay through its pidfd: TERM, KILL after grace, and
// returns once it is reaped. A relay that already exited is only waited for.
func (h *pidfdHold) stop(p *process, grace time.Duration) {
	select {
	case <-p.done:
		return
	default:
	}
	h.signal(p.pid, syscall.SIGTERM)
	t := time.NewTimer(grace)
	defer t.Stop()
	select {
	case <-p.done:
	case <-t.C:
		h.signal(p.pid, syscall.SIGKILL)
		<-p.done
	}
}

// signal sends sig through the pidfd, unless the process was reaped or its
// PID no longer shows the recorded start time.
func (h *pidfdHold) signal(pid int, sig syscall.Signal) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	ticks, err := procStartTicks(pid)
	switch {
	case errors.Is(err, errProcGone):
		return
	case err == nil && ticks != h.ticks:
		h.log.Warn("a relay is not signalled: its PID no longer shows its recorded start time", "pid", pid, "signal", sig.String())
		return
	case err != nil:
		// Not a check that failed open: the pidfd names the relay alone.
		h.log.Warn("a relay's start time cannot be read; it is signalled through its pidfd, which names it alone", "pid", pid, "signal", sig.String(), "err", err)
	}
	if err := unix.PidfdSendSignal(h.fd, sig, nil, 0); err != nil && !errors.Is(err, unix.ESRCH) {
		h.log.Warn("a signal to a relay failed", "pid", pid, "signal", sig.String(), "err", err)
	}
}

// close closes the pidfd once the process is reaped.
func (h *pidfdHold) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.closed {
		h.closed = true
		_ = unix.Close(h.fd)
	}
}

// errProcGone: /proc has no row for the PID.
var errProcGone = errors.New("no such process")

// procStartTicks reads a process's start time, in clock ticks since boot,
// from /proc/<pid>/stat: field 22, counted after the command name, which may
// hold spaces and parentheses, so after its last ')'.
func procStartTicks(pid int) (uint64, error) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	switch {
	case errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH):
		return 0, errProcGone
	case err != nil:
		return 0, err
	}
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return 0, fmt.Errorf("/proc/%d/stat has no command name", pid)
	}
	// f[0] is the state (field 3), so the start time (field 22) is f[19].
	f := strings.Fields(s[i+1:])
	if len(f) < 20 {
		return 0, fmt.Errorf("/proc/%d/stat has %d fields after the command name, want at least 20", pid, len(f))
	}
	ticks, err := strconv.ParseUint(f[19], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("/proc/%d/stat has an unreadable start time", pid)
	}
	return ticks, nil
}
