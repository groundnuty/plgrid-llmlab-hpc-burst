// Package procgroup runs a process in a process group of its own and owns the
// whole group until every member is gone: the allocation agent's engine
// workers, its ssh forward and staging's Apptainer runs.
//
// A group's cleanup is tracked apart from its leader's exit (PR #87 review:
// a leader that exits on TERM can leave a child that ignores it). The leader
// is observed exiting with waitid(WNOWAIT), which leaves it unreaped, a
// zombie, and it is reaped only once no other member of its group is alive.
// While it is unreaped its PID, which is the group's ID, cannot be reused, so
// a signal to the group never reaches another process (HBEP-6, teardown:
// processes are known by PID and start time, never by name).
//
// A stop is TERM to the group, then KILL to it once the grace has passed, if
// any member remains. A child that calls setsid leaves the group and escapes
// this, as it would escape any group signal; spike S2 phase B's job script
// (spikes/s2b/scripts/job.sh, stop_tree) walked the tree by parent PID
// instead, which a reparented child escapes.
//
// The package imports only the standard library. It is Linux-only: it reads
// /proc.
package procgroup

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

// pollEvery is how often a group with an exited leader is scanned for members
// that are still alive.
const pollEvery = 20 * time.Millisecond

// KilledExitCode is the exit code of a leader that a signal ended.
const KilledExitCode = -1

// ID identifies one process across PID reuse: its PID and its start time, the
// 22nd field of /proc/<pid>/stat, in clock ticks since boot.
type ID struct {
	PID       int
	StartTime uint64
}

// Alive reports whether the process with this PID and start time still runs:
// a process with the PID and another start time is another process, and a
// zombie runs nothing.
func (p ID) Alive() bool {
	st, err := readStat(p.PID)
	return err == nil && st.start == p.StartTime && st.state != 'Z'
}

// IDOf returns the ID of a running process.
func IDOf(pid int) (ID, error) {
	st, err := readStat(pid)
	if err != nil {
		return ID{}, err
	}
	return ID{PID: pid, StartTime: st.start}, nil
}

// stat is what this package reads of /proc/<pid>/stat.
type stat struct {
	state byte
	pgrp  int
	start uint64
}

func readStat(pid int) (stat, error) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return stat{}, err
	}
	// The command name is in parentheses and may hold spaces: the fields
	// after its last ")" are state (3), ppid (4), pgrp (5) ... starttime (22).
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return stat{}, fmt.Errorf("/proc/%d/stat: no command name", pid)
	}
	f := strings.Fields(s[i+1:])
	if len(f) < 20 {
		return stat{}, fmt.Errorf("/proc/%d/stat: %d fields after the name", pid, len(f))
	}
	pgrp, err := strconv.Atoi(f[2])
	if err != nil {
		return stat{}, err
	}
	start, err := strconv.ParseUint(f[19], 10, 64)
	if err != nil {
		return stat{}, err
	}
	return stat{state: f[0][0], pgrp: pgrp, start: start}, nil
}

// Members returns the PIDs of the live processes in the process group pgid,
// other than except (the group's leader, which may be an unreaped zombie).
func Members(pgid, except int) []int {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	var out []int
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil || pid == except {
			continue
		}
		st, err := readStat(pid)
		if err == nil && st.pgrp == pgid && st.state != 'Z' {
			out = append(out, pid)
		}
	}
	return out
}

// Spec is what Start runs.
type Spec struct {
	Path string
	Args []string // after the program
	Env  []string
	Dir  string
	// Stdout and Stderr receive the process's output. An *os.File is passed
	// as it is; any other writer is fed through a pipe, which a member that
	// leaves the group can hold open (Wait then gives up after a while).
	Stdout, Stderr io.Writer
}

// Group is one started process and its process group.
type Group struct {
	cmd   *exec.Cmd
	pid   int
	start time.Time
	id    ID

	exited chan struct{} // the leader has exited (unreaped)
	done   chan struct{} // every member is gone and the leader is reaped
	code   int

	mu       sync.Mutex
	reaping  bool // set before the leader is reaped: no signal after it
	stopping bool
	killer   *time.Timer
}

// Start starts the process as the leader of a new process group.
func Start(s Spec, now time.Time) (*Group, error) {
	cmd := exec.Command(s.Path, s.Args...)
	cmd.Env = s.Env
	cmd.Dir = s.Dir
	cmd.Stdout = s.Stdout
	cmd.Stderr = s.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	// I/O copying of a non-file writer must not hold up the reaping forever.
	cmd.WaitDelay = 5 * time.Second
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", s.Path, err)
	}
	g := &Group{
		cmd:    cmd,
		pid:    cmd.Process.Pid,
		start:  now,
		exited: make(chan struct{}),
		done:   make(chan struct{}),
	}
	g.id = ID{PID: g.pid}
	if st, err := readStat(g.pid); err == nil {
		g.id.StartTime = st.start
	}
	go g.run()
	return g, nil
}

// run waits for the leader to exit without reaping it, then for the group to
// empty, then reaps the leader.
func (g *Group) run() {
	code, err := waitNoReap(g.pid)
	if err != nil {
		// The leader cannot be observed without reaping it: reap it and give
		// up policing the group, whose ID may then be reused.
		_ = g.cmd.Wait()
		g.code = exitCodeOf(g.cmd.ProcessState)
		g.mu.Lock()
		g.reaping = true
		g.mu.Unlock()
		close(g.exited)
		g.finish()
		return
	}
	g.code = code
	close(g.exited)
	for len(Members(g.pid, g.pid)) > 0 {
		time.Sleep(pollEvery)
	}
	g.mu.Lock()
	g.reaping = true
	g.mu.Unlock()
	_ = g.cmd.Wait()
	g.finish()
}

func (g *Group) finish() {
	g.mu.Lock()
	if g.killer != nil {
		g.killer.Stop()
	}
	g.mu.Unlock()
	close(g.done)
}

// PID is the leader's PID, which is the group's ID.
func (g *Group) PID() int { return g.pid }

// Start is when the process was started, by the caller's clock.
func (g *Group) Start() time.Time { return g.start }

// ID is the leader's PID and start time.
func (g *Group) ID() ID { return g.id }

// Exited is closed once the leader has exited; ExitCode then holds its code.
func (g *Group) Exited() <-chan struct{} { return g.exited }

// Done is closed once every member of the group is gone and the leader is
// reaped.
func (g *Group) Done() <-chan struct{} { return g.done }

// ExitCode is the leader's exit code, KilledExitCode when a signal ended it.
// It is valid once Exited is closed.
func (g *Group) ExitCode() int { return g.code }

// Stopping reports whether Stop has been called.
func (g *Group) Stopping() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.stopping
}

// Stop stops the group: TERM to every member now, then KILL to those left
// once grace has passed. It returns at once; Done says when the group is
// gone. Only the first call counts, so a repeated instruction never moves
// the deadline.
func (g *Group) Stop(grace time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopping {
		return
	}
	g.stopping = true
	g.signalLocked(syscall.SIGTERM)
	g.killer = time.AfterFunc(grace, func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		g.signalLocked(syscall.SIGKILL)
	})
}

// Kill sends KILL to every member of the group now.
func (g *Group) Kill() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.signalLocked(syscall.SIGKILL)
}

// signalLocked signals the group while its leader is unreaped, so the group's
// ID is still ours. The caller holds g.mu.
func (g *Group) signalLocked(sig syscall.Signal) {
	if g.reaping {
		return
	}
	_ = syscall.Kill(-g.pid, sig)
}

// ---- waitid(WNOWAIT) ----

// siginfo is the part of Linux's siginfo_t that waitid fills for a child, on
// 64-bit architectures (amd64, arm64): si_signo, si_errno, si_code, padding
// to the union, then si_pid, si_uid and si_status.
type siginfo struct {
	signo  int32
	errno  int32
	code   int32
	_      int32
	pid    int32
	uid    uint32
	status int32
	_      [128 - 28]byte
}

// Values of si_code for a child (asm-generic/siginfo.h).
const (
	cldExited = 1
	cldKilled = 2
	cldDumped = 3
)

// pPID is waitid's P_PID: wait for the child with this PID.
const pPID = 1

// waitNoReap waits for the child to exit and returns its exit code, leaving it
// a zombie: waitid with WEXITED|WNOWAIT.
func waitNoReap(pid int) (int, error) {
	for {
		var info siginfo
		_, _, e := syscall.Syscall6(syscall.SYS_WAITID, pPID, uintptr(pid), uintptr(unsafe.Pointer(&info)), syscall.WEXITED|syscall.WNOWAIT, 0, 0)
		if e == syscall.EINTR {
			continue
		}
		if e != 0 {
			return 0, e
		}
		switch info.code {
		case cldExited:
			return int(info.status), nil
		case cldKilled, cldDumped:
			return KilledExitCode, nil
		default:
			return 0, errors.New("waitid: the child has not exited")
		}
	}
}

func exitCodeOf(st *os.ProcessState) int {
	if st == nil {
		return KilledExitCode
	}
	if ws, ok := st.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		return KilledExitCode
	}
	if c := st.ExitCode(); c >= 0 {
		return c
	}
	return KilledExitCode
}
