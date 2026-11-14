package keeperprobe

// How the keeper rehearsal (#343, PR 5) holds and signals the stack's
// processes. No signal ever goes to a numeric PID (the science agent's rule
// after the incident of #308): a target is pinned by a pidfd and verified,
// while the pidfd is held, along its chain of parents, and only a verified pin
// is ever signalled. The pattern is internal/hostdial/pins_test.go's and
// internal/relaykeeper/keeperproc/procs_test.go's, with one difference, which
// the stack forces: there the chain ends at this test process, whose
// descendants the pinned processes are. Here hpcburst up starts the supervisor
// in a session of its own and exits, so the supervisor is reparented away from
// the test, and every chain ends instead at the supervisor as the launcher
// recorded it in this test's own state directory (state.json and
// supervisor.json: PID, start time and boot ID), the identity hpcburst down
// checks before it signals anything (internal/launcher, stopGroup). That
// supervisor must also have started after this test process. Each hop's
// parent is the next hop, each hop started no earlier than its parent, a
// recorded hop's start time is the recorded one, and the pidfds are asked
// after the rows are read: a process that exited during the reads, its PID
// reused, fails the check.
//
// Only the controller is ever signalled, once per event; the keeper, its
// relays' ssh and the supervisor are pinned to be watched, through their
// pidfds. No pin registers a signal at the clean-up: hpcburst down, which the
// clean-up runs, stops the stack, and the run's scope is the backstop
// (hack/rehearsal.sh).

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// LauncherProc is a process as the launcher records it (internal/launcher,
// ProcRec).
type LauncherProc struct {
	Name       string `json:"name"`
	PID        int    `json:"pid"`
	StartTicks uint64 `json:"start_ticks"`
	BootID     string `json:"boot_id"`
}

// SupervisorEntry is one supervised process in supervisor.json.
type SupervisorEntry struct {
	Name     string        `json:"name"`
	State    string        `json:"state"`
	Proc     *LauncherProc `json:"proc"`
	Restarts int           `json:"restarts"`
	LastExit *struct {
		Code   int    `json:"code"`
		Detail string `json:"detail"`
	} `json:"last_exit"`
}

// SupervisorRecord is supervisor.json, the supervisor's record of its
// children, in the parts read here.
type SupervisorRecord struct {
	Supervisor LauncherProc      `json:"supervisor"`
	Entries    []SupervisorEntry `json:"entries"`
}

// ReadSupervisor reads <state>/controller/supervisor.json.
func ReadSupervisor(state string) (SupervisorRecord, error) {
	var rec SupervisorRecord
	b, err := os.ReadFile(filepath.Join(state, "controller", "supervisor.json"))
	if err != nil {
		return rec, err
	}
	return rec, json.Unmarshal(b, &rec)
}

// entryOf is the supervised process name as supervisor.json has it, and
// whether it runs there.
func (s SupervisorRecord) EntryOf(name string) (SupervisorEntry, bool) {
	for _, e := range s.Entries {
		if e.Name == name {
			return e, e.State == "running" && e.Proc != nil
		}
	}
	return SupervisorEntry{}, false
}

// Pin is a process of the stack pinned by a pidfd.
type Pin struct {
	What     string
	PID, FD  int
	Ticks    uint64
	verified bool
}

// StatRow reads a process's parent and start time, in clock ticks since boot,
// from /proc/<pid>/stat (fields 4 and 22, counted after the command name's
// last ')').
func StatRow(pid int) (ppid int, ticks uint64, err error) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, 0, err
	}
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return 0, 0, fmt.Errorf("/proc/%d/stat has no command name", pid)
	}
	f := strings.Fields(s[i+1:])
	if len(f) < 20 {
		return 0, 0, fmt.Errorf("/proc/%d/stat is short", pid)
	}
	if ppid, err = strconv.Atoi(f[1]); err != nil {
		return 0, 0, err
	}
	ticks, err = strconv.ParseUint(f[19], 10, 64)
	return ppid, ticks, err
}

// OpenPin opens a pidfd on pid, closed at the test's end.
func OpenPin(t stack.T, what string, pid int) (*Pin, error) {
	t.Helper()
	if pid <= 1 {
		return nil, fmt.Errorf("%s: no process to pin (pid %d)", what, pid)
	}
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		return nil, fmt.Errorf("%s: pidfd_open of process %d: %w", what, pid, err)
	}
	t.Cleanup(func() { _ = unix.Close(fd) })
	return &Pin{What: what, PID: pid, FD: fd}, nil
}

// PinSupervisor pins the supervisor that this test's up recorded in state:
// state.json and supervisor.json name the same process, its boot ID is this
// boot's, its start time is the recorded one, and it started after this test
// process.
func PinSupervisor(t stack.T, state string) (*Pin, error) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(state, "state.json"))
	if err != nil {
		return nil, err
	}
	var st struct {
		Procs []LauncherProc `json:"processes"`
	}
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, err
	}
	var rec *LauncherProc
	for i := range st.Procs {
		if st.Procs[i].Name == "supervisor" {
			rec = &st.Procs[i]
		}
	}
	sup, err := ReadSupervisor(state)
	switch {
	case err != nil:
		return nil, err
	case rec == nil:
		return nil, errors.New("state.json records no supervisor")
	case sup.Supervisor != *rec:
		return nil, fmt.Errorf("supervisor.json's supervisor %+v is not state.json's %+v", sup.Supervisor, *rec)
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return nil, err
	}
	if rec.BootID != strings.TrimSpace(string(boot)) {
		return nil, fmt.Errorf("the supervisor was recorded in boot %s, not this one", rec.BootID)
	}
	_, self, err := StatRow(os.Getpid())
	if err != nil {
		return nil, err
	}
	p, err := OpenPin(t, "the supervisor", rec.PID)
	if err != nil {
		return nil, err
	}
	_, ticks, err := StatRow(rec.PID)
	switch {
	case err != nil:
		return p, err
	case ticks != rec.StartTicks:
		return p, fmt.Errorf("the supervisor's pid %d started at tick %d, not the recorded %d", rec.PID, ticks, rec.StartTicks)
	case ticks < self:
		return p, fmt.Errorf("the supervisor's pid %d started before this test process", rec.PID)
	case p.Exited():
		return p, fmt.Errorf("the supervisor's pid %d exited during the check", rec.PID)
	}
	p.Ticks, p.verified = ticks, true
	return p, nil
}

// PinChild pins pid and verifies, while its pidfd is held, that it is a child
// of the verified pin parent, started no earlier than it; with want non-zero,
// that its start time is want, the recorded one. The parent's row is read
// again, so a parent whose PID now names another process fails the check.
func PinChild(t stack.T, what string, pid int, parent *Pin, want uint64) (*Pin, error) {
	t.Helper()
	if parent == nil || !parent.verified {
		return nil, fmt.Errorf("%s: its parent is not a verified pin", what)
	}
	p, err := OpenPin(t, what, pid)
	if err != nil {
		return nil, err
	}
	ppid, ticks, err := StatRow(pid)
	if err != nil {
		return p, err
	}
	_, pticks, err := StatRow(parent.PID)
	switch {
	case err != nil:
		return p, fmt.Errorf("%s's parent %s: %w", what, parent.What, err)
	case ppid != parent.PID:
		return p, fmt.Errorf("%s (pid %d) has parent %d, not %s (pid %d)", what, pid, ppid, parent.What, parent.PID)
	case pticks != parent.Ticks:
		return p, fmt.Errorf("%s's pid %d now started at tick %d, not %d: another process", parent.What, parent.PID, pticks, parent.Ticks)
	case ticks < parent.Ticks:
		return p, fmt.Errorf("%s (pid %d) started before its parent: a reused PID", what, pid)
	case want != 0 && ticks != want:
		return p, fmt.Errorf("%s (pid %d) started at tick %d, not the recorded %d", what, pid, ticks, want)
	case p.Exited() || parent.Exited():
		return p, fmt.Errorf("%s (pid %d) or its parent exited during the check", what, pid)
	}
	p.Ticks, p.verified = ticks, true
	return p, nil
}

// ErrNotVerified refuses a signal to a pin that failed its check.
var ErrNotVerified = errors.New("the pin was not verified: not signalled")

// signal sends sig through the pin's pidfd; an unverified pin is refused.
func (p *Pin) Signal(sig syscall.Signal) error {
	if !p.verified {
		return ErrNotVerified
	}
	return unix.PidfdSendSignal(p.FD, sig, nil, 0)
}

// exited reports whether the pinned process has exited (a zombie included):
// its pidfd is readable.
func (p *Pin) Exited() bool {
	pfd := []unix.PollFd{{Fd: int32(p.FD), Events: unix.POLLIN}}
	n, err := unix.Poll(pfd, 0)
	return err == nil && n > 0 && pfd[0].Revents&(unix.POLLIN|unix.POLLHUP) != 0
}

// waitExited waits until the pinned process has exited.
func (p *Pin) WaitExited(t stack.T, d time.Duration) {
	t.Helper()
	stack.WaitFor(t, d, p.What+" (pid "+strconv.Itoa(p.PID)+") to exit", p.Exited)
}

// stillChild checks that the pinned process still runs as the child of
// parent: its pidfd has not fired, and its row shows the same start time and
// the same parent.
func (p *Pin) StillChild(parent *Pin) error {
	if p.Exited() {
		return fmt.Errorf("%s (pid %d) exited", p.What, p.PID)
	}
	ppid, ticks, err := StatRow(p.PID)
	switch {
	case err != nil:
		return err
	case ticks != p.Ticks:
		return fmt.Errorf("%s's pid %d started at tick %d, not %d", p.What, p.PID, ticks, p.Ticks)
	case ppid != parent.PID:
		return fmt.Errorf("%s (pid %d) has parent %d, not %s (pid %d)", p.What, p.PID, ppid, parent.What, parent.PID)
	case p.Exited():
		return fmt.Errorf("%s (pid %d) exited during the check", p.What, p.PID)
	}
	return nil
}

// PinEntry pins the supervised process name, as supervisor.json records it
// now, verified as the supervisor's child with the recorded start time.
func PinEntry(t stack.T, state, name string, sup *Pin) (*Pin, SupervisorEntry, error) {
	t.Helper()
	rec, err := ReadSupervisor(state)
	if err != nil {
		return nil, SupervisorEntry{}, err
	}
	e, ok := rec.EntryOf(name)
	if !ok {
		return nil, e, fmt.Errorf("supervisor.json has no running %s", name)
	}
	p, err := PinChild(t, "the "+name, e.Proc.PID, sup, e.Proc.StartTicks)
	return p, e, err
}
