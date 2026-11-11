package scenario_test

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
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// launcherProc is a process as the launcher records it (internal/launcher,
// ProcRec).
type launcherProc struct {
	Name       string `json:"name"`
	PID        int    `json:"pid"`
	StartTicks uint64 `json:"start_ticks"`
	BootID     string `json:"boot_id"`
}

// supervisorEntry is one supervised process in supervisor.json.
type supervisorEntry struct {
	Name     string        `json:"name"`
	State    string        `json:"state"`
	Proc     *launcherProc `json:"proc"`
	Restarts int           `json:"restarts"`
	LastExit *struct {
		Code   int    `json:"code"`
		Detail string `json:"detail"`
	} `json:"last_exit"`
}

// supervisorRecord is supervisor.json, the supervisor's record of its
// children, in the parts read here.
type supervisorRecord struct {
	Supervisor launcherProc      `json:"supervisor"`
	Entries    []supervisorEntry `json:"entries"`
}

// readSupervisor reads <state>/controller/supervisor.json.
func readSupervisor(state string) (supervisorRecord, error) {
	var rec supervisorRecord
	b, err := os.ReadFile(filepath.Join(state, "controller", "supervisor.json"))
	if err != nil {
		return rec, err
	}
	return rec, json.Unmarshal(b, &rec)
}

// entryOf is the supervised process name as supervisor.json has it, and
// whether it runs there.
func (s supervisorRecord) entryOf(name string) (supervisorEntry, bool) {
	for _, e := range s.Entries {
		if e.Name == name {
			return e, e.State == "running" && e.Proc != nil
		}
	}
	return supervisorEntry{}, false
}

// stackPin is a process of the stack pinned by a pidfd.
type stackPin struct {
	what     string
	pid, fd  int
	ticks    uint64
	verified bool
}

// statRow reads a process's parent and start time, in clock ticks since boot,
// from /proc/<pid>/stat (fields 4 and 22, counted after the command name's
// last ')').
func statRow(pid int) (ppid int, ticks uint64, err error) {
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

// openPin opens a pidfd on pid, closed at the test's end.
func openPin(t *testing.T, what string, pid int) (*stackPin, error) {
	t.Helper()
	if pid <= 1 {
		return nil, fmt.Errorf("%s: no process to pin (pid %d)", what, pid)
	}
	fd, err := unix.PidfdOpen(pid, 0)
	if err != nil {
		return nil, fmt.Errorf("%s: pidfd_open of process %d: %w", what, pid, err)
	}
	t.Cleanup(func() { _ = unix.Close(fd) })
	return &stackPin{what: what, pid: pid, fd: fd}, nil
}

// pinSupervisor pins the supervisor that this test's up recorded in state:
// state.json and supervisor.json name the same process, its boot ID is this
// boot's, its start time is the recorded one, and it started after this test
// process.
func pinSupervisor(t *testing.T, state string) (*stackPin, error) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(state, "state.json"))
	if err != nil {
		return nil, err
	}
	var st struct {
		Procs []launcherProc `json:"processes"`
	}
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, err
	}
	var rec *launcherProc
	for i := range st.Procs {
		if st.Procs[i].Name == "supervisor" {
			rec = &st.Procs[i]
		}
	}
	sup, err := readSupervisor(state)
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
	_, self, err := statRow(os.Getpid())
	if err != nil {
		return nil, err
	}
	p, err := openPin(t, "the supervisor", rec.PID)
	if err != nil {
		return nil, err
	}
	_, ticks, err := statRow(rec.PID)
	switch {
	case err != nil:
		return p, err
	case ticks != rec.StartTicks:
		return p, fmt.Errorf("the supervisor's pid %d started at tick %d, not the recorded %d", rec.PID, ticks, rec.StartTicks)
	case ticks < self:
		return p, fmt.Errorf("the supervisor's pid %d started before this test process", rec.PID)
	case p.exited():
		return p, fmt.Errorf("the supervisor's pid %d exited during the check", rec.PID)
	}
	p.ticks, p.verified = ticks, true
	return p, nil
}

// pinChild pins pid and verifies, while its pidfd is held, that it is a child
// of the verified pin parent, started no earlier than it; with want non-zero,
// that its start time is want, the recorded one. The parent's row is read
// again, so a parent whose PID now names another process fails the check.
func pinChild(t *testing.T, what string, pid int, parent *stackPin, want uint64) (*stackPin, error) {
	t.Helper()
	if parent == nil || !parent.verified {
		return nil, fmt.Errorf("%s: its parent is not a verified pin", what)
	}
	p, err := openPin(t, what, pid)
	if err != nil {
		return nil, err
	}
	ppid, ticks, err := statRow(pid)
	if err != nil {
		return p, err
	}
	_, pticks, err := statRow(parent.pid)
	switch {
	case err != nil:
		return p, fmt.Errorf("%s's parent %s: %w", what, parent.what, err)
	case ppid != parent.pid:
		return p, fmt.Errorf("%s (pid %d) has parent %d, not %s (pid %d)", what, pid, ppid, parent.what, parent.pid)
	case pticks != parent.ticks:
		return p, fmt.Errorf("%s's pid %d now started at tick %d, not %d: another process", parent.what, parent.pid, pticks, parent.ticks)
	case ticks < parent.ticks:
		return p, fmt.Errorf("%s (pid %d) started before its parent: a reused PID", what, pid)
	case want != 0 && ticks != want:
		return p, fmt.Errorf("%s (pid %d) started at tick %d, not the recorded %d", what, pid, ticks, want)
	case p.exited() || parent.exited():
		return p, fmt.Errorf("%s (pid %d) or its parent exited during the check", what, pid)
	}
	p.ticks, p.verified = ticks, true
	return p, nil
}

// errNotVerified refuses a signal to a pin that failed its check.
var errNotVerified = errors.New("the pin was not verified: not signalled")

// signal sends sig through the pin's pidfd; an unverified pin is refused.
func (p *stackPin) signal(sig syscall.Signal) error {
	if !p.verified {
		return errNotVerified
	}
	return unix.PidfdSendSignal(p.fd, sig, nil, 0)
}

// exited reports whether the pinned process has exited (a zombie included):
// its pidfd is readable.
func (p *stackPin) exited() bool {
	pfd := []unix.PollFd{{Fd: int32(p.fd), Events: unix.POLLIN}}
	n, err := unix.Poll(pfd, 0)
	return err == nil && n > 0 && pfd[0].Revents&(unix.POLLIN|unix.POLLHUP) != 0
}

// waitExited waits until the pinned process has exited.
func (p *stackPin) waitExited(t *testing.T, d time.Duration) {
	t.Helper()
	stack.WaitFor(t, d, p.what+" (pid "+strconv.Itoa(p.pid)+") to exit", p.exited)
}

// stillChild checks that the pinned process still runs as the child of
// parent: its pidfd has not fired, and its row shows the same start time and
// the same parent.
func (p *stackPin) stillChild(parent *stackPin) error {
	if p.exited() {
		return fmt.Errorf("%s (pid %d) exited", p.what, p.pid)
	}
	ppid, ticks, err := statRow(p.pid)
	switch {
	case err != nil:
		return err
	case ticks != p.ticks:
		return fmt.Errorf("%s's pid %d started at tick %d, not %d", p.what, p.pid, ticks, p.ticks)
	case ppid != parent.pid:
		return fmt.Errorf("%s (pid %d) has parent %d, not %s (pid %d)", p.what, p.pid, ppid, parent.what, parent.pid)
	case p.exited():
		return fmt.Errorf("%s (pid %d) exited during the check", p.what, p.pid)
	}
	return nil
}

// pinEntry pins the supervised process name, as supervisor.json records it
// now, verified as the supervisor's child with the recorded start time.
func pinEntry(t *testing.T, state, name string, sup *stackPin) (*stackPin, supervisorEntry, error) {
	t.Helper()
	rec, err := readSupervisor(state)
	if err != nil {
		return nil, supervisorEntry{}, err
	}
	e, ok := rec.entryOf(name)
	if !ok {
		return nil, e, fmt.Errorf("supervisor.json has no running %s", name)
	}
	p, err := pinChild(t, "the "+name, e.Proc.PID, sup, e.Proc.StartTicks)
	return p, e, err
}
