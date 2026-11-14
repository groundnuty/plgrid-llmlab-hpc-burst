package keeperprobe

// The pins against real processes: a child this test starts is pinned by its pidfd and verified along its chain, a signal goes through the
// pin and to nothing else, and a pin that failed its check signals nothing (the rule of #308: no signal ever goes to a numeric PID). The
// supervisor's record is written as the launcher writes it, naming a real process.

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// startSleep starts a real child of this test and ends it, by its own handle, at the test's end.
func startSleep(t *testing.T) *exec.Cmd {
	t.Helper()
	sleep, err := exec.LookPath("sleep")
	if err != nil {
		t.Skip("no sleep program")
	}
	cmd := exec.Command(sleep, "60")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	return cmd
}

// selfPin is a verified pin of this test process: the parent of the children the tests start.
func selfPin(t *testing.T) *Pin {
	t.Helper()
	p, err := OpenPin(t, "the test process", os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	_, ticks, err := StatRow(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	p.Ticks, p.verified = ticks, true
	return p
}

func TestStatRowReadsTheParentAndTheStartTime(t *testing.T) {
	ppid, ticks, err := StatRow(os.Getpid())
	if err != nil || ppid != os.Getppid() || ticks == 0 {
		t.Fatalf("StatRow(self) = %d, %d, %v; want the real parent %d and a start time", ppid, ticks, err, os.Getppid())
	}
	cmd := startSleep(t)
	ppid, ticks2, err := StatRow(cmd.Process.Pid)
	if err != nil || ppid != os.Getpid() || ticks2 < ticks {
		t.Fatalf("StatRow(child) = %d, %d, %v; want this process as parent and a start no earlier than ours (%d)", ppid, ticks2, err, ticks)
	}
	if _, _, err := StatRow(1 << 30); err == nil {
		t.Error("StatRow of a PID that names no process read a row")
	}
	// The start time is field 22, in clock ticks since boot: a process started a moment ago began within seconds of the uptime, read
	// apart from /proc/<pid>/stat. Another field (a size, a time, a count) would be far from it.
	up, err := os.ReadFile("/proc/uptime")
	if err != nil {
		t.Fatal(err)
	}
	uptime, err := strconv.ParseFloat(strings.Fields(string(up))[0], 64)
	if err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command("getconf", "CLK_TCK").Output()
	if err != nil {
		t.Skip("no getconf CLK_TCK")
	}
	hz, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil {
		t.Fatal(err)
	}
	if age := uptime - float64(ticks2)/hz; age < 0 || age > 30 {
		t.Errorf("a child started a moment ago has a start time %d ticks (%.1f s since boot), %.1f s before the uptime %.1f s", ticks2, float64(ticks2)/hz, age, uptime)
	}
}

func TestOpenPinRefusesNoProcess(t *testing.T) {
	for _, pid := range []int{0, 1, -5} {
		if _, err := OpenPin(t, "nothing", pid); err == nil {
			t.Errorf("OpenPin(%d) pinned something", pid)
		}
	}
}

func TestAPinOfAChildIsVerifiedAndSignalsThroughItsPidfd(t *testing.T) {
	cmd := startSleep(t)
	parent := selfPin(t)
	child, err := PinChild(t, "the sleep", cmd.Process.Pid, parent, 0)
	if err != nil {
		t.Fatal(err)
	}
	if child.PID != cmd.Process.Pid || child.Ticks == 0 || child.Exited() {
		t.Fatalf("the pin is %+v, exited %t", child, child.Exited())
	}
	if err := child.StillChild(parent); err != nil {
		t.Errorf("a live child of its parent is not still a child: %v", err)
	}
	if err := child.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("a verified pin refused its signal: %v", err)
	}
	child.WaitExited(t, 10*time.Second)
	if !child.Exited() {
		t.Error("the signalled process did not exit")
	}
	if err := child.StillChild(parent); err == nil {
		t.Error("an exited process is still a child")
	}
}

func TestPinChildRefusesWhatIsNotAChildOfItsVerifiedParent(t *testing.T) {
	cmd := startSleep(t)
	other := startSleep(t)
	parent := selfPin(t)
	good, err := StatRowTicks(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name   string
		pid    int
		parent *Pin
		want   uint64
		err    string
	}{
		{"an unverified parent", cmd.Process.Pid, &Pin{What: "unverified", PID: os.Getpid()}, 0, "not a verified pin"},
		{"no parent", cmd.Process.Pid, nil, 0, "not a verified pin"},
		{"a process whose parent is another", cmd.Process.Pid, pinOf(t, other.Process.Pid), 0, "has parent"},
		{"a recorded start time that is not its own", cmd.Process.Pid, parent, good + 1, "not the recorded"},
		{"a parent whose start time is not its own", cmd.Process.Pid, func() *Pin { p := selfPin(t); p.Ticks++; return p }(), 0, "another process"},
	} {
		t.Run(c.name, func(t *testing.T) {
			p, err := PinChild(t, "the sleep", c.pid, c.parent, c.want)
			if err == nil || !strings.Contains(err.Error(), c.err) {
				t.Fatalf("PinChild = %+v, %v; want an error holding %q", p, err, c.err)
			}
			// A pin that failed its check signals nothing, and the process is untouched.
			if p != nil {
				if serr := p.Signal(syscall.SIGKILL); !errors.Is(serr, ErrNotVerified) {
					t.Errorf("a pin that failed its check was signalled: %v", serr)
				}
			}
			if p := cmd.ProcessState; p != nil {
				t.Error("the process was killed")
			}
		})
	}
	// The control: the same call with the right parent and the right start time verifies.
	if _, err := PinChild(t, "the sleep", cmd.Process.Pid, parent, good); err != nil {
		t.Errorf("a child of its verified parent with its recorded start time failed: %v", err)
	}
}

// pinOf is a verified pin of a process that is not the parent of the pinned child.
func pinOf(t *testing.T, pid int) *Pin {
	t.Helper()
	p, err := OpenPin(t, "another process", pid)
	if err != nil {
		t.Fatal(err)
	}
	_, ticks, _ := StatRow(pid)
	p.Ticks, p.verified = ticks, true
	return p
}

// StatRowTicks is the start time of a process.
func StatRowTicks(pid int) (uint64, error) {
	_, ticks, err := StatRow(pid)
	return ticks, err
}

// A child that has exited and is not yet reaped still has its row and its pidfd, and its pidfd says it exited: the pin refuses it.
func TestPinChildRefusesAChildThatExited(t *testing.T) {
	cmd := startSleep(t)
	parent := selfPin(t)
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	// Not yet reaped (Wait comes at the cleanup): the process is a zombie, and the pidfd fires once it is.
	// The kill is asynchronous, so a pin taken at once may still verify a process that is about to be one: wait until it is refused.
	stack.WaitFor(t, 10*time.Second, "the killed child to be refused as exited", func() bool {
		_, err := PinChild(t, "the sleep", cmd.Process.Pid, parent, 0)
		return err != nil && strings.Contains(err.Error(), "exited")
	})
}

// StillChild names the two ways a pinned process stops being the child it was: another parent, and another start time.
func TestStillChildNamesAnotherParentAndAnotherStartTime(t *testing.T) {
	cmd := startSleep(t)
	parent := selfPin(t)
	child, err := PinChild(t, "the sleep", cmd.Process.Pid, parent, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := child.StillChild(pinOf(t, os.Getppid())); err == nil || !strings.Contains(err.Error(), "has parent") {
		t.Errorf("StillChild against another parent = %v, want an error naming the parent", err)
	}
	moved := *child
	moved.Ticks++
	if err := moved.StillChild(parent); err == nil || !strings.Contains(err.Error(), "started at tick") {
		t.Errorf("StillChild with another start time = %v, want an error naming the tick", err)
	}
}

// A pin whose pidfd is no longer open has seen nothing: poll answers that the descriptor is invalid, which is not the process exiting, and a
// wait on the pin must not return as if it had.
func TestAPinWhoseDescriptorIsClosedHasNotSeenTheProcessExit(t *testing.T) {
	cmd := startSleep(t)
	p, err := OpenPin(t, "the sleep", cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	if p.Exited() {
		t.Fatal("a live process reads as exited")
	}
	if err := unix.Close(p.FD); err != nil {
		t.Fatal(err)
	}
	if p.Exited() {
		t.Error("a pin with a closed descriptor reads as exited")
	}
}

func TestAnUnverifiedPinSignalsNothing(t *testing.T) {
	cmd := startSleep(t)
	p, err := OpenPin(t, "the sleep", cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Signal(syscall.SIGKILL); !errors.Is(err, ErrNotVerified) {
		t.Fatalf("Signal on an unverified pin = %v, want ErrNotVerified", err)
	}
	if p.Exited() {
		t.Error("the process exited: the signal was sent")
	}
}

// writeRecord writes state.json and supervisor.json as the launcher writes them, naming proc as the supervisor and entries as its children.
func writeRecord(t *testing.T, state string, sup LauncherProc, supInSupervisorJSON LauncherProc, entries []SupervisorEntry) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(state, "controller"), 0o700); err != nil {
		t.Fatal(err)
	}
	st, _ := json.Marshal(map[string]any{"processes": []LauncherProc{sup}})
	if err := os.WriteFile(filepath.Join(state, "state.json"), st, 0o600); err != nil {
		t.Fatal(err)
	}
	rec, _ := json.Marshal(SupervisorRecord{Supervisor: supInSupervisorJSON, Entries: entries})
	if err := os.WriteFile(filepath.Join(state, "controller", "supervisor.json"), rec, 0o600); err != nil {
		t.Fatal(err)
	}
}

// bootID is this boot's, as the launcher records it.
func bootID(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(b))
}

func TestReadSupervisorAndItsEntries(t *testing.T) {
	state := t.TempDir()
	run := LauncherProc{Name: "controller", PID: 4242, StartTicks: 99, BootID: "b"}
	writeRecord(t, state, LauncherProc{Name: "supervisor", PID: 7, StartTicks: 5, BootID: "b"}, LauncherProc{Name: "supervisor", PID: 7, StartTicks: 5, BootID: "b"},
		[]SupervisorEntry{{Name: "controller", State: "running", Proc: &run}, {Name: "front", State: "exited"}, {Name: "frps", State: "running"}})
	rec, err := ReadSupervisor(state)
	if err != nil || rec.Supervisor.PID != 7 || len(rec.Entries) != 3 {
		t.Fatalf("ReadSupervisor = %+v, %v", rec, err)
	}
	for _, c := range []struct {
		name    string
		running bool
	}{{"controller", true}, {"front", false}, {"frps", false}, {"nothing", false}} {
		if _, running := rec.EntryOf(c.name); running != c.running {
			t.Errorf("EntryOf(%q) running = %t, want %t: running needs the state and a recorded process", c.name, running, c.running)
		}
	}
	if e, _ := rec.EntryOf("controller"); e.Proc == nil || e.Proc.PID != 4242 {
		t.Errorf("the controller's entry is %+v", e)
	}
	if _, err := ReadSupervisor(t.TempDir()); err == nil {
		t.Error("a state directory with no record read one")
	}
}

func TestPinSupervisorVerifiesTheRecordAgainstTheProcess(t *testing.T) {
	cmd := startSleep(t)
	_, ticks, err := StatRow(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	good := LauncherProc{Name: "supervisor", PID: cmd.Process.Pid, StartTicks: ticks, BootID: bootID(t)}
	for _, c := range []struct {
		name  string
		inSt  LauncherProc
		inSup LauncherProc
		err   string
	}{
		{"as recorded", good, good, ""},
		{"a start time that is not the process's", func() LauncherProc { g := good; g.StartTicks++; return g }(), func() LauncherProc { g := good; g.StartTicks++; return g }(), "not the recorded"},
		{"another boot", func() LauncherProc { g := good; g.BootID = "not-this-boot"; return g }(), func() LauncherProc { g := good; g.BootID = "not-this-boot"; return g }(), "not this one"},
		{"two files that name different processes", good, func() LauncherProc { g := good; g.PID++; return g }(), "is not state.json's"},
	} {
		t.Run(c.name, func(t *testing.T) {
			state := t.TempDir()
			writeRecord(t, state, c.inSt, c.inSup, nil)
			p, err := PinSupervisor(t, state)
			if c.err == "" {
				if err != nil || p.PID != cmd.Process.Pid || p.Ticks != ticks {
					t.Fatalf("PinSupervisor = %+v, %v; want the process verified", p, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.err) {
				t.Fatalf("PinSupervisor = %+v, %v; want an error holding %q", p, err, c.err)
			}
			if p != nil {
				if serr := p.Signal(syscall.SIGKILL); !errors.Is(serr, ErrNotVerified) {
					t.Errorf("a supervisor that failed its check was signalled: %v", serr)
				}
			}
		})
	}
	// A process that started before this test process is not the supervisor this test's up recorded, even when its recorded start time is
	// its own: this test's parent (go test, or the shell that ran the binary) is older than the test, and its record is right.
	t.Run("one that started before this test process", func(t *testing.T) {
		ppid := os.Getppid()
		if ppid <= 1 {
			t.Skip("this test has no parent to name")
		}
		_, ticks, err := StatRow(ppid)
		if err != nil {
			t.Fatal(err)
		}
		state := t.TempDir()
		old := LauncherProc{Name: "supervisor", PID: ppid, StartTicks: ticks, BootID: bootID(t)}
		writeRecord(t, state, old, old, nil)
		p, err := PinSupervisor(t, state)
		if err == nil || !strings.Contains(err.Error(), "started before this test process") {
			t.Fatalf("PinSupervisor of an older process = %+v, %v; want it refused as started before this test process", p, err)
		}
	})
}

func TestPinEntryPinsTheSupervisedChildWithItsRecordedStartTime(t *testing.T) {
	cmd := startSleep(t)
	sup := selfPin(t)
	_, ticks, err := StatRow(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	state := t.TempDir()
	run := LauncherProc{Name: "controller", PID: cmd.Process.Pid, StartTicks: ticks, BootID: bootID(t)}
	writeRecord(t, state, LauncherProc{Name: "supervisor", PID: os.Getpid()}, LauncherProc{Name: "supervisor", PID: os.Getpid()},
		[]SupervisorEntry{{Name: "controller", State: "running", Proc: &run}, {Name: "front", State: "exited"}})
	p, e, err := PinEntry(t, state, "controller", sup)
	if err != nil || p.PID != cmd.Process.Pid || e.Proc.PID != cmd.Process.Pid {
		t.Fatalf("PinEntry = %+v, %+v, %v", p, e, err)
	}
	if _, _, err := PinEntry(t, state, "front", sup); err == nil || !strings.Contains(err.Error(), "no running front") {
		t.Errorf("PinEntry of an entry that does not run = %v", err)
	}
	// A recorded start time that is not the process's refuses the pin.
	run.StartTicks = ticks + 1
	writeRecord(t, state, LauncherProc{Name: "supervisor", PID: os.Getpid()}, LauncherProc{Name: "supervisor", PID: os.Getpid()},
		[]SupervisorEntry{{Name: "controller", State: "running", Proc: &run}})
	if p, _, err := PinEntry(t, state, "controller", sup); err == nil {
		t.Errorf("PinEntry took a process whose start time is not the recorded one: %+v", p)
	}
}

var _ = strconv.Itoa
