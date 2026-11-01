package launcher

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// stampLog records each log line with the time it was written.
type stampLog struct {
	mu    sync.Mutex
	lines []string
	at    []time.Time
}

func (s *stampLog) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, l := range strings.Split(strings.TrimRight(string(p), "\n"), "\n") {
		s.lines = append(s.lines, l)
		s.at = append(s.at, time.Now())
	}
	return len(p), nil
}

// when returns the time of the first line containing sub.
func (s *stampLog) when(t *testing.T, sub string) time.Time {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, l := range s.lines {
		if strings.Contains(l, sub) {
			return s.at[i]
		}
	}
	t.Fatalf("no log line contains %q; log:\n%s", sub, strings.Join(s.lines, "\n"))
	return time.Time{}
}

// stopOnExit stops whatever the fixture's record names when the test ends, however it ends, so a
// failed assertion never leaves a supervisor or a controller running. f.st is updated in place by
// startSupervisor, so the stop sees the supervisor it recorded.
func stopOnExit(t *testing.T, f *ctlFixture) {
	t.Helper()
	t.Cleanup(func() {
		if _, err := stopRecorded(context.Background(), f.st, 10*time.Second, nil); err != nil {
			t.Errorf("clean-up: %v", err)
		}
	})
}

// ppidOf reads the parent of pid from /proc with a parser of its own.
func ppidOf(t *testing.T, pid int) int {
	t.Helper()
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	n, err := strconv.Atoi(strings.Fields(s[strings.LastIndexByte(s, ')')+1:])[1])
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// TestStartSupervisorAndDown: startSupervisor starts the real supervisor (the test binary, as
// "hpcburst supervise"), which starts the fake controller; down then stops the supervisor before
// k3s, and nothing is left. The stand-in for k3s is a sleep process, recorded first as k3s is, and
// the controller takes 400 ms to shut down, so the order shows: k3s is signalled only after the
// supervisor, and its child, are gone.
func TestStartSupervisorAndDown(t *testing.T) {
	f := newCtlFixture(t, Front{})
	k3s := startBystander(t, "sleep", "300")
	k3sRec, err := newRec("k3s", k3s.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	f.st.Procs = []ProcRec{k3sRec}
	f.steer("slow-term", "400")
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(superviseEnv, f.l.dir)
	stopOnExit(t, f)
	if err := startSupervisor(context.Background(), f.l, f.st, []string{self}, 30*time.Second, testLog{t}); err != nil {
		t.Fatal(err)
	}

	// What up recorded: k3s first, the supervisor after it, and the same on disk.
	if len(f.st.Procs) != 2 || f.st.Procs[0] != k3sRec || f.st.Procs[1].Name != supervisorName {
		t.Fatalf("procs %+v", f.st.Procs)
	}
	sup := f.st.Procs[1]
	onDisk, err := f.l.loadState()
	if err != nil || len(onDisk.Procs) != 2 || onDisk.Procs[1] != sup || onDisk.Controllers == nil {
		t.Fatalf("state.json: %+v, %v", onDisk, err)
	}
	// The supervisor is a process of its own, not this one, in its own session.
	if sup.PID == os.Getpid() || startTicksOf(t, sup.PID) != sup.StartTicks {
		t.Fatalf("supervisor record %+v", sup)
	}
	e := f.record().Entries[0]
	if e.State != entryRunning || e.Proc == nil || ppidOf(t, e.Proc.PID) != sup.PID {
		t.Fatalf("controller entry %+v is not a running child of pid %d", e, sup.PID)
	}
	if f.record().Supervisor != sup {
		t.Errorf("supervisor.json names %+v, state.json %+v", f.record().Supervisor, sup)
	}
	awaitHealthy(t, f.ports.ControllerAddr())
	for _, d := range []string{"controller", "credentials", "agent-builds"} {
		d = filepath.Join(f.l.dir, d)
		if fi, err := os.Stat(d); err != nil || !fi.IsDir() || fi.Mode().Perm() != 0o700 {
			t.Errorf("%s: %v, %v; want a directory with mode 0700", d, fi, err)
		}
	}
	if _, err := os.Stat(filepath.Join(f.l.dir, "controller", "credentials")); !os.IsNotExist(err) {
		t.Errorf("the earlier controller/credentials was created (%v)", err)
	}
	if fi, err := os.Stat(f.l.supervisorLog()); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("supervisor.log: %v, %v", fi, err)
	}

	log := &stampLog{}
	rep, err := Down(context.Background(), DownOptions{StateDir: f.l.dir, Grace: 15 * time.Second, Log: log})
	if err != nil {
		t.Fatal(err)
	}
	for name, pid := range map[string]int{"supervisor": sup.PID, "controller": e.Proc.PID, "k3s stand-in": k3s.Process.Pid} {
		if !waitNotRunning(pid, 5*time.Second) {
			t.Errorf("%s (pid %d) still runs after down", name, pid)
		}
	}
	for _, p := range []string{f.l.stateFile(), f.l.supervisorFile()} {
		if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s was not removed: %v", p, err)
		}
	}
	if code, err := healthzCode(f.ports.ControllerAddr()); err == nil {
		t.Errorf("the controller's health port still answers (%d)", code)
	}
	if len(rep.Procs) != 2 || rep.Procs[0].Name != "k3s" || rep.Procs[1].Name != supervisorName || rep.Procs[0].Before != "ours" || rep.Procs[1].Before != "ours" {
		t.Errorf("report %+v: want k3s then the supervisor, both ours", rep.Procs)
	}
	gap := log.when(t, "sent SIGTERM to k3s").Sub(log.when(t, "sent SIGTERM to supervisor"))
	if gap < 350*time.Millisecond {
		t.Errorf("k3s was signalled %s after the supervisor, before its controller (400 ms to stop) could have gone", gap)
	}
}

// TestDownKeepsTheRecordWhenTheSupervisorCannotBeChecked: a supervisor record whose identity
// cannot be established (no boot ID) makes down fail and keep the record, but k3s, which can be
// checked, is still stopped.
func TestDownKeepsTheRecordWhenTheSupervisorCannotBeChecked(t *testing.T) {
	f := newCtlFixture(t, Front{})
	k3s := startBystander(t, "sleep", "300")
	k3sRec, _ := newRec("k3s", k3s.Process.Pid)
	other := startBystander(t, "sleep", "300")
	supRec, _ := newRec(supervisorName, other.Process.Pid)
	supRec.BootID = "" // identity unknown
	f.st.Procs = []ProcRec{k3sRec, supRec}
	// loadState refuses a record without a boot ID, so write the file by hand, as a damaged one.
	if err := f.l.saveState(&State{Version: f.st.Version, Ports: f.st.Ports, Procs: []ProcRec{k3sRec}, StartedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	rep, err := stopRecorded(context.Background(), f.st, 10*time.Second, nil)
	if err == nil || !strings.Contains(err.Error(), "could not check the identity of supervisor") {
		t.Fatalf("stopRecorded = %v", err)
	}
	if !waitNotRunning(k3s.Process.Pid, 5*time.Second) {
		t.Error("k3s was left running because the supervisor could not be checked")
	}
	if !runningNow(other.Process.Pid) {
		t.Error("a process whose identity could not be checked was signalled")
	}
	if len(rep.Procs) != 2 || rep.Procs[1].Before != "unknown" {
		t.Errorf("report %+v", rep.Procs)
	}
}

// TestStartSupervisorFailures: each way the start can fail is reported, with the supervisor's log,
// and leaves the supervisor in the record (when it was started) so that the caller's stop reaches it.
func TestStartSupervisorFailures(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	sleep, err := exec.LookPath("sleep")
	if err != nil {
		t.Fatal(err)
	}
	shBin, err := exec.LookPath("sh")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name    string
		argv    []string
		prepare func(f *ctlFixture)
		timeout time.Duration
		want    []string // substrings of the error
	}{
		{"the controller binary cannot be started", []string{self}, func(f *ctlFixture) {
			f.st.Controllers.Binary = f.ctl + "/no-such-binary"
		}, 30 * time.Second, []string{"controller exited while it was being started", "start failed", "last lines of"}},
		{"the controller exits at once", []string{self}, func(f *ctlFixture) { f.steer("always-exit", "7") },
			30 * time.Second, []string{"controller exited while it was being started", "exit status 7"}},
		// The stand-in waits for the launcher's confirmation on the handoff descriptor, as the
		// real supervisor does, and then exits having started nothing. A stand-in that exits at
		// once (false) could be gone before startProc had pinned its identity, and then the start
		// failed in startProc instead of here (issue #130).
		{"the supervisor exits before starting anything", []string{shBin, "-c", "head -c 1 <&" + strconv.Itoa(HandoffFD) + " >/dev/null; exit 1"}, nil,
			30 * time.Second, []string{"the supervisor exited before its processes started"}},
		{"a stale record is not taken for this start's", []string{sleep, "300"}, func(f *ctlFixture) {
			stale := ProcRec{Name: supervisorName, PID: 2, StartTicks: 1, BootID: "old"}
			r := &reporter{l: f.l, f: SupervisorFile{Schema: supervisorSchema, Supervisor: stale,
				Entries: []EntryRecord{{Name: "controller", State: entryRunning, Proc: &stale}}}}
			if err := os.MkdirAll(f.l.controllerDir(), 0o700); err != nil {
				t.Fatal(err)
			}
			r.writeLocked()
			if _, err := os.Stat(f.l.supervisorFile()); err != nil {
				t.Fatal(err)
			}
		}, 600 * time.Millisecond, []string{"did not report its processes started within"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newCtlFixture(t, Front{})
			t.Setenv(superviseEnv, f.l.dir)
			// A stand-in for k3s that is not this test process: stopRecorded below signals it.
			k3s := startBystander(t, "sleep", "300")
			k3sRec, err := newRec("k3s", k3s.Process.Pid)
			if err != nil {
				t.Fatal(err)
			}
			f.st.Procs = []ProcRec{k3sRec}
			stopOnExit(t, f)
			if c.prepare != nil {
				c.prepare(f)
			}
			err = startSupervisor(context.Background(), f.l, f.st, c.argv, c.timeout, nil)
			if err == nil {
				t.Fatal("startSupervisor succeeded")
			}
			for _, w := range c.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q lacks %q", err, w)
				}
			}
			if len(f.st.Procs) != 2 || f.st.Procs[1].Name != supervisorName {
				t.Fatalf("procs %+v: the started supervisor is not recorded", f.st.Procs)
			}
			// The caller's stop reaches it.
			sup := f.st.Procs[1]
			if _, err := stopRecorded(context.Background(), f.st, 10*time.Second, nil); err != nil {
				t.Fatal(err)
			}
			if !waitNotRunning(sup.PID, 5*time.Second) {
				t.Errorf("the supervisor (pid %d) was left running", sup.PID)
			}
		})
	}
}

func TestStartSupervisorNeedsControllers(t *testing.T) {
	f := newCtlFixture(t, Front{})
	f.st.Controllers = nil
	if err := startSupervisor(context.Background(), f.l, f.st, []string{"/bin/true"}, time.Second, nil); err == nil {
		t.Fatal("startSupervisor succeeded without controllers")
	}
}

// TestResolveControllerBin: an explicit path must exist and be executable; the default is the
// binary beside the running executable, and its absence is not an error.
func TestResolveControllerBin(t *testing.T) {
	dir := t.TempDir()
	exe := dir + "/ctl"
	if err := os.WriteFile(exe, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	plain := dir + "/plain"
	if err := os.WriteFile(plain, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got, err := resolveControllerBin(exe); err != nil || got != exe {
		t.Errorf("explicit: %q, %v", got, err)
	}
	for name, path := range map[string]string{"missing": dir + "/none", "not executable": plain, "a directory": dir} {
		if got, err := resolveControllerBin(path); err == nil {
			t.Errorf("%s: accepted as %q", name, got)
		}
	}
	// The default: this test binary's directory holds no hpcburst-controller.
	if self, err := os.Executable(); err == nil {
		if _, err := os.Stat(self[:strings.LastIndexByte(self, '/')+1] + ControllerBinName); err == nil {
			t.Fatal("an hpcburst-controller sits beside the test binary, so the default case cannot be tested (and a skip would hide that)")
		}
	}
	if got, err := resolveControllerBin(""); err != nil || got != "" {
		t.Errorf("default with no binary beside the executable: %q, %v; want empty and no error", got, err)
	}
}
