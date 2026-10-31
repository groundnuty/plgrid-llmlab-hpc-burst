package launcher

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestCheckEntry runs checkEntry on real processes: this test process is the supervisor, and the
// fake controller is its child. Each case changes one thing, and the first is the control that
// shows the others fail because of that thing.
func TestCheckEntry(t *testing.T) {
	f := newCtlFixture(t, Front{}, "")
	f.supervise(SuperviseOptions{BackoffMin: 50 * time.Millisecond, BackoffMax: 50 * time.Millisecond})
	e := f.awaitRunning()
	awaitHealthy(t, f.ports.ControllerAddr())
	sup, v, d := openRec(f.st.Procs[0]) // this process
	if v != Ours {
		t.Fatalf("this process is %v: %s", v, d)
	}
	t.Cleanup(sup.Close)
	supPID := os.Getpid()
	joined := func(p []string) string { return strings.Join(p, "; ") }

	t.Run("control: a healthy child of the supervisor", func(t *testing.T) {
		eh, problems := checkEntry(sup, supPID, e)
		if len(problems) != 0 || !eh.Running || eh.HealthzOK == nil || !*eh.HealthzOK || eh.PID != e.Proc.PID {
			t.Fatalf("%+v, problems %v", eh, problems)
		}
	})
	t.Run("healthz answers 503", func(t *testing.T) {
		f.steer("unhealthy", "1")
		defer f.unsteer("unhealthy")
		eh, problems := checkEntry(sup, supPID, e)
		if eh.HealthzOK == nil || *eh.HealthzOK || !strings.Contains(joined(problems), "503") {
			t.Fatalf("%+v, problems %v", eh, problems)
		}
		if !eh.Running {
			t.Error("a child that answers 503 is still a running child")
		}
	})
	t.Run("the health port is held by another process, which answers 200", func(t *testing.T) {
		// The decoy is this test process, which answers 200 to everything. A status that only
		// asked /healthz would call that healthy.
		decoy := "127.0.0.1:" + strconv.Itoa(answeringServer(t))
		if code, err := healthzCode(decoy); err != nil || code != 200 {
			t.Fatalf("the decoy does not answer: %d, %v", code, err)
		}
		other := e
		other.Health = decoy
		eh, problems := checkEntry(sup, supPID, other)
		if !strings.Contains(joined(problems), "does not hold its health port") || eh.HealthzOK != nil {
			t.Fatalf("%+v, problems %v: the decoy's answer must not be asked for, let alone count", eh, problems)
		}
	})
	t.Run("the record names the PID but not the process", func(t *testing.T) {
		p := *e.Proc
		p.StartTicks++
		other := e
		other.Proc = &p
		eh, problems := checkEntry(sup, supPID, other)
		if eh.Running || !strings.Contains(joined(problems), "not running") || !strings.Contains(joined(problems), "another process") {
			t.Fatalf("%+v, problems %v", eh, problems)
		}
	})
	t.Run("between restarts", func(t *testing.T) {
		other := EntryRecord{Name: "controller", Health: e.Health, State: entryBackoff,
			LastExit: &ExitRecord{Code: 3, Detail: "exit status 3", At: time.Now()}}
		eh, problems := checkEntry(sup, supPID, other)
		if eh.Running || !strings.Contains(joined(problems), "exit status 3") || !strings.Contains(joined(problems), "code 3") {
			t.Fatalf("%+v, problems %v", eh, problems)
		}
	})
	t.Run("the supervisor is gone", func(t *testing.T) {
		eh, problems := checkEntry(nil, supPID, e)
		if eh.Running || !strings.Contains(joined(problems), "not a child of the supervisor") {
			t.Fatalf("%+v, problems %v", eh, problems)
		}
	})
	t.Run("the child died: the stale record is not trusted, the fresh one is", func(t *testing.T) {
		stale := e
		if err := syscall.Kill(stale.Proc.PID, syscall.SIGKILL); err != nil {
			t.Fatal(err)
		}
		if !waitNotRunning(stale.Proc.PID, 5*time.Second) {
			t.Fatal("could not kill the child")
		}
		eh, problems := checkEntry(sup, supPID, stale)
		if eh.Running || !strings.Contains(joined(problems), "not running") {
			t.Fatalf("stale record: %+v, problems %v", eh, problems)
		}
		fresh := f.await("the restart", func(e EntryRecord) bool {
			return e.State == entryRunning && e.Proc != nil && e.Proc.PID != stale.Proc.PID
		})
		awaitHealthy(t, f.ports.ControllerAddr())
		if eh, problems := checkEntry(sup, supPID, fresh); len(problems) != 0 || !eh.Running || eh.Restarts != 1 {
			t.Fatalf("fresh record: %+v, problems %v", eh, problems)
		}
	})
}

// supervisedFakeHost adds a supervisor and its controller to a fake host: processes 4343 (the
// supervisor) and 4344 (its child, which holds the port a real test server answers /healthz on),
// supervisor.json naming them, and the record of the controllers in state.json.
type supervisedFakeHost struct {
	*fakeHost
	supRec, childRec ProcRec
	health           string
	code             *int // the status code /healthz answers
}

func newSupervisedFakeHost(t *testing.T) *supervisedFakeHost {
	t.Helper()
	h := newFakeHost(t)
	code := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(code) }))
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL)
	s := &supervisedFakeHost{fakeHost: h, health: u.Host, code: &code}
	port, _ := strconv.Atoi(u.Port())
	const supPID, childPID = 4343, 4344
	writeFakeProc(t, h.root, supPID, "S", 1, 2000)
	writeFakeProc(t, h.root, childPID, "S", supPID, 3000)
	h.f.setAlive(supPID, true)
	h.f.setAlive(childPID, true)
	if err := os.MkdirAll(h.l.controllerDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	h.inodes[port] = "9100"
	h.giveSocket(childPID, port)
	h.writeListenTable()
	s.supRec = ProcRec{Name: supervisorName, PID: supPID, StartTicks: 2000, BootID: "boot-A"}
	s.childRec = ProcRec{Name: "controller", PID: childPID, StartTicks: 3000, BootID: "boot-A"}
	h.st.Procs = append(h.st.Procs, s.supRec)
	h.st.Controllers = &ControllerConfig{Binary: "/opt/hpcburst-controller"}
	h.st.Ports.Controller = port
	if err := h.l.saveState(h.st); err != nil {
		t.Fatal(err)
	}
	s.writeSupervisorFile(s.supRec, EntryRecord{Name: "controller", Health: s.health, State: entryRunning, Proc: &s.childRec, Restarts: 2})
	return s
}

func (s *supervisedFakeHost) writeSupervisorFile(sup ProcRec, e EntryRecord) {
	s.t.Helper()
	r := &reporter{l: s.l, f: SupervisorFile{Schema: supervisorSchema, Supervisor: sup, Entries: []EntryRecord{e}}}
	r.writeLocked()
}

func (s *supervisedFakeHost) status(noAPI bool) *StatusReport {
	s.t.Helper()
	rep, err := Status(context.Background(), StatusOptions{StateDir: s.l.dir, NoAPI: noAPI, NoGCProbe: true})
	if err != nil {
		s.t.Fatal(err)
	}
	return rep
}

// TestStatusWithControllers: Status on a fake host that also runs a supervisor and a controller.
// The healthy case also shows that the controller's listener is not counted among k3s's.
func TestStatusWithControllers(t *testing.T) {
	reasons := func(r *StatusReport) string { return strings.Join(r.Reasons, "; ") }
	t.Run("control: healthy", func(t *testing.T) {
		for _, noAPI := range []bool{true, false} {
			s := newSupervisedFakeHost(t)
			rep := s.status(noAPI)
			if rep.Health != Healthy {
				t.Fatalf("noAPI=%v: %s: %v", noAPI, rep.Health, rep.Reasons)
			}
			c := rep.Controllers
			if rep.Supervisor == nil || !rep.Supervisor.Running || rep.Supervisor.PID != 4343 ||
				len(c) != 1 || !c[0].Running || c[0].PID != 4344 || c[0].Restarts != 2 || c[0].HealthzOK == nil || !*c[0].HealthzOK {
				t.Fatalf("supervisor %+v, controllers %+v", rep.Supervisor, c)
			}
			if len(rep.Processes) != 2 || rep.Processes[1].Name != supervisorName || rep.Processes[1].Verdict != "ours" {
				t.Errorf("processes %+v", rep.Processes)
			}
		}
	})
	t.Run("the supervisor is gone: unhealthy, and k3s is still judged", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		s.f.setAlive(4343, false)
		rep := s.status(false)
		if rep.Health != Unhealthy || !strings.Contains(reasons(rep), "process supervisor (pid 4343) is not running") {
			t.Fatalf("%s: %v", rep.Health, rep.Reasons)
		}
		if rep.Supervisor == nil || rep.Supervisor.Running {
			t.Errorf("supervisor %+v", rep.Supervisor)
		}
		// A dead supervisor must not hide what k3s says.
		if rep.Readyz == nil || !*rep.Readyz || rep.Executable == "" || len(rep.Listeners) == 0 {
			t.Errorf("k3s was not judged: readyz %v, executable %q, listeners %v", rep.Readyz, rep.Executable, rep.Listeners)
		}
		if len(rep.Controllers) != 1 || rep.Controllers[0].Running {
			t.Errorf("controllers %+v: a child whose supervisor is gone is not a running controller", rep.Controllers)
		}
	})
	t.Run("the controller is gone", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		s.f.setAlive(4344, false)
		rep := s.status(false)
		if rep.Health != Unhealthy || !strings.Contains(reasons(rep), "controller: pid 4344 is not running") {
			t.Fatalf("%s: %v", rep.Health, rep.Reasons)
		}
		if !rep.Supervisor.Running || rep.Controllers[0].Running {
			t.Errorf("supervisor %+v, controllers %+v", rep.Supervisor, rep.Controllers)
		}
	})
	t.Run("the controller is between restarts", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		s.writeSupervisorFile(s.supRec, EntryRecord{Name: "controller", Health: s.health, State: entryBackoff, Restarts: 1,
			LastExit: &ExitRecord{Code: 2, Detail: "exit status 2", At: time.Now()}})
		rep := s.status(true)
		if rep.Health != Unhealthy || !strings.Contains(reasons(rep), "last exit: exit status 2") {
			t.Fatalf("%s: %v", rep.Health, rep.Reasons)
		}
	})
	t.Run("the child does not hold the health port", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		s.giveSocket(9999, s.st.Ports.Controller) // the socket now belongs to some other process
		rep := s.status(true)
		if rep.Health != Unhealthy || !strings.Contains(reasons(rep), "does not hold its health port") {
			t.Fatalf("%s: %v", rep.Health, rep.Reasons)
		}
		if c := rep.Controllers[0]; c.HealthzOK != nil {
			t.Errorf("/healthz was asked of a port the child does not hold: %+v", c)
		}
	})
	t.Run("healthz answers 503", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		*s.code = http.StatusServiceUnavailable
		rep := s.status(true)
		if rep.Health != Unhealthy || !strings.Contains(reasons(rep), "GET /healthz") || !strings.Contains(reasons(rep), "503") {
			t.Fatalf("%s: %v", rep.Health, rep.Reasons)
		}
	})
	t.Run("controllers are configured but no supervisor is recorded", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		s.st.Procs = s.st.Procs[:1]
		if err := s.l.saveState(s.st); err != nil {
			t.Fatal(err)
		}
		rep := s.status(true)
		if rep.Health != Unhealthy || !strings.Contains(reasons(rep), "no supervisor is recorded") {
			t.Fatalf("%s: %v", rep.Health, rep.Reasons)
		}
	})
	t.Run("no supervisor record file", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		if err := os.Remove(s.l.supervisorFile()); err != nil {
			t.Fatal(err)
		}
		rep := s.status(true)
		if rep.Health != Unhealthy || !strings.Contains(reasons(rep), "cannot read the supervisor's record") {
			t.Fatalf("%s: %v", rep.Health, rep.Reasons)
		}
	})
	t.Run("the record file belongs to another supervisor", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		other := s.supRec
		other.StartTicks++
		s.writeSupervisorFile(other, EntryRecord{Name: "controller", Health: s.health, State: entryRunning, Proc: &s.childRec})
		rep := s.status(true)
		if rep.Health != Unhealthy || !strings.Contains(reasons(rep), "another supervisor") {
			t.Fatalf("%s: %v", rep.Health, rep.Reasons)
		}
	})
}

// TestSupervisorFileMode: supervisor.json is private, like state.json.
func TestSupervisorFileMode(t *testing.T) {
	f := newCtlFixture(t, Front{}, "")
	f.supervise(SuperviseOptions{})
	f.awaitRunning()
	fi, err := os.Stat(f.l.supervisorFile())
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("supervisor.json: %v, %v", fi, err)
	}
	if got, want := f.l.supervisorFile(), filepath.Join(f.l.dir, "controller", "supervisor.json"); got != want {
		t.Errorf("path %s, want %s", got, want)
	}
}
