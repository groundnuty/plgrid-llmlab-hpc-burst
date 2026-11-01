package launcher

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/go-logr/logr"
	crhealthz "sigs.k8s.io/controller-runtime/pkg/healthz"
	crlog "sigs.k8s.io/controller-runtime/pkg/log"
)

// quietControllerRuntime gives controller-runtime's logger a sink once per process. Its healthz
// handler logs each failing check, and without a logger controller-runtime prints a notice and a
// goroutine dump into the test output, where a failure that is not one would be hard to tell apart.
var quietControllerRuntime sync.Once

// TestCheckEntry runs checkEntry on real processes: this test process is the supervisor, and the
// fake controller is its child. Each case changes one thing, and the first is the control that
// shows the others fail because of that thing.
func TestCheckEntry(t *testing.T) {
	f := newCtlFixture(t, Front{})
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
		if eh.ReadyzOK == nil || !*eh.ReadyzOK || len(eh.ReadyzFailing) != 0 {
			t.Fatalf("a controller that answers 200 on /readyz: %+v", eh)
		}
	})
	// The fake answers /healthz and /readyz on their own: not-ready changes /readyz only, so the
	// cases below fail because /readyz is asked, not because both endpoints changed together.
	for _, c := range []struct{ name, steer string }{
		{"as controller-runtime answers a failing check (500)", ""},
		{"as the API server answers (503)", "503"},
	} {
		t.Run("alive but not ready, "+c.name, func(t *testing.T) {
			f.steer("not-ready", c.steer)
			defer f.unsteer("not-ready")
			if code, err := healthzCode(f.ports.ControllerAddr()); err != nil || code != 200 {
				t.Fatalf("setup: /healthz answers %d, %v; the controller must stay alive", code, err)
			}
			eh, problems := checkEntry(sup, supPID, e)
			if !eh.Running || eh.HealthzOK == nil || !*eh.HealthzOK || eh.ReadyzOK == nil || *eh.ReadyzOK ||
				!slices.Equal(eh.ReadyzFailing, []string{"serving"}) {
				t.Fatalf("%+v, problems %v", eh, problems)
			}
			want := "not ready: GET /readyz on " + e.Health + " answered " + cmp.Or(c.steer, "500")
			if len(problems) != 1 || !strings.Contains(problems[0], want) || !strings.Contains(problems[0], "failing check: serving") {
				t.Fatalf("problems %q, want one with %q", problems, want)
			}
		})
	}
	t.Run("ready again once the controller says so", func(t *testing.T) {
		f.steer("not-ready", "")
		if _, problems := checkEntry(sup, supPID, e); len(problems) != 1 {
			t.Fatalf("setup: problems %v", problems)
		}
		f.unsteer("not-ready")
		eh, problems := checkEntry(sup, supPID, e)
		if len(problems) != 0 || eh.ReadyzOK == nil || !*eh.ReadyzOK {
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
		if !strings.Contains(joined(problems), "does not hold its health address") || eh.HealthzOK != nil {
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
	code             *int    // the status code /healthz answers
	body             *string // the body it answers with
	// /readyz is answered on its own by controller-runtime's own healthz.Handler with one check,
	// serving, as internal/lifecycle registers it: it passes while ready is true. A non-zero
	// readyzStatus answers that status instead, with the same body shape; readyzHits counts the
	// requests, so a test can show that a process was or was not asked.
	ready        *atomic.Bool
	readyzStatus *atomic.Int32
	readyzHits   *atomic.Int32
}

func newSupervisedFakeHost(t *testing.T) *supervisedFakeHost {
	t.Helper()
	h := newFakeHost(t)
	quietControllerRuntime.Do(func() { crlog.SetLogger(logr.Discard()) })
	code, body := http.StatusOK, ""
	ready, status, hits := new(atomic.Bool), new(atomic.Int32), new(atomic.Int32)
	ready.Store(true)
	readyz := http.StripPrefix("/readyz", &crhealthz.Handler{Checks: map[string]crhealthz.Checker{
		"ping": crhealthz.Ping,
		"serving": func(*http.Request) error {
			if !ready.Load() {
				return errors.New("not leading, or caches not synced")
			}
			return nil
		},
	}})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/readyz" {
			hits.Add(1)
			if st := int(status.Load()); st != 0 {
				w.WriteHeader(st)
				_, _ = io.WriteString(w, "[+]ping ok\n[-]serving failed: reason withheld\nhealthz check failed\n")
				return
			}
			readyz.ServeHTTP(w, r)
			return
		}
		w.WriteHeader(code)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL)
	s := &supervisedFakeHost{fakeHost: h, health: u.Host, code: &code, body: &body, ready: ready, readyzStatus: status, readyzHits: hits}
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
			if c[0].ReadyzOK == nil || !*c[0].ReadyzOK || s.readyzHits.Load() == 0 {
				t.Fatalf("noAPI=%v: the controller was not asked for /readyz, or it did not answer 200: %+v, %d requests", noAPI, c[0], s.readyzHits.Load())
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
		if rep.Health != Unhealthy || !strings.Contains(reasons(rep), "does not hold its health address") {
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
	f := newCtlFixture(t, Front{})
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

// TestStatusFrontUsageDegraded: a front whose /healthz says its usage sink is failing is reported
// as degraded, "usage writes failing", and the host stays healthy: status exits 0, and no reason
// is added (@hpc-burst-science-agent, 31 Oct 2026; HBEP-8, Usage records). The bodies follow
// HBEP-8's statement of the front's /healthz: a usage member with state ok or failing,
// lostRecords, since and the last error. That the real front writes this member is checked
// against the real binary in cmd/hpcburst's TestFrontSupervision.
func TestStatusFrontUsageDegraded(t *testing.T) {
	const failing = `{"synced":true,"usage":{"state":"failing","lostRecords":3,"since":"2026-10-31T12:00:00.000Z","error":"usage: write usage-2026-10-31.jsonl: no space left on device"},"usageError":"usage: write usage-2026-10-31.jsonl: no space left on device"}`
	const ok = `{"synced":true,"usage":{"state":"ok","lostRecords":0,"since":"2026-10-31T12:00:00.000Z"}}`
	front := func(s *supervisedFakeHost, body string) *StatusReport {
		s.t.Helper()
		*s.body = body
		s.writeSupervisorFile(s.supRec, EntryRecord{Name: "front", Health: s.health, State: entryRunning, Proc: &s.childRec})
		return s.status(true)
	}
	t.Run("failing usage: degraded, and still healthy", func(t *testing.T) {
		rep := front(newSupervisedFakeHost(t), failing)
		if rep.Health != Healthy || len(rep.Reasons) != 0 {
			t.Fatalf("health %s, reasons %v: a failing usage sink is not a health problem", rep.Health, rep.Reasons)
		}
		if len(rep.Controllers) != 1 {
			t.Fatalf("entries %+v", rep.Controllers)
		}
		if c := rep.Controllers[0]; c.Name != "front" || c.UsageState != "failing" || c.Degraded != "usage writes failing" || c.HealthzOK == nil || !*c.HealthzOK {
			t.Errorf("the front is reported as %+v", c)
		}
	})
	t.Run("usage ok: not degraded", func(t *testing.T) {
		rep := front(newSupervisedFakeHost(t), ok)
		if rep.Health != Healthy || len(rep.Controllers) != 1 || rep.Controllers[0].UsageState != "ok" || rep.Controllers[0].Degraded != "" {
			t.Errorf("health %s, entries %+v", rep.Health, rep.Controllers)
		}
	})
	// The front always states its sink, so a body without a known state is reported as unknown
	// (PR 134 review, N2), still without touching the health; a controller states none.
	for _, c := range []struct{ name, body string }{
		{"no body", ""},
		{"no usage member", `{"synced":true}`},
		{"a state HBEP-8 does not name", `{"synced":true,"usage":{"state":"draining"}}`},
		{"a body cut at the read limit", `{"synced":true,"usage":{"state":"ok"` + strings.Repeat(" ", healthzBodyLimit) + `}}`},
	} {
		t.Run("front, "+c.name+": usage state unknown", func(t *testing.T) {
			rep := front(newSupervisedFakeHost(t), c.body)
			if rep.Health != Healthy || len(rep.Reasons) != 0 || rep.Controllers[0].UsageState != "unknown" || rep.Controllers[0].Degraded != "" {
				t.Errorf("health %s, reasons %v, entries %+v", rep.Health, rep.Reasons, rep.Controllers)
			}
		})
	}
	t.Run("the controller, which states no usage: nothing said", func(t *testing.T) {
		s := newSupervisedFakeHost(t) // its entry is the controller
		rep := s.status(true)
		if rep.Health != Healthy || rep.Controllers[0].Name != "controller" || rep.Controllers[0].UsageState != "" {
			t.Errorf("health %s, entries %+v", rep.Health, rep.Controllers)
		}
	})
	t.Run("a 503 stays unhealthy, whatever its body says", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		*s.code = http.StatusServiceUnavailable
		rep := front(s, failing)
		if rep.Health != Unhealthy || rep.Controllers[0].Degraded != "" {
			t.Errorf("health %s, entries %+v", rep.Health, rep.Controllers)
		}
	})
}

// TestStatusControllerReadyz: status judges the lifecycle controller by its /readyz (HBEP-8,
// Observers), which fails until the caches have synced and the controller leads its Lease, and
// keeps /healthz as liveness. The fake host answers the two endpoints on their own, /readyz with
// controller-runtime's own handler, so each case changes one of them.
func TestStatusControllerReadyz(t *testing.T) {
	reasons := func(r *StatusReport) string { return strings.Join(r.Reasons, "; ") }
	notReady := func(t *testing.T, rep *StatusReport, answered string) {
		t.Helper()
		c := rep.Controllers[0]
		if rep.Health != Unhealthy || !strings.Contains(reasons(rep), "controller: not ready: GET /readyz on") ||
			!strings.Contains(reasons(rep), "answered "+answered) || !strings.Contains(reasons(rep), "failing check: serving (starting, or not leading its Lease") {
			t.Fatalf("%s: %v", rep.Health, rep.Reasons)
		}
		if !c.Running || c.HealthzOK == nil || !*c.HealthzOK || c.ReadyzOK == nil || *c.ReadyzOK || !slices.Equal(c.ReadyzFailing, []string{"serving"}) {
			t.Errorf("the controller is alive but not ready: %+v", c)
		}
	}
	t.Run("control: alive and ready", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		rep := s.status(true)
		if rep.Health != Healthy || len(rep.Reasons) != 0 || s.readyzHits.Load() == 0 {
			t.Fatalf("%s: %v, %d /readyz requests", rep.Health, rep.Reasons, s.readyzHits.Load())
		}
	})
	t.Run("alive but not ready: 200 on /healthz, 500 on /readyz from controller-runtime's handler", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		s.ready.Store(false)
		notReady(t, s.status(true), "500")
	})
	t.Run("alive but not ready: 200 on /healthz, 503 on /readyz", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		s.readyzStatus.Store(http.StatusServiceUnavailable)
		notReady(t, s.status(true), "503")
	})
	t.Run("not ready, and the API is asked too", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		s.ready.Store(false)
		notReady(t, s.status(false), "500")
	})
	t.Run("ready again: the same host turns healthy", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		s.ready.Store(false)
		if rep := s.status(true); rep.Health != Unhealthy {
			t.Fatalf("setup: %s: %v", rep.Health, rep.Reasons)
		}
		s.ready.Store(true)
		if rep := s.status(true); rep.Health != Healthy || len(rep.Reasons) != 0 {
			t.Fatalf("%s: %v", rep.Health, rep.Reasons)
		}
	})
	t.Run("not alive but answering ready: unhealthy by liveness, and /readyz is not asked", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		*s.code = http.StatusServiceUnavailable
		rep := s.status(true)
		if rep.Health != Unhealthy || !strings.Contains(reasons(rep), "GET /healthz") || strings.Contains(reasons(rep), "not ready") ||
			rep.Controllers[0].ReadyzOK != nil || s.readyzHits.Load() != 0 {
			t.Fatalf("%s: %v, entry %+v, %d /readyz requests", rep.Health, rep.Reasons, rep.Controllers[0], s.readyzHits.Load())
		}
	})
	t.Run("a controller that does not serve /readyz is not ready", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		s.readyzStatus.Store(http.StatusNotFound)
		rep := s.status(true)
		if rep.Health != Unhealthy || !strings.Contains(reasons(rep), "not ready: GET /readyz on") || !strings.Contains(reasons(rep), "answered 404") {
			t.Fatalf("%s: %v", rep.Health, rep.Reasons)
		}
	})
	t.Run("only the controller is asked: a front on a not-ready host is judged by /healthz", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		s.ready.Store(false)
		*s.body = `{"synced":true,"usage":{"state":"ok"}}`
		s.writeSupervisorFile(s.supRec, EntryRecord{Name: "front", Health: s.health, State: entryRunning, Proc: &s.childRec})
		rep := s.status(true)
		if rep.Health != Healthy || len(rep.Reasons) != 0 || rep.Controllers[0].ReadyzOK != nil || s.readyzHits.Load() != 0 {
			t.Fatalf("%s: %v, entry %+v, %d /readyz requests", rep.Health, rep.Reasons, rep.Controllers[0], s.readyzHits.Load())
		}
	})
}

// TestFailingChecks reads the failing checks' names from bodies that controller-runtime's own
// healthz.Handler writes, never from a body this package wrote, and shows that the handler answers
// a failing check with a status other than 200 (the code reads any such status as not ready).
func TestFailingChecks(t *testing.T) {
	fail := func(*http.Request) error { return errors.New("a reason that must not be needed") }
	pass := func(*http.Request) error { return nil }
	for _, c := range []struct {
		name   string
		checks map[string]crhealthz.Checker
		want   []string
	}{
		{"all pass", map[string]crhealthz.Checker{"ping": crhealthz.Ping, "serving": pass}, nil},
		{"serving fails", map[string]crhealthz.Checker{"ping": crhealthz.Ping, "serving": fail}, []string{"serving"}},
		{"two fail", map[string]crhealthz.Checker{"ping": crhealthz.Ping, "leading": fail, "caches": fail}, []string{"caches", "leading"}},
		{"a name with a dot and a dash", map[string]crhealthz.Checker{"a.b-c_D1": fail}, []string{"a.b-c_D1"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			http.StripPrefix("/readyz", &crhealthz.Handler{Checks: c.checks}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			if (len(c.want) == 0) != (rec.Code == http.StatusOK) {
				t.Fatalf("the handler answered %d for failing checks %v", rec.Code, c.want)
			}
			got := failingChecks(rec.Body.Bytes())
			slices.Sort(got)
			if !slices.Equal(got, c.want) {
				t.Errorf("failingChecks(%q) = %v, want %v", rec.Body.String(), got, c.want)
			}
		})
	}
	t.Run("odd lines give no name", func(t *testing.T) {
		long := strings.Repeat("x", 65)
		for _, body := range []string{"", "ok", "[+]ping ok\n", "[-] failed\n", "[-]bad\x00name failed\n", "[-]" + long + " failed\n", "[-]sp\u00e9cial failed\n", "x[-]serving failed\n"} {
			if got := failingChecks([]byte(body)); len(got) != 0 {
				t.Errorf("failingChecks(%q) = %v, want none", body, got)
			}
		}
	})
	t.Run("the list is bounded", func(t *testing.T) {
		var b strings.Builder
		for i := 0; i < 3*maxFailingChecks; i++ {
			fmt.Fprintf(&b, "[-]c%d failed: reason withheld\n", i)
		}
		if got := failingChecks([]byte(b.String())); len(got) != maxFailingChecks {
			t.Errorf("%d names, want %d", len(got), maxFailingChecks)
		}
	})
}

// TestStatusFrontAbsent: a host with no front configured shows the front as absent and stays
// healthy; a host whose configured front is down is not absent, it is unhealthy. A report that
// said absent for both would hide a crashed front.
func TestStatusFrontAbsent(t *testing.T) {
	withFront := func(s *supervisedFakeHost) {
		s.t.Helper()
		s.st.FrontRun = &FrontRunConfig{Binary: "/opt/hpcburst-front", Config: "/etc/front.yaml"}
		if err := s.l.saveState(s.st); err != nil {
			s.t.Fatal(err)
		}
	}
	frontEntry := func(s *supervisedFakeHost) {
		*s.body = `{"synced":true,"usage":{"state":"ok"}}`
		s.writeSupervisorFile(s.supRec, EntryRecord{Name: "front", Health: s.health, State: entryRunning, Proc: &s.childRec})
	}
	t.Run("no front configured: absent, and healthy", func(t *testing.T) {
		for _, noAPI := range []bool{true, false} {
			rep := newSupervisedFakeHost(t).status(noAPI)
			if !rep.FrontAbsent || rep.Health != Healthy || len(rep.Reasons) != 0 {
				t.Fatalf("noAPI=%v: absent %v, %s: %v", noAPI, rep.FrontAbsent, rep.Health, rep.Reasons)
			}
		}
	})
	t.Run("a front configured and running: not absent", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		withFront(s)
		frontEntry(s)
		rep := s.status(true)
		if rep.FrontAbsent || rep.Health != Healthy {
			t.Fatalf("absent %v, %s: %v", rep.FrontAbsent, rep.Health, rep.Reasons)
		}
	})
	t.Run("a front configured and down: not absent, and unhealthy", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		withFront(s)
		s.writeSupervisorFile(s.supRec, EntryRecord{Name: "front", Health: s.health, State: entryBackoff, Restarts: 1,
			LastExit: &ExitRecord{Code: 1, Detail: "exit status 1", At: time.Now()}})
		rep := s.status(true)
		if rep.FrontAbsent || rep.Health != Unhealthy || !strings.Contains(strings.Join(rep.Reasons, "; "), "front: not running") {
			t.Fatalf("absent %v, %s: %v", rep.FrontAbsent, rep.Health, rep.Reasons)
		}
	})
	t.Run("a stopped host says nothing about a front", func(t *testing.T) {
		rep, err := Status(context.Background(), StatusOptions{StateDir: filepath.Join(shortDir(t), "none")})
		if err != nil || rep.Health != Stopped || rep.FrontAbsent {
			t.Fatalf("%+v, %v", rep, err)
		}
	})
}
