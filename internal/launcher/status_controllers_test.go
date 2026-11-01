package launcher

import (
	"context"
	"errors"
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
		if eh.ReadyzOK == nil || !*eh.ReadyzOK || eh.ReadyzState != ReadyzReady {
			t.Fatalf("a controller that answers 200 on both readiness checks: %+v", eh)
		}
	})
	// The fake answers /healthz and /readyz on their own, /readyz with controller-runtime's own
	// handler: each steering file changes one readiness check, so the cases below fail because of
	// that check and nothing else. Each case also reads the three paths directly first, which shows
	// that the fake answers them differently (a fake that answered every path alike would prove
	// nothing about reading them apart).
	steered := func(t *testing.T, files ...string) {
		t.Helper()
		for _, name := range files {
			f.steer(name, "")
			t.Cleanup(func() { f.unsteer(name) })
		}
	}
	for _, c := range []struct {
		name     string
		files    []string
		paths    map[string]int // what the fake answers, read directly
		state    string
		wantProb string // "" when ready
	}{
		{"ready, a controller before the split (only serving)", []string{"readyz-before-split"},
			map[string]int{"/readyz": 200, "/readyz/caches": 404, "/readyz/leading": 404}, ReadyzReady, ""},
		{"starting: the caches have not synced", []string{"caches-failing"},
			map[string]int{"/readyz": 500, "/readyz/caches": 500, "/readyz/leading": 200}, ReadyzStarting,
			"not ready: starting: GET /readyz/caches on " + e.Health + " answered 500"},
		{"not leading: the Lease is held by another", []string{"leading-failing"},
			map[string]int{"/readyz": 500, "/readyz/caches": 200, "/readyz/leading": 500}, ReadyzNotLeading,
			"not ready: not leading: GET /readyz/leading on " + e.Health + " answered 500"},
		{"both fail: the caches decide first", []string{"caches-failing", "leading-failing"},
			map[string]int{"/readyz": 500, "/readyz/caches": 500, "/readyz/leading": 500}, ReadyzStarting,
			"not ready: starting: GET /readyz/caches on " + e.Health + " answered 500"},
		{"a controller before the split, not ready: /readyz does not say which", []string{"readyz-before-split", "serving-failing"},
			map[string]int{"/readyz": 500, "/readyz/caches": 404, "/readyz/leading": 404}, ReadyzUnspecified,
			"not ready: GET /readyz on " + e.Health + " answered 500 (starting, or not leading its Lease: /readyz does not say which)"},
	} {
		t.Run("alive, "+c.name, func(t *testing.T) {
			steered(t, c.files...)
			if code, err := healthzCode(f.ports.ControllerAddr()); err != nil || code != 200 {
				t.Fatalf("setup: /healthz answers %d, %v; the controller must stay alive", code, err)
			}
			for path, want := range c.paths {
				if code, _, err := probeEndpoint(e.Health, path, healthzTimeout); err != nil || code != want {
					t.Fatalf("setup: the fake answers %d on %s, %v; want %d", code, path, err, want)
				}
			}
			eh, problems := checkEntry(sup, supPID, e)
			if !eh.Running || eh.HealthzOK == nil || !*eh.HealthzOK || eh.ReadyzOK == nil || *eh.ReadyzOK != (c.wantProb == "") || eh.ReadyzState != c.state {
				t.Fatalf("%+v, problems %v", eh, problems)
			}
			if c.wantProb == "" {
				if len(problems) != 0 {
					t.Fatalf("problems %q, want none", problems)
				}
			} else if len(problems) != 1 || problems[0] != c.wantProb {
				t.Fatalf("problems %q, want one: %q", problems, c.wantProb)
			}
		})
	}
	t.Run("ready again once the controller says so", func(t *testing.T) {
		f.steer("caches-failing", "")
		if _, problems := checkEntry(sup, supPID, e); len(problems) != 1 {
			t.Fatalf("setup: problems %v", problems)
		}
		f.unsteer("caches-failing")
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
		// The child is dead when its pidfd says so, not when /proc reads as if it were (issue #154):
		// a killed process closes its descriptors and its leader reads Z for a moment before its
		// pidfd fires, and in that moment checkEntry rightly sees a live process. HBEP-11 (Operational
		// readiness, "Silent failure") and handle.go: death is read from the pidfd only.
		killAndWaitExited(t, *stale.Proc)
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
	ports            []int   // the fixed ports the controller's entry names (EntryRecord.Ports); none by default
	code             *int    // the status code /healthz answers
	body             *string // the body it answers with
	// readyz answers /readyz on its own (readyzFake): a test steers the readiness checks and reads
	// which paths were asked.
	readyz *readyzFake
}

func newSupervisedFakeHost(t *testing.T) *supervisedFakeHost {
	t.Helper()
	h := newFakeHost(t)
	quietControllerRuntime.Do(func() { crlog.SetLogger(logr.Discard()) })
	code, body := http.StatusOK, ""
	readyz := newReadyzFake()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// As the manager mounts its readiness handler (pkg/manager/internal.go:308-310): /readyz
		// and everything under /readyz/.
		if r.URL.Path == "/readyz" || strings.HasPrefix(r.URL.Path, "/readyz/") {
			readyz.ServeHTTP(w, r)
			return
		}
		w.WriteHeader(code)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL)
	s := &supervisedFakeHost{fakeHost: h, health: u.Host, code: &code, body: &body, readyz: readyz}
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
			if c[0].ReadyzOK == nil || !*c[0].ReadyzOK || len(s.readyz.paths()) == 0 {
				t.Fatalf("noAPI=%v: the controller was not asked for its readiness, or it did not answer 200: %+v, %d requests", noAPI, c[0], len(s.readyz.paths()))
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

// readyzFake is the lifecycle controller's /readyz on a test server: controller-runtime's own
// healthz.Handler, mounted as the manager mounts it (pkg/manager/internal.go:308-310), so the
// statuses are its: 200, 500 for a failing check, and 404 for a name it does not know. The
// controller registers two checks, caches and leading; beforeSplit registers the one check
// "serving" that controllers had before the split. A forced answer replaces the handler's on one
// path, to put a status or a body on the wire that the handler would not write.
type readyzFake struct {
	mu                  sync.Mutex
	cachesOK, leadingOK bool
	servingOK           bool
	beforeSplit         bool
	forced              map[string]forcedAnswer
	seen                []string
}

type forcedAnswer struct {
	code int
	body string
	drop bool // close the connection without an answer
}

func newReadyzFake() *readyzFake {
	return &readyzFake{cachesOK: true, leadingOK: true, servingOK: true, forced: map[string]forcedAnswer{}}
}

// set changes the fake's state while it serves.
func (f *readyzFake) set(change func(*readyzFake)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	change(f)
}

// paths returns the paths asked since the fake was made or last cleared.
func (f *readyzFake) paths() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.seen)
}

func (f *readyzFake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.seen = append(f.seen, r.URL.Path)
	fa, forced := f.forced[r.URL.Path]
	before := f.beforeSplit
	f.mu.Unlock()
	if forced && fa.drop {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
		return
	}
	if forced {
		w.WriteHeader(fa.code)
		_, _ = io.WriteString(w, fa.body)
		return
	}
	check := func(ok func(*readyzFake) bool, why string) crhealthz.Checker {
		return func(*http.Request) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			if !ok(f) {
				return errors.New(why)
			}
			return nil
		}
	}
	checks := map[string]crhealthz.Checker{
		"caches":  check(func(f *readyzFake) bool { return f.cachesOK }, "caches not synced"),
		"leading": check(func(f *readyzFake) bool { return f.leadingOK }, "not leading its Lease"),
	}
	if before {
		checks = map[string]crhealthz.Checker{"serving": check(func(f *readyzFake) bool { return f.servingOK }, "not leading, or caches not synced")}
	}
	http.StripPrefix("/readyz", &crhealthz.Handler{Checks: checks}).ServeHTTP(w, r)
}

// TestControllerRuntimeReadyzStatuses pins the facts about controller-runtime v0.23.1's handler that
// the status reading rests on (pkg/healthz/healthz.go:158-165 serves /readyz/<name> and answers 404
// for an unknown name, :175-180 answers 200 or 500 for one check, :96-97 the aggregate 500): a
// bump of controller-runtime that changes one of them fails here before it changes what status says.
func TestControllerRuntimeReadyzStatuses(t *testing.T) {
	quietControllerRuntime.Do(func() { crlog.SetLogger(logr.Discard()) })
	pass := func(*http.Request) error { return nil }
	fail := func(*http.Request) error { return errors.New("a reason") }
	h := http.StripPrefix("/readyz", &crhealthz.Handler{Checks: map[string]crhealthz.Checker{"caches": pass, "leading": fail}})
	for path, want := range map[string]int{"/readyz": 500, "/readyz/": 500, "/readyz/caches": 200, "/readyz/leading": 500, "/readyz/serving": 404, "/readyz/caches/x": 404} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != want {
			t.Errorf("GET %s answered %d, want %d", path, rec.Code, want)
		}
	}
	rec := httptest.NewRecorder()
	http.StripPrefix("/readyz", &crhealthz.Handler{Checks: map[string]crhealthz.Checker{"caches": pass, "leading": pass}}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != 200 {
		t.Errorf("the aggregate answered %d with every check passing", rec.Code)
	}
}

// TestStatusControllerReadyz: status judges the lifecycle controller by its readiness checks
// (HBEP-8, Observers), which fail until the caches have synced and the controller leads its Lease,
// and keeps /healthz as liveness. It reads the two checks on their own paths by status code only
// and tells "starting" from "not leading" (`@hpc-burst-science-agent`, 1 Nov 2026, #147). The fake
// host answers the endpoints on their own, /readyz with controller-runtime's own handler, so each
// case changes one of them.
func TestStatusControllerReadyz(t *testing.T) {
	reasons := func(r *StatusReport) string { return strings.Join(r.Reasons, "; ") }
	const cachesPath, leadingPath = "/readyz/caches", "/readyz/leading"
	// notReady checks the report of a controller that is alive and whose readiness is state.
	notReady := func(t *testing.T, rep *StatusReport, state, reason string) {
		t.Helper()
		c := rep.Controllers[0]
		if rep.Health != Unhealthy || len(rep.Reasons) != 1 || !strings.Contains(reasons(rep), "controller: not ready: ") || !strings.Contains(reasons(rep), reason) {
			t.Fatalf("%s: %v; want one reason with %q", rep.Health, rep.Reasons, reason)
		}
		if !c.Running || c.HealthzOK == nil || !*c.HealthzOK || c.ReadyzOK == nil || *c.ReadyzOK || c.ReadyzState != state {
			t.Errorf("the controller is alive but not ready (%s): %+v", state, c)
		}
	}
	t.Run("control: alive and ready, each check asked on its own path", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		rep := s.status(true)
		if rep.Health != Healthy || len(rep.Reasons) != 0 || !slices.Equal(s.readyz.paths(), []string{cachesPath, leadingPath}) {
			t.Fatalf("%s: %v, paths asked %v", rep.Health, rep.Reasons, s.readyz.paths())
		}
		if c := rep.Controllers[0]; c.ReadyzOK == nil || !*c.ReadyzOK || c.ReadyzState != ReadyzReady {
			t.Errorf("%+v", c)
		}
	})
	t.Run("starting: the caches have not synced, and nothing else is asked", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		s.readyz.set(func(f *readyzFake) { f.cachesOK = false })
		notReady(t, s.status(true), ReadyzStarting, "starting: GET "+cachesPath+" on "+s.health+" answered 500")
		if got := s.readyz.paths(); !slices.Equal(got, []string{cachesPath}) {
			t.Errorf("paths asked %v, want only %s", got, cachesPath)
		}
	})
	t.Run("not leading: the caches have synced and the Lease is held by another", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		s.readyz.set(func(f *readyzFake) { f.leadingOK = false })
		notReady(t, s.status(true), ReadyzNotLeading, "not leading: GET "+leadingPath+" on "+s.health+" answered 500")
		if got := s.readyz.paths(); !slices.Equal(got, []string{cachesPath, leadingPath}) {
			t.Errorf("paths asked %v, want %s then %s", got, cachesPath, leadingPath)
		}
	})
	t.Run("both fail: starting", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		s.readyz.set(func(f *readyzFake) { f.cachesOK, f.leadingOK = false, false })
		notReady(t, s.status(true), ReadyzStarting, "starting: GET "+cachesPath)
	})
	t.Run("not ready, and the API is asked too", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		s.readyz.set(func(f *readyzFake) { f.leadingOK = false })
		notReady(t, s.status(false), ReadyzNotLeading, "not leading: GET "+leadingPath)
	})
	// The decision is the status code, never the body: a body that says the opposite changes
	// nothing, and neither does the status class (the API server answers 503).
	for _, c := range []struct {
		name    string
		forced  map[string]forcedAnswer
		state   string
		healthy bool
	}{
		{"caches 200 whose body names a failing check", map[string]forcedAnswer{cachesPath: {code: 200, body: "[+]ping ok\n[-]caches failed: reason withheld\nhealthz check failed\n"}}, ReadyzReady, true},
		{"caches 500 whose body says ok", map[string]forcedAnswer{cachesPath: {code: 500, body: "ok"}}, ReadyzStarting, false},
		{"caches 503", map[string]forcedAnswer{cachesPath: {code: 503, body: ""}}, ReadyzStarting, false},
		{"leading 503 with a body that says ok", map[string]forcedAnswer{leadingPath: {code: 503, body: "ok"}}, ReadyzNotLeading, false},
		{"leading 200 whose body names a failing check", map[string]forcedAnswer{leadingPath: {code: 200, body: "[-]leading failed: reason withheld\n"}}, ReadyzReady, true},
	} {
		t.Run("by status code only: "+c.name, func(t *testing.T) {
			s := newSupervisedFakeHost(t)
			s.readyz.set(func(f *readyzFake) { f.forced = c.forced })
			rep := s.status(true)
			if c.healthy {
				if rep.Health != Healthy || len(rep.Reasons) != 0 || rep.Controllers[0].ReadyzState != c.state {
					t.Fatalf("%s: %v, %+v", rep.Health, rep.Reasons, rep.Controllers[0])
				}
				return
			}
			notReady(t, rep, c.state, "")
		})
	}
	// A controller from before the split registers one check, "serving". Its per-check paths
	// answer 404 from controller-runtime itself, and the aggregate decides.
	const unspecified = "answered 500 (starting, or not leading its Lease: /readyz does not say which)"
	t.Run("an older controller, not ready: the aggregate's code and the fallback text", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		s.readyz.set(func(f *readyzFake) { f.beforeSplit, f.servingOK = true, false })
		notReady(t, s.status(true), ReadyzUnspecified, "GET /readyz on "+s.health+" "+unspecified)
		if got := s.readyz.paths(); !slices.Equal(got, []string{cachesPath, "/readyz"}) {
			t.Errorf("paths asked %v, want %s then /readyz", got, cachesPath)
		}
	})
	t.Run("an older controller, ready: healthy", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		s.readyz.set(func(f *readyzFake) { f.beforeSplit = true })
		rep := s.status(true)
		if rep.Health != Healthy || len(rep.Reasons) != 0 || rep.Controllers[0].ReadyzState != ReadyzReady || rep.Controllers[0].ReadyzOK == nil || !*rep.Controllers[0].ReadyzOK {
			t.Fatalf("%s: %v, %+v", rep.Health, rep.Reasons, rep.Controllers[0])
		}
	})
	t.Run("a 404 on the second path alone is the older controller too", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		s.readyz.set(func(f *readyzFake) {
			f.forced = map[string]forcedAnswer{leadingPath: {code: 404, body: "404 page not found\n"}}
			f.leadingOK = false
		})
		// The fake would fail "leading" if it were asked as a check: the 404 sends status to the
		// aggregate instead, which the failing check turns to 500.
		notReady(t, s.status(true), ReadyzUnspecified, unspecified)
		if got := s.readyz.paths(); !slices.Equal(got, []string{cachesPath, leadingPath, "/readyz"}) {
			t.Errorf("paths asked %v", got)
		}
	})
	t.Run("a controller that serves no /readyz at all is not ready", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		s.readyz.set(func(f *readyzFake) {
			f.forced = map[string]forcedAnswer{cachesPath: {code: 404, body: ""}, leadingPath: {code: 404, body: ""}, "/readyz": {code: 404, body: ""}}
		})
		rep := s.status(true)
		if rep.Health != Unhealthy || !strings.Contains(reasons(rep), "not ready: GET /readyz on") || !strings.Contains(reasons(rep), "answered 404") {
			t.Fatalf("%s: %v", rep.Health, rep.Reasons)
		}
	})
	t.Run("the paths cannot be read: unhealthy, and no state", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		s.readyz.set(func(f *readyzFake) { f.forced = map[string]forcedAnswer{cachesPath: {drop: true}} })
		rep := s.status(true)
		if rep.Health != Unhealthy || len(rep.Reasons) != 1 || !strings.Contains(reasons(rep), "GET "+cachesPath+" on") {
			t.Fatalf("%s: %v", rep.Health, rep.Reasons)
		}
		if c := rep.Controllers[0]; c.ReadyzOK == nil || *c.ReadyzOK || c.ReadyzState != "" {
			t.Errorf("%+v", c)
		}
	})
	t.Run("ready again: the same host turns healthy", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		s.readyz.set(func(f *readyzFake) { f.leadingOK = false })
		if rep := s.status(true); rep.Health != Unhealthy {
			t.Fatalf("setup: %s: %v", rep.Health, rep.Reasons)
		}
		s.readyz.set(func(f *readyzFake) { f.leadingOK = true })
		if rep := s.status(true); rep.Health != Healthy || len(rep.Reasons) != 0 {
			t.Fatalf("%s: %v", rep.Health, rep.Reasons)
		}
	})
	t.Run("not alive but answering ready: unhealthy by liveness, and readiness is not asked", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		*s.code = http.StatusServiceUnavailable
		rep := s.status(true)
		if rep.Health != Unhealthy || !strings.Contains(reasons(rep), "GET /healthz") || strings.Contains(reasons(rep), "not ready") ||
			rep.Controllers[0].ReadyzOK != nil || len(s.readyz.paths()) != 0 {
			t.Fatalf("%s: %v, entry %+v, paths asked %v", rep.Health, rep.Reasons, rep.Controllers[0], s.readyz.paths())
		}
	})
	t.Run("only the controller is asked: a front on a not-ready host is judged by /healthz", func(t *testing.T) {
		s := newSupervisedFakeHost(t)
		s.readyz.set(func(f *readyzFake) { f.cachesOK = false })
		*s.body = `{"synced":true,"usage":{"state":"ok"}}`
		s.writeSupervisorFile(s.supRec, EntryRecord{Name: "front", Health: s.health, State: entryRunning, Proc: &s.childRec})
		rep := s.status(true)
		if rep.Health != Healthy || len(rep.Reasons) != 0 || rep.Controllers[0].ReadyzOK != nil || len(s.readyz.paths()) != 0 {
			t.Fatalf("%s: %v, entry %+v, paths asked %v", rep.Health, rep.Reasons, rep.Controllers[0], s.readyz.paths())
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
