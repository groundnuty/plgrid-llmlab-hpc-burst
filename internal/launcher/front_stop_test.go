package launcher

// The front's stop and start under the supervisor (PR 134 review: B1, B2). These run real
// processes (the fake controller, installed as hpcburst-front for the front) and no k3s. Every
// wait is on observable state: supervisor.json, a file the fake writes, a function's return.

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front"
)

// withFront adds a front to the fixture's record: the fake installed as hpcburst-front, a
// configuration file, and a free port for its /healthz.
func withFront(f *ctlFixture) {
	f.t.Helper()
	b, err := os.ReadFile(fakeControllerBin(f.t))
	if err != nil {
		f.t.Fatal(err)
	}
	dir := f.t.TempDir()
	bin := filepath.Join(dir, FrontBinName)
	if err := os.WriteFile(bin, b, 0o755); err != nil {
		f.t.Fatal(err)
	}
	cfg := filepath.Join(dir, "front.json")
	if err := os.WriteFile(cfg, []byte("{}\n"), 0o600); err != nil {
		f.t.Fatal(err)
	}
	f.st.FrontRun = &FrontRunConfig{Binary: bin, Config: cfg}
	f.st.Ports.FrontHealth = freePort(f.t)
	if err := f.l.saveState(f.st); err != nil {
		f.t.Fatal(err)
	}
}

// entryNamed polls supervisor.json until cond holds for the named entry.
func (f *ctlFixture) entryNamed(name, what string, cond func(e EntryRecord) bool) EntryRecord {
	f.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if sf, err := readSupervisorFile(f.l); err == nil {
			for _, e := range sf.Entries {
				if e.Name == name && cond(e) {
					return e
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	f.t.Fatalf("timed out waiting for %s; supervisor.json: %s", what, fileOf(f.l.supervisorFile()))
	return EntryRecord{}
}

// TestFrontStopBudget: the front's kill grace covers its longest normal stop, and each grace above
// it is longer than the one it contains. The shutdown-grace part is checked against the front's
// own constant, so a front that stops slower makes this fail instead of losing usage records.
func TestFrontStopBudget(t *testing.T) {
	if frontShutdownGrace != front.DefaultShutdownGrace {
		t.Errorf("the budget counts a shutdown grace of %s, the front's is %s", frontShutdownGrace, front.DefaultShutdownGrace)
	}
	if want := 2*front.DefaultShutdownGrace + 5*time.Second + time.Second; FrontStopBudget != want {
		t.Errorf("FrontStopBudget %s, want %s: in flight, handlers, a cut stream's last event, the sink", FrontStopBudget, want)
	}
	for _, c := range []struct {
		inner, outer       time.Duration
		innerName, outerOf string
	}{
		{FrontStopBudget, FrontTermGrace, "the front's stop", "the supervisor's kill grace for it"},
		{FrontTermGrace, SupervisorGraceWithFront, "the front's kill grace", "down's least grace for its supervisor"},
		{SupervisorGraceWithFront, DefaultGrace, "down's least grace with a front", "down's default grace"},
	} {
		if c.outer <= c.inner {
			t.Errorf("%s (%s) is not longer than %s (%s)", c.outerOf, c.outer, c.innerName, c.inner)
		}
	}
}

// TestSupervisorGivesTheFrontItsOwnGrace: on shutdown the supervisor waits for a front that takes
// longer to stop than the other processes' grace, instead of killing it, while the controller,
// which stops as slowly, is killed after that grace. Both fakes answer SIGTERM after 1.5 s; the
// test shortens the general grace to 0.3 s, so only the front's own grace lets it finish.
func TestSupervisorGivesTheFrontItsOwnGrace(t *testing.T) {
	f := newCtlFixture(t, Front{})
	withFront(f)
	f.steer("slow-term", "1500")
	stop := f.supervise(SuperviseOptions{TermGrace: 300 * time.Millisecond})
	running := func(e EntryRecord) bool { return e.State == entryRunning && e.Proc != nil }
	f.entryNamed("controller", "the controller to run", running)
	f.entryNamed(frontEntryName, "the front to run", running)
	// Each fake installs its SIGTERM handler before it binds its health port, so an answer on that
	// port means a SIGTERM now reaches the handler, not the default action.
	awaitHealthy(t, f.ports.ControllerAddr())
	awaitHealthy(t, f.st.Ports.FrontHealthAddr())
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	exited := func(e EntryRecord) bool { return e.State == entryStopped && e.LastExit != nil }
	if e := f.entryNamed(frontEntryName, "the front's exit", exited); e.LastExit.Detail != "exit status 0" {
		t.Errorf("the front ended with %q: it was not given its own grace to stop", e.LastExit.Detail)
	}
	if e := f.entryNamed("controller", "the controller's exit", exited); e.LastExit.Detail != "signal: killed" {
		t.Errorf("the controller ended with %q, want signal: killed (the control: the short grace applies to it)", e.LastExit.Detail)
	}
}

// TestDownRefusesAGraceTheFrontCannotStopWithin: down with a grace shorter than the front needs is
// refused before anything is stopped, with ErrGraceTooShort; the record and every process are
// left as they were. The same grace on a host without a front stops it (the control).
func TestDownRefusesAGraceTheFrontCannotStopWithin(t *testing.T) {
	for _, withFrontRun := range []bool{true, false} {
		name := map[bool]string{true: "a supervisor that runs the front", false: "control: no front"}[withFrontRun]
		t.Run(name, func(t *testing.T) {
			l, err := newLayout(filepath.Join(shortDir(t), "st"))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(l.dir, 0o700); err != nil {
				t.Fatal(err)
			}
			var procs []ProcRec
			for _, n := range []string{"k3s", supervisorName} {
				p := startBystander(t, "sleep", "300")
				rec, err := newRec(n, p.Process.Pid)
				if err != nil {
					t.Fatal(err)
				}
				procs = append(procs, rec)
			}
			st := &State{Version: "v1.36.5+k3s1", Procs: procs, Controllers: &ControllerConfig{Binary: "/opt/c"}}
			if withFrontRun {
				st.FrontRun = &FrontRunConfig{Binary: "/opt/f", Config: "/etc/f.json"}
			}
			if err := l.saveState(st); err != nil {
				t.Fatal(err)
			}
			_, err = Down(context.Background(), DownOptions{StateDir: l.dir, Grace: 20 * time.Second})
			if !withFrontRun {
				if err != nil {
					t.Fatalf("down without a front: %v", err)
				}
				return
			}
			if !errors.Is(err, ErrGraceTooShort) || !strings.Contains(err.Error(), SupervisorGraceWithFront.String()) {
				t.Fatalf("down --grace 20s with a front: %v; want ErrGraceTooShort naming %s", err, SupervisorGraceWithFront)
			}
			for _, p := range procs {
				if v, d := identify(p); v != Ours {
					t.Errorf("%s (pid %d) was touched by a refused down: %s", p.Name, p.PID, d)
				}
			}
			if kept, err := l.loadState(); err != nil || kept == nil {
				t.Errorf("the record was removed by a refused down: %v", err)
			}
		})
	}
}

// TestStartSupervisorWaitsForTheFront: the start waits until the front holds its health port and
// answers /healthz, which the real front does only after its first lists. The fake front binds
// only once the test opens its gate (bind-now), so the start cannot succeed before that; a port
// taken in between makes the start fail, and with the port free it succeeds.
func TestStartSupervisorWaitsForTheFront(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, taken := range []bool{true, false} {
		name := map[bool]string{true: "a port taken before the front binds fails the start", false: "control: the front binds late and the start succeeds"}[taken]
		t.Run(name, func(t *testing.T) {
			f := newCtlFixture(t, Front{})
			withFront(f)
			t.Setenv(superviseEnv, f.l.dir)
			k3s := startBystander(t, "sleep", "300")
			k3sRec, err := newRec("k3s", k3s.Process.Pid)
			if err != nil {
				t.Fatal(err)
			}
			f.st.Procs = []ProcRec{k3sRec}
			stopOnExit(t, f)
			f.steer("bind-gate", "")
			res := make(chan error, 1)
			go func() {
				res <- startSupervisor(context.Background(), f.l, f.st, []string{self}, 20*time.Second, testLog{t})
			}()
			waiting := filepath.Join(f.ctl, "front-waiting")
			for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(10 * time.Millisecond) {
				if _, err := os.Stat(waiting); err == nil {
					break
				}
				select {
				case err := <-res:
					t.Fatalf("the start returned before the front reached its bind: %v", err)
				default:
				}
				if time.Now().After(deadline) {
					t.Fatal("the fake front never reached its bind")
				}
			}
			if taken {
				l, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(f.st.Ports.FrontHealth)))
				if err != nil {
					t.Fatal(err)
				}
				defer l.Close()
			}
			// The gate is still closed, so the front cannot be ready: the start must still wait.
			select {
			case err := <-res:
				t.Fatalf("the start returned while the front had not bound its health port: %v", err)
			default:
			}
			f.steer("bind-now", "")
			var got error
			select {
			case got = <-res:
			case <-time.After(30 * time.Second):
				t.Fatal("the start did not return")
			}
			switch {
			case taken && (got == nil || !strings.Contains(got.Error(), "front")):
				t.Fatalf("with the front's health port taken: %v; want a failed start naming the front", got)
			case !taken && got != nil:
				t.Fatalf("with the port free: %v", got)
			}
		})
	}
}
