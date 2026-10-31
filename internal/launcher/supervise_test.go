package launcher

import (
	"bytes"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// syncBuffer is a log writer that tests read while the supervisor writes.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// TestBackoff: the delays are those of the brief: 1 s, doubling to a cap of 30 s, and back to 1 s
// after a child that ran for 60 s. The expected sequence is written out by hand.
func TestBackoff(t *testing.T) {
	b := &backoff{min: DefaultBackoffMin, max: DefaultBackoffMax, stable: DefaultStableAfter}
	for i, want := range []time.Duration{1, 2, 4, 8, 16, 30, 30, 30} {
		if got := b.next(time.Second); got != want*time.Second {
			t.Fatalf("delay %d = %s, want %s", i+1, got, want*time.Second)
		}
	}
	if got := b.next(59*time.Second + 900*time.Millisecond); got != 30*time.Second {
		t.Errorf("after 59.9 s of uptime the delay is %s: the run was not yet stable", got)
	}
	if got := b.next(60 * time.Second); got != time.Second {
		t.Errorf("after 60 s of uptime the delay is %s, want 1s", got)
	}
	if got := b.next(0); got != 2*time.Second {
		t.Errorf("the next quick exit waits %s, want 2s: the doubling starts again", got)
	}
}

func TestSuperviseDefaults(t *testing.T) {
	var o SuperviseOptions
	o.defaults()
	if o.BackoffMin != time.Second || o.BackoffMax != 30*time.Second || o.StableAfter != 60*time.Second || o.TermGrace != 10*time.Second {
		t.Fatalf("defaults %+v", o)
	}
}

// TestSuperviseRestartsAnyExit: a child that exits is started again, whatever its exit code, and
// the restart is counted. The fake child really exits (it is told to by a file), so the restart
// path runs; the old process is gone, and the new one is the process the record names.
func TestSuperviseRestartsAnyExit(t *testing.T) {
	for _, code := range []int{3, 0} {
		t.Run(fmt.Sprintf("exit code %d", code), func(t *testing.T) {
			f := newCtlFixture(t, Front{}, "")
			f.supervise(SuperviseOptions{BackoffMin: 20 * time.Millisecond, BackoffMax: 80 * time.Millisecond})
			first := f.awaitRunning()
			if first.Restarts != 0 || first.LastExit != nil {
				t.Fatalf("a first start: %+v", first)
			}
			pid1 := first.Proc.PID
			awaitHealthy(t, f.ports.ControllerAddr())

			f.steer("exit", strconv.Itoa(code))
			second := f.await("the restart", func(e EntryRecord) bool {
				return e.Restarts == 1 && e.State == entryRunning && e.Proc != nil && e.Proc.PID != pid1
			})
			if second.LastExit == nil || second.LastExit.Code != code {
				t.Errorf("last exit %+v, want code %d", second.LastExit, code)
			}
			if !waitNotRunning(pid1, 5*time.Second) {
				t.Errorf("the first child (pid %d) still runs", pid1)
			}
			if got := startTicksOf(t, second.Proc.PID); got != second.Proc.StartTicks {
				t.Errorf("the record gives start time %d for pid %d, /proc says %d", second.Proc.StartTicks, second.Proc.PID, got)
			}
			awaitHealthy(t, f.ports.ControllerAddr())
			if got := f.starts(); !slices.Equal(got, []int{pid1, second.Proc.PID}) {
				t.Errorf("the fake started as %v, want exactly two starts: %d then %d", got, pid1, second.Proc.PID)
			}
		})
	}
}

func awaitHealthy(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		code, err := healthzCode(addr)
		if err == nil && code == 200 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("GET /healthz on %s: %d, %v", addr, code, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type delayLog struct {
	mu sync.Mutex
	d  []time.Duration
}

func (l *delayLog) add(_ string, d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.d = append(l.d, d)
}

func (l *delayLog) get() []time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.d)
}

func (l *delayLog) await(t *testing.T, n int) []time.Duration {
	t.Helper()
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if d := l.get(); len(d) >= n {
			return d
		}
	}
	t.Fatalf("only %d restart delays were chosen, want %d", len(l.get()), n)
	return nil
}

// startTimes reads the fake's start times (unix nanoseconds) in order.
func (f *ctlFixture) startTimes() []time.Time {
	var out []time.Time
	for _, line := range strings.Split(strings.TrimSpace(fileOf(filepath.Join(f.ctl, "starts"))), "\n") {
		if fs := strings.Fields(line); len(fs) == 2 {
			if n, err := strconv.ParseInt(fs[1], 10, 64); err == nil {
				out = append(out, time.Unix(0, n))
			}
		}
	}
	return out
}

// TestSuperviseBackoffGrowsAndIsCapped: a child that exits at every start is restarted after 10,
// 20, 40 ms and then 40 ms again (the cap), and each restart really waited: the fake's own start
// times are at least the chosen delay apart.
func TestSuperviseBackoffGrowsAndIsCapped(t *testing.T) {
	f := newCtlFixture(t, Front{}, "")
	f.steer("always-exit", "1")
	var dl delayLog
	f.supervise(SuperviseOptions{BackoffMin: 10 * time.Millisecond, BackoffMax: 40 * time.Millisecond, StableAfter: time.Hour, onDelay: dl.add})
	got := dl.await(t, 6)[:6]
	want := []time.Duration{10, 20, 40, 40, 40, 40}
	for i := range want {
		if got[i] != want[i]*time.Millisecond {
			t.Fatalf("delays %v, want %v ms", got, want)
		}
	}
	starts := f.startTimes()
	for i := 0; i < 5 && i+1 < len(starts); i++ {
		if gap := starts[i+1].Sub(starts[i]); gap < got[i] {
			t.Errorf("start %d followed start %d after %s, less than the delay of %s", i+2, i+1, gap, got[i])
		}
	}
}

// TestSuperviseBackoffResetsAfterAStableRun: after quick exits have raised the delay, a child
// that ran for longer than the stable time is restarted after the minimum again.
func TestSuperviseBackoffResetsAfterAStableRun(t *testing.T) {
	f := newCtlFixture(t, Front{}, "")
	f.steer("always-exit", "1")
	var dl delayLog
	f.supervise(SuperviseOptions{BackoffMin: 10 * time.Millisecond, BackoffMax: time.Second, StableAfter: time.Second, onDelay: dl.add})
	dl.await(t, 3) // 10, 20, 40 ms at least
	f.unsteer("always-exit")
	f.awaitRunning()
	time.Sleep(1200 * time.Millisecond) // longer than the stable time
	before := len(dl.get())
	f.steer("exit", "1")
	got := dl.await(t, before+1)
	if last := got[before]; last != 10*time.Millisecond {
		t.Errorf("after a stable run the delay is %s, want the minimum of 10ms; all delays %v", last, got)
	}
	if slices.Max(got[:before]) < 40*time.Millisecond {
		t.Errorf("the delays before it never grew: %v", got[:before])
	}
}

// TestSuperviseStopsItsChildren: on cancel, the child is stopped, the supervisor returns nil, its
// record says stopped, and nothing is started again.
func TestSuperviseStopsItsChildren(t *testing.T) {
	f := newCtlFixture(t, Front{}, "")
	stop := f.supervise(SuperviseOptions{BackoffMin: 20 * time.Millisecond})
	pid := f.awaitRunning().Proc.PID
	if err := stop(); err != nil {
		t.Fatalf("Supervise returned %v", err)
	}
	if !waitNotRunning(pid, 5*time.Second) {
		t.Fatalf("the child (pid %d) still runs after the supervisor returned", pid)
	}
	e := f.record().Entries[0]
	if e.State != entryStopped || e.Proc != nil || e.LastExit == nil || e.LastExit.Code != 0 {
		t.Errorf("record after stop: %+v (last exit %+v); want stopped, no process, exit 0", e, e.LastExit)
	}
	time.Sleep(300 * time.Millisecond)
	if got := f.starts(); len(got) != 1 {
		t.Errorf("the fake started %d times, want 1: the supervisor restarted a child it had stopped", len(got))
	}
}

// TestSuperviseKillsAChildThatIgnoresTerm: SIGKILL follows the grace period.
func TestSuperviseKillsAChildThatIgnoresTerm(t *testing.T) {
	f := newCtlFixture(t, Front{}, "")
	f.steer("ignore-term", "1")
	var log syncBuffer
	stop := f.supervise(SuperviseOptions{TermGrace: 300 * time.Millisecond, Log: &log})
	pid := f.awaitRunning().Proc.PID
	t0 := time.Now()
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(t0); took < 300*time.Millisecond {
		t.Errorf("returned after %s, before the grace period", took)
	}
	if !waitNotRunning(pid, 5*time.Second) {
		t.Fatalf("pid %d survived SIGTERM and SIGKILL", pid)
	}
	if e := f.record().Entries[0]; e.LastExit == nil || !strings.Contains(e.LastExit.Detail, "killed") {
		t.Errorf("last exit %+v, want a kill", e.LastExit)
	}
	if !strings.Contains(log.String(), "sending SIGKILL") {
		t.Errorf("the log does not say SIGKILL was sent:\n%s", log.String())
	}
}

// TestSuperviseCommandLine: the controller gets exactly the flags of its contract, from the
// record; the front flags go only to a launcher that recorded a front, and the agent builds
// directory only when one was given. The environment loses KUBECONFIG and the in-cluster
// variables, and keeps the rest. The expected vectors are written out from the brief.
func TestSuperviseCommandLine(t *testing.T) {
	t.Setenv("KUBECONFIG", "/etc/rancher/k3s/k3s.yaml")
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.0.0.1")
	t.Setenv("KUBERNETES_SERVICE_PORT", "443")
	cases := []struct {
		name   string
		front  Front
		builds string
		extra  []string
	}{
		{"nothing optional", Front{}, "", nil},
		{"agent builds directory", Front{}, "/srv/agent-builds", []string{"--agent-builds-dir", "/srv/agent-builds"}},
		{"front", Front{Mode: "direct", Addr: "front.example.test:8443"}, "", []string{"--front-mode", "direct", "--front-addr", "front.example.test:8443"}},
		{"both", Front{Mode: "ssh", Addr: "front.example.test:22", KeyPath: "/ignored"}, "/srv/agent-builds",
			[]string{"--agent-builds-dir", "/srv/agent-builds", "--front-mode", "ssh", "--front-addr", "front.example.test:22"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newCtlFixture(t, c.front, c.builds)
			f.supervise(SuperviseOptions{})
			pid := f.awaitRunning().Proc.PID
			want := append([]string{
				"--kubeconfig", filepath.Join(f.l.dir, "kubeconfig"),
				"--credentials-dir", filepath.Join(f.l.dir, "controller", "credentials"),
				"--health-addr", "127.0.0.1:" + strconv.Itoa(f.ports.Controller),
			}, c.extra...)
			if got := f.argvOf(pid); !slices.Equal(got, want) {
				t.Errorf("controller argv\n got %q\nwant %q", got, want)
			}
			env := f.envOf(pid)
			for _, kv := range env {
				for _, banned := range []string{"KUBECONFIG=", "KUBERNETES_SERVICE_HOST=", "KUBERNETES_SERVICE_PORT="} {
					if strings.HasPrefix(kv, banned) {
						t.Errorf("the controller's environment has %s", kv)
					}
				}
			}
			if !slices.Contains(env, "FAKECTRL_DIR="+f.ctl) {
				t.Errorf("the controller's environment lost the supervisor's own variables: %q", env)
			}
		})
	}
}

func TestSuperviseNeedsAControllerRecord(t *testing.T) {
	f := newCtlFixture(t, Front{}, "")
	f.st.Controllers = nil
	if err := f.l.saveState(f.st); err != nil {
		t.Fatal(err)
	}
	err := Supervise(t.Context(), SuperviseOptions{StateDir: f.l.dir})
	if err == nil || !strings.Contains(err.Error(), "no controller binary") {
		t.Fatalf("Supervise = %v", err)
	}
}

// TestSuperviseReportsAChildThatCannotStart: a missing binary is retried, not fatal to the
// supervisor, and the record says why.
func TestSuperviseReportsAChildThatCannotStart(t *testing.T) {
	f := newCtlFixture(t, Front{}, "")
	f.st.Controllers.Binary = filepath.Join(f.ctl, "no-such-binary")
	if err := f.l.saveState(f.st); err != nil {
		t.Fatal(err)
	}
	f.supervise(SuperviseOptions{BackoffMin: 10 * time.Millisecond, BackoffMax: 20 * time.Millisecond})
	e := f.await("a failed start", func(e EntryRecord) bool { return e.LastExit != nil })
	if e.Proc != nil || e.State != entryBackoff || e.LastExit.Code != -1 || !strings.Contains(e.LastExit.Detail, "start failed") {
		t.Errorf("entry %+v, last exit %+v", e, e.LastExit)
	}
}
