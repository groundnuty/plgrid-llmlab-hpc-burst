package launcher

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// downFixture records the process as a launched host in a fresh state directory.
func downFixture(t *testing.T, rec ProcRec) layout {
	t.Helper()
	l, err := newLayout(shortDir(t))
	if err != nil {
		t.Fatal(err)
	}
	st := &State{Version: "v1.36.5+k3s1", Ports: Ports{API: 59001, HTTPS: 59002, KCM: 59003, LB: 59006}, Procs: []ProcRec{rec}, StartedAt: time.Now()}
	if err := l.saveState(st); err != nil {
		t.Fatal(err)
	}
	// A kubeconfig that down must remove together with the record.
	if err := os.WriteFile(l.kubeconfig(), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	return l
}

func waitNotRunning(pid int, d time.Duration) bool {
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if !runningNow(pid) {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return !runningNow(pid)
}

// TestDownNeverSignalsAForeignProcess is the PID-reuse test: the recorded PID is alive, but it is
// not the recorded process. The same fixture with the right start time is the control: it must
// be stopped, which shows that the foreign cases survive because of the identity check, not
// because down failed to signal anything.
func TestDownNeverSignalsAForeignProcess(t *testing.T) {
	cases := []struct {
		name      string
		tweak     func(*ProcRec)
		wantAlive bool
		verdict   string
	}{
		{"start time differs (the PID was reused)", func(r *ProcRec) { r.StartTicks++ }, true, "foreign"},
		{"start time is far off", func(r *ProcRec) { r.StartTicks += 100000 }, true, "foreign"},
		{"recorded in another boot", func(r *ProcRec) { r.BootID = "00000000-not-this-boot" }, true, "foreign"},
		{"control: identity matches", func(r *ProcRec) {}, false, "ours"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cmd := startBystander(t, "sleep", "300")
			pid := cmd.Process.Pid
			rec, err := newRec("k3s", pid)
			if err != nil {
				t.Fatal(err)
			}
			startBefore := rec.StartTicks
			c.tweak(&rec)
			l := downFixture(t, rec)

			rep, err := Down(context.Background(), DownOptions{StateDir: l.dir, Grace: 5 * time.Second})
			if err != nil {
				t.Fatalf("down: %v", err)
			}
			if rep == nil || len(rep.Procs) != 1 || rep.Procs[0].Before != c.verdict {
				t.Fatalf("report: %+v", rep)
			}
			if c.wantAlive {
				// Give a stray signal time to arrive, then check the process by its own /proc entry.
				time.Sleep(500 * time.Millisecond)
				if !runningNow(pid) {
					t.Fatalf("down signalled a process that was not the recorded one (state %c)", procState(pid))
				}
				if got, err := readStat(pid); err != nil || got.StartTicks != startBefore {
					t.Fatalf("the bystander changed: %+v, %v", got, err)
				}
				if rep.Procs[0].Signals != "" {
					t.Errorf("report claims signals %q", rep.Procs[0].Signals)
				}
				if rep.Procs[0].Detail == "" {
					t.Error("a foreign verdict must say why")
				}
			} else {
				if !waitNotRunning(pid, 5*time.Second) {
					t.Fatal("the control process was not stopped")
				}
				if rep.Procs[0].Signals != "TERM" {
					t.Errorf("signals = %q, want TERM", rep.Procs[0].Signals)
				}
			}
			// Either way the host is no longer recorded: the record and the kubeconfig are gone.
			for _, f := range []string{l.stateFile(), l.kubeconfig()} {
				if _, err := os.Stat(f); !os.IsNotExist(err) {
					t.Errorf("%s still exists after down (%v)", f, err)
				}
			}
		})
	}
}

func TestDownWithAGoneProcess(t *testing.T) {
	cmd := startBystander(t, "sleep", "300")
	pid := cmd.Process.Pid
	rec, _ := newRec("k3s", pid)
	_ = cmd.Process.Kill()
	if !waitNotRunning(pid, 5*time.Second) {
		t.Fatal("could not kill the fixture process")
	}
	l := downFixture(t, rec)
	rep, err := Down(context.Background(), DownOptions{StateDir: l.dir, Grace: time.Second})
	if err != nil || rep.Procs[0].Before != "gone" || rep.Procs[0].Signals != "" {
		t.Fatalf("%+v, %v", rep, err)
	}
	if _, err := os.Stat(l.stateFile()); !os.IsNotExist(err) {
		t.Error("record survived")
	}
}

func TestDownWithNothingRecorded(t *testing.T) {
	if rep, err := Down(context.Background(), DownOptions{StateDir: shortDir(t) + "/missing"}); rep != nil || err != nil {
		t.Errorf("missing directory: %v, %v", rep, err)
	}
	if rep, err := Down(context.Background(), DownOptions{StateDir: shortDir(t)}); rep != nil || err != nil {
		t.Errorf("empty directory: %v, %v", rep, err)
	}
}

// TestDownEscalatesAndStopsDescendants: the recorded process ignores SIGTERM (the signal is
// ignored by the shell and inherited as ignored by its child), so down must send SIGKILL after
// the grace period, to the recorded process and to the child found while the parent was verified.
func TestDownEscalatesAndStopsDescendants(t *testing.T) {
	cmd := startBystander(t, "sh", "-c", `trap "" TERM; sleep 300 & wait`)
	pid := cmd.Process.Pid
	// Wait for the child to exist, so that it can be found as a descendant.
	var child int
	for end := time.Now().Add(5 * time.Second); time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
		for _, d := range descendants([]int{pid}) {
			if d != pid {
				child = d
			}
		}
		if child != 0 {
			break
		}
	}
	if child == 0 {
		t.Fatal("the shell started no child")
	}
	rec, err := newRec("k3s", pid)
	if err != nil {
		t.Fatal(err)
	}
	l := downFixture(t, rec)
	start := time.Now()
	rep, err := Down(context.Background(), DownOptions{StateDir: l.dir, Grace: 400 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rep.Procs[0].Signals, "KILL") {
		t.Errorf("signals = %q, want an escalation to KILL", rep.Procs[0].Signals)
	}
	if time.Since(start) < 400*time.Millisecond {
		t.Errorf("down returned after %s, before the grace period", time.Since(start))
	}
	if !waitNotRunning(pid, 5*time.Second) || !waitNotRunning(child, 5*time.Second) {
		t.Fatalf("parent running=%v, child running=%v", runningNow(pid), runningNow(child))
	}
}

func TestSignalOursChecksIdentityFirst(t *testing.T) {
	cmd := startBystander(t, "sleep", "300")
	rec, _ := newRec("x", cmd.Process.Pid)
	rec.StartTicks++
	sent, err := signalOurs(rec, 15)
	if sent || err != nil {
		t.Fatalf("sent=%v err=%v", sent, err)
	}
	time.Sleep(200 * time.Millisecond)
	if !runningNow(cmd.Process.Pid) {
		t.Fatal("the process was signalled")
	}
}

// shortDir makes a temporary directory with a short path: the launcher refuses state
// directories whose paths are too long for k3s's unix sockets, and t.TempDir names are long.
func shortDir(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("", "hb")
	if err != nil {
		t.Fatal(err)
	}
	if len(d) > 60 { // a long $TMPDIR would defeat the purpose
		os.RemoveAll(d)
		if d, err = os.MkdirTemp("/tmp", "hb"); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return d
}
