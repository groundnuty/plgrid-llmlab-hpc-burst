package launcher

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// waitExecOfSleep waits until the shell with this PID has exec'd sleep, which is when its ignored
// SIGTERM is final: before that, bash could still handle it.
func waitExecOfSleep(t *testing.T, pid int) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if exe, err := os.Readlink("/proc/" + strconv.Itoa(pid) + "/exe"); err == nil && strings.HasSuffix(exe, "/sleep") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("bash never exec'd sleep")
		}
	}
}

// The tests here use ordinary processes as stand-ins for frps, so they run wherever there is a
// shell and sleep(1). The same rules are shown on a real frps in frps_real_test.go.

// TestStopFrpsNeverSignalsAReusedPID gives StopFrps records that name a live process, with the
// start time or the boot ID of a process that once had the PID. The live process (a sleep that
// knows nothing of the launcher) must be neither signalled nor stopped, and the stop must fail,
// for each kind of mismatch.
func TestStopFrpsNeverSignalsAReusedPID(t *testing.T) {
	cmd := startBystander(t, "sleep", "300")
	pid := cmd.Process.Pid
	own, err := newRec(FrpsProcName, pid)
	if err != nil {
		t.Fatal(err)
	}

	rec := func(pid int, start uint64, boot string) ProcRec {
		return ProcRec{Name: FrpsProcName, PID: pid, StartTicks: start, BootID: boot}
	}
	for _, c := range []struct {
		name    string
		rec     ProcRec
		foreign bool
	}{
		{"another start time (+1 tick)", rec(pid, own.StartTicks+1, own.BootID), true},
		{"another start time (-1 tick)", rec(pid, own.StartTicks-1, own.BootID), true},
		{"recorded in another boot", rec(pid, own.StartTicks, "00000000-0000-0000-0000-000000000000"), true},
		{"no boot ID: identity unknown", rec(pid, own.StartTicks, ""), false},
		{"no start time: identity unknown", rec(pid, 0, own.BootID), false},
		{"pid 1", rec(1, 1, own.BootID), true},
	} {
		t.Run(c.name, func(t *testing.T) {
			rep, err := StopFrps(context.Background(), c.rec, 200*time.Millisecond)
			if err == nil {
				t.Fatalf("StopFrps = %+v, want a refusal", rep)
			}
			if got := errors.Is(err, ErrFrpsForeign); got != c.foreign {
				t.Errorf("errors.Is(err, ErrFrpsForeign) = %v, want %v (%v)", got, c.foreign, err)
			}
			if rep.Signals != "" {
				t.Errorf("signals sent: %q", rep.Signals)
			}
			if rep.Detail == "" {
				t.Error("the report does not say why")
			}
			time.Sleep(100 * time.Millisecond)
			if !runningNow(pid) {
				t.Fatal("the process holding the PID was signalled")
			}
			if st, err := readStat(pid); err != nil || st.StartTicks != own.StartTicks {
				t.Fatalf("the process changed: %+v %v", st, err)
			}
		})
	}

	// With the true record the same call does stop it: the refusals above are about identity, not
	// about this process being unstoppable.
	rep, err := StopFrps(context.Background(), own, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Signals != "TERM" || rep.Before != "ours" {
		t.Errorf("report = %+v", rep)
	}
	if !waitNotRunning(pid, 5*time.Second) {
		t.Fatal("the process is still running after SIGTERM")
	}
	// A process that has gone is a stop that succeeded, with nothing signalled.
	rep, err = StopFrps(context.Background(), own, time.Second)
	if err != nil || rep.Before != "gone" || rep.Signals != "" {
		t.Errorf("after the exit: %+v %v", rep, err)
	}
}

// TestStopFrpsEscalatesToKill: a process that ignores SIGTERM gets SIGKILL once the grace period
// is over, and not before. SIGTERM ignored is inherited across exec, so sleep ignores it.
func TestStopFrpsEscalatesToKill(t *testing.T) {
	cmd := startBystander(t, "bash", "-c", `trap "" TERM; exec sleep 300`)
	pid := cmd.Process.Pid
	waitExecOfSleep(t, pid)
	rec, err := newRec(FrpsProcName, pid)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	rep, err := StopFrps(context.Background(), rec, 400*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Signals != "TERM+KILL" {
		t.Errorf("signals = %q, want TERM+KILL", rep.Signals)
	}
	if d := time.Since(start); d < 400*time.Millisecond {
		t.Errorf("SIGKILL came after %s, before the grace period was over", d)
	}
	if !waitNotRunning(pid, 5*time.Second) {
		t.Fatal("still running after SIGKILL")
	}
}

// TestStopFrpsCancelledContextCutsTheGraceShort: ctx ends the wait for SIGTERM to work, never the
// wait after SIGKILL.
func TestStopFrpsCancelledContextCutsTheGraceShort(t *testing.T) {
	cmd := startBystander(t, "bash", "-c", `trap "" TERM; exec sleep 300`)
	pid := cmd.Process.Pid
	waitExecOfSleep(t, pid)
	rec, err := newRec(FrpsProcName, pid)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	rep, err := StopFrps(ctx, rec, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 5*time.Second || rep.Signals != "TERM+KILL" {
		t.Errorf("took %s, signals %q: the cancelled context did not cut the hour-long grace short", d, rep.Signals)
	}
	if !waitNotRunning(pid, 5*time.Second) {
		t.Fatal("still running")
	}
}
