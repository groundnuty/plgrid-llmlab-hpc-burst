package launcher

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestIdentifyWithPidfds runs identify on a fake /proc tree, with fake pidfds saying which PIDs
// exist and which have exited. Death comes only from the pidfd: the cases where the leader reads
// Z, or the task directory is empty, or the stat file cannot be read, while the pidfd says the
// process lives, are the ones the task-scan rule got wrong.
func TestIdentifyWithPidfds(t *testing.T) {
	root := fakeProc(t, "boot-A")
	f := newFakePidfd(500, 501, 502, 503, 505, 506)
	f.setAlive(501, false) // exited, not yet reaped
	useFakePidfds(t, f)
	writeFakeProc(t, root, 500, "S", 1, 1000)
	writeFakeProc(t, root, 501, "Z", 1, 1000)
	writeFakeProc(t, root, 502, "S", 1, 2000) // the PID of a record below, another start time
	// The leader reads Z while another thread runs: execve from a non-leader thread.
	writeFakeProc(t, root, 503, "Z", 1, 1000)
	writeFakeThread(t, root, 503, 503, "Z")
	writeFakeThread(t, root, 503, 777, "S")
	// The leader reads Z and the task directory lists nothing, as it can in the middle of that
	// execve; the pidfd says the process lives. The old rule called this gone.
	writeFakeProc(t, root, 505, "Z", 1, 1000)
	if err := os.MkdirAll(filepath.Join(root, "505", "task"), 0o755); err != nil {
		t.Fatal(err)
	}
	// 506: alive by its pidfd, but /proc/506/stat cannot be read.
	if err := os.MkdirAll(filepath.Join(root, "506"), 0o755); err != nil {
		t.Fatal(err)
	}
	rec := func(pid int, start uint64, boot string) ProcRec {
		return ProcRec{Name: "k3s", PID: pid, StartTicks: start, BootID: boot}
	}
	cases := []struct {
		name string
		rec  ProcRec
		want Verdict
	}{
		{"alive and identical", rec(500, 1000, "boot-A"), Ours},
		{"no such process", rec(999, 1000, "boot-A"), Gone},
		{"exited (zombie) by its pidfd", rec(501, 1000, "boot-A"), Gone},
		{"zombie leader, live thread: execve in progress", rec(503, 1000, "boot-A"), Ours},
		{"zombie leader, empty task directory, pidfd alive", rec(505, 1000, "boot-A"), Ours},
		{"stat unreadable while the pidfd says alive", rec(506, 1000, "boot-A"), Unknown},
		{"pid reused: another start time", rec(502, 1000, "boot-A"), Foreign},
		{"recorded before a reboot", rec(500, 1000, "boot-B"), Foreign},
		{"pid 1 is never ours", rec(1, 1, "boot-A"), Foreign},
		{"pid 0 is never ours", rec(0, 1, "boot-A"), Foreign},
		{"record without a boot ID", rec(500, 1000, ""), Unknown},
		{"record without a start time", rec(500, 0, "boot-A"), Unknown},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, detail := identify(c.rec)
			if got != c.want {
				t.Fatalf("identify = %v (%s), want %v", got, detail, c.want)
			}
			if got != Ours && detail == "" {
				t.Error("a verdict other than ours must say why")
			}
		})
	}

	t.Run("no pidfd can be taken", func(t *testing.T) {
		f.openErr = syscall.ENOSYS
		defer func() { f.openErr = nil }()
		if got, d := identify(rec(500, 1000, "boot-A")); got != Unknown {
			t.Fatalf("identify = %v (%s), want unknown", got, d)
		}
	})
	t.Run("the boot ID cannot be read", func(t *testing.T) {
		p := filepath.Join(root, "sys/kernel/random/boot_id")
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
		defer os.WriteFile(p, []byte("boot-A\n"), 0o644)
		if got, d := identify(rec(500, 1000, "boot-A")); got != Unknown {
			t.Fatalf("identify = %v (%s), want unknown", got, d)
		}
		if _, err := newRec("k3s", 500); err == nil {
			t.Error("a process was recorded without a boot ID")
		}
	})
}

// TestSignalNeedsAPidfd: with no pidfd to be had, nothing is signalled, and down fails with the
// record kept. A fallback to signalling by PID number (what os.FindProcess does when pidfd_open
// fails) would kill the process here.
func TestSignalNeedsAPidfd(t *testing.T) {
	cmd := startBystander(t, "sleep", "300")
	rec, err := newRec("k3s", cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	l := downFixture(t, rec)
	f := newFakePidfd()
	f.openErr = syscall.ENOSYS
	useFakePidfds(t, f)

	sent, err := signalOurs(rec, syscall.SIGTERM)
	if sent || err == nil {
		t.Fatalf("signalOurs without a pidfd: sent=%v err=%v", sent, err)
	}
	rep, err := Down(context.Background(), DownOptions{StateDir: l.dir, Grace: time.Second})
	if err == nil || !strings.Contains(err.Error(), "record is kept") {
		t.Fatalf("down without a pidfd: %v", err)
	}
	if rep == nil || rep.Procs[0].Before != "unknown" || rep.Procs[0].Signals != "" {
		t.Errorf("report: %+v", rep)
	}
	if _, err := os.Stat(l.stateFile()); err != nil {
		t.Errorf("the record was not kept: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if !runningNow(cmd.Process.Pid) {
		t.Fatal("the process was signalled although no pidfd could be taken")
	}
}

// TestOpenTreeAdoptsOnlyVerifiedChildren: a descendant found in the /proc scan is adopted only if a
// pidfd taken afterwards shows the same start time and the same parent. Here PIDs are "reused"
// between the scan and the adoption, by rewriting their stat when their pidfd is taken.
func TestOpenTreeAdoptsOnlyVerifiedChildren(t *testing.T) {
	root := fakeProc(t, "boot-A")
	f := newFakePidfd(100, 101, 102, 103, 104, 200)
	useFakePidfds(t, f)
	writeFakeProc(t, root, 100, "S", 1, 1000)   // the recorded root
	writeFakeProc(t, root, 101, "S", 100, 1100) // child: stays as scanned
	writeFakeProc(t, root, 102, "S", 101, 1200) // grandchild: stays as scanned
	writeFakeProc(t, root, 103, "S", 100, 1300) // child whose PID is reused after the scan
	writeFakeProc(t, root, 104, "S", 103, 1400) // its child: its parent is not adopted
	writeFakeProc(t, root, 200, "S", 1, 1500)   // unrelated
	f.onOpen = func(pid int) {
		if pid == 103 {
			writeFakeProc(t, root, 103, "S", 1, 9999) // another process now, with another parent
		}
	}
	h, v, d := openRec(ProcRec{Name: "k3s", PID: 100, StartTicks: 1000, BootID: "boot-A"})
	if v != Ours {
		t.Fatalf("root: %v %s", v, d)
	}
	tree := openTree(h)
	defer closeAll(tree)
	var got []int
	for _, x := range tree {
		got = append(got, x.rec.PID)
	}
	if fmt.Sprint(got) != "[100 101 102]" {
		t.Fatalf("adopted %v, want [100 101 102]", got)
	}

	// A child whose parent exits between the scan and the adoption is not adopted either.
	writeFakeProc(t, root, 103, "S", 100, 1300)
	f.onOpen = func(pid int) {
		if pid == 101 {
			f.setAlive(100, false)
		}
	}
	tree2 := openTree(h)
	defer closeAll(tree2)
	if len(tree2) != 1 {
		t.Errorf("adopted %d processes under a parent that exited meanwhile, want only the root", len(tree2)-1)
	}
}

// TestCheckExe: k3s must run a binary from its own data directory. Expected values are written by
// hand from the layout HBEP-11 and the k3s source give: <data-dir>/data/<asset hash>/bin.
func TestCheckExe(t *testing.T) {
	h := newFakeHost(t)
	ph := h.handle()
	own := filepath.Join(h.assetRoot(), "abc", "bin", "k3s-server")
	if exe, err := checkExe(h.l, ph); err != nil || exe != own {
		t.Fatalf("own binary: %q, %v", exe, err)
	}
	for _, bad := range []string{
		"/var/lib/rancher/k3s/data/abc/bin/k3s-server",
		own + " (deleted)",
		h.assetRoot() + "-evil/abc/bin/k3s-server", // a sibling with the same prefix
		"/usr/local/bin/k3s",
	} {
		h.setExe(bad)
		if _, err := checkExe(h.l, ph); err == nil || !strings.Contains(err.Error(), bad) {
			t.Errorf("exe %s: %v; want a refusal naming the path", bad, err)
		}
	}
	// A data directory that is a symbolic link (the store-full test makes one): the kernel reports
	// the resolved path, and that is accepted.
	elsewhere := filepath.Join(shortDir(t), "vol")
	if err := os.MkdirAll(filepath.Join(elsewhere, "data", "abc", "bin"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(h.l.dataDir()); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(elsewhere, h.l.dataDir()); err != nil {
		t.Fatal(err)
	}
	resolved, _ := filepath.EvalSymlinks(elsewhere)
	h.setExe(filepath.Join(resolved, "data", "abc", "bin", "k3s-server"))
	if _, err := checkExe(h.l, ph); err != nil {
		t.Errorf("a data directory behind a symbolic link: %v", err)
	}
}

// TestStatusOnAFakeHost: the control is healthy; a k3s that runs the system k3s's unpacked binary
// is not (it is named); a controller-manager port taken over while the API was being asked is
// not; and a process that exits after the listener check is caught by the final identity check,
// with --no-api too.
func TestStatusOnAFakeHost(t *testing.T) {
	status := func(t *testing.T, h *fakeHost, noAPI bool) *StatusReport {
		t.Helper()
		rep, err := Status(context.Background(), StatusOptions{StateDir: h.l.dir, NoAPI: noAPI, NoGCProbe: true})
		if err != nil {
			t.Fatal(err)
		}
		return rep
	}
	for _, noAPI := range []bool{true, false} {
		t.Run("control, noAPI="+strconv.FormatBool(noAPI), func(t *testing.T) {
			h := newFakeHost(t)
			rep := status(t, h, noAPI)
			if rep.Health != Healthy {
				t.Fatalf("%s: %v", rep.Health, rep.Reasons)
			}
			if !strings.HasPrefix(rep.Executable, h.assetRoot()+"/") {
				t.Errorf("executable %q", rep.Executable)
			}
			if !noAPI && (h.kcmHits == 0 || rep.Writes == nil || !*rep.Writes) {
				t.Errorf("the API was not asked: kcm hits %d, writes %v", h.kcmHits, rep.Writes)
			}
		})
		t.Run("system k3s binary, noAPI="+strconv.FormatBool(noAPI), func(t *testing.T) {
			h := newFakeHost(t)
			const rancher = "/var/lib/rancher/k3s/data/abc/bin/k3s-server"
			h.setExe(rancher)
			rep := status(t, h, noAPI)
			if rep.Health != Unhealthy || !strings.Contains(strings.Join(rep.Reasons, ";"), rancher) {
				t.Fatalf("%s: %v; want unhealthy, naming %s", rep.Health, rep.Reasons, rancher)
			}
			if rep.Readyz != nil {
				t.Error("the API was asked although k3s runs another k3s's binary")
			}
		})
		t.Run("exits after the listener check, noAPI="+strconv.FormatBool(noAPI), func(t *testing.T) {
			h := newFakeHost(t)
			statusStageHook = func(stage string) { h.f.setAlive(h.pid, false) }
			defer func() { statusStageHook = nil }()
			rep := status(t, h, noAPI)
			if rep.Health != Unhealthy || !strings.Contains(strings.Join(rep.Reasons, ";"), "changed while it was being checked") {
				t.Fatalf("%s: %v", rep.Health, rep.Reasons)
			}
		})
	}
	t.Run("controller manager port taken over while it answered", func(t *testing.T) {
		h := newFakeHost(t)
		writeFakeProc(t, h.root, 5000, "S", 1, 3000)
		h.onKCM = func() { h.giveSocket(5000, h.st.Ports.KCM) }
		rep := status(t, h, false)
		if rep.KCM == nil || !*rep.KCM {
			t.Fatalf("the decoy controller manager did not answer: %+v", rep)
		}
		if rep.Health != Unhealthy || !strings.Contains(strings.Join(rep.Reasons, ";"), "after the API checks") {
			t.Fatalf("%s: %v; want unhealthy after the API checks", rep.Health, rep.Reasons)
		}
	})
}

// TestWaitReadyChecksTheExecutable: once the probes answer, waitReady confirms the host, including
// where k3s runs from. Ready with its own binary; refused, naming the path, with another k3s's.
func TestWaitReadyChecksTheExecutable(t *testing.T) {
	h := newFakeHost(t)
	if _, _, err := waitReady(context.Background(), h.l, h.st, h.handle(), 10*time.Second); err != nil {
		t.Fatalf("own binary: %v", err)
	}
	const rancher = "/var/lib/rancher/k3s/data/abc/bin/k3s-server"
	h.setExe(rancher)
	_, _, err := waitReady(context.Background(), h.l, h.st, h.handle(), 10*time.Second)
	if err == nil || !strings.Contains(err.Error(), rancher) {
		t.Fatalf("system k3s binary: %v; want a refusal naming %s", err, rancher)
	}
}

// TestStopWaitsOutTheKillWhenInterrupted: a process that ignores SIGTERM and takes 400 ms to die
// after SIGKILL, stopped with a context that is already cancelled (Ctrl-C). The grace period is cut
// short, but the wait after SIGKILL is not: down returns only once the pidfd says the process
// exited, and reports no error.
func TestStopWaitsOutTheKillWhenInterrupted(t *testing.T) {
	h := newFakeHost(t)
	var killedAt time.Time
	h.f.onSignal = func(pid int, sig syscall.Signal) {
		if sig == syscall.SIGKILL {
			killedAt = time.Now()
			go func() {
				time.Sleep(400 * time.Millisecond)
				// Its sockets go with it, as the kernel's do.
				_ = os.WriteFile(filepath.Join(h.root, "net", "tcp"), []byte("  sl  local_address rem_address   st\n"), 0o644)
				h.f.setAlive(pid, false)
			}()
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rep, err := stopRecorded(ctx, h.st, 30*time.Second, nil)
	returned := time.Now()
	if err != nil {
		t.Fatalf("stop: %v", err)
	}
	if got := strings.Join(h.f.signals(), ","); got != "4242:TERM,4242:KILL" {
		t.Fatalf("signals %s", got)
	}
	if returned.Sub(killedAt) < 350*time.Millisecond {
		t.Errorf("stop returned %s after SIGKILL, before the process exited", returned.Sub(killedAt))
	}
	if rep.Procs[0].Signals != "TERM+KILL" {
		t.Errorf("report: %+v", rep.Procs[0])
	}
}
