package launcher

// The review round of PR 361: an upgrade keeps the keeper, the keeper's health fails closed and
// counts only the keeper's own answer, up --relay-keeper refuses without pidfds, and restore takes
// the keeper's lock before anything changes.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// An upgrade of a keeper host starts the new host with the keeper (blocking item 1 of the
// review): the launch spec replaceHost passes the new host's start carries relayKeeper, and a host
// without the keeper is the control. This shows replaceHost's part only: the start's own record
// (startAndWait, which needs a real k3s) is pinned by the real-k3s CLI test, which upgrades a keeper
// host and reads relay_keeper from state.json; with startAndWait's assignment removed it fails.
func TestAnUpgradeKeepsTheRelayKeeper(t *testing.T) {
	for _, keeper := range []bool{true, false} {
		t.Run(map[bool]string{true: "a keeper host", false: "an alpha host"}[keeper], func(t *testing.T) {
			l := storeLayout(t)
			putStore(t, l, "v1", "tok", "")
			if err := l.saveStoreVersion("v1.35.9+k3s1", "up"); err != nil {
				t.Fatal(err)
			}
			ports, err := PortsFromBase(30020)
			if err != nil {
				t.Fatal(err)
			}
			st := &State{Version: "v1.35.9+k3s1", Ports: ports, Front: Front{Mode: string(agentapi.DialHomeHostDialed)},
				Controllers: &ControllerConfig{Binary: "/opt/example/hpcburst-controller"}, RelayKeeper: keeper}
			ops := &fakeHostOps{t: t, l: l}
			if _, _, _, _, err := replaceHost(context.Background(), l, st, Binary{Version: mustVersion(t, "v1.36.5+k3s1")}, ops, time.Second, time.Second, nil); err != nil {
				t.Fatal(err)
			}
			if ops.spec.relayKeeper != keeper {
				t.Fatalf("the new host's launch spec carries relayKeeper %v, want %v", ops.spec.relayKeeper, keeper)
			}
		})
	}
}

// The keeper's health fails closed when its descriptors cannot be read (blocking item 2): unknown
// is not "no port". The positive control is the same keeper read normally.
func TestTheKeepersHealthFailsClosedOnUnreadableDescriptors(t *testing.T) {
	f := keeperFixture(t)
	f.supervise(SuperviseOptions{})
	k := f.keeperUp()
	if eh, problems := f.keeperHealth(k); len(problems) != 0 || eh.PingOK == nil || !*eh.PingOK {
		t.Fatalf("the keeper read normally: %+v, problems %v; want healthy", eh, problems)
	}
	read := keeperFdInodes
	keeperFdInodes = func(int) (map[string]bool, error) { return nil, os.ErrPermission }
	t.Cleanup(func() { keeperFdInodes = read })
	sup := f.supervisorHandle()
	eh, problems := checkEntry(sup, sup.rec.PID, k)
	if eh.PingOK == nil || *eh.PingOK || !strings.Contains(strings.Join(problems, "; "), "cannot read the descriptors") {
		t.Errorf("the keeper with unreadable descriptors: %+v, problems %v; want unhealthy, saying its sockets are unknown", eh, problems)
	}
}

// keeperFdInodes itself returns the failure to read a descriptor directory, never an empty set:
// here /proc is a tree with no fd directory for the process.
func TestKeeperFdInodesReturnsAnUnreadableDirectory(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "4242"), 0o700); err != nil {
		t.Fatal(err)
	}
	old := procRoot
	procRoot = root
	t.Cleanup(func() { procRoot = old })
	if inodes, err := keeperFdInodes(4242); err == nil {
		t.Errorf("a missing fd directory: %v, nil; want an error", inodes)
	}
}

// Only the keeper's own answer counts (the ping nit, made strict by the delta review): the real
// keeper runs and listens; its socket's path is unlinked, and an impostor, this test, binds a new
// listener there and answers pings. /proc/net/unix still shows the keeper's listener at that path,
// so only the connection's peer tells them apart: the check refuses, naming the impostor's PID. The
// keeper answering on its own socket, before the swap, is the positive control.
func TestTheKeepersPingMustBeTheKeepersOwn(t *testing.T) {
	f := keeperFixture(t)
	f.supervise(SuperviseOptions{})
	k := f.keeperUp()
	if eh, problems := f.keeperHealth(k); len(problems) != 0 || eh.PingOK == nil || !*eh.PingOK {
		t.Fatalf("the keeper on its own socket: %+v, problems %v; want healthy", eh, problems)
	}
	sock := f.l.host(f.ports).RelayKeeperSocket()
	if err := os.Remove(sock); err != nil {
		t.Fatal(err)
	}
	ln, err := wire.Listen(sock)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() {
		served <- wire.Serve(ctx, ln, func(context.Context, wire.Envelope) (wire.Kind, any) {
			return wire.KindPong, wire.Pong{KeeperEpoch: firstKeeperE}
		}, slog.New(slog.DiscardHandler))
	}()
	t.Cleanup(func() { cancel(); <-served })
	if e, peer, err := pingKeeper(sock, keeperPingTimeout); err != nil || e != firstKeeperE || peer != os.Getpid() {
		t.Fatalf("the impostor's answer: E %d, peer %d, %v; want it to answer, as this process", e, peer, err)
	}
	sup := f.supervisorHandle()
	eh, problems := checkEntry(sup, sup.rec.PID, k)
	if eh.PingOK == nil || *eh.PingOK || !strings.Contains(strings.Join(problems, "; "), "answered by pid "+strconv.Itoa(os.Getpid())) {
		t.Errorf("a keeper whose socket path an impostor took: %+v, problems %v; want unhealthy, naming the impostor's PID", eh, problems)
	}
}

// keeperFdInodes reads the snapshot again once when a descriptor closes in the middle of it, and
// refuses when that happens again: a closing descriptor could hide a listener duplicated into a
// new one (the delta review of PR 361). It runs on a plain bystander, and on one that also holds a
// descriptor it inherited, as a CI runner leaks some into the test process and so into its
// children (descriptors 142 and 145 on GitHub's runners, PR 361's CI).
func TestKeeperFdInodesRetriesAClosedDescriptorOnce(t *testing.T) {
	for _, inherited := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "with an inherited descriptor"}[inherited], func(t *testing.T) {
			// Another process's descriptors: reading this process's own would meet the
			// directory descriptor ReadDir itself opened, closed by the time its link is read.
			// The bystander is read only once it has settled: a freshly exec'd sleep runs its
			// locale setup first, which opens and closes a descriptor for each locale file, and
			// a snapshot taken then really does see a descriptor close (CI, run 43082006242).
			other := startSleeper(t, inherited)
			settled(t, other)
			read := fdReadlink
			t.Cleanup(func() { fdReadlink = read })
			calls := 0
			fdReadlink = func(p string) (string, error) {
				calls++
				if calls == 1 {
					return "", fs.ErrNotExist
				}
				return read(p)
			}
			if _, err := keeperFdInodes(other); err != nil {
				t.Errorf("a descriptor that closed once: %v; want the snapshot read again, and passing", err)
			}
			fdReadlink = func(string) (string, error) { return "", fs.ErrNotExist }
			if _, err := keeperFdInodes(other); err == nil || !strings.Contains(err.Error(), "changed while they were read, twice") {
				t.Errorf("descriptors closing twice: %v; want refused", err)
			}
		})
	}
}

// settled holds for a bystander that keeps a descriptor it inherited: it settles on stability,
// never on a fixed set of descriptors, since the set a child gets is not the test's to choose.
func TestSettledAcceptsAnInheritedDescriptor(t *testing.T) {
	pid := startSleeper(t, true)
	snap := settled(t, pid)
	if _, ok := snap["3"]; !ok {
		t.Errorf("the settled bystander's descriptors %v lack the inherited descriptor 3", snap)
	}
}

// up --relay-keeper refuses without pidfds, before anything starts (the spawn nit): the keeper
// never starts where a start could get no pidfd.
func TestUpRefusesTheKeeperWithoutPidfds(t *testing.T) {
	probe := pidfdProbe
	pidfdProbe = func() error { return unix.ENOSYS }
	t.Cleanup(func() { pidfdProbe = probe })
	l, err := newLayout(filepath.Join(shortDir(t), "st"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = Up(context.Background(), UpOptions{StateDir: l.dir, PortBase: 30020, RelayKeeper: true, ControllerBin: fakeControllerBin(t),
		Front: Front{Mode: string(agentapi.DialHomeHostDialed)}, Log: io.Discard})
	if err == nil || !strings.Contains(err.Error(), "needs pidfds") {
		t.Fatalf("up --relay-keeper without pidfds: %v; want refused", err)
	}
	if _, err := os.Stat(l.stateFile()); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a record after the refusal: %v", err)
	}
}

// restore takes the keeper's lock before anything changes (the restore nit): with a stale launcher
// record (its process has exited) and a keeper holding its lock, the restore is refused and the
// record, the store and the keeper's fence are all as they were.
func TestRestoreTakesTheKeepersLockFirst(t *testing.T) {
	l := storeLayout(t)
	putStore(t, l, "v1", "tok1", "")
	backup, err := backupStore(l, "v1.35.9+k3s1")
	if err != nil {
		t.Fatal(err)
	}
	putStore(t, l, "v2", "tok2", "")
	if err := l.saveStoreVersion("v1.36.5+k3s1", "upgrade"); err != nil {
		t.Fatal(err)
	}
	// A stale record: a process that has exited.
	gone := exec.Command("true")
	if err := gone.Run(); err != nil {
		t.Fatal(err)
	}
	rec := ProcRec{Name: "k3s", PID: gone.Process.Pid, StartTicks: 1}
	if err := l.saveState(&State{Version: "v1.36.5+k3s1", Procs: []ProcRec{rec}}); err != nil {
		t.Fatal(err)
	}
	path := relaykeeper.StatePath(l.dir)
	ks, err := relaykeeper.OpenState(path)
	if err != nil {
		t.Fatal(err)
	}
	ks.Providers = map[string]relaykeeper.ProviderFence{"example-a": {HighestEpoch: 7}}
	if err := relaykeeper.SaveState(path, ks); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(relaykeeper.LockPath(l.dir), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := unix.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	if _, err := Restore(RestoreOptions{StateDir: l.dir, Backup: backup}); err == nil || !strings.Contains(err.Error(), "nothing was restored or cleared") {
		t.Fatalf("restore with the keeper's lock held: %v; want refused before anything changes", err)
	}
	if _, err := os.Stat(l.stateFile()); err != nil {
		t.Errorf("the stale launcher record after the refused restore: %v, want it kept", err)
	}
	if fileText(t, l.storeDB()) != "v2" {
		t.Error("the store changed though the restore was refused")
	}
	if got, _ := relaykeeper.LoadState(path); got == nil || got.Providers["example-a"].HighestEpoch != 7 {
		t.Errorf("the keeper's fence after the refusal: %+v, want it kept", got)
	}
}

// startSleeper starts "sleep 300" as a bystander. With inherited set, it also hands the sleep an
// open descriptor 3 (exec.Cmd.ExtraFiles, not close-on-exec in the child), as a CI runner's leaked
// descriptors reach every child of the test process.
func startSleeper(t *testing.T, inherited bool) int {
	t.Helper()
	if !inherited {
		return startBystander(t, "sleep", "300").Process.Pid
	}
	f, err := os.CreateTemp(t.TempDir(), "inherited")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	cmd := exec.Command("sleep", "300")
	cmd.ExtraFiles = []*os.File{f}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill() // this test's own child, through its handle
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	})
	return cmd.Process.Pid
}

// settled waits, within 10 s, until pid is asleep (state S) and three snapshots of its
// descriptors, 20 ms apart, are identical: the same descriptors, linked to the same targets. That
// is a settled sleep, whatever descriptors it inherited (a CI runner leaks some into its tests).
// It returns the settled snapshot.
func settled(t *testing.T, pid int) map[string]string {
	t.Helper()
	snapshot := func() (map[string]string, string, bool) {
		b, err := os.ReadFile(procPath(pid, "stat"))
		if err != nil {
			return nil, err.Error(), false
		}
		st, err := parseStat(string(b))
		if err != nil {
			return nil, err.Error(), false
		}
		fds, err := os.ReadDir(procPath(pid, "fd"))
		if err != nil {
			return nil, err.Error(), false
		}
		snap := map[string]string{}
		for _, fd := range fds {
			l, err := os.Readlink(procPath(pid, "fd/"+fd.Name()))
			if err != nil {
				return nil, fmt.Sprintf("fd %s: %v", fd.Name(), err), false
			}
			snap[fd.Name()] = l
		}
		return snap, fmt.Sprintf("state %c, descriptors %v", st.State, snap), st.State == 'S'
	}
	same := func(a, b map[string]string) bool {
		if len(a) != len(b) {
			return false
		}
		for k, v := range a {
			if b[k] != v {
				return false
			}
		}
		return true
	}
	deadline := time.Now().Add(10 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		first, d, ok := snapshot()
		last = d
		stable := ok
		for i := 0; stable && i < 2; i++ {
			time.Sleep(20 * time.Millisecond)
			next, d, ok := snapshot()
			last = d
			stable = ok && same(first, next)
		}
		if stable {
			return first
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("the bystander pid %d did not settle within 10 s: %s", pid, last)
	return nil
}
