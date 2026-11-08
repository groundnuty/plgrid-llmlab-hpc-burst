package launcher

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// Issue #264: down reported k3s's own ports as listeners "not as a launched process". The block of
// these tests is 29740-29759 (IMPL-COMMON's table), based at 29740: k3s holds base, base+1 and
// base+2, a supervised process base+3, and base+5 and base+6 stand for an operator's listener.
const downListenBase = 29740

// The listener helper: the test binary, started with listenEnv=ADDR[,ADDR...], listens on each
// address and then waits to be killed. With listenChildEnv=ADDR[,ADDR...] too, it starts itself
// once more as its child, which holds those addresses (and dies with it, by Pdeathsig). Each
// process appends "PID PPID" to the file named by listenReportEnv once its listeners are open, so
// the test waits on that file, never on a clock.
const (
	listenEnv       = "HPCBURST_TEST_LISTEN"
	listenChildEnv  = "HPCBURST_TEST_LISTEN_CHILD"
	listenReportEnv = "HPCBURST_TEST_LISTEN_REPORT"
)

func init() {
	if os.Getenv(listenEnv) != "" || os.Getenv(listenChildEnv) != "" {
		// The child's Pdeathsig fires when the thread that started it exits: keep that thread alive.
		runtime.LockOSThread()
	}
}

func runListenHelper() {
	var open []net.Listener
	for _, a := range strings.Split(os.Getenv(listenEnv), ",") {
		if a == "" {
			continue
		}
		l, err := net.Listen("tcp", a)
		if err != nil {
			fmt.Fprintln(os.Stderr, "listen helper:", err)
			os.Exit(3)
		}
		open = append(open, l)
	}
	if child := os.Getenv(listenChildEnv); child != "" {
		cmd := exec.Command("/proc/self/exe")
		cmd.Env = []string{listenEnv + "=" + child, listenReportEnv + "=" + os.Getenv(listenReportEnv)}
		cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
		if err := cmd.Start(); err != nil {
			fmt.Fprintln(os.Stderr, "listen helper:", err)
			os.Exit(3)
		}
	}
	f, err := os.OpenFile(os.Getenv(listenReportEnv), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		os.Exit(4)
	}
	fmt.Fprintf(f, "%d %d\n", os.Getpid(), os.Getppid())
	f.Close()
	time.Sleep(time.Hour)
	runtime.KeepAlive(open) // a listener that is collected closes its socket
}

// listenHelper is a started helper: the process, and the child it started if it was asked to.
type listenHelper struct {
	pid, child int
}

// startListenHelper starts a helper that listens on own and whose child listens on viaChild
// (both lists of "127.0.0.1:PORT"), and returns once every listener is open. A process that never
// reports fails the test after a generous deadline, which a loaded machine does not reach.
func startListenHelper(t *testing.T, own, viaChild []string) listenHelper {
	t.Helper()
	report := filepath.Join(shortDir(t), "report")
	cmd := exec.Command("/proc/self/exe")
	cmd.Env = []string{listenEnv + "=" + strings.Join(own, ","), listenReportEnv + "=" + report}
	if len(viaChild) > 0 {
		cmd.Env = append(cmd.Env, listenChildEnv+"="+strings.Join(viaChild, ","))
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
		}
	})
	want := 1
	if len(viaChild) > 0 {
		want = 2
	}
	h := listenHelper{pid: cmd.Process.Pid}
	for deadline := time.Now().Add(60 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		b, _ := os.ReadFile(report)
		lines := strings.Fields(string(b))
		if len(lines) >= 2*want {
			for i := 0; i+1 < len(lines); i += 2 {
				if pid, _ := strconv.Atoi(lines[i]); pid != h.pid {
					h.child = pid
				}
			}
			break
		}
		select {
		case <-done:
			t.Fatalf("the listener helper exited before it was listening (report %q)", b)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("the listener helper was not listening after 60 s (report %q)", b)
		}
	}
	if len(viaChild) > 0 {
		t.Cleanup(func() { _ = syscall.Kill(h.child, syscall.SIGKILL) })
		if h.child == 0 || ppidOf(t, h.child) != h.pid {
			t.Fatalf("the helper's child is pid %d, whose parent is not pid %d", h.child, h.pid)
		}
	}
	return h
}

func addrs(ports ...int) []string {
	out := make([]string, len(ports))
	for i, p := range ports {
		out[i] = "127.0.0.1:" + strconv.Itoa(p)
	}
	return out
}

// downBlock is a launched host's record at the test block: the same ports PortsFromBase gives.
func downBlock(t *testing.T) Ports {
	t.Helper()
	p, err := PortsFromBase(downListenBase)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// recorded records the helper as the launched process called name.
func recorded(t *testing.T, name string, pid int) ProcRec {
	t.Helper()
	rec, err := newRec(name, pid)
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

// lockedLog is a log writer that is safe to read while down writes.
type lockedLog struct {
	mu   sync.Mutex
	text strings.Builder
	on   func(line string) // called for every Write, after it is recorded
}

func (l *lockedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	l.text.Write(p)
	on := l.on
	l.mu.Unlock()
	if on != nil {
		on(string(p))
	}
	return len(p), nil
}

func (l *lockedLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.text.String()
}

func refused(addr string) bool {
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err == nil {
		c.Close()
		return false
	}
	return true
}

// TestDownCountsAListenerHeldThroughAChild: the launched process holds no socket itself; its
// child holds the listener on a recorded port. down finds the child as a descendant, counts its
// socket among the ones it waits for, and reports nothing foreign. Reading the count, not only the
// absence of a warning, is what shows the child's socket was in the set: by the time down looks, a
// dead child's socket may already be gone from the kernel's table either way.
func TestDownCountsAListenerHeldThroughAChild(t *testing.T) {
	ports := downBlock(t)
	h := startListenHelper(t, nil, addrs(ports.API))
	l := downFixtureAt(t, ports, recorded(t, "k3s", h.pid))
	log := &lockedLog{}
	rep, err := Down(context.Background(), DownOptions{StateDir: l.dir, Grace: 20 * time.Second, Log: log})
	if err != nil {
		t.Fatalf("down: %v", err)
	}
	if rep.GoneListeners != 1 {
		t.Errorf("listeners released = %d, want 1 (the child's)", rep.GoneListeners)
	}
	if len(rep.ForeignOnPorts) != 0 || strings.Contains(log.String(), "warning") {
		t.Errorf("foreign %v; log:\n%s", rep.ForeignOnPorts, log)
	}
	if !refused(addrs(ports.API)[0]) {
		t.Error("something still listens on the child's port")
	}
	if runningNow(h.pid) || runningNow(h.child) {
		t.Errorf("the process (running=%v) or its child (running=%v) was left", runningNow(h.pid), runningNow(h.child))
	}
}

// TestDownDoesNotCallTheNextGroupsListenersForeign is the real shape of issue #264. A launched
// host records a supervisor, stopped first with its child, and k3s, stopped after it. While the
// supervisor's group is being stopped, k3s is alive on its three ports, which are recorded ports
// and not held by the supervisor's tree. down has to wait for k3s's own stop before it judges
// anything foreign. The supervisor and k3s are stand-ins (the listener helper); the ports are k3s's
// (base, base+1, base+2) and the controller's (base+3, held through the supervisor's child).
func TestDownDoesNotCallTheNextGroupsListenersForeign(t *testing.T) {
	ports := downBlock(t)
	sup := startListenHelper(t, nil, addrs(ports.API+3))
	k3s := startListenHelper(t, addrs(ports.API, ports.HTTPS, ports.KCM), nil)
	l := downFixtureAt(t, ports, recorded(t, "k3s", k3s.pid), recorded(t, supervisorName, sup.pid))
	log := &lockedLog{}
	rep, err := Down(context.Background(), DownOptions{StateDir: l.dir, Grace: 20 * time.Second, Log: log})
	if err != nil {
		t.Fatalf("down: %v", err)
	}
	if len(rep.ForeignOnPorts) != 0 || strings.Contains(log.String(), "warning") {
		t.Errorf("foreign listeners %v reported for ports that were k3s's and the supervisor's own; log:\n%s", rep.ForeignOnPorts, log)
	}
	if rep.GoneListeners != 4 {
		t.Errorf("listeners released = %d, want 4 (k3s's three and the supervisor child's one)", rep.GoneListeners)
	}
	for _, a := range addrs(ports.API, ports.HTTPS, ports.KCM, ports.API+3) {
		if !refused(a) {
			t.Errorf("%s still listens after down", a)
		}
	}
	if runningNow(sup.pid) || runningNow(sup.child) || runningNow(k3s.pid) {
		t.Error("a launched process was left running")
	}
}

// setReleaseWait sets the wait for the whole test: short, so a foreign listener does not cost the
// full socketReleaseWait in every run, or long, so that the only way down can return early is a
// clean scan. The tests that depend on the wait read the value back.
func setReleaseWait(t *testing.T, d time.Duration) {
	t.Helper()
	old := socketReleaseWait
	socketReleaseWait = d
	t.Cleanup(func() { socketReleaseWait = old })
}

// TestDownReportsARealForeignListenerOnce: a listener of this test process, not of anything
// launched, sits on a recorded port the whole time, through the stop of both groups (a supervisor,
// then k3s). down reports it once, after the last group, and says so in its log; it leaves it
// listening, and gives it the release wait first (a listener that is gone by then is not foreign:
// the next test).
func TestDownReportsARealForeignListenerOnce(t *testing.T) {
	const wait = 1500 * time.Millisecond
	setReleaseWait(t, wait)
	ports := downBlock(t)
	foreign, err := net.Listen("tcp", addrs(ports.API + 5)[0])
	if err != nil {
		t.Fatal(err)
	}
	defer foreign.Close()
	sup := startListenHelper(t, nil, addrs(ports.API+3))
	k3s := startListenHelper(t, addrs(ports.API), nil)
	l := downFixtureAt(t, ports, recorded(t, "k3s", k3s.pid), recorded(t, supervisorName, sup.pid))
	log := &lockedLog{}
	start := time.Now()
	rep, err := Down(context.Background(), DownOptions{StateDir: l.dir, Grace: 20 * time.Second, Log: log})
	took := time.Since(start)
	if err != nil {
		t.Fatalf("down: %v", err)
	}
	want := addrs(ports.API + 5)
	if len(rep.ForeignOnPorts) != 1 || rep.ForeignOnPorts[0] != want[0] {
		t.Fatalf("foreign = %v, want exactly %v; log:\n%s", rep.ForeignOnPorts, want, log)
	}
	warning := "warning: " + want[0] + " listens on one of the recorded ports"
	text := log.String()
	if n := strings.Count(text, "warning:"); n != 1 || !strings.Contains(text, warning) {
		t.Errorf("want one warning, %q; log:\n%s", warning, text)
	}
	if strings.Index(text, warning) < strings.Index(text, "sent SIGTERM to k3s") {
		t.Errorf("the warning came before the last group was stopped; log:\n%s", text)
	}
	if took < wait {
		t.Errorf("down took %s: it reported the listener without waiting %s for it to be released", took, wait)
	}
	if refused(want[0]) {
		t.Error("down closed the foreign listener")
	}
	if !refused(addrs(ports.API)[0]) || !refused(addrs(ports.API + 3)[0]) {
		t.Error("a launched process's port still listens")
	}
}

// TestDownDoesNotReportAListenerThatGoesDuringTheWait: a listener that is not ours sits on a
// recorded port when down first looks, and is closed while down waits for it (a socket the kernel
// is still releasing, say). It is closed on the observable event, down's own log line that says
// it is waiting, not after a delay, so the test does not depend on how fast the machine is. It is
// not reported afterwards, and down stops polling at the first scan that finds the ports clean.
//
// That last point is judged on down's own return, with no clock deciding it: the release wait is so
// long that down cannot come back by waiting it out, so it returns within the test's deadline only
// if the clean scan ended the polling. The deadline is far above any machine's speed and far below
// the wait, and it matters only when the test fails.
func TestDownDoesNotReportAListenerThatGoesDuringTheWait(t *testing.T) {
	const wait, deadline = 10 * time.Minute, 2 * time.Minute
	setReleaseWait(t, wait)
	ports := downBlock(t)
	late, err := net.Listen("tcp", addrs(ports.API + 6)[0])
	if err != nil {
		t.Fatal(err)
	}
	defer late.Close()
	k3s := startListenHelper(t, addrs(ports.API), nil)
	l := downFixtureAt(t, ports, recorded(t, "k3s", k3s.pid))
	var once sync.Once
	closedAt := make(chan time.Time, 1)
	log := &lockedLog{on: func(line string) {
		if strings.Contains(line, "waiting up to") {
			once.Do(func() { late.Close(); closedAt <- time.Now() })
		}
	}}
	type outcome struct {
		rep *StopReport
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		rep, err := Down(context.Background(), DownOptions{StateDir: l.dir, Grace: 20 * time.Second, Log: log})
		done <- outcome{rep, err}
	}()
	var got outcome
	select {
	case got = <-done:
	case <-time.After(deadline):
		select {
		case <-closedAt:
			t.Fatalf("down was still polling %s after the listener it was waiting for had gone (release wait %s): it does not stop at the first clean scan; log:\n%s", deadline, wait, log)
		default:
			t.Fatalf("down never said it was waiting for the listener, so the test never closed it, and it did not return in %s; log:\n%s", deadline, log)
		}
	}
	if got.err != nil {
		t.Fatalf("down: %v", got.err)
	}
	select {
	case <-closedAt:
	default:
		t.Fatalf("down never said it was waiting for the listener, so the test never closed it; log:\n%s", log)
	}
	if len(got.rep.ForeignOnPorts) != 0 || strings.Contains(log.String(), "warning") {
		t.Errorf("a listener that went during the wait was reported: %v; log:\n%s", got.rep.ForeignOnPorts, log)
	}
}

// fakeRow is one LISTEN socket of a fake /proc/net/tcp.
type fakeRow struct {
	port  int
	inode string
}

// writeListenRows writes the fake kernel's table of listening sockets (IPv4 loopback).
func writeListenRows(t *testing.T, root string, rows []fakeRow) {
	t.Helper()
	tab := "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"
	for i, r := range rows {
		tab += fmt.Sprintf("   %d: 0100007F:%04X 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1001        0 %s 1 0000000000000000 100 0 0 10 0\n", i, r.port, r.inode)
	}
	if err := os.MkdirAll(filepath.Join(root, "net"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "net", "tcp"), []byte(tab), 0o644); err != nil {
		t.Fatal(err)
	}
}

const (
	lingerSupervisorPID = 4301
	lingerK3sPID        = 4302
)

// lingerHost is a launched host on a fake /proc tree, recorded as a supervisor (holding base+3) and
// k3s (holding base). SIGTERM ends a process and releases its socket, except for the process named
// keep: the fake kernel keeps that one's socket listening after the process is gone, which a real
// kernel does while another process, outside the tree, still holds the same socket.
type lingerHost struct {
	root string
	f    *fakePidfd
	st   *State
	rows map[int]fakeRow // pid -> the socket it holds, while the kernel still lists it
	t    *testing.T
}

func newLingerHost(t *testing.T, keep int) *lingerHost {
	t.Helper()
	root := fakeProc(t, "boot-A")
	ports := downBlock(t)
	h := &lingerHost{root: root, f: newFakePidfd(lingerSupervisorPID, lingerK3sPID), t: t,
		rows: map[int]fakeRow{lingerK3sPID: {ports.API, "9201"}, lingerSupervisorPID: {ports.API + 3, "9202"}}}
	useFakePidfds(t, h.f)
	var recs []ProcRec
	for _, p := range []struct {
		pid  int
		name string
	}{{lingerK3sPID, "k3s"}, {lingerSupervisorPID, supervisorName}} {
		writeFakeProc(t, root, p.pid, "S", 1, 1000)
		dir := filepath.Join(root, strconv.Itoa(p.pid), "fd")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("socket:["+h.rows[p.pid].inode+"]", filepath.Join(dir, "3")); err != nil {
			t.Fatal(err)
		}
		recs = append(recs, ProcRec{Name: p.name, PID: p.pid, StartTicks: 1000, BootID: "boot-A"})
	}
	h.st = &State{Version: "v1.36.5+k3s1", Ports: ports, Procs: recs, StartedAt: time.Now()}
	h.f.onSignal = func(pid int, sig syscall.Signal) {
		if sig != syscall.SIGTERM {
			return
		}
		h.f.setAlive(pid, false)
		if pid != keep {
			delete(h.rows, pid)
		}
		h.write()
	}
	h.write()
	return h
}

func (h *lingerHost) write() {
	var rows []fakeRow
	for _, r := range h.rows {
		rows = append(rows, r)
	}
	writeListenRows(h.t, h.root, rows)
}

// TestASocketOfOursThatStaysIsAHardErrorNotAForeignListener: a socket held by a launched process
// still listens after that process is gone and the release wait is over. That stays the hard
// error it was ("a socket they held still listens"), whichever group held it; it is never
// re-labelled a foreign listener, which down would only warn about. The control releases both
// sockets, so the errors come from the socket that stays and not from the fixture.
func TestASocketOfOursThatStaysIsAHardErrorNotAForeignListener(t *testing.T) {
	ports := downBlock(t)
	for _, c := range []struct {
		name string
		keep int // the pid whose socket stays; 0: none
		port int // that socket's port
	}{
		{"the supervisor group's socket stays", lingerSupervisorPID, ports.API + 3},
		{"the k3s group's socket stays", lingerK3sPID, ports.API},
		{"control: both are released", 0, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			setReleaseWait(t, 300*time.Millisecond)
			h := newLingerHost(t, c.keep)
			log := &lockedLog{}
			rep, err := stopRecorded(context.Background(), h.st, 5*time.Second, log)
			if len(rep.ForeignOnPorts) != 0 || strings.Contains(log.String(), "warning") {
				t.Errorf("foreign %v reported; log:\n%s", rep.ForeignOnPorts, log)
			}
			if c.keep == 0 {
				if err != nil || rep.GoneListeners != 2 {
					t.Errorf("control: err %v, %d listeners released, want nil and 2", err, rep.GoneListeners)
				}
				return
			}
			want := fmt.Sprintf("a socket they held still listens on 127.0.0.1:%d", c.port)
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("stop = %v, want an error containing %q", err, want)
			}
		})
	}
}

// TestForeignListenersLeavesOutTheSocketsOfLaunchedProcesses: of three listeners in the kernel's
// table, one held by a launched process (its inode is in the set), one outside the recorded ports,
// and one foreign on a recorded port, only the last is reported.
func TestForeignListenersLeavesOutTheSocketsOfLaunchedProcesses(t *testing.T) {
	setReleaseWait(t, 300*time.Millisecond)
	root := fakeProc(t, "boot-A")
	ports := downBlock(t)
	writeListenRows(t, root, []fakeRow{{ports.API, "9301"}, {ports.API + 5, "9302"}, {ports.API + 40, "9303"}})
	got, err := foreignListeners(ports, map[string]bool{"9301": true}, nil)
	if want := addrs(ports.API + 5); err != nil || len(got) != 1 || got[0] != want[0] {
		t.Fatalf("foreign = %v, %v; want %v", got, err, want)
	}
}
