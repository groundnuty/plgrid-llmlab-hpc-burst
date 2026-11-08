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

// shortReleaseWait shortens the wait for the whole test, so a foreign listener does not cost the
// full socketReleaseWait in every run. The tests that depend on the wait read the value back.
func shortReleaseWait(t *testing.T, d time.Duration) {
	t.Helper()
	old := socketReleaseWait
	socketReleaseWait = d
	t.Cleanup(func() { socketReleaseWait = old })
}

// TestDownReportsARealForeignListener: a listener of this test process, not of anything launched,
// sits on a recorded port the whole time. down reports it and says so in its log, leaves it
// listening, and gives it the release wait first: a listener that is still there at the end of the
// wait is foreign, one that is gone is not (the next test).
func TestDownReportsARealForeignListener(t *testing.T) {
	const wait = 1500 * time.Millisecond
	shortReleaseWait(t, wait)
	ports := downBlock(t)
	foreign, err := net.Listen("tcp", addrs(ports.API + 5)[0])
	if err != nil {
		t.Fatal(err)
	}
	defer foreign.Close()
	k3s := startListenHelper(t, addrs(ports.API), nil)
	l := downFixtureAt(t, ports, recorded(t, "k3s", k3s.pid))
	log := &lockedLog{}
	start := time.Now()
	rep, err := Down(context.Background(), DownOptions{StateDir: l.dir, Grace: 20 * time.Second, Log: log})
	took := time.Since(start)
	if err != nil {
		t.Fatalf("down: %v", err)
	}
	want := addrs(ports.API + 5)
	if len(rep.ForeignOnPorts) != 1 || rep.ForeignOnPorts[0] != want[0] {
		t.Fatalf("foreign = %v, want %v; log:\n%s", rep.ForeignOnPorts, want, log)
	}
	if !strings.Contains(log.String(), "warning: "+want[0]+" listens on one of the recorded ports") {
		t.Errorf("no warning for %s in the log:\n%s", want[0], log)
	}
	if took < wait {
		t.Errorf("down took %s: it reported the listener without waiting %s for it to be released", took, wait)
	}
	if refused(want[0]) {
		t.Error("down closed the foreign listener")
	}
	if !refused(addrs(ports.API)[0]) {
		t.Error("k3s's own port still listens")
	}
}

// TestDownDoesNotReportAListenerThatGoesDuringTheWait: a listener that is not ours sits on a
// recorded port when down first looks, and is closed while down waits for it (a socket the kernel
// is still releasing, say). It is closed on the observable event, down's own log line that says
// it is waiting, not after a delay, so the test does not depend on how fast the machine is. It is
// not reported afterwards.
func TestDownDoesNotReportAListenerThatGoesDuringTheWait(t *testing.T) {
	shortReleaseWait(t, 20*time.Second) // never reached: the listener goes at the first line
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
	start := time.Now()
	rep, err := Down(context.Background(), DownOptions{StateDir: l.dir, Grace: 20 * time.Second, Log: log})
	if err != nil {
		t.Fatalf("down: %v", err)
	}
	select {
	case <-closedAt:
	default:
		t.Fatalf("down never said it was waiting for the listener, so the test never closed it; log:\n%s", log)
	}
	if len(rep.ForeignOnPorts) != 0 || strings.Contains(log.String(), "warning") {
		t.Errorf("a listener that went during the wait was reported: %v; log:\n%s", rep.ForeignOnPorts, log)
	}
	if took := time.Since(start); took > 15*time.Second {
		t.Errorf("down took %s: it waited out the whole release wait for a listener that was already gone", took)
	}
}
