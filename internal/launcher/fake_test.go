package launcher

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"
)

// fakePidfd stands in for the kernel's pidfds on a fake /proc tree. A PID absent from alive does
// not exist (pidfd_open fails with ESRCH); alive[pid] false means the process has exited.
type fakePidfd struct {
	mu       sync.Mutex
	openErr  error
	alive    map[int]bool
	fds      map[int]int
	next     int
	sent     []string
	onOpen   func(pid int)
	onSignal func(pid int, sig syscall.Signal)
}

func newFakePidfd(alive ...int) *fakePidfd {
	f := &fakePidfd{alive: map[int]bool{}, fds: map[int]int{}, next: 1000}
	for _, p := range alive {
		f.alive[p] = true
	}
	return f
}

func (f *fakePidfd) open(pid int) (int, error) {
	if f.onOpen != nil {
		f.onOpen(pid)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.openErr != nil {
		return -1, f.openErr
	}
	if _, ok := f.alive[pid]; !ok {
		return -1, syscall.ESRCH
	}
	f.next++
	f.fds[f.next] = pid
	return f.next, nil
}

func (f *fakePidfd) exited(fd int) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	pid, ok := f.fds[fd]
	if !ok {
		return false, syscall.EBADF
	}
	return !f.alive[pid], nil
}

func (f *fakePidfd) signal(fd int, sig syscall.Signal) error {
	f.mu.Lock()
	pid, ok := f.fds[fd]
	if !ok {
		f.mu.Unlock()
		return syscall.EBADF
	}
	if !f.alive[pid] {
		f.mu.Unlock()
		return syscall.ESRCH
	}
	f.sent = append(f.sent, fmt.Sprintf("%d:%s", pid, sigName(sig)))
	hook := f.onSignal
	f.mu.Unlock()
	if hook != nil {
		hook(pid, sig)
	}
	return nil
}

func (f *fakePidfd) close(fd int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.fds, fd)
}

func (f *fakePidfd) setAlive(pid int, alive bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.alive[pid] = alive
}

func (f *fakePidfd) openFDs() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.fds)
}

func (f *fakePidfd) signals() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.sent...)
}

func sigName(s syscall.Signal) string {
	switch s {
	case syscall.SIGTERM:
		return "TERM"
	case syscall.SIGKILL:
		return "KILL"
	}
	return strconv.Itoa(int(s))
}

// useFakePidfds installs f for the test and checks at the end that every handle was closed.
func useFakePidfds(t *testing.T, f *fakePidfd) {
	t.Helper()
	old := pidfds
	pidfds = f
	t.Cleanup(func() {
		pidfds = old
		if n := f.openFDs(); n != 0 {
			t.Errorf("%d pidfd(s) left open", n)
		}
	})
}

// writeFakeProc writes /proc/<pid>/stat with the given state, parent and start time.
func writeFakeProc(t *testing.T, root string, pid int, state string, ppid int, start uint64) {
	t.Helper()
	dir := filepath.Join(root, strconv.Itoa(pid))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	line := fmt.Sprintf("%d (k3s-server) %s %d 1 1 0 -1 0 0 0 0 0 0 0 0 0 20 0 1 0 %d 1 1 0 0 0 0 0 0 0 0 0 0 0 0 17 0 0 0 0 0 0", pid, state, ppid, start)
	if err := os.WriteFile(filepath.Join(dir, "stat"), []byte(line), 0o644); err != nil {
		t.Fatal(err)
	}
}

// fakeHost is a launched host on a fake /proc tree: a recorded k3s process whose file descriptors
// hold the three expected listeners, served for real by test servers on loopback (an apiserver
// that answers /readyz and the write probe, and a controller manager that answers /healthz).
type fakeHost struct {
	t       *testing.T
	root    string
	f       *fakePidfd
	l       layout
	pid     int
	rec     ProcRec
	st      *State
	inodes  map[int]string // port -> socket inode
	onKCM   func()         // runs when the controller manager's /healthz is asked
	kcmHits int
}

const fakeK3sPID = 4242

func newFakeHost(t *testing.T) *fakeHost {
	t.Helper()
	h := &fakeHost{t: t, root: fakeProc(t, "boot-A"), pid: fakeK3sPID, inodes: map[int]string{}}
	h.f = newFakePidfd(h.pid)
	useFakePidfds(t, h.f)
	l, err := newLayout(filepath.Join(shortDir(t), "st"))
	if err != nil {
		t.Fatal(err)
	}
	h.l = l
	if err := os.MkdirAll(filepath.Join(l.dataDir(), "data", "abc", "bin"), 0o700); err != nil {
		t.Fatal(err)
	}
	writeFakeProc(t, h.root, h.pid, "S", 1, 1000)
	h.setExe(filepath.Join(h.assetRoot(), "abc", "bin", "k3s-server"))

	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/readyz":
			w.WriteHeader(200)
		case r.Method == http.MethodPatch && r.URL.Path == "/api/v1/namespaces/kube-system/configmaps/hpcburst-launcher-probe":
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"kind":"ConfigMap","apiVersion":"v1","metadata":{"name":"hpcburst-launcher-probe","namespace":"kube-system"}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(api.Close)
	kcm := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.kcmHits++
		if h.onKCM != nil {
			h.onKCM()
		}
		w.WriteHeader(200)
	}))
	t.Cleanup(kcm.Close)
	internal, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { internal.Close() })
	ports := Ports{API: portOf(t, internal), HTTPS: api.Listener.Addr().(*net.TCPAddr).Port, KCM: kcm.Listener.Addr().(*net.TCPAddr).Port, LB: 1}
	for i, p := range ports.Expected() {
		h.inodes[p] = strconv.Itoa(9001 + i)
		h.giveSocket(h.pid, p)
	}
	h.writeListenTable()
	kc := "apiVersion: v1\nkind: Config\nclusters:\n- name: c\n  cluster:\n    server: https://127.0.0.1:" + strconv.Itoa(ports.HTTPS) +
		"\n    insecure-skip-tls-verify: true\ncontexts:\n- name: c\n  context: {cluster: c, user: u}\ncurrent-context: c\nusers:\n- name: u\n  user: {token: t}\n"
	for _, f := range []string{l.kubeconfig(), l.adminKubeconfig()} {
		if err := os.WriteFile(f, []byte(kc), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	h.rec = ProcRec{Name: "k3s", PID: h.pid, StartTicks: 1000, BootID: "boot-A"}
	h.st = &State{Version: "v1.36.5+k3s1", Ports: ports, Procs: []ProcRec{h.rec}, StartedAt: time.Now()}
	if err := l.saveState(h.st); err != nil {
		t.Fatal(err)
	}
	return h
}

func (h *fakeHost) assetRoot() string {
	r, err := assetRoot(h.l)
	if err != nil {
		h.t.Fatal(err)
	}
	return r
}

func (h *fakeHost) setExe(target string) {
	h.t.Helper()
	link := filepath.Join(h.root, strconv.Itoa(h.pid), "exe")
	_ = os.Remove(link)
	if err := os.Symlink(target, link); err != nil {
		h.t.Fatal(err)
	}
}

// giveSocket makes the socket of port one of pid's file descriptors, and nobody else's.
func (h *fakeHost) giveSocket(pid, port int) {
	h.t.Helper()
	ents, _ := filepath.Glob(filepath.Join(h.root, "*", "fd", "sock-"+strconv.Itoa(port)))
	for _, e := range ents {
		_ = os.Remove(e)
	}
	dir := filepath.Join(h.root, strconv.Itoa(pid), "fd")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		h.t.Fatal(err)
	}
	if err := os.Symlink("socket:["+h.inodes[port]+"]", filepath.Join(dir, "sock-"+strconv.Itoa(port))); err != nil {
		h.t.Fatal(err)
	}
}

func (h *fakeHost) writeListenTable() {
	h.t.Helper()
	tab := "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"
	i := 0
	for port, inode := range h.inodes {
		tab += fmt.Sprintf("   %d: 0100007F:%04X 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1001        0 %s 1 0000000000000000 100 0 0 10 0\n", i, port, inode)
		i++
	}
	if err := os.MkdirAll(filepath.Join(h.root, "net"), 0o755); err != nil {
		h.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(h.root, "net", "tcp"), []byte(tab), 0o644); err != nil {
		h.t.Fatal(err)
	}
}

// handle opens a verified handle on the fake k3s.
func (h *fakeHost) handle() *procHandle {
	h.t.Helper()
	ph, v, d := openRec(h.rec)
	if v != Ours {
		h.t.Fatalf("the fake k3s is %v: %s", v, d)
	}
	h.t.Cleanup(ph.Close)
	return ph
}
