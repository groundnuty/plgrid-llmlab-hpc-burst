package launcher

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The rig for the tests that run the real frps (and frpc) from the directory named by
// HPCBURST_FRP_DIR (testenv.FrpBinaries: it skips without them, and fails when
// HPCBURST_REQUIRE_FRP=1, which CI sets). These tests bind only
// 29252-29259 (IMPL-COMMON, 31 Oct 2026: 29244-29259; 29240-29243 belong to the launcher's own
// block): frpsRigBase is the first rig's base (plugin 29252, bind 29253, tcpmux 29254, web 29255)
// and frpsRigBase2 the second's (29256-29259). Everything binds 127.0.0.1.
const (
	frpsRigBase  = 29244
	frpsRigBase2 = 29248
)

// pluginCall is one request frps posted to the plugin.
type pluginCall struct {
	Op      string // from the request body
	QueryOp string // from the query, where frps also puts it
	Path    string
	Content map[string]any
}

// pluginStub is the plugin endpoint (the lifecycle controller's, in the product) at base+8. It
// accepts everything, and records what frps posts.
type pluginStub struct {
	mu    sync.Mutex
	calls []pluginCall
}

func startPluginStub(t *testing.T, h agentapi.Host) *pluginStub {
	t.Helper()
	s := &pluginStub{}
	ln, err := net.Listen("tcp", h.FrpsPluginAddr())
	if err != nil {
		t.Fatalf("the plugin stub cannot listen: %v", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc(agentapi.FrpsPluginPath, func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Op      string         `json:"op"`
			Content map[string]any `json:"content"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.calls = append(s.calls, pluginCall{Op: body.Op, QueryOp: r.URL.Query().Get("op"), Path: r.URL.Path, Content: body.Content})
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"reject":false,"unchange":true}`)
	})
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return s
}

func (s *pluginStub) snapshot() []pluginCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]pluginCall(nil), s.calls...)
}

func (s *pluginStub) ops() []string {
	var out []string
	for _, c := range s.snapshot() {
		out = append(out, c.Op)
	}
	return out
}

// waitOps waits until the stub has seen at least want calls with op, or fails after d.
func (s *pluginStub) waitOp(t *testing.T, op string, d time.Duration) {
	t.Helper()
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(25 * time.Millisecond) {
		for _, c := range s.snapshot() {
			if c.Op == op {
				return
			}
		}
	}
	t.Fatalf("frps never posted %s to the plugin within %s; it posted %v", op, d, s.ops())
}

// frpsRig is a real frps started by the launcher's own code on one port block.
type frpsRig struct {
	t     *testing.T
	host  agentapi.Host
	pw    string
	bin   FrpsBinary
	proc  *FrpsProc
	stub  *pluginStub
	state string
	// stopped is set by stop; the cleanup stops frps only if the test did not.
	stopped bool
}

// newFrpsRig renders the configuration, starts frps with it, and waits until its web server
// answers. nice is the value passed to StartFrps. It also arranges that the root k3s is
// unchanged afterwards (IMPL-COMMON), and that the process is stopped at the end.
func newFrpsRig(t *testing.T, base, nice int) *frpsRig {
	t.Helper()
	frps, _ := testenv.FrpBinaries(t)
	return newFrpsRigFrom(t, base, nice, frps, nil)
}

// newFrpsRigFrom is newFrpsRig for the frps executable at frpsPath. afterResolve, when not nil,
// runs once the launcher has opened and hashed the file and before it starts frps: a test uses
// it to change what the path names.
func newFrpsRigFrom(t *testing.T, base, nice int, frpsPath string, afterResolve func(bin FrpsBinary)) *frpsRig {
	t.Helper()
	before := rootK3sListening(t)
	if os.Getenv("HPCBURST_TEST_ROOT_K3S") == "1" && before != "6443,6444,10250" {
		t.Fatalf("HPCBURST_TEST_ROOT_K3S=1, but the root k3s listens only on %q", before)
	}
	t.Cleanup(func() {
		if after := rootK3sListening(t); after != before {
			t.Errorf("the root k3s's listeners changed: %q -> %q", before, after)
		}
	})
	r := &frpsRig{t: t, state: shortDir(t)}
	r.host = agentapi.Host{StateDir: r.state, PortBase: base}
	var err error
	if r.pw, _, err = EnsureFrpsWebPassword(r.host); err != nil {
		t.Fatal(err)
	}
	cfg, err := WriteFrpsConfig(r.host, r.pw)
	if err != nil {
		t.Fatal(err)
	}
	if r.bin, err = ResolveFrpsFile(frpsPath, DefaultFrpVersion); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.bin.Close)
	if afterResolve != nil {
		afterResolve(r.bin)
	}
	r.stub = startPluginStub(t, r.host)
	r.proc, err = StartFrps(FrpsSpec{Bin: r.bin, Config: cfg, Log: FrpsLogFile(r.host), Dir: r.state, Nice: nice})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !r.stopped {
			_, _ = r.proc.Stop(context.Background(), 2*time.Second)
		}
	})
	r.waitHealthy()
	return r
}

// stop ends frps the way the launcher does, and fails the test if that fails.
func (r *frpsRig) stop() FrpsStop {
	r.t.Helper()
	r.stopped = true
	rep, err := r.proc.Stop(context.Background(), 5*time.Second)
	if err != nil {
		r.t.Fatalf("stop: %v (%+v)", err, rep)
	}
	return rep
}

func (r *frpsRig) waitHealthy() {
	r.t.Helper()
	for end := time.Now().Add(15 * time.Second); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
		select {
		case <-r.proc.Done():
			r.t.Fatalf("frps exited at start: %s\n%s", r.proc.ExitStatus(), r.logTail())
		default:
		}
		if code, _ := r.get("", "", "/healthz"); code == 200 {
			return
		}
	}
	r.t.Fatalf("frps's web server never answered /healthz\n%s", r.logTail())
}

func (r *frpsRig) logTail() string {
	b, _ := os.ReadFile(FrpsLogFile(r.host))
	if len(b) > 2000 {
		b = b[len(b)-2000:]
	}
	return string(b)
}

// get asks the web server at base+11; an empty user sends no credentials.
func (r *frpsRig) get(user, pass, path string) (int, string) {
	r.t.Helper()
	req, err := http.NewRequest("GET", "http://"+r.host.FrpsWebAddr()+path, nil)
	if err != nil {
		r.t.Fatal(err)
	}
	if user != "" {
		req.SetBasicAuth(user, pass)
	}
	c := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true}}
	resp, err := c.Do(req)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// proxies returns the web API's list of tcpmux proxies, name to status, read with the right password.
func (r *frpsRig) proxies() map[string]string {
	r.t.Helper()
	code, body := r.get(agentapi.FrpsWebUser, r.pw, "/api/proxy/tcpmux")
	if code != 200 {
		r.t.Fatalf("GET /api/proxy/tcpmux = %d %s", code, body)
	}
	var list struct {
		Proxies []struct{ Name, Status string } `json:"proxies"`
	}
	if err := json.Unmarshal([]byte(body), &list); err != nil {
		r.t.Fatalf("%v in %s", err, body)
	}
	out := map[string]string{}
	for _, p := range list.Proxies {
		out[p.Name] = p.Status
	}
	return out
}

// ssEntry is one listening socket from ss(8).
type ssEntry struct {
	Proto, Host string
	Port        int
	Pids        string // the process column, empty without -p
}

func (e ssEntry) String() string { return fmt.Sprintf("%s %s:%d", e.Proto, e.Host, e.Port) }

// ssListeners lists the listening TCP and UDP sockets whose port is in [lo, hi], as ss(8) reports
// them: an implementation independent of the launcher's own /proc parsing. withPids adds -p, which
// shows the processes of the calling user.
func ssListeners(t *testing.T, lo, hi int, withPids bool) []ssEntry {
	t.Helper()
	args := []string{"-H", "-l", "-t", "-u", "-n"}
	if withPids {
		args = append(args, "-p")
	}
	out, err := exec.Command("ss", args...).Output()
	if err != nil {
		t.Fatalf("ss: %v", err)
	}
	var res []ssEntry
	for _, l := range strings.Split(string(out), "\n") {
		f := strings.Fields(l)
		if len(f) < 5 {
			continue
		}
		i := strings.LastIndex(f[4], ":")
		port, err := strconv.Atoi(f[4][i+1:])
		if i < 0 || err != nil || port < lo || port > hi {
			continue
		}
		e := ssEntry{Proto: f[0], Host: f[4][:i], Port: port}
		if len(f) > 6 {
			e.Pids = strings.Join(f[6:], " ")
		}
		res = append(res, e)
	}
	return res
}

// procsMentioning lists the processes whose command line contains needle, from /proc.
func procsMentioning(needle string) []int {
	ents, _ := os.ReadDir("/proc")
	var out []int
	for _, e := range ents {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		if b, err := os.ReadFile("/proc/" + e.Name() + "/cmdline"); err == nil && strings.Contains(string(b), needle) {
			out = append(out, pid)
		}
	}
	return out
}

// gone reports whether /proc has no entry for the PID at all: not running, and not a zombie that
// nobody reaped.
func gone(pid int) bool {
	_, err := os.Stat("/proc/" + strconv.Itoa(pid))
	return os.IsNotExist(err)
}

// startFrpc starts the real frpc (the path from testenv.FrpBinaries) with a configuration file, from an empty environment (frp reads
// http_proxy for its connection to frps otherwise), and kills it at the end of the test. The
// returned function kills it earlier, as a crashed job would, and waits for it.
func startFrpc(t *testing.T, frpc, cfg string) (kill func()) {
	t.Helper()
	log, err := os.Create(cfg + ".out")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { log.Close() })
	cmd := exec.Command(frpc, "-c", cfg)
	cmd.Env = []string{"PATH=/usr/bin:/bin"}
	cmd.Stdout, cmd.Stderr = log, log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	killed := false
	kill = func() {
		if killed {
			return
		}
		killed = true
		_ = cmd.Process.Kill()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Errorf("frpc (pid %d) did not exit after SIGKILL", cmd.Process.Pid)
		}
	}
	t.Cleanup(kill)
	return kill
}

// copyFile copies src to dst with the given mode.
func copyFile(t *testing.T, src, dst string, mode os.FileMode) {
	t.Helper()
	b, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, b, mode); err != nil {
		t.Fatal(err)
	}
}
