package agent_test

// The rig for the bulk frpc's real-frp tests (HBEP-6, The bulk path; issue
// #448, part 4): real frps and frpc v0.71.1, a first frps with frp's
// multiplexing on and a second with transport.tcpMux = false (as the launcher
// renders them, part 2), and a Go stand-in for the front's dial-home listener
// with the bulk path on: one TLS listener that holds the first certificate in
// Certificates and serves the second, for the same key, through
// GetCertificate when the client names bulk.dial-home.hpcburst.invalid, and
// sends GET /~!frp to its own frps by the name the TLS handshake carried
// (HBEP-6: "The dial-home listener splits by TLS server name"; the front's own
// listener is part 5). Every port is the kernel's, on 127.0.0.1: frps's ports,
// which its configuration must name, are taken by binding port 0, reading the
// port, closing, and starting frps (again on another, when another process took
// one first).
//
// Every process is signalled through a pidfd and never through a process
// group (a child that shares the test's group, signalled by group, would take
// the test with it). The agent's own Frpc stops its frpc as production code
// does, which is its business.

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/procgroup"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// ---- processes, signalled by pidfd only ----

// pidfdKillID sends KILL to the process the ID names, through a pidfd opened for
// it, after checking that the process behind the descriptor is the recorded one:
// the start time of PID is read again once the descriptor is open, and a PID
// that shows another start time was reaped and reused, so nothing is sent.
func pidfdKillID(t testing.TB, id agent.ProcessID) {
	t.Helper()
	if err := pidfdSignal(id, syscall.SIGKILL); err != nil {
		t.Fatalf("kill %d: %v", id.PID, err)
	}
}

func pidfdSignal(id agent.ProcessID, sig syscall.Signal) error {
	fd, err := unix.PidfdOpen(id.PID, 0)
	if err != nil {
		return fmt.Errorf("pidfd_open: %w", err)
	}
	defer unix.Close(fd)
	now, err := procgroup.IDOf(id.PID)
	if err != nil {
		return fmt.Errorf("the process is gone: %w", err)
	}
	if now != id {
		return fmt.Errorf("PID %d now has start time %d, not the recorded %d: it was reused", id.PID, now.StartTime, id.StartTime)
	}
	return unix.PidfdSendSignal(fd, sig, nil, 0)
}

// rawProc is a process the test started with a pidfd of its own (clone's
// PidFD), so a signal through it reaches that process or nobody.
type rawProc struct {
	name string
	id   agent.ProcessID
	fd   int
	done chan struct{}
	once sync.Once
}

func startRaw(t testing.TB, w *procWatch, name, path string, args []string, env []string) *rawProc {
	t.Helper()
	fd := -1
	cmd := exec.Command(path, args...)
	cmd.Env = env
	cmd.SysProcAttr = &syscall.SysProcAttr{PidFD: &fd}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", name, err)
	}
	p := &rawProc{name: name, fd: fd, done: make(chan struct{})}
	if id, err := procgroup.IDOf(cmd.Process.Pid); err == nil {
		p.id = id
		w.add(name, id)
	}
	go func() { _ = cmd.Wait(); close(p.done) }()
	t.Cleanup(p.stop)
	return p
}

func (p *rawProc) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// stop sends TERM, then KILL after 3 s, both through the pidfd, waits for the
// process and closes the descriptor.
func (p *rawProc) stop() {
	p.once.Do(func() {
		if !p.exited() {
			_ = unix.PidfdSendSignal(p.fd, unix.SIGTERM, nil, 0)
			select {
			case <-p.done:
			case <-time.After(3 * time.Second):
				_ = unix.PidfdSendSignal(p.fd, unix.SIGKILL, nil, 0)
				<-p.done
			}
		}
		_ = unix.Close(p.fd)
	})
}

// assertNoneLeft waits a little for every recorded process to be gone and
// fails the test for any that is not, killing it through a pidfd; it also fails
// for an frpc, frps, ssh or sshd among the test's descendants that nobody
// recorded.
func (w *procWatch) assertNoneLeft(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		w.mu.Lock()
		var left []agent.ProcessID
		var names []string
		for id, name := range w.ids {
			if id.Alive() {
				left = append(left, id)
				names = append(names, fmt.Sprintf("%s %d", name, id.PID))
			}
		}
		w.mu.Unlock()
		for _, id := range descendants(os.Getpid()) {
			exe, err := os.Readlink("/proc/" + strconv.Itoa(id.PID) + "/exe")
			if err != nil {
				continue
			}
			switch filepath.Base(exe) {
			case "frpc", "frps", "ssh", "sshd":
				if id.Alive() {
					left = append(left, id)
					names = append(names, fmt.Sprintf("unrecorded %s %d", filepath.Base(exe), id.PID))
				}
			}
		}
		if len(left) == 0 {
			return
		}
		if time.Now().After(deadline) {
			for _, id := range left {
				_ = pidfdSignal(id, syscall.SIGKILL)
			}
			t.Errorf("processes left after the test: %s", strings.Join(names, ", "))
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// ---- kernel-chosen ports ----

// kernelPorts returns n distinct ports of 127.0.0.1 that were free a moment
// ago: bound together, read, and closed.
func kernelPorts(t testing.TB, n int) []int {
	t.Helper()
	var lns []net.Listener
	var ports []int
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns = append(lns, ln)
		ports = append(ports, ln.Addr().(*net.TCPAddr).Port)
	}
	for _, ln := range lns {
		ln.Close()
	}
	return ports
}

// ---- frps ----

// bulkFrps is one frps with HBEP-6's loopback settings and S1's heartbeats,
// and no plugin (these tests need no admission), on kernel-chosen ports. The
// second frps of the bulk path has transport.tcpMux = false, which the launcher
// renders with S1's set written verbatim (part 2). The queries of frpsProc
// (online, request, waitRoute) serve both.
type bulkFrps struct {
	*frpsProc
	muxOff bool
	proc   *rawProc
	dir    string
	w      *procWatch
}

// startBulkFrps starts an frps, again on other ports when the first try did
// not come up.
func startBulkFrps(t *testing.T, bin, dir string, muxOff bool, w *procWatch) *bulkFrps {
	t.Helper()
	label := "frps"
	if muxOff {
		label = "frps-bulk"
	}
	for attempt := 0; attempt < 4; attempt++ {
		ports := kernelPorts(t, 3)
		fdir := filepath.Join(dir, label)
		if err := os.MkdirAll(fdir, 0o700); err != nil {
			t.Fatal(err)
		}
		f := &frpsProc{bindPort: ports[0], muxPort: ports[1], webPort: ports[2], password: "test-web-password-" + label,
			bin: bin, log: filepath.Join(fdir, label+".log"), w: w}
		var mux string
		if muxOff {
			mux = "transport.tcpMux = false\n"
		}
		cfg := fmt.Sprintf(`bindAddr = "127.0.0.1"
bindPort = %d
proxyBindAddr = "127.0.0.1"
tcpmuxHTTPConnectPort = %d
webServer.addr = "127.0.0.1"
webServer.port = %d
webServer.user = %q
webServer.password = %q
log.to = %q
%stransport.tcpMuxKeepaliveInterval = 10
transport.heartbeatTimeout = 25
transport.tcpKeepalive = 30
`, f.bindPort, f.muxPort, f.webPort, agentapi.FrpsWebUser, f.password, f.log, mux)
		f.config = writeFile(t, filepath.Join(fdir, label+".toml"), []byte(cfg))
		bf := &bulkFrps{frpsProc: f, muxOff: muxOff, dir: fdir, w: w}
		bf.proc = startRaw(t, w, label, bin, []string{"-c", f.config}, nil)
		if bf.waitUp(5 * time.Second) {
			return bf
		}
		bf.proc.stop()
	}
	t.Fatalf("%s did not come up", label)
	return nil
}

func (f *bulkFrps) waitUp(d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if f.proc.exited() {
			return false
		}
		resp, err := http.Get("http://127.0.0.1:" + strconv.Itoa(f.webPort) + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return true
			}
		}
		time.Sleep(30 * time.Millisecond)
	}
	return false
}

func (f *bulkFrps) bindAddr() string { return "127.0.0.1:" + strconv.Itoa(f.bindPort) }

// ---- the dial-home listener with the bulk path on ----

// hsRecord is one TLS handshake the listener attempted.
type hsRecord struct {
	at time.Time // when the handshake returned
	ok bool
}

type nameStats struct {
	hs       []hsRecord
	frpConns int // GET /~!frp passed to an frps
	others   int // any other request
}

// splitFront stands in for the front's dial-home listener (HBEP-6, The bulk
// path). With second nil it is the front without the bulk path (or after a
// rollback): it serves the first certificate under every name. Statistics are
// kept by the server name the client sent.
type splitFront struct {
	ln          net.Listener
	first       tls.Certificate
	second      *tls.Certificate
	tokenTarget string
	bulkTarget  string
	wg          sync.WaitGroup
	mu          sync.Mutex
	by          map[string]*nameStats
	conns       map[net.Conn]bool
	// toToken and toBulk count the frps each GET /~!frp reached.
	toToken, toBulk atomic.Int32
}

func startSplitFront(t *testing.T, first tls.Certificate, second *tls.Certificate, tokenTarget, bulkTarget string) *splitFront {
	t.Helper()
	f := &splitFront{ln: kernelListener(t), first: first, second: second, tokenTarget: tokenTarget, bulkTarget: bulkTarget,
		by: map[string]*nameStats{}, conns: map[net.Conn]bool{}}
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		for {
			c, err := f.ln.Accept()
			if err != nil {
				return
			}
			f.wg.Add(1)
			go func() { defer f.wg.Done(); f.serve(c) }()
		}
	}()
	t.Cleanup(f.close)
	return f
}

func (f *splitFront) port() int { return portOf(f.ln) }

func (f *splitFront) stats(name string) *nameStats {
	s := f.by[name]
	if s == nil {
		s = &nameStats{}
		f.by[name] = s
	}
	return s
}

// handshakes returns a copy of the handshake records under a server name.
func (f *splitFront) handshakes(name string) []hsRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]hsRecord(nil), f.stats(name).hs...)
}

func (f *splitFront) frpConns(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stats(name).frpConns
}

func (f *splitFront) others(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stats(name).others
}

// failedHandshakes counts the handshakes under a name that did not complete.
func (f *splitFront) failedHandshakes(name string) int {
	n := 0
	for _, h := range f.handshakes(name) {
		if !h.ok {
			n++
		}
	}
	return n
}

func (f *splitFront) track(c net.Conn, on bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if on {
		f.conns[c] = true
	} else {
		delete(f.conns, c)
	}
}

func (f *splitFront) serve(raw net.Conn) {
	f.track(raw, true)
	defer f.track(raw, false)
	defer raw.Close()
	var sni string
	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{f.first},
		GetCertificate: func(h *tls.ClientHelloInfo) (*tls.Certificate, error) {
			sni = h.ServerName
			if f.second != nil && h.ServerName == bulkName {
				return f.second, nil
			}
			return nil, nil // the first, from Certificates
		},
	}
	c := tls.Server(raw, cfg)
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	err := c.Handshake()
	f.mu.Lock()
	f.stats(sni).hs = append(f.stats(sni).hs, hsRecord{at: time.Now(), ok: err == nil})
	f.mu.Unlock()
	if err != nil {
		return
	}
	br := bufio.NewReader(c)
	line, err := br.ReadString('\n')
	if err != nil {
		return
	}
	if !strings.HasPrefix(line, "GET /~!frp ") {
		f.mu.Lock()
		f.stats(sni).others++
		f.mu.Unlock()
		_, _ = io.WriteString(c, "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
		return
	}
	target, counter := f.tokenTarget, &f.toToken
	if f.second != nil && sni == bulkName {
		target, counter = f.bulkTarget, &f.toBulk
	}
	f.mu.Lock()
	f.stats(sni).frpConns++
	f.mu.Unlock()
	counter.Add(1)
	_ = c.SetDeadline(time.Time{})
	up, err := net.DialTimeout("tcp4", target, 2*time.Second)
	if err != nil {
		return
	}
	f.track(up, true)
	defer f.track(up, false)
	defer up.Close()
	if _, err := io.WriteString(up, line); err != nil {
		return
	}
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(up, br); done <- struct{}{} }()
	go func() { _, _ = io.Copy(c, up); done <- struct{}{} }()
	<-done
}

func (f *splitFront) close() {
	f.ln.Close()
	f.mu.Lock()
	for c := range f.conns {
		c.Close()
	}
	f.mu.Unlock()
	f.wg.Wait()
}

// ---- the whole rig ----

// startLog records the processes an Frpc started.
type startLog struct {
	mu  sync.Mutex
	ids []agent.ProcessID
}

func (s *startLog) add(id agent.ProcessID) {
	s.mu.Lock()
	s.ids = append(s.ids, id)
	s.mu.Unlock()
}

func (s *startLog) all() []agent.ProcessID {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]agent.ProcessID(nil), s.ids...)
}

func (s *startLog) count() int { return len(s.all()) }

// last returns the newest start; false before the first.
func (s *startLog) last() (agent.ProcessID, bool) {
	a := s.all()
	if len(a) == 0 {
		return agent.ProcessID{}, false
	}
	return a[len(a)-1], true
}

type bulkRigOptions struct {
	// bulkOff makes the listener the front without the bulk path, or after a
	// rollback: the first certificate under every name, every /~!frp to the
	// first frps.
	bulkOff bool
	// noFrps leaves both frps unstarted: a test whose frpc must never get past
	// TLS needs none.
	noFrps bool
}

// bulkRig is the two frps, the listener, the engine behind the agent's serving
// socket, and the certificates, all in one directory.
type bulkRig struct {
	t               *testing.T
	w               *procWatch
	dir, runDir     string
	frpsBin, frpcBn string
	pair            bulkPair
	frps1, frps2    *bulkFrps // the first (tcpMux on) and the second (tcpMux off)
	front           *splitFront
	firstFile       string // the staged certificates
	secondFile      string
	logs            *logBuf
}

func newBulkRig(t *testing.T, o bulkRigOptions) *bulkRig {
	t.Helper()
	frpsBin, frpcBin := frpBins(t)
	r := &bulkRig{t: t, w: newProcWatch(), frpsBin: frpsBin, frpcBn: frpcBin, logs: &logBuf{}}
	t.Cleanup(func() { r.w.assertNoneLeft(t) })
	r.dir = shortTemp(t)
	r.runDir = filepath.Join(r.dir, "run")
	if err := os.Mkdir(r.runDir, 0o700); err != nil {
		t.Fatal(err)
	}
	r.pair = makeBulkPair(t)
	r.firstFile = writeFile(t, filepath.Join(r.dir, "staged-first.crt"), r.pair.first.pem)
	r.secondFile = writeFile(t, filepath.Join(r.dir, "staged-second.crt"), r.pair.second.pem)
	startEngine(t, r.runDir, hInstUID)
	tokenTarget, bulkTarget := "127.0.0.1:1", "127.0.0.1:1"
	if !o.noFrps {
		r.frps1 = startBulkFrps(t, frpsBin, r.dir, false, r.w)
		r.frps2 = startBulkFrps(t, frpsBin, r.dir, true, r.w)
		tokenTarget, bulkTarget = r.frps1.bindAddr(), r.frps2.bindAddr()
	}
	var second *tls.Certificate
	if !o.bulkOff {
		second = &r.pair.second.tls
	}
	r.front = startSplitFront(t, r.pair.first.tls, second, tokenTarget, bulkTarget)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("the agent's log:\n%s", r.logs.String())
			for _, f := range []*bulkFrps{r.frps1, r.frps2} {
				if f != nil {
					b, _ := os.ReadFile(f.log)
					if len(b) > 3000 {
						b = b[len(b)-3000:]
					}
					t.Logf("%s's log:\n%s", filepath.Base(f.log), b)
				}
			}
			for _, n := range []string{"frpc.log", "frpc-bulk.log"} {
				b, _ := os.ReadFile(filepath.Join(r.runDir, n))
				if len(b) > 3000 {
					b = b[len(b)-3000:]
				}
				t.Logf("%s:\n%s", n, b)
			}
		}
	})
	return r
}

func (r *bulkRig) server() agent.FrpcServer {
	s, err := agent.DirectServer("127.0.0.1:" + strconv.Itoa(r.front.port()))
	if err != nil {
		r.t.Fatal(err)
	}
	return s
}

// frpc is the agent's Frpc, the token frpc or the bulk one, with the real
// frpc, its own staged certificate and the production restart backoff scaled
// down to a few hundred milliseconds (backoff 0 keeps the defaults).
func (r *bulkRig) frpc(bulk bool, backoff time.Duration) (*agent.Frpc, *startLog) {
	r.t.Helper()
	cert, name := r.firstFile, "frpc"
	if bulk {
		cert, name = r.secondFile, "frpc-bulk"
	}
	f := newFrpc(r.t, r.frpcBn, r.runDir, cert, r.pair.pin, r.server(), r.w)
	f.Bulk = bulk
	if backoff == 0 {
		f.MinBackoff, f.MaxBackoff = 0, 0
	} else {
		f.MinBackoff, f.MaxBackoff = backoff, 3*backoff
	}
	starts := &startLog{}
	f.OnStart = func(id agent.ProcessID) { r.w.add(name, id); starts.add(id) }
	f.Logger = slogTo(r.logs)
	return f, starts
}

// run runs the Frpc for the Instance until the test's end or the returned stop.
func (r *bulkRig) run(f *agent.Frpc, uid string) (stop func()) {
	r.t.Helper()
	f.Serve(uid)
	return runBackground(r.t, "an frpc", f.Run)
}

// rawBulk starts the real frpc directly with a bulk-shaped configuration of the
// agent's renderer, for a case the agent's own checks would refuse before frpc
// runs: what frpc itself does with a certificate or a name the agent would not
// have sent it. caFile is the certificate frpc trusts; name is its serverName;
// tcpMuxOff is the bulk frpc's setting.
func (r *bulkRig) rawBulk(label, uid, caFile, name string, tcpMuxOff bool) (cfgPath, logPath string, p *rawProc) {
	r.t.Helper()
	logPath = filepath.Join(r.dir, label+".log")
	cfg := agent.FrpcConfig{
		ServerAddr: "127.0.0.1", ServerPort: r.front.port(), LogFile: logPath, Token: hCredential,
		CAFile: caFile, ServerName: name, UID: uid, Socket: agent.ServingSocket(r.runDir), TCPMuxOff: tcpMuxOff,
	}
	b, err := cfg.Render()
	if err != nil {
		r.t.Fatal(err)
	}
	// A test with no trustedCaFile proves nothing: frpc then verifies nothing
	// (HBEP-6, Certificate checks), so the configuration must carry one.
	if !strings.Contains(string(b), "transport.tls.trustedCaFile = \""+caFile+"\"\n") {
		r.t.Fatalf("the raw frpc's configuration has no trustedCaFile:\n%s", b)
	}
	cfgPath = writeFile(r.t, filepath.Join(r.dir, label+".toml"), b)
	p = startRaw(r.t, r.w, label, r.frpcBn, []string{"-c", cfgPath}, agent.FrpcEnvForTest(os.Environ()))
	return cfgPath, logPath, p
}

// tail returns the last n bytes of a file, for a failure's message.
func tail(path string, n int) string {
	b, _ := os.ReadFile(path)
	if len(b) > n {
		b = b[len(b)-n:]
	}
	return string(b)
}
