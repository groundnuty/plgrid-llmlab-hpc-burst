package front

// The split on the real frp v0.71.1 (testenv.FrpBinaries; HBEP-6, The bulk path; #448, part 5):
// the launcher's two frps, from its own renderers (frps, and frps-bulk with tcpMux off), a token
// frpc and a bulk frpc, each through the production dial-home listener. Ports: base 30296 in
// #448's block (30280-30359, IMPL-COMMON), so the plugin stub binds 30304, frps 30305-30307 and
// frps-bulk 30309-30311; every other listener takes a kernel port. Everything binds 127.0.0.1.

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

const bulkRigBase = 30296

// bulkRig is the launcher's two frps on bulkRigBase, admitted by a stub plugin at both paths.
type bulkRig struct {
	t               *testing.T
	host            agentapi.Host
	frpc            string
	pwFirst, pwBulk string
	first, bulkCert string // the two certificate files
	n               int
}

func newBulkRig(t *testing.T) *bulkRig {
	t.Helper()
	return newBulkRigWith(t, "")
}

// newBulkRigWith is newBulkRig whose second frps also posts to the front's bulk plugin at
// frontBulk, 127.0.0.1:<port>, when it is set (launcher.WriteFrpsBulkConfigWithFront).
func newBulkRigWith(t *testing.T, frontBulk string) *bulkRig {
	t.Helper()
	return newBulkRigFronts(t, "", frontBulk)
}

// newBulkRigFronts is newBulkRig whose first frps posts to the front's plugin at frontToken and
// whose second posts to the front's bulk plugin at frontBulk, each when it is set.
func newBulkRigFronts(t *testing.T, frontToken, frontBulk string) *bulkRig {
	t.Helper()
	frps, frpc := testenv.FrpBinaries(t)
	checkRootK3s(t)
	r := &bulkRig{t: t, host: agentapi.Host{StateDir: shortDir(t), PortBase: bulkRigBase}, frpc: frpc}
	bulkPair(t, r.host)
	r.first, r.bulkCert = r.host.AgentCertFile(), r.host.BulkCertFile()
	ln, err := net.Listen("tcp", r.host.FrpsPluginAddr())
	if err != nil {
		t.Fatalf("the plugin stub cannot listen on %s: %v", r.host.FrpsPluginAddr(), err)
	}
	admit := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"reject":false,"unchange":true}`)
	}
	mux := http.NewServeMux()
	mux.HandleFunc(agentapi.FrpsPluginPath, admit)
	mux.HandleFunc(agentapi.FrpsBulkPluginPath, admit)
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	if r.pwFirst, _, err = launcher.EnsureFrpsWebPassword(r.host); err != nil {
		t.Fatal(err)
	}
	if r.pwBulk, _, err = launcher.EnsureFrpsBulkWebPassword(r.host); err != nil {
		t.Fatal(err)
	}
	cfgFirst, err := launcher.WriteFrpsConfig(r.host, r.pwFirst)
	if frontToken != "" {
		cfgFirst, err = launcher.WriteFrpsConfigWithFront(r.host, r.pwFirst, frontToken)
	}
	if err != nil {
		t.Fatal(err)
	}
	cfgBulk, err := launcher.WriteFrpsBulkConfig(r.host, r.pwBulk)
	if frontBulk != "" {
		cfgBulk, err = launcher.WriteFrpsBulkConfigWithFront(r.host, r.pwBulk, frontBulk)
	}
	if err != nil {
		t.Fatal(err)
	}
	bin, err := launcher.ResolveFrpsFile(frps, launcher.DefaultFrpVersion)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bin.Close)
	for _, s := range []struct{ cfg, log, web string }{
		{cfgFirst, launcher.FrpsLogFile(r.host), r.host.FrpsWebAddr()},
		{cfgBulk, launcher.FrpsBulkLogFile(r.host), r.host.FrpsBulkWebAddr()},
	} {
		p, err := launcher.StartFrps(launcher.FrpsSpec{Bin: bin, Config: s.cfg, Log: s.log, Dir: r.host.StateDir})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _, _ = p.Stop(context.Background(), 5*time.Second) })
		waitUntilFor(t, 15*time.Second, "frps's /healthz on "+s.web, func() bool {
			resp, err := (&http.Client{Timeout: time.Second}).Get("http://" + s.web + "/healthz")
			if err != nil {
				return false
			}
			resp.Body.Close()
			return resp.StatusCode == http.StatusOK
		})
	}
	return r
}

// waitUntilFor waits on observable state for at most d.
func waitUntilFor(t *testing.T, d time.Duration, what string, ok func() bool) {
	t.Helper()
	for end := time.Now().Add(d); !ok(); time.Sleep(50 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("timed out after %s waiting for %s", d, what)
		}
	}
}

// listed is the UID's status on the frps whose web server is web ("absent" when it is not listed).
func (r *bulkRig) listed(web, pw, uid string) string {
	req, _ := http.NewRequest(http.MethodGet, "http://"+web+"/api/proxy/tcpmux", nil)
	req.SetBasicAuth(agentapi.FrpsWebUser, pw)
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return "unreadable"
	}
	defer resp.Body.Close()
	var v struct {
		Proxies []struct {
			Name   string `json:"name"`
			Status string `json:"status"`
		} `json:"proxies"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return "unreadable"
	}
	for _, p := range v.Proxies {
		if p.Name == uid {
			return p.Status
		}
	}
	return "absent"
}

// engineSocket serves GET /health with label in X-Served-By on a Unix socket, as an engine double.
func (r *bulkRig) engineSocket(label string) string {
	r.t.Helper()
	sock := filepath.Join(r.host.StateDir, label+".sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		r.t.Fatal(err)
	}
	srv := &http.Server{ReadHeaderTimeout: 10 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Served-By", label)
		_, _ = io.WriteString(w, "ok")
	})}
	go func() { _ = srv.Serve(ln) }()
	r.t.Cleanup(func() { _ = srv.Close() })
	return sock
}

// startFrpc starts the real frpc with the agent's own rendering for uid against the dial-home
// listener at dialHome, naming serverName and trusting ca. bulk adds transport.tcpMux = false, as
// the bulk frpc runs (HBEP-6, The bulk path). It returns the log's path. frpc is stopped through
// its pidfd (os.Process) at the end of the test.
func (r *bulkRig) startFrpc(uid, sock, dialHome, serverName, ca string, bulk bool) string {
	r.t.Helper()
	log, _, _ := r.startFrpcProc(uid, sock, dialHome, serverName, ca, bulk)
	return log
}

// startFrpcProc is startFrpc, and also returns frpc's process, whose Kill and Signal go through its
// pidfd (os.Process on Linux), and a channel closed once it has exited.
func (r *bulkRig) startFrpcProc(uid, sock, dialHome, serverName, ca string, bulk bool) (string, *os.Process, <-chan struct{}) {
	r.t.Helper()
	r.n++
	host, port, _ := net.SplitHostPort(dialHome)
	pn, _ := strconv.Atoi(port)
	cfg := filepath.Join(r.host.StateDir, fmt.Sprintf("frpc%d.toml", r.n))
	text, err := agent.FrpcConfig{ServerAddr: host, ServerPort: pn, LogFile: cfg + ".log", Token: newKey(r.t),
		CAFile: ca, ServerName: serverName, UID: uid, Socket: sock}.Render()
	if err != nil {
		r.t.Fatal(err)
	}
	if bulk {
		text = []byte(strings.Replace(string(text), "transport.poolCount = 0\n", "transport.poolCount = 0\ntransport.tcpMux = false\n", 1))
	}
	writeMode600(r.t, cfg, string(text))
	cmd := exec.Command(r.frpc, "-c", cfg)
	cmd.Env = []string{"PATH=/usr/bin:/bin"}
	if err := cmd.Start(); err != nil {
		r.t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	r.t.Cleanup(func() {
		_ = cmd.Process.Kill() // a pidfd signal to this one process (os.Process on Linux)
		<-done
	})
	return cfg + ".log", cmd.Process, done
}

// via sends CONNECT uid:80 to the tcpmux port mux and GET /health, and returns who served it.
func via(t *testing.T, mux, uid string) (int, string) {
	t.Helper()
	c, err := net.DialTimeout("tcp", mux, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(15 * time.Second))
	fmt.Fprintf(c, "CONNECT %s:80 HTTP/1.1\r\nHost: %s:80\r\n\r\n", uid, uid)
	br := bufio.NewReader(c)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodConnect})
	if err != nil || resp.StatusCode != http.StatusOK {
		return 0, ""
	}
	fmt.Fprint(c, "GET /health HTTP/1.1\r\nHost: instance\r\nConnection: close\r\n\r\n")
	r2, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil {
		return 0, ""
	}
	_ = r2.Body.Close()
	return r2.StatusCode, r2.Header.Get("X-Served-By")
}

// TestRealFrpcEachNameReachesItsOwnFrps: through the dial-home listener with the bulk path on, the
// token frpc (the first certificate, the token's name) registers on the first frps only, and the
// bulk frpc (the second certificate, the bulk name, tcpMux off) on the second only, for one UID.
// Each frps's tcpmux port then reaches its own frpc's engine socket.
func TestRealFrpcEachNameReachesItsOwnFrps(t *testing.T) {
	r := newBulkRig(t)
	for mode, uid := range map[string]string{ModeDirect: "0a1b2c3d-0000-4000-8000-000000000001", ModeHostDialed: "0a1b2c3d-0000-4000-8000-000000000002"} {
		t.Run(mode, func(t *testing.T) {
			addr := serveSplit(t, mode, r.host, r.host.FrpsBindAddr(), r.host.FrpsBulkBindAddr(), true)
			r.startFrpc(uid, r.engineSocket("token-"+mode), addr, agentapi.DialHomeServerName, r.first, false)
			r.startFrpc(uid, r.engineSocket("bulk-"+mode), addr, bulkName, r.bulkCert, true)
			waitUntilFor(t, 30*time.Second, "both frpc to register", func() bool {
				return r.listed(r.host.FrpsWebAddr(), r.pwFirst, uid) == "online" && r.listed(r.host.FrpsBulkWebAddr(), r.pwBulk, uid) == "online"
			})
			var by [2]string
			waitUntilFor(t, 30*time.Second, "a reply through each frps", func() bool {
				_, by[0] = via(t, r.host.FrpsTCPMuxAddr(), uid)
				_, by[1] = via(t, r.host.FrpsBulkTCPMuxAddr(), uid)
				return by[0] != "" && by[1] != ""
			})
			if by[0] != "token-"+mode || by[1] != "bulk-"+mode {
				t.Errorf("the first frps reached %q and the second %q; want each its own frpc's engine", by[0], by[1])
			}
		})
	}
}

// TestRealBulkFrpcAgainstABulkOffFrontNeverLogsIn (M1's control; the plan's R8 situation): with the
// bulk path off, a bulk frpc, as an Allocation staged before a rollback runs it, fails at TLS (the
// first certificate does not name the bulk server) and registers on neither frps, while a token
// frpc beside it registers as usual.
func TestRealBulkFrpcAgainstABulkOffFrontNeverLogsIn(t *testing.T) {
	r := newBulkRig(t)
	const uid = "0a1b2c3d-0000-4000-8000-00000000000a"
	addr := serveSplit(t, ModeHostDialed, r.host, r.host.FrpsBindAddr(), r.host.FrpsBulkBindAddr(), false)
	r.startFrpc(uid, r.engineSocket("token"), addr, agentapi.DialHomeServerName, r.first, false)
	bulkLog := r.startFrpc(uid, r.engineSocket("bulk"), addr, bulkName, r.bulkCert, true)
	waitUntilFor(t, 30*time.Second, "the token frpc to register", func() bool {
		return r.listed(r.host.FrpsWebAddr(), r.pwFirst, uid) == "online"
	})
	waitUntilFor(t, 30*time.Second, "the bulk frpc's second refused TLS handshake", func() bool {
		b, _ := os.ReadFile(bulkLog)
		return strings.Count(string(b), "not "+bulkName) >= 2
	})
	if s := r.listed(r.host.FrpsBulkWebAddr(), r.pwBulk, uid); s != "absent" {
		t.Errorf("the second frps lists the UID %s with the bulk path off", s)
	}
	if code, by := via(t, r.host.FrpsTCPMuxAddr(), uid); code != http.StatusOK || by != "token" {
		t.Errorf("the first frps reached %q (%d): want only the token frpc's engine", by, code)
	}
}
