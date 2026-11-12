package launcher

// Tests of the bulk path's second frps's configuration and files (HBEP-6, The bulk path; #448).

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The first frps's configuration as main rendered it before the bulk path (#448), comments and
// all, for frpsTestHost and testWebPassword, without and with the front's plugin. The bulk path
// gave the renderer a shape; the first frps's rendering must not change by a byte.
const (
	firstFrpsConfigBeforeBulk          = "# Written by hpcburst at every start (HBEP-6). It holds the web password: mode 600, never edited by hand.\n# Loopback only: bindAddr defaults to every interface, and proxyBindAddr follows it.\nbindAddr = \"127.0.0.1\"\nbindPort = 29253\nproxyBindAddr = \"127.0.0.1\"\ntcpmuxHTTPConnectPort = 29254\n# Every other listener stays off. Port 0 is frp's default; it is written so a new default cannot turn one on.\nkcpBindPort = 0\nquicBindPort = 0\nvhostHTTPPort = 0\nvhostHTTPSPort = 0\nsshTunnelGateway.bindPort = 0\n# Token authentication with no token: frps's own check passes every login, and the plugin is the gate.\nauth.method = \"token\"\nwebServer.addr = \"127.0.0.1\"\nwebServer.port = 29255\nwebServer.user = \"hpcburst\"\nwebServer.password = \"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\"\nlog.to = \"/var/lib/hb-state/logs/frps.log\"\ntransport.tcpMuxKeepaliveInterval = 10\ntransport.heartbeatTimeout = 25\ntransport.tcpKeepalive = 30\n\n[[httpPlugins]]\nname = \"hpcburst\"\naddr = \"127.0.0.1:29252\"\npath = \"/frp/v1/handler\"\nops = [\"Login\", \"NewProxy\", \"CloseProxy\"]\n"
	firstFrpsConfigWithFrontBeforeBulk = "# Written by hpcburst at every start (HBEP-6). It holds the web password: mode 600, never edited by hand.\n# Loopback only: bindAddr defaults to every interface, and proxyBindAddr follows it.\nbindAddr = \"127.0.0.1\"\nbindPort = 29253\nproxyBindAddr = \"127.0.0.1\"\ntcpmuxHTTPConnectPort = 29254\n# Every other listener stays off. Port 0 is frp's default; it is written so a new default cannot turn one on.\nkcpBindPort = 0\nquicBindPort = 0\nvhostHTTPPort = 0\nvhostHTTPSPort = 0\nsshTunnelGateway.bindPort = 0\n# Token authentication with no token: frps's own check passes every login, and the plugin is the gate.\nauth.method = \"token\"\nwebServer.addr = \"127.0.0.1\"\nwebServer.port = 29255\nwebServer.user = \"hpcburst\"\nwebServer.password = \"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\"\nlog.to = \"/var/lib/hb-state/logs/frps.log\"\ntransport.tcpMuxKeepaliveInterval = 10\ntransport.heartbeatTimeout = 25\ntransport.tcpKeepalive = 30\n\n[[httpPlugins]]\nname = \"hpcburst\"\naddr = \"127.0.0.1:29252\"\npath = \"/frp/v1/handler\"\nops = [\"Login\", \"NewProxy\", \"CloseProxy\"]\n\n# The front's plugin, after the controller's: frps stops at the first refusal, so the front hears only admitted\n# proxies, and registers a proxy only once the front has stopped routing to its UID (HBEP-6, Admission).\n[[httpPlugins]]\nname = \"hpcburst-front\"\naddr = \"127.0.0.1:29248\"\npath = \"/frp/v1/front\"\nops = [\"NewProxy\", \"CloseProxy\"]\n"
)

// TestFirstFrpsConfigIsByteForByteAsBefore pins the first frps's whole file, comments included, to
// main's rendering before the bulk path, with and without the front's plugin. The existing tests
// pin its settings line by line; this pins every byte.
func TestFirstFrpsConfigIsByteForByteAsBefore(t *testing.T) {
	plain, err := RenderFrpsConfig(frpsTestHost(), testWebPassword)
	if err != nil {
		t.Fatal(err)
	}
	if plain != firstFrpsConfigBeforeBulk {
		t.Errorf("the first frps's configuration changed:\n%s", plain)
	}
	front, err := RenderFrpsConfigWithFront(frpsTestHost(), testWebPassword, "127.0.0.1:29248")
	if err != nil {
		t.Fatal(err)
	}
	if front != firstFrpsConfigWithFrontBeforeBulk {
		t.Errorf("the first frps's configuration with the front's plugin changed:\n%s", front)
	}
}

// settings are a configuration's lines that are neither empty nor comments.
func settings(text string) []string {
	var out []string
	for _, l := range strings.Split(text, "\n") {
		if l != "" && !strings.HasPrefix(l, "#") {
			out = append(out, l)
		}
	}
	return out
}

// TestRenderFrpsBulkConfigHasEverythingTheHBEPFixes: the second frps's settings, typed from HBEP-6
// (frps on the control-plane host, :197-226; The bulk path, :483-486): loopback only, its own
// ports at base+13 to base+15, its own password and log, tcpMux off, S1's heartbeat set written
// explicitly, and the controller's plugin at its bulk path.
func TestRenderFrpsBulkConfigHasEverythingTheHBEPFixes(t *testing.T) {
	const bulkPassword = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
	got, err := RenderFrpsBulkConfig(frpsTestHost(), bulkPassword)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		`bindAddr = "127.0.0.1"`,
		`bindPort = 29257`,
		`proxyBindAddr = "127.0.0.1"`,
		`tcpmuxHTTPConnectPort = 29258`,
		`kcpBindPort = 0`,
		`quicBindPort = 0`,
		`vhostHTTPPort = 0`,
		`vhostHTTPSPort = 0`,
		`sshTunnelGateway.bindPort = 0`,
		`auth.method = "token"`,
		`webServer.addr = "127.0.0.1"`,
		`webServer.port = 29259`,
		`webServer.user = "hpcburst"`,
		`webServer.password = "` + bulkPassword + `"`,
		`log.to = "/var/lib/hb-state/logs/frps-bulk.log"`,
		`transport.tcpMux = false`,
		`transport.tcpMuxKeepaliveInterval = 10`,
		`transport.heartbeatTimeout = 25`,
		`transport.tcpKeepalive = 30`,
		`[[httpPlugins]]`,
		`name = "hpcburst"`,
		`addr = "127.0.0.1:29252"`,
		`path = "/frp/v1/handler/bulk"`,
		`ops = ["Login", "NewProxy", "CloseProxy"]`,
	}
	if g := settings(got); strings.Join(g, "\n") != strings.Join(want, "\n") {
		t.Fatalf("the second frps's settings differ from HBEP-6's.\ngot:\n%s\nwant:\n%s", strings.Join(g, "\n"), strings.Join(want, "\n"))
	}
	for _, absent := range []string{"auth.token", "auth.oidc", "Ping", "NewWorkConn", "NewUserConn", "hpcburst-front"} {
		if strings.Contains(got, absent) {
			t.Errorf("the second frps's configuration mentions %q", absent)
		}
	}
}

// TestFrpsBulkConfigDiffersFromTheFirstOnlyInItsShape: line for line, the second frps's settings
// are the first's but for its ports, password, log, plugin path and the tcpMux line, so nothing of
// HBEP-6's loopback and heartbeat rules is lost in the bulk path.
func TestFrpsBulkConfigDiffersFromTheFirstOnlyInItsShape(t *testing.T) {
	first, err := RenderFrpsConfigWithFront(frpsTestHost(), testWebPassword, "127.0.0.1:29248")
	if err != nil {
		t.Fatal(err)
	}
	bulk, err := RenderFrpsBulkConfigWithFront(frpsTestHost(), testWebPassword, "127.0.0.1:29248")
	if err != nil {
		t.Fatal(err)
	}
	f, b := settings(first), settings(bulk)
	var onlyFirst, onlyBulk []string
	inF, inB := map[string]bool{}, map[string]bool{}
	for _, l := range f {
		inF[l] = true
	}
	for _, l := range b {
		inB[l] = true
		if !inF[l] {
			onlyBulk = append(onlyBulk, l)
		}
	}
	for _, l := range f {
		if !inB[l] {
			onlyFirst = append(onlyFirst, l)
		}
	}
	wantFirst := []string{`bindPort = 29253`, `tcpmuxHTTPConnectPort = 29254`, `webServer.port = 29255`, `log.to = "/var/lib/hb-state/logs/frps.log"`, `path = "/frp/v1/handler"`, `path = "/frp/v1/front"`}
	wantBulk := []string{`bindPort = 29257`, `tcpmuxHTTPConnectPort = 29258`, `webServer.port = 29259`, `log.to = "/var/lib/hb-state/logs/frps-bulk.log"`, `transport.tcpMux = false`, `path = "/frp/v1/handler/bulk"`, `path = "/frp/v1/front/bulk"`}
	if strings.Join(onlyFirst, "\n") != strings.Join(wantFirst, "\n") || strings.Join(onlyBulk, "\n") != strings.Join(wantBulk, "\n") {
		t.Errorf("only in the first:\n%s\nonly in the bulk path's:\n%s", strings.Join(onlyFirst, "\n"), strings.Join(onlyBulk, "\n"))
	}
}

// TestRenderFrpsBulkConfigWithFrontAddsTheFrontsBulkPluginSecond: with a front, the second frps
// posts to the front's plugin after the controller's, at the front's bulk path, so a bulk
// NewProxy or CloseProxy moves only the bulk epoch (HBEP-6, The bulk path). A front address that
// is not 127.0.0.1:<port> is refused.
func TestRenderFrpsBulkConfigWithFrontAddsTheFrontsBulkPluginSecond(t *testing.T) {
	plain, err := RenderFrpsBulkConfig(frpsTestHost(), testWebPassword)
	if err != nil {
		t.Fatal(err)
	}
	got, err := RenderFrpsBulkConfigWithFront(frpsTestHost(), testWebPassword, "127.0.0.1:29248")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got, plain) {
		t.Fatalf("the front's rendering does not start with the plain one:\n%s", got)
	}
	want := []string{`[[httpPlugins]]`, `name = "hpcburst-front"`, `addr = "127.0.0.1:29248"`, `path = "/frp/v1/front/bulk"`, `ops = ["NewProxy", "CloseProxy"]`}
	if tail := settings(strings.TrimPrefix(got, plain)); strings.Join(tail, "\n") != strings.Join(want, "\n") {
		t.Fatalf("after the controller's plugin:\ngot:\n%s\nwant:\n%s", strings.Join(tail, "\n"), strings.Join(want, "\n"))
	}
	for _, bad := range []string{"", "localhost:29248", "0.0.0.0:29248", "127.0.0.1", "127.0.0.1:0", "[::1]:29248"} {
		if out, err := RenderFrpsBulkConfigWithFront(frpsTestHost(), testWebPassword, bad); err == nil {
			t.Errorf("front address %q rendered without an error:\n%s", bad, out)
		}
	}
}

// TestFrpsRealVerifiesTheBulkConfig: the pinned frps's own strict check accepts the second frps's
// file, with the front's plugin, so tcpMux = false beside S1's tcpMuxKeepaliveInterval is a valid
// combination for it.
func TestFrpsRealVerifiesTheBulkConfig(t *testing.T) {
	frps, _ := testenv.FrpBinaries(t)
	h := agentapi.Host{StateDir: shortDir(t), PortBase: frpsRigBase}
	path, err := WriteFrpsBulkConfigWithFront(h, testWebPassword, "127.0.0.1:29248")
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command(frps, "verify", "-c", path, "--strict-config=true").CombinedOutput(); err != nil {
		t.Fatalf("frps verify rejects the second frps's file: %v\n%s", err, out)
	}
}

// TestFrpsBulkHasItsOwnWebPassword: the second frps's password is a file of its own, created
// beside the first's and never the same key, and each is kept once it exists.
func TestFrpsBulkHasItsOwnWebPassword(t *testing.T) {
	h := agentapi.Host{StateDir: shortDir(t), PortBase: 29244}
	first, created, err := EnsureFrpsWebPassword(h)
	if err != nil || !created {
		t.Fatalf("first password: %v, created %v", err, created)
	}
	bulk, created, err := EnsureFrpsBulkWebPassword(h)
	if err != nil || !created {
		t.Fatalf("bulk password: %v, created %v", err, created)
	}
	if first == bulk {
		t.Error("the two frps share a web password")
	}
	if b, err := os.ReadFile(h.FrpsBulkWebPasswordFile()); err != nil || string(b) != bulk {
		t.Errorf("the bulk password file holds %q (%v), want the returned key", b, err)
	}
	if fi, err := os.Stat(h.FrpsBulkWebPasswordFile()); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("the bulk password file: %v %v, want mode 0600", fi, err)
	}
	again, created, err := EnsureFrpsBulkWebPassword(h)
	if err != nil || created || again != bulk {
		t.Errorf("a second call: %q, created %v, %v; want the kept key", again, created, err)
	}
	if kept, _, _ := EnsureFrpsWebPassword(h); kept != first {
		t.Error("the first frps's password changed")
	}
}

// TestWriteFrpsBulkFilesWritesTheFrontsBulkPluginOnlyWithAFront: up's writer of the second frps's
// files, with the front's health address and without one; its file is its own.
func TestWriteFrpsBulkFilesWritesTheFrontsBulkPluginOnlyWithAFront(t *testing.T) {
	h := agentapi.Host{StateDir: shortDir(t), PortBase: 29244}
	for _, c := range []struct {
		front string
		want  bool
	}{{"127.0.0.1:29248", true}, {"", false}} {
		path, err := writeFrpsBulkFiles(h, c.front)
		if err != nil {
			t.Fatal(err)
		}
		if path != FrpsBulkConfigFile(h) || path == FrpsConfigFile(h) {
			t.Fatalf("the second frps's file is %s", path)
		}
		b, _ := os.ReadFile(path)
		if got := strings.Contains(string(b), `path = "/frp/v1/front/bulk"`) && strings.Contains(string(b), `addr = "`+c.front+`"`); got != c.want {
			t.Errorf("front %q: the front's bulk plugin is written %v, want %v:\n%s", c.front, got, c.want, b)
		}
		if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != 0o600 {
			t.Errorf("the second frps's file: %v %v, want mode 0600", fi, err)
		}
	}
}

// b448Base is a launcher base in #448's port block (30280-30359, IMPL-COMMON): a test that runs the
// second frps binds its plugin stub at base+8 (30288) and frps-bulk at base+13 to base+15
// (30293-30295), and nothing else of the block.
const b448Base = 30280

// startFrpsBulk starts the real frps from the launcher's own frps-bulk file, edited by edit when it
// is set, and waits for its web server. The caller serves the plugin at base+8 (startAdmitAll). It
// stops frps at the end of the test, and checks the root k3s is unchanged.
func startFrpsBulk(t *testing.T, edit func([]byte) []byte) (agentapi.Host, *FrpsProc) {
	t.Helper()
	frps, _ := testenv.FrpBinaries(t)
	before := rootK3sListening(t)
	t.Cleanup(func() {
		if after := rootK3sListening(t); after != before {
			t.Errorf("the root k3s's listeners changed: %q -> %q", before, after)
		}
	})
	h := agentapi.Host{StateDir: shortDir(t), PortBase: b448Base}
	path, err := writeFrpsBulkFiles(h, "")
	if err != nil {
		t.Fatal(err)
	}
	if edit != nil {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, edit(b), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	bin, err := ResolveFrpsFile(frps, DefaultFrpVersion)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bin.Close)
	spec := frpsBulkSpecFor(h)
	spec.Bin = bin
	p, err := StartFrps(spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = p.Stop(context.Background(), 2*time.Second) })
	for end := time.Now().Add(15 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		select {
		case <-p.Done():
			t.Fatalf("frps-bulk exited at start: %s", p.ExitStatus())
		default:
		}
		if resp, err := (&http.Client{Timeout: time.Second}).Get("http://" + h.FrpsBulkWebAddr() + "/healthz"); err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				break
			}
		}
		if time.Now().After(end) {
			t.Fatal("frps-bulk's web server never answered /healthz")
		}
	}
	return h, p
}

// startAdmitAll serves a plugin at addr that admits every call to path, as the controller's would
// admit the test's nothing: frps-bulk only posts to it.
func startAdmitAll(t *testing.T, addr, path string) {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("the plugin stub cannot listen on %s: %v", addr, err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc(path, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"reject":false,"unchange":true}`)
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
}

// exactPortProblems is the supervisor's own check of an frps's ports (checkEntry): the process
// tree's bound sockets against exactly the three, TCP on 127.0.0.1, and nothing else.
func exactPortProblems(t *testing.T, p *FrpsProc, ports []int) []string {
	t.Helper()
	ph, v, d := openRec(p.Rec())
	if v != Ours {
		t.Fatalf("frps-bulk is %v: %s", v, d)
	}
	defer ph.Close()
	owned, others, err := treeBound(ph)
	if err != nil {
		t.Fatal(err)
	}
	want := make([]sock, 0, len(ports))
	for _, port := range ports {
		want = append(want, sock{Listener: Listener{Addr: "127.0.0.1", Port: port}, proto: "tcp"})
	}
	return verifyExactListeners(want, owned, others)
}

// TestFrpsBulkRealHoldsExactlyItsThreeLoopbackPorts (HBEP-6, Test plan, Loopback only: "with the
// bulk path on, the same for the second frps"): the real frps, from the launcher's frps-bulk file,
// holds exactly base+13 to base+15 on 127.0.0.1, by the supervisor's own check and by ss(8). The
// positive control, as the Test plan asks: the same file without bindAddr makes frps listen on every
// interface, and the same check then fails, so it can.
func TestFrpsBulkRealHoldsExactlyItsThreeLoopbackPorts(t *testing.T) {
	testenv.FrpBinaries(t)
	startAdmitAll(t, agentapi.Host{PortBase: b448Base}.FrpsPluginAddr(), agentapi.FrpsBulkPluginPath)
	h, p := startFrpsBulk(t, nil)
	ports := []int{h.FrpsBulkBindPort(), h.FrpsBulkTCPMuxPort(), h.FrpsBulkWebPort()}
	if probs := exactPortProblems(t, p, ports); len(probs) != 0 {
		t.Errorf("frps-bulk from the launcher's file: %v", probs)
	}
	var mine []string
	for _, e := range ssListeners(t, 1, 65535, true) {
		if strings.Contains(e.Pids, "pid="+strconv.Itoa(p.Rec().PID)+",") {
			mine = append(mine, e.String())
		}
	}
	sort.Strings(mine)
	want := []string{"tcp " + h.FrpsBulkBindAddr(), "tcp " + h.FrpsBulkTCPMuxAddr(), "tcp " + h.FrpsBulkWebAddr()}
	sort.Strings(want)
	if !slices.Equal(mine, want) {
		t.Errorf("sockets of frps-bulk in ss -p:\n got %v\nwant %v", mine, want)
	}
	if _, err := p.Stop(context.Background(), 5*time.Second); err != nil {
		t.Fatal(err)
	}

	_, wild := startFrpsBulk(t, func(b []byte) []byte {
		return bytes.Replace(b, []byte("bindAddr = \"127.0.0.1\"\n"), nil, 1)
	})
	probs := exactPortProblems(t, wild, ports)
	if !strings.Contains(strings.Join(probs, "; "), "is missing") || !strings.Contains(strings.Join(probs, "; "), "also listens on") {
		t.Errorf("frps-bulk without bindAddr passed the check: %v", probs)
	}
}
