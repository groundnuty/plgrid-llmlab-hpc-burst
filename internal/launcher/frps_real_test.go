package launcher

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// These tests run the real frps (v0.71.1, from testenv.FrpBinaries) from the configuration the launcher
// renders, started and stopped by the launcher's own code. What they check is what frps does, as
// ss(8), /proc and its HTTP interfaces show it, not what the rendering says.

// TestFrpsRealHoldsExactlyItsThreeLoopbackPorts: the ports base+8 .. base+11 have four
// listeners in all, every one on 127.0.0.1: the three that frps opens (bind, tcpmux, web) and the
// plugin's, which the test's stub holds, because frps only dials it. Nothing is UDP and nothing
// is on IPv6, so KCP, QUIC and the other listeners are off. Then the web API, the identity
// guard on the live process, and the stop.
func TestFrpsRealHoldsExactlyItsThreeLoopbackPorts(t *testing.T) {
	r := newFrpsRig(t, frpsRigBase, 10)
	h, rec := r.host, r.proc.Rec()

	// It runs the sealed copy whose sum was checked, whatever wraps it (nice execs into frps): its
	// image is the memory file, and holds the verified bytes.
	if exe, err := os.Readlink("/proc/" + strconv.Itoa(rec.PID) + "/exe"); err != nil {
		t.Fatal(err)
	} else if exe != frpsMemfdExe {
		t.Errorf("pid %d runs %s, want the verified copy %s", rec.PID, exe, frpsMemfdExe)
	}
	if img, err := os.ReadFile("/proc/" + strconv.Itoa(rec.PID) + "/exe"); err != nil {
		t.Fatal(err)
	} else if sum := sha256.Sum256(img); hex.EncodeToString(sum[:]) != r.bin.SHA256 {
		t.Errorf("the image of pid %d has sha256 %x, not the verified %s", rec.PID, sum, r.bin.SHA256)
	}
	if cmdline, _ := os.ReadFile("/proc/" + strconv.Itoa(rec.PID) + "/cmdline"); !strings.Contains(string(cmdline), "--strict-config=true") {
		t.Errorf("command line %q lacks --strict-config=true", cmdline)
	}

	toStrings := func(es []ssEntry) []string {
		var out []string
		for _, e := range es {
			out = append(out, e.String())
		}
		sort.Strings(out)
		return out
	}
	wantBlock := []string{"tcp " + h.FrpsPluginAddr(), "tcp " + h.FrpsBindAddr(), "tcp " + h.FrpsTCPMuxAddr(), "tcp " + h.FrpsWebAddr()}
	sort.Strings(wantBlock)
	if got := toStrings(ssListeners(t, h.FrpsPluginPort(), h.FrpsWebPort(), false)); !reflect.DeepEqual(got, wantBlock) {
		t.Errorf("listeners in ports %d-%d:\n got %v\nwant %v", h.FrpsPluginPort(), h.FrpsWebPort(), got, wantBlock)
	}

	// frps's own sockets, wherever they are: the three, and no others.
	var mine []ssEntry
	for _, e := range ssListeners(t, 1, 65535, true) {
		if strings.Contains(e.Pids, "pid="+strconv.Itoa(rec.PID)+",") {
			mine = append(mine, e)
		}
	}
	wantMine := []string{"tcp " + h.FrpsBindAddr(), "tcp " + h.FrpsTCPMuxAddr(), "tcp " + h.FrpsWebAddr()}
	sort.Strings(wantMine)
	if got := toStrings(mine); !reflect.DeepEqual(got, wantMine) {
		t.Errorf("sockets of frps (pid %d) in ss -p:\n got %v\nwant %v", rec.PID, got, wantMine)
	}
	// And no socket of any other kind (UDP, Unix, an unclosed connection). The test's own HTTP
	// calls close their connections, which frps notices at once; give it a moment.
	n := len(fdSocketInodes(rec.PID))
	for end := time.Now().Add(2 * time.Second); n != 3 && time.Now().Before(end); n = len(fdSocketInodes(rec.PID)) {
		time.Sleep(20 * time.Millisecond)
	}
	if n != 3 {
		t.Errorf("frps holds %d sockets of any kind, want its 3 listeners", n)
	}

	// The web API: the right user and password list the proxies; wrong ones are refused.
	if code, body := r.get(agentapi.FrpsWebUser, r.pw, "/api/proxy/tcpmux"); code != 200 || !strings.Contains(body, `"proxies"`) {
		t.Errorf("right credentials: %d %s", code, body)
	}
	for _, c := range []struct{ name, user, pass string }{
		{"wrong password", agentapi.FrpsWebUser, strings.Repeat("0", 64)},
		{"wrong user", "admin", r.pw},
		{"the frp default login", "admin", "admin"},
		{"no credentials", "", ""},
	} {
		if code, body := r.get(c.user, c.pass, "/api/proxy/tcpmux"); code != 401 {
			t.Errorf("%s: %d %s, want 401", c.name, code, body)
		}
	}
	if code, _ := r.get("", "", "/healthz"); code != 200 {
		t.Errorf("/healthz without credentials = %d, want 200", code)
	}

	// The identity guard on the live process: a record with another start time (a reused PID)
	// is refused and frps goes on running.
	stale := rec
	stale.StartTicks++
	if rep, err := StopFrps(context.Background(), stale, time.Second); !errors.Is(err, ErrFrpsForeign) || rep.Signals != "" {
		t.Fatalf("StopFrps(stale record) = %+v, %v", rep, err)
	}
	time.Sleep(200 * time.Millisecond)
	if st, err := readStat(rec.PID); err != nil || st.StartTicks != rec.StartTicks || !runningNow(rec.PID) {
		t.Fatalf("frps did not survive a stop with a stale record: %+v %v", st, err)
	}
	if code, _ := r.get("", "", "/healthz"); code != 200 {
		t.Fatalf("/healthz = %d after the refused stop", code)
	}
	select {
	case <-r.proc.Done():
		t.Fatalf("frps exited: %s", r.proc.ExitStatus())
	default:
	}

	// The real stop: SIGTERM is enough for frps, it is reaped, and nothing is left.
	rep := r.stop()
	if rep.Signals != "TERM" || rep.Before != "ours" {
		t.Errorf("stop report = %+v, want a plain SIGTERM", rep)
	}
	select {
	case <-r.proc.Done():
	default:
		t.Error("Done is not closed after Stop")
	}
	if !gone(rec.PID) {
		t.Errorf("/proc/%d still exists after Stop: state %q", rec.PID, procState(rec.PID))
	}
	if left := procsMentioning(r.state); len(left) != 0 {
		t.Errorf("processes still mention the state directory: %v", left)
	}
	if got := toStrings(ssListeners(t, h.FrpsPluginPort(), h.FrpsWebPort(), false)); !reflect.DeepEqual(got, []string{"tcp " + h.FrpsPluginAddr()}) {
		t.Errorf("after the stop the block holds %v, want only the test's stub", got)
	}
	if log, _ := os.ReadFile(FrpsLogFile(h)); !strings.Contains(string(log), "frps started successfully") {
		t.Errorf("the log %s lacks frps's start-up line:\n%s", FrpsLogFile(h), log)
	}
}

// TestFrpsRealPluginSeesLoginNewProxyAndCloseProxyButNoPing connects the real frpc to the real
// frps over plain TCP, with a tcpmux proxy to a Unix socket as HBEP-6 describes, a heartbeat of
// one second so that a Ping subscription would show, and the plugin stub at base+8.
func TestFrpsRealPluginSeesLoginNewProxyAndCloseProxyButNoPing(t *testing.T) {
	_, frpc := testenv.FrpBinaries(t)
	r := newFrpsRig(t, frpsRigBase2, 0)
	h := r.host
	const uid = "uid-4711" // stands for an Instance's UID

	// The engine: an HTTP server on a Unix socket, which frpc's unix_domain_socket plugin reaches.
	sock := filepath.Join(r.state, "e.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	engine := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		fmt.Fprintf(w, "engine-ok %s", q.URL.Path)
	})}
	go func() { _ = engine.Serve(ln) }()
	t.Cleanup(func() { _ = engine.Close() })

	cfg := filepath.Join(r.state, "frpc.toml")
	frpcConf := fmt.Sprintf(`serverAddr = "127.0.0.1"
serverPort = %d
loginFailExit = false
log.to = %q
metadatas.token = "tok-test"
transport.protocol = "tcp"
transport.poolCount = 0
transport.tcpMuxKeepaliveInterval = 10
transport.heartbeatInterval = 1
transport.heartbeatTimeout = 5

[[proxies]]
name = %q
type = "tcpmux"
multiplexer = "httpconnect"
customDomains = [%q]
[proxies.plugin]
type = "unix_domain_socket"
unixPath = %q
`, h.FrpsBindPort(), filepath.Join(r.state, "frpc.log"), uid, uid, sock)
	if err := os.WriteFile(cfg, []byte(frpcConf), 0o600); err != nil {
		t.Fatal(err)
	}
	killFrpc := startFrpc(t, frpc, cfg)

	r.stub.waitOp(t, "NewProxy", 15*time.Second)
	for end := time.Now().Add(15 * time.Second); r.proxies()[uid] != "online"; time.Sleep(50 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("frps never listed %s online: %v\nops: %v", uid, r.proxies(), r.stub.ops())
		}
	}

	// What the plugin was told: the login's metadata (the credential, in the product) and the proxy.
	calls := r.stub.snapshot()
	if len(calls) < 2 || calls[0].Op != "Login" || calls[1].Op != "NewProxy" {
		t.Fatalf("the first calls were %v, want Login then NewProxy", r.stub.ops())
	}
	for _, c := range calls {
		if c.Path != agentapi.FrpsPluginPath || c.QueryOp != c.Op {
			t.Errorf("call %+v: path or query op differs from the body's op", c)
		}
	}
	if metas, _ := calls[0].Content["metas"].(map[string]any); metas["token"] != "tok-test" {
		t.Errorf("Login content metas = %v, want token tok-test", calls[0].Content["metas"])
	}
	np := calls[1].Content
	if np["proxy_name"] != uid || np["proxy_type"] != "tcpmux" || np["multiplexer"] != "httpconnect" || !reflect.DeepEqual(np["custom_domains"], []any{uid}) {
		t.Errorf("NewProxy content = %v", np)
	}

	// base+10 routes a CONNECT for the UID to the engine through the tunnel, and answers 404 for a
	// UID with no proxy.
	connect := func(host string) (status string, body string) {
		c, err := net.DialTimeout("tcp", h.FrpsTCPMuxAddr(), 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(10 * time.Second))
		fmt.Fprintf(c, "CONNECT %s:80 HTTP/1.1\r\nHost: %s:80\r\n\r\n", host, host)
		rd := bufio.NewReader(c)
		status, _ = rd.ReadString('\n')
		if !strings.Contains(status, " 200 ") {
			return strings.TrimSpace(status), ""
		}
		for l, _ := rd.ReadString('\n'); strings.TrimSpace(l) != ""; l, _ = rd.ReadString('\n') {
		}
		fmt.Fprintf(c, "GET /health HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", host)
		resp, err := http.ReadResponse(rd, nil)
		if err != nil {
			t.Fatalf("the engine's reply through the tunnel: %v", err)
		}
		defer resp.Body.Close()
		b := make([]byte, 64)
		n, _ := resp.Body.Read(b)
		return strings.TrimSpace(status), string(b[:n])
	}
	if status, body := connect(uid); !strings.Contains(status, " 200 ") || body != "engine-ok /health" {
		t.Errorf("CONNECT %s: %q, body %q", uid, status, body)
	}
	if status, _ := connect("no-such-uid"); !strings.Contains(status, " 404 ") {
		t.Errorf("CONNECT of an unknown UID: %q, want 404", status)
	}

	// frpc pings every second. Had the plugin subscribed to Ping, the stub would have seen a call
	// in each of these seconds.
	time.Sleep(4 * time.Second)
	for _, op := range r.stub.ops() {
		if op == "Ping" {
			t.Fatalf("frps posted Ping to the plugin: %v", r.stub.ops())
		}
	}

	// A crashed job: the kernel closes frpc's connection, and the plugin hears CloseProxy.
	killFrpc()
	r.stub.waitOp(t, "CloseProxy", 15*time.Second)
	if got, want := r.stub.ops(), []string{"Login", "NewProxy", "CloseProxy"}; !reflect.DeepEqual(got, want) {
		t.Errorf("the plugin saw %v, want exactly %v", got, want)
	}
	for end := time.Now().Add(10 * time.Second); r.proxies()[uid] != "offline"; time.Sleep(50 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("frps lists %s as %q after frpc was killed, want offline (offline entries stay in the list)", uid, r.proxies()[uid])
		}
	}

	r.stop()
	if !gone(r.proc.Rec().PID) || len(procsMentioning(r.state)) != 0 {
		t.Errorf("processes left: pid %d gone=%v, mentioning the state dir: %v", r.proc.Rec().PID, gone(r.proc.Rec().PID), procsMentioning(r.state))
	}
}

// TestFrpsRealRefusesAConfigWithAnUnknownKey: strict parsing is on, so a mistyped key is an error
// and never a setting left at its default (a mistyped bindAddr would listen on every interface).
// frps's own verify command also accepts the rendered file; and StartFrps reports the exit.
func TestFrpsRealRefusesAConfigWithAnUnknownKey(t *testing.T) {
	frps, _ := testenv.FrpBinaries(t)
	h := agentapi.Host{StateDir: shortDir(t), PortBase: frpsRigBase}
	pw, _, err := EnsureFrpsWebPassword(h)
	if err != nil {
		t.Fatal(err)
	}
	good, err := WriteFrpsConfig(h, pw)
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command(frps, "verify", "-c", good).CombinedOutput(); err != nil {
		t.Fatalf("frps verify rejects the rendered file: %v\n%s", err, out)
	}
	b, _ := os.ReadFile(good)
	bad := filepath.Join(h.StateDir, "bad.toml")
	if err := os.WriteFile(bad, append([]byte("bindAdr = \"0.0.0.0\"\n"), b...), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command(frps, "verify", "-c", bad).CombinedOutput(); err == nil || !strings.Contains(string(out), "bindAdr") {
		t.Fatalf("frps verify accepted a mistyped key: %v\n%s", err, out)
	}

	bin, err := ResolveFrpsFile(frps, DefaultFrpVersion)
	if err != nil {
		t.Fatal(err)
	}
	defer bin.Close()
	p, err := StartFrps(FrpsSpec{Bin: bin, Config: bad, Log: FrpsLogFile(h), Dir: h.StateDir})
	switch {
	case err == nil:
		defer p.Release()
		select {
		case <-p.Done():
		case <-time.After(10 * time.Second):
			_, _ = p.Stop(context.Background(), time.Second)
			t.Fatal("frps started with a mistyped key")
		}
		if st := p.ExitStatus(); st != "exit status 1" {
			t.Errorf("exit status = %q, want exit status 1", st)
		}
		for end := time.Now().Add(5 * time.Second); !gone(p.Rec().PID) && time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
		}
		if !gone(p.Rec().PID) {
			t.Errorf("/proc/%d still exists after the exit", p.Rec().PID)
		}
		// The exited process is a stop that has nothing left to do.
		if rep, err := StopFrps(context.Background(), p.Rec(), time.Second); err != nil || rep.Before != "gone" {
			t.Errorf("StopFrps of an exited frps = %+v, %v", rep, err)
		}
	case errors.Is(err, errExited):
		// The invalid file can make frps exit before StartFrps has read its identity: that is
		// an error from the start, and it must have left nothing behind.
		t.Logf("frps exited before its identity was recorded: %v", err)
	default:
		t.Fatalf("StartFrps: %v", err)
	}
	// Either way: frps said which key it refused, in the log, and no process is left.
	for end := time.Now().Add(5 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		log, _ := os.ReadFile(FrpsLogFile(h))
		if strings.Contains(string(log), `unknown field "bindAdr"`) {
			break
		}
		if time.Now().After(end) {
			t.Fatalf("the log %s never named the refused key:\n%s", FrpsLogFile(h), log)
		}
	}
	for end := time.Now().Add(5 * time.Second); len(procsMentioning(h.StateDir)) != 0; time.Sleep(20 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("processes still mention the state directory: %v", procsMentioning(h.StateDir))
		}
	}
}

// TestStartFrpsSurvivesAnImmediateExit runs the early-exit race of the test above on purpose, with
// a program that exits at once: StartFrps either reports that the process had exited before its
// identity was recorded, or returns a process whose Done closes. Neither leaves a process behind.
func TestStartFrpsSurvivesAnImmediateExit(t *testing.T) {
	truePath, err := exec.LookPath("true")
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(truePath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	bin := FrpsBinary{Version: "test", Path: truePath, file: f}
	dir := shortDir(t)
	cfg := filepath.Join(dir, "marker-config.toml")
	if err := os.WriteFile(cfg, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	var early, late int
	for i := 0; i < 40; i++ {
		p, err := StartFrps(FrpsSpec{Bin: bin, Config: cfg, Log: filepath.Join(dir, "out.log"), Dir: dir})
		switch {
		case errors.Is(err, errExited):
			early++
		case err != nil:
			t.Fatalf("run %d: %v", i, err)
		default:
			late++
			select {
			case <-p.Done():
			case <-time.After(5 * time.Second):
				t.Fatalf("run %d: Done never closed", i)
			}
			if st := p.ExitStatus(); st != "exit status 0" {
				t.Errorf("run %d: exit status %q", i, st)
			}
			p.Release()
		}
	}
	t.Logf("exited before the identity was recorded %d times, after it %d times", early, late)
	for end := time.Now().Add(5 * time.Second); len(procsMentioning(cfg)) != 0; time.Sleep(20 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("processes left: %v", procsMentioning(cfg))
		}
	}
}

// TestFrpsRealRunsTheVerifiedDescriptorNotTheReplacedPath: the launcher hashes a sealed copy of the
// file it opened and runs that copy's descriptor. After the verification the path is made to name another executable (a
// new file, as an installer or an attacker would leave it); frps must still be the verified one,
// with and without nice(1) in front. Executing the path instead would start the impostor, which
// writes a marker and never answers.
func TestFrpsRealRunsTheVerifiedDescriptorNotTheReplacedPath(t *testing.T) {
	frps, _ := testenv.FrpBinaries(t)
	for _, nice := range []int{0, 10} {
		t.Run(fmt.Sprintf("nice=%d", nice), func(t *testing.T) {
			dir := shortDir(t)
			path := filepath.Join(dir, "frps")
			copyFile(t, frps, path, 0o755)
			marker := filepath.Join(dir, "impostor-ran")
			replaced := false
			r := newFrpsRigFrom(t, frpsRigBase, nice, path, func(bin FrpsBinary) {
				impostor := []byte("#!/bin/sh\ntouch " + marker + "\nexec sleep 300\n")
				tmp := path + ".new"
				if err := os.WriteFile(tmp, impostor, 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(tmp, path); err != nil {
					t.Fatal(err)
				}
				replaced = true
			})
			if !replaced {
				t.Fatal("the test never replaced the file")
			}
			if b, _ := os.ReadFile(path); !strings.HasPrefix(string(b), "#!/bin/sh") {
				t.Fatalf("the path does not name the impostor: %.20q", b)
			}
			pid := r.proc.Rec().PID
			if _, err := os.Stat(marker); !os.IsNotExist(err) {
				t.Errorf("the impostor ran (%v)", err)
			}
			// The process runs the sealed copy: its image is the bytes that were hashed.
			exe, err := os.Readlink("/proc/" + strconv.Itoa(pid) + "/exe")
			if err != nil || exe != frpsMemfdExe {
				t.Errorf("/proc/%d/exe = %q, %v, want %q", pid, exe, err, frpsMemfdExe)
			}
			img, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/exe")
			if err != nil {
				t.Fatal(err)
			}
			if sum := sha256.Sum256(img); hex.EncodeToString(sum[:]) != r.bin.SHA256 {
				t.Errorf("the image of pid %d has sha256 %x, not the verified %s", pid, sum, r.bin.SHA256)
			}
			r.stop()
		})
	}
}

// frpsMemfdExe is what /proc/<pid>/exe of an frps the launcher started names: the sealed memory
// file (sealedCopy) that holds the verified bytes, not the file on disk.
const frpsMemfdExe = "/memfd:frps (deleted)"

// TestResolveFrpsRealBinaryAgainstThePin: the executable in HPCBURST_FRP_DIR has the sum that
// frp-pins.txt records for this architecture, which was computed from the verified release asset.
func TestResolveFrpsRealBinaryAgainstThePin(t *testing.T) {
	frps, _ := testenv.FrpBinaries(t)
	b, err := ResolveFrpsFile(frps, DefaultFrpVersion)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	out, err := exec.Command(b.Path, "--version").Output()
	if err != nil || strings.TrimSpace(string(out)) != DefaultFrpVersion {
		t.Errorf("%s --version = %q, %v, want %s", b.Path, out, err, DefaultFrpVersion)
	}
	// ResolveFrps lays the binary out as <bin dir>/frp-<version>/frps.
	if dir := filepath.Dir(frps); filepath.Base(dir) == FrpDirName(DefaultFrpVersion) {
		got, err := ResolveFrps(filepath.Dir(dir), DefaultFrpVersion)
		if err != nil || got.Path != b.Path {
			t.Errorf("ResolveFrps = %+v, %v", got, err)
		}
		got.Close()
	}
}
