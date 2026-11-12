package agent_test

// HBEP-5's Test plan, Beta, the router, Bytes: every valid S13 case reaches the engine through the
// router as its exact bytes, streamed and not. HBEP-7's Several workers makes this the pipe listener's
// first run "through the whole path (frpc, the agent's forwarder, Envoy, the worker)". So each of S13's
// variants goes as the front sends it: a CONNECT for the Instance's UID to a real frps's tcpmux port, then
// the request, through a real frpc serving the agent's serving socket, the agent's forwarder in router
// mode (as convergeRouter sets it), the Envoy the agent starts from its pin with its own config, to S13's
// recorder as worker-0, health-checked through the agent's readiness endpoint. The recorder keeps the
// exact bytes each request carried.
//
// The pieces are the agent's own (NewForwarder, RouteTo, StartEnvoy, NewReadiness, Frpc); the agent's
// converge loop, which wires them, is tested in router_agent_test.go. The recorder stands in for vLLM, as
// in S13.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/routerbytes/cases"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/routerbytes/rawjson"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/routerbytes/recorder"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

// s13Variants is S13's variant count, 298 on chat and 142 on completions (spikes/s13/README.md). The
// count is pinned so that a shrunken case list cannot pass by sending fewer.
const s13Variants = 440

// allVariants is every S13 variant, valid and invalid, streamed and not.
func allVariants(t *testing.T) []cases.Variant {
	t.Helper()
	var vs []cases.Variant
	for _, c := range cases.All() {
		vs = append(vs, c.Variants()...)
	}
	if len(vs) != s13Variants {
		t.Fatalf("S13's cases give %d variants, want %d", len(vs), s13Variants)
	}
	return vs
}

// startRecorder serves S13's recorder on the Unix socket at path, as worker-0.
func startRecorder(t *testing.T, path string) *recorder.Recorder {
	t.Helper()
	rec := &recorder.Recorder{}
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: rec.Handler("worker-0"), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return rec
}

// rawRequest is one variant's request, as the front sends it on: its exact body, the Instance key and
// the variant's key in S13's header, and no encoding asked for.
func rawRequest(v cases.Variant) string {
	return fmt.Sprintf("POST %s HTTP/1.1\r\nHost: instance\r\nContent-Type: application/json\r\nAuthorization: Bearer %s\r\n%s: %s\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s",
		v.Case.Endpoint, strings.Repeat("0", 64), recorder.CaseHeader, v.Key(), len(v.Body), v.Body)
}

// sendRaw writes req on c and reads the reply to its end; it returns the status.
func sendRaw(c net.Conn, br *bufio.Reader, req string) (int, error) {
	_ = c.SetDeadline(time.Now().Add(30 * time.Second))
	if _, err := io.WriteString(c, req); err != nil {
		return 0, err
	}
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, err = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, err
}

// sendThroughFrps sends req as the front does: CONNECT <uid>:80 to frps's tcpmux port, then req.
func sendThroughFrps(muxPort int, uid, req string) (int, error) {
	c, err := net.DialTimeout("tcp4", "127.0.0.1:"+strconv.Itoa(muxPort), 2*time.Second)
	if err != nil {
		return 0, err
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(30 * time.Second))
	fmt.Fprintf(c, "CONNECT %s:80 HTTP/1.1\r\nHost: %s:80\r\n\r\n", uid, uid)
	br := bufio.NewReader(c)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodConnect})
	if err != nil {
		return 0, fmt.Errorf("CONNECT: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("CONNECT: %d", resp.StatusCode)
	}
	return sendRaw(c, br, req)
}

// sendToSocket sends req straight to a Unix socket.
func sendToSocket(path, req string) (int, error) {
	c, err := net.DialTimeout("unix", path, 2*time.Second)
	if err != nil {
		return 0, err
	}
	defer c.Close()
	return sendRaw(c, bufio.NewReader(c), req)
}

// bytesProblems sends every variant with send and returns what the recorder saw wrong: a variant that
// did not arrive exactly once, arrived with other bytes (rawjson's first difference says where), or,
// when viaEnvoy, without Envoy's x-request-id. It also returns the number that arrived exact.
func bytesProblems(t *testing.T, rec *recorder.Recorder, vs []cases.Variant, viaEnvoy bool, send func(req string) (int, error)) (exact int, problems []string) {
	t.Helper()
	for _, v := range vs {
		if _, err := send(rawRequest(v)); err != nil {
			problems = append(problems, fmt.Sprintf("%s: %v", v.Key(), err))
			continue
		}
		got := rec.ByCase(v.Key())
		switch {
		case len(got) != 1:
			problems = append(problems, fmt.Sprintf("%s: arrived %d times, want once", v.Key(), len(got)))
		case !bytes.Equal(got[0].Body, v.Body):
			why := "the bytes differ"
			if sent, err := rawjson.Parse(v.Body); err == nil {
				if recv, err := rawjson.Parse(got[0].Body); err == nil {
					if ds := rawjson.Compare(sent, recv); len(ds) > 0 {
						why = fmt.Sprintf("%+v", ds[0])
					}
				}
			}
			problems = append(problems, fmt.Sprintf("%s: arrived changed: %s", v.Key(), why))
		case viaEnvoy && got[0].Header.Get("X-Request-Id") == "":
			problems = append(problems, fmt.Sprintf("%s: arrived without Envoy's x-request-id: it bypassed the router", v.Key()))
		default:
			exact++
		}
	}
	return exact, problems
}

// bytesRig is frps, the dial-home terminator and frpc in front of the agent's serving socket, routed to
// the agent-launched Envoy in front of S13's recorder.
type bytesRig struct {
	muxPort int
	rec     *recorder.Recorder
	runDir  string
	envoy   *agent.EnvoyProc
}

func newBytesRig(t *testing.T) *bytesRig {
	t.Helper()
	envoyBin := testenv.EnvoyBinary(t)
	frpsBin, frpcBin := frpBins(t)
	w := newProcWatch()
	t.Cleanup(func() { w.assertGone(t) })
	dir := shortTemp(t)
	runDir := filepath.Join(dir, "run")
	if err := os.Mkdir(runDir, 0o700); err != nil {
		t.Fatal(err)
	}
	r := &bytesRig{runDir: runDir}

	// The worker, then its readiness, then Envoy, so Envoy's first check finds the worker verified.
	worker := filepath.Join(runDir, "worker-0.sock")
	r.rec = startRecorder(t, worker)
	rd, err := agent.NewReadiness()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rd.Close() })
	rd.Set(map[string]agent.ReadyWorker{"worker-0": {Socket: worker, Verified: true}})
	p, err := agent.StartEnvoy(agent.EnvoyStart{Binary: envoyBin, Ready: 30 * time.Second,
		Config: agent.EnvoyConfig{RunDir: runDir, Workers: []agent.EnvoyWorker{{Name: "worker-0", Socket: worker}}, ReadinessPort: rd.Port()}}, time.Now())
	if err != nil {
		t.Fatalf("StartEnvoy: %v", err)
	}
	t.Cleanup(func() { p.Group.Stop(5 * time.Second); <-p.Group.Done(); p.Close() })
	r.envoy = p
	(&router{dir: runDir, proc: p}).waitWorker(t, "healthy", 30*time.Second)

	// The agent's serving socket, routed to Envoy, as convergeRouter sets it.
	fwd, err := agent.NewForwarder(agent.ServingSocket(runDir))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { fwd.Close() })
	fwd.RouteTo(filepath.Join(runDir, agent.EnvoyListenerSocket))

	// The tunnel: frps behind the dial-home terminator, and frpc serving the serving socket.
	frps := startFrps(t, frpsBin, dir, w)
	r.muxPort = frps.muxPort
	cert := makeCert(t, launcherName)
	term := startTerminator(t, cert.tls, "127.0.0.1:"+strconv.Itoa(frps.bindPort))
	staged := writeFile(t, filepath.Join(dir, "dial-home.crt"), cert.pem)
	server, err := agent.DirectServer("127.0.0.1:" + strconv.Itoa(term.port()))
	if err != nil {
		t.Fatal(err)
	}
	fc := newFrpc(t, frpcBin, runDir, staged, cert.pin, server, w)
	logs := &logBuf{}
	fc.Logger = slog.New(slog.NewTextHandler(logs, nil))
	t.Cleanup(func() {
		if t.Failed() {
			b, _ := os.ReadFile(fc.LogPath())
			t.Logf("frpc's agent log:\n%s\nfrpc log:\n%s", logs.String(), b)
		}
	})
	fc.Serve(hInstUID)
	runBackground(t, "frpc", fc.Run)
	eventually(t, 15*time.Second, "frps to list the Instance's proxy online", func() bool { return frps.online(t, hInstUID) })
	// The route is proven by one request that reaches the recorder through every hop.
	frps.waitRoute(t, hInstUID, "/health", "")
	return r
}

func TestEveryS13VariantCrossesTheRouterAsItsBytes(t *testing.T) {
	vs := allVariants(t)
	r := newBytesRig(t)
	exact, problems := bytesProblems(t, r.rec, vs, true, func(req string) (int, error) { return sendThroughFrps(r.muxPort, hInstUID, req) })
	for i, p := range problems {
		if i == 10 {
			t.Errorf("... and %d more", len(problems)-10)
			break
		}
		t.Error(p)
	}
	if exact != s13Variants {
		t.Errorf("%d of %d variants reached the worker as their bytes through frps, frpc, the forwarder and Envoy", exact, s13Variants)
	}
	t.Logf("%d of %d variants reached the worker as their exact bytes, each through Envoy", exact, len(vs))
	// What carried them is the pinned Envoy with the agent's config.
	cmd, err := os.ReadFile("/proc/" + strconv.Itoa(r.envoy.Group.PID()) + "/cmdline")
	if err != nil || !strings.Contains(string(cmd), filepath.Join(r.runDir, agent.EnvoyConfigFile)) {
		t.Errorf("Envoy's command line %q (%v) does not name the agent's config", strings.ReplaceAll(string(cmd), "\x00", " "), err)
	}
}

// The direct control: every variant sent straight to the recorder arrives as its bytes, so the recorder
// and the comparison keep the bytes; and none carries Envoy's x-request-id, which is what the router
// test's check for it tells apart.
func TestS13VariantsReachTheRecorderDirectlyAsTheirBytes(t *testing.T) {
	vs := allVariants(t)
	dir := shortTemp(t)
	sock := filepath.Join(dir, "worker-0.sock")
	rec := startRecorder(t, sock)
	exact, problems := bytesProblems(t, rec, vs, false, func(req string) (int, error) { return sendToSocket(sock, req) })
	if exact != s13Variants || len(problems) != 0 {
		t.Errorf("straight to the recorder: %d of %d exact, problems %q", exact, s13Variants, problems)
	}
	for _, a := range rec.All() {
		if a.Header.Get("X-Request-Id") != "" {
			t.Errorf("a direct request carries x-request-id %q", a.Header.Get("X-Request-Id"))
			break
		}
	}
}

// The positive control of the byte check: a proxy that decodes and re-encodes each body, as a router
// that re-marshals would, changes variants, and the check reports them.
func TestTheBytesCheckSeesARemarshallingProxy(t *testing.T) {
	vs := allVariants(t)
	dir := shortTemp(t)
	sock := filepath.Join(dir, "worker-0.sock")
	rec := startRecorder(t, sock)
	upstream := &http.Client{Transport: &http.Transport{DialContext: unixDialer(sock), DisableCompression: true}}
	proxy := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var v any
		body, _ := io.ReadAll(r.Body)
		out := body
		if json.Unmarshal(body, &v) == nil {
			out, _ = json.Marshal(v)
		}
		req, _ := http.NewRequest(r.Method, "http://engine"+r.URL.Path, bytes.NewReader(out))
		req.Header = r.Header.Clone()
		resp, err := upstream.Do(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
	ln, err := net.Listen("unix", filepath.Join(dir, "proxy.sock"))
	if err != nil {
		t.Fatal(err)
	}
	proxy.Listener = ln
	proxy.Start()
	defer proxy.Close()
	exact, problems := bytesProblems(t, rec, vs, false, func(req string) (int, error) { return sendToSocket(filepath.Join(dir, "proxy.sock"), req) })
	if exact == s13Variants || len(problems) == 0 {
		t.Errorf("a re-marshalling proxy passed the byte check: %d of %d exact", exact, s13Variants)
	}
	t.Logf("through a re-marshalling proxy, %d of %d variants arrived exact; first problem: %.200s", exact, s13Variants, firstProblem(problems))
}

// firstProblem is the first of ps, or "" when there is none.
func firstProblem(ps []string) string {
	if len(ps) == 0 {
		return ""
	}
	return ps[0]
}

// unixDialer dials the Unix socket at path, whatever address a client names.
func unixDialer(path string) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", path)
	}
}
