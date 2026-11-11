package front

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/testenv"
)

// The front's re-probe after frpc registers again (#371), over the real frps and frpc: frps runs
// the launcher's configuration with the front's plugin (launcher.WriteFrpsConfigWithFront), and
// the plugin is the front's own handler (frpsPlugin) on a slot the test opens. The bookkeeping on
// its own is reregister_test.go.

// frontPluginFixedAddr is where TestFrpsRefusesNewProxiesWhileTheFrontsPluginIsDown puts the
// front's plugin, which must be known before anything listens there: a port of #371's block,
// 30050-30069 (IMPL-COMMON). The other tests take theirs from the kernel.
const frontPluginFixedAddr = "127.0.0.1:30050"

// frontPluginSlot serves the front's frps plugin for a rig. frps is configured with its address
// before the front exists, so it answers 503 (frps then refuses the proxy) until set gives it the
// front's handler. only, when set, is the one op passed to the handler; any other is accepted
// unheard, as if frps had not posted it. hook, when set, runs before the handler for each call it
// passes.
type frontPluginSlot struct {
	addr string
	srv  *http.Server

	mu   sync.Mutex
	h    http.Handler
	only string
	hook func(op string)
}

func listenFrontPlugin(t *testing.T, addr string) *frontPluginSlot {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("the front's plugin slot cannot listen on %s: %v", addr, err)
	}
	s := &frontPluginSlot{addr: ln.Addr().String()}
	s.srv = &http.Server{Handler: s, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = s.srv.Serve(ln) }()
	t.Cleanup(func() { _ = s.srv.Close() })
	return s
}

func (s *frontPluginSlot) set(h http.Handler) {
	s.mu.Lock()
	s.h = h
	s.mu.Unlock()
}

func (s *frontPluginSlot) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, pluginBodyLimit))
	var req pluginRequest
	_ = json.Unmarshal(body, &req)
	s.mu.Lock()
	h, only, hook := s.h, s.only, s.hook
	s.mu.Unlock()
	switch {
	case h == nil:
		http.Error(w, "the front is not up", http.StatusServiceUnavailable)
		return
	case only != "" && req.Op != only:
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"reject":false,"unchange":true}`)
		return
	}
	if hook != nil {
		hook(req.Op)
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	h.ServeHTTP(w, r)
}

// newPluggedTunFront is a tunFront over a rig whose frps posts to the front's plugin, served by
// the front's own handler; edit, when not nil, rewrites frps's configuration.
func newPluggedTunFront(t *testing.T, edit func([]byte) []byte) (*tunFront, *frontPluginSlot) {
	t.Helper()
	slot := listenFrontPlugin(t, "127.0.0.1:0")
	rig := newFrpRigWith(t, frpRigOptions{frontPlugin: slot.addr, edit: edit})
	f := newTunFront(t, rig)
	slot.set(&frpsPlugin{tun: f.tun, prober: f.prober, log: f.log})
	return f, slot
}

// waitUnregistered waits until the front no longer routes to the UID.
func (f *tunFront) waitUnregistered(uid types.UID, why string) {
	f.t.Helper()
	for end := time.Now().Add(20 * time.Second); f.tun.Registered(uid); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(end) {
			f.t.Fatalf("the front still routes to %s %s\n%s", uid, why, f.logs.String())
		}
	}
}

// waitReprobed runs the re-probe schedule as Run does between rounds until the UID is registered,
// and a round once the UID has left the schedule.
func (f *tunFront) waitReprobed(uid types.UID) {
	f.t.Helper()
	for end := time.Now().Add(20 * time.Second); !f.tun.Registered(uid); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(end) {
			f.t.Fatalf("no probe registered %s\n%s", uid, f.logs.String())
		}
		f.prober.Reprobe(context.Background())
		if !f.tun.Registered(uid) && !f.prober.isAwaiting(uid) {
			f.prober.Round(context.Background())
		}
	}
}

// isAwaiting reports whether the UID is on the re-probe schedule.
func (p *Prober) isAwaiting(uid types.UID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.awaiting[uid] != nil
}

// TestFrontRoutesNothingToAUIDRegisteredAgainUntilAProbePasses: an Instance's frpc dies while its
// stream is idle, and a new frpc registers its UID again. From the new registration on, the front
// routes nothing to the UID, and refuses a request cleanly (503 no_ready_target, nothing reaches
// the engine), until a probe has passed through the new registration; then the request crosses.
// Once with both of frps's calls heard, and once with only the NewProxy, as when the old session's
// CloseProxy is lost (frps's post of it is a goroutine of its own, server/control.go:667-681): the
// new registration alone takes the route away.
func TestFrontRoutesNothingToAUIDRegisteredAgainUntilAProbePasses(t *testing.T) {
	for _, onlyNewProxy := range []bool{false, true} {
		name := "both calls heard"
		if onlyNewProxy {
			name = "only the NewProxy heard"
		}
		t.Run(name, func(t *testing.T) {
			f, slot := newPluggedTunFront(t, nil)
			if onlyNewProxy {
				slot.mu.Lock()
				slot.only = opNewProxy
				slot.mu.Unlock()
			}
			dial, _ := f.rig.serveDialHome(ModeSSH)
			f.tunDeployment(1)
			key := newKey(t)
			e, sock := f.rig.engine(key)
			a := f.tunInstance("md-a", key)
			p1 := f.rig.startFrpc(a, sock, dial)
			f.rig.waitOnline(a, p1)
			f.apply()
			f.waitRegistered(a)
			if status, body, _ := f.chat("s0"); status != http.StatusOK {
				t.Fatalf("before the reconnect: %d %s", status, body)
			}

			p1.kill()
			if !onlyNewProxy {
				f.waitUnregistered(a, "after frps closed its proxy")
			} else {
				// The close is unheard: the stale registration stays until the next NewProxy.
				for end := time.Now().Add(20 * time.Second); f.rig.proxyStatus(string(a)) == "online"; time.Sleep(20 * time.Millisecond) {
					if time.Now().After(end) {
						t.Fatal("frps never dropped the killed frpc's proxy")
					}
				}
				if !f.tun.Registered(a) {
					t.Fatal("the front lost the route although it heard no call")
				}
			}
			p2 := f.rig.startFrpc(a, sock, dial)
			f.rig.waitOnline(a, p2)
			if f.tun.Registered(a) {
				t.Fatal("the front routes to a UID that frps registered again, before any probe of the new registration")
			}
			before := len(e.Requests())
			if status, body, _ := f.chat("s1"); status != http.StatusServiceUnavailable || !strings.Contains(body, "no_ready_target") {
				t.Fatalf("after the new registration, before a probe: %d %s, want 503 no_ready_target", status, body)
			}
			if n := len(e.Requests()) - before; n != 0 {
				t.Fatalf("the engine saw %d requests before the new registration was probed", n)
			}
			f.waitReprobed(a)
			if status, body, _ := f.chat("s2"); status != http.StatusOK {
				t.Fatalf("after the re-probe passed: %d %s", status, body)
			}
		})
	}
}

// TestFrontRefusesARouteThroughARegistrationFrpcHasNotStarted is #368's race on the front's own
// path, made certain: a dialer the router held from before a reconnect is used while frps already
// routes the UID's new registration and frpc has not started it yet. A relay between the front's
// dial-home passthrough and frps (testenv.HoldRelay) holds what frps sends on the new frpc's
// control connection from the front's own NewProxy call, so frpc never reads its NewProxyResp
// (#369's reproduction). An unchecked dial there gets 200 and then the end of the connection, and
// a request written into it is lost (the positive control). The front's dialer instead finds after
// frps's 200 that the epoch has moved, and refuses the connection before any byte of a request: a
// dial error, resent once (HBEP-5). Through the front the request is refused cleanly, since the UID
// is not routable. After the release the re-probe registers the UID, and requests cross.
//
// frps and frpc run with transport.tcpMux off, so that each work connection is a wss connection of
// its own and the relay sees the control connection apart (frp's client/connector.go:165-182); what
// frpc does with its messages is the same either way.
func TestFrontRefusesARouteThroughARegistrationFrpcHasNotStarted(t *testing.T) {
	noMux := func(cfg []byte) []byte { return append([]byte("transport.tcpMux = false\n"), cfg...) }
	f, slot := newPluggedTunFront(t, noMux)
	f.tunDeployment(1)
	key := newKey(t)
	e, sock := f.rig.engine(key)
	a := f.tunInstance("md-a", key)
	relay1 := testenv.StartHoldRelay(t, f.rig.host.FrpsBindAddr())
	dial1 := f.rig.serveDialHomeTo(ModeSSH, "127.0.0.1:"+strconv.Itoa(relay1.Port()))
	p1 := f.rig.startFrpcEdited(a, sock, dial1, noMux)
	f.rig.waitOnline(a, p1)
	f.apply()
	f.waitRegistered(a)
	routed, ok := f.tun.Dialer(a) // the router's dialer, from before the reconnect
	if !ok {
		t.Fatal("no dialer for the registered UID")
	}

	relay2 := testenv.StartHoldRelay(t, f.rig.host.FrpsBindAddr())
	dial2 := f.rig.serveDialHomeTo(ModeSSH, "127.0.0.1:"+strconv.Itoa(relay2.Port()))
	armed := make(chan error, 1)
	var once sync.Once
	slot.mu.Lock()
	slot.hook = func(op string) {
		if op != opNewProxy {
			return
		}
		once.Do(func() {
			// Hold only once the new frpc has dialled its spare work connection: frps asks for it at
			// the login, on the control connection, and a held request would leave the CONNECT
			// waiting for frps's userConnTimeout instead (#369).
			for end := time.Now().Add(10 * time.Second); relay2.Accepted() < 2; time.Sleep(10 * time.Millisecond) {
				if time.Now().After(end) {
					armed <- errors.New("the new frpc never dialled its spare work connection")
					return
				}
			}
			relay2.Hold()
			armed <- nil
		})
	}
	slot.mu.Unlock()
	p1.kill()
	p2 := f.rig.startFrpcEdited(a, sock, dial2, noMux)
	select {
	case err := <-armed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("frps never posted the new registration to the front's plugin")
	}
	f.rig.waitOnline(a, p2)
	if f.tun.Registered(a) {
		t.Fatal("the front routes to the UID while frpc has not started its new registration")
	}
	before := len(e.Requests())

	// The positive control: a plain CONNECT now gets frps's 200, then the end of the connection
	// before any reply, since frpc closes the spare it is handed.
	status, _, _, err := testenv.TCPMuxGet(f.rig.host.FrpsTCPMuxAddr(), string(a), "/health", time.Now().Add(5*time.Second))
	if !errors.Is(err, testenv.ErrClosedBeforeReply) || !strings.Contains(status, " 200 ") {
		t.Fatalf("a plain CONNECT while frps's answer to frpc is held: %q, %v; want 200, then the end of the connection (#368)", status, err)
	}

	// The router's dialer from before refuses the connection after the 200, before any request byte.
	if c, err := routed(context.Background()); err == nil {
		c.Close()
		t.Fatal("the dialer from before the reconnect connected through the registration frpc has not started")
	} else if !errors.Is(err, errReregistered) {
		t.Fatalf("the dialer from before the reconnect: %v, want errReregistered", err)
	}
	if status, body, _ := f.chat("s1"); status != http.StatusServiceUnavailable || !strings.Contains(body, "no_ready_target") {
		t.Fatalf("a request while the new registration is unproven: %d %s, want 503 no_ready_target", status, body)
	}
	if n := len(e.Requests()) - before; n != 0 {
		t.Fatalf("the engine saw %d requests through a registration frpc had not started", n)
	}

	relay2.Release()
	f.waitReprobed(a)
	if status, body, _ := f.chat("s2"); status != http.StatusOK {
		t.Fatalf("after the release and the re-probe: %d %s", status, body)
	}
}

// TestFrpsRefusesNewProxiesWhileTheFrontsPluginIsDownThenFrpcRegistersAlone: frps posts every
// admitted NewProxy to the front's plugin and stops at a plugin that does not answer (frp v0.71.1,
// pkg/plugin/server/manager.go:90-95), so while the front's plugin is down no proxy registers
// (HBEP-6, Admission: fail closed). frpc retries on its own (its start-error wait, 30 s,
// client/proxy/proxy_wrapper.go), and once the plugin answers the proxy registers and its probe
// passes, with nothing restarted.
func TestFrpsRefusesNewProxiesWhileTheFrontsPluginIsDownThenFrpcRegistersAlone(t *testing.T) {
	rig := newFrpRigWith(t, frpRigOptions{frontPlugin: frontPluginFixedAddr})
	f := newTunFront(t, rig)
	dial, _ := rig.serveDialHome(ModeSSH)
	f.tunDeployment(1)
	key := newKey(t)
	_, sock := rig.engine(key)
	a := f.tunInstance("md-a", key)
	p := rig.startFrpc(a, sock, dial)
	f.apply()
	for end := time.Now().Add(30 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		b, _ := os.ReadFile(p.log)
		if strings.Contains(string(b), "start error") {
			break
		}
		if time.Now().After(end) {
			t.Fatalf("frpc never reported its proxy refused while the front's plugin was down:\n%s\nfrps log:\n%s", b, rig.logTail())
		}
	}
	if s := rig.proxyStatus(string(a)); s == "online" {
		t.Fatal("frps registered a proxy while the front's plugin was down")
	}
	if f.tun.Registered(a) {
		t.Fatal("the front routes to a UID frps never registered")
	}

	slot := listenFrontPlugin(t, frontPluginFixedAddr)
	slot.set(&frpsPlugin{tun: f.tun, prober: f.prober, log: f.log})
	for end := time.Now().Add(60 * time.Second); rig.proxyStatus(string(a)) != "online"; time.Sleep(100 * time.Millisecond) {
		if time.Now().After(end) {
			b, _ := os.ReadFile(p.log)
			t.Fatalf("frpc never registered once the front's plugin answered:\n%s\nfrps log:\n%s", b, rig.logTail())
		}
	}
	f.waitReprobed(a)
	if status, body, _ := f.chat("s0"); status != http.StatusOK {
		t.Fatalf("after frpc registered on its own: %d %s", status, body)
	}
}

// TestFrpsRestartTakesEveryRouteAwayUntilEachProbePasses: frps restarts (the supervisor's restart
// of one that exited), every frpc logs in again, and each NewProxy takes its UID's route away. Once
// both proxies are registered again neither is routable, and each becomes so only after a probe of
// its own passed through its new registration: B's engine fails its probes for a while, and A is
// routable again while B is not. What happens between frps's exit and the new logins is not this
// test's: it asserts only after both NewProxy calls.
func TestFrpsRestartTakesEveryRouteAwayUntilEachProbePasses(t *testing.T) {
	f, _ := newPluggedTunFront(t, nil)
	dial, _ := f.rig.serveDialHome(ModeSSH)
	f.tunDeployment(2)
	keyA, keyB := newKey(t), newKey(t)
	_, sockA := f.rig.engine(keyA)
	eB, sockB := f.rig.engine(keyB)
	a, b := f.tunInstance("md-a", keyA), f.tunInstance("md-b", keyB)
	pa, pb := f.rig.startFrpc(a, sockA, dial), f.rig.startFrpc(b, sockB, dial)
	f.rig.waitOnline(a, pa)
	f.rig.waitOnline(b, pb)
	f.apply()
	f.waitRegistered(a)
	f.waitRegistered(b)
	epochs := map[types.UID]uint64{a: f.tun.Epoch(a), b: f.tun.Epoch(b)}

	eB.Set(enginetest.Behaviour{Health: http.StatusServiceUnavailable})
	f.rig.restartFrps()
	for _, u := range []types.UID{a, b} {
		for end := time.Now().Add(40 * time.Second); f.rig.proxyStatus(string(u)) != "online" || f.tun.Epoch(u) == epochs[u]; time.Sleep(50 * time.Millisecond) {
			if time.Now().After(end) {
				t.Fatalf("%s did not register with the restarted frps (status %q, epoch %d, was %d)\n%s", u, f.rig.proxyStatus(string(u)), f.tun.Epoch(u), epochs[u], f.rig.logTail())
			}
		}
	}
	// Nothing has probed since (the test drives the prober), so neither is routable.
	for _, u := range []types.UID{a, b} {
		if f.tun.Registered(u) {
			t.Fatalf("%s is routable after frps's restart, with no probe of its new registration", u)
		}
	}
	if status, body, _ := f.chat("s0"); status != http.StatusServiceUnavailable || !strings.Contains(body, "no_ready_target") {
		t.Fatalf("after frps's restart, before any probe: %d %s, want 503 no_ready_target", status, body)
	}
	f.waitReprobed(a)
	if f.tun.Registered(b) {
		t.Fatal("B is routable although its probes fail")
	}
	eB.Set(enginetest.Behaviour{})
	f.waitReprobed(b)
	if status, body, _ := f.chat("s1"); status != http.StatusOK {
		t.Fatalf("after both re-probes: %d %s", status, body)
	}
}
