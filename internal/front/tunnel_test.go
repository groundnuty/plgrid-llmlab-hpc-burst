package front

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// These tests run the real frps and frpc (frprig_test.go). The front's
// Instance transport is the Tunnel: CONNECT <UID>:80 to frps's tcpmux port,
// registered by the front's probe (HBEP-6, The front's half).

const tunChat = `{"model":"example-27b","messages":[{"role":"user","content":"hello there"}]}`

// tunInstance adds a ready Instance of md on helios-a whose TunnelConnected
// condition is True, and returns its UID; its frpc is the test's to start.
func (f *tunFront) tunInstance(name, key string) types.UID {
	f.t.Helper()
	uid := f.instance("md", name, "helios-a", nil, key, instanceOpts{leaseEnd: time.Now().Add(48 * time.Hour)})
	in := f.inst(uid)
	in.Status.Conditions = []metav1.Condition{{Type: v1alpha1.ConditionTunnelConnected, Status: metav1.ConditionTrue,
		Reason: v1alpha1.ReasonProxyOnline, LastTransitionTime: metav1.Now()}}
	return uid
}

// tunDeployment adds the profile, the Provider and a served ModelDeployment
// of example-27b with n Instances desired.
func (f *tunFront) tunDeployment(n int32) {
	f.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
	f.batchProvider("helios-a", "helios-gh200")
	f.servedMD("md", "example-27b", n, "helios-a")
}

// routing is the router's view of an Instance: whether it is a member of its
// deployment's group, whether it is eligible now, and its in-flight count.
func (f *tunFront) routing(uid types.UID) (member, eligible bool, inFlight int64) {
	r := f.router
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.view == nil {
		return false, false, 0
	}
	for _, m := range r.view.Models {
		for _, d := range m.Served {
			for _, i := range d.Members {
				if i.UID != uid {
					continue
				}
				member, eligible = true, i.Eligible && !r.pausedLocked(i.Key, r.clk.Now())
				if g := r.groups[d.Key]; g != nil {
					inFlight, _ = g.InFlight(i.Key)
				}
			}
		}
	}
	return member, eligible, inFlight
}

// chat posts a chat request with a session key and returns its status, body
// and usage record.
func (f *tunFront) chat(session string) (int, string, string) {
	f.t.Helper()
	resp := f.post("/v1/chat/completions", tunChat, "X-Session-Id", session)
	id := resp.Header.Get("hpcburst-request-id")
	return resp.StatusCode, readAll(f.t, resp), id
}

// TestTunnelReachesEachEngineByItsUID: two Instances, each behind its own
// frpc and engine. Before a probe passes neither is routable; then every
// request through the front reaches the engine of the Instance it was routed
// to, by that Instance's UID. The engine double checks its own Instance key,
// so a request delivered to the other engine would get 401, not 200.
func TestTunnelReachesEachEngineByItsUID(t *testing.T) {
	rig := newFrpRig(t)
	f := newTunFront(t, rig)
	dial, _ := rig.serveDialHome(ModeSSH)
	f.tunDeployment(2)
	keyA, keyB := newKey(t), newKey(t)
	eA, sockA := rig.engine(keyA)
	eB, sockB := rig.engine(keyB)
	a, b := f.tunInstance("md-a", keyA), f.tunInstance("md-b", keyB)
	pa, pb := rig.startFrpc(a, sockA, dial), rig.startFrpc(b, sockB, dial)
	rig.waitOnline(a, pa)
	rig.waitOnline(b, pb)
	f.apply()

	// frps routes both, but the front has not probed them: not routable.
	if member, _, _ := f.routing(a); member {
		t.Fatal("an Instance whose probe never passed is routable")
	}
	if status, body, _ := f.chat("s0"); status != http.StatusServiceUnavailable || !strings.Contains(body, "no_ready_target") {
		t.Fatalf("before any probe: %d %s, want 503 no_ready_target", status, body)
	}
	f.waitRegistered(a)
	f.waitRegistered(b)

	engines := map[types.UID]*enginetest.Engine{a: eA, b: eB}
	keys := map[types.UID]string{a: keyA, b: keyB}
	routed := map[types.UID]int{}
	for i := 0; i < 40 && (routed[a] == 0 || routed[b] == 0); i++ {
		status, body, id := f.chat(fmt.Sprintf("session-%d", i))
		if status != http.StatusOK {
			t.Fatalf("request %d: %d %s", i, status, body)
		}
		routed[types.UID(f.record(id).InstanceUID)]++
	}
	if routed[a] == 0 || routed[b] == 0 {
		t.Fatalf("40 sessions all went to one Instance: %v", routed)
	}
	for uid, e := range engines {
		reqs := e.Requests()
		if len(reqs) != routed[uid] {
			t.Errorf("the engine of %s saw %d requests, and %d were routed to its Instance", uid, len(reqs), routed[uid])
		}
		for _, r := range reqs {
			if r.Header.Get("Authorization") != "Bearer "+keys[uid] || r.Path != "/v1/chat/completions" {
				t.Errorf("the engine of %s got %s with another key", uid, r.Path)
			}
		}
		if e.HealthChecks() == 0 {
			t.Errorf("the engine of %s saw no probe", uid)
		}
	}
}

// TestTunnelUnknownUIDFailsFastAsAConnectionFailure: frps answers 404 at once
// to a CONNECT for a UID it has no proxy for. Directly, the Tunnel's dial
// fails with frps's 404 well inside tunnelSetupTimeout. Through the front: B
// is registered, then its frpc dies (its FIN drops the proxy at once) before
// the next probe; a request routed to B first gets the 404, which counts
// toward B's pause and is resent once to A, which answers. B's engine sees
// nothing.
func TestTunnelUnknownUIDFailsFastAsAConnectionFailure(t *testing.T) {
	rig := newFrpRig(t)
	f := newTunFront(t, rig)
	dial, _ := rig.serveDialHome(ModeSSH)

	// errNoProxy comes only from frps's 404, never from the set-up deadline,
	// so the error itself shows the dial failed at once.
	_, err := f.tun.Dial(context.Background(), "00000000-0000-4000-8000-999999999999")
	if !errors.Is(err, errNoProxy) {
		t.Fatalf("CONNECT for an unknown UID: %v, want frps's 404", err)
	}

	f.tunDeployment(2)
	keyA, keyB := newKey(t), newKey(t)
	eA, sockA := rig.engine(keyA)
	eB, sockB := rig.engine(keyB)
	a, b := f.tunInstance("md-a", keyA), f.tunInstance("md-b", keyB)
	pa, pb := rig.startFrpc(a, sockA, dial), rig.startFrpc(b, sockB, dial)
	rig.waitOnline(a, pa)
	rig.waitOnline(b, pb)
	f.apply()
	f.waitRegistered(a)
	f.waitRegistered(b)

	pb.kill()
	for end := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		c, err := f.tun.Dial(context.Background(), b)
		if errors.Is(err, errNoProxy) {
			break
		}
		if c != nil {
			c.Close()
		}
		if time.Now().After(end) {
			t.Fatalf("frps still routes %s after its frpc was killed: %v", b, err)
		}
	}
	if !f.tun.Registered(b) {
		t.Fatal("B lost its registration with no probe run; the test needs it registered")
	}
	keyOfB := "ns/md-b"
	for i := 0; i < 40 && f.router.failures(keyOfB) == 0; i++ {
		status, body, id := f.chat(fmt.Sprintf("session-%d", i))
		if status != http.StatusOK {
			t.Fatalf("request %d: %d %s, want 200 from A after the one resend", i, status, body)
		}
		if f.router.failures(keyOfB) == 1 {
			if rec := f.record(id); rec.InstanceUID != string(a) {
				t.Errorf("the request that met B's 404 was answered by %q, want A", rec.InstanceUID)
			}
		}
	}
	// The failure logged is frps's 404, not the set-up deadline: a 404 fails
	// at once.
	if logs := f.logs.String(); !strings.Contains(logs, errNoProxy.Error()) || strings.Contains(logs, "no answer within tunnelSetupTimeout") {
		t.Errorf("the logged set-up failure is not frps's 404 alone:\n%s", logs)
	}
	if n := f.router.failures(keyOfB); n != 1 {
		t.Fatalf("B's failures = %d after its 404, want 1 (40 sessions never routed to B first?)", n)
	}
	if n := len(eB.Requests()); n != 0 {
		t.Errorf("B's engine saw %d requests", n)
	}
	if len(eA.Requests()) == 0 {
		t.Error("A's engine saw no request")
	}
}

// TestTunnelSetupDeadlineCutsAHungConnect: frps frozen with SIGSTOP still
// completes the TCP handshake from its listen backlog, so the dial succeeds
// and the CONNECT waits for an answer that does not come. The Tunnel cuts the
// set-up at tunnelSetupTimeout (5 s, the default). Through the front it is a
// connection failure: no byte of the request was written, so it is sent once
// more, to the same Instance on a fresh connection since there is no other
// (HBEP-5, open question 5), and after the second cut set-up the client gets
// 502 upstream_unreachable; each set-up counts toward the Instance's pause,
// and the engine sees nothing.
//
// A hung frps costs a request two 5 s set-ups by design, and both count
// toward the pause (@hpc-burst-science-agent, 1 Nov 2026). HBEP-5's open
// question 5 allows the resend "on a fresh connection to the same one", and
// frps is shared by every Instance, so a resend fails wherever it goes; the
// pause and the probe then take the Instance out of routing. Do not "fix" the
// second set-up away.
//
// The bounds below are a lower bound, which a deadline cannot beat, and
// generous watchdogs; that the cut comes from tunnelSetupTimeout itself is
// TestRunBoundsTheTunnelSetUpByTunnelSetupTimeout.
func TestTunnelSetupDeadlineCutsAHungConnect(t *testing.T) {
	rig := newFrpRig(t)
	f := newTunFront(t, rig)
	if got := f.cfg.TunnelSetupTimeout.D(); got != 5*time.Second {
		t.Fatalf("tunnelSetupTimeout = %s, want HBEP-5's 5 s", got)
	}
	dial, _ := rig.serveDialHome(ModeSSH)
	f.tunDeployment(1)
	key := newKey(t)
	e, sock := rig.engine(key)
	a := f.tunInstance("md-a", key)
	rig.waitOnline(a, rig.startFrpc(a, sock, dial))
	f.apply()
	f.waitRegistered(a)
	if status, body, _ := f.chat("warm"); status != http.StatusOK {
		t.Fatalf("before the freeze: %d %s", status, body)
	}
	before := len(e.Requests())
	// A pooled connection would carry the next request past frps's muxer.
	if tr, ok := f.proxy.inst.get(a); ok {
		tr.CloseIdleConnections()
	}

	rig.stop()
	defer rig.cont()
	// The positive control: the dial itself succeeds, and nothing answers.
	c, err := net.DialTimeout("tcp", rig.host.FrpsTCPMuxAddr(), time.Second)
	if err != nil {
		t.Fatalf("a dial to the frozen frps failed (%v): the test would show a refused dial, not a hung CONNECT", err)
	}
	fmt.Fprintf(c, "CONNECT %s:80 HTTP/1.1\r\nHost: %s:80\r\n\r\n", a, a)
	_ = c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if n, err := c.Read(make([]byte, 1)); n != 0 || err == nil {
		t.Fatalf("the frozen frps answered a CONNECT: %d bytes, %v", n, err)
	}
	c.Close()

	// The Tunnel itself, at the same time as the request below.
	type dialed struct {
		err  error
		took time.Duration
	}
	direct := make(chan dialed, 1)
	go func() {
		// Bounded, so a set-up with no deadline fails the test rather than
		// hanging it past its cleanups (which resume and stop frps).
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		start := time.Now()
		c, err := f.tun.Dial(ctx, a)
		if c != nil {
			c.Close()
		}
		direct <- dialed{err, time.Since(start)}
	}()

	start := time.Now()
	req, err := http.NewRequest(http.MethodPost, f.url+"/v1/chat/completions", strings.NewReader(tunChat))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+f.clientKey)
	var status int
	var body string
	if resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req); err != nil {
		t.Errorf("the request through a hung CONNECT: %v", err)
	} else {
		status, body = resp.StatusCode, readAll(t, resp)
	}
	took := time.Since(start)
	d := <-direct
	if d.err == nil || !strings.Contains(d.err.Error(), "tunnelSetupTimeout") {
		t.Errorf("the Tunnel's dial to a frozen frps: %v, want the set-up deadline", d.err)
	}
	if !errors.Is(d.err, os.ErrDeadlineExceeded) && !errors.Is(d.err, context.DeadlineExceeded) {
		t.Errorf("the Tunnel's dial to a frozen frps ended with %v, not the set-up deadline", d.err)
	}
	if d.took < 5*time.Second-50*time.Millisecond || d.took > 15*time.Second {
		t.Errorf("the hung CONNECT was cut after %s, want tunnelSetupTimeout (5 s)", d.took)
	}
	if status != http.StatusBadGateway || !strings.Contains(body, "upstream_unreachable") {
		t.Fatalf("through a hung CONNECT: %d %s, want 502 upstream_unreachable", status, body)
	}
	if took < 10*time.Second-100*time.Millisecond || took > 30*time.Second {
		t.Errorf("the request took %s, want two set-ups cut at 5 s each", took)
	}
	if n := f.router.failures("ns/md-a"); n != 2 {
		t.Errorf("failures = %d after two cut set-ups, want 2: each is a connection failure", n)
	}
	if n := strings.Count(f.logs.String(), "no connection to the target; nothing was sent"); n != 2 {
		t.Errorf("%d set-up failures logged, want 2", n)
	}
	rig.cont()
	if n := len(e.Requests()); n != before {
		t.Errorf("the engine saw %d new requests through a hung CONNECT", n-before)
	}
	if status, body, _ := f.chat("thawed"); status != http.StatusOK {
		t.Errorf("after frps resumed: %d %s", status, body)
	}
}

// TestTunnelFrozenPathClosesBeforeTheHeaders: with frpc frozen (SIGSTOP, no
// FIN), frps still answers the CONNECT, since the UID is routed, and then
// closes each request itself, within userConnTimeout (10 s) or at session
// teardown, before any response byte (HBEP-6, The front's half). A streamed
// and a non-streamed request each get 502 upstream_closed with detail tunnel
// and cutBy tunnel, each counts toward the pause, and neither is resent.
func TestTunnelFrozenPathClosesBeforeTheHeaders(t *testing.T) {
	rig := newFrpRig(t)
	f := newTunFront(t, rig)
	dial, _ := rig.serveDialHome(ModeSSH)
	f.tunDeployment(1)
	key := newKey(t)
	e, sock := rig.engine(key)
	a := f.tunInstance("md-a", key)
	p := rig.startFrpc(a, sock, dial)
	rig.waitOnline(a, p)
	f.apply()
	f.waitRegistered(a)
	p.pause()
	defer p.resume()

	bodies := map[string]string{
		"non-streamed": tunChat,
		"streamed":     `{"model":"example-27b","stream":true,"messages":[{"role":"user","content":"hello there"}]}`,
	}
	type result struct {
		name, body, id string
		status         int
		took           time.Duration
	}
	results := make(chan result, len(bodies))
	for name, body := range bodies {
		go func() {
			start := time.Now()
			resp := f.post("/v1/chat/completions", body, "X-Session-Id", name)
			results <- result{name: name, status: resp.StatusCode, body: readAll(t, resp), id: resp.Header.Get("hpcburst-request-id"), took: time.Since(start)}
		}()
	}
	for range bodies {
		r := <-results
		if r.status != http.StatusBadGateway || !strings.Contains(r.body, `"upstream_closed"`) || !strings.Contains(r.body, `"tunnel"`) {
			t.Errorf("%s through a frozen path: %d %s, want 502 upstream_closed, detail tunnel", r.name, r.status, r.body)
		}
		if r.took > 26*time.Second {
			t.Errorf("%s was closed after %s, want within userConnTimeout or the session's teardown", r.name, r.took)
		}
		if rec := f.record(r.id); rec.CutBy == nil || *rec.CutBy != cutTunnel {
			t.Errorf("%s: usage record cutBy %v, want tunnel", r.name, rec.CutBy)
		}
	}
	if n := f.router.failures("ns/md-a"); n != 2 {
		t.Errorf("failures = %d, want 2: each closed request counts toward the pause", n)
	}
	if n := strings.Count(f.logs.String(), "no connection to the target; nothing was sent"); n != 0 {
		t.Errorf("a written request was treated as never sent %d times, and resent", n)
	}
	// Every client has its 502 and frpc is still frozen: nothing reached the
	// engine. The count is read before the thaw, because a thawed frpc may
	// still hand the engine a request that frps had already written to it
	// (2 of 50 runs did): the client was told upstream_closed, and HBEP-5
	// does not resend a written request since it may yet run.
	if n := len(e.Requests()); n != 0 {
		t.Errorf("the engine saw %d requests through the frozen path before the thaw", n)
	}
	p.resume()
}
