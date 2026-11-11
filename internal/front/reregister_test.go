package front

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The registration epoch of #371 on its own, over the tcpmux double: what a
// new registration does to a dialer handed out before it, to a probe that
// passed through the registration it replaced, and to the re-probe schedule;
// and the front's frps plugin. Over the real frps and frpc it is
// TestFrontRefusesARouteThroughARegistrationFrpcHasNotStarted and its
// neighbours (reregister_frp_test.go).

// TestDialerHandedOutBeforeANewRegistrationSendsNothing: a dialer the router
// obtained while the UID was registered, used after frps announced a new
// registration (Bump), gets frps's 200 and then refuses the connection, so no
// byte of a request reaches the tunnel (HBEP-5's resend-once dial error). One
// obtained after a probe passed in the new epoch dials as before.
func TestDialerHandedOutBeforeANewRegistrationSendsNothing(t *testing.T) {
	m := startMuxDouble(t)
	const uid = types.UID("00000000-0000-4000-8000-000000000031")
	m.set(uid, http.StatusOK)
	tun := NewTunnel(m.ln.Addr().String(), 5*time.Second)
	tun.setRegistered(uid, true)
	d, ok := tun.Dialer(uid)
	if !ok {
		t.Fatal("no dialer for a registered UID")
	}
	tun.Bump(uid)
	if _, ok := tun.Dialer(uid); ok {
		t.Fatal("a UID is still routable after a new registration was announced")
	}
	c, err := d(context.Background())
	if err == nil {
		c.Close()
		t.Fatal("the dialer handed out before the new registration returned a connection")
	}
	if !errors.Is(err, errReregistered) {
		t.Fatalf("dial error %v, want errReregistered", err)
	}
	if n := m.hit(uid); n != 0 {
		t.Fatalf("the tunnel received %d requests through the refused connection", n)
	}

	// The control: a pass in the new epoch registers it, and its dialer works.
	if _, applied := tun.setRegisteredAt(uid, true, pointerTo(tun.Epoch(uid))); !applied {
		t.Fatal("a pass in the current epoch did not register the UID")
	}
	d, ok = tun.Dialer(uid)
	if !ok {
		t.Fatal("no dialer after the pass in the new epoch")
	}
	c, err = d(context.Background())
	if err != nil {
		t.Fatalf("dial in the new epoch: %v", err)
	}
	c.Close()
}

// TestBumpCallsTheSubscribersWithoutWaitingForThem: Bump returns while a
// subscriber is still stuck (the router's rebuild, say), because frps waits
// for the plugin's answer on a session's control connection.
func TestBumpCallsTheSubscribersWithoutWaitingForThem(t *testing.T) {
	tun := NewTunnel("127.0.0.1:1", time.Second)
	const uid = types.UID("00000000-0000-4000-8000-000000000032")
	tun.setRegistered(uid, true)
	stuck, called := make(chan struct{}), make(chan struct{}, 1)
	defer close(stuck)
	tun.Subscribe(func() { called <- struct{}{}; <-stuck })
	returned := make(chan struct{})
	go func() { tun.Bump(uid); close(returned) }()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("Bump waited for a stuck subscriber")
	}
	select {
	case <-called:
	case <-time.After(5 * time.Second):
		t.Fatal("Bump of a registered UID never called the subscriber")
	}
	if tun.Registered(uid) {
		t.Fatal("the UID is still registered after Bump")
	}
}

// TestProbePassThroughAReplacedRegistrationDoesNotCount: a probe whose GET
// /health was answered after a new registration was announced passed through
// the registration that one replaces, so it registers nothing and the UID goes
// on the re-probe schedule; the re-probe that follows registers it.
func TestProbePassThroughAReplacedRegistrationDoesNotCount(t *testing.T) {
	m := startMuxDouble(t)
	var mu sync.Mutex
	insts := []v1alpha1.Instance{probeInstance("00000000-0000-4000-8000-000000000033", v1alpha1.InstanceReady, metav1.ConditionTrue)}
	uid := insts[0].UID
	m.set(uid, http.StatusOK)
	p, tun, _ := newTestProber(t, m, clock.NewFake(t0), &insts, &mu)

	held, release := m.holdNext(t, uid, "health")
	done := make(chan struct{})
	go func() { p.Round(context.Background()); close(done) }()
	select {
	case <-held:
	case <-time.After(5 * time.Second):
		t.Fatal("the probe never reached GET /health")
	}
	tun.Bump(uid)
	p.Reregistered(uid)
	release()
	<-done
	if tun.Registered(uid) {
		t.Fatal("a pass through the replaced registration registered the UID")
	}
	if _, ok := p.reprobeAt(); !ok {
		t.Fatal("the UID is not on the re-probe schedule")
	}
	p.Reprobe(context.Background())
	if !tun.Registered(uid) {
		t.Fatal("the re-probe in the new epoch did not register the UID")
	}
	if _, ok := p.reprobeAt(); ok {
		t.Fatal("the UID stayed on the re-probe schedule after its pass")
	}
}

// TestReregisteredUIDIsReprobedOnItsBackoffUntilAPass: a UID registered again
// is probed at once, then 100 ms, 200 ms and 400 ms later while its probes
// fail, and leaves the schedule at its first pass. The schedule runs apart from
// the rounds: the next round stays probeInterval after the previous one.
func TestReregisteredUIDIsReprobedOnItsBackoffUntilAPass(t *testing.T) {
	m := startMuxDouble(t)
	var mu sync.Mutex
	insts := []v1alpha1.Instance{probeInstance("00000000-0000-4000-8000-000000000034", v1alpha1.InstanceReady, metav1.ConditionTrue)}
	uid := insts[0].UID
	m.set(uid, http.StatusOK)
	clk := clock.NewFake(t0)
	p, tun, _ := newTestProber(t, m, clk, &insts, &mu)
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() { p.Run(ctx); close(stopped) }()
	defer func() { cancel(); <-stopped }()
	waitFor := func(what string, cond func() bool) {
		t.Helper()
		for end := time.Now().Add(5 * time.Second); !cond(); time.Sleep(5 * time.Millisecond) {
			if time.Now().After(end) {
				t.Fatalf("timed out waiting for %s (hits %d, registered %v)", what, m.hit(uid), tun.Registered(uid))
			}
		}
	}
	waitFor("the first round", func() bool { return tun.Registered(uid) && clk.Pending() == 1 })

	// frps announces a new registration; the engine is not answering yet.
	m.set(uid, http.StatusServiceUnavailable)
	tun.Bump(uid)
	p.Reregistered(uid)
	// Two timers from here: the next round's, and the next re-probe's.
	waitFor("the re-probe at once", func() bool { return m.hit(uid) == 2 && clk.Pending() == 2 })
	for i, wait := range []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond} {
		clk.Advance(wait - time.Millisecond)
		if n := m.hit(uid); n != 2+i {
			t.Fatalf("re-probe %d came before its wait of %s: %d probes", i+1, wait, n)
		}
		clk.Advance(time.Millisecond)
		waitFor("the re-probe after "+wait.String(), func() bool { return m.hit(uid) == 3+i && clk.Pending() == 2 })
		if tun.Registered(uid) {
			t.Fatal("a failing re-probe registered the UID")
		}
	}
	m.set(uid, http.StatusOK)
	clk.Advance(800 * time.Millisecond)
	waitFor("the passing re-probe", func() bool { return tun.Registered(uid) })
	if _, ok := p.reprobeAt(); ok {
		t.Fatal("the UID stayed on the re-probe schedule after its pass")
	}
	// No more re-probes: the next probe is the round at probeInterval.
	hits := m.hit(uid)
	clk.Advance(10*time.Second - (100+200+400+800)*time.Millisecond - time.Millisecond)
	if n := m.hit(uid); n != hits {
		t.Fatalf("%d probes after the pass and before the next round, want none", n-hits)
	}
	clk.Advance(time.Millisecond)
	waitFor("the next round", func() bool { return m.hit(uid) == hits+1 })
}

// TestSetRegisteredAtRefusesAStaleEpoch: a pass reached in an epoch that has
// moved since registers nothing, and tells no subscriber; one in the current
// epoch registers.
func TestSetRegisteredAtRefusesAStaleEpoch(t *testing.T) {
	tun := NewTunnel("127.0.0.1:1", time.Second)
	const uid = types.UID("00000000-0000-4000-8000-000000000041")
	stale := tun.Epoch(uid)
	tun.Bump(uid)
	calls := 0
	tun.Subscribe(func() { calls++ })
	if was, applied := tun.setRegisteredAt(uid, true, &stale); was || applied || tun.Registered(uid) || calls != 0 {
		t.Fatalf("a pass in a stale epoch: was %v, applied %v, registered %v, %d subscriber calls", was, applied, tun.Registered(uid), calls)
	}
	cur := tun.Epoch(uid)
	if _, applied := tun.setRegisteredAt(uid, true, &cur); !applied || !tun.Registered(uid) || calls != 1 {
		t.Fatalf("a pass in the current epoch: applied %v, registered %v, %d subscriber calls", applied, tun.Registered(uid), calls)
	}
}

// TestProbePassWhoseEpochMovesAfterTheEarlyCheckDoesNotCount: the epoch moves
// after the probe's early check and before its verdict is applied (the
// beforeVerdict hook), which only the check under the Tunnel's lock
// (setRegisteredAt) can catch. The pass registers nothing, tells no subscriber,
// and puts the UID back on the re-probe schedule; the re-probe registers it.
func TestProbePassWhoseEpochMovesAfterTheEarlyCheckDoesNotCount(t *testing.T) {
	m := startMuxDouble(t)
	var mu sync.Mutex
	insts := []v1alpha1.Instance{probeInstance("00000000-0000-4000-8000-000000000042", v1alpha1.InstanceReady, metav1.ConditionTrue)}
	uid := insts[0].UID
	m.set(uid, http.StatusOK)
	p, tun, _ := newTestProber(t, m, clock.NewFake(t0), &insts, &mu)
	var once sync.Once
	p.beforeVerdict = func(u types.UID) { once.Do(func() { tun.Bump(u) }) }
	calls := 0
	tun.Subscribe(func() { calls++ })
	p.Round(context.Background())
	if tun.Registered(uid) || calls != 0 {
		t.Fatalf("a pass whose epoch moved before its verdict: registered %v, %d subscriber calls", tun.Registered(uid), calls)
	}
	if !p.isAwaiting(uid) {
		t.Fatal("the UID is not back on the re-probe schedule")
	}
	p.Reprobe(context.Background())
	if !tun.Registered(uid) {
		t.Fatal("the re-probe in the new epoch did not register the UID")
	}
}

// TestReprobeIsNotHeldUpByASlowRound: a round waits for its probes, up to
// probeTimeout, so one Instance whose /health hangs keeps a round open for
// that long. A UID registered again meanwhile is still re-probed at once, and
// routable again while that round goes on.
func TestReprobeIsNotHeldUpByASlowRound(t *testing.T) {
	m := startMuxDouble(t)
	var mu sync.Mutex
	insts := []v1alpha1.Instance{
		probeInstance("00000000-0000-4000-8000-000000000043", v1alpha1.InstanceReady, metav1.ConditionTrue),
		probeInstance("00000000-0000-4000-8000-000000000044", v1alpha1.InstanceReady, metav1.ConditionTrue),
	}
	a, slow := insts[0].UID, insts[1].UID
	m.set(a, http.StatusOK)
	m.set(slow, http.StatusOK)
	clk := clock.NewFake(t0)
	p, tun, _ := newTestProber(t, m, clk, &insts, &mu)
	held, release := m.holdNext(t, slow, "health")
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() { p.Run(ctx); close(stopped) }()
	defer func() { release(); cancel(); <-stopped }()
	select {
	case <-held:
	case <-time.After(5 * time.Second):
		t.Fatal("the round never reached the slow Instance's GET /health")
	}
	for end := time.Now().Add(5 * time.Second); !tun.Registered(a); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatal("the first round's probe of A never registered it")
		}
	}

	tun.Bump(a)
	p.Reregistered(a)
	for end := time.Now().Add(5 * time.Second); !tun.Registered(a); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("A was not re-probed while the round waited on the slow Instance (hits %d)", m.hit(a))
		}
	}
	if m.hit(slow) != 0 {
		t.Fatal("the slow Instance's probe ended before A's re-probe: the round was not held open")
	}
}

// TestForgottenUIDGetsAFreshEpoch: an Instance the front stops probing gets a
// fresh epoch, not 0 again, so a dialer handed out before can never match,
// even once the UID is probed and registered again.
func TestForgottenUIDGetsAFreshEpoch(t *testing.T) {
	m := startMuxDouble(t)
	var mu sync.Mutex
	insts := []v1alpha1.Instance{probeInstance("00000000-0000-4000-8000-000000000045", v1alpha1.InstanceReady, metav1.ConditionTrue)}
	uid := insts[0].UID
	m.set(uid, http.StatusOK)
	p, tun, _ := newTestProber(t, m, clock.NewFake(t0), &insts, &mu)
	p.Round(context.Background())
	old, ok := tun.Dialer(uid)
	if !ok || tun.Epoch(uid) != 0 {
		t.Fatalf("after the first pass: dialer %v, epoch %d, want a dialer at epoch 0", ok, tun.Epoch(uid))
	}
	mu.Lock()
	saved := insts
	insts = nil
	mu.Unlock()
	p.Round(context.Background()) // the Instance is no longer probed: forgotten
	if tun.Epoch(uid) == 0 {
		t.Fatal("a forgotten UID's epoch went back to 0")
	}
	mu.Lock()
	insts = saved
	mu.Unlock()
	p.Round(context.Background())
	if !tun.Registered(uid) {
		t.Fatal("the UID did not register again")
	}
	if c, err := old(context.Background()); err == nil {
		c.Close()
		t.Fatal("a dialer from before the UID was forgotten connected")
	} else if !errors.Is(err, errReregistered) {
		t.Fatalf("the old dialer: %v, want errReregistered", err)
	}
}

// TestReprobeScheduleEndsAtProbeInterval: a UID whose re-probes keep failing
// leaves the schedule once its wait would reach probeInterval; the rounds go
// on probing it.
func TestReprobeScheduleEndsAtProbeInterval(t *testing.T) {
	m := startMuxDouble(t)
	var mu sync.Mutex
	insts := []v1alpha1.Instance{probeInstance("00000000-0000-4000-8000-000000000035", v1alpha1.InstanceReady, metav1.ConditionTrue)}
	uid := insts[0].UID
	m.set(uid, http.StatusServiceUnavailable)
	clk := clock.NewFake(t0)
	p, _, _ := newTestProber(t, m, clk, &insts, &mu)
	p.Reregistered(uid)
	attempts := 0
	for {
		at, ok := p.reprobeAt()
		if !ok {
			break
		}
		clk.Advance(at.Sub(clk.Now()))
		p.Reprobe(context.Background())
		attempts++
		if attempts > 20 {
			t.Fatal("the re-probe schedule never ended")
		}
	}
	// At once, then 0.1, 0.2, 0.4, 0.8, 1.6, 3.2 and 6.4 s later; the next wait, 12.8 s, would reach
	// probeInterval (10 s).
	if attempts != 8 || m.hit(uid) != 8 {
		t.Fatalf("%d re-probes (%d probes), want 8", attempts, m.hit(uid))
	}
}

// TestReprobeDropsAUIDTheFrontDoesNotProbe: a new registration of a UID no
// probed Instance has (an Instance not yet verifying, or gone) leaves the
// schedule at the next re-probe, so Run does not spin on it.
func TestReprobeDropsAUIDTheFrontDoesNotProbe(t *testing.T) {
	m := startMuxDouble(t)
	var mu sync.Mutex
	insts := []v1alpha1.Instance{probeInstance("00000000-0000-4000-8000-000000000036", v1alpha1.InstancePending, "")}
	p, _, _ := newTestProber(t, m, clock.NewFake(t0), &insts, &mu)
	p.Reregistered(insts[0].UID)
	p.Reregistered("00000000-0000-4000-8000-000000000037")
	p.Reprobe(context.Background())
	if _, ok := p.reprobeAt(); ok {
		t.Fatal("a UID the front does not probe stayed on the re-probe schedule")
	}
}

// TestFrontPluginTakesTheRouteAwayBeforeFrpsRegisters: the front's frps plugin
// at FrontFrpsPluginPath. A NewProxy for a UID takes its registration away,
// moves its epoch and puts it on the re-probe schedule; so does a CloseProxy,
// which frps may post after the next NewProxy; a call it cannot read, or a NewProxy
// whose proxy name is no UID, is refused (failing closed, nothing changes);
// any other op is answered with no action.
func TestFrontPluginTakesTheRouteAwayBeforeFrpsRegisters(t *testing.T) {
	m := startMuxDouble(t)
	var mu sync.Mutex
	insts := []v1alpha1.Instance{probeInstance("00000000-0000-4000-8000-000000000038", v1alpha1.InstanceReady, metav1.ConditionTrue)}
	uid := insts[0].UID
	p, tun, _ := newTestProber(t, m, clock.NewFake(t0), &insts, &mu)
	h := &frpsPlugin{tun: tun, prober: p, log: discardLog()}
	call := func(body string) pluginResponse {
		t.Helper()
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, agentapi.FrontFrpsPluginPath, strings.NewReader(body)))
		if w.Code != http.StatusOK {
			t.Fatalf("%s: HTTP %d %s", body, w.Code, w.Body)
		}
		var r pluginResponse
		if err := json.Unmarshal(w.Body.Bytes(), &r); err != nil {
			t.Fatalf("%s: %v in %s", body, err, w.Body)
		}
		return r
	}
	newProxy := `{"version":"0.1.0","op":"NewProxy","content":{"user":{"metas":{"token":"x"}},"proxy_name":"` + string(uid) + `","proxy_type":"tcpmux"}}`
	closeProxy := `{"version":"0.1.0","op":"CloseProxy","content":{"user":{},"proxy_name":"` + string(uid) + `"}}`

	tun.setRegistered(uid, true)
	e := tun.Epoch(uid)
	if r := call(newProxy); r.Reject || !r.Unchange {
		t.Fatalf("NewProxy: %+v, want accepted and unchanged", r)
	}
	if tun.Registered(uid) || tun.Epoch(uid) == e {
		t.Fatalf("after NewProxy: registered %v, epoch %d (was %d)", tun.Registered(uid), tun.Epoch(uid), e)
	}
	if _, ok := p.reprobeAt(); !ok {
		t.Fatal("NewProxy put no re-probe on the schedule")
	}

	p.awaiting = map[types.UID]*reprobe{}
	tun.setRegistered(uid, true)
	e = tun.Epoch(uid)
	if r := call(closeProxy); r.Reject {
		t.Fatalf("CloseProxy: %+v, want accepted", r)
	}
	if tun.Registered(uid) || tun.Epoch(uid) == e {
		t.Fatalf("after CloseProxy: registered %v, epoch %d (was %d)", tun.Registered(uid), tun.Epoch(uid), e)
	}
	if _, ok := p.reprobeAt(); !ok {
		t.Fatal("CloseProxy put no re-probe on the schedule")
	}

	tun.setRegistered(uid, true)
	e = tun.Epoch(uid)
	for name, body := range map[string]string{
		"a body that is no JSON":  `{"op":"NewProxy",`,
		"a NewProxy with no name": `{"op":"NewProxy","content":{"proxy_type":"tcpmux"}}`,
		"a NewProxy with no UID":  `{"op":"NewProxy","content":{"proxy_name":"Not_A_UID"}}`,
		"a body over the limit":   `{"op":"NewProxy","content":{"proxy_name":"` + strings.Repeat("a", pluginBodyLimit) + `"}}`,
	} {
		if r := call(body); !r.Reject || r.RejectReason == "" {
			t.Errorf("%s: %+v, want a refusal with a reason", name, r)
		}
	}
	if r := call(`{"op":"Ping","content":{}}`); r.Reject {
		t.Errorf("another op: %+v, want accepted", r)
	}
	if !tun.Registered(uid) || tun.Epoch(uid) != e {
		t.Fatalf("a refused or other call changed the UID: registered %v, epoch %d (was %d)", tun.Registered(uid), tun.Epoch(uid), e)
	}

	for _, c := range []struct {
		method, path string
		code         int
	}{{http.MethodGet, agentapi.FrontFrpsPluginPath, http.StatusMethodNotAllowed}, {http.MethodPost, "/elsewhere", http.StatusNotFound}} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(c.method, c.path, strings.NewReader(newProxy)))
		if w.Code != c.code {
			t.Errorf("%s %s: HTTP %d, want %d", c.method, c.path, w.Code, c.code)
		}
	}
}

// TestFrontPluginAnswersWithinASecondUnderLoad: HBEP-6's 1 s rule for a plugin,
// over loopback on the front's own health handler: 400 NewProxy calls at once,
// for 40 UIDs, while the router's rebuild (a subscriber) is stuck, each
// answered within a second.
func TestFrontPluginAnswersWithinASecondUnderLoad(t *testing.T) {
	m := startMuxDouble(t)
	var mu sync.Mutex
	var insts []v1alpha1.Instance
	p, tun, _ := newTestProber(t, m, clock.NewFake(t0), &insts, &mu)
	f := &Front{Tunnel: tun, Prober: p, Metrics: NewMetrics(nil, t.TempDir(), func(string) (uint64, error) { return 1 << 30, nil }, nil)}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: healthHandler(f, discardLog()), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	url := "http://" + ln.Addr().String() + agentapi.FrontFrpsPluginPath
	client := &http.Client{Transport: &http.Transport{Proxy: nil, MaxConnsPerHost: 0, MaxIdleConnsPerHost: 400}}
	const calls, uids = 400, 40
	for i := 0; i < uids; i++ {
		tun.setRegistered(types.UID(uidN(i)), true)
	}
	// From here the router's rebuild is stuck: each Bump of a registered UID
	// calls it, and the calls must not wait for it.
	stuck := make(chan struct{})
	defer close(stuck)
	tun.Subscribe(func() { <-stuck })
	var wg sync.WaitGroup
	slow := make(chan string, calls)
	for i := 0; i < calls; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := `{"version":"0.1.0","op":"NewProxy","content":{"proxy_name":"` + uidN(i%uids) + `"}}`
			began := time.Now()
			resp, err := client.Post(url, "application/json", bytes.NewBufferString(body))
			took := time.Since(began)
			if err != nil {
				slow <- err.Error()
				return
			}
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if took >= time.Second || resp.StatusCode != http.StatusOK || !strings.Contains(string(b), `"reject":false`) {
				slow <- took.String() + " " + resp.Status + " " + string(b)
			}
		}(i)
	}
	wg.Wait()
	close(slow)
	var bad []string
	for s := range slow {
		bad = append(bad, s)
	}
	if len(bad) > 0 {
		t.Fatalf("%d of %d calls failed or took a second or more; the first: %s", len(bad), calls, bad[0])
	}
	for i := 0; i < uids; i++ {
		if tun.Registered(types.UID(uidN(i))) {
			t.Fatalf("UID %d is still registered after its NewProxy calls", i)
		}
	}
}

func uidN(i int) string {
	return "00000000-0000-4000-8000-0000000001" + string(rune('0'+i/10)) + string(rune('0'+i%10))
}

func pointerTo[T any](v T) *T { return &v }
