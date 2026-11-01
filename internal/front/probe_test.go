package front

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
)

// The probe's bookkeeping on its own: which Instances it probes, when it
// reports PathProbeFailing, and its schedule. A small stand-in for frps's
// tcpmux port answers here, so each case is cheap to set up; what the probe
// does to routing over the real frps and frpc is
// TestProbeTakesAFailingPathOutOfRoutingAndBack.

// muxDouble answers CONNECT <UID>:80 with 200 for the UIDs it has and 404
// otherwise, then answers GET /health with the UID's status. holdNext makes it
// hold an exchange unanswered.
type muxDouble struct {
	ln     net.Listener
	mu     sync.Mutex
	health map[types.UID]int
	hits   map[types.UID]int
	holds  map[types.UID]*muxHold
}

// muxHold holds a UID's exchanges at one step until it is released.
type muxHold struct {
	step    string        // "connect" or "health"
	held    chan struct{} // one value per exchange held
	release chan struct{}
	once    sync.Once
}

// holdNext holds the UID's exchanges at step until release is called: at
// "connect" the double reads the CONNECT and answers nothing; at "health" it
// answers the CONNECT, reads GET /health and answers nothing. held gets a
// value as each exchange is held; released, each one answers as usual. The
// test's cleanup releases it too, so no exchange outlives the test.
func (m *muxDouble) holdNext(t *testing.T, uid types.UID, step string) (held <-chan struct{}, release func()) {
	t.Helper()
	h := &muxHold{step: step, held: make(chan struct{}, 64), release: make(chan struct{})}
	m.mu.Lock()
	m.holds[uid] = h
	m.mu.Unlock()
	release = func() {
		h.once.Do(func() {
			m.mu.Lock()
			if m.holds[uid] == h {
				delete(m.holds, uid)
			}
			m.mu.Unlock()
			close(h.release)
		})
	}
	t.Cleanup(release)
	return h.held, release
}

// wait holds the exchange when a hold is set for the UID at this step.
func (m *muxDouble) wait(c net.Conn, uid types.UID, step string) {
	m.mu.Lock()
	h := m.holds[uid]
	m.mu.Unlock()
	if h == nil || h.step != step {
		return
	}
	_ = c.SetDeadline(time.Time{})
	select {
	case h.held <- struct{}{}:
	default:
	}
	<-h.release
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
}

func startMuxDouble(t *testing.T) *muxDouble {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	m := &muxDouble{ln: ln, health: map[types.UID]int{}, hits: map[types.UID]int{}, holds: map[types.UID]*muxHold{}}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go m.serve(c)
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return m
}

func (m *muxDouble) set(uid types.UID, status int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.health[uid] = status
}

// drop takes the UID's proxy away: the double answers 404 for it, as frps does for a UID with no
// proxy online.
func (m *muxDouble) drop(uid types.UID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.health, uid)
}

func (m *muxDouble) hit(uid types.UID) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hits[uid]
}

func (m *muxDouble) serve(c net.Conn) {
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	br := bufio.NewReader(c)
	req, err := http.ReadRequest(br)
	if err != nil || req.Method != http.MethodConnect {
		return
	}
	uid := types.UID(strings.TrimSuffix(req.Host, ":80"))
	m.wait(c, uid, "connect")
	m.mu.Lock()
	_, ok := m.health[uid]
	m.mu.Unlock()
	if !ok {
		_, _ = io.WriteString(c, "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
		return
	}
	_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
	if req, err = http.ReadRequest(br); err != nil || req.URL.Path != "/health" || req.Header.Get("Authorization") != "" {
		return
	}
	m.wait(c, uid, "health")
	m.mu.Lock()
	m.hits[uid]++
	status := m.health[uid]
	m.mu.Unlock()
	fmt.Fprintf(c, "HTTP/1.1 %d %s\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", status, http.StatusText(status))
}

func probeInstance(uid types.UID, phase v1alpha1.InstancePhase, tunnel metav1.ConditionStatus) v1alpha1.Instance {
	in := v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "i-" + string(uid)[len(uid)-2:], UID: uid}, Status: v1alpha1.InstanceStatus{Phase: phase}}
	if tunnel != "" {
		in.Status.Conditions = []metav1.Condition{{Type: v1alpha1.ConditionTunnelConnected, Status: tunnel, Reason: "R", LastTransitionTime: metav1.Now()}}
	}
	return in
}

func newTestProber(t *testing.T, m *muxDouble, clk clock.Clock, insts *[]v1alpha1.Instance, mu *sync.Mutex, edit ...func(*Config)) (*Prober, *Tunnel, *recordedPathEvents) {
	t.Helper()
	cfg := Config{}.WithDefaults()
	for _, e := range edit {
		e(&cfg)
	}
	tun := NewTunnel(m.ln.Addr().String(), cfg.TunnelSetupTimeout.D())
	ev := &recordedPathEvents{}
	p := NewProber(cfg, tun, clk, func() []v1alpha1.Instance {
		mu.Lock()
		defer mu.Unlock()
		return append([]v1alpha1.Instance(nil), *insts...)
	}, ev, discardLog())
	return p, tun, ev
}

// TestProbeReportsPathProbeFailingOnlyForAReadyInstanceWithItsTunnelUp: all
// three lose their registration after two failed probes, and each subscriber
// hears each change once; only the ready Instance whose TunnelConnected is
// True is reported, once (HBEP-6, Reporting).
func TestProbeReportsPathProbeFailingOnlyForAReadyInstanceWithItsTunnelUp(t *testing.T) {
	m := startMuxDouble(t)
	var mu sync.Mutex
	insts := []v1alpha1.Instance{
		probeInstance("00000000-0000-4000-8000-000000000001", v1alpha1.InstanceReady, metav1.ConditionTrue),
		probeInstance("00000000-0000-4000-8000-000000000002", v1alpha1.InstanceReady, metav1.ConditionFalse),
		probeInstance("00000000-0000-4000-8000-000000000003", v1alpha1.InstanceVerifying, metav1.ConditionTrue),
		probeInstance("00000000-0000-4000-8000-000000000004", v1alpha1.InstanceDraining, metav1.ConditionTrue),
	}
	for _, in := range insts {
		m.set(in.UID, http.StatusOK)
	}
	p, tun, ev := newTestProber(t, m, clock.Real{}, &insts, &mu)
	var changes int
	var cmu sync.Mutex
	tun.Subscribe(func() { cmu.Lock(); changes++; cmu.Unlock() })
	ctx := context.Background()
	p.Round(ctx)
	for _, in := range insts {
		if !tun.Registered(in.UID) {
			t.Fatalf("%s (%s) not registered after a passing probe", in.Name, in.Status.Phase)
		}
		m.set(in.UID, http.StatusServiceUnavailable)
	}
	// probeFailures is 2: one failed probe keeps every registration, and the
	// second takes them all.
	p.Round(ctx)
	for _, in := range insts {
		if !tun.Registered(in.UID) {
			t.Errorf("%s (%s) lost its registration after one failed probe, want two", in.Name, in.Status.Phase)
		}
	}
	if f := ev.failings(); len(f) != 0 {
		t.Errorf("PathProbeFailing reports %+v after one failed probe, want none", f)
	}
	p.Round(ctx)
	for _, in := range insts {
		if tun.Registered(in.UID) {
			t.Errorf("%s (%s) still registered after two failed probes", in.Name, in.Status.Phase)
		}
	}
	if f := ev.failings(); len(f) != 1 || f[0].UID != insts[0].UID || f[0].Failures != 2 {
		t.Errorf("PathProbeFailing reports %+v after two failed probes, want one, for the ready Instance with TunnelConnected True", f)
	}
	p.Round(ctx)
	if f := ev.failings(); len(f) != 1 {
		t.Errorf("PathProbeFailing reports %+v after a third failed probe, want still the one", f)
	}
	cmu.Lock()
	defer cmu.Unlock()
	if changes != 2*len(insts) {
		t.Errorf("subscribers heard %d changes, want %d: one registration and one loss each", changes, 2*len(insts))
	}
}

// TestProbeRoutesOnTheProbeNotTheTunnelCondition: a ready Instance is eligible when its probe
// passes and not when it fails, whatever its TunnelConnected condition says (HBEP-6, The front's
// half: the front routes on its own probe, and the controller restarts nothing on the path). The
// condition here is False for the whole test, as it is for a restarted Instance that returned to
// ready with its tunnel down (`@hpc-burst-science-agent`, 1 Nov 2026, 17:29Z):
//   - no proxy online (frps answers 404): not eligible, the same as a ready Instance whose
//     tunnel dropped, and no PathProbeFailing, which is for an Instance whose tunnel is up;
//   - the proxy back (frpc reconnected), one passing probe: eligible, with the condition still
//     False, since the writer's next read has not come, and a passing probe is the front's
//     evidence;
//   - the proxy gone again: eligible after one failed probe, not after the second (HBEP-6's two),
//     and eligible again after one passing probe.
func TestProbeRoutesOnTheProbeNotTheTunnelCondition(t *testing.T) {
	m := startMuxDouble(t)
	var mu sync.Mutex
	in := probeInstance("00000000-0000-4000-8000-000000000021", v1alpha1.InstanceReady, metav1.ConditionFalse)
	insts := []v1alpha1.Instance{in}
	p, tun, ev := newTestProber(t, m, clock.Real{}, &insts, &mu)
	ctx := context.Background()
	want := func(step string, eligible bool) {
		t.Helper()
		if got := tun.Registered(in.UID); got != eligible {
			t.Fatalf("%s: eligible %t, want %t", step, got, eligible)
		}
	}

	p.Round(ctx) // no proxy: the double answers 404
	want("ready, no proxy online", false)
	p.Round(ctx)
	want("ready, still no proxy online", false)

	m.set(in.UID, http.StatusOK) // frpc is back
	p.Round(ctx)
	want("the proxy back, one passing probe", true)

	m.drop(in.UID)
	p.Round(ctx)
	want("the proxy gone again, one failed probe", true)
	p.Round(ctx)
	want("the proxy gone again, two failed probes", false)

	m.set(in.UID, http.StatusOK)
	p.Round(ctx)
	want("the proxy back again, one passing probe", true)

	if f := ev.failings(); len(f) != 0 {
		t.Errorf("PathProbeFailing reports %+v for an Instance whose TunnelConnected is False, want none", f)
	}
	mu.Lock()
	defer mu.Unlock()
	if c := insts[0].Status.Conditions; len(c) != 1 || c[0].Status != metav1.ConditionFalse {
		t.Errorf("conditions %+v: the test holds TunnelConnected False throughout", c)
	}
}

// TestProbeSkipsOtherPhasesAndForgetsInstancesThatLeave: pending, starting
// and ended Instances, and one being deleted, are not probed; an Instance that
// leaves the probed phases loses its registration at the next round.
func TestProbeSkipsOtherPhasesAndForgetsInstancesThatLeave(t *testing.T) {
	m := startMuxDouble(t)
	var mu sync.Mutex
	deleting := probeInstance("00000000-0000-4000-8000-000000000014", v1alpha1.InstanceReady, metav1.ConditionTrue)
	deleting.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	insts := []v1alpha1.Instance{
		probeInstance("00000000-0000-4000-8000-000000000011", v1alpha1.InstancePending, ""),
		probeInstance("00000000-0000-4000-8000-000000000012", v1alpha1.InstanceStarting, ""),
		probeInstance("00000000-0000-4000-8000-000000000013", v1alpha1.InstanceEnded, ""),
		deleting,
		probeInstance("00000000-0000-4000-8000-000000000015", v1alpha1.InstanceReady, metav1.ConditionTrue),
	}
	for _, in := range insts {
		m.set(in.UID, http.StatusOK)
	}
	p, tun, _ := newTestProber(t, m, clock.Real{}, &insts, &mu)
	p.Round(context.Background())
	for _, in := range insts[:4] {
		if m.hit(in.UID) != 0 || tun.Registered(in.UID) {
			t.Errorf("%s (%s, deleting %v) was probed", in.Name, in.Status.Phase, in.DeletionTimestamp != nil)
		}
	}
	ready := insts[4].UID
	if !tun.Registered(ready) {
		t.Fatal("the ready Instance was not registered")
	}
	mu.Lock()
	insts[4].Status.Phase = v1alpha1.InstanceEnded
	mu.Unlock()
	p.Round(context.Background())
	if tun.Registered(ready) || m.hit(ready) != 1 {
		t.Errorf("an ended Instance: registered %v, probed %d times; want forgotten after one probe", tun.Registered(ready), m.hit(ready))
	}
}

// TestProberRunsEveryProbeInterval: a round at the start, and the next one
// when probeInterval (10 s) has passed on the front's clock, not before.
func TestProberRunsEveryProbeInterval(t *testing.T) {
	m := startMuxDouble(t)
	var mu sync.Mutex
	insts := []v1alpha1.Instance{probeInstance("00000000-0000-4000-8000-000000000021", v1alpha1.InstanceReady, metav1.ConditionTrue)}
	uid := insts[0].UID
	m.set(uid, http.StatusOK)
	clk := clock.NewFake(t0)
	p, _, _ := newTestProber(t, m, clk, &insts, &mu)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	waitFor := func(what string, cond func() bool) {
		t.Helper()
		for end := time.Now().Add(5 * time.Second); !cond(); time.Sleep(5 * time.Millisecond) {
			if time.Now().After(end) {
				t.Fatalf("timed out waiting for %s", what)
			}
		}
	}
	waitFor("the first round", func() bool { return m.hit(uid) == 1 && clk.Pending() == 1 })
	clk.Advance(10*time.Second - time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	if n := m.hit(uid); n != 1 {
		t.Fatalf("%d probes before probeInterval passed, want 1", n)
	}
	clk.Advance(time.Millisecond)
	waitFor("the second round", func() bool { return m.hit(uid) == 2 })
}

// TestProbeCutByTheFrontsStopIsNoVerdict: a probe that the front's own
// context ended neither fails nor passes: the registration stays and nothing
// is reported, so a stop raises no PathProbeFailing.
func TestProbeCutByTheFrontsStopIsNoVerdict(t *testing.T) {
	m := startMuxDouble(t)
	var mu sync.Mutex
	insts := []v1alpha1.Instance{probeInstance("00000000-0000-4000-8000-000000000031", v1alpha1.InstanceReady, metav1.ConditionTrue)}
	uid := insts[0].UID
	m.set(uid, http.StatusOK)
	p, tun, ev := newTestProber(t, m, clock.Real{}, &insts, &mu)
	p.Round(context.Background())
	if !tun.Registered(uid) {
		t.Fatal("not registered after a passing probe")
	}
	m.set(uid, http.StatusServiceUnavailable) // a verdict now would be a failure
	stopped, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 3; i++ {
		p.Round(stopped)
	}
	ev.mu.Lock()
	probed := len(ev.probed)
	ev.mu.Unlock()
	if !tun.Registered(uid) || probed != 1 || len(ev.failings()) != 0 {
		t.Errorf("after probes cut by a stop: registered %v, %d results, %d PathProbeFailing; want registered, 1, 0", tun.Registered(uid), probed, len(ev.failings()))
	}
	// The positive control: the same Instance does lose its registration to
	// two real failures, so the verdicts above came from the cut.
	p.Round(context.Background())
	p.Round(context.Background())
	if tun.Registered(uid) || len(ev.failings()) != 1 {
		t.Errorf("after two real failures: registered %v, %d PathProbeFailing; want unregistered and 1", tun.Registered(uid), len(ev.failings()))
	}
}

// TestProbeCutInFlightByTheStopIsNoVerdict: the front stops while a probe
// waits, held at frps's answer to its CONNECT or at the engine's answer to
// GET /health. Prober.Run returns at once, well before tunnelSetupTimeout or
// probeTimeout (a minute each here, so only the cut can end the probe in
// time), and the cut probe is no verdict: the registration stays and nothing
// is reported.
func TestProbeCutInFlightByTheStopIsNoVerdict(t *testing.T) {
	for _, step := range []string{"connect", "health"} {
		t.Run(step, func(t *testing.T) {
			m := startMuxDouble(t)
			var mu sync.Mutex
			insts := []v1alpha1.Instance{probeInstance("00000000-0000-4000-8000-000000000041", v1alpha1.InstanceReady, metav1.ConditionTrue)}
			uid := insts[0].UID
			m.set(uid, http.StatusOK)
			p, tun, ev := newTestProber(t, m, clock.NewFake(t0), &insts, &mu, func(c *Config) {
				c.TunnelSetupTimeout, c.ProbeTimeout = Duration(time.Minute), Duration(time.Minute)
			})
			p.Round(context.Background())
			if !tun.Registered(uid) {
				t.Fatal("not registered after a passing probe")
			}
			held, _ := m.holdNext(t, uid, step)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			go func() { p.Run(ctx); close(done) }()
			select {
			case <-held:
			case <-time.After(30 * time.Second):
				t.Fatalf("the probe never reached the held %s", step)
			}
			cancel()
			select {
			case <-done:
			case <-time.After(20 * time.Second):
				t.Fatalf("Prober.Run still runs 20 s after the stop: the probe held at %s waited for its deadline", step)
			}
			ev.mu.Lock()
			probed := len(ev.probed)
			ev.mu.Unlock()
			if !tun.Registered(uid) || probed != 1 || len(ev.failings()) != 0 {
				t.Errorf("after a probe cut at %s: registered %v, %d results, %d PathProbeFailing; want registered, 1 (the first pass), 0",
					step, tun.Registered(uid), probed, len(ev.failings()))
			}
		})
	}
}
