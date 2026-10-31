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
// otherwise, then answers GET /health with the UID's status.
type muxDouble struct {
	ln     net.Listener
	mu     sync.Mutex
	health map[types.UID]int
	hits   map[types.UID]int
}

func startMuxDouble(t *testing.T) *muxDouble {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	m := &muxDouble{ln: ln, health: map[types.UID]int{}, hits: map[types.UID]int{}}
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
	m.mu.Lock()
	status, ok := m.health[uid]
	m.mu.Unlock()
	if !ok {
		_, _ = io.WriteString(c, "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
		return
	}
	_, _ = io.WriteString(c, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
	if req, err = http.ReadRequest(br); err != nil || req.URL.Path != "/health" || req.Header.Get("Authorization") != "" {
		return
	}
	m.mu.Lock()
	m.hits[uid]++
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

func newTestProber(t *testing.T, m *muxDouble, clk clock.Clock, insts *[]v1alpha1.Instance, mu *sync.Mutex) (*Prober, *Tunnel, *recordedPathEvents) {
	t.Helper()
	cfg := Config{}.WithDefaults()
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
	p.Round(ctx)
	p.Round(ctx)
	p.Round(ctx)
	for _, in := range insts {
		if tun.Registered(in.UID) {
			t.Errorf("%s still registered after three failed probes", in.Name)
		}
	}
	if f := ev.failings(); len(f) != 1 || f[0].UID != insts[0].UID {
		t.Errorf("PathProbeFailing reports %+v, want one, for the ready Instance with TunnelConnected True", f)
	}
	cmu.Lock()
	defer cmu.Unlock()
	if changes != 2*len(insts) {
		t.Errorf("subscribers heard %d changes, want %d: one registration and one loss each", changes, 2*len(insts))
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
