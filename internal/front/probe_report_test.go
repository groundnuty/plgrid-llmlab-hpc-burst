package front

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/inventory"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
)

// What the probe reports through Run: PathProbeFailing on the front's
// Recorder (HBEP-6, Reporting), and HeartbeatMissed with the probe's state in
// /healthz (HBEP-8, Observers). frps's tcpmux port is the muxDouble, so Run
// builds its own Tunnel and Prober with no frp binaries.

// waitState waits for an observable state with a generous deadline.
func waitState(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(30 * time.Second); !cond(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// seedInstances is a fake control plane holding the Instances.
func seedInstances(t *testing.T, insts ...v1alpha1.Instance) *dynfake.FakeDynamicClient {
	t.Helper()
	var objs []runtime.Object
	for i := range insts {
		m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&insts[i])
		if err != nil {
			t.Fatal(err)
		}
		u := &unstructured.Unstructured{Object: m}
		u.SetAPIVersion(v1alpha1.GroupVersion.String())
		u.SetKind("Instance")
		objs = append(objs, u)
	}
	return dynfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		inventory.GVRModelDeployments: "ModelDeploymentList", inventory.GVRInstances: "InstanceList",
		inventory.GVRProviders: "ProviderList", inventory.GVRPlatformProfiles: "PlatformProfileList",
	}, objs...)
}

// runProbeFront runs the front with its own Tunnel to the double, over a fake
// control plane holding insts, with no dial-home mode. edit changes the
// options before the start. It returns the running front and its stop, which
// the test's cleanup also calls.
func runProbeFront(t *testing.T, m *muxDouble, edit func(*Options), insts ...v1alpha1.Instance) (*Front, func()) {
	t.Helper()
	dir := t.TempDir()
	keys := filepath.Join(dir, "keys.json")
	writeMode600(t, keys, `{"keys":[{"id":"k","key":"client-key"}]}`)
	mux := m.ln.Addr().String()
	o := Options{
		Config:   Config{ClientKeysFile: keys, UsageDir: filepath.Join(dir, "usage"), FrpsBindAddr: mux, FrpsMuxAddr: mux}.WithDefaults(),
		Dynamic:  seedInstances(t, insts...),
		Recorder: &MemoryRecorder{}, Log: discardLog(),
		ClientListener: kernelListener(t), DialHomeListener: kernelListener(t), HealthListener: kernelListener(t),
	}
	if edit != nil {
		edit(&o)
	}
	ready := make(chan *Front, 1)
	o.Ready = func(f *Front) { ready <- f }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, o) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("Run: %v", err)
				}
			case <-time.After(30 * time.Second):
				t.Error("Run did not return 30 s after its context ended")
			}
		})
	}
	t.Cleanup(stop)
	select {
	case f := <-ready:
		if f.Tunnel == nil || f.Prober == nil {
			t.Fatal("Run built no Tunnel or no Prober")
		}
		return f, stop
	case err := <-done:
		once.Do(func() { cancel() }) // Run has finished: the stop must not wait for it
		t.Fatalf("Run returned early: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("the front did not start")
	}
	return nil, nil
}

// TestRunRaisesPathProbeFailingThroughItsRecorder (HBEP-6, Reporting; PR #129
// review, B1): with no Options.PathEvents, as the command runs it, the
// front's own probe raises the degraded event, reason PathProbeFailing,
// regarding the Instance, on the Recorder it was given. The ready Instance,
// TunnelConnected True, passes, then fails twice: one event, and no more as
// further probes fail.
func TestRunRaisesPathProbeFailingThroughItsRecorder(t *testing.T) {
	m := startMuxDouble(t)
	in := probeInstance("00000000-0000-4000-8000-000000000051", v1alpha1.InstanceReady, metav1.ConditionTrue)
	m.set(in.UID, http.StatusOK)
	rec := &MemoryRecorder{}
	f, _ := runProbeFront(t, m, func(o *Options) {
		o.Recorder = rec
		o.Config.ProbeInterval = Duration(20 * time.Millisecond)
	}, in)
	waitState(t, "the first passing probe", func() bool { return f.Tunnel.Registered(in.UID) })
	m.set(in.UID, http.StatusServiceUnavailable)
	failing := func() []RecordedEvent { return rec.Find(eventDegraded, reasonPathProbeFailing) }
	waitState(t, "the PathProbeFailing event", func() bool { return len(failing()) > 0 })
	hits := m.hit(in.UID)
	waitState(t, "three more failed probes", func() bool { return m.hit(in.UID) >= hits+3 })
	ev := failing()
	if len(ev) != 1 {
		t.Fatalf("%d PathProbeFailing events, want 1: %+v", len(ev), ev)
	}
	if ev[0].Object != "*v1alpha1.Instance ns/"+in.Name || !ev[0].Warning || !strings.Contains(ev[0].Message, "2 times") || !strings.Contains(ev[0].Message, "503") {
		t.Errorf("event %+v: want a warning regarding the Instance, naming 2 failures and the 503", ev[0])
	}
	if f.Tunnel.Registered(in.UID) {
		t.Error("the Instance is still registered after its probe failed")
	}
}

// probeHealthOf reads /healthz's probe member.
func probeHealthOf(t *testing.T, f *Front) probeHealth {
	t.Helper()
	resp, err := http.Get("http://" + f.Health.String() + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		Probe *probeHealth `json:"probe"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || body.Probe == nil {
		t.Fatalf("/healthz: %v, probe member %v", err, body.Probe)
	}
	return *body.Probe
}

// TestProbeHeartbeatMissedWhenStaleThenHourly (HBEP-8, Observers and
// Repeating conditions; PR #129 review, B2): the probe's last result at t0,
// probeInterval 10 s. At exactly three intervals it is not yet stale; just
// past them HeartbeatMissed goes out once, failing, regarding the front's
// Lease, and /healthz shows the Instance's probe state as unknown while its
// registration stays. No more within the hour; at the hour, the next. A round
// that ends clears it, so the next staleness raises its event at once. With
// no Lease held, nothing goes out.
func TestProbeHeartbeatMissedWhenStaleThenHourly(t *testing.T) {
	m := startMuxDouble(t)
	var mu sync.Mutex
	insts := []v1alpha1.Instance{probeInstance("00000000-0000-4000-8000-000000000061", v1alpha1.InstanceReady, metav1.ConditionTrue)}
	in := insts[0]
	m.set(in.UID, http.StatusOK)
	clk := clock.NewFake(t0)
	p, tun, _ := newTestProber(t, m, clk, &insts, &mu)
	rec := &MemoryRecorder{}
	lease := &leaseRef{}
	lease.set(true, "lease-uid")
	missed := func() int { return len(rec.Find(eventFailing, reasonHeartbeatMissed)) }
	check := func(step string, want int) {
		t.Helper()
		p.raiseHeartbeatMissed(rec, lease)
		if n := missed(); n != want {
			t.Fatalf("%s: %d HeartbeatMissed events, want %d", step, n, want)
		}
	}
	state := func() (string, instanceProbeHealth) {
		h := p.Health()
		return h.State, h.Instances["ns/"+in.Name]
	}

	p.Round(context.Background())
	if s, ih := state(); s != "ok" || ih.State != "passing" || !ih.Registered || ih.UID != string(in.UID) {
		t.Fatalf("after a passing probe: probe %s, Instance %+v; want ok, passing, registered", s, ih)
	}
	clk.Advance(30 * time.Second)
	check("at exactly three intervals", 0)
	if s, ih := state(); s != "ok" || ih.State != "passing" {
		t.Errorf("at exactly three intervals: probe %s, Instance %s; want ok, passing", s, ih.State)
	}
	clk.Advance(time.Millisecond)
	check("just past three intervals", 1)
	ev := rec.Find(eventFailing, reasonHeartbeatMissed)[0]
	if ev.Object != leaseDesc || !ev.Warning || !strings.Contains(ev.Message, usage.Time(t0)) {
		t.Errorf("event %+v: want a warning regarding %s naming the last result, %s", ev, leaseDesc, usage.Time(t0))
	}
	if s, ih := state(); s != "stale" || ih.State != "unknown" || !ih.Registered || ih.LastResult != usage.Time(t0) {
		t.Errorf("while stale: probe %s, Instance %+v; want stale, unknown, still registered, last result %s", s, ih, usage.Time(t0))
	}
	if !tun.Registered(in.UID) {
		t.Error("staleness took the registration: only failed probes change eligibility")
	}
	for i := 0; i < 359; i++ { // 59 min 50 s after the event, in beats
		clk.Advance(10 * time.Second)
		check("within the hour", 1)
	}
	clk.Advance(10 * time.Second)
	check("an hour after the event", 2)

	p.Round(context.Background())
	check("after a fresh result", 2)
	if s, ih := state(); s != "ok" || ih.State != "passing" {
		t.Errorf("after a fresh result: probe %s, Instance %s; want ok, passing", s, ih.State)
	}
	clk.Advance(30*time.Second + time.Millisecond)
	check("a new staleness, within the hour of the last event", 3)

	lease.set(false, "")
	clk.Advance(time.Hour)
	check("with no Lease held", 3)
}

// TestRunReportsAStalledProbeAndItsReturn (HBEP-8, Observers; PR #129
// review, B2) through Run, on the fake clock, with a Lease: the first round
// passes at t0, and the second is held at GET /health, so the probe gives no
// result. The Lease keeper judges the heartbeat on its 10 s beats: nothing at
// 10, 20 and 30 s, HeartbeatMissed at 40 s, none again at 50 s. Meanwhile
// /healthz shows the probe stale and the Instance unknown, still registered.
// Released, the round ends and /healthz shows it passing again.
func TestRunReportsAStalledProbeAndItsReturn(t *testing.T) {
	m := startMuxDouble(t)
	in := probeInstance("00000000-0000-4000-8000-000000000071", v1alpha1.InstanceReady, metav1.ConditionTrue)
	m.set(in.UID, http.StatusOK)
	clk := clock.NewFake(t0)
	rec := &MemoryRecorder{}
	cs := kubefake.NewSimpleClientset()
	f, _ := runProbeFront(t, m, func(o *Options) {
		o.Clock, o.Recorder, o.Kube = clk, rec, cs
		o.Config.ProbeTimeout = Duration(5 * time.Minute) // the held probe outlasts the test
	}, in)
	leaseHeld := func() bool {
		_, err := cs.CoordinationV1().Leases(leaseNS).Get(context.Background(), leaseObj, metav1.GetOptions{})
		return err == nil
	}
	// Two timers wait: the probe's next round and the keeper's next beat.
	waitState(t, "the first passing probe, the Lease and both timers", func() bool {
		return f.Tunnel.Registered(in.UID) && leaseHeld() && clk.Pending() == 2
	})
	held, release := m.holdNext(t, in.UID, "health")
	missed := func() int { return len(rec.Find(eventFailing, reasonHeartbeatMissed)) }
	// beat moves the clock to the next beat and waits for the keeper to
	// re-arm, which it does after the beat's check; the held round arms no
	// timer.
	beat := func(want int) {
		t.Helper()
		clk.Advance(10 * time.Second)
		waitState(t, "the keeper's beat at "+clk.Now().Sub(t0).String(), func() bool { return clk.Pending() == 1 })
		if n := missed(); n != want {
			t.Fatalf("at %s: %d HeartbeatMissed events, want %d", clk.Now().Sub(t0), n, want)
		}
	}
	clk.Advance(10 * time.Second)
	select {
	case <-held:
	case <-time.After(30 * time.Second):
		t.Fatal("the second round never reached GET /health")
	}
	waitState(t, "the keeper's beat at 10s", func() bool { return clk.Pending() == 1 })
	beat(0) // 20 s
	beat(0) // 30 s: three intervals, not older
	beat(1) // 40 s
	ev := rec.Find(eventFailing, reasonHeartbeatMissed)[0]
	if ev.Object != leaseDesc || !ev.Warning {
		t.Errorf("event %+v: want a warning regarding %s", ev, leaseDesc)
	}
	h := probeHealthOf(t, f)
	if ih := h.Instances["ns/"+in.Name]; h.State != "stale" || ih.State != "unknown" || !ih.Registered {
		t.Errorf("/healthz while stale: probe %s, Instance %+v; want stale, unknown, registered", h.State, ih)
	}
	if !f.Tunnel.Registered(in.UID) {
		t.Error("staleness took the registration")
	}
	beat(1) // 50 s: within the hour
	release()
	waitState(t, "/healthz to show the probe passing after the held round ended", func() bool {
		h := probeHealthOf(t, f)
		return h.State == "ok" && h.Instances["ns/"+in.Name].State == "passing"
	})
}
