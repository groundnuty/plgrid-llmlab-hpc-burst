package front

// A pause accumulated against an Instance's earlier registration is cleared once the Instance registers again in a new
// epoch and a probe passes there (#418; the science agent's ruling (a) of 11 Nov 2026; HBEP-5, Pauses). A pause from
// the current epoch holds for its full pauseDuration, and a new registration whose probe fails clears nothing. These run
// on the fake clock, over the Tunnel and a tcpmux double: no wait of pauseDuration decides any of them, and each run of
// failures is the pause rule's 3. The router's rebuilds are coalesced (#417), so a pass clears the pause at the rebuild
// that follows it: each test waits for the rebuilds to settle (quiesce, on the router's own state) before it reads the
// pause, with the fake clock frozen.

import (
	"context"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// epochFront is a harness whose router runs over a Tunnel to a tcpmux double, with a prober over the harness's snapshot,
// both on the harness's fake clock: one ready Instance of a served deployment, registered by its first probe.
type epochFront struct {
	*harness
	m      *muxDouble
	tun    *Tunnel
	prober *Prober
	uid    types.UID
	key    string
}

func newEpochFront(t *testing.T) *epochFront {
	t.Helper()
	h := newHarness(t, nil)
	h.profile("helios-gh200", v1alpha1.PlatformBatch, "", "")
	h.batchProvider("helios-a", "helios-gh200")
	h.servedMD("md", "example-27b", 1, "helios-a")
	uid := h.instance("md", "md-0", "helios-a", nil, newKey(t), instanceOpts{})
	m := startMuxDouble(t)
	m.set(uid, http.StatusOK)
	tun := NewTunnel(m.ln.Addr().String(), h.cfg.TunnelSetupTimeout.D())
	log := slog.New(slog.NewTextHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	h.router = NewRouter(h.cfg, h.clk, tun, h.rec, log, h.metrics)
	h.apply()
	f := &epochFront{harness: h, m: m, tun: tun, uid: uid, key: "ns/md-0"}
	f.prober = NewProber(h.cfg, tun, h.clk, func() []v1alpha1.Instance { return append([]v1alpha1.Instance(nil), h.snap.Instances...) }, nil, log)
	f.prober.Round(context.Background())
	if !tun.Registered(uid) {
		t.Fatal("the first probe did not register the Instance")
	}
	return f
}

// pauseIt reports the pause rule's run of connection failures toward the Instance, and requires it paused.
func (f *epochFront) pauseIt() {
	f.t.Helper()
	for i := 0; i < f.cfg.PauseAfterFailures; i++ {
		f.router.ReportConnect(f.key, true)
	}
	if f.cfg.PauseAfterFailures != 3 || !f.router.Paused(f.key) {
		f.t.Fatalf("after %d failures the Instance is not paused", f.cfg.PauseAfterFailures)
	}
}

// reregister is frps's new registration of the Instance's proxy, as the front's plugin hears it: a new epoch, the
// registration taken away, and a re-probe scheduled; then the re-probes that are due.
func (f *epochFront) reregister() {
	f.t.Helper()
	before := f.tun.Epoch(f.uid)
	f.tun.Bump(f.uid)
	f.prober.Reregistered(f.uid)
	if f.tun.Epoch(f.uid) == before || f.tun.Registered(f.uid) {
		f.t.Fatal("the new registration did not move the epoch and take the registration away")
	}
	f.prober.Reprobe(context.Background())
	f.router.quiesce(f.t)
}

// TestAPassInANewEpochClearsTheOldEpochsPause: a pause accumulated before the Instance registered again is cleared by
// the first probe that passes in the new registration, at once: the fake clock has not moved, so pauseDuration has not
// run out, and the Instance is eligible again with its failure count at 0.
func TestAPassInANewEpochClearsTheOldEpochsPause(t *testing.T) {
	f := newEpochFront(t)
	f.pauseIt()
	f.reregister()
	if !f.tun.Registered(f.uid) {
		t.Fatal("the re-probe in the new epoch did not register the Instance")
	}
	if f.router.Paused(f.key) || f.router.failures(f.key) != 0 {
		t.Fatalf("after a pass in the new epoch: paused %t, failures %d; want neither", f.router.Paused(f.key), f.router.failures(f.key))
	}
	if member, eligible, _ := f.routingOf(); !member || !eligible {
		t.Fatalf("after a pass in the new epoch: member %t, eligible %t; want both", member, eligible)
	}
}

// TestARunOfFailuresFromTheOldEpochIsClearedToo: two failures, short of a pause, are the old registration's as well:
// the new registration's first pass starts the run again, so a third failure does not pause it.
func TestARunOfFailuresFromTheOldEpochIsClearedToo(t *testing.T) {
	f := newEpochFront(t)
	f.router.ReportConnect(f.key, true)
	f.router.ReportConnect(f.key, true)
	f.reregister()
	if n := f.router.failures(f.key); n != 0 {
		t.Fatalf("the old registration's run of %d failures survived a pass in the new epoch", n)
	}
	f.router.ReportConnect(f.key, true)
	if f.router.Paused(f.key) || f.router.failures(f.key) != 1 {
		t.Fatalf("one failure in the new epoch: paused %t, failures %d; want a run of 1", f.router.Paused(f.key), f.router.failures(f.key))
	}
}

// TestAPauseFromTheCurrentEpochHoldsItsFullDuration: with no new registration, neither a rebuild nor a passing probe
// in the same epoch clears a pause: it holds to the second before pauseDuration ends, and lifts at it.
func TestAPauseFromTheCurrentEpochHoldsItsFullDuration(t *testing.T) {
	f := newEpochFront(t)
	f.pauseIt()
	f.prober.Round(context.Background())
	f.router.Refresh()
	if !f.tun.Registered(f.uid) || !f.router.Paused(f.key) {
		t.Fatalf("a pass in the same epoch: registered %t, paused %t; want the pause to stand", f.tun.Registered(f.uid), f.router.Paused(f.key))
	}
	d := f.cfg.PauseDuration.D()
	f.clk.Advance(d - time.Second)
	f.router.Refresh()
	if !f.router.Paused(f.key) {
		t.Fatalf("the pause from the current epoch lifted %s before its %s", time.Second, d)
	}
	f.clk.Advance(time.Second)
	if f.router.Paused(f.key) {
		t.Fatalf("the pause still holds at its %s", d)
	}
}

// TestANewRegistrationWhoseProbeFailsClearsNothing: frps registers the Instance again and its probes fail: the pause
// stands, through rebuilds and failing re-probes, because nothing registered the Instance in the new epoch. Once a probe
// passes there, it clears: the pass, not the new registration, is what clears.
func TestANewRegistrationWhoseProbeFailsClearsNothing(t *testing.T) {
	f := newEpochFront(t)
	f.pauseIt()
	f.m.set(f.uid, http.StatusServiceUnavailable)
	f.reregister()
	for i := 0; i < 3; i++ {
		f.router.Refresh()
		f.prober.Reprobe(context.Background())
	}
	f.router.quiesce(f.t)
	if f.tun.Registered(f.uid) || !f.router.Paused(f.key) {
		t.Fatalf("a new registration whose probes fail: registered %t, paused %t; want the pause to stand", f.tun.Registered(f.uid), f.router.Paused(f.key))
	}
	f.m.set(f.uid, http.StatusOK)
	f.prober.Round(context.Background())
	f.router.quiesce(f.t)
	if !f.tun.Registered(f.uid) || f.router.Paused(f.key) {
		t.Fatalf("a pass in the new epoch at last: registered %t, paused %t; want it registered and the pause cleared", f.tun.Registered(f.uid), f.router.Paused(f.key))
	}
}

// routingOf is the router's view of the Instance: a member of its group, and eligible now.
func (f *epochFront) routingOf() (member, eligible bool, inFlight int64) {
	tf := &tunFront{harness: f.harness}
	return tf.routing(f.uid)
}

// pauseUntil is when the target's pause ends; zero when it has none.
func (r *Router) pauseUntil(key string) time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p := r.pauses[key]; p != nil {
		return p.until
	}
	return time.Time{}
}
