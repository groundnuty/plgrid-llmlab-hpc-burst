package front

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
)

// A probe is over when its verdict is recorded, not when the mux double has counted its GET /health
// (#473). The double counts the hit on arrival, before it replies, and the probe still has to read the
// reply, clear its running flag and record the verdict. A test that drives the re-probe schedule on a
// fake clock and takes the hit for the end of the probe may advance the clock while the probe runs;
// the schedule then puts the due re-probe off by reprobeFirst (startReprobes) without using up a step,
// and nothing advances the clock again. This file holds the seam that makes that order happen on
// demand, and the test of what the schedule does in it.

// verdictHold holds a prober's probes just before their verdict is applied (Prober.beforeVerdict), the
// one place where a probe has read its reply, so that the double has counted its hit, and has not yet
// cleared its running flag or recorded anything. The hook is installed before the prober runs; a gate
// armed later holds the next probe that reaches it.
type verdictHold struct {
	mu    sync.Mutex
	gate  *verdictGate   // the armed gate, or nil
	gates []*verdictGate // every gate armed so far: release lets all of them go
}

// verdictGate is one hold: reached gets a value for each probe that arrives, and a probe waits on open.
type verdictGate struct {
	reached chan struct{}
	open    chan struct{}
	once    sync.Once
}

// newVerdictHold installs a hold on p. Call it before p runs: the probes read the hook.
func newVerdictHold(p *Prober) *verdictHold {
	h := &verdictHold{}
	p.beforeVerdict = func(types.UID) {
		h.mu.Lock()
		g := h.gate
		h.mu.Unlock()
		if g == nil {
			return
		}
		select {
		case g.reached <- struct{}{}:
		default:
		}
		<-g.open
	}
	return h
}

// arm makes the next probe that reaches its verdict wait there until release. A nil hold arms
// nothing and returns nil, so a test body can run with and without the hold.
func (h *verdictHold) arm() *verdictGate {
	if h == nil {
		return nil
	}
	g := &verdictGate{reached: make(chan struct{}, 8), open: make(chan struct{})}
	h.mu.Lock()
	h.gate = g
	h.gates = append(h.gates, g)
	h.mu.Unlock()
	return g
}

// release lets every held probe go, and disarms the hold. It does nothing when no gate was armed,
// and on a nil hold. It lets go the gates armed earlier too: a test that fails between two arms
// must not leave a probe held, or the prober's Run, which waits for its probes, never returns and
// the test's cleanup hangs with it.
func (h *verdictHold) release() {
	if h == nil {
		return
	}
	h.mu.Lock()
	gates := h.gates
	h.gate, h.gates = nil, nil
	h.mu.Unlock()
	for _, g := range gates {
		g.once.Do(func() { close(g.open) })
	}
}

// waitReached waits until a probe is held at the gate. The 5 s only ends a hang.
func (g *verdictGate) waitReached(t *testing.T, what string) {
	t.Helper()
	select {
	case <-g.reached:
	case <-time.After(5 * time.Second):
		t.Fatalf("no probe reached its held verdict: %s", what)
	}
}

// pollUntil polls cond every 5 ms until it holds. After each look that finds it false, each runs
// (when not nil). The 5 s deadline only ends a hang: cond is a state the code under test reaches by
// itself, never one that real time brings about. report adds what the failure message should say.
func pollUntil(t *testing.T, what string, cond func() bool, each func(), report func() string) {
	t.Helper()
	for end := time.Now().Add(5 * time.Second); !cond(); time.Sleep(5 * time.Millisecond) {
		if each != nil {
			each()
		}
		if time.Now().After(end) {
			extra := ""
			if report != nil {
				extra = " (" + report() + ")"
			}
			t.Fatalf("timed out waiting for %s%s", what, extra)
		}
	}
}

// isProbing reports whether a probe of the UID is running: its running flag is set from the moment
// it starts until probeOne has read its verdict.
func (p *Prober) isProbing(uid types.UID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	st := p.state[uid]
	return st != nil && st.running
}

// reprobeStep is the UID's place on the re-probe schedule: when its next attempt is due, and the
// wait after that attempt. ok is false when the UID is not on the schedule.
func (p *Prober) reprobeStep(uid types.UID) (next time.Time, wait time.Duration, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	r := p.awaiting[uid]
	if r == nil {
		return time.Time{}, 0, false
	}
	return r.next, r.wait, true
}

// TestReprobeDueWhileItsPreviousProbeStillRunsIsPutOffAndKeepsItsStep: the order that made the
// backoff test flaky under load, forced. A re-probe's verdict is held after the double has counted its
// GET /health, so the probe is certainly still running. The test then moves the fake clock to the next
// attempt's due time. The attempt starts no second probe and is put off by reprobeFirst, and it keeps
// its wait: the put-off uses up no step of the backoff. Once the held probe is recorded, the put-off
// attempt goes at its new time, and the schedule goes on from the wait it had. The expected times are
// worked out from the schedule (HBEP-6, the front's half): the attempt at once, then waits of 100 ms,
// 200 ms and 400 ms, each starting at the previous attempt.
func TestReprobeDueWhileItsPreviousProbeStillRunsIsPutOffAndKeepsItsStep(t *testing.T) {
	m := startMuxDouble(t)
	var mu sync.Mutex
	insts := []v1alpha1.Instance{probeInstance("00000000-0000-4000-8000-000000000046", v1alpha1.InstanceReady, metav1.ConditionTrue)}
	uid := insts[0].UID
	m.set(uid, http.StatusOK)
	clk := clock.NewFake(t0)
	p, tun, ev := newTestProber(t, m, clk, &insts, &mu)
	hold := newVerdictHold(p)
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() { p.Run(ctx); close(stopped) }()
	defer func() { hold.release(); cancel(); <-stopped }()
	wait := func(what string, cond func() bool) {
		t.Helper()
		pollUntil(t, what, cond, nil, func() string {
			next, w, _ := p.reprobeStep(uid)
			return "hits " + strconv.Itoa(m.hit(uid)) + ", recorded " + strconv.Itoa(ev.probes(uid)) + ", pending timers " + strconv.Itoa(clk.Pending()) +
				", clock +" + clk.Now().Sub(t0).String() + ", next re-probe +" + next.Sub(t0).String() + ", wait " + w.String()
		})
	}
	wait("the first round", func() bool { return ev.probes(uid) == 1 && tun.Registered(uid) && clk.Pending() == 1 })

	// The UID is registered again and its engine does not answer: the re-probe at once is held.
	m.set(uid, http.StatusServiceUnavailable)
	g := hold.arm()
	tun.Bump(uid)
	p.Reregistered(uid)
	g.waitReached(t, "the re-probe at once")
	if hits, recorded := m.hit(uid), ev.probes(uid); hits != 2 || recorded != 1 || !p.isProbing(uid) {
		t.Fatalf("held: %d hits, %d recorded, probing %v; want 2 hits (arrived), 1 recorded (not over), probing", hits, recorded, p.isProbing(uid))
	}
	wait("the next re-probe's timer", func() bool { return clk.Pending() == 2 })
	if next, w, ok := p.reprobeStep(uid); !ok || !next.Equal(t0.Add(reprobeFirst)) || w != 2*reprobeFirst {
		t.Fatalf("before the due time: next +%s, wait %s, on schedule %v; want next +100ms, wait 200ms", next.Sub(t0), w, ok)
	}

	// The clock reaches the due time while the probe still runs.
	clk.Advance(reprobeFirst)
	wait("the put-off", func() bool {
		next, _, _ := p.reprobeStep(uid)
		return next.Equal(t0.Add(2*reprobeFirst)) && clk.Pending() == 2
	})
	if hits := m.hit(uid); hits != 2 {
		t.Fatalf("%d probes after the due time with the first still running, want 2: a second probe of the UID started", hits)
	}
	if _, w, _ := p.reprobeStep(uid); w != 2*reprobeFirst {
		t.Fatalf("the put-off left the wait at %s, want 200ms: it used up a step of the backoff", w)
	}

	// The held probe ends, and is recorded; the put-off attempt then goes at its new time.
	hold.release()
	wait("the held probe's record", func() bool { return ev.probes(uid) == 2 && !p.isProbing(uid) })
	clk.Advance(reprobeFirst)
	wait("the put-off attempt", func() bool { return ev.probes(uid) == 3 && clk.Pending() == 2 })
	next, w, _ := p.reprobeStep(uid)
	if want := t0.Add(2*reprobeFirst + 2*reprobeFirst); !next.Equal(want) || w != 4*reprobeFirst {
		t.Fatalf("after the put-off attempt: next +%s, wait %s; want next +%s (200 ms after it), wait 400ms", next.Sub(t0), w, want.Sub(t0))
	}
	if tun.Registered(uid) {
		t.Fatal("a failing re-probe registered the UID")
	}
}
