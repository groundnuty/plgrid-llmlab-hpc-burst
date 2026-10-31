package front

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// HBEP-5, Test plan, Bound and cap, at the front: S10c's hand-worked case. A
// cap of 2 on three Instances. The sequence below was worked from LoadOK and
// the HBEP text, and it is the one balancer/cap_test.go works at the group
// level (r1 to r7, then a release places the waiter on H); here it goes through
// the front, with real requests held at engine doubles. Below full membership no request spills; at
// full membership one spills; when all are full, requests wait at the front
// and a completion places them. A run where the cap never binds does not
// count as a pass. During a handover's overlap (ready above desired), the
// tight bound and the cap stay.
//
// The rule, from HBEP-5 (Load bound and cap) and the KubeAI group it copies
// (LoadOK in balancer/group.go, upstream's chwblLoadOK): the group walks the
// ring from the key's home and takes the first member whose load before the
// request is at most c * (T+1) / n, where T is the group's in-flight total
// before the request and n its members; c is 1.25 (tight) at ready >= desired
// and 2.0 (loose, no cap) below. If the member chosen holds its cap, the
// request goes to the next member on the ring below its cap; if there is none,
// it waits at the front.
//
// Every request of a run carries one x-session-id, so they share one walk
// W[0] (the key's home), W[1], W[2], ..., worked from balancer.NewRing
// (scWalk). Each request is held open at its engine (Behaviour.Hold), so the
// loads persist, and the request reaches its engine before the next is sent.
// Placement is read from the engines, which saw the request, and the
// Hpcburst-Instance headers are checked against it once the requests end.

// capRun sends requests of one session key and holds each at its engine.
type capRun struct {
	t      *testing.T
	f      *scFix
	key    string
	holds  []chan struct{}
	closed []bool
	calls  []*scCall
	at     []int // the engine each request reached, in order
}

func newCapRun(t *testing.T, f *scFix, key string) *capRun {
	r := &capRun{t: t, f: f, key: key}
	t.Cleanup(r.releaseAll) // runs before the front and the engines close
	return r
}

// send sends one more request and returns the engine it reached, once it has.
func (r *capRun) send() int {
	r.t.Helper()
	before := r.f.counts()
	ch := make(chan struct{})
	r.holds = append(r.holds, ch)
	r.closed = append(r.closed, false)
	r.f.setAll(enginetest.Behaviour{Hold: ch}) // one channel per request: releasing it ends that request only
	r.calls = append(r.calls, scGo(r.f.harness, errChatBody(scModel, false, "hello there"), "x-session-id", r.key))
	n := len(r.calls)
	errWaitFor(r.t, fmt.Sprintf("request %d to reach an engine", n), func() bool { return r.f.arrived() == n })
	for i, c := range r.f.counts() {
		if c != before[i] {
			r.at = append(r.at, i)
			return i
		}
	}
	r.t.Fatalf("request %d reached no engine", n)
	return -1
}

// release ends request k (0-based).
func (r *capRun) release(k int) {
	if !r.closed[k] {
		r.closed[k] = true
		close(r.holds[k])
	}
}

func (r *capRun) releaseAll() {
	for k := range r.holds {
		r.release(k)
	}
}

// expect sends len(want) requests and fails at the first that reaches another
// Instance than want says (a later one would be in another state).
func (r *capRun) expect(want []string) {
	r.t.Helper()
	for _, w := range want {
		if got := r.f.names[r.send()]; got != w {
			r.t.Fatalf("request %d of the run reached %s, want %s (placements so far %v)", len(r.at), got, w, r.placed())
		}
	}
}

// placed names the Instances the requests reached.
func (r *capRun) placed() []string {
	out := make([]string, len(r.at))
	for i, a := range r.at {
		out[i] = r.f.names[a]
	}
	return out
}

// finish releases every request and checks each response: 200, from the
// Instance whose engine held it.
func (r *capRun) finish() {
	r.t.Helper()
	r.releaseAll()
	for k, c := range r.calls {
		resp, body := c.wait(r.t)
		if resp.StatusCode != 200 || scInstanceOf(resp) != r.f.names[r.at[k]] {
			r.t.Errorf("request %d: status %d instance header %q, want 200 from %s (the engine that held it): %s",
				k+1, resp.StatusCode, scInstanceOf(resp), r.f.names[r.at[k]], body)
		}
	}
}

func capOpts(cap int32, edit func(*Config)) scOpts {
	return scOpts{cfg: edit, md: func(md *v1alpha1.ModelDeployment) { md.Spec.Routing = &v1alpha1.Routing{InstanceCap: &cap} }}
}

// loads is the group's in-flight count of each member, by name; an Instance
// that is not in the group has no entry.
func (f *scFix) loads() map[string]int64 {
	out := map[string]int64{}
	g := f.scGroup(scDep)
	for _, n := range f.names {
		if l, ok := g.InFlight(n); ok {
			out[n] = l
		}
	}
	return out
}

// TestCapFullMembershipOneSpillsThenWaitsThenIsPlaced: three ready Instances,
// spec.instances 3, cap 2, c = 1.25. Hand work for one key, walk H, W1, W2
// (load before the request <= 1.25 * (T+1) / 3):
//
//	R1 T=0: the first request: H            -> H  (H=1)
//	R2 T=1: bound 0.833; H holds 1, W1 0    -> W1 (W1=1)
//	R3 T=2: bound 1.25;  H holds 1          -> H  (H=2)
//	R4 T=3: bound 1.667; H holds 2, W1 1    -> W1 (W1=2)
//	R5 T=4: bound 2.083; H holds 2: chosen, but at its cap 2. Spill: the next
//	        on the ring below its cap: W1 holds 2, W2 0 -> W2 (W2=1)
//	R6 T=5: bound 2.5;   H chosen at its cap; W1 full, W2 1 -> W2 (W2=2)
//	R7 T=6: bound 2.917; H chosen at its cap; all hold 2: waits at the front.
//
// Without the cap the same bound gives R5 -> H and R6 -> W1 (H=3, W1=3): W2
// would get nothing in these six. So W2 holding two requests is the cap
// binding, twice.
func TestCapFullMembershipOneSpillsThenWaitsThenIsPlaced(t *testing.T) {
	f := newScFix(t, scOpts{instances: 3, desired: 3, md: capOpts(2, nil).md})
	const key = "conv-cap"
	w := scWalk(f.names, key)
	r := newCapRun(t, f, key)

	r.expect([]string{w[0], w[1], w[0], w[1]}) // R1-R4: the bound alone spreads them; no spill yet
	if got := f.loads(); got[w[0]] != 2 || got[w[1]] != 2 || got[w[2]] != 0 {
		t.Fatalf("loads after R4 = %v, want H=2 W1=2 W2=0", got)
	}
	r.expect([]string{w[2], w[2]}) // R5, R6: H is chosen at its cap and the request spills to W2
	if got := f.loads(); got[w[0]] != 2 || got[w[1]] != 2 || got[w[2]] != 2 {
		t.Fatalf("loads after R6 = %v, want 2 on each", got)
	}
	// At least one spill happened: W2 got requests that no bound sent it.
	spills := 0
	for _, i := range r.at {
		if f.names[i] == w[2] {
			spills++
		}
	}
	if spills < 1 {
		t.Fatalf("no request spilled: the cap never bound, so this run proves nothing")
	}

	// R7: every Instance is at its cap, so the request waits at the front.
	f.setAll(enginetest.Behaviour{}) // once placed, it is answered at once
	waiter := scGo(f.harness, errChatBody(scModel, false, "hello there"), "x-session-id", key)
	errWaitFor(t, "R7 to wait at the front", func() bool { return f.router.Status().Waiting == 1 })
	if n := f.arrived(); n != 6 {
		t.Errorf("%d requests reached the engines while R7 waited, want the 6 held ones", n)
	}

	// A completion places it. R1 (on H) ends; H is the only Instance with room.
	r.release(0)
	resp, body := waiter.wait(t)
	if resp.StatusCode != 200 || scInstanceOf(resp) != w[0] {
		t.Errorf("R7: status %d instance %q, want 200 from %s, the one with room: %s", resp.StatusCode, scInstanceOf(resp), w[0], body)
	}
	if n := f.router.Status().Waiting; n != 0 {
		t.Errorf("%d requests still wait, want 0", n)
	}
	r.finish()
	f.scNoDrift()
}

// TestCapBelowFullMembershipNoRequestSpills: three Instances, spec.instances 3,
// cap 2, one Instance not ready. Ready is 2 of 3, so c = 2.0 and no cap is in
// force. The Instance that is not ready is the last on the key's walk (W2), so
// the other two are the key's home H and W1.
//
//   - Not joined (it was never ready; not routable, not in the group; n = 2):
//     the bound is 2.0 * (T+1) / 2 = T+1, and H holds T: every request goes to
//     H. Four requests put 4 on H, above the cap of 2. With the cap in force
//     R3 would have spilled to W1.
//   - Joined, then not ready (in the group but ineligible; T and n stay those of
//     the whole group, n = 3): R1 H; R2 T=1 bound 1.333, H holds 1 -> H; R3 T=2
//     bound 2.0, H holds 2 -> H (3, above the cap). R3 is where a cap in force
//     would have spilled.
func TestCapBelowFullMembershipNoRequestSpills(t *testing.T) {
	const key = "conv-cap"
	names := scNames(3)
	w := scWalk(names, key)
	notReady := -1
	for i, n := range names {
		if n == w[2] {
			notReady = i
		}
	}

	t.Run("an Instance that never became ready", func(t *testing.T) {
		o := capOpts(2, nil)
		o.instances, o.desired = 3, 3
		o.inst = func(i int, io *instanceOpts) {
			if i == notReady {
				io.phase = v1alpha1.InstanceVerifying
			}
		}
		f := newScFix(t, o)
		r := newCapRun(t, f, key)
		r.expect([]string{w[0], w[0], w[0], w[0]})
		if got := f.loads()[w[0]]; got != 4 {
			t.Errorf("H holds %d, want 4: no cap below full membership", got)
		}
		r.finish()
		f.scNoDrift()
	})
	t.Run("an Instance that went back to verifying", func(t *testing.T) {
		o := capOpts(2, nil)
		o.instances, o.desired = 3, 3
		f := newScFix(t, o)
		f.inst(f.uids[notReady]).Status.Phase = v1alpha1.InstanceVerifying
		f.apply()
		r := newCapRun(t, f, key)
		r.expect([]string{w[0], w[0], w[0]})
		if got := f.loads()[w[0]]; got != 3 {
			t.Errorf("H holds %d, want 3: no cap below full membership", got)
		}
		r.finish()
		f.scNoDrift()
	})
}

// TestCapHandoverOverlapKeepsTheTightBoundAndTheCap: four ready Instances for
// spec.instances 3 (a replacement joined before the old one drains), cap 2.
// Ready 4 >= desired 3, so c stays 1.25 and the cap stays. Walk H, W1, W2, W3;
// the load before the request must be <= 1.25 * (T+1) / 4:
//
//	R1 T=0 -> H(1)
//	R2 T=1 bound 0.625: H holds 1 no, W1 0  -> W1(1)
//	R3 T=2 bound 0.9375: H 1 no, W1 1 no, W2 0 -> W2(1)
//	R4 T=3 bound 1.25: H holds 1            -> H(2)
//	R5 T=4 bound 1.5625: H 2 no, W1 1       -> W1(2)
//	R6 T=5 bound 1.875: H 2, W1 2 no, W2 1  -> W2(2)
//	R7 T=6 bound 2.1875: H chosen at its cap; W1, W2 full, W3 0 -> W3(1)
//	R8 T=7 bound 2.5: H chosen at its cap; W3 holds 1 -> W3(2)
//	R9 T=8: H chosen at its cap; all hold 2: waits.
//
// A loose bound of 2.0 would send R3 to H (bound 1.5, H holds 1), and no cap
// would send R7 to H (H holds 2 <= 2.1875): W3 takes requests only when the
// tight bound and the cap are both in force.
func TestCapHandoverOverlapKeepsTheTightBoundAndTheCap(t *testing.T) {
	o := capOpts(2, func(c *Config) { c.QueueTimeout = Duration(300 * time.Millisecond) })
	o.instances, o.desired = 4, 3
	f := newScFix(t, o)
	const key = "conv-cap"
	w := scWalk(f.names, key)
	r := newCapRun(t, f, key)

	r.expect([]string{w[0], w[1], w[2], w[0], w[1], w[2], w[3], w[3]})
	for n, l := range f.loads() {
		if l != 2 {
			t.Errorf("%s holds %d after R8, want 2", n, l)
		}
	}
	// R9 finds every Instance at its cap and waits; with no cap it would have
	// been placed at once.
	resp := f.errPost(scChat, errChatBody(scModel, false, "hello there"), "x-session-id", key)
	body := readAll(t, resp)
	checkError(t, resp, body, wantError{status: 503, code: "capacity", retryAfter: "10"})
	r.finish()
	f.scNoDrift()
}

// TestCapQueueTimeout: when every Instance is at its cap, a request waits at
// the front, and past queueTimeout (300 ms here, on the wall clock) it gets 503
// capacity with Retry-After 10 (Errors, row 6), naming the front and the
// ModelDeployment; it never reaches an engine.
func TestCapQueueTimeout(t *testing.T) {
	o := capOpts(2, func(c *Config) { c.QueueTimeout = Duration(300 * time.Millisecond) })
	o.instances, o.desired = 3, 3
	f := newScFix(t, o)
	const key = "conv-cap"
	r := newCapRun(t, f, key)
	for i := 0; i < 6; i++ {
		r.send()
	}
	for n, l := range f.loads() {
		if l != 2 {
			t.Fatalf("%s holds %d, want 2: the run did not fill the Instances", n, l)
		}
	}

	call := scGo(f.harness, errChatBody(scModel, false, "hello there"), "x-session-id", key)
	errWaitFor(t, "the request to wait at the front", func() bool { return f.router.Status().Waiting == 1 })
	start := time.Now()
	resp, body := call.wait(t)
	waited := time.Since(start)
	checkError(t, resp, body, wantError{status: 503, code: "capacity", retryAfter: "10"})
	if waited > 5*time.Second {
		t.Errorf("the 503 came after another %v, want within a moment of queueTimeout (300 ms)", waited)
	}
	id := checkAttribution(t, resp, wantAttr{emitter: "front"})
	if rec := f.record(id); rec.Status != 503 || rec.Emitter != "front" || rec.ModelDeployment != scDep {
		t.Errorf("record: status %d emitter %q modelDeployment %q, want 503 front %s", rec.Status, rec.Emitter, rec.ModelDeployment, scDep)
	}
	if !strings.Contains(body, scDep) {
		t.Errorf("the error does not name the ModelDeployment %s: %s", scDep, body)
	}
	if n := f.arrived(); n != 6 {
		t.Errorf("%d requests reached the engines, want the 6 held ones", n)
	}
	if n := f.router.Status().Waiting; n != 0 {
		t.Errorf("%d requests still wait after the time-out", n)
	}
	r.finish()
}
