package front

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// HBEP-5, Workloads, The count in tokens, through the front: an Instance whose status carries kvCacheTokens has a token budget B, and with
// cap, N (the tokens charged to its Workload requests in flight) and u (its unnamed requests in flight) it takes
//
//	a Workload request   only when count < cap and cap * (N + c) + u * B <= cap * B
//	an unnamed request   only when count < cap and cap * N + (u + 1) * B <= cap * B
//
// The fixtures are one Instance, a-0, at routing.instanceCap 10 with kvCacheTokens 1,000 (one worker, so B = 1,000), and two Workloads:
//
//	long   typical context 400 + typical output 100 = c 500
//	short  typical context 100 + typical output 25  = c 125
//
// Every expected number below is worked from those two inequalities, in the comment beside it. The requests are held on the engine double until the
// test lets them go, and each is started only once the one before it has reached the engine or the queue, so which request is admitted is known,
// never a race. Waiting is read from the router's count of waiters and the engine's count of open requests, never from a window.

// tbCall is a chat request on its own goroutine that the test can cut by cancelling its context.
func tbCall(h *harness, ctx context.Context, body string, hdr ...string) *scCall {
	c := &scCall{done: make(chan struct{})}
	go func() {
		defer close(c.done)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url+scChat, strings.NewReader(body))
		if err != nil {
			c.err = err
			return
		}
		req.Header.Set("Authorization", "Bearer "+h.clientKey)
		req.Header.Set("Content-Type", "application/json")
		for i := 0; i+1 < len(hdr); i += 2 {
			req.Header.Set(hdr[i], hdr[i+1])
		}
		resp, err := errClient.Do(req)
		if err != nil {
			c.err = err
			return
		}
		b, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		c.resp, c.body = resp, string(b)
		if err != nil && err != io.ErrUnexpectedEOF {
			c.err = err
		}
	}()
	return c
}

// tbFix is md-a with the Instances named, at routing.instanceCap capN and the same kvCacheTokens on each, and the two Workloads. Its engines hold every
// request until release.
type tbFix struct {
	*pwFix
	hold    chan struct{}
	freed   bool
	cancels []context.CancelFunc
	calls   []*scCall
}

func newTbFix(t *testing.T, capN int32, kv int64, instances ...string) *tbFix {
	t.Helper()
	f := &tbFix{pwFix: newPwFix(t, pwDep{name: "md-a", instances: instances, cap: capN}), hold: make(chan struct{})}
	for _, w := range []struct {
		name     string
		ctx, out int32
	}{{"long", 400, 100}, {"short", 100, 25}} {
		o := f.workload("ns", w.name, scModel)
		o.Spec.Shape.ContextTokens.Typical, o.Spec.Shape.OutputTokens.Typical = w.ctx, w.out
	}
	for _, in := range instances {
		if kv > 0 {
			v := kv
			f.inst(f.uids["ns/"+in]).Status.KVCacheTokens = &v
		}
		f.engines["ns/"+in].Set(enginetest.Behaviour{Hold: f.hold})
	}
	f.apply()
	t.Cleanup(f.release)
	return f
}

// release lets every held request finish. Held requests that are placed later find the channel closed.
func (f *tbFix) release() {
	if !f.freed {
		f.freed = true
		close(f.hold)
	}
}

// start sends one request naming model (a Workload by suffix, or none) and returns the cancel that cuts it.
func (f *tbFix) start(model string) context.CancelFunc {
	f.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	f.t.Cleanup(cancel)
	f.cancels = append(f.cancels, cancel)
	f.calls = append(f.calls, tbCall(f.harness, ctx, errChatBody(model, false, "hello there"), "x-session-id", "tb-key"))
	return cancel
}

func (f *tbFix) open() int { return f.engines["ns/a-0"].Open() }

// settled waits until exactly open requests are open on a-0 and waiting are queued at the front: every request started is then in one of the two.
func (f *tbFix) settled(open, waiting int) {
	f.t.Helper()
	errWaitFor(f.t, "the requests to settle", func() bool { return f.open() == open && f.router.Status().Waiting == waiting })
}

// charged is the group's N and u for an Instance.
func (f *tbFix) charged(inst string) (n, u int64) {
	f.t.Helper()
	n, u, ok := f.scGroup("ns/md-a").Charged("ns/" + inst)
	if !ok {
		f.t.Fatalf("ns/%s is not a member of ns/md-a", inst)
	}
	return n, u
}

func (f *tbFix) wantCharged(inst string, wantN, wantU int64, when string) {
	f.t.Helper()
	if n, u := f.charged(inst); n != wantN || u != wantU {
		f.t.Fatalf("%s: %s holds N = %d, u = %d, want N = %d, u = %d", when, inst, n, u, wantN, wantU)
	}
}

// wantDrained waits for every charge to come back and checks that the counts agree.
func (f *tbFix) wantDrained(inst string) {
	f.t.Helper()
	errWaitFor(f.t, "every charge to be released", func() bool { n, u := f.charged(inst); return n == 0 && u == 0 && f.open() == 0 })
	f.scNoDrift()
}

// finish lets every request through and checks that each one that was not cut answered 200.
func (f *tbFix) finish(cut map[int]bool) {
	f.t.Helper()
	f.release()
	for i, c := range f.calls {
		if cut[i] {
			continue
		}
		if resp, body := c.wait(f.t); resp == nil || resp.StatusCode != 200 {
			status := 0
			if resp != nil {
				status = resp.StatusCode
			}
			f.t.Fatalf("request %d: status %d: %s", i, status, body)
		}
	}
}

// Long sessions. B = 1,000, cap 10, c = 500. The second long request fits exactly: 10 * (500 + 500) + 0 = 10,000 <= 10 * 1,000. The third does not:
// 10 * 1,500 = 15,000 > 10,000, with the count at 2 < 10, so it waits. When the first ends N is 500 again and the third takes its place.
func TestTwoLongSessionsFillAnInstanceAndTheThirdWaitsUntilOneEnds(t *testing.T) {
	f := newTbFix(t, 10, 1000, "a-0")
	long := scModel + "#long"
	cancelFirst := f.start(long)
	f.settled(1, 0)
	f.wantCharged("a-0", 500, 0, "one long request")
	f.start(long)
	f.settled(2, 0)
	f.wantCharged("a-0", 1000, 0, "two long requests: the budget is full")
	f.start(long)
	f.settled(2, 1)
	f.wantCharged("a-0", 1000, 0, "the third long request waits and reserves nothing")

	cancelFirst() // the first long request is cut by its client: its 500 tokens come back, and the waiter takes them
	f.settled(2, 0)
	f.wantCharged("a-0", 1000, 0, "the third long request placed")
	f.finish(map[int]bool{0: true})
	f.wantDrained("a-0")
}

// Short sessions on the same Instance. c = 125: the eighth fits exactly (10 * (875 + 125) = 10,000 <= 10,000), the ninth does not
// (10 * 1,125 = 11,250 > 10,000), with the count at 8 < 10: it is the budget that holds it, not the count.
func TestEightShortSessionsFillAnInstanceAndTheNinthWaits(t *testing.T) {
	f := newTbFix(t, 10, 1000, "a-0")
	short := scModel + "#short"
	for i := 1; i <= 8; i++ {
		f.start(short)
		f.settled(i, 0)
	}
	f.wantCharged("a-0", 1000, 0, "eight short requests")
	f.start(short)
	f.settled(8, 1)
	f.wantCharged("a-0", 1000, 0, "the ninth waits")
	f.finish(nil)
	f.wantDrained("a-0")
}

// The control: requests that name no Workload on the same Instance follow the count rule exactly. With N = 0 the unnamed condition is u + 1 <= 10:
// ten are admitted (the tenth: 10 * 0 + 10 * 1,000 = 10,000 <= 10,000) and the eleventh waits, as it would before the budget existed.
func TestRequestsThatNameNoWorkloadFollowTheCountRuleOnAnInstanceWithABudget(t *testing.T) {
	f := newTbFix(t, 10, 1000, "a-0")
	for i := 1; i <= 10; i++ {
		f.start(scModel)
		f.settled(i, 0)
	}
	f.wantCharged("a-0", 0, 10, "ten unnamed requests")
	f.start(scModel)
	f.settled(10, 1)
	f.finish(nil)
	f.wantDrained("a-0")
}

// Both kinds at once. One long request is in flight, N = 500. An unnamed request then needs 10 * 500 + (u + 1) * 1,000 <= 10,000, so u + 1 <= 5:
// five are admitted (the fifth: 5,000 + 5,000 = 10,000) and the sixth waits (5,000 + 6,000 = 11,000), with the count at 6 < 10. The count rule
// alone would have taken nine more.
func TestAnUnnamedRequestIsHeldWhenNLeavesNoRoomForItsShare(t *testing.T) {
	f := newTbFix(t, 10, 1000, "a-0")
	f.start(scModel + "#long")
	f.settled(1, 0)
	for i := 1; i <= 5; i++ {
		f.start(scModel)
		f.settled(1+i, 0)
	}
	f.wantCharged("a-0", 500, 5, "one long and five unnamed requests")
	f.start(scModel)
	f.settled(6, 1)
	f.wantCharged("a-0", 500, 5, "the sixth unnamed request waits")
	f.finish(nil)
	f.wantDrained("a-0")
}

// Both Workloads on one Instance. One long request (N = 500) and four short ones (c = 125 each) fill B: the fourth short one is
// 10 * (500 + 3 * 125 + 125) + 0 = 10 * 1,000 = 10,000 <= 10,000, admitted. The fifth is 10 * (1,000 + 125) = 11,250 > 10,000: it waits, with
// the count at 5 < 10.
func TestALongAndFourShortSessionsFillAnInstanceAndAFifthShortOneWaits(t *testing.T) {
	f := newTbFix(t, 10, 1000, "a-0")
	f.start(scModel + "#long")
	f.settled(1, 0)
	for i := 1; i <= 4; i++ {
		f.start(scModel + "#short")
		f.settled(1+i, 0)
	}
	f.wantCharged("a-0", 1000, 0, "one long and four short requests")
	f.start(scModel + "#short")
	f.settled(5, 1)
	f.wantCharged("a-0", 1000, 0, "the fifth short request waits")
	f.finish(nil)
	f.wantDrained("a-0")
}

// An Instance with no kvCacheTokens has no B: both kinds meet the count rule alone, whatever the charges are. Ten long requests (N = 5,000) and the
// eleventh waits only because the count is 10.
func TestAnInstanceWithoutKVCacheTokensMeetsTheCountRuleAlone(t *testing.T) {
	f := newTbFix(t, 10, 0, "a-0")
	long := scModel + "#long"
	for i := 1; i <= 10; i++ {
		f.start(long)
		f.settled(i, 0)
	}
	f.wantCharged("a-0", 5000, 0, "ten long requests with no B")
	f.start(long)
	f.settled(10, 1)
	f.finish(nil)
	f.wantDrained("a-0")
}

// Below full membership (ready below desired) the count cap is off, and with it the unnamed condition; a Workload request's condition stays, with the cap
// at the value it had. One Instance, cap 2, B = 100 (kvCacheTokens 100), and a Workload "mid" whose c is 60 (context 40, output 20). By hand:
//
//	desired 1, so full membership, the cap in force:
//	  mid #1: 2 * (0 + 60) + 0 = 120 <= 200: admitted. N = 60.
//	  unnamed: count 1 < 2, but 2 * 60 + 1 * 100 = 220 > 200: refused, so it waits. The count cap is in force.
//	desired 2, so partial membership, the count cap off:
//	  the waiting unnamed request is placed (its condition is off): u = 1.
//	  mid #2: 2 * (60 + 60) + 1 * 100 = 340 > 200: refused by its budget although the count cap is off: it waits.
//	  another unnamed request: admitted at once, with the count at 3 > cap 2: the count cap is off. u = 2.
func TestBelowFullMembershipTheUnnamedConditionIsOffAndAWorkloadRequestsStays(t *testing.T) {
	f := newTbFix(t, 2, 100, "a-0")
	mid := f.workload("ns", "mid", scModel)
	mid.Spec.Shape.ContextTokens.Typical, mid.Spec.Shape.OutputTokens.Typical = 40, 20
	f.apply()

	f.start(scModel + "#mid")
	f.settled(1, 0)
	f.wantCharged("a-0", 60, 0, "mid #1, full membership")
	f.start(scModel)
	f.settled(1, 1)
	f.wantCharged("a-0", 60, 0, "the unnamed request waits: its condition is in force at full membership")

	f.md("md-a").Spec.Instances = 2 // ready (1) is now below desired (2)
	f.apply()
	f.settled(2, 0)
	f.wantCharged("a-0", 60, 1, "partial membership: the unnamed request is placed")
	f.start(scModel + "#mid")
	f.settled(2, 1)
	f.wantCharged("a-0", 60, 1, "partial membership: mid #2 is refused by its budget")
	f.start(scModel)
	f.settled(3, 1)
	f.wantCharged("a-0", 60, 2, "partial membership: the count is 3 > cap 2 and nothing holds an unnamed request")
	f.finish(nil)
	f.wantDrained("a-0")
}

// A cut: the request's client goes away while it is held on the Instance, and its charge comes back once. With the budget full, a waiter that was
// refused takes it (the first test); here the cut is the only thing that happens to the one request.
func TestACutRequestReleasesItsChargeOnce(t *testing.T) {
	f := newTbFix(t, 10, 1000, "a-0")
	cancel := f.start(scModel + "#long")
	f.settled(1, 0)
	f.wantCharged("a-0", 500, 0, "one long request in flight")
	cancel()
	f.wantDrained("a-0")
	f.wantCharged("a-0", 0, 0, "after the cut")
	f.finish(map[int]bool{0: true})
}

// A retry: the first Instance closes the connection before any response byte, and the request is sent again to the other one, which holds it. The
// first charge is back (a-0 at 0) and the retry carries its own (a-1 at 500); when it finishes, both are 0. Both Instances serve the Workload,
// the session is homed on a-0 (the fork's own ring is the oracle, pwHome), and each has B = 1,000.
func TestARetryReleasesTheFirstChargeAndReservesItsOwnOnTheNextInstance(t *testing.T) {
	f := newTbFix(t, 10, 1000, "a-0", "a-1")
	names := []string{"ns/a-0", "ns/a-1"}
	session := ""
	for i := 0; i < 500 && session == ""; i++ {
		if k := "tb-retry-" + string(rune('a'+i%26)) + string(rune('a'+i/26%26)); pwHome(t, names, k) == "ns/a-0" {
			session = k
		}
	}
	if session == "" {
		t.Fatal("no session is homed on a-0")
	}
	f.engines["ns/a-0"].Set(enginetest.Behaviour{CloseBeforeHeaders: true})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	call := tbCall(f.harness, ctx, errChatBody(scModel+"#long", false, "hello there"), "x-session-id", session)

	errWaitFor(t, "the retry to be held on a-1", func() bool { return f.engines["ns/a-1"].Open() == 1 })
	f.wantCharged("a-0", 0, 0, "a-0 after the failed attempt: its charge was released")
	f.wantCharged("a-1", 500, 0, "a-1 holding the retry with its own charge")
	if got := len(f.engines["ns/a-0"].Requests()); got != 1 {
		t.Fatalf("a-0 saw %d requests, want the one that failed", got)
	}
	f.release()
	if resp, body := call.wait(t); resp == nil || resp.StatusCode != 200 || scInstanceOf(resp) != "ns/a-1" {
		t.Fatalf("the retried request: %v: %s", resp, body)
	}
	errWaitFor(t, "every charge to be released", func() bool {
		n0, u0, _ := f.scGroup("ns/md-a").Charged("ns/a-0")
		n1, u1, _ := f.scGroup("ns/md-a").Charged("ns/a-1")
		return n0 == 0 && u0 == 0 && n1 == 0 && u1 == 0
	})
	f.scNoDrift()
}
