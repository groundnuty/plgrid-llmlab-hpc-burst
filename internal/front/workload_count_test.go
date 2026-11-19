package front

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/naming"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/managed/llmlabtest"
)

// The Workload's managed count under concurrency (HBEP-5, Workloads, Route order: managedSessions bounds the requests of a Workload in flight on managed
// targets, all of its managed targets together; the science agent's rulings Q6 and Q13 on #764). The count is reserved under the Workload's mutex in the same
// critical section as the check of the bound, and given back exactly once however the request ends. Every expected number is typed by hand from the rule it
// names, and where a request went is read from the LLMLab double's own count of arrivals and from the response's attribution.

// wcInFlight is the in-flight series (HBEP-8, Front metrics): its managed samples carry the Workload, the managed Provider, the managed-source
// ModelDeployment and an empty instance_uid.
const wcInFlight = "hpcburst_front_in_flight"

// wcWaitInFlight waits until the managed route of alpha through provider and deployment reads want in the in-flight series. A series read twice never
// moves for a request that is still held, so the wait is on the series itself, not on a window.
func wcWaitInFlight(t *testing.T, f *wrFix, provider, deployment string, want float64) {
	t.Helper()
	errWaitFor(t, fmt.Sprintf("the in-flight series of %s on %s to read %v", provider, deployment, want), func() bool {
		v, ok := find(t, scrape(t, f.metrics), wcInFlight, "workload", "alpha", "provider", provider, "model_deployment", deployment, "instance_uid", "")
		return ok && v == want
	})
}

// wcWaitCount waits until alpha's count of requests in flight on managed targets reads want.
func wcWaitCount(t *testing.T, f *wrFix, want int) {
	t.Helper()
	errWaitFor(t, fmt.Sprintf("alpha's managed count to read %d", want), func() bool { return f.managedCount() == want })
}

// N+1 at once (Route order, ManagedFirst with managedSessions 3): four requests of alpha sent together, the bound is 3, so exactly three reach the LLMLab
// double, where they are held, and the fourth is answered by a-0.
func TestFourRequestsAtOnceOnABoundOfThreeSendThreeToTheManagedTargetAndOneToOurInstance(t *testing.T) {
	f := newWrFix(t, wrOpts{managedFirst: true, managedSessions: 3, stallManaged: true})
	start := make(chan struct{})
	launched := make(chan *scCall, 4)
	for i := 0; i < 4; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		go func(session string) {
			<-start // the four goroutines send together, when the test closes the gate
			launched <- tbCall(f.harness, ctx, errChatBody(f.alpha(), false, "hello there"), "x-session-id", session)
		}(fmt.Sprintf("nplus1-%d", i))
	}
	close(start)
	calls := make([]*scCall, 0, 4)
	for i := 0; i < 4; i++ {
		calls = append(calls, <-launched)
	}

	answered := func() (n int) {
		for _, c := range calls {
			select {
			case <-c.done:
				n++
			default:
			}
		}
		return n
	}
	errWaitFor(t, "one of the four to be answered, by a-0", func() bool { return answered() >= 1 })
	f.settled(3, 0, 0) // three on the LLMLab double, none held on ours (the fourth was answered), nothing waits
	if n := answered(); n != 1 {
		t.Errorf("%d of the four requests are answered while the managed target holds the other three, want 1", n)
	}
	for _, c := range calls {
		select {
		case <-c.done:
			resp, body := c.wait(t)
			if resp.StatusCode != 200 || scInstanceOf(resp) != "ns/a-0" || scEmitterOf(resp) != "instance" {
				t.Errorf("the answered request: status %d, emitter %q, instance %q, want 200 from instance ns/a-0: %s", resp.StatusCode, scEmitterOf(resp), scInstanceOf(resp), body)
			}
		default:
		}
	}
	if got := f.arrivedManaged(); got != 3 {
		t.Errorf("the LLMLab double has had %d requests, want 3", got)
	}
	if got := f.managedCount(); got != 3 {
		t.Errorf("the Workload's count is %d, want 3", got)
	}
}

// The race at the last slot, forced (Q6: the check of the bound and the reservation are one critical section under the Workload's mutex). managedSessions 1.
// R1 passes the check, and the hook afterManagedCheck stops it there, holding alpha's mutex and not yet counted. R2 arrives and finds the mutex held
// (onWlContended says so), so it cannot read the count before R1 has reserved it. With the gate open R1 reserves and goes to the managed target, and R2,
// which now finds the bound reached, goes to a-0. Had the check and the reservation been two critical sections, R2 would pass the check with R1 and
// both would go to LLMLab: it never finds the mutex held, and the row fails there.
func TestTwoRequestsAtTheLastManagedSlotCannotBothTakeIt(t *testing.T) {
	f := newWrFix(t, wrOpts{managedFirst: true, managedSessions: 1, stallManaged: true})
	checked, gate, contended := make(chan struct{}), make(chan struct{}), make(chan struct{}, 8)
	var first, once atomic.Bool
	t.Cleanup(func() {
		if once.CompareAndSwap(false, true) {
			close(gate)
		}
	})
	f.router.afterManagedCheck = func() { // under alpha's mutex, after the bound was checked: the first caller only
		if first.CompareAndSwap(false, true) {
			close(checked)
			<-gate
		}
	}
	f.router.onWlContended = func() {
		select {
		case contended <- struct{}{}:
		default:
		}
	}

	r1, _ := f.start(f.alpha(), "last-1")
	select {
	case <-checked:
	case <-time.After(5 * time.Second):
		t.Fatal("R1 did not reach the managed bound's check")
	}
	r2, _ := f.start(f.alpha(), "last-2")
	select {
	case <-contended:
	case <-time.After(5 * time.Second):
		t.Fatal("R2 never found alpha's mutex held while R1 stood between the check and the reservation: the check and the reservation are not one critical section")
	}

	if once.CompareAndSwap(false, true) {
		close(gate)
	}
	resp, body := r2.wait(t) // R2 is the one answered, by a-0, since R1 holds the one managed slot
	if resp.StatusCode != 200 || scInstanceOf(resp) != "ns/a-0" || scEmitterOf(resp) != "instance" {
		t.Errorf("R2: status %d, emitter %q, instance %q, want 200 from instance ns/a-0: %s", resp.StatusCode, scEmitterOf(resp), scInstanceOf(resp), body)
	}
	f.settled(1, 0, 0) // exactly one went to the LLMLab double, and it is held there
	select {
	case <-r1.done:
		t.Errorf("R1 is answered, want it held on the managed target")
	default:
	}
	if got := f.managedCount(); got != 1 {
		t.Errorf("the Workload's count is %d, want 1", got)
	}
}

// Released exactly once, each way a managed request can end. Each row holds one request on the managed target and reads the count and the in-flight series
// at 1, ends the request, and requires both back at 0: a count released twice would read -1 and one never released would stay at 1.

// (a) A completion.
func TestAManagedRequestThatCompletesGivesItsCountBackOnce(t *testing.T) {
	f := newWrFix(t, wrOpts{managedFirst: true, managedSessions: 1, stallManaged: true})
	c, _ := f.start(f.alpha(), "once-complete")
	f.settled(1, 0, 0)
	wcWaitCount(t, f, 1)
	wcWaitInFlight(t, f, "llm-a", "ns/md-llm", 1)

	f.unstallManaged() // the LLMLab double answers
	if resp, body := c.wait(t); resp.StatusCode != 200 || scEmitterOf(resp) != "provider" {
		t.Fatalf("the request: status %d, emitter %q, want 200 from the provider: %s", resp.StatusCode, scEmitterOf(resp), body)
	}
	wcWaitCount(t, f, 0)
	wcWaitInFlight(t, f, "llm-a", "ns/md-llm", 0)
}

// (b) A client cut while the request is held.
func TestAManagedRequestCutByItsClientWhileHeldGivesItsCountBackOnce(t *testing.T) {
	f := newWrFix(t, wrOpts{managedFirst: true, managedSessions: 1, stallManaged: true})
	_, cut := f.start(f.alpha(), "once-cut")
	f.settled(1, 0, 0)
	wcWaitCount(t, f, 1)
	wcWaitInFlight(t, f, "llm-a", "ns/md-llm", 1)

	cut()
	wcWaitCount(t, f, 0)
	wcWaitInFlight(t, f, "llm-a", "ns/md-llm", 0)
}

// (c) An error: the LLMLab double answers 400 for a model it has blocked (an unknown model and a grant-blocked one read alike, L-6.5), or 503 for one it
// has made inactive. A managed target's answer of any status is relayed and its request ends.
func TestAManagedRequestAnsweredWithAnErrorGivesItsCountBackOnce(t *testing.T) {
	for name, c := range map[string]struct {
		set    func(f *wrFix)
		status int
	}{
		"blocked model":  {func(f *wrFix) { f.llm.SetBlocked(wrCatalog, true) }, 400},
		"inactive model": {func(f *wrFix) { f.llm.SetActive(wrCatalog, false) }, 503},
	} {
		t.Run(name, func(t *testing.T) {
			f := newWrFix(t, wrOpts{managedFirst: true, managedSessions: 1})
			c.set(f)
			call, _ := f.start(f.alpha(), "once-error")
			resp, body := call.wait(t)
			if resp.StatusCode != c.status {
				t.Fatalf("status %d, want %d from the LLMLab double: %s", resp.StatusCode, c.status, body)
			}
			if got := f.arrivedManaged(); got != 1 {
				t.Errorf("the LLMLab double has had %d requests, want 1", got)
			}
			wcWaitCount(t, f, 0)
			wcWaitInFlight(t, f, "llm-a", "ns/md-llm", 0)
		})
	}
}

// (d) A retry. Two managed targets (llm-a under md-llm, llm-b under md-llm-b), ManagedFirst with managedSessions 1, and the first target's double is closed,
// so the dial to it fails and the front sends the request again (HBEP-5, open question 5: the dial refused goes to the next target at once; Q13). The
// count must be given back BEFORE the retry reserves again: with the bound at 1, a count still held would keep the retry off the second managed target,
// and it would go to a-0. The session is homed on llm-a on the ring of the two managed targets, so llm-a is the first choice. The retry's target is the
// second double, typed.
func TestARetryAfterAFailedDialGivesTheCountBackBeforeItReservesAgain(t *testing.T) {
	f := newWrFix(t, wrOpts{managedFirst: true, managedSessions: 1})
	llmB := llmlabtest.Start(t, llmlabtest.Options{PortLo: testPortLo, PortHi: testPortHi, Keys: []string{wrKey}, Models: []llmlabtest.Model{{ID: wrCatalog}}})
	llmB.Stall(true) // the retry is held on it, so where it went can be read
	t.Cleanup(func() { llmB.Stall(false) })
	f.managedProvider("llm-b", "llmlab", "llmlab", llmB.BaseURL(), wrKey, wrCatalog)
	f.managedMD("md-llm-b", scModel, "llmlab", wrCatalog, "llm-b")
	f.apply()
	f.llm.Close() // llm-a's double is gone: connections to it are refused

	// The managed targets' keys are namespace/name@provider (inventory.ManagedTarget.Key); a session homed on the first is its first choice.
	session := scKeys(t, []string{"ns/md-llm@llm-a", "ns/md-llm-b@llm-b"}, "ns/md-llm@llm-a", 1)[0]
	call, cut := f.start(f.alpha(), session)
	errWaitFor(t, "the retry to reach the second managed target", func() bool { return llmB.Arrived("POST", scChat) == 1 })
	wcWaitInFlight(t, f, "llm-b", "ns/md-llm-b", 1)
	if got := len(f.engines["ns/a-0"].Requests()); got != 0 {
		t.Errorf("a-0 has received %d requests, want 0: the retry took the second managed target, the count of the first having been given back", got)
	}
	select {
	case <-call.done:
		t.Errorf("the request is answered, want it held on the second managed target")
	default:
	}
	if got := f.managedCount(); got != 1 {
		t.Errorf("the Workload's count is %d, want 1 (the retry's)", got)
	}
	wcWaitInFlight(t, f, "llm-a", "ns/md-llm", 0) // the first attempt's unit is back

	cut()
	wcWaitCount(t, f, 0)
	wcWaitInFlight(t, f, "llm-a", "ns/md-llm", 0)
	wcWaitInFlight(t, f, "llm-b", "ns/md-llm-b", 0)
}

// (e) The once-guard, directly: a managed target chosen for a named request carries one unit of the count, and releasing it twice gives back one, never
// two (the count never reads -1).
func TestAManagedTargetsReleaseIsOnceWhateverTheCallsToIt(t *testing.T) {
	f := newWrFix(t, wrOpts{managedFirst: true, managedSessions: 2, stallManaged: true})
	n, err := naming.Parse(f.alpha())
	if err != nil {
		t.Fatal(err)
	}
	tg, rerr := f.router.choose(context.Background(), n, "once-guard", nil)
	if rerr != nil {
		t.Fatalf("choose: %+v", rerr)
	}
	if tg.managed == nil || tg.managed.Provider != "llm-a" {
		t.Fatalf("the choice is %+v, want the managed target of llm-a (ManagedFirst, count 0 of 2)", tg)
	}
	if got := f.managedCount(); got != 1 {
		t.Fatalf("the Workload's count is %d after the choice, want 1", got)
	}
	wcWaitInFlight(t, f, "llm-a", "ns/md-llm", 1)

	tg.release()
	tg.release()
	if got := f.managedCount(); got != 0 {
		t.Errorf("the Workload's count is %d after two releases, want 0", got)
	}
	ss := scrape(t, f.metrics)
	wantSample(t, ss, 0, wcInFlight, "workload", "alpha", "provider", "llm-a", "model_deployment", "ns/md-llm", "instance_uid", "")
}
