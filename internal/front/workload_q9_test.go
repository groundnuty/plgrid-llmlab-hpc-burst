package front

import (
	"strings"
	"testing"
	"time"
)

// The science agent's ruling Q9 on #764 (third case): a full managed target, with none of our Instances eligible, means waiting in the Workload's FIFO,
// then 503 capacity, never no_ready_target. Our only Instance is paused (three failed connections, HBEP-5, Pauses), so it is not eligible; the managed
// target takes the first request and the Workload's managedSessions 1 is then reached.
func TestAFullManagedTargetWithNoneOfOursEligibleWaitsInTheWorkloadsFifoThenGets503Capacity(t *testing.T) {
	f := newWrFix(t, wrOpts{stallManaged: true, managedSessions: 1, edit: func(c *Config) { c.QueueTimeout = Duration(300 * time.Millisecond) }})
	for i := 0; i < f.cfg.PauseAfterFailures; i++ {
		f.router.ReportConnect("ns/a-0", true)
	}
	if !f.router.Paused("ns/a-0") {
		t.Fatalf("ns/a-0 is not paused after %d failed connections: the fixture has no ineligible Instance", f.cfg.PauseAfterFailures)
	}
	f.start(f.alpha(), "q9-1")
	f.settled(1, 0, 0) // none of ours is eligible: the managed target takes it
	waiter, _ := f.start(f.alpha(), "q9-2")
	resp, body := waiter.wait(t) // the bound is reached and nothing of ours is eligible: it waits, and after queueTimeout it is refused
	checkError(t, resp, body, wantError{status: 503, code: "capacity", retryAfter: "10"})
	if !strings.Contains(body, "Workload alpha") {
		t.Errorf("the error does not name the Workload alpha: %s", body)
	}
	rec := f.record(resp.Header.Get("hpcburst-request-id"))
	wantWorkload(t, "refused after the wait", rec, str("alpha"))
	if rec.Status != 503 || rec.ModelDeployment != "" {
		t.Errorf("record: status %d modelDeployment %q, want 503 and none", rec.Status, rec.ModelDeployment)
	}
	if got := f.arrivedManaged(); got != 1 {
		t.Errorf("the LLMLab double has had %d requests, want 1: the waiter was never placed", got)
	}
}
