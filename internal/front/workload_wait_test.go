package front

import (
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// A request that names a Workload and waits at the front for an Instance of the Workload's deployment (HBEP-5, Load bound and cap; the Workload's own
// FIFO is a later part of the build) keeps its Workload: served after the wait it is recorded under it, and refused after the wait with 503 capacity
// it is too, since the Workload had resolved. The wait is on observable state, the router's count of waiters, never on a window.

// waitFix is md-a with one Instance, a-0, at routing.instanceCap 1 and listing alpha, and one request of alpha held on it, so the Instance is at its
// cap. It returns the fix and the release of the held request.
func waitFix(t *testing.T, edit func(*Config)) (*pwFix, func(), *scCall) {
	t.Helper()
	f := newPwFixOn(t, edit, pwDep{name: "md-a", instances: []string{"a-0"}, cap: 1, workloads: []string{"alpha"}})
	f.workload("ns", "alpha", scModel)
	f.apply()
	hold := make(chan struct{})
	f.engines["ns/a-0"].Set(enginetest.Behaviour{Hold: hold})
	held := scGo(f.harness, errChatBody(scModel+"#alpha", false, "hello there"), "x-session-id", "waiter-holder")
	errWaitFor(t, "the first request to reach a-0", func() bool { return f.engines["ns/a-0"].Open() == 1 })
	f.engines["ns/a-0"].Set(enginetest.Behaviour{})
	released := false
	release := func() {
		if !released {
			released = true
			close(hold)
		}
	}
	t.Cleanup(release)
	return f, release, held
}

func TestAWorkloadRequestThatWaitedAndWasServedIsRecordedUnderItsWorkload(t *testing.T) {
	f, release, held := waitFix(t, nil)
	waiter := scGo(f.harness, errChatBody(scModel, false, "hello there"), "x-session-id", "waiter-key", wlHeader, "alpha")
	errWaitFor(t, "the request to wait at the front", func() bool { return f.router.Status().Waiting == 1 })
	release() // the Instance has room: the waiter takes it
	resp, body := waiter.wait(t)
	if resp.StatusCode != 200 || scInstanceOf(resp) != "ns/a-0" {
		t.Fatalf("the waiter: status %d from %q, want 200 from ns/a-0: %s", resp.StatusCode, scInstanceOf(resp), body)
	}
	rec := f.record(resp.Header.Get("hpcburst-request-id"))
	wantWorkload(t, "served after the wait", rec, str("alpha"))
	if rec.Instance != "ns/a-0" || rec.Status != 200 {
		t.Errorf("record: instance %q status %d, want ns/a-0 and 200", rec.Instance, rec.Status)
	}
	if hresp, hbody := held.wait(t); hresp.StatusCode != 200 {
		t.Errorf("the held request: status %d: %s", hresp.StatusCode, hbody)
	}
}

func TestAWorkloadRequestRefusedAfterWaitingIsRecordedAndCountedUnderItsWorkload(t *testing.T) {
	f, release, held := waitFix(t, func(c *Config) { c.QueueTimeout = Duration(300 * time.Millisecond) })
	waiter := scGo(f.harness, errChatBody(scModel+"#alpha", false, "hello there"), "x-session-id", "waiter-key")
	errWaitFor(t, "the request to wait at the front", func() bool { return f.router.Status().Waiting == 1 })
	resp, body := waiter.wait(t) // past queueTimeout, 503 capacity
	checkError(t, resp, body, wantError{status: 503, code: "capacity", retryAfter: "10"})
	if !strings.Contains(body, "ns/md-a") {
		t.Errorf("the error does not name the ModelDeployment ns/md-a: %s", body)
	}
	rec := f.record(resp.Header.Get("hpcburst-request-id"))
	wantWorkload(t, "refused after the wait", rec, str("alpha"))
	if rec.Status != 503 || rec.ModelDeployment != "ns/md-a" {
		t.Errorf("record: status %d modelDeployment %q, want 503 and ns/md-a", rec.Status, rec.ModelDeployment)
	}
	// The series: a refusal before a target was chosen has empty provider and model_deployment, and the Workload it had resolved.
	wantSample(t, scrape(t, f.metrics), 1, mRequests, "code", "503", "endpoint", "chat", "provider", "", "model_deployment", "", "workload", "alpha")
	release()
	if hresp, hbody := held.wait(t); hresp.StatusCode != 200 {
		t.Errorf("the held request: status %d: %s", hresp.StatusCode, hbody)
	}
}
