package front

import (
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// HBEP-5, Draining, and Test plan, Drain and Drain-window check.
//
// The drain rule: an Instance starts draining at the latest at its lease end
// minus the drain window, whether or not its replacement is ready. The front
// enforces that itself: from the drain start it sends the Instance no new
// request, although the controller has not marked it draining; in-flight
// requests finish; the lost capacity is reported by an event.
//
// The window is the profile's drainWindow (default 2 h 10 min); the lease end
// of the Instance in these tests is t0 + 3 h, so the drain start is
// t0 + 50 min (3 h − 2 h 10 min). The controller never marks anything
// draining here, so the front's own deadline is what is tested.

const (
	// drainLeaseEnd is the near lease end: t0 + 3 h.
	drainLeaseEnd = 3 * time.Hour
	// drainStartAt is the drain start after t0 with the default window:
	// 3 h − 2 h 10 min.
	drainStartAt = 50 * time.Minute
)

// drainOpts gives the first Instance (md-0) the near lease end.
func drainOpts(i int, o *instanceOpts) {
	if i == 0 {
		o.leaseEnd = t0.Add(drainLeaseEnd)
	}
}

// TestDrainStopsNewRequestsAtLeaseEndMinusWindow: md-0's lease ends at
// t0 + 3 h and md-1's far later; spec.instances is 2, so when md-0 drains
// there is no ready replacement for it and capacity drops.
func TestDrainStopsNewRequestsAtLeaseEndMinusWindow(t *testing.T) {
	f := newScFix(t, scOpts{instances: 2, desired: 2, inst: drainOpts})
	keys := scKeys(t, f.names, f.names[0], 5) // all homed on md-0
	f.clk.Advance(drainStartAt - time.Second)

	// One second before the drain start md-0 still takes its keys.
	if resp, body := scChatOnce(t, f.harness, keys[0]); resp.StatusCode != 200 || scInstanceOf(resp) != f.names[0] {
		t.Fatalf("1 s before the drain start: status %d instance %q, want 200 from %s: %s", resp.StatusCode, scInstanceOf(resp), f.names[0], body)
	}
	// A request running when the drain starts: held at the engine.
	hold := make(chan struct{})
	f.setAll(enginetest.Behaviour{Hold: hold})
	running := scGo(f.harness, errChatBody(scModel, true, "hello there"), "x-session-id", keys[1])
	errWaitFor(t, "the running request to reach md-0", func() bool { return f.engines[0].Open() == 1 })
	if n := f.scLoad(scDep, f.names[0]); n != 1 {
		t.Fatalf("md-0's count = %d with one request open, want 1", n)
	}

	f.clk.Advance(2 * time.Second) // past the drain start; the router's timer rebuilds the view

	// The phase did not change: only the front's own deadline took md-0 out.
	if p := f.inst(f.uids[0]).Status.Phase; p != v1alpha1.InstanceReady {
		t.Fatalf("md-0's phase = %q, want ready: this test is about the front's deadline", p)
	}
	// An event reports the lost capacity: 1 of the 2 desired Instances is left.
	ev := f.rec.Find("degraded", "CapacityDropped")
	if len(ev) != 1 || !strings.Contains(ev[0].Object, "ModelDeployment ns/md") || !ev[0].Warning {
		t.Errorf("CapacityDropped events = %+v, want exactly one warning on ModelDeployment ns/md", ev)
	}
	// At the drain start the Instance left its group and its open request was
	// released at once: the counts are KubeAI's total 0, members 0, open 0.
	if total, members, open := f.scGroup(scDep).Counts(); total != 0 || members != 0 || open != 0 {
		t.Errorf("counts after the leave = total %d, members %d, open %d, want all 0", total, members, open)
	}

	// New requests, from the keys homed on md-0, all go to md-1.
	f.setAll(enginetest.Behaviour{})
	before := f.counts()
	for _, k := range keys[2:] {
		if resp, body := scChatOnce(t, f.harness, k); resp.StatusCode != 200 || scInstanceOf(resp) != f.names[1] {
			t.Errorf("after the drain start, key %s: status %d instance %q, want 200 from %s: %s", k, resp.StatusCode, scInstanceOf(resp), f.names[1], body)
		}
	}
	if got := f.counts(); got[0] != before[0] {
		t.Errorf("md-0 got %d new requests after its drain start, want 0", got[0]-before[0])
	}

	// The running request finishes and is not cut.
	close(hold)
	resp, body := running.wait(t)
	if resp.StatusCode != 200 || scInstanceOf(resp) != f.names[0] {
		t.Fatalf("the running request: status %d instance %q, want 200 from %s", resp.StatusCode, scInstanceOf(resp), f.names[0])
	}
	if strings.Contains(body, "stream_cut") || !strings.HasSuffix(body, "data: [DONE]\n\n") || scContentEvents(body) != 3 {
		t.Errorf("the running request was cut: %q", body)
	}
	rec := f.record(resp.Header.Get("hpcburst-request-id"))
	if rec.Finish == nil || *rec.Finish != "stop" || rec.CutBy != nil {
		t.Errorf("record finish %v cutBy %v, want stop and none", rec.Finish, rec.CutBy)
	}
	// Its completion changed nothing: the release was once, at the leave.
	if total, members, open := f.scGroup(scDep).Counts(); total != 0 || members != 0 || open != 0 {
		t.Errorf("counts after the completion = total %d, members %d, open %d, want all 0", total, members, open)
	}
	f.scNoDrift()
}

// TestDrainWithNoReplacementLeavesNoTarget: the one Instance of the
// ModelDeployment reaches its drain start. Nothing replaces it, so requests
// get 503 model_inactive (no_ready_target) and the lost capacity is reported.
func TestDrainWithNoReplacementLeavesNoTarget(t *testing.T) {
	f := newScFix(t, scOpts{inst: drainOpts})
	f.clk.Advance(drainStartAt - time.Second)
	if resp, body := scChatOnce(t, f.harness, "conv-a"); resp.StatusCode != 200 {
		t.Fatalf("1 s before the drain start: status %d: %s", resp.StatusCode, body)
	}
	if ev := f.rec.Find("degraded", "CapacityDropped"); len(ev) != 0 {
		t.Fatalf("a CapacityDropped event before the drain start: %+v", ev)
	}

	f.clk.Advance(2 * time.Second)

	resp, body := scChatOnce(t, f.harness, "conv-a")
	checkError(t, resp, body, wantError{status: 503, code: "model_inactive", detail: "no_ready_target", retryAfter: "30"})
	if n := len(f.engines[0].Requests()); n != 1 {
		t.Errorf("the engine saw %d requests, want only the one before the drain start", n)
	}
	if p := f.inst(f.uids[0]).Status.Phase; p != v1alpha1.InstanceReady {
		t.Errorf("phase = %q, want ready", p)
	}
	if ev := f.rec.Find("degraded", "CapacityDropped"); len(ev) != 1 {
		t.Errorf("CapacityDropped events = %+v, want exactly one", ev)
	}
}

// TestDrainMarkedDrainingEarlierGetsNothing: the controller marks md-0
// draining once its replacement is ready, long before its drain start
// (its lease end is the default t0 + 48 h and the clock has not moved). From
// the next snapshot it gets no new request, it leaves its balancer group, and
// the requests open on it are released from the count at once (Load bound and
// cap, "Releases at a leave"). This is the controller's path, not the front's
// own deadline, which the tests above cover. An Instance that is merely
// ineligible also gets no new request, so the group's membership and the
// release are what tell a leave from a skip.
func TestDrainMarkedDrainingEarlierGetsNothing(t *testing.T) {
	f := newScFix(t, scOpts{instances: 2, desired: 2})
	keys := scKeys(t, f.names, f.names[0], 4)

	// Before the mark, md-0 takes its keys: the control.
	if resp, _ := scChatOnce(t, f.harness, keys[0]); scInstanceOf(resp) != f.names[0] {
		t.Fatalf("before the mark: instance %q, want %s", scInstanceOf(resp), f.names[0])
	}
	// A request open on md-0 when it is marked.
	hold := make(chan struct{})
	f.engines[0].Set(enginetest.Behaviour{Hold: hold})
	open := scGo(f.harness, errChatBody(scModel, false, "hello there"), "x-session-id", keys[1])
	errWaitFor(t, "the open request to reach md-0", func() bool { return f.engines[0].Open() == 1 })
	f.engines[0].Set(enginetest.Behaviour{})
	if got := f.scLoad(scDep, f.names[0]); got != 1 {
		t.Fatalf("md-0's count = %d with one request open, want 1", got)
	}

	f.inst(f.uids[0]).Status.Phase = v1alpha1.InstanceDraining
	f.apply()

	if _, member := f.scGroup(scDep).InFlight(f.names[0]); member {
		t.Errorf("md-0 is still a member of the group after it was marked draining")
	}
	if total, members, openN := f.scGroup(scDep).Counts(); total != 0 || members != 0 || openN != 0 {
		t.Errorf("counts after the mark = total %d, members %d, open %d, want all 0: the open request is released at the leave", total, members, openN)
	}
	before := f.counts()
	for _, k := range keys[2:] {
		if resp, body := scChatOnce(t, f.harness, k); resp.StatusCode != 200 || scInstanceOf(resp) != f.names[1] {
			t.Errorf("after the mark, key %s: status %d instance %q, want 200 from %s: %s", k, resp.StatusCode, scInstanceOf(resp), f.names[1], body)
		}
	}
	if got := f.counts(); got[0] != before[0] {
		t.Errorf("md-0 got %d new requests after it was marked draining, want 0", got[0]-before[0])
	}

	// The open request finishes; its completion changes nothing.
	close(hold)
	if resp, body := open.wait(t); resp.StatusCode != 200 || scInstanceOf(resp) != f.names[0] {
		t.Errorf("the open request: status %d instance %q, want 200 from %s: %s", resp.StatusCode, scInstanceOf(resp), f.names[0], body)
	}
	if total, members, openN := f.scGroup(scDep).Counts(); total != 0 || members != 0 || openN != 0 {
		t.Errorf("counts after the completion = total %d, members %d, open %d, want all 0", total, members, openN)
	}
	f.scNoDrift()
}

// TestDrainLeaseEndCutsAStreamStillRunning: the lease-end cut. A stream still
// running at the lease end itself is cut: the client gets one stream_cut event
// with detail lease_end, and the record says cutBy lease_end.
//
// A stream can still be running at the lease end only when the drain window is
// shorter than maxRequestDuration plus drainMargin, which is what the
// drain-window check reports. The brief proposed shortening the window through
// the ModelDeployment's routing.maxRequestDuration, but that value also
// shortens the request's own deadline: with it at M the window is M plus the
// margin, and a request routed before the drain start meets its deadline
// at least a margin before the lease end, so it is never cut by the lease end.
// (TestDrainDefaultWindowCutsNoRequestAtTheLeaseEnd shows that side.) So the
// window is shortened through the profile.
//
// The numbers: the profile's drainWindow is 20 min, the front's
// maxRequestDuration 2 h, the lease end t0 + 1 h, the drain start t0 + 40 min.
// The request is routed at t0 + 39 min, before the drain start; its deadline
// is t0 + 2 h 39 min and its idle time-out resets with every token. The test
// moves the clock 30 s after each token (scLockstep), 42 times: 21 min, to
// exactly the lease end.
func TestDrainLeaseEndCutsAStreamStillRunning(t *testing.T) {
	f := newScFix(t, scOpts{engineReal: true, profileWindow: "20m", inst: func(i int, o *instanceOpts) { o.leaseEnd = t0.Add(time.Hour) }})
	f.engines[0].Set(enginetest.Behaviour{Tokens: 100000, PerToken: 2 * time.Millisecond})
	f.clk.Advance(39 * time.Minute)

	resp := f.errPost(scChat, errChatBody(scModel, true, "hello there"))
	if resp.StatusCode != 200 {
		t.Fatalf("status %d, want 200 before the cut", resp.StatusCode)
	}
	id := resp.Header.Get("hpcburst-request-id")
	content, body := scLockstep(t, f.harness, resp, 30*time.Second, 21*time.Minute)

	checkStreamCut(t, body, "lease_end") // the last event is stream_cut, detail lease_end; no [DONE]
	scOneCut(t, body)
	if content < 42 {
		t.Errorf("the client got %d token events, want at least the 42 read before the cut", content)
	}
	rec := f.record(id)
	if rec.Finish == nil || *rec.Finish != "cut" || rec.CutBy == nil || *rec.CutBy != "lease_end" {
		t.Errorf("finish %v cutBy %v, want cut and lease_end", rec.Finish, rec.CutBy)
	}
	// The request lived from t0 + 39 min to the lease end t0 + 60 min.
	if rec.DurationMs != 21*60*1000 {
		t.Errorf("durationMs = %d, want 1,260,000 (21 min)", rec.DurationMs)
	}
	// The drain start passed meanwhile: the Instance left its group and the
	// open stream went on to the lease end.
	if ev := f.rec.Find("degraded", "CapacityDropped"); len(ev) != 1 {
		t.Errorf("CapacityDropped events = %+v, want exactly one", ev)
	}
}

// TestDrainDefaultWindowCutsNoRequestAtTheLeaseEnd: with the default window
// (2 h 10 min = the default maxRequestDuration 2 h plus the default margin
// 10 min), the longest request, routed 1 s before the drain start, meets its
// own deadline 10 min and 1 s before the lease end: no request is cut at the
// lease end (Test plan, Drain). Lease end t0 + 3 h; drain start t0 + 50 min;
// routed at t0 + 49 min 59 s; deadline t0 + 2 h 49 min 59 s. The test moves the
// clock 60 s after each token, 120 times: exactly the 2 h deadline. The engine
// sends each token only on the test's go-ahead (scLockstepGated), so the
// front reads a token between any two moves: 30 moves with no read between
// them would reach the 30 min idle time-out first (issue #136).
func TestDrainDefaultWindowCutsNoRequestAtTheLeaseEnd(t *testing.T) {
	f := newScFix(t, scOpts{engineReal: true, inst: drainOpts})
	gate := make(chan struct{})
	f.engines[0].Set(enginetest.Behaviour{Tokens: 100000, PerToken: 2 * time.Millisecond, Gate: gate})
	f.clk.Advance(drainStartAt - time.Second)

	resp := f.errPost(scChat, errChatBody(scModel, true, "hello there"))
	id := resp.Header.Get("hpcburst-request-id")
	_, body := scLockstepGated(t, f.harness, resp, gate, time.Minute, 2*time.Hour)

	checkStreamCut(t, body, "deadline")
	scOneCut(t, body)
	rec := f.record(id)
	if rec.CutBy == nil || *rec.CutBy != "front_deadline" {
		t.Errorf("cutBy = %v, want front_deadline, never lease_end", rec.CutBy)
	}
	if rec.DurationMs != 2*60*60*1000 {
		t.Errorf("durationMs = %d, want 7,200,000", rec.DurationMs)
	}
	if left := t0.Add(drainLeaseEnd).Sub(f.clk.Now()); left != 10*time.Minute+time.Second {
		t.Errorf("the lease end is %v after the cut, want 10 min 1 s", left)
	}
}

// TestDrainWindowCheck: at start and at every PlatformProfile change, each
// profile with a batch Provider needs drainWindow >= the front's
// maxRequestDuration (2 h) + the profile's drainMargin (10 min by default). A
// profile that fails gets a degraded event naming it, with reason
// DrainWindowShort, once per version of the profile.
func TestDrainWindowCheck(t *testing.T) {
	h := newHarness(t, nil)
	h.profile("helios-short", v1alpha1.PlatformBatch, "1h", "") // 1 h < 2 h + 10 min
	h.batchProvider("helios-a", "helios-short")
	h.profile("helios-ok", v1alpha1.PlatformBatch, "2h10m", "") // exactly the sum: passes
	h.batchProvider("helios-b", "helios-ok")
	h.profile("llmlab", v1alpha1.PlatformManaged, "1h", "") // a short window, but no batch Provider
	h.managedProvider("llmlab-a", "llmlab", "llmlab", "http://127.0.0.1:1/v1", "key-for-the-check-0001")
	h.apply()

	short := func() []RecordedEvent { return h.rec.Find("degraded", "DrainWindowShort") }
	if ev := short(); len(ev) != 1 || !strings.Contains(ev[0].Object, "helios-short") || !strings.Contains(ev[0].Message, "helios-short") || !ev[0].Warning {
		t.Fatalf("events after the first snapshot = %+v, want exactly one warning naming helios-short", ev)
	}
	for _, e := range short() {
		if strings.Contains(e.Object, "helios-ok") || strings.Contains(e.Object, "llmlab") {
			t.Errorf("an event on a profile that passes or has no batch Provider: %+v", e)
		}
	}

	// A second snapshot of the same resourceVersion does not raise it again.
	h.apply()
	if ev := short(); len(ev) != 1 {
		t.Errorf("events after a second apply of the same version = %d, want still 1", len(ev))
	}

	// A change of the profile (a new resourceVersion) is checked again.
	h.snap.Profiles[0].ResourceVersion = "2"
	h.apply()
	if ev := short(); len(ev) != 2 {
		t.Errorf("events after the profile changed = %d, want 2", len(ev))
	}
	// Fixing the window ends it: no third event.
	w := v1alpha1.Duration("3h")
	h.snap.Profiles[0].Spec.DrainWindow = &w
	h.snap.Profiles[0].ResourceVersion = "3"
	h.apply()
	if ev := short(); len(ev) != 2 {
		t.Errorf("events after the window was fixed = %d, want still 2", len(ev))
	}
}
