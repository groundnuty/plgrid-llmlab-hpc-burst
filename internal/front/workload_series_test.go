package front

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
)

// The three series of the Workload build (HBEP-8, Metrics, Front metrics; HBEP-764's PR 5; the science agent's rulings Q10 and Q15 on #764):
//
//	hpcburst_front_in_flight                  gauge:     workload, provider, model_deployment, instance_uid
//	hpcburst_front_waiting                    gauge:     workload, model_deployment
//	hpcburst_front_output_tokens_per_second   histogram: workload, provider, model_deployment; buckets 1, 2, 5, 10, 20, 50, 100, 200, 500
//
// Every value is read back from the text /metrics serves, after real requests held on the engine doubles and the LLMLab double, and every expected
// number and label value is typed by hand from the rule it names. A request that ended is a barrier for the series: its usage record is written after
// its reservation is released (and, for the speed series, after the front observed it), so a scrape after the record sees the final state.

const (
	srInFlight = "hpcburst_front_in_flight"
	srWaiting  = "hpcburst_front_waiting"
	srSpeed    = "hpcburst_front_output_tokens_per_second"
)

// srUID is the UID the fixture gave the Instance with that key ("ns/a-0"): the value the instance_uid label must carry.
func (f *wrFix) srUID(key string) string {
	f.t.Helper()
	uid, ok := f.uids[key]
	if !ok || uid == "" {
		f.t.Fatalf("the fixture has no UID for %s", key)
	}
	return string(uid)
}

// srCount is how many samples of name the scrape holds.
func srCount(ss []sample, name string) int {
	n := 0
	for _, s := range ss {
		if s.name == name {
			n++
		}
	}
	return n
}

// srWantCount fails unless the scrape holds exactly n samples of name: a series that should not exist is as wrong as a missing one.
func srWantCount(t *testing.T, ss []sample, name string, n int) {
	t.Helper()
	if got := srCount(ss, name); got != n {
		t.Errorf("%d samples of %s, want %d:\n%s", got, name, n, describeSeries(ss, name))
	}
}

// srNoLabel fails when any sample of name carries label=value, whatever its other labels.
func srNoLabel(t *testing.T, ss []sample, name, label, value string) {
	t.Helper()
	for _, s := range ss {
		if s.name == name && s.labels[label] == value {
			t.Errorf("a sample %s carries %s=%q, want none:\n%s", name, label, value, describeSeries(ss, name))
			return
		}
	}
}

// srDone waits for a request that is expected to be answered 200 and for its usage record, which is written after its reservation was released.
func srDone(t *testing.T, f *wrFix, c *scCall) {
	t.Helper()
	resp, body := c.wait(t)
	if resp.StatusCode != 200 {
		t.Errorf("a request ended with status %d, want 200: %s", resp.StatusCode, body)
	}
	f.record(resp.Header.Get("hpcburst-request-id"))
}

// srDropInstance removes the Instance with that UID from the snapshot, as a delete does, without touching the snapshot the router holds.
func srDropInstance(t *testing.T, f *wrFix, uid types.UID) {
	t.Helper()
	for i := range f.snap.Instances {
		if f.snap.Instances[i].UID == uid {
			f.snap.Instances = append(f.snap.Instances[:i:i], f.snap.Instances[i+1:]...)
			return
		}
	}
	t.Fatalf("no Instance %s in the snapshot", uid)
}

// srSettled waits until open requests are open on our engines and waiting wait at the front: wrFix.settled for a fixture with no managed target, whose
// LLMLab double is nil.
func srSettled(t *testing.T, f *wrFix, open, waiting int) {
	t.Helper()
	errWaitFor(t, "the requests to settle", func() bool { return f.openOurs() == open && f.waiting() == waiting })
}

// srLongQueue sets queueTimeout to an hour, so no wait ends by the timeout.
func srLongQueue(c *Config) { c.QueueTimeout = Duration(time.Hour) }

// A named request held on a-0 is 1 under workload alpha, provider helios-a, model_deployment ns/md-a and the Instance's UID; a request that names no
// Workload held there is its own series, with workload "". Both go back to 0 when the requests end, and the series stay at 0: the Instance is still in
// its group.
func TestInFlightCountsNamedAndUnnamedRequestsOnAnInstanceUnderTheirOwnLabels(t *testing.T) {
	f := newWrFix(t, wrOpts{holdOurs: true})
	uid := f.srUID("ns/a-0")
	named, _ := f.start(f.alpha(), "if-named")
	f.settled(0, 1, 0)
	ss := scrape(t, f.metrics)
	wantSample(t, ss, 1, srInFlight, "workload", "alpha", "provider", "helios-a", "model_deployment", "ns/md-a", "instance_uid", uid)
	srWantCount(t, ss, srInFlight, 1)

	unnamed, _ := f.start(scModel, "if-unnamed")
	f.settled(0, 2, 0)
	ss = scrape(t, f.metrics)
	wantSample(t, ss, 1, srInFlight, "workload", "alpha", "provider", "helios-a", "model_deployment", "ns/md-a", "instance_uid", uid)
	wantSample(t, ss, 1, srInFlight, "workload", "", "provider", "helios-a", "model_deployment", "ns/md-a", "instance_uid", uid)
	srWantCount(t, ss, srInFlight, 2)

	f.freeOurs()
	srDone(t, f, named)
	srDone(t, f, unnamed)
	ss = scrape(t, f.metrics)
	wantSample(t, ss, 0, srInFlight, "workload", "alpha", "provider", "helios-a", "model_deployment", "ns/md-a", "instance_uid", uid)
	wantSample(t, ss, 0, srInFlight, "workload", "", "provider", "helios-a", "model_deployment", "ns/md-a", "instance_uid", uid)
	srWantCount(t, ss, srInFlight, 2)
}

// A request held on the managed target is counted under the target's provider llm-a and ModelDeployment ns/md-llm, with an empty instance_uid, and its
// Workload: alpha for a named one (ManagedFirst with managedSessions 5 takes it first; a ManagedFirst Workload with none is routed OwnFirst), "" for an
// unnamed one (it overflows there, past a-0's cap of 1, in the next test). Each series returns to 0 when its request ends.
func TestInFlightCountsRequestsOnTheManagedTargetWithNoInstanceUID(t *testing.T) {
	f := newWrFix(t, wrOpts{managedFirst: true, managedSessions: 5, stallManaged: true})
	named, _ := f.start(f.alpha(), "im-named")
	f.settled(1, 0, 0)
	ss := scrape(t, f.metrics)
	wantSample(t, ss, 1, srInFlight, "workload", "alpha", "provider", "llm-a", "model_deployment", "ns/md-llm", "instance_uid", "")
	srWantCount(t, ss, srInFlight, 1)
	f.unstallManaged()
	srDone(t, f, named)
	wantSample(t, scrape(t, f.metrics), 0, srInFlight, "workload", "alpha", "provider", "llm-a", "model_deployment", "ns/md-llm", "instance_uid", "")
}

func TestInFlightCountsAnUnnamedRequestThatOverflowedToTheManagedTarget(t *testing.T) {
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, stallManaged: true})
	uid := f.srUID("ns/a-0")
	held, _ := f.start(scModel, "io-held")
	f.settled(0, 1, 0)
	over, _ := f.start(scModel, "io-over")
	f.settled(1, 1, 0)
	ss := scrape(t, f.metrics)
	wantSample(t, ss, 1, srInFlight, "workload", "", "provider", "helios-a", "model_deployment", "ns/md-a", "instance_uid", uid)
	wantSample(t, ss, 1, srInFlight, "workload", "", "provider", "llm-a", "model_deployment", "ns/md-llm", "instance_uid", "")
	srWantCount(t, ss, srInFlight, 2)
	f.unstallManaged()
	srDone(t, f, over)
	ss = scrape(t, f.metrics)
	wantSample(t, ss, 0, srInFlight, "workload", "", "provider", "llm-a", "model_deployment", "ns/md-llm", "instance_uid", "")
	wantSample(t, ss, 1, srInFlight, "workload", "", "provider", "helios-a", "model_deployment", "ns/md-a", "instance_uid", uid) // a-0's is unmoved
	f.freeOurs()
	srDone(t, f, held)
	wantSample(t, scrape(t, f.metrics), 0, srInFlight, "workload", "", "provider", "helios-a", "model_deployment", "ns/md-a", "instance_uid", uid)
}

// Two Instances count apart. Three requests of alpha are held, two with session keys homed on a-0 and one homed on a-1, on a ring of the two. By hand,
// with c = 1.25 and n = 2: the first goes to its home a-0 (T = 0 passes); the second, T = 1, bound 1.25 * (1 + 1) / 2 = 1.25, finds a-0's load 1 within it
// and goes to its home a-0; the third goes to a-1, load 0. So a-0 holds 2 and a-1 holds 1, which the engine doubles' own counts confirm first.
func TestInFlightCountsEachInstanceOfAGroupApart(t *testing.T) {
	f := newWrFix(t, wrOpts{instances: []string{"a-0", "a-1"}, holdOurs: true})
	ring := []string{"ns/a-0", "ns/a-1"}
	home0, home1 := scKeys(t, ring, "ns/a-0", 2), scKeys(t, ring, "ns/a-1", 1)
	f.start(f.alpha(), home0[0])
	f.settled(0, 1, 0)
	f.start(f.alpha(), home0[1])
	f.settled(0, 2, 0)
	f.start(f.alpha(), home1[0])
	f.settled(0, 3, 0)
	if a0, a1 := f.engines["ns/a-0"].Open(), f.engines["ns/a-1"].Open(); a0 != 2 || a1 != 1 {
		t.Fatalf("the fixture: engines hold %d on a-0 and %d on a-1, want 2 and 1 (a placement the hand-worked bound does not give)", a0, a1)
	}
	ss := scrape(t, f.metrics)
	wantSample(t, ss, 2, srInFlight, "workload", "alpha", "provider", "helios-a", "model_deployment", "ns/md-a", "instance_uid", f.srUID("ns/a-0"))
	wantSample(t, ss, 1, srInFlight, "workload", "alpha", "provider", "helios-a", "model_deployment", "ns/md-a", "instance_uid", f.srUID("ns/a-1"))
	srWantCount(t, ss, srInFlight, 2)
}

// An Instance's series goes when it leaves its group and does not come back (HBEP-8: "an Instance's series goes when it leaves its group"). a-0 and
// a-1 each hold one request. a-0 is deleted from the snapshot: no in-flight sample with a-0's UID remains, whatever its other labels, while a-1's is
// untouched. Then a-0's request, still open on its engine, ends: its release finds the reservation released by the leave, so it must not recreate the
// series. a-1's series is still 1, and 0 only when its own request ends.
func TestInFlightSeriesOfAnInstanceGoesWhenItLeavesAndIsNotRecreatedByItsLateCompletion(t *testing.T) {
	f := newWrFix(t, wrOpts{instances: []string{"a-0", "a-1"}})
	hold0, hold1 := make(chan struct{}), make(chan struct{})
	var once0, once1 sync.Once
	t.Cleanup(func() { once0.Do(func() { close(hold0) }); once1.Do(func() { close(hold1) }) })
	f.engines["ns/a-0"].Set(enginetest.Behaviour{Hold: hold0})
	f.engines["ns/a-1"].Set(enginetest.Behaviour{Hold: hold1})
	uid0, uid1 := f.srUID("ns/a-0"), f.srUID("ns/a-1")
	ring := []string{"ns/a-0", "ns/a-1"}
	c0, _ := f.start(f.alpha(), scKeys(t, ring, "ns/a-0", 1)[0])
	f.settled(0, 1, 0)
	c1, _ := f.start(f.alpha(), scKeys(t, ring, "ns/a-1", 1)[0])
	f.settled(0, 2, 0)
	if a0, a1 := f.engines["ns/a-0"].Open(), f.engines["ns/a-1"].Open(); a0 != 1 || a1 != 1 {
		t.Fatalf("the fixture: engines hold %d on a-0 and %d on a-1, want 1 and 1", a0, a1)
	}
	ss := scrape(t, f.metrics)
	wantSample(t, ss, 1, srInFlight, "workload", "alpha", "provider", "helios-a", "model_deployment", "ns/md-a", "instance_uid", uid0)
	wantSample(t, ss, 1, srInFlight, "workload", "alpha", "provider", "helios-a", "model_deployment", "ns/md-a", "instance_uid", uid1)

	srDropInstance(t, f, f.uids["ns/a-0"])
	f.apply() // a-0 leaves its group: its open reservation is released at once, and its series goes with it
	ss = scrape(t, f.metrics)
	srNoLabel(t, ss, srInFlight, "instance_uid", uid0)
	wantSample(t, ss, 1, srInFlight, "workload", "alpha", "provider", "helios-a", "model_deployment", "ns/md-a", "instance_uid", uid1)
	srWantCount(t, ss, srInFlight, 1)

	once0.Do(func() { close(hold0) }) // a-0's request ends after the leave
	srDone(t, f, c0)
	ss = scrape(t, f.metrics)
	srNoLabel(t, ss, srInFlight, "instance_uid", uid0)
	wantSample(t, ss, 1, srInFlight, "workload", "alpha", "provider", "helios-a", "model_deployment", "ns/md-a", "instance_uid", uid1)
	srWantCount(t, ss, srInFlight, 1)

	once1.Do(func() { close(hold1) })
	srDone(t, f, c1)
	ss = scrape(t, f.metrics)
	wantSample(t, ss, 0, srInFlight, "workload", "alpha", "provider", "helios-a", "model_deployment", "ns/md-a", "instance_uid", uid1)
	srNoLabel(t, ss, srInFlight, "instance_uid", uid0)
}

// hpcburst_front_waiting has a series for a ModelDeployment's group queue, with workload "" and the deployment's key, and one for a Workload's FIFO, with
// the Workload's name and model_deployment "" (the science agent's ruling Q10). Here ours has one Instance at its cap of 1 and there is no managed target.
// One request holds a-0, an unnamed one waits in the group's queue, and a named one waits in alpha's FIFO: both series are 1 at once, and both read 0
// once the three are served.
func TestWaitingSeriesLabelsAGroupsQueueAndAWorkloadsFIFOApart(t *testing.T) {
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, noManaged: true, edit: srLongQueue})
	hold, _ := f.start(f.alpha(), "wa-hold")
	srSettled(t, f, 1, 0)
	unnamed, _ := f.start(scModel, "wa-unnamed")
	srSettled(t, f, 1, 1)
	named, _ := f.start(f.alpha(), "wa-named")
	srSettled(t, f, 1, 2)
	ss := scrape(t, f.metrics)
	wantSample(t, ss, 1, srWaiting, "workload", "", "model_deployment", "ns/md-a")
	wantSample(t, ss, 1, srWaiting, "workload", "alpha", "model_deployment", "")
	srWantCount(t, ss, srWaiting, 2)

	f.freeOurs() // each release places the next: the group's own waiter first, then alpha's FIFO (the science agent's ruling Q5)
	srDone(t, f, hold)
	srDone(t, f, unnamed)
	srDone(t, f, named)
	ss = scrape(t, f.metrics)
	wantSample(t, ss, 0, srWaiting, "workload", "", "model_deployment", "ns/md-a")
	wantSample(t, ss, 0, srWaiting, "workload", "alpha", "model_deployment", "")
	srWantCount(t, ss, srWaiting, 2)
}

// Every live group has its series, at 0 while nothing waits: two served ModelDeployments, md-a and md-b, with no request made, have two samples and no
// Workload's. Once a request names alpha, alpha's FIFO has its series, at 0, beside them.
func TestWaitingSeriesOfEveryLiveGroupIsZeroWhileNothingWaits(t *testing.T) {
	f := newPwFix(t, pwDep{name: "md-a", instances: []string{"a-0"}}, pwDep{name: "md-b", instances: []string{"b-0"}})
	f.workload("ns", "alpha", scModel)
	f.apply()
	ss := scrape(t, f.metrics)
	wantSample(t, ss, 0, srWaiting, "workload", "", "model_deployment", "ns/md-a")
	wantSample(t, ss, 0, srWaiting, "workload", "", "model_deployment", "ns/md-b")
	srWantCount(t, ss, srWaiting, 2)

	if _, resp, body := f.chat(t, scModel+"#alpha", "wz-1"); resp.StatusCode != 200 {
		t.Fatalf("a request of alpha: status %d: %s", resp.StatusCode, body)
	}
	ss = scrape(t, f.metrics)
	wantSample(t, ss, 0, srWaiting, "workload", "", "model_deployment", "ns/md-a")
	wantSample(t, ss, 0, srWaiting, "workload", "", "model_deployment", "ns/md-b")
	wantSample(t, ss, 0, srWaiting, "workload", "alpha", "model_deployment", "")
	srWantCount(t, ss, srWaiting, 3)
}

// A deleted Workload's series stays while its FIFO holds a request (the ruling Q15: it goes only when nothing of it waits or is in flight on a managed
// target) and goes once the FIFO has drained by the request's placement: the Workload's state is dropped, and with it its sample. The group's own series
// stays, at 0.
func TestWaitingSeriesOfADeletedWorkloadGoesAfterItsFIFODrainsByPlacement(t *testing.T) {
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, noManaged: true, edit: srLongQueue})
	hold, _ := f.start(f.alpha(), "wd-hold")
	srSettled(t, f, 1, 0)
	waiter, _ := f.start(f.alpha(), "wd-waiter")
	srSettled(t, f, 1, 1)
	wantSample(t, scrape(t, f.metrics), 1, srWaiting, "workload", "alpha", "model_deployment", "")

	f.dropWorkload("ns", "alpha")
	f.apply() // alpha leaves the view; its FIFO holds a request, so its state and its series stay
	if f.wlState() == nil {
		t.Fatalf("alpha's state was dropped while its FIFO holds a request")
	}
	wantSample(t, scrape(t, f.metrics), 1, srWaiting, "workload", "alpha", "model_deployment", "")

	f.freeOurs() // a-0's request ends, the waiter is placed (it keeps the Workload it resolved), and the FIFO is empty
	srDone(t, f, hold)
	srDone(t, f, waiter)
	errWaitFor(t, "alpha's state to be dropped", func() bool { return f.wlState() == nil })
	ss := scrape(t, f.metrics)
	srNoLabel(t, ss, srWaiting, "workload", "alpha")
	wantSample(t, ss, 0, srWaiting, "workload", "", "model_deployment", "ns/md-a")
	srWantCount(t, ss, srWaiting, 1)
}

// srGaveUp is a front where alpha is deleted while its one waiter waits, and then the waiter gives up (its client cancels), with a-0's holder still open:
// alpha's FIFO is empty, and nothing of alpha is in flight on a managed target, so nothing but the give-up itself can prompt its state to go.
func srGaveUp(t *testing.T) *wrFix {
	t.Helper()
	f := newWrFix(t, wrOpts{cap: 1, holdOurs: true, noManaged: true, edit: srLongQueue})
	f.start(f.alpha(), "wg-hold")
	srSettled(t, f, 1, 0)
	_, cancel := f.start(f.alpha(), "wg-waiter")
	srSettled(t, f, 1, 1)
	f.dropWorkload("ns", "alpha")
	f.apply()
	cancel()
	srSettled(t, f, 1, 0)
	return f
}

// The same when the FIFO drains because its last waiter gives up and not because it is placed: the Workload has left the view and nothing of it waits or
// is in flight on a managed target (the ruling Q15), so its state and its series go. No completion comes to run a wake pass here (a-0's holder is still
// open), so the state must go on the give-up itself.
func TestWaitingSeriesOfADeletedWorkloadGoesWhenItsLastWaiterGivesUp(t *testing.T) {
	f := srGaveUp(t)
	errWaitFor(t, "alpha's state to be dropped", func() bool { return f.wlState() == nil })
	ss := scrape(t, f.metrics)
	srNoLabel(t, ss, srWaiting, "workload", "alpha")
	wantSample(t, ss, 0, srWaiting, "workload", "", "model_deployment", "ns/md-a")
}

// The weaker rule the give-up case keeps if it lingers: the next change of the view drops the state, since nothing of alpha is left, and with it the series.
func TestWaitingSeriesOfADeletedWorkloadGoesAtTheNextRebuildAfterItsLastWaiterGaveUp(t *testing.T) {
	f := srGaveUp(t)
	f.apply() // any view change: the rebuild's wake pass drops a state that has left the view with nothing waiting
	errWaitFor(t, "alpha's state to be dropped by the rebuild", func() bool { return f.wlState() == nil })
	srNoLabel(t, scrape(t, f.metrics), srWaiting, "workload", "alpha")
}

// outputSpeed is the output tokens per second of a completed stream that carried usage: completion tokens over the time from its first token to its
// end (HBEP-8, Front metrics). The base row is the rule's own example: 40 tokens, first token at 1,000 ms, ended at 3,000 ms, so over 2 s: 20. Each
// other row changes ONE thing of the base, so each guard of the function is pinned on its own; the speeds are worked by hand.
func TestOutputSpeedIsDefinedOnlyForACompletedStreamWithEngineUsageAndASpan(t *testing.T) {
	base := func(edit func(*usage.Record)) usage.Record {
		r := usage.Record{Stream: true, Finish: strp("stop"), CompletionTokens: i64p(40), FirstTokenMs: i64p(1000), DurationMs: 3000, CountsSource: strp("engine")}
		if edit != nil {
			edit(&r)
		}
		return r
	}
	rows := []struct {
		name string
		rec  usage.Record
		want float64 // 0 with ok false: not observed
		ok   bool
	}{
		// Observed: 40 / ((3000 - 1000) / 1000) = 20.
		{"a completed stream with engine usage: 40 tokens over 2 s", base(nil), 20, true},
		{"5 tokens over 2 s", base(func(r *usage.Record) { r.CompletionTokens = i64p(5) }), 2.5, true},
		{"3 tokens over a 1,000 ms span: the span is in milliseconds", base(func(r *usage.Record) { r.FirstTokenMs, r.DurationMs = i64p(100), 1100; r.CompletionTokens = i64p(3) }), 3, true},
		// A capped reply and a Responses stream that ends incomplete are complete streams (HBEP-5: a reply capped by length, or response.incomplete, ends
		// the stream normally): their tokens and time measure a speed.
		{"a capped reply (finish length) is complete", base(func(r *usage.Record) { r.Finish = strp("length") }), 20, true},
		{"a Responses stream that ended incomplete is complete", base(func(r *usage.Record) { r.Finish = strp("incomplete") }), 20, true},
		{"a Responses stream that completed", base(func(r *usage.Record) { r.Finish = strp("completed") }), 20, true},

		// Not observed: one thing missing or wrong at a time.
		{"no usage: the engine's counts are the only ones used (completion tokens null)", base(func(r *usage.Record) { r.CompletionTokens = nil }), 0, false},
		{"counts from the front's own events, not the engine's", base(func(r *usage.Record) { r.CountsSource = strp("front_events") }), 0, false},
		{"counts of no source", base(func(r *usage.Record) { r.CountsSource = nil }), 0, false},
		{"no usage and counted by the front: the record of a stream the client did not ask usage of", base(func(r *usage.Record) { r.CompletionTokens, r.CountsSource = nil, strp("front_events") }), 0, false},
		{"first token == end (1,000 ms and 1,000 ms)", base(func(r *usage.Record) { r.DurationMs = 1000 }), 0, false},
		{"first token after the end: a negative span", base(func(r *usage.Record) { r.FirstTokenMs = i64p(4000) }), 0, false},
		{"no first token", base(func(r *usage.Record) { r.FirstTokenMs = nil }), 0, false},
		{"a cut stream, cutBy client and finish cut", base(func(r *usage.Record) { r.CutBy, r.Finish = strp("client"), strp("cut") }), 0, false},
		{"cutBy alone: finish stop, cutBy upstream", base(func(r *usage.Record) { r.CutBy = strp("upstream") }), 0, false},
		{"finish cut alone: cutBy null", base(func(r *usage.Record) { r.Finish = strp("cut") }), 0, false},
		{"a truncated stream, cutBy upstream", base(func(r *usage.Record) { r.CutBy, r.Finish = strp("upstream"), strp("truncated") }), 0, false},
		{"finish truncated alone: cutBy null", base(func(r *usage.Record) { r.Finish = strp("truncated") }), 0, false},
		{"a failed stream (response.failed): finish failed, no cutBy", base(func(r *usage.Record) { r.Finish = strp("failed") }), 0, false},
		{"finish error", base(func(r *usage.Record) { r.Finish = strp("error") }), 0, false},
		{"no finish (a path the front does not judge)", base(func(r *usage.Record) { r.Finish = nil }), 0, false},
		{"not a stream", base(func(r *usage.Record) { r.Stream = false }), 0, false},
	}
	for _, row := range rows {
		got, ok := outputSpeed(row.rec)
		if ok != row.ok || got != row.want {
			t.Errorf("%s: outputSpeed = %v, %v; want %v, %v", row.name, got, ok, row.want, row.ok)
		}
	}
}

// srStreamBody is a streamed chat request for model, asking the engine for usage when usage is set (stream_options.include_usage).
func srStreamBody(model string, usage bool) string {
	m := map[string]any{"model": model, "stream": true, "messages": []map[string]string{{"role": "user", "content": "hello there"}}}
	if usage {
		m["stream_options"] = map[string]bool{"include_usage": true}
	}
	b, _ := json.Marshal(m)
	return string(b)
}

// srRun sends one request under the session key and reads its answer. For a stream it reads events until the first one that carries a token, which the
// front has stamped as the first token by then, and only then lets the engine go on (it holds every token after the first at the Gate): so a fake clock
// the engine moves by FirstToken and by PerToken gives the record a first token at exactly FirstToken and an end at exactly FirstToken plus the gaps,
// whatever the pace of the test's goroutines. It returns the usage record, written after the request's reservation was released.
func srRun(t *testing.T, f *wrFix, path, body, session string, beh enginetest.Behaviour, isContent func(string) bool) usage.Record {
	t.Helper()
	gate := make(chan struct{})
	beh.Gate = gate
	f.engines["ns/a-0"].Set(beh)
	if isContent == nil {
		// A whole reply is generated before its headers are sent, so its engine must not wait at the gate for a client that waits for the headers.
		close(gate)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.url+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+f.clientKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-session-id", session)
	resp, err := errClient.Do(req)
	if err != nil {
		t.Fatalf("the request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d, want 200: %s", resp.StatusCode, b)
	}
	if isContent != nil {
		br := bufio.NewReader(resp.Body)
		for {
			ev, err := scReadEvent(br)
			if err != nil {
				t.Fatalf("no event with a token before the stream ended (%v): %q", err, ev)
			}
			if isContent(ev) {
				break
			}
		}
		close(gate)
		if _, err := io.ReadAll(br); err != nil {
			t.Fatalf("reading the rest of the stream: %v", err)
		}
	} else {
		if _, err := io.ReadAll(resp.Body); err != nil {
			t.Fatalf("reading the reply: %v", err)
		}
	}
	return f.record(resp.Header.Get("hpcburst-request-id"))
}

func srChatContent(ev string) bool { return scIsContent(ev) }

func srResponsesContent(ev string) bool { return strings.Contains(ev, "response.output_text.delta") }

// srWantRecord fails unless a stream's record shows what the row relies on: its counts' source, its completion tokens and its two times.
func srWantRecord(t *testing.T, rec usage.Record, source string, tokens *int64, firstMs, durationMs int64) {
	t.Helper()
	if rec.CountsSource == nil || *rec.CountsSource != source {
		t.Fatalf("the record's countsSource = %s, want %s", strStr(rec.CountsSource), source)
	}
	if (rec.CompletionTokens == nil) != (tokens == nil) || (tokens != nil && *rec.CompletionTokens != *tokens) {
		t.Fatalf("the record's completionTokens = %s, want %s", ptrStr(rec.CompletionTokens), ptrStr(tokens))
	}
	if rec.FirstTokenMs == nil || *rec.FirstTokenMs != firstMs || rec.DurationMs != durationMs {
		t.Fatalf("the record's firstTokenMs = %s and durationMs = %d, want %d and %d", ptrStr(rec.FirstTokenMs), rec.DurationMs, firstMs, durationMs)
	}
}

// srSpeedLabels are the labels of one series of the speed histogram.
func srSpeedLabels(workload string) []string {
	return []string{"workload", workload, "provider", "helios-a", "model_deployment", "ns/md-a"}
}

// srWantHistogram checks a speed series: its count, its sum, and its cumulative buckets from HBEP-8's list (1, 2, 5, 10, 20, 50, 100, 200 and 500),
// where le maps each bound to the count of observations at or below it.
func srWantHistogram(t *testing.T, ss []sample, workload string, count, sum float64, le map[string]float64) {
	t.Helper()
	labels := srSpeedLabels(workload)
	wantSample(t, ss, count, srSpeed+"_count", labels...)
	wantSample(t, ss, sum, srSpeed+"_sum", labels...)
	for bound, want := range le {
		wantSample(t, ss, want, srSpeed+"_bucket", append([]string{"le", bound}, labels...)...)
	}
}

// A streamed request that asked for usage is observed once, in the bucket of its speed, under its labels. By hand, for 41 tokens, a first token at 1 s and
// a token every 100 ms after it: the first at 1,000 ms, the 40 gaps of 100 ms end the stream at 5,000 ms, so the span is 4 s and the speed 41 / 4 = 10.25,
// in the bucket le=20 (not le=10). A second one that names no Workload, 3 tokens, first token at 1 s and a token every 1 s after: ended at 3,000 ms, span
// 2 s, 3 / 2 = 1.5, in le=2, and in the series with workload "": the first series is untouched.
func TestOutputSpeedOfACompletedStreamWithUsageIsObservedOnceUnderItsLabels(t *testing.T) {
	f := newWrFix(t, wrOpts{})
	rec := srRun(t, f, scChat, srStreamBody(f.alpha(), true), "sp-named", enginetest.Behaviour{Tokens: 41, FirstToken: time.Second, PerToken: 100 * time.Millisecond}, srChatContent)
	srWantRecord(t, rec, "engine", i64p(41), 1000, 5000)
	if rec.Finish == nil || *rec.Finish != "stop" || rec.CutBy != nil {
		t.Fatalf("the record's finish %s and cutBy %s, want stop and null", strStr(rec.Finish), strStr(rec.CutBy))
	}
	ss := scrape(t, f.metrics)
	srWantHistogram(t, ss, "alpha", 1, 10.25, map[string]float64{"1": 0, "2": 0, "5": 0, "10": 0, "20": 1, "50": 1, "100": 1, "200": 1, "500": 1, "+Inf": 1})
	srWantCount(t, ss, srSpeed+"_count", 1)

	rec = srRun(t, f, scChat, srStreamBody(scModel, true), "sp-unnamed", enginetest.Behaviour{Tokens: 3, FirstToken: time.Second, PerToken: time.Second}, srChatContent)
	srWantRecord(t, rec, "engine", i64p(3), 1000, 3000)
	ss = scrape(t, f.metrics)
	srWantHistogram(t, ss, "", 1, 1.5, map[string]float64{"1": 0, "2": 1, "5": 1, "10": 1, "20": 1, "50": 1, "100": 1, "200": 1, "500": 1, "+Inf": 1})
	srWantHistogram(t, ss, "alpha", 1, 10.25, map[string]float64{"10": 0, "20": 1})
	srWantCount(t, ss, srSpeed+"_count", 2)
}

// A /v1/responses stream carries its usage in response.completed, as usage.output_tokens, with no request for it: 21 tokens, a first token at 1 s and one
// every 100 ms after: first token at 1,000 ms, end at 3,000 ms, 21 / 2 = 10.5, in le=20, with workload "" for a request that names none.
func TestOutputSpeedOfAResponsesStreamUsesItsOutputTokens(t *testing.T) {
	f := newWrFix(t, wrOpts{})
	rec := srRun(t, f, "/v1/responses", `{"model":"`+scModel+`","stream":true,"input":"hello there"}`, "sp-resp",
		enginetest.Behaviour{Tokens: 21, FirstToken: time.Second, PerToken: 100 * time.Millisecond}, srResponsesContent)
	srWantRecord(t, rec, "engine", i64p(21), 1000, 3000)
	ss := scrape(t, f.metrics)
	srWantHistogram(t, ss, "", 1, 10.5, map[string]float64{"5": 0, "10": 0, "20": 1, "50": 1, "+Inf": 1})
}

// Streams that are not observed add nothing to a series that already holds one observation (41 tokens, 10.25), each with a positive span except the
// one whose first token is its end, and each record shows the reason the row names:
//   - a stream whose request did not ask for usage: countsSource front_events, no completion tokens (5 tokens over a 4 s span would have been 1.25);
//   - a whole, non-streamed reply: stream false;
//   - a stream whose one token is its end: first token 1,000 ms and end 1,000 ms (span 0);
//   - a stream that carried usage and was cut: the engine ends it without [DONE] after its usage chunk, so the record has the engine's counts (5 tokens,
//     a 4 s span: 1.25 would have been observed) and is truncated by the upstream.
func TestOutputSpeedIsNotObservedForAStreamWithoutUsageAWholeReplyASpanOfZeroOrACutStream(t *testing.T) {
	f := newWrFix(t, wrOpts{})
	srRun(t, f, scChat, srStreamBody(f.alpha(), true), "sn-ok", enginetest.Behaviour{Tokens: 41, FirstToken: time.Second, PerToken: 100 * time.Millisecond}, srChatContent)
	wantHist := func(when string) {
		t.Helper()
		ss := scrape(t, f.metrics)
		srWantHistogram(t, ss, "alpha", 1, 10.25, map[string]float64{"10": 0, "20": 1, "+Inf": 1})
		srWantCount(t, ss, srSpeed+"_count", 1)
		if t.Failed() {
			t.Fatalf("%s: the speed series changed", when)
		}
	}
	wantHist("after the observed stream")

	// No usage asked: the front counts content events itself, and observes nothing.
	rec := srRun(t, f, scChat, srStreamBody(f.alpha(), false), "sn-nousage", enginetest.Behaviour{Tokens: 5, FirstToken: time.Second, PerToken: time.Second}, srChatContent)
	srWantRecord(t, rec, "front_events", nil, 1000, 5000)
	wantHist("after a stream with no usage")

	// A whole reply: not a stream.
	rec = srRun(t, f, scChat, errChatBody(f.alpha(), false, "hello there"), "sn-whole", enginetest.Behaviour{Tokens: 5, FirstToken: time.Second, PerToken: time.Second}, nil)
	if rec.Stream {
		t.Fatalf("the record of the whole reply has stream true")
	}
	wantHist("after a whole reply")

	// Its first token is its end.
	rec = srRun(t, f, scChat, srStreamBody(f.alpha(), true), "sn-oneshot", enginetest.Behaviour{Tokens: 1, FirstToken: time.Second}, srChatContent)
	srWantRecord(t, rec, "engine", i64p(1), 1000, 1000)
	wantHist("after a stream whose first token is its end")

	// Cut with usage: the engine's stream ends without [DONE] after the finish and usage chunks, so it is truncated by the upstream.
	rec = srRun(t, f, scChat, srStreamBody(f.alpha(), true), "sn-cut", enginetest.Behaviour{Tokens: 5, FirstToken: time.Second, PerToken: time.Second, NoDone: true}, srChatContent)
	srWantRecord(t, rec, "engine", i64p(5), 1000, 5000)
	if rec.CutBy == nil || *rec.CutBy != "upstream" || rec.Finish == nil || *rec.Finish != "truncated" {
		t.Fatalf("the cut stream's record has cutBy %s and finish %s, want upstream and truncated", strStr(rec.CutBy), strStr(rec.Finish))
	}
	wantHist("after a cut stream that carried usage")
}
