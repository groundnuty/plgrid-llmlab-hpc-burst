package front

import (
	"fmt"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// HBEP-5, Test plan, Fault injection: kill an Instance mid-stream (one error
// event and no [DONE]); refuse an Instance's connections three times (it is
// paused, keeps its count, and is eligible again after 30 s); flap one until
// the controller marks it ready again. Plus, from the Errors table: no byte
// reached any target is 502 upstream_unreachable naming the target tried, and
// an error from a target passes through and is never sent again.

// TestFaultKillMidStream: the engine double closes the connection abruptly
// after 2 of 6 tokens. The client had its 200 and two tokens; it gets one
// stream_cut event with detail upstream and no [DONE]. The record says the
// stream was cut by the upstream.
func TestFaultKillMidStream(t *testing.T) {
	f := newScFix(t, scOpts{})
	f.engines[0].Set(enginetest.Behaviour{Tokens: 6, KillAfter: 2})

	resp := f.errPost(scChat, errChatBody(scModel, true, "hello there"))
	if resp.StatusCode != 200 {
		t.Fatalf("status %d, want 200: the headers were sent before the kill", resp.StatusCode)
	}
	id := resp.Header.Get("hpcburst-request-id")
	body := readAll(t, resp)

	before := checkStreamCut(t, body, "upstream") // the last event is stream_cut, detail upstream; no [DONE]
	scOneCut(t, body)
	if n := scContentEvents(body); n != 2 || len(before) != 2 {
		t.Errorf("the client got %d token events before the cut (%d events in all), want the 2 the engine sent", n, len(before))
	}
	rec := f.record(id)
	if rec.Finish == nil || *rec.Finish != "cut" || rec.CutBy == nil || *rec.CutBy != "upstream" {
		t.Errorf("finish %v cutBy %v, want cut and upstream", rec.Finish, rec.CutBy)
	}
	if rec.Instance != f.names[0] || rec.Emitter != "instance" {
		t.Errorf("record names instance %q emitter %q, want %s and instance", rec.Instance, rec.Emitter, f.names[0])
	}
}

// TestFaultRefusedConnectionsPauseTheInstanceAndKeepItsCount: three refused
// connections in a row pause an Instance for 30 s; it keeps its count
// meanwhile and is eligible again after.
//
// Two Instances A (md-0) and B (md-1), cap 256 so the cap does not intervene.
// A has one request open (held), so its count is 1, and only then are its
// connections refused: the transport A is registered at moves to a port
// nothing listens on (refusedAddr), while the open request keeps its
// connection. Three requests whose home is A (scKeys) each find A chosen
// first (bound 1.25 * 2 / 2 = 1.25, A holds 1), meet a refused connection,
// and are sent once more, to B (open question 5). The third failure pauses A.
// The count is the check that the pause did not reset it: it must stay 1 (the
// open request), while a pause that released A would show 0.
func TestFaultRefusedConnectionsPauseTheInstanceAndKeepItsCount(t *testing.T) {
	f := newScFix(t, scOpts{instances: 2, desired: 2})
	a, b := f.names[0], f.names[1]
	keys := scKeys(t, f.names, a, 6)

	// A holds one request.
	hold := make(chan struct{})
	f.engines[0].Set(enginetest.Behaviour{Hold: hold})
	open := scGo(f.harness, errChatBody(scModel, false, "hello there"), "x-session-id", keys[0])
	errWaitFor(t, "the first request to reach A", func() bool { return f.engines[0].Open() == 1 })
	f.engines[0].Set(enginetest.Behaviour{}) // later requests are not held
	if got := f.scLoad(scDep, a); got != 1 {
		t.Fatalf("A's count = %d with one request open, want 1", got)
	}

	// Refuse A's connections from now on.
	f.trans.Set(f.uids[0], refusedAddr)

	for i := 1; i <= 3; i++ {
		resp, body := scChatOnce(t, f.harness, keys[i])
		if resp.StatusCode != 200 || scInstanceOf(resp) != b {
			t.Fatalf("refused request %d: status %d instance %q, want 200 from %s after one resend: %s", i, resp.StatusCode, scInstanceOf(resp), b, body)
		}
		if paused := f.router.Paused(a); paused != (i == 3) {
			t.Fatalf("after %d refused connections Paused(A) = %v, want %v (pauseAfterFailures is 3)", i, paused, i == 3)
		}
	}
	if got := f.counts(); got[0] != 1 || got[1] != 3 {
		t.Errorf("engines saw %v requests, want A only its open one and B the three resent", got)
	}

	// Paused: A keeps its count, and a key homed on A goes to B without a try at A.
	if got := f.scLoad(scDep, a); got != 1 {
		t.Errorf("A's count while paused = %d, want 1: the open request is still counted", got)
	}
	if total, members, open := f.scGroup(scDep).Counts(); total != 1 || members != 1 || open != 1 {
		t.Errorf("counts while paused = total %d, members %d, open %d, want 1 each", total, members, open)
	}
	if resp, body := scChatOnce(t, f.harness, keys[4]); resp.StatusCode != 200 || scInstanceOf(resp) != b {
		t.Errorf("while paused: status %d instance %q, want 200 from %s: %s", resp.StatusCode, scInstanceOf(resp), b, body)
	}

	// 30 s later A is eligible again, with the same count.
	f.clk.Advance(30 * time.Second)
	if f.router.Paused(a) {
		t.Fatalf("A is still paused 30 s after the third failure")
	}
	if got := f.scLoad(scDep, a); got != 1 {
		t.Errorf("A's count after the pause = %d, want 1", got)
	}
	f.trans.Set(f.uids[0], f.engines[0].Addr()) // the path works again
	if resp, body := scChatOnce(t, f.harness, keys[5]); resp.StatusCode != 200 || scInstanceOf(resp) != a {
		t.Errorf("after the pause: status %d instance %q, want 200 from %s: %s", resp.StatusCode, scInstanceOf(resp), a, body)
	}

	close(hold)
	resp, _ := open.wait(t)
	if scInstanceOf(resp) != a {
		t.Errorf("the open request ended on %q, want %s", scInstanceOf(resp), a)
	}
	f.record(resp.Header.Get("hpcburst-request-id")) // its reservation is released before its record
	errWaitFor(t, "A's count to return to 0", func() bool { return f.scLoad(scDep, a) == 0 })
	f.scNoDrift()
}

// TestFaultNoByteReachesAnyTarget: both Instances refuse their connections.
// The request is tried on one and sent once more to the other; no byte
// reached either, so 502 upstream_unreachable, naming the front and a target
// it tried.
func TestFaultNoByteReachesAnyTarget(t *testing.T) {
	f := newScFix(t, scOpts{instances: 2, desired: 2})
	f.trans.Set(f.uids[0], refusedAddr)
	f.trans.Set(f.uids[1], refusedAddr)

	resp, body := scChatOnce(t, f.harness, "conv-a")
	checkError(t, resp, body, wantError{status: 502, code: "upstream_unreachable"})
	if scEmitterOf(resp) != "front" {
		t.Errorf("emitter = %q, want front", scEmitterOf(resp))
	}
	named := scInstanceOf(resp)
	if named != f.names[0] && named != f.names[1] {
		t.Errorf("the 502 names instance %q, want one of the two it tried: %v", named, f.names)
	}
	rec := f.record(resp.Header.Get("hpcburst-request-id"))
	if rec.Status != 502 || rec.Instance != named {
		t.Errorf("record: status %d instance %q, want 502 and the named %s", rec.Status, rec.Instance, named)
	}
	if got := f.counts(); got[0] != 0 || got[1] != 0 {
		t.Errorf("the engines saw %v requests, want none: nothing was dialled", got)
	}
}

// TestFaultEngineErrorPassesThroughAndIsNeverResent: an Instance answers
// with an error status. The request had reached the engine, so it is not sent
// again, not even to the other Instance that would answer it: the client gets
// the engine's status and body unchanged, and the engines together saw it once.
func TestFaultEngineErrorPassesThroughAndIsNeverResent(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
	}{
		{500, `{"error":{"message":"CUDA out of memory in the engine","type":"server_error","param":null,"code":500}}`},
		{503, `{"error":{"message":"the engine is not ready","type":"server_error","param":null,"code":503}}`},
	} {
		t.Run(fmt.Sprintf("status_%d", tc.status), func(t *testing.T) {
			f := newScFix(t, scOpts{instances: 2, desired: 2})
			keys := scKeys(t, f.names, f.names[0], 1)
			f.engines[0].Set(enginetest.Behaviour{Status: tc.status, ErrorBody: tc.body}) // B is healthy

			resp, body := scChatOnce(t, f.harness, keys[0])
			if resp.StatusCode != tc.status || body != tc.body {
				t.Errorf("status %d body %s, want the engine's %d and body unchanged", resp.StatusCode, body, tc.status)
			}
			if scEmitterOf(resp) != "instance" || scInstanceOf(resp) != f.names[0] {
				t.Errorf("emitter %q instance %q, want instance and %s", scEmitterOf(resp), scInstanceOf(resp), f.names[0])
			}
			if got := f.counts(); got[0] != 1 || got[1] != 0 {
				t.Errorf("the engines saw %v requests, want [1 0]: never sent again", got)
			}
		})
	}
}

// TestFaultFlapAnInstance: A (md-0) goes ready, verifying, ready, verifying,
// ready, verifying and finally ready. While it is verifying no request goes
// to it, whatever the key; once ready again it gets the keys homed on it. A
// request open on A all along keeps A's count at 1 through every flap: a
// restart's return to verifying only makes A ineligible (Load bound and cap,
// "Keeps the count through a skip").
func TestFaultFlapAnInstance(t *testing.T) {
	f := newScFix(t, scOpts{instances: 2, desired: 2})
	a, b := f.names[0], f.names[1]
	keys := scKeys(t, f.names, a, 8)

	hold := make(chan struct{})
	f.engines[0].Set(enginetest.Behaviour{Hold: hold})
	open := scGo(f.harness, errChatBody(scModel, false, "hello there"), "x-session-id", keys[0])
	errWaitFor(t, "the first request to reach A", func() bool { return f.engines[0].Open() == 1 })
	f.engines[0].Set(enginetest.Behaviour{})

	setPhase := func(p v1alpha1.InstancePhase) {
		f.inst(f.uids[0]).Status.Phase = p
		f.apply()
	}
	k := 1
	for round := 1; round <= 3; round++ {
		setPhase(v1alpha1.InstanceVerifying)
		atA := len(f.engines[0].Requests())
		resp, body := scChatOnce(t, f.harness, keys[k])
		k++
		if resp.StatusCode != 200 || scInstanceOf(resp) != b {
			t.Errorf("round %d, verifying: status %d instance %q, want 200 from %s: %s", round, resp.StatusCode, scInstanceOf(resp), b, body)
		}
		if got := len(f.engines[0].Requests()); got != atA {
			t.Errorf("round %d: A got %d requests while verifying, want 0", round, got-atA)
		}
		if got := f.scLoad(scDep, a); got != 1 {
			t.Errorf("round %d: A's count while verifying = %d, want 1", round, got)
		}

		setPhase(v1alpha1.InstanceReady)
		resp, body = scChatOnce(t, f.harness, keys[k])
		k++
		if resp.StatusCode != 200 || scInstanceOf(resp) != a {
			t.Errorf("round %d, ready again: status %d instance %q, want 200 from %s: %s", round, resp.StatusCode, scInstanceOf(resp), a, body)
		}
		if got := f.scLoad(scDep, a); got != 1 {
			t.Errorf("round %d: A's count once ready = %d, want 1", round, got)
		}
	}

	close(hold)
	open.wait(t)
	errWaitFor(t, "A's count to return to 0", func() bool { return f.scLoad(scDep, a) == 0 })
	f.scNoDrift()
}
