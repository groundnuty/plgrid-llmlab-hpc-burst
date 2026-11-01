package front

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// TestProbeTakesAFailingPathOutOfRoutingAndBack runs HBEP-6's probe test
// against the real frps and frpc: frpc stays up while the engine behind it
// fails, first with /health answering 503 (vLLM's dead engine), then killed.
//   - One failed probe changes nothing: the Instance stays eligible.
//   - The second makes it not eligible, a skip: it stays in its group with its
//     in-flight count, a new request gets 503 no_ready_target, and
//     PathProbeFailing is reported once, since it is ready and its
//     TunnelConnected is True.
//   - One passing probe makes it eligible again, and requests reach it.
func TestProbeTakesAFailingPathOutOfRoutingAndBack(t *testing.T) {
	rig := newFrpRig(t)
	f := newTunFront(t, rig)
	if f.cfg.ProbeFailures != 2 || f.cfg.ProbeInterval.D() != 10*time.Second || f.cfg.ProbeTimeout.D() != 10*time.Second {
		t.Fatalf("probe settings %d, %s, %s; want HBEP-6's 2, 10 s, 10 s", f.cfg.ProbeFailures, f.cfg.ProbeInterval.D(), f.cfg.ProbeTimeout.D())
	}
	dial, _ := rig.serveDialHome(ModeDirect)
	f.tunDeployment(1)
	key := newKey(t)
	e, sock := rig.engine(key)
	a := f.tunInstance("md-a", key)
	rig.waitOnline(a, rig.startFrpc(a, sock, dial))
	f.apply()
	f.waitRegistered(a)
	round := func() { f.prober.Round(context.Background()) }
	want := func(step string, wantEligible bool, wantInFlight int64) {
		t.Helper()
		member, eligible, inFlight := f.routing(a)
		if !member || eligible != wantEligible || inFlight != wantInFlight {
			t.Fatalf("%s: member %v, eligible %v, in flight %d; want a member, eligible %v, in flight %d\n%s",
				step, member, eligible, inFlight, wantEligible, wantInFlight, f.logs.String())
		}
	}
	want("registered", true, 0)

	// A request held by the engine, in flight through the tunnel.
	hold := make(chan struct{})
	e.Set(enginetest.Behaviour{Hold: hold})
	held := make(chan int, 1)
	go func() {
		resp := f.post("/v1/chat/completions", tunChat, "X-Session-Id", "held")
		readAll(t, resp)
		held <- resp.StatusCode
	}()
	for end := time.Now().Add(10 * time.Second); e.Open() != 1; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatal("the held request never reached the engine")
		}
	}
	want("one request in flight", true, 1)

	e.Set(enginetest.Behaviour{Hold: hold, Health: http.StatusServiceUnavailable})
	round()
	want("after one failed probe", true, 1)
	if n := len(f.events.failings()); n != 0 {
		t.Fatalf("PathProbeFailing after one failed probe: %d reports", n)
	}
	round()
	want("after two failed probes", false, 1)
	if status, body, _ := f.chat("while-skipped"); status != http.StatusServiceUnavailable || !strings.Contains(body, "no_ready_target") {
		t.Errorf("a request while the probe fails: %d %s, want 503 no_ready_target", status, body)
	}
	fails := f.events.failings()
	if len(fails) != 1 || fails[0].UID != a || fails[0].Failures != 2 || fails[0].Status != http.StatusServiceUnavailable || fails[0].Registered {
		t.Fatalf("PathProbeFailing reports %+v, want one for %s after 2 failures with status 503", fails, a)
	}
	round()
	if n := len(f.events.failings()); n != 1 {
		t.Errorf("a third failed probe reported PathProbeFailing again: %d reports", n)
	}
	close(hold)
	if status := <-held; status != http.StatusOK {
		t.Errorf("the request in flight through the skip ended %d, want 200", status)
	}
	want("the held request done", false, 0)

	e.Set(enginetest.Behaviour{})
	round()
	want("after one passing probe", true, 0)
	if status, body, _ := f.chat("back"); status != http.StatusOK {
		t.Fatalf("after the probe passed again: %d %s", status, body)
	}

	// The engine killed with frpc up: the probe's request dies in the tunnel.
	e.Close()
	round()
	want("killed, one failed probe", true, 0)
	round()
	want("killed, two failed probes", false, 0)
	if n := len(f.events.failings()); n != 2 {
		t.Errorf("PathProbeFailing reports = %d after the second loss, want 2", n)
	}
	e2 := rig.engineOn(sock, key)
	round()
	want("restarted, one passing probe", true, 0)
	if status, body, _ := f.chat("restarted"); status != http.StatusOK || len(e2.Requests()) != 1 {
		t.Errorf("after the restart: %d %s, the new engine saw %d requests", status, body, len(e2.Requests()))
	}
}
