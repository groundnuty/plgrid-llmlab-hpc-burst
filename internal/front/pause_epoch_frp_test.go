package front

// #418 over the real frps and frpc, with the front's plugin: frps goes down while the front still routes to the
// Instance, and the requests in the gap pause it; frps comes back, frpc logs in again on its own, and the first request
// once a probe has passed in the new registration gets its reply, long before the pause would have run out (S16's
// finding on #412: about 30 s with 503 model_inactive in between).

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"
)

// TestAnFrpsRestartWithRequestsInTheGapRoutesAgainOnTheFirstPass: three requests in the gap, each dial refused while
// frps is down, pause the Instance (the positive control: without it the test would not reach the case). The front's
// pause lasts 10 minutes here, so no wait of it can pass the test. frps posts no CloseProxy for the session it lost (the
// front's plugin hears NewProxy alone, as from an frps that crashed), so the route stays until frpc registers again.
func TestAnFrpsRestartWithRequestsInTheGapRoutesAgainOnTheFirstPass(t *testing.T) {
	slot := listenFrontPlugin(t, "127.0.0.1:0")
	rig := newFrpRigWith(t, frpRigOptions{frontPlugin: slot.addr})
	f := newTunFrontWith(t, rig, func(c *Config) { c.PauseDuration = Duration(10 * time.Minute) })
	slot.set(&frpsPlugin{tun: f.tun, prober: f.prober, log: f.log})
	slot.mu.Lock()
	slot.only = opNewProxy
	slot.mu.Unlock()
	dial, _ := rig.serveDialHome(ModeSSH)
	f.tunDeployment(1)
	key := newKey(t)
	_, sock := rig.engine(key)
	a := f.tunInstance("md-a", key)
	p := rig.startFrpc(a, sock, dial)
	rig.waitOnline(a, p)
	f.apply()
	f.waitRegistered(a)
	if status, body, _ := f.chat("before"); status != http.StatusOK {
		t.Fatalf("before the restart: %d %s", status, body)
	}

	if _, err := rig.proc.Stop(context.Background(), 5*time.Second); err != nil {
		t.Fatalf("stop frps: %v", err)
	}
	const target = "ns/md-a"
	for i := 0; i < 3; i++ {
		if status, body, _ := f.chat(fmt.Sprintf("gap-%d", i)); status == http.StatusOK {
			t.Fatalf("request %d in the gap, with frps down: %d %s", i, status, body)
		}
	}
	if !f.router.Paused(target) {
		t.Fatalf("three requests in the gap did not pause the Instance (failures %d): the test does not reach the case", f.router.failures(target))
	}
	until := f.router.pauseUntil(target)

	rig.startFrps()
	rig.waitOnline(a, p)
	f.waitReprobed(a)
	f.router.quiesce(t) // the rebuild that follows the pass, which clears the pause (#417 coalesces them)
	registered := time.Now()
	status, body, _ := f.chat("after")
	if status != http.StatusOK {
		t.Fatalf("the first request after a probe passed in the new registration: %d %s, want its reply", status, body)
	}
	if !time.Now().Before(until) {
		t.Fatal("the pause had run out before the reply: the test proves nothing")
	}
	t.Logf("first reply %s after the new registration was registered, with the old pause %s from its end", time.Since(registered).Round(time.Millisecond), time.Until(until).Round(time.Second))
}
