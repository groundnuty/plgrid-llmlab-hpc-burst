package agent_test

// The bulk frpc against the real frps and frpc v0.71.1 (HBEP-6, The bulk path;
// Test plan, Certificate; issue #448, part 4 and the science agent's ruling R8):
// it logs in through the second certificate and no other, to the tcpMux-off
// frps, and the token frpc beside it is untouched. The rig is bulkrig_test.go's.
// A test with no trustedCaFile proves nothing, since frpc then verifies nothing:
// every configuration here carries one (rawBulk checks it).

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	// uidNeg names the proxy of a frpc that must never register: a name no
	// token frpc uses, so the token frpc's own proxy cannot stand for it.
	uidNeg     = "aaaaaaaa-bbbb-cccc-dddd-000000000002"
	enginePath = "/health"
)

func engineBody() string { return "engine for " + hInstUID + ": " + enginePath }

// The bulk frpc logs in through a TLS listener that serves the second
// certificate for the bulk name (GetCertificate), to a frps with tcpMux off;
// the token frpc logs in through the same listener under its own name to the
// first frps. Each frps lists the Instance's proxy online and routes a CONNECT
// to the engine; before the bulk frpc starts, the second frps lists nothing.
func TestBulkFrpcLogsInThroughTheSecondCertificate(t *testing.T) {
	r := newBulkRig(t, bulkRigOptions{})
	tok, tokStarts := r.frpc(false, 300*time.Millisecond)
	r.run(tok, hInstUID)
	eventually(t, 20*time.Second, "the first frps to list the token proxy", func() bool { return r.frps1.online(t, hInstUID) })
	if r.frps2.online(t, hInstUID) {
		t.Fatal("the second frps lists the Instance's proxy before the bulk frpc started")
	}
	bulk, bulkStarts := r.frpc(true, 300*time.Millisecond)
	r.run(bulk, hInstUID)
	eventually(t, 20*time.Second, "the second frps to list the bulk proxy", func() bool { return r.frps2.online(t, hInstUID) })
	r.frps1.waitRoute(t, hInstUID, enginePath, engineBody())
	r.frps2.waitRoute(t, hInstUID, enginePath, engineBody())
	if connect, status, body, err := r.frps2.request(hInstUID, enginePath); err != nil || connect != 200 || status != 200 || body != engineBody() {
		t.Fatalf("through the bulk tunnel: CONNECT %d, %d %q, %v", connect, status, body, err)
	}
	// An unknown UID gets 404 on the bulk frps, as on the first.
	if connect, _, _, err := r.frps2.request("ffffffff-0000-0000-0000-000000000000", enginePath); err != nil || connect != 404 {
		t.Fatalf("an unknown UID on the second frps: CONNECT %d, %v; want 404", connect, err)
	}
	if r.front.toToken.Load() < 1 || r.front.toBulk.Load() < 1 {
		t.Fatalf("GET /~!frp reached the first frps %d times and the second %d times, want both at least once", r.front.toToken.Load(), r.front.toBulk.Load())
	}
	for _, n := range []string{tokenName, bulkName} {
		if got := r.front.others(n); got != 0 {
			t.Errorf("%d requests other than GET /~!frp under %s", got, n)
		}
		if got := r.front.failedHandshakes(n); got != 0 {
			t.Errorf("%d failed handshakes under %s", got, n)
		}
	}
	if tokStarts.count() != 1 || bulkStarts.count() != 1 {
		t.Fatalf("frpc started %d (token) and %d (bulk) times, want once each", tokStarts.count(), bulkStarts.count())
	}
	// Its files: the bulk frpc's own, mode 600, and frpc accepts its configuration.
	for _, n := range []string{"frpc-bulk.toml", "frpc-bulk.log", "dial-home-bulk.crt", "frpc.toml", "dial-home.crt"} {
		if m := factsOf(t, filepath.Join(r.runDir, n)).mode; m != 0o600 {
			t.Errorf("%s has mode %o, want 600 (frpc makes its own log 600)", n, m)
		}
	}
	frpcVerifies(t, r.frpcBn, bulk.ConfigPath())
	frpcVerifies(t, r.frpcBn, tok.ConfigPath())
}

// The negative controls, all of one rig with the token frpc running beside
// them (HBEP-6, Test plan, Certificate: "frpc facing another certificate, and
// frpc with the right certificate but another serverName, are refused"; "the
// bulk path's second certificate ... only the bulk frpc trusts"). Each refused
// frpc is the real one, started with a configuration of the agent's renderer
// that the agent's own checks would never send it, and its proxy has a name of
// its own: it must never appear online on either frps, the failure must come at
// TLS or at the mux handshake and not at the TCP connection, and the token frpc,
// whose process ID and start time were recorded first, runs on untouched.
func TestBulkFrpcTrustsOnlyTheSecondCertificate(t *testing.T) {
	r := newBulkRig(t, bulkRigOptions{})
	tok, tokStarts := r.frpc(false, 300*time.Millisecond)
	r.run(tok, hInstUID)
	eventually(t, 20*time.Second, "the token frpc to log in", func() bool { return r.frps1.online(t, hInstUID) })
	r.frps1.waitRoute(t, hInstUID, enginePath, engineBody())
	tokID, ok := tokStarts.last()
	if !ok {
		t.Fatal("the token frpc recorded no start")
	}
	assertTokenUntouched := func(t *testing.T) {
		t.Helper()
		if !tokID.Alive() {
			t.Fatalf("the token frpc %+v is gone", tokID)
		}
		if got := tokStarts.all(); len(got) != 1 || got[0] != tokID {
			t.Fatalf("the token frpc was started again: %+v, was one start %+v", got, tokID)
		}
		if !r.frps1.online(t, hInstUID) {
			t.Fatal("the first frps no longer lists the token proxy")
		}
	}
	neverOnline := func(t *testing.T) {
		t.Helper()
		for name, f := range map[string]*bulkFrps{"first": r.frps1, "second": r.frps2} {
			if f.online(t, uidNeg) {
				t.Fatalf("the %s frps lists the refused frpc's proxy online", name)
			}
		}
	}

	// The control, in this rig: the same real frpc with the second certificate
	// and the bulk name logs in. Without it a refusal below could be the rig's.
	t.Run("control: it trusts the second certificate and names the bulk server", func(t *testing.T) {
		_, _, p := r.rawBulk("raw-control", uidNeg, r.secondFile, bulkName, true)
		eventually(t, 20*time.Second, "the control frpc to log in to the second frps", func() bool { return r.frps2.online(t, uidNeg) })
		p.stop()
		eventually(t, 10*time.Second, "the second frps to drop the control's proxy", func() bool { return !r.frps2.online(t, uidNeg) })
		assertTokenUntouched(t)
	})

	t.Run("it trusts the first certificate", func(t *testing.T) {
		before, passedBefore := r.front.failedHandshakes(bulkName), r.front.frpConns(bulkName)
		_, log, p := r.rawBulk("raw-first", uidNeg, r.firstFile, bulkName, true)
		eventually(t, 25*time.Second, "a handshake under the bulk name to fail", func() bool { return r.front.failedHandshakes(bulkName) > before })
		eventually(t, 10*time.Second, "frpc to log its refusal", func() bool { return strings.Contains(tail(log, 20000), "connect to server error") })
		b := tail(log, 20000)
		if !strings.Contains(b, "x509") && !strings.Contains(b, "certificate") {
			t.Fatalf("frpc's refusal names no certificate (the failure must come at TLS):\n%s", b)
		}
		if strings.Contains(b, "login to server success") {
			t.Fatalf("frpc logged in:\n%s", b)
		}
		neverOnline(t)
		// The counter is the rig's, so the control above is in it: only what came after counts.
		if n := r.front.frpConns(bulkName) - passedBefore; n != 0 {
			t.Fatalf("%d GET /~!frp reached an frps under the bulk name from a frpc that trusts the first certificate", n)
		}
		p.stop()
		assertTokenUntouched(t)
	})

	t.Run("it names the token server", func(t *testing.T) {
		before, passedBefore := r.front.failedHandshakes(tokenName), r.front.frpConns(tokenName)
		_, log, p := r.rawBulk("raw-tokenname", uidNeg, r.secondFile, tokenName, true)
		eventually(t, 25*time.Second, "a handshake under the token name to fail", func() bool { return r.front.failedHandshakes(tokenName) > before })
		eventually(t, 10*time.Second, "frpc to log its refusal", func() bool { return strings.Contains(tail(log, 20000), "connect to server error") })
		b := tail(log, 20000)
		if !strings.Contains(b, "x509") && !strings.Contains(b, "certificate") {
			t.Fatalf("frpc's refusal names no certificate:\n%s", b)
		}
		if strings.Contains(b, "login to server success") {
			t.Fatalf("frpc logged in:\n%s", b)
		}
		neverOnline(t)
		if n := r.front.frpConns(tokenName) - passedBefore; n != 0 {
			t.Fatalf("%d GET /~!frp reached an frps under the token name from a frpc that trusts only the second certificate", n)
		}
		p.stop()
		assertTokenUntouched(t)
	})
}

// A bulk frpc that reaches the first frps never registers (M1, issue #448: a
// frpc with tcpMux off whose websocket reaches an frps with tcpMux on fails at
// once with "invalid protocol version"): the failure is closed. The frpc trusts
// the first certificate and names the token server, so its TLS passes and the
// listener sends it to the first frps. The control is the same rig and the same
// trust with tcpMux left on, which logs in.
func TestBulkFrpcThatReachesTheFirstFrpsNeverRegisters(t *testing.T) {
	r := newBulkRig(t, bulkRigOptions{})
	_, _, ctl := r.rawBulk("raw-muxon", uidNeg, r.firstFile, tokenName, false)
	eventually(t, 20*time.Second, "the control (tcpMux on) to register on the first frps", func() bool { return r.frps1.online(t, uidNeg) })
	ctl.stop()
	eventually(t, 10*time.Second, "the first frps to drop the control's proxy", func() bool { return !r.frps1.online(t, uidNeg) })

	before := r.front.frpConns(tokenName)
	_, log, p := r.rawBulk("raw-muxoff", uidNeg, r.firstFile, tokenName, true)
	eventually(t, 25*time.Second, "the mux-off frpc to reach the first frps", func() bool { return r.front.frpConns(tokenName) > before })
	eventually(t, 10*time.Second, "frpc to log that its connection failed", func() bool { return strings.Contains(tail(log, 20000), "connect to server error") })
	b := tail(log, 20000)
	if strings.Contains(b, "login to server success") {
		t.Fatalf("a tcpMux-off frpc logged in to the first frps:\n%s", b)
	}
	if r.frps1.online(t, uidNeg) || r.frps2.online(t, uidNeg) {
		t.Fatal("the mux-off frpc's proxy is online")
	}
	p.stop()
}

// handshakeGaps is the least wait between two of frpc's consecutive login
// attempts when every one fails, from frp v0.71.1's source (the bound's origin,
// not this repository's code): the first login is client/service.go:276
// loopLoginUntilSuccess(10 s, loginFailExit = false), which retries through
// wait.BackoffUntil with pkg/util/wait/backoff.go's NewFastBackoffManager
// {Duration 1 s, Factor 2, Jitter 0.1, MaxDuration 10 s}, with no fast retry:
// the delay after a failure is the previous one (1 s at first) times 2, plus
// 0-10% of itself, never less, capped at 10 s. So the waits are at least 2 s,
// 4 s, 8 s, then 10 s each. frpc does not exit on a login failure
// (loginFailExit = false), so the agent's restart backoff, which governs an frpc
// that exits (frpc.go; its own tests), never enters here.
var handshakeGaps = []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second}

// handshakeSlack covers the clock's and the scheduler's noise between the
// listener's stamp and frpc's own timer: the listener can only stamp a failed
// handshake late, never early, and a late stamp shortens the measured gap.
const handshakeSlack = 500 * time.Millisecond

// checkHandshakeBackoff returns an error when two consecutive failed
// handshakes came closer than frp's backoff allows.
func checkHandshakeBackoff(recs []hsRecord) error {
	for i := 1; i < len(recs); i++ {
		want := handshakeGaps[min(i-1, len(handshakeGaps)-1)] - handshakeSlack
		if gap := recs[i].at.Sub(recs[i-1].at); gap < want {
			return fmt.Errorf("handshakes %d and %d were %s apart, less than the %s frp's backoff allows (minus %s of slack)", i, i+1, gap.Round(time.Millisecond), want+handshakeSlack, handshakeSlack)
		}
	}
	return nil
}

// R8 (the science agent, 12 Nov 2026, #448): against a dial-home listener that
// serves only the first certificate, as after a rollback (an Allocation staged
// with the bulk block, the host turned the bulk path off), the real bulk frpc
// retries with backoff, never in a tight loop. The failure comes at TLS (the
// listener accepts the connection and serves its certificate, which the bulk
// frpc does not trust), nothing reaches an frps, and the handshakes' spacing
// follows frp's login backoff (handshakeGaps). The test waits for the fourth
// handshake, the observable, not for a fixed window, and then checks the spacing
// of the first four: at least 2 s, 4 s and 8 s. Load only widens the gaps.
func TestBulkFrpcBacksOffAgainstABulkOffListener(t *testing.T) {
	r := newBulkRig(t, bulkRigOptions{bulkOff: true, noFrps: true})
	bulk, starts := r.frpc(true, 0) // the production restart backoff (1 s doubling to 30 s)
	r.run(bulk, hInstUID)
	eventually(t, 90*time.Second, "four handshakes under the bulk name", func() bool { return len(r.front.handshakes(bulkName)) >= 4 })
	recs := r.front.handshakes(bulkName)
	for i, h := range recs {
		if h.ok {
			t.Fatalf("handshake %d completed: the bulk frpc trusted the first certificate", i+1)
		}
	}
	if n := r.front.frpConns(bulkName) + r.front.frpConns(tokenName); n != 0 {
		t.Fatalf("%d GET /~!frp passed the listener: the failure was not at TLS", n)
	}
	var gaps []time.Duration
	for i := 1; i < 4; i++ {
		gaps = append(gaps, recs[i].at.Sub(recs[i-1].at).Round(10*time.Millisecond))
	}
	t.Logf("the bulk frpc's gaps between its first four failed handshakes: %v (frp's least: 2 s, 4 s, 8 s)", gaps)
	if err := checkHandshakeBackoff(recs[:4]); err != nil {
		t.Fatalf("the bulk frpc did not back off: %v\nagent log:\n%s", err, r.logs.String())
	}
	if got := starts.count(); got != 1 {
		t.Fatalf("the agent started frpc %d times: frpc must retry its login itself (loginFailExit = false), not exit", got)
	}
	b := tail(bulk.LogPath(), 20000)
	if !strings.Contains(b, "x509") && !strings.Contains(b, "certificate") {
		t.Fatalf("frpc's log names no certificate error:\n%s", b)
	}
}

// R8's positive control: the bound can fail. A client that dials the same
// listener in a tight loop, with the same trust and name as the bulk frpc,
// gets four failed handshakes at once, and checkHandshakeBackoff says so.
func TestTheBackoffBoundCatchesATightLoop(t *testing.T) {
	pair := makeBulkPair(t)
	front := startSplitFront(t, pair.first.tls, nil, "127.0.0.1:1", "127.0.0.1:1")
	pool := x509.NewCertPool()
	pool.AddCert(pair.second.tls.Leaf)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			c, err := tls.Dial("tcp4", fmt.Sprintf("127.0.0.1:%d", front.port()), &tls.Config{ServerName: bulkName, RootCAs: pool, MinVersion: tls.VersionTLS12})
			if err == nil {
				c.Close()
			}
		}
	}()
	eventually(t, 30*time.Second, "four handshakes from the tight loop", func() bool { return len(front.handshakes(bulkName)) >= 4 })
	close(stop)
	<-done
	recs := front.handshakes(bulkName)
	if front.failedHandshakes(bulkName) < 4 {
		t.Fatalf("the control's handshakes did not fail as the bulk frpc's: %d failed", front.failedHandshakes(bulkName))
	}
	if err := checkHandshakeBackoff(recs[:4]); err == nil {
		t.Fatal("the bound accepted a tight loop: R8's check cannot fail")
	}
	// And it accepts spacing that follows the backoff.
	t0 := time.Now()
	spaced := []hsRecord{{at: t0}, {at: t0.Add(2100 * time.Millisecond)}, {at: t0.Add(6200 * time.Millisecond)}, {at: t0.Add(14500 * time.Millisecond)}}
	if err := checkHandshakeBackoff(spaced); err != nil {
		t.Fatalf("the bound refused a backoff-shaped spacing: %v", err)
	}
}

// A kill of the bulk frpc, by pidfd, leaves the token frpc alone: the agent
// starts the bulk frpc again on its own, with backoff, and the survivor's
// process ID and start time, recorded before the kill, are unchanged after it,
// and it was never started a second time (HBEP-6, The bulk path: "the agent
// restarts it with backoff on its own, apart from the token frpc"). The bulk
// frpc logs in again.
func TestBulkFrpcRestartsAlone(t *testing.T) {
	r := newBulkRig(t, bulkRigOptions{})
	tok, tokStarts := r.frpc(false, 300*time.Millisecond)
	bulk, bulkStarts := r.frpc(true, 300*time.Millisecond)
	r.run(tok, hInstUID)
	r.run(bulk, hInstUID)
	eventually(t, 20*time.Second, "both proxies online", func() bool { return r.frps1.online(t, hInstUID) && r.frps2.online(t, hInstUID) })
	r.frps1.waitRoute(t, hInstUID, enginePath, engineBody())
	r.frps2.waitRoute(t, hInstUID, enginePath, engineBody())
	tokID, _ := tokStarts.last()
	bulkID, _ := bulkStarts.last()
	if tokID.PID == 0 || tokID.StartTime == 0 || bulkID.PID == 0 || bulkID.StartTime == 0 {
		t.Fatalf("recorded IDs %+v and %+v lack a PID or a start time", tokID, bulkID)
	}
	pidfdKill(t, bulkID)
	eventually(t, 20*time.Second, "the bulk frpc to start again", func() bool { return bulkStarts.count() >= 2 })
	again, _ := bulkStarts.last()
	if again == bulkID {
		t.Fatalf("the bulk frpc's second start is the killed process %+v", bulkID)
	}
	eventually(t, 20*time.Second, "the second frps to list the restarted bulk proxy", func() bool { return r.frps2.online(t, hInstUID) })
	r.frps2.waitRoute(t, hInstUID, enginePath, engineBody())
	if !tokID.Alive() {
		t.Fatalf("the token frpc %+v did not survive the bulk frpc's kill", tokID)
	}
	if got := tokStarts.all(); len(got) != 1 || got[0] != tokID {
		t.Fatalf("the token frpc was started again: %+v, was one start %+v", got, tokID)
	}
	if !r.frps1.online(t, hInstUID) {
		t.Fatal("the first frps lost the token proxy")
	}
}

// The reverse: a kill of the token frpc leaves the bulk frpc alone.
func TestTokenFrpcRestartLeavesBulk(t *testing.T) {
	r := newBulkRig(t, bulkRigOptions{})
	tok, tokStarts := r.frpc(false, 300*time.Millisecond)
	bulk, bulkStarts := r.frpc(true, 300*time.Millisecond)
	r.run(tok, hInstUID)
	r.run(bulk, hInstUID)
	eventually(t, 20*time.Second, "both proxies online", func() bool { return r.frps1.online(t, hInstUID) && r.frps2.online(t, hInstUID) })
	r.frps1.waitRoute(t, hInstUID, enginePath, engineBody())
	r.frps2.waitRoute(t, hInstUID, enginePath, engineBody())
	tokID, _ := tokStarts.last()
	bulkID, _ := bulkStarts.last()
	if tokID.PID == 0 || tokID.StartTime == 0 || bulkID.PID == 0 || bulkID.StartTime == 0 {
		t.Fatalf("recorded IDs %+v and %+v lack a PID or a start time", tokID, bulkID)
	}
	pidfdKill(t, tokID)
	eventually(t, 20*time.Second, "the token frpc to start again", func() bool { return tokStarts.count() >= 2 })
	again, _ := tokStarts.last()
	if again == tokID {
		t.Fatalf("the token frpc's second start is the killed process %+v", tokID)
	}
	eventually(t, 20*time.Second, "the first frps to list the restarted token proxy", func() bool { return r.frps1.online(t, hInstUID) })
	r.frps1.waitRoute(t, hInstUID, enginePath, engineBody())
	if !bulkID.Alive() {
		t.Fatalf("the bulk frpc %+v did not survive the token frpc's kill", bulkID)
	}
	if got := bulkStarts.all(); len(got) != 1 || got[0] != bulkID {
		t.Fatalf("the bulk frpc was started again: %+v, was one start %+v", got, bulkID)
	}
	if !r.frps2.online(t, hInstUID) {
		t.Fatal("the second frps lost the bulk proxy")
	}
}
