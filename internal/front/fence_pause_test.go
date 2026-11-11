package front

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// The pause and the front's own fence (#371; the science agent's ruling on #380, 11 Nov 2026): a
// dial aborted because the registration epoch changed, or one that finds the transport already
// gone from the router's stale view, is not the Instance failing and does not count toward the
// pause. Every other failed dial counts as before.

// fencedTransports is the harness's registry with two failures to inject per UID: fail makes the
// UID's dialer fail with that error, and hide makes it return no dialer without telling the
// subscribers, as the router's view stays behind for a moment after a registration was taken away.
type fencedTransports struct {
	*StaticTransports
	mu   sync.Mutex
	fail map[types.UID]error
	hide map[types.UID]bool
}

func (f *fencedTransports) Dialer(uid types.UID) (DialFunc, bool) {
	f.mu.Lock()
	err, hidden := f.fail[uid], f.hide[uid]
	f.mu.Unlock()
	if hidden {
		return nil, false
	}
	d, ok := f.StaticTransports.Dialer(uid)
	if !ok || err == nil {
		return d, ok
	}
	return func(context.Context) (net.Conn, error) { return nil, err }, true
}

func (f *fencedTransports) set(uid types.UID, err error, hide bool) {
	f.mu.Lock()
	f.fail[uid], f.hide[uid] = err, hide
	f.mu.Unlock()
}

// overFenced rebuilds the fixture's router and proxy over a fencedTransports around its registry,
// as newHarnessOn builds them, and serves them on a new client listener.
func overFenced(f *scFix) *fencedTransports {
	f.t.Helper()
	h := f.harness
	ft := &fencedTransports{StaticTransports: h.trans, fail: map[types.UID]error{}, hide: map[types.UID]bool{}}
	var clk clock.Clock = clock.Real{}
	if h.clk != nil {
		clk = h.clk
	}
	log := slog.New(slog.NewTextHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	keys, err := ReadClientKeys(h.cfg.ClientKeysFile)
	if err != nil {
		f.t.Fatal(err)
	}
	h.router = NewRouter(h.cfg, clk, ft, KubeRecorder{R: eventcount.Events(h.memEvents, h.events, eventcount.ControllerFront)}, log, h.metrics)
	h.proxy = NewProxy(ProxyOptions{Config: h.cfg, Host: h.host, Clock: clk, Router: h.router, Transports: ft, Keys: keys, Sink: h.sink, Metrics: h.metrics, Log: log})
	ln, err := enginetest.Listen(testPortLo, testPortHi)
	if err != nil {
		f.t.Fatal(err)
	}
	srv := clientServer(h.cfg, h.proxy, h.base)
	go func() { _ = srv.Serve(ln) }()
	f.t.Cleanup(func() { _ = srv.Close() })
	h.url = "http://" + ln.Addr().String()
	h.apply()
	return ft
}

const fencedChat = `{"model":"` + scModel + `","messages":[{"role":"user","content":"hi"}]}`

// TestTheFrontsOwnFenceDoesNotCountTowardThePause: three requests to one Instance whose dials all
// stop at the front's fence (each request is tried twice, so six dials) leave it with no failure
// in its run and unpaused: once the fence is gone, the next request is served. Once for a dial
// aborted because the epoch moved, and once for a transport gone from the router's stale view.
func TestTheFrontsOwnFenceDoesNotCountTowardThePause(t *testing.T) {
	for _, c := range []struct {
		name string
		err  error
		hide bool
	}{
		{"a dial aborted because the registration epoch changed", fmt.Errorf("CONNECT 00000000-0000-4000-8000-000000000001: %w", errReregistered), false},
		{"no transport left in the router's stale view", nil, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newScFix(t, scOpts{})
			ft := overFenced(f)
			uid := f.uids[0]
			ft.set(uid, c.err, c.hide)
			for i := 0; i < 3; i++ {
				resp, body := f.prSend(t, scChat, fencedChat)
				if resp.StatusCode != http.StatusBadGateway || !strings.Contains(body, "upstream_unreachable") {
					t.Fatalf("request %d through the fence: %d %s, want 502 upstream_unreachable", i, resp.StatusCode, body)
				}
			}
			f.wantFailures(t, "six dials stopped at the front's fence", 0)
			if n := f.arrived(); n != 0 {
				t.Fatalf("the engine saw %d requests through the fence", n)
			}
			ft.set(uid, nil, false)
			if resp, body := f.prSend(t, scChat, fencedChat); resp.StatusCode != http.StatusOK {
				t.Fatalf("after the fence: %d %s, want 200 from an Instance that was never paused", resp.StatusCode, body)
			}
		})
	}
}

// TestAFailedDialStillCountsTowardThePause: the exemption is the front's fence alone. A dial that
// fails for any other reason counts, as before: one request tried twice leaves two failures in the
// Instance's run.
func TestAFailedDialStillCountsTowardThePause(t *testing.T) {
	f := newScFix(t, scOpts{})
	ft := overFenced(f)
	ft.set(f.uids[0], errors.New("dial tcp 127.0.0.1:1: connect: connection refused"), false)
	if resp, body := f.prSend(t, scChat, fencedChat); resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("a request whose dials fail: %d %s, want 502", resp.StatusCode, body)
	}
	f.wantFailures(t, "one request whose two dials failed", 2)
}
