package front

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// PR #95 review (Astra B1, Opus B2): a client's Idempotency-Key made Go's
// transport resend a POST that was already written, when the reused
// connection then failed. The front keeps the header and refuses that
// resend, while the one resend HBEP-5 allows, a reused connection that
// failed before any byte was written, still happens.

// TestNoReplayOfAWrittenRequest: the first request leaves the connection
// idle; the second meets an engine that reads it and closes before the
// headers. The engine must get it once, with or without the header.
func TestNoReplayOfAWrittenRequest(t *testing.T) {
	for _, hdr := range []string{"", "Idempotency-Key", "X-Idempotency-Key"} {
		name := hdr
		if name == "" {
			name = "no header"
		}
		t.Run(name, func(t *testing.T) {
			f := newScFix(t, scOpts{})
			e := f.engines[0]
			var extra []string
			if hdr != "" {
				extra = []string{hdr, "key-0001"}
			}
			f.prSend(t, scChat, errChatBody(scModel, false, "hi"))
			if n := len(e.Conns()); n != 1 {
				t.Fatalf("%d connections after the first request, want 1", n)
			}
			e.Set(enginetest.Behaviour{CloseBeforeHeaders: true})
			resp := f.errPost(scChat, errChatBody(scModel, false, "again"), extra...)
			body := readAll(t, resp)
			reqs := e.Requests()
			if len(reqs) != 2 {
				t.Errorf("the engine received %d requests, want 2: the second was sent again", len(reqs))
			}
			if n := len(e.Conns()); n != 1 {
				t.Errorf("%d connections, want 1: a second one means the request was sent again, or the first was not reused", n)
			}
			if resp.StatusCode != 502 {
				t.Errorf("status %d, want 502 upstream_closed: %s", resp.StatusCode, body)
			}
			if hdr != "" && reqs[len(reqs)-1].Header.Get(hdr) != "key-0001" {
				t.Errorf("the %s header did not pass to the engine", hdr)
			}
		})
	}
}

// failWriteTransports wraps StaticTransports: when failNext is set, the next
// write on any connection fails before writing a byte, as a write on a
// connection the peer already closed does.
type failWriteTransports struct {
	*StaticTransports
	failNext atomic.Bool
}

func (ft *failWriteTransports) Dialer(uid types.UID) (DialFunc, bool) {
	d, ok := ft.StaticTransports.Dialer(uid)
	if !ok {
		return nil, false
	}
	return func(ctx context.Context) (net.Conn, error) {
		c, err := d(ctx)
		if err != nil {
			return nil, err
		}
		return &failWriteConn{Conn: c, ft: ft}, nil
	}, true
}

type failWriteConn struct {
	net.Conn
	ft *failWriteTransports
}

func (c *failWriteConn) Write(p []byte) (int, error) {
	if c.ft.failNext.CompareAndSwap(true, false) {
		return 0, errors.New("write: broken pipe (test)")
	}
	return c.Conn.Write(p)
}

// TestReplayWhenNothingWasWritten: the second request's write on the reused
// connection fails with no byte written; Go's transport resends it once on a
// fresh connection, and the client gets the engine's answer. A guard keyed
// on httptrace's WroteHeaders would refuse this resend.
func TestReplayWhenNothingWasWritten(t *testing.T) {
	f := newScFix(t, scOpts{})
	e := f.engines[0]
	ft := &failWriteTransports{StaticTransports: f.trans}
	f.proxy.inst = newInstanceTransports(f.cfg, ft)
	f.prSend(t, scChat, errChatBody(scModel, false, "hi"))
	if n := len(e.Conns()); n != 1 {
		t.Fatalf("%d connections after the first request, want 1", n)
	}
	ft.failNext.Store(true)
	resp := f.errPost(scChat, errChatBody(scModel, false, "again"), "Idempotency-Key", "key-0002")
	body := readAll(t, resp)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d, want 200 after the nothing-written resend: %s", resp.StatusCode, body)
	}
	if ft.failNext.Load() {
		t.Fatal("the failing write was never attempted: the case is not tested")
	}
	if n := len(e.Requests()); n != 2 {
		t.Errorf("the engine received %d requests, want 2", n)
	}
	if n := len(e.Conns()); n != 2 {
		t.Errorf("%d connections, want 2: the resend goes on a fresh connection", n)
	}
}
