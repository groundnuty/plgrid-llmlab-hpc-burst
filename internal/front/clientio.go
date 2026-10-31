package front

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"
)

// clientIO ties the client connection's read and write deadlines to the
// request's own life (PR #95 review: Astra B9, Opus's stalled-client nit).
// The client listener sets no deadline of its own (HBEP-5: none shorter than
// maxRequestDuration), so a client that stops sending its body, or stops
// reading the response, would hold the handler, the connection and the
// reservation past every cut. When the front cuts the request (its deadline,
// idle time-out, lease end or shutdown) or the client goes away, a body read
// in progress ends at once, and writes get writeGrace to send the front's
// error, then fail.
type clientIO struct {
	rc      *http.ResponseController
	reading atomic.Bool
	fired   chan struct{}
	unbind  func() bool
}

func bindClientIO(w http.ResponseWriter, x *exchange, grace time.Duration) *clientIO {
	c := &clientIO{rc: http.NewResponseController(w), fired: make(chan struct{})}
	c.reading.Store(true)
	c.unbind = context.AfterFunc(x.ctx, func() {
		defer close(c.fired)
		if c.reading.Load() {
			_ = c.rc.SetReadDeadline(time.Now())
		}
		_ = c.rc.SetWriteDeadline(time.Now().Add(grace))
	})
	return c
}

// bodyRead says the body has been read: a later cut leaves reads alone.
func (c *clientIO) bodyRead() { c.reading.Store(false) }

// stop ends the binding before the request's context is cancelled at its
// end. After a cut it flushes what is buffered within the grace, then clears
// the write deadline so a kept-alive connection can serve its next request;
// a flush that fails means the client is gone, and the deadline stays.
func (c *clientIO) stop() {
	if c.unbind() {
		return
	}
	<-c.fired
	if c.rc.Flush() == nil {
		_ = c.rc.SetWriteDeadline(time.Time{})
	}
}
