package front

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
)

// Why a request was cut, as the cancel cause of its context; each maps to the
// usage record's cutBy and the error's detail (HBEP-5, Time-outs and
// keep-alive: every cut is logged and recorded with the hop that made it).
var (
	errCutDeadline = errors.New("maxRequestDuration reached")
	errCutIdle     = errors.New("idleTimeout reached: no byte from the upstream")
	errCutLeaseEnd = errors.New("the Instance's lease ended")
)

// Values of cutBy.
const (
	cutFrontDeadline = "front_deadline"
	cutFrontIdle     = "front_idle"
	cutUpstream      = "upstream"
	cutLeaseEnd      = "lease_end"
	cutClient        = "client"
)

// exchange is one request in flight: its timers, its attribution and its
// usage record.
type exchange struct {
	id    string
	start time.Time
	clk   clock.Clock
	rec   usage.Record
	attr  attribution

	ctx    context.Context
	cancel context.CancelCauseFunc

	mu       sync.Mutex
	deadline clock.Timer
	idle     clock.Timer
	idleFor  time.Duration
	lease    clock.Timer
	leaseGen int // the target the lease timer belongs to; a resend arms a new one
}

func newExchange(r *http.Request, clk clock.Clock, maxReq time.Duration) *exchange {
	x := &exchange{id: newRequestID(), start: clk.Now(), clk: clk}
	x.ctx, x.cancel = context.WithCancelCause(r.Context())
	x.attr = attribution{requestID: x.id, emitter: emitterFront}
	x.rec = usage.Record{RequestID: x.id, StartTime: usage.Time(x.start), Path: r.URL.Path, Emitter: emitterFront}
	x.deadline = clk.AfterFunc(maxReq, func() { x.cancel(errCutDeadline) })
	return x
}

// shortenDeadline applies a target's shorter maxRequestDuration, counted from
// the request's arrival.
func (x *exchange) shortenDeadline(maxReq time.Duration) {
	x.mu.Lock()
	defer x.mu.Unlock()
	left := maxReq - x.clk.Now().Sub(x.start)
	if left <= 0 {
		x.cancel(errCutDeadline)
		return
	}
	x.deadline.Reset(left)
}

// armIdle starts the idle time-out: no byte from the upstream for d cuts the
// request. touch resets it.
func (x *exchange) armIdle(d time.Duration) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.idleFor = d
	if x.idle == nil {
		x.idle = x.clk.AfterFunc(d, func() { x.cancel(errCutIdle) })
		return
	}
	x.idle.Reset(d)
}

func (x *exchange) touch() {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.idle != nil {
		x.idle.Reset(x.idleFor)
	}
}

// armLease cuts the request at the Instance's lease end. When the timer
// fires it reads the lease end again, since a later hard end, once observed,
// moves it (HBEP-5, Draining, The end). A resend to another Instance replaces
// the timer: the old one is stopped, and if it fires anyway its generation no
// longer matches.
func (x *exchange) armLease(current func() (time.Time, bool)) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.lease != nil {
		x.lease.Stop()
		x.lease = nil
	}
	x.leaseGen++
	gen := x.leaseGen
	end, ok := current()
	if !ok {
		return
	}
	var fire func()
	fire = func() {
		end, ok := current()
		x.mu.Lock()
		defer x.mu.Unlock()
		if gen != x.leaseGen {
			return
		}
		if ok {
			if left := end.Sub(x.clk.Now()); left > 0 {
				x.lease = x.clk.AfterFunc(left, fire)
				return
			}
		}
		x.cancel(errCutLeaseEnd)
	}
	x.lease = x.clk.AfterFunc(end.Sub(x.clk.Now()), fire)
}

// stopTimers ends the request's timers.
func (x *exchange) stopTimers() {
	x.mu.Lock()
	defer x.mu.Unlock()
	for _, t := range []clock.Timer{x.deadline, x.idle, x.lease} {
		if t != nil {
			t.Stop()
		}
	}
}

// cutBy maps the context's cause to the hop that cut the request, or "".
func (x *exchange) cutBy() string {
	switch cause := context.Cause(x.ctx); {
	case cause == nil:
		return ""
	case errors.Is(cause, errCutDeadline):
		return cutFrontDeadline
	case errors.Is(cause, errCutIdle):
		return cutFrontIdle
	case errors.Is(cause, errCutLeaseEnd):
		return cutLeaseEnd
	default:
		return cutClient
	}
}

// detail is the error detail of a cut: deadline, idle, lease_end, upstream.
func detailOf(cutBy string) string {
	switch cutBy {
	case cutFrontDeadline:
		return "deadline"
	case cutFrontIdle:
		return "idle"
	case cutLeaseEnd:
		return "lease_end"
	}
	return "upstream"
}

func newRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("front: crypto/rand: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

func strp(s string) *string { return &s }
func i64p(v int64) *int64   { return &v }
