package front

// The front's half of the bulk path (beta; HBEP-6, The bulk path; HBEP-5, Inside an Instance; #448,
// part 6). With the bulk path on, the front keeps a second transport per Instance, on the second
// frps's tcpmux port: its own Tunnel (registration and epoch fence, #380's code), its own pool, and
// its own Prober at the token probe's cadence. A routed POST whose top-level stream is not true
// takes it when it is registered. Everything the router decides (eligibility, the cap, pauses) stays
// the token transport's: the bulk transport only changes which tunnel carries a request.
//
//   - A bulk dial that fails before any byte is written (set-up, frps's 404, the epoch fence, no
//     transport) unregisters the bulk transport until its next passing probe and sends the request
//     through the same Instance's token transport. It counts nothing, and it isn't the request's
//     resend: the token send then follows HBEP-5 as today, resent once to the next target on a dial
//     error of its own.
//   - A bulk request closed before its response headers (cutBy tunnel), or cut by the front's
//     idleTimeout with no byte, unregisters the bulk transport and puts it on the re-probe
//     schedule. It counts nothing toward the Instance's pause and is not replayed.
//   - A response's own outcome counts as on the token path: an upstream cut after the headers, and a
//     5xx, count toward the pause.
//
// Without the bulk path none of this exists: Proxy.bulk is nil, and every request takes the token
// transport through the code that ran before.

import (
	"context"
	"log/slog"
	"net"
	"net/http"

	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// bulkPath is the front's bulk transport: the second frps's Tunnel, its per-Instance HTTP
// transports (a pool of their own, with the token pool's upstreamIdleTimeout and size), and its
// Prober, which raises no event.
type bulkPath struct {
	tun    *Tunnel
	prober *Prober
	inst   *instanceTransports
	log    *slog.Logger
}

// newBulkPath builds the bulk transport on the second frps's tcpmux port, mux. source is the
// Instances the front probes, as the token Prober's.
func newBulkPath(cfg Config, mux string, clk clock.Clock, source func() []v1alpha1.Instance, log *slog.Logger) *bulkPath {
	log = log.With("tunnel", "bulk")
	tun := NewTunnel(mux, cfg.TunnelSetupTimeout.D())
	return &bulkPath{
		tun:    tun,
		prober: NewProber(cfg, tun, clk, source, NopPathEvents{}, log),
		inst:   newInstanceTransports(cfg, tun),
		log:    log,
	}
}

// run probes the bulk transports until ctx ends, as Prober.Run does the token ones; the returned
// stop ends it and waits.
func (b *bulkPath) run(ctx context.Context) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.prober.Run(ctx)
	}()
	return func() { cancel(); <-done }
}

// plugin is the front's plugin for the second frps, at agentapi.FrontFrpsBulkPluginPath: a bulk
// NewProxy or CloseProxy moves only the UID's bulk epoch, and leaves the token path's routing alone
// (HBEP-6, The bulk path: admission and the front's plugin).
func (b *bulkPath) plugin(log *slog.Logger) *frpsPlugin {
	return &frpsPlugin{tun: b.tun, prober: b.prober, log: log.With("tunnel", "bulk"), path: agentapi.FrontFrpsBulkPluginPath}
}

// takes reports whether the request goes to the bulk transport (HBEP-6, The bulk path: the
// request-time rule): a target that is an Instance whose bulk transport is registered, and a
// request whose top-level stream is not true, which the front parsed before dispatch.
func (b *bulkPath) takes(t *target, rb requestBody) bool {
	if b == nil || t.inst == nil || rb.stream {
		return false
	}
	return b.tun.Registered(t.inst.UID)
}

// unregister takes the Instance's bulk registration away until a probe passes again (a dial that
// failed before any byte).
func (b *bulkPath) unregister(uid types.UID) {
	b.tun.setRegistered(uid, false)
}

// lose takes the Instance's bulk registration away and puts it on the re-probe schedule (a request
// closed before its headers).
func (b *bulkPath) lose(uid types.UID) {
	b.tun.setRegistered(uid, false)
	b.prober.Reregistered(uid)
}

// bulkOutcome is what forwardBulk did with the request.
type bulkOutcome int

const (
	bulkAnswered bulkOutcome = iota // it answered the client
	bulkFellBack                    // its dial failed before any byte: the same target's token transport takes it
	bulkHeld                        // a 5xx before any body byte, held for forward's retry (HBEP-5, open question 5)
	bulkClosed                      // written and then closed before the response headers, for forward's retry
)

// forwardBulk sends the request through the target's bulk transport (HBEP-6, The bulk path). It
// answers the client, or returns bulkFellBack after a dial that failed before any byte was written:
// forward then sends the request through the same target's token transport, as if the bulk path had
// not been tried, so the token send keeps HBEP-5's one resend. A failure the front may still retry to
// another Instance (a close before the headers, or a 5xx before any body byte, held unwritten) is
// handed back to forward, which retries it as on the token path; a close still counts nothing toward
// the pause.
func (p *Proxy) forwardBulk(w http.ResponseWriter, r *http.Request, x *exchange, t *target, body []byte, rb requestBody, key string) (bulkOutcome, *http.Response, error) {
	uid := t.inst.UID
	// conn is the connection the attempt took and base its count of written bytes then: the
	// request counts as sent on the bulk path only if some byte of it reached that connection
	// (the plan's R1), whatever error a cut leaves, such as the context's cause when the client
	// leaves during the bulk dial.
	var conn *countingConn
	var base int64
	resp, err := p.sendWith(x, r, t, body, rb, key, p.bulk.inst, func(c net.Conn) {
		if conn = countingOf(c); conn != nil {
			base = conn.written.Load()
		}
	})
	wrote := resp != nil || (conn != nil && conn.written.Load() != base)
	if err != nil && isDialError(err) {
		if x.ctx.Err() != nil {
			// The client left, or the front cut the request, while the bulk dial ran: nothing
			// was written anywhere, so it counts neither as bulk nor as a fallback.
			t.release()
			p.failedBulkBeforeHeaders(w, x, t, err)
			return bulkAnswered, nil, nil
		}
		p.bulk.unregister(uid)
		p.metrics.bulkFellBack()
		p.bulk.log.Warn("the bulk transport could not be dialled; nothing was sent, and the request takes the token transport", "request", x.id, "target", t.key(), "error", err.Error())
		return bulkFellBack, nil, nil
	}
	if wrote {
		p.metrics.bulkSent()
	}
	switch {
	case err == nil && p.retryable(x, t) && retryableStatus(resp.StatusCode):
		return bulkHeld, resp, nil
	case err == nil:
		// Deferred, so a cut body's abort panic reports too, as on the token path.
		defer p.reportOutcome(t, x)
		p.relay(w, x, t, resp, rb, r.URL.Path)
		t.release()
		return bulkAnswered, nil, nil
	case p.retryable(x, t):
		return bulkClosed, nil, err
	}
	t.release()
	p.failedBulkBeforeHeaders(w, x, t, err)
	return bulkAnswered, nil, nil
}

// failedBulkBeforeHeaders answers a bulk request that failed after it was written, or was cut,
// before any response header, and never sends it again. A close from the tunnel's side (cutBy
// tunnel) and the front's idle cut take the bulk registration away and re-probe it; neither counts
// toward the Instance's pause, since the token path may still serve (HBEP-6, The bulk path: failures).
// The client's own disconnect and the front's other cuts are answered as on the token path, where
// none of them counts either.
func (p *Proxy) failedBulkBeforeHeaders(w http.ResponseWriter, x *exchange, t *target, err error) {
	switch cut := x.cutBy(); cut {
	case cutClient, cutFrontDeadline, cutLeaseEnd, cutFrontShutdown:
		p.failedBeforeHeaders(w, x, t, err)
	case cutFrontIdle:
		p.bulk.lose(t.inst.UID)
		p.failedBeforeHeaders(w, x, t, err)
	default:
		p.bulk.lose(t.inst.UID)
		x.attr = forTarget(x.id, t, emitterFront)
		x.rec.Emitter = emitterFront
		x.rec.CutBy = strp(cutTunnel)
		p.bulk.log.Warn("the bulk connection closed after the request was written, before the response headers; the bulk transport is re-probed, and the request is not sent again", "request", x.id, "target", t.key(), "error", err.Error())
		p.fail(w, x, http.StatusBadGateway, codeUpstreamClosed, cutTunnel, "", "the connection to the target closed before it answered; the request is not sent again")
	}
}
