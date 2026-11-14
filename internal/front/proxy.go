package front

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptrace"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/clock"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/convkey"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/naming"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// MaxBodyBytes bounds a request body the front reads to find its model.
const MaxBodyBytes = 64 << 20

// Proxy serves the LLMLab-compatible contract on the client listener.
type Proxy struct {
	cfg      Config
	host     agentapi.Host
	clk      clock.Clock
	router   *Router
	keys     *ClientKeys
	sink     *usage.Sink
	metrics  *Metrics // nil: none
	log      *slog.Logger
	inst     *instanceTransports
	managed  *http.Transport
	running  sync.WaitGroup // the handlers in flight, for a stop
	instKeys sync.Map       // Instance UID -> its key, for redaction
	// afterFinish, when set, runs at the end of each request's finish, after
	// its usage record's write and any event that write raised: the request
	// path has no pass marker of its own, so a test proves that a request
	// raised no event by it (HBEP-8, Raised events, condition 3).
	afterFinish func(requestID string)
	// beforeWrite, when set, runs in each request's finish just before its
	// usage record's write, so a test holds one handler there, after the
	// shutdown cut and before its record exists: a stop that does not wait
	// for the handler loses that record (#432). Nil in production.
	beforeWrite func(requestID string)
	// bulk is the bulk path's transport (bulk.go); nil without the bulk path.
	bulk *bulkPath
}

// ProxyOptions are the inputs of NewProxy.
type ProxyOptions struct {
	Config     Config
	Host       agentapi.Host // the Instance keys live in Host.CredentialsDir()
	Clock      clock.Clock
	Router     *Router
	Transports Transports
	Keys       *ClientKeys
	Sink       *usage.Sink
	Metrics    *Metrics // nil: none
	Log        *slog.Logger
}

// NewProxy returns the client listener's handler.
func NewProxy(o ProxyOptions) *Proxy {
	var d net.Dialer
	return &Proxy{
		cfg: o.Config, host: o.Host, clk: o.Clock, router: o.Router, keys: o.Keys, sink: o.Sink, metrics: o.Metrics, log: o.Log,
		inst:    newInstanceTransports(o.Config, o.Transports),
		managed: upstreamTransport(d.DialContext, o.Config, true),
	}
}

// ServeHTTP routes the client listener: the contract under /v1/, 404 under
// /agent/v1/ and everywhere else (HBEP-5, Listeners).
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.running.Add(1)
	defer p.running.Done()
	if !strings.HasPrefix(r.URL.Path, "/v1/") {
		// /agent/v1/ belongs to the dial-home listener only. Still attributed,
		// since a response without the headers is a failure of the front,
		// and recorded, since HBEP-5 keeps one record per request.
		x := newExchange(r, p.clk, p.cfg.MaxRequestDuration.D())
		defer p.finish(x)
		x.attr.set(w.Header())
		x.rec.Status, x.rec.Finish = http.StatusNotFound, strp("error")
		http.NotFound(w, r)
		return
	}
	p.serveV1(w, r)
}

// waitHandlers waits at most d for the handlers in flight to return.
func (p *Proxy) waitHandlers(d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		p.running.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

func (p *Proxy) serveV1(w http.ResponseWriter, r *http.Request) {
	x := newExchange(r, p.clk, p.cfg.MaxRequestDuration.D())
	defer p.finish(x)
	cio := bindClientIO(w, x, p.cfg.writeGrace)
	defer cio.stop()
	id, ok := p.authenticate(r)
	if !ok {
		p.fail(w, x, http.StatusUnauthorized, codeInvalidAPIKey, "", "", "no client key, or a wrong one")
		return
	}
	x.rec.ClientKeyID = id
	if r.Method == http.MethodGet && (r.URL.Path == "/v1/models" || strings.HasPrefix(r.URL.Path, "/v1/models/")) {
		p.serveModels(w, r, x)
		return
	}
	if r.Method != http.MethodPost {
		p.fail(w, x, http.StatusBadRequest, codeInvalidRequest, "", "", "the front routes POST requests with a JSON body; GET /v1/models lists the models")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
	cio.bodyRead()
	if err != nil {
		switch cut := x.cutBy(); cut {
		case "":
			p.fail(w, x, http.StatusBadRequest, codeInvalidRequest, "", "", "the body could not be read: "+err.Error())
		case cutClient:
			x.rec.Status, x.rec.Finish, x.rec.CutBy = statusClientClosed, strp("cut"), strp(cut)
		default:
			p.cutBeforeFirstByte(w, x, cut, "the request was cut while its body was read: ")
		}
		return
	}
	rb, err := parseBody(body)
	if err != nil {
		p.fail(w, x, http.StatusBadRequest, codeInvalidRequest, "", "model", err.Error())
		return
	}
	x.rec.RequestedModel = rb.model
	x.rec.Stream = rb.stream
	n, err := naming.Parse(rb.model)
	if err != nil {
		p.fail(w, x, http.StatusBadRequest, codeInvalidModelName, "grammar", "model", "the model name breaks the naming grammar: model, model@provider or model@provider/<12 hex>")
		return
	}
	x.rec.Level = string(n.Level)
	key, _ := convkey.Key(r.Header, p.cfg.SessionHeaders, rb.messages)
	p.forward(w, r, x, n, key, body, rb)
}

// authenticate checks the bearer key against the client keys file.
func (p *Proxy) authenticate(r *http.Request) (string, bool) {
	scheme, key, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || key == "" {
		return "", false
	}
	return p.keys.Match(strings.TrimSpace(key))
}

// forward chooses a target, sends, and relays (HBEP-5, Request path steps 5
// to 9). A request is sent again in two ways, each at most once (open
// question 5):
//   - alpha's resend: a connection no byte of which was written (refused,
//     timed out, or a reused idle connection closed before the write) goes to
//     the next target on the ring, or to the same one on a fresh connection
//     when there is no other. Go's transport resends on its own only a request
//     on a reused connection that wrote nothing, since withOneReplay's GetBody
//     refuses once a byte was written;
//   - beta's retry: toward an Instance, a request written and then closed
//     before the response headers, or answered with a 5xx before any body byte
//     (not a 504), goes to another ready Instance, never the same one. The 5xx
//     is held, not written, until the front has decided.
//
// Every target the request failed on is skipped by the next choice; alpha's
// fallback to a fresh connection may take a target again only when its dial
// failed, never one the request was written to.
func (p *Proxy) forward(w http.ResponseWriter, r *http.Request, x *exchange, n naming.Name, key string, body []byte, rb requestBody) {
	var written, unreached []string // the targets the request was written to and lost, and those a dial failed on
	var lastUnreached *target
	dialResent := false
	t, rerr := p.router.choose(x.ctx, n, key, nil)
	if rerr != nil {
		p.routeFail(w, x, rerr)
		return
	}
	for {
		// Released exactly once however the request ends, including the
		// http.ErrAbortHandler panic of a cut body (HBEP-5, Request path
		// step 9); Release is idempotent, so the explicit calls stay.
		defer t.release()
		p.setTarget(x, t)
		var held *http.Response // a 5xx before any body byte, not yet written
		var err error
		bulk := false // the failure came through the bulk transport: a close there counts nothing toward the pause
		if p.bulk.takes(t, rb) {
			var o bulkOutcome
			switch o, held, err = p.forwardBulk(w, r, x, t, body, rb, key); o {
			case bulkAnswered:
				return
			case bulkHeld, bulkClosed:
				bulk = true
			}
		}
		var resp *http.Response
		if !bulk {
			resp, err = p.send(x, r, t, body, rb, key)
		}
		switch {
		case bulk:
		case err == nil && !(p.retryable(x, t) && retryableStatus(resp.StatusCode)):
			// Only the attempt that answers the client defers its report:
			// a deferred report reads the request's final record. Deferred,
			// so a cut body's abort panic reports too.
			defer p.reportOutcome(t, x)
			p.relay(w, x, t, resp, rb, r.URL.Path)
			t.release()
			return
		case err == nil:
			held = resp
		case isDialError(err) && x.ctx.Err() == nil:
			t.release()
			// The front's own fence is not the target failing, so it
			// doesn't count toward the pause: a dial aborted because the
			// Instance registered again, or no transport left in the
			// router's view (#371). The re-probe that follows decides
			// whether the Instance takes traffic again.
			if !isOwnFence(err) {
				p.router.ReportConnect(t.key(), true)
			}
			p.log.Warn("no connection to the target; nothing was sent", "request", x.id, "target", t.key(), "error", err.Error())
			if dialResent {
				p.unreachable(w, x, t)
				return
			}
			dialResent, lastUnreached = true, t
			unreached = append(unreached, t.key())
			// Once the request was written somewhere, it goes only to another
			// Instance, never to a managed provider.
			choose := p.router.choose
			if len(written) > 0 {
				choose = p.router.chooseInstance
			}
			next, rerr := choose(x.ctx, n, key, append(slices.Clip(written), unreached...))
			if rerr != nil {
				// No other target: the same one again on a fresh connection,
				// but never one the request was written to.
				next, rerr = choose(x.ctx, n, key, written)
			}
			if rerr != nil {
				p.unreachable(w, x, lastUnreached)
				return
			}
			t = next
			continue
		case !p.retryable(x, t):
			t.release()
			p.failedBeforeHeaders(w, x, t, err)
			return
		}
		// A written request lost before any response byte, retried once: the
		// target is chosen before the failure is answered or the held 5xx
		// closed, so with no other Instance ready the client gets the first
		// failure as it came.
		t.release()
		// The failed Instance's idle and lease timers stop across the choice,
		// so neither cuts a request bound elsewhere while it waits in the FIFO;
		// the retry's target arms its own (setTarget, sendWith).
		x.stopIdle()
		x.armLease(func() (time.Time, bool) { return time.Time{}, false })
		written = append(written, t.key())
		next, rerr := p.router.chooseInstance(x.ctx, n, key, append(slices.Clip(written), unreached...))
		if rerr != nil && rerr.code != codeCapacity && rerr.cutBy == "" && rerr.status != statusClientClosed {
			// No other Instance ready: no retry, and no header, as alpha. The
			// failed Instance's timers are its own again.
			p.setTarget(x, t)
			x.armIdle(p.cfg.IdleTimeout.D())
			switch {
			case held != nil:
				defer p.reportOutcome(t, x)
				p.relay(w, x, t, held, rb, r.URL.Path)
			case bulk:
				p.failedBulkBeforeHeaders(w, x, t, err)
			default:
				p.failedBeforeHeaders(w, x, t, err)
			}
			return
		}
		// The retry is decided. The failure counts toward its own Instance's
		// pause once, here, but a close on the bulk transport, which takes the
		// bulk registration away instead (HBEP-6, The bulk path); its answer is
		// never written.
		if bulk && held == nil {
			p.bulk.lose(t.inst.UID)
		} else {
			p.router.ReportConnect(t.key(), true)
		}
		if held != nil {
			_ = held.Body.Close()
			p.log.Warn("the target answered a 5xx before any body byte; the request is sent once more, to another Instance", "request", x.id, "target", t.key(), "status", held.StatusCode)
		} else {
			p.log.Warn("the connection closed after the request was written, before the response headers; the request is sent once more, to another Instance", "request", x.id, "target", t.key(), "error", err.Error())
		}
		x.retried = true
		if rerr != nil {
			// Placed in the FIFO and not served within queueTimeout, or cut
			// while it waited there: the front's answer, attributed to the
			// front as before any target was chosen (Errors), not to the
			// Instance that failed.
			x.untarget()
			p.routeFail(w, x, rerr)
			return
		}
		t = next
	}
}

// retryable reports whether a written request that failed before any byte of
// its response may still be retried (HBEP-5, open question 5): toward an
// Instance, not cut by the client or by the front (a time-out is never
// retried), and not retried before.
func (p *Proxy) retryable(x *exchange, t *target) bool {
	return t.inst != nil && x.ctx.Err() == nil && !x.retried
}

// retryableStatus reports whether a status that came before any body byte is
// a failure the front retries: a 5xx, but not a 504, which leaves the engine
// possibly still running the request.
func retryableStatus(status int) bool {
	return status >= 500 && status != http.StatusGatewayTimeout
}

// headerShouldRetry is set to "false" on a give-up answer after the front's
// own retry, which the OpenAI SDK obeys and doesn't retry (HBEP-5, open
// question 5).
const headerShouldRetry = "X-Should-Retry"

// giveUpAfterRetry reports whether an answer of status is a give-up after the
// front's own retry: every final non-2xx once the retry was decided (#539,
// Q2), whether the front raised it or the target answered it.
func giveUpAfterRetry(x *exchange, status int) bool {
	return x.retried && (status < 200 || status > 299)
}

// reportOutcome counts a relayed response toward the target's pause, by
// the rule #96 states (@hpc-burst-science-agent, 31 Oct 2026):
//   - a 5xx and every upstream-side cut after the headers count: an upstream
//     or tunnel cut, a stream that ended without its end, an engine error
//     event, response.failed;
//   - a completed request resets the run, a capped reply included;
//   - a 4xx neither counts nor resets, and neither does a client disconnect.
//
// Choices where #96 is silent: a cut the front made (deadline, idle,
// lease_end, front_shutdown) neither counts nor resets, since the target did
// nothing wrong that the front saw. A managed target's statuses pause
// nothing: a response of any status resets its run, and only its
// connections count (HBEP-5, The LLMLab route).
func (p *Proxy) reportOutcome(t *target, x *exchange) {
	if t.managed != nil {
		p.router.ReportConnect(t.key(), false)
		return
	}
	switch status := x.rec.Status; {
	case status >= 500:
		p.router.ReportConnect(t.key(), true)
		return
	case status >= 400:
		return
	}
	if x.rec.CutBy != nil {
		switch *x.rec.CutBy {
		case cutUpstream, cutTunnel:
			p.router.ReportConnect(t.key(), true)
		}
		return
	}
	p.router.ReportConnect(t.key(), x.failed)
}

// setTarget records the chosen target and applies its deadline and lease end.
func (p *Proxy) setTarget(x *exchange, t *target) {
	if t.managed != nil {
		x.attr = forTarget(x.id, t, emitterProvider)
		x.rec.Emitter, x.rec.Provider, x.rec.ModelDeployment = emitterProvider, t.managed.Provider, t.managed.Deployment
		x.rec.DeploymentID, x.rec.Instance, x.rec.InstanceUID = t.managed.ManagedID, "", ""
		x.shortenDeadline(t.managed.MaxRequestDuration)
		// A managed target has no lease: drop the timer an earlier attempt
		// armed for an Instance, so its lease end cannot cut this request.
		x.armLease(func() (time.Time, bool) { return time.Time{}, false })
		return
	}
	x.attr = forTarget(x.id, t, emitterInstance)
	x.rec.Emitter, x.rec.Provider, x.rec.ModelDeployment = emitterInstance, t.inst.Provider, t.dep.Key
	x.rec.DeploymentID, x.rec.Instance, x.rec.InstanceUID = t.inst.DeploymentID, t.inst.Key, string(t.inst.UID)
	x.shortenDeadline(t.dep.MaxRequestDuration)
	uid := t.inst.UID
	x.armLease(func() (time.Time, bool) { return p.router.LeaseEnd(uid) })
}

// send writes the request to the target: the model value rewritten, the body
// framed with its new Content-Length, the client's key replaced by the
// target's, and toward an Instance the x-session-id set and the client's
// x-envoy-* and X-Forwarded-For headers removed (HBEP-5, Contract and Request
// headers).
func (p *Proxy) send(x *exchange, r *http.Request, t *target, body []byte, rb requestBody, key string) (*http.Response, error) {
	return p.sendWith(x, r, t, body, rb, key, p.inst, nil)
}

// sendWith is send toward an Instance through inst: the token transports (send), or the bulk
// path's (forwardBulk). Toward a managed target inst is not used. gotConn, when set, is told each
// connection an attempt takes (httptrace's GotConn), so the bulk path can tell whether any byte of
// the request reached it.
func (p *Proxy) sendWith(x *exchange, r *http.Request, t *target, body []byte, rb requestBody, key string, inst *instanceTransports, gotConn func(net.Conn)) (*http.Response, error) {
	h := r.Header.Clone()
	stripHop(h)
	h.Del("Authorization")
	h.Del("Content-Length")
	// The front redacts what it relays, so it takes bodies uncompressed
	// (@hpc-burst-science-agent, 31 Oct 2026).
	h.Set("Accept-Encoding", "identity")
	if _, ok := h["User-Agent"]; !ok {
		h["User-Agent"] = []string{""} // Go adds none then: headers pass as sent
	}
	var rt http.RoundTripper
	var url string
	var out []byte
	path := r.URL.EscapedPath()
	if t.inst != nil {
		tr, ok := inst.get(t.inst.UID)
		if !ok {
			return nil, &dialError{err: errNoTransport}
		}
		k, err := agentapi.ReadKeyFile(agentapi.InstanceKeyPath(p.host.CredentialsDir(), string(t.inst.UID)))
		if err != nil {
			return nil, &dialError{err: fmt.Errorf("the Instance key cannot be read: %w", err)}
		}
		rt, url, out = tr, "http://instance"+path, rb.rewrite(body, t.dep.ServedName)
		stripEnvoyControl(h) // only toward an Instance: LLMLab gets the client's headers as before
		h.Set("Authorization", "Bearer "+k)
		h.Set("X-Session-Id", key)
	} else {
		c, err := provider.ReadManagedCredentials(t.managed.CredentialsFile)
		if err != nil {
			return nil, &dialError{err: fmt.Errorf("the Provider's credentials cannot be read: %w", err)}
		}
		if url, err = c.URLFor(path); err != nil {
			return nil, &dialError{err: err}
		}
		rt, out = p.managed, rb.rewrite(body, t.managed.CatalogName)
		h.Set("Authorization", "Bearer "+c.Key)
	}
	if r.URL.RawQuery != "" {
		url += "?" + r.URL.RawQuery
	}
	ctx := x.ctx
	gen := x.startAttempt()
	if t.inst != nil && rb.stream {
		// Toward an Instance a streamed reply's headers must come within
		// streamHeadersTimeout of the request's last byte (HBEP-5, Time-outs
		// and keep-alive); vLLM sends them before prefill. A non-streamed
		// request has no such deadline.
		d := p.cfg.StreamHeadersTimeout.D()
		ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{WroteRequest: func(httptrace.WroteRequestInfo) { x.armHeaders(gen, d) }})
	}
	if gotConn != nil {
		ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{GotConn: func(i httptrace.GotConnInfo) { gotConn(i.Conn) }})
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return nil, &dialError{err: err}
	}
	req = withOneReplay(req, out)
	req.Header = h
	x.armIdle(p.cfg.IdleTimeout.D())
	resp, err := rt.RoundTrip(req)
	x.stopHeaders(gen)
	return resp, err
}

// failedBeforeHeaders answers a request that failed after it was written, or
// was cut, before any response header: never sent again.
func (p *Proxy) failedBeforeHeaders(w http.ResponseWriter, x *exchange, t *target, err error) {
	x.attr = forTarget(x.id, t, emitterFront)
	x.rec.Emitter = emitterFront
	switch cut := x.cutBy(); cut {
	case cutClient:
		x.rec.Status, x.rec.Finish, x.rec.CutBy = statusClientClosed, strp("cut"), strp(cut)
	case cutFrontHeaders:
		// Written, so not resent; a missed streamed-headers deadline counts
		// toward the pause (HBEP-5, Pauses).
		p.router.ReportConnect(t.key(), true)
		x.rec.CutBy = strp(cut)
		p.log.Warn("no response headers within streamHeadersTimeout; the request is not sent again", "request", x.id, "target", t.key())
		p.fail(w, x, http.StatusGatewayTimeout, codeRequestTimeout, detailOf(cut), "", "the target sent no response headers within streamHeadersTimeout; the request is not sent again")
	case cutFrontDeadline, cutFrontIdle, cutLeaseEnd, cutFrontShutdown:
		p.cutBeforeFirstByte(w, x, cut, "the request was cut before its first byte: ")
	default:
		// Written, then closed before any response header: never resent
		// (open question 5). Toward an Instance that is the tunnel's row of
		// the Errors table, and it counts toward the pause. A managed target
		// has no tunnel: the same code, with detail upstream.
		p.router.ReportConnect(t.key(), true)
		cut := cutTunnel
		if t.managed != nil {
			cut = cutUpstream
		}
		x.rec.CutBy = strp(cut)
		p.log.Warn("the connection closed after the request was written, before the response headers", "request", x.id, "target", t.key(), "error", err.Error())
		p.fail(w, x, http.StatusBadGateway, codeUpstreamClosed, cut, "", "the connection to the target closed before it answered; the request is not sent again")
	}
}

// cutBeforeFirstByte answers a request the front cut before any byte of its
// response: 504 request_timeout with the cut's detail, or at a stop 503
// shutting_down with Retry-After 5 (@hpc-burst-science-agent, 31 Oct 2026).
func (p *Proxy) cutBeforeFirstByte(w http.ResponseWriter, x *exchange, cut, msg string) {
	x.rec.CutBy = strp(cut)
	if cut == cutFrontShutdown {
		w.Header().Set("Retry-After", "5")
		p.fail(w, x, http.StatusServiceUnavailable, codeShuttingDown, detailOf(cut), "", "the front is shutting down; send the request again")
		return
	}
	p.fail(w, x, http.StatusGatewayTimeout, codeRequestTimeout, detailOf(cut), "", msg+detailOf(cut))
}

// unreachable is the 502 after the one resend; it names the target tried.
func (p *Proxy) unreachable(w http.ResponseWriter, x *exchange, t *target) {
	x.attr = forTarget(x.id, t, emitterFront)
	x.rec.Emitter = emitterFront
	p.fail(w, x, http.StatusBadGateway, codeUpstreamUnreachable, "", "", "no byte reached the target, after the one resend")
}

func (p *Proxy) routeFail(w http.ResponseWriter, x *exchange, e *routeError) {
	if e.dep != nil {
		x.rec.ModelDeployment = e.dep.Key
	}
	if e.status == statusClientClosed {
		x.rec.Status, x.rec.Finish, x.rec.CutBy = e.status, strp("cut"), strp(cutClient)
		return
	}
	if e.cutBy != "" {
		x.rec.CutBy = strp(e.cutBy)
	}
	if e.retryAfter != "" {
		w.Header().Set("Retry-After", e.retryAfter)
	}
	p.fail(w, x, e.status, e.code, e.detail, "", e.message)
}

// fail writes an error the front raises, with the attribution headers.
func (p *Proxy) fail(w http.ResponseWriter, x *exchange, status int, code, detail, param, message string) {
	h := w.Header()
	x.attr.set(h)
	if giveUpAfterRetry(x, status) && code != codeShuttingDown {
		// A 503 shutting_down asks the client to send again, to a front that
		// will be back (#539, Q2).
		h.Set(headerShouldRetry, "false")
	}
	h.Set("Content-Type", "application/json")
	x.rec.Status = status
	if x.rec.Finish == nil {
		x.rec.Finish = strp("error")
	}
	w.WriteHeader(status)
	_, _ = w.Write(encodeError(code, detail, message, param))
}

// finish ends the request's timers and writes its usage record.
func (p *Proxy) finish(x *exchange) {
	x.stopTimers()
	x.cancel(nil)
	end := p.clk.Now()
	x.rec.EndTime = usage.Time(end)
	x.rec.DurationMs = end.Sub(x.start).Milliseconds()
	// Counted whether or not its record could be written: the client got
	// its answer either way, and the sink counts the loss on its own.
	p.metrics.observe(x.rec)
	if p.beforeWrite != nil {
		p.beforeWrite(x.id)
	}
	if _, err := p.sink.Write(x.rec); err != nil {
		p.log.Error("usage record not written", "request", x.id, "error", err.Error())
		p.router.usageFailed(x.id, err)
	} else {
		p.router.usageWritten()
	}
	p.log.Info("request", "request", x.id, "path", x.rec.Path, "model", x.rec.RequestedModel, "status", x.rec.Status,
		"emitter", x.rec.Emitter, "provider", x.rec.Provider, "instance", x.rec.Instance, "durationMs", x.rec.DurationMs)
	if p.afterFinish != nil {
		p.afterFinish(x.id)
	}
}
