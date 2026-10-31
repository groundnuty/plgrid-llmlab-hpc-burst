package front

import (
	"bufio"
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// maxEventBytes bounds one SSE line; maxWholeParse bounds how much of a
// non-streamed body is kept to read its usage.
const (
	maxEventBytes = 64 << 20
	maxWholeParse = 16 << 20
)

// hopHeaders are the hop-by-hop headers (RFC 9110 section 7.6.1) and the
// headers the front frames itself; they never pass to the other side.
var hopHeaders = []string{"Connection", "Keep-Alive", "Proxy-Connection", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade"}

// stripHop removes the hop-by-hop headers, including those Connection names.
func stripHop(h http.Header) {
	for _, v := range h.Values("Connection") {
		for _, name := range strings.Split(v, ",") {
			if name = strings.TrimSpace(name); name != "" {
				h.Del(name)
			}
		}
	}
	for _, k := range hopHeaders {
		h.Del(k)
	}
}

// idleReader resets the idle time-out on every byte from the upstream.
type idleReader struct {
	r     io.Reader
	touch func()
}

func (i *idleReader) Read(p []byte) (int, error) {
	n, err := i.r.Read(p)
	if n > 0 {
		i.touch()
	}
	return n, err
}

// relay returns the target's response: its status and headers plus the
// attribution headers, then its body (HBEP-5, Request path step 8). Every
// header and body byte from the target passes with every target credential
// redacted, on every path and status; the front frames the body itself, so
// the target's Content-Length is dropped (@hpc-burst-science-agent, 31 Oct
// 2026). A compressed body is decoded first (gzip, deflate); another
// encoding cannot be redacted and is answered 502 upstream_unreachable,
// detail encoding.
func (p *Proxy) relay(w http.ResponseWriter, x *exchange, t *target, resp *http.Response, rb requestBody, path string) {
	defer resp.Body.Close()
	raw, err := decodeBody(resp)
	if err != nil {
		x.attr = forTarget(x.id, t, emitterFront)
		x.rec.Emitter = emitterFront
		p.log.Warn("the target's response cannot be read", "request", x.id, "target", t.key(), "error", err.Error())
		p.fail(w, x, http.StatusBadGateway, codeUpstreamUnreachable, "encoding", "", err.Error())
		return
	}
	secrets := p.secrets(p.targetKey(t))
	h := w.Header()
	for k, vs := range resp.Header {
		h[k] = append([]string(nil), vs...)
	}
	stripHop(h)
	h.Del("Content-Length")
	h.Del("Content-Encoding")
	redactHeader(h, secrets)
	x.attr.set(h)
	x.rec.Status = resp.StatusCode
	kind := endpointOf(path)
	sse := strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream")
	x.rec.Stream = sse
	body := newRedactReader(&idleReader{r: raw, touch: x.touch}, secrets)
	w.WriteHeader(resp.StatusCode)
	if sse {
		// The status and the attribution reach the client at once, not
		// with the first event after a long prefill.
		_ = http.NewResponseController(w).Flush()
	}
	switch {
	case resp.StatusCode >= 400:
		x.rec.Finish = strp("error")
		p.copyRaw(w, x, body, sse)
	case sse && kind.streams():
		j := p.relayEvents(w, x, body, kind)
		x.failed = j.failed || j.engineError || j.badError
	case sse:
		p.copyRaw(w, x, body, true)
	default:
		p.relayWhole(w, x, body, kind, true)
	}
}

// decodeBody returns the response's body decoded from its Content-Encoding.
// The front asks for identity, so a target that compresses anyway is rare.
func decodeBody(resp *http.Response) (io.Reader, error) {
	switch enc := strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Encoding"))); enc {
	case "", "identity":
		return resp.Body, nil
	case "gzip", "x-gzip":
		zr, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("the target's gzip body cannot be read: %w", err)
		}
		return zr, nil
	case "deflate":
		// HTTP's deflate is zlib (RFC 9110 section 8.4.1.2); some servers
		// send raw deflate, told apart by the zlib header.
		br := bufio.NewReader(resp.Body)
		hdr, err := br.Peek(2)
		if err == nil && hdr[0]&0x0f == 8 && (uint16(hdr[0])<<8|uint16(hdr[1]))%31 == 0 {
			zr, err := zlib.NewReader(br)
			if err != nil {
				return nil, fmt.Errorf("the target's deflate body cannot be read: %w", err)
			}
			return zr, nil
		}
		return flate.NewReader(br), nil
	default:
		return nil, fmt.Errorf("the target answered with Content-Encoding %q, which the front cannot read to redact", enc)
	}
}

// relayEvents passes a stream through event by event, judging it by its
// endpoint's completion rule. A stream that ends without its end, or is cut,
// gets one error event and never [DONE] (HBEP-5, Errors; open question 10).
func (p *Proxy) relayEvents(w http.ResponseWriter, x *exchange, body io.Reader, kind endpoint) *judge {
	rc := http.NewResponseController(w)
	j := &judge{kind: kind}
	br := bufio.NewReaderSize(body, 64<<10)
	var ev bytes.Buffer
	var readErr error
	handle := func() bool {
		forward, done := j.event(ev.Bytes())
		if j.sawContent && x.rec.FirstTokenMs == nil {
			x.rec.FirstTokenMs = i64p(x.clk.Now().Sub(x.start).Milliseconds())
		}
		if forward {
			if _, err := w.Write(ev.Bytes()); err != nil {
				x.cancel(errors.New("client gone"))
				return true
			}
			_ = rc.Flush()
		}
		ev.Reset()
		return done
	}
	for {
		line, err := br.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			ev.Write(line)
			if ev.Len() > maxEventBytes {
				readErr = errors.New("an SSE line is larger than the front accepts")
				break
			}
			continue
		}
		ev.Write(line)
		if err == nil && len(bytes.TrimRight(line, "\r\n")) == 0 {
			if handle() {
				break
			}
		}
		if err != nil {
			readErr = err
			// A cut inside a line drops that line: forwarded, it would run
			// into the error event that follows. Complete lines still pass,
			// ended as an event, since one may carry the finish reason.
			ev.Truncate(bytes.LastIndexByte(ev.Bytes(), '\n') + 1)
			if len(bytes.TrimSpace(ev.Bytes())) > 0 {
				ev.WriteString("\n")
				handle()
			}
			break
		}
	}
	p.endStream(w, x, j, readErr)
	return j
}

// endStream records how a judged stream ended and sends the error event for a
// stream that did not complete.
func (p *Proxy) endStream(w http.ResponseWriter, x *exchange, j *judge, readErr error) {
	if j.usage != nil {
		x.rec.PromptTokens, x.rec.CachedTokens, x.rec.CompletionTokens = j.usage.prompt, j.usage.cached, j.usage.completion
		x.rec.CountsSource = strp("engine")
	} else {
		x.rec.CountsSource = strp("front_events")
		x.rec.ContentEvents = i64p(j.contentEvents)
	}
	if j.complete && !j.engineError && !j.badError {
		x.rec.Finish = strp(j.finish)
		if j.errorCode != "" {
			x.rec.ErrorCode = strp(j.errorCode)
		}
		return
	}
	cut := x.cutBy()
	if j.engineError || j.badError {
		// The engine failed the stream and said so: truncated, cut by the
		// upstream, with the event's code. Its own event was the client's
		// one error event; one that was not JSON was withheld, and the
		// front's stream_cut stands in for it (@hpc-burst-science-agent,
		// 31 Oct 2026).
		x.rec.Finish, x.rec.CutBy = strp("truncated"), strp(cutUpstream)
		if cut != "" {
			x.rec.Finish, x.rec.CutBy = strp("cut"), strp(cut)
		}
		if j.errorCode != "" {
			x.rec.ErrorCode = strp(j.errorCode)
		}
		p.log.Warn("the engine failed the stream", "request", x.id, "errorCode", j.errorCode)
		if j.badError && cut != cutClient {
			p.writeStreamError(w, "upstream", "the engine failed the stream; its error event was not JSON")
		}
		return
	}
	switch {
	case cut != "":
	case readErr != nil && !errors.Is(readErr, io.EOF):
		cut = cutUpstream
	default:
		// The upstream ended the stream cleanly, without its end.
		x.rec.Finish, x.rec.CutBy = strp("truncated"), strp(cutUpstream)
		p.writeStreamError(w, "upstream", "the stream ended without its end; it is truncated")
		return
	}
	x.rec.Finish, x.rec.CutBy = strp("cut"), strp(cut)
	p.log.Warn("stream cut", "request", x.id, "cutBy", cut)
	if cut != cutClient {
		p.writeStreamError(w, detailOf(cut), "the stream was cut: "+detailOf(cut))
	}
}

func (p *Proxy) writeStreamError(w http.ResponseWriter, detail, msg string) {
	if _, err := w.Write(streamErrorEvent(detail, msg)); err == nil {
		_ = http.NewResponseController(w).Flush()
	}
}

// copyRaw relays a body as it comes, flushing each read when it streams.
func (p *Proxy) copyRaw(w http.ResponseWriter, x *exchange, body io.Reader, flush bool) {
	rc := http.NewResponseController(w)
	buf := make([]byte, 32<<10)
	first := true
	for {
		n, err := body.Read(buf)
		if n > 0 {
			if first {
				first = false
				x.rec.FirstTokenMs = i64p(x.clk.Now().Sub(x.start).Milliseconds())
			}
			if _, werr := w.Write(buf[:n]); werr != nil {
				x.cancel(errors.New("client gone"))
				p.cutWhole(x, werr)
				return
			}
			if flush {
				_ = rc.Flush()
			}
		}
		if err == io.EOF {
			return
		}
		if err != nil {
			p.cutWhole(x, err)
			return
		}
	}
}

// relayWhole relays a non-streamed body and reads its finish and usage.
func (p *Proxy) relayWhole(w http.ResponseWriter, x *exchange, body io.Reader, kind endpoint, identity bool) {
	var keep bytes.Buffer
	tee := io.TeeReader(body, &limitWriter{w: &keep, n: maxWholeParse})
	before := x.rec.Finish
	p.copyRaw(w, x, tee, false)
	if x.rec.Finish != before || !identity || keep.Len() >= maxWholeParse {
		return
	}
	finish, u := wholeResult(kind, keep.Bytes())
	x.rec.Finish = finish
	if u != nil {
		x.rec.PromptTokens, x.rec.CachedTokens, x.rec.CompletionTokens = u.prompt, u.cached, u.completion
		x.rec.CountsSource = strp("engine")
	}
}

// cutWhole records a body cut after its headers. With no event to send, the
// connection is aborted, so the client cannot take a short body for a whole
// one.
func (p *Proxy) cutWhole(x *exchange, err error) {
	cut := x.cutBy()
	if cut == "" {
		cut = cutUpstream
	}
	x.rec.Finish, x.rec.CutBy = strp("cut"), strp(cut)
	p.log.Warn("response cut", "request", x.id, "cutBy", cut, "error", err.Error())
	panic(http.ErrAbortHandler)
}

// limitWriter keeps at most n bytes and drops the rest without failing.
type limitWriter struct {
	w io.Writer
	n int
}

func (l *limitWriter) Write(p []byte) (int, error) {
	if l.n <= 0 {
		return len(p), nil
	}
	k := len(p)
	if k > l.n {
		k = l.n
	}
	l.n -= k
	if _, err := l.w.Write(p[:k]); err != nil {
		return 0, err
	}
	return len(p), nil
}
