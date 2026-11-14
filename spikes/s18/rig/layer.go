package rig

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front"
)

// Policy is a retry arm of #528. Arm (a), alpha's rule, is no layer at all: the front as it is.
//   - (b): one retry to another Instance (MaxRetries 1, no Bucket).
//   - (c): retries within a per-deployment token bucket (Bucket), at most to every other Instance.
//   - (d): (c) plus HBEP-5's stop: never after two Instances have died under one request
//     (StopAfterDeaths 2). On (b) the stop never binds (two attempts at most), so (d) is built on (c).
//
// A failure is retried when it came before any response header: written, then closed (to the front the
// only sign of a death, Q2), or a 5xx answer. A refused dial wrote nothing, so it is resent once more to
// another Instance with no token and no death counted, as alpha's own rule does.
type Policy struct {
	Name            string
	MaxRetries      int
	Bucket          *Bucket
	StopAfterDeaths int
}

// Budget0 is the layer's control: no retry. It must equal arm (a) request for request.
var Budget0 = Policy{Name: "budget0"}

// Bucket is a per-deployment token bucket: Burst tokens, refilled at Refill per second; each retry
// takes one.
type Bucket struct {
	Burst  float64
	Refill float64

	mu     sync.Mutex
	tokens float64
	last   time.Time
}

// NewBucket returns a full bucket.
func NewBucket(burst, refill float64) *Bucket {
	return &Bucket{Burst: burst, Refill: refill, tokens: burst, last: time.Now()}
}

// Take takes a token, reporting whether there was one.
func (b *Bucket) Take() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.tokens = min(b.Burst, b.tokens+now.Sub(b.last).Seconds()*b.Refill)
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// Attempt is one upstream attempt of the layer, for the analysis (the candidate front-side
// crash-correlation event counts a request's closed attempts).
type Attempt struct {
	ID     string    `json:"id"`
	Home   int       `json:"home"`
	Target int       `json:"target"`
	Kind   string    `json:"kind"` // ok, closed, dial, 5xx, cancelled
	Status int       `json:"status,omitempty"`
	At     time.Time `json:"at"`
}

// Layer is arms (b)-(d): a retry layer at each Instance's tunnel position. The front dials Instance i's
// layer socket; the layer sends the request to Instance i, and on a failure before any header retries
// it on another Instance under its Policy, returning headers to the front only once it has a good reply.
// The other Instances' outcomes are reported to the front's own Router.ReportConnect, and home's the
// front counts itself, so each upstream failure counts toward a pause once, as in the product. Fidelity
// gaps (README): the front attributes the reply and its in-flight count to Instance i; when the layer
// rescues a request that failed on i, the front sees a success on i and resets i's count, so a dead
// Instance behind the layer is paused only when a request to it cannot be rescued; "another Instance" is
// the next ready one in order, not the ring's.
type Layer struct {
	Policy Policy

	rig     *Rig
	servers []*http.Server
	up      []*http.Transport

	mu       sync.Mutex
	attempts []Attempt
}

func newLayer(r *Rig, p Policy) (*Layer, error) {
	l := &Layer{Policy: p, rig: r}
	for i := range r.Front.Instances {
		i := i
		dial := r.Cutter.Wrap(DialUnix(r.Doubles.Socket(i)), r.watch[i].Admitted)
		l.up = append(l.up, &http.Transport{DisableKeepAlives: true, DisableCompression: true,
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				c, err := dial(ctx)
				if err != nil {
					return nil, &dialErr{err}
				}
				return &countConn{Conn: c}, nil
			}})
		sock := filepath.Join(r.dir, fmt.Sprintf("layer%d.sock", i))
		_ = os.Remove(sock)
		ln, err := net.Listen("unix", sock)
		if err != nil {
			return nil, err
		}
		srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { l.serve(i, w, req) })}
		go func() { _ = srv.Serve(ln) }()
		l.servers = append(l.servers, srv)
		r.Trans.Set(r.Front.Instances[i].UID, DialUnix(sock))
	}
	return l, nil
}

type dialErr struct{ err error }

func (e *dialErr) Error() string { return "dial: " + e.err.Error() }

type countConn struct {
	net.Conn
	n atomic.Int64
}

func (c *countConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.n.Add(int64(n))
	return n, err
}

func (l *Layer) record(a Attempt) {
	l.mu.Lock()
	l.attempts = append(l.attempts, a)
	l.mu.Unlock()
}

// Attempts is every upstream attempt so far.
func (l *Layer) Attempts() []Attempt {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Attempt(nil), l.attempts...)
}

func (l *Layer) serve(home int, w http.ResponseWriter, req *http.Request) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return
	}
	id := IDIn(body)
	tried := map[int]bool{}
	deaths, retries, freeResend := 0, 0, true
	target := home
	var last *http.Response
	lastKind := ""
	for {
		tried[target] = true
		resp, kind := l.send(req.Context(), target, req, body)
		l.record(Attempt{ID: id, Home: home, Target: target, Kind: kind, Status: statusOf(resp), At: time.Now()})
		key := l.rig.Front.Instances[target].Key
		// Each upstream outcome is counted toward the front's pause once, as the front's own rule does
		// (reportOutcome: a 5xx and a written-then-closed connection count, a completion resets). Home's
		// outcome is the front's to count, from what the layer returns it: a closed connection or a relayed
		// 5xx when the layer gives up, a success when it rescues the request. So the layer reports only
		// the other Instances' outcomes. (It once reported home's failure too, and the front counted it
		// again on a give-up: TestTheLayerAtBudget0PausesAsAlphaDoes.)
		report := func(failed bool) {
			if target != home {
				l.rig.Front.Router.ReportConnect(key, failed)
			}
		}
		switch kind {
		case "ok":
			report(false)
			relay(w, resp)
			return
		case "cancelled":
			return
		case "closed":
			deaths++
			report(true)
		case "5xx", "dial":
			report(true)
		}
		if last != nil {
			last.Body.Close()
		}
		last, lastKind = resp, kind
		next := l.next(home, tried)
		allowed := false
		switch {
		case next < 0:
		case kind == "dial" && freeResend:
			freeResend, allowed = false, true // alpha's own resend: nothing was written
		case retries >= l.Policy.MaxRetries:
		case l.Policy.StopAfterDeaths > 0 && deaths >= l.Policy.StopAfterDeaths:
		case l.Policy.Bucket != nil && !l.Policy.Bucket.Take():
		default:
			retries++
			allowed = true
		}
		if !allowed {
			break
		}
		target = next
	}
	// Give up with the last failure, as the front would have seen it from that upstream.
	switch lastKind {
	case "5xx":
		relay(w, last)
	default:
		// Written then closed, or no Instance reached: close with no answer, so the front raises its own
		// tunnel error, as under alpha.
		if hj, ok := w.(http.Hijacker); ok {
			if c, _, err := hj.Hijack(); err == nil {
				_ = c.Close()
			}
		}
	}
}

// next is the next ready Instance after home not yet tried, -1 if none.
func (l *Layer) next(home int, tried map[int]bool) int {
	n := len(l.rig.Front.Instances)
	for k := 1; k < n; k++ {
		j := (home + k) % n
		if !tried[j] && l.rig.Ready(j) {
			return j
		}
	}
	return -1
}

// send sends the request to Instance target, and classifies the outcome.
func (l *Layer) send(ctx context.Context, target int, in *http.Request, body []byte) (*http.Response, string) {
	out, err := http.NewRequestWithContext(ctx, in.Method, "http://instance"+in.URL.RequestURI(), bytes.NewReader(body))
	if err != nil {
		return nil, "dial"
	}
	out.Header = in.Header.Clone()
	out.Header.Set("Authorization", "Bearer "+l.rig.Front.Instances[target].APIKey)
	out.ContentLength = int64(len(body))
	resp, err := l.up[target].RoundTrip(out)
	switch {
	case err == nil && resp.StatusCode >= 500:
		return resp, "5xx"
	case err == nil:
		return resp, "ok"
	case ctx.Err() != nil:
		return nil, "cancelled"
	}
	var de *dialErr
	if errors.As(err, &de) {
		return nil, "dial"
	}
	return nil, "closed"
}

func statusOf(r *http.Response) int {
	if r == nil {
		return 0
	}
	return r.StatusCode
}

// relay copies resp to w, flushing as it reads, so a stream's events pass at once.
func relay(w http.ResponseWriter, resp *http.Response) {
	defer resp.Body.Close()
	for k, vs := range resp.Header {
		w.Header()[k] = vs
	}
	w.WriteHeader(resp.StatusCode)
	fl, _ := w.(http.Flusher)
	if fl != nil {
		fl.Flush()
	}
	b := make([]byte, 32<<10)
	for {
		n, err := resp.Body.Read(b)
		if n > 0 {
			if _, werr := w.Write(b[:n]); werr != nil {
				return
			}
			if fl != nil {
				fl.Flush()
			}
		}
		if err != nil {
			return
		}
	}
}

func (l *Layer) close() {
	for _, s := range l.servers {
		_ = s.Close()
	}
	for _, t := range l.up {
		t.CloseIdleConnections()
	}
}

var _ front.DialFunc = DialUnix("")
