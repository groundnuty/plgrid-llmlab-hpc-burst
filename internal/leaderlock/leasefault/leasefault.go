// Package leasefault fails chosen requests of a leader Lease's lock, at the
// transport of a rest.Config, for the tests of the managers' stop (issue
// #160). It reaches the request the tests need, the elector's release read,
// without a hook in the code under test.
//
// It sees only the lock's own requests: those for the Lease that carry the
// user agent leaderlock.New adds (leaderlock.UserAgent, through
// rest.AddUserAgent, whose round tripper wraps the config's WrapTransport:
// client-go v0.35.3, transport/round_trippers.go:42-62). Other readers of the
// Lease, such as the lifecycle manager's ObservationGap, pass untouched and
// unrecorded.
//
// client-go's elector (k8s.io/client-go v0.35.3) reads and writes the Lease
// for its acquire and its renews with a context derived from the one the
// manager gave it, and controller-runtime v0.23.1 derives that from the
// context of the manager's Start (pkg/manager/internal.go:464-465,
// context.WithoutCancel), so they carry its values. Its release alone reads
// with a new context (tools/leaderelection/leaderelection.go:311,
// context.Background()). A test that starts the manager with a context from
// Mark therefore tells the release read apart: of the lock's reads, it is the
// one that is not marked. Those line numbers pin the versions: after an
// upgrade, a failed match shows as a test whose fault never fired, which the
// tests check, never as a test that passes without it.
package leasefault

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"k8s.io/client-go/rest"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/leaderlock"
)

type startMark struct{}

// Mark returns ctx marked as the context of a manager's Start.
func Mark(ctx context.Context) context.Context {
	return context.WithValue(ctx, startMark{}, true)
}

// Marked reports whether ctx carries Mark's value.
func Marked(ctx context.Context) bool {
	return ctx.Value(startMark{}) != nil
}

// Mode is which requests for the Lease fail.
type Mode int

const (
	// None fails nothing.
	None Mode = iota
	// ReleaseRead fails every read of the lock whose context is not marked:
	// with the manager started on a marked context, client-go's release read
	// and nothing else.
	ReleaseRead
	// All fails every request of the lock, marked or not: the elector's
	// renews fail until it gives up its leadership, and its release read fails
	// too, so the Lease keeps naming the holder.
	All
)

// Request is one request of the lock for the Lease that reached the
// transport.
type Request struct {
	Method string
	Marked bool
	// Failed: the request was failed by the Mode, without reaching the API
	// server.
	Failed bool
	// Conflict: the request was answered with an injected 409 Conflict,
	// without reaching the API server (ConflictNextReleaseWrite).
	Conflict bool
	// Sent: the request was passed on to the API server; Status is its
	// answer's status code, 0 when it returned no answer.
	Sent   bool
	Status int
	// At is when the request reached the transport, Done when the transport
	// returned it, after any delay (SetDelay). A request whose context ended
	// during the delay is returned then, not sent.
	At, Done time.Time
}

// Faults fails requests of the lock for one Lease in its Mode, and records
// every request of the lock for that Lease.
type Faults struct {
	path string

	mu        sync.Mutex
	mode      Mode
	delay     time.Duration
	conflicts int
	requests  []Request
}

// New returns Faults for the Lease name in namespace, failing nothing yet.
func New(namespace, name string) *Faults {
	return &Faults{path: "/apis/coordination.k8s.io/v1/namespaces/" + namespace + "/leases/" + name}
}

// Set changes the mode; the requests already recorded are kept.
func (f *Faults) Set(m Mode) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mode = m
}

// SetDelay holds every later request of the lock for d before the
// transport passes it on, fails it or answers it: the API server's latency as
// the lock sees it. A request whose context ends during the hold returns its
// context's error and is never sent, so a write cut by its deadline is never
// committed.
func (f *Faults) SetDelay(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.delay = d
}

// ConflictNextReleaseWrite answers the next write of the lock whose context
// is not marked, the elector's release write (it writes with the context of
// client-go's release, leaderelection.go:311), with a 409 Conflict, as the
// API server answers a write whose resourceVersion a later write has passed.
// The write is not sent, so the Lease keeps naming its holder.
func (f *Faults) ConflictNextReleaseWrite() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.conflicts++
}

// Config returns a copy of cfg whose transport goes through f, after any
// wrapper cfg already has.
func (f *Faults) Config(cfg *rest.Config) *rest.Config {
	out := rest.CopyConfig(cfg)
	inner := out.WrapTransport
	out.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
		if inner != nil {
			rt = inner(rt)
		}
		return &transport{f: f, next: rt}
	}
	return out
}

// Requests returns the requests recorded so far, in order.
func (f *Faults) Requests() []Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Request(nil), f.requests...)
}

// Failed returns how many requests failed, and how many of them were reads
// that were not marked.
func (f *Faults) Failed() (all, unmarkedReads int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.requests {
		if r.Failed {
			all++
			if r.Method == http.MethodGet && !r.Marked {
				unmarkedReads++
			}
		}
	}
	return all, unmarkedReads
}

// MarkedAfterReleaseRead returns how many marked requests came after the
// first read that was not marked, the elector's release read: the requests
// of a release made after Start has returned, which carries Start's values
// (leaderlock.ReleaseAfterStart). It is -1 when no read was unmarked.
func (f *Faults) MarkedAfterReleaseRead() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := -1
	for _, r := range f.requests {
		switch {
		case n < 0 && r.Method == http.MethodGet && !r.Marked:
			n = 0
		case n >= 0 && r.Marked:
			n++
		}
	}
	return n
}

// arrive records the request and returns its index, its hold and what to do
// with it.
func (f *Faults) arrive(req *http.Request) (i int, delay time.Duration, fail, conflict bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := Request{Method: req.Method, Marked: Marked(req.Context()), At: time.Now()}
	switch f.mode {
	case ReleaseRead:
		r.Failed = r.Method == http.MethodGet && !r.Marked
	case All:
		r.Failed = true
	}
	if !r.Failed && f.conflicts > 0 && r.Method == http.MethodPut && !r.Marked {
		f.conflicts--
		r.Conflict = true
	}
	f.requests = append(f.requests, r)
	return len(f.requests) - 1, f.delay, r.Failed, r.Conflict
}

// done records how the request at i ended.
func (f *Faults) done(i int, sent bool, status int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests[i].Sent, f.requests[i].Status, f.requests[i].Done = sent, status, time.Now()
}

type transport struct {
	f    *Faults
	next http.RoundTripper
}

// conflictStatus is the body of the API server's answer to a write whose
// resourceVersion is stale, which client-go decodes into a Conflict error.
const conflictStatus = `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"leasefault: injected conflict","reason":"Conflict","code":409}`

// RoundTrip implements http.RoundTripper. A failed request returns an error
// that client-go does not retry: it retries a GET only on a reset connection,
// a probable EOF or a lost HTTP/2 connection (rest/request.go:1064-1076).
func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Path != t.f.path || !strings.HasSuffix(req.Header.Get("User-Agent"), "/"+leaderlock.UserAgent) {
		return t.next.RoundTrip(req)
	}
	i, delay, fail, conflict := t.f.arrive(req)
	// A request that is not passed on still has its body closed, as a
	// RoundTripper must.
	unsent := func(status int) {
		if req.Body != nil {
			_ = req.Body.Close()
		}
		t.f.done(i, false, status)
	}
	if delay > 0 {
		hold := time.NewTimer(delay)
		select {
		case <-hold.C:
		case <-req.Context().Done():
			hold.Stop()
			unsent(0)
			return nil, req.Context().Err()
		}
	}
	switch {
	case fail:
		unsent(0)
		return nil, fmt.Errorf("leasefault: injected failure of %s %s", req.Method, req.URL.Path)
	case conflict:
		unsent(http.StatusConflict)
		return &http.Response{
			Status: "409 Conflict", StatusCode: http.StatusConflict, Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
			Header:  http.Header{"Content-Type": []string{"application/json"}},
			Body:    io.NopCloser(strings.NewReader(conflictStatus)),
			Request: req,
		}, nil
	}
	resp, err := t.next.RoundTrip(req)
	status := 0
	if resp != nil {
		status = resp.StatusCode
	}
	t.f.done(i, true, status)
	return resp, err
}
