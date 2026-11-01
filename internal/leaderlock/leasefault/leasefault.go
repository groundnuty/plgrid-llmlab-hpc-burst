// Package leasefault fails chosen requests for one leader Lease, at the
// transport of a rest.Config, for the tests of the managers' stop (issue
// #160). It reaches the request the tests need, the elector's release read,
// without a hook in the code under test.
//
// client-go's elector (k8s.io/client-go v0.35.3) reads and writes the Lease
// for its acquire and its renews with a context derived from the one the
// manager gave it, and controller-runtime v0.23.1 derives that from the
// context of the manager's Start (pkg/manager/internal.go:464-465,
// context.WithoutCancel), so they carry its values. Its release alone reads
// with a new context (tools/leaderelection/leaderelection.go:311,
// context.Background()). A test that starts the manager with a context from
// Mark therefore tells the release read apart: it is the one read of the
// Lease that is not marked. Those line numbers pin the versions: after an
// upgrade, a failed match shows as a test whose fault never fired, which the
// tests check, never as a test that passes without it.
package leasefault

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"k8s.io/client-go/rest"
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
	// ReleaseRead fails every read of the Lease whose context is not marked:
	// with the manager started on a marked context, client-go's release read
	// and nothing else.
	ReleaseRead
	// All fails every request for the Lease, marked or not: the elector's
	// renews fail until it gives up its leadership, and its release read fails
	// too, so the Lease keeps naming the holder.
	All
)

// Request is one request for the Lease that reached the transport.
type Request struct {
	Method string
	Marked bool
	Failed bool
}

// Faults fails requests for one Lease in its Mode, and records every request
// for that Lease.
type Faults struct {
	path string

	mu       sync.Mutex
	mode     Mode
	requests []Request
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

// fail records the request and reports whether it fails.
func (f *Faults) fail(req *http.Request) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := Request{Method: req.Method, Marked: Marked(req.Context())}
	switch f.mode {
	case ReleaseRead:
		r.Failed = r.Method == http.MethodGet && !r.Marked
	case All:
		r.Failed = true
	}
	f.requests = append(f.requests, r)
	return r.Failed
}

type transport struct {
	f    *Faults
	next http.RoundTripper
}

// RoundTrip implements http.RoundTripper. A failed request returns an error
// that client-go does not retry: it retries a GET only on a reset connection,
// a probable EOF or a lost HTTP/2 connection (rest/request.go:1064-1076).
func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Path == t.f.path && t.f.fail(req) {
		return nil, fmt.Errorf("leasefault: injected failure of %s %s", req.Method, req.URL.Path)
	}
	return t.next.RoundTrip(req)
}
