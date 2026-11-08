package leasefault

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/leaderlock"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// TestRenewsFailReleaseHeld: in RenewsFailReleaseHeld a marked request, a
// renew's read or write, fails at once without reaching the API server; a
// read that is not marked, client-go's release read, is held past its own
// context's end until OpenHeld, then fails, unsent; once open, the next such
// read fails at once. A request for another path passes untouched.
func TestRenewsFailReleaseHeld(t *testing.T) {
	var sent atomic.Int32
	next := roundTripFunc(func(*http.Request) (*http.Response, error) {
		sent.Add(1)
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
	})
	f := New("ns", "lease")
	f.Set(RenewsFailReleaseHeld)
	defer f.OpenHeld()
	tr := &transport{f: f, next: next}
	req := func(ctx context.Context, method, path string) *http.Request {
		r := httptest.NewRequest(method, "http://api"+path, nil).WithContext(ctx)
		r.Header.Set("User-Agent", "test/"+leaderlock.UserAgent)
		return r
	}
	const lease = "/apis/coordination.k8s.io/v1/namespaces/ns/leases/lease"
	marked := Mark(context.Background())
	for _, m := range []string{http.MethodGet, http.MethodPut} {
		if _, err := tr.RoundTrip(req(marked, m, lease)); err == nil {
			t.Fatalf("a marked %s passed", m)
		}
	}
	if _, err := tr.RoundTrip(req(marked, http.MethodGet, "/api/v1/namespaces/ns/pods")); err != nil || sent.Load() != 1 {
		t.Fatalf("a request for another path: %v, %d sent; want it passed on", err, sent.Load())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := tr.RoundTrip(req(ctx, http.MethodGet, lease))
		done <- err
	}()
	<-ctx.Done()
	// A hold that followed the context would return about now; give it ample
	// time to. A correct hold cannot fail this check, however slow the machine.
	time.Sleep(100 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("the release read returned (%v) before OpenHeld: the hold followed its context", err)
	default:
	}
	f.OpenHeld()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("the held read passed after OpenHeld, want it failed")
		}
	case <-time.After(time.Minute):
		t.Fatal("the held read did not return within a minute of OpenHeld")
	}
	if _, err := tr.RoundTrip(req(context.Background(), http.MethodGet, lease)); err == nil {
		t.Fatal("an unmarked read after OpenHeld passed, want it failed at once")
	}
	if sent.Load() != 1 {
		t.Fatalf("%d requests reached the API server, want only the other path's", sent.Load())
	}
	reqs := f.Requests()
	if len(reqs) != 4 {
		t.Fatalf("%d requests recorded, want 4: %+v", len(reqs), reqs)
	}
	held := reqs[2]
	if held.Marked || !held.Failed || held.Sent || !held.Done.After(held.At.Add(100*time.Millisecond)) {
		t.Fatalf("the held read was recorded as %+v; want unmarked, failed, unsent, done after the hold", held)
	}
}
