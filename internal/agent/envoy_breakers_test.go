package agent_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// The Instance's circuit breakers, with the real Envoy 1.40.2: the agent sets the cluster's three
// thresholds to Envoy's field maximum, because the front's cap governs how many requests an Instance
// gets and an overflow 503 from Envoy would count toward the Instance's pause (the ruling on #429, Q8).
// Envoy's own default is 1,024. The control is the test's own, never the agent's.

// holdClient is a client of the router's listener for a test that holds more than a thousand requests
// open at once. It dials at most 100 connections at a time, so the burst does not overrun the
// listener's backlog, and has no timeout of its own.
func holdClient(t *testing.T, path string) *http.Client {
	t.Helper()
	dials := make(chan struct{}, 100)
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		select {
		case dials <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		defer func() { <-dials }()
		var d net.Dialer
		return d.DialContext(ctx, "unix", path)
	}}
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr}
}

// answer is what the client of one held request got.
type answer struct {
	seq    string
	status int
	body   string
	err    error
}

// holdRequests sends n requests to the router at once, each with its own X-Test-Seq, and returns what
// each got. onStatus is told every status as it arrives (it may be nil).
func holdRequests(t *testing.T, r *router, n int, onStatus func(status int)) []answer {
	t.Helper()
	raiseSoftNoFile(t)
	// This process holds two descriptors for each request (the client's and the worker's) and Envoy two
	// more; a limit below that would fail the test for the wrong reason, so it fails here and says so.
	var lim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
		t.Fatal(err)
	}
	if need := uint64(2*n + 500); lim.Cur < need {
		t.Fatalf("the descriptor limit is %d, below the %d that %d held requests need: raise the hard limit (ulimit -Hn)", lim.Cur, need, n)
	}
	c := holdClient(t, r.listener())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	answers := make([]answer, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			a := answer{seq: strconv.Itoa(i)}
			defer func() { answers[i] = a }()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://instance/v1/models", nil)
			if err != nil {
				a.err = err
				return
			}
			req.Header.Set("X-Test-Seq", a.seq)
			resp, err := c.Do(req)
			if err != nil {
				a.err = err
				return
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			a.status, a.body, a.err = resp.StatusCode, string(body), err
			if onStatus != nil {
				onStatus(a.status)
			}
		}(i)
	}
	wg.Wait()
	return answers
}

// seenByWorker is the X-Test-Seq of every request that reached the worker.
func seenByWorker(w *testWorker) map[string]bool {
	seen := map[string]bool{}
	for _, r := range w.Requests() {
		seen[r.Header.Get("X-Test-Seq")] = true
	}
	return seen
}

// heldRequests is more than Envoy's defaults can hold open at once: 1,024 connections to the worker and
// 1,024 requests waiting for one. It is also above the 2,048 an 8-worker Instance admits (HBEP-5, line
// 121), so a router that refused one would refuse a request the Instance's cap lets in. 1,100, which
// is above 1,024 alone, made the control's 503 depend on how fast Envoy's connections came up: in one
// run of twelve none of the 1,100 was refused.
const heldRequests = 2100

func TestEnvoyCircuitBreakersSitAboveEnvoysDefault(t *testing.T) {
	t.Run("the agent's config", func(t *testing.T) {
		gate := newHoldGate(heldRequests, 60*time.Second)
		r := agentRouter(t, workerSpec{Hold: gate})
		answers := holdRequests(t, r, heldRequests, nil)
		// The worker held each request until all of them had reached it: they were all in flight at once.
		if gate.How() != "count" || gate.Arrived() != heldRequests {
			t.Errorf("the worker's gate opened by %q with %d of %d requests arrived, want %q with all of them: Envoy did not pass them all at once",
				gate.How(), gate.Arrived(), heldRequests, "count")
		}
		bad := 0
		for _, a := range answers {
			if a.err != nil || a.status != http.StatusOK || a.body != workerBody {
				if bad++; bad <= 3 {
					t.Errorf("request %s: status %d, body %q, error %v; want the worker's 200", a.seq, a.status, a.body, a.err)
				}
			}
		}
		if bad > 0 {
			t.Errorf("%d of %d requests did not get 200", bad, heldRequests)
		}
		if seen := seenByWorker(r.worker); len(seen) != heldRequests {
			t.Errorf("%d distinct requests reached the worker, want %d", len(seen), heldRequests)
		}
	})
	t.Run("control: circuit_breakers deleted, so Envoy's own 1,024", func(t *testing.T) {
		gate := newHoldGate(heldRequests, 60*time.Second)
		r := controlRouter(t, workerSpec{Hold: gate}, func(m map[string]any) { delete(clusterOf(t, m), "circuit_breakers") })
		// Once Envoy has refused a request, let the held ones go instead of waiting out the gate's deadline.
		var once sync.Once
		answers := holdRequests(t, r, heldRequests, func(status int) {
			if status == http.StatusServiceUnavailable {
				once.Do(gate.Release)
			}
		})
		seen := seenByWorker(r.worker)
		overflowed, ok := 0, 0
		for _, a := range answers {
			switch {
			case a.err == nil && a.status == http.StatusServiceUnavailable:
				overflowed++
				if seen[a.seq] {
					t.Errorf("request %s got a 503 but reached the worker", a.seq)
				}
				// Envoy's circuit breaker says so in its own reply; a refused connection would not.
				if !strings.Contains(a.body, "overflow") {
					t.Errorf("request %s got a 503 that is not Envoy's overflow: %q", a.seq, a.body)
				}
				if overflowed == 1 {
					t.Logf("the first 503: %q", a.body)
				}
			case a.err == nil && a.status == http.StatusOK:
				ok++
			default:
				t.Errorf("request %s: status %d, error %v; want a 200 or Envoy's 503", a.seq, a.status, a.err)
			}
		}
		if overflowed == 0 {
			t.Errorf("none of the %d requests got a 503 (gate opened by %q, %d reached the worker): Envoy's default limit did not bind", heldRequests, gate.How(), len(seen))
		}
		t.Logf("with Envoy's own limits: %d x 200, %d x 503, %d reached the worker, the gate opened by %q", ok, overflowed, len(seen), gate.How())
	})
}
