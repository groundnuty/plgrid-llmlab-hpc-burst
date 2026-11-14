package run

// Each fault under arm (a), the front as it is, with its positive control (#528, Valid: a fault fires
// only where it was injected). The expectations are HBEP-5's (open question 5, and the Errors table),
// not the code's:
//   - written, then closed before any response header (F1, F3, F4): 502 upstream_closed, detail tunnel,
//     and never sent again: the request is begun exactly once;
//   - an engine's 500 before any byte (F2): passed through unchanged, begun once.
// Each fault is proven by the engines' records and the client's, never the injector's: the double's
// admitted record, then its process gone (F1, F4); then refused (F2); then cancelled (F3).

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/spikes/s18/rig"
)

// pinned returns a client and a session the front routes to Instance i.
func pinned(t *testing.T, ctx context.Context, r *rig.Rig, run string, i int) (*rig.Client, string, []rig.Result) {
	t.Helper()
	c := rig.NewClient(r.Front.URL, r.Front.ClientKey, run)
	s, probes, err := r.SessionOn(ctx, c, i)
	if err != nil {
		t.Fatal(err)
	}
	return c, s, probes
}

// deaths is the engines that died on their own.
func deaths(r *rig.Rig) []rig.Exit {
	var out []rig.Exit
	for _, e := range r.Doubles.Exits() {
		if !e.Stopped {
			out = append(out, e)
		}
	}
	return out
}

func wantTunnelError(t *testing.T, res rig.Result) {
	t.Helper()
	if res.Status != 502 || res.Code != "upstream_closed" || res.Detail != "tunnel" {
		t.Errorf("%s: %d %s/%s %q; want HBEP-5's 502 upstream_closed, detail tunnel", res.ID, res.Status, res.Code, res.Detail, res.Err)
	}
}

func TestF1UnderAlpha(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%v", stream), func(t *testing.T) {
			r := stand(t, "F1", nil)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			c, s, probes := pinned(t, ctx, r, "f1a", 1)
			// Armed, nothing marked; and Instance 0's marker on Instance 1: no fault fires.
			ok1 := c.Do(ctx, c.NextBase(), 0, s, stream, "")
			ok2 := c.Do(ctx, c.NextBase(), 0, s, stream, rig.MarkF1+"0")
			if len(deaths(r)) != 0 || ok1.Status != 200 || ok2.Status != 200 {
				t.Fatalf("an engine died, or a request failed, with no fault aimed at it: %+v; %d %d", deaths(r), ok1.Status, ok2.Status)
			}
			bad := c.Do(ctx, c.NextBase(), 0, s, stream, rig.MarkF1+"1")
			wantTunnelError(t, bad)
			begins, err := r.Begins()
			if err != nil {
				t.Fatal(err)
			}
			if bs := begins[bad.ID]; rig.Admissions(bs) != 1 || bs[0].Instance != 1 || bs[0].Outcome != "" {
				t.Errorf("the marked request's engine records: %+v; want it admitted once, on Instance 1, and never ended", bs)
			}
			if d := deaths(r); len(d) != 1 || d[0].Instance != 1 || d[0].Signal != "killed" {
				t.Errorf("deaths %+v, want Instance 1's engine killed under it, alone", d)
			}
			for _, p := range rig.Reconcile(append(probes, ok1, ok2, bad), begins) {
				t.Error(p)
			}
		})
	}
}

func TestF2UnderAlpha(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%v", stream), func(t *testing.T) {
			r := stand(t, "F2", nil)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			c, s, probes := pinned(t, ctx, r, "f2a", 1)
			ok := c.Do(ctx, c.NextBase(), 0, s, stream, rig.MarkF2+"0")
			bad := c.Do(ctx, c.NextBase(), 0, s, stream, rig.MarkF2+"1")
			if ok.Status != 200 {
				t.Errorf("Instance 0's marker on Instance 1: %d, want 200", ok.Status)
			}
			if bad.Status != 500 || !strings.HasPrefix(bad.Code, "InternalServerError") {
				t.Errorf("%s: %d %s, want the engine's 500 passed through", bad.ID, bad.Status, bad.Code)
			}
			begins, err := r.Begins()
			if err != nil {
				t.Fatal(err)
			}
			if bs := begins[bad.ID]; rig.Admissions(bs) != 1 || bs[0].Outcome != "refused" {
				t.Errorf("the marked request's engine records: %+v; want it admitted once, then refused", bs)
			}
			if d := deaths(r); len(d) != 0 {
				t.Errorf("deaths %+v, want none", d)
			}
			for _, p := range rig.Reconcile(append(probes, ok, bad), begins) {
				t.Error(p)
			}
		})
	}
}

func TestF3UnderAlpha(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%v", stream), func(t *testing.T) {
			r := stand(t, "F3", nil)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			c := rig.NewClient(r.Front.URL, r.Front.ClientKey, "f3a")
			ok := c.Do(ctx, c.NextBase(), 0, "", stream, "")
			bad := c.Do(ctx, c.NextBase(), 0, "", stream, rig.MarkF3)
			if ok.Status != 200 {
				t.Errorf("an unmarked request: %d, want 200", ok.Status)
			}
			wantTunnelError(t, bad)
			begins, err := r.Begins()
			if err != nil {
				t.Fatal(err)
			}
			eventually(t, 10*time.Second, "the cut request's end in its engine's log", func() bool {
				begins, _ = r.Begins()
				bs := begins[bad.ID]
				return len(bs) == 1 && bs[0].Outcome != ""
			})
			if bs := begins[bad.ID]; rig.Admissions(bs) != 1 || bs[0].Outcome != "cancelled" {
				t.Errorf("the cut request's engine records: %+v; want it admitted once, then cancelled", bs)
			}
			if d := deaths(r); len(d) != 0 {
				t.Errorf("deaths %+v, want none: F3 kills no engine", d)
			}
			for _, p := range rig.Reconcile([]rig.Result{ok, bad}, begins) {
				t.Error(p)
			}
		})
	}
}

func TestF4UnderAlpha(t *testing.T) {
	r := stand(t, "F4", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c, s, probes := pinned(t, ctx, r, "f4a", 1)
	bad := c.Do(ctx, c.NextBase(), 0, s, false, rig.MarkPoison)
	wantTunnelError(t, bad)
	begins, err := r.Begins()
	if err != nil {
		t.Fatal(err)
	}
	if bs := begins[bad.ID]; rig.Admissions(bs) != 1 || bs[0].Instance != 1 {
		t.Errorf("the poison request's engine records: %+v; want it begun once, on Instance 1", bs)
	}
	if d := deaths(r); len(d) != 1 || d[0].Instance != 1 {
		t.Errorf("deaths %+v, want Instance 1's alone: alpha never resends a written request", d)
	}
	for _, p := range rig.Reconcile(append(probes, bad), begins) {
		t.Error(p)
	}
}

func eventually(t *testing.T, within time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", within, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
