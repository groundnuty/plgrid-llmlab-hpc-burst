package alpha

// The bulk row's live flow (issue #521; betabulk.go): row A's world in host-dialed mode with the bulk path, on the engine double. Nothing here
// judges: Run does (VerifyBetaBulk, BulkInvalid). The Instance ready; the front's registered bulk transports read; then two phases of the one measurement, the
// control (the transfer down the token transport, as a streamed reply) and the bulk path (the same bytes as a non-streamed reply), each with eight
// token streams open beside it; then the deployment goes and its job is seen ended.

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/obscheck"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// ErrNoShim is what the bulk row's live steps return when the world has no test shim: the engine double reaches a cluster job only as the shim in
// hb-agent's place (issue #521, option (A)), since nothing in the product sets a job's environment, and a run on a real engine would read nothing of
// the bulk path's buffer, so it does not start.
var ErrNoShim = errors.New("the bulk row's live run goes through the test shim, which puts the engine double behind the shipped agent, and this world has none")

// BetaBulk is the bulk row.
func (r *liveRun) BetaBulk(ctx context.Context, p Plan) (BulkObs, error) {
	if r.w.shim == nil {
		return BulkObs{}, ErrNoShim
	}
	r.start(p)
	in, err := r.firstReady(p)
	if err != nil {
		return BulkObs{}, err
	}
	var o BulkObs
	if in.Status.ReadyTime != nil {
		o.ReadyAt = in.Status.ReadyTime.Time
	}
	ds, ok := DrainStartOf(&in, r.dw)
	if !ok {
		r.t.Fatal("the Instance has no lease end")
	}
	o.DrainStart = ds
	o.Deployment = r.observedDeployment()
	o.Double = r.w.double
	key := r.fc.KeyFor(&in)
	o.Registered = int(r.frontSeries("hpcburst_front_bulk_registered", nil))
	p.Logf("%s: the front has %d Instance(s) with a registered bulk transport", p.Row.Name, o.Registered)
	// The control first, then the bulk path: the streams of each phase are their own, and each ends before the next begins.
	o.Control = r.bulkPhase(p, "the control (a streamed reply, down the token transport)", key, true)
	o.Bulk = r.bulkPhase(p, "the bulk path (a non-streamed reply)", key, false)
	// What ran, while the job still runs: the Allocation's agent build, and on the node the agent, the engine and its Apptainer.
	r.shimReadings(ctx, p, in, &o.Shim)
	r.endDeployment(ctx, p)
	tags, err := r.runTags()
	if err != nil {
		return BulkObs{}, err
	}
	o.Tags = tags
	o.Responses, o.Attributed = r.attribution()
	return o, nil
}

// frontSeries is one series of the front's /metrics (a counter by its path label when want is set), -1 when the front has none.
func (r *liveRun) frontSeries(name string, want map[string]string) float64 {
	ss, err := obscheck.Scrape(r.h.Ports.URL(4, "/metrics"))
	if err != nil {
		r.t.Fatalf("the front's /metrics: %v", err)
	}
	found := obscheck.Find(ss, name, want)
	if len(found) == 0 {
		return -1
	}
	return found[0].Value
}

// bulkPhase opens BulkStreams streams, waits for each to send a token and for a second of steady pace, sends the transfer (streamed for the control,
// not for the bulk path), and reads what the streams and the front's bulk counter say of it.
func (r *liveRun) bulkPhase(p Plan, what, key string, streamed bool) BulkPhase {
	r.t.Helper()
	var ss []stream
	for i := 0; i < BulkStreams; i++ {
		ss = append(ss, r.openStream(BulkStreamTokens, key, ""))
	}
	for i, s := range ss {
		select {
		case <-s.first:
		case <-time.After(stack.WaitDeadline):
			r.t.Fatalf("%s: stream %d sent no first token", what, i)
		}
	}
	time.Sleep(time.Second) // the streams in their steady state before the transfer: a wait for the pace, not for an event
	bulk0, fb0 := r.frontSeries("hpcburst_front_bulk_requests_total", map[string]string{"path": "bulk"}), r.frontSeries("hpcburst_front_bulk_requests_total", map[string]string{"path": "fallback"})
	start := time.Now()
	n, status, err := r.drainTransfer(key, streamed)
	end := time.Now()
	bulk1, fb1 := r.frontSeries("hpcburst_front_bulk_requests_total", map[string]string{"path": "bulk"}), r.frontSeries("hpcburst_front_bulk_requests_total", map[string]string{"path": "fallback"})
	// The bytes are counted as they arrive through the front's client listener, timed from the send to the last byte: with the streams' cover below,
	// the control's are the run's saturation witness (BulkInvalid). A read that ended with an error is a cut, whatever it delivered.
	ph := BulkPhase{ReplyBytes: n, Status: status, Took: end.Sub(start), Cut: err != nil, BulkSent: counterDelta(bulk0, bulk1), FallbackSent: counterDelta(fb0, fb1)}
	if err != nil {
		p.Logf("%s: the transfer's read ended with %v after %d bytes", p.Row.Name, err, n)
	}
	var gaps []time.Duration
	for _, s := range ss {
		out := r.wait(s)
		done, long := StreamBeside(r.observe(out), out.Ticks, end)
		if done {
			ph.StreamsDone++
		}
		if long {
			ph.StreamsLong++
		}
		gaps = append(gaps, GapsOverlapping(out.Ticks, start, end)...)
	}
	ph.Gaps, ph.P50, ph.P99 = len(gaps), Percentile(gaps, 0.5), Percentile(gaps, 0.99)
	p.Logf("%s: %s: %d bytes in %s with a status of %d; the %d streams' gap p99 %s (p50 %s) over %d gaps; the front's bulk counter rose by %d and its fallback by %d",
		p.Row.Name, what, ph.ReplyBytes, ph.Took.Round(time.Millisecond), ph.Status, BulkStreams, ph.P99, ph.P50, ph.Gaps, ph.BulkSent, ph.FallbackSent)
	return ph
}

// counterDelta is how much a counter rose, -1 when either reading is absent.
func counterDelta(before, after float64) int {
	if before < 0 || after < 0 {
		return -1
	}
	return int(after - before)
}

// drainTransfer sends the transfer's request and reads its whole reply, counting the bytes: the stack's client keeps at most 8 MiB of a reply, and
// would cut a reply of 64 MiB. The request's last message is the double's marker for the padding, streamed or not, so that the agent's own checks
// of the engine get small replies. The reply's headers are kept, for the attribution count.
func (r *liveRun) drainTransfer(key string, streamed bool) (int64, int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), stack.WaitDeadline)
	defer cancel()
	marker := enginedouble.ReplyBytesMarker
	if streamed {
		marker = enginedouble.StreamReplyBytesMarker
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+r.h.Ports.ClientAddr+"/v1/chat/completions",
		strings.NewReader(stack.ChatBody(r.fc.Model, 8, 0, streamed, marker)))
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+r.h.ClientKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Session-Id", key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	r.keep(resp.Header)
	n, err := io.Copy(io.Discard, resp.Body)
	return n, resp.StatusCode, err
}
