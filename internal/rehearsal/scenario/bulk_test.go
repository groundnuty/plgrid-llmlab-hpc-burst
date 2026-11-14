package scenario_test

// HBEP-6's bulk path on the whole stack (HBEP-6, Test plan, The bulk path, :655-656; #448, part 7):
// a 64 MiB non-streamed reply beside 8 token streams of 20 tokens/s, through S17's link in its fair
// mode (48 MB/s, 2.3 ms round trip), in direct and host-dialed mode. The link sits where S17 put it
// (spikes/s17/s17.py): in direct mode in front of the dial-home listener the jobs dial, in host-dialed
// mode on the relays' ssh to the login node.
//
// Each mode runs twice, one stack after the other: first without the bulk path, the run's own
// control, then with it. The science agent's ruling S4 on #448, as revised after the third hosted run
// (comment 6828152670; judgeS4): the run is valid when its control stalls the streams to a gap p99 of
// at least 85 ms, and it passes when the bulk-on p99 is 75 ms or less and at least 30 ms below its own
// control. A control under 85 ms makes the run INVALID, never a pass; both figures and the link's
// achieved rate are always logged.
//
// What would make this prove nothing, and how it's ruled out:
//   - a reply that fits in frp's 6 MiB stream window: the reply is 64 MiB, and the link's byte
//     counters must grow by at least that much across it, so it took the link and wasn't cut;
//   - a bulk-on run whose reply took the token tunnel: the front's hpcburst_front_bulk_requests_total
//     {path="bulk"} must grow by exactly one across it, and the control run has no such series;
//   - gaps measured outside the transfer: only gaps that overlap the transfer's window count, and each
//     stream must still be running when the transfer ends.

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The bulk rehearsal's ports, in #448's block (28280-28359, IMPL-COMMON): up's 16 at 28340-28355
// (the bulk path's block reaches base+15), the client at 28356, and in direct mode the front's
// dial-home listener at 28357; S17's link takes a kernel port. Host-dialed mode's dial-home listener
// is its loopback default, base+7.
var (
	portsBulkDirect     = stack.Ports{Base: 28340, ClientAddr: "127.0.0.1:28356", DialHomeListen: "127.0.0.1:28357"}
	portsBulkHostDialed = stack.Ports{Base: 28340, ClientAddr: "127.0.0.1:28356"}
)

// The measurement, as S17 ran it (HBEP-6, The bulk path, S17's figures) and the rulings fix it.
const (
	bulkStreams     = 8
	bulkTokenDelay  = 50 * time.Millisecond // 20 tokens/s
	bulkStreamLen   = 600                   // tokens: 30 s, well past the transfer
	bulkReplyBytes  = 64 << 20
	s4ControlMinP99 = 85 * time.Millisecond // a control below this makes the run invalid (S4, revised)
	s4PassMaxP99    = 75 * time.Millisecond // a bulk-on p99 above this fails (S4)
	s4PassMinMargin = 30 * time.Millisecond // a bulk-on p99 less than this below its control fails (S4, revised)
)

// bulkRun is one stack's measurement.
type bulkRun struct {
	p99        time.Duration
	gaps       int
	took       time.Duration // the 64 MiB reply, request sent to last byte read
	replyBytes int64
	linkBytes  int64 // the link's counters' growth across the reply, both directions
	bulkSent   float64
}

// TestBulk64MiBReplyBesideEightStreams (HBEP-6:656): the bulk path keeps 8 token streams' inter-token
// gap within S17's figure while a 64 MiB non-streamed reply goes through, against the same run without
// the bulk path.
//
//livecheck:none a hosted throwaway runs it (IMPL-COMMON: rehearsals on hosted runners)
func TestBulk64MiBReplyBesideEightStreams(t *testing.T) {
	requireRehearsal(t) // skips the whole test without HPCBURST_REHEARSAL=1, before any judgement
	for _, c := range []struct {
		mode  string
		ports stack.Ports
	}{
		{"direct", portsBulkDirect},
		{string(agentapi.DialHomeHostDialed), portsBulkHostDialed},
	} {
		t.Run(c.mode, func(t *testing.T) {
			var control, on bulkRun
			t.Run("control without the bulk path", func(t *testing.T) { control = measureBulk(t, c.mode, c.ports, false) })
			if t.Failed() {
				t.FailNow()
			}
			t.Run("with the bulk path", func(t *testing.T) { on = measureBulk(t, c.mode, c.ports, true) })
			if t.Failed() {
				t.FailNow()
			}
			if control.gaps == 0 || on.gaps == 0 {
				t.Fatalf("a run measured nothing (%d and %d gaps): nothing to judge", control.gaps, on.gaps)
			}
			t.Logf("%s: gap p99 without the bulk path %s (%d gaps), with it %s (%d gaps); the reply took %s and %s (%.1f and %.1f MB/s through the link)",
				c.mode, control.p99, control.gaps, on.p99, on.gaps, control.took, on.took,
				float64(control.replyBytes)/control.took.Seconds()/1e6, float64(on.replyBytes)/on.took.Seconds()/1e6)
			switch v, why := judgeS4(control.p99, on.p99); v {
			case s4Invalid:
				t.Fatalf("INVALID run, neither a pass nor a fail: %s", why)
			case s4Fail:
				t.Errorf("S4 fails: %s", why)
			default:
				t.Logf("S4 passes: %s", why)
			}
		})
	}
}

// s4Verdict is S4's judgement of one run.
type s4Verdict int

const (
	s4Invalid s4Verdict = iota // the control shows no stall to remove: neither a pass nor a fail
	s4Fail
	s4Pass
)

// judgeS4 judges one run by S4 as revised (#448, comment 6828152670): valid when the control's gap
// p99 is at least s4ControlMinP99; then a pass when the bulk-on p99 is at most s4PassMaxP99 and at
// least s4PassMinMargin below the control, and a fail otherwise. It says why, with both figures.
func judgeS4(control, bulk time.Duration) (s4Verdict, string) {
	figures := fmt.Sprintf("gap p99 %s without the bulk path, %s with it, %s apart", control, bulk, control-bulk)
	switch {
	case control < s4ControlMinP99:
		return s4Invalid, fmt.Sprintf("%s: the control is under %s, so this run can't show a stall to remove", figures, s4ControlMinP99)
	case bulk > s4PassMaxP99:
		return s4Fail, fmt.Sprintf("%s: the bulk path's is above %s", figures, s4PassMaxP99)
	case control-bulk < s4PassMinMargin:
		return s4Fail, fmt.Sprintf("%s: less than %s apart", figures, s4PassMinMargin)
	}
	return s4Pass, figures
}

// measureBulk starts a stack in mode, through S17's link, with or without the bulk path, starts one
// Instance, runs the 8 streams and the 64 MiB reply beside them, and returns the gaps' p99 in the
// reply's window.
func measureBulk(t *testing.T, mode string, ports stack.Ports, bulkPath bool) bulkRun {
	r := startRigWith(t, rigSetup{mode: mode, ports: ports, link: true, bulkPath: bulkPath})
	r.engineEnv = []v1alpha1.EnvVar{{Name: "HB_ENGINE_DOUBLE", Value: "token-delay=" + bulkTokenDelay.String() + ",reply-bytes=" + strconv.Itoa(bulkReplyBytes)}}
	in := r.startServing(bulkPath)
	key := r.fc.KeyFor(in)
	// Without the bulk path (the control, HBEP-6:659's "bulk path off"), the real agent runs the
	// token frpc alone: the controller wrote no bulk block. With it, the bulk frpc beside it.
	r.waitFrpcs(in, 1, map[bool]int{true: 1, false: 0}[bulkPath])

	// KeyFor's key search sent short non-streamed requests through the front: with the bulk path they
	// took the bulk transport already, which is the rule at work on the whole stack.
	before := frontMetric(t, ports, `hpcburst_front_bulk_requests_total{path="bulk"}`)
	if bulkPath && before < 1 {
		t.Fatalf("the bulk counter is %v after the key search's non-streamed requests: they did not take the bulk transport", before)
	}
	if !bulkPath && before != -1 {
		t.Fatalf("without the bulk path the front exports the bulk counter (%v)", before)
	}

	type stream struct {
		first <-chan struct{}
		done  <-chan stack.Streamed
	}
	var ss []stream
	for i := 0; i < bulkStreams; i++ {
		f, d := r.fc.StartStream(stack.ChatBody(stack.Model, bulkStreamLen, bulkStreamLen, true, ""), key)
		ss = append(ss, stream{f, d})
	}
	for i, s := range ss {
		select {
		case <-s.first:
		case <-time.After(stack.WaitDeadline):
			t.Fatalf("stream %d sent no first token", i)
		}
	}
	time.Sleep(time.Second) // the streams in their steady state before the transfer: a wait for the pace, not for an event

	linkBefore := r.link.Up.Load() + r.link.Down.Load()
	start := time.Now()
	n, status, err := drainChat(t, r, key)
	end := time.Now()
	linkGrew := r.link.Up.Load() + r.link.Down.Load() - linkBefore
	if err != nil || status != http.StatusOK || n < bulkReplyBytes {
		t.Fatalf("the 64 MiB reply: status %d, %d bytes, %v", status, n, err)
	}
	if linkGrew < bulkReplyBytes {
		t.Fatalf("the link carried %d bytes across the reply, below its %d: the reply did not take the link", linkGrew, bulkReplyBytes)
	}
	after := frontMetric(t, ports, `hpcburst_front_bulk_requests_total{path="bulk"}`)
	switch {
	case bulkPath && after != before+1:
		t.Fatalf("the bulk counter went %v -> %v across the reply: it did not take the bulk transport", before, after)
	case bulkPath && frontMetric(t, ports, `hpcburst_front_bulk_requests_total{path="fallback"}`) != 0:
		t.Fatal("the reply fell back to the token transport")
	case !bulkPath && after != -1:
		t.Fatalf("without the bulk path the front exports the bulk counter (%v)", after)
	}

	var gaps []time.Duration
	for i, s := range ss {
		var got stack.Streamed
		select {
		case got = <-s.done:
		case <-time.After(stack.WaitDeadline):
			t.Fatalf("stream %d did not end", i)
		}
		if got.Err != nil || got.Status != http.StatusOK {
			t.Fatalf("stream %d: %d, %v", i, got.Status, got.Err)
		}
		if len(got.Ticks) == 0 || !got.Ticks[len(got.Ticks)-1].After(end) {
			t.Fatalf("stream %d ended before the reply did: its gaps don't cover the transfer", i)
		}
		gaps = append(gaps, gapsOverlapping(got.Ticks, start, end)...)
	}
	if len(gaps) < bulkStreams*int(end.Sub(start)/bulkTokenDelay)/2 {
		t.Fatalf("only %d gaps overlap the %s transfer", len(gaps), end.Sub(start))
	}
	run := bulkRun{p99: p99(gaps), gaps: len(gaps), took: end.Sub(start), replyBytes: n, linkBytes: linkGrew, bulkSent: after}
	t.Logf("%s, bulk path %v: gap p99 %s over %d gaps in the reply's window; the %d-byte reply took %s (%.1f MB/s through the link, which carried %d bytes)",
		mode, bulkPath, run.p99, run.gaps, run.replyBytes, run.took, float64(run.replyBytes)/run.took.Seconds()/1e6, run.linkBytes)
	return run
}

// gapsOverlapping returns the inter-token gaps whose interval overlaps [start, end]: a stream stalled
// by the transfer gets its next token after it ends, and that gap is the stall.
func gapsOverlapping(ticks []time.Time, start, end time.Time) []time.Duration {
	var out []time.Duration
	for i := 1; i < len(ticks); i++ {
		if ticks[i].After(start) && ticks[i-1].Before(end) {
			out = append(out, ticks[i].Sub(ticks[i-1]))
		}
	}
	return out
}

// p99 is the nearest-rank 99th percentile.
func p99(d []time.Duration) time.Duration {
	if len(d) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), d...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[int(math.Ceil(0.99*float64(len(s))))-1]
}

// drainChat sends one non-streamed chat request marked for the engine double's reply-bytes (its last
// message is enginedouble.ReplyBytesMarker, so the agent's own checks of the engine get small replies)
// and reads its whole body, counting the bytes: the stack's client keeps at most 8 MiB of a reply, and
// would cut a 64 MiB one.
func drainChat(t *testing.T, r *rig, key string) (int64, int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), stack.WaitDeadline)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+r.h.Ports.ClientAddr+"/v1/chat/completions",
		strings.NewReader(stack.ChatBody(stack.Model, 8, 0, false, enginedouble.ReplyBytesMarker)))
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
	n, err := io.Copy(io.Discard, resp.Body)
	return n, resp.StatusCode, err
}

// frontMetric reads one series' value from the front's /metrics on its health address (base+4); -1
// when the series is absent.
func frontMetric(t *testing.T, p stack.Ports, series string) float64 {
	t.Helper()
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Get(p.URL(4, "/metrics"))
	if err != nil {
		t.Fatalf("the front's /metrics: %v", err)
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		if v, ok := strings.CutPrefix(sc.Text(), series+" "); ok {
			var f float64
			if _, err := fmt.Sscan(v, &f); err != nil {
				t.Fatalf("the front's %s: %q", series, v)
			}
			return f
		}
	}
	return -1
}

// agentFrpcs counts the frpc processes the agent of the Instance's job runs: the token frpc
// (frpc.toml) and the bulk frpc (frpc-bulk.toml), told apart by the configuration on their command
// lines (internal/agent, Frpc.ConfigPath).
func (r *rig) agentFrpcs(in *v1alpha1.Instance) (token, bulk int) {
	r.t.Helper()
	agent := r.jobAgent(r.jobOf(in))
	if agent == 0 {
		return 0, 0
	}
	for _, c := range stack.ChildrenOf(agent) {
		if !isStagedFrpc(stack.ProcessExe(c)) {
			continue
		}
		b, err := os.ReadFile("/proc/" + strconv.Itoa(c) + "/cmdline")
		if err != nil {
			continue
		}
		switch args := strings.Split(strings.TrimRight(string(b), "\x00"), "\x00"); {
		case strings.HasSuffix(args[len(args)-1], "/frpc-bulk.toml"):
			bulk++
		case strings.HasSuffix(args[len(args)-1], "/frpc.toml"):
			token++
		}
	}
	return token, bulk
}

// waitFrpcs waits until the Instance's agent runs exactly token and bulk frpc, and they stay so for
// a few seconds: a bulk frpc that is missing at first must also never appear.
func (r *rig) waitFrpcs(in *v1alpha1.Instance, token, bulk int) {
	r.t.Helper()
	var since time.Time
	stack.WaitState(r.t, stack.WaitDeadline, fmt.Sprintf("the agent to run %d token and %d bulk frpc", token, bulk), func() (bool, string) {
		tk, bk := r.agentFrpcs(in)
		if tk != token || bk != bulk {
			since = time.Time{}
			return false, fmt.Sprintf("%d token, %d bulk", tk, bk)
		}
		if since.IsZero() {
			since = time.Now()
		}
		return time.Since(since) >= 3*time.Second, "settling"
	})
}

// jobOf is the job ID of the Allocation the Instance runs on.
func (r *rig) jobOf(in *v1alpha1.Instance) int {
	r.t.Helper()
	for _, a := range r.allocations() {
		if a.Name == in.Spec.AllocationRef.Name && a.Status.AtProvider.JobID != "" {
			id, err := strconv.Atoi(a.Status.AtProvider.JobID)
			if err != nil {
				r.t.Fatalf("job ID %q: %v", a.Status.AtProvider.JobID, err)
			}
			return id
		}
	}
	r.t.Fatalf("no job for the Instance %s", in.Name)
	return 0
}

// writeCredentials writes the Provider's ssh credentials for the login node: through S17's link in
// host-dialed mode with rigSetup.link, so the relays' ssh takes it (spikes/s17/s17.py); directly
// otherwise, as before.
func (r *rig) writeCredentials(s rigSetup, creds string) error {
	if !s.link || s.mode != string(agentapi.DialHomeHostDialed) {
		return r.cl.Login.WriteCredentials(creds)
	}
	var addr string
	addr, r.link = stack.StartLink(r.t, r.cl.Login.Addr())
	return r.cl.Login.WriteCredentialsVia(creds, addr)
}

// startServing starts the served deployment and waits for its Instance to be ready and routed: the
// front has registered its token transport and, with the bulk path, its bulk transport too
// (hpcburst_front_bulk_registered).
func (r *rig) startServing(bulkPath bool) *v1alpha1.Instance {
	r.t.Helper()
	r.providerReady()
	stack.Create(r.t, r.c, stack.ServedDeployment(rehearsalTiming, r.engineEnv...))
	var ready *v1alpha1.Instance
	stack.WaitState(r.t, stack.WaitDeadline, "a ready Instance whose transports the front registered", func() (bool, string) {
		for _, in := range r.instances() {
			if in.Status.Phase != v1alpha1.InstanceReady {
				continue
			}
			in := in
			if reg, _ := stack.Registered(r.ports, &in); !reg {
				return false, "the token transport is not registered"
			}
			if bulkPath && frontMetric(r.t, r.ports, "hpcburst_front_bulk_registered") < 1 {
				return false, "the bulk transport is not registered"
			}
			ready = &in
			return true, ""
		}
		return false, "no ready Instance:" + r.describe()
	})
	return ready
}
