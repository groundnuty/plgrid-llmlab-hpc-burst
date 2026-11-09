package alpha

import (
	"fmt"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The text to post on the issues a row names (docs/alpha-runbook.md). It holds
// counts, durations and fixed labels only: no job ID, host, account or path. The
// slot use (UTC start and end) is printed by hack/live-checks.sh and goes in the
// same comment.

func verdictLine(row Row, failures []string) string {
	if len(failures) == 0 {
		return fmt.Sprintf("%s: PASS (posts to %s)", row.Name, row.PostsTo)
	}
	return fmt.Sprintf("%s: FAIL (posts to %s)\n- failed checks: %s", row.Name, row.PostsTo, Summary(failures))
}

// watchResetsLine is the count of the run's watch resets, on every row that runs the stack.
func watchResetsLine(n int) string { return fmt.Sprintf("- watch resets over the run: %d\n", n) }

func deploymentLine(d DeploymentSpec) string {
	var on []string
	for _, p := range d.Providers {
		on = append(on, p.Cluster)
	}
	return fmt.Sprintf("- deployment: Providers on %s; leaseLimit %s; maxRequestDuration %s", strings.Join(on, ", "), d.LeaseLimit, d.MaxRequestDuration)
}

func inFlight(streams []Stream, at time.Time) (n, cut int) {
	for _, s := range streams {
		if s.InFlightAt(at) {
			n++
		}
		if s.CutBy != "" {
			cut++
		}
	}
	return n, cut
}

func handoverPost(row Row, o HandoverObs, failures []string) string {
	n, cut := inFlight(o.Streams, o.DrainStart)
	return fmt.Sprintf("%s\n%s\n- handovers completed: %d; streams in flight at the drain start: %d of %d; cut: %d\n- restarts at the worker rung: %d; responses with attribution: %d of %d\n",
		verdictLine(row, failures), deploymentLine(o.Deployment), o.Handovers, n, len(o.Streams), cut, o.WorkerRestarts, o.Attributed, o.Responses) +
		restartLine(o.WorkerRestart)
}

// restartLine is the worker-rung fault's times, when the run made one.
func restartLine(r RestartTimes) string {
	if r.Killed.IsZero() {
		return ""
	}
	return "- " + r.Line() + "\n"
}

func noOverlapPost(row Row, o NoOverlapObs, failures []string, jobs []provider.Job) string {
	var b strings.Builder
	n, cut := inFlight(o.Streams, o.DrainStart)
	fmt.Fprintf(&b, "%s\n%s\n- gap from the drain start to the successor ready: %s\n- streams in flight at the drain start: %d of %d; cut: %d; NoOverlapSlot seen: %t; new requests answered 503 no_ready_target: %d; restarts at the worker rung: %d\n",
		verdictLine(row, failures), deploymentLine(o.Deployment), o.Gap().Round(time.Second), n, len(o.Streams), cut, o.NoOverlapSlotSeen, o.NoReadyTarget, o.WorkerRestarts)
	b.WriteString(restartLine(o.WorkerRestart))
	if len(o.Tags) == 2 {
		by := jobsByTag(jobs)
		if w, ok := SuccessorQueueWait(by[o.Tags[0]], by[o.Tags[1]]); ok {
			fmt.Fprintf(&b, "- the successor's queue wait once the GPU freed: %s\n", w.Round(time.Second))
		}
	}
	return b.String()
}

func tunnelHourPost(row Row, o TunnelHourObs, failures []string) string {
	var b strings.Builder
	first, last := span(o.Requests)
	cut := countCut(o.Requests)
	tm := row.Clock()
	sessions := map[time.Time]bool{}
	connected := 0
	for _, s := range o.Samples {
		sessions[s.LastStart] = true
		if s.Connected {
			connected++
		}
	}
	fmt.Fprintf(&b, "%s\n%s\n- the hour: %d requests over %s; longest run back to back: %d; peak streams at %.0f tokens/s or more: %d; longest idle stretch: %s; requests cut: %d\n- the tunnel session: %d distinct frps lastStartTime across %d looks (one is the pass); TunnelConnected True in %d of them\n%s",
		verdictLine(row, failures), deploymentLine(o.Deployment), len(o.Requests), last.Sub(first).Round(time.Second), longestChain(o.Requests, tm.MaxChainGap),
		tm.StreamRate, concurrentStreams(o.Requests, tm.StreamRate), longestIdle(o.Requests).Round(time.Second), cut,
		len(sessions), len(o.Samples), connected, largeLine(o, tm)+rateLines(o)+headOfLineLine(o))
	for _, fl := range TunnelHourFlags(o) {
		fmt.Fprintf(&b, "- FLAG: %s\n", fl)
	}
	return b.String()
}

// largeLine: how many requests had a body of 0.1 MB or more, the range of the bodies, and how many of them were
// sent while the 16 streams were open (issue #224).
func largeLine(o TunnelHourObs, tm Timing) string {
	reqs := o.Requests
	large := LargeRequests(reqs)
	if len(large) == 0 {
		return "- large requests (a body of 0.1 MB or more): none; largest body sent: " + fmt.Sprint(largestBody(reqs)) + " bytes\n"
	}
	lo, hi := large[0].BodyBytes, large[0].BodyBytes
	for _, r := range large {
		lo, hi = min(lo, r.BodyBytes), max(hi, r.BodyBytes)
	}
	if o.UploadNotMeasured != "" {
		arrived := 0
		for _, r := range large {
			if r.ArrivalVerified() {
				arrived++
			}
		}
		return fmt.Sprintf("- large requests (a body of 0.1 MB or more): %d, bodies of %d to %d bytes; %d of them in flight while the %d streams were open (by the client's window); %d shown arrived whole by the engine's count of their prompt (usage.prompt_tokens)\n",
			len(large), lo, hi, largeInFlightAmongStreams(reqs, tm), tm.ConcurrentStreams, arrived)
	}
	return fmt.Sprintf("- large requests (a body of 0.1 MB or more): %d, bodies of %d to %d bytes; %d of them sent while the %d streams were open\n", len(large), lo, hi, largeAmongStreams(reqs, tm), tm.ConcurrentStreams)
}

// headOfLineLine is the head-of-line measure, for a run that took one.
func headOfLineLine(o TunnelHourObs) string {
	h := o.HeadOfLine
	if h == nil {
		return ""
	}
	return fmt.Sprintf("- head-of-line: longest gap between two tokens of a stream while a large body was in flight: %s (over %d gaps; %s in a send window, %s in a prefill window); elsewhere in the hour: %s\n",
		h.InFlight.Round(time.Millisecond), h.Gaps, h.Send.Round(time.Millisecond), h.Prefill.Round(time.Millisecond), h.Elsewhere.Round(time.Millisecond))
}

// rateLines: the path rate of each direction, separately, upload first (issue #224). The download is the token
// direction, with S1's tokens a second; a direction that was not measured says so.
func rateLines(o TunnelHourObs) string {
	rates := o.Rates
	var b strings.Builder
	for _, dir := range []string{DirUpload, DirDownload} {
		label := "download (node to front)"
		if dir == DirUpload {
			label = "upload (front to node)"
		}
		var r *PathRate
		for i := range rates {
			if rates[i].Direction == dir {
				r = &rates[i]
				break
			}
		}
		switch {
		case r == nil && dir == DirUpload && o.UploadNotMeasured != "":
			fmt.Fprintf(&b, "- path rate, %s: not measured in alpha (%s)\n", label, o.UploadNotMeasured)
		case r == nil:
			fmt.Fprintf(&b, "- path rate, %s: not posted\n", label)
		case dir == DirDownload:
			fmt.Fprintf(&b, "- path rate, %s: %.0f B/s and %.0f tokens/s under load (%d bytes, %d tokens timed over %.1f s, asked %.0f s)\n", label, r.BytesPerSecond(), r.TokensPerSecond(), r.Bytes, r.Tokens, r.TimedSeconds, r.AskedSeconds)
		default:
			fmt.Fprintf(&b, "- path rate, %s: %.0f B/s under load (%d bytes timed over %.1f s, asked %.0f s)\n", label, r.BytesPerSecond(), r.Bytes, r.TimedSeconds, r.AskedSeconds)
		}
	}
	return b.String()
}

func countCut(streams []Stream) int {
	n := 0
	for _, s := range streams {
		if s.CutBy != "" {
			n++
		}
	}
	return n
}
