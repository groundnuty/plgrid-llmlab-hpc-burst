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
	return fmt.Sprintf("%s\n- handovers completed: %d; streams in flight at the drain start: %d of %d; cut: %d\n- restarts at the worker rung: %d; responses with attribution: %d of %d\n",
		verdictLine(row, failures), o.Handovers, n, len(o.Streams), cut, o.WorkerRestarts, o.Attributed, o.Responses)
}

func noOverlapPost(row Row, o NoOverlapObs, failures []string, jobs []provider.Job) string {
	var b strings.Builder
	n, cut := inFlight(o.Streams, o.DrainStart)
	fmt.Fprintf(&b, "%s\n- gap from the drain start to the successor ready: %s\n- streams in flight at the drain start: %d of %d; cut: %d; NoOverlapSlot seen: %t; new requests answered 503 no_ready_target: %d\n",
		verdictLine(row, failures), o.Gap().Round(time.Second), n, len(o.Streams), cut, o.NoOverlapSlotSeen, o.NoReadyTarget)
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
	var held time.Duration
	for _, s := range o.Lanes {
		if d := s.Ended.Sub(s.Opened); held == 0 || d < held {
			held = d
		}
	}
	fmt.Fprintf(&b, "%s\n- lanes held: %d, the shortest %s; streams cut: %d; tunnel drops: %d\n- path rate, node to front: %.0f B/s (%d bytes timed over %.1f s, asked %.0f s)\n",
		verdictLine(row, failures), len(o.Lanes), held.Round(time.Second), countCut(o.Lanes), o.TunnelDrops,
		o.Rate.BytesPerSecond(), o.Rate.Bytes, o.Rate.TimedSeconds, o.Rate.AskedSeconds)
	for _, fl := range TunnelHourFlags(o) {
		fmt.Fprintf(&b, "- FLAG: %s\n", fl)
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
