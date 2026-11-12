package alpha

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// The pass criteria. Each function returns the failed checks as fixed labels
// (no job ID, host or account can be in one) and nothing when the run passes.
// Each check quotes the criterion it enforces, with the file and line at main's
// 9dce393 (2 Nov 2026); docs/alpha-runbook.md lists the same quotes.

// Hour is S1's stream length: "a completed stream of at least 3600 s".
const Hour = time.Hour

// minRate is S1's flag line: "a rate under 629,146 B/s" is flagged, not failed
// (spikes/s1-frp/README.md:288, C6; branch spike/s1-frp-dial-home).
const minRate = 629146

// jobsByTag maps the cluster's record of the run's jobs by their tags. A tag
// that names several jobs maps to the first by job ID; jobChecks fails the run
// for it, so the choice never decides a pass.
func jobsByTag(jobs []provider.Job) map[string]provider.Job {
	m := make(map[string]provider.Job, len(jobs))
	for _, j := range jobs {
		if prev, ok := m[j.Name]; !ok || j.ID < prev.ID {
			m[j.Name] = j
		}
	}
	return m
}

// streamChecks judges the streams of a handover or a no-overlap gap. Both
// criteria say "no request cut", and both tests warn that a run with nothing in
// flight proves nothing:
//
//	hbep/0007-lifecycle-controller/README.md:621: "A run with no stream in flight at the drain start proves nothing here"
//	hbep/0007-lifecycle-controller/README.md:660: "one deployment completes one handover end to end through the front with no request cut"
//	hbep/0006-instance-reachability/README.md:430: "A test that restarts the controller with no stream running proves nothing about cuts."
func streamChecks(streams []Stream, drainStart time.Time) []string {
	var f []string
	inFlight := 0
	for _, s := range streams {
		if s.InFlightAt(drainStart) {
			inFlight++
		}
		if s.CutBy != "" {
			f = append(f, "a stream was cut")
		} else if !s.Complete {
			f = append(f, "a stream did not finish by its completion rule")
		}
	}
	if inFlight == 0 {
		f = append(f, "no stream was in flight at the drain start")
	}
	return dedupe(f)
}

// deploymentChecks judges the ModelDeployment the scenario applied against the
// row's plan (Row.Plan).
//
// One Provider, the row's. For Helios, hbep/0007-lifecycle-controller/README.md:468
// ("Alpha's fence"): "QOS `now`'s one GPU per user makes a second deployment's
// primary on the Helios Provider pend behind the first ... So in alpha the Helios
// Provider serves one deployment, and that deployment allows the Helios Provider
// alone". For every stack row, the run measures one cluster, and a deployment
// that could also place an Instance elsewhere measures none.
//
// The shortest lease: a lease at or under the lead is PartitionTooShort and
// makes no Allocation (internal/lifecycle/timelimit.go:42), and a longer one
// holds the owner's shared slot, or a queue place, for longer than the run needs.
//
// The no-overlap row: "one deployment with `ReleaseWhenIdle`, a short `idleLimit`
// and a stream in flight at the drain start" (hbep/0007-lifecycle-controller/
// README.md:614).
func deploymentChecks(row Row, d DeploymentSpec) []string {
	var f []string
	tm := row.Clock()
	if len(d.Providers) != 1 {
		f = append(f, "the deployment does not allow exactly one Provider")
	} else if d.Providers[0].Cluster != row.Cluster {
		f = append(f, "the deployment's Provider is not on the row's cluster")
	}
	switch {
	case d.LeaseLimit <= tm.Lead():
		f = append(f, "the deployment's leaseLimit is not longer than the lead an Instance needs (PartitionTooShort)")
	case d.LeaseLimit != row.Lease:
		f = append(f, "the deployment's leaseLimit is not the row's shortest lease")
	}
	if d.MaxRequestDuration != tm.MaxRequestDuration {
		f = append(f, "the deployment's routing.maxRequestDuration is not the row's")
	}
	if want := row.Plan(); want.ReleaseWhenIdle && (!d.ReleaseWhenIdle || d.IdleLimit <= 0 || d.IdleLimit > tm.MaxIdleLimit) {
		f = append(f, "the no-overlap deployment lacks ReleaseWhenIdle with a short idleLimit")
	}
	return f
}

// ledgerChecks judges what the cluster itself recorded of the run's jobs: the
// partition each ran on, and that every tag the run wrote down is a job there.
func ledgerChecks(row Row, tags []string, jobs []provider.Job) ([]string, map[string]provider.Job) {
	var f []string
	by := jobsByTag(jobs)
	for _, tag := range tags {
		j, ok := by[tag]
		switch {
		case !ok:
			f = append(f, "the cluster has no record of a job the run made")
		case j.Partition != row.Partition:
			f = append(f, "a job ran on another partition than the row's")
		case j.TimeLimit != row.Lease:
			f = append(f, "a job's time limit in the cluster's record is not the row's lease")
		}
	}
	return append(f, jobChecks(row, tags, jobs)...), by
}

// jobChecks judges the run's jobs by their identities and by how many were in the
// queue at once, against the row's MostJobs: the number the runbook promises the
// owner. A tag names one job (a held duplicate submission, or a requeue, makes
// several under one tag, and a lookup by tag alone would see one); the run's jobs
// together are no more than MostJobs; and no more than MostJobs of them were
// queued or running at the same moment, a job counting from its submission to its
// end (one with no end is still there). A job with no submission time cannot be
// placed, so it fails the run.
func jobChecks(row Row, tags []string, jobs []provider.Job) []string {
	var f []string
	perTag := map[string]int{}
	ids := map[string]bool{}
	var distinct []provider.Job
	for _, j := range jobs {
		if j.ID != "" {
			if ids[j.ID] {
				continue
			}
			ids[j.ID] = true
		}
		distinct = append(distinct, j)
		perTag[j.Name]++
	}
	for _, tag := range tags {
		if perTag[tag] > 1 {
			f = append(f, "a tag names more than one job in the cluster's record")
		}
	}
	if len(distinct) > row.MostJobs {
		f = append(f, "the cluster's record has more jobs of the run than the row's maximum")
	}
	peak, complete := peakQueued(distinct)
	if !complete {
		f = append(f, "the cluster's record has no submission time for a job")
	}
	if peak > row.MostJobs {
		f = append(f, "more of the run's jobs were queued or running at once than the row's maximum")
	}
	return f
}

// farFuture is the end of a job that has none: later than any real time.
var farFuture = time.Date(9999, time.January, 1, 0, 0, 0, 0, time.UTC)

// peakQueued is the most of the jobs queued or running at the same moment, a job
// counting from its submission to its end (one with no end is still there). A job
// that ended as the next was submitted did not overlap it. complete is false when
// a job has no submission time: it cannot be placed, and the other jobs are
// counted without it.
func peakQueued(jobs []provider.Job) (peak int, complete bool) {
	type edge struct {
		at    time.Time
		delta int
	}
	complete = true
	var edges []edge
	for _, j := range jobs {
		if j.SubmitTime.IsZero() {
			complete = false
			continue
		}
		end := farFuture
		if j.EndTime != nil {
			end = *j.EndTime
		}
		edges = append(edges, edge{j.SubmitTime, +1}, edge{end, -1})
	}
	sort.Slice(edges, func(a, b int) bool {
		if !edges[a].at.Equal(edges[b].at) {
			return edges[a].at.Before(edges[b].at)
		}
		return edges[a].delta < edges[b].delta // an end before a submission at the same instant
	})
	live := 0
	for _, e := range edges {
		live += e.delta
		if live > peak {
			peak = live
		}
	}
	return peak, complete
}

// restartChecks: "on each alpha platform, Helios `plgrid-gpu-gh200` and Athena `plgrid-gpu-a100`, one restart at
// the worker rung" (hbep/0007-lifecycle-controller/README.md:637).
func restartChecks(n int) []string {
	if n != 1 {
		return []string{fmt.Sprintf("%d restarts at the worker rung, want 1", n)}
	}
	return nil
}

// tagCountChecks: the run made as many Allocations as the row's jobs, no more and no fewer.
func tagCountChecks(row Row, tags []string, label string) []string {
	if len(tags) != row.MostJobs {
		return []string{label}
	}
	return nil
}

// VerifyHandover judges alpha-athena-handover.
//
//	hbep/0007-lifecycle-controller/README.md:660: "on Athena `plgrid-gpu-a100`, one deployment completes one handover end to end through the front with no request cut;"
//	hbep/0007-lifecycle-controller/README.md:662: "on each alpha platform, Helios `plgrid-gpu-gh200` and Athena `plgrid-gpu-a100`, one restart at the worker rung;"
//	hbep/0005-llmlab-compatible-front/README.md:450: "on Athena `plgrid-gpu-a100`, through the front, one handover cuts no request (with HBEP-7)."
func VerifyHandover(row Row, o HandoverObs, jobs []provider.Job) []string {
	f := handoverFirstJobChecks(row, o, jobs)
	f = append(f, tagCountChecks(row, o.Tags, "a handover makes exactly two Allocations: a predecessor and a successor")...)
	if o.Handovers != 1 {
		f = append(f, fmt.Sprintf("%d handovers completed, want 1", o.Handovers))
	}
	if len(o.Instances) != 2 {
		f = append(f, "the run did not record both Instances")
	} else if p, s := o.Instances[0], o.Instances[1]; p.EndedAt.IsZero() || s.ReadyAt.IsZero() || s.ReadyAt.After(p.EndedAt) {
		// A handover is make-before-break: the successor is ready before the predecessor ends.
		f = append(f, "the successor was not ready before the predecessor ended")
	}
	return dedupe(f)
}

// handoverFirstJobChecks are the handover row's checks that hold whether or not the successor ever started, so they
// are what a run judges when its first job ran and its second never started (ErrSecondNotStarted; review of PR 284,
// B3): the deployment, the streams across the drain start, the worker-rung restart, the attribution of every response,
// and the cluster's record of the jobs. A failure here is a failure of a part of the run that did happen, and it
// outranks the NOT RUN. The handover itself and the successor's readiness were never put to the test.
func handoverFirstJobChecks(row Row, o HandoverObs, jobs []provider.Job) []string {
	f := append(deploymentChecks(row, o.Deployment), streamChecks(o.Streams, o.DrainStart)...)
	lf, _ := ledgerChecks(row, o.Tags, jobs)
	f = append(f, lf...)
	f = append(f, restartChecks(o.WorkerRestarts)...)
	// Attribution on every response (HBEP-5): no response recorded is no evidence of it.
	if o.Responses <= 0 {
		f = append(f, "the run recorded no response, so attribution is unchecked")
	} else if o.Attributed != o.Responses {
		f = append(f, "a response lacked its attribution headers")
	}
	return f
}

// VerifyNoOverlap judges alpha-helios-nooverlap.
//
//	hbep/0007-lifecycle-controller/README.md:661: "on Helios `plgrid-gpu-gh200` under QOS `now`, one deployment goes through no-overlap mode end to end through the front with no request cut, and the gap is measured"
//	hbep/0007-lifecycle-controller/README.md:632: "The replacement is created at its planned time, accepted, and pends on the fake's GPU cap while the predecessor runs; it never starts before the predecessor's job ends ... A fixture without a GPU cap proves nothing here, since its replacement would start beside the predecessor."
//	hbep/0007-lifecycle-controller/README.md:484: "the gap is the time from the primary's drain start until its successor is `ready`."
func VerifyNoOverlap(row Row, o NoOverlapObs, jobs []provider.Job) []string {
	f := noOverlapFirstJobChecks(row, o, jobs)
	f = append(f, tagCountChecks(row, o.Tags, "a no-overlap run makes exactly two Allocations: a predecessor and a successor")...)
	_, by := ledgerChecks(row, o.Tags, jobs)
	if o.Gap() <= 0 {
		f = append(f, "the gap was not measured: the drain start and the successor's ready are not both seen, in that order")
	}
	if !o.NoOverlapSlotSeen {
		f = append(f, "the deployment never showed NoOverlapSlot")
	}
	if len(o.Tags) == 2 {
		pre, okP := by[o.Tags[0]]
		suc, okS := by[o.Tags[1]]
		if okP && okS {
			f = append(f, queuedAheadChecks(pre, suc)...)
		}
	}
	return dedupe(f)
}

// noOverlapFirstJobChecks are the no-overlap row's checks that hold whether or not the successor ever started, as
// handoverFirstJobChecks are the handover's: the deployment, the stream across the drain start, the worker-rung
// restart and the cluster's record of the jobs. The gap, NoOverlapSlot and the successor's place in the queue were
// never put to the test.
func noOverlapFirstJobChecks(row Row, o NoOverlapObs, jobs []provider.Job) []string {
	f := append(deploymentChecks(row, o.Deployment), streamChecks(o.Streams, o.DrainStart)...)
	// One restart at the worker rung on each alpha platform (hbep/0007-lifecycle-controller/README.md:637), Helios's included.
	f = append(f, restartChecks(o.WorkerRestarts)...)
	lf, _ := ledgerChecks(row, o.Tags, jobs)
	return append(f, lf...)
}

// queuedAheadChecks judges, from the cluster's own times, that the successor was
// queued ahead of its predecessor's end and never ran beside it.
func queuedAheadChecks(pre, suc provider.Job) []string {
	var f []string
	switch {
	case suc.SubmitTime.IsZero() || pre.EndTime == nil || suc.StartTime == nil:
		f = append(f, "the cluster's times for the two jobs are incomplete")
	default:
		if !suc.SubmitTime.Before(*pre.EndTime) {
			f = append(f, "the successor was not queued before the predecessor ended")
		}
		if suc.StartTime.Before(*pre.EndTime) {
			f = append(f, "the successor started before the predecessor ended")
		}
	}
	return f
}

// SuccessorQueueWait is the successor's queue wait once the GPU freed: its start
// minus the predecessor's end, which the Helios run records
// (hbep/0007-lifecycle-controller/README.md:614). ok is false when the cluster
// does not give both times.
func SuccessorQueueWait(pre, suc provider.Job) (d time.Duration, ok bool) {
	if pre.EndTime == nil || suc.StartTime == nil {
		return 0, false
	}
	return suc.StartTime.Sub(*pre.EndTime), true
}

// VerifyTunnelHour judges tunnel-hour-helios and tunnel-hour-athena.
//
//	hbep/0006-instance-reachability/README.md:435: "S1's one-hour runs pass with this wiring on Helios and on Athena, in the mode each uses, with the path rate measured."
//	hbep/0006-instance-reachability/README.md:456: "S1's one-hour runs pass with this wiring on Helios and on Athena, with the path rate from compute node to front measured;"
//	spikes/s1-frp/README.md:284 and :285 (C2, C3; branch spike/s1-frp-dial-home, 01c2ecd): every lane stays reachable, uncut, for at least an hour.
//	spikes/s1-frp/README.md:288 (C6): the path rate is "ok, at least 30 s asked, at least 29 s timed at the VM's receiver", and moved bytes.
//
// The science agent's ruling of 2 Nov 2026 reads the criterion as an hour of one
// tunnel staying healthy under real use, not one stream an hour long (the front
// cuts a request at the deployment's deadline). A pass needs:
//  1. one tunnel session for the whole hour: frps's lastStartTime for the proxy
//     the same in every sample, no frpc login between, TunnelConnected True throughout;
//  2. traffic that tests what an hour tests: requests back to back, one about
//     4 minutes, 16 concurrent streams at 20 tokens a second, an idle stretch
//     of 2 minutes with heartbeats only;
//  3. no cut: no response with cutBy, no stream_cut;
//  4. the path rate in both directions, upload (front to node) and download
//     (node to front, in tokens as well as bytes), each under load (issue #224);
//  5. the upload exercised: request bodies of 0.1 MB or more, sent while the 16
//     streams were open.
//
// A run in which frpc logged in again mid-hour but every request passed is no
// pass: the session check is what fails it.
func VerifyTunnelHour(row Row, o TunnelHourObs, jobs []provider.Job) []string {
	f := deploymentChecks(row, o.Deployment)
	tm := row.Clock()
	if o.DrainStart.IsZero() {
		f = append(f, "the Instance's drain start was not observed, so the hour cannot be placed before it")
	}
	f = append(f, requestChecks(o.Requests, o.DrainStart, tm)...)
	f = append(f, sessionChecks(o.Requests, o.Samples, tm)...)
	// A live row in alpha has no sender-side timing of the upload (the science agent's ruling (4) on PR 255).
	live := o.UploadNotMeasured != ""
	f = append(f, trafficChecks(o.Requests, tm.withLongPlan(o.LongPlan), live)...)
	f = append(f, rateChecks(o.Rates, live)...)
	lf, by := ledgerChecks(row, o.Tags, jobs)
	f = append(f, lf...)
	f = append(f, tagCountChecks(row, o.Tags, "a tunnel run makes exactly one Allocation")...)
	// The cluster's own times corroborate the requests': a request's clock is the
	// scenario's, the job's start and end are the scheduler's. Both are required.
	for _, tag := range o.Tags {
		j, ok := by[tag]
		switch {
		case !ok:
		case j.StartTime == nil || j.EndTime == nil:
			f = append(f, "the cluster's record lacks the job's start or end time, so it cannot show the hour")
		case j.EndTime.Sub(*j.StartTime) < tm.Hour:
			f = append(f, "the cluster's record shows the job ran under an hour")
		}
	}
	return dedupe(f)
}

// rateChecks: the path rate is measured in both directions, one rate each, upload
// (front to node) and download (node to front), as S1's C6 asks of the token
// direction: ok, at least 30 s asked, at least 29 s timed, bytes moved, measured
// under load; the download also moves tokens (issue #224).
func rateChecks(rates []PathRate, uploadNotMeasured bool) []string {
	var f []string
	by := map[string]PathRate{}
	for _, r := range rates {
		if _, dup := by[r.Direction]; dup {
			f = append(f, "the path rates are not one per direction (upload and download)")
			continue
		}
		by[r.Direction] = r
	}
	for dir := range by {
		if dir != DirUpload && dir != DirDownload {
			f = append(f, "a path rate names a direction that is neither upload nor download")
		}
	}
	for _, dir := range []string{DirUpload, DirDownload} {
		r, found := by[dir]
		switch {
		case !found && dir == DirUpload && uploadNotMeasured:
			// Not measured in alpha on a live row, and said so in the text to post.
		case !found:
			f = append(f, "the "+dir+" path rate was not posted: both directions are needed")
		case !r.OK:
			f = append(f, "the "+dir+" path rate was not measured")
		case r.AskedSeconds < 30 || r.TimedSeconds < 29 || r.Bytes <= 0:
			f = append(f, "the "+dir+" path rate was asked for under 30 s, timed under 29 s, or moved no bytes")
		case !r.UnderLoad:
			f = append(f, "the "+dir+" path rate was not measured under load")
		case dir == DirDownload && r.Tokens <= 0:
			f = append(f, "the download path rate moved no tokens")
		}
	}
	return f
}

// span is the time from the first request opening to the last ending; zero when
// there is none or one has no times.
func span(reqs []Stream) (first, last time.Time) {
	for _, r := range reqs {
		if r.Opened.IsZero() || r.Ended.IsZero() {
			return time.Time{}, time.Time{}
		}
		if first.IsZero() || r.Opened.Before(first) {
			first = r.Opened
		}
		if r.Ended.After(last) {
			last = r.Ended
		}
	}
	return first, last
}

// requestChecks judges each request, and that together they cover the hour and end before the drain start.
func requestChecks(reqs []Stream, drainStart time.Time, tm Timing) []string {
	if len(reqs) == 0 {
		return []string{"no request was made during the hour"}
	}
	var f []string
	for _, s := range reqs {
		switch {
		case s.Opened.IsZero() || s.Ended.IsZero():
			f = append(f, "a request has no opening or ending time")
		case s.CutBy != "" || s.Error == "stream_cut":
			f = append(f, "a stream was cut during the hour")
		case !s.Complete && !s.ArrivalVerified():
			// A large request the engine answered with its count, showing it read the whole prompt, is the live
			// hour's proof of arrival, not a failed stream.
			f = append(f, "a stream did not complete")
		case s.Ended.Sub(s.Opened) > tm.MaxRequestDuration:
			// The front would have cut it: an observation of one means the deadline was not the deployment's.
			f = append(f, "a request outlasted the deployment's request deadline")
		}
	}
	first, last := span(reqs)
	if last.Sub(first) < tm.Hour {
		f = append(f, "the requests lasted less than an hour")
	}
	if !drainStart.IsZero() && last.After(drainStart) {
		f = append(f, "the hour ended after the Instance's drain start")
	}
	return f
}

// sessionChecks: one tunnel session for the whole hour. The samples must open
// the hour (one at or before the first request) and close it (one at or after the
// last), come no more than MaxSampleGap apart, all show the Instance's
// TunnelConnected True, and all carry the same frps lastStartTime for its proxy:
// a different one is a login of frpc in between, which the requests alone cannot show.
func sessionChecks(reqs []Stream, samples []TunnelSample, tm Timing) []string {
	if len(samples) < 2 {
		return []string{"the tunnel session was looked at fewer than twice, so one session for the hour is not shown"}
	}
	var f []string
	ss := append([]TunnelSample(nil), samples...)
	sort.Slice(ss, func(a, b int) bool { return ss[a].At.Before(ss[b].At) })
	first, last := span(reqs)
	if !first.IsZero() {
		if ss[0].At.After(first) {
			f = append(f, "the tunnel session was not looked at before the first request")
		}
		if ss[len(ss)-1].At.Before(last) {
			f = append(f, "the tunnel session was not looked at after the last request")
		}
	}
	for i, s := range ss {
		switch {
		case s.At.IsZero() || s.LastStart.IsZero():
			f = append(f, "a look at the tunnel session lacks its time or frps's lastStartTime")
		case !s.LastStart.Equal(ss[0].LastStart):
			f = append(f, "frps's lastStartTime for the proxy changed during the hour: frpc logged in again")
		}
		if !s.Connected {
			f = append(f, "TunnelConnected was not True throughout the hour")
		}
		if i > 0 && s.At.Sub(ss[i-1].At) > tm.MaxSampleGap {
			f = append(f, "the tunnel session went more than 2 minutes without a look")
		}
	}
	return f
}

// trafficChecks: the hour must have contained what tests an hour of a tunnel.
func trafficChecks(reqs []Stream, tm Timing, uploadNotMeasured bool) []string {
	var f []string
	if longestChain(reqs, tm.MaxChainGap) < tm.MinChain {
		f = append(f, "the hour held no run of requests back to back")
	}
	long := false
	for _, r := range reqs {
		// A completed stream (the delta round of PR 255, B3): a large request that came late is no request near
		// the deadline, though it passes requestChecks.
		if d := r.Ended.Sub(r.Opened); r.Long && r.Complete && d >= tm.NearDeadline && d <= tm.MaxRequestDuration {
			long = true
		}
	}
	if !long {
		f = append(f, longStreamFailure)
	}
	if concurrentStreams(reqs, tm.StreamRate) < tm.ConcurrentStreams {
		f = append(f, "the hour held no 16 concurrent streams at 20 tokens a second")
	}
	if longestIdle(reqs) < tm.MinIdleGap {
		f = append(f, "the hour held no idle stretch of 2 minutes with heartbeats only")
	}
	// The upload direction (issue #224): large request bodies, sent while the 16 streams were open, so that the
	// shared tunnel's head-of-line pressure is exercised in the direction the front's request bodies take.
	switch {
	case largestBody(reqs) < MinLargeBody:
		f = append(f, "no request body reached 0.1 MB, so the upload direction was not exercised")
	case uploadNotMeasured:
		// Live, in alpha (the science agent's ruling (4) on PR 255): each large body shows it arrived whole by the
		// engine's count of its prompt, and one was in flight, by the client's window of it, while the 16 streams
		// were open. First a completed response with usage: a body cut short in the byte tunnel is no longer JSON
		// and is answered otherwise (the delta round's B1). Then its count: the whole well-formed body was tokenized.
		otherwise, miscounted := false, false
		for _, l := range LargeRequests(reqs) {
			switch {
			case !l.PromptCounted():
				otherwise = true
			case !l.ArrivalVerified():
				miscounted = true
			}
		}
		if otherwise {
			f = append(f, "a request of 0.1 MB or more was not answered with a completed response counting its prompt (usage.prompt_tokens): a body cut short in the tunnel is no longer JSON")
		}
		if miscounted {
			f = append(f, "a request of 0.1 MB or more did not show it arrived whole: the engine's usage.prompt_tokens must be its prompt's own input tokens")
		}
		if largeInFlightAmongStreams(reqs, tm) == 0 {
			f = append(f, "no request of 0.1 MB or more was in flight while the 16 streams were open")
		}
	case largeAmongStreams(reqs, tm) == 0:
		f = append(f, "no request of 0.1 MB or more was sent while the 16 streams were open")
	}
	return f
}

// largeInFlightAmongStreams counts the large requests in flight, from the client's first byte sent to its reply read
// (SendStart to SendEnd), at a moment when at least tm.ConcurrentStreams streams were open: the live hour's measure,
// where the upload interval itself is not measured. A request whose window is not recorded is not counted.
func largeInFlightAmongStreams(reqs []Stream, tm Timing) int {
	n := 0
	for _, l := range LargeRequests(reqs) {
		if l.SendStart.IsZero() || !l.SendEnd.After(l.SendStart) {
			continue
		}
		if streamsOpenDuring(reqs, l.SendStart, l.SendEnd, tm) {
			n++
		}
	}
	return n
}

// largestBody is the largest request body any request sent, in bytes.
func largestBody(reqs []Stream) int {
	best := 0
	for _, r := range reqs {
		best = max(best, r.BodyBytes)
	}
	return best
}

// LargeRequests is the requests with a body of MinLargeBody or more.
func LargeRequests(reqs []Stream) []Stream {
	var out []Stream
	for _, r := range reqs {
		if r.BodyBytes >= MinLargeBody {
			out = append(out, r)
		}
	}
	return out
}

// largeAmongStreams counts the large requests whose upload (the body, from its first byte to its last) was in progress
// at a moment when at least tm.ConcurrentStreams streams, each at tm.StreamRate or more, were open too. It is the upload
// interval that counts (PR 244, B2): a 100,000-byte body can be sent before the streams open, and its response then stay
// open across their start; the request was open among the streams, and its body was not. A request whose upload interval
// is not recorded is not counted.
func largeAmongStreams(reqs []Stream, tm Timing) int {
	n := 0
	for _, l := range LargeRequests(reqs) {
		if !l.UploadRecorded() {
			continue
		}
		if streamsOpenDuring(reqs, l.BodyStart, l.BodyEnd, tm) {
			n++
		}
	}
	return n
}

// streamsOpenDuring reports whether, at some moment from from to to, at least tm.ConcurrentStreams streams, each at
// tm.StreamRate or more, were open. The moments that matter are from itself and each stream's opening inside it.
func streamsOpenDuring(reqs []Stream, from, to time.Time, tm Timing) bool {
	at := []time.Time{from}
	for _, r := range reqs {
		if isStream(r, tm.StreamRate) && r.Opened.After(from) && r.Opened.Before(to) {
			at = append(at, r.Opened)
		}
	}
	for _, t := range at {
		open := 0
		for _, r := range reqs {
			if isStream(r, tm.StreamRate) && !r.Opened.After(t) && r.Ended.After(t) {
				open++
			}
		}
		if open >= tm.ConcurrentStreams {
			return true
		}
	}
	return false
}

// longestChain is the most requests that follow one another with no more than maxGap between. The large requests
// (MinLargeBody or more) are not counted: the live hour sends them a fraction of a second apart, inside the burst,
// and they must not stand in for the run back to back near the deadline.
func longestChain(reqs []Stream, maxGap time.Duration) int {
	var rs []Stream
	for _, r := range reqs {
		if r.BodyBytes < MinLargeBody {
			rs = append(rs, r)
		}
	}
	sort.Slice(rs, func(a, b int) bool { return rs[a].Opened.Before(rs[b].Opened) })
	best, run := 0, 0
	for i, r := range rs {
		// One that starts after the last ended, within MaxChainGap: concurrent streams are not a run.
		if gap := r.Opened.Sub(rs[max(i-1, 0)].Ended); i > 0 && gap >= 0 && gap <= maxGap {
			run++
		} else {
			run = 1
		}
		if run > best {
			best = run
		}
	}
	return best
}

// isStream: a request that ran at rate tokens a second or more is one of the streams the hour's load is made of.
func isStream(r Stream, rate float64) bool { return r.TokensPerSecond() >= rate }

// concurrentStreams is the most streams open together at one moment that each ran at rate tokens a second or more.
func concurrentStreams(reqs []Stream, rate float64) int {
	best := 0
	for _, at := range reqs {
		open := 0
		for _, r := range reqs {
			if !r.Opened.After(at.Opened) && r.Ended.After(at.Opened) && isStream(r, rate) {
				open++
			}
		}
		if open > best {
			best = open
		}
	}
	return best
}

// longestIdle is the longest stretch between the first opening and the last ending with no request open at all.
func longestIdle(reqs []Stream) time.Duration {
	rs := append([]Stream(nil), reqs...)
	sort.Slice(rs, func(a, b int) bool { return rs[a].Opened.Before(rs[b].Opened) })
	var end time.Time
	var best time.Duration
	for i, r := range rs {
		if i > 0 {
			if gap := r.Opened.Sub(end); gap > best {
				best = gap
			}
		}
		if r.Ended.After(end) {
			end = r.Ended
		}
	}
	return best
}

// TunnelHourFlags are the observations S1's verdict prints as FLAG lines: they
// are posted with the result and do not fail it.
func TunnelHourFlags(o TunnelHourObs) []string {
	var fl []string
	// S1's flag is on the token direction, node to front (C6); the upload has no flag.
	for _, r := range o.Rates {
		if bps := r.BytesPerSecond(); r.Direction == DirDownload && r.OK && bps > 0 && bps < minRate {
			fl = append(fl, fmt.Sprintf("download path rate %.0f B/s is under 629146 B/s (6 MiB window over 10 s)", bps))
			break
		}
	}
	// Head-of-line (the science agent's ruling (4) on PR 255): a stall of the token direction, each FLAG naming
	// the window its gap overlapped (HeadOfLineOf), so that a reader can tell a tunnel stall from engine work.
	if h := o.HeadOfLine; h != nil {
		if h.Send > HeadOfLineStall {
			fl = append(fl, fmt.Sprintf("head-of-line, in a send window: a stream waited %s between two tokens while a large body was being sent, over the %s that marks a stall (a body on its way through the tunnel; another body's prefill may overlap)",
				h.Send.Round(time.Millisecond), HeadOfLineStall))
		}
		if h.Prefill > HeadOfLineStall {
			fl = append(fl, fmt.Sprintf("head-of-line, in a prefill window: a stream waited %s between two tokens after a large body was sent and before its response, with no body being sent, over the %s that marks a stall (the engine prefilling a body's whole prompt, not the tunnel's upload)",
				h.Prefill.Round(time.Millisecond), HeadOfLineStall))
		}
		if h.Elsewhere > HeadOfLineStall {
			fl = append(fl, fmt.Sprintf("head-of-line, in neither window: a stream waited %s between two tokens while no large body was in flight, over the %s that marks a stall",
				h.Elsewhere.Round(time.Millisecond), HeadOfLineStall))
		}
	}
	return fl
}

// VerifyAccounting judges gpu-accounting-*.
//
//	hbep/0008-observability-and-events/README.md:444: "the kept accounting record's GPU entry is checked once on a real GPU job on each alpha partition;"
//	hbep/0008-observability-and-events/README.md:331: the record holds "`allocatedGPUs` ... and `basis: allocated`".
//
// The record is built from the scheduler's accounting, so it is never checked
// against the scheduler's accounting: NodeGPUs is what the job itself saw.
func VerifyAccounting(row Row, o AccountingObs) []string {
	var f []string
	if o.Partition != row.Partition {
		f = append(f, "the record names another partition than the row's")
	}
	if o.Basis != "allocated" {
		f = append(f, "the record's basis is not allocated")
	}
	if o.RequestedGPUs <= 0 || o.NodeGPUs <= 0 {
		// A 0 = 0 agreement would prove nothing: a GPU job is the point.
		f = append(f, "the job asked for or saw no GPU")
	}
	switch {
	case o.RecordGPUs == nil:
		f = append(f, "the record marks its GPU entry unknown")
	case int(*o.RecordGPUs) != o.NodeGPUs:
		f = append(f, "the record's GPU entry differs from the GPUs the job saw")
	}
	if o.RequestedGPUs != o.NodeGPUs {
		f = append(f, "the GPUs the job saw differ from what it asked for")
	}
	return dedupe(f)
}

func dedupe(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

// Summary joins failures for a log line.
func Summary(failures []string) string { return strings.Join(failures, "; ") }
