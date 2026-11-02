package alpha

// The verdicts on observations built by hand, at the boundaries the criteria
// name. Expected values come from the criteria's text (HBEP-5, -6, -7, -8 and
// S1's verdict), never from running the code under test on the same input.

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

var t0 = time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC)

func at(d time.Duration) time.Time { return t0.Add(d) }
func ptr(t time.Time) *time.Time   { return &t }

func job(tag, partition string, submit, start, end time.Duration) provider.Job {
	return provider.Job{ID: "id-" + tag, Name: tag, Partition: partition, SubmitTime: at(submit), StartTime: ptr(at(start)), EndTime: ptr(at(end))}
}

// leased sets the time limit the cluster's record gives a job.
func leased(row Row, jobs ...provider.Job) []provider.Job {
	for i := range jobs {
		jobs[i].TimeLimit = row.Lease
	}
	return jobs
}

func rowOf(name string) Row { r, _ := RowByName(name); return r }

func wantFailure(t *testing.T, got []string, want string) {
	t.Helper()
	if want == "" {
		if len(got) != 0 {
			t.Errorf("failed checks %q, want a pass", Summary(got))
		}
		return
	}
	if !strings.Contains(Summary(got), want) {
		t.Errorf("failed checks %q lack %q", Summary(got), want)
	}
}

func goodHandover() (HandoverObs, []provider.Job) {
	row, _ := RowByName("alpha-athena-handover")
	o := HandoverObs{
		Tags:    []string{"hb-pre-aaaaaaaaaaaa", "hb-suc-bbbbbbbbbbbb"},
		Streams: []Stream{{ID: "s", Opened: at(time.Minute), Ended: at(20 * time.Minute), Complete: true}},
		Instances: []InstanceSpan{
			{Tag: "hb-pre-aaaaaaaaaaaa", ReadyAt: at(time.Minute), EndedAt: at(30 * time.Minute)},
			{Tag: "hb-suc-bbbbbbbbbbbb", ReadyAt: at(15 * time.Minute)}},
		DrainStart: at(10 * time.Minute), Handovers: 1, WorkerRestarts: 1, Responses: 5, Attributed: 5, Deployment: row.Plan(),
	}
	return o, leased(row, job(o.Tags[0], row.Partition, 0, time.Minute, 30*time.Minute), job(o.Tags[1], row.Partition, 5*time.Minute, 14*time.Minute, 50*time.Minute))
}

func TestHandoverBoundaries(t *testing.T) {
	row, _ := RowByName("alpha-athena-handover")
	cases := []struct {
		name   string
		mutate func(*HandoverObs, *[]provider.Job)
		want   string
	}{
		{"as it should be", func(*HandoverObs, *[]provider.Job) {}, ""},
		// A stream that ends exactly at the drain start was not in flight: the criterion is about a stream running across it.
		{"a stream ending at the drain start", func(o *HandoverObs, _ *[]provider.Job) { o.Streams[0].Ended = o.DrainStart }, "no stream was in flight"},
		{"a stream opened at the drain start", func(o *HandoverObs, _ *[]provider.Job) { o.Streams[0].Opened = o.DrainStart }, ""},
		{"a stream opened after it", func(o *HandoverObs, _ *[]provider.Job) { o.Streams[0].Opened = o.DrainStart.Add(time.Second) }, "no stream was in flight"},
		{"an unfinished stream", func(o *HandoverObs, _ *[]provider.Job) { o.Streams[0].Complete = false }, "did not finish by its completion rule"},
		{"a cut stream", func(o *HandoverObs, _ *[]provider.Job) { o.Streams[0].CutBy = "front" }, "a stream was cut"},
		{"a second handover", func(o *HandoverObs, _ *[]provider.Job) { o.Handovers = 2 }, "2 handovers completed, want 1"},
		{"the successor ready at the predecessor's end", func(o *HandoverObs, _ *[]provider.Job) { o.Instances[1].ReadyAt = o.Instances[0].EndedAt }, ""},
		{"the successor ready a second after", func(o *HandoverObs, _ *[]provider.Job) {
			o.Instances[1].ReadyAt = o.Instances[0].EndedAt.Add(time.Second)
		}, "the successor was not ready before the predecessor ended"},
		{"a successor never ready", func(o *HandoverObs, _ *[]provider.Job) { o.Instances[1].ReadyAt = time.Time{} }, "the successor was not ready"},
		{"one Instance recorded", func(o *HandoverObs, _ *[]provider.Job) { o.Instances = o.Instances[:1] }, "did not record both Instances"},
		{"one Allocation", func(o *HandoverObs, j *[]provider.Job) { o.Tags = o.Tags[:1] }, "exactly two Allocations"},
		{"a third Allocation", func(o *HandoverObs, j *[]provider.Job) {
			o.Tags = append(o.Tags, "hb-x-cccccccccccc")
			*j = append(*j, leased(rowOf("alpha-athena-handover"), job("hb-x-cccccccccccc", "plgrid-gpu-a100", 6*time.Minute, 7*time.Minute, 20*time.Minute))...)
		}, "more jobs of the run than the row's maximum"},
		{"a held duplicate submission under the predecessor's tag", func(o *HandoverObs, j *[]provider.Job) {
			dup := job(o.Tags[0], "plgrid-gpu-a100", 2*time.Minute, 2*time.Minute, 3*time.Minute)
			dup.ID, dup.TimeLimit = "id-duplicate", rowOf("alpha-athena-handover").Lease
			*j = append(*j, dup)
		}, "a tag names more than one job"},
		{"the same job listed twice is one job", func(o *HandoverObs, j *[]provider.Job) { *j = append(*j, (*j)[0]) }, ""},
		{"no response recorded", func(o *HandoverObs, _ *[]provider.Job) { o.Responses, o.Attributed = 0, 0 }, "no response, so attribution is unchecked"},
		{"a job missing from the cluster's record", func(o *HandoverObs, j *[]provider.Job) { *j = (*j)[:1] }, "no record of a job"},
		{"a job on another partition", func(o *HandoverObs, j *[]provider.Job) { (*j)[1].Partition = "other" }, "another partition"},
		{"no worker restart", func(o *HandoverObs, _ *[]provider.Job) { o.WorkerRestarts = 0 }, "0 restarts at the worker rung"},
		{"a second worker restart", func(o *HandoverObs, _ *[]provider.Job) { o.WorkerRestarts = 2 }, "2 restarts at the worker rung"},
		{"one response unattributed", func(o *HandoverObs, _ *[]provider.Job) { o.Attributed = 4 }, "lacked its attribution headers"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			o, jobs := goodHandover()
			c.mutate(&o, &jobs)
			wantFailure(t, VerifyHandover(row, o, jobs), c.want)
		})
	}
}

func goodNoOverlap() (NoOverlapObs, []provider.Job) {
	row, _ := RowByName("alpha-helios-nooverlap")
	o := NoOverlapObs{
		Tags:    []string{"hb-pre-aaaaaaaaaaaa", "hb-suc-bbbbbbbbbbbb"},
		Streams: []Stream{{ID: "s", Opened: at(time.Minute), Ended: at(31 * time.Minute), Complete: true}},
		// The drain start at 30 min, the successor ready at 50 min: a gap of 20 min.
		DrainStart: at(30 * time.Minute), SuccessorReady: at(50 * time.Minute), NoOverlapSlotSeen: true, WorkerRestarts: 1, Deployment: row.Plan(),
	}
	return o, leased(row,
		job(o.Tags[0], row.Partition, 0, time.Minute, 40*time.Minute),
		job(o.Tags[1], row.Partition, 5*time.Minute, 41*time.Minute, 90*time.Minute))
}

func TestNoOverlapBoundaries(t *testing.T) {
	row, _ := RowByName("alpha-helios-nooverlap")
	cases := []struct {
		name   string
		mutate func(*NoOverlapObs, *[]provider.Job)
		want   string
	}{
		{"as it should be", func(*NoOverlapObs, *[]provider.Job) {}, ""},
		{"no stream in flight", func(o *NoOverlapObs, _ *[]provider.Job) { o.Streams[0].Ended = o.DrainStart }, "no stream was in flight"},
		{"a cut stream", func(o *NoOverlapObs, _ *[]provider.Job) { o.Streams[0].CutBy = "tunnel" }, "a stream was cut"},
		{"a zero gap", func(o *NoOverlapObs, _ *[]provider.Job) { o.SuccessorReady = o.DrainStart }, "the gap was not measured"},
		{"a negative gap", func(o *NoOverlapObs, _ *[]provider.Job) { o.SuccessorReady = o.DrainStart.Add(-time.Second) }, "the gap was not measured"},
		{"a successor never ready", func(o *NoOverlapObs, _ *[]provider.Job) { o.SuccessorReady = time.Time{} }, "the gap was not measured"},
		{"no drain start", func(o *NoOverlapObs, _ *[]provider.Job) { o.DrainStart = time.Time{} }, "the gap was not measured"},
		{"NoOverlapSlot never shown", func(o *NoOverlapObs, _ *[]provider.Job) { o.NoOverlapSlotSeen = false }, "never showed NoOverlapSlot"},
		{"a third Allocation", func(o *NoOverlapObs, _ *[]provider.Job) { o.Tags = append(o.Tags, "hb-x-cccccccccccc") }, "exactly two Allocations"},
		{"one Allocation", func(o *NoOverlapObs, _ *[]provider.Job) { o.Tags = o.Tags[:1] }, "exactly two Allocations"},
		{"no worker restart on Helios", func(o *NoOverlapObs, _ *[]provider.Job) { o.WorkerRestarts = 0 }, "0 restarts at the worker rung, want 1"},
		{"two worker restarts on Helios", func(o *NoOverlapObs, _ *[]provider.Job) { o.WorkerRestarts = 2 }, "2 restarts at the worker rung, want 1"},
		{"a held duplicate submission under the successor's tag", func(o *NoOverlapObs, j *[]provider.Job) {
			dup := job(o.Tags[1], "plgrid-gpu-gh200", 6*time.Minute, 6*time.Minute, 7*time.Minute)
			dup.ID, dup.TimeLimit = "id-duplicate", rowOf("alpha-helios-nooverlap").Lease
			*j = append(*j, dup)
		}, "a tag names more than one job"},
		{"the successor starts at the predecessor's end", func(o *NoOverlapObs, j *[]provider.Job) { (*j)[1].StartTime = (*j)[0].EndTime }, ""},
		{"the successor starts a second early", func(o *NoOverlapObs, j *[]provider.Job) {
			(*j)[1].StartTime = ptr((*j)[0].EndTime.Add(-time.Second))
		}, "started before the predecessor ended"},
		{"the successor submitted at the predecessor's end", func(o *NoOverlapObs, j *[]provider.Job) { (*j)[1].SubmitTime = *(*j)[0].EndTime }, "not queued before the predecessor ended"},
		{"the successor submitted after it", func(o *NoOverlapObs, j *[]provider.Job) { (*j)[1].SubmitTime = (*j)[0].EndTime.Add(time.Second) }, "not queued before the predecessor ended"},
		{"a missing time", func(o *NoOverlapObs, j *[]provider.Job) { (*j)[1].StartTime = nil }, "times for the two jobs are incomplete"},
		{"a job on another partition", func(o *NoOverlapObs, j *[]provider.Job) { (*j)[0].Partition = "plgrid" }, "another partition"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			o, jobs := goodNoOverlap()
			c.mutate(&o, &jobs)
			wantFailure(t, VerifyNoOverlap(row, o, jobs), c.want)
		})
	}
}

func TestTheGapAndTheQueueWaitAreWorkedOutFromTheTimes(t *testing.T) {
	o, jobs := goodNoOverlap()
	if g := o.Gap(); g != 20*time.Minute {
		t.Errorf("gap %s, want 20m0s (drain start at 30 min, successor ready at 50 min)", g)
	}
	// The predecessor ended at 40 min and the successor started at 41 min.
	if w, ok := SuccessorQueueWait(jobs[0], jobs[1]); !ok || w != time.Minute {
		t.Errorf("queue wait %s %t, want 1m0s true", w, ok)
	}
	jobs[1].StartTime = nil
	if _, ok := SuccessorQueueWait(jobs[0], jobs[1]); ok {
		t.Error("a queue wait was given with no start time")
	}
}

// mix is the traffic of an hour, with each ingredient adjustable at its boundary: three requests of near
// length gap apart, a burst of concurrent streams of 2 minutes at rate tokens a second, an idle stretch, three
// more requests like the first three, and requests of 5 minutes to the hour's end. One large request (a body of
// body bytes, 20 s long, at 2 tokens a second) opens largeAt after the burst opens: inside the burst's 2 minutes
// it is among the streams; a negative largeAt puts it before them, 2 minutes or more after it, after them.
type mix struct {
	near, gap time.Duration
	burst     int
	rate      float64
	idle      time.Duration
	body      int
	largeAt   time.Duration
}

func goodMix() mix {
	return mix{near: NearDeadline, gap: 0, burst: ConcurrentStreams, rate: StreamRate, idle: MinIdleGap, body: MinLargeBody, largeAt: 10 * time.Second}
}

func (m mix) requests(start time.Time) []Stream {
	var out []Stream
	at := start
	run := func(tag string) {
		for i := 0; i < 3; i++ {
			out = append(out, Stream{ID: fmt.Sprintf("%s%d", tag, i), Opened: at, Ended: at.Add(m.near), Complete: true})
			at = at.Add(m.near + m.gap)
		}
	}
	run("a")
	at = at.Add(MaxChainGap + time.Second) // the burst does not continue the run
	for i := 0; i < m.burst; i++ {
		out = append(out, Stream{ID: fmt.Sprintf("c%d", i), Opened: at, Ended: at.Add(2 * time.Minute), Complete: true, Tokens: int(m.rate * 120)})
	}
	// The large request: among the streams by default; the tests move it to the edges of their window.
	burstAt := at.Add(m.largeAt)
	out = append(out, Stream{ID: "u0", Opened: burstAt, Ended: burstAt.Add(20 * time.Second), Complete: true, Tokens: 40, BodyBytes: m.body,
		BodyStart: burstAt, BodyEnd: burstAt.Add(20 * time.Second)})
	at = at.Add(2*time.Minute + m.idle)
	run("b")
	// Requests of the deadline's length to the end of the hour, exactly.
	for end := start.Add(Hour); at.Before(end); {
		l := MaxRequestDuration
		if end.Sub(at) < l {
			l = end.Sub(at)
		}
		out = append(out, Stream{ID: "p", Opened: at, Ended: at.Add(l), Complete: true})
		at = at.Add(l)
	}
	return out
}

func samplesEvery(from, to time.Time, every time.Duration, session time.Time) []TunnelSample {
	var out []TunnelSample
	for t := from; !t.After(to); t = t.Add(every) {
		out = append(out, TunnelSample{At: t, LastStart: session, Connected: true})
	}
	return out
}

func goodTunnel() (TunnelHourObs, []provider.Job) {
	row := rowOf("tunnel-hour-athena")
	ready := at(20 * time.Minute)
	reqs := goodMix().requests(ready)
	o := TunnelHourObs{
		Tags:       []string{"hb-t-aaaaaaaaaaaa"},
		Requests:   reqs,
		Samples:    samplesEvery(ready, ready.Add(Hour), time.Minute, ready.Add(-10*time.Second)),
		DrainStart: ready.Add(Hour), // an hour to the second, the drain starting as it ends
		Rates: []PathRate{
			{Direction: DirUpload, OK: true, AskedSeconds: 30, TimedSeconds: 29, Bytes: 1, UnderLoad: true},
			{Direction: DirDownload, OK: true, AskedSeconds: 30, TimedSeconds: 29, Bytes: 1, Tokens: 1, UnderLoad: true},
		},
		Deployment: row.Plan(),
	}
	return o, leased(row, job(o.Tags[0], row.Partition, 0, 0, 85*time.Minute))
}

// rateOf is the observation's rate of a direction, to change in place; largeOf the large request of the good mix;
// streamsOpen the moment the good mix's 16 streams open.
func rateOf(o *TunnelHourObs, dir string) *PathRate {
	for i := range o.Rates {
		if o.Rates[i].Direction == dir {
			return &o.Rates[i]
		}
	}
	panic("no rate for " + dir)
}

func largeOf(o *TunnelHourObs) *Stream {
	for i := range o.Requests {
		if o.Requests[i].ID == "u0" {
			return &o.Requests[i]
		}
	}
	panic("no large request")
}

// moveLarge puts the large request, and its upload with it, between two times: the body takes the whole request.
func moveLarge(o *TunnelHourObs, opened, ended time.Time) {
	l := largeOf(o)
	l.Opened, l.Ended, l.BodyStart, l.BodyEnd = opened, ended, opened, ended
}

func streamsOpen(o *TunnelHourObs) time.Time {
	for _, r := range o.Requests {
		if r.ID == "c0" {
			return r.Opened
		}
	}
	panic("no stream")
}

func TestTunnelHourBoundaries(t *testing.T) {
	row := rowOf("tunnel-hour-athena")
	lastReq := func(o *TunnelHourObs) *Stream { return &o.Requests[len(o.Requests)-1] }
	cases := []struct {
		name   string
		mutate func(*TunnelHourObs, *[]provider.Job)
		want   string
	}{
		{"exactly an hour, requests of exactly the deadline, a look every minute, the drain start at the last ending", func(*TunnelHourObs, *[]provider.Job) {}, ""},
		{"a second short of the hour", func(o *TunnelHourObs, _ *[]provider.Job) { lastReq(o).Ended = lastReq(o).Ended.Add(-time.Second) }, "lasted less than an hour"},
		{"no request", func(o *TunnelHourObs, _ *[]provider.Job) { o.Requests = nil }, "no request was made"},
		{"a cut request", func(o *TunnelHourObs, _ *[]provider.Job) { o.Requests[3].CutBy = "tunnel" }, "cut during the hour"},
		{"a stream_cut answer", func(o *TunnelHourObs, _ *[]provider.Job) { o.Requests[3].Error = "stream_cut" }, "cut during the hour"},
		{"an unfinished request", func(o *TunnelHourObs, _ *[]provider.Job) { o.Requests[3].Complete = false }, "did not complete"},
		// The front cuts a request at the deployment's deadline: a stream observed longer than it was not behind that deadline.
		{"a request a second past the deployment's deadline", func(o *TunnelHourObs, _ *[]provider.Job) {
			o.Requests[len(o.Requests)-2].Ended = o.Requests[len(o.Requests)-2].Ended.Add(time.Second)
		}, "outlasted the deployment's request deadline"},
		{"the hour ends a second after the drain start", func(o *TunnelHourObs, _ *[]provider.Job) { o.DrainStart = o.DrainStart.Add(-time.Second) }, "ended after the Instance's drain start"},
		{"no drain start observed", func(o *TunnelHourObs, _ *[]provider.Job) { o.DrainStart = time.Time{} }, "drain start was not observed"},
		{"a request with no ending time", func(o *TunnelHourObs, _ *[]provider.Job) { o.Requests[1].Ended = time.Time{} }, "no opening or ending time"},
		// One session. A re-login of frpc mid-hour changes frps's lastStartTime for the proxy, and every request can still pass.
		{"frpc logged in again mid-hour and every request passed", func(o *TunnelHourObs, _ *[]provider.Job) {
			for i := 30; i < len(o.Samples); i++ {
				o.Samples[i].LastStart = o.Samples[30].At
			}
		}, "lastStartTime for the proxy changed"},
		{"a single look differs", func(o *TunnelHourObs, _ *[]provider.Job) {
			o.Samples[45].LastStart = o.Samples[45].LastStart.Add(time.Nanosecond)
		}, "lastStartTime for the proxy changed"},
		{"a look with no lastStartTime", func(o *TunnelHourObs, _ *[]provider.Job) { o.Samples[7].LastStart = time.Time{} }, "lacks its time or frps's lastStartTime"},
		{"TunnelConnected false in one look", func(o *TunnelHourObs, _ *[]provider.Job) { o.Samples[20].Connected = false }, "TunnelConnected was not True"},
		{"the first look a second after the first request", func(o *TunnelHourObs, _ *[]provider.Job) { o.Samples[0].At = o.Samples[0].At.Add(time.Second) }, "not looked at before the first request"},
		{"the last look a second before the last request ended", func(o *TunnelHourObs, _ *[]provider.Job) {
			o.Samples[len(o.Samples)-1].At = o.Samples[len(o.Samples)-1].At.Add(-time.Second)
		}, "not looked at after the last request"},
		{"a hole of exactly 2 minutes between looks", func(o *TunnelHourObs, _ *[]provider.Job) { o.Samples = append(o.Samples[:30], o.Samples[31:]...) }, ""},
		{"a hole of 3 minutes between looks", func(o *TunnelHourObs, _ *[]provider.Job) { o.Samples = append(o.Samples[:30], o.Samples[32:]...) }, "more than 2 minutes without a look"},
		{"one look only", func(o *TunnelHourObs, _ *[]provider.Job) { o.Samples = o.Samples[:1] }, "looked at fewer than twice"},
		{"no look", func(o *TunnelHourObs, _ *[]provider.Job) { o.Samples = nil }, "looked at fewer than twice"},
		// The path rate, one per direction (issue #224): each is held to S1's C6; the download also moves tokens.
		{"upload rate not ok", func(o *TunnelHourObs, _ *[]provider.Job) { rateOf(o, DirUpload).OK = false }, "the upload path rate was not measured"},
		{"download rate not ok", func(o *TunnelHourObs, _ *[]provider.Job) { rateOf(o, DirDownload).OK = false }, "the download path rate was not measured"},
		{"upload rate asked for 29.9 s", func(o *TunnelHourObs, _ *[]provider.Job) { rateOf(o, DirUpload).AskedSeconds = 29.9 }, "the upload path rate was asked for under 30 s"},
		{"download rate asked for 29.9 s", func(o *TunnelHourObs, _ *[]provider.Job) { rateOf(o, DirDownload).AskedSeconds = 29.9 }, "the download path rate was asked for under 30 s"},
		{"upload rate timed 28.9 s", func(o *TunnelHourObs, _ *[]provider.Job) { rateOf(o, DirUpload).TimedSeconds = 28.9 }, "the upload path rate was asked for under 30 s, timed under 29 s"},
		{"download rate timed 28.9 s", func(o *TunnelHourObs, _ *[]provider.Job) { rateOf(o, DirDownload).TimedSeconds = 28.9 }, "the download path rate was asked for under 30 s, timed under 29 s"},
		{"upload rate moved no bytes", func(o *TunnelHourObs, _ *[]provider.Job) { rateOf(o, DirUpload).Bytes = 0 }, "the upload path rate was asked for under 30 s, timed under 29 s, or moved no bytes"},
		{"download rate moved no bytes", func(o *TunnelHourObs, _ *[]provider.Job) { rateOf(o, DirDownload).Bytes = 0 }, "the download path rate was asked for under 30 s, timed under 29 s, or moved no bytes"},
		{"upload rate not under load", func(o *TunnelHourObs, _ *[]provider.Job) { rateOf(o, DirUpload).UnderLoad = false }, "the upload path rate was not measured under load"},
		{"download rate not under load", func(o *TunnelHourObs, _ *[]provider.Job) { rateOf(o, DirDownload).UnderLoad = false }, "the download path rate was not measured under load"},
		{"download rate in bytes only", func(o *TunnelHourObs, _ *[]provider.Job) { rateOf(o, DirDownload).Tokens = 0 }, "the download path rate moved no tokens"},
		// The upload carries request bodies, not tokens: none is asked of it.
		{"upload rate with tokens counted anyway", func(o *TunnelHourObs, _ *[]provider.Job) { rateOf(o, DirUpload).Tokens = 99 }, ""},
		{"only the download posted", func(o *TunnelHourObs, _ *[]provider.Job) { o.Rates = []PathRate{*rateOf(o, DirDownload)} }, "the upload path rate was not posted: both directions are needed"},
		{"only the upload posted", func(o *TunnelHourObs, _ *[]provider.Job) { o.Rates = []PathRate{*rateOf(o, DirUpload)} }, "the download path rate was not posted: both directions are needed"},
		{"no rate posted", func(o *TunnelHourObs, _ *[]provider.Job) { o.Rates = nil }, "the upload path rate was not posted"},
		{"two rates for one direction", func(o *TunnelHourObs, _ *[]provider.Job) { o.Rates[0].Direction = DirDownload }, "the path rates are not one per direction"},
		{"a third rate", func(o *TunnelHourObs, _ *[]provider.Job) {
			o.Rates = append(o.Rates, PathRate{Direction: DirUpload, OK: true})
		}, "the path rates are not one per direction"},
		{"the old up", func(o *TunnelHourObs, _ *[]provider.Job) { o.Rates[1].Direction = "up" }, "neither upload nor download"},
		// The upload exercised (issue #224): a body of 0.1 MB (100,000 bytes) or more, sent while the 16 streams are open.
		{"the largest body one byte under 0.1 MB", func(o *TunnelHourObs, _ *[]provider.Job) { largeOf(o).BodyBytes = MinLargeBody - 1 }, "no request body reached 0.1 MB"},
		{"no body size recorded at all", func(o *TunnelHourObs, _ *[]provider.Job) { largeOf(o).BodyBytes = 0 }, "no request body reached 0.1 MB"},
		{"a body of exactly 0.1 MB", func(o *TunnelHourObs, _ *[]provider.Job) { largeOf(o).BodyBytes = MinLargeBody }, ""},
		{"a body of 0.4 MB is no fault", func(o *TunnelHourObs, _ *[]provider.Job) { largeOf(o).BodyBytes = 400_000 }, ""},
		{"a body of 5 MB is no fault", func(o *TunnelHourObs, _ *[]provider.Job) { largeOf(o).BodyBytes = 5_000_000 }, ""},
		{"the large request opens as the streams open", func(o *TunnelHourObs, _ *[]provider.Job) {
			moveLarge(o, streamsOpen(o), streamsOpen(o).Add(20*time.Second))
		}, ""},
		{"the large request opens a nanosecond before the streams end", func(o *TunnelHourObs, _ *[]provider.Job) {
			// It ends with the streams, so the idle stretch after them keeps its 2 minutes.
			moveLarge(o, streamsOpen(o).Add(2*time.Minute-time.Nanosecond), streamsOpen(o).Add(2*time.Minute))
		}, ""},
		{"the large request opens as the streams end", func(o *TunnelHourObs, _ *[]provider.Job) {
			moveLarge(o, streamsOpen(o).Add(2*time.Minute), streamsOpen(o).Add(2*time.Minute+20*time.Second))
		}, "no request of 0.1 MB or more was sent while the 16 streams were open"},
		{"the large request ends as the streams open", func(o *TunnelHourObs, _ *[]provider.Job) {
			moveLarge(o, streamsOpen(o).Add(-20*time.Second), streamsOpen(o))
		}, "no request of 0.1 MB or more was sent while the 16 streams were open"},
		// The upload interval, not the request's own (PR 244, B2). A body can be sent before the streams open and its
		// response stay open across their start: the request is open among the streams, the upload was not.
		{"the body is sent before the streams open and the response stays open across their start", func(o *TunnelHourObs, _ *[]provider.Job) {
			l := largeOf(o)
			l.Opened, l.BodyStart = streamsOpen(o).Add(-30*time.Second), streamsOpen(o).Add(-30*time.Second)
			l.BodyEnd, l.Ended = streamsOpen(o).Add(-10*time.Second), streamsOpen(o).Add(30*time.Second)
		}, "no request of 0.1 MB or more was sent while the 16 streams were open"},
		{"the body ends as the streams open, the response goes on", func(o *TunnelHourObs, _ *[]provider.Job) {
			l := largeOf(o)
			l.Opened, l.BodyStart = streamsOpen(o).Add(-30*time.Second), streamsOpen(o).Add(-30*time.Second)
			l.BodyEnd, l.Ended = streamsOpen(o), streamsOpen(o).Add(30*time.Second)
		}, "no request of 0.1 MB or more was sent while the 16 streams were open"},
		{"the body is still being sent as the streams open", func(o *TunnelHourObs, _ *[]provider.Job) {
			l := largeOf(o)
			l.Opened, l.BodyStart = streamsOpen(o).Add(-30*time.Second), streamsOpen(o).Add(-30*time.Second)
			l.BodyEnd, l.Ended = streamsOpen(o).Add(time.Nanosecond), streamsOpen(o).Add(30*time.Second)
		}, ""},
		{"the body is sent after the streams opened and the request was opened before them", func(o *TunnelHourObs, _ *[]provider.Job) {
			l := largeOf(o)
			l.Opened = streamsOpen(o).Add(-30 * time.Second)
			l.BodyStart, l.BodyEnd, l.Ended = streamsOpen(o).Add(10*time.Second), streamsOpen(o).Add(20*time.Second), streamsOpen(o).Add(40*time.Second)
		}, ""},
		{"no upload interval recorded", func(o *TunnelHourObs, _ *[]provider.Job) {
			largeOf(o).BodyStart, largeOf(o).BodyEnd = time.Time{}, time.Time{}
		}, "no request of 0.1 MB or more was sent while the 16 streams were open"},
		{"no upload start recorded", func(o *TunnelHourObs, _ *[]provider.Job) { largeOf(o).BodyStart = time.Time{} }, "no request of 0.1 MB or more was sent while the 16 streams were open"},
		{"no upload end recorded", func(o *TunnelHourObs, _ *[]provider.Job) { largeOf(o).BodyEnd = time.Time{} }, "no request of 0.1 MB or more was sent while the 16 streams were open"},
		{"an upload that ends before it starts", func(o *TunnelHourObs, _ *[]provider.Job) {
			largeOf(o).BodyStart, largeOf(o).BodyEnd = largeOf(o).BodyEnd, largeOf(o).BodyStart
		}, "no request of 0.1 MB or more was sent while the 16 streams were open"},
		{"15 streams while the large request is open", func(o *TunnelHourObs, _ *[]provider.Job) {
			// One stream too few at every moment the large request is open: its rate is under 20 tokens a second.
			for i := range o.Requests {
				if o.Requests[i].ID == "c0" {
					o.Requests[i].Tokens = 100
				}
			}
		}, "the hour held no 16 concurrent streams"},
		{"the cluster shows 59 minutes", func(o *TunnelHourObs, j *[]provider.Job) { (*j)[0].EndTime = ptr(at(59 * time.Minute)) }, "ran under an hour"},
		{"the cluster shows exactly an hour", func(o *TunnelHourObs, j *[]provider.Job) { (*j)[0].EndTime = ptr(at(Hour)) }, ""},
		// Evidence that is absent is not evidence (the scheduler's times corroborate the requests').
		{"no start time in the cluster's record", func(o *TunnelHourObs, j *[]provider.Job) { (*j)[0].StartTime = nil }, "lacks the job's start or end time"},
		{"no end time in the cluster's record", func(o *TunnelHourObs, j *[]provider.Job) { (*j)[0].EndTime = nil }, "lacks the job's start or end time"},
		{"no tag", func(o *TunnelHourObs, _ *[]provider.Job) { o.Tags = nil }, "exactly one Allocation"},
		{"another partition", func(o *TunnelHourObs, j *[]provider.Job) { (*j)[0].Partition = "plgrid" }, "another partition"},
		{"two tunnel jobs", func(o *TunnelHourObs, j *[]provider.Job) {
			o.Tags = append(o.Tags, "hb-t-bbbbbbbbbbbb")
			*j = append(*j, leased(row, job("hb-t-bbbbbbbbbbbb", row.Partition, 86*time.Minute, 86*time.Minute, 90*time.Minute))...)
		}, "exactly one Allocation"},
		{"two tunnel jobs in the cluster's record under one recorded tag", func(o *TunnelHourObs, j *[]provider.Job) {
			dup := job(o.Tags[0], row.Partition, 86*time.Minute, 86*time.Minute, 90*time.Minute)
			dup.ID, dup.TimeLimit = "id-held-duplicate", row.Lease
			*j = append(*j, dup)
		}, "a tag names more than one job"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			o, jobs := goodTunnel()
			c.mutate(&o, &jobs)
			wantFailure(t, VerifyTunnelHour(row, o, jobs), c.want)
		})
	}
}

// The traffic of the hour, each ingredient at its boundary: requests back to back (a gap of 10 s is still back to
// back, 11 s is not), one of about 4 minutes (240 s, no longer than the 300 s deadline), 16 concurrent streams
// at 20 tokens a second (15 or 19.9 do not), and an idle stretch of 2 minutes (119 s is not one).
func TestTheTrafficOfTheHourAtItsBoundaries(t *testing.T) {
	cases := []struct {
		name string
		m    func(*mix)
		want string
	}{
		{"as it should be", func(*mix) {}, ""},
		{"a gap of 10 s between requests", func(m *mix) { m.gap = 10 * time.Second }, ""},
		{"a gap of 11 s between requests", func(m *mix) { m.gap = 11 * time.Second }, "no run of requests back to back"},
		{"requests of 239 s", func(m *mix) { m.near = 239 * time.Second }, "no request of about 4 minutes"},
		{"requests of 300 s", func(m *mix) { m.near = 300 * time.Second }, ""},
		{"15 concurrent streams", func(m *mix) { m.burst = 15 }, "no 16 concurrent streams"},
		{"streams at 19.9 tokens a second", func(m *mix) { m.rate = 19.9 }, "no 16 concurrent streams"},
		{"an idle stretch of 119 s", func(m *mix) { m.idle = 119 * time.Second }, "no idle stretch"},
		{"an idle stretch of exactly 2 minutes", func(m *mix) { m.idle = 2 * time.Minute }, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := goodMix()
			c.m(&m)
			// Without the requests of the deadline's length that pad the hour: they would be a run back to back, and near the deadline, themselves.
			var reqs []Stream
			for _, r := range m.requests(at(20 * time.Minute)) {
				if r.ID != "p" {
					reqs = append(reqs, r)
				}
			}
			wantFailure(t, trafficChecks(reqs), c.want)
		})
	}
	// The burst's streams, side by side, are not a run back to back: without the chains there is none.
	burstOnly := goodMix().requests(at(0))[3 : 3+ConcurrentStreams]
	// And the runs of the good mix are exactly MinChain long, so a run of one fewer is not enough.
	if n := longestChain(goodMix().requests(at(0))[:3]); n != MinChain {
		t.Errorf("the good mix's first run is %d requests, want %d", n, MinChain)
	}
	if n := longestChain(burstOnly); n != 1 {
		t.Errorf("a run of %d among concurrent streams, want 1", n)
	}
	// And the shortest idle stretch is the longest gap, not the first.
	if g := longestIdle(goodMix().requests(at(0))); g != MinIdleGap {
		t.Errorf("longest idle stretch %s, want %s", g, MinIdleGap)
	}
}

// The most jobs in the queue at once, with a job counting from its submission to its end.
func TestPeakQueuedCountsOverlapAtTheBoundary(t *testing.T) {
	mk := func(submit, end time.Duration) provider.Job {
		return provider.Job{ID: "j", SubmitTime: at(submit), EndTime: ptr(at(end))}
	}
	cases := []struct {
		name string
		jobs []provider.Job
		want int
	}{
		{"none", nil, 0},
		{"one", []provider.Job{mk(0, 10)}, 1},
		{"one ends as the next is submitted", []provider.Job{mk(0, 10), mk(10, 20)}, 1},
		{"one ends a minute after the next is submitted", []provider.Job{mk(0, 10), mk(9, 20)}, 2},
		{"two overlap", []provider.Job{mk(0, 10), mk(5, 15)}, 2},
		{"three overlap", []provider.Job{mk(0, 20), mk(5, 15), mk(6, 7)}, 3},
		{"a job with no end is still there", []provider.Job{mk(0, 10), {SubmitTime: at(20)}, {SubmitTime: at(30)}}, 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// durations in the table are minutes
			scale := func(js []provider.Job) []provider.Job {
				out := make([]provider.Job, len(js))
				for i, j := range js {
					j.SubmitTime = t0.Add(j.SubmitTime.Sub(t0) * time.Minute)
					if j.EndTime != nil {
						j.EndTime = ptr(t0.Add(j.EndTime.Sub(t0) * time.Minute))
					}
					out[i] = j
				}
				return out
			}
			if got, ok := peakQueued(scale(c.jobs)); got != c.want || !ok {
				t.Errorf("peak %d complete %t, want %d true", got, ok, c.want)
			}
		})
	}
	if _, ok := peakQueued([]provider.Job{{ID: "x"}}); ok {
		t.Error("a job with no submission time was counted as placed")
	}
}

func TestASlowTunnelIsFlaggedNotFailed(t *testing.T) {
	row, _ := RowByName("tunnel-hour-athena")
	o, jobs := goodTunnel()
	// 600,000 B/s is under S1's 629,146 B/s flag line and passes the verdict.
	*rateOf(&o, DirDownload) = PathRate{Direction: DirDownload, OK: true, AskedSeconds: 30, TimedSeconds: 30, Bytes: 18_000_000, Tokens: 9600, UnderLoad: true}
	wantFailure(t, VerifyTunnelHour(row, o, jobs), "")
	if f := TunnelHourFlags(o); len(f) != 1 || !strings.Contains(f[0], "600000 B/s") {
		t.Errorf("flags %v, want one naming 600000 B/s", f)
	}
	rateOf(&o, DirDownload).Bytes = 19_000_000 // 633,333 B/s
	if f := TunnelHourFlags(o); len(f) != 0 {
		t.Errorf("flags %v at 633333 B/s, want none", f)
	}
	// S1's flag is on the token direction: a slow upload is posted, not flagged.
	rateOf(&o, DirUpload).Bytes = 18_000_000
	if f := TunnelHourFlags(o); len(f) != 0 {
		t.Errorf("flags %v for a slow upload, want none", f)
	}
}

func TestAccountingBoundaries(t *testing.T) {
	row, _ := RowByName("tunnel-hour-athena") // any row: the check reads its partition
	two := int32(2)
	one := int32(1)
	good := func() AccountingObs {
		return AccountingObs{Partition: row.Partition, RecordGPUs: &one, Basis: "allocated", RequestedGPUs: 1, NodeGPUs: 1}
	}
	cases := []struct {
		name   string
		mutate func(*AccountingObs)
		want   string
	}{
		{"as it should be", func(*AccountingObs) {}, ""},
		// nvidia-smi lists the whole node where devices are not confined to the job; it is posted, not judged.
		{"nvidia-smi listing the whole node", func(o *AccountingObs) { o.SmiGPUs = 8 }, ""},
		{"the record says 1, the node showed 2", func(o *AccountingObs) { o.NodeGPUs = 2; o.RequestedGPUs = 2 }, "differs from the GPUs the job saw"},
		{"the record says 2, the node showed 1", func(o *AccountingObs) { o.RecordGPUs = &two }, "differs from the GPUs the job saw"},
		{"the node showed 2 and the job asked for 1", func(o *AccountingObs) { o.NodeGPUs = 2; o.RecordGPUs = &two }, "differ from what it asked for"},
		{"an unknown GPU entry", func(o *AccountingObs) { o.RecordGPUs = nil }, "marks its GPU entry unknown"},
		// 0 = 0 would agree: a CPU job is not what is asked for.
		{"no GPU at all", func(o *AccountingObs) { z := int32(0); o.RecordGPUs, o.RequestedGPUs, o.NodeGPUs = &z, 0, 0 }, "asked for or saw no GPU"},
		{"a billed basis", func(o *AccountingObs) { o.Basis = "billed" }, "basis is not allocated"},
		{"another partition", func(o *AccountingObs) { o.Partition = "plgrid" }, "another partition"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			o := good()
			c.mutate(&o)
			wantFailure(t, VerifyAccounting(row, o), c.want)
		})
	}
}
