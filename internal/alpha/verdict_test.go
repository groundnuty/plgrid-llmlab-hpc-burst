package alpha

// The verdicts on observations built by hand, at the boundaries the criteria
// name. Expected values come from the criteria's text (HBEP-5, -6, -7, -8 and
// S1's verdict), never from running the code under test on the same input.

import (
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

// lane is a chain of requests of 300 s, the deployment's deadline, one straight after the other from start.
func lane(id string, start time.Duration, count int) Lane {
	l := Lane{ID: id}
	for i := 0; i < count; i++ {
		o := start + time.Duration(i)*MaxRequestDuration
		l.Requests = append(l.Requests, Stream{ID: id, Opened: at(o), Ended: at(o + MaxRequestDuration), Complete: true})
	}
	return l
}

func goodTunnel() (TunnelHourObs, []provider.Job) {
	row := rowOf("tunnel-hour-athena")
	// Two lanes of 12 requests of exactly 5 minutes from minute 20: an hour to the second, ending at minute 80.
	o := TunnelHourObs{
		Tags:       []string{"hb-t-aaaaaaaaaaaa"},
		Lanes:      []Lane{lane("l1", 20*time.Minute, 12), lane("l2", 20*time.Minute, 12)},
		DrainStart: at(80 * time.Minute),
		Rate:       PathRate{Direction: "up", OK: true, AskedSeconds: 30, TimedSeconds: 29, Bytes: 1}, Deployment: row.Plan(),
	}
	return o, leased(row, job(o.Tags[0], row.Partition, 0, 0, 85*time.Minute))
}

func TestTunnelHourBoundaries(t *testing.T) {
	row := rowOf("tunnel-hour-athena")
	last := func(o *TunnelHourObs) *Stream { r := o.Lanes[0].Requests; return &r[len(r)-1] }
	cases := []struct {
		name   string
		mutate func(*TunnelHourObs, *[]provider.Job)
		want   string
	}{
		{"exactly an hour, requests of exactly the deadline, the drain start at the last ending", func(*TunnelHourObs, *[]provider.Job) {}, ""},
		{"a second short of the hour", func(o *TunnelHourObs, _ *[]provider.Job) { last(o).Ended = last(o).Ended.Add(-time.Second) }, "lasted less than an hour"},
		{"no lanes", func(o *TunnelHourObs, _ *[]provider.Job) { o.Lanes = nil }, "no lane was held"},
		{"a lane with no request", func(o *TunnelHourObs, _ *[]provider.Job) { o.Lanes[1].Requests = nil }, "a lane carried no request"},
		{"a cut request", func(o *TunnelHourObs, _ *[]provider.Job) { o.Lanes[1].Requests[3].CutBy = "tunnel" }, "cut during the hour"},
		{"an unfinished request", func(o *TunnelHourObs, _ *[]provider.Job) { o.Lanes[1].Requests[3].Complete = false }, "did not complete"},
		// The front cuts a request at the deployment's deadline: a stream observed longer than it was not behind that deadline.
		{"a request a second past the deployment's deadline", func(o *TunnelHourObs, _ *[]provider.Job) {
			o.Lanes[0].Requests[2].Ended = o.Lanes[0].Requests[2].Ended.Add(time.Second)
		}, "outlasted the deployment's request deadline"},
		{"one stream of an hour", func(o *TunnelHourObs, _ *[]provider.Job) {
			o.Lanes[0].Requests = []Stream{{ID: "long", Opened: at(20 * time.Minute), Ended: at(80 * time.Minute), Complete: true}}
		}, "outlasted the deployment's request deadline"},
		{"a pause of exactly 10 s", func(o *TunnelHourObs, _ *[]provider.Job) {
			shift(&o.Lanes[0], 5, MaxLaneGap)
			o.DrainStart = o.DrainStart.Add(MaxLaneGap)
		}, ""},
		{"a pause of 11 s", func(o *TunnelHourObs, _ *[]provider.Job) {
			shift(&o.Lanes[0], 5, MaxLaneGap+time.Second)
			o.DrainStart = o.DrainStart.Add(time.Minute)
		}, "paused for more than 10 s"},
		{"the hour ends a second after the drain start", func(o *TunnelHourObs, _ *[]provider.Job) { o.DrainStart = o.DrainStart.Add(-time.Second) }, "ended after the Instance's drain start"},
		{"no drain start observed", func(o *TunnelHourObs, _ *[]provider.Job) { o.DrainStart = time.Time{} }, "drain start was not observed"},
		{"a request with no ending time", func(o *TunnelHourObs, _ *[]provider.Job) { o.Lanes[0].Requests[1].Ended = time.Time{} }, "no opening or ending time"},
		{"one tunnel drop", func(o *TunnelHourObs, _ *[]provider.Job) { o.TunnelDrops = 1 }, "dropped during the hour"},
		{"rate not ok", func(o *TunnelHourObs, _ *[]provider.Job) { o.Rate.OK = false }, "path rate was not measured"},
		{"rate asked for 29.9 s", func(o *TunnelHourObs, _ *[]provider.Job) { o.Rate.AskedSeconds = 29.9 }, "asked for under 30 s"},
		{"rate timed 28.9 s", func(o *TunnelHourObs, _ *[]provider.Job) { o.Rate.TimedSeconds = 28.9 }, "timed under 29 s"},
		{"rate moved no bytes", func(o *TunnelHourObs, _ *[]provider.Job) { o.Rate.Bytes = 0 }, "moved no bytes"},
		{"rate in the other direction", func(o *TunnelHourObs, _ *[]provider.Job) { o.Rate.Direction = "down" }, "not in the token direction"},
		{"the cluster shows 59 minutes", func(o *TunnelHourObs, j *[]provider.Job) { (*j)[0].EndTime = ptr(at(59 * time.Minute)) }, "ran under an hour"},
		{"the cluster shows exactly an hour", func(o *TunnelHourObs, j *[]provider.Job) { (*j)[0].EndTime = ptr(at(Hour)) }, ""},
		// Evidence that is absent is not evidence (the scheduler's times corroborate the lanes').
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

// shift moves every request of the lane from index i on later by d.
func shift(l *Lane, i int, d time.Duration) {
	for k := i; k < len(l.Requests); k++ {
		l.Requests[k].Opened, l.Requests[k].Ended = l.Requests[k].Opened.Add(d), l.Requests[k].Ended.Add(d)
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
	o.Rate = PathRate{Direction: "up", OK: true, AskedSeconds: 30, TimedSeconds: 30, Bytes: 18_000_000}
	wantFailure(t, VerifyTunnelHour(row, o, jobs), "")
	if f := TunnelHourFlags(o); len(f) != 1 || !strings.Contains(f[0], "600000 B/s") {
		t.Errorf("flags %v, want one naming 600000 B/s", f)
	}
	o.Rate.Bytes = 19_000_000 // 633,333 B/s
	if f := TunnelHourFlags(o); len(f) != 0 {
		t.Errorf("flags %v at 633333 B/s, want none", f)
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
