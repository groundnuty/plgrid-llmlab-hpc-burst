package alpha

// A row's clock (issue #245). The live rows keep the constants of rows.go,
// written out here by hand from the runbook and the science agent's rulings;
// a scaled row, as a stand-in run of the live steps uses, is judged on its own
// clock and on no other.

import (
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// liveTimingByHand is the live clock as docs/alpha-runbook.md and rows.go's
// rulings state it.
var liveTimingByHand = Timing{
	MaxRequestDuration: 5 * time.Minute, DrainMargin: 10 * time.Minute, HandoverMargin: 30 * time.Minute, StartTimeout: time.Hour,
	IdleLimit: 2 * time.Minute, MaxIdleLimit: 10 * time.Minute,
	TunnelHour: 61 * time.Minute, Hour: time.Hour,
	MinChain: 3, MaxChainGap: 10 * time.Second, NearDeadline: 4 * time.Minute,
	ConcurrentStreams: 16, StreamRate: 20, MinIdleGap: 2 * time.Minute, MaxSampleGap: 2 * time.Minute,
	// The large requests (issue #224; the ruling (4) on PR 255): 0.4 MB prompts from two senders, half a second apart.
	UploadBody: 400_000, UploadStreams: 2, UploadPause: 500 * time.Millisecond,
	// HBEP-5's long stream on plgrid-now (issue #376, Q4): 7,000 tokens.
	ServeTokens: 7000,
}

// Every live row keeps the zero Timing, but the rows under profiles/athena-now.yaml (issue #376), whose clock is that
// profile's margins (TestTheNowRowsClockIsTheShippedProfiles reads the file).
func TestLiveRowsKeepTheLiveTiming(t *testing.T) {
	if LiveTiming() != liveTimingByHand {
		t.Errorf("LiveTiming() = %+v, want %+v", LiveTiming(), liveTimingByHand)
	}
	all := slices.Concat(Rows(), AccountingRows(), ReachRows(), HostDialedRows())
	for _, r := range all {
		if r.Profile == "athena-now" {
			if r.Timing != AthenaNowTiming() || r.Clock().Lead() != 20*time.Minute {
				t.Errorf("row %s runs under athena-now.yaml with the Timing %+v and a lead of %s, want the profile's margins and 20 min", r.Name, r.Timing, r.Clock().Lead())
			}
			continue
		}
		if r.Profile != "" {
			t.Errorf("row %s names the profile %s, whose clock no test holds", r.Name, r.Profile)
		}
		if r.Timing != (Timing{}) {
			t.Errorf("row %s has a Timing of its own, %+v: a live row keeps the zero Timing", r.Name, r.Timing)
		}
		if r.Clock() != liveTimingByHand {
			t.Errorf("row %s's clock is %+v, want the live one", r.Name, r.Clock())
		}
		if r.Clock().Lead() != 105*time.Minute {
			t.Errorf("row %s's lead is %s, want 1 h 45 min (profiles/athena.yaml:39)", r.Name, r.Clock().Lead())
		}
	}
}

// TestClockTakesEachFieldItsOwn sets one field at a time: that field is the
// row's, and every other is the live one.
func TestClockTakesEachFieldItsOwn(t *testing.T) {
	typ := reflect.TypeOf(Timing{})
	for i := 0; i < typ.NumField(); i++ {
		var own Timing
		f := reflect.ValueOf(&own).Elem().Field(i)
		switch f.Kind() {
		case reflect.Int64:
			f.SetInt(int64(7 * time.Second))
		case reflect.Int:
			f.SetInt(7)
		case reflect.Float64:
			f.SetFloat(7)
		default:
			t.Fatalf("field %s has kind %s, which this test does not set", typ.Field(i).Name, f.Kind())
		}
		got := Row{Timing: own}.Clock()
		want := liveTimingByHand
		reflect.ValueOf(&want).Elem().Field(i).Set(f)
		if got != want {
			t.Errorf("with only %s set, Clock() = %+v, want %+v", typ.Field(i).Name, got, want)
		}
	}
}

// scaledTunnel is a tunnel row on a scaled clock: a 4-minute hour, a
// 60-second deadline, a lead of 6 minutes and an 8-minute lease.
func scaledTunnel() Row {
	r, _ := RowByName("tunnel-hour-helios")
	r.Timing = Timing{MaxRequestDuration: time.Minute, DrainMargin: 30 * time.Second, HandoverMargin: 30 * time.Second, StartTimeout: 4 * time.Minute,
		TunnelHour: 4*time.Minute + 4*time.Second, Hour: 4 * time.Minute, NearDeadline: 48 * time.Second,
		MinIdleGap: 30 * time.Second, MaxSampleGap: 30 * time.Second, IdleLimit: 10 * time.Second, MaxIdleLimit: time.Minute}
	r.Lease = 8 * time.Minute
	return r
}

// scaledHour is a passing hour on scaledTunnel's clock, worked out by hand: three
// requests of 50 s back to back (0-152 s), 16 streams of 20 s at 25 tokens/s
// (152-172 s) with a 0.2 MB body sent among them (155-157 s), 40 s idle, one
// more request of 50 s (212-262 s); a look every 10 s; both path rates.
func scaledHour(r Row) (TunnelHourObs, []provider.Job) {
	t0 := time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC)
	s := func(d time.Duration) time.Time { return t0.Add(d) }
	var reqs []Stream
	for i, from := range []time.Duration{0, 51 * time.Second, 102 * time.Second} {
		reqs = append(reqs, Stream{ID: "a" + string(rune('0'+i)), Opened: s(from), Ended: s(from + 50*time.Second), Complete: true})
	}
	for i := 0; i < 16; i++ {
		reqs = append(reqs, Stream{ID: "c" + string(rune('a'+i)), Opened: s(152 * time.Second), Ended: s(172 * time.Second), Complete: true, Tokens: 500})
	}
	// A large request (issue #224), its body sent while the 16 streams were open.
	reqs = append(reqs, Stream{ID: "l0", Opened: s(155 * time.Second), Ended: s(160 * time.Second), Complete: true,
		BodyBytes: 200_000, BodyStart: s(155 * time.Second), BodyEnd: s(157 * time.Second)})
	reqs = append(reqs, Stream{ID: "b0", Opened: s(212 * time.Second), Ended: s(262 * time.Second), Complete: true})
	var samples []TunnelSample
	for at := time.Duration(0); at <= 272*time.Second; at += 10 * time.Second {
		samples = append(samples, TunnelSample{At: s(at), LastStart: s(-30 * time.Second), Connected: true})
	}
	tag := "hb-tunnel-0123456789"
	o := TunnelHourObs{Tags: []string{tag}, Requests: reqs, Samples: samples, DrainStart: s(10 * time.Minute), Deployment: r.Plan(),
		Rates: []PathRate{{Direction: DirUpload, OK: true, AskedSeconds: 30, TimedSeconds: 30.1, Bytes: 24 << 20, UnderLoad: true},
			{Direction: DirDownload, OK: true, AskedSeconds: 30, TimedSeconds: 30.2, Bytes: 31 << 20, Tokens: 9600, UnderLoad: true}}}
	start, end := s(-2*time.Minute), s(5*time.Minute)
	jobs := []provider.Job{{ID: "101", Name: tag, Partition: r.Partition, TimeLimit: r.Lease, SubmitTime: s(-3 * time.Minute), StartTime: &start, EndTime: &end}}
	return o, jobs
}

func TestAScaledRowIsJudgedOnItsClock(t *testing.T) {
	r := scaledTunnel()
	o, jobs := scaledHour(r)
	if f := VerifyTunnelHour(r, o, jobs); len(f) > 0 {
		t.Fatalf("a passing hour on the row's own clock failed: %v", f)
	}
	// The same run judged on the live clock fails: its hour is 4 minutes, its lease under the live lead.
	live := r
	live.Timing = Timing{}
	f := VerifyTunnelHour(live, o, jobs)
	for _, want := range []string{
		"the deployment's leaseLimit is not longer than the lead an Instance needs (PartitionTooShort)",
		"the requests lasted less than an hour",
		"the cluster's record shows the job ran under an hour",
		"the hour held no request of about 4 minutes, near the deadline",
		"the hour held no idle stretch of 2 minutes with heartbeats only",
	} {
		if !slices.Contains(f, want) {
			t.Errorf("judged on the live clock, the scaled run lacks the failure %q (got %v)", want, f)
		}
	}
	// What the scaled clock forbids, it fails, even where the live clock would not.
	slow := o
	slow.Samples = append([]TunnelSample(nil), o.Samples[:5]...)
	slow.Samples = append(slow.Samples, o.Samples[10:]...) // a 60 s gap: over the scaled 30 s, under the live 2 min
	if f := VerifyTunnelHour(r, slow, jobs); !slices.Contains(f, "the tunnel session went more than 2 minutes without a look") {
		t.Errorf("a 60 s gap between looks on a 30 s clock passed: %v", f)
	}
	long := o
	long.Requests = append(append([]Stream(nil), o.Requests...), Stream{ID: "long", Opened: o.Requests[0].Opened, Ended: o.Requests[0].Opened.Add(70 * time.Second), Complete: true})
	if f := VerifyTunnelHour(r, long, jobs); !slices.Contains(f, "a request outlasted the deployment's request deadline") {
		t.Errorf("a 70 s request on a 60 s deadline passed: %v", f)
	}
}

func TestAScaledNoOverlapPlanKeepsItsIdleLimit(t *testing.T) {
	r, _ := RowByName("alpha-helios-nooverlap")
	r.Timing = Timing{IdleLimit: 10 * time.Second, MaxRequestDuration: time.Minute}
	p := r.Plan()
	if !p.ReleaseWhenIdle || p.IdleLimit != 10*time.Second || p.MaxRequestDuration != time.Minute {
		t.Errorf("the scaled no-overlap plan is %+v, want ReleaseWhenIdle with the row's 10 s idleLimit and 1 min deadline", p)
	}
}
