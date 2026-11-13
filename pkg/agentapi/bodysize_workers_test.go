package agentapi

// A report of several workers' checks fits the 4 MiB body limit (HBEP-7, Several workers: each worker is
// checked for ceil(targetSessions / N) sessions, so the report stays within its bound at any N). Each of
// the N workers carries the worst warm-up at its share of 1024 sessions, five rounds of 4 + 1 + share
// timings of 526 bytes, and restOfReport's worker and checks besides: 200 files, the longest generation
// config, the longest log tail, messages and health. The Instance's own checks are absent, as with
// several workers (Q6).

import (
	"strings"
	"testing"
)

// workersReport is the worst report of n workers.
func workersReport(n int) Report {
	base := restOfReport(nil)
	share := int(MaxWorkerTargetSessions(n))
	r := base
	in := *base.Instance
	in.Checks = nil
	in.Workers = nil
	for i := 0; i < n; i++ {
		w := base.Instance.Workers[0]
		w.Name = WorkerName(i)
		w.EngineSocket = "/tmp/hb-run/" + w.Name + ".sock"
		c := *base.Instance.Checks
		warm := &WarmUpResult{Done: true}
		for round := 1; round <= MaxWarmUpRounds; round++ {
			wr := WarmUpRound{Round: int32(round)}
			for range 4 + 1 + share {
				wr.Requests = append(wr.Requests, worstTiming())
			}
			warm.Rounds = append(warm.Rounds, wr)
		}
		c.WarmUp = warm
		w.Checks = &c
		in.Workers = append(in.Workers, w)
	}
	r.Instance = &in
	return r
}

func TestSeveralWorkersReportsFitTheBodyLimit(t *testing.T) {
	largest, at := 0, 0
	for n := 2; n <= 8; n++ {
		body, err := EncodeBody(workersReport(n))
		if err != nil {
			t.Errorf("%d workers: the worst report is refused: %v", n, err)
			continue
		}
		if len(body) > MaxBodyBytes {
			t.Errorf("%d workers: the worst report is %d bytes, above the %d limit", n, len(body), MaxBodyBytes)
		}
		if len(body) > largest {
			largest, at = len(body), n
		}
		// Each worker's checks are in its report, all of them.
		var got Report
		if err := DecodeBody(strings.NewReader(string(body)), &got); err != nil || len(got.Instance.Workers) != n || got.Instance.Workers[n-1].Checks == nil ||
			len(got.Instance.Workers[n-1].Checks.WarmUp.Rounds) != MaxWarmUpRounds {
			t.Errorf("%d workers: the report did not come back whole: %v", n, err)
		}
	}
	t.Logf("the largest worst report is %d bytes, at %d workers, of the %d limit", largest, at, MaxBodyBytes)
}

// Each worker's share of MaxTargetSessions, ceil(1024 / n), worked out by hand.
func TestMaxWorkerTargetSessions(t *testing.T) {
	for n, want := range map[int]int32{0: 1024, 1: 1024, 2: 512, 3: 342, 7: 147, 8: 128} {
		if got := MaxWorkerTargetSessions(n); got != want {
			t.Errorf("%d workers: %d, want %d", n, got, want)
		}
	}
}
