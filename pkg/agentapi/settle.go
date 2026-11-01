package agentapi

import "time"

// The warm-up settling rule (HBEP-7, Instance start: warm-up; science-agent
// ruling, 1 Nov 2026, issue #125). The rule lives here, beside Shape and
// WarmUpRound, so the lifecycle controller that judges warm-up and the
// real-engine driver that reports on it apply one function. Moved unchanged
// from the controller's judge.go and settings.go (issue #151).
const (
	// WarmUpTolerance is the share of the round before that a fixed-length
	// shape's mean Total may rise by and still count as settled.
	WarmUpTolerance = 0.10
	// WarmUpFloor is the least a shape's mean Total may rise by and still
	// count as settled: it keeps one request's noise on a short shape from
	// blocking settling (in #94's run, 0.208 s to 0.253 s between two
	// rounds).
	WarmUpFloor = 100 * time.Millisecond
)

// WarmUpSettled reports whether cur settles warm-up after prev, the round
// before it: no fixed-length shape (Shape.FixedLength) has a mean Total that
// rose by more than max(WarmUpTolerance of prev's, WarmUpFloor), and cur
// holds the same shapes as prev. A shape whose length varies is not judged. It
// judges the requests it is given: a round with a failed request fails
// warm-up before settling is asked (the controller's judgeWarmUp), so a caller
// that wants such a round to count as not settled checks for it first.
func WarmUpSettled(prev, cur WarmUpRound) bool {
	a, b := meanTotals(prev), meanTotals(cur)
	if len(b) == 0 || len(a) != len(b) {
		return false
	}
	for s, t := range b {
		p, ok := a[s]
		if !ok || t-p > settleLimit(p) {
			return false
		}
	}
	return true
}

// meanTotals is the mean Total of each fixed-length shape's requests in r;
// a shape whose length varies is left out.
func meanTotals(r WarmUpRound) map[Shape]time.Duration {
	sum := map[Shape]time.Duration{}
	n := map[Shape]int{}
	for _, q := range r.Requests {
		if !q.Shape.FixedLength() {
			continue
		}
		sum[q.Shape] += time.Duration(q.Total)
		n[q.Shape]++
	}
	for s := range sum {
		sum[s] /= time.Duration(n[s])
	}
	return sum
}

// settleLimit is how much slower than prev a shape may be in the next round
// and still count as settled: max(WarmUpTolerance of prev, WarmUpFloor).
func settleLimit(prev time.Duration) time.Duration {
	return max(time.Duration(float64(prev)*WarmUpTolerance), WarmUpFloor)
}
