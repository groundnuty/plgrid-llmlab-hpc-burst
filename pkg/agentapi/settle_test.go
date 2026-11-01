package agentapi

import (
	"testing"
	"time"
)

// The expected verdicts are written out by hand from the rule (HBEP-7,
// Instance start; WarmUpRound's documentation): a fixed-length shape is
// settled when its mean Total rose by at most max(10% of the round before's,
// 100 ms).

func rnd(n int32, reqs ...RequestTiming) WarmUpRound { return WarmUpRound{Round: n, Requests: reqs} }

func tm(s Shape, total time.Duration) RequestTiming {
	return RequestTiming{Shape: s, Total: Duration(total)}
}

func TestWarmUpSettledBoundaries(t *testing.T) {
	const ms, s = time.Millisecond, time.Second
	for _, tc := range []struct {
		name      string
		prev, cur time.Duration
		want      bool
	}{
		// 10% governs above 1 s: 4 s may rise by 400 ms.
		{"10% of 4 s, at the line", 4 * s, 4*s + 400*ms, true},
		{"10% of 4 s, 1 ns over", 4 * s, 4*s + 400*ms + 1, false},
		// The 100 ms floor governs below 1 s: 500 ms may rise by 100 ms, not 50.
		{"floor on 500 ms, at the line", 500 * ms, 600 * ms, true},
		{"floor on 500 ms, 1 ns over", 500 * ms, 600*ms + 1, false},
		// At 1 s the two coincide.
		{"1 s, at the line", s, s + 100*ms, true},
		{"1 s, 1 ns over", s, s + 100*ms + 1, false},
		// 1.5 s: 10% is 150 ms, above the floor.
		{"10% of 1.5 s, at the line", 1500 * ms, 1650 * ms, true},
		{"10% of 1.5 s, 1 ns over", 1500 * ms, 1650*ms + 1, false},
		// Faster is settled.
		{"faster", 2 * s, s, true},
		{"unchanged", 2 * s, 2 * s, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := WarmUpSettled(rnd(1, tm(ShapeTypical, tc.prev)), rnd(2, tm(ShapeTypical, tc.cur)))
			if got != tc.want {
				t.Errorf("WarmUpSettled(prev %v, cur %v) = %v, want %v", tc.prev, tc.cur, got, tc.want)
			}
		})
	}
}

// One shape over the line is enough to block settling, and the mean of a
// shape's requests is what is compared (the concurrent shape has several).
func TestWarmUpSettledJudgesEachShapeByItsMean(t *testing.T) {
	const s = time.Second
	prev := rnd(1, tm(ShapeShort, s), tm(ShapeTypical, 2*s), tm(ShapeConcurrent, 3*s), tm(ShapeConcurrent, 5*s))
	// concurrent mean 4 s -> 4.4 s is the line: (3.9 + 4.9) / 2 = 4.4 s.
	atLine := rnd(2, tm(ShapeShort, s), tm(ShapeTypical, 2*s), tm(ShapeConcurrent, 3900*time.Millisecond), tm(ShapeConcurrent, 4900*time.Millisecond))
	if !WarmUpSettled(prev, atLine) {
		t.Error("concurrent mean 4.4 s after 4 s, every other shape unchanged: want settled")
	}
	// 2 ns more on one of the two requests is 1 ns on the mean: over the line.
	over := rnd(2, tm(ShapeShort, s), tm(ShapeTypical, 2*s), tm(ShapeConcurrent, 3900*time.Millisecond), tm(ShapeConcurrent, 4900*time.Millisecond+2))
	if WarmUpSettled(prev, over) {
		t.Error("concurrent mean 1 ns over the line: want not settled")
	}
	slowShort := rnd(2, tm(ShapeShort, s+101*time.Millisecond), tm(ShapeTypical, 2*s), tm(ShapeConcurrent, 4*s), tm(ShapeConcurrent, 4*s))
	if WarmUpSettled(prev, slowShort) {
		t.Error("short 101 ms slower after 1 s, others settled: want not settled")
	}
}

// A shape whose length varies is not judged: the tool call may be as slow as
// it likes, and a round holding only such shapes settles nothing.
func TestWarmUpSettledIgnoresShapesOfVaryingLength(t *testing.T) {
	const s = time.Second
	prev := rnd(1, tm(ShapeTypical, 2*s), tm(ShapeToolCall, s))
	cur := rnd(2, tm(ShapeTypical, 2*s), tm(ShapeToolCall, 50*s))
	if !WarmUpSettled(prev, cur) {
		t.Error("a tool call 50x slower, the fixed-length shape unchanged: want settled")
	}
	onlyTool := WarmUpSettled(rnd(1, tm(ShapeToolCall, s)), rnd(2, tm(ShapeToolCall, s)))
	if onlyTool {
		t.Error("two rounds with no fixed-length shape: want not settled")
	}
	unnamed := WarmUpSettled(rnd(1, tm("mystery", s)), rnd(2, tm("mystery", s)))
	if unnamed {
		t.Error("a shape this package does not name is not fixed-length: want not settled")
	}
}

// A shape that the new round lacks, or the old round lacked, means not
// settled: the rounds are not comparable.
func TestWarmUpSettledNeedsTheSameShapes(t *testing.T) {
	const s = time.Second
	both := rnd(1, tm(ShapeShort, s), tm(ShapeTypical, s))
	missing := rnd(2, tm(ShapeShort, s))
	if WarmUpSettled(both, missing) {
		t.Error("typical missing from the new round: want not settled")
	}
	if WarmUpSettled(missing, both) {
		t.Error("typical new in the new round: want not settled")
	}
	if WarmUpSettled(both, rnd(2)) {
		t.Error("an empty new round: want not settled")
	}
}
