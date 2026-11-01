package agentapi

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// Expected values are worked by hand: (10s - 1s) / 9 = 1s, and so on.
func TestTimePerOutputToken(t *testing.T) {
	ms := func(n int64) Duration { return Duration(time.Duration(n) * time.Millisecond) }
	for _, c := range []struct {
		name string
		in   RequestTiming
		want time.Duration
		ok   bool
	}{
		{"no output token", RequestTiming{Shape: ShapeTypical, TimeToFirstToken: ms(1000), Total: ms(10000), OutputTokens: 0}, 0, false},
		{"one output token has no interval", RequestTiming{Shape: ShapeTypical, TimeToFirstToken: ms(1000), Total: ms(1000), OutputTokens: 1}, 0, false},
		{"one output token with a longer total is still no interval", RequestTiming{Shape: ShapeTypical, TimeToFirstToken: ms(1000), Total: ms(10000), OutputTokens: 1}, 0, false},
		{"two output tokens: one interval", RequestTiming{Shape: ShapeTypical, TimeToFirstToken: ms(1000), Total: ms(10000), OutputTokens: 2}, 9 * time.Second, true},
		{"ten output tokens: nine intervals", RequestTiming{Shape: ShapeTypical, TimeToFirstToken: ms(1000), Total: ms(10000), OutputTokens: 10}, time.Second, true},
		{"many output tokens", RequestTiming{Shape: ShapeConcurrent, TimeToFirstToken: ms(500), Total: ms(2000), OutputTokens: 4}, 500 * time.Millisecond, true},
		{"a thousand output tokens", RequestTiming{Shape: ShapeLongOutput, TimeToFirstToken: ms(250), Total: ms(50000), OutputTokens: 1000}, 49750 * time.Millisecond / 999, true},
		{"an error with many output tokens counts for nothing", RequestTiming{Shape: ShapeTypical, TimeToFirstToken: ms(1000), Total: ms(10000), OutputTokens: 10, Error: "connection reset"}, 0, false},
		{"an error with two output tokens counts for nothing", RequestTiming{Shape: ShapeTypical, TimeToFirstToken: ms(1000), Total: ms(10000), OutputTokens: 2, Error: "timeout"}, 0, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, ok := c.in.TimePerOutputToken()
			if ok != c.ok || got != c.want {
				t.Errorf("TimePerOutputToken() = %v, %v; want %v, %v", got, ok, c.want, c.ok)
			}
		})
	}
}

// Staging.BuildPeakTmpBytes round-trips, and a staging with no build (a cache
// hit) carries no buildPeakTmpBytes key at all (issue #125).
func TestStagingBuildPeakRoundTrip(t *testing.T) {
	b, err := json.Marshal(Staging{Phase: StagingDone, BuildPeakTmpBytes: 17_179_869_184})
	if err != nil || !strings.Contains(string(b), `"buildPeakTmpBytes":17179869184`) {
		t.Fatalf("%s %v", b, err)
	}
	var back Staging
	if err := json.Unmarshal(b, &back); err != nil || back.BuildPeakTmpBytes != 17_179_869_184 {
		t.Errorf("round trip %+v %v", back, err)
	}
	if b, _ := json.Marshal(Staging{Phase: StagingDone}); strings.Contains(string(b), "buildPeakTmpBytes") {
		t.Errorf("a staging with no build carries the field: %s", b)
	}
}
