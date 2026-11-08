package alpha

// The upload direction of the tunnel hour (issue #224): the body of a large
// request, the upload as the front's usage record gives it, and the upload's
// path rate, worked out by hand.

import (
	"encoding/json"
	"testing"
	"time"

)

func TestTheUploadsRequestBody(t *testing.T) {
	for _, n := range []int{100_000, 128 << 10, 8 << 20} {
		b := UploadRequestBody("example-org/example-model", n)
		if len(b) != n {
			t.Errorf("a body asked for %d bytes has %d", n, len(b))
		}
		var m struct {
			Model     string `json:"model"`
			MaxTokens int    `json:"max_tokens"`
			Stream    bool   `json:"stream"`
			User      string `json:"user"`
			Messages  []struct {
				Content string `json:"content"`
			} `json:"messages"`
		}
		if err := json.Unmarshal([]byte(b), &m); err != nil {
			t.Fatalf("%d bytes: not JSON: %v", n, err)
		}
		// The bytes are in "user", never in the prompt: one short message, one token asked, streamed.
		if m.Model != "example-org/example-model" || m.MaxTokens != 1 || !m.Stream || len(m.Messages) != 1 || m.Messages[0].Content != "hello" || len(m.User) < n-200 {
			t.Errorf("%d bytes: %+v (user %d bytes)", n, m.Messages, len(m.User))
		}
	}
}

// Three uploads of 8 MiB from 09:00:01 to 09:00:31.5, the load's 16 streams all open from 09:00:00 to 09:01:00:
// 24 MiB (25,165,824 bytes) over 30.5 s, asked 30 s, under load.
func TestUploadRate(t *testing.T) {
	at := func(ms int) time.Time {
		return time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC).Add(time.Duration(ms) * time.Millisecond)
	}
	up := func(from, to int) Stream {
		return Stream{Complete: true, BodyBytes: 8 << 20, BodyStart: at(from), BodyEnd: at(to)}
	}
	ups := []Stream{up(1000, 11000), up(11500, 21000), up(21500, 31500)}
	r := UploadRate(ups, 30*time.Second, at(0), at(60_000))
	if r.Direction != DirUpload || !r.OK || !r.UnderLoad || r.Bytes != 25_165_824 || r.TimedSeconds != 30.5 || r.AskedSeconds != 30 || r.Tokens != 0 {
		t.Fatalf("UploadRate = %+v", r)
	}
	if bps := r.BytesPerSecond(); bps < 825_108 || bps > 825_110 {
		t.Errorf("the rate is %.1f B/s, want 25,165,824 / 30.5 = 825,109.0", bps)
	}
	for name, c := range map[string]struct {
		ups             []Stream
		allOpen, endsAt time.Time
		ok, under       bool
	}{
		"no upload":                          {nil, at(0), at(60_000), false, false},
		"an upload that did not complete":    {[]Stream{up(1000, 11000), {BodyBytes: 8 << 20, BodyStart: at(12000), BodyEnd: at(13000)}}, at(0), at(60_000), false, true},
		"an upload with no interval":         {[]Stream{up(1000, 11000), {Complete: true, BodyBytes: 8 << 20}}, at(0), at(60_000), false, true},
		"one that began before all opened":   {ups, at(1500), at(60_000), true, false},
		"one that ended after the first end": {ups, at(0), at(31_000), true, false},
	} {
		r := UploadRate(c.ups, 30*time.Second, c.allOpen, c.endsAt)
		if r.OK != c.ok || r.UnderLoad != c.under {
			t.Errorf("%s: OK %v under load %v, want %v %v", name, r.OK, r.UnderLoad, c.ok, c.under)
		}
	}
}
