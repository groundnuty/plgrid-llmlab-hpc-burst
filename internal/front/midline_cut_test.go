package front

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// TestCutInsideAnEvent (PR #95 review: Astra B6, Opus N1): the upstream ends
// inside an event. The client must still get the front's stream_cut as a
// standalone event whose data is JSON, not glued onto the fragment. A cut
// inside a line drops that line; a complete line that only lacks its blank
// line is still forwarded as its own event.
func TestCutInsideAnEvent(t *testing.T) {
	cases := []struct{ name, tail, wantKept string }{
		{"mid-line", `data: {"choices":[{"delta":{"cont`, ""},
		{"mid-JSON, line complete", "data: {\"choices\":[\n", `data: {"choices":[`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newScFix(t, scOpts{})
			f.retarget(f.uids[0], rawEngine(t, func(w http.ResponseWriter, r *http.Request) {
				abortAfter(w, 200, map[string]string{"Content-Type": "text/event-stream"}, chatToken+c.tail)
			}))
			_, body := f.prSend(t, scChat, prChatStream)
			events := strings.Split(strings.TrimSuffix(body, "\n\n"), "\n\n")
			last := events[len(events)-1]
			data, ok := strings.CutPrefix(last, "data: ")
			var v struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if !ok || strings.Contains(data, "\n") || json.Unmarshal([]byte(data), &v) != nil || v.Error.Code != "stream_cut" {
				t.Fatalf("the last event is not a standalone stream_cut with JSON data: %q\nbody: %q", last, body)
			}
			if c.wantKept != "" && !strings.Contains(body, c.wantKept+"\n\n") {
				t.Errorf("the complete line %q was not forwarded as its own event: %q", c.wantKept, body)
			}
			if c.wantKept == "" && strings.Contains(body, `"choices":[{"delta":{"cont`) {
				t.Errorf("the cut line's fragment reached the client: %q", body)
			}
		})
	}
}
