package enginedouble

import (
	"strings"
	"testing"
)

// TestReplyBytesPadsOnlyTheMarkedNonStreamedReply: with reply-bytes=N, a non-streamed chat reply whose
// request's last message is ReplyBytesMarker is the usual answer followed by spaces, at least N bytes
// in all. Every other request gets the usual reply: one without the marker (as the agent's own checks
// of the engine, which read at most 16 MiB, send), and a streamed one with it. #448's bulk-path
// rehearsal asks for a reply larger than frp's 6 MiB stream window this way.
func TestReplyBytesPadsOnlyTheMarkedNonStreamedReply(t *testing.T) {
	cfg := modeCfg(t, "reply-bytes=1048576")
	if cfg.ReplyBytes != 1<<20 {
		t.Fatalf("ReplyBytes = %d", cfg.ReplyBytes)
	}
	marked := func(extra map[string]any) map[string]any {
		b := chatBody(5, extra)
		b["messages"] = append(b["messages"].([]map[string]string), map[string]string{"role": "user", "content": ReplyBytesMarker})
		return b
	}
	padded := serve(t, cfg).chat(marked(nil))
	plainMarked := serve(t, cfgFor()).chat(marked(nil))
	got, want := *padded.Choices[0].Message.Content, *plainMarked.Choices[0].Message.Content
	if len(got) < 1<<20 || !strings.HasPrefix(got, want) || strings.TrimRight(got, " ") != want {
		t.Errorf("the marked reply has %d bytes, starts with the plain answer %v, is it plus spaces %v", len(got), strings.HasPrefix(got, want), strings.TrimRight(got, " ") == want)
	}
	if len(want) >= 1<<10 {
		t.Errorf("the control reply is %d bytes: the padding is not what makes the reply large", len(want))
	}
	if c := *serve(t, cfg).chat(chatBody(5, nil)).Choices[0].Message.Content; len(c) >= 1<<10 {
		t.Errorf("a request without the marker got %d bytes: the agent's own checks would get large replies", len(c))
	}
	r := serve(t, cfg).post("/v1/chat/completions", marked(map[string]any{"stream": true}))
	if r.status != 200 || len(r.body) >= 1<<20 {
		t.Errorf("a streamed reply with the marker: %d, %d bytes; want 200 and no padding", r.status, len(r.body))
	}
	for _, bad := range []string{"reply-bytes", "reply-bytes=0", "reply-bytes=x"} {
		if _, err := ParseModes(bad); err == nil {
			t.Errorf("%q was accepted", bad)
		}
	}
}
