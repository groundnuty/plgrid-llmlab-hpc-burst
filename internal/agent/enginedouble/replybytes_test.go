package enginedouble

import (
	"strings"
	"testing"
)

// TestReplyBytesPadsANonStreamedReplyOnly: with reply-bytes=N a non-streamed chat reply's content is
// the usual answer followed by spaces, at least N bytes in all; a streamed reply is as without the
// mode; and without the mode a non-streamed reply is short (the control). #448's bulk-path rehearsal
// asks for a reply larger than frp's 6 MiB stream window this way.
func TestReplyBytesPadsANonStreamedReplyOnly(t *testing.T) {
	cfg := modeCfg(t, "reply-bytes=1048576")
	if cfg.ReplyBytes != 1<<20 {
		t.Fatalf("ReplyBytes = %d", cfg.ReplyBytes)
	}
	padded := serve(t, cfg).chat(chatBody(5, nil))
	plain := serve(t, cfgFor()).chat(chatBody(5, nil))
	got, want := *padded.Choices[0].Message.Content, *plain.Choices[0].Message.Content
	if len(got) < 1<<20 || !strings.HasPrefix(got, want) || strings.TrimRight(got, " ") != want {
		t.Errorf("the padded reply has %d bytes, starts with the plain answer %v, is it plus spaces %v", len(got), strings.HasPrefix(got, want), strings.TrimRight(got, " ") == want)
	}
	if len(want) >= 1<<10 {
		t.Errorf("the control reply is %d bytes: the padding is not what makes the reply large", len(want))
	}
	r := serve(t, cfg).post("/v1/chat/completions", chatBody(5, map[string]any{"stream": true}))
	if r.status != 200 || len(r.body) >= 1<<20 {
		t.Errorf("a streamed reply with reply-bytes: %d, %d bytes; want 200 and no padding", r.status, len(r.body))
	}
	for _, bad := range []string{"reply-bytes", "reply-bytes=0", "reply-bytes=x"} {
		if _, err := ParseModes(bad); err == nil {
			t.Errorf("%q was accepted", bad)
		}
	}
}
