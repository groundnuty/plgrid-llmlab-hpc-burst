package enginedouble

import (
	"bufio"
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// TestStreamReplyBytesPadsOnlyTheMarkedStreamedReply: with reply-bytes=N, a streamed chat reply whose request's last message is
// StreamReplyBytesMarker is the usual answer's tokens, then chunks of spaces to N bytes of content in all, then the finish chunk and [DONE]. A
// streamed reply with ReplyBytesMarker, one without a marker, and a non-streamed one with the stream marker get the usual reply. The bulk row
// (issue #521) sends the same bytes as the non-streamed reply down the token transport this way, as its control.
func TestStreamReplyBytesPadsOnlyTheMarkedStreamedReply(t *testing.T) {
	const n = 1<<20 + 123 // not a multiple of the chunk, so the last chunk is the remainder
	cfg := modeCfg(t, "reply-bytes=1048699")
	if cfg.ReplyBytes != n {
		t.Fatalf("ReplyBytes = %d", cfg.ReplyBytes)
	}
	body := func(marker string, stream bool) map[string]any {
		b := chatBody(5, map[string]any{"stream": stream})
		if marker != "" {
			b["messages"] = append(b["messages"].([]map[string]string), map[string]string{"role": "user", "content": marker})
		}
		return b
	}
	// content, finish and the order of the finish chunk and the padding, as the client reads the stream.
	read := func(raw []byte) (content string, finishAt, lastContentAt, chunks int, done bool) {
		sc := bufio.NewScanner(bytes.NewReader(raw))
		sc.Buffer(make([]byte, 1<<20), 1<<24)
		for sc.Scan() {
			line := sc.Text()
			payload, ok := strings.CutPrefix(line, "data: ")
			if !ok {
				continue
			}
			if payload == "[DONE]" {
				done = true
				continue
			}
			var c struct {
				Choices []struct {
					Delta struct {
						Content *string `json:"content"`
					} `json:"delta"`
					FinishReason *string `json:"finish_reason"`
				} `json:"choices"`
			}
			if err := json.Unmarshal([]byte(payload), &c); err != nil {
				t.Fatalf("a chunk is not JSON: %v", err)
			}
			chunks++
			for _, ch := range c.Choices {
				if ch.Delta.Content != nil && *ch.Delta.Content != "" {
					content += *ch.Delta.Content
					lastContentAt = chunks
				}
				if ch.FinishReason != nil {
					finishAt = chunks
				}
			}
		}
		return
	}
	padded := serve(t, cfg).post("/v1/chat/completions", body(StreamReplyBytesMarker, true))
	plain := serve(t, cfgFor()).post("/v1/chat/completions", body(StreamReplyBytesMarker, true))
	pc, pFinish, pLast, _, pDone := read(padded.body)
	qc, _, _, _, qDone := read(plain.body)
	if padded.status != 200 || !pDone || !qDone {
		t.Fatalf("status %d, [DONE] seen %v and %v", padded.status, pDone, qDone)
	}
	// The padding goes in before the answer's last token, so the answer is the content with exactly one run of n spaces taken out.
	if len(pc) < n || strings.Replace(pc, strings.Repeat(" ", n), "", 1) != qc {
		t.Errorf("the padded stream's content is %d bytes (want at least %d), and is the plain answer plus a run of n spaces: %v", len(pc), n, strings.Replace(pc, strings.Repeat(" ", n), "", 1) == qc)
	}
	if len(qc) >= 1<<10 {
		t.Errorf("the control stream's content is %d bytes: the padding is not what makes the reply large", len(qc))
	}
	if pFinish == 0 || pLast > pFinish {
		t.Errorf("the finish chunk is %d and the last content chunk %d: the padding must come before the finish chunk", pFinish, pLast)
	}
	// No SSE line is as long as a bufio.Scanner's default limit (64 KiB): a reader of the stream needs no larger buffer.
	if longest := longestLine(padded.body); longest >= 64<<10 {
		t.Errorf("the longest SSE line is %d bytes, at or over 64 KiB", longest)
	}
	if got := len(pc) - len(qc); got != n {
		t.Errorf("the padding is %d bytes, want exactly %d", got, n)
	}
	// What is not marked for streamed padding is not padded.
	for _, c := range []struct {
		name   string
		body   map[string]any
		stream bool
	}{
		{"a streamed reply with the non-streamed marker", body(ReplyBytesMarker, true), true},
		{"a streamed reply with no marker", body("", true), true},
	} {
		r := serve(t, cfg).post("/v1/chat/completions", c.body)
		if got, _, _, _, _ := read(r.body); r.status != 200 || len(got) >= 1<<10 {
			t.Errorf("%s: %d, %d bytes of content; want 200 and the usual reply", c.name, r.status, len(got))
		}
	}
	if r := serve(t, cfg).post("/v1/chat/completions", body(StreamReplyBytesMarker, false)); r.status != 200 || len(r.body) >= 1<<20 {
		t.Errorf("a non-streamed reply with the stream marker: %d, %d bytes; want 200 and no padding", r.status, len(r.body))
	}
}

// longestLine is the length of the longest line of an SSE body, in bytes.
func longestLine(b []byte) int {
	longest := 0
	for _, l := range bytes.Split(b, []byte("\n")) {
		longest = max(longest, len(l))
	}
	return longest
}
