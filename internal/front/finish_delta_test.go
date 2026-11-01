package front

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
)

// The finish chunk's delta (@hpc-burst-science-agent, 1 Nov 2026, spike S94
// on GH200). vLLM v0.32.0 puts the token that ends a reply in the chunk that
// carries finish_reason, for example "delta":{"reasoning":","} next to
// "finish_reason":"length". That delta is ordinary content or reasoning: it
// can be the first token (TTFT), it counts as a content event, and it passes
// to the client unchanged.

// The events below are the shape of the real capture (S94, results/r3,
// before-stream-length.sse): a role chunk, a reasoning piece, the finish
// chunk with a delta, the usage chunk, [DONE]. The expected values in the
// tests come from this capture, not from the engine double.
const (
	s94Role      = `data: {"id":"chatcmpl-1","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":{"role":"assistant","content":""},"logprobs":null,"finish_reason":null}],"prompt_token_ids":null,"prompt_text":null}` + "\n\n"
	s94Reasoning = `data: {"id":"chatcmpl-1","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":{"reasoning":"Okay"},"logprobs":null,"finish_reason":null,"token_ids":null}]}` + "\n\n"
	s94Finish    = `data: {"id":"chatcmpl-1","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":{"reasoning":","},"logprobs":null,"finish_reason":"length","stop_reason":null,"token_ids":null}]}` + "\n\n"
	s94Usage     = `data: {"id":"chatcmpl-1","object":"chat.completion.chunk","model":"m","choices":[],"usage":{"prompt_tokens":15,"total_tokens":19,"completion_tokens":4,"completion_tokens_details":{"reasoning_tokens":3}},"system_fingerprint":"vllm-0.32.0-f5c1e941"}` + "\n\n"
)

// TestJudgeCountsTheFinishChunksDelta: the judge reads the finish reason and
// the delta of one chunk, and the delta is content.
func TestJudgeCountsTheFinishChunksDelta(t *testing.T) {
	j := &judge{kind: endpointChat}
	for i, ev := range []string{s94Role, s94Reasoning} {
		j.event([]byte(ev))
		if j.sawFinish {
			t.Fatalf("event %d: a finish reason before the finish chunk", i)
		}
	}
	if j.contentEvents != 1 {
		t.Fatalf("contentEvents = %d after the role chunk and one reasoning piece, want 1", j.contentEvents)
	}
	forward, done := j.event([]byte(s94Finish))
	if !forward || done {
		t.Errorf("finish chunk: forward %v done %v, want it forwarded and the stream not yet done", forward, done)
	}
	if !j.sawFinish || j.finish != "length" {
		t.Errorf("finish chunk: sawFinish %v finish %q, want length", j.sawFinish, j.finish)
	}
	if !j.sawContent || j.contentEvents != 2 {
		t.Errorf("finish chunk: sawContent %v contentEvents %d, want a content event and 2", j.sawContent, j.contentEvents)
	}
	j.event([]byte(s94Usage))
	if j.usage == nil || j.usage.cached != nil || j.usage.prompt == nil || *j.usage.prompt != 15 || *j.usage.completion != 4 {
		t.Errorf("usage = %+v, want prompt 15, completion 4 and no cached count (the capture names none)", j.usage)
	}
}

// TestFinishDeltaOfARealEngineStream: the S94 capture through the front. The
// client gets every byte unchanged; the record has the finish reason from
// the finish chunk, and with no usage chunk counts the finish delta among the
// content events.
func TestFinishDeltaOfARealEngineStream(t *testing.T) {
	for _, c := range []struct {
		name       string
		events     []string
		wantSource string
		wantEvents int64 // contentEvents, when the front counted them
	}{
		{"with usage", []string{s94Role, s94Reasoning, s94Finish, s94Usage, vllmDone}, "engine", -1},
		{"without usage", []string{s94Role, s94Reasoning, s94Finish, vllmDone}, "front_events", 2},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newScFix(t, scOpts{})
			f.retarget(f.uids[0], sseEngine(t, c.events...))
			resp, body := f.prSend(t, scChat, prChatStream)
			if want := strings.Join(c.events, ""); body != want {
				t.Errorf("the client's stream differs from the engine's:\n got %q\nwant %q", body, want)
			}
			rec := f.record(resp.Header.Get("hpcburst-request-id"))
			if rec.Finish == nil || *rec.Finish != "length" {
				t.Errorf("finish = %s, want length", strStr(rec.Finish))
			}
			if rec.CountsSource == nil || *rec.CountsSource != c.wantSource {
				t.Errorf("countsSource = %s, want %s", strStr(rec.CountsSource), c.wantSource)
			}
			if rec.FirstTokenMs == nil {
				t.Errorf("firstTokenMs is null, want the first reasoning piece's time")
			}
			if c.wantEvents >= 0 && !ptrIs(rec.ContentEvents, c.wantEvents) {
				t.Errorf("contentEvents = %s, want %d: the reasoning piece and the finish chunk's delta", ptrStr(rec.ContentEvents), c.wantEvents)
			}
		})
	}
}

// finishChoice returns the choice of the stream's chunk that carries a
// finish reason.
func finishChoice(t *testing.T, body string) (finish string, delta map[string]any, text string) {
	t.Helper()
	for _, ev := range strings.Split(body, "\n\n") {
		data := strings.TrimPrefix(strings.TrimSpace(ev), "data: ")
		var c struct {
			Choices []struct {
				FinishReason *string        `json:"finish_reason"`
				Text         string         `json:"text"`
				Delta        map[string]any `json:"delta"`
			} `json:"choices"`
		}
		if json.Unmarshal([]byte(data), &c) != nil {
			continue
		}
		for _, ch := range c.Choices {
			if ch.FinishReason != nil {
				return *ch.FinishReason, ch.Delta, ch.Text
			}
		}
	}
	t.Fatalf("no chunk with a finish reason in %q", body)
	return "", nil, ""
}

// TestFinishDeltaIsTheOnlyContentOfAOneChunkReply: a reply whose only token
// rides in the finish chunk. The engine waits 2 s on the front's fake clock
// before that chunk, so the first token is at 2,000 ms only if the front took
// it from the finish chunk; the content is counted and reaches the client in
// the field the engine used.
func TestFinishDeltaIsTheOnlyContentOfAOneChunkReply(t *testing.T) {
	const completionsBody = `{"model":"` + efModel + `","stream":true,"prompt":"hi"}`
	for _, c := range []struct {
		name, path, body, field string
	}{
		{"chat content", "/v1/chat/completions", errChatBody(efModel, true, "hi"), "content"},
		{"chat reasoning", "/v1/chat/completions", errChatBody(efModel, true, "hi"), "reasoning"},
		{"chat reasoning_content", "/v1/chat/completions", errChatBody(efModel, true, "hi"), "reasoning_content"},
		{"completions text", "/v1/completions", completionsBody, "text"},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := newErrFix(t, nil, instanceOpts{}, "")
			s.e.Set(enginetest.Behaviour{Tokens: 1, LastWithFinish: true, DeltaField: c.field, FirstToken: 2 * time.Second, FinishReason: "length"})
			resp := s.post(c.path, c.body)
			body := readAll(t, resp)
			finish, delta, text := finishChoice(t, body)
			if finish != "length" {
				t.Errorf("the client's finish reason = %q, want length", finish)
			}
			got := text
			if c.field != "text" {
				got, _ = delta[c.field].(string)
			}
			if got != "t0 " {
				t.Errorf("the client's finish chunk holds %q in %s (delta %v), want the engine's token %q", got, c.field, delta, "t0 ")
			}
			if n := strings.Count(body, "t0 "); n != 1 {
				t.Errorf("the token reached the client %d times, want once", n)
			}
			rec := s.record(resp.Header.Get("Hpcburst-Request-Id"))
			if !ptrIs(rec.FirstTokenMs, 2000) {
				t.Errorf("firstTokenMs = %s, want 2000: the finish chunk is the first content", ptrStr(rec.FirstTokenMs))
			}
			if !ptrIs(rec.ContentEvents, 1) || rec.CountsSource == nil || *rec.CountsSource != "front_events" {
				t.Errorf("contentEvents %s source %s, want 1 and front_events", ptrStr(rec.ContentEvents), strStr(rec.CountsSource))
			}
			if rec.Finish == nil || *rec.Finish != "length" {
				t.Errorf("finish = %s, want length", strStr(rec.Finish))
			}
		})
	}
}

// TestFinishDeltaCarriesTheLastReasoningPiece: a reasoning stream of 4
// tokens whose last piece rides in the finish chunk. The front counts 4
// content events, the same as when the finish chunk is empty, and the
// piece reaches the client in delta.reasoning.
func TestFinishDeltaCarriesTheLastReasoningPiece(t *testing.T) {
	for _, c := range []struct {
		name  string
		rides bool
	}{
		{"last piece in the finish chunk", true},
		{"control: empty finish chunk", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := newErrFix(t, nil, instanceOpts{}, "")
			s.e.Set(enginetest.Behaviour{Tokens: 4, DeltaField: "reasoning", LastWithFinish: c.rides, FinishReason: "length"})
			resp := s.post("/v1/chat/completions", errChatBody(efModel, true, "hi"))
			body := readAll(t, resp)
			_, delta, _ := finishChoice(t, body)
			if got, _ := delta["reasoning"].(string); (got == "t3 ") != c.rides {
				t.Errorf("the finish chunk's delta.reasoning = %q (delta %v); the last piece in it is %v", got, delta, c.rides)
			}
			if n := strings.Count(body, `"reasoning":"t3 "`); n != 1 {
				t.Errorf("the last piece reached the client %d times, want once", n)
			}
			rec := s.record(resp.Header.Get("Hpcburst-Request-Id"))
			if !ptrIs(rec.ContentEvents, 4) {
				t.Errorf("contentEvents = %s, want 4: three reasoning chunks and the last piece", ptrStr(rec.ContentEvents))
			}
			if rec.Finish == nil || *rec.Finish != "length" {
				t.Errorf("finish = %s, want length", strStr(rec.Finish))
			}
		})
	}
}
