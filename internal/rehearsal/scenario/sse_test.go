package scenario_test

// What a client received in a streamed chat reply (issue #193, review of PR
// #247): each data event parsed, the tokens counted from the content the
// client got, not from the usage record the front wrote.

import (
	"encoding/json"
	"fmt"
	"strings"
)

// sseReceived is a streamed chat reply as the client received it.
type sseReceived struct {
	// words is the number of whitespace-separated words of the content
	// deltas joined: the engine double's tokens are single words, each
	// after the first sent with a leading space, so it counts the tokens
	// whatever chunks the front relays them in. chunks counts the events
	// whose content was not empty.
	words, chunks int
	// finishes are the finish reasons in the order they came; finishLast
	// reports whether the last event with choices carried the only one.
	finishes   []string
	finishLast bool
	// usageTokens is the usage event's completion_tokens, -1 with none.
	usageTokens int
	// done reports whether "data: [DONE]" was the last event.
	done bool
	// problems are events that did not parse, or came after [DONE].
	problems []string
}

// sseParse reads a streamed chat reply's body.
func sseParse(body []byte) sseReceived {
	rec := sseReceived{usageTokens: -1}
	var content strings.Builder
	var events []string
	for _, l := range strings.Split(string(body), "\n") {
		l = strings.TrimSpace(l)
		if v, ok := strings.CutPrefix(l, "data:"); ok {
			events = append(events, strings.TrimSpace(v))
		}
	}
	lastChoice := -1
	for i, ev := range events {
		if ev == "[DONE]" {
			if i != len(events)-1 {
				rec.problems = append(rec.problems, fmt.Sprintf("event %d of %d is [DONE], not the last", i, len(events)))
			}
			rec.done = i == len(events)-1
			continue
		}
		var c struct {
			Choices []struct {
				Delta struct {
					Content *string `json:"content"`
				} `json:"delta"`
				FinishReason *string `json:"finish_reason"`
			} `json:"choices"`
			Usage *struct {
				CompletionTokens int `json:"completion_tokens"`
			} `json:"usage"`
		}
		if err := json.Unmarshal([]byte(ev), &c); err != nil {
			rec.problems = append(rec.problems, fmt.Sprintf("event %d does not parse: %v: %.120s", i, err, ev))
			continue
		}
		if c.Usage != nil {
			rec.usageTokens = c.Usage.CompletionTokens
		}
		for _, ch := range c.Choices {
			lastChoice = i
			if ch.Delta.Content != nil && *ch.Delta.Content != "" {
				content.WriteString(*ch.Delta.Content)
				rec.chunks++
			}
			if ch.FinishReason != nil {
				rec.finishes = append(rec.finishes, *ch.FinishReason)
				rec.finishLast = lastChoice == i
			} else if len(rec.finishes) > 0 {
				rec.finishLast = false
			}
		}
	}
	rec.words = len(strings.Fields(content.String()))
	return rec
}

// streamProblems holds a received stream to a reply of exactly tokens tokens
// cut by max_tokens: that many tokens of content, one finish reason, length,
// on the last event with choices, the usage event's completion_tokens equal
// to it, and [DONE] last.
func streamProblems(rec sseReceived, tokens int) []string {
	out := append([]string(nil), rec.problems...)
	if rec.words != tokens {
		out = append(out, fmt.Sprintf("the client received %d tokens of content (in %d events), want %d", rec.words, rec.chunks, tokens))
	}
	if len(rec.finishes) != 1 || rec.finishes[0] != "length" || !rec.finishLast {
		out = append(out, fmt.Sprintf("finish reasons %q (on the last event with choices: %v), want one, length, on the last", rec.finishes, rec.finishLast))
	}
	if rec.usageTokens != tokens {
		out = append(out, fmt.Sprintf("the usage event's completion_tokens is %d, want %d", rec.usageTokens, tokens))
	}
	if !rec.done {
		out = append(out, "the stream did not end with data: [DONE]")
	}
	return out
}
