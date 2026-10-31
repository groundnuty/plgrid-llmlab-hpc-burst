// Copied from spikes/s2/cmd/s2probe/sse.go (spike S2 phase A, PR #44), this
// repository, Apache-2.0. Changed: the package name, and chatChunk carries the
// fields the warm-up reads (tool-call deltas, token ids, usage).

package engine

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// errUnfinishedEvent means the body ended inside an event: some bytes of it had arrived, but not
// its terminating blank line. The HTML Standard's event stream interpretation discards such an
// event, so a consumer must not act on it, not even on a "data: [DONE]" line.
var errUnfinishedEvent = errors.New("body ended inside an unfinished SSE event (no terminating blank line)")

// sseEvent is one dispatched event: the fields of a block of lines closed by a blank line.
type sseEvent struct {
	Event string // value of the last "event:" field, "" if none
	Data  string // the "data:" values joined by "\n"
}

// sseReader assembles events from a byte stream, however the transport sliced it into reads.
type sseReader struct {
	rd      *bufio.Reader
	event   string
	data    []string
	pending bool // fields of an event are buffered and its blank line has not arrived
}

func newSSEReader(r io.Reader) *sseReader { return &sseReader{rd: bufio.NewReaderSize(r, 64<<10)} }

// next returns the next complete event. onComment is called for each comment line (": ...").
// It returns io.EOF only at an event boundary, errUnfinishedEvent if the body ends inside an
// event or inside a line, and the transport's error otherwise.
func (s *sseReader) next(onComment func()) (sseEvent, error) {
	for {
		line, err := s.rd.ReadString('\n')
		if err != nil {
			if err == io.EOF && (line != "" || s.pending) {
				return sseEvent{}, errUnfinishedEvent
			}
			return sseEvent{}, err
		}
		line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		switch {
		case line == "": // blank line: dispatch the buffered event, if it has data
			ev := sseEvent{Event: s.event, Data: strings.Join(s.data, "\n")}
			had := len(s.data) > 0
			s.event, s.data, s.pending = "", nil, false
			if had {
				return ev, nil
			}
		case line[0] == ':':
			onComment()
		default:
			field, value, _ := strings.Cut(line, ":")
			value = strings.TrimPrefix(value, " ") // the specification strips one leading space only
			switch field {
			case "data":
				s.data, s.pending = append(s.data, value), true
			case "event":
				s.event, s.pending = value, true
			default: // id, retry and unknown fields: part of the event, not used here
				s.pending = true
			}
		}
	}
}

// chatChunk is the part of an OpenAI-style streamed chunk the warm-up reads.
type chatChunk struct {
	Choices []struct {
		Delta struct {
			Content   string            `json:"content"`
			ToolCalls []json.RawMessage `json:"tool_calls"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
		TokenIDs     []int32 `json:"token_ids"`
	} `json:"choices"`
	Usage *usage          `json:"usage"`
	Error json.RawMessage `json:"error"`
}

// parseChunk interprets one complete event. done is true for the [DONE] sentinel. A non-nil error
// means the stream carries a failure or a payload the client cannot trust.
func parseChunk(ev sseEvent) (ch chatChunk, done bool, err error) {
	if ev.Event == "error" {
		return ch, false, fmt.Errorf("error event: %.200s", ev.Data)
	}
	if strings.TrimSpace(ev.Data) == "[DONE]" {
		return ch, true, nil
	}
	if jerr := json.Unmarshal([]byte(ev.Data), &ch); jerr != nil {
		return ch, false, fmt.Errorf("malformed event payload (%v): %.200s", jerr, ev.Data)
	}
	if e := strings.TrimSpace(string(ch.Error)); e != "" && e != "null" {
		return ch, false, fmt.Errorf("error payload: %.200s", e)
	}
	return ch, false, nil
}
