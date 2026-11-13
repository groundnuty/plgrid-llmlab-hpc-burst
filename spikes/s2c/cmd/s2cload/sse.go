// SSE event framing for the load client.
//
// Copied unchanged from phase A's client, spikes/s2/cmd/s2probe/sse.go (same repository, commit
// 320a84f998e5e8938edd661729b7a9013fdf2699), except that phase A's parseChunk and chatChunk are left out:
// this client reads usage as well and parses its own chunks in stream.go. It is a copy because phase A's
// is package main of another module. Phase B's first client accepted a "data: [DONE]" line without its
// terminating blank line; this reader does not.

package main

import (
	"bufio"
	"errors"
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
