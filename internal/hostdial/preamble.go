package hostdial

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The relay's preamble (HBEP-6, The wire): "relay <channel>" first writes one
// line, "hpcburst-relay/1 <Allocation UID>", to its standard output, then the
// session follows. The host side requires it within PreambleTimeout of the
// start; anything else first is refused as unexpected output, and the relay
// is started again.
const (
	// PreambleTimeout is how long the host side waits for the preamble.
	PreambleTimeout = 30 * time.Second
	// maxPreambleLine bounds the first line the host side reads.
	maxPreambleLine = 256
)

// ErrUnexpectedOutput is a relay whose first output was not its preamble
// ("unexpected output before the preamble"): for example a login shell that
// prints to standard output for a non-interactive ssh.
var ErrUnexpectedOutput = errors.New("unexpected output before the preamble")

// ErrNoPreamble is a relay that sent nothing within PreambleTimeout.
var ErrNoPreamble = errors.New("no preamble")

// readPreamble reads r's first line and checks it is uid's preamble. An r
// that ends before a line is returned as io.EOF, so the caller judges the
// relay by its process. r is left just after the preamble's newline.
func readPreamble(r *bufio.Reader, uid string) error {
	line := make([]byte, 0, len(agentapi.RelayPreamble(uid))+1)
	for {
		b, err := r.ReadByte()
		if err != nil {
			if len(line) == 0 {
				return io.EOF
			}
			return fmt.Errorf("%w: %d bytes and then the end", ErrUnexpectedOutput, len(line))
		}
		if b == '\n' {
			break
		}
		line = append(line, b)
		if len(line) > maxPreambleLine {
			return fmt.Errorf("%w: a first line longer than %d bytes", ErrUnexpectedOutput, maxPreambleLine)
		}
	}
	if string(line) != agentapi.RelayPreamble(uid) {
		// The line is not repeated: a login node's output can name the host.
		return fmt.Errorf("%w: a first line of %d bytes", ErrUnexpectedOutput, len(line))
	}
	return nil
}

// afterPreamble is the relay's process as the session's transport once the
// preamble is read: reads come from the buffered reader, writes and the close
// go to the process.
type afterPreamble struct {
	r *bufio.Reader
	io.WriteCloser
}

func (a afterPreamble) Read(p []byte) (int, error) { return a.r.Read(p) }
