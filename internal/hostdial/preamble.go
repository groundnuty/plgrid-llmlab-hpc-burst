package hostdial

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The relay's preamble: before the session, "relay <channel>" writes one line,
// "\x00hpcburst-relay/1 <channel>\n", to its standard output, and the
// control-plane end reads up to MaxPreambleScan bytes for it before it starts
// the session. Anything before it (a login node's shell init that prints to
// standard output for a non-interactive ssh, a banner) is dropped and counted,
// so it cannot corrupt the session; a relay that sends none within the bound,
// or names another channel, is refused by name. The NUL keeps it from
// matching ordinary text.
const (
	preambleMagic = "\x00hpcburst-relay/1 "
	// MaxPreambleScan bounds what the control-plane end reads for the
	// preamble.
	MaxPreambleScan = 64 << 10
)

// Preamble is the line a relay for ch writes first.
func Preamble(ch agentapi.RelayChannel) string { return preambleMagic + string(ch) + "\n" }

// ErrNoPreamble is a relay whose output did not start with a preamble within
// MaxPreambleScan bytes.
var ErrNoPreamble = errors.New("no relay preamble")

// ErrWrongChannel is a relay whose preamble names another channel than the
// one the control-plane end serves.
var ErrWrongChannel = errors.New("the relay serves another channel")

// readPreamble reads r up to its preamble and returns how many bytes came
// before it. r is left just after the preamble's newline.
func readPreamble(r *bufio.Reader, want agentapi.RelayChannel) (int, error) {
	skipped := 0
	window := make([]byte, 0, len(preambleMagic))
	for skipped+len(window) < MaxPreambleScan {
		b, err := r.ReadByte()
		if err != nil {
			return skipped, fmt.Errorf("%w: the relay's output ended after %d bytes: %v", ErrNoPreamble, skipped+len(window), err)
		}
		if len(window) == len(preambleMagic) {
			window = append(window[:0], window[1:]...)
			skipped++
		}
		window = append(window, b)
		if string(window) != preambleMagic {
			continue
		}
		line, err := r.ReadString('\n')
		if err != nil || len(line) > 64 {
			return skipped, fmt.Errorf("%w: a preamble with no channel", ErrNoPreamble)
		}
		if got := strings.TrimSuffix(line, "\n"); got != string(want) {
			return skipped, fmt.Errorf("%w: it announced %q, this end serves %q", ErrWrongChannel, got, want)
		}
		return skipped, nil
	}
	return skipped, fmt.Errorf("%w within %d bytes: the login node may print to standard output for a non-interactive ssh", ErrNoPreamble, MaxPreambleScan)
}

// afterPreamble is the relay's process as the session's transport once the
// preamble is read: reads come from the buffered reader, writes and the close
// go to the process.
type afterPreamble struct {
	r *bufio.Reader
	io.WriteCloser
}

func (a afterPreamble) Read(p []byte) (int, error) { return a.r.Read(p) }
