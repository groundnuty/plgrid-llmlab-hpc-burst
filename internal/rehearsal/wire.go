package rehearsal

// New. The relay's wire format. There are two, told apart by the first eight
// bytes of a connection.
//
// # Request and reply
//
// Every line but an srun line: one request and one reply per unix-socket
// connection, each field length-prefixed raw bytes (no base64: a staged agent
// binary of tens of MB goes through standard input).
//
//	request: u64 len(line) | line | u64 len(stdin) | stdin
//	reply:   i32 exit code | u64 len(stdout) | stdout | u64 len(stderr) | stderr
//
// Integers are big-endian. A field above maxField is refused on both ends.
//
// # Streaming
//
// An srun line (the relay of HBEP-6's host-dialed mode runs one for a job's
// whole life, with a multiplexed session over its standard input and output)
// is a full-duplex stream: bytes cross in both directions as they arrive, and
// the end of one direction leaves the other open.
//
//	request: u64 streamMagic | u64 len(line) | line      (client to server)
//	frames:  u8 kind | u32 len | payload                 (both ways, until exit)
//
// streamMagic is all ones, a length no ordinary request has (maxField is
// smaller, and readField refuses a larger one), so the first eight bytes tell
// the two apart and an ordinary request keeps its format byte for byte. A
// frame's payload is at most maxFrame bytes; a longer write goes out as
// several frames, in order, and the bytes are never changed, added or dropped.
// The kinds, by direction:
//
//	client to server: frameStdin (payload: bytes of standard input),
//	                  frameStdinEOF (no payload: standard input has ended)
//	server to client: frameStdout, frameStderr (payload: bytes of the step's
//	                  output), frameExit (payload: i32 exit code; the last
//	                  frame the server sends)
//
// The server ends the step when the client's connection closes (the ssh
// session went away), which models srun cancelling its step on SIGHUP. A
// frame of the wrong kind for its direction, or another one after
// frameStdinEOF, ends the stream.

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

// maxField bounds every length-prefixed field: generous for a staged binary,
// small enough that a corrupt length cannot exhaust memory.
const maxField = 256 << 20

type request struct {
	line  string
	stdin []byte
	// stream is a streaming request (srun): stdin is not in it, but in the
	// frames that follow.
	stream bool
}

// streamMagic starts a streaming request, where an ordinary one has the
// length of its line (at most maxField).
const streamMagic = ^uint64(0)

// maxFrame bounds a frame's payload, so one frame is a bounded chunk of a
// stream and a corrupt length cannot exhaust memory.
const maxFrame = 64 << 10

// The kinds of frame.
const (
	frameStdin    byte = 1 // client to server
	frameStdinEOF byte = 2 // client to server
	frameStdout   byte = 3 // server to client
	frameStderr   byte = 4 // server to client
	frameExit     byte = 5 // server to client, the last
)

type reply struct {
	exit           int32
	stdout, stderr []byte
}

func writeField(w io.Writer, b []byte) error {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(b)))
	if _, err := w.Write(n[:]); err != nil {
		return err
	}
	if len(b) == 0 {
		// Nothing to send. A zero-length write to a socket whose peer has
		// already answered and closed fails with EPIPE, though the peer needed
		// nothing more.
		return nil
	}
	_, err := w.Write(b)
	return err
}

func readField(r io.Reader) ([]byte, error) {
	var n [8]byte
	if _, err := io.ReadFull(r, n[:]); err != nil {
		return nil, err
	}
	return readBody(r, binary.BigEndian.Uint64(n[:]))
}

// readBody reads a field whose length has been read.
func readBody(r io.Reader, size uint64) ([]byte, error) {
	if size > maxField {
		return nil, fmt.Errorf("a field of %d bytes is above the limit of %d", size, maxField)
	}
	b := make([]byte, size)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	return b, nil
}

func writeRequest(w io.Writer, q request) error {
	if err := writeField(w, []byte(q.line)); err != nil {
		return err
	}
	return writeField(w, q.stdin)
}

func writeStreamRequest(w io.Writer, line string) error {
	var m [8]byte
	binary.BigEndian.PutUint64(m[:], streamMagic)
	if _, err := w.Write(m[:]); err != nil {
		return err
	}
	return writeField(w, []byte(line))
}

func readRequest(r io.Reader) (request, error) {
	var n [8]byte
	if _, err := io.ReadFull(r, n[:]); err != nil {
		return request{}, fmt.Errorf("read the command line: %w", err)
	}
	if size := binary.BigEndian.Uint64(n[:]); size == streamMagic {
		line, err := readField(r)
		if err != nil {
			return request{}, fmt.Errorf("read the command line: %w", err)
		}
		return request{line: string(line), stream: true}, nil
	}
	line, err := readBody(r, binary.BigEndian.Uint64(n[:]))
	if err != nil {
		return request{}, fmt.Errorf("read the command line: %w", err)
	}
	stdin, err := readField(r)
	if err != nil {
		return request{}, fmt.Errorf("read standard input: %w", err)
	}
	return request{line: string(line), stdin: stdin}, nil
}

func writeReply(w io.Writer, p reply) error {
	var code [4]byte
	binary.BigEndian.PutUint32(code[:], uint32(p.exit))
	if _, err := w.Write(code[:]); err != nil {
		return err
	}
	if err := writeField(w, p.stdout); err != nil {
		return err
	}
	return writeField(w, p.stderr)
}

func readReply(r io.Reader) (reply, error) {
	var code [4]byte
	if _, err := io.ReadFull(r, code[:]); err != nil {
		return reply{}, fmt.Errorf("read the exit code: %w", err)
	}
	stdout, err := readField(r)
	if err != nil {
		return reply{}, fmt.Errorf("read standard output: %w", err)
	}
	stderr, err := readField(r)
	if err != nil {
		return reply{}, fmt.Errorf("read standard error: %w", err)
	}
	return reply{exit: int32(binary.BigEndian.Uint32(code[:])), stdout: stdout, stderr: stderr}, nil
}

// writeFrame writes one frame; p is at most maxFrame bytes.
func writeFrame(w io.Writer, kind byte, p []byte) error {
	if len(p) > maxFrame {
		return fmt.Errorf("a frame of %d bytes is above the limit of %d", len(p), maxFrame)
	}
	var h [5]byte
	h[0] = kind
	binary.BigEndian.PutUint32(h[1:], uint32(len(p)))
	if _, err := w.Write(h[:]); err != nil {
		return err
	}
	if len(p) == 0 {
		return nil
	}
	_, err := w.Write(p)
	return err
}

// readFrame reads one frame. A payload above maxFrame is refused.
func readFrame(r io.Reader) (kind byte, payload []byte, err error) {
	var h [5]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return 0, nil, err
	}
	size := binary.BigEndian.Uint32(h[1:])
	if size > maxFrame {
		return 0, nil, fmt.Errorf("a frame of %d bytes is above the limit of %d", size, maxFrame)
	}
	payload = make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return h[0], payload, nil
}

// frameWriter writes the frames of one stream from several goroutines (the two
// output copies and the final frames), each frame whole.
type frameWriter struct {
	mu sync.Mutex
	w  io.Writer
}

// data is an io.Writer that sends what it is given as frames of the kind,
// split at maxFrame.
func (f *frameWriter) data(kind byte) io.Writer { return frameData{f, kind} }

func (f *frameWriter) frame(kind byte, p []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return writeFrame(f.w, kind, p)
}

type frameData struct {
	f    *frameWriter
	kind byte
}

func (d frameData) Write(p []byte) (int, error) {
	for sent := 0; sent < len(p); {
		n := min(len(p)-sent, maxFrame)
		if err := d.f.frame(d.kind, p[sent:sent+n]); err != nil {
			return sent, err
		}
		sent += n
	}
	return len(p), nil
}
