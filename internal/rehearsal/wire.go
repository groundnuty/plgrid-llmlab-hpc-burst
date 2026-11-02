package rehearsal

// New. The relay's wire format: one request and one reply per unix-socket
// connection, each field length-prefixed raw bytes (no base64: a staged agent
// binary of tens of MB goes through standard input).
//
//	request: u64 len(line) | line | u64 len(stdin) | stdin
//	reply:   i32 exit code | u64 len(stdout) | stdout | u64 len(stderr) | stderr
//
// Integers are big-endian. A field above maxField is refused on both ends.

import (
	"encoding/binary"
	"fmt"
	"io"
)

// maxField bounds every length-prefixed field: generous for a staged binary,
// small enough that a corrupt length cannot exhaust memory.
const maxField = 256 << 20

type request struct {
	line  string
	stdin []byte
}

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
	size := binary.BigEndian.Uint64(n[:])
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

func readRequest(r io.Reader) (request, error) {
	line, err := readField(r)
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
