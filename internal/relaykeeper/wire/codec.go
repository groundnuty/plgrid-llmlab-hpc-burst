package wire

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// ErrTooLong is a line longer than MaxMessage.
var ErrTooLong = fmt.Errorf("a message longer than %d bytes", MaxMessage)

// ErrMalformed is a line that is not one envelope of a known form.
var ErrMalformed = errors.New("a malformed message")

// VersionError is an envelope of another protocol version.
type VersionError struct{ Got int }

func (e *VersionError) Error() string {
	return fmt.Sprintf("protocol version %d; this keeper serves %d", e.Got, Version)
}

// NewReader is a reader for Read: its buffer holds one whole line.
func NewReader(r io.Reader) *bufio.Reader { return bufio.NewReaderSize(r, MaxMessage+1) }

// Write writes one message, kind with body, as one line.
func Write(w io.Writer, kind Kind, body any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	line, err := json.Marshal(Envelope{V: Version, Kind: kind, Body: b})
	if err != nil {
		return err
	}
	if len(line) > MaxMessage {
		return ErrTooLong
	}
	_, err = w.Write(append(line, '\n'))
	return err
}

// Read reads one message from r, a reader from NewReader. A line longer than
// MaxMessage is ErrTooLong. An object whose "v" names another version is
// returned with a *VersionError whatever else it holds, so its sender can be
// told which versions are served, even when its envelope has fields this
// version does not know. Anything else that is not an envelope of this
// version and a known kind, with no other field and nothing after it,
// ErrMalformed, an envelope with no version included.
func Read(r *bufio.Reader) (Envelope, error) {
	line, err := r.ReadSlice('\n')
	switch {
	case errors.Is(err, bufio.ErrBufferFull):
		return Envelope{}, ErrTooLong
	case err != nil:
		return Envelope{}, err
	}
	var head struct {
		V    *int `json:"v"`
		Kind Kind `json:"kind"`
	}
	if json.Unmarshal(line, &head) == nil && head.V != nil && *head.V != Version {
		return Envelope{V: *head.V, Kind: head.Kind}, &VersionError{Got: *head.V}
	}
	var e Envelope
	if err := strict(line, &e); err != nil {
		return Envelope{}, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if head.V == nil || e.V != Version {
		return Envelope{}, fmt.Errorf("%w: no protocol version", ErrMalformed)
	}
	if !known(e.Kind) {
		return Envelope{}, fmt.Errorf("%w: kind %q", ErrMalformed, e.Kind)
	}
	return e, nil
}

// Decode decodes the envelope's body into v, refusing unknown fields: a new
// field needs a new version.
func (e Envelope) Decode(v any) error {
	body := e.Body
	if len(body) == 0 {
		body = []byte("{}")
	}
	if err := strict(body, v); err != nil {
		return fmt.Errorf("%w: the %s body: %v", ErrMalformed, e.Kind, err)
	}
	return nil
}

// strict decodes exactly one JSON value from b into v, refusing unknown
// fields and anything after the value.
func strict(b []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	// Only white space may follow: Decoder.More is false before a '}' or a
	// ']', so it would let one through (found by the package's tests).
	var after json.RawMessage
	if err := d.Decode(&after); !errors.Is(err, io.EOF) {
		return errors.New("something after the value")
	}
	return nil
}

func known(k Kind) bool {
	switch k {
	case KindPing, KindPong, KindSet, KindReport, KindAck, KindAcked, KindStatus, KindStatusReport, KindRefused:
		return true
	}
	return false
}
