package wire

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"
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

// DecodeStrict is the codec's strict decoding, for other files of the keeper
// (its state file): exactly one JSON value, then only white space; every key
// spelled exactly as its field's JSON name, so no unknown field, and no key
// that encoding/json would match only case-insensitively; and no key twice in
// one object, nor two that differ only in case, where encoding/json would let
// the last one win.
func DecodeStrict(b []byte, v any) error { return strict(b, v) }

// strict decodes exactly one JSON value from b into v (DecodeStrict).
func strict(b []byte, v any) error {
	if err := noDuplicateKeys(b); err != nil {
		return err
	}
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
	var generic any
	if err := json.Unmarshal(b, &generic); err != nil {
		return err
	}
	return exactKeys(generic, reflect.TypeOf(v))
}

// noDuplicateKeys refuses a key that appears twice in one object, in any
// case: encoding/json matches keys case-insensitively and keeps the last.
func noDuplicateKeys(b []byte) error {
	d := json.NewDecoder(bytes.NewReader(b))
	var walk func() error
	walk = func() error {
		tok, err := d.Token()
		if err != nil {
			return err
		}
		switch tok {
		case json.Delim('{'):
			var seen []string
			for d.More() {
				k, err := d.Token()
				if err != nil {
					return err
				}
				key, _ := k.(string)
				for _, s := range seen {
					if strings.EqualFold(s, key) {
						return fmt.Errorf("the key %q appears twice in one object (keys are matched in any case)", key)
					}
				}
				seen = append(seen, key)
				if err := walk(); err != nil {
					return err
				}
			}
			_, err := d.Token() // '}'
			return err
		case json.Delim('['):
			for d.More() {
				if err := walk(); err != nil {
					return err
				}
			}
			_, err := d.Token() // ']'
			return err
		}
		return nil
	}
	return walk()
}

// exactKeys refuses a key of v's JSON that is not exactly a field's JSON name
// in t, at every depth: DisallowUnknownFields lets a case variant through.
func exactKeys(v any, t reflect.Type) error {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t {
	case reflect.TypeOf(json.RawMessage{}), reflect.TypeOf(time.Time{}):
		return nil
	}
	switch t.Kind() {
	case reflect.Struct:
		obj, ok := v.(map[string]any)
		if !ok {
			return nil // a null, or a type error encoding/json reported already
		}
		fields := map[string]reflect.Type{}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() {
				continue
			}
			name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
			if name == "-" {
				continue
			}
			if name == "" {
				name = f.Name
			}
			fields[name] = f.Type
		}
		for k, sub := range obj {
			ft, ok := fields[k]
			if !ok {
				return fmt.Errorf("the key %q is not one of this object's (keys are case-sensitive)", k)
			}
			if err := exactKeys(sub, ft); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		if arr, ok := v.([]any); ok {
			for _, sub := range arr {
				if err := exactKeys(sub, t.Elem()); err != nil {
					return err
				}
			}
		}
	case reflect.Map:
		if obj, ok := v.(map[string]any); ok {
			for _, sub := range obj {
				if err := exactKeys(sub, t.Elem()); err != nil {
					return err
				}
			}
		}
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
