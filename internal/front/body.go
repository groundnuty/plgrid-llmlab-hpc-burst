package front

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

// requestBody is what the front reads of a request body. It never
// re-serialises the body: the one change is a splice of the top-level
// "model" value's bytes (HBEP-5, Contract, The one change to a request body).
type requestBody struct {
	model                string
	modelStart, modelEnd int // the value's literal, quotes included
	stream               bool
	includeUsage         bool
	messages             []byte // the raw "messages" value; nil when absent
}

var (
	errNotObject      = errors.New("the body is not a JSON object")
	errNoModel        = errors.New("the body has no string model")
	errDuplicateModel = errors.New("the body has more than one top-level model member")
)

// parseBody reads a JSON object's top-level members. A member name counts
// after JSON unescaping, so "model" is the model member; a second
// top-level model member is refused, because parsers disagree on which one
// wins. Members nested in other values are never read as the model.
func parseBody(b []byte) (requestBody, error) {
	var rb requestBody
	dec := json.NewDecoder(bytes.NewReader(b))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return rb, errNotObject
	}
	found := false
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return rb, errNotObject
		}
		name, _ := tok.(string)
		keyEnd := int(dec.InputOffset())
		if name == "model" {
			if found {
				return rb, errDuplicateModel
			}
			found = true
			if rb.model, rb.modelStart, rb.modelEnd, err = readModel(dec, b, keyEnd); err != nil {
				return rb, err
			}
			continue
		}
		if err := readMember(dec, name, &rb); err != nil {
			return rb, errNotObject
		}
	}
	if _, err := dec.Token(); err != nil { // the closing brace
		return rb, errNotObject
	}
	if _, err := dec.Token(); err != io.EOF {
		return rb, errNotObject
	}
	if !found {
		return rb, errNoModel
	}
	return rb, nil
}

// readModel reads the model member's value, which must be a string, and
// returns its byte span: the value's literal starts at the first quote after
// the member name, since only white space and the colon come between.
func readModel(dec *json.Decoder, b []byte, keyEnd int) (string, int, int, error) {
	tok, err := dec.Token()
	if err != nil {
		return "", 0, 0, errNotObject
	}
	s, ok := tok.(string)
	if !ok {
		return "", 0, 0, errNoModel
	}
	end := int(dec.InputOffset())
	start := bytes.IndexByte(b[keyEnd:end], '"')
	if start < 0 {
		return "", 0, 0, errNotObject
	}
	return s, keyEnd + start, end, nil
}

func readMember(dec *json.Decoder, name string, rb *requestBody) error {
	switch name {
	case "stream":
		var v any
		if err := dec.Decode(&v); err != nil {
			return err
		}
		rb.stream = v == true
	case "stream_options":
		var v struct {
			IncludeUsage bool `json:"include_usage"`
		}
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return err
		}
		if json.Unmarshal(raw, &v) == nil {
			rb.includeUsage = v.IncludeUsage
		}
	case "messages":
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return err
		}
		rb.messages = raw
	default:
		var raw json.RawMessage
		return dec.Decode(&raw)
	}
	return nil
}

// rewrite returns the body with the model value's literal replaced by served,
// written as a JSON string. Every other byte is the client's.
func (rb requestBody) rewrite(b []byte, served string) []byte {
	lit := quote(served)
	out := make([]byte, 0, len(b)-(rb.modelEnd-rb.modelStart)+len(lit))
	out = append(out, b[:rb.modelStart]...)
	out = append(out, lit...)
	return append(out, b[rb.modelEnd:]...)
}

// quote writes s as a JSON string without HTML escaping.
func quote(s string) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s)
	return bytes.TrimRight(buf.Bytes(), "\n")
}
