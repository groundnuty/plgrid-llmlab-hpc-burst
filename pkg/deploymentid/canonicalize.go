package deploymentid

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

// Canonicalize returns the RFC 8785 text of a JSON value of scheme v1's types: objects, arrays and strings
// (HBEP-3, the v1 canonical object). It turns a stored canonical object back into the bytes its ID hashes:
// the records' writers store it through encoding/json, which escapes <, >, &, U+2028 and U+2029, so the
// stored text is JSON-equivalent to the canonical form but not byte-exact. The front's record fetch serves
// its result (HBEP-9, the record fetch; science-agent ruling on #585's plan, 15 Nov 2026).
//
// A number, a boolean or null, a duplicate key, invalid UTF-8, an unpaired surrogate escape (RFC 8785
// section 3.2.2.2) and anything after the value are errors.
func Canonicalize(data []byte) ([]byte, error) {
	if !utf8.Valid(data) {
		return nil, errInvalidJSONString
	}
	// encoding/json decodes an unpaired surrogate escape as U+FFFD, silently; RFC 8785 needs an error.
	if p := unpairedSurrogate(data); p != "" {
		return nil, fmt.Errorf("the JSON text has %s (RFC 8785 section 3.2.2.2)", p)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	v, err := readJValue(dec)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errors.New("data follows the JSON value")
	}
	return canonicalize(v) // a duplicate key is refused here
}

// readJValue reads the next JSON value from dec.
func readJValue(dec *json.Decoder) (jvalue, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case string:
		return jstring(t), nil
	case json.Delim:
		switch t {
		case '{':
			var o jobject
			for dec.More() {
				k, err := dec.Token()
				if err != nil {
					return nil, err
				}
				v, err := readJValue(dec)
				if err != nil {
					return nil, err
				}
				o = append(o, jmember{key: k.(string), val: v}) // an object's keys are strings
			}
			_, err := dec.Token() // the closing brace
			return o, err
		case '[':
			a := jarray{}
			for dec.More() {
				v, err := readJValue(dec)
				if err != nil {
					return nil, err
				}
				a = append(a, v)
			}
			_, err := dec.Token() // the closing bracket
			return a, err
		}
	}
	return nil, fmt.Errorf("the JSON value %v is not an object, an array or a string, the only types of scheme v1", tok)
}
