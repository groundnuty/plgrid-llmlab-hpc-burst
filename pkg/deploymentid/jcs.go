package deploymentid

import (
	"errors"
	"fmt"
	"slices"
	"unicode/utf16"
	"unicode/utf8"
)

// This file is a small RFC 8785 (JSON Canonicalization Scheme) serialiser. It
// covers exactly the value types scheme v1 uses: objects, arrays and strings.
// HBEP-3 makes every scalar a string, so numbers, booleans and null never
// occur and are not implemented.
//
// encoding/json is not used for output: it escapes <, >, & and U+2028/U+2029,
// and it replaces invalid UTF-8 with U+FFFD. RFC 8785 section 3.2.2.2 keeps
// those characters as they are and requires an error for data that is not
// valid Unicode.

// errInvalidJSONString reports a string that is not valid UTF-8, the Go form
// of the lone surrogates that RFC 8785 section 3.2.2.2 says must be an error.
var errInvalidJSONString = errors.New("string is not valid UTF-8")

// jvalue is a JSON value the serialiser can write.
type jvalue interface {
	appendJCS(dst []byte) ([]byte, error)
}

// jstring is a JSON string.
type jstring string

// jarray is a JSON array. Its element order is kept (RFC 8785 section 3.2.3).
type jarray []jvalue

// jmember is one property of a jobject.
type jmember struct {
	key string
	val jvalue
}

// jobject is a JSON object. The members may be given in any order; they are
// sorted when written.
type jobject []jmember

// canonicalize writes v as RFC 8785 canonical JSON, UTF-8 encoded.
func canonicalize(v jvalue) ([]byte, error) {
	return v.appendJCS(nil)
}

func (s jstring) appendJCS(dst []byte) ([]byte, error) {
	return appendJCSString(dst, string(s))
}

func (a jarray) appendJCS(dst []byte) ([]byte, error) {
	dst = append(dst, '[')
	for i, e := range a {
		if i > 0 {
			dst = append(dst, ',')
		}
		var err error
		if dst, err = e.appendJCS(dst); err != nil {
			return nil, err
		}
	}
	return append(dst, ']'), nil
}

func (o jobject) appendJCS(dst []byte) ([]byte, error) {
	type keyed struct {
		units []uint16
		m     jmember
	}
	ks := make([]keyed, len(o))
	for i, m := range o {
		if !utf8.ValidString(m.key) {
			return nil, fmt.Errorf("object key: %w", errInvalidJSONString)
		}
		ks[i] = keyed{units: utf16.Encode([]rune(m.key)), m: m}
	}
	// RFC 8785 section 3.2.3: keys are compared as arrays of UTF-16 code
	// units, as unsigned integers. That differs from a bytewise comparison of
	// the UTF-8 for supplementary-plane characters against U+E000..U+FFFF.
	slices.SortFunc(ks, func(a, b keyed) int { return slices.Compare(a.units, b.units) })
	for i := 1; i < len(ks); i++ {
		if slices.Equal(ks[i-1].units, ks[i].units) {
			return nil, fmt.Errorf("duplicate object key %q", ks[i].m.key)
		}
	}
	dst = append(dst, '{')
	for i, k := range ks {
		if i > 0 {
			dst = append(dst, ',')
		}
		var err error
		if dst, err = appendJCSString(dst, k.m.key); err != nil {
			return nil, err
		}
		dst = append(dst, ':')
		if dst, err = k.m.val.appendJCS(dst); err != nil {
			return nil, err
		}
	}
	return append(dst, '}'), nil
}

const lowerHex = "0123456789abcdef"

// appendJCSString writes s as an RFC 8785 string (section 3.2.2.2): the
// characters U+0000..U+001F as \b \t \n \f \r or lowercase \u00xx, the quote
// and the backslash escaped, everything else, '/' and U+007F and U+2028
// included, written as is.
func appendJCSString(dst []byte, s string) ([]byte, error) {
	if !utf8.ValidString(s) {
		return nil, errInvalidJSONString
	}
	dst = append(dst, '"')
	// Every character that needs an escape is ASCII, and no byte of a
	// multi-byte UTF-8 sequence is below 0x80, so a byte loop is exact.
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '"':
			dst = append(dst, '\\', '"')
		case c == '\\':
			dst = append(dst, '\\', '\\')
		case c == '\b':
			dst = append(dst, '\\', 'b')
		case c == '\t':
			dst = append(dst, '\\', 't')
		case c == '\n':
			dst = append(dst, '\\', 'n')
		case c == '\f':
			dst = append(dst, '\\', 'f')
		case c == '\r':
			dst = append(dst, '\\', 'r')
		case c < 0x20:
			dst = append(dst, '\\', 'u', '0', '0', lowerHex[c>>4], lowerHex[c&0xf])
		default:
			dst = append(dst, c)
		}
	}
	return append(dst, '"'), nil
}
