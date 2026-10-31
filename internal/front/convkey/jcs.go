package convkey

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// This file is an RFC 8785 (JSON Canonicalization Scheme) serialiser for any
// JSON value: null, booleans, numbers, strings, arrays and objects. The
// opening hash needs all of them, because a message's content may be an array
// of parts with any members. pkg/deploymentid has its own serialiser for the
// string-only objects of HBEP-3; the string and key rules below are the same.

// value is a parsed JSON value.
type value interface {
	appendJCS(dst []byte) ([]byte, error)
}

type (
	jnull   struct{}
	jbool   bool
	jnumber string // the literal as sent; written in ES6 form
	jstring string
	jarray  []value
	jobject []member
)

type member struct {
	key string
	val value
}

var errNotJSON = errors.New("not a single JSON value")

// parse reads exactly one JSON value. A duplicate object key is an error, as
// RFC 8785 section 3.1 requires I-JSON input.
func parse(b []byte) (value, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	v, err := parseValue(dec)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errNotJSON
	}
	return v, nil
}

func parseValue(dec *json.Decoder) (value, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case nil:
		return jnull{}, nil
	case bool:
		return jbool(t), nil
	case json.Number:
		return jnumber(t), nil
	case string:
		return jstring(t), nil
	case json.Delim:
		if t == '[' {
			return parseArray(dec)
		}
		if t == '{' {
			return parseObject(dec)
		}
	}
	return nil, fmt.Errorf("unexpected token %v", tok)
}

func parseArray(dec *json.Decoder) (value, error) {
	var a jarray
	for dec.More() {
		v, err := parseValue(dec)
		if err != nil {
			return nil, err
		}
		a = append(a, v)
	}
	_, err := dec.Token() // ']'
	return a, err
}

func parseObject(dec *json.Decoder) (value, error) {
	var o jobject
	seen := map[string]bool{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		k, _ := tok.(string)
		if seen[k] {
			return nil, fmt.Errorf("duplicate object key %q", k)
		}
		seen[k] = true
		v, err := parseValue(dec)
		if err != nil {
			return nil, err
		}
		o = append(o, member{key: k, val: v})
	}
	_, err := dec.Token() // '}'
	return o, err
}

func (jnull) appendJCS(dst []byte) ([]byte, error) { return append(dst, "null"...), nil }

func (b jbool) appendJCS(dst []byte) ([]byte, error) {
	return strconv.AppendBool(dst, bool(b)), nil
}

func (n jnumber) appendJCS(dst []byte) ([]byte, error) {
	f, err := strconv.ParseFloat(string(n), 64)
	if err != nil {
		return nil, fmt.Errorf("number %s: %w", n, err)
	}
	s, err := formatES6(f)
	if err != nil {
		return nil, err
	}
	return append(dst, s...), nil
}

func (s jstring) appendJCS(dst []byte) ([]byte, error) { return appendString(dst, string(s)) }

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
		m     member
	}
	ks := make([]keyed, len(o))
	for i, m := range o {
		if !utf8.ValidString(m.key) {
			return nil, errors.New("object key is not valid UTF-8")
		}
		ks[i] = keyed{units: utf16.Encode([]rune(m.key)), m: m}
	}
	// RFC 8785 section 3.2.3: keys compare as arrays of UTF-16 code units.
	slices.SortFunc(ks, func(a, b keyed) int { return slices.Compare(a.units, b.units) })
	dst = append(dst, '{')
	for i, k := range ks {
		if i > 0 {
			dst = append(dst, ',')
		}
		var err error
		if dst, err = appendString(dst, k.m.key); err != nil {
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

// appendString writes s as RFC 8785 section 3.2.2.2 asks: U+0000..U+001F as
// \b \t \n \f \r or lowercase \u00xx, the quote and backslash escaped, and
// everything else, '/' and U+007F..U+FFFF included, as is.
func appendString(dst []byte, s string) ([]byte, error) {
	if !utf8.ValidString(s) {
		return nil, errors.New("string is not valid UTF-8")
	}
	dst = append(dst, '"')
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

// formatES6 writes a double as ECMAScript's Number.prototype.toString does
// (ECMA-262, Number::toString), which RFC 8785 section 3.2.2.3 requires: the
// shortest digits that round-trip, in fixed notation for exponents from -7 to
// 20 and in exponential notation otherwise; -0 is "0". NaN and infinities are
// not JSON and are an error.
func formatES6(f float64) (string, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return "", errors.New("number is not finite")
	}
	if f == 0 {
		return "0", nil
	}
	sign := ""
	if f < 0 {
		sign, f = "-", -f
	}
	// Go's shortest 'e' form: d[.ddd]e±XX, the same digits ES6 chooses.
	mant, expPart, _ := strings.Cut(strconv.FormatFloat(f, 'e', -1, 64), "e")
	digits := strings.Replace(mant, ".", "", 1)
	e, err := strconv.Atoi(expPart)
	if err != nil {
		return "", err
	}
	k, n := len(digits), e+1 // value = 0.digits x 10^n
	switch {
	case k <= n && n <= 21:
		return sign + digits + strings.Repeat("0", n-k), nil
	case 0 < n && n <= 21:
		return sign + digits[:n] + "." + digits[n:], nil
	case -6 < n && n <= 0:
		return sign + "0." + strings.Repeat("0", -n) + digits, nil
	}
	exp := n - 1
	es := "+"
	if exp < 0 {
		es, exp = "-", -exp
	}
	m := digits[:1]
	if k > 1 {
		m += "." + digits[1:]
	}
	return sign + m + "e" + es + strconv.Itoa(exp), nil
}

// canonical returns the RFC 8785 bytes of a JSON text.
func canonical(b []byte) ([]byte, error) {
	v, err := parse(b)
	if err != nil {
		return nil, err
	}
	return v.appendJCS(nil)
}
