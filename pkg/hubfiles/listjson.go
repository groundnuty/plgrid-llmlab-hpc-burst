package hubfiles

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

// treeEntry is one entry of the Hub's tree listing, as parseListing has
// checked its shape. Type and Path are always set; OID, Size and LFS only for
// an entry of type "file".
type treeEntry struct {
	Type string
	OID  string
	Size int64
	Path string
	LFS  *lfsInfo
}

type lfsInfo struct {
	OID  string
	Size int64
}

// parseListing reads one page of the tree listing, strictly. The path of a
// file is an input of the deployment ID, so a decoder that quietly repairs or
// reinterprets the text would give a successful answer for another file
// list. It refuses:
//
//   - a body that is not valid UTF-8, or that holds an escape of half a
//     surrogate pair ("\ud800"), which encoding/json would replace with
//     U+FFFD without an error;
//   - anything but exactly one JSON document, and a document that is not an
//     array of objects (a null, an object, a second array after the first);
//   - a key written in another case than the Hub's ("PATH" for "path"), a
//     key given twice, and a required key that is missing or null;
//   - a size that is not a non-negative integer, and, for an LFS file, a
//     top-level size that differs from lfs.size.
//
// Keys that this package does not read (xetHash, pointerSize) are ignored.
func parseListing(body []byte) ([]treeEntry, error) {
	if !utf8.Valid(body) {
		return nil, fmt.Errorf("%w: the page is not valid UTF-8", ErrInvalidListing)
	}
	if !json.Valid(body) { // also refuses data after the first document
		return nil, fmt.Errorf("%w: the page is not exactly one JSON document", ErrInvalidListing)
	}
	if t := bytes.TrimLeft(body, " \t\r\n"); len(t) == 0 || t[0] != '[' {
		return nil, fmt.Errorf("%w: the page is not a JSON array", ErrInvalidListing)
	}
	if err := checkSurrogateEscapes(body); err != nil {
		return nil, err
	}
	var elements []json.RawMessage
	if err := json.Unmarshal(body, &elements); err != nil {
		return nil, fmt.Errorf("%w: the page is not a JSON array: %v", ErrInvalidListing, err)
	}
	out := make([]treeEntry, 0, len(elements))
	for i, raw := range elements {
		obj, err := parseObject(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: entry %d: %v", ErrInvalidListing, i, err)
		}
		e, err := entryFromObject(obj)
		if err != nil {
			return nil, fmt.Errorf("%w: entry %d: %v", ErrInvalidListing, i, err)
		}
		out = append(out, e)
	}
	return out, nil
}

// parseObject decodes a JSON object into its members, keeping each value raw.
// Unlike decoding into a struct, it matches keys exactly and sees a key that
// is given twice.
func parseObject(raw []byte) (map[string]json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil, fmt.Errorf("not a JSON object")
	}
	obj := map[string]json.RawMessage{}
	for dec.More() {
		tok, err := dec.Token()
		key, ok := tok.(string)
		if err != nil || !ok {
			return nil, fmt.Errorf("an object member has no string key")
		}
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return nil, fmt.Errorf("member %q: %v", key, err)
		}
		if _, dup := obj[key]; dup {
			return nil, fmt.Errorf("the key %q is given twice", key)
		}
		obj[key] = v
	}
	return obj, nil
}

// checkKeys refuses a key that is a different spelling of a known one.
func checkKeys(obj map[string]json.RawMessage, known ...string) error {
	for k := range obj {
		for _, want := range known {
			if k != want && strings.EqualFold(k, want) {
				return fmt.Errorf("the key %q is not spelled %q", k, want)
			}
		}
	}
	return nil
}

func stringMember(obj map[string]json.RawMessage, key string) (string, error) {
	raw, ok := obj[key]
	if !ok {
		return "", fmt.Errorf("%q is missing", key)
	}
	var s string
	if len(raw) == 0 || raw[0] != '"' || json.Unmarshal(raw, &s) != nil {
		return "", fmt.Errorf("%q is not a string", key)
	}
	return s, nil
}

// sizeMember reads a required non-negative integer. A float, a string and a
// null are refused rather than read as 0.
func sizeMember(obj map[string]json.RawMessage, key string) (int64, error) {
	raw, ok := obj[key]
	if !ok {
		return 0, fmt.Errorf("%q is missing", key)
	}
	n, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%q is not an integer", key)
	}
	if n < 0 {
		return 0, fmt.Errorf("%q is negative", key)
	}
	return n, nil
}

func entryFromObject(obj map[string]json.RawMessage) (treeEntry, error) {
	var e treeEntry
	if err := checkKeys(obj, "type", "oid", "size", "path", "lfs"); err != nil {
		return e, err
	}
	var err error
	if e.Type, err = stringMember(obj, "type"); err != nil {
		return e, err
	}
	if e.Path, err = stringMember(obj, "path"); err != nil {
		return e, err
	}
	if e.Type != "file" {
		// A directory needs only its path. Any other type is refused later,
		// with its name, by toEntries.
		return e, nil
	}
	if e.OID, err = stringMember(obj, "oid"); err != nil {
		return e, fmt.Errorf("%q: %v", e.Path, err)
	}
	if e.Size, err = sizeMember(obj, "size"); err != nil {
		return e, fmt.Errorf("%q: %v", e.Path, err)
	}
	// "lfs": null is read as absent. Such a file is then downloaded, and a
	// real LFS file fails the blob id check, so it cannot pass unnoticed.
	if raw, ok := obj["lfs"]; ok && !bytes.Equal(raw, []byte("null")) {
		lfs, err := lfsFromRaw(raw)
		if err != nil {
			return e, fmt.Errorf("%q: lfs: %v", e.Path, err)
		}
		if lfs.Size != e.Size {
			return e, fmt.Errorf("%q: size %d differs from lfs.size %d", e.Path, e.Size, lfs.Size)
		}
		e.LFS = &lfs
	}
	return e, nil
}

func lfsFromRaw(raw []byte) (lfsInfo, error) {
	var l lfsInfo
	obj, err := parseObject(raw)
	if err != nil {
		return l, err
	}
	if err := checkKeys(obj, "oid", "size", "pointerSize"); err != nil {
		return l, err
	}
	if l.OID, err = stringMember(obj, "oid"); err != nil {
		return l, err
	}
	if l.Size, err = sizeMember(obj, "size"); err != nil {
		return l, err
	}
	return l, nil
}

// checkSurrogateEscapes refuses a \uD800-\uDFFF escape that is not half of a
// pair: a high surrogate that no low surrogate escape follows, or a low one
// with no high one before it. encoding/json turns such an escape into U+FFFD
// without an error, so the path read back would be another path. The scan
// tracks string state, so a backslash that is itself escaped (\\ud800) is
// text, not an escape. Same approach as checkSurrogateEscapes in
// pkg/deploymentid (chattemplate.go), which is unexported; the text is not
// shared so that this package depends only on that package's public API.
func checkSurrogateEscapes(raw []byte) error {
	inString := false
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		if !inString {
			inString = c == '"'
			continue
		}
		switch {
		case c == '"':
			inString = false
		case c == '\\' && i+1 < len(raw) && raw[i+1] != 'u':
			i++ // a two-character escape; the next byte is not a delimiter
		case c == '\\':
			u, ok := escapedUnit(raw, i)
			switch {
			case !ok:
				return fmt.Errorf("%w: the page has a malformed \\u escape", ErrInvalidListing)
			case u >= 0xDC00 && u <= 0xDFFF:
				return fmt.Errorf("%w: the page has an unpaired low surrogate escape", ErrInvalidListing)
			case u >= 0xD800 && u <= 0xDBFF:
				lo, ok := escapedUnit(raw, i+6)
				if !ok || lo < 0xDC00 || lo > 0xDFFF {
					return fmt.Errorf("%w: the page has an unpaired high surrogate escape", ErrInvalidListing)
				}
				i += 6 // the pair's second escape; the loop steps past its last digit
			}
			i += 5 // the escape's \u and four digits
		}
	}
	return nil
}

// escapedUnit reads the \uXXXX escape that starts at raw[i], hex digits of
// either case. ok is false when raw[i:] is not such an escape.
func escapedUnit(raw []byte, i int) (u rune, ok bool) {
	if i+6 > len(raw) || raw[i] != '\\' || raw[i+1] != 'u' {
		return 0, false
	}
	for _, d := range raw[i+2 : i+6] {
		switch {
		case d >= '0' && d <= '9':
			u = u<<4 | rune(d-'0')
		case d >= 'a' && d <= 'f':
			u = u<<4 | rune(d-'a'+10)
		case d >= 'A' && d <= 'F':
			u = u<<4 | rune(d-'A'+10)
		default:
			return 0, false
		}
	}
	return u, true
}
