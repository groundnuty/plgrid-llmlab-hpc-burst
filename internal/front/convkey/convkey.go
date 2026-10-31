// Package convkey computes the front's conversation key (HBEP-5,
// Conversation key): the client's session header when it sends one, else the
// hash of the conversation's opening, else a fresh key per request. The same
// string is the key on the front's ring and the x-session-id the front sends
// to an Instance.
package convkey

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
)

// Prefixes of the keys the front makes. o1 names the opening rule, so a later
// rule shows in logs and moves each conversation once (HBEP-5, Compatibility).
const (
	OpeningPrefix = "o1-"
	RandomPrefix  = "r-"
)

// DefaultSessionHeaders is the default of the front's sessionHeaders.
var DefaultSessionHeaders = []string{"x-session-id"}

// Source says which rule produced a key.
type Source string

// Sources.
const (
	SourceHeader  Source = "header"
	SourceOpening Source = "opening"
	SourceRandom  Source = "random"
)

// Key returns a request's conversation key. headers are the request's
// headers, names the honoured session headers in order, and messages the raw
// value of the body's top-level "messages" member (nil when absent).
//
// An opening that cannot be hashed (not an array of objects, or not valid
// I-JSON, such as a duplicate key) gets a fresh key like a request without
// messages: HBEP-5 does not name that case, and no affinity is the
// conservative reading.
func Key(headers http.Header, names []string, messages []byte) (string, Source) {
	if k, ok := FromHeader(headers, names); ok {
		return k, SourceHeader
	}
	if messages != nil {
		if k, err := Opening(messages); err == nil {
			return k, SourceOpening
		}
	}
	return Random(), SourceRandom
}

// FromHeader returns the value of the first header in names that the request
// carries with a non-empty value, unchanged.
func FromHeader(headers http.Header, names []string) (string, bool) {
	for _, n := range names {
		for _, v := range headers.Values(n) {
			if v != "" {
				return v, true
			}
		}
	}
	return "", false
}

var errNoOpening = errors.New("convkey: messages is not a non-empty array of objects")

// Opening returns the opening hash of a messages array: the messages from the
// first up to and including the first whose role is "user" (all of them when
// none is), each reduced to exactly its role and its content as sent (an
// absent member counts as null), serialised by RFC 8785 and hashed with
// SHA-256, as "o1-" and 64 lowercase hex characters.
func Opening(messages []byte) (string, error) {
	v, err := parse(messages)
	if err != nil {
		return "", err
	}
	arr, ok := v.(jarray)
	if !ok || len(arr) == 0 {
		return "", errNoOpening
	}
	var opening jarray
	for _, m := range arr {
		obj, ok := m.(jobject)
		if !ok {
			return "", errNoOpening
		}
		role, content := memberOr(obj, "role"), memberOr(obj, "content")
		opening = append(opening, jobject{{key: "role", val: role}, {key: "content", val: content}})
		if s, ok := role.(jstring); ok && s == "user" {
			break
		}
	}
	b, err := opening.appendJCS(nil)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return OpeningPrefix + hex.EncodeToString(sum[:]), nil
}

func memberOr(o jobject, key string) value {
	for _, m := range o {
		if m.key == key {
			return m.val
		}
	}
	return jnull{}
}

// Random returns a fresh key with no affinity: "r-" and 32 random lowercase
// hex characters.
func Random() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand does not fail on Linux; a key must still be unique.
		panic("convkey: crypto/rand: " + err.Error())
	}
	return RandomPrefix + hex.EncodeToString(b[:])
}
