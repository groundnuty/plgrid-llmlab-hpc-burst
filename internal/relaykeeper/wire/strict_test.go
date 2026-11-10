package wire_test

// New for the review of PR 354: keys are matched exactly, never twice, and a
// listing's age is bounded.

import (
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/relaykeeper/wire"
)

// encoding/json matches keys in any case, and keeps the last of two, which
// DisallowUnknownFields lets through. The strict decoding refuses a key in
// another case, alone or beside its own spelling, and a key twice, in the
// envelope, in a body and inside a body's nested objects; the exact
// spellings pass.
func TestTheStrictDecodingRefusesDuplicateAndCaseVariantKeys(t *testing.T) {
	for _, line := range []string{
		`{"v":1,"kind":"ping","Kind":"set"}`,
		`{"v":1,"KIND":"ping"}`,
		`{"v":1,"v":1,"kind":"ping"}`,
	} {
		if _, err := readLine(line + "\n"); !errors.Is(err, wire.ErrMalformed) {
			t.Errorf("Read(%s): %v, want ErrMalformed", line, err)
		}
	}
	a, _ := json.Marshal(validAck())
	good := string(a)
	for _, body := range []string{
		strings.Replace(good, `"leaseEpoch":`, `"LeaseEpoch":`, 1),
		strings.Replace(good, `"leaseEpoch":`, `"LeaseEpoch":9,"leaseEpoch":`, 1),
		strings.Replace(good, `"leaseEpoch":`, `"leaseEpoch":9,"leaseEpoch":`, 1),
	} {
		var got wire.Ack
		if err := (wire.Envelope{V: 1, Kind: wire.KindAck, Body: []byte(body)}).Decode(&got); !errors.Is(err, wire.ErrMalformed) {
			t.Errorf("an ack body %s: %v, want ErrMalformed", body, err)
		}
	}
	s, _ := json.Marshal(validSet())
	nested := strings.Replace(string(s), `"hostName":`, `"HostName":`, 1)
	var set wire.DesiredSet
	if err := (wire.Envelope{V: 1, Kind: wire.KindSet, Body: []byte(nested)}).Decode(&set); !errors.Is(err, wire.ErrMalformed) {
		t.Errorf("a set with a nested key in another case: %v, want ErrMalformed", err)
	}
	var ok wire.Ack
	if err := (wire.Envelope{V: 1, Kind: wire.KindAck, Body: a}).Decode(&ok); err != nil || ok != validAck() {
		t.Errorf("the exact spellings: %v, %+v", err, ok)
	}
	if err := (wire.Envelope{V: 1, Kind: wire.KindSet, Body: s}).Decode(&set); err != nil {
		t.Errorf("a set's exact spellings: %v", err)
	}
}

// A listing's age is bounded (wire.MaxListingAge), so the keeper's arithmetic
// on it cannot overflow: 2^58 and MaxInt64 are refused, naming the field;
// the bound itself passes.
func TestAListingAgeIsBounded(t *testing.T) {
	for _, age := range []int64{wire.MaxListingAge.Milliseconds() + 1, 1 << 58, math.MaxInt64} {
		s := validSet()
		s.ListingAgeMS = age
		if err := s.Validate(); err == nil || !strings.Contains(err.Error(), "listingAgeMs") {
			t.Errorf("an age of %d: %v, want refused, naming listingAgeMs", age, err)
		}
	}
	s := validSet()
	s.ListingAgeMS = wire.MaxListingAge.Milliseconds()
	if err := s.Validate(); err != nil {
		t.Errorf("an age at the bound: %v", err)
	}
}
