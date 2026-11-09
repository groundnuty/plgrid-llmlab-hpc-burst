package stack

// What a catch-up found (catchup.go).

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// Behind is one object the watch has not caught up on.
type Behind struct {
	Kind      Kinds
	Name      string
	UID       types.UID
	Listed    string // the resourceVersion the API listed; empty when the API no longer has the object
	Delivered string // the newest resourceVersion the watch delivered; empty when none
}

// Explain says what the watch is behind on.
func (b Behind) Explain() string {
	switch {
	case b.Listed == "":
		return fmt.Sprintf("%s %s: gone from the API, and the watch delivered no deletion (its newest version is %s)", b.Kind.label(), b.Name, b.Delivered)
	case b.Delivered == "":
		return fmt.Sprintf("%s %s: the API lists version %s, and the watch delivered none", b.Kind.label(), b.Name, b.Listed)
	}
	return fmt.Sprintf("%s %s: the API lists version %s, and the watch delivered up to %s", b.Kind.label(), b.Name, b.Listed, b.Delivered)
}

// CatchUpResult is what a catch-up found.
type CatchUpResult struct {
	// Waited is how long the watch trailed the API, zero when it did not.
	Waited time.Duration
	// Missing are the objects whose events did not come, on a stream that
	// reached the LIST's version without them: lost events, found by this
	// catch-up.
	Missing []Behind
	// EarlierLost are the objects an earlier catch-up found an event lost on
	// and reported: this one did not wait for them, and does not report them.
	EarlierLost []Behind
	// Unknown says why it could not be told whether the watch is behind: the
	// API could not be listed, a version did not parse, the wait was cut short,
	// a stream did not reach the LIST's version, with what it has yet to deliver,
	// or a stream was reset. Empty otherwise.
	Unknown string
}

// OK reports whether the watch holds everything the API has shown.
func (r CatchUpResult) OK() bool {
	return len(r.Missing) == 0 && len(r.EarlierLost) == 0 && r.Unknown == ""
}

// Lost reports whether a catch-up, this one or an earlier one, found the
// object's event lost.
func (r CatchUpResult) Lost(uid types.UID) bool {
	for _, b := range slices.Concat(r.Missing, r.EarlierLost) {
		if b.UID == uid {
			return true
		}
	}
	return false
}

// Problem is what to report of the catch-up, empty when there is nothing new:
// an object an earlier catch-up reported lost is not reported again.
func (r CatchUpResult) Problem() string {
	var parts []string
	for _, b := range r.Missing {
		parts = append(parts, b.Explain())
	}
	lost := ""
	if len(parts) > 0 {
		lost = fmt.Sprintf("the watch delivered nothing newer in %s after the API listed it: %s: the event was lost", r.Waited.Round(time.Millisecond), strings.Join(parts, "; "))
	}
	switch {
	case r.Unknown != "" && lost != "":
		return "whether the watch has caught up with the API is not known: " + r.Unknown + "; and " + lost
	case r.Unknown != "":
		return "whether the watch has caught up with the API is not known: " + r.Unknown
	}
	return lost
}

// Err is the catch-up's outcome for a caller that reports a result of its own
// from the record: what Problem says, or that an earlier catch-up found an event
// of the record lost, which a result read from it cannot hide. nil when OK.
func (r CatchUpResult) Err(what string) error {
	if p := r.Problem(); p != "" {
		return fmt.Errorf("%s: %s", what, p)
	}
	if len(r.EarlierLost) == 0 {
		return nil
	}
	var parts []string
	for _, b := range r.EarlierLost {
		parts = append(parts, b.Explain())
	}
	return fmt.Errorf("%s: an earlier catch-up found an event lost: %s", what, strings.Join(parts, "; "))
}
