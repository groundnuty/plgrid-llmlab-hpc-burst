package stack

// How far each of the Recorder's streams has come, whether one was reset (Follow,
// catchup.go), and which reads' windows crossed a reset.

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/resourceversion"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// streamState is how far one kind's stream has come.
type streamState struct {
	reached  string          // the newest version it reached, an event's or a bookmark's
	progress time.Time       // when it last passed a version on
	resets   []streamReset   // each time it started again from the current versions
	crossed  map[string]bool // the reads whose windows crossed a reset, by name
}

// streamReset is one time a stream started again from the current versions.
type streamReset struct {
	why  string
	from time.Time // the stream's last progress before it: what came after that can be gone
	to   time.Time // when the LIST that started it again was passed on; zero until then
}

// crossedBy reports whether a read of what arrived from from to to (zero from: from
// the record's start; zero to: until now) can have missed what the reset lost. Only
// a window wholly before the reset's gap or wholly after it cannot.
func (r streamReset) crossedBy(from, to time.Time) bool {
	before := !to.IsZero() && to.Before(r.from)
	after := !from.IsZero() && !r.to.IsZero() && from.After(r.to)
	return !before && !after
}

// noteRead keeps the read's name if its window crosses a reset of the kind's
// stream; the caller holds rec.mu. A read made before a reset crosses nothing, and
// finds none here. The names are few, so a polling read keeps no more than one.
func (rec *Recorder) noteRead(kind Kinds, what string, from, to time.Time) {
	s := rec.streams[kind]
	if s == nil {
		return
	}
	if slices.ContainsFunc(s.resets, func(r streamReset) bool { return r.crossedBy(from, to) }) {
		if s.crossed == nil {
			s.crossed = map[string]bool{}
		}
		s.crossed[what] = true
	}
}

// stream is what Follow passes on for the kind.
func (rec *Recorder) stream(kind Kinds, event func(watch.EventType, client.Object)) Stream {
	return Stream{
		Event: event,
		Reached: func(rv string) {
			rec.mu.Lock()
			defer rec.mu.Unlock()
			s := rec.streamOf(kind)
			now := time.Now()
			s.progress = now
			if n := len(s.resets); n > 0 && s.resets[n-1].to.IsZero() {
				s.resets[n-1].to = now // the LIST after the reset: Follow reaches nothing before it
			}
			// Never back: the current versions a connection starts with come in
			// no order. One that does not parse stays, for a catch-up to report,
			// only when there is nothing better.
			if c, err := resourceversion.CompareResourceVersion(rv, s.reached); s.reached == "" || (err == nil && c > 0) {
				s.reached = rv
			}
		},
		Reset: func(why string) {
			rec.mu.Lock()
			defer rec.mu.Unlock()
			s := rec.streamOf(kind)
			s.resets = append(s.resets, streamReset{why: why, from: s.progress})
		},
	}
}

// streamOf is the kind's state; the caller holds rec.mu.
func (rec *Recorder) streamOf(kind Kinds) *streamState {
	if rec.streams == nil {
		rec.streams = map[Kinds]*streamState{}
	}
	if rec.streams[kind] == nil {
		rec.streams[kind] = &streamState{}
	}
	return rec.streams[kind]
}

// resets says which of the kinds' streams were reset, why, and whether any read's
// window crossed it; empty with none.
func (rec *Recorder) resets(kinds Kinds) string {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return rec.resetsLocked(kinds)
}

// resetsLocked is resets; the caller holds rec.mu.
func (rec *Recorder) resetsLocked(kinds Kinds) string {
	var parts []string
	for _, kind := range allKinds {
		s := rec.streams[kind]
		if kinds&kind == 0 || s == nil || len(s.resets) == 0 {
			continue
		}
		var whys []string
		for _, r := range s.resets {
			whys = append(whys, r.why)
		}
		crossed := "no read's window crossed it"
		if len(s.crossed) > 0 {
			crossed = "these reads' windows crossed it: " + strings.Join(slices.Sorted(maps.Keys(s.crossed)), ", ")
		}
		parts = append(parts, fmt.Sprintf("the %s watch was reset (%s), so that versions before its current ones can be gone, and %s", kind.label(), strings.Join(whys, "; "), crossed))
	}
	return strings.Join(parts, "; ")
}

// ResetCount is how many times the Recorder's streams started again from the
// current versions: a measured fact, for a row's post. It reads no version, so it
// is no read of the watch (inventory.go). None for no watch.
func (rec *Recorder) ResetCount() int {
	if rec == nil {
		return 0
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	n := 0
	for _, s := range rec.streams {
		n += len(s.resets)
	}
	return n
}
