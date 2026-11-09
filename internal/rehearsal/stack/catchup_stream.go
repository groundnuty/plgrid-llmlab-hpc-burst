package stack

// How far each of the Recorder's streams has come, and whether one was reset
// (Follow, catchup.go).

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/resourceversion"
	"k8s.io/apimachinery/pkg/watch"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// streamState is how far one kind's stream has come.
type streamState struct {
	reached string   // the newest version it reached, an event's or a bookmark's
	resets  []string // why it started again from the current versions, each time
}

// stream is what Follow passes on for the kind.
func (rec *Recorder) stream(kind Kinds, event func(watch.EventType, client.Object)) Stream {
	return Stream{
		Event: event,
		Reached: func(rv string) {
			rec.mu.Lock()
			defer rec.mu.Unlock()
			s := rec.streamOf(kind)
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
			s.resets = append(s.resets, why)
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

// resets says which of the kinds' streams were reset and why, empty with none.
func (rec *Recorder) resets(kinds Kinds) string {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var parts []string
	for _, kind := range allKinds {
		if s := rec.streams[kind]; kinds&kind != 0 && s != nil && len(s.resets) > 0 {
			parts = append(parts, fmt.Sprintf("the %s watch was reset (%s), so that versions before its current ones can be gone", kind.label(), strings.Join(s.resets, "; ")))
		}
	}
	return strings.Join(parts, "; ")
}
