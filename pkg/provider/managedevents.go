package provider

import (
	"sync"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
)

// The form of the events a managed Provider's controller raises, and the rate
// rule for the ones that repeat, in one place so the real controller
// (internal/managed) and its stand-in (pkg/provider/fake.ManagedController)
// cannot drift (HBEP-8, Events: Form and Repeating conditions;
// @hpc-burst-science-agent, 31 Oct 2026).

// The types an event can have, which go in its action (HBEP-8, Events: Form).
// A managed Provider's controller raises degraded, failing and ready.
const (
	EventActionReady    = "ready"
	EventActionDegraded = "degraded"
	EventActionFailing  = "failing"
)

const (
	// EventRepeatInterval is the least time between two emissions of an event
	// for a condition that persists (HBEP-8, Repeating conditions).
	EventRepeatInterval = time.Hour
	// MaxEventNote is the longest note, in bytes, the API accepts (HBEP-8,
	// Events: Form). Every emitter cuts its note to it.
	MaxEventNote = 1024
	// MaxSkipEventsPerHour is the most CatalogEntrySkipped events one
	// Provider's controller raises for individual entries in an hour; the rest
	// are counted in one summary event (@hpc-burst-science-agent, 31 Oct 2026).
	MaxSkipEventsPerHour = 10
)

// ManagedEvent is one event of a managed Provider's controller, ready for
// events.EventRecorder.Eventf(provider, nil, Type, Reason, Action, "%s", Note).
type ManagedEvent struct {
	// Type is the Kubernetes event type: Warning for degraded and failing,
	// Normal for the others.
	Type string
	// Reason is the CamelCase code alone, such as CatalogTruncated.
	Reason string
	// Action is HBEP-8's type: one of the EventAction constants.
	Action string
	// Note is the human message, at most MaxEventNote bytes.
	Note string
}

// NewManagedEvent returns an event of HBEP-8's type action with the given
// reason code, its Kubernetes type set from the action and its note cut to
// MaxEventNote bytes.
func NewManagedEvent(action, reason, note string) ManagedEvent {
	typ := corev1.EventTypeNormal
	if action == EventActionDegraded || action == EventActionFailing {
		typ = corev1.EventTypeWarning
	}
	return ManagedEvent{Type: typ, Reason: reason, Action: action, Note: CutNote(note)}
}

// CutNote returns s cut to at most MaxEventNote bytes, on a character
// boundary, so a long message from a file or a gateway cannot make the API
// refuse the event.
func CutNote(s string) string {
	if len(s) <= MaxEventNote {
		return s
	}
	cut := MaxEventNote
	for cut > MaxEventNote-utf8.UTFMax && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

// RepeatGate is the rate rule of Repeating conditions: an event for a
// condition that persists is emitted when the condition starts, and then at
// most once an hour while it lasts. It keeps, in memory, when each condition
// last fired, so a restart of the process starts every condition afresh. The
// zero value is ready for use, and a RepeatGate must not be copied after use.
//
// One RepeatGate serves one Provider.
type RepeatGate struct {
	mu   sync.Mutex
	last map[repeatKey]time.Time
	// skipFired holds when each individual CatalogEntrySkipped event of the
	// last hour fired: the budget of MaxSkipEventsPerHour.
	skipFired []time.Time
	// summaryAt is when the last summary event fired; zero if none has.
	summaryAt time.Time
}

// repeatKey names one condition: a reason, and for CatalogEntrySkipped the
// entry and the category of its failure. It holds no measured value, so a
// value that changes (a longer revision) is the same condition.
type repeatKey struct {
	reason   string
	name     string
	category SkipCategory
}

// CatalogEvents returns the events a successful catalog read at now (m is its
// merge) is due to raise, and records them as fired:
//
//   - one degraded CatalogEntrySkipped for each skipped entry, a condition of
//     its own for each entry and category of failure (not for the words, whose
//     measured values can change), so an entry that starts to be skipped fires
//     at once. At most MaxSkipEventsPerHour of them fire in any hour; when
//     more are due, one summary event, also CatalogEntrySkipped, says how many
//     more entries are skipped and names as many of them as fit in
//     MaxEventNote bytes (SkippedSummaryNote), and fires at most once an hour.
//     The named entries are the first MaxSkipEventsPerHour in listing order,
//     and the summary never takes their place. An entry over the limit stays
//     due, and fires when the budget frees;
//   - one degraded CatalogTruncated while the listing alone does not fit.
//
// A condition fires when it was not reported by the previous call, or fired at
// least EventRepeatInterval ago (also when now is before that time, a clock
// stepped back). A condition the read no longer reports is forgotten, so its
// return fires at once, apart from the limit on skipped entries, which counts
// every event of the last hour. Call it only for a read that succeeded: a
// failed read cannot say whether the listing still truncates, and must leave
// the state alone.
func (g *RepeatGate) CatalogEvents(now time.Time, m CatalogMerge) []ManagedEvent {
	g.mu.Lock()
	defer g.mu.Unlock()
	next := make(map[repeatKey]time.Time, len(m.Skipped)+1)
	due := func(k repeatKey) bool {
		prev, held := g.last[k]
		if held && !prev.After(now) && now.Sub(prev) < EventRepeatInterval {
			next[k] = prev
			return false
		}
		return true
	}

	// The budget: the individual skip events of the last hour.
	fired := make([]time.Time, 0, MaxSkipEventsPerHour)
	for _, t := range g.skipFired {
		if !t.After(now) && now.Sub(t) < EventRepeatInterval {
			fired = append(fired, t)
		}
	}
	var out []ManagedEvent
	seen := make(map[repeatKey]bool, len(m.Skipped))
	var over []SkippedEntry // due, but beyond the budget: the summary names them
	for _, s := range m.Skipped {
		k := repeatKey{reason: EventCatalogEntrySkipped, name: s.Name, category: s.Category}
		if seen[k] { // the same condition twice in one read: one event
			continue
		}
		seen[k] = true
		switch {
		case !due(k):
		case len(fired) >= MaxSkipEventsPerHour:
			over = append(over, s)
		default:
			fired = append(fired, now)
			next[k] = now
			out = append(out, NewManagedEvent(EventActionDegraded, EventCatalogEntrySkipped, SkippedNote(s)))
		}
	}
	g.skipFired = fired
	if len(over) > 0 && (g.summaryAt.IsZero() || g.summaryAt.After(now) || now.Sub(g.summaryAt) >= EventRepeatInterval) {
		g.summaryAt = now
		out = append(out, NewManagedEvent(EventActionDegraded, EventCatalogEntrySkipped, SkippedSummaryNote(over)))
	}

	if m.Truncated > 0 {
		k := repeatKey{reason: EventCatalogTruncated}
		if due(k) {
			next[k] = now
			out = append(out, NewManagedEvent(EventActionDegraded, EventCatalogTruncated, TruncatedNote(m.Truncated)))
		}
	}
	g.last = next
	return out
}
