package stack

// How the Recorder stands against a LIST (catchup.go): one look at it, taken under
// one lock, and what that look is behind on.

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/resourceversion"
)

// newest is the newest version the Recorder holds of one object.
type newest struct {
	name    string
	rv      string
	deleted bool
}

// holding returns the newest version of each object of the kind; the caller holds
// rec.mu.
func (rec *Recorder) holding(kind Kinds) map[types.UID]newest {
	out := map[types.UID]newest{}
	put := func(uid types.UID, name, rv string, deleted bool) {
		h := out[uid]
		out[uid] = newest{name: name, rv: rv, deleted: h.deleted || deleted}
	}
	switch kind {
	case KindInstances:
		for _, o := range rec.obs {
			put(o.UID, o.Name, o.Instance.ResourceVersion, o.Deleted)
		}
	case KindAllocations:
		for _, a := range rec.allocs {
			put(a.UID, a.Name, a.Allocation.ResourceVersion, a.Deleted)
		}
	case KindDeployments:
		for _, d := range rec.mds {
			put(d.Deployment.UID, d.Deployment.Name, d.Deployment.ResourceVersion, false)
		}
	}
	return out
}

// lag is how the Recorder stands against a listing.
type lag struct {
	behind   []Behind         // the objects it is behind on
	trailing map[Kinds]string // the streams that have not reached the listing's version, and how far they are
	earlier  []Behind         // what earlier catch-ups found lost, of the kinds
	unknown  string           // why it cannot tell
}

// caughtUp reports whether nothing is left to wait for.
func (g lag) caughtUp() bool { return len(g.behind) == 0 && len(g.trailing) == 0 }

// settle splits what is behind by its stream, once a wait has ended: a stream at
// the listing's version has passed the event, which is lost; a stream short of it
// can still deliver it (PR #292's D2).
func (g lag) settle() (lost, late []Behind) {
	for _, b := range g.behind {
		if _, short := g.trailing[b.Kind]; short {
			late = append(late, b)
		} else {
			lost = append(lost, b)
		}
	}
	return lost, late
}

// notReached says how far the streams short of the listing's version are, and what
// they have yet to deliver.
func (g lag) notReached(late []Behind) string {
	var parts []string
	for _, kind := range allKinds {
		if s, short := g.trailing[kind]; short {
			parts = append(parts, s)
		}
	}
	for _, b := range late {
		parts = append(parts, b.Explain())
	}
	return strings.Join(parts, "; ")
}

// recordLook is one look at the Recorder for the kinds, taken under one lock. Read
// apart, "no reset" could come from before a reset and the progress from the LIST
// after it, which passes a stream that never delivered what lay between (PR #292's
// D1).
type recordLook struct {
	resets  string
	reached map[Kinds]string
	have    map[Kinds]map[types.UID]newest
	lost    map[types.UID]Behind
}

// look takes the look for a catch-up on the kinds; what names its read. The
// catch-up's promise is of the whole record, so its window starts at the record's
// start, and crosses any reset it finds.
func (rec *Recorder) look(kinds Kinds, what string) recordLook {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	at := recordLook{reached: map[Kinds]string{}, have: map[Kinds]map[types.UID]newest{}, lost: maps.Clone(rec.lost)}
	for _, kind := range allKinds {
		if kinds&kind == 0 {
			continue
		}
		rec.noteRead(kind, "the catch-up for "+what, time.Time{}, time.Time{})
		if s := rec.streams[kind]; s != nil {
			at.reached[kind] = s.reached
		}
		at.have[kind] = rec.holding(kind)
	}
	at.resets = rec.resetsLocked(kinds)
	return at
}

// behindOn compares what the Recorder holds with the listing, for the kinds.
func (rec *Recorder) behindOn(kinds Kinds, l listing, what string) lag {
	at := rec.look(kinds, what)
	if rec.afterLook != nil {
		rec.afterLook()
	}
	return at.against(kinds, l)
}

// against compares the look with the listing, for the kinds. A reset comes first,
// also one that comes during the wait: the versions after it are current, and say
// nothing of those before.
func (at recordLook) against(kinds Kinds, l listing) lag {
	g := lag{unknown: at.resets, trailing: map[Kinds]string{}}
	for _, b := range at.lost {
		if kinds&b.Kind != 0 {
			g.earlier = append(g.earlier, b)
		}
	}
	compare := func(a, b string) (int, bool) {
		c, err := resourceversion.CompareResourceVersion(a, b)
		if err != nil {
			if g.unknown == "" {
				g.unknown = fmt.Sprintf("comparing the versions %q and %q: %v", a, b, err)
			}
			return 0, false
		}
		return c, true
	}
	for _, kind := range allKinds {
		if kinds&kind == 0 {
			continue
		}
		switch r := at.reached[kind]; {
		case r == "":
			g.trailing[kind] = fmt.Sprintf("the %s stream has reached no version, and the LIST was at %s", kind.label(), l.collection[kind])
		default:
			if c, ok := compare(r, l.collection[kind]); ok && c < 0 {
				g.trailing[kind] = fmt.Sprintf("the %s stream is at version %s, and the LIST was at %s", kind.label(), r, l.collection[kind])
			}
		}
		g.behind = append(g.behind, at.behindOnKind(kind, l, compare)...)
	}
	byKindAndName := func(a, b Behind) int {
		return cmp.Or(cmp.Compare(a.Kind, b.Kind), cmp.Compare(a.Name, b.Name))
	}
	slices.SortFunc(g.behind, byKindAndName)
	slices.SortFunc(g.earlier, byKindAndName)
	return g
}

// behindOnKind is the objects of the kind the look is behind the listing on.
func (at recordLook) behindOnKind(kind Kinds, l listing, compare func(a, b string) (int, bool)) []Behind {
	var behind []Behind
	have := at.have[kind]
	listed := map[types.UID]bool{}
	for _, o := range l.objs {
		if o.kind != kind {
			continue
		}
		listed[o.uid] = true
		if _, was := at.lost[o.uid]; was {
			continue
		}
		h, ok := have[o.uid]
		if !ok {
			behind = append(behind, Behind{Kind: kind, Name: o.name, UID: o.uid, Listed: o.rv})
			continue
		}
		if c, ok := compare(h.rv, o.rv); ok && c < 0 {
			behind = append(behind, Behind{Kind: kind, Name: o.name, UID: o.uid, Listed: o.rv, Delivered: h.rv})
		}
	}
	if kind == KindDeployments {
		return behind // the Recorder keeps no deletion of a ModelDeployment
	}
	// An object the Recorder holds live and the LIST lacks was deleted, if the
	// newest version held is no newer than the LIST: a newer one was created or
	// changed after it.
	for uid, h := range have {
		_, was := at.lost[uid]
		if listed[uid] || h.deleted || was {
			continue
		}
		if c, ok := compare(h.rv, l.collection[kind]); ok && c <= 0 {
			behind = append(behind, Behind{Kind: kind, Name: h.name, UID: uid, Delivered: h.rv})
		}
	}
	return behind
}
