package stack

// The watch's catch-up barrier (issue #281). The API server sends an event to
// its watchers after a LIST has started to show the change, so a read of the
// Recorder straight after an API poll (a LIST or a GET, or another kind's
// stream, a process, a clock) can precede an event that is only late. CatchUp is
// the one wait every such read takes first.
//
// Its promise: the Recorder holds everything the API has shown up to this LIST.
// It LISTs the kinds the read covers and waits until each kind's stream has
// reached the LIST's collection resourceVersion, through an event or a bookmark.
// A kind's watch delivers its events in version order, with no gap while a
// connection resumes from the last version it reached (Follow), so a stream at
// that version has delivered every event up to it, also those of an object
// created and deleted while the watch was behind, which no LIST shows. It also
// waits until the Recorder holds, for every object the API lists, a version at
// its resourceVersion or later, and a DELETED version for every object it holds
// live that the LIST no longer has: those name what is missing.
//
// A check that cannot tell reports not known, never caught up: a LIST that
// failed, a version that does not parse, a wait cut short, a stream that did not
// reach the LIST's version in time, and a stream that was reset (Follow): after
// a reset a current version no longer shows that the earlier ones were delivered.
//
// Versions are compared as apimachinery does
// (resourceversion.CompareResourceVersion: positive integers, as etcd and k3s's
// kine give them).

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/resourceversion"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// Kinds selects the Recorder's streams a catch-up covers.
type Kinds uint8

const (
	KindInstances Kinds = 1 << iota
	KindAllocations
	KindDeployments
)

// allKinds are the Recorder's streams.
var allKinds = []Kinds{KindInstances, KindAllocations, KindDeployments}

// label names the kind of an object, for a report.
func (k Kinds) label() string {
	switch k {
	case KindInstances:
		return "Instance"
	case KindAllocations:
		return "Allocation"
	case KindDeployments:
		return "ModelDeployment"
	}
	return fmt.Sprintf("kinds(%d)", uint8(k))
}

// CatchUpWait is how long a catch-up waits for the watch. On a healthy server
// the watch trails a LIST by microseconds to milliseconds (measured on the
// rehearsal's k3s for #281: at most a few milliseconds in 1,400 deletions,
// quiet and loaded), and a quiet stream reaches a LIST's version with its next
// bookmark, in seconds (connectionSeconds), so what has not come in this long is
// not coming.
const CatchUpWait = 30 * time.Second

// CatchUpWorstCase is the most one catch-up takes of its caller's time: its LIST's
// timeout, then its wait. A row's budget counts it once for each catch-up on the
// row's path; the row's context still stops it.
const CatchUpWorstCase = APICallTimeout + CatchUpWait

// catchUpPoll is how often a catch-up reads the Recorder.
const catchUpPoll = 50 * time.Millisecond

// listedObject is one object the API listed.
type listedObject struct {
	kind Kinds
	uid  types.UID
	name string
	rv   string
}

// listing is what the API listed: its objects, and each kind's collection
// resourceVersion.
type listing struct {
	objs       []listedObject
	collection map[Kinds]string
}

// invalid says why a version of the listing does not parse, for the kinds; it is
// checked before any is compared, so that one the Recorder has not seen the
// object of counts too.
func (l listing) invalid(kinds Kinds) string {
	for _, kind := range allKinds {
		if kinds&kind == 0 {
			continue
		}
		if _, err := resourceversion.CompareResourceVersion(l.collection[kind], "1"); err != nil {
			return fmt.Sprintf("the %s collection's version %q: %v", kind.label(), l.collection[kind], err)
		}
	}
	for _, o := range l.objs {
		if _, err := resourceversion.CompareResourceVersion(o.rv, "1"); err != nil {
			return fmt.Sprintf("the version %q of %s %s: %v", o.rv, o.kind.label(), o.name, err)
		}
	}
	return ""
}

// newest is the newest version the Recorder holds of one object.
type newest struct {
	name    string
	rv      string
	deleted bool
}

// holding returns the newest version of each object of the kind.
func (rec *Recorder) holding(kind Kinds) map[types.UID]newest {
	rec.mu.Lock()
	defer rec.mu.Unlock()
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
	behind   []Behind // the objects it is behind on
	trailing []string // the streams that have not reached the listing's version
	earlier  []Behind // what earlier catch-ups found lost, of the kinds
	unknown  string   // why it cannot tell
}

// caughtUp reports whether nothing is left to wait for.
func (g lag) caughtUp() bool { return len(g.behind) == 0 && len(g.trailing) == 0 }

// behindOn compares what the Recorder holds with the listing, for the kinds.
func (rec *Recorder) behindOn(kinds Kinds, l listing) lag {
	// A reset first, also one that comes during the wait: the versions after it
	// are current, and say nothing of those before.
	g := lag{unknown: rec.resets(kinds)}
	rec.mu.Lock()
	lost := map[types.UID]Behind{}
	for uid, b := range rec.lost {
		lost[uid] = b
		if kinds&b.Kind != 0 {
			g.earlier = append(g.earlier, b)
		}
	}
	reached := map[Kinds]string{}
	for kind, s := range rec.streams {
		reached[kind] = s.reached
	}
	rec.mu.Unlock()
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
		switch r := reached[kind]; {
		case r == "":
			g.trailing = append(g.trailing, fmt.Sprintf("the %s stream has reached no version, and the LIST was at %s", kind.label(), l.collection[kind]))
		default:
			if c, ok := compare(r, l.collection[kind]); ok && c < 0 {
				g.trailing = append(g.trailing, fmt.Sprintf("the %s stream is at version %s, and the LIST was at %s", kind.label(), r, l.collection[kind]))
			}
		}
		have := rec.holding(kind)
		listed := map[types.UID]bool{}
		for _, o := range l.objs {
			if o.kind != kind {
				continue
			}
			listed[o.uid] = true
			if _, was := lost[o.uid]; was {
				continue
			}
			h, ok := have[o.uid]
			if !ok {
				g.behind = append(g.behind, Behind{Kind: kind, Name: o.name, UID: o.uid, Listed: o.rv})
				continue
			}
			if c, ok := compare(h.rv, o.rv); ok && c < 0 {
				g.behind = append(g.behind, Behind{Kind: kind, Name: o.name, UID: o.uid, Listed: o.rv, Delivered: h.rv})
			}
		}
		if kind == KindDeployments {
			continue // the Recorder keeps no deletion of a ModelDeployment
		}
		// An object the Recorder holds live and the LIST lacks was deleted, if
		// the newest version held is no newer than the LIST: a newer one was
		// created or changed after it.
		for uid, h := range have {
			_, was := lost[uid]
			if listed[uid] || h.deleted || was {
				continue
			}
			if c, ok := compare(h.rv, l.collection[kind]); ok && c <= 0 {
				g.behind = append(g.behind, Behind{Kind: kind, Name: h.name, UID: uid, Delivered: h.rv})
			}
		}
	}
	byKindAndName := func(a, b Behind) int {
		return cmp.Or(cmp.Compare(a.Kind, b.Kind), cmp.Compare(a.Name, b.Name))
	}
	slices.SortFunc(g.behind, byKindAndName)
	slices.SortFunc(g.earlier, byKindAndName)
	return g
}

// CatchUp waits until the watch has caught up with the API on the kinds, for a
// read of the Recorder that follows something the API showed (what names the
// read, for the log). The API is listed with c in the namespace; ctx bounds the
// LIST and the wait, as the test's deadline does, so that a row with a deadline
// of its own gets its time back. The log says how long each catch-up took.
func (rec *Recorder) CatchUp(ctx context.Context, t T, c client.Reader, ns string, kinds Kinds, what string) CatchUpResult {
	t.Helper()
	list := func() (listing, error) { return listKinds(ctx, t, c, ns, kinds) }
	return catchUp(ctx, t, rec, list, kinds, what, CatchUpWait, func() { time.Sleep(catchUpPoll) })
}

// catchUp is CatchUp with its LIST, its wait and its pause between two reads
// given, so that a test can order the API and the watch itself.
func catchUp(ctx context.Context, t T, rec *Recorder, list func() (listing, error), kinds Kinds, what string, wait time.Duration, pause func()) CatchUpResult {
	t.Helper()
	l, err := list()
	if err != nil {
		return CatchUpResult{Unknown: fmt.Sprintf("the API could not be listed: %v", err)}
	}
	if why := l.invalid(kinds); why != "" {
		return CatchUpResult{Unknown: why}
	}
	start := time.Now()
	deadline, cut := waitDeadline(ctx, t, wait)
	g := rec.behindOn(kinds, l)
	waited := !g.caughtUp()
	for g.unknown == "" && !g.caughtUp() && ctx.Err() == nil && time.Now().Before(deadline) {
		pause()
		g = rec.behindOn(kinds, l)
	}
	res := CatchUpResult{EarlierLost: g.earlier}
	if waited {
		res.Waited = time.Since(start)
	}
	cutShort := ctx.Err() != nil || (cut && res.Waited < wait)
	switch {
	case g.unknown != "":
		res.Unknown = g.unknown
	case g.caughtUp():
		t.Logf("catch-up (%s): the watch reached the API's version in %s", what, res.Waited.Round(time.Microsecond))
	case cutShort:
		res.Unknown = fmt.Sprintf("the wait was cut short by a deadline after %s, with %d object(s) behind and %d stream(s) short of the LIST's version", res.Waited.Round(time.Millisecond), len(g.behind), len(g.trailing))
	case len(g.behind) > 0:
		res.Missing = g.behind
		rec.mu.Lock()
		if rec.lost == nil {
			rec.lost = map[types.UID]Behind{}
		}
		for _, b := range g.behind {
			rec.lost[b.UID] = b
		}
		rec.mu.Unlock()
	default:
		res.Unknown = fmt.Sprintf("after %s %s", res.Waited.Round(time.Millisecond), strings.Join(g.trailing, "; "))
	}
	return res
}

// waitDeadline is when a wait of d gives up: now plus d, the test's WaitCap or
// ctx's deadline, whichever comes first; cut says one of the last two shortened it.
func waitDeadline(ctx context.Context, t T, d time.Duration) (time.Time, bool) {
	deadline, cut := BoundedDeadline(t, d)
	if c, ok := ctx.Deadline(); ok && c.Before(deadline) {
		return c, true
	}
	return deadline, cut
}

// listKinds lists the kinds in the namespace, bounded by ctx as well as by one
// call's timeout and the test's deadline.
func listKinds(ctx context.Context, t T, c client.Reader, ns string, kinds Kinds) (listing, error) {
	t.Helper()
	l := listing{collection: map[Kinds]string{}}
	d, _ := BoundedDeadline(t, APICallTimeout)
	ctx, cancel := context.WithDeadline(ctx, d)
	defer cancel()
	if kinds&KindInstances != 0 {
		var list v1alpha1.InstanceList
		if err := c.List(ctx, &list, client.InNamespace(ns)); err != nil {
			return l, err
		}
		l.collection[KindInstances] = list.ResourceVersion
		for _, o := range list.Items {
			l.objs = append(l.objs, listedObject{KindInstances, o.UID, o.Name, o.ResourceVersion})
		}
	}
	if kinds&KindAllocations != 0 {
		var list v1alpha1.AllocationList
		if err := c.List(ctx, &list, client.InNamespace(ns)); err != nil {
			return l, err
		}
		l.collection[KindAllocations] = list.ResourceVersion
		for _, o := range list.Items {
			l.objs = append(l.objs, listedObject{KindAllocations, o.UID, o.Name, o.ResourceVersion})
		}
	}
	if kinds&KindDeployments != 0 {
		var list v1alpha1.ModelDeploymentList
		if err := c.List(ctx, &list, client.InNamespace(ns)); err != nil {
			return l, err
		}
		l.collection[KindDeployments] = list.ResourceVersion
		for _, o := range list.Items {
			l.objs = append(l.objs, listedObject{KindDeployments, o.UID, o.Name, o.ResourceVersion})
		}
	}
	return l, nil
}
