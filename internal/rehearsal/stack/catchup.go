package stack

// The watch's catch-up barrier (issue #281). The API server sends an event to
// its watchers after a LIST has started to show the change, so a read of the
// Recorder straight after an API poll (a LIST or a GET, or another kind's
// stream, a process, a clock) can precede an event that is only late. CatchUp is
// the one wait every such read takes first.
//
// It LISTs the kinds the read covers and waits until the Recorder holds, for
// every object the API lists, a version at its resourceVersion or later, and a
// DELETED version for every object the Recorder knows live that the LIST, taken
// at its collection's resourceVersion, no longer has. A kind's watch delivers
// its events in version order, with no gap while a connection resumes from the
// last version it delivered (Follow), so once the newest listed version of an
// object is in, every earlier event of it is; this is why a reader of every
// version ever seen (a scan for a Failed phase, the tags of a run) is covered.
// What it does not cover is an object created and deleted after the newest
// version listed. Versions are compared as apimachinery does
// (resourceversion.CompareResourceVersion: positive integers, as etcd and k3s's
// kine give them); one that does not parse is reported as not known.

import (
	"fmt"
	"sort"
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

func (k Kinds) String() string {
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
// quiet and loaded), so an event that has not come in this long is lost.
const CatchUpWait = 30 * time.Second

// catchUpPoll is how often a catch-up reads the Recorder.
const catchUpPoll = 50 * time.Millisecond

// Behind is one object the watch has not caught up on.
type Behind struct {
	Kind      Kinds
	Name      string
	UID       types.UID
	Listed    string // the resourceVersion the API listed; empty when the API no longer has the object
	Delivered string // the newest resourceVersion the watch delivered; empty when none
}

func (b Behind) String() string {
	switch {
	case b.Listed == "":
		return fmt.Sprintf("%s %s: gone from the API, and the watch delivered no deletion (its newest version is %s)", b.Kind, b.Name, b.Delivered)
	case b.Delivered == "":
		return fmt.Sprintf("%s %s: the API lists version %s, and the watch delivered none", b.Kind, b.Name, b.Listed)
	}
	return fmt.Sprintf("%s %s: the API lists version %s, and the watch delivered up to %s", b.Kind, b.Name, b.Listed, b.Delivered)
}

// CatchUpResult is what a catch-up found.
type CatchUpResult struct {
	// Waited is how long the watch trailed the API, zero when it did not.
	Waited time.Duration
	// Missing are the objects whose events did not come in CatchUpWait: lost
	// events. Each is named once; the Recorder leaves it out of later catch-ups.
	Missing []Behind
	// Unknown says why it could not be told whether the watch is behind: the
	// API could not be listed, a version did not parse, or the wait was cut
	// short by the test's deadline. Empty otherwise.
	Unknown string
}

// OK reports whether the watch had caught up.
func (r CatchUpResult) OK() bool { return len(r.Missing) == 0 && r.Unknown == "" }

// Lost reports whether the catch-up found the object's event lost.
func (r CatchUpResult) Lost(uid types.UID) bool {
	for _, b := range r.Missing {
		if b.UID == uid {
			return true
		}
	}
	return false
}

// Problem is what to report of the catch-up, empty when it is OK.
func (r CatchUpResult) Problem() string {
	switch {
	case r.Unknown != "":
		return "whether the watch has caught up with the API is not known: " + r.Unknown
	case len(r.Missing) == 0:
		return ""
	}
	var parts []string
	for _, b := range r.Missing {
		parts = append(parts, b.String())
	}
	return fmt.Sprintf("the watch delivered nothing newer in %s after the API listed it: %s: the event was lost", r.Waited.Round(time.Millisecond), strings.Join(parts, "; "))
}

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

// held is the newest version the Recorder holds of one object.
type held struct {
	name    string
	rv      string
	deleted bool
}

// holding returns the newest version of each object of the kind.
func (rec *Recorder) holding(kind Kinds) map[types.UID]held {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	out := map[types.UID]held{}
	put := func(uid types.UID, name, rv string, deleted bool) {
		h := out[uid]
		out[uid] = held{name: name, rv: rv, deleted: h.deleted || deleted}
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

// behindOn compares what the Recorder holds with the listing, for the kinds.
// It returns the objects the watch is behind on, or why it cannot tell.
func (rec *Recorder) behindOn(kinds Kinds, l listing) (behind []Behind, unknown string) {
	rec.mu.Lock()
	lost := map[types.UID]bool{}
	for uid := range rec.lost {
		lost[uid] = true
	}
	rec.mu.Unlock()
	compare := func(a, b string) (int, bool) {
		c, err := resourceversion.CompareResourceVersion(a, b)
		if err != nil {
			if unknown == "" {
				unknown = fmt.Sprintf("comparing the versions %q and %q: %v", a, b, err)
			}
			return 0, false
		}
		return c, true
	}
	for _, kind := range []Kinds{KindInstances, KindAllocations, KindDeployments} {
		if kinds&kind == 0 {
			continue
		}
		have := rec.holding(kind)
		listed := map[types.UID]bool{}
		for _, o := range l.objs {
			if o.kind != kind {
				continue
			}
			listed[o.uid] = true
			if lost[o.uid] {
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
			continue // the Recorder keeps no deletion of a ModelDeployment
		}
		// An object the Recorder holds live and the LIST lacks was deleted, if
		// the newest version held is no newer than the LIST: a newer one was
		// created or changed after it.
		for uid, h := range have {
			if listed[uid] || h.deleted || lost[uid] {
				continue
			}
			if c, ok := compare(h.rv, l.collection[kind]); ok && c <= 0 {
				behind = append(behind, Behind{Kind: kind, Name: h.name, UID: uid, Delivered: h.rv})
			}
		}
	}
	sort.Slice(behind, func(i, j int) bool {
		if behind[i].Kind != behind[j].Kind {
			return behind[i].Kind < behind[j].Kind
		}
		return behind[i].Name < behind[j].Name
	})
	return behind, unknown
}

// CatchUp waits until the watch has caught up with the API on the kinds, for a
// read of the Recorder that follows something the API showed (what names the
// read, for the log). The API is listed with c in the namespace. When the
// watch trailed, the log says by how long.
func (rec *Recorder) CatchUp(t T, c client.Reader, ns string, kinds Kinds, what string) CatchUpResult {
	t.Helper()
	list := func() (listing, error) { return listKinds(t, c, ns, kinds) }
	return catchUp(t, rec, list, kinds, what, CatchUpWait, func() { time.Sleep(catchUpPoll) })
}

// catchUp is CatchUp with its LIST, its wait and its pause between two reads
// given, so that a test can order the API and the watch itself.
func catchUp(t T, rec *Recorder, list func() (listing, error), kinds Kinds, what string, wait time.Duration, pause func()) CatchUpResult {
	t.Helper()
	l, err := list()
	if err != nil {
		return CatchUpResult{Unknown: fmt.Sprintf("the API could not be listed: %v", err)}
	}
	start := time.Now()
	deadline, cut := BoundedDeadline(t, wait)
	behind, unknown := rec.behindOn(kinds, l)
	first := len(behind)
	for unknown == "" && len(behind) > 0 && time.Now().Before(deadline) {
		pause()
		behind, unknown = rec.behindOn(kinds, l)
	}
	res := CatchUpResult{Unknown: unknown}
	if first > 0 {
		res.Waited = time.Since(start)
	}
	switch {
	case unknown != "":
	case len(behind) == 0:
		if first > 0 {
			t.Logf("catch-up (%s): the watch trailed the API on %d object(s) by %s", what, first, res.Waited.Round(time.Microsecond))
		}
	case cut && res.Waited < wait:
		res.Unknown = fmt.Sprintf("the wait was cut short by the test's deadline after %s, with %d object(s) behind", res.Waited.Round(time.Millisecond), len(behind))
	default:
		res.Missing = behind
		rec.mu.Lock()
		if rec.lost == nil {
			rec.lost = map[types.UID]bool{}
		}
		for _, b := range behind {
			rec.lost[b.UID] = true
		}
		rec.mu.Unlock()
	}
	return res
}

// listKinds lists the kinds in the namespace.
func listKinds(t T, c client.Reader, ns string, kinds Kinds) (listing, error) {
	t.Helper()
	l := listing{collection: map[Kinds]string{}}
	ctx, cancel := CallCtx(t)
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
