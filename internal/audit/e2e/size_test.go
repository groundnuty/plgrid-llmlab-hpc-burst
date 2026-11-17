package e2e

// The size of the log (issue #660, S3): per Operation and per minute of an idle controller, measured in the stand-in's run.

import (
	"encoding/json"
	"testing"
	"time"
)

// reportSize logs the log's size, in two figures. Per Operation: the bytes of the events on Operations, their status,
// Approvals and ApprovalPolicies in the window the two Operations lived in, split into writes (by anyone) and reads by
// the controllers; the test's own polling reads are listed apart and left out of the figure. Per minute of an idle
// controller: the lifecycle identity's events over a quiet window, by resource and verb, with the share that the
// objects' managedFields are of the bodies. The figures come from the stand-in's envtest run: projected to a week they
// are a projection from the stand-in (HBEP-10, Audit, Size; the science agent's ruling S3), never a live week's.
func (w *world) reportSize(t *testing.T, rd *reader, from, to time.Time, operations int) {
	t.Helper()
	set := rd.read(t)
	type tally struct{ n, bytes, mf int }
	var all tally
	classes := map[string]*tally{}
	var written, controllerReads, testReads tally
	for _, e := range set.events {
		if e.RequestReceivedTimestamp.Before(from) || e.RequestReceivedTimestamp.After(to) {
			continue
		}
		all.n++
		all.bytes += e.size
		switch e.ObjectRef.Resource {
		case "operations", "approvals", "approvalpolicies":
		default:
			continue
		}
		kind := e.ObjectRef.Resource
		if e.ObjectRef.Subresource != "" {
			kind += "/" + e.ObjectRef.Subresource
		}
		isRead := e.Verb == "get" || e.Verb == "list" || e.Verb == "watch"
		by := userKind(e.User.Username, w)
		controller := by == "lifecycle" || by == "provider"
		key := kind + " " + map[bool]string{true: "read", false: "write"}[isRead] + " by " + by
		if classes[key] == nil {
			classes[key] = &tally{}
		}
		classes[key].n++
		classes[key].bytes += e.size
		switch {
		case !isRead:
			written.bytes += e.size
		case controller:
			controllerReads.bytes += e.size
		default:
			testReads.bytes += e.size
		}
	}
	t.Logf("size: the window of the %d Operations, %.1f s: %d events, %d bytes of log in all", operations, to.Sub(from).Seconds(), all.n, all.bytes)
	for key, c := range classes {
		t.Logf("size:   %-48s %5d events %8d bytes", key, c.n, c.bytes)
	}
	t.Logf("size: per Operation: %d bytes of writes and %d bytes of the controllers' reads (the test's own polling, %d bytes, left out)",
		written.bytes/operations, controllerReads.bytes/operations, testReads.bytes)

	// An idle controller: the lifecycle identity's events over a window in which the test makes no request of its own.
	quiet := 30 * time.Second
	if testing.Short() {
		quiet = 3 * time.Second // a repeated run (-count, a mutant) measures nothing it reports: it need not wait
	}
	start := time.Now()
	time.Sleep(quiet)
	end := time.Now()
	set = rd.read(t)
	perUser := map[string]*tally{}
	lifecycleBy := map[string]*tally{}
	var lifecycleBodies tally
	for _, e := range set.events {
		if e.RequestReceivedTimestamp.Before(start) || e.RequestReceivedTimestamp.After(end) {
			continue
		}
		u := userKind(e.User.Username, w)
		if perUser[u] == nil {
			perUser[u] = &tally{}
		}
		perUser[u].n++
		perUser[u].bytes += e.size
		if u != "lifecycle" {
			continue
		}
		r := e.ObjectRef.Resource
		if e.ObjectRef.Subresource != "" {
			r += "/" + e.ObjectRef.Subresource
		}
		key := e.Verb + " " + r + " at " + e.Level
		if lifecycleBy[key] == nil {
			lifecycleBy[key] = &tally{}
		}
		lifecycleBy[key].n++
		lifecycleBy[key].bytes += e.size
		lifecycleBodies.bytes += len(e.RequestObject) + len(e.ResponseObject)
		lifecycleBodies.mf += managedFieldsBytes(e.RequestObject) + managedFieldsBytes(e.ResponseObject)
	}
	perMinute := int(time.Minute / quiet)
	for u, c := range perUser {
		t.Logf("size: idle %.0f s, %-10s %4d events %7d bytes, %d bytes per minute", quiet.Seconds(), u, c.n, c.bytes, c.bytes*perMinute)
	}
	for key, c := range lifecycleBy {
		t.Logf("size:   an idle lifecycle controller: %-44s %4d events %7d bytes in %.0f s", key, c.n, c.bytes, quiet.Seconds())
	}
	if c := perUser["lifecycle"]; c != nil {
		t.Logf("size: an idle lifecycle controller writes %d bytes of audit log per minute (%d events), from the stand-in; managedFields are %d of the %d body bytes in its events",
			c.bytes*perMinute, c.n*perMinute, lifecycleBodies.mf, lifecycleBodies.bytes)
	} else {
		t.Logf("size: an idle lifecycle controller wrote no event in %.0f s", quiet.Seconds())
	}
}

// managedFieldsBytes is the size of every managedFields array in a logged body, found at any depth.
func managedFieldsBytes(raw json.RawMessage) int {
	if len(raw) == 0 {
		return 0
	}
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return 0
	}
	var walk func(any) int
	walk = func(x any) int {
		n := 0
		switch x := x.(type) {
		case map[string]any:
			for k, val := range x {
				if k == "managedFields" {
					b, _ := json.Marshal(val)
					n += len(b)
					continue
				}
				n += walk(val)
			}
		case []any:
			for _, val := range x {
				n += walk(val)
			}
		}
		return n
	}
	return walk(v)
}

// userKind names a user for the size report without printing a name that is not ours.
func userKind(user string, w *world) string {
	switch user {
	case w.lc.user:
		return "lifecycle"
	case w.pv.user:
		return "provider"
	case w.a.user:
		return "agent-a"
	case w.b.user:
		return "agent-b"
	case w.admin.user:
		return "person"
	}
	return "other"
}
