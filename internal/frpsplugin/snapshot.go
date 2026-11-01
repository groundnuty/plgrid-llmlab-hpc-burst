package frpsplugin

import (
	"cmp"
	"errors"
	"io/fs"
	"slices"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Skipped is an object BuildSnapshot left out of the snapshot although it looked live: it is not
// ended, yet the plugin cannot admit it. It names the object by UID and says why in words; it never
// holds a credential, and the reason is never built from an error's text.
type Skipped struct {
	// Kind is "Allocation" or "Instance".
	Kind string
	UID  string
	// Reason is one of the Skip* texts.
	Reason string
}

// The reasons of a Skipped. SkipNoCredentialFile is the ordinary one: an Allocation that is still
// Pending has no credential file until the lifecycle controller has the UID and writes it.
const (
	SkipNoCredentialFile   = "the Allocation's credential file does not exist yet"
	SkipCredentialUnusable = "the Allocation's credential file cannot be read, or does not hold a key"
	SkipCredentialShared   = "the Allocation shares its credential with another live Allocation"
	SkipAllocationNoUID    = "the Allocation has no UID"
	SkipAllocationRepeated = "the Allocation's UID, or its namespace and name, is listed more than once"
	SkipInstanceNoUID      = "the Instance has no UID"
	SkipInstanceRepeated   = "the Instance's UID is listed more than once"
)

// allocationEnded reports whether an Allocation no longer admits a login (HBEP-6, Admission: "an
// Allocation that exists and has not ended"): its deletion has begun, or its phase is one of the
// four on which the lifecycle controller ends its Instances (internal/lifecycle/instances.go,
// allocationFailed: Failed, Lost and Refused fail the Instances, Ended drains them).
func allocationEnded(a *v1alpha1.Allocation) bool {
	if !a.DeletionTimestamp.IsZero() {
		return true
	}
	switch a.Status.AtProvider.Phase {
	case v1alpha1.AllocationEnded, v1alpha1.AllocationFailed, v1alpha1.AllocationLost, v1alpha1.AllocationRefused:
		return true
	}
	return false
}

// namespaced keys an Allocation by where an Instance's allocationRef looks for it.
type namespaced struct{ namespace, name string }

// BuildSnapshot turns the controller's cached Allocations and Instances into the Snapshot a
// MemIndex answers from, with the credential of each Allocation read by readCredential (the
// controller passes agentapi.ReadKeyFile on agentapi.AllocationCredentialPath). It is pure: every
// read of a file is the callback's.
//
// The rules are HBEP-6's (Admission):
//   - an Allocation is listed when it exists and has not ended (allocationEnded). One that has
//     ended is absent, and is not reported: that is the ordinary state of an old Allocation;
//   - a listed Allocation whose credential cannot be read, or is not a key's form
//     (agentapi.CheckKey), is skipped, as a Pending one with no file yet is;
//   - Allocations that share a credential are all skipped, since a token that names several
//     Allocations admits one it should not;
//   - an Instance is listed under the Allocation it is placed on, named by its allocationRef in
//     the Instance's own namespace, when that Allocation is listed and the Instance's phase is
//     not ended.
//
// Whatever else is ambiguous is dropped, never guessed: an Allocation with no UID or a repeated UID
// (or a repeated namespace and name, which an Instance's reference could not tell apart), and an
// Instance with no UID or a UID that appears more than once. The result is never refused by
// MemIndex.Replace, which keeps the old state when it refuses one, and an old state would go on
// admitting an Allocation that has ended. Allocations and Instances come out sorted by UID, and
// the skipped ones by kind, UID and reason.
func BuildSnapshot(allocs []v1alpha1.Allocation, insts []v1alpha1.Instance, readCredential func(allocationUID string) (string, error)) (Snapshot, []Skipped) {
	var skipped []Skipped
	skip := func(kind, uid, reason string) {
		skipped = append(skipped, Skipped{Kind: kind, UID: uid, Reason: reason})
	}

	// The Allocations that have not ended, and how often each identity appears among them.
	var live []*v1alpha1.Allocation
	uidCount := map[string]int{}
	nameCount := map[namespaced]int{}
	for i := range allocs {
		a := &allocs[i]
		if allocationEnded(a) {
			continue
		}
		live = append(live, a)
		uidCount[string(a.UID)]++
		nameCount[namespaced{a.Namespace, a.Name}]++
	}

	// Their credentials. An Allocation with no readable credential is skipped, and so is each
	// that shares one.
	type candidate struct {
		alloc *v1alpha1.Allocation
		cred  string
	}
	var cands []candidate
	credCount := map[string]int{}
	for _, a := range live {
		uid := string(a.UID)
		switch {
		case uid == "":
			skip("Allocation", uid, SkipAllocationNoUID)
			continue
		case uidCount[uid] > 1 || nameCount[namespaced{a.Namespace, a.Name}] > 1:
			skip("Allocation", uid, SkipAllocationRepeated)
			continue
		}
		cred, err := readCredential(uid)
		switch {
		case errors.Is(err, fs.ErrNotExist):
			skip("Allocation", uid, SkipNoCredentialFile)
			continue
		case err != nil || agentapi.CheckKey([]byte(cred)) != nil:
			skip("Allocation", uid, SkipCredentialUnusable)
			continue
		}
		credCount[cred]++
		cands = append(cands, candidate{a, cred})
	}
	var snap Snapshot
	listed := map[namespaced]int{} // an Allocation's identity to its index in snap.Allocations
	for _, c := range cands {
		if credCount[c.cred] > 1 {
			skip("Allocation", string(c.alloc.UID), SkipCredentialShared)
			continue
		}
		listed[namespaced{c.alloc.Namespace, c.alloc.Name}] = len(snap.Allocations)
		snap.Allocations = append(snap.Allocations, AllocationEntry{UID: string(c.alloc.UID), Credential: c.cred})
	}

	// The Instances that have not ended, placed on a listed Allocation. An Instance UID that
	// appears twice among them is ambiguous wherever it is placed.
	instCount := map[string]int{}
	for i := range insts {
		if insts[i].Status.Phase != v1alpha1.InstanceEnded {
			instCount[string(insts[i].UID)]++
		}
	}
	for i := range insts {
		in := &insts[i]
		if in.Status.Phase == v1alpha1.InstanceEnded {
			continue
		}
		at, ok := listed[namespaced{in.Namespace, in.Spec.AllocationRef.Name}]
		if !ok {
			continue // its Allocation has ended, is skipped (reported there), or is not known
		}
		uid := string(in.UID)
		switch {
		case uid == "":
			skip("Instance", uid, SkipInstanceNoUID)
		case instCount[uid] > 1:
			skip("Instance", uid, SkipInstanceRepeated)
		default:
			snap.Allocations[at].Instances = append(snap.Allocations[at].Instances, uid)
		}
	}

	slices.SortFunc(snap.Allocations, func(a, b AllocationEntry) int { return cmp.Compare(a.UID, b.UID) })
	for i := range snap.Allocations {
		slices.Sort(snap.Allocations[i].Instances)
	}
	slices.SortFunc(skipped, func(a, b Skipped) int {
		return cmp.Or(cmp.Compare(a.Kind, b.Kind), cmp.Compare(a.UID, b.UID), cmp.Compare(a.Reason, b.Reason))
	})
	return snap, skipped
}
