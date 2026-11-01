package frpsplugin

// BuildSnapshot's expected values are written by hand from HBEP-6's Admission rules ("an Allocation
// that exists and has not ended"; "an Instance placed on the token's Allocation, which has not
// ended") and from the phases on which internal/lifecycle/instances.go ends an Instance. Nothing
// here derives an expectation from BuildSnapshot or from the MemIndex it feeds.

import (
	"errors"
	"fmt"
	"io/fs"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// tokD..tokF are keys' form too (tokA..tokC are in frp_real_test.go).
const (
	tokD = "d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4"
	tokE = "e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5"
	tokF = "f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6"
)

func testAlloc(ns, name, uid string, phase v1alpha1.AllocationPhase) v1alpha1.Allocation {
	a := v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID(uid)}}
	a.Status.AtProvider.Phase = phase
	return a
}

func testInst(ns, name, uid, allocName string, phase v1alpha1.InstancePhase) v1alpha1.Instance {
	in := v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID(uid)}}
	in.Spec.AllocationRef.Name = allocName
	in.Status.Phase = phase
	return in
}

// credFiles stands for the credentials directory: an Allocation UID absent from it has no file.
type credFiles map[string]string

func (c credFiles) read(uid string) (string, error) {
	v, ok := c[uid]
	if !ok {
		return "", fmt.Errorf("key file: %w", fs.ErrNotExist)
	}
	return v, nil
}

func TestBuildSnapshotRules(t *testing.T) {
	const ns = "team-a"
	running := v1alpha1.AllocationRunning
	deleting := metav1.NewTime(time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC))
	allocDeleting := testAlloc(ns, "al-del", "uid-del", running)
	allocDeleting.DeletionTimestamp = &deleting

	cases := []struct {
		name      string
		allocs    []v1alpha1.Allocation
		insts     []v1alpha1.Instance
		files     credFiles
		want      Snapshot
		wantSkips []Skipped
	}{
		{
			name:   "a running Allocation lists its Instances, sorted",
			allocs: []v1alpha1.Allocation{testAlloc(ns, "al-a", "uid-a", running)},
			insts: []v1alpha1.Instance{
				testInst(ns, "in-2", "in-uid-2", "al-a", v1alpha1.InstanceReady),
				testInst(ns, "in-1", "in-uid-1", "al-a", ""),
				testInst(ns, "in-3", "in-uid-3", "al-a", v1alpha1.InstanceDraining),
			},
			files: credFiles{"uid-a": tokA},
			want:  Snapshot{Allocations: []AllocationEntry{{UID: "uid-a", Credential: tokA, Instances: []string{"in-uid-1", "in-uid-2", "in-uid-3"}}}},
		},
		{
			name:   "an Allocation with no Instance is listed with none",
			allocs: []v1alpha1.Allocation{testAlloc(ns, "al-a", "uid-a", running)},
			files:  credFiles{"uid-a": tokA},
			want:   Snapshot{Allocations: []AllocationEntry{{UID: "uid-a", Credential: tokA}}},
		},
		{
			name:   "an Allocation Ended is absent, with its Instances",
			allocs: []v1alpha1.Allocation{testAlloc(ns, "al-a", "uid-a", v1alpha1.AllocationEnded)},
			insts:  []v1alpha1.Instance{testInst(ns, "in-1", "in-uid-1", "al-a", v1alpha1.InstanceReady)},
			files:  credFiles{"uid-a": tokA},
		},
		{
			name:   "an Allocation Failed is absent",
			allocs: []v1alpha1.Allocation{testAlloc(ns, "al-a", "uid-a", v1alpha1.AllocationFailed)},
			insts:  []v1alpha1.Instance{testInst(ns, "in-1", "in-uid-1", "al-a", v1alpha1.InstanceReady)},
			files:  credFiles{"uid-a": tokA},
		},
		{
			name:   "an Allocation Lost is absent",
			allocs: []v1alpha1.Allocation{testAlloc(ns, "al-a", "uid-a", v1alpha1.AllocationLost)},
			insts:  []v1alpha1.Instance{testInst(ns, "in-1", "in-uid-1", "al-a", v1alpha1.InstanceReady)},
			files:  credFiles{"uid-a": tokA},
		},
		{
			name:   "an Allocation Refused is absent",
			allocs: []v1alpha1.Allocation{testAlloc(ns, "al-a", "uid-a", v1alpha1.AllocationRefused)},
			insts:  []v1alpha1.Instance{testInst(ns, "in-1", "in-uid-1", "al-a", v1alpha1.InstanceReady)},
			files:  credFiles{"uid-a": tokA},
		},
		{
			name:   "an Allocation being deleted is absent although its phase is Running",
			allocs: []v1alpha1.Allocation{allocDeleting},
			insts:  []v1alpha1.Instance{testInst(ns, "in-1", "in-uid-1", "al-del", v1alpha1.InstanceReady)},
			files:  credFiles{"uid-del": tokA},
		},
		{
			name: "every phase before the end keeps its Allocation listed",
			allocs: []v1alpha1.Allocation{
				testAlloc(ns, "al-1", "uid-1", ""),
				testAlloc(ns, "al-2", "uid-2", v1alpha1.AllocationSubmitting),
				testAlloc(ns, "al-3", "uid-3", v1alpha1.AllocationSubmissionUnknown),
				testAlloc(ns, "al-4", "uid-4", v1alpha1.AllocationHeld),
				testAlloc(ns, "al-5", "uid-5", v1alpha1.AllocationPending),
				testAlloc(ns, "al-6", "uid-6", v1alpha1.AllocationRunning),
			},
			files: credFiles{"uid-1": tokA, "uid-2": tokB, "uid-3": tokC, "uid-4": tokD, "uid-5": tokE, "uid-6": tokF},
			want: Snapshot{Allocations: []AllocationEntry{
				{UID: "uid-1", Credential: tokA}, {UID: "uid-2", Credential: tokB}, {UID: "uid-3", Credential: tokC},
				{UID: "uid-4", Credential: tokD}, {UID: "uid-5", Credential: tokE}, {UID: "uid-6", Credential: tokF},
			}},
		},
		{
			name:      "an Allocation with no credential file yet is skipped, and its Instances are absent",
			allocs:    []v1alpha1.Allocation{testAlloc(ns, "al-a", "uid-a", v1alpha1.AllocationPending), testAlloc(ns, "al-b", "uid-b", running)},
			insts:     []v1alpha1.Instance{testInst(ns, "in-1", "in-uid-1", "al-a", ""), testInst(ns, "in-2", "in-uid-2", "al-b", "")},
			files:     credFiles{"uid-b": tokB},
			want:      Snapshot{Allocations: []AllocationEntry{{UID: "uid-b", Credential: tokB, Instances: []string{"in-uid-2"}}}},
			wantSkips: []Skipped{{"Allocation", "uid-a", SkipNoCredentialFile}},
		},
		{
			name:   "a credential that is not a key's form is skipped: short, upper case, with a newline, empty",
			allocs: []v1alpha1.Allocation{testAlloc(ns, "al-1", "uid-1", running), testAlloc(ns, "al-2", "uid-2", running), testAlloc(ns, "al-3", "uid-3", running), testAlloc(ns, "al-4", "uid-4", running)},
			insts:  []v1alpha1.Instance{testInst(ns, "in-1", "in-uid-1", "al-1", "")},
			files:  credFiles{"uid-1": "abc", "uid-2": strings.ToUpper(tokB), "uid-3": tokC + "\n", "uid-4": ""},
			wantSkips: []Skipped{
				{"Allocation", "uid-1", SkipCredentialUnusable}, {"Allocation", "uid-2", SkipCredentialUnusable},
				{"Allocation", "uid-3", SkipCredentialUnusable}, {"Allocation", "uid-4", SkipCredentialUnusable},
			},
		},
		{
			name:   "Allocations that share a credential are all skipped; another stays",
			allocs: []v1alpha1.Allocation{testAlloc(ns, "al-1", "uid-1", running), testAlloc(ns, "al-2", "uid-2", running), testAlloc(ns, "al-3", "uid-3", running), testAlloc(ns, "al-4", "uid-4", running)},
			insts: []v1alpha1.Instance{
				testInst(ns, "in-1", "in-uid-1", "al-1", ""), testInst(ns, "in-2", "in-uid-2", "al-2", ""),
				testInst(ns, "in-3", "in-uid-3", "al-3", ""), testInst(ns, "in-4", "in-uid-4", "al-4", ""),
			},
			files: credFiles{"uid-1": tokA, "uid-2": tokA, "uid-3": tokB, "uid-4": tokA},
			want:  Snapshot{Allocations: []AllocationEntry{{UID: "uid-3", Credential: tokB, Instances: []string{"in-uid-3"}}}},
			wantSkips: []Skipped{
				{"Allocation", "uid-1", SkipCredentialShared}, {"Allocation", "uid-2", SkipCredentialShared}, {"Allocation", "uid-4", SkipCredentialShared},
			},
		},
		{
			name:   "a credential shared with an Allocation that has ended is not shared: the live one stays",
			allocs: []v1alpha1.Allocation{testAlloc(ns, "al-old", "uid-old", v1alpha1.AllocationEnded), testAlloc(ns, "al-new", "uid-new", running)},
			files:  credFiles{"uid-old": tokA, "uid-new": tokA},
			want:   Snapshot{Allocations: []AllocationEntry{{UID: "uid-new", Credential: tokA}}},
		},
		{
			name:   "an Instance that has ended is absent; its neighbours stay",
			allocs: []v1alpha1.Allocation{testAlloc(ns, "al-a", "uid-a", running)},
			insts: []v1alpha1.Instance{
				testInst(ns, "in-1", "in-uid-1", "al-a", v1alpha1.InstanceEnded),
				testInst(ns, "in-2", "in-uid-2", "al-a", v1alpha1.InstanceStarting),
			},
			files: credFiles{"uid-a": tokA},
			want:  Snapshot{Allocations: []AllocationEntry{{UID: "uid-a", Credential: tokA, Instances: []string{"in-uid-2"}}}},
		},
		{
			name:   "an Instance whose allocationRef names an Allocation in another namespace is absent",
			allocs: []v1alpha1.Allocation{testAlloc("team-a", "al-a", "uid-a", running)},
			insts: []v1alpha1.Instance{
				testInst("team-b", "in-1", "in-uid-1", "al-a", ""), // same name, other namespace
				testInst("team-a", "in-2", "in-uid-2", "al-a", ""),
			},
			files: credFiles{"uid-a": tokA},
			want:  Snapshot{Allocations: []AllocationEntry{{UID: "uid-a", Credential: tokA, Instances: []string{"in-uid-2"}}}},
		},
		{
			name:   "an Instance whose Allocation does not exist is absent",
			allocs: []v1alpha1.Allocation{testAlloc(ns, "al-a", "uid-a", running)},
			insts:  []v1alpha1.Instance{testInst(ns, "in-1", "in-uid-1", "al-nowhere", "")},
			files:  credFiles{"uid-a": tokA},
			want:   Snapshot{Allocations: []AllocationEntry{{UID: "uid-a", Credential: tokA}}},
		},
		{
			name:   "one Allocation name in two namespaces: each Instance goes to the Allocation of its own namespace",
			allocs: []v1alpha1.Allocation{testAlloc("team-b", "al-x", "uid-b", running), testAlloc("team-a", "al-x", "uid-a", running)},
			insts:  []v1alpha1.Instance{testInst("team-a", "in-1", "in-uid-1", "al-x", ""), testInst("team-b", "in-2", "in-uid-2", "al-x", "")},
			files:  credFiles{"uid-a": tokA, "uid-b": tokB},
			want: Snapshot{Allocations: []AllocationEntry{
				{UID: "uid-a", Credential: tokA, Instances: []string{"in-uid-1"}},
				{UID: "uid-b", Credential: tokB, Instances: []string{"in-uid-2"}},
			}},
		},
		{
			name:      "an Allocation with no UID is skipped",
			allocs:    []v1alpha1.Allocation{testAlloc(ns, "al-a", "", running), testAlloc(ns, "al-b", "uid-b", running)},
			insts:     []v1alpha1.Instance{testInst(ns, "in-1", "in-uid-1", "al-a", "")},
			files:     credFiles{"": tokA, "uid-b": tokB},
			want:      Snapshot{Allocations: []AllocationEntry{{UID: "uid-b", Credential: tokB}}},
			wantSkips: []Skipped{{"Allocation", "", SkipAllocationNoUID}},
		},
		{
			name:      "two Allocations with one UID are both skipped",
			allocs:    []v1alpha1.Allocation{testAlloc("team-a", "al-a", "uid-same", running), testAlloc("team-b", "al-b", "uid-same", running)},
			insts:     []v1alpha1.Instance{testInst("team-a", "in-1", "in-uid-1", "al-a", "")},
			files:     credFiles{"uid-same": tokA},
			wantSkips: []Skipped{{"Allocation", "uid-same", SkipAllocationRepeated}, {"Allocation", "uid-same", SkipAllocationRepeated}},
		},
		{
			name:      "two Allocations with one namespace and name are both skipped",
			allocs:    []v1alpha1.Allocation{testAlloc(ns, "al-a", "uid-1", running), testAlloc(ns, "al-a", "uid-2", running)},
			insts:     []v1alpha1.Instance{testInst(ns, "in-1", "in-uid-1", "al-a", "")},
			files:     credFiles{"uid-1": tokA, "uid-2": tokB},
			wantSkips: []Skipped{{"Allocation", "uid-1", SkipAllocationRepeated}, {"Allocation", "uid-2", SkipAllocationRepeated}},
		},
		{
			name:   "an Instance with no UID is skipped",
			allocs: []v1alpha1.Allocation{testAlloc(ns, "al-a", "uid-a", running)},
			insts:  []v1alpha1.Instance{testInst(ns, "in-1", "", "al-a", ""), testInst(ns, "in-2", "in-uid-2", "al-a", "")},
			files:  credFiles{"uid-a": tokA},
			want:   Snapshot{Allocations: []AllocationEntry{{UID: "uid-a", Credential: tokA, Instances: []string{"in-uid-2"}}}},
			wantSkips: []Skipped{
				{"Instance", "", SkipInstanceNoUID},
			},
		},
		{
			name:   "an Instance UID listed under two Allocations, and one listed twice under one, are dropped; others stay",
			allocs: []v1alpha1.Allocation{testAlloc(ns, "al-a", "uid-a", running), testAlloc(ns, "al-b", "uid-b", running)},
			insts: []v1alpha1.Instance{
				testInst(ns, "in-1", "in-uid-x", "al-a", ""), testInst(ns, "in-2", "in-uid-x", "al-b", ""),
				testInst(ns, "in-3", "in-uid-y", "al-a", ""), testInst(ns, "in-4", "in-uid-y", "al-a", ""),
				testInst(ns, "in-5", "in-uid-z", "al-a", ""),
			},
			files: credFiles{"uid-a": tokA, "uid-b": tokB},
			want: Snapshot{Allocations: []AllocationEntry{
				{UID: "uid-a", Credential: tokA, Instances: []string{"in-uid-z"}},
				{UID: "uid-b", Credential: tokB},
			}},
			wantSkips: []Skipped{
				{"Instance", "in-uid-x", SkipInstanceRepeated}, {"Instance", "in-uid-x", SkipInstanceRepeated},
				{"Instance", "in-uid-y", SkipInstanceRepeated}, {"Instance", "in-uid-y", SkipInstanceRepeated},
			},
		},
		{
			name:   "nothing in, nothing out",
			allocs: nil,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, skips := BuildSnapshot(c.allocs, c.insts, c.files.read)
			if !reflect.DeepEqual(got, c.want) && !(len(got.Allocations) == 0 && len(c.want.Allocations) == 0) {
				t.Errorf("snapshot = %+v\nwant       %+v", got, c.want)
			}
			if !reflect.DeepEqual(skips, c.wantSkips) && !(len(skips) == 0 && len(c.wantSkips) == 0) {
				t.Errorf("skipped = %+v\nwant       %+v", skips, c.wantSkips)
			}
			// Whatever it did, the Index accepts it, and answers as the rules say.
			var idx MemIndex
			if err := idx.Replace(got); err != nil {
				t.Errorf("MemIndex.Replace refused the snapshot: %v", err)
			}
		})
	}
}

// The Index built from a snapshot answers by HBEP-6's rules: an ended Allocation's token is no
// Allocation's, and an Instance counts only on the Allocation it is placed on. Each answer is written
// out by hand.
func TestSnapshotAnswersAsAdmissionRules(t *testing.T) {
	const ns = "team-a"
	allocs := []v1alpha1.Allocation{
		testAlloc(ns, "al-live", "uid-live", v1alpha1.AllocationRunning),
		testAlloc(ns, "al-ended", "uid-ended", v1alpha1.AllocationEnded),
		testAlloc(ns, "al-other", "uid-other", v1alpha1.AllocationPending),
	}
	insts := []v1alpha1.Instance{
		testInst(ns, "in-live", "in-live-uid", "al-live", v1alpha1.InstanceReady),
		testInst(ns, "in-on-ended", "in-ended-uid", "al-ended", v1alpha1.InstanceReady),
		testInst(ns, "in-other", "in-other-uid", "al-other", v1alpha1.InstancePending),
		testInst(ns, "in-done", "in-done-uid", "al-live", v1alpha1.InstanceEnded),
	}
	snap, _ := BuildSnapshot(allocs, insts, credFiles{"uid-live": tokA, "uid-ended": tokB, "uid-other": tokC}.read)
	var idx MemIndex
	if err := idx.Replace(snap); err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	for _, c := range []struct {
		token   string
		wantUID string
		wantOK  bool
	}{
		{tokA, "uid-live", true},
		{tokB, "", false}, // the ended Allocation's credential
		{tokC, "uid-other", true},
		{tokD, "", false}, // nobody's
	} {
		if uid, ok := idx.AllocationForToken(ctx, c.token); uid != c.wantUID || ok != c.wantOK {
			t.Errorf("AllocationForToken(%s...) = %q, %v; want %q, %v", c.token[:2], uid, ok, c.wantUID, c.wantOK)
		}
	}
	for _, c := range []struct {
		alloc, inst string
		want        bool
	}{
		{"uid-live", "in-live-uid", true},
		{"uid-live", "in-other-uid", false}, // another Allocation's Instance
		{"uid-live", "in-done-uid", false},  // an Instance that has ended
		{"uid-live", "in-ended-uid", false}, // an Instance of an ended Allocation
		{"uid-other", "in-other-uid", true},
		{"uid-ended", "in-ended-uid", false}, // the ended Allocation is not in the Index at all
	} {
		if got := idx.InstanceOnAllocation(ctx, c.alloc, c.inst); got != c.want {
			t.Errorf("InstanceOnAllocation(%s, %s) = %v, want %v", c.alloc, c.inst, got, c.want)
		}
	}
}

// A skipped object is named by UID and reason only: not its credential, and not the text of the
// error that made it skipped, which a reader may fill with a path or worse.
func TestSnapshotSkippedNeverHoldsACredential(t *testing.T) {
	allocs := []v1alpha1.Allocation{
		testAlloc("ns", "al-1", "uid-1", v1alpha1.AllocationRunning),
		testAlloc("ns", "al-2", "uid-2", v1alpha1.AllocationRunning),
		testAlloc("ns", "al-3", "uid-3", v1alpha1.AllocationRunning),
		testAlloc("ns", "al-4", "uid-4", v1alpha1.AllocationRunning),
	}
	const secretInError = "sekrit-from-an-error-text"
	read := func(uid string) (string, error) {
		switch uid {
		case "uid-1", "uid-2":
			return tokA, nil // shared
		case "uid-3":
			return "", errors.New("cannot read " + secretInError)
		}
		return tokB[:60] + "SECRET", nil // not a key's form, but it holds key characters
	}
	_, skips := BuildSnapshot(allocs, nil, read)
	if len(skips) != 4 {
		t.Fatalf("skipped %+v, want all four", skips)
	}
	for _, s := range skips {
		text := fmt.Sprintf("%+v", s)
		for _, bad := range []string{tokA, tokB[:20], secretInError, "SECRET"} {
			if strings.Contains(text, bad) {
				t.Errorf("a skipped entry holds %q: %s", bad, text)
			}
		}
	}
}

// Replace keeps the old state when it refuses a snapshot, and the old state would go on admitting an
// Allocation that has ended. So BuildSnapshot's output must never be refused, whatever the cache
// holds. The inputs here are drawn from a small universe, so that every kind of collision the Index
// refuses (a repeated UID, a repeated credential, an Instance under two Allocations, an empty UID, a
// credential that is not a key) turns up many times; the expectation is only that Replace says nil.
func TestBuildSnapshotIsNeverRefusedByReplace(t *testing.T) {
	rnd := rand.New(rand.NewSource(20261101))
	uids := []string{"", "u1", "u2", "u3"}
	names := []string{"n1", "n2", "n3"}
	nss := []string{"ns1", "ns2"}
	creds := []string{tokA, tokB, tokC, "", "short", tokA + "x", strings.ToUpper(tokB)}
	phases := []v1alpha1.AllocationPhase{"", v1alpha1.AllocationPending, v1alpha1.AllocationRunning, v1alpha1.AllocationEnded, v1alpha1.AllocationFailed, v1alpha1.AllocationLost, v1alpha1.AllocationRefused}
	iphases := []v1alpha1.InstancePhase{"", v1alpha1.InstancePending, v1alpha1.InstanceReady, v1alpha1.InstanceEnded}
	pick := func(s []string) string { return s[rnd.Intn(len(s))] }
	for round := 0; round < 3000; round++ {
		var allocs []v1alpha1.Allocation
		for i := rnd.Intn(7); i > 0; i-- {
			allocs = append(allocs, testAlloc(pick(nss), pick(names), pick(uids), phases[rnd.Intn(len(phases))]))
		}
		var insts []v1alpha1.Instance
		for i := rnd.Intn(9); i > 0; i-- {
			insts = append(insts, testInst(pick(nss), "in", pick(uids), pick(names), iphases[rnd.Intn(len(iphases))]))
		}
		files := map[string]string{}
		for _, u := range uids {
			if rnd.Intn(5) > 0 {
				files[u] = pick(creds)
			}
		}
		read := func(uid string) (string, error) {
			if v, ok := files[uid]; ok {
				return v, nil
			}
			if rnd.Intn(2) == 0 {
				return "", fs.ErrNotExist
			}
			return "", errors.New("unreadable")
		}
		snap, _ := BuildSnapshot(allocs, insts, read)
		var idx MemIndex
		if err := idx.Replace(snap); err != nil {
			t.Fatalf("round %d: Replace refused BuildSnapshot's output %+v: %v\nallocs %+v\ninsts %+v", round, snap, err, allocs, insts)
		}
	}
}
