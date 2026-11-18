package rbacscan

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
)

// The fixture is testdata/fix: one function for each way a call reaches an API server that the scan has to read. It is
// scanned once, with a list of identities of its own: "a" and "b" are managers, "c" a clientset, and a fourth that is none's.
const fixPkg = "internal/rbacscan/testdata/fix"

func fixOptions() (Options, error) {
	root := repoRoot()
	// the fallback's store is found by its marker, so that an edit of the fixture does not move the entry
	src, err := os.ReadFile(filepath.Join(root, fixPkg, "fix.go"))
	if err != nil {
		return Options{}, err
	}
	var skipLine int
	for i, line := range strings.Split(string(src), "\n") {
		if strings.Contains(line, "// skip-store") {
			skipLine = i + 1
		}
	}
	if skipLine == 0 {
		return Options{}, fmt.Errorf("the fixture has no skip-store marker")
	}
	cite := Cite{cgModule, cgVer, "tools/leaderelection/resourcelock/leaselock.go", "ll.Client.Leases(ll.LeaseMeta.Namespace).Get("}
	return Options{
		Root:     root,
		Patterns: []string{"./" + fixPkg},
		CRDDir:   filepath.Join(root, "config", "crd"),
		Managers: []Source{
			{Where: fixPkg + ".newA", Ident: "a", Why: "fixture"},
			{Where: fixPkg + ".newB", Ident: "b", Why: "fixture"},
		},
		Clients: []Source{
			{Where: fixPkg + ".newTyped", Ident: "c", Why: "fixture"},
			{Where: fixPkg + ".newAdmin", Ident: Exempt, Why: "fixture: the administrator's"},
		},
		SkipStores: []Skip{
			{Pos: fmt.Sprintf("%s/fix.go:%d", fixPkg, skipLine), Why: "fixture"},
			{Pos: fixPkg + "/fix.go:1", Why: "fixture: a store that is not there"},
		},
		Library: []LibraryEntry{
			{ID: "fixture lock", Trigger: "{mod}/" + fixPkg + ".libTrigger", For: []Source{{Where: fixPkg + ".wire", Ident: "a", Why: "fixture"}},
				Rights: []Triple{{"coordination.k8s.io", "leases", "get"}}, Cites: []Cite{cite}},
			{ID: "no trigger", Trigger: "{mod}/" + fixPkg + ".nothingCallsThis", For: []Source{{Where: fixPkg, Ident: "a", Why: "fixture"}},
				Rights: []Triple{{"", "secrets", "get"}}, Cites: []Cite{cite}},
			{ID: "no cite", Trigger: "{mod}/" + fixPkg + ".libTrigger", For: []Source{{Where: fixPkg, Ident: "b", Why: "fixture"}},
				Rights: []Triple{{"", "configmaps", "get"}}},
			{ID: "old cite", Trigger: "{mod}/" + fixPkg + ".libTrigger", For: []Source{{Where: fixPkg, Ident: "b", Why: "fixture"}},
				Rights: []Triple{{"", "configmaps", "list"}}, Cites: []Cite{{cgModule, "v0.1.0", cite.File, cite.Contains}}},
			{ID: "cite that says other", Trigger: "{mod}/" + fixPkg + ".libTrigger", For: []Source{{Where: fixPkg, Ident: "b", Why: "fixture"}},
				Rights: []Triple{{"", "configmaps", "watch"}}, Cites: []Cite{{cgModule, cgVer, cite.File, "text the file does not contain"}}},
			{ID: "trigger with no identity", Trigger: "{mod}/" + fixPkg + ".libTrigger", For: []Source{{Where: "internal/elsewhere", Ident: "b", Why: "fixture"}},
				Rights: []Triple{{"", "configmaps", "delete"}}, Cites: []Cite{cite}},
		},
	}, nil
}

var fixture = sync.OnceValues(func() (*Result, error) {
	o, err := fixOptions()
	if err != nil {
		return nil, err
	}
	return Scan(o)
})

// sitesOf returns "ident: function: verb group/resource" for each need of each site of the fixture, sorted.
func sitesOf(res *Result) []string {
	var out []string
	for _, st := range res.Sites {
		fn := strings.TrimPrefix(st.Func, fixPkg+".")
		for _, n := range st.Need {
			out = append(out, fmt.Sprintf("%s: %s: %s", st.Ident, fn, n))
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

func TestTheFixtureYieldsTheRightsOfEachWayACallIsMade(t *testing.T) {
	res, err := fixture()
	if err != nil {
		t.Fatal(err)
	}
	const h = "hpcburst.groundnuty.github.io"
	want := []string{
		// a cached client reads through an informer; the API reader reads its own
		"a: (*reconciler).cachedGet: list " + h + "/providers",
		"a: (*reconciler).cachedGet: watch " + h + "/providers",
		"a: (*reconciler).cachedList: list " + h + "/modeldeployments",
		"a: (*reconciler).cachedList: watch " + h + "/modeldeployments",
		"a: (*reconciler).readerGet: get " + h + "/allocations",
		"a: (*reconciler).readerList: list " + h + "/instances",
		"a: (*reconciler).staleNote: delete " + h + "/allocations",
		// each write is its own verb
		"a: (*reconciler).writes: create " + h + "/allocations",
		"a: (*reconciler).writes: update " + h + "/instances",
		"a: (*reconciler).writes: delete " + h + "/providers",
		"a: (*reconciler).writes: deletecollection " + h + "/platformprofiles",
		"a: (*reconciler).writes: patch " + h + "/modeldeployments",
		// Status() and SubResource(name) write the subresource
		"a: (*reconciler).statusUpdate: update " + h + "/instances/status",
		"a: (*reconciler).statusUpdate: update " + h + "/providers/scale",
		// an annotation names the resources of a generic call; a conversion to the interface is followed, to the callers'
		"a: (*reconciler).followed: update " + h + "/instances",
		"a: (*reconciler).annotated: update " + h + "/approvals",
		"a: (*reconciler).annotated: update " + h + "/approvalpolicies",
		"a: (*reconciler).converted: update " + h + "/operations",
		// the one helper both managers go through gives each its own: b's reads are b's alone, reached through a function value
		"b: (*other).readerGet: get " + h + "/allocations",
		// the builder's For and Owns start informers of b's manager
		"b: watched: list " + h + "/operations",
		"b: watched: watch " + h + "/operations",
		"b: watched: list " + h + "/approvals",
		"b: watched: watch " + h + "/approvals",
		// a clientset's calls are its own requests; the administrator's are no one's
		"c: typed: list apps/controllerrevisions",
		"c: typed: update coordination.k8s.io/leases",
		// the library entry's rights, for the identity of the function that triggers it
		"a: wire: get coordination.k8s.io/leases",
		// an entry whose citation is bad still yields its rights, so that the one finding is the citation and not a grant
		"b: wire: get core/configmaps", "b: wire: list core/configmaps", "b: wire: watch core/configmaps",
	}
	slices.Sort(want)
	got := sitesOf(res)
	if !slices.Equal(got, want) {
		var missing, extra []string
		for _, w := range want {
			if !slices.Contains(got, w) {
				missing = append(missing, w)
			}
		}
		for _, g := range got {
			if !slices.Contains(want, g) {
				extra = append(extra, g)
			}
		}
		t.Fatalf("the fixture's sites differ\n missing: %q\n extra:   %q", missing, extra)
	}
}

func TestTheFixtureReportsWhatTheScanCannotReadOrAccept(t *testing.T) {
	res, err := fixture()
	if err != nil {
		t.Fatal(err)
	}
	var all []string
	for _, p := range res.Problems {
		all = append(all, p.String())
	}
	joined := strings.Join(all, "\n")
	for _, want := range []string{
		"whose resource is not static: annotate the call with //rbac:resources", // the unannotated generic call
		"fix.go:1: this store is listed in Options.SkipStores",                  // a skipped store that is not there
		`library entry "no trigger"`,                                            // an entry whose trigger nothing calls
		`library entry "no cite" cites no source`,                               // an entry that cites nothing
		`library entry "old cite" cites`,                                        // a citation of a version go.mod does not pin
		`library entry "cite that says other"`,                                  // a citation the file does not bear out
		`library entry "trigger with no identity"`,                              // a trigger in a place the entry has no identity for
		"an //rbac:resources annotation that no call needs",                     // an annotation on a call whose type is static
		"a REST request (Do) whose path is a string",                            // a request the scan cannot read
		"the client method Get is taken as a value",                             // a call the scan cannot attribute
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("no problem says %q; the problems:\n%s", want, joined)
		}
	}
	// and nothing else: each of those is the fixture's own, and the clean cases raise none
	if len(res.Problems) != 10 && !t.Failed() {
		t.Errorf("the fixture raised %d problems, want the 10 above:\n%s", len(res.Problems), joined)
	}
	// the generic call that is not annotated is named by its position
	var generic bool
	for _, p := range res.Problems {
		if strings.Contains(p.Msg, "not static") && strings.HasPrefix(p.Pos, fixPkg+"/fix.go:") {
			generic = true
		}
	}
	if !generic {
		t.Errorf("the unannotated generic call is not named by a position:\n%s", joined)
	}
	if !slices.ContainsFunc(res.Dead, func(d string) bool { return strings.Contains(d, "Delete Instance") }) {
		t.Errorf("the call in the function no call reaches is not listed as dead: %q", res.Dead)
	}
}
