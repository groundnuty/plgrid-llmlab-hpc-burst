package rbacscan

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/config/rbac"
)

const (
	lifecycle = "hpcburst-lifecycle"
	provider  = "hpcburst-provider"
	front     = "hpcburst-front"
)

func repoRoot() string {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		panic(err)
	}
	return root
}

func realOptions() Options {
	root := repoRoot()
	return Options{
		Root:     root,
		Patterns: []string{"./cmd/hpcburst-controller", "./cmd/hpcburst-front"},
		CRDDir:   filepath.Join(root, "config", "crd"),
		Managers: []Source{
			{Where: "internal/lifecycle.NewManager", Ident: lifecycle, Why: "the lifecycle controller's manager, made with --kubeconfig"},
			{Where: "internal/providerctl.NewProviderManager", Ident: provider, Why: "every Provider's manager, made with --provider-kubeconfig"},
		},
		Library: realLibrary(),
		SkipStores: []Skip{
			{Pos: "internal/managed/controller.go:167", Why: "the fallback of a nil Reader to the cached client, which only a test leaves nil: the controller is built with mgr.GetAPIReader() (controller.go:213), and its identity-record reads are the API reader's"},
		},
		Clients: []Source{
			{Where: "cmd/hpcburst-front", Ident: front, Why: "the front's clientset and dynamic client, made with its --kubeconfig"},
			{Where: "internal/launcher", Ident: Exempt, Why: "the launcher acts as the administrator"},
			{Where: "internal/leaderlock", Ident: Exempt, Why: "the Lease lock's clients: the library entry for the lock stands for its calls"},
		},
	}
}

// The real tree is scanned once for all the tests below.
var realScan = sync.OnceValues(func() (*Result, error) { return Scan(realOptions()) })

func realResult(t *testing.T) (*Result, Roles) {
	t.Helper()
	res, err := realScan()
	if err != nil {
		t.Fatal(err)
	}
	roles, err := ParseRoles(rbac.Controllers)
	if err != nil {
		t.Fatal(err)
	}
	return res, roles
}

// TestEveryControllerCallIsGrantedAndEveryGrantIsCalled is the check itself: the controllers' roles in
// config/rbac/controllers.yaml against the calls the code makes, in both directions. It fails naming the call site of a
// call a role does not grant (a refused call shows only as a recurring pass error, so no other test would), and the rule of
// a role that nothing calls (S2: each role grants exactly what its component calls).
func TestEveryControllerCallIsGrantedAndEveryGrantIsCalled(t *testing.T) {
	res, roles := realResult(t)
	if fs := Check(res, roles); len(fs) > 0 {
		t.Fatalf("%d disagreements between the code and config/rbac/controllers.yaml:\n%s\nA call a role lacks: add the rule to the role of the identity named, in the same change as the call. A rule nothing "+
			"calls: delete it. A call the scan cannot read: see the message, and doc.go.", len(fs), Format(fs))
	}
}

// A scan that read nothing would pass the check above, with every role "unused" aside. These floors say the scan saw each
// identity at work, and name the call sites it must find: the three the lifecycle controller's roles missed on 17 Nov.
func TestTheScanSeesEachIdentityAtWork(t *testing.T) {
	res, roles := realResult(t)
	count := map[string]int{}
	for _, st := range res.Sites {
		count[st.Ident]++
	}
	for id, floor := range map[string]int{lifecycle: 60, provider: 25, front: 3} {
		if count[id] < floor {
			t.Errorf("the scan found %d sites for %s, want at least %d", count[id], id, floor)
		}
	}
	for _, id := range []string{lifecycle, provider, front} {
		if len(roles[id]) == 0 {
			t.Errorf("the manifest has no rules for %s", id)
		}
	}
	for _, want := range []struct{ ident, file, fn, what string }{
		{lifecycle, "internal/lifecycle/owner.go", "syncOwnerLabel", ""},
		{lifecycle, "internal/lifecycle/opexec.go", "afterCommitError", ""},
		{lifecycle, "internal/lifecycle/opexec.go", "keepOperationUntilFinished", ""},
		{lifecycle, "cmd/hpcburst-controller/tunnel.go", "pluginFeed", "cache.GetInformer"}, // through the interface tunnel.go declares
		{provider, "internal/providerctl/keeper.go", "", ""},
		{front, "internal/front/records.go", "listRecords", ""},
	} {
		found := false
		for _, st := range res.Sites {
			if st.Ident == want.ident && strings.HasPrefix(st.Pos, want.file+":") && strings.Contains(st.Func, want.fn) && strings.Contains(st.What, want.what) {
				found = true
			}
		}
		if !found {
			t.Errorf("the scan found no site of %s in %s %s %s", want.ident, want.file, want.fn, want.what)
		}
	}
	t.Logf("%d sites (%d for %s, %d for %s, %d for %s), %d library entries, %d call sites in code nothing calls", len(res.Sites),
		count[lifecycle], lifecycle, count[provider], provider, count[front], front, len(res.Entries), len(res.Dead))
}

// The calls in code that nothing calls are not counted, so a call the scan wrongly takes for unreached would pass unread. The
// module has none: the Operation removal controller (opttl.go), which was the one allowed to be in that state, is wired into the
// lifecycle manager (#660), and its calls are read like any other (its delete of an Operation is the lifecycle role's rule). Any
// call the scan finds dead fails here, until it is looked at and either removed or explained here.
func TestNoCallOfTheModuleIsInCodeNothingCalls(t *testing.T) {
	res, _ := realResult(t)
	for _, d := range res.Dead {
		t.Errorf("a call in code the scan takes for unreached: %s\n"+
			"If nothing really calls it, list it here with the reason; if something does, the scan lost the call.", d)
	}
}

// The removal controller's delete of an Operation is a live site of the lifecycle identity, found at opttl.go's own call now that
// the lifecycle manager wires the controller, and the lifecycle role grants it: the check above is clean with it, and this names
// the site, so a scan that lost it (the controller unwired, or its client no longer followed) fails here and not silently.
func TestTheOperationRemovalsDeleteIsALiveSiteThatTheLifecycleRoleGrants(t *testing.T) {
	res, roles := realResult(t)
	want := Triple{Group: v1alpha1.Group, Resource: "operations", Verb: "delete"}
	var found []string
	for _, st := range res.Sites {
		if st.Ident == lifecycle && strings.HasPrefix(st.Pos, "internal/lifecycle/opttl.go:") && slices.Contains(st.Need, want) {
			found = append(found, st.Pos+" "+st.What)
		}
	}
	if len(found) == 0 {
		t.Errorf("the scan found no site of %s in internal/lifecycle/opttl.go that needs %s", lifecycle, want)
	}
	if !roles[lifecycle][want] {
		t.Errorf("the role of %s does not grant %s", lifecycle, want)
	}
	t.Logf("%s needs %s at: %s", lifecycle, want, strings.Join(found, "; "))
}

// Every rule of every role is a mutant: with one verb removed, the check fails and names a call site that needs it (for a
// verb a library makes, the call that triggers the library). The same loop is the proof that no rule is there for nothing.
func TestAVerbRemovedFromAnyRoleFailsNamingACallSite(t *testing.T) {
	res, roles := realResult(t)
	n := 0
	for _, id := range roles.Idents() {
		for tr := range roles[id] {
			n++
			var named []string
			for _, f := range Check(res, roles.Without(id, tr)) {
				if f.Kind == "not granted" && f.Ident == id && f.Need == tr {
					named = append(named, f.Msg)
				}
			}
			if len(named) == 0 {
				t.Errorf("%s's role without %q passes: nothing names a call site that needs it", id, tr)
				continue
			}
			if !strings.Contains(named[0], ".go:") {
				t.Errorf("the finding for %s without %q does not name a call site: %s", id, tr, named[0])
			}
		}
	}
	t.Logf("%d rules, each one's removal named a call site", n)
}

// The three seams of 17 Nov 2026, found by hand, each removed in turn: each fails, naming the call it would have refused.
func TestTheThreeSilentSeamsWouldHaveFailed(t *testing.T) {
	res, roles := realResult(t)
	for _, tc := range []struct {
		name  string
		tr    Triple
		names []string // what the findings must name, all of it
	}{
		{"#715's label patch (owner.go)", Triple{hpcburst, "modeldeployments", "patch"}, []string{"internal/lifecycle/owner.go:", "syncOwnerLabel"}},
		{"#707's Operation finalizer (opexec.go)", Triple{hpcburst, "operations", "update"}, []string{"internal/lifecycle/opexec.go:", "keepOperationUntilFinished", "releaseOperationFinalizer"}},
		{"#664's read of the Operation after a lost commit (afterCommitError)", Triple{hpcburst, "operations", "get"}, []string{"internal/lifecycle/opexec.go:", "afterCommitError"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var msgs []string
			for _, f := range Check(res, roles.Without(lifecycle, tc.tr)) {
				if f.Kind != "not granted" {
					t.Errorf("a finding that is not about the missing rule: %s", f)
				}
				msgs = append(msgs, f.Msg)
			}
			all := strings.Join(msgs, "\n")
			if len(msgs) == 0 {
				t.Fatalf("the role without %q passes", tc.tr)
			}
			for _, n := range tc.names {
				if !strings.Contains(all, n) {
					t.Errorf("no finding names %q:\n%s", n, all)
				}
			}
		})
	}
}

// A ClusterRoleBinding whose subject is the right name in another namespace binds nothing to the controller's account, and
// every role mutant would still see the permissions it read before. The parse refuses it.
func TestABindingSubjectInAnotherNamespaceFailsTheParse(t *testing.T) {
	m := string(rbac.Controllers)
	i := strings.Index(m, "kind: ClusterRoleBinding\nmetadata:\n  name: hpcburst-lifecycle")
	if i < 0 {
		t.Fatal("the manifest has no ClusterRoleBinding of the lifecycle identity")
	}
	const subjectNS = "    namespace: hpcburst-system"
	j := strings.Index(m[i:], subjectNS)
	if j < 0 {
		t.Fatal("the lifecycle binding's subject names no namespace")
	}
	mutated := m[:i+j] + "    namespace: elsewhere" + m[i+j+len(subjectNS):]
	if _, err := ParseRoles([]byte(mutated)); err == nil || !strings.Contains(err.Error(), "name and namespace") {
		t.Fatalf("ParseRoles of a binding whose subject is in another namespace: %v, want a refusal naming the namespace", err)
	}
}

// A status write is checked against the status rule, not its parent's: with operations/status's update taken from the
// lifecycle role, which keeps operations' update, the check fails and names the Operation status write.
func TestAStatusWriteIsNotSatisfiedByItsParentInTheRealRole(t *testing.T) {
	res, roles := realResult(t)
	parent, status := Triple{hpcburst, "operations", "update"}, Triple{hpcburst, "operations/status", "update"}
	if !roles[lifecycle][parent] || !roles[lifecycle][status] {
		t.Fatalf("the lifecycle role lacks %q or %q", parent, status)
	}
	var named bool
	for _, f := range Check(res, roles.Without(lifecycle, status)) {
		if f.Kind == "not granted" && f.Need == status && strings.Contains(f.Msg, "internal/lifecycle/operations.go:") {
			named = true
		}
	}
	if !named {
		t.Errorf("the lifecycle role without %q, with %q kept, passes", status, parent)
	}
}

// The managed reconciler's Secret path has no library entry because the Allocation CRD refuses the field that turns it on:
// without the rule, the roles would lack what an Allocation naming a connection Secret makes the reconciler do.
func TestTheManagedReconcilersSecretPathIsRefusedAtAdmission(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(repoRoot(), "config", "crd", "hpcburst.groundnuty.github.io_allocations.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "rule: '!has(self.writeConnectionSecretToRef)'") {
		t.Error("the Allocation CRD no longer refuses spec.writeConnectionSecretToRef: crossplane-runtime's managed reconciler would then publish a connection Secret " +
			"(pkg/reconciler/managed/api.go APILocalSecretPublisher.PublishConnection) through an informer the Provider's role cannot sync, so either restore the rule or give the managed-reconciler library entry that path's rights")
	}
	// and the reconciler's other Allocation-driven paths are the ones the entry lists (crossplane-runtime v2.4.3): references
	// are resolved only for a type with a ResolveReferences method, which Allocation has not
	if _, ok := reflect.TypeOf(&v1alpha1.Allocation{}).MethodByName("ResolveReferences"); ok {
		t.Error("v1alpha1.Allocation has a ResolveReferences method: crossplane-runtime's reference resolver (pkg/reconciler/managed/api.go:237-244) would read and patch what it names")
	}
}

// A rule added to a role that nothing calls fails the check, naming the rule.
func TestARuleWithNoCallFails(t *testing.T) {
	res, roles := realResult(t)
	for _, tc := range []struct {
		ident string
		tr    Triple
	}{
		{lifecycle, Triple{hpcburst, "approvals", "delete"}},
		{provider, Triple{"", "secrets", "get"}},
		{front, Triple{hpcburst, "allocations", "watch"}},
	} {
		fs := Check(res, roles.With(tc.ident, tc.tr))
		if len(fs) != 1 || fs[0].Kind != "unused rule" || fs[0].Ident != tc.ident || fs[0].Need != tc.tr || !strings.Contains(fs[0].Msg, tc.tr.String()) {
			t.Errorf("%s with the extra rule %q: findings %v, want exactly the unused rule", tc.ident, tc.tr, fs)
		}
	}
}

// An identity the manifest does not hold, whose code makes calls, is a finding for each call.
func TestACallOfAnIdentityWithNoRoleFails(t *testing.T) {
	res, roles := realResult(t)
	delete(roles, front)
	var named bool
	for _, f := range Check(res, roles) {
		if f.Kind == "not granted" && f.Ident == front && strings.Contains(f.Msg, "internal/front/") {
			named = true
		}
	}
	if !named {
		t.Error("the front without a role passes")
	}
}

const (
	hpcburst = "hpcburst.groundnuty.github.io"
	crVer    = "v0.23.1"
	cgVer    = "v0.35.3"
	xpVer    = "v2.4.3"
	crModule = "sigs.k8s.io/controller-runtime"
	cgModule = "k8s.io/client-go"
	xpModule = "github.com/crossplane/crossplane-runtime/v2"
)

// realLibrary are the calls the libraries make for the controllers, which no walk of the module's code can see. Each entry
// is triggered by a call (or a reference) in the code, so that the rights go when the call does, and cites the pinned
// source of the library that makes the requests. Not an entry, on purpose: the Secrets crossplane-runtime's managed
// reconciler would publish for an Allocation that names spec.writeConnectionSecretToRef. The Allocation CRD refuses the
// field at admission (config/crd, the rule "!has(self.writeConnectionSecretToRef)", with its rows in api/v1alpha1/e2e and
// hack/mutate-cel.sh; TestTheManagedReconcilersSecretPathIsRefusedAtAdmission pins it), because the Provider's identity has
// no rights on Secrets and S2 grants only what each controller does today: a reconciler that tried would start a
// cluster-wide Secret informer that its role cannot sync, and the Allocation would never be reconciled.
func realLibrary() []LibraryEntry {
	events := func(group string) []Triple {
		return []Triple{{group, "events", "create"}, {group, "events", "patch"}}
	}
	return []LibraryEntry{
		{
			ID:      "lease lock",
			Trigger: "{mod}/internal/leaderlock.New",
			For: []Source{
				{Where: "internal/lifecycle", Ident: lifecycle, Why: "the lifecycle controller's Lease, in its manager's lock"},
				{Where: "internal/providerctl", Ident: provider, Why: "each Provider manager's Lease, provider-<name>"},
			},
			Rights: []Triple{{"coordination.k8s.io", "leases", "get"}, {"coordination.k8s.io", "leases", "create"}, {"coordination.k8s.io", "leases", "update"}},
			Cites: []Cite{
				{cgModule, cgVer, "tools/leaderelection/resourcelock/leaselock.go", "ll.Client.Leases(ll.LeaseMeta.Namespace).Get("},
				{cgModule, cgVer, "tools/leaderelection/resourcelock/leaselock.go", "ll.Client.Leases(ll.LeaseMeta.Namespace).Create("},
				{cgModule, cgVer, "tools/leaderelection/resourcelock/leaselock.go", "ll.Client.Leases(ll.LeaseMeta.Namespace).Update("},
			},
		},
		{
			ID:           "events.k8s.io recorder of a manager",
			Trigger:      "(sigs.k8s.io/controller-runtime/pkg/recorder.Provider).GetEventRecorder",
			FromReceiver: true,
			Rights:       events("events.k8s.io"),
			Cites: []Cite{
				{crModule, crVer, "pkg/internal/recorder/recorder.go", "StartRecordingToSinkWithContext"},
				{cgModule, cgVer, "tools/events/event_broadcaster.go", "newEvent, err = sink.Patch(ctx, event, patch)"},
				{cgModule, cgVer, "tools/events/event_broadcaster.go", "newEvent, err = sink.Create(ctx, event)"},
			},
		},
		{
			ID:           "core recorder of a manager",
			Trigger:      "(sigs.k8s.io/controller-runtime/pkg/recorder.Provider).GetEventRecorderFor",
			FromReceiver: true,
			Rights:       events(""),
			Cites: []Cite{
				{crModule, crVer, "pkg/internal/recorder/recorder.go", "corev1client.EventSinkImpl"},
				{cgModule, cgVer, "tools/record/event.go", "newEvent, err = sink.Patch(event, patch)"},
				{cgModule, cgVer, "tools/record/event.go", "newEvent, err = sink.Create(event)"},
			},
		},
		{
			ID:      "events.k8s.io broadcaster of the front",
			Trigger: "k8s.io/client-go/tools/events.NewBroadcaster",
			For:     []Source{{Where: "internal/front", Ident: front, Why: "the front's recorder, over its clientset"}},
			Rights:  events("events.k8s.io"),
			Cites: []Cite{
				{cgModule, cgVer, "tools/events/event_broadcaster.go", "newEvent, err = sink.Patch(ctx, event, patch)"},
				{cgModule, cgVer, "tools/events/event_broadcaster.go", "newEvent, err = sink.Create(ctx, event)"},
			},
		},
		{
			ID:      "managed reconciler",
			Trigger: "github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed.NewReconciler",
			For:     []Source{{Where: "internal/providerctl", Ident: provider, Why: "each Provider's reconciler over Allocations"}},
			Rights: []Triple{
				{hpcburst, "allocations", "list"}, {hpcburst, "allocations", "watch"}, // r.client.Get, through the cached client
				{hpcburst, "allocations", "update"}, {hpcburst, "allocations/status", "update"},
			},
			Cites: []Cite{
				{xpModule, xpVer, "pkg/reconciler/managed/reconciler.go", "if err := r.client.Get(ctx, req.NamespacedName, managed); err != nil {"},
				{xpModule, xpVer, "pkg/reconciler/managed/reconciler.go", "r.client.Status().Update(ctx, managed)"},
				{xpModule, xpVer, "pkg/reconciler/managed/reconciler.go", "if err := r.client.Update(ctx, managed); err != nil {"},
			},
		},
		{
			ID:      "managed finalizer",
			Trigger: "github.com/crossplane/crossplane-runtime/v2/pkg/resource.NewAPIFinalizer",
			For:     []Source{{Where: "internal/providerctl", Ident: provider, Why: "the Allocation's finalizer"}},
			Rights:  []Triple{{hpcburst, "allocations", "update"}},
			Cites: []Cite{
				{xpModule, xpVer, "pkg/resource/api.go", "errors.Wrap(a.client.Update(ctx, obj), errUpdateObject)"},
			},
		},
	}
}
