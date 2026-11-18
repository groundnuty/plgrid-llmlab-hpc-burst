package inventory_test

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/inventory"
)

// Every expected row below is written by hand from the fixture in fixture_test.go: never produced by the lister, and
// never by the printer.

func list(t *testing.T, o inventory.Options) *inventory.Inventory {
	t.Helper()
	inv, err := inventory.List(context.Background(), newReader(t, world()...), o, now)
	if err != nil {
		t.Fatal(err)
	}
	return inv
}

func asJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

const (
	reasonNoOwner   = "no owner annotation"
	reasonNoPurpose = "no purpose annotation"
)

func TestTheFullViewOfANamespaceIsWrittenByHand(t *testing.T) {
	hour := inventory.Span(time.Hour) // the profile "batch" sets maxIdle 1h
	at := func(s string) inventory.Fact {
		tm, err := time.Parse(time.RFC3339, s)
		if err != nil {
			t.Fatal(err)
		}
		return inventory.Instant(tm)
	}
	keep := func(extra ...inventory.Fact) inventory.RetentionRow {
		r := inventory.RetentionRow{Mode: "Keep"}
		if len(extra) > 0 {
			r.ProfileMaxIdle = extra[0]
		}
		return r
	}
	want := &inventory.Inventory{
		SchemaVersion: 1, ObservedAt: "2026-11-20T10:01:30Z", Scope: inventory.Scope{Namespace: "team"},
		ModelDeployments: []inventory.DeploymentRow{
			{Namespace: "team", Name: "md-alice", Owner: inventory.Text(alice), Purpose: inventory.Text("nightly run"), Instances: 2,
				ReadyInstances: inventory.Count(1), EndTime: "2026-11-22T10:00:00Z", Providers: []string{"sano"}},
			{Namespace: "team", Name: "md-bob", Owner: inventory.Text(bob), Purpose: inventory.Text("ablation"), Instances: 1,
				ReadyInstances: inventory.Unknown("the controller has not reported on the ModelDeployment yet"), Providers: []string{"helios"}},
			{Namespace: "team", Name: "md-old", Owner: inventory.Unknown(reasonNoOwner), Purpose: inventory.Unknown(reasonNoPurpose), Instances: 1,
				ReadyInstances: inventory.Count(0), Providers: []string{"helios"}},
		},
		Allocations: []inventory.AllocationRow{
			{Namespace: "team", Name: "alloc-a1", ModelDeployment: inventory.Text("md-alice"), Owner: inventory.Text(alice), Purpose: inventory.Text("nightly run"),
				Phase: inventory.Text("Running"), Provider: inventory.Text("sano"), HardEnd: at("2026-11-20T13:00:00Z"), Retention: keep(hour)},
			{Namespace: "team", Name: "alloc-a2", ModelDeployment: inventory.Text("md-alice"), Owner: inventory.Text(alice), Purpose: inventory.Text("nightly run"),
				Phase: inventory.Text("Running"), Provider: inventory.Text("sano"), HardEnd: at("2026-11-20T12:00:00Z"),
				Retention: inventory.RetentionRow{Mode: "Keep", Until: at("2026-11-21T10:00:00Z"), ProfileMaxIdle: hour},
				IdleSince: at("2026-11-20T09:30:00Z"), KeptNoInstance: true},
			{Namespace: "team", Name: "alloc-a3", ModelDeployment: inventory.Text("md-alice"), Owner: inventory.Text(alice), Purpose: inventory.Text("nightly run"),
				Phase: inventory.Text("Pending"), Provider: inventory.Text("sano"), HardEnd: inventory.Unknown("not observed yet"),
				Retention: inventory.RetentionRow{Mode: "ReleaseWhenIdle", IdleLimit: inventory.Span(15 * time.Minute), ProfileMaxIdle: hour}},
			// An older child of alice's deployment: its owner is unknown, not the deployment's.
			{Namespace: "team", Name: "alloc-a4", ModelDeployment: inventory.Text("md-alice"), Owner: inventory.Unknown(reasonNoOwner), Purpose: inventory.Unknown(reasonNoPurpose),
				Phase: inventory.Text("Running"), Provider: inventory.Text("sano"), HardEnd: at("2026-11-20T13:00:00Z"), Retention: keep(hour)},
			{Namespace: "team", Name: "alloc-a5", ModelDeployment: inventory.Text("md-alice"), Owner: inventory.Text(alice), Purpose: inventory.Text("nightly run"),
				Phase: inventory.Text("Running"), Provider: inventory.Text("sano"), HardEnd: at("2026-11-20T13:00:00Z"), Retention: keep(hour),
				IdleSince: at("2026-11-20T09:40:00Z")},
			{Namespace: "team", Name: "alloc-b1", ModelDeployment: inventory.Text("md-bob"), Owner: inventory.Text(bob), Purpose: inventory.Text("ablation"),
				Phase: inventory.Text("Running"), Provider: inventory.Text("helios"), HardEnd: at("2026-11-20T11:00:00Z"), Retention: keep()},
			{Namespace: "team", Name: "alloc-b2", ModelDeployment: inventory.Text("md-bob"), Owner: inventory.Text(bob), Purpose: inventory.Text("ablation"),
				Phase: inventory.Text("Running"), Provider: inventory.Text("helios"), HardEnd: at("2026-11-20T11:00:00Z"), Retention: keep(),
				IdleSince: inventory.Unknown("the idle-since annotation is not an RFC 3339 time"), KeptNoInstance: true},
			{Namespace: "team", Name: "alloc-b3", ModelDeployment: inventory.Text("md-bob"), Owner: inventory.Text(bob), Purpose: inventory.Text("ablation"),
				Phase: inventory.Text("Ended"), Provider: inventory.Text("ghost"), HardEnd: at("2026-11-20T09:00:00Z"),
				Retention: keep(inventory.Unknown("PlatformProfile missing not found")), IdleSince: at("2026-11-20T08:00:00Z")},
			{Namespace: "team", Name: "alloc-b4", ModelDeployment: inventory.Text("md-bob"), Owner: inventory.Text(bob), Purpose: inventory.Text("ablation"),
				Phase: inventory.Text("Running"), Provider: inventory.Text("helios"), HardEnd: at("2026-11-20T11:00:00Z"),
				Retention: inventory.RetentionRow{Mode: "ReleaseWhenIdle", IdleLimit: inventory.Span(30 * time.Minute)}, IdleSince: at("2026-11-20T09:50:00Z"), KeptNoInstance: true},
			{Namespace: "team", Name: "alloc-b5", ModelDeployment: inventory.Text("md-bob"), Owner: inventory.Text(bob), Purpose: inventory.Text("ablation"),
				Phase: inventory.Text("Running"), Provider: inventory.Text("helios"), HardEnd: at("2026-11-20T11:00:00Z"), Retention: keep(),
				IdleSince: at("2026-11-20T09:00:00Z")},
			{Namespace: "team", Name: "alloc-bare", ModelDeployment: inventory.Unknown("the Allocation names no ModelDeployment"),
				Owner: inventory.Unknown(reasonNoOwner), Purpose: inventory.Unknown(reasonNoPurpose), Phase: inventory.Unknown("no phase reported yet"),
				Provider: inventory.Unknown("the Allocation names no Provider"), HardEnd: inventory.Unknown("not observed yet"),
				Retention: keep(inventory.Unknown("the Allocation names no Provider"))},
			{Namespace: "team", Name: "alloc-old", ModelDeployment: inventory.Text("md-old"), Owner: inventory.Unknown(reasonNoOwner), Purpose: inventory.Unknown(reasonNoPurpose),
				Phase: inventory.Text("Running"), Provider: inventory.Text("helios"), HardEnd: at("2026-11-20T11:00:00Z"), Retention: keep()},
		},
		Instances: []inventory.InstanceRow{
			{Namespace: "team", Name: "inst-a1", Owner: inventory.Text(alice), Purpose: inventory.Text("nightly run"), Phase: inventory.Text("ready"), Allocation: "alloc-a1"},
			{Namespace: "team", Name: "inst-a3", Owner: inventory.Text(alice), Purpose: inventory.Text("nightly run"), Phase: inventory.Text("pending"), Allocation: "alloc-a3"},
			{Namespace: "team", Name: "inst-a4-old", Owner: inventory.Unknown(reasonNoOwner), Purpose: inventory.Unknown(reasonNoPurpose), Phase: inventory.Text("ready"), Allocation: "alloc-a4"},
			{Namespace: "team", Name: "inst-a5-old", Owner: inventory.Unknown(reasonNoOwner), Purpose: inventory.Unknown(reasonNoPurpose), Phase: inventory.Text("ready"), Allocation: "alloc-a5"},
			{Namespace: "team", Name: "inst-b2-ended", Owner: inventory.Text(bob), Purpose: inventory.Text("ablation"), Phase: inventory.Text("ended"), Allocation: "alloc-b2"},
			{Namespace: "team", Name: "inst-b5", Owner: inventory.Text(bob), Purpose: inventory.Text("ablation"), Phase: inventory.Text("ready"), Allocation: "alloc-b5"},
			{Namespace: "team", Name: "inst-old", Owner: inventory.Unknown(reasonNoOwner), Purpose: inventory.Unknown(reasonNoPurpose),
				Phase: inventory.Unknown("no phase reported yet"), Allocation: "alloc-old"},
		},
		Providers: []inventory.ProviderRow{
			{Name: "ghost", LastSuccessfulStatusRead: at("2026-11-20T10:00:00Z"), Age: inventory.Span(90 * time.Second)},
			{Name: "helios", LastSuccessfulStatusRead: inventory.Unknown("never read"), Age: inventory.Unknown("never read")},
			{Name: "sano", LastSuccessfulStatusRead: at("2026-11-20T10:00:00Z"), Age: inventory.Span(90 * time.Second)},
		},
		Operations: []inventory.OperationRow{
			{Namespace: "team", Name: "op-bob", ModelDeployment: "md-bob", Action: "End", PlanID: inventory.Text("plan-2"), Owner: bob},
			{Namespace: "team", Name: "op-wait", ModelDeployment: "md-alice", Action: "Scale", PlanID: inventory.Text("plan-1"), Owner: alice},
		},
	}
	got := list(t, inventory.Options{Namespace: "team"})
	if !reflect.DeepEqual(got, want) {
		t.Errorf("the inventory differs from the rows written by hand.\ngot:\n%s\nwant:\n%s", asJSON(t, got), asJSON(t, want))
	}
}

func names[T any](rows []T, name func(T) string) []string {
	out := []string{}
	for _, r := range rows {
		out = append(out, name(r))
	}
	return out
}

func selected(inv *inventory.Inventory) map[string][]string {
	return map[string][]string{
		"modelDeployments": names(inv.ModelDeployments, func(r inventory.DeploymentRow) string { return r.Name }),
		"allocations":      names(inv.Allocations, func(r inventory.AllocationRow) string { return r.Name }),
		"instances":        names(inv.Instances, func(r inventory.InstanceRow) string { return r.Name }),
		"providers":        names(inv.Providers, func(r inventory.ProviderRow) string { return r.Name }),
		"operations":       names(inv.Operations, func(r inventory.OperationRow) string { return r.Name }),
	}
}

// Two owners' deployments, with children that carry their labels: --owner selects each owner's alone. A deployment of
// no owner, and the children that predate the label, are in the full view and in neither selection.
func TestOwnerSelectsEachOwnersObjectsAlone(t *testing.T) {
	providers := []string{"ghost", "helios", "sano"} // not owned: every selection lists them all
	for _, c := range []struct {
		owner string
		want  map[string][]string
	}{
		{alice, map[string][]string{
			"modelDeployments": {"md-alice"}, "allocations": {"alloc-a1", "alloc-a2", "alloc-a3", "alloc-a5"},
			"instances": {"inst-a1", "inst-a3"}, "providers": providers, "operations": {"op-wait"}}},
		{bob, map[string][]string{
			"modelDeployments": {"md-bob"}, "allocations": {"alloc-b1", "alloc-b2", "alloc-b3", "alloc-b4", "alloc-b5"},
			"instances": {"inst-b2-ended", "inst-b5"}, "providers": providers, "operations": {"op-bob"}}},
		{"example-nobody", map[string][]string{
			"modelDeployments": {}, "allocations": {}, "instances": {}, "providers": providers, "operations": {}}},
	} {
		inv := list(t, inventory.Options{Namespace: "team", Owner: c.owner})
		if got := selected(inv); !reflect.DeepEqual(got, c.want) {
			t.Errorf("--owner %s: got %v, want %v", c.owner, got, c.want)
		}
		if inv.SelectedOwner != c.owner {
			t.Errorf("--owner %s: the inventory names owner %q", c.owner, inv.SelectedOwner)
		}
	}
}

// A deployment with no owner annotation is in the full view with its owner unknown (the science agent's S5), and only a
// selection by owner leaves it out; its children, which have no label, read unknown too. Alice's deployment has an
// owner, and her older child alloc-a4 does not borrow it.
func TestADeploymentCreatedBeforeTheAnnotationIsUnknownAndSoAreItsChildren(t *testing.T) {
	full := list(t, inventory.Options{Namespace: "team"})
	byName := map[string]inventory.AllocationRow{}
	for _, a := range full.Allocations {
		byName[a.Name] = a
	}
	for _, md := range full.ModelDeployments {
		if md.Name == "md-old" && !reflect.DeepEqual(md.Owner, inventory.Unknown(reasonNoOwner)) {
			t.Errorf("md-old: owner %v, want unknown", md.Owner)
		}
	}
	for _, name := range []string{"alloc-old", "alloc-a4"} {
		if !reflect.DeepEqual(byName[name].Owner, inventory.Unknown(reasonNoOwner)) || !reflect.DeepEqual(byName[name].Purpose, inventory.Unknown(reasonNoPurpose)) {
			t.Errorf("%s: owner %v, purpose %v, want both unknown: a child shows what it carries itself", name, byName[name].Owner, byName[name].Purpose)
		}
	}
	for _, in := range full.Instances {
		if (in.Name == "inst-old" || in.Name == "inst-a4-old") && !reflect.DeepEqual(in.Owner, inventory.Unknown(reasonNoOwner)) {
			t.Errorf("%s: owner %v, want unknown", in.Name, in.Owner)
		}
	}
	for _, owner := range []string{alice, bob} {
		sel := selected(list(t, inventory.Options{Namespace: "team", Owner: owner}))
		for _, n := range sel["modelDeployments"] {
			if n == "md-old" {
				t.Errorf("--owner %s lists md-old, which has no owner", owner)
			}
		}
		for _, n := range append(sel["allocations"], sel["instances"]...) {
			if slices.Contains([]string{"alloc-old", "alloc-a4", "inst-old", "inst-a4-old", "inst-a5-old"}, n) {
				t.Errorf("--owner %s lists %s, which carries no label", owner, n)
			}
		}
	}
}

// Capacity held with nothing serving shows its mode, its keep date, its idle-since and the marker, whatever keeps it: a keep
// date (alloc-a2), no date at all (alloc-b2, whose anchor is unreadable), and ReleaseWhenIdle still inside its limit
// (alloc-b4: idle since 09:50, limit 30 minutes, the clock at 10:01:30). Each of the others lacks one of the conditions of the
// marker: a Running job, the anchor, and no live Instance in the full listing.
func TestHeldCapacityWithNoInstanceIsMarkedWhateverKeepsIt(t *testing.T) {
	inv := list(t, inventory.Options{Namespace: "team"})
	marked := map[string]bool{}
	for _, a := range inv.Allocations {
		marked[a.Name] = a.KeptNoInstance
	}
	for name, want := range map[string]bool{
		"alloc-a2": true,  // Running, a keep date, idle-since set, no Instance
		"alloc-b2": true,  // the same, though its anchor is no time: the anchor is there, only its value is unreadable; its one Instance has ended
		"alloc-b4": true,  // ReleaseWhenIdle, inside its limit: held, nothing serving
		"alloc-a1": false, // hosts inst-a1
		"alloc-a3": false, // Pending, and hosts inst-a3
		"alloc-a5": false, // an anchor, but inst-a5-old is on it, and carries no owner label
		"alloc-b1": false, // no idle-since
		"alloc-b3": false, // Ended, though idle-since is set
		"alloc-b5": false, // a stale anchor, and inst-b5 is on it: the Instance of the listing wins
	} {
		if marked[name] != want {
			t.Errorf("%s: marked %v, want %v", name, marked[name], want)
		}
	}
}

// Under --owner the lister selects the owner's Instances, which leaves out inst-a5-old, an Instance of alloc-a5 that predates
// the label. alloc-a5 carries an anchor, so only the full listing says it hosts an Instance: it stays unmarked, and the
// Allocations the selection holds are marked as in the full view.
func TestOwnerNeverMakesAnAllocationLookEmpty(t *testing.T) {
	for _, owner := range []string{alice, bob} {
		sel := list(t, inventory.Options{Namespace: "team", Owner: owner})
		full := list(t, inventory.Options{Namespace: "team"})
		fullMarks := map[string]bool{}
		for _, a := range full.Allocations {
			fullMarks[a.Name] = a.KeptNoInstance
		}
		if len(sel.Allocations) == 0 {
			t.Fatalf("--owner %s lists no Allocation", owner)
		}
		for _, a := range sel.Allocations {
			if a.KeptNoInstance != fullMarks[a.Name] {
				t.Errorf("--owner %s: %s is marked %v, and %v in the full view", owner, a.Name, a.KeptNoInstance, fullMarks[a.Name])
			}
		}
	}
	for _, a := range list(t, inventory.Options{Namespace: "team", Owner: alice}).Allocations {
		if a.Name == "alloc-a5" && a.KeptNoInstance {
			t.Error("alloc-a5 is marked under --owner: the selection without its unlabelled Instance made it look empty")
		}
	}
}

// A Provider's age counts from the clock the caller read once and handed in: read at T, shown at T+90 s, it is 1m30s.
func TestAProvidersAgeComesFromTheInjectedClock(t *testing.T) {
	read := t0
	for _, c := range []struct {
		name string
		p    *v1alpha1.Provider
		now  time.Time
		read inventory.Fact
		age  inventory.Fact
	}{
		{"at T+90s", provider("p", "x", &read), t0.Add(90 * time.Second), inventory.Text("2026-11-20T10:00:00Z"), inventory.Text("1m30s")},
		{"at T+1h2m3s", provider("p", "x", &read), t0.Add(time.Hour + 2*time.Minute + 3*time.Second), inventory.Text("2026-11-20T10:00:00Z"), inventory.Text("1h2m3s")},
		{"the clock has a fraction of a second", provider("p", "x", &read), t0.Add(90*time.Second + 900*time.Millisecond), inventory.Text("2026-11-20T10:00:00Z"), inventory.Text("1m30s")},
		{"a read after the clock", provider("p", "x", &read), t0.Add(-5 * time.Second), inventory.Text("2026-11-20T10:00:00Z"), inventory.Unknown("the read is after the command's clock")},
		{"never read", provider("p", "x", nil), t0, inventory.Unknown("never read"), inventory.Unknown("never read")},
	} {
		inv, err := inventory.List(context.Background(), newReader(t, c.p), inventory.Options{Namespace: "team"}, c.now)
		if err != nil {
			t.Fatal(err)
		}
		if len(inv.Providers) != 1 {
			t.Fatalf("%s: %d Provider rows", c.name, len(inv.Providers))
		}
		pr := inv.Providers[0]
		if got := asJSON(t, pr.LastSuccessfulStatusRead); got != asJSON(t, c.read) {
			t.Errorf("%s: read %s, want %s", c.name, got, asJSON(t, c.read))
		}
		if got := asJSON(t, pr.Age); got != asJSON(t, c.age) {
			t.Errorf("%s: age %s, want %s", c.name, got, asJSON(t, c.age))
		}
		if inv.ObservedAt != c.now.UTC().Truncate(time.Second).Format(time.RFC3339) {
			t.Errorf("%s: observedAt %q is not the clock handed in", c.name, inv.ObservedAt)
		}
	}
}

// Only an Operation in AwaitingApproval is listed: one in each other phase is left out.
func TestOnlyAnOperationAwaitingApprovalIsListed(t *testing.T) {
	var objs []client.Object
	for _, p := range []v1alpha1.OperationPhase{
		v1alpha1.OperationPhasePlanning, v1alpha1.OperationPhasePlanned, v1alpha1.OperationPhaseAwaitingApproval, v1alpha1.OperationPhaseExecuting,
		v1alpha1.OperationPhaseDone, v1alpha1.OperationPhasePartlyFailed, v1alpha1.OperationPhaseStale, v1alpha1.OperationPhaseRefused, "",
	} {
		objs = append(objs, op("team", "op-"+string(p), "md", alice, v1alpha1.OperationActionScale, p, "plan-x"))
	}
	inv, err := inventory.List(context.Background(), newReader(t, objs...), inventory.Options{Namespace: "team"}, now)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := selected(inv)["operations"], []string{"op-AwaitingApproval"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("listed %v, want %v", got, want)
	}
	if got := inv.Operations[0]; got.Namespace != "team" || got.ModelDeployment != "md" || got.Action != "Scale" || got.Owner != alice || !reflect.DeepEqual(got.PlanID, inventory.Text("plan-x")) {
		t.Errorf("the row is %+v", got)
	}
}

// The lister sends every selection to the API server: the namespace, and for an owner the label selector on Allocations
// and Instances, which no other kind gets. With an owner it also reads every Instance in scope, with no selector, to say which
// Allocation hosts one. A full view sends no selector.
func TestTheOwnerSelectionIsALabelSelectorOnTheServer(t *testing.T) {
	for _, c := range []struct {
		name string
		o    inventory.Options
		want []listCall
	}{
		{"a namespace, one owner", inventory.Options{Namespace: "team", Owner: alice}, []listCall{
			{"ModelDeployments", "team", ""}, {"Allocations", "team", "hpcburst.groundnuty.github.io/owner-hash=" + aliceHash},
			{"Instances", "team", "hpcburst.groundnuty.github.io/owner-hash=" + aliceHash}, {"Instances", "team", ""}, {"Operations", "team", ""}, {"Providers", "", ""}, {"PlatformProfiles", "", ""}}},
		{"every namespace, one owner", inventory.Options{AllNamespaces: true, Owner: bob}, []listCall{
			{"ModelDeployments", "", ""}, {"Allocations", "", "hpcburst.groundnuty.github.io/owner-hash=" + bobHash},
			{"Instances", "", "hpcburst.groundnuty.github.io/owner-hash=" + bobHash}, {"Instances", "", ""}, {"Operations", "", ""}, {"Providers", "", ""}, {"PlatformProfiles", "", ""}}},
		{"a namespace, every owner", inventory.Options{Namespace: "team"}, []listCall{
			{"ModelDeployments", "team", ""}, {"Allocations", "team", ""}, {"Instances", "team", ""}, {"Operations", "team", ""}, {"Providers", "", ""}, {"PlatformProfiles", "", ""}}},
	} {
		rr := &recordingReader{Reader: newReader(t, world()...)}
		if _, err := inventory.List(context.Background(), rr, c.o, now); err != nil {
			t.Fatal(err)
		}
		slices.SortStableFunc(rr.calls, func(a, b listCall) int { return strings.Compare(a.kind, b.kind) })
		want := slices.Clone(c.want)
		slices.SortStableFunc(want, func(a, b listCall) int { return strings.Compare(a.kind, b.kind) })
		if !reflect.DeepEqual(rr.calls, want) {
			t.Errorf("%s: lists %+v, want %+v", c.name, rr.calls, want)
		}
	}
}

// -A reads every namespace: one cluster-wide list per kind, and rows of both namespaces.
func TestEveryNamespaceListsEveryNamespace(t *testing.T) {
	inv := list(t, inventory.Options{AllNamespaces: true})
	if !inv.Scope.AllNamespaces || inv.Scope.Namespace != "" {
		t.Errorf("scope %+v", inv.Scope)
	}
	sel := selected(inv)
	if want := []string{"md-other", "md-alice", "md-bob", "md-old"}; !reflect.DeepEqual(sel["modelDeployments"], want) {
		t.Errorf("deployments %v, want %v", sel["modelDeployments"], want)
	}
	if want := []string{"op-other", "op-bob", "op-wait"}; !reflect.DeepEqual(sel["operations"], want) {
		t.Errorf("operations %v, want %v (sorted by namespace, then name)", sel["operations"], want)
	}
	other := list(t, inventory.Options{Namespace: "other"})
	if want := []string{"md-other"}; !reflect.DeepEqual(selected(other)["modelDeployments"], want) {
		t.Errorf("namespace other: %v, want %v", selected(other)["modelDeployments"], want)
	}
}

// The lists are arrays, never null, and an unknown is an object that holds its reason.
func TestEmptyListsAreArraysAndAnUnknownHoldsItsReason(t *testing.T) {
	inv, err := inventory.List(context.Background(), newReader(t), inventory.Options{Namespace: "empty"}, now)
	if err != nil {
		t.Fatal(err)
	}
	b := asJSON(t, inv)
	if strings.Contains(b, "null") {
		t.Errorf("the empty inventory holds a null:\n%s", b)
	}
	for _, key := range []string{`"modelDeployments": []`, `"allocations": []`, `"instances": []`, `"providers": []`, `"operationsAwaitingApproval": []`} {
		if !strings.Contains(b, key) {
			t.Errorf("the empty inventory lacks %s:\n%s", key, b)
		}
	}
	for _, c := range []struct {
		f    inventory.Fact
		want string
	}{
		{inventory.Text("x"), `"x"`}, {inventory.Count(0), `0`}, {inventory.Span(90 * time.Second), `"1m30s"`},
		{inventory.Unknown("because"), `{"unknown":"because"}`},
	} {
		got, err := json.Marshal(c.f)
		if err != nil || string(got) != c.want {
			t.Errorf("%v marshals to %s (%v), want %s", c.f, got, err, c.want)
		}
	}
}

func TestListWithNoNamespaceAndNotEveryNamespaceIsRefused(t *testing.T) {
	if _, err := inventory.List(context.Background(), newReader(t), inventory.Options{}, now); err == nil {
		t.Error("a listing of no namespace succeeded")
	}
}
