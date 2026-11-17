package lifecycle

// Row 11 of TestAgentCLIOnAControlPlane (agentcli_envtest_test.go): hpcburst inventory (internal/inventory; issue #662)
// against a real API server, as agents A and B, never as the admin. It is a subtest of the SAME test, so it adds no control
// plane start: the world is the one the earlier rows made, with the Operations they left, and this file adds to it what the
// rows need.
//
// No lifecycle controller runs in this world, so nothing stamps an owner on a child. The person stores what the controller would
// have stamped, with the owner-hash label worked out in the shell with sha256sum (the first 12 hex characters of the SHA-256 of
// the user name, behind "u-"), never by api/v1alpha1.OwnerHash. Each agent creates its own deployment, as the owner pin requires
// of an agent's create. The person's deployment "md", which carries no owner annotation, is the S5 case: it is listed with its
// owner unknown, and --owner leaves it out. Every expected value is written by hand from HBEP-10 and docs/agent-cli.md; the set
// of Operations that wait is read from the API server by a plain list, not by the lister.

import (
	"encoding/json"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agentcli"
)

const (
	invUserA = "system:serviceaccount:hpcburst-agents:agent-a"
	invUserB = "system:serviceaccount:hpcburst-agents:agent-b"
	invHashA = "u-77d125ed9ec1"
	invHashB = "u-494c5a26e66f"
)

// agentCreatesDeployment is an agent's own create of a deployment: the owner annotation is its user name, which the owner pin
// requires, and nothing is named in the spec that the capacity guard forbids.
func (w *cliWorld) agentCreatesDeployment(t *testing.T, as *envIdentity, name, purpose string) {
	t.Helper()
	md := mdSpec(name)
	md.Generation = 0
	md.Spec.OverAllocation = ptrTo(int32(0))
	md.Spec.Providers = v1alpha1.ProviderSelection{Names: []string{cliProvider}}
	md.Annotations = map[string]string{v1alpha1.AnnotationOwner: as.user, v1alpha1.AnnotationPurpose: purpose}
	if err := as.c.Create(w.e.ctx, md); err != nil {
		t.Fatalf("%s creates ModelDeployment %s: %v", as.user, name, err)
	}
	if err := w.e.admin.c.Get(w.e.ctx, client.ObjectKeyFromObject(md), md); err != nil {
		t.Fatal(err)
	}
	// What the lifecycle controller would have reported: one Instance ready.
	md.Status.ReadyInstances, md.Status.LastReconcileTime = 1, ptrTo(metav1.Now())
	if err := w.e.admin.c.Status().Update(w.e.ctx, md); err != nil {
		t.Fatalf("write the status of ModelDeployment %s: %v", name, err)
	}
}

// stampOwner is what the lifecycle controller does for each Allocation and Instance it creates: the owner and purpose
// annotations of its deployment, and the owner-hash label.
func (w *cliWorld) stampOwner(t *testing.T, obj client.Object, user, hash, purpose string) {
	t.Helper()
	if err := w.e.admin.c.Get(w.e.ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
		t.Fatal(err)
	}
	ann, lab := obj.GetAnnotations(), obj.GetLabels()
	if ann == nil {
		ann = map[string]string{}
	}
	if lab == nil {
		lab = map[string]string{}
	}
	ann[v1alpha1.AnnotationOwner], ann[v1alpha1.AnnotationPurpose], lab[v1alpha1.LabelOwnerHash] = user, purpose, hash
	obj.SetAnnotations(ann)
	obj.SetLabels(lab)
	if err := w.e.admin.c.Update(w.e.ctx, obj); err != nil {
		t.Fatalf("stamp %s: %v", obj.GetName(), err)
	}
}

// storeOwnedChildren stores a started Allocation with a ready Instance on it for an agent's deployment, stamped with the owner.
func (w *cliWorld) storeOwnedChildren(t *testing.T, md, user, hash, purpose string) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)
	started, hardEnd := metav1.NewTime(now.Add(-time.Hour)), metav1.NewTime(now.Add(3*time.Hour))
	alloc, in := "alloc-"+md, "inst-"+md
	w.cliAllocation(t, alloc, md, v1alpha1.AllocationStatus{AtProvider: v1alpha1.AllocationObservation{
		Phase: v1alpha1.AllocationRunning, StartTime: &started, HardEnd: &hardEnd}})
	w.cliInstance(t, in, alloc, v1alpha1.InstanceStatus{Phase: v1alpha1.InstanceReady, ReadyTime: &started, LeaseEnd: &hardEnd})
	w.stampOwner(t, &v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: alloc}}, user, hash, purpose)
	w.stampOwner(t, &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Namespace: rigNS, Name: in}}, user, hash, purpose)
}

// requestLog records every request a command makes: its method, its path and its query.
type requestLog struct {
	mu   sync.Mutex
	reqs []requestRecord
}

type requestRecord struct{ method, path, query string }

func (l *requestLog) wrap(next http.RoundTripper) http.RoundTripper {
	return roundTripFunc(func(r *http.Request) (*http.Response, error) {
		l.mu.Lock()
		l.reqs = append(l.reqs, requestRecord{r.Method, r.URL.Path, r.URL.RawQuery})
		l.mu.Unlock()
		return next.RoundTrip(r)
	})
}

func (l *requestLog) all() []requestRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.reqs)
}

// invDoc is the JSON the command printed, as generic values: the test reads it by hand-written keys, and never through the
// package's own structs.
type invDoc map[string]any

func (d invDoc) rows(t *testing.T, list string) map[string]map[string]any {
	t.Helper()
	items, ok := d[list].([]any)
	if !ok {
		t.Fatalf("%q is %T, want a list (null is not one)", list, d[list])
	}
	out := map[string]map[string]any{}
	for _, it := range items {
		row := it.(map[string]any)
		out[row["name"].(string)] = row
	}
	return out
}

func invKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// invOwners maps each row's name to its owner when it is known, and to "unknown" for the object the reason {"unknown": ...} holds.
func invOwners(rows map[string]map[string]any) map[string]string {
	out := map[string]string{}
	for name, r := range rows {
		switch v := r["owner"].(type) {
		case string:
			out[name] = v
		case map[string]any:
			if _, ok := v["unknown"].(string); ok {
				out[name] = "unknown"
				continue
			}
			out[name] = "?"
		}
	}
	return out
}

func invSameOwners(got, want map[string]string) bool {
	if len(got) != len(want) {
		return false
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

func (w *cliWorld) inventoryAs(t *testing.T, env agentcli.Env, args ...string) invDoc {
	t.Helper()
	r := w.cli(t, env, false, "", append([]string{"inventory", "--json", "-n", rigNS}, args...)...)
	if r.code != 0 {
		t.Fatalf("inventory %v: exit %d, stderr %q", args, r.code, r.errs)
	}
	if strings.Contains(r.out, "null") {
		t.Fatalf("the JSON holds a null:\n%s", r.out)
	}
	var d invDoc
	if err := json.Unmarshal([]byte(r.out), &d); err != nil {
		t.Fatalf("inventory %v printed no JSON: %v\n%s", args, err, r.out)
	}
	if d["schemaVersion"] != float64(1) {
		t.Errorf("schemaVersion %v, want 1", d["schemaVersion"])
	}
	return d
}

// waitingOperations are the names of the Operations of the namespace in AwaitingApproval, read by a plain list as the person.
// If the person's world is read this way and the command lists the same names, the command read the API server's phase.
func (w *cliWorld) waitingOperations(t *testing.T, owner string) []string {
	t.Helper()
	var l v1alpha1.OperationList
	if err := w.e.admin.c.List(w.e.ctx, &l, client.InNamespace(rigNS)); err != nil {
		t.Fatal(err)
	}
	out := []string{}
	for _, o := range l.Items {
		if o.Status.Phase == v1alpha1.OperationPhaseAwaitingApproval && (owner == "" || o.Spec.Owner == owner) {
			out = append(out, o.Name)
		}
	}
	slices.Sort(out)
	return out
}

func (w *cliWorld) inventoryRow(t *testing.T, asA, asB agentcli.Env) {
	e := w.e
	if e.a.user != invUserA || e.b.user != invUserB {
		t.Fatalf("the identities are %q and %q, not the names the owner-hash literals were worked out for", e.a.user, e.b.user)
	}
	w.agentCreatesDeployment(t, e.a, "inv-a", "inventory row of agent A")
	w.agentCreatesDeployment(t, e.b, "inv-b", "inventory row of agent B")
	w.storeOwnedChildren(t, "inv-a", invUserA, invHashA, "inventory row of agent A")
	w.storeOwnedChildren(t, "inv-b", invUserB, invHashB, "inventory row of agent B")
	if got := w.waitingOperations(t, ""); !slices.Contains(got, "row1") || slices.Contains(got, "row5") {
		t.Fatalf("the earlier rows left %v waiting, want row1 among them and the Planned row5 not", got)
	}

	// Agent A, the full view of the namespace, with every request recorded.
	log := &requestLog{}
	logged := asA
	logged.WrapTransport = log.wrap
	before := w.worldVersions(t)
	full := w.inventoryAs(t, logged)
	if after := w.worldVersions(t); !sameMap(before, after) {
		t.Errorf("the inventory changed the world: %v -> %v", before, after)
	}

	mds := full.rows(t, "modelDeployments")
	if got, want := invOwners(mds), map[string]string{opMD: "unknown", "md-b": "unknown", "md-c": "unknown", "inv-a": invUserA, "inv-b": invUserB}; !invSameOwners(got, want) {
		t.Errorf("the deployments' owners are %v, want %v (the person's own, with no annotation, are unknown)", got, want)
	}
	if mds["inv-a"]["purpose"] != "inventory row of agent A" || mds["inv-a"]["readyInstances"] != float64(1) || mds["inv-a"]["instances"] != float64(1) {
		t.Errorf("agent A's deployment: %v", mds["inv-a"])
	}
	if p, _ := mds["inv-a"]["providers"].([]any); len(p) != 1 || p[0] != cliProvider {
		t.Errorf("agent A's deployment uses %v, want [%s]", mds["inv-a"]["providers"], cliProvider)
	}
	if u, _ := mds[opMD]["readyInstances"].(map[string]any); u["unknown"] == nil {
		t.Errorf("the person's deployment, which no controller reported on, shows %v ready, want an unknown", mds[opMD]["readyInstances"])
	}

	allocs, insts := full.rows(t, "allocations"), full.rows(t, "instances")
	if got, want := invOwners(allocs), map[string]string{"alloc-r": "unknown", "alloc-p": "unknown", "alloc-b1": "unknown", "alloc-c1": "unknown", "alloc-inv-a": invUserA, "alloc-inv-b": invUserB}; !invSameOwners(got, want) {
		t.Errorf("the Allocations' owners are %v, want %v", got, want)
	}
	if got, want := invOwners(insts), map[string]string{"inst-r": "unknown", "inst-p": "unknown", "inst-inv-a": invUserA, "inst-inv-b": invUserB}; !invSameOwners(got, want) {
		t.Errorf("the Instances' owners are %v, want %v", got, want)
	}
	if allocs["alloc-r"]["phase"] != "Running" || allocs["alloc-p"]["phase"] != "Pending" || allocs["alloc-r"]["provider"] != cliProvider || allocs["alloc-r"]["modelDeployment"] != opMD {
		t.Errorf("the world's Allocations: %v and %v", allocs["alloc-r"], allocs["alloc-p"])
	}
	if h, _ := allocs["alloc-r"]["hardEnd"].(string); h == "" {
		t.Errorf("a started Allocation shows no hard end: %v", allocs["alloc-r"])
	}
	if allocs["alloc-r"]["retention"].(map[string]any)["mode"] != "Keep" {
		t.Errorf("the effective retention of an Allocation with no policy is %v, want Keep", allocs["alloc-r"]["retention"])
	}
	if insts["inst-r"]["phase"] != "ready" || insts["inst-r"]["allocation"] != "alloc-r" || insts["inst-p"]["phase"] != "pending" {
		t.Errorf("the world's Instances: %v and %v", insts["inst-r"], insts["inst-p"])
	}

	// The Provider's age is the command's clock minus its last read, from the one clock the document names.
	provs := full.rows(t, "providers")
	read, err := time.Parse(time.RFC3339, provs[cliProvider]["lastSuccessfulStatusRead"].(string))
	if err != nil {
		t.Fatalf("the Provider's last read: %v", err)
	}
	observed, err := time.Parse(time.RFC3339, full["observedAt"].(string))
	if err != nil {
		t.Fatalf("observedAt: %v", err)
	}
	age, err := time.ParseDuration(provs[cliProvider]["age"].(string))
	if err != nil || age != observed.Sub(read) {
		t.Errorf("the Provider's age is %v (%v), want observedAt %v minus its read %v", provs[cliProvider]["age"], err, observed, read)
	}

	// The Operations that wait are the API server's: row1, and each other Operation in AwaitingApproval, and no Planned one.
	ops := full.rows(t, "operationsAwaitingApproval")
	if got, want := invKeys(ops), w.waitingOperations(t, ""); !slices.Equal(got, want) {
		t.Errorf("the inventory lists the Operations %v, the API server has %v waiting", got, want)
	}
	if ops["row1"]["action"] != "Scale" || ops["row1"]["owner"] != invUserA || ops["row1"]["modelDeployment"] != opMD || ops["row1"]["planID"] == nil {
		t.Errorf("row1: %v", ops["row1"])
	}

	// The command only reads: every request is a GET, but the one SelfSubjectReview that says who the caller is. The two lists
	// that the owner owns carry no selector in the full view.
	posts, lists := 0, map[string]int{}
	for _, r := range log.all() {
		switch {
		case r.method == http.MethodPost && strings.HasSuffix(r.path, "/selfsubjectreviews"):
			posts++
		case r.method == http.MethodGet:
			lists[r.path[strings.LastIndex(r.path, "/")+1:]]++
		default:
			t.Errorf("the inventory sent %s %s: it may read, and ask who the caller is, and nothing else", r.method, r.path)
		}
	}
	if posts != 1 {
		t.Errorf("%d SelfSubjectReviews, want 1", posts)
	}
	for _, kind := range []string{"modeldeployments", "allocations", "instances", "operations", "providers", "platformprofiles"} {
		if lists[kind] != 1 {
			t.Errorf("%d reads of %s, want 1 (%v)", lists[kind], kind, lists)
		}
	}

	// --owner A: A's deployment, its Allocation and Instance, A's waiting Operations, and not the person's deployment, which has
	// no owner, nor the old children, nor B's.
	log = &requestLog{}
	logged.WrapTransport = log.wrap
	onlyA := w.inventoryAs(t, logged, "--owner", invUserA)
	if got := invKeys(onlyA.rows(t, "modelDeployments")); !slices.Equal(got, []string{"inv-a"}) {
		t.Errorf("--owner A lists the deployments %v, want [inv-a]", got)
	}
	if got := invKeys(onlyA.rows(t, "allocations")); !slices.Equal(got, []string{"alloc-inv-a"}) {
		t.Errorf("--owner A lists the Allocations %v, want [alloc-inv-a]", got)
	}
	if got := invKeys(onlyA.rows(t, "instances")); !slices.Equal(got, []string{"inst-inv-a"}) {
		t.Errorf("--owner A lists the Instances %v, want [inst-inv-a]", got)
	}
	if got, want := invKeys(onlyA.rows(t, "operationsAwaitingApproval")), w.waitingOperations(t, invUserA); !slices.Equal(got, want) || !slices.Contains(got, "row1") {
		t.Errorf("--owner A lists the Operations %v, want A's that wait: %v", got, want)
	}
	if onlyA["selectedOwner"] != invUserA {
		t.Errorf("selectedOwner %v", onlyA["selectedOwner"])
	}
	// The selection of Allocations and Instances was made by the API server, with the label selector on the request.
	selector := "hpcburst.groundnuty.github.io/owner-hash=" + invHashA
	selected := map[string]bool{}
	for _, r := range log.all() {
		if q, err := url.ParseQuery(r.query); err == nil && q.Get("labelSelector") == selector {
			selected[r.path[strings.LastIndex(r.path, "/")+1:]] = true
		}
	}
	if !selected["allocations"] || !selected["instances"] || len(selected) != 2 {
		t.Errorf("the label selector %q went with the reads of %v, want allocations and instances only", selector, invKeys(selected))
	}
	// Which Allocation hosts an Instance is read from every Instance in scope: a second read of the Instances, with no selector.
	reads := map[string]int{}
	for _, r := range log.all() {
		if r.method == http.MethodGet {
			reads[r.path[strings.LastIndex(r.path, "/")+1:]]++
		}
	}
	if reads["instances"] != 2 || reads["allocations"] != 1 || reads["modeldeployments"] != 1 {
		t.Errorf("--owner A read %v, want the Instances twice (selected, then all) and every other kind once", reads)
	}

	// --owner B, as agent B: its own, and none of A's.
	onlyB := w.inventoryAs(t, asB, "--owner", invUserB)
	if got := invKeys(onlyB.rows(t, "modelDeployments")); !slices.Equal(got, []string{"inv-b"}) {
		t.Errorf("--owner B lists the deployments %v, want [inv-b]", got)
	}
	if got := invKeys(onlyB.rows(t, "allocations")); !slices.Equal(got, []string{"alloc-inv-b"}) {
		t.Errorf("--owner B lists the Allocations %v, want [alloc-inv-b]", got)
	}
	if got := invKeys(onlyB.rows(t, "operationsAwaitingApproval")); len(got) != 0 {
		t.Errorf("--owner B lists the Operations %v, but B made none that wait", got)
	}
}
