package managed

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/managed/llmlabtest"
)

// The catalog loop against the gateway double: the stand-in's scenarios
// (pkg/provider/fake's managed controller tests) ported to the real
// controller, plus HBEP-3's golden vectors.

func TestCatalogFollowsTheListing(t *testing.T) {
	e := newEnv(t, withModels(model("example-a"), model("example-b")))
	e.syncAt(at(12, 0, 0))
	prov := e.provider()
	if got := e.names(); len(got) != 2 || got[0] != "example-a" || got[1] != "example-b" {
		t.Fatalf("catalog = %v, want [example-a example-b]", got)
	}
	wantEntry(t, e.entry("example-a"), true, "", at(12, 0, 0))
	wantEntry(t, e.entry("example-b"), true, "", at(12, 0, 0))
	wantMT(t, "lastCatalogRead", prov.Status.LastCatalogRead, at(12, 0, 0))
	wantMT(t, "lastReconcileTime", prov.Status.LastReconcileTime, at(12, 0, 0))
	if prov.Status.ObservedGeneration != 1 {
		t.Fatalf("observedGeneration = %d, want 1", prov.Status.ObservedGeneration)
	}
	e.wantCondition(v1alpha1.ConditionProviderReady, metav1.ConditionTrue, "CatalogRead")
	e.wantCondition(v1alpha1.ConditionCredentialsRejected, metav1.ConditionFalse, "KeyAccepted")
	e.wantCondition(v1alpha1.ConditionCredentialsInvalid, metav1.ConditionFalse, "FileRead")
	// The file has no expiry: False, NotExpiring (science-agent ruling, 31
	// Oct 2026).
	e.wantCondition(v1alpha1.ConditionCredentialsExpiring, metav1.ConditionFalse, "NotExpiring")
	wantMT(t, "lastSuccessfulStatusRead", prov.Status.LastSuccessfulStatusRead, at(12, 0, 0))
	aID := e.entry("example-a").ManagedID

	// A removal and an add: example-a leaves the listing and example-c
	// appears. a is kept, unlisted, with its revision, ID and lastListedTime
	// as they were.
	e.gw.RemoveModel("example-a")
	e.gw.AddModel(model("example-c"))
	e.syncAt(at(12, 1, 0))
	if got := e.names(); len(got) != 3 || got[0] != "example-a" || got[1] != "example-b" || got[2] != "example-c" {
		t.Fatalf("catalog = %v, want [example-a example-b example-c]: an entry that left is kept", got)
	}
	wantEntry(t, e.entry("example-a"), false, "", at(12, 0, 0))
	wantEntry(t, e.entry("example-b"), true, "", at(12, 1, 0))
	wantEntry(t, e.entry("example-c"), true, "", at(12, 1, 0))
	if e.entry("example-a").ManagedID != aID {
		t.Fatal("an entry that left the listing changed its managed ID")
	}
	wantMT(t, "lastCatalogRead", e.provider().Status.LastCatalogRead, at(12, 1, 0))

	// A return with a new revision: listed again, and a new managed ID.
	e.gw.AddModel(modelRev("example-a", "r2"))
	e.syncAt(at(12, 2, 0))
	wantEntry(t, e.entry("example-a"), true, "r2", at(12, 2, 0))
	if e.entry("example-a").ManagedID == aID {
		t.Fatal("example-a kept its managed ID although its revision changed")
	}
	r2ID := e.entry("example-a").ManagedID

	// An empty listing unlists everything and removes nothing.
	e.gw.SetModels()
	e.syncAt(at(12, 3, 0))
	if got := e.names(); len(got) != 3 {
		t.Fatalf("catalog = %v, want the three entries kept", got)
	}
	wantEntry(t, e.entry("example-a"), false, "r2", at(12, 2, 0))
	wantEntry(t, e.entry("example-b"), false, "", at(12, 2, 0))
	wantEntry(t, e.entry("example-c"), false, "", at(12, 2, 0))
	wantMT(t, "lastCatalogRead", e.provider().Status.LastCatalogRead, at(12, 3, 0))
	e.wantCondition(v1alpha1.ConditionProviderReady, metav1.ConditionTrue, "CatalogRead")

	// A return with the revision it had before: the same managed ID.
	e.gw.AddModel(modelRev("example-a", "r2"))
	e.syncAt(at(12, 4, 0))
	wantEntry(t, e.entry("example-a"), true, "r2", at(12, 4, 0))
	if e.entry("example-a").ManagedID != r2ID {
		t.Fatal("a return with the same revision changed the managed ID")
	}
	wantEntry(t, e.entry("example-b"), false, "", at(12, 2, 0))

	if n := e.gets(); n != 5 {
		t.Fatalf("the gateway logged %d reads, want 5: one per pass", n)
	}
}

func TestCatalogIsSortedByNameWhateverTheListingOrder(t *testing.T) {
	e := newEnv(t)
	var desc, shuffled []llmlabtest.Model
	for i := 11; i >= 0; i-- { // the gateway lists in descending order
		desc = append(desc, model(fmt.Sprintf("example-%02d", i)))
	}
	e.gw.SetModels(desc...)
	e.syncAt(at(12, 0, 0))
	for _, i := range []int{20, 3, 15, 7, 24, 1, 18} { // then new names in no order
		shuffled = append(shuffled, model(fmt.Sprintf("example-%02d", i)))
	}
	e.gw.SetModels(shuffled...)
	e.syncAt(at(12, 1, 0))

	got := e.names()
	if len(got) != 12+4 { // 03, 07 and 01 were listed before; 20, 15, 24 and 18 are new
		t.Fatalf("%d entries, want 16: %v", len(got), got)
	}
	for i := 1; i < len(got); i++ {
		if got[i-1] >= got[i] {
			t.Fatalf("catalog is not sorted by name: %v", got)
		}
	}
}

// The expected IDs are the committed vectors' hashes, read from their files at
// test time and hard-coded here as well: neither deploymentid.ComputeManaged
// nor any helper of the controller computes an expectation.
const (
	goldenWithRevision      = "dd88e3413dc6415f5b16a590b91d3d0e0508de5ab9c630055d535f9be9eba9d3"
	goldenUnicodeNoRevision = "8a2051bec23d0e78256b2a36f068add7f1513c353507cd8fcf2ca9b8a3d11d18"
)

func vectorHash(t *testing.T, dir string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "pkg", "deploymentid", "testdata", "vectors", dir, "sha256.txt"))
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(b))
	if len(fields) == 0 {
		t.Fatalf("vector %s: empty sha256.txt", dir)
	}
	return fields[0] // "<hex>  canonical.json"
}

func TestManagedIDsAreHBEP3GoldenVectors(t *testing.T) {
	const unicodeName = "example-\u00fc-\U0001F600-27b" // escapes: no editor can normalize it
	e := newEnv(t, withModels(modelRev("example-27b", "2026-09"), model(unicodeName)))
	e.syncAt(at(12, 0, 0))

	for _, tc := range []struct {
		name, vector, golden string
		revision             string
	}{
		{"example-27b", "02-managed-with-revision", goldenWithRevision, "2026-09"},
		{unicodeName, "03-managed-no-revision-unicode", goldenUnicodeNoRevision, ""},
	} {
		if file := vectorHash(t, tc.vector); file != tc.golden {
			t.Fatalf("vector %s: sha256.txt says %s, this test hard-codes %s", tc.vector, file, tc.golden)
		}
		got := e.entry(tc.name)
		if string(got.ManagedID) != tc.golden {
			t.Fatalf("entry %q: managedID = %s, want %s (vector %s)", tc.name, got.ManagedID, tc.golden, tc.vector)
		}
		if tc.revision == "" && got.Revision != nil || tc.revision != "" && (got.Revision == nil || *got.Revision != tc.revision) {
			t.Fatalf("entry %q: revision = %v, want %q", tc.name, got.Revision, tc.revision)
		}
	}
	// The by-hand canonical form of wantManagedID reproduces the ASCII
	// vector, so its use in the other tests rests on the committed file too.
	if got := wantManagedID("llmlab", "example-27b", "2026-09"); got != goldenWithRevision {
		t.Fatalf("wantManagedID = %s, want the vector's %s", got, goldenWithRevision)
	}
}

func TestManagedIDFollowsThePlatformLabel(t *testing.T) {
	e := newEnv(t, withModels(modelRev("example-27b", "2026-09"), model("example-9b")))
	e.syncAt(at(12, 0, 0))
	first := e.entry("example-27b").ManagedID

	p := e.provider()
	p.Labels[v1alpha1.LabelPlatform] = "example-other-platform"
	if err := e.c.Update(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	e.syncAt(at(12, 1, 0))
	got := e.entry("example-27b").ManagedID
	if got == first || string(got) != wantManagedID("example-other-platform", "example-27b", "2026-09") {
		t.Fatalf("managedID = %s after the platform changed, want the sha256 over example-other-platform and not %s", got, first)
	}
	if want := wantManagedID("example-other-platform", "example-9b", ""); string(e.entry("example-9b").ManagedID) != want {
		t.Fatalf("example-9b managedID = %s, want %s", e.entry("example-9b").ManagedID, want)
	}
}

func TestProviderWithoutAPlatformLabelIsAnError(t *testing.T) {
	t.Run("from the start", func(t *testing.T) {
		e := newEnv(t, withNoLabel(), withModels(model("example-a")))
		before := e.provider()
		_, err := e.trySyncAt(at(12, 0, 0))
		if err == nil || !strings.Contains(err.Error(), v1alpha1.LabelPlatform) {
			t.Fatalf("Sync = %v, want an error naming the %s label", err, v1alpha1.LabelPlatform)
		}
		if n := len(e.gw.Requests()); n != 0 {
			t.Fatalf("the gateway logged %d requests, want 0: no read without a platform", n)
		}
		if after := e.provider(); after.ResourceVersion != before.ResourceVersion {
			t.Fatal("the Provider was written without a platform")
		}
	})

	t.Run("the label goes after a read", func(t *testing.T) {
		e := newEnv(t, withModels(model("example-a")))
		e.syncAt(at(12, 0, 0))
		p := e.provider()
		delete(p.Labels, v1alpha1.LabelPlatform)
		if err := e.c.Update(context.Background(), p); err != nil {
			t.Fatal(err)
		}
		before := e.provider() // taken after the label update
		_, err := e.trySyncAt(at(12, 1, 0))
		if err == nil || !strings.Contains(err.Error(), v1alpha1.LabelPlatform) {
			t.Fatalf("Sync = %v, want an error naming the %s label", err, v1alpha1.LabelPlatform)
		}
		if n := e.gets(); n != 1 {
			t.Fatalf("the gateway logged %d reads, want 1: none for the pass without a label", n)
		}
		if after := e.provider(); after.ResourceVersion != before.ResourceVersion {
			t.Fatal("the Provider was written without a platform")
		}
	})
}

// The bounded catalog (@hpc-burst-science-agent's ruling of 31 Oct 2026): an
// entry the status cannot hold is skipped with an event naming it, the rest
// is recorded and the read counts; room is made by pruning unlisted entries,
// oldest lastListedTime first; a listing above 1024 keeps the first 1024 by
// name and raises CatalogTruncated on every read. Ready stays True.
func TestBoundedCatalog(t *testing.T) {
	drain := func(rec *events.FakeRecorder) []string {
		var out []string
		for {
			select {
			case ev := <-rec.Events:
				out = append(out, ev)
			default:
				return out
			}
		}
	}
	for _, tc := range []struct {
		name   string
		models []llmlabtest.Model
		note   string
	}{
		{"an entry with no name", []llmlabtest.Model{model("")}, `catalog entry "" skipped: an empty name`},
		{"a name of 254 characters", []llmlabtest.Model{model(strings.Repeat("n", 254))}, `skipped: a name of 254 characters, above 253`},
		{"a name of 254 multi-byte characters", []llmlabtest.Model{model(strings.Repeat("\u00e9", 254))}, `skipped: a name of 254 characters, above 253`},
		{"a revision of 257 characters", []llmlabtest.Model{modelRev("example-a", strings.Repeat("r", 257))}, `catalog entry "example-a" skipped: a revision of 257 characters, above 256`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := eventsRecorder()
			e := newEnv(t, withModels(model("example-ok")), withRecorder(rec))
			e.syncAt(at(12, 0, 0))
			e.gw.SetModels(append([]llmlabtest.Model{model("example-fine")}, tc.models...)...)
			e.syncAt(at(12, 1, 0))
			if got := e.names(); len(got) != 2 || got[0] != "example-fine" || got[1] != "example-ok" {
				t.Fatalf("catalog = %v, want example-fine listed and example-ok kept", got)
			}
			wantEntry(t, e.entry("example-fine"), true, "", at(12, 1, 0))
			wantMT(t, "lastCatalogRead", e.provider().Status.LastCatalogRead, at(12, 1, 0))
			e.wantCondition(v1alpha1.ConditionProviderReady, metav1.ConditionTrue, "CatalogRead")
			evs := drain(rec)
			if len(evs) != 1 || !strings.HasPrefix(evs[0], "Warning CatalogEntrySkipped ") || !strings.Contains(evs[0], tc.note) {
				t.Fatalf("events = %q, want one CatalogEntrySkipped holding %q", evs, tc.note)
			}
		})
	}

	t.Run("the largest values fit", func(t *testing.T) {
		longName, wideName := strings.Repeat("n", 253), strings.Repeat("\u00e9", 253) // 253 characters, 506 bytes
		longRev := strings.Repeat("r", 256)
		e := newEnv(t, withModels(model(longName), model(wideName), modelRev("example-a", longRev)))
		e.syncAt(at(12, 0, 0))
		if got := e.names(); len(got) != 3 {
			t.Fatalf("catalog = %d entries, want 3", len(got))
		}
		if got := e.entry("example-a").Revision; got == nil || *got != longRev {
			t.Fatalf("the 256-character revision was not kept: %v", got)
		}
		e.entry(wideName) // 253 characters count as 253, not as 506 bytes
	})

	t.Run("a 1025th name prunes the unlisted entry listed longest ago", func(t *testing.T) {
		var many []llmlabtest.Model
		for i := range 1024 {
			many = append(many, model(fmt.Sprintf("example-%04d", i)))
		}
		e := newEnv(t, withModels(many[:512]...))
		e.syncAt(at(12, 0, 0)) // example-0000 to 0511, listed at 12:00:00
		e.gw.SetModels(many...)
		e.syncAt(at(12, 1, 0)) // all 1024, listed at 12:01:00
		e.gw.SetModels(append(many[1:], model("example-extra"))...)
		e.syncAt(at(12, 2, 0)) // example-0000 leaves, a new name arrives: 1025 ever
		if n := len(e.provider().Status.Catalog); n != 1024 {
			t.Fatalf("%d entries, want 1024", n)
		}
		e.wantCondition(v1alpha1.ConditionProviderReady, metav1.ConditionTrue, "CatalogRead")
		wantEntry(t, e.entry("example-extra"), true, "", at(12, 2, 0))
		for _, n := range e.names() {
			if n == "example-0000" {
				t.Fatal("example-0000, the one unlisted entry, was kept although there was no room")
			}
		}
		// Now 1023 listed and room for one: example-0001 leaves and stays.
		e.gw.SetModels(append(many[2:], model("example-extra"))...)
		e.syncAt(at(12, 3, 0))
		wantEntry(t, e.entry("example-0001"), false, "", at(12, 2, 0))
	})

	t.Run("a listing above 1024 keeps the first 1024 by name", func(t *testing.T) {
		rec := eventsRecorder()
		var many []llmlabtest.Model
		for i := 1029; i >= 0; i-- { // 1030 names, listed in reverse order
			many = append(many, model(fmt.Sprintf("example-%04d", i)))
		}
		e := newEnv(t, withModels(many...), withRecorder(rec))
		for i, when := range []time.Time{at(12, 0, 0), at(12, 1, 0)} {
			e.syncAt(when)
			got := e.names()
			if len(got) != 1024 || got[0] != "example-0000" || got[1023] != "example-1023" {
				t.Fatalf("read %d: %d entries from %s to %s, want example-0000 to example-1023", i, len(got), got[0], got[len(got)-1])
			}
			e.wantCondition(v1alpha1.ConditionProviderReady, metav1.ConditionTrue, "CatalogRead")
			evs := drain(rec)
			if len(evs) != 1 || evs[0] != "Warning CatalogTruncated the gateway listed 6 entries more than status.catalog holds (1024); the first 1024 by name are kept" {
				t.Fatalf("read %d: events = %q, want one CatalogTruncated", i, evs)
			}
		}
	})
}

// eventsRecorder is a fake recorder with room for every event of a test.
func eventsRecorder() *events.FakeRecorder { return events.NewFakeRecorder(4096) }

// A conflict on the status update: another writer (the lifecycle controller's
// DialHomeUnreachable apply) got in between the read and the write. The pass
// re-reads the Provider and applies the same read again, keeping the other
// writer's condition, and does not read the gateway a second time.
func TestConflictOnTheStatusUpdateReusesTheListing(t *testing.T) {
	dialHome := metav1.Condition{
		Type: v1alpha1.ConditionDialHomeUnreachable, Status: metav1.ConditionTrue,
		Reason: "NoRegistration", Message: "example: the agent did not register",
		LastTransitionTime: metav1.NewTime(at(11, 59, 0)), ObservedGeneration: 1,
	}
	// Update 1 is the reservation before the read, update 2 the first write
	// of its result: the conflict hits either.
	for _, conflictAt := range []int32{1, 2} {
		t.Run(fmt.Sprintf("on status update %d", conflictAt), func(t *testing.T) {
			testConflictReusesTheListing(t, conflictAt, dialHome)
		})
	}
}

func testConflictReusesTheListing(t *testing.T, conflictAt int32, dialHome metav1.Condition) {
	var updates atomic.Int32
	e := newEnv(t, withModels(model("example-a")), withBuilder(withManagedFields), withBuilder(
		func(b *clientfake.ClientBuilder) *clientfake.ClientBuilder {
			return b.WithInterceptorFuncs(interceptor.Funcs{
				SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					if updates.Add(1) == conflictAt {
						applyDialHome(t, cl, testProvider, dialHome) // the other writer wins the race
						return apierrors.NewConflict(schema.GroupResource{Group: "example.invalid", Resource: "providers"}, obj.GetName(), errors.New("example conflict"))
					}
					return cl.SubResource(sub).Update(ctx, obj, opts...)
				},
			})
		}))

	e.syncAt(at(12, 0, 0))
	if n := updates.Load(); n != 3 {
		t.Fatalf("%d status updates, want 3: the reservation, the result, and the retry of the one that conflicted", n)
	}
	if n := e.gets(); n != 1 {
		t.Fatalf("the gateway logged %d reads, want exactly 1: the retry reuses the listing", n)
	}
	wantEntry(t, e.entry("example-a"), true, "", at(12, 0, 0))
	wantMT(t, "lastCatalogRead", e.provider().Status.LastCatalogRead, at(12, 0, 0))
	e.wantCondition(v1alpha1.ConditionProviderReady, metav1.ConditionTrue, "CatalogRead")
	if got := findDialHome(t, e.provider()); got.Status != metav1.ConditionTrue || got.Reason != "NoRegistration" {
		t.Fatalf("DialHomeUnreachable = %+v, want the other writer's condition kept through the retry", got)
	}
}

func TestKeepsAConditionItDoesNotOwn(t *testing.T) {
	e := newEnv(t, withModels(model("example-a")), withBuilder(withManagedFields))
	e.syncAt(at(12, 0, 0))
	set := metav1.Condition{
		Type: v1alpha1.ConditionDialHomeUnreachable, Status: metav1.ConditionTrue,
		Reason: "NoRegistration", Message: "example: the agent did not register",
		LastTransitionTime: metav1.NewTime(at(12, 0, 30)), ObservedGeneration: 1,
	}
	applyDialHome(t, e.c, testProvider, set)
	want := findDialHome(t, e.provider())

	for i, st := range []struct {
		when   time.Time
		reject int  // the gateway rejects the key with this status; 0: accepts
		fail   bool // the gateway answers 504 after 100 ms
	}{
		{at(12, 1, 0), 0, false},   // a success
		{at(12, 2, 0), 401, false}, // a rejected key writes the status
		{at(12, 3, 0), 0, true},    // an unknown error writes nothing
		{at(12, 4, 0), 0, false},   // a success again
	} {
		e.gw.RejectKey(testKey, st.reject)
		undo := func() {}
		if st.fail {
			undo = failReads(e.gw)
		}
		e.syncAt(st.when)
		undo()
		if got := findDialHome(t, e.provider()); !apiequality.Semantic.DeepEqual(got, want) {
			t.Fatalf("step %d: DialHomeUnreachable = %+v, want it unchanged: %+v", i, got, want)
		}
	}
	if n := e.gets(); n != 5 {
		t.Fatalf("the gateway logged %d reads, want 5 (the first pass and four steps)", n)
	}

	// Every manager on the stored Provider is one of the two writers, and the
	// controller is one of them.
	var sawProvider bool
	for _, mf := range e.provider().ManagedFields {
		switch mf.Manager {
		case v1alpha1.FieldManagerProvider:
			sawProvider = true
		case v1alpha1.FieldManagerLifecycle:
		default:
			t.Fatalf("a write by field manager %q on the Provider", mf.Manager)
		}
	}
	if !sawProvider {
		t.Fatalf("managed fields = %+v, want an entry for %s", e.provider().ManagedFields, v1alpha1.FieldManagerProvider)
	}
}
