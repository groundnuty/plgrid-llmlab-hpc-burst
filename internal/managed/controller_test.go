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
	e.wantNoCondition(v1alpha1.ConditionCredentialsExpiring) // the file has no expiry
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

func TestLimitsOfTheCatalogStatus(t *testing.T) {
	for _, tc := range []struct {
		name   string
		models []llmlabtest.Model
	}{
		{"an entry with no name", []llmlabtest.Model{model("")}},
		{"a name of 254 characters", []llmlabtest.Model{model(strings.Repeat("n", 254))}},
		{"a name of 254 multi-byte characters", []llmlabtest.Model{model(strings.Repeat("\u00e9", 254))}},
		{"a revision of 257 characters", []llmlabtest.Model{modelRev("example-a", strings.Repeat("r", 257))}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t, withModels(model("example-ok")))
			e.syncAt(at(12, 0, 0))
			before := e.provider()
			e.gw.SetModels(append([]llmlabtest.Model{model("example-fine")}, tc.models...)...)
			if _, err := e.trySyncAt(at(12, 1, 0)); err == nil {
				t.Fatal("Sync = nil, want an error")
			}
			if e.gets() != 2 {
				t.Fatalf("the gateway logged %d reads, want 2: the failing pass read", e.gets())
			}
			if after := e.provider(); after.ResourceVersion != before.ResourceVersion {
				t.Fatal("a listing the status cannot hold was partly written")
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

	t.Run("1024 names fit and the 1025th does not", func(t *testing.T) {
		var many []llmlabtest.Model
		for i := range 1024 {
			many = append(many, model(fmt.Sprintf("example-%04d", i)))
		}
		e := newEnv(t, withModels(many...))
		e.syncAt(at(12, 0, 0))
		if n := len(e.provider().Status.Catalog); n != 1024 {
			t.Fatalf("%d entries, want 1024", n)
		}
		before := e.provider()
		e.gw.SetModels(model("example-extra")) // 1025 names ever listed
		_, err := e.trySyncAt(at(12, 1, 0))
		if err == nil || !strings.Contains(err.Error(), "1024") {
			t.Fatalf("Sync = %v, want an error naming the limit of 1024", err)
		}
		if e.provider().ResourceVersion != before.ResourceVersion {
			t.Fatal("the over-long catalog was written")
		}
	})
}

// A conflict on the status update: another writer (the lifecycle controller's
// DialHomeUnreachable apply) got in between the read and the write. The pass
// re-reads the Provider and applies the same read again, keeping the other
// writer's condition, and does not read the gateway a second time.
func TestConflictOnTheStatusUpdateReusesTheListing(t *testing.T) {
	var updates atomic.Int32
	dialHome := metav1.Condition{
		Type: v1alpha1.ConditionDialHomeUnreachable, Status: metav1.ConditionTrue,
		Reason: "NoRegistration", Message: "example: the agent did not register",
		LastTransitionTime: metav1.NewTime(at(11, 59, 0)), ObservedGeneration: 1,
	}
	e := newEnv(t, withModels(model("example-a")), withBuilder(withManagedFields), withBuilder(
		func(b *clientfake.ClientBuilder) *clientfake.ClientBuilder {
			return b.WithInterceptorFuncs(interceptor.Funcs{
				SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					if updates.Add(1) == 1 {
						applyDialHome(t, cl, testProvider, dialHome) // the other writer wins the race
						return apierrors.NewConflict(schema.GroupResource{Group: "example.invalid", Resource: "providers"}, obj.GetName(), errors.New("example conflict"))
					}
					return cl.SubResource(sub).Update(ctx, obj, opts...)
				},
			})
		}))

	e.syncAt(at(12, 0, 0))
	if n := updates.Load(); n != 2 {
		t.Fatalf("%d status updates, want 2: the conflict and the retry", n)
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
