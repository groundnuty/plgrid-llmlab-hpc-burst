package managed

// The identity record of each managed catalog entry's ID (HBEP-9, the managed entry's identity record; HBEP-11,
// Identity records; issue #574): the catalog loop writes it in hpcburst-system before it publishes the entry,
// creates it once and never rewrites it, gives it no owner reference, and leaves an entry whose record holds
// another ID out of the catalog with a CatalogEntrySkipped. The expected IDs and canonical objects are HBEP-3's
// committed golden vectors, read from their files: nothing the writer computes is an expectation.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// vectorCanonical is a golden vector's committed canonical object.
func vectorCanonical(t *testing.T, dir string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "pkg", "deploymentid", "testdata", "vectors", dir, "canonical.json"))
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(b))
}

// identityRecord reads hpcburst-system/id-<first 12 hex of id>; ok is false when there is none.
func (e *env) identityRecord(id string) (appsv1.ControllerRevision, bool) {
	e.t.Helper()
	var rec appsv1.ControllerRevision
	err := e.c.Get(context.Background(), client.ObjectKey{Namespace: v1alpha1.ControllerNamespace, Name: "id-" + id[:12]}, &rec)
	if apierrors.IsNotFound(err) {
		return rec, false
	}
	if err != nil {
		e.t.Fatal(err)
	}
	return rec, true
}

// records lists every ControllerRevision the client holds.
func (e *env) records() []appsv1.ControllerRevision {
	e.t.Helper()
	var l appsv1.ControllerRevisionList
	if err := e.c.List(context.Background(), &l); err != nil {
		e.t.Fatal(err)
	}
	return l.Items
}

func TestTheCatalogLoopWritesEachEntrysIdentityRecord(t *testing.T) {
	const unicodeName = "example-ü-\U0001F600-27b"
	e := newEnv(t, withModels(modelRev("example-27b", "2026-09"), model(unicodeName)))
	e.syncAt(at(12, 0, 0))
	for _, tc := range []struct{ name, vector, golden string }{
		{"example-27b", "02-managed-with-revision", goldenWithRevision},
		{unicodeName, "03-managed-no-revision-unicode", goldenUnicodeNoRevision},
	} {
		if file := vectorHash(t, tc.vector); file != tc.golden {
			t.Fatalf("vector %s: sha256.txt says %s, this test hard-codes %s", tc.vector, file, tc.golden)
		}
		rec, ok := e.identityRecord(tc.golden)
		if !ok {
			t.Fatalf("entry %q: no identity record %s/id-%s", tc.name, v1alpha1.ControllerNamespace, tc.golden[:12])
		}
		var d struct {
			Scheme    string          `json:"scheme"`
			SHA256    string          `json:"sha256"`
			Canonical json.RawMessage `json:"canonical"`
		}
		if err := json.Unmarshal(rec.Data.Raw, &d); err != nil {
			t.Fatal(err)
		}
		if want := vectorCanonical(t, tc.vector); d.Scheme != "v1" || d.SHA256 != tc.golden || string(d.Canonical) != want {
			t.Errorf("entry %q: record data %s; want scheme v1, sha256 %s and the vector's canonical object %s", tc.name, rec.Data.Raw, tc.golden, want)
		}
		if rec.Labels[v1alpha1.LabelRecord] != v1alpha1.RecordIdentity || rec.Labels[v1alpha1.LabelDeploymentID] != tc.golden[:12] ||
			rec.Labels[v1alpha1.LabelPlatform] != testPlatform || rec.Annotations[v1alpha1.AnnotationCatalogName] != tc.name {
			t.Errorf("entry %q: labels %v, annotations %v", tc.name, rec.Labels, rec.Annotations)
		}
		if len(rec.OwnerReferences) != 0 {
			t.Errorf("entry %q: owner references %v: the record must outlive its entry and its Provider", tc.name, rec.OwnerReferences)
		}
	}
	if n := len(e.records()); n != 2 {
		t.Errorf("%d records, want the two entries' two", n)
	}
}

// The record outlives its entry and its Provider: the entry leaves the catalog, then the Provider is deleted,
// and the record stays, untouched. The fake client has no garbage collector, so the record's lack of an owner
// reference, which keeps it from a real one, is asserted above and here.
func TestAnIdentityRecordOutlivesItsEntryAndItsProvider(t *testing.T) {
	e := newEnv(t, withModels(modelRev("example-27b", "2026-09")))
	e.syncAt(at(12, 0, 0))
	before, ok := e.identityRecord(goldenWithRevision)
	if !ok {
		t.Fatal("no record after the first pass")
	}
	e.gw.SetModels(model("example-other"))
	e.storeStatus(func(s *v1alpha1.ProviderStatus) { s.Catalog = nil })
	e.syncAt(at(12, 1, 0))
	if got := e.names(); len(got) != 1 || got[0] != "example-other" {
		t.Fatalf("catalog %v, want example-27b gone", got)
	}
	if err := e.c.Delete(context.Background(), e.provider()); err != nil {
		t.Fatal(err)
	}
	after, ok := e.identityRecord(goldenWithRevision)
	if !ok || after.ResourceVersion != before.ResourceVersion || len(after.OwnerReferences) != 0 {
		t.Errorf("after the entry left and the Provider was deleted: record kept %v, resourceVersion %s (was %s), owners %v",
			ok, after.ResourceVersion, before.ResourceVersion, after.OwnerReferences)
	}
}

// Two Providers on one platform give one entry the same ID, so one record: the second Provider's pass reads it
// and leaves it as it was; and a record is never rewritten by a later pass of the first either.
func TestASecondProviderMakesNoSecondRecord(t *testing.T) {
	e := newEnv(t, withModels(modelRev("example-27b", "2026-09")))
	e.syncAt(at(12, 0, 0))
	before, _ := e.identityRecord(goldenWithRevision)
	p := e.provider()
	second := &v1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: testProvider + "-2", Generation: 1, Labels: map[string]string{v1alpha1.LabelPlatform: testPlatform}},
		Spec:       v1alpha1.ProviderSpec{PlatformProfileRef: p.Spec.PlatformProfileRef, CredentialsRef: p.Spec.CredentialsRef},
	}
	if err := e.c.Create(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	ctl2 := e.secondController(func(o *Options) { o.Provider = second.Name })
	e.clock.t = at(12, 1, 0)
	if _, err := ctl2.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.syncAt(at(12, 2, 0))
	var got v1alpha1.Provider
	if err := e.c.Get(context.Background(), client.ObjectKeyFromObject(second), &got); err != nil || len(got.Status.Catalog) != 1 {
		t.Fatalf("the second Provider's catalog: %v (%v), want its one entry", got.Status.Catalog, err)
	}
	after, _ := e.identityRecord(goldenWithRevision)
	if n := len(e.records()); n != 1 || after.ResourceVersion != before.ResourceVersion {
		t.Errorf("%d records, resourceVersion %s (was %s): want one, never rewritten", n, after.ResourceVersion, before.ResourceVersion)
	}
}

// A record of the entry's name that holds another ID is a collision: the entry is left out of the catalog, a
// CatalogEntrySkipped names the record, and the record is untouched (HBEP-3, the collision rule).
func TestACollidingEntryIsLeftOutOfTheCatalog(t *testing.T) {
	other := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Namespace: v1alpha1.ControllerNamespace, Name: "id-" + goldenWithRevision[:12]},
		Data:       runtime.RawExtension{Raw: []byte(`{"scheme":"v1","sha256":"` + goldenWithRevision[:12] + strings.Repeat("0", 52) + `","canonical":{}}`)},
		Revision:   1,
	}
	rec := events.NewFakeRecorder(16)
	e := newEnv(t, withModels(modelRev("example-27b", "2026-09"), model("example-b")), withRecorder(rec),
		withBuilder(func(b *clientfake.ClientBuilder) *clientfake.ClientBuilder { return b.WithObjects(other) }))
	before, _ := e.identityRecord(goldenWithRevision)
	e.syncAt(at(12, 0, 0))
	if got := e.names(); len(got) != 1 || got[0] != "example-b" {
		t.Errorf("catalog %v, want only example-b: example-27b's ID collides", got)
	}
	found := false
	for len(rec.Events) > 0 {
		ev := <-rec.Events
		if strings.Contains(ev, "CatalogEntrySkipped") && strings.Contains(ev, "example-27b") && strings.Contains(ev, other.Name) {
			found = true
		}
	}
	if !found {
		t.Error("no CatalogEntrySkipped naming example-27b and the record it collides with")
	}
	if after, _ := e.identityRecord(goldenWithRevision); after.ResourceVersion != before.ResourceVersion {
		t.Errorf("the colliding record was rewritten: resourceVersion %s -> %s", before.ResourceVersion, after.ResourceVersion)
	}
}

// The record comes first: when the Provider's status is written, every entry it publishes already has its
// record; and a record that cannot be written fails the pass, publishing nothing.
func TestTheRecordIsWrittenBeforeTheEntryIsPublished(t *testing.T) {
	var mu sync.Mutex
	var missing []string
	e := newEnv(t, withModels(modelRev("example-27b", "2026-09"), model("example-b")),
		withBuilder(func(b *clientfake.ClientBuilder) *clientfake.ClientBuilder {
			return b.WithInterceptorFuncs(interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if p, ok := obj.(*v1alpha1.Provider); ok {
					for _, en := range p.Status.Catalog {
						var r appsv1.ControllerRevision
						if err := c.Get(ctx, client.ObjectKey{Namespace: v1alpha1.ControllerNamespace, Name: "id-" + string(en.ManagedID)[:12]}, &r); err != nil {
							mu.Lock()
							missing = append(missing, en.Name)
							mu.Unlock()
						}
					}
				}
				return c.SubResource(sub).Update(ctx, obj, opts...)
			}})
		}))
	e.syncAt(at(12, 0, 0))
	if len(e.names()) != 2 {
		t.Fatalf("catalog %v, want both entries", e.names())
	}
	if len(missing) != 0 {
		t.Errorf("the status published %v before their identity records existed", missing)
	}
}

func TestAFailedRecordWriteFailsThePass(t *testing.T) {
	e := newEnv(t, withModels(modelRev("example-27b", "2026-09")),
		withBuilder(func(b *clientfake.ClientBuilder) *clientfake.ClientBuilder {
			return b.WithInterceptorFuncs(interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if _, ok := obj.(*appsv1.ControllerRevision); ok {
					return apierrors.NewServiceUnavailable("the API server is away")
				}
				return c.Create(ctx, obj, opts...)
			}})
		}))
	if _, err := e.trySyncAt(at(12, 0, 0)); err == nil || !strings.Contains(err.Error(), "identity record") {
		t.Fatalf("Sync = %v, want the record's failure", err)
	}
	if got := e.names(); len(got) != 0 {
		t.Errorf("catalog %v published without its records", got)
	}
}
