package managedrecord_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider/managedrecord"
)

// ns is any namespace: the namespace's choice is the caller's.
const ns = "records-ns"

// vector is one of HBEP-3's committed golden vectors of a managed entry: its input, and the canonical object
// and hash committed beside it. The expectations come from these files, never from the writer.
type vector struct {
	entry     deploymentid.CatalogEntry
	canonical string
	sha256    string
}

func readVector(t *testing.T, dir string) vector {
	t.Helper()
	base := filepath.Join("..", "..", "deploymentid", "testdata", "vectors", dir)
	var in struct {
		Catalog struct {
			Platform    string `json:"platform"`
			CatalogName string `json:"catalog_name"`
			Revision    string `json:"revision"`
		} `json:"catalog"`
	}
	b, err := os.ReadFile(filepath.Join(base, "input.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &in); err != nil {
		t.Fatal(err)
	}
	canon, err := os.ReadFile(filepath.Join(base, "canonical.json"))
	if err != nil {
		t.Fatal(err)
	}
	sum, err := os.ReadFile(filepath.Join(base, "sha256.txt"))
	if err != nil {
		t.Fatal(err)
	}
	return vector{
		entry:     deploymentid.CatalogEntry{Platform: in.Catalog.Platform, CatalogName: in.Catalog.CatalogName, Revision: in.Catalog.Revision},
		canonical: strings.TrimSpace(string(canon)),
		sha256:    strings.Fields(string(sum))[0],
	}
}

func newClient(t *testing.T, objs ...client.Object) client.WithWatch {
	t.Helper()
	s := runtime.NewScheme()
	if err := appsv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return clientfake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
}

func record(t *testing.T, c client.Client, name string) appsv1.ControllerRevision {
	t.Helper()
	var rec appsv1.ControllerRevision
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: name}, &rec); err != nil {
		t.Fatalf("record %s/%s: %v", ns, name, err)
	}
	return rec
}

// An entry gives its record: id-<first 12 hex> of the vector's committed hash, holding the committed canonical
// object and the full hash, labelled with the display form and the platform, and with no owner reference.
func TestEnsureWritesTheGoldenRecord(t *testing.T) {
	for _, dir := range []string{"02-managed-with-revision", "03-managed-no-revision-unicode"} {
		t.Run(dir, func(t *testing.T) {
			v := readVector(t, dir)
			c := newClient(t)
			name, err := managedrecord.Ensure(context.Background(), c, c, ns, v.entry, "test")
			if err != nil {
				t.Fatal(err)
			}
			if name != "id-"+v.sha256[:12] {
				t.Errorf("record name %s, want id-%s", name, v.sha256[:12])
			}
			rec := record(t, c, name)
			var d struct {
				Scheme    string          `json:"scheme"`
				SHA256    string          `json:"sha256"`
				Canonical json.RawMessage `json:"canonical"`
			}
			if err := json.Unmarshal(rec.Data.Raw, &d); err != nil {
				t.Fatal(err)
			}
			if d.Scheme != "v1" || d.SHA256 != v.sha256 || string(d.Canonical) != v.canonical {
				t.Errorf("record data %s; want scheme v1, sha256 %s, canonical %s", rec.Data.Raw, v.sha256, v.canonical)
			}
			if rec.Labels[v1alpha1.LabelDeploymentID] != v.sha256[:12] || rec.Labels[v1alpha1.LabelPlatform] != v.entry.Platform ||
				rec.Labels[v1alpha1.LabelRecord] != v1alpha1.RecordIdentity || rec.Annotations[v1alpha1.AnnotationCatalogName] != v.entry.CatalogName {
				t.Errorf("labels %v, annotations %v", rec.Labels, rec.Annotations)
			}
			if len(rec.OwnerReferences) != 0 {
				t.Errorf("owner references %v: the record must outlive its entry and its Provider", rec.OwnerReferences)
			}
		})
	}
}

// An existing record is read and never rewritten: a second Ensure, as a second Provider's catalog loop on the
// same platform gives, leaves its resourceVersion as it was.
func TestEnsureNeverRewritesARecord(t *testing.T) {
	v := readVector(t, "02-managed-with-revision")
	c := newClient(t)
	name, err := managedrecord.Ensure(context.Background(), c, c, ns, v.entry, "first")
	if err != nil {
		t.Fatal(err)
	}
	before := record(t, c, name)
	if _, err := managedrecord.Ensure(context.Background(), c, c, ns, v.entry, "second"); err != nil {
		t.Fatal(err)
	}
	var list appsv1.ControllerRevisionList
	if err := c.List(context.Background(), &list, client.InNamespace(ns)); err != nil {
		t.Fatal(err)
	}
	if after := record(t, c, name); after.ResourceVersion != before.ResourceVersion || len(list.Items) != 1 {
		t.Errorf("after a second Ensure: resourceVersion %s (was %s), %d records; want it untouched and one record", after.ResourceVersion, before.ResourceVersion, len(list.Items))
	}
}

// A record of the same name that holds another ID is a collision: refused, and the record left untouched.
func TestEnsureRefusesACollision(t *testing.T) {
	v := readVector(t, "02-managed-with-revision")
	other := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "id-" + v.sha256[:12]},
		Data:       runtime.RawExtension{Raw: []byte(`{"scheme":"v1","sha256":"` + v.sha256[:12] + strings.Repeat("0", 52) + `","canonical":{}}`)},
		Revision:   1,
	}
	c := newClient(t, other)
	before := record(t, c, other.Name)
	_, err := managedrecord.Ensure(context.Background(), c, c, ns, v.entry, "test")
	var col *managedrecord.Collision
	if !errors.Is(err, managedrecord.ErrCollision) || !errors.As(err, &col) || col.Record != other.Name || col.Want != v.sha256 {
		t.Fatalf("Ensure = %v, want a collision with %s", err, other.Name)
	}
	if after := record(t, c, other.Name); after.ResourceVersion != before.ResourceVersion {
		t.Errorf("the colliding record was rewritten: resourceVersion %s -> %s", before.ResourceVersion, after.ResourceVersion)
	}
}

// A record another writer created between the read and the create is read and compared, not an error.
func TestEnsureReadsARecordCreatedMeanwhile(t *testing.T) {
	v := readVector(t, "02-managed-with-revision")
	base := newClient(t)
	if _, err := managedrecord.Ensure(context.Background(), base, base, ns, v.entry, "first"); err != nil {
		t.Fatal(err)
	}
	// The read sees no record (a stale reader), the create finds it exists.
	stale := newClient(t)
	c := interceptor.NewClient(base, interceptor.Funcs{})
	name, err := managedrecord.Ensure(context.Background(), c, &readerThatMissesOnce{Reader: base, miss: stale}, ns, v.entry, "second")
	if err != nil || name != "id-"+v.sha256[:12] {
		t.Errorf("Ensure = %q, %v; want the existing record read", name, err)
	}
}

// readerThatMissesOnce answers its first Get from miss (which has no record), then from Reader.
type readerThatMissesOnce struct {
	client.Reader
	miss   client.Reader
	missed bool
}

func (r *readerThatMissesOnce) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if !r.missed {
		r.missed = true
		return r.miss.Get(ctx, key, obj, opts...)
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

// A failed create is returned, and no record is left.
func TestEnsureReturnsAFailedCreate(t *testing.T) {
	v := readVector(t, "02-managed-with-revision")
	base := newClient(t)
	c := interceptor.NewClient(base, interceptor.Funcs{Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
		return apierrors.NewServiceUnavailable("the API server is away")
	}})
	if _, err := managedrecord.Ensure(context.Background(), c, base, ns, v.entry, "test"); err == nil || !apierrors.IsServiceUnavailable(errors.Unwrap(err)) {
		t.Errorf("Ensure = %v, want the create's error", err)
	}
	var list appsv1.ControllerRevisionList
	if err := base.List(context.Background(), &list, client.InNamespace(ns)); err != nil || len(list.Items) != 0 {
		t.Errorf("records after a failed create: %d (%v), want none", len(list.Items), err)
	}
}
