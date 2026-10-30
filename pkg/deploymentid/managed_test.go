package deploymentid

import (
	"bytes"
	"errors"
	"testing"
)

func TestComputeManaged(t *testing.T) {
	e := CatalogEntry{Platform: "llmlab", CatalogName: "example-27b", Revision: "2026-09"}
	id, err := ComputeManaged(e)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"catalog_name":"example-27b","kind":"managed","opaque":"true","platform":"llmlab","revision":"2026-09","scheme":"v1"}`
	if string(id.Canonical) != want {
		t.Errorf("got  %s\nwant %s", id.Canonical, want)
	}
	noRev, err := ComputeManaged(CatalogEntry{Platform: "llmlab", CatalogName: "example-27b"})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(noRev.Canonical, []byte("revision")) {
		t.Errorf("an empty revision must be omitted: %s", noRev.Canonical)
	}
	for name, other := range map[string]CatalogEntry{
		"platform": {"llmlab2", "example-27b", "2026-09"}, "catalog name": {"llmlab", "example-28b", "2026-09"},
		"revision": {"llmlab", "example-27b", "2026-10"}, "no revision": {"llmlab", "example-27b", ""},
	} {
		o, err := ComputeManaged(other)
		if err != nil {
			t.Fatal(err)
		}
		if o.SHA256 == id.SHA256 {
			t.Errorf("changing the %s did not change the ID", name)
		}
	}
	for _, bad := range []CatalogEntry{{"", "x", ""}, {"x", "", ""}, {"x\xff", "y", ""}} {
		if _, err := ComputeManaged(bad); !errors.Is(err, ErrInvalid) {
			t.Errorf("%+v: got %v, want ErrInvalid", bad, err)
		}
	}
}

// A catalog entry and a served deployment of the same checkpoint never get
// the same ID (HBEP-3, Test plan).
func TestManagedAndServedNeverCollide(t *testing.T) {
	spec, accel, files := baseSpec()
	served := mustCompute(t, spec, accel, files)
	managed, err := ComputeManaged(CatalogEntry{Platform: "p", CatalogName: spec.Checkpoint.Repository, Revision: spec.Checkpoint.Revision})
	if err != nil {
		t.Fatal(err)
	}
	if served.SHA256 == managed.SHA256 {
		t.Error("a served and a managed deployment share an ID")
	}
	if !bytes.Contains(served.Canonical, []byte(`"kind":"served"`)) || !bytes.Contains(managed.Canonical, []byte(`"kind":"managed"`)) {
		t.Error("kind is not in the canonical object")
	}
}
