package managed

import (
	"context"
	"strings"
	"testing"
)

// A managed Provider that sets stagingDir is refused (HBEP-4, Provider settings; #757): a managed platform stages no files,
// so the field would be ignored, and a field that looks as if it did something is refused instead. The refusal names the
// field and never its value, which, like the account, is account data. Like a batch profile, it is refused before anything
// is read, sent or written. The control is the same Provider without the field, which a pass reads.
func TestAManagedProviderThatSetsAStagingDirIsRefused(t *testing.T) {
	const dir = "/invented-stage/zq7f3akx/home/someone"
	rt := newCountingTransport()
	e := newEnv(t, withModels(model("example-a")), withTransport(rt))

	prov := e.provider()
	prov.Spec.StagingDir = dir
	if err := e.c.Update(context.Background(), prov); err != nil {
		t.Fatal(err)
	}
	before := e.provider()
	_, err := e.trySyncAt(at(12, 0, 0))
	if err == nil || !strings.Contains(err.Error(), "spec.stagingDir") || !strings.Contains(err.Error(), "managed") {
		t.Fatalf("Sync = %v, want an error that names spec.stagingDir and says the Provider is managed", err)
	}
	if strings.Contains(err.Error(), "zq7f3akx") || strings.Contains(err.Error(), dir) {
		t.Errorf("the refusal echoes the value: %v", err)
	}
	if rt.count() != 0 || e.provider().ResourceVersion != before.ResourceVersion {
		t.Fatalf("%d requests sent, written %v: want none", rt.count(), e.provider().ResourceVersion != before.ResourceVersion)
	}

	// The same Provider with the field removed is read.
	cur := e.provider()
	cur.Spec.StagingDir = ""
	if err := e.c.Update(context.Background(), cur); err != nil {
		t.Fatal(err)
	}
	if _, err := e.trySyncAt(at(12, 1, 0)); err != nil {
		t.Fatalf("Sync without the field = %v, want the pass to run", err)
	}
	if rt.count() == 0 || e.gets() == 0 {
		t.Errorf("no read after the field was removed (%d requests, %d reads): the refusal was not about the field", rt.count(), e.gets())
	}
}
