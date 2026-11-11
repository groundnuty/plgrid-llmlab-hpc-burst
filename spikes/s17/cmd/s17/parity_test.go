package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The listener's passthrough is a copy of internal/front/frppass.go (its type
// is unexported). This test keeps the copy honest: past its provenance header,
// the copy must equal the root module's file with only the package clause
// changed, so a change to the front fails the spike's tests until the copy is
// taken again, and no rerun measures a passthrough the front no longer has.
func TestFrpPassthroughIsTheFrontsOwn(t *testing.T) {
	root := filepath.Join("..", "..", "..", "..")
	orig, err := os.ReadFile(filepath.Join(root, "internal", "front", "frppass.go"))
	if err != nil {
		t.Fatalf("the front's passthrough, the copy's source: %v", err)
	}
	copied, err := os.ReadFile("frppass.go")
	if err != nil {
		t.Fatal(err)
	}
	const clause = "package front\n"
	if strings.Count(string(orig), clause) != 1 {
		t.Fatalf("internal/front/frppass.go has no single %q line", strings.TrimSpace(clause))
	}
	want := strings.Replace(string(orig), clause, "package main\n", 1)
	got := string(copied)
	i := strings.Index(got, "package main\n")
	if i < 0 {
		t.Fatal("the copy has no package clause")
	}
	header, body := got[:i], got[i:]
	if !strings.Contains(header, "internal/front/frppass.go") {
		t.Error("the copy's header does not name its source")
	}
	if body != want {
		t.Errorf("cmd/s17/frppass.go has diverged from internal/front/frppass.go: copy the file again (package main) and update its header's commit")
	}
}
