package e2e

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// verifyGenerated runs hack/verify-generated.sh and returns its exit error and output.
func verifyGenerated(t *testing.T, args ...string) (error, string) {
	t.Helper()
	script, err := filepath.Abs(filepath.Join("..", "..", "..", "hack", "verify-generated.sh"))
	must(t, err, "script path")
	cmd := exec.Command("bash", append([]string{script}, args...)...)
	out, err := cmd.CombinedOutput()
	return err, string(out)
}

func copyFile(t *testing.T, from, to string) {
	t.Helper()
	b, err := os.ReadFile(from)
	must(t, err, "read "+from)
	must(t, os.WriteFile(to, b, 0o644), "write "+to)
}

// The committed CRDs and deepcopy file match a fresh generation, and the check
// notices when they do not. The edits are made to copies in a temporary
// directory; the committed files are never written.
func TestVerifyGeneratedCatchesDrift(t *testing.T) {
	crdDir := configDir("crd")
	deepcopy := filepath.Join("..", "zz_generated.deepcopy.go")

	if err, out := verifyGenerated(t); err != nil {
		t.Fatalf("the committed files are out of date (%v):\n%s", err, out)
	}

	// An edited CRD fails the check.
	tmp := t.TempDir()
	editedDir := filepath.Join(tmp, "crd")
	must(t, os.MkdirAll(editedDir, 0o755), "mkdir")
	entries, err := os.ReadDir(crdDir)
	must(t, err, "read the CRD directory")
	for _, e := range entries {
		copyFile(t, filepath.Join(crdDir, e.Name()), filepath.Join(editedDir, e.Name()))
	}
	target := filepath.Join(editedDir, "hpcburst.groundnuty.github.io_providers.yaml")
	raw, err := os.ReadFile(target)
	must(t, err, "read")
	edited := strings.Replace(string(raw), "name: v1alpha1", "name: v1alpha2", 1)
	if edited == string(raw) {
		t.Fatal("the edit changed nothing; the test would pass for the wrong reason")
	}
	must(t, os.WriteFile(target, []byte(edited), 0o644), "write the edited CRD")
	err, out := verifyGenerated(t, editedDir)
	if err == nil {
		t.Fatalf("verify-generated.sh accepted an edited CRD:\n%s", out)
	}
	if !strings.Contains(out, "v1alpha2") || !strings.Contains(out, "differs from a fresh generation") {
		t.Fatalf("verify-generated.sh failed, but not on the edit:\n%s", out)
	}

	// An extra file in the CRD directory fails it too.
	must(t, os.WriteFile(filepath.Join(editedDir, "stray.yaml"), []byte("x: 1\n"), 0o644), "stray")
	copyFile(t, filepath.Join(crdDir, "hpcburst.groundnuty.github.io_providers.yaml"), target)
	if err, _ := verifyGenerated(t, editedDir); err == nil {
		t.Fatal("verify-generated.sh accepted a stray file")
	}

	// An edited deepcopy file fails it.
	dc, err := os.ReadFile(deepcopy)
	must(t, err, "read the deepcopy file")
	badDC := filepath.Join(tmp, "zz_generated.deepcopy.go")
	must(t, os.WriteFile(badDC, append(dc, []byte("\n// edited by hand\n")...), 0o644), "write")
	err, out = verifyGenerated(t, crdDir, badDC)
	if err == nil || !strings.Contains(out, "edited by hand") {
		t.Fatalf("verify-generated.sh did not catch an edited deepcopy file (err %v):\n%s", err, out)
	}
}
