package e2e

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
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

var camelCase = regexp.MustCompile(`^[a-z][A-Za-z0-9]*$`)

// Every field of every CRD is camelCase, as the Kubernetes API conventions ask
// (owner decision, PR #48). HBEP-3's hash object keeps snake_case keys; the
// deployment-ID library maps the API's fields onto them. The scan reads the
// committed manifests, so it covers each field the schema declares. Free-form
// blocks (x-kubernetes-preserve-unknown-fields) declare none.
func TestEveryCRDFieldIsCamelCase(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(configDir("crd"), "*.yaml"))
	must(t, err, "glob")
	if len(files) != 5 {
		t.Fatalf("want 5 CRD manifests, found %d", len(files))
	}
	var walk func(path string, s *apiextensionsv1.JSONSchemaProps, n *int)
	walk = func(path string, s *apiextensionsv1.JSONSchemaProps, n *int) {
		for name, p := range s.Properties {
			*n++
			if !camelCase.MatchString(name) {
				t.Errorf("%s.%s is not camelCase", path, name)
			}
			walk(path+"."+name, &p, n)
		}
		if s.Items != nil && s.Items.Schema != nil {
			walk(path+"[]", s.Items.Schema, n)
		}
	}
	total := 0
	for _, f := range files {
		crd := &apiextensionsv1.CustomResourceDefinition{}
		must(t, utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(readAll(t, f)), 4096).Decode(crd), "decode "+f)
		for _, v := range crd.Spec.Versions {
			walk(crd.Spec.Names.Kind, v.Schema.OpenAPIV3Schema, &total)
		}
	}
	if total < 150 {
		t.Fatalf("the scan saw only %d fields; it is not walking the schemas", total)
	}
}
