package launcher

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFiles(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for n, c := range files {
		if err := os.WriteFile(filepath.Join(dir, n), []byte(c), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestReadManifests(t *testing.T) {
	dir := writeFiles(t, map[string]string{
		"20-b.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: second\n---\n---\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: third\n",
		"10-a.yml":  "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: first\n",
		"30-c.json": `{"apiVersion":"apiextensions.k8s.io/v1","kind":"CustomResourceDefinition","metadata":{"name":"things.example.test"}}`,
		"README.md": "not a manifest, and not YAML either: [",
		"notes.txt": "ignored",
	})
	objs, err := readManifests(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, o := range objs {
		names = append(names, o.GetName())
	}
	// File name order, documents in order, empty documents skipped, other extensions ignored.
	if got := strings.Join(names, ","); got != "first,second,third,things.example.test" {
		t.Fatalf("got %s", got)
	}
	if isCRD(objs[0]) || !isCRD(objs[3]) {
		t.Error("CRD detection is wrong")
	}
}

func TestReadManifestsRejects(t *testing.T) {
	cases := map[string]map[string]string{
		"no kind":       {"a.yaml": "apiVersion: v1\nmetadata:\n  name: x\n"},
		"no name":       {"a.yaml": "apiVersion: v1\nkind: ConfigMap\n"},
		"bad yaml":      {"a.yaml": "apiVersion: [unterminated\n"},
		"empty dir":     {},
		"only ignored":  {"notes.txt": "x"},
		"good then bad": {"a.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: ok\n", "b.yaml": "kind: ConfigMap\n"},
	}
	for name, files := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := readManifests(writeFiles(t, files)); err == nil {
				t.Fatal("accepted")
			}
		})
	}
	if _, err := readManifests(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Error("a missing directory was accepted")
	}
	// The error names the file, so the operator knows which to fix.
	_, err := readManifests(writeFiles(t, map[string]string{"zz-broken.yaml": "kind: X\n"}))
	if err == nil || !strings.Contains(err.Error(), "zz-broken.yaml") {
		t.Errorf("error does not name the file: %v", err)
	}
}
