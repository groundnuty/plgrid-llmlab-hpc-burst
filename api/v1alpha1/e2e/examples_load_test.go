package e2e

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// An example is one object of one file of examples/.
type example struct {
	file string // the file's name
	doc  int    // the object's index in the file, from 0
	obj  map[string]any
}

func (e example) kind() string { s, _ := e.obj["kind"].(string); return s }

func (e example) name() string {
	n, _, _ := unstructured.NestedString(e.obj, "metadata", "name")
	return n
}

func (e example) label() string { return fmt.Sprintf("%s#%d %s %s", e.file, e.doc, e.kind(), e.name()) }

// hasContent says whether a YAML document holds anything besides blank lines, comments and
// "---" separators. Only such a document may be skipped: it is a comment-only part of a file
// (a header, or a trailing separator), not an object. The decision is made on the document's
// text, because the parsed value cannot tell a comment from "{}" or "null": both parse to
// nothing.
func hasContent(doc []byte) bool {
	for _, line := range strings.Split(string(doc), "\n") {
		if l := strings.TrimSpace(line); l != "" && !strings.HasPrefix(l, "#") && l != "---" {
			return true
		}
	}
	return false
}

// parseManifests splits a YAML stream into its objects. A document that holds only comments
// is skipped. Any other document must be a mapping with at least one key, so "{}", "null", "~",
// a list and a scalar are errors: a manifest that lost its content must not pass as one that
// was never there (PR #170 review, B1).
func parseManifests(raw []byte) ([]map[string]any, error) {
	rd := utilyaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(raw)))
	var out []map[string]any
	for i := 0; ; i++ {
		doc, err := rd.Read()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, fmt.Errorf("read document %d: %w", i, err)
		}
		if !hasContent(doc) {
			continue
		}
		obj := map[string]any{}
		if err := yaml.UnmarshalStrict(doc, &obj); err != nil {
			return nil, fmt.Errorf("parse document %d: %w", i, err)
		}
		if len(obj) == 0 {
			return nil, fmt.Errorf("document %d is an empty object (%q): a manifest has at least apiVersion and kind", i, strings.TrimSpace(string(doc)))
		}
		out = append(out, obj)
	}
}

// readManifestFile reads the objects of one file and fails the test on a file it cannot parse.
func readManifestFile(t *testing.T, path string) []map[string]any {
	t.Helper()
	raw, err := os.ReadFile(path)
	must(t, err, "read "+path)
	objs, err := parseManifests(raw)
	must(t, err, path)
	return objs
}

// loadExamplesFrom reads every entry of a directory of examples. It refuses an entry that is not
// a .yaml manifest, a file with no object in it (empty, or comments only), a document that is an
// empty object, an object of another API group, and a directory with no object at all, so a wrong
// path, a stray file or a hollowed-out manifest cannot leave the checks with nothing to check.
func loadExamplesFrom(dir string) ([]example, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", dir, err)
	}
	var out []example
	for _, ent := range entries {
		if ent.IsDir() || filepath.Ext(ent.Name()) != ".yaml" {
			return nil, fmt.Errorf("examples/%s is not a .yaml manifest: examples/ holds only manifests that are applied as they are", ent.Name())
		}
		raw, err := os.ReadFile(filepath.Join(dir, ent.Name()))
		if err != nil {
			return nil, err
		}
		objs, err := parseManifests(raw)
		if err != nil {
			return nil, fmt.Errorf("examples/%s: %w", ent.Name(), err)
		}
		if len(objs) == 0 {
			return nil, fmt.Errorf("examples/%s holds no object: an example file with nothing to apply is a mistake, not a skip", ent.Name())
		}
		for i, obj := range objs {
			e := example{file: ent.Name(), doc: i, obj: obj}
			if e.obj["apiVersion"] != v1alpha1.GroupVersion.String() {
				return nil, fmt.Errorf("%s: apiVersion is %v; want %s", e.label(), e.obj["apiVersion"], v1alpha1.GroupVersion)
			}
			out = append(out, e)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%s holds no object: the checks would pass on nothing", dir)
	}
	return out, nil
}

// loadExamples reads examples/ and fails the test on any refusal of loadExamplesFrom.
func loadExamples(t *testing.T) []example {
	t.Helper()
	out, err := loadExamplesFrom(examplesDir())
	must(t, err, "load examples/")
	return out
}

// Every file of examples/ is visited, and each yields an object. The list of files comes from a
// glob of the directory, not from the loader, so a loader that stopped early or skipped a file
// would be seen.
func TestExamplesLoaderVisitsEveryFile(t *testing.T) {
	want, err := filepath.Glob(filepath.Join(examplesDir(), "*"))
	must(t, err, "list examples/")
	if len(want) == 0 {
		t.Fatalf("examples/ lists no file")
	}
	seen := map[string]bool{}
	for _, e := range loadExamples(t) {
		seen[e.file] = true
	}
	for _, p := range want {
		if !seen[filepath.Base(p)] {
			t.Errorf("examples/%s yielded no object", filepath.Base(p))
		}
	}
	if len(seen) != len(want) {
		t.Errorf("the loader saw %d files, examples/ holds %d", len(seen), len(want))
	}
}

// What the loader skips is only a document of comments; everything else that would hold no
// object is refused. Each case writes a directory of one good example and one more file or
// document, the way an edit that hollowed out a manifest would leave it (PR #170 review, B1).
func TestExamplesLoaderRefusesWhatItWouldSkip(t *testing.T) {
	goodRaw, err := os.ReadFile(filepath.Join(examplesDir(), "provider-helios-gh200.yaml"))
	must(t, err, "read the example the cases build on")
	good := string(goodRaw)
	other := string(mustRead(t, filepath.Join(examplesDir(), "modeldeployment-llmlab.yaml")))
	cases := []struct {
		name    string
		files   map[string]string
		want    string // a substring of the refusal; empty means accepted
		objects int    // when accepted, the number of objects
	}{
		{"an empty object after a separator", map[string]string{"a.yaml": good + "---\n{}\n"}, "empty object", 0},
		{"an empty object before the manifest", map[string]string{"a.yaml": "---\n{}\n---\n" + good}, "empty object", 0},
		{"a null document", map[string]string{"a.yaml": good + "---\nnull\n"}, "empty object", 0},
		{"a tilde document", map[string]string{"a.yaml": good + "---\n~\n"}, "empty object", 0},
		{"an empty file beside a manifest", map[string]string{"a.yaml": good, "b.yaml": ""}, "holds no object", 0},
		{"a file of comments beside a manifest", map[string]string{"a.yaml": good, "b.yaml": "# nothing here\n"}, "holds no object", 0},
		{"a file of separators beside a manifest", map[string]string{"a.yaml": good, "b.yaml": "---\n---\n"}, "holds no object", 0},
		{"a list document", map[string]string{"a.yaml": good + "---\n- a\n- b\n"}, "parse document", 0},
		{"a file that is not yaml by name", map[string]string{"a.yaml": good, "b.yml": other}, "not a .yaml manifest", 0},
		{"a directory", map[string]string{"a.yaml": good, "sub/b.yaml": other}, "not a .yaml manifest", 0},
		{"only an empty file", map[string]string{"a.yaml": ""}, "holds no object", 0},
		// The controls: what the loader may skip, so that the refusals above are not a loader that
		// refuses everything.
		{"a trailing separator", map[string]string{"a.yaml": good + "---\n"}, "", 1},
		{"a leading separator and a header comment", map[string]string{"a.yaml": "# header\n---\n" + good}, "", 1},
		{"a comment-only document between two objects", map[string]string{"a.yaml": good + "---\n# only a comment\n---\n" + other}, "", 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, content := range tc.files {
				p := filepath.Join(dir, name)
				must(t, os.MkdirAll(filepath.Dir(p), 0o755), "mkdir")
				must(t, os.WriteFile(p, []byte(content), 0o644), "write "+name)
			}
			got, err := loadExamplesFrom(dir)
			if tc.want == "" {
				must(t, err, "the loader must accept this directory")
				if len(got) != tc.objects {
					t.Fatalf("loaded %d objects, want %d", len(got), tc.objects)
				}
				return
			}
			if err == nil {
				t.Fatalf("the loader accepted it and returned %d objects (%s)", len(got), exampleFiles(got))
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("the refusal does not mention %q: %v", tc.want, err)
			}
		})
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	must(t, err, "read "+path)
	return b
}

func exampleFiles(exs []example) string {
	set := map[string]bool{}
	for _, e := range exs {
		set[e.file] = true
	}
	return sortedKeys(set)
}
