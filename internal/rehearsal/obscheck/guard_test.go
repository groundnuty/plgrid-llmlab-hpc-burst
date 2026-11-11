package obscheck

// No product code imports this package: it is test support, as internal/rehearsal is, and a product binary that
// linked it would carry the checks and the k8s types they use for nothing. The guard reads the import lists of
// every non-test Go file of the module (the spikes and third_party are modules of their own).

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const importPath = "github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/obscheck"

// importers returns the module's Go files that import the package, split into test files and others.
func importers(t *testing.T, root string) (tests, others []string) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "spikes", "third_party", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return nil // a file that does not parse is another test's finding
		}
		for _, imp := range f.Imports {
			if strings.Trim(imp.Path.Value, `"`) == importPath {
				if strings.HasSuffix(path, "_test.go") {
					tests = append(tests, path)
				} else {
					others = append(others, path)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return tests, others
}

func TestOnlyTestFilesImportIt(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("the module's root is not at %s: %v", root, err)
	}
	tests, others := importers(t, root)
	// The control: the guard sees the files that import it, so an empty list of others is not a blind walk.
	if len(tests) == 0 {
		t.Fatal("no test file imports the package: the guard read nothing it was made to see")
	}
	for _, f := range others {
		t.Errorf("%s is not a test file and imports internal/rehearsal/obscheck: a product binary would link the checks", f)
	}
}
