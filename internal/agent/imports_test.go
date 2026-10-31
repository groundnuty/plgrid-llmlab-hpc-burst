package agent

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The agent is a static binary for amd64 and arm64 that runs inside the job:
// this package and its non-test subpackages import only the standard library,
// package agentapi and each other, so no Kubernetes library is linked into it.
// It reads the sources, so it needs neither the network nor the go tool.
// Package enginedouble is a test double, imported only by tests, and is not
// checked here.
func TestAgentImportsOnlyTheStandardLibrary(t *testing.T) {
	const module = "github.com/groundnuty/plgrid-llmlab-hpc-burst/"
	allowed := func(path string) bool {
		first, _, _ := strings.Cut(path, "/")
		if !strings.Contains(first, ".") {
			return true // standard library
		}
		return path == module+"pkg/agentapi" || strings.HasPrefix(path, module+"internal/agent")
	}
	checked := 0
	err := filepath.WalkDir(".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && d.Name() == "enginedouble" {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		f, err := parser.ParseFile(token.NewFileSet(), p, src, parser.ImportsOnly)
		if err != nil {
			return err
		}
		checked++
		for _, imp := range f.Imports {
			path, _ := strconv.Unquote(imp.Path.Value)
			if !allowed(path) {
				t.Errorf("%s imports %s: the agent links only the standard library and package agentapi", p, path)
			}
			if strings.HasSuffix(path, "/enginedouble") {
				t.Errorf("%s imports the engine double outside a test", p)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if checked < 8 {
		t.Errorf("checked %d source files, expected the package's whole source", checked)
	}
}
