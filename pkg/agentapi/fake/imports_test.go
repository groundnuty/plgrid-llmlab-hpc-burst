package fake_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The agent is a static binary: package fake, which it may link, imports only
// the standard library and package agentapi. This test reads the package's own
// source, so it needs neither the network nor the go tool.
func TestPackageImportsOnlyTheStandardLibraryAndAgentapi(t *testing.T) {
	const agentapi = "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	files, err := filepath.Glob("*.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("no source files found: %v", err)
	}
	checked := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), f, src, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		checked++
		for _, imp := range parsed.Imports {
			path, _ := strconv.Unquote(imp.Path.Value)
			first, _, _ := strings.Cut(path, "/")
			if path != agentapi && strings.Contains(first, ".") {
				t.Errorf("%s imports %s: only the standard library and %s are allowed", f, path, agentapi)
			}
		}
	}
	if checked < 5 {
		t.Errorf("checked %d source files, expected the package's whole source", checked)
	}
}
