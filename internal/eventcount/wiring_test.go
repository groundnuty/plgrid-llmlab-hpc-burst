package eventcount

// Every recorder of the module's programs is built with the durable log's option (HBEP-8, The durable
// log, Who writes: "a publisher in the wrapper where each events.k8s.io recorder is built, beside the
// raised-events counter, so no emit site raises a typed event that skips the log"). The drift guard
// (internal/reasondrift) checks that every recorder is wrapped to be counted; this checks that every
// wrapper takes the log option, which is a no-op in a process with no log, so a site added without it
// publishes nothing and no test of delivery would notice. It reads the source at run time, so a mutant
// of it must be in place while the test runs.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the test's directory")
		}
		dir = parent
	}
}

func TestEveryEventsWrapperTakesTheLogOption(t *testing.T) {
	root := moduleRoot(t)
	var sites, bare []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		if d.IsDir() {
			switch {
			case rel == ".git", rel == "spikes", rel == "third_party", d.Name() == "testdata", rel == filepath.Join("internal", "eventcount"):
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Events" {
				return true
			}
			if id, ok := sel.X.(*ast.Ident); !ok || id.Name != "eventcount" {
				return true
			}
			site := rel + ":" + strconv.Itoa(fset.Position(call.Pos()).Line)
			sites = append(sites, site)
			if len(call.Args) < 4 {
				bare = append(bare, site)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// The lifecycle manager, the tunnel-state reader, the batch Provider's typed recorder, the managed
	// catalog's and the front's: five sites. Fewer would mean the walk found nothing.
	if len(sites) < 5 {
		t.Fatalf("found %d eventcount.Events call sites %v, want at least the five of the controller and the front", len(sites), sites)
	}
	for _, b := range bare {
		t.Errorf("%s builds a recorder with eventcount.Events and no log option: it would never publish to the durable log", b)
	}
}
