package lifecycle

// Every Allocation delete goes through deleteAllocation, which writes the release cause first (issue #609). This
// test type-checks the module's shipped code and lists every call that deletes an Allocation through a
// controller-runtime client (Delete with an *Allocation, DeleteAllOf with an *AllocationList), every such Delete or
// DeleteAllOf whose object's kind it cannot tell, and every delete through a dynamic client in a file that names the
// allocations resource. Only deleteAllocation may hold one; any other is a delete that bypasses the cause.

import (
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

const (
	allocationType     = "github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1.Allocation"
	allocationListType = "github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1.AllocationList"
)

// moduleRoot is the repository root: the nearest parent with go.mod.
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

// allocationDeletes returns every delete of an Allocation in the packages, as "file:function", and every client
// delete whose object's kind cannot be told, the same way, each with its reason.
func allocationDeletes(t *testing.T, pkgs []*packages.Package) []string {
	t.Helper()
	var out []string
	for _, pkg := range pkgs {
		for _, f := range pkg.Syntax {
			file := pkg.Fset.File(f.Pos()).Name()
			namesAllocations := false
			ast.Inspect(f, func(n ast.Node) bool {
				if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING && lit.Value == `"allocations"` {
					namesAllocations = true
				}
				return true
			})
			for _, decl := range f.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				where := filepath.Base(file) + ":" + fn.Name.Name
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok || (sel.Sel.Name != "Delete" && sel.Sel.Name != "DeleteAllOf") {
						return true
					}
					obj := pkg.TypesInfo.Uses[sel.Sel]
					fun, ok := obj.(*types.Func)
					if !ok || fun.Pkg() == nil {
						return true
					}
					switch path := fun.Pkg().Path(); {
					case path == "sigs.k8s.io/controller-runtime/pkg/client" && len(call.Args) >= 2:
						arg := types.TypeString(pkg.TypesInfo.TypeOf(call.Args[1]), nil)
						switch strings.TrimPrefix(arg, "*") {
						case allocationType, allocationListType:
							out = append(out, where+" (an Allocation)")
						default:
							if !strings.HasPrefix(arg, "*github.com/") {
								out = append(out, where+" (an object of type "+arg+", whose kind this test cannot tell)")
							}
						}
					case path == "k8s.io/client-go/dynamic" && namesAllocations:
						out = append(out, where+" (a dynamic delete in a file that names the allocations resource)")
					}
					return true
				})
			}
		}
	}
	return out
}

func TestEveryAllocationDeleteWritesItsCause(t *testing.T) {
	cfg := &packages.Config{
		Mode:  packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports | packages.NeedDeps,
		Dir:   moduleRoot(t),
		Tests: false,
	}
	pkgs, err := packages.Load(cfg, "./internal/...", "./cmd/...", "./pkg/...")
	if err != nil {
		t.Fatal(err)
	}
	if n := packages.PrintErrors(pkgs); n > 0 {
		t.Fatalf("%d errors loading the module's packages", n)
	}
	got := allocationDeletes(t, pkgs)
	want := []string{"writes.go:deleteAllocation (an Allocation)"}
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("the Allocation deletes are %q, want only %q: every other path bypasses the release cause", got, want)
	}
}
