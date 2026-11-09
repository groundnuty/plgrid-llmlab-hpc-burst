package stack

// The inventory of a package's reads of the watch (issue #281), for a test that
// holds them to a table of its own. It is an inventory of the source, not a
// dataflow proof: it shows that a named function still calls the catch-up with
// the kinds named, and that a function reading the Recorder cannot appear
// without a class. It does not show that the call comes before the read, nor
// that a reader classed as covered runs after the wait that covers it.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// WatchUse is how one function of a package uses the watch.
type WatchUse struct {
	// Reads are the Recorder's methods the function calls or passes on,
	// repeats dropped, sorted.
	Reads []string
	// Syncs is one entry for each syncWatch call: the kinds it covers, sorted
	// and joined by "|".
	Syncs []string
	// SyncContexts is the first argument of each syncWatch call when that is a
	// plain name, "" otherwise.
	SyncContexts []string
}

// recorderReads are the Recorder's exported methods, except the catch-up: a new
// one counts as a read without anyone listing it here.
func recorderReads() map[string]bool {
	out := map[string]bool{}
	ty := reflect.TypeFor[*Recorder]()
	for i := range ty.NumMethod() {
		if name := ty.Method(i).Name; name != "CatchUp" {
			out[name] = true
		}
	}
	return out
}

// WatchUses reads the Go files of the directory and returns, for every function
// that reads the watch or calls syncWatch, how it does; and every other use of
// a field named watch, by position, such as a copy of it into a variable, which
// would hide the reads made through it. The watch is the field named watch: the
// rehearsal's and alpha's runs both hold it so.
func WatchUses(dir string) (uses map[string]WatchUse, other []string, err error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		return nil, nil, err
	}
	reads := recorderReads()
	uses = map[string]WatchUse{}
	fset := token.NewFileSet()
	for _, f := range files {
		af, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			return nil, nil, fmt.Errorf("parse %s: %w", f, err)
		}
		for _, d := range af.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok && fd.Body != nil {
				use, bad := watchUse(fset, fd, reads)
				other = append(other, bad...)
				if len(use.Reads) > 0 || len(use.Syncs) > 0 {
					uses[fd.Name.Name] = use
				}
			}
		}
	}
	slices.Sort(other)
	return uses, other, nil
}

// watchUse is how the function uses the watch, and the uses it cannot class.
func watchUse(fset *token.FileSet, fd *ast.FuncDecl, reads map[string]bool) (WatchUse, []string) {
	var use WatchUse
	var other []string
	classed := map[*ast.SelectorExpr]bool{}
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.AssignStmt:
			// The assignment of the field itself: where the run starts its watch.
			for _, l := range x.Lhs {
				if s, ok := l.(*ast.SelectorExpr); ok && s.Sel.Name == "watch" {
					classed[s] = true
				}
			}
		case *ast.CallExpr:
			if s, ok := x.Fun.(*ast.SelectorExpr); ok && s.Sel.Name == "syncWatch" {
				use.Syncs = append(use.Syncs, kindsIn(x.Args))
				name := ""
				if len(x.Args) > 0 {
					if id, ok := x.Args[0].(*ast.Ident); ok {
						name = id.Name
					}
				}
				use.SyncContexts = append(use.SyncContexts, name)
			}
		case *ast.SelectorExpr:
			inner, ok := x.X.(*ast.SelectorExpr)
			if !ok || inner.Sel.Name != "watch" {
				break
			}
			switch {
			case reads[x.Sel.Name]:
				classed[inner] = true
				if !slices.Contains(use.Reads, x.Sel.Name) {
					use.Reads = append(use.Reads, x.Sel.Name)
				}
			case x.Sel.Name == "CatchUp":
				classed[inner] = true
			}
		}
		return true
	})
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		if s, ok := n.(*ast.SelectorExpr); ok && s.Sel.Name == "watch" && !classed[s] {
			other = append(other, fmt.Sprintf("%s: %s", fset.Position(s.Pos()), fd.Name.Name))
		}
		return true
	})
	slices.Sort(use.Reads)
	return use, other
}

// kindsIn is the Kind* names in the arguments, sorted, joined by "|".
func kindsIn(args []ast.Expr) string {
	var kinds []string
	for _, a := range args {
		ast.Inspect(a, func(n ast.Node) bool {
			if s, ok := n.(*ast.SelectorExpr); ok && strings.HasPrefix(s.Sel.Name, "Kind") {
				kinds = append(kinds, s.Sel.Name)
			}
			return true
		})
	}
	slices.Sort(kinds)
	return strings.Join(kinds, "|")
}

// WatchClass is what a test's table says of one function that reads the watch.
type WatchClass struct {
	// Syncs is what WatchUse.Syncs must be: empty unless the function makes its
	// own catch-up.
	Syncs []string
	// Why says how a function with no catch-up of its own is safe: the read is
	// a wait's condition on the watch, or it follows a wait or a catch-up on the
	// same stream. It is checked by no one but the reader of the table.
	Why string
}

// CheckWatchInventory holds the Go files of dir to the table: every function
// that reads the watch or calls syncWatch is in it, with exactly the catch-ups it
// names, each with the context named when ctx is not empty; every function named
// is still there; and nothing uses the watch in a way the inventory cannot class.
func CheckWatchInventory(t testing.TB, dir string, table map[string]WatchClass, ctx string) {
	t.Helper()
	uses, other, err := WatchUses(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range other {
		t.Errorf("a use of the watch the inventory cannot class: %s", o)
	}
	for name, use := range uses {
		c, ok := table[name]
		if !ok {
			t.Errorf("%s reads the watch (%v, catch-ups %v) and is not in the table: say whether it makes a catch-up, or why it needs none", name, use.Reads, use.Syncs)
			continue
		}
		if !slices.Equal(use.Syncs, c.Syncs) {
			t.Errorf("%s makes the catch-ups %q, and the table says %q", name, use.Syncs, c.Syncs)
		}
		if len(c.Syncs) == 0 && c.Why == "" {
			t.Errorf("%s has no catch-up, and the table does not say why it needs none", name)
		}
		if ctx != "" {
			for _, got := range use.SyncContexts {
				if got != ctx {
					t.Errorf("%s calls syncWatch with %q as its context, want %q", name, got, ctx)
				}
			}
		}
	}
	for name := range table {
		if _, ok := uses[name]; !ok {
			t.Errorf("the table names %s, which no longer reads the watch", name)
		}
	}
}
