package scenario_test

// Each helper has one implementation (issue #251): internal/rehearsal/stack's.
// The scenario used to hold copies of them, and #245's near-copies then drifted
// from it. This guard reads both packages' sources, so an ordinary go test
// fails on a helper copied back, under its own name or another.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// notCopies are the scenario's names that a helper of stack also has, and why
// each is a helper of its own: the same word for another job.
var notCopies = map[string]string{
	"rig.allocations": "lists the deployment's Allocations from the API; stack's Recorder.Allocations returns the versions the watch saw",
}

// minShapeNodes is the smallest body the shape check compares, in syntax nodes:
// a few lines, below which unrelated helpers have the same shape.
const minShapeNodes = 40

// decl is one top-level declaration, or a method.
type decl struct {
	name  string // Name, or Recv.Name for a method
	short string // the name, case-folded and without its receiver
	shape string // a function's body with every name and literal dropped
	nodes int    // the size of that body
	at    string
}

// TestNoCopyOfStack: nothing in the scenario's test files repeats a helper of
// the stack package's sources.
func TestNoCopyOfStack(t *testing.T) {
	own, err := declsOf(".", func(name string) bool { return strings.HasSuffix(name, "_test.go") })
	if err != nil {
		t.Fatal(err)
	}
	st, err := declsOf(filepath.Join("..", "stack"), func(name string) bool { return !strings.HasSuffix(name, "_test.go") })
	if err != nil {
		t.Fatal(err)
	}
	if len(own) == 0 || len(st) == 0 {
		t.Fatalf("%d declarations in the scenario and %d in stack: the guard read no sources", len(own), len(st))
	}
	for _, p := range copyProblems(own, st, notCopies) {
		t.Error(p)
	}
}

// TestCopyProblems: the check itself, on fixtures: it finds a copy under the
// same name in another case, a renamed copy of a body, a method's copy as a
// free function, and a type or constant of the same name, and it passes a
// different body and an excused name.
func TestCopyProblems(t *testing.T) {
	st := parseDecls(t, `package stack

const WaitDeadline = 1

type Reply struct{ Status int }

func (c *Client) RowOf(id string) string {
	c.T.Helper()
	for i := 0; i < 3; i++ {
		if x := c.read(id, i); x != "" {
			return x
		}
		c.T.Logf("no row of %s yet: try %d", id, i)
	}
	c.T.Logf("giving up on %s", id)
	return ""
}

func Listening(t T, ports []int) []int { return ports }
`)
	for name, src := range map[string]string{
		"the same name in another case":       `func listening(t *testing.T, ports []int) []int { return ports }`,
		"a method's name on another receiver": `func (r *rig) listening() {}`,
		"a type of the same name":             `type reply struct{ status int }`,
		"a constant of the same name":         `const waitDeadline = 2`,
		"a variable of the same name":         `var waitDeadline = 2`,
		"a renamed copy of a method's body": `func lookFor(r *rig, id string) string {
	r.t.Helper()
	for n := 0; n < 3; n++ {
		if y := r.fetch(id, n); y != "" {
			return y
		}
		r.t.Logf("nothing for %s: try %d", id, n)
	}
	r.t.Logf("none for %s", id)
	return ""
}`,
	} {
		if p := copyProblems(parseDecls(t, "package scenario_test\n\n"+src+"\n"), st, nil); len(p) != 1 {
			t.Errorf("%s: %v, want one problem", name, p)
		}
	}
	for name, src := range map[string]string{
		"another name and body":                             `func checkNoGap(rows []int) int { return len(rows) }`,
		"a small body of the same shape under another name": `func ownPorts(t *testing.T, p []int) []int { return p }`,
	} {
		if p := copyProblems(parseDecls(t, "package scenario_test\n\n"+src+"\n"), st, nil); len(p) != 0 {
			t.Errorf("%s: %v, want none", name, p)
		}
	}
	excused := parseDecls(t, "package scenario_test\n\nfunc (r *rig) listening() {}\n")
	if p := copyProblems(excused, st, map[string]string{"rig.listening": "another job"}); len(p) != 0 {
		t.Errorf("an excused name: %v, want none", p)
	}
}

// copyProblems lists each declaration of own that repeats one of st's: by its
// name, case-folded, whatever its receiver; or, for a function of minShapeNodes
// or more, by the shape of its body. excused holds own's names (Name, or
// Recv.Name) that are not copies.
func copyProblems(own, st []decl, excused map[string]string) []string {
	names := map[string][]decl{}
	shapes := map[string][]decl{}
	for _, d := range st {
		names[d.short] = append(names[d.short], d)
		if d.nodes >= minShapeNodes {
			shapes[d.shape] = append(shapes[d.shape], d)
		}
	}
	var out []string
	for _, d := range own {
		if _, ok := excused[d.name]; ok {
			continue
		}
		if m := names[d.short]; len(m) > 0 {
			out = append(out, fmt.Sprintf("%s (%s) has the name of stack's %s (%s): use stack's, or give notCopies the reason", d.name, d.at, m[0].name, m[0].at))
			continue
		}
		if d.nodes >= minShapeNodes {
			if m := shapes[d.shape]; len(m) > 0 {
				out = append(out, fmt.Sprintf("%s (%s) has the body of stack's %s (%s), with other names: use stack's", d.name, d.at, m[0].name, m[0].at))
			}
		}
	}
	sort.Strings(out)
	return out
}

// declsOf reads the declarations of the Go files in dir that keep accepts.
func declsOf(dir string, keep func(name string) bool) ([]decl, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		return nil, err
	}
	var out []decl
	for _, f := range files {
		if !keep(filepath.Base(f)) {
			continue
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			return nil, err
		}
		out = append(out, fileDecls(fset, file)...)
	}
	return out, nil
}

func parseDecls(t *testing.T, src string) []decl {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", src, 0)
	if err != nil {
		t.Fatalf("a fixture: %v\n%s", err, src)
	}
	return fileDecls(fset, file)
}

func fileDecls(fset *token.FileSet, file *ast.File) []decl {
	at := func(p token.Pos) string {
		pos := fset.Position(p)
		return fmt.Sprintf("%s:%d", filepath.Base(pos.Filename), pos.Line)
	}
	var out []decl
	add := func(id *ast.Ident) {
		if id.Name != "_" {
			out = append(out, decl{name: id.Name, short: strings.ToLower(id.Name), at: at(id.Pos())})
		}
	}
	for _, d := range file.Decls {
		switch d := d.(type) {
		case *ast.FuncDecl:
			x := decl{name: d.Name.Name, short: strings.ToLower(d.Name.Name), at: at(d.Name.Pos())}
			if d.Recv != nil && len(d.Recv.List) == 1 {
				x.name = recvName(d.Recv.List[0].Type) + "." + d.Name.Name
			}
			if d.Body != nil {
				x.shape, x.nodes = shapeOf(d.Body)
			}
			out = append(out, x)
		case *ast.GenDecl:
			for _, s := range d.Specs {
				switch s := s.(type) {
				case *ast.TypeSpec:
					add(s.Name)
				case *ast.ValueSpec:
					for _, id := range s.Names {
						add(id)
					}
				}
			}
		}
	}
	return out
}

// recvName is a receiver's type name, without its pointer or type parameters.
func recvName(e ast.Expr) string {
	switch e := e.(type) {
	case *ast.StarExpr:
		return recvName(e.X)
	case *ast.IndexExpr:
		return recvName(e.X)
	case *ast.Ident:
		return e.Name
	}
	return "?"
}

// shapeOf is a body's syntax with every name and literal dropped. A selector
// counts as one name, so r.t.Helper() has the shape of t.Helper(), and a
// method's copy on another receiver has the shape of the original.
func shapeOf(body *ast.BlockStmt) (string, int) {
	var b strings.Builder
	nodes := 0
	ast.Inspect(body, func(n ast.Node) bool {
		if n == nil {
			b.WriteByte(')')
			return false
		}
		nodes++
		switch n.(type) {
		case *ast.SelectorExpr, *ast.Ident, *ast.BasicLit:
			b.WriteByte('x')
			// Inspect still calls with nil after a false return only for
			// nodes it descended into, so this leaf writes no closing mark.
			return false
		}
		fmt.Fprintf(&b, "(%T", n)
		return true
	})
	return b.String(), nodes
}
