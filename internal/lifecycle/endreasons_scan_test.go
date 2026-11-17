package lifecycle

// TestEveryEndReasonHasAnOutcome: every reason an Instance can end with has a start-up outcome (endOutcome, #650),
// strengthened for #640, whose failNoted forwards the reason as a parameter and which the first scan therefore
// stopped seeing.
//
// The scan type-checks the package's shipped code. (*pass).end, the one writer of Status.EndReason, and (*pass).fail
// are the sinks. A function that passes its own parameter, never reassigned, as the reason of a sink or of another
// forwarder is a forwarder at that parameter, found to a fixpoint by flow, never by name. A reason site is the reason
// argument of every call of a sink or a forwarder, except where the caller itself forwards its own parameter. Each
// site is resolved to constants through constants, conversions, package functions' returns and same-function
// assignments, at most 4 steps deep. A leaf it cannot resolve (a parameter that is no forwarder's, a field, a call
// outside the package or through a value, a function literal) fails the test, naming the site: a reason it cannot
// see is never skipped. The constants found must equal endOutcome's keys.

import (
	"fmt"
	"go/ast"
	"go/constant"
	"go/token"
	gotypes "go/types"
	"maps"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

const (
	// reasonParam is the position of the reason among the parameters of (*pass).end and (*pass).fail.
	reasonParam = 2
	// maxResolveDepth is how many assignments and returns the resolver follows from a reason site.
	maxResolveDepth = 4
)

// endScan is the type-checked package and the forwarders found in it.
type endScan struct {
	pkg   *packages.Package
	decls []*ast.FuncDecl // the package's functions with a body, in source order
	obj   map[*ast.FuncDecl]*gotypes.Func
	decl  map[*gotypes.Func]*ast.FuncDecl
	// fwd holds, for the sinks and every forwarder, the parameters passed on unchanged as a reason.
	fwd map[*gotypes.Func]map[int]bool
}

// resolved is what resolving one reason site found: the constants, and each leaf it could not resolve.
type resolved struct {
	consts map[string]bool
	gaps   []string
}

// reasonSite is a call of a sink or a forwarder with the reason argument at its forwarding position, which is not a
// forwarding position of the caller itself. arg is nil when the call has no argument there.
type reasonSite struct {
	fd   *ast.FuncDecl
	call *ast.CallExpr
	to   *gotypes.Func
	arg  ast.Expr
}

func TestEveryEndReasonHasAnOutcome(t *testing.T) {
	s := loadEndScan(t)
	s.seedSinks(t)
	s.findForwarders()
	got := map[string]bool{}
	sites := s.sites()
	for _, st := range sites {
		out := &resolved{consts: map[string]bool{}}
		shown := "(no argument)"
		if st.arg == nil {
			out.gap(s, st.call, "the call has no argument at the reason's position")
		} else {
			shown = gotypes.ExprString(st.arg)
			s.resolve(st.arg, st.fd, 0, out)
		}
		maps.Copy(got, out.consts)
		for _, g := range out.gaps {
			t.Errorf("%s: the reason %s passed to %s cannot be resolved to constants: %s", s.at(st.call.Pos()), shown, st.to.Name(), g)
		}
	}
	for _, e := range s.escapes() {
		t.Errorf("%s", e)
	}
	emitted, table := slices.Sorted(maps.Keys(got)), slices.Sorted(maps.Keys(endOutcome))
	if !slices.Equal(emitted, table) {
		t.Errorf("the end reasons the package writes are %v; endOutcome classifies %v: each must be classified, and no other", emitted, table)
	}
	t.Logf("%d reason sites; forwarders: %s", len(sites), strings.Join(s.forwarderNames(), ", "))
}

// loadEndScan loads and type-checks the package's shipped code, with its dependencies from source, as the release-cause
// guard loads them: export data may be newer than this binary's type checker reads.
func loadEndScan(t *testing.T) *endScan {
	t.Helper()
	pkgs, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports | packages.NeedDeps,
		Dir:  ".",
	}, ".")
	if err != nil || len(pkgs) != 1 || packages.PrintErrors(pkgs) > 0 {
		t.Fatalf("load the package: %v", err)
	}
	s := &endScan{pkg: pkgs[0], obj: map[*ast.FuncDecl]*gotypes.Func{}, decl: map[*gotypes.Func]*ast.FuncDecl{}, fwd: map[*gotypes.Func]map[int]bool{}}
	for _, f := range s.pkg.Syntax {
		for _, d := range f.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok && fd.Body != nil {
				if fn, ok := s.pkg.TypesInfo.Defs[fd.Name].(*gotypes.Func); ok {
					s.decls = append(s.decls, fd)
					s.obj[fd], s.decl[fn] = fn, fd
				}
			}
		}
	}
	sort.Slice(s.decls, func(i, j int) bool { return s.decls[i].Pos() < s.decls[j].Pos() })
	return s
}

// seedSinks marks (*pass).end and (*pass).fail, each taking the reason as its parameter reasonParam.
func (s *endScan) seedSinks(t *testing.T) {
	t.Helper()
	for _, name := range []string{"end", "fail"} {
		found := 0
		for _, fd := range s.decls {
			fn := s.obj[fd]
			if fn.Name() != name || !onPass(fn) {
				continue
			}
			found++
			if ps := s.params(fd); len(ps) <= reasonParam || ps[reasonParam] == nil || ps[reasonParam].Name() != "reason" {
				t.Fatalf("(*pass).%s no longer takes the reason as its parameter %d: the scan reads the wrong argument", name, reasonParam)
			}
			s.fwd[fn] = map[int]bool{reasonParam: true}
		}
		if found != 1 {
			t.Fatalf("want one (*pass).%s, the scan's sink, found %d", name, found)
		}
	}
}

// findForwarders adds to fwd, to a fixpoint, each function that passes one of its own parameters on as a reason.
func (s *endScan) findForwarders() {
	for changed := true; changed; {
		changed = false
		for _, fd := range s.decls {
			fn, ps := s.obj[fd], s.params(fd)
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				for _, j := range s.positions(call) {
					if k := s.passedOn(fd, ps, call, j); k >= 0 && !s.fwd[fn][k] {
						if s.fwd[fn] == nil {
							s.fwd[fn] = map[int]bool{}
						}
						s.fwd[fn][k] = true
						changed = true
					}
				}
				return true
			})
		}
	}
}

// sites lists every reason site in the package, in source order.
func (s *endScan) sites() []reasonSite {
	var out []reasonSite
	for _, fd := range s.decls {
		ps := s.params(fd)
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			for _, j := range s.positions(call) {
				switch {
				case j >= len(call.Args):
					out = append(out, reasonSite{fd: fd, call: call, to: s.callee(call)})
				case s.passedOn(fd, ps, call, j) >= 0: // a forwarding position of the caller, not a site
				default:
					out = append(out, reasonSite{fd: fd, call: call, to: s.callee(call), arg: call.Args[j]})
				}
			}
			return true
		})
	}
	return out
}

// escapes lists each use of a sink or a forwarder other than as the callee of a call (a method value, a method
// expression, a parenthesised callee): a call through it has an argument list the scan cannot read.
func (s *endScan) escapes() []string {
	info := s.pkg.TypesInfo
	callees := map[*ast.Ident]bool{}
	for _, f := range s.pkg.Syntax {
		ast.Inspect(f, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok && s.callee(call) != nil {
				callees[calleeIdent(call.Fun)] = true
			}
			return true
		})
	}
	var out []string
	for _, f := range s.pkg.Syntax {
		ast.Inspect(f, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok {
				if fn, ok := info.Uses[id].(*gotypes.Func); ok && s.fwd[fn] != nil && !callees[id] {
					out = append(out, fmt.Sprintf("%s: %s is used as a value, or called through one: the scan cannot read the reason it is given", s.at(id.Pos()), id.Name))
				}
			}
			return true
		})
	}
	return out
}

// forwarderNames names the sinks and forwarders with their forwarding parameters, for the test's log.
func (s *endScan) forwarderNames() []string {
	var out []string
	for fn, ks := range s.fwd {
		out = append(out, fmt.Sprintf("%s%v", fn.Name(), slices.Sorted(maps.Keys(ks))))
	}
	slices.Sort(out)
	return out
}

// positions are the parameters of the call's callee that carry a reason, in order.
func (s *endScan) positions(call *ast.CallExpr) []int {
	fn := s.callee(call)
	if fn == nil {
		return nil
	}
	return slices.Sorted(maps.Keys(s.fwd[fn]))
}

// callee is the function or method a call names, or nil for a conversion, a function value or a method expression
// (which shifts the arguments by one).
func (s *endScan) callee(call *ast.CallExpr) *gotypes.Func {
	id := calleeIdent(call.Fun)
	if id == nil {
		return nil
	}
	if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
		if tv, ok := s.pkg.TypesInfo.Types[sel.X]; ok && tv.IsType() {
			return nil
		}
	}
	fn, _ := s.pkg.TypesInfo.Uses[id].(*gotypes.Func)
	return fn
}

// params are fd's parameters in order, the receiver excluded; an unnamed one is nil.
func (s *endScan) params(fd *ast.FuncDecl) []*gotypes.Var {
	var ps []*gotypes.Var
	for _, f := range fd.Type.Params.List {
		if len(f.Names) == 0 {
			ps = append(ps, nil)
		}
		for _, n := range f.Names {
			v, _ := s.pkg.TypesInfo.Defs[n].(*gotypes.Var)
			ps = append(ps, v)
		}
	}
	return ps
}

// passedOn is the index of fd's own parameter that the call passes, unchanged, as its argument j, or -1: the
// argument is one of fd's parameters, and fd never reassigns it.
func (s *endScan) passedOn(fd *ast.FuncDecl, ps []*gotypes.Var, call *ast.CallExpr, j int) int {
	if j >= len(call.Args) {
		return -1
	}
	id, ok := ast.Unparen(call.Args[j]).(*ast.Ident)
	if !ok {
		return -1
	}
	v, ok := s.pkg.TypesInfo.Uses[id].(*gotypes.Var)
	if !ok {
		return -1
	}
	if k := slices.Index(ps, v); k >= 0 && !s.reassigned(fd, v) {
		return k
	}
	return -1
}

// reassigned reports whether fd assigns to v, takes its address or ranges into it.
func (s *endScan) reassigned(fd *ast.FuncDecl, v *gotypes.Var) bool {
	uses := func(e ast.Expr) bool {
		id, ok := ast.Unparen(e).(*ast.Ident)
		return ok && s.pkg.TypesInfo.Uses[id] == v
	}
	found := false
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.AssignStmt:
			for _, l := range x.Lhs {
				found = found || uses(l)
			}
		case *ast.IncDecStmt:
			found = found || uses(x.X)
		case *ast.UnaryExpr:
			found = found || (x.Op == token.AND && uses(x.X))
		case *ast.RangeStmt:
			found = found || (x.Key != nil && uses(x.Key)) || (x.Value != nil && uses(x.Value))
		}
		return !found
	})
	return found
}

// resolve collects into out the constants e can be, and a gap for each leaf it cannot resolve.
func (s *endScan) resolve(e ast.Expr, in *ast.FuncDecl, depth int, out *resolved) {
	e = ast.Unparen(e)
	if depth > maxResolveDepth {
		out.gap(s, e, fmt.Sprintf("more than %d steps from the reason site", maxResolveDepth))
		return
	}
	info := s.pkg.TypesInfo
	if tv, ok := info.Types[e]; ok && tv.Value != nil {
		if tv.Value.Kind() != constant.String {
			out.gap(s, e, "a constant that is not a string")
		} else if v := constant.StringVal(tv.Value); v != "" {
			out.consts[v] = true
		}
		return
	}
	switch x := e.(type) {
	case *ast.CallExpr:
		if tv, ok := info.Types[x.Fun]; ok && tv.IsType() && len(x.Args) == 1 { // a conversion
			s.resolve(x.Args[0], in, depth, out)
			return
		}
		s.resolveCall(x, 0, depth, out)
	case *ast.Ident:
		s.resolveVar(x, in, depth, out)
	default:
		out.gap(s, e, "a form the scan does not follow (a field, an index, a function literal, an operation)")
	}
}

// resolveCall resolves result i of a call of a package function through the function's return statements.
func (s *endScan) resolveCall(call *ast.CallExpr, i, depth int, out *resolved) {
	fd := s.decl[s.callee(call)]
	if fd == nil {
		out.gap(s, call, "a call to a function outside the package, or through a value")
		return
	}
	n := fd.Type.Results.NumFields()
	if i >= n {
		out.gap(s, call, "the callee has no such result")
		return
	}
	returns := 0
	ast.Inspect(fd.Body, func(x ast.Node) bool {
		if _, ok := x.(*ast.FuncLit); ok {
			return false
		}
		r, ok := x.(*ast.ReturnStmt)
		if !ok {
			return true
		}
		returns++
		inner, isCall := (*ast.CallExpr)(nil), false
		if len(r.Results) == 1 {
			inner, isCall = ast.Unparen(r.Results[0]).(*ast.CallExpr)
		}
		switch {
		case len(r.Results) == n:
			s.resolve(r.Results[i], fd, depth+1, out)
		case isCall:
			s.resolveCall(inner, i, depth+1, out)
		default:
			out.gap(s, call, fd.Name.Name+" returns its named results bare")
		}
		return true
	})
	if returns == 0 {
		out.gap(s, call, fd.Name.Name+" has no return statement")
	}
}

// resolveVar resolves a local variable through every assignment to it in the function in.
func (s *endScan) resolveVar(id *ast.Ident, in *ast.FuncDecl, depth int, out *resolved) {
	info := s.pkg.TypesInfo
	v, ok := info.Uses[id].(*gotypes.Var)
	switch {
	case !ok:
		out.gap(s, id, "not a constant, a variable or a call")
		return
	case v.Parent() == s.pkg.Types.Scope():
		out.gap(s, id, "a package variable")
		return
	case slices.Contains(s.params(in), v):
		out.gap(s, id, "a parameter of "+in.Name.Name+", which does not forward it as a reason")
		return
	}
	is := func(n *ast.Ident) bool { return info.Defs[n] == v || info.Uses[n] == v }
	defs := 0
	ast.Inspect(in.Body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.AssignStmt:
			for li, lhs := range x.Lhs {
				l, ok := ast.Unparen(lhs).(*ast.Ident)
				if !ok || !is(l) {
					continue
				}
				defs++
				s.resolveDef(x.Tok == token.ASSIGN || x.Tok == token.DEFINE, l, x.Rhs, li, len(x.Lhs), in, depth, out)
			}
		case *ast.ValueSpec:
			for li, name := range x.Names {
				if is(name) {
					defs++
					if len(x.Values) > 0 { // none: the zero value, which names no reason
						s.resolveDef(true, name, x.Values, li, len(x.Names), in, depth, out)
					}
				}
			}
		case *ast.RangeStmt:
			for _, e := range []ast.Expr{x.Key, x.Value} {
				if l, ok := e.(*ast.Ident); ok && is(l) {
					defs++
					out.gap(s, l, "a range variable")
				}
			}
		case *ast.UnaryExpr:
			if l, ok := ast.Unparen(x.X).(*ast.Ident); ok && x.Op == token.AND && is(l) {
				out.gap(s, l, "its address is taken")
			}
		}
		return true
	})
	if defs == 0 {
		out.gap(s, id, "no assignment of it is found in "+in.Name.Name)
	}
}

// resolveDef resolves the value one definition of a variable gives it: the li-th of nl targets, from rhs.
func (s *endScan) resolveDef(plain bool, l *ast.Ident, rhs []ast.Expr, li, nl int, in *ast.FuncDecl, depth int, out *resolved) {
	switch call, isCall := (*ast.CallExpr)(nil), false; {
	case !plain:
		out.gap(s, l, "built by a compound assignment")
	case len(rhs) == nl:
		s.resolve(rhs[li], in, depth+1, out)
	default:
		if len(rhs) == 1 {
			call, isCall = ast.Unparen(rhs[0]).(*ast.CallExpr)
		}
		if isCall {
			s.resolveCall(call, li, depth, out)
		} else {
			out.gap(s, l, "assigned from a form the scan does not follow")
		}
	}
}

// gap records a leaf n that could not be resolved, with where it is and what it is.
func (r *resolved) gap(s *endScan, n ast.Expr, why string) {
	r.gaps = append(r.gaps, fmt.Sprintf("%s: %s (%s)", s.at(n.Pos()), gotypes.ExprString(n), why))
}

// at is a position as file:line.
func (s *endScan) at(p token.Pos) string {
	pos := s.pkg.Fset.Position(p)
	return fmt.Sprintf("%s:%d", filepath.Base(pos.Filename), pos.Line)
}

// onPass reports whether fn is a method of *pass.
func onPass(fn *gotypes.Func) bool {
	sig, ok := fn.Type().(*gotypes.Signature)
	if !ok || sig.Recv() == nil {
		return false
	}
	ptr, ok := sig.Recv().Type().(*gotypes.Pointer)
	if !ok {
		return false
	}
	named, ok := ptr.Elem().(*gotypes.Named)
	return ok && named.Obj().Name() == "pass"
}

// calleeIdent is the identifier a call names: f in f(), or m in x.m().
func calleeIdent(e ast.Expr) *ast.Ident {
	switch x := e.(type) {
	case *ast.Ident:
		return x
	case *ast.SelectorExpr:
		return x.Sel
	}
	return nil
}
