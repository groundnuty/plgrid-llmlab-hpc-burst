package scan

import (
	"errors"
	"fmt"
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"
	"strings"

	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/reasondrift"
)

// The managed catalog controller raises its events from the conditions it
// writes: internal/managed's emitTransitions reads a Provider's conditions of
// the watched types and raises an event with the reason it finds there. That
// reason is a field of a condition read at run time, so no recorder call names
// it. What the code can say statically is which reasons it can write into those
// conditions: an over-approximation of what it emits at any moment. collectManaged
// finds them by reference and fails closed on a write it cannot read.

// defaultSetters are the functions that write a condition; the second argument
// is the condition.
var defaultSetters = []string{"k8s.io/apimachinery/pkg/api/meta.SetStatusCondition"}

// collectManaged reads the watched condition types and the map of bad states of
// internal/managed, then every call of a condition setter in the module: one
// whose type can be a watched type contributes the reasons it can write. A write
// whose type or reason cannot be resolved, and a reason that is a string
// literal or is built, are problems.
func (s *scanner) collectManaged(code *reasondrift.Code, managed *packages.Package, setters []string) error {
	watched, err := watchedTypes(managed)
	if err != nil {
		return err
	}
	if code.ManagedBad, err = badActions(managed); err != nil {
		return err
	}
	code.WatchedTypes = watched
	isWatched := map[string]bool{}
	for _, w := range watched {
		isWatched[w] = true
	}
	isSetter := map[string]bool{}
	for _, f := range setters {
		isSetter[f] = true
	}
	reach := s.reachableFrom("internal/managed")
	for _, fn := range s.fns {
		if !reach[fn] {
			continue // the Provider controller's and the lifecycle controller's conditions are not the managed controller's
		}
		for _, b := range fn.Blocks {
			for _, in := range b.Instrs {
				ci, ok := in.(ssa.CallInstruction)
				if !ok {
					continue
				}
				cc := ci.Common()
				full, _ := calleeName(cc)
				if !isSetter[strings.ReplaceAll(full, s.mod, "{mod}")] {
					continue
				}
				problem := func(kind, detail string) {
					code.Problems = append(code.Problems, reasondrift.Problem{
						Kind: kind, Package: s.pkgDir(fn), Func: funcName(fn), Detail: detail + " (" + s.pos(ci.Pos()) + ")",
					})
				}
				if len(cc.Args) < 2 {
					problem(reasondrift.ManagedUnresolved, "the call of "+full+" has no condition argument")
					continue
				}
				types, why := s.fieldStrings(cc.Args[1], "Type")
				if why != "" {
					problem(reasondrift.ManagedUnresolved, "the type of the condition: "+why)
					continue
				}
				var typ string
				for _, t := range types {
					if t = strings.ReplaceAll(t, litMark, ""); isWatched[t] {
						typ = t
					}
				}
				if typ == "" {
					continue // not a watched condition
				}
				reasons, why := s.fieldStrings(cc.Args[1], "Reason")
				if why != "" {
					problem(reasondrift.ManagedUnresolved, "the reason written into "+typ+": "+why)
					continue
				}
				for _, r := range reasons {
					if strings.Contains(r, litMark) {
						problem(reasondrift.ManagedNotConstant, fmt.Sprintf("the reason %q written into %s is a string literal, or built from one", strings.ReplaceAll(r, litMark, ""), typ))
						continue
					}
					if r != "" {
						code.ManagedWrites = append(code.ManagedWrites, reasondrift.ManagedWrite{Reason: r, Type: typ, Pos: s.pos(ci.Pos())})
					}
				}
			}
		}
	}
	return nil
}

// reachableFrom returns the functions of the module that the functions of the
// package in dir can reach through static calls and closures they build: the
// managed controller is internal/managed and what it calls, in pkg/provider as
// well. A call it makes through an interface or a function value is not
// followed, which the PR body states.
func (s *scanner) reachableFrom(dir string) map[*ssa.Function]bool {
	reach := map[*ssa.Function]bool{}
	var queue []*ssa.Function
	for _, fn := range s.fns {
		if fn.Pkg != nil && s.pkgDir(fn) == dir {
			reach[fn] = true
			queue = append(queue, fn)
		}
	}
	for len(queue) > 0 {
		fn := queue[0]
		queue = queue[1:]
		for _, callee := range s.callees[fn] {
			if !reach[callee] {
				reach[callee] = true
				queue = append(queue, callee)
			}
		}
	}
	return reach
}

// watchedTypes reads the first element of each entry of internal/managed's
// watched variable (the condition type), through the type-checker's constant
// values.
func watchedTypes(p *packages.Package) ([]string, error) {
	lit, err := packageVar(p, "watched")
	if err != nil {
		return nil, err
	}
	var out []string
	for _, el := range lit.Elts {
		entry, ok := el.(*ast.CompositeLit)
		if !ok || len(entry.Elts) == 0 {
			return nil, errors.New("an entry of internal/managed's watched is not a composite literal")
		}
		first := entry.Elts[0]
		if kv, ok := first.(*ast.KeyValueExpr); ok {
			first = kv.Value
		}
		v, ok := constString(p, first)
		if !ok {
			return nil, fmt.Errorf("the type of an entry of internal/managed's watched is not a string constant: %s", p.Fset.Position(first.Pos()))
		}
		out = append(out, v)
	}
	if len(out) == 0 {
		return nil, errors.New("internal/managed's watched is empty")
	}
	return out, nil
}

// badActions reads internal/managed's badAction, the map from the reason of a
// bad state to the type of its event.
func badActions(p *packages.Package) (map[string]string, error) {
	lit, err := packageVar(p, "badAction")
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, el := range lit.Elts {
		kv, ok := el.(*ast.KeyValueExpr)
		if !ok {
			return nil, errors.New("an entry of internal/managed's badAction is not a key and a value")
		}
		k, kok := constString(p, kv.Key)
		v, vok := constString(p, kv.Value)
		if !kok || !vok {
			return nil, fmt.Errorf("an entry of internal/managed's badAction is not a pair of string constants: %s", p.Fset.Position(kv.Pos()))
		}
		out[k] = v
	}
	if len(out) == 0 {
		return nil, errors.New("internal/managed's badAction is empty")
	}
	return out, nil
}

// packageVar finds the composite literal a package-level variable is initialized
// with.
func packageVar(p *packages.Package, name string) (*ast.CompositeLit, error) {
	if p == nil {
		return nil, errors.New("internal/managed was not loaded")
	}
	for _, f := range p.Syntax {
		for _, d := range f.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok || gd.Tok != token.VAR {
				continue
			}
			for _, spec := range gd.Specs {
				vs := spec.(*ast.ValueSpec)
				for i, n := range vs.Names {
					if n.Name != name {
						continue
					}
					if i < len(vs.Values) {
						if lit, ok := vs.Values[i].(*ast.CompositeLit); ok {
							return lit, nil
						}
					}
					return nil, fmt.Errorf("internal/managed's %s is not initialized with a composite literal", name)
				}
			}
		}
	}
	return nil, fmt.Errorf("internal/managed declares no %s", name)
}

func constString(p *packages.Package, e ast.Expr) (string, bool) {
	tv, ok := p.TypesInfo.Types[e]
	if !ok || tv.Value == nil || tv.Value.Kind() != constant.String {
		return "", false
	}
	return strings.ReplaceAll(constant.StringVal(tv.Value), litMark, ""), true
}

// fieldStrings resolves the string values the named field of a struct value can
// hold: the values stored into the field of the variable that holds the struct,
// wherever the struct was built, passed in, or returned from.
func (s *scanner) fieldStrings(v ssa.Value, field string) ([]string, string) {
	vals, why := s.fieldValues(v, field, map[ssa.Value]bool{}, 0)
	if why != "" {
		return nil, why
	}
	set := map[string]bool{}
	for _, val := range vals {
		if why := s.walk(val, stringLeaf, set, map[key]bool{}, nil, 0); why != "" {
			return nil, why
		}
	}
	return sorted(set), ""
}

// fieldValues returns the values that can be in field of the struct value v.
func (s *scanner) fieldValues(v ssa.Value, field string, seen map[ssa.Value]bool, depth int) ([]ssa.Value, string) {
	if depth > maxDepth {
		return nil, "the trace is deeper than the scan follows"
	}
	if seen[v] {
		return nil, ""
	}
	seen[v] = true
	defer delete(seen, v)
	switch v := v.(type) {
	case *ssa.UnOp:
		if a, ok := v.X.(*ssa.Alloc); ok && v.Op == token.MUL {
			return s.allocField(a, field, seen, depth)
		}
		return nil, fmt.Sprintf("a struct loaded through %T", v.X)
	case *ssa.Phi:
		var out []ssa.Value
		for _, e := range v.Edges {
			vals, why := s.fieldValues(e, field, seen, depth+1)
			if why != "" {
				return nil, why
			}
			out = append(out, vals...)
		}
		return out, ""
	case *ssa.Parameter:
		fn := v.Parent()
		if s.escaped[fn] {
			return nil, fmt.Sprintf("parameter %s of %s, which is used as a value", v.Name(), fn.Name())
		}
		callers := s.callers[fn]
		if len(callers) == 0 {
			return nil, fmt.Sprintf("parameter %s of %s, which no call in the module reaches", v.Name(), fn.Name())
		}
		idx := 0
		for i, p := range fn.Params {
			if p == v {
				idx = i
			}
		}
		var out []ssa.Value
		for _, cc := range callers {
			if idx >= len(cc.Args) {
				return nil, "a caller passes too few arguments"
			}
			vals, why := s.fieldValues(cc.Args[idx], field, seen, depth+1)
			if why != "" {
				return nil, why
			}
			out = append(out, vals...)
		}
		return out, ""
	case *ssa.Call:
		return s.fieldValuesOfResult(v.Common(), 0, field, seen, depth)
	case *ssa.Extract:
		if call, ok := v.Tuple.(*ssa.Call); ok {
			return s.fieldValuesOfResult(call.Common(), v.Index, field, seen, depth)
		}
	}
	return nil, fmt.Sprintf("%s (%T) is not a struct the scan can follow", v.Name(), v)
}

func (s *scanner) fieldValuesOfResult(cc *ssa.CallCommon, idx int, field string, seen map[ssa.Value]bool, depth int) ([]ssa.Value, string) {
	callee := cc.StaticCallee()
	if callee == nil || len(callee.Blocks) == 0 {
		return nil, "the struct is the result of a call that is not static or has no body in the module"
	}
	var out []ssa.Value
	for _, b := range callee.Blocks {
		if len(b.Instrs) == 0 {
			continue
		}
		ret, ok := b.Instrs[len(b.Instrs)-1].(*ssa.Return)
		if !ok {
			continue
		}
		if idx >= len(ret.Results) {
			return nil, fmt.Sprintf("result %d of %s", idx, callee.Name())
		}
		vals, why := s.fieldValues(ret.Results[idx], field, seen, depth+1)
		if why != "" {
			return nil, why
		}
		out = append(out, vals...)
	}
	return out, ""
}

// allocField returns the values stored into a field of the variable a struct is
// held in: the stores to the field's address, and the fields of every whole
// struct assigned to the variable. It gives up if the variable's address is used
// for anything else.
func (s *scanner) allocField(a *ssa.Alloc, field string, seen map[ssa.Value]bool, depth int) ([]ssa.Value, string) {
	ptr, ok := a.Type().Underlying().(*types.Pointer)
	if !ok {
		return nil, "a variable that is not a pointer"
	}
	st, ok := ptr.Elem().Underlying().(*types.Struct)
	if !ok {
		return nil, "a variable that does not hold a struct"
	}
	idx := -1
	for i := 0; i < st.NumFields(); i++ {
		if st.Field(i).Name() == field {
			idx = i
		}
	}
	if idx < 0 {
		return nil, fmt.Sprintf("the struct has no field %s", field)
	}
	refs := a.Referrers()
	if refs == nil {
		return nil, "a variable with no referrers"
	}
	var out []ssa.Value
	for _, r := range *refs {
		switch r := r.(type) {
		case *ssa.FieldAddr:
			frefs := r.Referrers()
			if frefs == nil {
				continue
			}
			for _, fr := range *frefs {
				switch fr := fr.(type) {
				case *ssa.Store:
					if r.Field == idx && fr.Addr == ssa.Value(r) {
						out = append(out, fr.Val)
					}
				case *ssa.UnOp, *ssa.DebugRef:
				default:
					return nil, fmt.Sprintf("the address of a field of the condition is used by %T", fr)
				}
			}
		case *ssa.Store:
			if r.Addr != ssa.Value(a) {
				return nil, "the address of the condition is stored elsewhere"
			}
			vals, why := s.fieldValues(r.Val, field, seen, depth+1)
			if why != "" {
				return nil, why
			}
			out = append(out, vals...)
		case *ssa.UnOp, *ssa.DebugRef:
		default:
			return nil, fmt.Sprintf("the address of the condition is used by %T", r)
		}
	}
	return out, ""
}
