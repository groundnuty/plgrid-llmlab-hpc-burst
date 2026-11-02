package scan

import (
	"fmt"
	"go/constant"
	"go/token"
	"go/types"
	"slices"
	"sort"

	"golang.org/x/tools/go/ssa"
)

// leafFn looks at one value. It returns done with the answer (the values, or
// why there are none), or, when it only sees through the value, next, the value
// to continue with. A value it neither answers nor sees through is left to
// the structural walk: phis, parameters and call results.
type leafFn func(v ssa.Value) (vals []string, why string, next ssa.Value, done bool)

// stringLeaf reads a string argument: a constant is its value, and a
// conversion passes its operand through.
func stringLeaf(v ssa.Value) ([]string, string, ssa.Value, bool) {
	switch v := v.(type) {
	case *ssa.Const:
		if v.Value == nil { // the zero value of a string
			return []string{""}, "", nil, true
		}
		if v.Value.Kind() != constant.String {
			return nil, fmt.Sprintf("the constant %s is not a string", v.Name()), nil, true
		}
		return []string{constant.StringVal(v.Value)}, "", nil, true
	case *ssa.Convert:
		return nil, "", v.X, false
	case *ssa.ChangeType:
		return nil, "", v.X, false
	}
	return nil, "", nil, false
}

// kindLeaf reads a regarding argument: the kind is the name of its type, and an
// interface value is followed to the concrete values converted into it.
func kindLeaf(v ssa.Value) ([]string, string, ssa.Value, bool) {
	if !types.IsInterface(v.Type()) {
		t := v.Type()
		if p, ok := t.(*types.Pointer); ok {
			t = p.Elem()
		}
		if n, ok := t.(*types.Named); ok {
			return []string{n.Obj().Name()}, "", nil, true
		}
		return nil, fmt.Sprintf("the type %s of the regarding object is not a named type", v.Type()), nil, true
	}
	switch v := v.(type) {
	case *ssa.MakeInterface:
		return nil, "", v.X, false
	case *ssa.ChangeInterface:
		return nil, "", v.X, false
	}
	return nil, "", nil, false
}

// acc collects what a walk finds: the values, and those of them that arrived
// as a string literal at a call site (see scanner.literalArg).
type acc struct{ set, lits map[string]bool }

func newAcc() *acc { return &acc{set: map[string]bool{}, lits: map[string]bool{}} }

func (a *acc) merge(b *acc) {
	for x := range b.set {
		a.set[x] = true
	}
	for x := range b.lits {
		a.lits[x] = true
	}
}

func sorted(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for x := range m {
		out = append(out, x)
	}
	sort.Strings(out)
	return out
}

// resolve returns the values that can reach v, sorted and without repeats, and
// those that arrived as a literal, or why the values cannot be told: it follows phis, the parameters of functions that
// are only called statically (to their callers' arguments) and the results of
// static calls (to the values their functions return). Anything else, such as a
// field, a map entry or a call through an interface, is a reason.
func (s *scanner) resolve(v ssa.Value, leaf leafFn) (vals, lits []string, why string) {
	a := newAcc()
	if why := s.walk(v, leaf, a, map[ssa.Value]bool{}, 0); why != "" {
		return nil, nil, why
	}
	return sorted(a.set), sorted(a.lits), ""
}

func (s *scanner) walk(v ssa.Value, leaf leafFn, a *acc, path map[ssa.Value]bool, depth int) string {
	if depth > maxDepth {
		return "the trace is deeper than the scan follows"
	}
	if path[v] {
		return "" // a loop through a phi adds nothing new
	}
	path[v] = true
	defer delete(path, v)

	vals, why, next, done := leaf(v)
	switch {
	case done:
		for _, x := range vals {
			a.set[x] = true
		}
		return why
	case next != nil:
		return s.walk(next, leaf, a, path, depth+1)
	}

	switch v := v.(type) {
	case *ssa.Phi:
		for _, e := range v.Edges {
			if why := s.walk(e, leaf, a, path, depth+1); why != "" {
				return why
			}
		}
		return ""
	case *ssa.Parameter:
		fn := v.Parent()
		if s.escaped[fn] {
			return fmt.Sprintf("parameter %s of %s, which is used as a value, so its callers are not all known", v.Name(), fn.Name())
		}
		idx := slices.Index(fn.Params, v)
		callers := s.callers[fn]
		if len(callers) == 0 {
			return fmt.Sprintf("parameter %s of %s, which no call in the module reaches", v.Name(), fn.Name())
		}
		for _, cc := range callers {
			if idx < 0 || idx >= len(cc.Args) {
				return fmt.Sprintf("parameter %s of %s: a caller passes too few arguments", v.Name(), fn.Name())
			}
			sub := newAcc()
			if why := s.walk(cc.Args[idx], leaf, sub, path, depth+1); why != "" {
				return why
			}
			astIdx := idx
			if fn.Signature.Recv() != nil {
				astIdx--
			}
			if s.literalArg(cc, astIdx) {
				for x := range sub.set {
					sub.lits[x] = true
				}
			}
			a.merge(sub)
		}
		return ""
	case *ssa.UnOp:
		if v.Op != token.MUL {
			break
		}
		// A load from a variable that escapes to the heap, as one a closure
		// captures is: every value stored into it can be what is loaded.
		vals, why := stores(v.X)
		if why != "" {
			return why
		}
		for _, val := range vals {
			if why := s.walk(val, leaf, a, path, depth+1); why != "" {
				return why
			}
		}
		return ""
	case *ssa.Call:
		return s.walkResult(v.Common(), 0, leaf, a, path, depth)
	case *ssa.Extract:
		call, ok := v.Tuple.(*ssa.Call)
		if !ok {
			return fmt.Sprintf("a result of %T", v.Tuple)
		}
		return s.walkResult(call.Common(), v.Index, leaf, a, path, depth)
	}
	return fmt.Sprintf("%s (%T) is not a constant, a parameter or the result of a call in the module", v.Name(), v)
}

// walkResult follows result idx of a static call to what its function returns.
func (s *scanner) walkResult(cc *ssa.CallCommon, idx int, leaf leafFn, a *acc, path map[ssa.Value]bool, depth int) string {
	callee := cc.StaticCallee()
	if callee == nil {
		return fmt.Sprintf("the result of a call through %s, which is not a static call", callTarget(cc))
	}
	if len(callee.Blocks) == 0 {
		return fmt.Sprintf("the result of %s, which has no body in the module", callee.Name())
	}
	found := false
	for _, b := range callee.Blocks {
		if len(b.Instrs) == 0 {
			continue
		}
		ret, ok := b.Instrs[len(b.Instrs)-1].(*ssa.Return)
		if !ok {
			continue
		}
		found = true
		if idx >= len(ret.Results) {
			return fmt.Sprintf("result %d of %s", idx, callee.Name())
		}
		if why := s.walk(ret.Results[idx], leaf, a, path, depth+1); why != "" {
			return why
		}
	}
	if !found {
		return fmt.Sprintf("%s never returns", callee.Name())
	}
	return ""
}

// stores returns the values stored into a variable (an Alloc, or a FreeVar of a
// closure that captured one). It returns why it cannot tell when the variable's
// address is used for anything but loads and stores, so a store through a
// pointer that was passed on would not be missed.
func stores(addr ssa.Value) ([]ssa.Value, string) {
	switch addr.(type) {
	case *ssa.Alloc, *ssa.FreeVar:
	default:
		return nil, fmt.Sprintf("a load through %T, which is not a local variable", addr)
	}
	refs := addr.Referrers()
	if refs == nil {
		return nil, "a variable with no referrers"
	}
	var vals []ssa.Value
	for _, r := range *refs {
		switch r := r.(type) {
		case *ssa.Store:
			if r.Val == addr {
				return nil, "the address of a variable is stored elsewhere"
			}
			vals = append(vals, r.Val)
		case *ssa.UnOp: // a load
			if r.Op != token.MUL {
				return nil, fmt.Sprintf("a variable is used by %s", r)
			}
		case *ssa.DebugRef:
		case *ssa.MakeClosure:
			fn, ok := r.Fn.(*ssa.Function)
			if !ok {
				return nil, "a variable is captured by a closure that is not a function"
			}
			for i, b := range r.Bindings {
				if b != addr {
					continue
				}
				inner, why := stores(fn.FreeVars[i])
				if why != "" {
					return nil, why
				}
				vals = append(vals, inner...)
			}
		default:
			return nil, fmt.Sprintf("a variable is used by %T, which the scan does not follow", r)
		}
	}
	return vals, ""
}

func callTarget(cc *ssa.CallCommon) string {
	if cc.IsInvoke() {
		return cc.Method.FullName()
	}
	return cc.Value.Name()
}
