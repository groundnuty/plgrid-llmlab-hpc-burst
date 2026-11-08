package scan

import (
	"fmt"
	"go/constant"
	"go/token"
	"go/types"
	"slices"

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

// frame is a call being followed into: the callee, the arguments of that call
// and the frame the call was made in. A parameter met while following the
// callee's results is the call's own argument, not the union of every caller's.
type frame struct {
	fn   *ssa.Function
	args []ssa.Value
	up   *frame
}

// key identifies a value in the context it is reached in: the same value
// reached through two calls is two walks, and neither is a loop.
type key struct {
	v   ssa.Value
	ctx *frame
}

// resolve returns the values that can reach v, sorted and without repeats, or
// why they cannot be told: it follows phis, the parameters of functions only
// called statically (to their callers' arguments), and the results of static
// calls (to the values their functions return, for the arguments of that call
// alone). Anything else, such as a field, a map entry or a call through an
// interface, is a reason.
func (s *scanner) resolve(v ssa.Value, leaf leafFn) ([]string, string) {
	set := map[string]bool{}
	if why := s.walk(v, leaf, set, map[key]bool{}, nil, 0); why != "" {
		return nil, why
	}
	return sorted(set), ""
}

func (s *scanner) walk(v ssa.Value, leaf leafFn, set map[string]bool, path map[key]bool, ctx *frame, depth int) string {
	if depth > maxDepth {
		return "the trace is deeper than the scan follows"
	}
	k := key{v, ctx}
	if path[k] {
		return "" // a loop through a phi adds nothing new
	}
	path[k] = true
	defer delete(path, k)

	vals, why, next, done := leaf(v)
	switch {
	case done:
		for _, x := range vals {
			set[x] = true
		}
		return why
	case next != nil:
		return s.walk(next, leaf, set, path, ctx, depth+1)
	}

	switch v := v.(type) {
	case *ssa.Phi:
		for _, e := range v.Edges {
			if why := s.walk(e, leaf, set, path, ctx, depth+1); why != "" {
				return why
			}
		}
		return ""
	case *ssa.Parameter:
		fn := v.Parent()
		idx := slices.Index(fn.Params, v)
		// Followed into a call of this function: the call's own argument.
		if ctx != nil && ctx.fn == fn && idx >= 0 && idx < len(ctx.args) {
			return s.walk(ctx.args[idx], leaf, set, path, ctx.up, depth+1)
		}
		if s.escaped[fn] {
			return fmt.Sprintf("parameter %s of %s, which is used as a value, so its callers are not all known", v.Name(), fn.Name())
		}
		callers := s.callers[fn]
		if len(callers) == 0 {
			return fmt.Sprintf("parameter %s of %s, which no call in the module reaches", v.Name(), fn.Name())
		}
		for _, cc := range callers {
			if idx < 0 || idx >= len(cc.Args) {
				return fmt.Sprintf("parameter %s of %s: a caller passes too few arguments", v.Name(), fn.Name())
			}
			if why := s.walk(cc.Args[idx], leaf, set, path, nil, depth+1); why != "" {
				return why
			}
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
			if why := s.walk(val, leaf, set, path, ctx, depth+1); why != "" {
				return why
			}
		}
		return ""
	case *ssa.Call:
		return s.walkResult(v.Common(), 0, leaf, set, path, ctx, depth)
	case *ssa.Extract:
		call, ok := v.Tuple.(*ssa.Call)
		if !ok {
			return fmt.Sprintf("a result of %T", v.Tuple)
		}
		return s.walkResult(call.Common(), v.Index, leaf, set, path, ctx, depth)
	}
	return fmt.Sprintf("%s (%T) is not a constant, a parameter or the result of a call in the module", v.Name(), v)
}

// walkResult follows result idx of a static call to what its function returns
// for the arguments of that call.
func (s *scanner) walkResult(cc *ssa.CallCommon, idx int, leaf leafFn, set map[string]bool, path map[key]bool, ctx *frame, depth int) string {
	callee := cc.StaticCallee()
	if callee == nil {
		return fmt.Sprintf("the result of a call through %s, which is not a static call", callTarget(cc))
	}
	if len(callee.Blocks) == 0 {
		return fmt.Sprintf("the result of %s, which has no body in the module", callee.Name())
	}
	in := &frame{fn: callee, args: cc.Args, up: ctx}
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
		if why := s.walk(ret.Results[idx], leaf, set, path, in, depth+1); why != "" {
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
