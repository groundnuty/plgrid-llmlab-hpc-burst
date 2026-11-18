package rbacscan

import (
	"fmt"
	"go/token"
	"go/types"
	"slices"

	"golang.org/x/tools/go/ssa"
)

func (x *index) walk(v ssa.Value, r *result, seen seenSet, ctx *frame) {
	k := seenKey{v, ctx}
	if seen[k] {
		return
	}
	seen[k] = true
	if x.trace != nil {
		x.trace(v)
	}
	if r.leaf != nil && r.leaf(v) {
		return
	}
	switch v := v.(type) {
	case *ssa.Const:
		if v.Value != nil {
			r.why("the constant %s", v.Name())
		}
		// a nil client: no origin
	case *ssa.Phi:
		for _, e := range v.Edges {
			x.walk(e, r, seen, ctx)
		}
	case *ssa.ChangeInterface:
		x.walk(v.X, r, seen, ctx)
	case *ssa.ChangeType:
		x.walk(v.X, r, seen, ctx)
	case *ssa.Convert:
		x.walk(v.X, r, seen, ctx)
	case *ssa.TypeAssert:
		x.walk(v.X, r, seen, ctx)
	case *ssa.MakeInterface:
		x.walkWrapped(v, r, seen, ctx)
	case *ssa.Extract:
		switch t := v.Tuple.(type) {
		case *ssa.Call:
			x.walkCall(t, v.Index, r, seen, ctx)
		case *ssa.TypeAssert: // v, ok := x.(T)
			if v.Index == 0 {
				x.walk(t.X, r, seen, ctx)
			}
		default:
			r.why("result %d of %T", v.Index, v.Tuple)
		}
	case *ssa.Call:
		x.walkCall(v, 0, r, seen, ctx)
	case *ssa.Parameter:
		x.walkParam(v, r, seen, ctx)
	case *ssa.UnOp:
		if v.Op != token.MUL {
			r.why("%s", v.String())
			return
		}
		x.walkLoad(v.X, r, seen, ctx)
	case *ssa.Field:
		x.walkStores(x.fieldStores[keyOf(v.X.Type(), v.Field)], r, seen, nil)
	default:
		r.why("%s (%T) is not a call, a field, a parameter or a variable", v.Name(), v)
	}
}

// walkStores follows the values stored into a field or a variable, each in the context it is stored in.
func (x *index) walkStores(vals []ssa.Value, r *result, seen seenSet, ctx *frame) {
	for _, val := range vals {
		x.walk(val, r, seen, ctx)
	}
}

// walkLoad follows a load through the address it reads.
func (x *index) walkLoad(addr ssa.Value, r *result, seen seenSet, ctx *frame) {
	switch a := addr.(type) {
	case *ssa.FieldAddr:
		// A field of an object this function made (a composite literal still being built) is what was stored into that
		// object in this function; a field of any other is what was stored into the field anywhere.
		if alloc, ok := a.X.(*ssa.Alloc); ok {
			if vals, ok := allocFieldStores(alloc, a.Field); ok {
				x.walkStores(vals, r, seen, ctx)
				return
			}
		}
		x.walkStores(x.fieldStores[keyOf(a.X.Type(), a.Field)], r, seen, nil)
	case *ssa.Global:
		x.walkStores(x.globalStores[a], r, seen, nil)
	case *ssa.Alloc:
		vals, why := localStores(a)
		if why != "" {
			r.why("%s", why)
			return
		}
		x.walkStores(vals, r, seen, ctx)
	case *ssa.FreeVar:
		// A variable a closure captured by reference: what is stored into it is stored where it was made, and in the closures.
		fn := a.Parent()
		idx := slices.Index(fn.FreeVars, a)
		mcs := x.closures[fn]
		if len(mcs) == 0 || idx < 0 {
			r.why("the captured variable %s of %s, whose closure is not made in the module", a.Name(), x.s.funcName(fn))
			return
		}
		for _, mc := range mcs {
			if idx >= len(mc.Bindings) {
				r.why("the captured variable %s of %s", a.Name(), x.s.funcName(fn))
				return
			}
			x.walkLoad(mc.Bindings[idx], r, seen, nil)
		}
	default:
		r.why("a load through %T", addr)
	}
}

// allocFieldStores returns the values stored into field idx of a local object, when the object's address is used only to
// store into and load from its fields and to be wrapped or returned (a composite literal still being built): false when it
// is passed on or stored away, so that a store through another pointer would be missed, and the field is then followed
// to everything stored into it anywhere.
func allocFieldStores(alloc *ssa.Alloc, idx int) ([]ssa.Value, bool) {
	refs := alloc.Referrers()
	if refs == nil {
		return nil, false
	}
	var vals []ssa.Value
	for _, r := range *refs {
		switch r := r.(type) {
		case *ssa.FieldAddr:
			frefs := r.Referrers()
			if frefs == nil {
				return nil, false
			}
			for _, fr := range *frefs {
				switch fr := fr.(type) {
				case *ssa.Store:
					if fr.Addr != ssa.Value(r) {
						return nil, false
					}
					if r.Field == idx {
						vals = append(vals, fr.Val)
					}
				case *ssa.UnOp, *ssa.DebugRef:
				default:
					return nil, false // the field's address is passed on
				}
			}
		case *ssa.MakeInterface, *ssa.DebugRef, *ssa.UnOp, *ssa.Return, *ssa.Phi, *ssa.ChangeInterface, *ssa.ChangeType:
		default:
			return nil, false
		}
	}
	return vals, len(vals) > 0
}

// localStores returns the values stored into a local variable (an Alloc, or a FreeVar of a closure that captured one). It
// says why it cannot tell when the variable's address is used for anything but loads and stores.
func localStores(addr ssa.Value) ([]ssa.Value, string) {
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
		case *ssa.UnOp:
			if r.Op != token.MUL {
				return nil, fmt.Sprintf("a variable is used by %s", r)
			}
		case *ssa.DebugRef:
		case *ssa.FieldAddr: // a struct variable: its fields are followed by their own stores
		case *ssa.MakeClosure:
			fn, ok := r.Fn.(*ssa.Function)
			if !ok {
				return nil, "a variable is captured by a closure that is not a function"
			}
			for i, b := range r.Bindings {
				if b != addr {
					continue
				}
				inner, why := localStores(fn.FreeVars[i])
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

// argsOf is a call's arguments with the receiver first, as the callee's parameters are.
func argsOf(cc *ssa.CallCommon) []ssa.Value {
	if cc.IsInvoke() {
		return append([]ssa.Value{cc.Value}, cc.Args...)
	}
	return cc.Args
}

// walkParam follows a parameter to the argument of the call being followed, or else to the arguments every caller passes.
func (x *index) walkParam(p *ssa.Parameter, r *result, seen seenSet, ctx *frame) {
	fn := p.Parent()
	idx := slices.Index(fn.Params, p)
	if ctx != nil && ctx.fn == fn && idx >= 0 && idx < len(ctx.args) {
		x.walk(ctx.args[idx], r, seen, ctx.up)
		return
	}
	callers := slices.Clone(x.callers[fn])
	if x.escaped[fn] {
		if sig := fn.Signature; sig != nil {
			callers = append(callers, x.dynamic[sigKey(sig)]...)
		}
	}
	if len(callers) == 0 {
		r.unreached = append(r.unreached, x.s.funcName(fn))
		return
	}
	for _, cc := range callers {
		args := argsOf(cc)
		if idx < 0 || idx >= len(args) {
			r.why("parameter %s of %s: a caller passes too few arguments", p.Name(), x.s.funcName(fn))
			return
		}
		x.walk(args[idx], r, seen, nil)
	}
}

// walkWrapped follows an interface made of a concrete value. A type of the module with a field of the interface's own
// type (an embedded Manager, a wrapped Reader) is followed through that field, in the object this function made when it
// made it here; a type of the module with none runs its own code, whose calls are sites of their own.
func (x *index) walkWrapped(mi *ssa.MakeInterface, r *result, seen seenSet, ctx *frame) {
	t := mi.X.Type()
	if p, ok := t.Underlying().(*types.Pointer); ok {
		t = p.Elem()
	}
	named, ok := t.(*types.Named)
	if !ok || named.Obj().Pkg() == nil || !inModule(named.Obj().Pkg().Path(), x.s.l.mod) {
		// a library type, or a basic one: it was made by a call walkCall already followed to its source, or it is no client.
		x.walk(mi.X, r, seen, ctx)
		return
	}
	st, ok := named.Underlying().(*types.Struct)
	if !ok {
		r.origins[origin{Kind: oModuleImpl, Site: named.Obj().Name()}] = true
		return
	}
	found := false
	for i := 0; i < st.NumFields(); i++ {
		if !types.Identical(st.Field(i).Type(), mi.Type()) {
			continue
		}
		found = true
		if alloc, ok := mi.X.(*ssa.Alloc); ok {
			if vals, ok := allocFieldStores(alloc, i); ok {
				x.walkStores(vals, r, seen, ctx)
				continue
			}
		}
		x.walkStores(x.fieldStores[keyOf(named, i)], r, seen, nil)
	}
	if !found {
		r.origins[origin{Kind: oModuleImpl, Site: named.Obj().Name()}] = true
	}
}

// objectTypes follows an interface value (the object a client call passes) to the concrete types converted into it: the
// kinds of the watch list a loop registers, the object a helper is called with. It reports false when a leaf is not a
// conversion of a value of a named type, so that a type that cannot be told is never left out.
func (x *index) objectTypes(v ssa.Value) ([]types.Type, bool) {
	r := newResult()
	var ts []types.Type
	bad := false
	r.leaf = func(v ssa.Value) bool {
		mi, ok := v.(*ssa.MakeInterface)
		if !ok {
			return false
		}
		t := mi.X.Type()
		if p, ok := t.Underlying().(*types.Pointer); ok {
			t = p.Elem()
		}
		if _, ok := t.(*types.Named); !ok {
			bad = true
			return true
		}
		for _, have := range ts {
			if types.Identical(have, t) {
				return true
			}
		}
		ts = append(ts, t)
		return true
	}
	x.walk(v, r, seenSet{}, nil)
	if bad || len(r.whys) > 0 || len(ts) == 0 {
		return nil, false
	}
	return ts, true
}
