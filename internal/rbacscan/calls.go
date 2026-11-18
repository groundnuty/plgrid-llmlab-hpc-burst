package rbacscan

import (
	"go/token"
	"go/types"
	"slices"
	"strings"

	"golang.org/x/tools/go/ssa"
)

// walkCall follows result idx of a call: through a manager's getters to the manager's identity, through a constructor to
// the identity it is listed under, through a known pass-through to its argument, and through a function of the module
// to what that function returns, for the arguments of that call.
func (x *index) walkCall(call *ssa.Call, idx int, r *result, seen seenSet, ctx *frame) {
	cc := call.Common()
	if cc.IsInvoke() {
		x.walkInvoke(call, cc, r, seen, ctx)
		return
	}
	callee := cc.StaticCallee()
	if callee == nil {
		// A package-level variable that holds a function (controller-runtime's ctrl.NewManager is one): its name is the
		// callee's.
		if g := globalFunc(cc.Value); g != "" {
			x.walkNamed(call, cc, g, idx, r, seen, ctx)
			return
		}
		// A function value: whatever function of that signature was used as a value can be what runs.
		sig, ok := cc.Value.Type().Underlying().(*types.Signature)
		if !ok {
			r.why("the result of a call of %s, which is not a function", cc.Value.Name())
			return
		}
		fns := x.valueFns[sigKey(sig)]
		if len(fns) == 0 {
			r.why("the result of a call through a function value, at %s, whose signature no function used as a value has", x.s.pos(call.Pos()))
			return
		}
		for _, fn := range fns {
			x.walkReturns(fn, idx, cc.Args, r, seen, ctx)
		}
		return
	}
	if len(callee.Blocks) > 0 && inModule(pkgPathOf(callee), x.s.l.mod) {
		x.walkReturns(callee, idx, argsOf(cc), r, seen, ctx)
		return
	}
	if passThroughStatic(callee) {
		x.walk(argsOf(cc)[0], r, seen, ctx)
		return
	}
	x.walkNamed(call, cc, funcID(callee), idx, r, seen, ctx)
}

// walkReturns follows result idx of a function of the module to everything it returns, for the arguments of the call.
func (x *index) walkReturns(fn *ssa.Function, idx int, args []ssa.Value, r *result, seen seenSet, ctx *frame) {
	in := x.in(fn, args, ctx)
	found := false
	for _, b := range fn.Blocks {
		if len(b.Instrs) == 0 {
			continue
		}
		ret, ok := b.Instrs[len(b.Instrs)-1].(*ssa.Return)
		if !ok {
			continue
		}
		found = true
		if idx >= len(ret.Results) {
			r.why("result %d of %s", idx, fn.Name())
			return
		}
		x.walk(ret.Results[idx], r, seen, in)
	}
	if !found {
		r.why("%s never returns", fn.Name())
	}
}

// walkNamed follows a call of a library function by its name: a pass-through to its argument, a constructor to the
// identity it is listed under.
func (x *index) walkNamed(call *ssa.Call, cc *ssa.CallCommon, full string, idx int, r *result, seen seenSet, ctx *frame) {
	if arg, ok := passThrough[full]; ok {
		args := argsOf(cc)
		if arg >= len(args) {
			r.why("%s called with too few arguments", full)
			return
		}
		x.walk(args[arg], r, seen, ctx)
		return
	}
	x.walkNamedConstructor(call, full, idx, r)
}

// walkNamedConstructor follows a call of a library constructor, by its name, to the identity it is listed under.
func (x *index) walkNamedConstructor(call *ssa.Call, full string, idx int, r *result) {
	site := x.s.pos(call.Pos())
	if k, ok := x.s.constructors[full]; ok && idx == k.result {
		ident, kind, why := x.s.constructorIdentity(k, call)
		if why != "" {
			r.why("%s", why)
			return
		}
		r.origins[origin{Ident: ident, Kind: kind, Site: site}] = true
		return
	}
	r.why("the result of %s, which has no body in the module and is not a known pass-through or constructor (%s)", full, site)
}

// globalFunc returns the full name of the package-level variable a function value was loaded from, or "".
func globalFunc(v ssa.Value) string {
	u, ok := v.(*ssa.UnOp)
	if !ok || u.Op != token.MUL {
		return ""
	}
	g, ok := u.X.(*ssa.Global)
	if !ok || g.Pkg == nil || g.Pkg.Pkg == nil {
		return ""
	}
	return g.Pkg.Pkg.Path() + "." + g.Name()
}

// walkInvoke follows the result of a call through an interface.
func (x *index) walkInvoke(call *ssa.Call, cc *ssa.CallCommon, r *result, seen seenSet, ctx *frame) {
	m := cc.Method
	site := x.s.pos(call.Pos())
	pkg := ""
	if m.Pkg() != nil {
		pkg = m.Pkg().Path()
	}
	if inModule(pkg, x.s.l.mod) {
		impls := x.implementers[m]
		if len(impls) == 0 {
			r.why("the result of %s through an interface of the module that no type implements (%s)", m.FullName(), site)
			return
		}
		for _, impl := range impls {
			x.walkReturns(impl, 0, argsOf(cc), r, seen, ctx)
		}
		return
	}
	switch getterKind(m) {
	case getCached, getAPI:
		// The receiver is a manager (or a cluster): the getter makes the client of its identity.
		sub := newResult()
		x.walk(cc.Value, sub, seenSet{}, ctx)
		r.whys = appendUnique(r.whys, sub.whys)
		r.unreached = append(r.unreached, sub.unreached...)
		found := false
		for o := range sub.origins {
			if o.Kind != oManager {
				if o.Kind == oExempt || o.Kind == oModuleImpl {
					r.origins[o] = true
					found = true
					continue
				}
				r.why("the receiver of %s is a %s, not a manager (%s)", m.Name(), o.Kind, site)
				continue
			}
			kind := oCached
			if getterKind(m) == getAPI {
				kind = oAPI
			}
			r.origins[origin{Ident: o.Ident, Kind: kind, Site: site}] = true
			found = true
		}
		if !found && len(sub.whys) == 0 && len(sub.unreached) == 0 {
			r.why("the receiver of %s has no manager behind it (%s)", m.Name(), site)
		}
		return
	}
	if passThroughInvoke(m) {
		x.walk(cc.Value, r, seen, ctx)
		return
	}
	r.why("the result of %s, which is not a known getter or accessor (%s)", m.FullName(), site)
}

func appendUnique(dst, src []string) []string {
	for _, s := range src {
		if !slices.Contains(dst, s) {
			dst = append(dst, s)
		}
	}
	return dst
}

type getter int

const (
	getNone getter = iota
	getCached
	getAPI
)

// getterKind says whether a method is one of a manager's (or a cluster's) client getters.
func getterKind(m *types.Func) getter {
	if m.Pkg() == nil {
		return getNone
	}
	switch m.Pkg().Path() {
	case "sigs.k8s.io/controller-runtime/pkg/cluster", "sigs.k8s.io/controller-runtime/pkg/manager":
	default:
		return getNone
	}
	switch m.Name() {
	case "GetClient", "GetCache", "GetFieldIndexer":
		return getCached
	case "GetAPIReader":
		return getAPI
	}
	return getNone
}

// passThroughInvoke says whether a method of a library interface returns a client derived from its receiver: the typed
// clientset's accessors (AppsV1, ControllerRevisions(ns)), the dynamic client's Resource and Namespace, and the status and
// subresource clients of a controller-runtime client.
func passThroughInvoke(m *types.Func) bool {
	if m.Pkg() == nil {
		return false
	}
	p := m.Pkg().Path()
	switch {
	case strings.HasPrefix(p, "k8s.io/client-go/kubernetes"):
		return true
	case p == "k8s.io/client-go/dynamic":
		return m.Name() == "Resource" || m.Name() == "Namespace"
	case p == "sigs.k8s.io/controller-runtime/pkg/client":
		return m.Name() == "Status" || m.Name() == "SubResource"
	}
	return false
}

// passThrough lists the functions of libraries that return a client derived from an argument (the index counts the receiver
// first), so that the argument is followed in place of the result.
var passThrough = map[string]int{
	"sigs.k8s.io/controller-runtime/pkg/client.WithFieldOwner":                         0,
	"sigs.k8s.io/controller-runtime/pkg/client.NewNamespacedClient":                    0,
	"sigs.k8s.io/controller-runtime/pkg/client.NewDryRunClient":                        0,
	"sigs.k8s.io/controller-runtime/pkg/builder.ControllerManagedBy":                   0,
	"sigs.k8s.io/controller-runtime.NewControllerManagedBy":                            0,
	"k8s.io/client-go/dynamic/dynamicinformer.NewDynamicSharedInformerFactory":         0,
	"k8s.io/client-go/dynamic/dynamicinformer.NewFilteredDynamicSharedInformerFactory": 0,
}

// passThroughStatic says whether a function is a method of a library's concrete client that returns a client derived from
// its receiver: a clientset's group accessors (CoreV1) and a dynamic client's Resource, and a builder's chain.
func passThroughStatic(fn *ssa.Function) bool {
	obj, ok := fn.Object().(*types.Func)
	if !ok || obj.Pkg() == nil {
		return false
	}
	if sig, ok := obj.Type().(*types.Signature); !ok || sig.Recv() == nil {
		return false
	}
	p := obj.Pkg().Path()
	switch {
	case strings.HasPrefix(p, "k8s.io/client-go/kubernetes"):
		return true
	case p == "k8s.io/client-go/dynamic":
		return obj.Name() == "Resource" || obj.Name() == "Namespace"
	case p == "sigs.k8s.io/controller-runtime/pkg/builder":
		switch obj.Name() {
		case "Named", "For", "Owns", "Watches", "WithOptions", "WithEventFilter", "WatchesRawSource", "WatchesMetadata":
			return true
		}
	}
	return false
}

func pkgPathOf(fn *ssa.Function) string {
	if fn.Pkg != nil && fn.Pkg.Pkg != nil {
		return fn.Pkg.Pkg.Path()
	}
	if fn.Object() != nil && fn.Object().Pkg() != nil {
		return fn.Object().Pkg().Path()
	}
	return ""
}

// funcID is a function's full name, with its instantiation removed: the name a table is keyed by.
func funcID(fn *ssa.Function) string {
	if o := fn.Origin(); o != nil {
		fn = o
	}
	if obj, ok := fn.Object().(*types.Func); ok {
		return obj.FullName()
	}
	return fn.String()
}
