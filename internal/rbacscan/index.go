package rbacscan

import (
	"fmt"
	"go/token"
	"go/types"
	"slices"
	"sort"

	"golang.org/x/tools/go/ssa"
)

// originKind says what a client value was made by.
type originKind int

const (
	// oManager is a controller-runtime manager (ctrl.NewManager).
	oManager originKind = iota
	// oCached is a manager's cached client, cache or field indexer: a read through it is an informer's LIST and WATCH.
	oCached
	// oAPI is a manager's API reader: a read through it is a GET or a LIST on its own.
	oAPI
	// oDirect is a clientset, a dynamic client or a client.New client: every call is its own request.
	oDirect
	// oExempt is a client that belongs to no controller identity (the launcher's own admin client), or whose calls are the
	// library entries' (the Lease lock): its calls are not checked here.
	oExempt
	// oModuleImpl is a value of a type the module declares, which implements the interface itself: a call on it runs the
	// type's own code, whose calls are sites of their own.
	oModuleImpl
)

func (k originKind) String() string {
	return [...]string{"manager", "cached client", "API reader", "direct client", "exempt client", "module implementation"}[k]
}

// origin is where a client value came from: the identity it acts as and the call that made it.
type origin struct {
	Ident string
	Kind  originKind
	Site  string // file:line of the call that made it
}

// result is what following a value found: its origins, why some part could not be followed, and whether it was followed to
// a function that nothing calls.
type result struct {
	origins   map[origin]bool
	whys      []string
	unreached []string // functions with a parameter on the path that no call reaches
	// leaf, when set, sees each value before it is followed and says whether it answered it (the object-type follow
	// below stops at a conversion to an interface).
	leaf func(v ssa.Value) bool
}

func newResult() *result { return &result{origins: map[origin]bool{}} }

func (r *result) why(format string, args ...any) {
	w := fmt.Sprintf(format, args...)
	if !slices.Contains(r.whys, w) {
		r.whys = append(r.whys, w)
	}
}

func (r *result) sorted() []origin {
	out := make([]origin, 0, len(r.origins))
	for o := range r.origins {
		out = append(out, o)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		return a.Ident+a.Site+a.Kind.String() < b.Ident+b.Site+b.Kind.String()
	})
	return out
}

// fieldKey names a struct field by its struct type and index: every value of the type shares it, so a field is followed
// to everything stored into it anywhere in the module.
type fieldKey struct {
	typ string
	idx int
}

func keyOf(t types.Type, idx int) fieldKey {
	if p, ok := t.Underlying().(*types.Pointer); ok {
		t = p.Elem()
	}
	return fieldKey{types.TypeString(t, nil), idx}
}

// index holds what following a value needs, built once over the module's functions.
type index struct {
	s            *scanner
	callers      map[*ssa.Function][]*ssa.CallCommon // static callers, and those through an interface the function implements
	escaped      map[*ssa.Function]bool              // used as a value, so its callers are not all static
	dynamic      map[string][]*ssa.CallCommon        // calls through a function value, by the signature called
	fieldStores  map[fieldKey][]ssa.Value
	globalStores map[*ssa.Global][]ssa.Value
	closures     map[*ssa.Function][]*ssa.MakeClosure
	implementers map[*types.Func][]*ssa.Function // a module interface's method, to the module's methods that implement it
	valueFns     map[string][]*ssa.Function      // the functions used as values, by signature: what a call through a function value can run
	trace        func(v ssa.Value)               // for the scan's own tests: called at each value followed
	skipped      map[string]bool                 // the SkipStores positions that matched a store
}

func (x *index) build() {
	x.callers = map[*ssa.Function][]*ssa.CallCommon{}
	x.escaped = map[*ssa.Function]bool{}
	x.dynamic = map[string][]*ssa.CallCommon{}
	x.fieldStores = map[fieldKey][]ssa.Value{}
	x.globalStores = map[*ssa.Global][]ssa.Value{}
	x.closures = map[*ssa.Function][]*ssa.MakeClosure{}
	x.implementers = map[*types.Func][]*ssa.Function{}
	x.valueFns = map[string][]*ssa.Function{}
	x.skipped = map[string]bool{}
	skip := map[string]bool{}
	for _, sk := range x.s.o.SkipStores {
		skip[sk.Pos] = true
	}
	x.indexImplementers()
	for _, fn := range x.s.l.fns {
		for _, b := range fn.Blocks {
			for _, in := range b.Instrs {
				var cc *ssa.CallCommon
				if ci, ok := in.(ssa.CallInstruction); ok {
					cc = ci.Common()
					switch {
					case cc.IsInvoke():
						for _, impl := range x.implementers[cc.Method] {
							x.callers[impl] = append(x.callers[impl], cc)
						}
					case cc.StaticCallee() != nil:
						callee := cc.StaticCallee()
						x.callers[callee] = append(x.callers[callee], cc)
					default:
						if sig, ok := cc.Value.Type().Underlying().(*types.Signature); ok {
							k := sigKey(sig)
							x.dynamic[k] = append(x.dynamic[k], cc)
						}
					}
				}
				switch in := in.(type) {
				case *ssa.Store:
					if p := x.s.pos(in.Pos()); skip[p] && types.IsInterface(in.Val.Type()) {
						x.skipped[p] = true
						break
					}
					switch a := in.Addr.(type) {
					case *ssa.FieldAddr:
						k := keyOf(a.X.Type(), a.Field)
						x.fieldStores[k] = append(x.fieldStores[k], in.Val)
					case *ssa.Global:
						x.globalStores[a] = append(x.globalStores[a], in.Val)
					}
				case *ssa.MakeClosure:
					if f, ok := in.Fn.(*ssa.Function); ok {
						x.closures[f] = append(x.closures[f], in)
					}
				}
				for _, op := range in.Operands(nil) {
					f, ok := (*op).(*ssa.Function)
					if !ok {
						continue
					}
					if cc != nil && op == &cc.Value && !cc.IsInvoke() {
						continue // the callee of a static call
					}
					if mc, isClosure := in.(*ssa.MakeClosure); isClosure && calledOnly(mc) {
						continue // a closure that is only ever called
					}
					x.escaped[f] = true
				}
			}
		}
	}
	for f := range x.escaped {
		if len(f.Blocks) > 0 && f.Signature != nil {
			k := sigKey(f.Signature)
			x.valueFns[k] = append(x.valueFns[k], f)
		}
	}
}

// sigKey is a function's signature without its receiver and without parameter names, the key a call through a function
// value is matched to the functions that were used as values by.
func sigKey(sig *types.Signature) string {
	strip := func(t *types.Tuple) *types.Tuple {
		vars := make([]*types.Var, t.Len())
		for i := range vars {
			vars[i] = types.NewVar(token.NoPos, nil, "", t.At(i).Type())
		}
		return types.NewTuple(vars...)
	}
	return types.TypeString(types.NewSignatureType(nil, nil, nil, strip(sig.Params()), strip(sig.Results()), sig.Variadic()), nil)
}

// calledOnly reports whether the only uses of a closure are calls of it.
func calledOnly(mc *ssa.MakeClosure) bool {
	refs := mc.Referrers()
	if refs == nil || len(*refs) == 0 {
		return false
	}
	for _, r := range *refs {
		ci, ok := r.(ssa.CallInstruction)
		if !ok {
			return false
		}
		cc := ci.Common()
		if cc.IsInvoke() || cc.Value != ssa.Value(mc) {
			return false
		}
		for _, a := range cc.Args {
			if a == ssa.Value(mc) {
				return false
			}
		}
	}
	return true
}

// indexImplementers maps each method of an interface the module declares to the module's concrete methods that
// implement it, so that a call through the interface reaches their parameters and results.
func (x *index) indexImplementers() {
	prog := x.s.l.prog
	var ifaces []*types.Named
	var concretes []types.Type
	for _, p := range x.s.l.pkgs {
		if !inModule(p.PkgPath, x.s.l.mod) || p.Types == nil {
			continue
		}
		scope := p.Types.Scope()
		for _, name := range scope.Names() {
			tn, ok := scope.Lookup(name).(*types.TypeName)
			if !ok || tn.IsAlias() {
				continue
			}
			named, ok := tn.Type().(*types.Named)
			if !ok || named.TypeParams().Len() > 0 {
				continue
			}
			if types.IsInterface(named) {
				ifaces = append(ifaces, named)
			} else {
				concretes = append(concretes, named, types.NewPointer(named))
			}
		}
	}
	for _, in := range ifaces {
		iface := in.Underlying().(*types.Interface)
		for _, c := range concretes {
			if !types.Implements(c, iface) {
				continue
			}
			for i := 0; i < iface.NumMethods(); i++ {
				m := iface.Method(i)
				sel := prog.MethodSets.MethodSet(c).Lookup(m.Pkg(), m.Name())
				if sel == nil {
					continue
				}
				if f := prog.MethodValue(sel); f != nil && len(f.Blocks) > 0 {
					x.implementers[m] = append(x.implementers[m], f)
				}
			}
		}
	}
}

// frame is a call being followed into: the callee, the call's arguments (the receiver first) and the frame the call was
// made in. A parameter met while following the callee's results is that call's own argument, not the union of every
// caller's, so that a helper called from two identities (a wrapper of a manager) does not give one the other's.
type frame struct {
	fn    *ssa.Function
	args  []ssa.Value
	up    *frame
	depth int
}

// maxFrames is how many calls deep a result is followed with its arguments; deeper, the arguments are the union of every
// caller's.
const maxFrames = 6

type seenKey struct {
	v   ssa.Value
	ctx *frame
}

type seenSet map[seenKey]bool

func (x *index) in(fn *ssa.Function, args []ssa.Value, ctx *frame) *frame {
	d := 1
	if ctx != nil {
		d = ctx.depth + 1
	}
	if d > maxFrames {
		return nil
	}
	return &frame{fn: fn, args: args, up: ctx, depth: d}
}

// resolve follows v to the calls that made it.
func (x *index) resolve(v ssa.Value) *result {
	r := newResult()
	x.walk(v, r, seenSet{}, nil)
	return r
}
