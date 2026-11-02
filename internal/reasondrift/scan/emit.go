package scan

import (
	"fmt"
	"go/token"
	"go/types"
	"sort"
	"strings"

	"golang.org/x/tools/go/ssa"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/reasondrift"
)

// scanner holds the SSA program and what is known about calls in it.
type scanner struct {
	mod, root string
	fset      *token.FileSet
	fns       []*ssa.Function

	wrappers     map[string]reasondrift.Wrapper // named in Options
	seenWrappers map[string]bool                // met in the code

	// callers lists the static calls of each function of the module, and
	// escaped the functions that are used as a value, which a static caller
	// list does not cover.
	callers map[*ssa.Function][]*ssa.CallCommon
	escaped map[*ssa.Function]bool
}

// functions lists the functions that can hold a call or a use of a function:
// every function of the module's packages, with their methods and function
// literals, and the compiler-made ones that can be reached from them (a
// package's initializer, which runs its variable initializers; the wrapper of
// a method value; the wrappers of promoted methods). A function of another
// package has no body here, as only the module's packages are built from
// syntax.
func functions(prog *ssa.Program, spkgs []*ssa.Package) []*ssa.Function {
	seen := map[*ssa.Function]bool{}
	var out []*ssa.Function
	var add func(f *ssa.Function)
	add = func(f *ssa.Function) {
		if f == nil || seen[f] {
			return
		}
		seen[f] = true
		if len(f.Blocks) > 0 {
			out = append(out, f)
		}
		for _, a := range f.AnonFuncs {
			add(a)
		}
	}
	ours := map[*ssa.Package]bool{}
	for _, p := range spkgs {
		if p == nil {
			continue
		}
		ours[p] = true
		for _, m := range p.Members {
			switch m := m.(type) {
			case *ssa.Function:
				add(m)
			case *ssa.Type:
				for _, t := range []types.Type{m.Type(), types.NewPointer(m.Type())} {
					ms := p.Prog.MethodSets.MethodSet(t)
					for i := 0; i < ms.Len(); i++ {
						if f := p.Prog.MethodValue(ms.At(i)); f != nil && (f.Pkg == p || f.Pkg == nil) {
							add(f)
						}
					}
				}
			}
		}
	}
	// The compiler-made functions an instruction refers to: the wrapper a method
	// value is built from (MakeClosure), whose call of the method no function of
	// the package shows.
	for i := 0; i < len(out); i++ {
		for _, b := range out[i].Blocks {
			for _, in := range b.Instrs {
				for _, op := range in.Operands(nil) {
					if f, ok := (*op).(*ssa.Function); ok && f.Synthetic != "" && (f.Pkg == nil || ours[f.Pkg]) {
						add(f)
					}
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

// index records the static callers of each function and the functions used as
// values.
func (s *scanner) index() {
	for _, fn := range s.fns {
		for _, b := range fn.Blocks {
			for _, in := range b.Instrs {
				var cc *ssa.CallCommon
				if ci, ok := in.(ssa.CallInstruction); ok {
					cc = ci.Common()
					if callee := cc.StaticCallee(); callee != nil {
						s.callers[callee] = append(s.callers[callee], cc)
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
					s.escaped[f] = true
				}
			}
		}
	}
}

// collect finds the emit sites among the calls of the module's functions, and
// reports what it cannot accept as problems.
func (s *scanner) collect(code *reasondrift.Code) {
	problem := func(kind string, fn *ssa.Function, pos token.Pos, detail string) {
		code.Problems = append(code.Problems, reasondrift.Problem{
			Kind: kind, Package: s.pkgDir(fn), Func: funcName(fn), Detail: detail + " (" + s.pos(pos) + ")",
		})
	}
	for _, fn := range s.fns {
		root := fn
		for root.Parent() != nil {
			root = root.Parent()
		}
		for _, b := range fn.Blocks {
			for _, in := range b.Instrs {
				ci, ok := in.(ssa.CallInstruction)
				if !ok {
					continue
				}
				cc := ci.Common()
				full, name := calleeName(cc)
				sk, isSink := s.sink(full)
				if !emitMethods[name] && !isSink {
					continue
				}
				// A call in a listed wrapper is accepted if it passes on what the
				// wrapper's Forwards say. (The compiler's pointer-receiver wrapper
				// of a listed method is no wrapper: it calls one.)
				if w, listed := s.wrappers[s.pkgDir(root)+"."+funcName(root)]; listed && root.Synthetic == "" {
					s.seenWrappers[w.Func] = true
					if why := s.verifyForwards(w, fn, root, cc); why != "" {
						problem(reasondrift.WrapperDoesNotForward, fn, ci.Pos(), w.Func+": "+why)
					}
					continue
				}
				if isSink {
					code.Emits = append(code.Emits, s.emit(fn, ci, cc, sk))
					continue
				}
				if s.callsWrapper(cc) {
					continue
				}
				what := "calls " + full
				if full == "" {
					what = "calls a method named " + name + " through a value"
				}
				problem(reasondrift.UnknownRecorder, fn, ci.Pos(),
					what+"; add it to sinks, or list the function that holds the call as a wrapper")
			}
		}
	}
}

// callsWrapper reports whether a call is a static call of a listed wrapper.
func (s *scanner) callsWrapper(cc *ssa.CallCommon) bool {
	f := cc.StaticCallee()
	if f == nil || f.Synthetic != "" || f.Pkg == nil {
		return false
	}
	_, ok := s.wrappers[s.pkgDir(f)+"."+funcName(f)]
	return ok
}

// receiverOffset is 1 for a static call of a method, whose receiver is the
// first of the call's arguments, and 0 otherwise.
func receiverOffset(cc *ssa.CallCommon) int {
	if !cc.IsInvoke() {
		if f := cc.StaticCallee(); f != nil && f.Signature.Recv() != nil {
			return 1
		}
	}
	return 0
}

// verifyForwards checks one recorder call in a wrapper against its Forwards and
// returns why it does not pass on what they say, or "".
func (s *scanner) verifyForwards(w reasondrift.Wrapper, fn, root *ssa.Function, cc *ssa.CallCommon) string {
	if fn != root {
		return "the recorder call is inside a function literal"
	}
	if len(w.Forwards) == 0 {
		return "the wrapper lists nothing it forwards"
	}
	off := receiverOffset(cc)
	skip := 0
	if root.Signature.Recv() != nil {
		skip = 1
	}
	for _, f := range w.Forwards {
		if off+f.Arg >= len(cc.Args) {
			return fmt.Sprintf("the call has no argument %d", f.Arg)
		}
		arg := unwrap(cc.Args[off+f.Arg])
		base, typeName, field, isField := fieldOf(arg)
		var param *ssa.Parameter
		if f.Of == "" {
			if skip+f.Param >= len(root.Params) {
				return fmt.Sprintf("the wrapper has no parameter %d", f.Param)
			}
			param = root.Params[skip+f.Param]
		}
		switch {
		case f.Of != "":
			if !isField || typeName != f.Of || field != f.Field {
				return fmt.Sprintf("argument %d is not the %s of a %s", f.Arg, f.Field, f.Of)
			}
		case f.Field != "":
			if !isField || field != f.Field || !isParam(base, param) {
				return fmt.Sprintf("argument %d is not the %s of parameter %d", f.Arg, f.Field, f.Param)
			}
		default:
			if !isParam(arg, param) {
				return fmt.Sprintf("argument %d is not parameter %d, unchanged", f.Arg, f.Param)
			}
		}
	}
	return ""
}

// isParam reports whether v is the parameter p, unchanged: p itself, or p read
// back from the slot go/ssa spilled it to because its address is taken (a
// struct parameter whose fields are read is one). A slot that anything else is
// stored into, or whose address goes anywhere but a field read, is not p.
func isParam(v ssa.Value, p *ssa.Parameter) bool {
	v = unwrap(v)
	if v == ssa.Value(p) {
		return true
	}
	switch x := v.(type) {
	case *ssa.UnOp:
		if a, ok := x.X.(*ssa.Alloc); ok && x.Op == token.MUL {
			return spillOf(a, p)
		}
	case *ssa.Alloc:
		return spillOf(x, p)
	}
	return false
}

func spillOf(a *ssa.Alloc, p *ssa.Parameter) bool {
	refs := a.Referrers()
	if refs == nil {
		return false
	}
	stores := 0
	for _, r := range *refs {
		switch r := r.(type) {
		case *ssa.Store:
			if r.Addr != ssa.Value(a) || r.Val != ssa.Value(p) {
				return false
			}
			stores++
		case *ssa.UnOp:
			if r.Op != token.MUL {
				return false
			}
		case *ssa.FieldAddr:
			frefs := r.Referrers()
			if frefs == nil {
				return false
			}
			for _, fr := range *frefs {
				switch fr := fr.(type) {
				case *ssa.UnOp, *ssa.DebugRef:
					_ = fr
				default:
					return false // a store into the field, or its address passed on
				}
			}
		case *ssa.DebugRef:
		default:
			return false
		}
	}
	return stores == 1
}

// unwrap strips the conversions that change a value's type and not the value.
func unwrap(v ssa.Value) ssa.Value {
	for {
		switch x := v.(type) {
		case *ssa.ChangeInterface:
			v = x.X
		case *ssa.MakeInterface:
			v = x.X
		case *ssa.ChangeType:
			v = x.X
		case *ssa.Convert:
			v = x.X
		default:
			return v
		}
	}
}

// fieldOf reads a field selection: the struct value (or pointer) it selects
// from, that struct type's name, and the field's name.
func fieldOf(v ssa.Value) (base ssa.Value, typeName, field string, ok bool) {
	var x ssa.Value
	var idx int
	switch v := v.(type) {
	case *ssa.Field:
		x, idx = v.X, v.Field
	case *ssa.UnOp:
		fa, isAddr := v.X.(*ssa.FieldAddr)
		if v.Op != token.MUL || !isAddr {
			return nil, "", "", false
		}
		x, idx = fa.X, fa.Field
	default:
		return nil, "", "", false
	}
	t := x.Type()
	if p, isPtr := t.Underlying().(*types.Pointer); isPtr {
		t = p.Elem()
	}
	st, isStruct := t.Underlying().(*types.Struct)
	if !isStruct || idx >= st.NumFields() {
		return nil, "", "", false
	}
	if n, isNamed := t.(*types.Named); isNamed {
		typeName = n.Obj().Name()
	}
	return x, typeName, st.Field(idx).Name(), true
}

// calleeName returns the full name and the bare name of the function a call
// reaches, when it can be named. A call through a function value has neither.
func calleeName(cc *ssa.CallCommon) (full, name string) {
	if cc.IsInvoke() {
		return cc.Method.FullName(), cc.Method.Name()
	}
	if f := cc.StaticCallee(); f != nil {
		if obj, ok := f.Object().(*types.Func); ok {
			return obj.FullName(), obj.Name()
		}
	}
	return "", ""
}

// sink looks a callee up in sinks, which key their module path as {mod}.
func (s *scanner) sink(full string) (sink, bool) {
	sk, ok := sinks[strings.ReplaceAll(full, s.mod, "{mod}")]
	return sk, ok
}

// emit resolves one call of a sink to an Emit.
func (s *scanner) emit(fn *ssa.Function, ci ssa.CallInstruction, cc *ssa.CallCommon, sk sink) reasondrift.Emit {
	e := reasondrift.Emit{Package: s.pkgDir(fn), Func: funcName(fn), Pos: s.pos(ci.Pos())}
	off := receiverOffset(cc)
	arg := func(i int) (ssa.Value, bool) {
		if off+i >= len(cc.Args) {
			return nil, false
		}
		return cc.Args[off+i], true
	}
	fail := func(what, why string) reasondrift.Emit {
		e.Unresolved = what + ": " + why
		return e
	}
	reasonV, ok := arg(sk.reason)
	if !ok {
		return fail("the reason", "the call has too few arguments")
	}
	raw, why := s.resolve(reasonV, stringLeaf)
	if why != "" {
		return fail("the reason", why)
	}
	// A value that carries the mark was fed by a string literal on some path.
	seen, litSeen := map[string]bool{}, map[string]bool{}
	for _, r := range raw {
		c := strings.ReplaceAll(r, litMark, "")
		if c == "" {
			continue // the empty string is no reason
		}
		seen[c] = true
		if strings.Contains(r, litMark) {
			litSeen[c] = true
		}
	}
	if len(seen) == 0 {
		return fail("the reason", "only empty strings reach it")
	}
	e.Reasons, e.LiteralReasons = sorted(seen), sorted(litSeen)

	actionV, ok := arg(sk.action)
	if !ok {
		return fail("the type", "the call has too few arguments")
	}
	actions, why := s.resolve(actionV, stringLeaf)
	if why != "" {
		return fail("the type", why)
	}
	seen = map[string]bool{}
	for _, a := range actions {
		if c := strings.ReplaceAll(a, litMark, ""); c != "" {
			seen[c] = true
		}
	}
	if len(seen) == 0 {
		return fail("the type", "only empty strings reach it")
	}
	e.Actions = sorted(seen)

	regV, ok := arg(sk.regarding)
	if !ok {
		return fail("the regarding object", "the call has too few arguments")
	}
	kinds, why := s.resolve(regV, kindLeaf)
	if why != "" {
		return fail("the regarding object", why)
	}
	e.Kinds = kinds
	return e
}

func sorted(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for x := range m {
		out = append(out, x)
	}
	sort.Strings(out)
	return out
}

// pkgDir returns the directory of the function's package, relative to the
// module root.
func (s *scanner) pkgDir(fn *ssa.Function) string {
	if fn.Pkg == nil {
		return "(compiler-made)"
	}
	return strings.TrimPrefix(strings.TrimPrefix(fn.Pkg.Pkg.Path(), s.mod), "/")
}

// funcName names a function as Emit.Func does: the receiver's type, a dot and
// the method, or the function alone; a function literal takes its enclosing
// function's name.
func funcName(fn *ssa.Function) string {
	for fn.Parent() != nil {
		fn = fn.Parent()
	}
	if fn.Synthetic != "" && fn.Signature.Recv() == nil && fn.Pkg == nil {
		return fn.String()
	}
	if recv := fn.Signature.Recv(); recv != nil {
		t := recv.Type()
		if p, ok := t.(*types.Pointer); ok {
			t = p.Elem()
		}
		if n, ok := t.(*types.Named); ok {
			return n.Obj().Name() + "." + fn.Name()
		}
	}
	return fn.Name()
}

// pos formats a position relative to the root.
func (s *scanner) pos(p token.Pos) string {
	pp := s.fset.Position(p)
	return fmt.Sprintf("%s:%d", strings.TrimPrefix(pp.Filename, s.root+"/"), pp.Line)
}
