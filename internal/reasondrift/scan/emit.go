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

	wrappers     map[string]bool // named in Options
	seenWrappers map[string]bool // met in the code

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

// collect finds the emit sites among the calls of the module's functions.
func (s *scanner) collect(code *reasondrift.Code) error {
	var unknown []string
	for _, fn := range s.fns {
		for _, b := range fn.Blocks {
			for _, in := range b.Instrs {
				ci, ok := in.(ssa.CallInstruction)
				if !ok {
					continue
				}
				cc := ci.Common()
				full, name := calleeName(cc)
				if !emitMethods[name] && !s.isSink(full) {
					continue
				}
				here := s.pkgDir(fn) + "." + funcName(fn)
				if s.wrappers[here] {
					s.seenWrappers[here] = true
					continue
				}
				sk, isSink := s.sink(full)
				if !isSink && fn.Synthetic != "" {
					// A compiler-made wrapper only forwards its arguments to the
					// method it wraps; the calls of the wrapper are what count.
					continue
				}
				if !isSink {
					unknown = append(unknown, fmt.Sprintf("%s calls %s (%s)", here, full, s.pos(ci.Pos())))
					continue
				}
				code.Emits = append(code.Emits, s.emit(fn, ci, cc, sk))
			}
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return fmt.Errorf("a call raises an event through a method the scan does not know; add it to sinks, or name its caller a wrapper:\n  %s",
			strings.Join(unknown, "\n  "))
	}
	return nil
}

// calleeName returns the full name and the bare name of the function a call
// reaches, when it can be named.
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

func (s *scanner) isSink(full string) bool {
	_, ok := s.sink(full)
	return ok
}

// emit resolves one call of a sink to an Emit.
func (s *scanner) emit(fn *ssa.Function, ci ssa.CallInstruction, cc *ssa.CallCommon, sk sink) reasondrift.Emit {
	e := reasondrift.Emit{Package: s.pkgDir(fn), Func: funcName(fn), Pos: s.pos(ci.Pos())}
	off := 0
	if !cc.IsInvoke() {
		if f := cc.StaticCallee(); f != nil && f.Signature.Recv() != nil {
			off = 1
		}
	}
	arg := func(i int) (ssa.Value, bool) {
		if i < 0 {
			return nil, true
		}
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
	reasons, why := s.resolve(reasonV, stringLeaf)
	reasons = dropEmpty(reasons)
	if why == "" && len(reasons) == 0 {
		why = "only empty strings reach it"
	}
	if why != "" {
		return fail("the reason", why)
	}
	e.Reasons = reasons

	if actionV, ok := arg(sk.action); !ok {
		return fail("the type", "the call has too few arguments")
	} else if actionV == nil {
		e.Actions = []string{reasondrift.NoAction}
	} else {
		actions, why := s.resolve(actionV, stringLeaf)
		if actions = dropEmpty(actions); why == "" && len(actions) == 0 {
			why = "only empty strings reach it"
		}
		if why != "" {
			return fail("the type", why)
		}
		e.Actions = actions
	}

	if regV, ok := arg(sk.regarding); !ok {
		return fail("the regarding object", "the call has too few arguments")
	} else if regV == nil {
		e.Kinds = sk.kinds
		if len(e.Kinds) == 0 {
			e.Kinds = []string{reasondrift.NoKind}
		}
	} else {
		kinds, why := s.resolve(regV, kindLeaf)
		if why != "" {
			return fail("the regarding object", why)
		}
		e.Kinds = kinds
	}
	return e
}

func dropEmpty(in []string) []string {
	var out []string
	for _, s := range in {
		if s != "" {
			out = append(out, s)
		}
	}
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
