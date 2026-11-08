// Package scan reads the code side of the drift guard (#239): the reason
// constants of api/v1alpha1, the code's Degraded ranking and every emit site of
// the module's non-test code.
//
// Emit sites are found by reference. go/packages type-checks the module, go/ssa
// builds its functions, and each call to a recorder is resolved to the
// constants that reach its reason, type and regarding arguments, through
// variables, parameters and results, one call site at a time. The scan fails
// closed: packages that do not type-check and a missing required package are
// errors, and a call to a method named Event, Eventf or AnnotatedEventf that is
// neither a known sink nor a verified wrapper, a wrapper that does not forward,
// a recorder method taken as a value, an argument that does not resolve and a
// use of a recorder outside the packages loaded are all reported, never skipped.
//
// A reason must come from a named constant. To tell that from a string literal
// after go/ssa has folded both into the same constant, the loader gives every
// string literal of the module's non-test code (outside constant declarations,
// import paths and struct tags) a sentinel suffix before type-checking, so the
// value that reaches a recorder says in itself whether a literal fed it.
//
// Events built by a constructor (the Provider controller's through
// crossplane-runtime's event.Normal and event.Warning, the managed catalog
// controller's through provider.NewManagedEvent) raise nothing where they are
// built. They count as emit sites only if the value is shown to reach a
// recorder call, or a listed wrapper at the parameter that carries the event;
// otherwise the reasons are reported as unproven, and a recorder or wrapper
// that receives an event no constructor built is a problem.
package scan

import (
	"errors"
	"fmt"
	"go/ast"
	"go/constant"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/reasondrift"
)

// Options say what to scan.
type Options struct {
	// Root is the repository root, which holds go.mod.
	Root string
	// Overlay replaces or adds source files, by absolute path (the mutant
	// checks add a file that emits one more reason).
	Overlay map[string][]byte
	// Wrappers are the pass-through wrappers (reasondrift.Wrappers).
	Wrappers []reasondrift.Wrapper
	// ConditionSetters are the functions that write a condition, whose second
	// argument is the condition, as {mod}-templated full names; the default is
	// meta.SetStatusCondition.
	ConditionSetters []string
	// Builders are the methods that build an event recorder, which
	// collectRecorders checks (the default is defaultBuilders).
	Builders []Builder
	// Counters are the functions whose first argument a recorder must go to,
	// as {mod}-templated full names (the default is defaultCounters).
	Counters []string
	// Require lists package directories that must be loaded with their
	// syntax, so a scan that lost one fails instead of finding fewer sites.
	Require []string
}

// sink is a method that raises an event. The indexes count the declared
// parameters, the receiver left out.
type sink struct {
	reason, action, regarding int
}

// sinks maps a callee's full name, with {mod} for the module path, to its
// shape. Every recorder that takes a reason and a type is here. A call to any
// other method named Event, Eventf or AnnotatedEventf fails the scan.
var sinks = map[string]sink{
	// The lifecycle controller's recorder, which cmd/hpcburst-controller also uses.
	"({mod}/internal/lifecycle.Recorder).Event": {regarding: 0, action: 2, reason: 3},
	// The front's recorder.
	"({mod}/internal/front.Recorder).Event": {regarding: 0, action: 2, reason: 3},
	// client-go's events.k8s.io recorder, called directly.
	"(k8s.io/client-go/tools/internal/events.EventRecorder).Eventf": {regarding: 0, action: 4, reason: 3},
}

// ctor is a function that builds an event and raises nothing: the event is
// raised when a recorder receives the value. reason and action count the
// declared parameters; action -1 means the event has none (the Provider
// controller's, through crossplane-runtime, are untyped).
type ctor struct{ reason, action int }

// constructors maps a function's full name, with {mod} for the module path, to
// its shape. A call counts as an emit only if the value reaches a recorder.
var constructors = map[string]ctor{
	"github.com/crossplane/crossplane-runtime/v2/pkg/event.Normal":  {reason: 0, action: -1},
	"github.com/crossplane/crossplane-runtime/v2/pkg/event.Warning": {reason: 0, action: -1},
	"{mod}/pkg/provider.NewManagedEvent":                            {reason: 1, action: 0},
}

// eventRecorder is a recorder method that takes a built event: argument event
// is the event, and regarding the object it is about.
type eventRecorder struct{ regarding, event int }

var eventRecorders = map[string]eventRecorder{
	"(github.com/crossplane/crossplane-runtime/v2/pkg/event.Recorder).Event": {regarding: 0, event: 1},
}

// emitMethods are the method names that raise events.
var emitMethods = map[string]bool{"Event": true, "Eventf": true, "AnnotatedEventf": true}

// maxDepth bounds the walk from an argument to the constants behind it.
const maxDepth = 12

// litMark is the suffix the loader gives every string literal of the module's
// code. No reason contains a NUL.
const litMark = "\x00literal\x00"

// Scan loads the module and returns what the code does.
func Scan(o Options) (*reasondrift.Code, error) {
	mod, err := modulePath(o.Root)
	if err != nil {
		return nil, err
	}
	// Every package of the module is read, so a new emitter is found wherever it is.
	patterns := []string{"./..."}
	// Dependencies are type-checked from source, not read from export data:
	// x/tools reads the export data of the Go it was released with, and CI's Go
	// (go.mod's) and a developer's can differ. Their function bodies are cut
	// off before checking, since only their declarations matter here.
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedImports | packages.NeedDeps |
			packages.NeedTypes | packages.NeedTypesSizes | packages.NeedSyntax | packages.NeedTypesInfo,
		Dir:       o.Root,
		Env:       append(os.Environ(), "CGO_ENABLED=0"),
		Tests:     false,
		Overlay:   o.Overlay,
		ParseFile: parseFile(o.Root),
	}
	pkgs, err := packages.Load(cfg, patterns...)
	if err != nil {
		return nil, fmt.Errorf("loading %v: %w", patterns, err)
	}
	var bad []string
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		for _, e := range p.Errors {
			// A dependency whose function bodies were cut off imports packages
			// only its bodies used; that is the one error to expect there.
			if !inModule(p, mod) && strings.HasSuffix(e.Msg, "and not used") && strings.Contains(e.Msg, "imported") {
				continue
			}
			bad = append(bad, e.Error())
		}
	})
	if len(bad) > 0 {
		sort.Strings(bad)
		if len(bad) > 8 {
			bad = append(bad[:8], fmt.Sprintf("... and %d more", len(bad)-8))
		}
		return nil, fmt.Errorf("the packages do not type-check, so the emit sites cannot be read (the scan marks the string literals of the module's code, which a constant expression over a literal can notice):\n  %s", strings.Join(bad, "\n  "))
	}
	byDir := map[string]*packages.Package{}
	for _, p := range pkgs {
		if len(p.Syntax) == 0 {
			continue
		}
		byDir[strings.TrimPrefix(strings.TrimPrefix(p.PkgPath, mod), "/")] = p
	}
	for _, dir := range o.Require {
		if byDir[dir] == nil {
			return nil, fmt.Errorf("package %s was not loaded with its syntax; patterns %v", dir, patterns)
		}
	}

	code := &reasondrift.Code{}
	if code.Constants, err = reasonConstants(byDir["api/v1alpha1"]); err != nil {
		return nil, err
	}
	if code.Ranking, err = degradedOrder(byDir["internal/lifecycle"]); err != nil {
		return nil, err
	}

	prog, spkgs := buildSSA(pkgs)
	s := &scanner{
		mod: mod, root: o.Root, fset: prog.Fset, fns: functions(prog, spkgs),
		wrappers: map[string]reasondrift.Wrapper{}, seenWrappers: map[string]bool{},
		callers: map[*ssa.Function][]*ssa.CallCommon{}, escaped: map[*ssa.Function]bool{},
		callees: map[*ssa.Function][]*ssa.Function{},
	}
	for _, w := range o.Wrappers {
		s.wrappers[w.Func] = w
	}
	s.index()
	s.collect(code)
	s.builders = o.Builders
	if len(s.builders) == 0 {
		s.builders = defaultBuilders
	}
	s.counters = map[string]bool{}
	counters := o.Counters
	if len(counters) == 0 {
		counters = defaultCounters
	}
	for _, c := range counters {
		s.counters[strings.ReplaceAll(c, "{mod}", mod)] = true
	}
	s.collectRecorders(code)
	setters := o.ConditionSetters
	if len(setters) == 0 {
		setters = defaultSetters
	}
	if err := s.collectManaged(code, byDir["internal/managed"], setters); err != nil {
		return nil, err
	}
	for w := range s.seenWrappers {
		code.Wrappers = append(code.Wrappers, w)
	}
	sort.Strings(code.Wrappers)
	sort.Slice(code.Emits, func(i, j int) bool { return code.Emits[i].Pos < code.Emits[j].Pos })
	sort.SliceStable(code.Problems, func(i, j int) bool {
		a, b := code.Problems[i], code.Problems[j]
		return a.Kind+a.Package+a.Func+a.Detail < b.Kind+b.Package+b.Func+b.Detail
	})
	return code, nil
}

// inModule reports whether a package belongs to the module being scanned.
func inModule(p *packages.Package, mod string) bool {
	return p.PkgPath == mod || strings.HasPrefix(p.PkgPath, mod+"/")
}

// parseFile parses a Go file. In the module's own files every string literal
// that is not in a constant declaration, an import or a struct tag gets the
// suffix litMark, so that a value that reaches a recorder says whether a
// literal fed it. In the files outside root, which are the dependencies, it
// cuts the function bodies off: only their declarations matter here, and their
// bodies are most of what type-checking holds in memory. An init function keeps
// an empty body, since it must have one, and a generic function keeps its own.
func parseFile(root string) func(*token.FileSet, string, []byte) (*ast.File, error) {
	return func(fset *token.FileSet, filename string, src []byte) (*ast.File, error) {
		f, err := parser.ParseFile(fset, filename, src, parser.SkipObjectResolution)
		if err != nil {
			return f, err
		}
		if strings.HasPrefix(filename, root+string(filepath.Separator)) {
			markLiterals(f)
			return f, nil
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || isGeneric(fd) {
				continue // a generic function must have its body
			}
			if fd.Recv == nil && fd.Name.Name == "init" {
				fd.Body = &ast.BlockStmt{}
			} else {
				fd.Body = nil
			}
		}
		return f, nil
	}
}

// markLiterals appends litMark to the value of each string literal of f, except
// those in a constant declaration (the declaration of a named constant is where
// a literal belongs), in an import and in a struct tag.
func markLiterals(f *ast.File) {
	tags := map[*ast.BasicLit]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		if fld, ok := n.(*ast.Field); ok && fld.Tag != nil {
			tags[fld.Tag] = true
		}
		return true
	})
	ast.Inspect(f, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.GenDecl:
			if n.Tok == token.CONST || n.Tok == token.IMPORT {
				return false
			}
		case *ast.BasicLit:
			if n.Kind == token.STRING && !tags[n] {
				if v, err := strconv.Unquote(n.Value); err == nil {
					n.Value = strconv.Quote(v + litMark)
				}
			}
		}
		return true
	})
}

// isGeneric reports whether a function, or a method of a generic type, is
// generic: go/types refuses one without a body.
func isGeneric(fd *ast.FuncDecl) bool {
	if fd.Type.TypeParams != nil {
		return true
	}
	if fd.Recv == nil || len(fd.Recv.List) == 0 {
		return false
	}
	t := ast.Unparen(fd.Recv.List[0].Type)
	if star, ok := t.(*ast.StarExpr); ok {
		t = ast.Unparen(star.X)
	}
	switch t.(type) {
	case *ast.IndexExpr, *ast.IndexListExpr:
		return true
	}
	return false
}

// buildSSA builds the SSA form of the loaded root packages. ssautil.Packages
// would skip a package that imports one with a type error, and a dependency
// whose bodies were cut off has the unused-import errors Scan lets pass; so the
// packages are created here, dependencies first, from their types, and only the
// roots with their syntax.
func buildSSA(roots []*packages.Package) (*ssa.Program, []*ssa.Package) {
	isRoot := map[*packages.Package]bool{}
	for _, r := range roots {
		isRoot[r] = true
	}
	var prog *ssa.Program
	if len(roots) > 0 {
		prog = ssa.NewProgram(roots[0].Fset, ssa.InstantiateGenerics)
	}
	var spkgs []*ssa.Package
	packages.Visit(roots, nil, func(p *packages.Package) {
		if p.Types == nil || p.Fset == nil {
			return
		}
		var files []*ast.File
		var info *types.Info
		if isRoot[p] {
			files, info = p.Syntax, p.TypesInfo
		}
		sp := prog.CreatePackage(p.Types, files, info, true)
		if isRoot[p] {
			spkgs = append(spkgs, sp)
		}
	})
	prog.Build()
	return prog, spkgs
}

// modulePath reads the module path from the go.mod under root.
func modulePath(root string) (string, error) {
	b, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		return "", fmt.Errorf("reading go.mod: %w", err)
	}
	for _, line := range strings.Split(string(b), "\n") {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), "module "); ok {
			return strings.TrimSpace(rest), nil
		}
	}
	return "", errors.New("go.mod has no module line")
}

// reasonConstants maps the value of each string constant of api/v1alpha1 named
// Reason... or EventReason... to the constants' names.
func reasonConstants(p *packages.Package) (map[string][]string, error) {
	if p == nil {
		return nil, errors.New("api/v1alpha1 was not loaded")
	}
	out := map[string][]string{}
	scope := p.Types.Scope()
	for _, name := range scope.Names() {
		c, ok := scope.Lookup(name).(*types.Const)
		if !ok || c.Val().Kind() != constant.String {
			continue
		}
		if strings.HasPrefix(name, "Reason") || strings.HasPrefix(name, "EventReason") {
			v := constant.StringVal(c.Val())
			out[v] = append(out[v], name)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("api/v1alpha1 holds no Reason constant: the scan read the wrong package")
	}
	return out, nil
}

// degradedOrder reads the initializer of internal/lifecycle's degradedOrder
// through the type-checker's constant values, so each element is the value of
// the constant it names.
func degradedOrder(p *packages.Package) ([]string, error) {
	if p == nil {
		return nil, errors.New("internal/lifecycle was not loaded")
	}
	for _, f := range p.Syntax {
		for _, d := range f.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok || gd.Tok != token.VAR {
				continue
			}
			for _, spec := range gd.Specs {
				vs := spec.(*ast.ValueSpec)
				for i, name := range vs.Names {
					if name.Name != "degradedOrder" {
						continue
					}
					if i >= len(vs.Values) {
						return nil, errors.New("degradedOrder has no initializer")
					}
					lit, ok := vs.Values[i].(*ast.CompositeLit)
					if !ok {
						return nil, errors.New("degradedOrder is not a composite literal")
					}
					var out []string
					for _, el := range lit.Elts {
						tv, ok := p.TypesInfo.Types[el]
						if !ok || tv.Value == nil || tv.Value.Kind() != constant.String {
							return nil, fmt.Errorf("an element of degradedOrder is not a string constant: %s", p.Fset.Position(el.Pos()))
						}
						// The ranking is compared by its names; a literal in it is no
						// reason's contract, so the mark is only taken off.
						out = append(out, strings.ReplaceAll(constant.StringVal(tv.Value), litMark, ""))
					}
					if len(out) == 0 {
						return nil, errors.New("degradedOrder is empty")
					}
					return out, nil
				}
			}
		}
	}
	return nil, errors.New("internal/lifecycle declares no degradedOrder")
}
