// Package scan reads the code side of the drift guard (#239): the reason
// constants of api/v1alpha1, the code's Degraded ranking and every emit site
// of the module's non-test code.
//
// Emit sites are found by reference. go/packages type-checks the module, go/ssa
// builds its functions, and each call to a recorder is resolved to the
// constants that reach its reason, type and regarding arguments, through
// variables, parameters and results. The scan fails closed: a package that does
// not type-check, a call to a method named Event or Eventf that is neither a
// known sink nor a named wrapper, and an argument that does not resolve to
// constants are all reported, never skipped.
package scan

import (
	"errors"
	"fmt"
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/reasondrift"
)

// Options say what to scan.
type Options struct {
	// Root is the repository root, which holds go.mod.
	Root string
	// Patterns are the packages to load; the default is ./..., so a new
	// emitter package is covered without a change here.
	Patterns []string
	// Overlay replaces or adds source files, by absolute path (the mutant
	// checks add a file that emits one more reason).
	Overlay map[string][]byte
	// Wrappers are the pass-through wrappers (reasondrift.Wrapper.Func).
	Wrappers []string
	// Require lists package directories that must be loaded with their
	// syntax, so a scan that lost one fails instead of finding fewer sites.
	Require []string
}

// sink is a function or method that raises an event. The indexes count the
// declared parameters, the receiver left out; a negative index means the call
// has no such argument.
type sink struct {
	reason, action, regarding int
	// kinds are the regarding kinds of a sink that takes no regarding
	// argument.
	kinds []string
}

// sinks maps a callee's full name, with {mod} for the module path, to its
// shape. Every recorder of the module is here. A call to any other method
// named Event or Eventf fails the scan.
var sinks = map[string]sink{
	// The lifecycle controller's recorder, which cmd/hpcburst-controller also uses.
	"({mod}/internal/lifecycle.Recorder).Event": {regarding: 0, action: 2, reason: 3},
	// The front's recorder.
	"({mod}/internal/front.Recorder).Event": {regarding: 0, action: 2, reason: 3},
	// client-go's events.k8s.io recorder, called directly.
	"(k8s.io/client-go/tools/internal/events.EventRecorder).Eventf": {regarding: 0, action: 4, reason: 3},
	// The managed catalog controller's events regard its Provider
	// (internal/managed, record).
	"{mod}/pkg/provider.NewManagedEvent": {regarding: -1, action: 0, reason: 1, kinds: []string{"Provider"}},
	// The Provider controller's events go through crossplane-runtime's
	// recorder, which writes no action, and name no regarding kind here.
	"github.com/crossplane/crossplane-runtime/v2/pkg/event.Normal":  {regarding: -1, action: -1, reason: 0},
	"github.com/crossplane/crossplane-runtime/v2/pkg/event.Warning": {regarding: -1, action: -1, reason: 0},
}

// emitMethods are the method names that raise events. A call to one that is no
// sink fails the scan.
var emitMethods = map[string]bool{"Event": true, "Eventf": true}

// maxDepth bounds the walk from an argument to the constants behind it.
const maxDepth = 12

// Scan loads the module and returns what the code does.
func Scan(o Options) (*reasondrift.Code, error) {
	mod, err := modulePath(o.Root)
	if err != nil {
		return nil, err
	}
	patterns := o.Patterns
	if len(patterns) == 0 {
		patterns = []string{"./..."}
	}
	// Dependencies are type-checked from source, not read from export data:
	// x/tools reads the export data of the Go it was released with, and CI's Go
	// (go.mod's) and a developer's can differ. Their function bodies are cut
	// off before checking, since only their declarations matter here.
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedImports | packages.NeedDeps |
			packages.NeedTypes | packages.NeedTypesSizes | packages.NeedSyntax | packages.NeedTypesInfo,
		Dir:     o.Root,
		Env:     append(os.Environ(), "CGO_ENABLED=0"),
		Tests:   false,
		Overlay: o.Overlay,
	}
	pkgs, err := packages.Load(cfg, patterns...)
	if err != nil {
		return nil, fmt.Errorf("loading %v: %w", patterns, err)
	}
	var bad []string
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		for _, e := range p.Errors {
			bad = append(bad, e.Error())
		}
	})
	if len(bad) > 0 {
		sort.Strings(bad)
		if len(bad) > 8 {
			bad = append(bad[:8], fmt.Sprintf("... and %d more", len(bad)-8))
		}
		return nil, fmt.Errorf("the packages do not type-check, so the emit sites cannot be read:\n  %s", strings.Join(bad, "\n  "))
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

	prog, spkgs := ssautil.Packages(pkgs, ssa.InstantiateGenerics)
	prog.Build()
	fns := functions(spkgs)
	s := &scanner{
		mod: mod, root: o.Root, fset: prog.Fset, fns: fns,
		wrappers: map[string]bool{}, seenWrappers: map[string]bool{},
		callers: map[*ssa.Function][]*ssa.CallCommon{}, escaped: map[*ssa.Function]bool{},
	}
	for _, w := range o.Wrappers {
		s.wrappers[w] = true
	}
	s.index()
	if err := s.collect(code); err != nil {
		return nil, err
	}
	for w := range s.seenWrappers {
		code.Wrappers = append(code.Wrappers, w)
	}
	sort.Strings(code.Wrappers)
	sort.Slice(code.Emits, func(i, j int) bool {
		a, b := code.Emits[i], code.Emits[j]
		return a.Pos < b.Pos
	})
	return code, nil
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
						out = append(out, constant.StringVal(tv.Value))
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
