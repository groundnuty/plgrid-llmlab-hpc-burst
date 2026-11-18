package rbacscan

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
)

// loaded is the type-checked module code the scan reads: the packages the patterns reach and the SSA form of the module's own.
type loaded struct {
	mod    string // the module path
	root   string // the repository root
	pkgs   []*packages.Package
	byPath map[string]*packages.Package
	prog   *ssa.Program
	fns    []*ssa.Function // the module's functions with a body, sorted
}

// load type-checks the packages the patterns name and everything they import, from source, and builds the SSA form of the
// module's own packages. A package that does not type-check is an error: a partial check drops uses silently, and a scan
// that sees fewer calls than there are would pass.
func load(root string, patterns []string, overlay map[string][]byte) (*loaded, error) {
	mod, err := modulePath(root)
	if err != nil {
		return nil, err
	}
	// Dependencies are type-checked from source, not read from export data (x/tools reads the export data of the Go it was
	// released with, and CI's Go and a developer's can differ). Their function bodies are cut off first: only their
	// declarations matter here, and the bodies are most of what type-checking holds in memory.
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedImports | packages.NeedDeps |
			packages.NeedTypes | packages.NeedTypesSizes | packages.NeedSyntax | packages.NeedTypesInfo | packages.NeedModule,
		Dir:       root,
		Env:       append(os.Environ(), "CGO_ENABLED=0"),
		Tests:     false,
		Overlay:   overlay,
		ParseFile: parseFile(root),
	}
	roots, err := packages.Load(cfg, patterns...)
	if err != nil {
		return nil, fmt.Errorf("loading %v: %w", patterns, err)
	}
	var bad []string
	packages.Visit(roots, nil, func(p *packages.Package) {
		for _, e := range p.Errors {
			// A dependency whose function bodies were cut off imports packages only its bodies used.
			if !inModule(p.PkgPath, mod) && strings.HasSuffix(e.Msg, "and not used") && strings.Contains(e.Msg, "imported") {
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
		return nil, fmt.Errorf("the packages do not type-check, so the calls cannot be read:\n  %s", strings.Join(bad, "\n  "))
	}
	l := &loaded{mod: mod, root: root, byPath: map[string]*packages.Package{}}
	packages.Visit(roots, nil, func(p *packages.Package) {
		l.pkgs = append(l.pkgs, p)
		l.byPath[p.PkgPath] = p
	})
	var spkgs []*ssa.Package
	l.prog, spkgs = buildSSA(l.pkgs, mod)
	l.fns = functions(spkgs)
	return l, nil
}

func inModule(path, mod string) bool { return path == mod || strings.HasPrefix(path, mod+"/") }

// modulePath reads the module path from root's go.mod.
func modulePath(root string) (string, error) {
	b, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(b), "\n") {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), "module "); ok {
			return strings.Trim(strings.TrimSpace(rest), `"`), nil
		}
	}
	return "", fmt.Errorf("no module line in %s/go.mod", root)
}

// parseFile parses one file. In the files outside root, the dependencies, it cuts the function bodies off; an init function
// keeps an empty body, since it must have one, and a generic function keeps its own, which go/types insists on.
func parseFile(root string) func(*token.FileSet, string, []byte) (*ast.File, error) {
	return func(fset *token.FileSet, filename string, src []byte) (*ast.File, error) {
		f, err := parser.ParseFile(fset, filename, src, parser.SkipObjectResolution|parser.ParseComments)
		if err != nil {
			return f, err
		}
		if strings.HasPrefix(filename, root+string(filepath.Separator)) {
			return f, nil
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || isGeneric(fd) {
				continue
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

// isGeneric reports whether a function, or a method of a generic type, is generic.
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

// buildSSA builds the SSA form of the module's packages, with their syntax, and creates the others from their types alone,
// so that a call into a dependency is a call of a function with no body. ssautil.Packages would skip a package that
// imports one with a type error, and a dependency whose bodies were cut off has the unused-import errors load lets pass.
func buildSSA(pkgs []*packages.Package, mod string) (*ssa.Program, []*ssa.Package) {
	var prog *ssa.Program
	for _, p := range pkgs {
		if p.Fset != nil {
			prog = ssa.NewProgram(p.Fset, ssa.InstantiateGenerics)
			break
		}
	}
	if prog == nil {
		return nil, nil
	}
	var spkgs []*ssa.Package
	// pkgs is in dependency order (packages.Visit's post-order), so each package's imports exist before it.
	for _, p := range pkgs {
		if p.Types == nil || p.Fset == nil {
			continue
		}
		var files []*ast.File
		var info *types.Info
		ours := inModule(p.PkgPath, mod)
		if ours {
			files, info = p.Syntax, p.TypesInfo
		}
		sp := prog.CreatePackage(p.Types, files, info, true)
		if ours {
			spkgs = append(spkgs, sp)
		}
	}
	prog.Build()
	return prog, spkgs
}

// functions lists every function with a body of the module's packages: the declared ones, the methods, the closures inside
// them and the wrappers the compiler made for a method value.
func functions(spkgs []*ssa.Package) []*ssa.Function {
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
