package rbacscan

import (
	"fmt"
	"go/token"
	"go/types"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"golang.org/x/tools/go/ssa"
)

// Triple is one thing an identity may do: a verb on a resource of an API group. A resource of a subresource is
// "resource/sub". The group of the core API is "".
type Triple struct {
	Group, Resource, Verb string
}

func (t Triple) String() string {
	g := t.Group
	if g == "" {
		g = "core"
	}
	return fmt.Sprintf("%s %s/%s", t.Verb, g, t.Resource)
}

func lessTriple(a, b Triple) bool { return a.String() < b.String() }

// Exempt is the identity of a client that belongs to no controller: its calls are not checked.
const Exempt = "-"

// Source says which identity a client constructor's calls make a client for: Where is a package directory of the module
// ("internal/lifecycle"), or a function in it ("internal/lifecycle.NewManager", "internal/providerctl.(*Supervisor).start"),
// and the function wins over its package. Why says why, for a reader of the list.
type Source struct {
	Where string
	Ident string
	Why   string
}

// Options say what to scan.
type Options struct {
	// Root is the repository root, which holds go.mod.
	Root string
	// Patterns are the packages to load: the controller commands, whose closure is read.
	Patterns []string
	// Overlay replaces or adds source files by absolute path (the fixture checks use it).
	Overlay map[string][]byte
	// Managers list the ctrl.NewManager calls, by where they are, with the identity each manager acts as.
	Managers []Source
	// Clients list the calls that make a clientset, a dynamic client or a client.New client, with the identity each acts
	// as, or Exempt for one that is no controller's.
	Clients []Source
	// Library lists the calls a library makes on the module's behalf.
	Library []LibraryEntry
	// SkipStores are stores the follow of a client leaves out, at file:line: a fallback only a test takes, such as
	// "if reader == nil { reader = client }", which would give a field that holds the API reader the cached client as well.
	// The scan reads every store into a field, so without the entry the field's calls would need both. An entry that no
	// store matches is a problem: the line moved or the fallback went.
	SkipStores []Skip
	// CRDDir is the directory of the CRDs (config/crd), which says each kind's resource.
	CRDDir string
}

// Skip names a store by position and says why the scan leaves it out.
type Skip struct {
	Pos string
	Why string
}

// Site is one place the code needs rights: a call of a client, the start of an informer, or a call a library entry stands for.
type Site struct {
	Pos   string // file:line
	Func  string
	What  string // what the call is, as a reader sees it
	Ident string
	Need  []Triple
	Via   string // the library entry's ID, for a site that an entry stands for
}

// Problem is something the scan could not read or accept; it fails the check.
type Problem struct {
	Pos string
	Msg string
}

func (p Problem) String() string { return p.Pos + ": " + p.Msg }

// Result is what the scan found.
type Result struct {
	Sites    []Site
	Problems []Problem
	// Entries are the library entries that matched a trigger, by ID, with the triggers' positions.
	Entries map[string][]string
	// Cites are the modules, versions and files the entries cite, for CheckCitations.
	Cites []Cite
	// Dead lists the call sites whose client comes only from parameters of functions that no call in the module reaches:
	// code that is written and not wired, and helpers that only tests call. Their calls are not counted. When something
	// calls such a function, its sites are live and the check reads them; when a live function loses its last caller, the
	// rights it needed are unused rules.
	Dead []string
}

// scanner is one scan's state.
type scanner struct {
	o            Options
	l            *loaded
	x            *index
	res          *Result
	constructors map[string]constructor
	kinds        map[string]kindInfo // "pkgpath.Kind" to its group and resource
	notes        map[string]map[int][]string
	usedNotes    map[string]map[int]bool
	problemSeen  map[string]bool
}

// constructor is a library function that makes a client or a manager: result is the index of what it returns.
type constructor struct {
	result int
	kind   originKind
}

// Scan loads the closure of the patterns and finds every call that needs rights.
func Scan(o Options) (*Result, error) {
	if o.Root == "" || len(o.Patterns) == 0 {
		return nil, fmt.Errorf("Scan needs a root and patterns")
	}
	l, err := load(o.Root, o.Patterns, o.Overlay)
	if err != nil {
		return nil, err
	}
	s := &scanner{o: o, l: l, res: &Result{Entries: map[string][]string{}}, problemSeen: map[string]bool{}}
	s.constructors = map[string]constructor{
		"sigs.k8s.io/controller-runtime.NewManager":      {0, oManager},
		"sigs.k8s.io/controller-runtime/pkg/manager.New": {0, oManager},
	}
	s.kinds, err = s.loadKinds()
	if err != nil {
		return nil, err
	}
	s.readNotes()
	s.usedNotes = map[string]map[int]bool{}
	s.addClientConstructors()
	s.x = &index{s: s}
	s.x.build()
	for _, sk := range o.SkipStores {
		if !s.x.skipped[sk.Pos] {
			s.problem(sk.Pos, "this store is listed in Options.SkipStores, but no store of a client-typed value is there: delete the entry, or move it to the line the store moved to")
		}
	}
	s.collect()
	s.cacheBypass()
	s.collectLibrary()
	s.staleNotes()
	sort.Strings(s.res.Dead)
	sort.Slice(s.res.Sites, func(i, j int) bool {
		a, b := s.res.Sites[i], s.res.Sites[j]
		return a.Pos+a.Ident+a.What < b.Pos+b.Ident+b.What
	})
	sort.Slice(s.res.Problems, func(i, j int) bool { return s.res.Problems[i].String() < s.res.Problems[j].String() })
	return s.res, nil
}

// addClientConstructors lists the constructors of clients that make direct requests.
func (s *scanner) addClientConstructors() {
	for _, f := range []string{
		"k8s.io/client-go/kubernetes.NewForConfig", "k8s.io/client-go/kubernetes.NewForConfigAndClient", "k8s.io/client-go/kubernetes.NewForConfigOrDie",
		"k8s.io/client-go/dynamic.NewForConfig", "k8s.io/client-go/dynamic.NewForConfigAndClient", "k8s.io/client-go/dynamic.NewForConfigOrDie",
		"sigs.k8s.io/controller-runtime/pkg/client.New", "sigs.k8s.io/controller-runtime/pkg/client.NewWithWatch",
	} {
		s.constructors[f] = constructor{0, oDirect}
	}
	for _, p := range s.l.pkgs {
		if strings.HasPrefix(p.PkgPath, "k8s.io/client-go/kubernetes/typed/") {
			for _, n := range []string{"NewForConfig", "NewForConfigAndClient", "NewForConfigOrDie"} {
				s.constructors[p.PkgPath+"."+n] = constructor{0, oDirect}
			}
		}
	}
}

// constructorIdentity says whose a client or manager is, from the function the constructor call is in.
func (s *scanner) constructorIdentity(k constructor, call *ssa.Call) (string, originKind, string) {
	list := s.o.Clients
	what := "client"
	if k.kind == oManager {
		list = s.o.Managers
		what = "manager"
	}
	fn := call.Parent()
	for fn.Parent() != nil {
		fn = fn.Parent()
	}
	name := s.funcName(fn)
	dir := s.pkgDir(fn)
	var pkgHit *Source
	for i := range list {
		src := &list[i]
		if src.Where == name {
			return s.identOf(*src, k)
		}
		if src.Where == dir {
			pkgHit = src
		}
	}
	if pkgHit != nil {
		return s.identOf(*pkgHit, k)
	}
	return "", 0, fmt.Sprintf("a %s is made in %s (%s), which no entry of the scan's list says whose it is", what, name, s.pos(call.Pos()))
}

func (s *scanner) identOf(src Source, k constructor) (string, originKind, string) {
	if src.Ident == Exempt {
		return Exempt, oExempt, ""
	}
	return src.Ident, k.kind, ""
}

// pos is a position as file:line relative to the root.
func (s *scanner) pos(p token.Pos) string {
	if !p.IsValid() {
		return "(no position)"
	}
	pp := s.l.prog.Fset.Position(p)
	rel, err := filepath.Rel(s.l.root, pp.Filename)
	if err != nil || strings.HasPrefix(rel, "..") {
		rel = pp.Filename
	}
	return fmt.Sprintf("%s:%d", filepath.ToSlash(rel), pp.Line)
}

// pkgDir is the directory of a function's package under the module ("internal/lifecycle"), the module's own for the root.
func (s *scanner) pkgDir(fn *ssa.Function) string {
	p := pkgPathOf(fn)
	return strings.TrimPrefix(strings.TrimPrefix(p, s.l.mod), "/")
}

// funcName is a function's name as the scan's lists spell it: "internal/lifecycle.NewManager", or with its receiver,
// "internal/providerctl.(*Supervisor).start".
func (s *scanner) funcName(fn *ssa.Function) string {
	root := fn
	for root.Parent() != nil {
		root = root.Parent()
	}
	dir := s.pkgDir(root)
	name := root.Name()
	if sig := root.Signature; sig != nil && sig.Recv() != nil {
		t := sig.Recv().Type()
		ptr := ""
		if p, ok := t.(*types.Pointer); ok {
			t, ptr = p.Elem(), "*"
		}
		tn := t.String()
		if n, ok := t.(*types.Named); ok {
			tn = n.Obj().Name()
		}
		if ptr != "" {
			name = fmt.Sprintf("(%s%s).%s", ptr, tn, name)
		} else {
			name = fmt.Sprintf("(%s).%s", tn, name)
		}
	}
	if dir == "" {
		return name
	}
	return dir + "." + name
}

func (s *scanner) problem(pos, msg string) {
	k := pos + "|" + msg
	if s.problemSeen[k] {
		return
	}
	s.problemSeen[k] = true
	s.res.Problems = append(s.res.Problems, Problem{Pos: pos, Msg: msg})
}

// siteWithOrigins resolves a receiver and records a site for each identity it acts as.
type siteSpec struct {
	pos  token.Pos
	fn   *ssa.Function
	what string
	// recv is the value whose origin says whose request it is.
	recv ssa.Value
	// allowManager says the receiver may be a manager itself (a builder's, which starts the manager's informer).
	allowManager bool
	// need says what one origin of that value needs: the kind of client decides the verbs of a read.
	need func(o origin) []Triple
}

func (s *scanner) record(spec siteSpec) {
	res := s.x.resolve(spec.recv)
	pos := s.pos(spec.pos)
	fnName := s.funcName(spec.fn)
	for _, w := range res.whys {
		s.problem(pos, fmt.Sprintf("%s: cannot follow its client to the call that made it: %s", spec.what, w))
	}
	origins := res.sorted()
	if len(origins) == 0 && len(res.whys) == 0 {
		if len(res.unreached) > 0 {
			s.res.Dead = append(s.res.Dead, pos+" "+spec.what)
			return
		}
		s.problem(pos, fmt.Sprintf("%s: no call that made its client was found", spec.what))
		return
	}
	byIdent := map[string][]Triple{}
	for _, o := range origins {
		switch o.Kind {
		case oExempt, oModuleImpl:
			continue
		case oManager:
			if !spec.allowManager {
				s.problem(pos, fmt.Sprintf("%s: its client is a manager itself, not a client of one", spec.what))
				continue
			}
		}
		byIdent[o.Ident] = append(byIdent[o.Ident], spec.need(o)...)
	}
	for ident, need := range byIdent {
		s.res.Sites = append(s.res.Sites, Site{Pos: pos, Func: fnName, What: spec.what, Ident: ident, Need: dedupe(need)})
	}
}

func dedupe(ts []Triple) []Triple {
	out := slices.Clone(ts)
	sort.Slice(out, func(i, j int) bool { return lessTriple(out[i], out[j]) })
	return slices.Compact(out)
}

// readNeed is what one read of a resource needs from one kind of client.
func readNeed(o origin, verb string, res Triple) []Triple {
	if o.Kind == oCached {
		return []Triple{{res.Group, res.Resource, "list"}, {res.Group, res.Resource, "watch"}}
	}
	return []Triple{{res.Group, res.Resource, verb}}
}

// collect finds the call sites of the module's functions.
func (s *scanner) collect() {
	for _, fn := range s.l.fns {
		if fn.Synthetic != "" && fn.Synthetic != "package initializer" {
			continue // a wrapper the compiler made for a promoted method or a method value: the call it forwards is a site of its own
		}
		for _, b := range fn.Blocks {
			for _, in := range b.Instrs {
				s.methodValues(fn, in)
				ci, ok := in.(ssa.CallInstruction)
				if !ok {
					continue
				}
				s.visitCall(fn, ci)
			}
		}
	}
}

// siteKey names a method by the package and interface or type it is declared in, as the scan's tables do.
func recvName(m *types.Func) string {
	sig, ok := m.Type().(*types.Signature)
	if !ok || sig.Recv() == nil {
		return ""
	}
	t := sig.Recv().Type()
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	if n, ok := t.(*types.Named); ok {
		return n.Obj().Name()
	}
	return ""
}

// enclosingFn is the declared function a closure is in.
func rootFn(fn *ssa.Function) *ssa.Function {
	for fn.Parent() != nil {
		fn = fn.Parent()
	}
	return fn
}
