package rbacscan

import (
	"fmt"
	"go/constant"
	"go/types"
	"strings"

	"golang.org/x/tools/go/ssa"
)

const (
	pkgClient  = "sigs.k8s.io/controller-runtime/pkg/client"
	pkgBuilder = "sigs.k8s.io/controller-runtime/pkg/builder"
	pkgSource  = "sigs.k8s.io/controller-runtime/pkg/source"
	pkgCache   = "sigs.k8s.io/controller-runtime/pkg/cache"
	pkgTyped   = "k8s.io/client-go/kubernetes/typed/"
	pkgDynamic = "k8s.io/client-go/dynamic"
	pkgDynInf  = "k8s.io/client-go/dynamic/dynamicinformer"
)

// methodValues reports a client method taken as a value (get := reader.Get): it is called where the scan cannot tell
// which client, so it fails the scan, never passes unread.
func (s *scanner) methodValues(fn *ssa.Function, in ssa.Instruction) {
	for _, op := range in.Operands(nil) {
		f, ok := (*op).(*ssa.Function)
		if !ok || !(strings.Contains(f.Synthetic, "bound method wrapper") || strings.Contains(f.Synthetic, "thunk")) {
			continue
		}
		if m, ok := f.Object().(*types.Func); ok && apiMethod(m) {
			s.problem(s.pos(in.Pos()), fmt.Sprintf("the client method %s is taken as a value: call it directly, so that the scan reads the call", m.Name()))
		}
	}
}

// visitCall recognises a call that needs rights.
func (s *scanner) visitCall(fn *ssa.Function, ci ssa.CallInstruction) {
	cc := ci.Common()
	m := callee(cc)
	if m == nil || m.Pkg() == nil {
		return
	}
	if cc.IsInvoke() && inModule(m.Pkg().Path(), s.l.mod) {
		// A method of an interface the module declares, with the name and signature of one the scan reads (a local
		// "interface { GetInformer(...) }" that a cache satisfies): the call is the library method's.
		if tw := s.libraryTwin(m); tw != nil {
			m = tw
		}
	}
	pkg, name, rn := m.Pkg().Path(), m.Name(), recvName(m)
	args := argsOf(cc)
	switch {
	case pkg == pkgClient && (rn == "Reader" || rn == "Writer" || rn == "SubResourceReader" || rn == "SubResourceWriter"):
		s.clientCall(fn, ci, cc, name, rn)
	case pkg == pkgClient && rn == "FieldIndexer" && name == "IndexField":
		s.informerStart(fn, ci, cc.Value, args[2], "IndexField")
	case pkg == pkgBuilder && (name == "For" || name == "Owns" || name == "Watches") && len(args) >= 2:
		s.informerStart(fn, ci, args[0], args[1], "builder "+name)
	case pkg == pkgSource && name == "Kind" && len(args) >= 2:
		s.informerStart(fn, ci, args[0], args[1], "source.Kind")
	case pkg == pkgCache && (name == "GetInformer") && len(args) >= 3:
		s.informerStart(fn, ci, args[0], args[2], "cache.GetInformer")
	case pkg == pkgCache && name == "GetInformerForKind":
		s.informerStart(fn, ci, args[0], nil, "cache.GetInformerForKind")
	case strings.HasPrefix(pkg, pkgTyped) && strings.HasSuffix(rn, "Interface") && typedVerbs[name] != "":
		s.typedCall(fn, ci, cc, m, rn)
	case pkg == pkgDynamic && rn == "ResourceInterface" && dynVerbs[name] != "":
		s.dynamicCall(fn, ci, cc, name)
	case pkg == pkgDynInf && rn == "DynamicSharedInformerFactory" && name == "ForResource":
		s.dynamicInformer(fn, ci, cc)
	case pkg == "k8s.io/client-go/rest" && rn == "Request" && (name == "Do" || name == "DoRaw" || name == "Stream" || name == "Watch"):
		// A request built by hand names its path in a string: the scan cannot read it, so it must not be used where the
		// identities' code runs.
		if !s.exemptFn(fn) {
			s.problem(s.pos(ci.Pos()), fmt.Sprintf("a REST request (%s) whose path is a string: the scan reads typed, dynamic and controller-runtime clients only", name))
		}
	}
}

// clientCall is a call of a controller-runtime client's reader or writer.
func (s *scanner) clientCall(fn *ssa.Function, ci ssa.CallInstruction, cc *ssa.CallCommon, name, rn string) {
	recv := cc.Value
	sub := ""
	isSub := rn == "SubResourceReader" || rn == "SubResourceWriter"
	if isSub {
		sub, recv = s.subresourceOf(cc.Value, ci)
		if sub == "" && recv == nil {
			return
		}
	}
	var objIdx int
	switch {
	case rn == "Reader" && name == "Get":
		objIdx = 2
	case rn == "Reader" && name == "List":
		objIdx = 1
	case rn == "SubResourceReader" && name == "Get":
		objIdx = 1
	case writeVerbs[name] != "":
		objIdx = 1
	default:
		return
	}
	if objIdx >= len(cc.Args) {
		s.problem(s.pos(ci.Pos()), fmt.Sprintf("a %s call with too few arguments", name))
		return
	}
	obj := cc.Args[objIdx]
	what := fmt.Sprintf("%s %s", name, typeLabel(obj))
	if isSub {
		what = fmt.Sprintf("%s of %s %s", name, sub, typeLabel(obj))
	}
	reads := name == "Get" || name == "List"
	var resources []Triple
	var resolved bool
	s.record(siteSpec{
		pos: ci.Pos(), fn: fn, what: what, recv: recv,
		need: func(o origin) []Triple {
			if !resolved {
				var why string
				resources, why = s.objectResources(obj, ci.Pos())
				if why != "" {
					s.problem(s.pos(ci.Pos()), fmt.Sprintf("%s: %s", what, why))
				}
				resolved = true
			}
			var out []Triple
			for _, r := range resources {
				if sub != "" {
					r.Resource += "/" + sub
				}
				switch {
				case reads && isSub:
					// A subresource has no informer: its read is a request of its own, whichever client makes it.
					out = append(out, Triple{r.Group, r.Resource, strings.ToLower(name)})
				case reads:
					out = append(out, readNeed(o, strings.ToLower(name), r)...)
				default:
					out = append(out, Triple{r.Group, r.Resource, writeVerbs[name]})
				}
			}
			return out
		},
	})
}

// subresourceOf peels Status() or SubResource(name) off the receiver of a subresource client's call.
func (s *scanner) subresourceOf(v ssa.Value, ci ssa.CallInstruction) (string, ssa.Value) {
	c, ok := v.(*ssa.Call)
	if ok && c.Common().IsInvoke() && c.Common().Method.Pkg() != nil && c.Common().Method.Pkg().Path() == pkgClient {
		switch c.Common().Method.Name() {
		case "Status":
			return "status", c.Common().Value
		case "SubResource":
			if len(c.Common().Args) == 1 {
				if k, ok := c.Common().Args[0].(*ssa.Const); ok && k.Value != nil {
					return strings.Trim(k.Value.ExactString(), `"`), c.Common().Value
				}
			}
			s.problem(s.pos(ci.Pos()), "a SubResource client of a name that is not a constant")
			return "", nil
		}
	}
	s.problem(s.pos(ci.Pos()), "a subresource client that was not made by Status() or SubResource(name) in the same function")
	return "", nil
}

func typeLabel(v ssa.Value) string {
	if t := concreteType(v); t != nil {
		if n, ok := t.(*types.Named); ok {
			return n.Obj().Name()
		}
	}
	return v.Type().String()
}

// informerStart is a call that starts an informer for the kind of an object: a builder's For, Owns and Watches, a cache's
// GetInformer, a field index, source.Kind. Its client is a manager (or a cache or an indexer from one).
func (s *scanner) informerStart(fn *ssa.Function, ci ssa.CallInstruction, recv, obj ssa.Value, what string) {
	if obj == nil {
		s.problem(s.pos(ci.Pos()), what+": a kind given by its GVK; annotate the call with //rbac:resources group/resource")
		return
	}
	var resources []Triple
	var resolved bool
	label := what + " " + typeLabel(obj)
	s.record(siteSpec{
		pos: ci.Pos(), fn: fn, what: label, recv: recv, allowManager: true,
		need: func(o origin) []Triple {
			if !resolved {
				var why string
				resources, why = s.objectResources(obj, ci.Pos())
				if why != "" {
					s.problem(s.pos(ci.Pos()), fmt.Sprintf("%s: %s", label, why))
				}
				resolved = true
			}
			var out []Triple
			for _, r := range resources {
				out = append(out, Triple{r.Group, r.Resource, "list"}, Triple{r.Group, r.Resource, "watch"})
			}
			return out
		},
	})
}

// typedCall is a call of a typed clientset's resource client: ControllerRevisions(ns).List and the like.
func (s *scanner) typedCall(fn *ssa.Function, ci ssa.CallInstruction, cc *ssa.CallCommon, m *types.Func, rn string) {
	dir := strings.TrimPrefix(m.Pkg().Path(), pkgTyped)
	group, ok := typedGroups[strings.SplitN(dir, "/", 2)[0]]
	if !ok {
		s.problem(s.pos(ci.Pos()), fmt.Sprintf("a typed client of %s, whose API group the scan does not know", dir))
		return
	}
	res := plural(strings.TrimSuffix(rn, "Interface"))
	sub, ok := s.subresourceArg(ci, cc, typedSub[m.Name()], typedSubArg[m.Name()])
	if !ok {
		return
	}
	if sub != "" {
		res += "/" + sub
	}
	verb := typedVerbs[m.Name()]
	s.record(siteSpec{
		pos: ci.Pos(), fn: fn, what: fmt.Sprintf("%s of %s", m.Name(), res), recv: cc.Value,
		need: func(o origin) []Triple { return []Triple{{group, res, verb}} },
	})
}

// dynamicCall is a call of a dynamic client's resource: its resource is the GVR it was made with, which is not static
// where a controller reaches it, so it is annotated.
func (s *scanner) dynamicCall(fn *ssa.Function, ci ssa.CallInstruction, cc *ssa.CallCommon, name string) {
	sub, ok := s.subresourceArg(ci, cc, dynSub[name], dynSubArg[name])
	if !ok {
		return
	}
	s.annotated(fn, ci, cc.Value, "dynamic "+name, func(r Triple) []Triple {
		if sub != "" {
			r.Resource += "/" + sub
		}
		return []Triple{{r.Group, r.Resource, dynVerbs[name]}}
	})
}

// typedSubArg and dynSubArg say which argument of a call (the receiver left out) holds its variadic subresources: Patch of
// a typed client, and the dynamic client's Create, Update, Delete, Get, Patch and Apply. typedSub and dynSub give the
// subresource a method is made for (UpdateStatus, ApplyStatus, CreateToken).
var (
	typedSubArg = map[string]int{"Patch": 5}
	dynSub      = map[string]string{"UpdateStatus": "status", "ApplyStatus": "status"}
	dynSubArg   = map[string]int{"Create": 3, "Update": 3, "Delete": 3, "Get": 3, "Patch": 5, "Apply": 4}
)

// subresourceArg returns the subresource a call writes or reads: the one its method is made for, or the constant strings
// of its variadic subresources argument (argument arg, when the method has one). False, with a problem raised, when the
// arguments are not constant: a subresource that cannot be read would be taken for the parent's.
func (s *scanner) subresourceArg(ci ssa.CallInstruction, cc *ssa.CallCommon, fixed string, arg int) (string, bool) {
	if arg == 0 || arg >= len(cc.Args) {
		return fixed, true
	}
	subs, ok := constStrings(cc.Args[arg])
	if !ok {
		s.problem(s.pos(ci.Pos()), "the subresources of this call are not constant strings, so the scan cannot tell which resource it writes (a /status write is not the parent's)")
		return "", false
	}
	if len(subs) == 0 {
		return fixed, true
	}
	if fixed != "" {
		subs = append([]string{fixed}, subs...)
	}
	return strings.Join(subs, "/"), true
}

// constStrings reads a variadic []string argument: nil for none, or a slice literal of constants. False when it is built
// any other way.
func constStrings(v ssa.Value) ([]string, bool) {
	if c, ok := v.(*ssa.Const); ok {
		return nil, c.Value == nil // the nil slice of a call with no variadic argument
	}
	sl, ok := v.(*ssa.Slice)
	if !ok {
		return nil, false
	}
	alloc, ok := sl.X.(*ssa.Alloc)
	if !ok {
		return nil, false
	}
	vals, ok := arrayStores(alloc)
	if !ok {
		return nil, false
	}
	var out []string
	for _, val := range vals {
		c, ok := val.(*ssa.Const)
		if !ok || c.Value == nil || c.Value.Kind() != constant.String {
			return nil, false
		}
		out = append(out, constant.StringVal(c.Value))
	}
	return out, true
}

// dynamicInformer is the start of a dynamic informer: a LIST and a WATCH of the GVR given.
func (s *scanner) dynamicInformer(fn *ssa.Function, ci ssa.CallInstruction, cc *ssa.CallCommon) {
	s.annotated(fn, ci, cc.Value, "dynamic informer", func(r Triple) []Triple {
		return []Triple{{r.Group, r.Resource, "list"}, {r.Group, r.Resource, "watch"}}
	})
}

// annotated records a call whose resources only an annotation can give.
func (s *scanner) annotated(fn *ssa.Function, ci ssa.CallInstruction, recv ssa.Value, what string, need func(Triple) []Triple) {
	var resources []Triple
	var resolved bool
	s.record(siteSpec{
		pos: ci.Pos(), fn: fn, what: what, recv: recv,
		need: func(o origin) []Triple {
			if !resolved {
				notes, ok := s.noteFor(ci.Pos())
				if !ok {
					s.problem(s.pos(ci.Pos()), fmt.Sprintf("%s: its resource is not static: annotate the call with //rbac:resources group/resource", what))
				}
				for _, n := range notes {
					t, err := parseResource(n)
					if err != nil {
						s.problem(s.pos(ci.Pos()), err.Error())
						continue
					}
					resources = append(resources, t)
				}
				resolved = true
			}
			var out []Triple
			for _, r := range resources {
				out = append(out, need(r)...)
			}
			return out
		},
	})
}

// exemptFn says whether a function is in a package or is a function that a client list calls no identity's.
func (s *scanner) exemptFn(fn *ssa.Function) bool {
	name, dir := s.funcName(fn), s.pkgDir(fn)
	for _, src := range s.o.Clients {
		if src.Ident == Exempt && (src.Where == name || src.Where == dir) {
			return true
		}
	}
	return false
}

// cacheBypass reports each use of controller-runtime's Cache.DisableFor: a cached client reads the kinds it names straight
// from the API server, a GET or a LIST in place of the informer's list and watch, and the manager that carries the option
// is not told by the option's own value. Nothing in the module sets it, so the scan does not model it; the first use is a
// problem, to be read by whoever adds it.
func (s *scanner) cacheBypass() {
	for _, p := range s.l.pkgs {
		if !inModule(p.PkgPath, s.l.mod) || p.TypesInfo == nil {
			continue
		}
		for id, obj := range p.TypesInfo.Uses {
			v, ok := obj.(*types.Var)
			if !ok || !v.IsField() || v.Name() != "DisableFor" || v.Pkg() == nil || !strings.HasPrefix(v.Pkg().Path(), "sigs.k8s.io/controller-runtime") {
				continue
			}
			s.problem(s.pos(id.Pos()), "Cache.DisableFor makes a cached client read the kinds it names from the API server (a get or a list, not the informer's list and watch), which the scan does not model: "+
				"teach the scan which manager carries it before using it")
		}
	}
}

// twinInterfaces are the library interfaces whose methods the scan reads, by package and name.
var twinInterfaces = []struct{ pkg, name string }{
	{pkgClient, "Reader"}, {pkgClient, "Writer"}, {pkgClient, "SubResourceReader"}, {pkgClient, "SubResourceWriter"},
	{pkgClient, "FieldIndexer"}, {pkgCache, "Informers"},
}

// libraryTwin returns the method of a library interface that a method of an interface the module declares is the twin of:
// the same name and an identical signature. A module interface that a manager's cache or reader satisfies is how a
// controller keeps its dependencies narrow, and the call through it is the library's, with the receiver's identity.
func (s *scanner) libraryTwin(m *types.Func) *types.Func {
	sig, ok := m.Type().(*types.Signature)
	if !ok {
		return nil
	}
	for _, ti := range twinInterfaces {
		p := s.l.byPath[ti.pkg]
		if p == nil || p.Types == nil {
			continue
		}
		tn, ok := p.Types.Scope().Lookup(ti.name).(*types.TypeName)
		if !ok {
			continue
		}
		iface, ok := tn.Type().Underlying().(*types.Interface)
		if !ok {
			continue
		}
		for i := 0; i < iface.NumMethods(); i++ {
			lm := iface.Method(i)
			if lm.Name() != m.Name() {
				continue
			}
			if lsig, ok := lm.Type().(*types.Signature); ok && types.Identical(sig, lsig) {
				return lm
			}
		}
	}
	return nil
}
