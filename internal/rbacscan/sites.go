package rbacscan

import (
	"fmt"
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
				if reads {
					out = append(out, readNeed(o, strings.ToLower(name), r)...)
				} else {
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
	if sub := typedSub[m.Name()]; sub != "" {
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
	s.annotated(fn, ci, cc.Value, "dynamic "+name, func(r Triple) []Triple {
		return []Triple{{r.Group, r.Resource, dynVerbs[name]}}
	})
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
