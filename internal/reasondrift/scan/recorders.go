package scan

import (
	"fmt"
	"go/types"
	"strings"

	"golang.org/x/tools/go/ssa"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/reasondrift"
)

// Builder is a method that builds an event recorder: a method called Name of a
// type from one of the packages Pkgs, which are path prefixes ({mod} stands for
// the module path). A method is matched by its name, the package it is declared
// in and the package of the type it is called on, never by its full name:
// controller-runtime's Manager gets GetEventRecorder from the interface
// cluster.Cluster embeds, and the name that go/types gives it depends on which
// of them the call goes through.
type Builder struct {
	Name string
	Pkgs []string
}

// defaultBuilders are the methods that build the recorders of this module
// (#256): a manager's two recorders, through controller-runtime, and the
// recorders of client-go's event broadcasters, events.k8s.io and core/v1.
// client-go declares its recorder in tools/internal/events, behind aliases.
var defaultBuilders = []Builder{
	{Name: "GetEventRecorder", Pkgs: []string{"sigs.k8s.io/controller-runtime"}},
	{Name: "GetEventRecorderFor", Pkgs: []string{"sigs.k8s.io/controller-runtime"}},
	{Name: "NewRecorder", Pkgs: []string{"k8s.io/client-go/tools/events", "k8s.io/client-go/tools/record", "k8s.io/client-go/tools/internal/events"}},
}

// defaultCounters are the functions that wrap a recorder so that it counts
// what it records, as {mod}-templated full names (internal/eventcount).
var defaultCounters = []string{"{mod}/internal/eventcount.Events", "{mod}/internal/eventcount.Legacy"}

// builder reports whether the call is a call of a recorder builder, and names
// the method.
func (s *scanner) builder(cc *ssa.CallCommon) (string, bool) {
	var obj *types.Func
	var recv types.Type
	if cc.IsInvoke() {
		obj, recv = cc.Method, cc.Value.Type()
	} else if f := cc.StaticCallee(); f != nil {
		var ok bool
		if obj, ok = f.Object().(*types.Func); !ok {
			return "", false
		}
		if r := f.Signature.Recv(); r != nil {
			recv = r.Type()
		}
	}
	if obj == nil {
		return "", false
	}
	return s.builderMethod(obj, recv)
}

// builderMethod reports whether a method is a recorder builder: its name is a
// builder's, and it is declared in, or called on a type of, one of the
// builder's packages. recv is the type it is called on, if the caller knows
// it.
func (s *scanner) builderMethod(obj *types.Func, recv types.Type) (string, bool) {
	var paths []string
	if obj.Pkg() != nil {
		paths = append(paths, obj.Pkg().Path())
	}
	for _, t := range []types.Type{recv, ownReceiver(obj)} {
		if p := typePackage(t); p != "" {
			paths = append(paths, p)
		}
	}
	for _, b := range s.builders {
		if obj.Name() != b.Name {
			continue
		}
		for _, p := range b.Pkgs {
			p = strings.ReplaceAll(p, "{mod}", s.mod)
			for _, path := range paths {
				if path == p || strings.HasPrefix(path, p+"/") {
					return obj.FullName(), true
				}
			}
		}
	}
	return "", false
}

// ownReceiver returns the type a method is declared on.
func ownReceiver(obj *types.Func) types.Type {
	if sig, ok := obj.Type().(*types.Signature); ok && sig.Recv() != nil {
		return sig.Recv().Type()
	}
	return nil
}

// typePackage returns the path of the package of a named type, or of the type
// a pointer points to, and "" for any other.
func typePackage(t types.Type) string {
	if t == nil {
		return ""
	}
	t = types.Unalias(t)
	if p, ok := t.(*types.Pointer); ok {
		t = types.Unalias(p.Elem())
	}
	if n, ok := t.(*types.Named); ok && n.Obj().Pkg() != nil {
		return n.Obj().Pkg().Path()
	}
	return ""
}

// isBuilderWrapper reports whether a function the compiler made is the wrapper
// of a recorder builder: the closure of a method value, the thunk of a method
// expression, or the wrapper of a promoted method. Its own call of the builder
// is not a use: what uses the wrapper is.
func (s *scanner) isBuilderWrapper(fn *ssa.Function) bool {
	if fn.Synthetic == "" {
		return false
	}
	obj, ok := fn.Object().(*types.Func)
	if !ok {
		return false
	}
	_, is := s.builderMethod(obj, nil)
	return is
}

// collectRecorders checks that every recorder the module builds is wrapped to
// count its events (#256, HBEP-8 Raised events). The value of a call of a
// builder must be used only as the first argument of a call of a counter
// (eventcount.Events or eventcount.Legacy), through conversions that only
// change its type; a builder taken as a value, started with go or defer, or
// whose value is unused, stored, returned or passed anywhere else is a
// RecorderNotCounted problem. A recorder that is not wrapped raises events that
// the counter does not see, so the check fails closed. It does not read a type
// whose method set holds a promoted builder (a struct that embeds a Manager)
// and is only used through an interface of the module's own.
func (s *scanner) collectRecorders(code *reasondrift.Code) {
	problem := func(fn *ssa.Function, pos ssa.Instruction, detail string) {
		code.Problems = append(code.Problems, reasondrift.Problem{
			Kind: reasondrift.RecorderNotCounted, Package: s.pkgDir(fn), Func: funcName(fn),
			Detail: detail + " (" + s.pos(pos.Pos()) + ")",
		})
	}
	for _, fn := range s.fns {
		if s.isBuilderWrapper(fn) {
			continue
		}
		for _, b := range fn.Blocks {
			for _, in := range b.Instrs {
				ci, isCall := in.(ssa.CallInstruction)
				var cc *ssa.CallCommon
				if isCall {
					cc = ci.Common()
				}
				// A builder used as a value: its wrapper is an operand.
				for _, op := range in.Operands(nil) {
					f, ok := (*op).(*ssa.Function)
					if !ok || !s.isBuilderWrapper(f) {
						continue
					}
					if cc != nil && op == &cc.Value && !cc.IsInvoke() {
						continue // the callee of a static call, which is checked below
					}
					problem(fn, in, fmt.Sprintf("%s is taken as a value, so the recorder it builds cannot be seen to reach eventcount.Events or eventcount.Legacy", f.Object().(*types.Func).FullName()))
					break
				}
				if !isCall {
					continue
				}
				full, ok := s.builder(cc)
				if !ok {
					continue
				}
				call, isValue := ci.(*ssa.Call)
				if !isValue {
					problem(fn, in, fmt.Sprintf("%s is started with go or defer, so the recorder it builds is dropped", full))
					continue
				}
				counter, why := s.countedBy(call, 0)
				if why != "" {
					problem(fn, in, fmt.Sprintf("the recorder %s builds is not wrapped to count its events: %s; its value must be the first argument of eventcount.Events or eventcount.Legacy, and go nowhere else", full, why))
					continue
				}
				code.Recorders = append(code.Recorders, reasondrift.Recorder{
					Package: s.pkgDir(fn), Func: funcName(fn), Pos: s.pos(in.Pos()), Builder: full, Counter: counter,
				})
			}
		}
	}
}

// countedBy follows the value of a recorder through the conversions that only
// change its type, and returns the counter every use of it passes it to as the
// first argument, or why some use does not.
func (s *scanner) countedBy(v ssa.Value, depth int) (counter, why string) {
	refs := v.Referrers()
	if refs == nil || len(*refs) == 0 {
		return "", "its value is not used"
	}
	if depth > maxDepth {
		return "", "its value goes through too many conversions"
	}
	for _, r := range *refs {
		switch r := r.(type) {
		case *ssa.DebugRef:
			continue
		case *ssa.ChangeInterface, *ssa.MakeInterface, *ssa.ChangeType:
			c, w := s.countedBy(r.(ssa.Value), depth+1)
			if w != "" {
				return "", w
			}
			counter = c
		case *ssa.Call:
			cc := r.Common()
			full, _ := calleeName(cc)
			at := -1
			for i, a := range cc.Args {
				if a == v && at < 0 {
					at = i
				} else if a == v {
					return "", fmt.Sprintf("its value is passed to %s at arguments %d and %d", full, at, i)
				}
			}
			switch {
			case cc.IsInvoke() || !s.counters[full]:
				if full == "" {
					full = "a function value"
				}
				return "", fmt.Sprintf("its value is passed to %s, which is not a counter", full)
			case at != 0:
				return "", fmt.Sprintf("its value is passed to %s at argument %d, not 0", full, at)
			}
			counter = full
		default:
			return "", fmt.Sprintf("its value is used by a %s instruction, not passed to a counter", strings.TrimPrefix(fmt.Sprintf("%T", r), "*ssa."))
		}
	}
	return counter, ""
}
