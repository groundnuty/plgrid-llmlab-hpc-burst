package scan

import (
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/reasondrift"
)

// syntheticRoot is a small module with the package names the scan knows:
// testdata/mod, whose lifecycle package has one function for each way a reason
// reaches a Recorder call.
func syntheticRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("testdata/mod")
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// kubeWrapper is the synthetic module's wrapper: KubeRecorder.Event passes its
// regarding object, its code and its type to Eventf as arguments 0, 3 and 4.
var kubeWrapper = reasondrift.Wrapper{
	Func: "internal/lifecycle.KubeRecorder.Event", Why: "forwards to Eventf",
	Forwards: []reasondrift.Forward{{Arg: 0, Param: 0}, {Arg: 3, Param: 3}, {Arg: 4, Param: 2}},
}

// controllerWrapper is the synthetic module's second wrapper: its parameter 1
// carries an event, whose Reason and Action it passes to Eventf.
var controllerWrapper = reasondrift.Wrapper{
	Func: "internal/lifecycle.Controller.record", Why: "writes an event to Eventf",
	Forwards: []reasondrift.Forward{
		{Arg: 0, Param: 0}, {Arg: 3, Param: 1, Field: "Reason", Event: true}, {Arg: 4, Param: 1, Field: "Action", Event: true},
	},
}

// scanSynthetic scans the synthetic module with its wrapper listed, and the
// given files added or replaced under internal/lifecycle.
func scanSynthetic(t *testing.T, files map[string]string, mutate func(*Options)) *reasondrift.Code {
	t.Helper()
	o := Options{
		Root: syntheticRoot(t), Wrappers: []reasondrift.Wrapper{kubeWrapper, controllerWrapper},
		ConditionSetters: []string{"{mod}/internal/managed.SetStatusCondition"},
	}
	if len(files) > 0 {
		o.Overlay = map[string][]byte{}
		for name, src := range files {
			o.Overlay[filepath.Join(syntheticRoot(t), name)] = []byte(src)
		}
	}
	if mutate != nil {
		mutate(&o)
	}
	code, err := Scan(o)
	if err != nil {
		t.Fatal(err)
	}
	return code
}

func emitOf(t *testing.T, code *reasondrift.Code, fn string) reasondrift.Emit {
	t.Helper()
	var found []reasondrift.Emit
	for _, e := range code.Emits {
		if e.Package == "internal/lifecycle" && e.Func == fn {
			found = append(found, e)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%d emit sites in %s, want 1: %+v", len(found), fn, found)
	}
	return found[0]
}

func problemsOf(code *reasondrift.Code, kind string) []reasondrift.Problem {
	var out []reasondrift.Problem
	for _, p := range code.Problems {
		if p.Kind == kind {
			out = append(out, p)
		}
	}
	return out
}

// TestResolvesEachWayAReasonArrives: the expectations are written out from the
// source of testdata/mod/internal/lifecycle/lifecycle.go.
func TestResolvesEachWayAReasonArrives(t *testing.T) {
	code := scanSynthetic(t, nil, nil)
	for _, c := range []struct {
		fn      string
		reasons []string
		action  string
		kinds   []string
		lits    []string // the reasons a string literal fed
	}{
		{"pass.direct", []string{"A"}, "failing", []string{"Instance"}, nil},
		{"pass.local", []string{"Local"}, "failing", []string{"Instance"}, nil},
		{"pass.typed", []string{"Typed"}, "failing", []string{"Instance"}, nil},
		// fail forwards its parameter: the reasons are its callers' arguments.
		{"pass.fail", []string{"B", "D"}, "failing", []string{"Instance"}, nil},
		// A variable set twice, captured by a closure: both values.
		{"pass.variable", []string{"A", "C"}, "degraded", []string{"Instance"}, nil},
		// A closure assigns to the variable: its store counts too.
		{"pass.assignedInClosure", []string{"A", "C"}, "degraded", []string{"Instance"}, nil},
		// The empty string a variable starts from is no reason.
		{"pass.guarded", []string{"C"}, "degraded", []string{"Instance"}, nil},
		{"pass.fromCall", []string{"A", "B"}, "failing", []string{"Instance"}, nil},
		// An interface-typed regarding object: the kinds converted into it.
		{"pass.either", []string{"A"}, "degraded", []string{"Instance", "ModelDeployment"}, nil},
		// A string literal is told from a named constant wherever it comes from:
		// the call, a conversion, a constant expression with a literal in it, a
		// variable it was put in, a function's result, a forwarder's caller.
		{"pass.literal", []string{"Literal"}, "failing", []string{"Instance"}, []string{"Literal"}},
		{"pass.literalConversion", []string{"TypedLiteral"}, "failing", []string{"Instance"}, []string{"TypedLiteral"}},
		{"pass.literalConcat", []string{"LitConcat"}, "failing", []string{"Instance"}, []string{"LitConcat"}},
		{"pass.namedConcat", []string{"XA"}, "failing", []string{"Instance"}, []string{"XA"}},
		{"pass.viaVariable", []string{"ViaVariable"}, "failing", []string{"Instance"}, []string{"ViaVariable"}},
		{"pass.viaReturn", []string{"ViaReturn"}, "failing", []string{"Instance"}, []string{"ViaReturn"}},
		{"pass.failLit", []string{"A", "ForwardedLiteral"}, "failing", []string{"Instance"}, []string{"ForwardedLiteral"}},
		// A helper's result is that of the call, not of every call: ident(A) is
		// discarded and ident(B) is emitted.
		{"pass.discarded", []string{"B"}, "failing", []string{"Instance"}, nil},
		// The same helper reached twice in one walk, through nested calls.
		{"pass.nested", []string{"C"}, "failing", []string{"Instance"}, nil},
	} {
		e := emitOf(t, code, c.fn)
		if e.Unresolved != "" {
			t.Errorf("%s: unresolved: %s", c.fn, e.Unresolved)
			continue
		}
		if !slices.Equal(e.Reasons, c.reasons) || !slices.Equal(e.Actions, []string{c.action}) || !slices.Equal(e.Kinds, c.kinds) {
			t.Errorf("%s: reasons %v actions %v kinds %v, want %v [%s] %v", c.fn, e.Reasons, e.Actions, e.Kinds, c.reasons, c.action, c.kinds)
		}
		if !slices.Equal(e.LiteralReasons, c.lits) {
			t.Errorf("%s: literal reasons %v, want %v", c.fn, e.LiteralReasons, c.lits)
		}
	}
	// What cannot be told is reported, never skipped.
	for fn, why := range map[string]string{
		"pass.field": "reason",
		// A method value is called through a wrapper the compiler makes.
		"pass.viaValue": "used as a value",
		// A function used as a value has callers a static list does not hold,
		// even though it is also called with a constant.
		"valueAndCalled": "used as a value",
		// A method value is a wrapper that calls the method, and a package-level
		// table is built by the package's initializer: both hold uses of a
		// function that the functions of the package do not show.
		"pass.boundAndCalled": "used as a value",
		// A function nothing calls has no caller to take its parameter from.
		"orphan": "no call in the module reaches",
		"tabled": "used as a value",
	} {
		e := emitOf(t, code, fn)
		if e.Unresolved == "" || !strings.Contains(e.Unresolved, why) {
			t.Errorf("%s: unresolved %q, want a message containing %q", fn, e.Unresolved, why)
		}
	}
	// The recorder's method as a value is an emit through a wrapper the
	// compiler makes, so it is reported, not skipped.
	var wrapped []reasondrift.Emit
	for _, e := range code.Emits {
		if e.Package == "(compiler-made)" {
			wrapped = append(wrapped, e)
		}
	}
	if len(wrapped) != 1 || wrapped[0].Unresolved == "" {
		t.Errorf("emits through a compiler-made wrapper: %+v, want one, unresolved", wrapped)
	}
	// The constants are those named Reason... or EventReason..., typed or not.
	wantConsts := map[string][]string{"A": {"ReasonA"}, "B": {"ReasonB"}, "C": {"ReasonC"}, "D": {"EventReasonD"}, "Typed": {"ReasonTyped"}}
	for v, names := range wantConsts {
		if !slices.Equal(code.Constants[v], names) {
			t.Errorf("constants of %q: %v, want %v", v, code.Constants[v], names)
		}
	}
	if _, ok := code.Constants["NotAReason"]; ok || len(code.Constants) != len(wantConsts) {
		t.Errorf("constants %v, want only %v", code.Constants, wantConsts)
	}
	if want := []string{"B", "A"}; !slices.Equal(code.Ranking, want) {
		t.Errorf("ranking %v, want %v", code.Ranking, want)
	}
	// Events built by a constructor count only if the value reaches a recorder
	// or a wrapper that carries it (B6).
	for _, c := range []struct {
		fn      string
		reasons []string
		action  string
		kinds   []string
		lits    []string
	}{
		{"Controller.direct", []string{"A"}, "degraded", []string{"Instance"}, nil},
		{"Controller.viaVariable", []string{"B"}, "failing", []string{"Instance"}, nil},
		{"Controller.literalReason", []string{"CtorLiteral"}, "degraded", []string{"Instance"}, []string{"CtorLiteral"}},
	} {
		e := emitOf(t, code, c.fn)
		if e.Unresolved != "" || e.Unproven != "" {
			t.Errorf("%s: unresolved %q unproven %q", c.fn, e.Unresolved, e.Unproven)
			continue
		}
		if !slices.Equal(e.Reasons, c.reasons) || !slices.Equal(e.Actions, []string{c.action}) || !slices.Equal(e.Kinds, c.kinds) || !slices.Equal(e.LiteralReasons, c.lits) {
			t.Errorf("%s: %v %v %v lits %v, want %v [%s] %v lits %v", c.fn, e.Reasons, e.Actions, e.Kinds, e.LiteralReasons, c.reasons, c.action, c.kinds, c.lits)
		}
	}
	for fn, why := range map[string]string{
		"Controller.discarded":      "not used",                // built and thrown away
		"Controller.stored":         "*ssa.Store",              // handed over through a slice
		"Controller.wrongParameter": "does not carry an event", // passed where no event goes
	} {
		e := emitOf(t, code, fn)
		if e.Unproven == "" || !strings.Contains(e.Unproven, why) || len(e.Reasons) != 1 {
			t.Errorf("%s: unproven %q reasons %v, want an unproven emit that says %q", fn, e.Unproven, e.Reasons, why)
		}
	}
	// A reason that arrives through a caller's argument is read like any other.
	if e := emitOf(t, code, "Controller.unreadReason"); e.Unresolved != "" || !slices.Equal(e.Reasons, []string{"B"}) {
		t.Errorf("Controller.unreadReason: %+v, want reason B", e)
	}
	// Two calls hand a wrapper an event that no constructor built.
	got := problemsOf(code, reasondrift.EventNotConstructed)
	var funcs []string
	for _, p := range got {
		funcs = append(funcs, p.Func)
	}
	if want := []string{"Controller.notBuilt", "Controller.stored", "Controller.wrongParameter"}; !slices.Equal(funcs, want) {
		t.Errorf("event-not-constructed problems in %v, want %v", funcs, want)
	}
	// The synthetic module has no other problem, and its wrappers are met,
	// through the compiler's pointer-receiver wrapper of KubeRecorder.Event too.
	if n := len(code.Problems) - len(got); n != 0 {
		t.Errorf("problems %+v, want only the two above", code.Problems)
	}
	if want := []string{controllerWrapper.Func, kubeWrapper.Func}; !slices.Equal(code.Wrappers, want) {
		t.Errorf("wrappers met %v, want %v", code.Wrappers, want)
	}
}

const unknownRecorder = `package lifecycle

type otherRecorder interface{ Event(string) }

func relay(o otherRecorder, s string) { o.Event(s) }
`

// TestManagedWritesAreCollected: the reasons the synthetic managed controller
// can write into its watched condition are found by reference, through a direct
// write, a closure that is only called and a variable filled field by field.
// The closure's write is an over-approximation: its two calls pass different
// condition types, and the setter call is one, so both reasons are collected.
func TestManagedWritesAreCollected(t *testing.T) {
	code := scanSynthetic(t, nil, nil)
	got := map[string]bool{}
	for _, w := range code.ManagedWrites {
		got[w.Type+"/"+w.Reason] = true
	}
	want := map[string]bool{"Watched/A": true, "Watched/B": true, "Watched/C": true, "Watched/D": true}
	if len(got) != len(want) {
		t.Errorf("managed writes %v, want %v", got, want)
	}
	for k := range want {
		if !got[k] {
			t.Errorf("managed writes %v lack %s", got, k)
		}
	}
	if !slices.Equal(code.WatchedTypes, []string{"Watched"}) {
		t.Errorf("watched types %v, want [Watched]", code.WatchedTypes)
	}
	if len(code.ManagedBad) != 1 || code.ManagedBad["A"] != "failing" {
		t.Errorf("managed bad states %v, want A: failing", code.ManagedBad)
	}
}

const managedWrites = `package managed

import (
	"fmt"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func literalReason(conds *[]Condition) {
	SetStatusCondition(conds, Condition{Type: v1alpha1.ConditionWatched, Reason: "Literal"})
}

func builtReason(conds *[]Condition, n int) {
	SetStatusCondition(conds, Condition{Type: v1alpha1.ConditionWatched, Reason: fmt.Sprintf("R%d", n)})
}

func unknownCondition(conds *[]Condition, c Condition) { SetStatusCondition(conds, c) }

func setCond(conds *[]Condition, c Condition) { SetStatusCondition(conds, c) }

func callSetCond(conds *[]Condition) {
	setCond(conds, Condition{Type: v1alpha1.ConditionWatched, Reason: v1alpha1.ReasonA})
}

var setters = []func(*[]Condition, Condition){setCond}
`

// TestAWriteThatIsNotAConstantIsAProblem: any write that can reach a watched
// condition with a reason that is a literal or built, or with a type or reason
// the scan cannot read, fails closed.
func TestAWriteThatIsNotAConstantIsAProblem(t *testing.T) {
	code := scanSynthetic(t, map[string]string{"internal/managed/zz_overlay.go": managedWrites}, nil)
	byFunc := map[string]string{}
	for _, p := range code.Problems {
		if p.Kind == reasondrift.ManagedNotConstant || p.Kind == reasondrift.ManagedUnresolved {
			byFunc[p.Func] = p.Kind
		}
	}
	want := map[string]string{
		"literalReason":    reasondrift.ManagedNotConstant,
		"builtReason":      reasondrift.ManagedUnresolved,
		"unknownCondition": reasondrift.ManagedUnresolved,
		// Also called with a constant, but used as a value in a table, so a list of
		// its callers is not complete.
		"setCond": reasondrift.ManagedUnresolved,
	}
	if len(byFunc) != len(want) {
		t.Errorf("managed problems %v, want %v", byFunc, want)
	}
	for f, k := range want {
		if byFunc[f] != k {
			t.Errorf("%s: problem %q, want %q (all: %v)", f, byFunc[f], k, byFunc)
		}
	}
}

// TestAnUnknownRecorderIsAProblem: a call to Event that is neither a sink nor
// in a named wrapper is reported, so a new emitter path cannot go unseen. It is
// a problem in the result, not an error, so that one scan can carry several.
func TestAnUnknownRecorderIsAProblem(t *testing.T) {
	code := scanSynthetic(t, map[string]string{"internal/lifecycle/zz_overlay.go": unknownRecorder}, nil)
	got := problemsOf(code, reasondrift.UnknownRecorder)
	if len(got) != 1 || got[0].Func != "relay" || got[0].Package != "internal/lifecycle" {
		t.Fatalf("unknown-recorder problems %+v, want one in internal/lifecycle relay", got)
	}
}

// TestANamedWrapperIsAcceptedOnlyIfItForwards: the same call inside a listed
// wrapper is no problem when it passes on what the wrapper's Forwards say, and
// is one when it does not.
func TestANamedWrapperIsAcceptedOnlyIfItForwards(t *testing.T) {
	relay := reasondrift.Wrapper{Func: "internal/lifecycle.relay", Why: "x", Forwards: []reasondrift.Forward{{Arg: 0, Param: 1}}}
	withRelay := func(o *Options) { o.Wrappers = append(o.Wrappers, relay) }
	code := scanSynthetic(t, map[string]string{"internal/lifecycle/zz_overlay.go": unknownRecorder}, withRelay)
	if got := problemsOf(code, reasondrift.UnknownRecorder); len(got) != 0 {
		t.Errorf("a forwarding wrapper: unknown-recorder problems %+v, want none", got)
	}
	if got := problemsOf(code, reasondrift.WrapperDoesNotForward); len(got) != 0 {
		t.Errorf("a forwarding wrapper: wrapper problems %+v, want none", got)
	}
	if want := []string{controllerWrapper.Func, kubeWrapper.Func, relay.Func}; !slices.Equal(code.Wrappers, want) {
		t.Errorf("wrappers met %v, want %v", code.Wrappers, want)
	}
	// A wrapper that is listed and is not in the code is not reported as met.
	code = scanSynthetic(t, nil, withRelay)
	if want := []string{controllerWrapper.Func, kubeWrapper.Func}; !slices.Equal(code.Wrappers, want) {
		t.Errorf("wrappers met %v, want %v", code.Wrappers, want)
	}
	// The call changes the argument: it is no longer parameter 1, unchanged.
	changed := strings.Replace(unknownRecorder, "o.Event(s)", `o.Event(s + "!")`, 1)
	code = scanSynthetic(t, map[string]string{"internal/lifecycle/zz_overlay.go": changed}, withRelay)
	if got := problemsOf(code, reasondrift.WrapperDoesNotForward); len(got) != 1 || !strings.Contains(got[0].Detail, "not parameter 1") {
		t.Errorf("a wrapper that changes its argument: %+v, want one problem that says so", got)
	}
}

// TestAWrapperThatChangesTheReasonIsAProblem: KubeRecorder.Event passing a
// reason of its own to Eventf is found, though the wrapper is listed.
func TestAWrapperThatChangesTheReasonIsAProblem(t *testing.T) {
	src, err := readFile(filepath.Join(syntheticRoot(t), "internal/lifecycle/kube.go"))
	if err != nil {
		t.Fatal(err)
	}
	for name, mutated := range map[string]string{
		"a reason of its own":     strings.Replace(src, "code, typ, message", `"WrongReason", typ, message`, 1),
		"the type of its own":     strings.Replace(src, "code, typ, message", `code, "other", message`, 1),
		"in a function literal":   strings.Replace(src, "k.R.Eventf(regarding, related, \"Normal\", code, typ, message)", "func() { k.R.Eventf(regarding, related, \"Normal\", code, typ, message) }()", 1),
		"another regarding value": strings.Replace(src, "Eventf(regarding, related,", "Eventf(related, related,", 1),
	} {
		if mutated == src {
			t.Fatalf("%s: the mutation changed nothing", name)
		}
		code := scanSynthetic(t, map[string]string{"internal/lifecycle/kube.go": mutated}, nil)
		if got := problemsOf(code, reasondrift.WrapperDoesNotForward); len(got) != 1 {
			t.Errorf("a wrapper with %s: wrapper problems %+v, want one", name, got)
		}
	}
}

// TestAMethodValueOfAnUnknownRecorderIsAProblem: passing the method of a
// recorder the scan does not know as a value leaves a dynamic call with no name
// and a wrapper the compiler makes; the wrapper's call is reported.
func TestAMethodValueOfAnUnknownRecorderIsAProblem(t *testing.T) {
	src := `package lifecycle

type otherRecorder interface{ Event(string) }

func run(f func(string)) { f("X") }

func useValue(o otherRecorder) { run(o.Event) }
`
	code := scanSynthetic(t, map[string]string{"internal/lifecycle/zz_overlay.go": src}, nil)
	got := problemsOf(code, reasondrift.UnknownRecorder)
	if len(got) != 1 || got[0].Package != "(compiler-made)" {
		t.Fatalf("unknown-recorder problems %+v, want one in a compiler-made function", got)
	}
}

// overlay adds one file to the synthetic lifecycle package.
func overlay(t *testing.T, src string) map[string][]byte {
	t.Helper()
	return map[string][]byte{filepath.Join(syntheticRoot(t), "internal/lifecycle/zz_overlay.go"): []byte(src)}
}

// TestATypeErrorFails: packages that do not type-check fail the scan; they do
// not read as having no emit sites.
func TestATypeErrorFails(t *testing.T) {
	_, err := Scan(Options{Root: syntheticRoot(t), Overlay: overlay(t, "package lifecycle\n\nvar broken int = \"text\"\n")})
	if err == nil || !strings.Contains(err.Error(), "do not type-check") {
		t.Fatalf("error %v, want one that says the packages do not type-check", err)
	}
}

// TestAnUnusedImportInTheModuleFails: the unused-import errors the scan lets
// pass are those of dependencies, whose function bodies it cuts off. The
// module's own packages get no such allowance.
func TestAnUnusedImportInTheModuleFails(t *testing.T) {
	_, err := Scan(Options{Root: syntheticRoot(t), Overlay: overlay(t, "package lifecycle\n\nimport \"fmt\"\n")})
	if err == nil || !strings.Contains(err.Error(), "imported and not used") {
		t.Fatalf("error %v, want one that says an import is not used", err)
	}
}

// TestAMissingRequiredPackageFails: a scan that loaded fewer packages than the
// caller needs fails.
func TestAMissingRequiredPackageFails(t *testing.T) {
	_, err := Scan(Options{Root: syntheticRoot(t), Require: []string{"internal/lifecycle", "internal/front"}})
	if err == nil || !strings.Contains(err.Error(), "internal/front") {
		t.Fatalf("error %v, want one that names internal/front", err)
	}
}

func readFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	return string(b), err
}

// scanRecorders scans the synthetic module with recorder builders named after
// its own package rb, and counters that include two the default list does not
// have, so that the tests reach what the real builders (controller-runtime's
// manager, client-go's broadcasters) and counters (eventcount.Events and
// Legacy) do through the defaults.
func scanRecorders(t *testing.T) *reasondrift.Code {
	t.Helper()
	return scanSynthetic(t, nil, func(o *Options) {
		o.Builders = []Builder{
			{Name: "GetEventRecorder", Pkgs: []string{"{mod}/internal/rb"}},
			{Name: "NewRecorder", Pkgs: []string{"{mod}/internal/rb"}},
		}
		o.Counters = []string{
			"{mod}/internal/eventcount.Events", "{mod}/internal/eventcount.Legacy",
			"{mod}/internal/eventcount.Behind", "{mod}/internal/eventcount.Twice",
		}
	})
}

// TestARecorderMustBeWrappedToCount: the expectations are written out from
// testdata/mod/internal/recorders/recorders.go. A recorder is accepted when its
// value is the first argument of a counter, through a variable and through
// the conversion of a Logger into a Recorder, and refused in every other use.
func TestARecorderMustBeWrappedToCount(t *testing.T) {
	code := scanRecorders(t)
	const pkg = "internal/recorders"
	wrapped := map[string]string{}
	for _, r := range code.Recorders {
		if r.Package != pkg {
			t.Errorf("a recorder outside %s: %+v", pkg, r)
		}
		wrapped[r.Func] = r.Counter
	}
	const events, legacy = "github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount.Events", "github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount.Legacy"
	if want := map[string]string{
		"countedOnManager":               events,
		"countedLegacy":                  legacy,
		"countedThroughVariable":         events,
		"countedThroughMethodExpression": events,
	}; !maps.Equal(wrapped, want) {
		t.Errorf("recorders accepted: %v, want %v", wrapped, want)
	}

	// Each of the others is one problem, in the function that holds it, and
	// says why. The method that is only named (a value, an expression) is a
	// problem where it is named, not in the wrapper the compiler makes.
	refused := map[string]string{
		"inField":        "used by a Store instruction",
		"returned":       "used by a Return instruction",
		"unused":         "its value is not used",
		"goroutine":      "started with go or defer",
		"deferred":       "started with go or defer",
		"passedOn":       "passed to github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/recorders.identity, which is not a counter",
		"countedAndKept": "used by a Return instruction",
		"twice":          "at arguments 0 and 1",
		"behind":         "at argument 1, not 0",
		// A variable a closure captures lives in a cell the closure shares.
		"captured":            "used by a Store instruction",
		"methodValue":         "is taken as a value",
		"concreteMethodValue": "is taken as a value",
		"methodExpression":    "is taken as a value",
	}
	got := map[string][]string{}
	for _, p := range problemsOf(code, reasondrift.RecorderNotCounted) {
		if p.Package != pkg {
			t.Errorf("a problem outside %s: %+v", pkg, p)
		}
		got[p.Func] = append(got[p.Func], p.Detail)
	}
	for fn, why := range refused {
		if len(got[fn]) != 1 || !strings.Contains(got[fn][0], why) {
			t.Errorf("%s: problems %q, want one that says %q", fn, got[fn], why)
		}
	}
	for fn := range got {
		if _, ok := refused[fn]; !ok {
			t.Errorf("%s is refused, and the test does not expect it: %q", fn, got[fn])
		}
	}
	// No other problem of the kinds this module's recorders can cause: the two
	// that are no builder (a method of another package, a function) raise
	// nothing, and the compiler's wrappers of the method values add none of
	// their own. (The event-not-constructed ones are the lifecycle package's,
	// which TestResolvesEachWayAReasonArrives lists.)
	for _, p := range code.Problems {
		if p.Kind != reasondrift.RecorderNotCounted && p.Kind != reasondrift.EventNotConstructed {
			t.Errorf("problem %+v, want only the recorders'", p)
		}
	}
}

// TestTheDefaultBuildersAreThoseOfThisModule: with no builders named, the scan
// looks for controller-runtime's and client-go's, which the synthetic module
// has none of, so it finds none of its recorders; and a builder is matched by
// its package, so the names the synthetic module's own package uses are no
// builders to it.
func TestTheDefaultBuildersAreThoseOfThisModule(t *testing.T) {
	code := scanSynthetic(t, nil, nil)
	if len(code.Recorders) != 0 || len(problemsOf(code, reasondrift.RecorderNotCounted)) != 0 {
		t.Errorf("recorders %+v, problems %+v: the default builders matched the synthetic module's", code.Recorders, code.Problems)
	}
}
