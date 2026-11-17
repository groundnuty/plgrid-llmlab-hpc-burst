package scan

import (
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/reasondrift"
)

// The tests of the sink that takes its type from a map (the Provider
// controller's typed helper, #625): the fixture is
// testdata/mod/internal/providerctl/events.go, whose map holds
//
//	A -> ready, B -> failing, JobSubmitted -> started, JobEnded -> ended
//
// and whose helper typed(a *Allocation, reason, fact, note, related) is a sink
// and a listed wrapper (typedWrapper). The expectations are written out from
// that source, not from what the scan prints.

const (
	providerctl = "internal/providerctl"
	typedMap    = "internal/providerctl.allocationEventType"
	eventsFile  = "internal/providerctl/events.go"
)

// emitsIn returns the Emits of one function of a package, in scan order.
func emitsIn(code *reasondrift.Code, pkg, fn string) []reasondrift.Emit {
	var out []reasondrift.Emit
	for _, e := range code.Emits {
		if e.Package == pkg && e.Func == fn {
			out = append(out, e)
		}
	}
	return out
}

// wantEmit is what one Emit of the fixture must hold.
type wantEmit struct {
	reasons, literals []string
	action            string // "" for an emit with no type
	unresolved        string // a text the Unresolved must contain
}

func checkEmits(t *testing.T, code *reasondrift.Code, fn string, want ...wantEmit) {
	t.Helper()
	got := emitsIn(code, providerctl, fn)
	if len(got) != len(want) {
		t.Errorf("%s: %d emits %+v, want %d", fn, len(got), got, len(want))
		return
	}
	for i, w := range want {
		e := got[i]
		if !slices.Equal(e.Reasons, w.reasons) || !slices.Equal(e.LiteralReasons, w.literals) {
			t.Errorf("%s emit %d: reasons %v literal %v, want %v literal %v", fn, i, e.Reasons, e.LiteralReasons, w.reasons, w.literals)
		}
		if w.unresolved != "" {
			if !strings.Contains(e.Unresolved, w.unresolved) || len(e.Actions) != 0 || len(e.Kinds) != 0 {
				t.Errorf("%s emit %d: unresolved %q actions %v kinds %v, want an unresolved that says %q and no type or kind", fn, i, e.Unresolved, e.Actions, e.Kinds, w.unresolved)
			}
			continue
		}
		if e.Unresolved != "" || e.Unproven != "" || !slices.Equal(e.Actions, []string{w.action}) || !slices.Equal(e.Kinds, []string{"Allocation"}) {
			t.Errorf("%s emit %d: unresolved %q unproven %q actions %v kinds %v, want type %s regarding Allocation", fn, i, e.Unresolved, e.Unproven, e.Actions, e.Kinds, w.action)
		}
	}
}

// TestATypedEmitTakesItsReasonFromTheCallAndItsTypeFromTheMap: the reason is
// the call's argument, through a constant of the api package, a constant of the
// package and a caller's argument; the type is the map's value under the
// reason, one Emit for each reason, so that a call that can raise two reasons
// never pairs one with the other's type; the kind is that of the regarding
// argument. The helper's own call of EventfID is no emit site.
func TestATypedEmitTakesItsReasonFromTheCallAndItsTypeFromTheMap(t *testing.T) {
	code := scanSynthetic(t, nil, nil)
	checkEmits(t, code, "external.plain", wantEmit{reasons: []string{"A"}, action: "ready"})
	checkEmits(t, code, "external.own", wantEmit{reasons: []string{"JobSubmitted"}, action: "started"})
	// forwarded takes its reason from its two callers, B and JobEnded: two
	// emits, in the order of the reasons.
	checkEmits(t, code, "external.forwarded",
		wantEmit{reasons: []string{"B"}, action: "failing"},
		wantEmit{reasons: []string{"JobEnded"}, action: "ended"})
	if got := emitsIn(code, providerctl, "external.typed"); len(got) != 0 {
		t.Errorf("the helper's own EventfID call is an emit site: %+v", got)
	}
	n := 0
	for _, e := range code.Emits {
		if e.Package == providerctl {
			n++
		}
	}
	if n != 4 {
		t.Errorf("%d emits in %s, want 4 (plain, own and forwarded's two)", n, providerctl)
	}
	for _, p := range code.Problems {
		if p.Package == providerctl {
			t.Errorf("problem in the fixture's providerctl: %+v", p)
		}
	}
}

// typedCode keeps the emits of the fixture's providerctl and what Compare needs.
func typedCode(code *reasondrift.Code) *reasondrift.Code {
	out := &reasondrift.Code{Constants: code.Constants}
	for _, e := range code.Emits {
		if e.Package == providerctl {
			out.Emits = append(out.Emits, e)
		}
	}
	return out
}

func tableOf(rows ...[2]string) *reasondrift.Docs {
	d := &reasondrift.Docs{}
	for _, r := range rows {
		d.Table = append(d.Table, reasondrift.TableEntry{Action: r[0], Reason: r[1], Kinds: []string{"Allocation"}})
	}
	return d
}

// TestTheComparisonPairsEachReasonWithItsOwnType: a table that holds exactly
// the fixture's four (type, reason) pairs, regarding an Allocation, leaves no
// mismatch; one with B under ended instead of failing leaves exactly the
// missing row and the unlisted emit. If forwarded's two reasons were one emit,
// the product of its reasons and types would add (B, ended) and (JobEnded,
// failing) to the first.
func TestTheComparisonPairsEachReasonWithItsOwnType(t *testing.T) {
	code := typedCode(scanSynthetic(t, nil, nil))
	good := tableOf([2]string{"ready", "A"}, [2]string{"failing", "B"}, [2]string{"started", "JobSubmitted"}, [2]string{"ended", "JobEnded"})
	if got := reasondrift.Compare(good, code); len(got) != 0 {
		t.Errorf("mismatches %v, want none", got)
	}
	wrong := tableOf([2]string{"ready", "A"}, [2]string{"ended", "B"}, [2]string{"started", "JobSubmitted"}, [2]string{"ended", "JobEnded"})
	var got []string
	for _, m := range reasondrift.Compare(wrong, code) {
		got = append(got, m.Key())
	}
	want := []string{
		reasondrift.Mismatch{Kind: reasondrift.CodeOnly, Action: "failing", Reason: "B", Regarding: "Allocation", Package: providerctl}.Key(),
		reasondrift.Mismatch{Kind: reasondrift.TableOnly, Action: "ended", Reason: "B", Regarding: "Allocation"}.Key(),
	}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("mismatches %v, want %v", got, want)
	}
}

const unmappedOverlay = `package providerctl

import "github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"

const reasonUnmapped = "NotInTheMap"

// both reaches one call with a reason the map holds and one it lacks.
func (e *external) both(a *v1alpha1.Allocation, first bool) {
	reason := reasonSubmitted
	if !first {
		reason = reasonUnmapped
	}
	e.typed(a, reason, "f", "n", nil)
}

// alone passes only the reason the map lacks.
func (e *external) alone(a *v1alpha1.Allocation) { e.typed(a, reasonUnmapped, "f", "n", nil) }
`

// TestAReasonThatTheMapLacksIsUnresolvedForThatReasonOnly.
func TestAReasonThatTheMapLacksIsUnresolvedForThatReasonOnly(t *testing.T) {
	code := scanSynthetic(t, map[string]string{"internal/providerctl/zz_overlay.go": unmappedOverlay}, nil)
	const why = "the type: reason NotInTheMap is not in " + typedMap
	checkEmits(t, code, "external.both",
		wantEmit{reasons: []string{"JobSubmitted"}, action: "started"},
		wantEmit{reasons: []string{"NotInTheMap"}, unresolved: why})
	checkEmits(t, code, "external.alone", wantEmit{reasons: []string{"NotInTheMap"}, unresolved: why})
	// Compare reports each unresolved emit by its function, whatever the table says.
	var funcs []string
	for _, m := range reasondrift.Compare(tableOf(), typedCode(code)) {
		if m.Kind == reasondrift.Unresolved {
			funcs = append(funcs, m.Func)
		}
	}
	slices.Sort(funcs)
	if want := []string{"external.alone", "external.both"}; !slices.Equal(funcs, want) {
		t.Errorf("unresolved mismatches in %v, want %v", funcs, want)
	}
}

const literalOverlay = `package providerctl

import "github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"

// literal writes the reason of a typed event as a string literal.
func (e *external) literal(a *v1alpha1.Allocation) { e.typed(a, "JobSubmitted", "f", "n", nil) }

// literalCaller hands forwarded a literal, beside the constants its other callers pass.
func (e *external) literalCaller(a *v1alpha1.Allocation) { e.forwarded(a, "JobEnded") }
`

// TestALiteralReasonOfATypedEmitIsReported: the literal is told from a named
// constant through the new sink, and each reason of a call site has its own
// LiteralReasons: forwarded's callers pass B as a constant, JobEnded as a
// constant and, from literalCaller, as a literal.
func TestALiteralReasonOfATypedEmitIsReported(t *testing.T) {
	code := scanSynthetic(t, map[string]string{"internal/providerctl/zz_overlay.go": literalOverlay}, nil)
	checkEmits(t, code, "external.literal", wantEmit{reasons: []string{"JobSubmitted"}, literals: []string{"JobSubmitted"}, action: "started"})
	checkEmits(t, code, "external.forwarded",
		wantEmit{reasons: []string{"B"}, action: "failing"},
		wantEmit{reasons: []string{"JobEnded"}, literals: []string{"JobEnded"}, action: "ended"})
	// Compare turns the two literals into mismatches, one per function.
	var got []string
	for _, m := range reasondrift.Compare(tableOf(), typedCode(code)) {
		if m.Kind == reasondrift.Literal {
			got = append(got, m.Func+"/"+m.Reason)
		}
	}
	slices.Sort(got)
	if want := []string{"external.forwarded/JobEnded", "external.literal/JobSubmitted"}; !slices.Equal(got, want) {
		t.Errorf("literal mismatches %v, want %v", got, want)
	}
}

const valueOverlay = `package providerctl

import (
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
)

// viaMethodValue calls the helper through a method value.
func (e *external) viaMethodValue(a *v1alpha1.Allocation) {
	f := e.typed
	f(a, reasonSubmitted, "f", "n", nil)
}

// viaFuncValue calls it through a method expression.
func viaFuncValue(e *external, a *v1alpha1.Allocation) {
	f := (*external).typed
	f(e, a, reasonSubmitted, "f", "n", nil)
}

var kept []func(*v1alpha1.Allocation, string, string, string, eventcount.Object)

// keepMethodValue stores a method value, so the calls of it are not all known.
func (e *external) keepMethodValue() { kept = append(kept, e.typed) }
`

// TestATypedCallThroughAValueIsReported: the helper taken as a method value or
// a method expression is an emit through a function the compiler makes, and the
// call of the value is one at its own site: none is skipped.
func TestATypedCallThroughAValueIsReported(t *testing.T) {
	code := scanSynthetic(t, map[string]string{"internal/providerctl/zz_overlay.go": valueOverlay}, nil)
	// The call of a method value is the call of the helper, read at its site.
	checkEmits(t, code, "external.viaMethodValue", wantEmit{reasons: []string{"JobSubmitted"}, action: "started"})
	// The call of a method expression passes the receiver first, which the sink's
	// indexes do not expect: the call is reported, not read.
	got := emitsIn(code, providerctl, "viaFuncValue")
	if len(got) != 1 || got[0].Unresolved == "" {
		t.Errorf("viaFuncValue: %+v, want one unresolved emit", got)
	}
	// A method value that is stored has callers no list holds: its emit in the
	// function the compiler made says so.
	var kept []reasondrift.Emit
	for _, e := range code.Emits {
		if e.Package == "(compiler-made)" && strings.Contains(e.Func, "external).typed$bound") && strings.Contains(e.Unresolved, "used as a value") {
			kept = append(kept, e)
		}
	}
	if len(kept) != 1 {
		t.Errorf("emits of a stored method value: %+v, want one that says it is used as a value", kept)
	}
}

// eventsSource reads the fixture's providerctl source.
func eventsSource(t *testing.T) string {
	t.Helper()
	src, err := readFile(filepath.Join(syntheticRoot(t), eventsFile))
	if err != nil {
		t.Fatal(err)
	}
	return src
}

// replaceOnceIn returns src with the one occurrence of from replaced by to.
func replaceOnceIn(t *testing.T, src, from, to string) string {
	t.Helper()
	if n := strings.Count(src, from); n != 1 {
		t.Fatalf("%d occurrences of %q in the fixture, want 1", n, from)
	}
	return strings.Replace(src, from, to, 1)
}

// withMapDecl returns the fixture with the declaration of allocationEventType,
// from "var" to the closing brace of its literal, replaced by decl.
func withMapDecl(t *testing.T, decl string) string {
	t.Helper()
	src := eventsSource(t)
	start := strings.Index(src, "var allocationEventType = ")
	end := strings.Index(src[start:], "\n}\n")
	if start < 0 || end < 0 {
		t.Fatal("the fixture's allocationEventType is not where the test expects it")
	}
	return src[:start] + decl + src[start+end+len("\n}\n"):]
}

// scanError scans the synthetic module with a file replaced and returns the
// error, which the caller expects.
func scanError(t *testing.T, name, src string) error {
	t.Helper()
	_, err := Scan(Options{
		Root: syntheticRoot(t), Wrappers: []reasondrift.Wrapper{kubeWrapper, controllerWrapper, typedWrapper},
		Overlay: map[string][]byte{filepath.Join(syntheticRoot(t), name): []byte(src)},
	})
	return err
}

// lineOf returns the line number, from 1, of the first occurrence of text.
func lineOf(t *testing.T, src, text string) int {
	t.Helper()
	i := strings.Index(src, text)
	if i < 0 {
		t.Fatalf("%q is not in the source", text)
	}
	return strings.Count(src[:i], "\n") + 1
}

// TestAMapTheScanCannotReadFailsTheScan: an entry whose key or value is a call
// or a variable, a map that is no composite literal, and one that is empty or
// absent are errors when a call of the sink needs the map, which name the
// package, the variable and, for an entry, its line. The scan never reads a call
// site against a map it did not see.
func TestAMapTheScanCannotReadFailsTheScan(t *testing.T) {
	// decl is the map with one entry in the body, the last of its two, and what
	// follows the declaration.
	decl := func(entry, after string) string {
		return "var allocationEventType = map[string]string{\n" + entry + "\treasonEnded: typeEnded,\n}\n" + after
	}
	const notAPair = "an entry of internal/providerctl's allocationEventType is not a pair of string constants"
	for _, c := range []struct {
		name, entry, after, want string
		onLine                   string // the text of the line of the bad entry; "" for an error with no entry
	}{
		{"a value that is a call", "\treasonSubmitted: startedType(),\n", "\nfunc startedType() string { return typeStarted }\n", notAPair, "startedType(),"},
		{"a value that is a variable", "\treasonSubmitted: startedVar,\n", "\nvar startedVar = typeStarted\n", notAPair, "startedVar,"},
		{"a key that is a call", "\tsubmitted(): typeStarted,\n", "\nfunc submitted() string { return reasonSubmitted }\n", notAPair, "submitted(): "},
		{"a key that is a variable", "\tsubmittedVar: typeStarted,\n", "\nvar submittedVar = reasonSubmitted\n", notAPair, "submittedVar: "},
	} {
		t.Run(c.name, func(t *testing.T) {
			src := withMapDecl(t, decl(c.entry, c.after))
			err := scanError(t, eventsFile, src)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %v, want one that says %q", err, c.want)
			}
			if at := "events.go:" + strconv.Itoa(lineOf(t, src, c.onLine)) + ":"; !strings.Contains(err.Error(), at) {
				t.Errorf("error %v, want it at %s", err, at)
			}
		})
	}
	for _, c := range []struct{ name, src, want string }{
		{"a variable that is no composite literal",
			withMapDecl(t, "var allocationEventType = loadTypes()\n\nfunc loadTypes() map[string]string { return map[string]string{reasonEnded: typeEnded} }\n"),
			"internal/providerctl's allocationEventType is not initialized with a composite literal"},
		{"an empty map", withMapDecl(t, "var allocationEventType = map[string]string{\n}\n"),
			"internal/providerctl's allocationEventType is empty"},
		{"a map of another name", strings.ReplaceAll(eventsSource(t), "allocationEventType", "renamedEventType"),
			"internal/providerctl declares no allocationEventType"},
	} {
		t.Run(c.name, func(t *testing.T) {
			if err := scanError(t, eventsFile, c.src); err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %v, want one that says %q", err, c.want)
			}
		})
	}
	// A map is read when a call of its sink needs it, not before: a tree with no
	// such call (the commit before the helper exists, or a module without the
	// package) scans, and the guard's required packages are what notice a package
	// that was lost.
	t.Run("a tree with no call of the sink is not read for its map", func(t *testing.T) {
		copied := filepath.Join(t.TempDir(), "mod")
		if err := os.CopyFS(copied, os.DirFS(syntheticRoot(t))); err != nil {
			t.Fatal(err)
		}
		if err := os.RemoveAll(filepath.Join(copied, providerctl)); err != nil {
			t.Fatal(err)
		}
		code, err := Scan(Options{Root: copied})
		if err != nil {
			t.Fatalf("a module with no internal/providerctl: %v", err)
		}
		for _, e := range code.Emits {
			if e.Package == providerctl {
				t.Errorf("an emit in the package that was removed: %+v", e)
			}
		}
		// The package is there and its helper and map are gone: the same.
		if err := os.MkdirAll(filepath.Join(copied, providerctl), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(copied, eventsFile), []byte("package providerctl\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := Scan(Options{Root: copied}); err != nil {
			t.Fatalf("a providerctl with no helper and no map: %v", err)
		}
	})
}

// TestTheManagedMapIsReadThroughTheSameHelper: internal/managed's badAction is
// read by stringMap too, and its errors name internal/managed.
func TestTheManagedMapIsReadThroughTheSameHelper(t *testing.T) {
	src, err := readFile(filepath.Join(syntheticRoot(t), "internal/managed/managed.go"))
	if err != nil {
		t.Fatal(err)
	}
	mutated := replaceOnceIn(t, src, `v1alpha1.ReasonA: "failing",`, "v1alpha1.ReasonA: failingType(),") + "\nfunc failingType() string { return \"failing\" }\n"
	err = scanError(t, "internal/managed/managed.go", mutated)
	if err == nil || !strings.Contains(err.Error(), "an entry of internal/managed's badAction is not a pair of string constants") {
		t.Fatalf("error %v, want one that names internal/managed's badAction", err)
	}
}

// TestTheHelpersEventfIDCallIsAcceptedOnlyIfItPassesTheMapsValue: the helper's
// EventfID call, in the listed wrapper, is accepted when its type is the lookup
// of its reason parameter in allocationEventType, with or without the comma-ok
// form, and is a WrapperDoesNotForward problem when it passes a constant, the
// value of another map, a lookup keyed by another parameter or by the reason
// changed, a lookup in a copy of the map, the reason itself, or another
// regarding object.
func TestTheHelpersEventfIDCallIsAcceptedOnlyIfItPassesTheMapsValue(t *testing.T) {
	const call = `e.rec.EventfID(id, a, related, "Normal", reason, action, note)`
	const lookup = "action, ok := allocationEventType[reason]"
	src := eventsSource(t)
	if !strings.Contains(src, call) || !strings.Contains(src, lookup) {
		t.Fatal("the fixture's helper is not where the test expects it")
	}
	// variant replaces the helper's lookup by one that keeps ok (the type is then
	// not used by name) and its call by one with the given arguments.
	variant := func(args string) string {
		s := replaceOnceIn(t, src, lookup, "_, ok := allocationEventType[reason]")
		return replaceOnceIn(t, s, call, "e.rec.EventfID(id, "+args+")")
	}
	for name, mutated := range map[string]string{
		"the comma-ok lookup": src,
		"the plain lookup":    variant(`a, related, "Normal", reason, allocationEventType[reason], note`),
	} {
		code := scanSynthetic(t, map[string]string{eventsFile: mutated}, nil)
		if got := problemsOf(code, reasondrift.WrapperDoesNotForward); len(got) != 0 {
			t.Errorf("%s: wrapper problems %+v, want none", name, got)
		}
		if !slices.Contains(code.Wrappers, typedWrapper.Func) {
			t.Errorf("%s: wrappers met %v lack %s", name, code.Wrappers, typedWrapper.Func)
		}
	}
	for _, c := range []struct{ name, args, detail string }{
		{"a constant type", `a, related, "Normal", reason, "started", note`, "argument 5"},
		{"a named constant type", `a, related, "Normal", reason, typeStarted, note`, "argument 5"},
		{"the value of another map", `a, related, "Normal", reason, otherEventType[reason], note`, "argument 5"},
		{"a lookup keyed by another parameter", `a, related, "Normal", reason, allocationEventType[fact], note`, "argument 5"},
		{"a lookup keyed by the reason changed", `a, related, "Normal", reason, allocationEventType[reason+"x"], note`, "argument 5"},
		{"a lookup in a copy of the map", `a, related, "Normal", reason, map[string]string{reasonEnded: typeEnded}[reason], note`, "argument 5"},
		{"the reason as the type", `a, related, "Normal", reason, reason, note`, "argument 5"},
		{"another regarding object", `related, related, "Normal", reason, allocationEventType[reason], note`, "argument 1"},
		{"a reason of its own", `a, related, "Normal", "JobEnded", allocationEventType[reason], note`, "argument 4"},
	} {
		t.Run(c.name, func(t *testing.T) {
			code := scanSynthetic(t, map[string]string{eventsFile: variant(c.args)}, nil)
			got := problemsOf(code, reasondrift.WrapperDoesNotForward)
			if len(got) != 1 || got[0].Package != providerctl || got[0].Func != "external.typed" || !strings.Contains(got[0].Detail, c.detail) {
				t.Errorf("wrapper problems %+v, want one in external.typed that says %q", got, c.detail)
			}
		})
	}
}

const relayIDOverlay = `package providerctl

import (
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
)

// relayID calls EventfID outside a listed wrapper.
func relayID(r eventcount.IDRecorder, a *v1alpha1.Allocation) {
	r.EventfID("id", a, nil, "Normal", reasonSubmitted, typeStarted, "n")
}
`

// TestEventfIDIsAnEmitMethod: a call of it that is neither a sink nor in a
// listed wrapper is an UnknownRecorder problem, as one of Event and Eventf is.
func TestEventfIDIsAnEmitMethod(t *testing.T) {
	code := scanSynthetic(t, map[string]string{"internal/providerctl/zz_overlay.go": relayIDOverlay}, nil)
	got := problemsOf(code, reasondrift.UnknownRecorder)
	if len(got) != 1 || got[0].Package != providerctl || got[0].Func != "relayID" || !strings.Contains(got[0].Detail, "IDRecorder).EventfID") {
		t.Fatalf("unknown-recorder problems %+v, want one in relayID that names IDRecorder.EventfID", got)
	}
}
