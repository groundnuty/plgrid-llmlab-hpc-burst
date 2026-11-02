package scan

import (
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

// TestResolvesEachWayAReasonArrives: the expectations are written out from the
// source of testdata/mod/internal/lifecycle/lifecycle.go.
func TestResolvesEachWayAReasonArrives(t *testing.T) {
	code, err := Scan(Options{Root: syntheticRoot(t)})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		fn      string
		reasons []string
		action  string
		kinds   []string
	}{
		{"pass.direct", []string{"A"}, "failing", []string{"Instance"}},
		{"pass.local", []string{"Local"}, "failing", []string{"Instance"}},
		{"pass.typed", []string{"Typed"}, "failing", []string{"Instance"}},
		// fail forwards its parameter: the reasons are its callers' arguments.
		{"pass.fail", []string{"B", "D"}, "failing", []string{"Instance"}},
		// A variable set twice, captured by a closure: both values.
		{"pass.variable", []string{"A", "C"}, "degraded", []string{"Instance"}},
		// The empty string a variable starts from is no reason.
		{"pass.guarded", []string{"C"}, "degraded", []string{"Instance"}},
		{"pass.fromCall", []string{"A", "B"}, "failing", []string{"Instance"}},
		// An interface-typed regarding object: the kinds converted into it.
		{"pass.either", []string{"A"}, "degraded", []string{"Instance", "ModelDeployment"}},
	} {
		e := emitOf(t, code, c.fn)
		if e.Unresolved != "" {
			t.Errorf("%s: unresolved: %s", c.fn, e.Unresolved)
			continue
		}
		if !slices.Equal(e.Reasons, c.reasons) || !slices.Equal(e.Actions, []string{c.action}) || !slices.Equal(e.Kinds, c.kinds) {
			t.Errorf("%s: reasons %v actions %v kinds %v, want %v [%s] %v", c.fn, e.Reasons, e.Actions, e.Kinds, c.reasons, c.action, c.kinds)
		}
	}
	// What cannot be told is reported, never skipped.
	for fn, why := range map[string]string{
		"pass.field":    "reason",
		"pass.viaValue": "no call in the module reaches",
	} {
		e := emitOf(t, code, fn)
		if e.Unresolved == "" || !strings.Contains(e.Unresolved, why) {
			t.Errorf("%s: unresolved %q, want a message containing %q", fn, e.Unresolved, why)
		}
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
}

// overlay adds one file to the synthetic lifecycle package.
func overlay(t *testing.T, src string) map[string][]byte {
	t.Helper()
	return map[string][]byte{filepath.Join(syntheticRoot(t), "internal/lifecycle/zz_overlay.go"): []byte(src)}
}

const unknownRecorder = `package lifecycle

type otherRecorder interface{ Event(string) }

func relay(o otherRecorder) { o.Event("X") }
`

// TestAnUnknownRecorderFails: a call to Event that is neither a sink nor in a
// named wrapper fails the scan, so a new emitter path cannot go unseen.
func TestAnUnknownRecorderFails(t *testing.T) {
	_, err := Scan(Options{Root: syntheticRoot(t), Overlay: overlay(t, unknownRecorder)})
	if err == nil || !strings.Contains(err.Error(), "does not know") || !strings.Contains(err.Error(), "internal/lifecycle.relay") {
		t.Fatalf("error %v, want one that names internal/lifecycle.relay as unknown", err)
	}
}

// TestANamedWrapperIsSkippedAndSeen: the same call inside a named wrapper is no
// emit site, and the scan reports the wrapper as met.
func TestANamedWrapperIsSkippedAndSeen(t *testing.T) {
	code, err := Scan(Options{Root: syntheticRoot(t), Overlay: overlay(t, unknownRecorder), Wrappers: []string{"internal/lifecycle.relay"}})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"internal/lifecycle.relay"}; !slices.Equal(code.Wrappers, want) {
		t.Errorf("wrappers seen %v, want %v", code.Wrappers, want)
	}
	// And a wrapper that is named but not in the code is not reported as met.
	code, err = Scan(Options{Root: syntheticRoot(t), Wrappers: []string{"internal/lifecycle.relay"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(code.Wrappers) != 0 {
		t.Errorf("wrappers seen %v, want none", code.Wrappers)
	}
}

// TestATypeErrorFails: packages that do not type-check fail the scan; they do
// not read as having no emit sites.
func TestATypeErrorFails(t *testing.T) {
	_, err := Scan(Options{Root: syntheticRoot(t), Overlay: overlay(t, "package lifecycle\n\nvar broken int = \"text\"\n")})
	if err == nil || !strings.Contains(err.Error(), "do not type-check") {
		t.Fatalf("error %v, want one that says the packages do not type-check", err)
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
