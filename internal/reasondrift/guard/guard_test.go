package guard_test

import (
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/reasondrift"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/reasondrift/scan"
)

// strict ignores knownDriftUntilHBEP8Erratum, the temporary allowlist of the
// drift that exists today: with REASONDRIFT_STRICT=1 the guard fails on today's
// tree, which is what it is for. Delete the switch with the list when the
// erratum has landed.
var strict = os.Getenv("REASONDRIFT_STRICT") == "1"

// knownDriftUntilHBEP8Erratum is debt, not a policy: the drift between HBEP-8
// and the code on the day the guard was added (2 Nov 2026, #239), allowed so
// that the guard can merge before the erratum. Remove each entry in the pull
// request that fixes what it names. The test also fails on a STALE entry, one
// that no longer drifts, so the list can only shrink and the erratum's merge
// forces its entries out. When the list is empty, delete it and the switch.
//
// Both entries are for the HBEP: a deployment's event regards the Instance once
// the ModelDeployment is gone (internal/lifecycle, mdObject), and HBEP-8's table
// says ModelDeployment only. The erratum splits the lifecycle row so that
// VerificationFailed and CapacityDropped regard the ModelDeployment, else the
// Instance; the report on #239 has the wording the guard accepts.
//
// A reason an emit site takes from a package-local constant needs no entry: the
// scan reaches it by reference. A reason written as a string literal needs a
// constant in api/v1alpha1; the three that were literals have theirs in this PR.
var knownDriftUntilHBEP8Erratum = []reasondrift.Entry{
	{Kind: reasondrift.CodeOnly, Action: "degraded", Reason: "CapacityDropped", Regarding: "Instance", Package: "internal/lifecycle", Why: "mdObject falls back to the Instance when the ModelDeployment is gone; the table lists ModelDeployment only"},
	{Kind: reasondrift.CodeOnly, Action: "degraded", Reason: "VerificationFailed", Regarding: "Instance", Package: "internal/lifecycle", Why: "mdObject falls back to the Instance when the ModelDeployment is gone; the table lists ModelDeployment only"},
}

// emitterPatterns are the packages the scan loads: those that raise events, and
// the fake that wraps a recorder. The rest of the module is only looked at, with
// the scan's CheckRest, for a use of a recorder method, so a new emitter in
// another package fails the guard instead of going unread. Loading only these
// takes the scan from 0.85 GB to 0.67 GB.
var emitterPatterns = []string{
	"./api/v1alpha1", "./cmd/hpcburst-controller", "./internal/front", "./internal/lifecycle",
	"./internal/managed", "./internal/providerctl", "./pkg/provider", "./pkg/provider/fake",
}

// requiredPackages are the packages whose syntax the scan must have loaded.
var requiredPackages = []string{
	"api/v1alpha1", "cmd/hpcburst-controller", "internal/front", "internal/lifecycle",
	"internal/managed", "internal/providerctl", "pkg/provider", "pkg/provider/fake",
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "go.mod")); err != nil {
		t.Fatalf("the repository root is not three levels up: %v", err)
	}
	return dir
}

// Each scan holds the whole module's syntax and types, so each runs once per
// test binary, however many times the tests run (-count=N): the results are kept.
type scanned struct {
	once sync.Once
	code *reasondrift.Code
	err  error
}

func (s *scanned) get(t *testing.T, overlay map[string][]byte) *reasondrift.Code {
	t.Helper()
	s.once.Do(func() {
		s.code, s.err = scan.Scan(scan.Options{
			Root: repoRoot(t), Patterns: emitterPatterns, CheckRest: true, Overlay: overlay,
			Wrappers: reasondrift.Wrappers, Require: requiredPackages,
		})
		// Give the memory back before the next scan, so the test binary's peak
		// is one scan's.
		runtime.GC()
		debug.FreeOSMemory()
	})
	return s.code
}

var (
	realScanned    scanned // the unchanged tree
	overlayScanned scanned // the tree with the mutants of TestMutantsAreCaught
)

func realScan(t *testing.T) *reasondrift.Code {
	t.Helper()
	code := realScanned.get(t, nil)
	if realScanned.err != nil {
		t.Fatal(realScanned.err)
	}
	return code
}

func realDocs(t *testing.T) (md8, md7 []byte) {
	t.Helper()
	var err error
	if md8, err = os.ReadFile(filepath.Join(repoRoot(t), reasondrift.HBEP8Path)); err != nil {
		t.Fatal(err)
	}
	if md7, err = os.ReadFile(filepath.Join(repoRoot(t), reasondrift.HBEP7Path)); err != nil {
		t.Fatal(err)
	}
	return md8, md7
}

// drift compares the documents with the code and takes the allowances off:
// the exceptions, then (unless ignoreDebt) the debt list. It returns what is
// left and the stale entries of both lists.
func drift(docs *reasondrift.Docs, code *reasondrift.Code, exceptions, debt []reasondrift.Entry, ignoreDebt bool) (left []reasondrift.Mismatch, stale []reasondrift.Entry) {
	lists := [][]reasondrift.Entry{exceptions}
	if !ignoreDebt {
		lists = append(lists, debt)
	}
	return reasondrift.Apply(reasondrift.Compare(docs, code), lists...)
}

// key builds the identity of a mismatch from its fields, so the expectations
// below do not count separators.
func key(kind, action, reason, regarding, pkg string) string {
	return reasondrift.Mismatch{Kind: kind, Action: action, Reason: reason, Regarding: regarding, Package: pkg}.Key()
}

// keyF is key for a mismatch that names a function.
func keyF(kind, reason, pkg, fn string) string {
	return reasondrift.Mismatch{Kind: kind, Reason: reason, Package: pkg, Func: fn}.Key()
}

func keys(ms []reasondrift.Mismatch) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = m.Key()
	}
	sort.Strings(out)
	return out
}

// TestHBEPsMatchTheCode is the guard (#239). It compares HBEP-8's reasons
// table and HBEP-7's Degraded ranking with the code, both ways:
//   - every reason of the table or the ranking has a Reason constant in
//     api/v1alpha1;
//   - every (type, reason, regarding kind) of the table has an emit site, found
//     by reference, and every one an emit site raises is in the table;
//   - HBEP-7's ranking equals the order the code ranks by.
//
// The only allowances are Exceptions (true to the design) and
// knownDriftUntilHBEP8Erratum (debt). An entry that matches nothing is stale.
func TestHBEPsMatchTheCode(t *testing.T) {
	docs, err := reasondrift.LoadDocs(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	code := realScan(t)
	left, stale := drift(docs, code, reasondrift.Exceptions, knownDriftUntilHBEP8Erratum, strict)
	for _, m := range left {
		t.Errorf("drift: %s", m)
	}
	for _, e := range stale {
		t.Errorf("stale allowance, remove it: %s", e)
	}
	if len(left) > 0 && strict {
		t.Logf("REASONDRIFT_STRICT=1: knownDriftUntilHBEP8Erratum is ignored, so this is the drift of today's tree")
	}
}

// TestTheScanSawTheEmitters guards the guard: a scan that found nothing in a
// package whose events HBEP-8 lists would pass for the wrong reason.
func TestTheScanSawTheEmitters(t *testing.T) {
	code := realScan(t)
	perPkg := map[string]int{}
	for _, e := range code.Emits {
		perPkg[e.Package]++
	}
	// The three packages that raise typed events. (The Provider controller's
	// and the managed controller's events are built by constructors, which the
	// scan does not read.)
	for _, pkg := range []string{"internal/lifecycle", "internal/front", "cmd/hpcburst-controller"} {
		if perPkg[pkg] == 0 {
			t.Errorf("no emit site found in %s; found %v", pkg, perPkg)
		}
	}
	want := slices.Clone(reasondrift.WrapperNames())
	sort.Strings(want)
	if !slices.Equal(code.Wrappers, want) {
		t.Errorf("wrappers met %v, listed %v: a stale name, or one that is not in the code", code.Wrappers, want)
	}
	if len(code.Ranking) == 0 || len(code.Constants) == 0 {
		t.Errorf("ranking %v and %d constants: the scan read nothing", code.Ranking, len(code.Constants))
	}
}

// TestAStaleAllowanceFails is a mutant check: an entry added to either list
// that matches no mismatch is reported as stale, so the debt list can only
// shrink and the erratum's merge forces its entries out. The unchanged lists
// have none.
func TestAStaleAllowanceFails(t *testing.T) {
	docs, err := reasondrift.LoadDocs(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	code := realScan(t)
	if _, stale := drift(docs, code, reasondrift.Exceptions, knownDriftUntilHBEP8Erratum, false); len(stale) != 0 {
		t.Fatalf("the unchanged lists have stale entries: %v", stale)
	}
	gone := reasondrift.Entry{Kind: reasondrift.CodeOnly, Action: "failing", Reason: "NoLongerDrifting", Regarding: "Instance", Package: "internal/lifecycle", Why: "the mutant"}
	debt := append(slices.Clone(knownDriftUntilHBEP8Erratum), gone)
	if _, stale := drift(docs, code, reasondrift.Exceptions, debt, false); !slices.Equal(stale, []reasondrift.Entry{gone}) {
		t.Errorf("a stale entry in the debt list: stale %v, want only it", stale)
	}
	exceptions := append(slices.Clone(reasondrift.Exceptions), gone)
	if _, stale := drift(docs, code, exceptions, knownDriftUntilHBEP8Erratum, false); !slices.Equal(stale, []reasondrift.Entry{gone}) {
		t.Errorf("a stale entry in the exceptions: stale %v, want only it", stale)
	}
	// An entry that is the debt of a mismatch the exceptions already cover is
	// stale too: it is reached after them, and finds nothing.
	covered := append(slices.Clone(knownDriftUntilHBEP8Erratum), reasondrift.Entry{Kind: reasondrift.CodeOnly, Package: "internal/providerctl", Why: "already an exception"})
	if _, stale := drift(docs, code, reasondrift.Exceptions, covered, false); len(stale) != 1 {
		t.Errorf("an entry for what an exception covers: stale %v, want it alone", stale)
	}
}

// TestEveryAllowanceStatesItsReason: an entry with no Why, or with no field to
// match on (which would allow everything), is refused.
func TestEveryAllowanceStatesItsReason(t *testing.T) {
	for _, list := range [][]reasondrift.Entry{reasondrift.Exceptions, knownDriftUntilHBEP8Erratum} {
		for _, e := range list {
			if strings.TrimSpace(e.Why) == "" {
				t.Errorf("%s has no Why", e)
			}
			if e.Kind == "" {
				t.Errorf("%s names no Kind, so it would allow every kind of mismatch", e)
			}
		}
	}
	for _, w := range reasondrift.Wrappers {
		if strings.TrimSpace(w.Why) == "" {
			t.Errorf("wrapper %s has no Why", w.Func)
		}
		if len(w.Forwards) == 0 {
			t.Errorf("wrapper %s lists nothing it forwards, so the scan would accept anything it passes on", w.Func)
		}
	}
}

// leftover compares parsed documents with the code and returns what both lists
// of allowances do not cover, whatever REASONDRIFT_STRICT says, and ignoring
// staleness: a mutant removes the lines an entry matches, which would make the
// entry stale. The mutant checks read "exactly this one mismatch more than the
// unchanged tree".
func leftover(t *testing.T, md8, md7 []byte, code *reasondrift.Code) []string {
	t.Helper()
	docs, err := reasondrift.ParseDocs(md8, md7)
	if err != nil {
		t.Fatal(err)
	}
	left, _ := drift(docs, code, reasondrift.Exceptions, knownDriftUntilHBEP8Erratum, false)
	return keys(left)
}

// TestTheUnchangedTreeIsTheBaselineOfTheMutants: the mutant checks below read
// "exactly this one mismatch more", so the base must have none left.
func TestTheUnchangedTreeIsTheBaselineOfTheMutants(t *testing.T) {
	md8, md7 := realDocs(t)
	if got := leftover(t, md8, md7, realScan(t)); len(got) != 0 {
		t.Errorf("the unchanged tree leaves %v", got)
	}
}

// mutateRows returns the HBEP-8 text with the lines of its reasons table that
// start with prefix replaced by repl (nothing, to remove them), and fails when
// no line matched.
func mutateRows(t *testing.T, md8 []byte, prefix string, repl ...string) []byte {
	t.Helper()
	var out []string
	hit := 0
	for _, l := range strings.Split(string(md8), "\n") {
		if strings.HasPrefix(l, prefix) {
			hit++
			out = append(out, repl...)
			continue
		}
		out = append(out, l)
	}
	if hit != 1 {
		t.Fatalf("%d lines start with %q, want 1", hit, prefix)
	}
	return []byte(strings.Join(out, "\n"))
}

// TestARemovedRowFails is a mutant check (#239): the row of AgentRegistered is
// cut from HBEP-8's table, so the code raises an event the table no longer
// lists, and exactly that mismatch appears.
func TestARemovedRowFails(t *testing.T) {
	md8, md7 := realDocs(t)
	mutated := mutateRows(t, md8, "| `started` | `AgentRegistered`")
	want := []string{key(reasondrift.CodeOnly, "started", "AgentRegistered", "Instance", "internal/lifecycle")}
	if got := leftover(t, mutated, md7, realScan(t)); !slices.Equal(got, want) {
		t.Errorf("with the row removed: %v, want %v", got, want)
	}
}

// TestAnAddedRowFails is a mutant check: a row for a reason no code emits adds
// exactly the mismatch that says.
func TestAnAddedRowFails(t *testing.T) {
	md8, md7 := realDocs(t)
	mutated := mutateRows(t, md8, "| `replaced` | `HandedOver`",
		"| `replaced` | `HandedOver` (proposed in PR #88) | Instance | lifecycle controller |",
		"| `failing` | `ReasonOfTheMutant` | Instance | lifecycle controller |")
	want := []string{key(reasondrift.TableOnly, "failing", "ReasonOfTheMutant", "Instance", "")}
	if got := leftover(t, mutated, md7, realScan(t)); !slices.Equal(got, want) {
		t.Errorf("with a row added: %v, want %v", got, want)
	}
}

// TestAChangedRegardingFails is a mutant check: a row that regards another kind
// than the code's emit sites do fails both ways.
func TestAChangedRegardingFails(t *testing.T) {
	md8, md7 := realDocs(t)
	mutated := mutateRows(t, md8, "| `degraded` | `DrainWindowShort`",
		"| `degraded` | `DrainWindowShort` | Provider | front |")
	want := []string{
		key(reasondrift.CodeOnly, "degraded", "DrainWindowShort", "PlatformProfile", "internal/front"),
		key(reasondrift.TableOnly, "degraded", "DrainWindowShort", "Provider", ""),
	}
	if got := leftover(t, mutated, md7, realScan(t)); !slices.Equal(got, want) {
		t.Errorf("with the regarding kind changed: %v, want %v", got, want)
	}
}

// mutantEmits is a file added to internal/lifecycle through an overlay. It holds
// the emits and the other code that each blocking finding of the review of
// #246 is about: a reason written as a string literal, a package-local
// constant, a literal put in a variable or returned by a function, a helper's
// discarded and emitted results, a constructor that is called and discarded,
// and a recorder the scan does not know, called and taken as a value.
const mutantEmits = `package lifecycle

import (
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

const (
	namedMutantReason     = "NamedMutantReason"
	discardedMutantReason = "DiscardedMutantReason"
)

func mutantIdent(r string) string { return r }

func mutantLiteralReturn() string { return "ViaReturnMutantReason" }

func (p *pass) mutantEmits(in *v1alpha1.Instance) {
	p.r.Recorder.Event(in, nil, eventFailing, "LiteralMutantReason", "m")
	p.r.Recorder.Event(in, nil, eventFailing, namedMutantReason, "m")
	p.r.Recorder.Event(in, nil, eventFailing, "UnlistedMutantReason", "m")
	reason := "ViaVariableMutantReason"
	p.r.Recorder.Event(in, nil, eventFailing, reason, "m")
	p.r.Recorder.Event(in, nil, eventFailing, mutantLiteralReturn(), "m")
	_ = mutantIdent(discardedMutantReason)
	p.r.Recorder.Event(in, nil, eventFailing, mutantIdent(namedMutantReason), "m")
	_ = provider.NewManagedEvent("degraded", "ConstructedMutantReason", "m")
}

type mutantOtherRecorder interface{ Event(string) }

func mutantRelay(o mutantOtherRecorder) { o.Event("X") }

func mutantRun(f func(string)) { f("X") }

func mutantUseValue(o mutantOtherRecorder) { mutantRun(o.Event) }
`

// replaceOnce returns the file at rel (under the root) with the one occurrence
// of from replaced by to.
func replaceOnce(t *testing.T, rel, from, to string) []byte {
	t.Helper()
	src, err := os.ReadFile(filepath.Join(repoRoot(t), rel))
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(src), from); n != 1 {
		t.Fatalf("%s has %d occurrences of %q, want 1", rel, n, from)
	}
	return []byte(strings.Replace(string(src), from, to, 1))
}

// mutantOverlay is every change the mutant checks make to the tree, so one scan
// of the changed tree serves them all.
func mutantOverlay(t *testing.T) map[string][]byte {
	root := repoRoot(t)
	return map[string][]byte{
		filepath.Join(root, "internal/lifecycle/zz_mutant_overlay.go"): []byte(mutantEmits),
		// B1: the three call sites that were literals are literals again, though
		// api/v1alpha1 holds a constant with the same value.
		filepath.Join(root, "internal/lifecycle/verify.go"): replaceOnce(t, "internal/lifecycle/verify.go",
			"eventReady, v1alpha1.EventReasonReady, ", `eventReady, "Ready", `),
		// A package-local constant of the front is replaced by its literal, which
		// api/v1alpha1 has no constant for.
		filepath.Join(root, "internal/front/router.go"): replaceOnce(t, "internal/front/router.go",
			"eventDegraded, reasonCountsDrift, msg)", `eventDegraded, "CountsDrift", msg)`),
		// B5: a listed wrapper that passes a reason of its own.
		filepath.Join(root, "internal/lifecycle/events.go"): replaceOnce(t, "internal/lifecycle/events.go",
			"rel, et, code, typ,", `rel, et, "WrongReason", typ,`),
		// A recorder method used in a package the scan did not load.
		filepath.Join(root, "internal/zzmutant/zz.go"): []byte("package zzmutant\n\ntype R interface{ Eventf() }\n\nfunc F(r R) { r.Eventf() }\n"),
	}
}

// TestMutantsAreCaught is the mutant check of the blocking findings of the
// review of #246 and of the original ones (#239): the changed tree is scanned
// once, and the mismatches are exactly those the changes cause, for the
// documents as they are and for documents with rows added.
func TestMutantsAreCaught(t *testing.T) {
	code := overlayScanned.get(t, mutantOverlay(t))
	if overlayScanned.err != nil {
		t.Fatal(overlayScanned.err)
	}
	md8, md7 := realDocs(t)
	const lc = "internal/lifecycle"

	// What the changes cause, whatever the documents say.
	always := []string{
		// B1: a literal where the constant exists, and where none does.
		keyF(reasondrift.Literal, "Ready", lc, "pass.becomeReady"),
		keyF(reasondrift.Literal, "CountsDrift", "internal/front", "Router.drift"),
		// B2: a literal at the call, in a variable and in a result.
		keyF(reasondrift.Literal, "LiteralMutantReason", lc, "pass.mutantEmits"),
		keyF(reasondrift.Literal, "UnlistedMutantReason", lc, "pass.mutantEmits"),
		keyF(reasondrift.Literal, "ViaVariableMutantReason", lc, "pass.mutantEmits"),
		keyF(reasondrift.Literal, "ViaReturnMutantReason", lc, "pass.mutantEmits"),
		// B5: a wrapper that does not pass its reason on.
		keyF(reasondrift.WrapperDoesNotForward, "", lc, "KubeRecorder.Event"),
		// B7: a call through a recorder the scan does not know.
		keyF(reasondrift.UnknownRecorder, "", lc, "mutantRelay"),
		// A recorder method outside the packages the scan loaded (its file's name).
		keyF(reasondrift.OutsideTheScan, "", "internal/zzmutant", "zz.go"),
		// The emits the table does not list. DiscardedMutantReason, whose helper
		// result is thrown away (B4), is not among them, and ConstructedMutantReason,
		// whose event is built and never raised (B6), neither.
		key(reasondrift.CodeOnly, "failing", "UnlistedMutantReason", "Instance", lc),
		key(reasondrift.CodeOnly, "failing", "ViaVariableMutantReason", "Instance", lc),
		key(reasondrift.CodeOnly, "failing", "ViaReturnMutantReason", "Instance", lc),
	}
	listed := []string{
		key(reasondrift.CodeOnly, "failing", "LiteralMutantReason", "Instance", lc),
		key(reasondrift.CodeOnly, "failing", "NamedMutantReason", "Instance", lc),
	}
	// B7 also leaves one in a function the compiler made, whose name is not ours
	// to spell: the wrapper of the method value.
	compilerMade := func(got []string) ([]string, int) {
		var rest []string
		n := 0
		for _, k := range got {
			if strings.HasPrefix(k, reasondrift.UnknownRecorder+"|") && strings.Contains(k, "|(compiler-made)|") {
				n++
				continue
			}
			rest = append(rest, k)
		}
		return rest, n
	}

	got, n := compilerMade(leftover(t, md8, md7, code))
	want := slices.Concat(always, listed)
	slices.Sort(want)
	if !slices.Equal(got, want) || n != 1 {
		t.Errorf("with the changes: %v and %d compiler-made, want %v and 1", got, n, want)
	}

	// With rows for the first two, the code-only ones go, and a row for an event
	// that is only built (B6) is not satisfied by it.
	mutated := mutateRows(t, md8, "| `replaced` | `HandedOver`",
		"| `replaced` | `HandedOver` | Instance | lifecycle controller |",
		"| `failing` | `LiteralMutantReason`, `NamedMutantReason` | Instance | lifecycle controller |",
		"| `degraded` | `ConstructedMutantReason` | Provider | managed catalog controller |")
	got, n = compilerMade(leftover(t, mutated, md7, code))
	want = slices.Concat(always, []string{key(reasondrift.TableOnly, "degraded", "ConstructedMutantReason", "Provider", "")})
	slices.Sort(want)
	if !slices.Equal(got, want) || n != 1 {
		t.Errorf("with rows for three of the reasons: %v and %d compiler-made, want %v and 1", got, n, want)
	}
}

// TestAnInterruptedTableFails (B3) breaks the table of HBEP-8 with a blank line
// and goes on with a row of it, and puts a row of it far from the table: the
// parser reads neither as a shorter table.
func TestAnInterruptedTableFails(t *testing.T) {
	md8, md7 := realDocs(t)
	if _, err := reasondrift.ParseDocs(md8, md7); err != nil {
		t.Fatal(err)
	}
	last := "| `ending-soon` | `NoReplacementReady` (proposed in PR #88) | Instance | lifecycle controller |"
	for name, mutated := range map[string][]byte{
		"a blank line, then a row": mutateRows(t, md8, "| `ending-soon` | `NoReplacementReady`",
			last, "", "| `failing` | `NeverEmitted` | Instance | lifecycle controller |"),
		"a row after text, far below": mutateRows(t, md8, "| `ending-soon` | `NoReplacementReady`",
			last, "", "Some text.", "", "| `failing` | `NeverEmitted` | Instance | lifecycle controller |"),
	} {
		if docs, err := reasondrift.ParseDocs(mutated, md7); err == nil {
			t.Errorf("%s: parsed %d entries, want an error", name, len(docs.Table))
		}
	}
}

// mutateRanking returns HBEP-7's text with old replaced by new on the line of
// the ranking paragraph only: the same names appear elsewhere in the HBEP.
func mutateRanking(t *testing.T, md7 []byte, old, new string) []byte {
	t.Helper()
	lines := strings.Split(string(md7), "\n")
	hit := 0
	for i, l := range lines {
		if strings.Contains(l, "**The ranking**") {
			hit++
			if !strings.Contains(l, old) {
				t.Fatalf("the ranking paragraph does not contain %q", old)
			}
			lines[i] = strings.Replace(l, old, new, 1)
		}
	}
	if hit != 1 {
		t.Fatalf("%d ranking paragraphs, want 1", hit)
	}
	return []byte(strings.Join(lines, "\n"))
}

// TestARankingThatDiffersFails are mutant checks of the HBEP-7 side: two names
// swapped, and one name removed.
func TestARankingThatDiffersFails(t *testing.T) {
	md8, md7 := realDocs(t)
	swapped := mutateRanking(t, md7, "`SpecUnsupported`, `PartitionTooShort`", "`PartitionTooShort`, `SpecUnsupported`")
	if got, want := leftover(t, md8, swapped, realScan(t)), []string{key(reasondrift.RankingOrder, "", "", "", "")}; !slices.Equal(got, want) {
		t.Errorf("with two names swapped: %v, want %v", got, want)
	}
	removed := mutateRanking(t, md7, "`SubmitLimitReached`, ", "")
	if got, want := leftover(t, md8, removed, realScan(t)), []string{key(reasondrift.RankingCodeOnly, "", "SubmitLimitReached", "", "")}; !slices.Equal(got, want) {
		t.Errorf("with a name removed: %v, want %v", got, want)
	}
}
