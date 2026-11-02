package reasondrift_test

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
var knownDriftUntilHBEP8Erratum = []reasondrift.Entry{
	{Kind: reasondrift.NoConstant, Reason: "HandedOver", Why: "string literal in internal/lifecycle; the next commit names it"},
	{Kind: reasondrift.NoConstant, Reason: "NoReplacementReady", Why: "string literal in internal/lifecycle; the next commit names it"},
	{Kind: reasondrift.NoConstant, Reason: "Ready", Why: "string literal in internal/lifecycle; the next commit names it"},
	{Kind: reasondrift.CodeOnly, Action: "degraded", Reason: "CapacityDropped", Regarding: "Instance", Package: "internal/lifecycle", Why: "mdObject falls back to the Instance when the ModelDeployment is gone; the table lists ModelDeployment only"},
	{Kind: reasondrift.CodeOnly, Action: "degraded", Reason: "VerificationFailed", Regarding: "Instance", Package: "internal/lifecycle", Why: "mdObject falls back to the Instance when the ModelDeployment is gone; the table lists ModelDeployment only"},
}

// requiredPackages are the packages whose emit sites the guard reads. The scan
// loads ./..., and fails when one of these is missing.
var requiredPackages = []string{
	"api/v1alpha1", "cmd/hpcburst-controller", "internal/front", "internal/lifecycle",
	"internal/managed", "internal/providerctl", "pkg/provider",
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "go.mod")); err != nil {
		t.Fatalf("the repository root is not two levels up: %v", err)
	}
	return dir
}

func scanTree(t *testing.T, overlay map[string][]byte) *reasondrift.Code {
	t.Helper()
	code, err := scan.Scan(scan.Options{
		Root: repoRoot(t), Overlay: overlay, Wrappers: reasondrift.WrapperNames(), Require: requiredPackages,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Each scan holds the whole module's syntax and types; give it back before
	// the next one, so the test binary's peak is one scan's.
	runtime.GC()
	debug.FreeOSMemory()
	return code
}

// The scan of the unchanged tree takes seconds, so the tests share one.
var (
	realOnce sync.Once
	realCode *reasondrift.Code
)

func realScan(t *testing.T) *reasondrift.Code {
	t.Helper()
	realOnce.Do(func() { realCode = scanTree(t, nil) })
	if realCode == nil {
		t.Fatal("the scan of the tree failed in an earlier test")
	}
	return realCode
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
	for _, pkg := range []string{"internal/lifecycle", "internal/front", "cmd/hpcburst-controller", "pkg/provider", "internal/providerctl", "internal/managed"} {
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

// mutantEmits is a file added to internal/lifecycle through an overlay: three
// calls of the Recorder, with a reason written as a string literal, a reason a
// package-local constant names, and a reason no table row lists.
const mutantEmits = `package lifecycle

import "github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"

const namedMutantReason = "NamedMutantReason"

func (p *pass) mutantEmits(in *v1alpha1.Instance) {
	p.r.Recorder.Event(in, nil, eventFailing, "LiteralMutantReason", "m")
	p.r.Recorder.Event(in, nil, eventFailing, namedMutantReason, "m")
	p.r.Recorder.Event(in, nil, eventFailing, "UnlistedMutantReason", "m")
}
`

// TestEmitsAddedByAnOverlayAreCaught is a mutant check (#239): new calls of the
// lifecycle Recorder, added through go/packages' overlay, appear as the
// mismatches they cause and no others. One scan serves both cases.
func TestEmitsAddedByAnOverlayAreCaught(t *testing.T) {
	overlay := map[string][]byte{filepath.Join(repoRoot(t), "internal/lifecycle/zz_mutant_overlay.go"): []byte(mutantEmits)}
	code := scanTree(t, overlay)
	md8, md7 := realDocs(t)

	// Not in the table: each is an emit the table lacks.
	want := []string{
		key(reasondrift.CodeOnly, "failing", "LiteralMutantReason", "Instance", "internal/lifecycle"),
		key(reasondrift.CodeOnly, "failing", "NamedMutantReason", "Instance", "internal/lifecycle"),
		key(reasondrift.CodeOnly, "failing", "UnlistedMutantReason", "Instance", "internal/lifecycle"),
	}
	slices.Sort(want)
	if got := leftover(t, md8, md7, code); !slices.Equal(got, want) {
		t.Errorf("with three emits added: %v, want %v", got, want)
	}

	// With rows for the first two, what is left is the literal, which has no
	// constant in api/v1alpha1 (a constant named in the emitter's package is
	// fine, `@hpc-burst-science-agent`, 2 Nov 2026), and the unlisted reason.
	mutated := mutateRows(t, md8, "| `replaced` | `HandedOver`",
		"| `replaced` | `HandedOver` | Instance | lifecycle controller |",
		"| `failing` | `LiteralMutantReason`, `NamedMutantReason` | Instance | lifecycle controller |")
	want = []string{
		key(reasondrift.NoConstant, "", "LiteralMutantReason", "", ""),
		key(reasondrift.CodeOnly, "failing", "UnlistedMutantReason", "Instance", "internal/lifecycle"),
	}
	slices.Sort(want)
	if got := leftover(t, mutated, md7, code); !slices.Equal(got, want) {
		t.Errorf("with rows for two of them: %v, want %v", got, want)
	}
}

// TestAnEmitThroughAnUnknownRecorderFails: a call to Event through a recorder
// the scan does not know fails the scan itself.
func TestAnEmitThroughAnUnknownRecorderFails(t *testing.T) {
	src := "package lifecycle\n\ntype mutantRecorder interface{ Event(string) }\n\nfunc mutantRelay(m mutantRecorder) { m.Event(\"X\") }\n"
	overlay := map[string][]byte{filepath.Join(repoRoot(t), "internal/lifecycle/zz_mutant_overlay.go"): []byte(src)}
	_, err := scan.Scan(scan.Options{Root: repoRoot(t), Overlay: overlay, Wrappers: reasondrift.WrapperNames(), Require: requiredPackages})
	if err == nil || !strings.Contains(err.Error(), "mutantRelay") {
		t.Errorf("error %v, want one that names mutantRelay", err)
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
