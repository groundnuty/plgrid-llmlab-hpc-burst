package reasondrift_test

import (
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/reasondrift"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/reasondrift/scan"
)

// strict ignores KnownDrift, the temporary allowlist of the drift that exists
// today: with REASONDRIFT_STRICT=1 the guard fails on today's tree, which is
// what it is for. Delete the switch with KnownDrift when the erratum has landed.
var strict = os.Getenv("REASONDRIFT_STRICT") == "1"

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

func allowances() [][]reasondrift.Entry {
	if strict {
		return [][]reasondrift.Entry{reasondrift.Exceptions}
	}
	return [][]reasondrift.Entry{reasondrift.Exceptions, reasondrift.KnownDrift}
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
// The only allowances are Exceptions (true to the design) and KnownDrift (the
// drift the erratum removes). An entry that matches nothing is stale.
func TestHBEPsMatchTheCode(t *testing.T) {
	docs, err := reasondrift.LoadDocs(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	code := realScan(t)
	left, stale := reasondrift.Apply(reasondrift.Compare(docs, code), allowances()...)
	for _, m := range left {
		t.Errorf("drift: %s", m)
	}
	for _, e := range stale {
		t.Errorf("stale allowance, remove it: %s", e)
	}
	if len(left) > 0 && strict {
		t.Logf("REASONDRIFT_STRICT=1: KnownDrift is ignored, so this is the drift of today's tree")
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

// TestEveryAllowanceStatesItsReason: an entry with no Why, or with no field to
// match on (which would allow everything), is refused.
func TestEveryAllowanceStatesItsReason(t *testing.T) {
	for _, list := range [][]reasondrift.Entry{reasondrift.Exceptions, reasondrift.KnownDrift} {
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
	left, _ := reasondrift.Apply(reasondrift.Compare(docs, code), reasondrift.Exceptions, reasondrift.KnownDrift)
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

// TestAnAddedRowFails is a mutant check: a row for a reason no code emits and
// no constant holds adds exactly the two mismatches that says.
func TestAnAddedRowFails(t *testing.T) {
	md8, md7 := realDocs(t)
	mutated := mutateRows(t, md8, "| `replaced` | `HandedOver`",
		"| `replaced` | `HandedOver` (proposed in PR #88) | Instance | lifecycle controller |",
		"| `failing` | `ReasonOfTheMutant` | Instance | lifecycle controller |")
	want := []string{
		key(reasondrift.NoConstant, "", "ReasonOfTheMutant", "", ""),
		key(reasondrift.TableOnly, "failing", "ReasonOfTheMutant", "Instance", ""),
	}
	// HandedOver stays in the table (it is KnownDrift's), so only the two new
	// mismatches are left.
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

// TestAnAddedEmitFails is a mutant check: a new call to the lifecycle
// Recorder with a new reason, added to internal/lifecycle through an overlay,
// appears as exactly one mismatch.
func TestAnAddedEmitFails(t *testing.T) {
	src := `package lifecycle

import "github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"

func (p *pass) mutantEmit(in *v1alpha1.Instance) {
	p.r.Recorder.Event(in, nil, eventFailing, "ReasonOfTheMutant", "m")
}
`
	overlay := map[string][]byte{filepath.Join(repoRoot(t), "internal/lifecycle/zz_mutant_overlay.go"): []byte(src)}
	md8, md7 := realDocs(t)
	want := []string{key(reasondrift.CodeOnly, "failing", "ReasonOfTheMutant", "Instance", "internal/lifecycle")}
	if got := leftover(t, md8, md7, scanTree(t, overlay)); !slices.Equal(got, want) {
		t.Errorf("with an emit added: %v, want %v", got, want)
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
