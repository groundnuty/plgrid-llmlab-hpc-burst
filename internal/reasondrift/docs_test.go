package reasondrift

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// repoRoot finds the repository root from the package directory.
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

// entry finds a table entry, failing the test when it is absent.
func entry(t *testing.T, table []TableEntry, action, reason string) TableEntry {
	t.Helper()
	for _, e := range table {
		if e.Action == action && e.Reason == reason {
			return e
		}
	}
	t.Fatalf("the table has no %s/%s", action, reason)
	return TableEntry{}
}

// TestParsesTheRealHBEPs reads the two HBEPs of this tree. The expectations
// below are written out from HBEP-8's "Reasons in alpha" table, its Observers
// table and HBEP-7's ranking paragraph, never taken from the parser or the
// code.
func TestParsesTheRealHBEPs(t *testing.T) {
	d, err := LoadDocs(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		action, reason string
		kinds          []string
	}{
		{"started", "AgentRegistered", []string{"Instance"}},
		// The ready row pairs its reasons with its Regarding cell by position.
		{"ready", "Ready", []string{"Instance"}},
		{"ready", "ShapesFit", []string{"ModelDeployment"}},
		{"degraded", "DrainWindowShort", []string{"PlatformProfile"}},
		{"degraded", "PathProbeFailing", []string{"Instance"}},
		{"degraded", "TunnelOrphaned", []string{"Allocation", "Lease"}},
		{"degraded", "ObservationGap", []string{"Lease"}},
		{"failing", "UsageWriteFailed", []string{"Lease"}},
		{"failing", "AllocationEndedEarly", []string{"Instance"}},
		// The Regarding cell of HeartbeatMissed points at the Observers table,
		// whose "Event when stale" column regards a Provider, an Instance and
		// the Leases.
		{"failing", "HeartbeatMissed", []string{"Instance", "Lease", "Provider"}},
		{"replaced", "HandedOver", []string{"Instance"}},
		{"ending-soon", "NoReplacementReady", []string{"Instance"}},
	}
	for _, w := range want {
		if got := entry(t, d.Table, w.action, w.reason); !slices.Equal(got.Kinds, w.kinds) {
			t.Errorf("%s/%s regards %v, want %v", w.action, w.reason, got.Kinds, w.kinds)
		}
	}
	// CapacityDropped sits in two rows (the lifecycle controller's and the
	// front's). Its kinds are not pinned: the erratum of #239 adds the Instance
	// to the lifecycle row, and this test must not fail on that.
	if e := entry(t, d.Table, "degraded", "CapacityDropped"); !slices.Contains(e.Kinds, "ModelDeployment") {
		t.Errorf("degraded/CapacityDropped regards %v, want ModelDeployment among them", e.Kinds)
	}
	// RestartLoop and VerificationFailed sit under two types.
	for _, r := range []string{"RestartLoop", "VerificationFailed"} {
		entry(t, d.Table, "degraded", r)
		entry(t, d.Table, "failing", r)
	}
	wantRanking := []string{
		"SpecUnsupported", "PartitionTooShort", "FrontNotConfigured", "VerificationFailed",
		"IdentityUnresolved", "RestartLoop", "SubmissionRejected", "NoOverlapSlot",
		"SubmitLimitReached", "ProviderUnavailable", "ReplacementLate", "CapacityDropped",
	}
	if !slices.Equal(d.Ranking, wantRanking) {
		t.Errorf("the ranking is %v, want %v", d.Ranking, wantRanking)
	}
}

const goodTable = `Intro.

**Reasons in alpha.** The reason strings are the contracts':

| Type | Reasons | Regarding | Emitter |
|---|---|---|---|
| ` + "`ready`" + ` | ` + "`Ready` (proposed in PR #1); `ShapesFit` (proposed, HBEP-6)" + ` | Instance; ModelDeployment | lifecycle controller |
| ` + "`failing`" + ` | ` + "`WorkerRestart`, `HeartbeatMissed`" + ` | the stale observer's resource (Observers) | the component that judges it |

- bullet

**Observers.** Each observer has a heartbeat:

| Observer | Rule | Event when stale | Judged by |
|---|---|---|---|
| One | a | ` + "`failing`, `HeartbeatMissed`, regarding the Provider" + ` | x |
| Two | b | ` + "`failing`, `HeartbeatMissed`, regarding the front's `Lease`" + ` | y |
`

func TestParsesAWellFormedTable(t *testing.T) {
	got, err := ParseEventTable([]byte(goodTable))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("%d entries, want 4: %+v", len(got), got)
	}
	if e := entry(t, got, "ready", "ShapesFit"); !slices.Equal(e.Kinds, []string{"ModelDeployment"}) {
		t.Errorf("ShapesFit regards %v, want [ModelDeployment]", e.Kinds)
	}
	if e := entry(t, got, "failing", "HeartbeatMissed"); !slices.Equal(e.Kinds, []string{"Lease", "Provider"}) {
		t.Errorf("HeartbeatMissed regards %v, want [Lease Provider]", e.Kinds)
	}
}

// TestMergesTheKindsOfARepeatedReason: a reason under one type in two rows
// regards the kinds of both rows.
func TestMergesTheKindsOfARepeatedReason(t *testing.T) {
	md := strings.Replace(goodTable, "| `failing` | `WorkerRestart`, `HeartbeatMissed` |",
		"| `ready` | `ShapesFit` | Lease | other emitter |\n| `failing` | `WorkerRestart`, `HeartbeatMissed` |", 1)
	got, err := ParseEventTable([]byte(md))
	if err != nil {
		t.Fatal(err)
	}
	if e := entry(t, got, "ready", "ShapesFit"); !slices.Equal(e.Kinds, []string{"Lease", "ModelDeployment"}) {
		t.Errorf("ShapesFit regards %v, want [Lease ModelDeployment]", e.Kinds)
	}
}

// TestParseEventTableIsStrict: a table it cannot read fails the parse. It never
// reads as empty (#237).
func TestParseEventTableIsStrict(t *testing.T) {
	rowReady := "| `ready` | `Ready` (proposed in PR #1); `ShapesFit` (proposed, HBEP-6) | Instance; ModelDeployment | lifecycle controller |"
	cases := []struct {
		name   string
		mutate func(string) string
		want   string // part of the error
	}{
		{"no marker", func(s string) string { return strings.Replace(s, "**Reasons in alpha.**", "**Reasons.**", 1) }, "no line contains"},
		{"marker twice", func(s string) string { return s + "\n**Reasons in alpha.** again\n" }, "more than once"},
		{"no table after the marker", func(s string) string {
			return strings.Replace(s, "\n| Type | Reasons | Regarding | Emitter |\n|---|---|---|---|", "\n\n\n\n\nx", 1)
		}, "no table within"},
		{"another header", func(s string) string { return strings.Replace(s, "| Emitter |", "| Who |", 1) }, "header is"},
		{"no separator", func(s string) string { return strings.Replace(s, "|---|---|---|---|", "| a | b | c | d |", 1) }, "separator line has the cell"},
		{"no rows", func(s string) string {
			a := strings.Index(s, "| `ready`")
			b := strings.Index(s, "- bullet")
			return s[:a] + "\n" + s[b:]
		}, "no row"},
		{"table that goes on after a blank line", func(s string) string {
			return strings.Replace(s, "\n\n- bullet", "\n\n| `failing` | `NeverEmitted` | Instance | x |\n\n- bullet", 1)
		}, "goes on after a blank line"},
		{"row of the table far below it", func(s string) string {
			return s + "\nSome text.\n\n| `failing` | `NeverEmitted` | Instance | x |\n"
		}, "lies outside it"},
		{"row of the table above it", func(s string) string {
			return "| `failing` | `NeverEmitted` | Instance | x |\n\n" + s
		}, "lies outside it"},
		{"observers table that goes on after a blank line", func(s string) string {
			return s + "\n| Three | c | `failing`, `HeartbeatMissed`, regarding the Provider | z |\n"
		}, "goes on after a blank line"},
		{"short row", func(s string) string { return strings.Replace(s, rowReady, "| `ready` | `Ready` | Instance |", 1) }, "3 cells"},
		{"unknown type", func(s string) string { return strings.Replace(s, "| `ready` |", "| `readyish` |", 1) }, "the type"},
		{"type without backticks", func(s string) string { return strings.Replace(s, "| `ready` |", "| ready |", 1) }, "the type"},
		{"reason without backticks", func(s string) string { return strings.Replace(s, "`WorkerRestart`", "WorkerRestart", 1) }, "not a reason name"},
		{"reason in lower case", func(s string) string { return strings.Replace(s, "`WorkerRestart`", "`workerRestart`", 1) }, "not a reason name"},
		{"empty reason", func(s string) string {
			return strings.Replace(s, "`WorkerRestart`, `HeartbeatMissed`", "`WorkerRestart`, ", 1)
		}, "not a reason name"},
		{"no kind", func(s string) string {
			return strings.Replace(s, "Instance; ModelDeployment", "the thing; the other", 1)
		}, "names none"},
		{"groups that do not pair", func(s string) string {
			return strings.Replace(s, rowReady, "| `ready` | `A`; `B`; `C` | Instance; Lease | x |", 1)
		}, "neither pair nor share"},
		{"observers pointer without an observers table", func(s string) string {
			return strings.Replace(s, "**Observers.**", "**Watchers.**", 1)
		}, "Observers table"},
		{"observers row without HeartbeatMissed", func(s string) string {
			return strings.Replace(s, "`failing`, `HeartbeatMissed`, regarding the Provider", "`failing`, `Other`, regarding the Provider", 1)
		}, "does not raise HeartbeatMissed"},
		{"observers row without a kind", func(s string) string {
			return strings.Replace(s, "regarding the Provider", "regarding it", 1)
		}, "names none"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if mutated := c.mutate(goodTable); mutated == goodTable {
				t.Fatal("the mutation changed nothing")
			}
			got, err := ParseEventTable([]byte(c.mutate(goodTable)))
			if err == nil {
				t.Fatalf("parsed %d entries, want an error containing %q", len(got), c.want)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("error %q lacks %q", err, c.want)
			}
		})
	}
}

const goodRanking = "Intro.\n" +
	"    **The ranking** (decision, 1 Nov): the first in this order, as `degradedOrder` on `main` does (`internal/lifecycle/status.go`): `A`, `B`, `C`. `main`'s order lacks `Z`;\n"

func TestParsesAWellFormedRanking(t *testing.T) {
	got, err := ParseRanking([]byte(goodRanking))
	if err != nil {
		t.Fatal(err)
	}
	// The names after the list (main, Z) are not part of it.
	if want := []string{"A", "B", "C"}; !slices.Equal(got, want) {
		t.Errorf("ranking %v, want %v", got, want)
	}
}

func TestParseRankingIsStrict(t *testing.T) {
	cases := []struct {
		name, text, want string
	}{
		{"no paragraph", "nothing here\n", "found 0"},
		{"two paragraphs", goodRanking + goodRanking, "found 2"},
		{"no list marker", strings.Replace(goodRanking, "status.go`):", "status.go`)", 1), "has no"},
		{"name without backticks", strings.Replace(goodRanking, "`B`", "B", 1), "expects a reason name"},
		{"missing separator", strings.Replace(goodRanking, "`B`, `C`", "`B` `C`", 1), "expects a comma or a period"},
		{"duplicate", strings.Replace(goodRanking, "`C`", "`A`", 1), "twice"},
		{"list never ends", strings.Replace(goodRanking, "`C`. `main`'s order lacks `Z`;", "`C`", 1), "expects a comma or a period"},
		{"empty list", strings.Replace(goodRanking, "`A`, `B`, `C`.", "", 1), "expects a reason name"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := ParseRanking([]byte(c.text))
			if err == nil {
				t.Fatalf("parsed %v, want an error containing %q", got, c.want)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("error %q lacks %q", err, c.want)
			}
		})
	}
}

const goodManaged = "intro\n" +
	"- The managed catalog controller's events take these types too. Their reasons are the condition reasons it writes: " +
	"`failing` for `A` and `B` (`CondOne`), `C` (`CondTwo`) and `D` (`Ready`); `degraded` for `E`, `F` and `G`; " +
	"and `ready` for `H`, `I`, `J` and `K` when a failing or degraded condition clears.\n" +
	"more\n"

// TestParsesTheManagedBullet: the expectations are written out from the bullet
// of HBEP-8 under its reasons table, never taken from the parser.
func TestParsesTheManagedBullet(t *testing.T) {
	got, err := ParseManaged([]byte(goodManaged))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"A": "failing", "B": "failing", "C": "failing", "D": "failing", "E": "degraded", "F": "degraded", "G": "degraded", "H": "ready", "I": "ready", "J": "ready", "K": "ready"}
	if len(got) != len(want) {
		t.Fatalf("%d entries, want %d: %v", len(got), len(want), got)
	}
	for _, e := range got {
		if want[e.Reason] != e.Action {
			t.Errorf("%s is %s, want %s", e.Reason, e.Action, want[e.Reason])
		}
	}
	// The real HBEP-8.
	md8, err := os.ReadFile(filepath.Join(repoRoot(t), HBEP8Path))
	if err != nil {
		t.Fatal(err)
	}
	real, err := ParseManaged(md8)
	if err != nil {
		t.Fatal(err)
	}
	wantReal := map[string]string{
		"FileRefused": "failing", "NoCredentialsFile": "failing", "KeyRejected": "failing", "Expired": "failing", "ReadsFailing": "failing",
		"ExpiresWithinADay": "degraded", "CatalogEntrySkipped": "degraded", "CatalogTruncated": "degraded",
		"FileRead": "ready", "KeyAccepted": "ready", "NotExpiring": "ready", "CatalogRead": "ready",
	}
	if len(real) != len(wantReal) {
		t.Errorf("HBEP-8 lists %d managed reasons: %v, want %d", len(real), real, len(wantReal))
	}
	for _, e := range real {
		if wantReal[e.Reason] != e.Action {
			t.Errorf("HBEP-8: %s is %s, want %s", e.Reason, e.Action, wantReal[e.Reason])
		}
	}
}

// TestParseManagedIsStrict: a bullet it cannot read is an error, never an empty
// set.
func TestParseManagedIsStrict(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(string) string
		want   string
	}{
		{"no bullet", func(s string) string {
			return strings.Replace(s, "The managed catalog controller's events", "The managed events", 1)
		}, "found 0"},
		{"two bullets", func(s string) string { return s + s }, "found 2"},
		{"no lead", func(s string) string {
			return strings.Replace(s, "reasons are the condition reasons it writes: ", "reasons: ", 1)
		}, "has no"},
		{"no period", func(s string) string { return strings.Replace(s, "clears.", "clears", 1) }, "does not end in a period"},
		{"a group that is not a type", func(s string) string { return strings.Replace(s, "`degraded` for", "`degraded`: ", 1) }, "not \"`type` for\""},
		{"an unknown type", func(s string) string { return strings.Replace(s, "`degraded` for", "`broken` for", 1) }, "not \"`type` for\""},
		{"a type twice", func(s string) string { return strings.Replace(s, "`degraded` for", "`failing` for", 1) }, "failing twice"},
		{"a name without backticks", func(s string) string { return strings.Replace(s, "`F`", "F", 1) }, "not reason names"},
		{"a name in lower case", func(s string) string { return strings.Replace(s, "`F`", "`f`", 1) }, "not reason names"},
		{"a reason twice", func(s string) string { return strings.Replace(s, "`F`", "`E`", 1) }, "E twice"},
		{"a separator that is none", func(s string) string { return strings.Replace(s, "`E`, `F`", "`E`; `F`", 1) }, "not \"`type` for\""},
		{"an empty list", func(s string) string {
			return strings.Replace(s, "`degraded` for `E`, `F` and `G`", "`degraded` for ", 1)
		}, "not"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mutated := c.mutate(goodManaged)
			if mutated == goodManaged {
				t.Fatal("the mutation changed nothing")
			}
			got, err := ParseManaged([]byte(mutated))
			if err == nil {
				t.Fatalf("parsed %d entries, want an error containing %q", len(got), c.want)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("error %q lacks %q", err, c.want)
			}
		})
	}
}
