package lifecycle

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/reasondrift"
	afake "github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi/fake"
)

// HBEP-8, Test plan, The OpenTelemetry mapping: every hpcburst_ name in the
// mapping's first column is exported by a serving lifecycle copy or by the
// front. This file is the lifecycle controller's half: the names come from
// HBEP-8's table through reasondrift.LoadOTelNames, the controller's are those
// that start hpcburst_instance_, and a serving collector's /metrics text, with
// one Instance whose report is full and fresh, must have a metric family for
// each (a summary or histogram counts by its family name, its _sum and _count
// under it). No name is written into this file but the invented one of the
// control: a list copied from the table would prove nothing, and neither would
// a check against the collector's descriptors rather than the text it serves.
// reasondrift's own test requires every mapped name to start with
// hpcburst_front_ or hpcburst_instance_, so a name with any other prefix fails
// there until a check covers it.

const instanceOTelPrefix = "hpcburst_instance_"

// otelRepoRoot finds the repository root from the package directory.
func otelRepoRoot(t *testing.T) string {
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

// otelInstanceNames returns the names the lifecycle controller should export,
// failing the test when the mapping names none.
func otelInstanceNames(t *testing.T, names []string) []string {
	t.Helper()
	var own []string
	for _, n := range names {
		if strings.HasPrefix(n, instanceOTelPrefix) {
			own = append(own, n)
		}
	}
	if len(own) == 0 {
		t.Fatalf("HBEP-8's mapping names no %s series (it names %v): the lifecycle half of the check would prove nothing", instanceOTelPrefix, names)
	}
	return own
}

// servingScrapeWithAFullReport is the /metrics text of a serving collector over
// one Instance whose engine report has every field known and fresh (the fixture
// of TestMetricsFreshEngineGauges).
func servingScrapeWithAFullReport(t *testing.T) []byte {
	t.Helper()
	rg, _, _ := healthRig(t, func() afake.Engine { return afake.Engine{Health: knownHealth} })
	return scrapeBody(t, rg.collector(always))
}

// TestOTelMappingInstanceNamesAreExported: a serving collector's /metrics, with
// a full fresh report, has a family for every hpcburst_instance_ name of the
// mapping, and every series of those families carries the labels the mapping's
// second table maps and the Engine gauges give them.
func TestOTelMappingInstanceNamesAreExported(t *testing.T) {
	root := otelRepoRoot(t)
	names, err := reasondrift.LoadOTelNames(root)
	if err != nil {
		t.Fatal(err)
	}
	own := otelInstanceNames(t, names)
	body := servingScrapeWithAFullReport(t)

	missing, err := reasondrift.MissingFamilies(body, own)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 0 {
		t.Errorf("the serving collector's /metrics has no series of %v, which HBEP-8's OpenTelemetry mapping names", missing)
	}

	mapped, err := reasondrift.LoadOTelLabels(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range reasondrift.OTelInstanceLabels {
		if !slices.Contains(mapped, l) {
			t.Errorf("the label %s is not among the labels %v that HBEP-8's mapping maps", l, mapped)
		}
	}
	for _, n := range own {
		problems, err := reasondrift.SeriesMissingLabels(body, n, reasondrift.OTelInstanceLabels)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range problems {
			t.Error(p)
		}
	}
}

// TestOTelMappingCheckFailsWhenAnInstanceNameIsNotExported is the control of the
// check above: it fails for a name the collector does not export. A name
// invented here joins the mapping's real hpcburst_instance_ names, and the
// helper must report exactly it. And a copy of the real HBEP-8 with one mapped
// name renamed, read by the same parser, makes the helper report the new name,
// so the names reach the check from the document and are not a list.
func TestOTelMappingCheckFailsWhenAnInstanceNameIsNotExported(t *testing.T) {
	root := otelRepoRoot(t)
	names, err := reasondrift.LoadOTelNames(root)
	if err != nil {
		t.Fatal(err)
	}
	own := otelInstanceNames(t, names)
	body := servingScrapeWithAFullReport(t)

	const invented = "hpcburst_instance_nonexistent_total"
	missing, err := reasondrift.MissingFamilies(body, append(slices.Clone(own), invented))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(missing, []string{invented}) {
		t.Errorf("with %s added to the real names, the helper reports %v, want only it", invented, missing)
	}

	md, err := os.ReadFile(filepath.Join(root, reasondrift.HBEP8Path))
	if err != nil {
		t.Fatal(err)
	}
	renamed := own[0] + "_renamed"
	mutated := strings.ReplaceAll(string(md), "`"+own[0]+"`", "`"+renamed+"`")
	if mutated == string(md) {
		t.Fatalf("HBEP-8 has no %s in backticks to rename", own[0])
	}
	mutatedNames, err := reasondrift.ParseOTelTable([]byte(mutated))
	if err != nil {
		t.Fatal(err)
	}
	missing, err = reasondrift.MissingFamilies(body, otelInstanceNames(t, mutatedNames))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(missing, []string{renamed}) {
		t.Errorf("with %s renamed in HBEP-8's text, the helper reports %v, want [%s]", own[0], missing, renamed)
	}
}
