package reasondrift

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// The OpenTelemetry mapping's parsers (HBEP-8, Test plan, The OpenTelemetry
// mapping). The fixtures are Markdown written by hand here, with invented
// metric names; the real HBEP-8 is parsed in TestParsesTheRealOTelMapping.

// otelDoc is a good document: the marker, its bullets, the first table with a
// prose row, and the second table.
const otelDoc = `# Metrics

**OpenTelemetry (beta)** (open question 1). The mapping, in two tables:
- **A bullet.** Some text about status, with ` + "`hpcburst_`" + ` in it.
- **Another bullet.** More text.

| ` + "`hpcburst_`" + ` metric | OpenTelemetry metric (instrument, unit) | Differences |
|---|---|---|
| ` + "`hpcburst_front_alpha_seconds`" + ` | ` + "`gen_ai.server.alpha`" + ` (Histogram, ` + "`s`" + `) | see ` + "`hpcburst_other_total`" + ` |
| ` + "`hpcburst_instance_beta_total`" + ` | none | |
| every other ` + "`hpcburst_`" + ` series: running and waiting requests | none at the pinned commit | |

| Label or usage field (HBEP-5, Usage records) | OpenTelemetry attribute | Differences |
|---|---|---|
| the ` + "`endpoint`" + ` label; the field ` + "`path`" + ` | ` + "`gen_ai.operation.name`" + ` | x |
| ` + "`requestedModel`" + ` | ` + "`gen_ai.request.model`" + ` | as sent |
| the ` + "`model_deployment`" + ` and ` + "`instance_uid`" + ` labels | none | |
| the ` + "`provider`" + ` label | none | below |

### Usage records

Text.
`

// firstTableOnly is otelDoc without the second table.
func firstTableOnly(s string) string {
	return s[:strings.Index(s, "| Label or usage field")] + "### Usage records\n"
}

func TestParseOTelTableGood(t *testing.T) {
	got, err := ParseOTelTable([]byte(otelDoc))
	if err != nil {
		t.Fatal(err)
	}
	// The name in the third column and the prose row's bare prefix are not names.
	if want := []string{"hpcburst_front_alpha_seconds", "hpcburst_instance_beta_total"}; !slices.Equal(got, want) {
		t.Errorf("names %v, want %v", got, want)
	}
	labels, err := ParseOTelLabels([]byte(otelDoc))
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"endpoint", "model_deployment", "instance_uid", "provider"}; !slices.Equal(labels, want) {
		t.Errorf("labels %v, want %v", labels, want)
	}
}

func TestParseOTelTableRefusals(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(string) string
		want string
	}{
		{"no marker", func(s string) string { return strings.Replace(s, "**OpenTelemetry (beta)**", "**OpenTelemetry**", 1) }, "no line contains"},
		{"marker twice", func(s string) string { return s + "\n**OpenTelemetry (beta)** again\n" }, "more than once"},
		{"a heading before the table", func(s string) string {
			return strings.Replace(s, "\n| `hpcburst_` metric", "\n### Elsewhere\n\n| `hpcburst_` metric", 1)
		}, "no table between"},
		{"no table at all", func(s string) string { return s[:strings.Index(s, "| `hpcburst_` metric")] }, "no table after"},
		{"the other table first", func(s string) string {
			a := strings.Index(s, "| `hpcburst_` metric")
			b := strings.Index(s, "| Label or usage field")
			c := strings.Index(s, "### Usage records")
			return s[:a] + s[b:c] + s[a:b] + s[c:]
		}, "first header cell"},
		{"another first header", func(s string) string { return strings.Replace(s, "| `hpcburst_` metric |", "| Metric |", 1) }, "first header cell"},
		{"no separator", func(s string) string {
			return strings.Replace(s, "|---|---|---|\n| `hpcburst_front", "| a | b | c |\n| `hpcburst_front", 1)
		}, "separator line has the cell"},
		{"no rows", func(s string) string {
			a := strings.Index(s, "| `hpcburst_front_alpha")
			b := strings.Index(s, "| Label or usage field")
			return s[:a] + "\n" + s[b:]
		}, "the table has no row"},
		{"no name in any row", func(s string) string {
			a := strings.Index(s, "| `hpcburst_front_alpha")
			b := strings.Index(s, "| every other")
			return s[:a] + s[b:]
		}, "names no hpcburst_ metric"},
		{"a short row", func(s string) string {
			return strings.Replace(s, "| `hpcburst_instance_beta_total` | none | |", "| `hpcburst_instance_beta_total` | none |", 1)
		}, "row 2 has 2 cells, want 3"},
		{"a name in upper case", func(s string) string {
			return strings.Replace(s, "hpcburst_front_alpha_seconds", "hpcburst_Front_alpha_seconds", 1)
		}, "is no metric name"},
		{"a name with a hyphen", func(s string) string {
			return strings.Replace(s, "hpcburst_front_alpha_seconds", "hpcburst_front-alpha_seconds", 1)
		}, "is no metric name"},
		{"a name with a dot", func(s string) string {
			return strings.Replace(s, "hpcburst_instance_beta_total", "hpcburst_instance.beta_total", 1)
		}, "is no metric name"},
		{"a name twice", func(s string) string {
			return strings.Replace(s, "hpcburst_instance_beta_total", "hpcburst_front_alpha_seconds", 1)
		}, "twice"},
		// No row is skipped silently (Fable's note 1 on #633): a name outside
		// backticks, or a row that names no metric and is not the prose row,
		// would otherwise drop out of the export checks unseen.
		{"a name outside backticks", func(s string) string {
			return strings.Replace(s, "| `hpcburst_instance_beta_total` |", "| hpcburst_instance_beta_total |", 1)
		}, "hpcburst_instance_beta_total is not backticked"},
		{"a name outside backticks beside one in them", func(s string) string {
			return strings.Replace(s, "| `hpcburst_instance_beta_total` |", "| `hpcburst_instance_beta_total` and hpcburst_instance_gamma_total |", 1)
		}, "hpcburst_instance_gamma_total is not backticked"},
		{"a row that names no metric", func(s string) string {
			return strings.Replace(s, "| `hpcburst_instance_beta_total` |", "| the instance's beta series |", 1)
		}, "row 2 names no backticked hpcburst_ metric"},
		{"a second prose row", func(s string) string {
			return strings.Replace(s, "| every other `hpcburst_` series: running and waiting requests | none at the pinned commit | |",
				"| every other `hpcburst_` series: running and waiting requests | none at the pinned commit | |\n| every other `hpcburst_` series: more | none | |", 1)
		}, "a second row that starts"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseOTelTable([]byte(tc.edit(otelDoc)))
			if err == nil {
				t.Fatalf("parsed %v, want an error that has %q", got, tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q lacks %q", err, tc.want)
			}
			if got != nil {
				t.Errorf("an error with the result %v: a table it cannot read must give no names", got)
			}
		})
	}
}

// TestParseOTelTableTakesTheFirstTable: the second table follows the first after
// a blank line, which the reasons table's parser would refuse as a table that
// goes on; here it is a table of its own, and its backticked hpcburst_ tokens
// are not metric names.
func TestParseOTelTableTakesTheFirstTable(t *testing.T) {
	doc := strings.Replace(otelDoc, "| the `provider` label | none | below |", "| the `provider` label | none: `hpcburst.provider` | `hpcburst_in_the_second_table` |", 1)
	got, err := ParseOTelTable([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(got, "hpcburst_in_the_second_table") || len(got) != 2 {
		t.Errorf("names %v, want the first table's two", got)
	}
}

func TestParseOTelLabelsRefusals(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(string) string
		want string
	}{
		{"no marker", func(s string) string { return strings.Replace(s, "**OpenTelemetry (beta)**", "**x**", 1) }, "no line contains"},
		{"no second table", firstTableOnly, "does not follow"},
		{"another header", func(s string) string {
			return strings.Replace(s, "| Label or usage field (HBEP-5, Usage records) |", "| Field |", 1)
		}, "first header cell"},
		{"a short row", func(s string) string {
			return strings.Replace(s, "| the `provider` label | none | below |", "| the `provider` label | none |", 1)
		}, "has 2 cells, want 3"},
		{"a label in upper case", func(s string) string { return strings.Replace(s, "the `provider` label", "the `Provider` label", 1) }, "is no label name"},
		{"a group that is no list", func(s string) string {
			return strings.Replace(s, "the `model_deployment` and `instance_uid` labels", "the `model_deployment` or `instance_uid` labels", 1)
		}, "says label but is not one of the forms"},
		// A group that says label in another form is refused, never read as a
		// usage field, whose labels nobody would then check (Fable's note 4 on
		// #633).
		{"a label with a note after it", func(s string) string {
			return strings.Replace(s, "the `provider` label |", "the `provider` label (HBEP-3) |", 1)
		}, "says label but is not one of the forms"},
		{"the word label first", func(s string) string {
			return strings.Replace(s, "the `provider` label |", "label `provider` |", 1)
		}, "says label but is not one of the forms"},
		{"labels without the", func(s string) string {
			return strings.Replace(s, "the `model_deployment` and `instance_uid` labels", "`model_deployment` and `instance_uid` labels", 1)
		}, "says label but is not one of the forms"},
		{"a label twice", func(s string) string { return strings.Replace(s, "the `provider` label", "the `endpoint` label", 1) }, "twice"},
		{"no label at all", func(s string) string {
			s = strings.Replace(s, "the `endpoint` label; the field `path`", "the field `path`", 1)
			s = strings.Replace(s, "the `model_deployment` and `instance_uid` labels", "`other`", 1)
			return strings.Replace(s, "the `provider` label", "`more`", 1)
		}, "maps no label"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseOTelLabels([]byte(tc.edit(otelDoc)))
			if err == nil {
				t.Fatalf("parsed %v, want an error that has %q", got, tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q lacks %q", err, tc.want)
			}
		})
	}
}

// TestParsesTheRealOTelMapping reads HBEP-8 of this tree. The names are not
// pinned here: the export checks (internal/front and internal/lifecycle) read
// them from the document, so an amendment that maps one more metric must not
// need an edit to this test, and a list copied from the table would prove
// nothing there. What is pinned is what those checks depend on. The parse
// succeeds, which means the marker, the first table, its header and each name
// are well-formed; every name starts with hpcburst_front_ or hpcburst_instance_,
// the two prefixes the export checks cover, so a name mapped under any other
// prefix fails here until a check covers it; each prefix has a name, or its
// check would have nothing to read; and the second table maps the labels (read
// from HBEP-8's Front metrics table and Engine gauges section) that those
// checks look for on the series.
func TestParsesTheRealOTelMapping(t *testing.T) {
	names, err := LoadOTelNames(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for _, n := range names {
		switch {
		case strings.HasPrefix(n, "hpcburst_front_"):
			seen["hpcburst_front_"]++
		case strings.HasPrefix(n, "hpcburst_instance_"):
			seen["hpcburst_instance_"]++
		default:
			t.Errorf("%s starts neither hpcburst_front_ nor hpcburst_instance_: no export check covers it, so add one before this name is mapped", n)
		}
	}
	for _, prefix := range []string{"hpcburst_front_", "hpcburst_instance_"} {
		if seen[prefix] == 0 {
			t.Errorf("the mapping names no %s series (it names %v), so that export check would read nothing", prefix, names)
		}
	}
	// Every mapped label is required by one export check, and every label a
	// check requires is mapped: the checks then prove each mapped label
	// exported (Fable's note 4 on #633).
	labels, err := LoadOTelLabels(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range UnassignedOTelLabels(labels, OTelFrontLabels, OTelInstanceLabels) {
		t.Error(p)
	}
}

// TestUnassignedOTelLabels: a label the mapping newly maps, with no check that
// requires it, is named; so is a label a check requires that the mapping does
// not map. The new label is added to a copy of the real HBEP-8's second table.
func TestUnassignedOTelLabels(t *testing.T) {
	md := string(readHBEP8(t))
	row := "| the `provider` label |"
	if !strings.Contains(md, row) {
		t.Fatalf("HBEP-8's second table has no row starting %q to add a label beside", row)
	}
	more := strings.Replace(md, row, "| the `worker` label | none | |\n"+row, 1)
	labels, err := ParseOTelLabels([]byte(more))
	if err != nil {
		t.Fatal(err)
	}
	got := UnassignedOTelLabels(labels, OTelFrontLabels, OTelInstanceLabels)
	if len(got) != 1 || !strings.Contains(got[0], "maps the label worker, which no export check requires") {
		t.Errorf("a newly mapped label gives %q; want one line naming worker", got)
	}
	mapped, err := ParseOTelLabels([]byte(md))
	if err != nil {
		t.Fatal(err)
	}
	got = UnassignedOTelLabels(mapped, OTelFrontLabels, OTelInstanceLabels, []string{"worker"})
	if len(got) != 1 || !strings.Contains(got[0], "requires the label worker, which the mapping does not map") {
		t.Errorf("a required label the mapping lacks gives %q; want one line naming worker", got)
	}
	if got := UnassignedOTelLabels([]string{"a", "b"}, []string{"b"}, []string{"a"}); len(got) != 0 {
		t.Errorf("a full assignment gives %q, want none", got)
	}
}

// TestParseOTelTableOfARenamedName: the mapping's names are read from the
// document, so a name renamed in the text is what the export checks then look
// for. Renamed here in a copy of the real HBEP-8's text, the parser returns the
// new name and not the old.
func TestParseOTelTableOfARenamedName(t *testing.T) {
	md := readHBEP8(t)
	renamed := strings.Replace(string(md), "| `hpcburst_instance_generation_tokens_total` |", "| `hpcburst_instance_generated_tokens_total` |", 1)
	if renamed == string(md) {
		t.Fatal("the real HBEP-8 has no row to rename")
	}
	got, err := ParseOTelTable([]byte(renamed))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(got, "hpcburst_instance_generated_tokens_total") || slices.Contains(got, "hpcburst_instance_generation_tokens_total") {
		t.Errorf("names %v after the rename", got)
	}
}

// readHBEP8 reads the real HBEP-8 of this tree.
func readHBEP8(t *testing.T) []byte {
	t.Helper()
	md, err := os.ReadFile(filepath.Join(repoRoot(t), HBEP8Path))
	if err != nil {
		t.Fatal(err)
	}
	return md
}

const exposition = `# HELP hpcburst_front_a_total A counter.
# TYPE hpcburst_front_a_total counter
hpcburst_front_a_total{code="200"} 3
# HELP hpcburst_front_h_seconds A histogram.
# TYPE hpcburst_front_h_seconds histogram
hpcburst_front_h_seconds_bucket{endpoint="chat",le="1"} 1
hpcburst_front_h_seconds_bucket{endpoint="chat",le="+Inf"} 1
hpcburst_front_h_seconds_sum{endpoint="chat"} 0.5
hpcburst_front_h_seconds_count{endpoint="chat"} 1
# HELP hpcburst_instance_s_seconds A summary.
# TYPE hpcburst_instance_s_seconds summary
hpcburst_instance_s_seconds_sum{instance_uid="u"} 1.5
hpcburst_instance_s_seconds_count{instance_uid="u"} 30
# HELP hpcburst_front_empty_total A vector with no series.
# TYPE hpcburst_front_empty_total counter
`

func TestMissingFamilies(t *testing.T) {
	for _, tc := range []struct {
		name  string
		names []string
		want  []string
	}{
		{"every family present, a histogram and a summary by base name",
			[]string{"hpcburst_front_a_total", "hpcburst_front_h_seconds", "hpcburst_instance_s_seconds"}, nil},
		{"a name that is no family", []string{"hpcburst_front_a_total", "hpcburst_front_nonexistent_total"}, []string{"hpcburst_front_nonexistent_total"}},
		{"a _sum line is not a family name", []string{"hpcburst_front_h_seconds_sum", "hpcburst_instance_s_seconds_count"},
			[]string{"hpcburst_front_h_seconds_sum", "hpcburst_instance_s_seconds_count"}},
		{"a family with no series", []string{"hpcburst_front_empty_total"}, []string{"hpcburst_front_empty_total"}},
		{"sorted", []string{"hpcburst_z_total", "hpcburst_a_total"}, []string{"hpcburst_a_total", "hpcburst_z_total"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := MissingFamilies([]byte(exposition), tc.names)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("missing %v, want %v", got, tc.want)
			}
		})
	}
	if got, err := MissingFamilies([]byte("not a metric line\n"), []string{"x"}); err == nil {
		t.Errorf("a body that is no exposition gave %v and no error", got)
	}
}

func TestSeriesMissingLabels(t *testing.T) {
	got, err := SeriesMissingLabels([]byte(exposition), "hpcburst_front_h_seconds", []string{"endpoint"})
	if err != nil || len(got) != 0 {
		t.Errorf("the labels every series has: %v, %v", got, err)
	}
	got, err = SeriesMissingLabels([]byte(exposition), "hpcburst_front_h_seconds", []string{"endpoint", "provider"})
	if err != nil || len(got) != 1 || !strings.Contains(got[0], "no label provider") {
		t.Errorf("a label no series has: %v, %v", got, err)
	}
	got, err = SeriesMissingLabels([]byte(exposition), "hpcburst_front_nonexistent_total", []string{"endpoint"})
	if err != nil || len(got) != 1 || !strings.Contains(got[0], "no series") {
		t.Errorf("a family that is absent: %v, %v", got, err)
	}
	got, err = SeriesMissingLabels([]byte(exposition), "hpcburst_front_empty_total", []string{"endpoint"})
	if err != nil || len(got) != 1 || !strings.Contains(got[0], "no series") {
		t.Errorf("a family with no series: %v, %v", got, err)
	}
}
