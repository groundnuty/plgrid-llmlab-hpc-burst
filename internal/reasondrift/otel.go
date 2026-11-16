package reasondrift

// HBEP-8's OpenTelemetry mapping (open question 1, beta) and its check (Test
// plan, The OpenTelemetry mapping): every hpcburst_ name in the first column of
// the mapping's first table is exported by a serving lifecycle copy or by the
// front. The names are read from the document here, strictly like the other
// tables, and the export tests in internal/front and internal/lifecycle compare
// them with a real /metrics scrape through MissingFamilies. A list of names
// written into a test would prove nothing: the table is where they come from.

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

const (
	// otelMarker is the bold lead-in of HBEP-8's OpenTelemetry paragraph. The
	// mapping's two tables follow its bullets.
	otelMarker = "**OpenTelemetry (beta)**"
	// otelNamesHeader is the first header cell of the table of metric names, and
	// otelLabelsHeader the start of the first header cell of the table of labels
	// and usage fields. They tell the two tables apart.
	otelNamesHeader  = "`hpcburst_` metric"
	otelLabelsHeader = "Label or usage field"
)

var (
	backticked = regexp.MustCompile("`([^`]*)`")
	// otelName is the shape of a metric name in the first table's first column.
	// The prose row ("every other `hpcburst_` series: ...") names the prefix
	// alone, "hpcburst_" with nothing after the underscore, so it matches no
	// name and is no error.
	otelName = regexp.MustCompile(`^hpcburst_[a-z0-9_]+$`)
	// otelLabel is the shape of a label name, as Prometheus's text format has it.
	otelLabel = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	// otelLabelGroup is a first-column group of the second table that names
	// labels, "the `a` label" or "the `a` and `b` labels", once each backticked
	// token is replaced by X.
	otelLabelGroup = regexp.MustCompile(`^the X(?:(?:, and |, | and )X)* labels?$`)
)

// LoadOTelNames reads HBEP-8 under the repository root and returns the
// hpcburst_ names of the OpenTelemetry mapping's first column.
func LoadOTelNames(root string) ([]string, error) {
	md, err := os.ReadFile(filepath.Join(root, HBEP8Path))
	if err != nil {
		return nil, fmt.Errorf("reading HBEP-8: %w", err)
	}
	names, err := ParseOTelTable(md)
	if err != nil {
		return nil, fmt.Errorf("HBEP-8: %w", err)
	}
	return names, nil
}

// LoadOTelLabels reads HBEP-8 under the repository root and returns the labels
// the OpenTelemetry mapping's second table maps.
func LoadOTelLabels(root string) ([]string, error) {
	md, err := os.ReadFile(filepath.Join(root, HBEP8Path))
	if err != nil {
		return nil, fmt.Errorf("reading HBEP-8: %w", err)
	}
	labels, err := ParseOTelLabels(md)
	if err != nil {
		return nil, fmt.Errorf("HBEP-8: %w", err)
	}
	return labels, nil
}

// ParseOTelTable returns, in the table's order, every backticked token of the
// form hpcburst_<lower case, digits and underscores> in the first column of the
// first table after the line holding "**OpenTelemetry (beta)**". It returns an
// error for a table it cannot read: a missing or repeated marker, no table
// before the next heading, a first header cell other than "`hpcburst_` metric",
// a row of the wrong width, a first-column token that starts hpcburst_ and has
// more after it but is no valid name (a typo that would drop out of the check),
// a name twice, or no name at all, never an empty result.
func ParseOTelTable(md []byte) ([]string, error) {
	header, rows, _, err := otelFirstTable(strings.Split(string(md), "\n"))
	if err != nil {
		return nil, err
	}
	if header[0] != otelNamesHeader {
		return nil, fmt.Errorf("the OpenTelemetry mapping's first table has the first header cell %q, want %q", header[0], otelNamesHeader)
	}
	var names []string
	for n, row := range rows {
		if len(row) != len(header) {
			return nil, fmt.Errorf("the OpenTelemetry mapping's first table, row %d has %d cells, want %d: %q", n+1, len(row), len(header), clip(strings.Join(row, "|")))
		}
		for _, m := range backticked.FindAllStringSubmatch(row[0], -1) {
			tok := m[1]
			switch {
			case otelName.MatchString(tok):
				if slices.Contains(names, tok) {
					return nil, fmt.Errorf("the OpenTelemetry mapping names %s twice", tok)
				}
				names = append(names, tok)
			case strings.HasPrefix(tok, "hpcburst_") && tok != "hpcburst_":
				return nil, fmt.Errorf("the OpenTelemetry mapping's first table, row %d: %q is no metric name (hpcburst_ and lower case letters, digits and underscores)", n+1, tok)
			}
		}
	}
	if len(names) == 0 {
		return nil, errors.New("the OpenTelemetry mapping's first table names no hpcburst_ metric")
	}
	return names, nil
}

// ParseOTelLabels returns, in the table's order, the labels the second table
// of the OpenTelemetry mapping maps to attributes: the backticked names of its
// first-column groups that read "the `a` label" or "the `a` and `b` labels".
// The other groups name usage fields and are skipped. The second table is the
// one that follows the first, after blank lines, and its first header cell
// starts "Label or usage field". It returns an error for a table it cannot
// read, a row of the wrong width, a group that reads as labels and is not
// well-formed, a label twice, or no label at all.
func ParseOTelLabels(md []byte) ([]string, error) {
	lines := strings.Split(string(md), "\n")
	_, _, end, err := otelFirstTable(lines)
	if err != nil {
		return nil, err
	}
	start := end
	for start < len(lines) && strings.TrimSpace(lines[start]) == "" {
		start++
	}
	if start >= len(lines) || !strings.HasPrefix(strings.TrimSpace(lines[start]), "|") {
		return nil, errors.New("the OpenTelemetry mapping's second table does not follow its first")
	}
	header, rows, _, err := readTable(lines, start)
	if err != nil {
		return nil, fmt.Errorf("the OpenTelemetry mapping's second table: %w", err)
	}
	if !strings.HasPrefix(header[0], otelLabelsHeader) {
		return nil, fmt.Errorf("the OpenTelemetry mapping's second table has the first header cell %q, want one that starts %q", header[0], otelLabelsHeader)
	}
	var labels []string
	for n, row := range rows {
		if len(row) != len(header) {
			return nil, fmt.Errorf("the OpenTelemetry mapping's second table, row %d has %d cells, want %d: %q", n+1, len(row), len(header), clip(strings.Join(row, "|")))
		}
		for _, group := range splitGroups(row[0]) {
			toks := backticked.FindAllStringSubmatch(group, -1)
			shape := backticked.ReplaceAllString(group, "X")
			if !strings.HasPrefix(shape, "the ") || !(strings.HasSuffix(shape, " label") || strings.HasSuffix(shape, " labels")) {
				continue // a usage field
			}
			if !otelLabelGroup.MatchString(shape) {
				return nil, fmt.Errorf("the OpenTelemetry mapping's second table, row %d: %q is no list of labels (\"the `a` label\", \"the `a` and `b` labels\")", n+1, group)
			}
			for _, m := range toks {
				if !otelLabel.MatchString(m[1]) {
					return nil, fmt.Errorf("the OpenTelemetry mapping's second table, row %d: %q is no label name", n+1, m[1])
				}
				if slices.Contains(labels, m[1]) {
					return nil, fmt.Errorf("the OpenTelemetry mapping maps the label %s twice", m[1])
				}
				labels = append(labels, m[1])
			}
		}
	}
	if len(labels) == 0 {
		return nil, errors.New("the OpenTelemetry mapping's second table maps no label")
	}
	return labels, nil
}

// otelFirstTable finds the one line that holds the marker, then reads the first
// table after it. Between them it passes the marker's bullets, its blank lines
// and any other text, and stops at the first line of a table; a heading, or the
// end of the document, first is an error, so that a missing table cannot hand
// back one of a later section. It returns the table's header, its rows, and
// the index of the first line after it.
func otelFirstTable(lines []string) (header []string, rows [][]string, end int, err error) {
	at := -1
	for i, l := range lines {
		if strings.Contains(l, otelMarker) {
			if at >= 0 {
				return nil, nil, 0, fmt.Errorf("%q appears more than once", otelMarker)
			}
			at = i
		}
	}
	if at < 0 {
		return nil, nil, 0, fmt.Errorf("no line contains %q", otelMarker)
	}
	for i := at + 1; i < len(lines); i++ {
		t := strings.TrimSpace(lines[i])
		switch {
		case strings.HasPrefix(t, "|"):
			header, rows, end, err = readTable(lines, i)
			if err != nil {
				return nil, nil, 0, fmt.Errorf("the OpenTelemetry mapping's first table: %w", err)
			}
			return header, rows, end, nil
		case strings.HasPrefix(t, "#"):
			return nil, nil, 0, fmt.Errorf("no table between %q and the heading on line %d", otelMarker, i+1)
		}
	}
	return nil, nil, 0, fmt.Errorf("no table after %q", otelMarker)
}

// ParseExposition reads a /metrics body in Prometheus's text format into its
// metric families, keyed by family name. A histogram's _bucket, _sum and _count
// lines and a summary's _sum and _count lines belong to the one family of their
// base name.
func ParseExposition(body []byte) (map[string]*dto.MetricFamily, error) {
	p := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := p.TextToMetricFamilies(bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("parse the /metrics body: %w", err)
	}
	return fams, nil
}

// MissingFamilies returns the names, sorted, that body (a /metrics text) has no
// metric family of with at least one series. A name is a family's own name: a
// histogram or summary counts by its base name, with _sum and _count under it.
func MissingFamilies(body []byte, names []string) ([]string, error) {
	fams, err := ParseExposition(body)
	if err != nil {
		return nil, err
	}
	var missing []string
	for _, n := range names {
		if f := fams[n]; f == nil || len(f.GetMetric()) == 0 {
			missing = append(missing, n)
		}
	}
	sort.Strings(missing)
	return missing, nil
}

// SeriesMissingLabels describes, one line per problem, what body's metric
// family name lacks of the labels want: "no series" when the family is absent
// or empty, else each series that carries no label of want. It returns nil
// when every series carries every label.
func SeriesMissingLabels(body []byte, name string, want []string) ([]string, error) {
	fams, err := ParseExposition(body)
	if err != nil {
		return nil, err
	}
	f := fams[name]
	if f == nil || len(f.GetMetric()) == 0 {
		return []string{name + ": no series"}, nil
	}
	var out []string
	for _, m := range f.GetMetric() {
		have := map[string]bool{}
		var shown []string
		for _, l := range m.GetLabel() {
			have[l.GetName()] = true
			shown = append(shown, l.GetName())
		}
		for _, w := range want {
			if !have[w] {
				out = append(out, fmt.Sprintf("%s{%s} has no label %s", name, strings.Join(shown, ","), w))
			}
		}
	}
	return out, nil
}
