package front

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/reasondrift"
)

// HBEP-8, Test plan, The OpenTelemetry mapping: every hpcburst_ name in the
// mapping's first column is exported by a serving lifecycle copy or by the
// front. This file is the front's half: the names come from HBEP-8's table
// through reasondrift.LoadOTelNames, the front's are those that start
// hpcburst_front_, and a real front's /metrics text, after a stream with a first
// token, must have a metric family for each. No name is written into this file
// but the invented one of the control: a list copied from the table would prove
// nothing, and neither would a check against the collectors' registrations
// rather than the text they serve. reasondrift's own test requires every mapped
// name to start with hpcburst_front_ or hpcburst_instance_, so a name with any
// other prefix fails there until a check covers it.

const frontOTelPrefix = "hpcburst_front_"

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

// otelFrontNames reads HBEP-8's mapping and returns the names the front should
// export, failing the test when there is none.
func otelFrontNames(t *testing.T, names []string) []string {
	t.Helper()
	var own []string
	for _, n := range names {
		if strings.HasPrefix(n, frontOTelPrefix) {
			own = append(own, n)
		}
	}
	if len(own) == 0 {
		t.Fatalf("HBEP-8's mapping names no %s series (it names %v): the front's half of the check would prove nothing", frontOTelPrefix, names)
	}
	return own
}

// scrapeBody serves the front's /metrics through its handler and returns the
// text.
func scrapeBody(t *testing.T, m *Metrics) []byte {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics: status %d: %s", rec.Code, rec.Body.String())
	}
	return rec.Body.Bytes()
}

// frontScrapeAfterAStream is a real front's /metrics text after one streamed
// chat request whose first token comes after 3 s (the fixture of
// TestMetricTimeToFirstTokenIsObservedPerStream), once its record is written.
func frontScrapeAfterAStream(t *testing.T) []byte {
	t.Helper()
	f := newScFix(t, scOpts{})
	f.engines[0].Set(enginetest.Behaviour{Tokens: 3, FirstToken: 3 * time.Second})
	resp := f.errPost(scChat, errChatBody(scModel, true, "hi"))
	readAll(t, resp)
	f.waitRecord(resp.Header.Get("hpcburst-request-id"))
	return scrapeBody(t, f.metrics)
}

// TestOTelMappingFrontNamesAreExported: the front's /metrics, after a stream,
// has a family for every hpcburst_front_ name of the mapping, and each of the
// front's histograms there carries the labels the mapping's second table maps.
func TestOTelMappingFrontNamesAreExported(t *testing.T) {
	root := otelRepoRoot(t)
	names, err := reasondrift.LoadOTelNames(root)
	if err != nil {
		t.Fatal(err)
	}
	own := otelFrontNames(t, names)
	body := frontScrapeAfterAStream(t)

	missing, err := reasondrift.MissingFamilies(body, own)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 0 {
		t.Errorf("the front's /metrics has no series of %v, which HBEP-8's OpenTelemetry mapping names", missing)
	}

	// The labels: each is one the mapping's second table maps, and each series
	// of the front's histogram carries it. A mapped name that is no histogram
	// has other labels, which HBEP-8's Front metrics table lists per metric.
	mapped, err := reasondrift.LoadOTelLabels(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range reasondrift.OTelFrontLabels {
		if !slices.Contains(mapped, l) {
			t.Errorf("the label %s is not among the labels %v that HBEP-8's mapping maps", l, mapped)
		}
	}
	fams, err := reasondrift.ParseExposition(body)
	if err != nil {
		t.Fatal(err)
	}
	histograms := 0
	for _, n := range own {
		if f := fams[n]; f == nil || f.GetType() != dto.MetricType_HISTOGRAM {
			continue
		}
		histograms++
		problems, err := reasondrift.SeriesMissingLabels(body, n, reasondrift.OTelFrontLabels)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range problems {
			t.Error(p)
		}
	}
	if histograms == 0 {
		t.Errorf("none of %v is a histogram in the front's /metrics: the label check had nothing to read", own)
	}
}

// TestOTelMappingCheckFailsWhenANameIsNotExported is the control of the check
// above: it fails for a name the front does not export. A name invented here
// joins the mapping's real hpcburst_front_ names, and the helper must report
// exactly it. And a copy of the real HBEP-8 with one mapped name renamed, read
// by the same parser, makes the helper report the new name, so the names reach
// the check from the document and are not a list.
func TestOTelMappingCheckFailsWhenANameIsNotExported(t *testing.T) {
	root := otelRepoRoot(t)
	names, err := reasondrift.LoadOTelNames(root)
	if err != nil {
		t.Fatal(err)
	}
	own := otelFrontNames(t, names)
	body := frontScrapeAfterAStream(t)

	const invented = "hpcburst_front_nonexistent_total"
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
	missing, err = reasondrift.MissingFamilies(body, otelFrontNames(t, mutatedNames))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(missing, []string{renamed}) {
		t.Errorf("with %s renamed in HBEP-8's text, the helper reports %v, want [%s]", own[0], missing, renamed)
	}
}
