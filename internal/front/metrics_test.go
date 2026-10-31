package front

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/enginetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
)

// HBEP-8, Metrics, Front metrics. Every value is read back from the text the
// /metrics handler serves, after the front handled real requests against
// engine doubles; metric names, label names, label values and bucket bounds
// are written here as literals from the HBEP, not taken from the code under
// test.

// sample is one line of the Prometheus text format.
type sample struct {
	name   string
	labels map[string]string
	value  float64
}

// parseSamples reads the text exposition format's sample lines.
func parseSamples(t *testing.T, text string) []sample {
	t.Helper()
	var out []sample
	for _, line := range strings.Split(text, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		i := strings.LastIndexByte(line, ' ')
		if i < 0 {
			t.Fatalf("metrics line %q has no value", line)
		}
		v, err := strconv.ParseFloat(line[i+1:], 64)
		if err != nil {
			t.Fatalf("metrics line %q: %v", line, err)
		}
		s := sample{labels: map[string]string{}, value: v}
		head := line[:i]
		if j := strings.IndexByte(head, '{'); j >= 0 {
			s.name = head[:j]
			body := strings.TrimSuffix(head[j+1:], "}")
			for body != "" {
				eq := strings.Index(body, `="`)
				if eq < 0 {
					t.Fatalf("metrics line %q: bad labels", line)
				}
				key := body[:eq]
				rest := body[eq+2:]
				end := strings.Index(rest, `"`)
				if end < 0 {
					t.Fatalf("metrics line %q: bad label value", line)
				}
				s.labels[key] = rest[:end]
				body = strings.TrimPrefix(rest[end+1:], ",")
			}
		} else {
			s.name = head
		}
		out = append(out, s)
	}
	return out
}

// scrape GETs /metrics through the handler and parses it.
func scrape(t *testing.T, m *Metrics) []sample {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("GET /metrics: status %d: %s", rec.Code, rec.Body.String())
	}
	return parseSamples(t, rec.Body.String())
}

// find returns the value of the one sample with exactly this name and these
// labels (key, value, key, value, ...), and whether there is one.
func find(t *testing.T, ss []sample, name string, kv ...string) (float64, bool) {
	t.Helper()
	want := map[string]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		want[kv[i]] = kv[i+1]
	}
	var found []sample
	for _, s := range ss {
		if s.name != name || len(s.labels) != len(want) {
			continue
		}
		match := true
		for k, v := range want {
			if got, ok := s.labels[k]; !ok || got != v {
				match = false
				break
			}
		}
		if match {
			found = append(found, s)
		}
	}
	if len(found) > 1 {
		t.Fatalf("%d samples of %s%v", len(found), name, want)
	}
	if len(found) == 0 {
		return 0, false
	}
	return found[0].value, true
}

// wantSample fails the test unless the sample exists with that value.
func wantSample(t *testing.T, ss []sample, v float64, name string, kv ...string) {
	t.Helper()
	got, ok := find(t, ss, name, kv...)
	if !ok {
		t.Errorf("no sample %s%v; the series are:\n%s", name, kv, describeSeries(ss, name))
		return
	}
	if got != v {
		t.Errorf("%s%v = %v, want %v", name, kv, got, v)
	}
}

// wantNoSample fails the test when any sample of name carries the labels.
func wantNoSample(t *testing.T, ss []sample, name string, kv ...string) {
	t.Helper()
	if v, ok := find(t, ss, name, kv...); ok {
		t.Errorf("sample %s%v = %v, want none", name, kv, v)
	}
}

func describeSeries(ss []sample, name string) string {
	var lines []string
	for _, s := range ss {
		if s.name == name {
			var ks []string
			for k, v := range s.labels {
				ks = append(ks, k+"="+v)
			}
			sort.Strings(ks)
			lines = append(lines, "  {"+strings.Join(ks, ",")+"} "+strconv.FormatFloat(s.value, 'g', -1, 64))
		}
	}
	return strings.Join(lines, "\n")
}

const (
	mRequests = "hpcburst_front_requests_total"
	mCuts     = "hpcburst_front_cuts_total"
	mPauses   = "hpcburst_front_pauses_total"
	mTTFT     = "hpcburst_front_time_to_first_token_seconds"
)

// TestMetricRequestsMoveWithTheStatusAndTheEndpoint: each request adds one to
// hpcburst_front_requests_total with the status the client got, its endpoint,
// and its provider and ModelDeployment; those two are empty for an error
// raised before a target was chosen. A 200 twice reads 2, a 401 and a model
// that is not attached read 1 each with empty target labels, and a listing and
// a path outside /v1/ have their own endpoint.
func TestMetricRequestsMoveWithTheStatusAndTheEndpoint(t *testing.T) {
	f := newScFix(t, scOpts{})
	f.prSend(t, scChat, errChatBody(scModel, false, "hi"))
	ss := scrape(t, f.metrics)
	wantSample(t, ss, 1, mRequests, "code", "200", "endpoint", "chat", "provider", "helios-a", "model_deployment", "ns/md")
	f.prSend(t, scChat, errChatBody(scModel, false, "again"))

	// A wrong key: refused before routing.
	req, _ := http.NewRequest(http.MethodPost, f.url+scChat, strings.NewReader(errChatBody(scModel, false, "hi")))
	req.Header.Set("Authorization", "Bearer not-the-key")
	resp, err := errClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	readAll(t, resp)
	f.waitRecord(resp.Header.Get("hpcburst-request-id"))

	// A model that no ModelDeployment names: 503 model_inactive, no target.
	resp = f.errPost(scChat, errChatBody("nobody-serves-this", false, "hi"))
	readAll(t, resp)
	f.waitRecord(resp.Header.Get("hpcburst-request-id"))

	// The listing, and a path the front does not serve.
	resp = f.errGet("/v1/models")
	readAll(t, resp)
	f.waitRecord(resp.Header.Get("hpcburst-request-id"))
	resp = f.errGet("/health")
	readAll(t, resp)
	f.waitRecord(resp.Header.Get("hpcburst-request-id"))

	ss = scrape(t, f.metrics)
	wantSample(t, ss, 2, mRequests, "code", "200", "endpoint", "chat", "provider", "helios-a", "model_deployment", "ns/md")
	wantSample(t, ss, 1, mRequests, "code", "401", "endpoint", "chat", "provider", "", "model_deployment", "")
	wantSample(t, ss, 1, mRequests, "code", "503", "endpoint", "chat", "provider", "", "model_deployment", "")
	wantSample(t, ss, 1, mRequests, "code", "200", "endpoint", "models", "provider", "", "model_deployment", "")
	wantSample(t, ss, 1, mRequests, "code", "404", "endpoint", "other", "provider", "", "model_deployment", "")
}

// TestEndpointLabel: the endpoint label's values for the paths the front
// serves (HBEP-8: chat, completions, responses, embeddings, models, other).
func TestEndpointLabel(t *testing.T) {
	for path, want := range map[string]string{
		"/v1/chat/completions":    "chat",
		"/v1/completions":         "completions",
		"/v1/responses":           "responses",
		"/v1/embeddings":          "embeddings",
		"/v1/models":              "models",
		"/v1/models/Org/Model-7B": "models",
		"/v1/audio/speech":        "other",
		"/agent/v1/register":      "other",
		"/":                       "other",
	} {
		if got := endpointLabel(path); got != want {
			t.Errorf("endpointLabel(%q) = %q, want %q", path, got, want)
		}
	}
}

// TestMetricCutsMoveWithCutBy: a stream the engine kills after two tokens is
// cut by the upstream, one the client leaves is cut by the client, and a
// request cut while it waited at the front, before any target was chosen, is
// cut by front_deadline with empty target labels. Each adds one to
// hpcburst_front_cuts_total under its own cut_by, and a request that ends
// whole adds none.
func TestMetricCutsMoveWithCutBy(t *testing.T) {
	one := int32(1)
	f := newScFix(t, scOpts{
		cfg: func(c *Config) { c.MaxRequestDuration = Duration(5 * time.Second) },
		md:  func(md *v1alpha1.ModelDeployment) { md.Spec.Routing = &v1alpha1.Routing{InstanceCap: &one} },
	})
	f.engines[0].Set(enginetest.Behaviour{Tokens: 3})
	f.prSend(t, scChat, errChatBody(scModel, false, "whole"))
	ss := scrape(t, f.metrics)
	if got := describeSeries(ss, mCuts); got != "" {
		t.Fatalf("a request that ended whole moved the cuts:\n%s", got)
	}

	// The engine kills the stream after two tokens.
	f.engines[0].Set(enginetest.Behaviour{Tokens: 6, KillAfter: 2})
	resp := f.errPost(scChat, errChatBody(scModel, true, "hello"))
	readAll(t, resp)
	f.waitRecord(resp.Header.Get("hpcburst-request-id"))

	// A client that leaves after the first event.
	f.retarget(f.uids[0], rawEngine(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, chatToken)
		_ = http.NewResponseController(w).Flush()
		<-r.Context().Done()
	}))
	resp = f.errPost(scChat, prChatStream)
	buf := make([]byte, 1)
	if _, err := resp.Body.Read(buf); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	f.waitRecord(resp.Header.Get("hpcburst-request-id"))

	// A request that waits at the front for the one slot, then meets the
	// front's deadline.
	hold := make(chan struct{})
	defer close(hold)
	f.retarget(f.uids[0], f.engines[0].Addr())
	f.engines[0].Set(enginetest.Behaviour{Hold: hold})
	first := scGo(f.harness, errChatBody(scModel, false, "first"))
	errWaitFor(t, "the first request to hold the slot", func() bool { return f.engines[0].Open() == 1 })
	second := scGo(f.harness, errChatBody(scModel, false, "second"))
	errWaitFor(t, "the second request to wait", func() bool { return f.router.Status().Waiting == 1 })
	f.clk.Advance(5 * time.Second)
	resp, _ = second.wait(t)
	f.waitRecord(resp.Header.Get("hpcburst-request-id"))
	_ = first

	ss = scrape(t, f.metrics)
	wantSample(t, ss, 1, mCuts, "cut_by", "upstream", "provider", "helios-a", "model_deployment", "ns/md")
	wantSample(t, ss, 1, mCuts, "cut_by", "client", "provider", "helios-a", "model_deployment", "ns/md")
	wantSample(t, ss, 1, mCuts, "cut_by", "front_deadline", "provider", "", "model_deployment", "")
	wantSample(t, ss, 1, mRequests, "code", "504", "endpoint", "chat", "provider", "", "model_deployment", "")
}

// TestMetricPausesCountEachPause: three failed requests in a row pause the
// Instance, and that adds one to hpcburst_front_pauses_total for its provider
// and ModelDeployment; after pauseDuration (30 s) three more pause it again.
func TestMetricPausesCountEachPause(t *testing.T) {
	f := newScFix(t, scOpts{})
	f.engines[0].Set(enginetest.Behaviour{Status: 500, ErrorBody: `{"error":{"message":"boom","type":"server_error","code":500}}`})
	for range 2 {
		f.prSend(t, scChat, errChatBody(scModel, false, "hi"))
	}
	if _, ok := find(t, scrape(t, f.metrics), mPauses, "provider", "helios-a", "model_deployment", "ns/md"); ok {
		t.Fatal("two failures paused the Instance; the rule is three in a row")
	}
	f.prSend(t, scChat, errChatBody(scModel, false, "hi"))
	wantSample(t, scrape(t, f.metrics), 1, mPauses, "provider", "helios-a", "model_deployment", "ns/md")

	f.clk.Advance(30 * time.Second)
	for range 3 {
		f.prSend(t, scChat, errChatBody(scModel, false, "hi"))
	}
	wantSample(t, scrape(t, f.metrics), 2, mPauses, "provider", "helios-a", "model_deployment", "ns/md")
}

// TestMetricTimeToFirstTokenIsObservedPerStream: a stream whose first token
// comes after 3 s adds one observation of 3 s to the histogram: its count and
// sum, and the cumulative buckets from HBEP-8's list (0.1, 0.25, 0.5, 1, 2,
// 5, 10, 30, 60, 120, 300, 600, 1200, 1800, 3600, 7200 s). A second, after
// 400 s, lands in the 600 s bucket. A stream with no content event adds none.
func TestMetricTimeToFirstTokenIsObservedPerStream(t *testing.T) {
	f := newScFix(t, scOpts{})
	labels := []string{"provider", "helios-a", "model_deployment", "ns/md", "endpoint", "chat"}
	bucket := func(le string) []string { return append([]string{"le", le}, labels...) }

	f.engines[0].Set(enginetest.Behaviour{Tokens: 3, FirstToken: 3 * time.Second})
	resp := f.errPost(scChat, errChatBody(scModel, true, "hi"))
	readAll(t, resp)
	f.waitRecord(resp.Header.Get("hpcburst-request-id"))
	ss := scrape(t, f.metrics)
	wantSample(t, ss, 1, mTTFT+"_count", labels...)
	wantSample(t, ss, 3, mTTFT+"_sum", labels...)
	for le, want := range map[string]float64{"0.1": 0, "0.25": 0, "0.5": 0, "1": 0, "2": 0, "5": 1, "10": 1, "30": 1, "60": 1, "120": 1, "300": 1, "600": 1, "1200": 1, "1800": 1, "3600": 1, "7200": 1, "+Inf": 1} {
		wantSample(t, ss, want, mTTFT+"_bucket", bucket(le)...)
	}

	f.engines[0].Set(enginetest.Behaviour{Tokens: 3, FirstToken: 400 * time.Second})
	resp = f.errPost(scChat, errChatBody(scModel, true, "hi"))
	readAll(t, resp)
	f.waitRecord(resp.Header.Get("hpcburst-request-id"))
	ss = scrape(t, f.metrics)
	wantSample(t, ss, 2, mTTFT+"_count", labels...)
	wantSample(t, ss, 403, mTTFT+"_sum", labels...)
	wantSample(t, ss, 1, mTTFT+"_bucket", bucket("300")...)
	wantSample(t, ss, 2, mTTFT+"_bucket", bucket("600")...)

	// A stream that ends with no content event has no first token.
	f.retarget(f.uids[0], sseEngine(t, vllmDone))
	resp = f.errPost(scChat, prChatStream)
	readAll(t, resp)
	id := resp.Header.Get("hpcburst-request-id")
	if rec := f.record(id); rec.FirstTokenMs != nil {
		t.Fatalf("the stream with no content event has firstTokenMs %d; the test needs it to have none", *rec.FirstTokenMs)
	}
	wantSample(t, scrape(t, f.metrics), 2, mTTFT+"_count", labels...)
}

// TestMetricUsageRecordsAndFreeSpace: hpcburst_usage_records_total counts the
// records written and synced, starting at 0; hpcburst_usage_dir_free_bytes
// follows the free space the front reads; and it is absent, never 0, while
// that cannot be read. hpcburst_usage_write_failures_total starts at 0 too.
func TestMetricUsageRecordsAndFreeSpace(t *testing.T) {
	f := newScFix(t, scOpts{})
	ss := scrape(t, f.metrics)
	wantSample(t, ss, 0, "hpcburst_usage_records_total")
	wantSample(t, ss, 0, "hpcburst_usage_write_failures_total")
	wantSample(t, ss, float64(uint64(1)<<40), "hpcburst_usage_dir_free_bytes")

	for range 3 {
		f.prSend(t, scChat, errChatBody(scModel, false, "hi"))
	}
	f.free.set(123456789, nil)
	ss = scrape(t, f.metrics)
	wantSample(t, ss, 3, "hpcburst_usage_records_total")
	wantSample(t, ss, 0, "hpcburst_usage_write_failures_total")
	wantSample(t, ss, 123456789, "hpcburst_usage_dir_free_bytes")

	f.free.set(0, errors.New("statfs: unreadable"))
	wantNoSample(t, scrape(t, f.metrics), "hpcburst_usage_dir_free_bytes")
}

// TestMetricUsageWriteFailuresCountRecordsLost: when the day's file cannot be
// opened (its name is a directory), each request's record is lost, the front
// keeps answering 200, hpcburst_usage_write_failures_total counts the records
// and the sink states failing; once the obstacle is gone the next record is
// written and the state is ok again, with the count kept.
func TestMetricUsageWriteFailuresCountRecordsLost(t *testing.T) {
	f := newScFix(t, scOpts{})
	blocker := filepath.Join(f.cfg.UsageDir, "usage-2026-10-31.jsonl") // the fake clock's day
	if err := os.Mkdir(blocker, 0o700); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		resp := f.errPost(scChat, errChatBody(scModel, false, "hi"))
		if resp.StatusCode != 200 {
			t.Fatalf("status %d while usage fails, want 200: the front keeps serving", resp.StatusCode)
		}
		readAll(t, resp)
	}
	errWaitFor(t, "both records to be lost", func() bool { return f.sink.Stats().Lost == 2 })
	ss := scrape(t, f.metrics)
	wantSample(t, ss, 2, "hpcburst_usage_write_failures_total")
	wantSample(t, ss, 0, "hpcburst_usage_records_total")
	u := usageHealthOf(f.sink.Stats())
	if u.State != "failing" || u.Failures != 2 || u.Error == "" || u.Since != "2026-10-31T12:00:00.000Z" {
		t.Errorf("usage health = %+v, want failing, 2 failures, an error and since 2026-10-31T12:00:00.000Z", u)
	}

	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	f.clk.Advance(time.Minute)
	f.prSend(t, scChat, errChatBody(scModel, false, "hi"))
	ss = scrape(t, f.metrics)
	wantSample(t, ss, 1, "hpcburst_usage_records_total")
	wantSample(t, ss, 2, "hpcburst_usage_write_failures_total")
	u = usageHealthOf(f.sink.Stats())
	if u.State != "ok" || u.Failures != 2 || u.Error != "" || u.Since != "2026-10-31T12:01:00.000Z" {
		t.Errorf("usage health after recovery = %+v, want ok, the 2 failures kept, no error and since 2026-10-31T12:01:00.000Z", u)
	}
}

// TestMetricsNilIsHarmless: a front built without metrics (a test) counts
// nothing and panics nowhere.
func TestMetricsNilIsHarmless(t *testing.T) {
	var m *Metrics
	m.observe(usage.Record{Status: 200, Path: "/v1/chat/completions"})
	m.paused("p", "d")
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != 404 {
		t.Errorf("a nil Metrics served status %d, want 404", rec.Code)
	}
}
