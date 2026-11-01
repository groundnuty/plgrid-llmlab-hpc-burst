package enginedouble

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

func (s *testServer) metrics() string { return string(s.get("/metrics").body) }

// has reports whether the exposition holds the exact line.
func has(metrics, line string) bool {
	for _, l := range strings.Split(metrics, "\n") {
		if l == line {
			return true
		}
	}
	return false
}

func TestMetricsAfterARequest(t *testing.T) {
	s := serve(t, cfgFor())
	// 9 template tokens + 3 words = 12 prompt tokens; 5 generated
	s.chat(chatBody(3, map[string]any{"max_tokens": 5, "ignore_eos": true}))
	s.chat(chatBody(3, map[string]any{"max_tokens": 100})) // 24 generated, finish stop
	m := s.metrics()
	for _, line := range []string{
		`vllm:num_requests_running{engine="0",model_name="m"} 0.0`,
		`vllm:num_requests_waiting{engine="0",model_name="m"} 0.0`,
		`vllm:num_requests_waiting_by_reason{engine="0",model_name="m",reason="capacity"} 0.0`,
		`vllm:num_requests_waiting_by_reason{engine="0",model_name="m",reason="deferred"} 0.0`,
		`vllm:kv_cache_usage_perc{engine="0",model_name="m"} 0.0`,
		`vllm:num_preemptions_total{engine="0",model_name="m"} 0.0`,
		`vllm:prompt_tokens_total{engine="0",model_name="m"} 24.0`,
		`vllm:generation_tokens_total{engine="0",model_name="m"} 29.0`,
		`vllm:request_success_total{engine="0",finished_reason="length",model_name="m"} 1.0`,
		`vllm:request_success_total{engine="0",finished_reason="stop",model_name="m"} 1.0`,
		`vllm:request_success_total{engine="0",finished_reason="abort",model_name="m"} 0.0`,
		`vllm:request_success_total{engine="0",finished_reason="error",model_name="m"} 0.0`,
		`vllm:request_success_total{engine="0",finished_reason="repetition",model_name="m"} 0.0`,
		`http_requests_total{handler="/v1/chat/completions",method="POST",status="2xx"} 2.0`,
	} {
		if !has(m, line) {
			t.Errorf("missing line %s\nin:\n%s", line, m)
		}
	}
}

func TestInstrumentatorCountsByGroupedStatusAndSkipsExcluded(t *testing.T) {
	s := serve(t, cfgFor())
	s.get("/health")
	s.get("/version")
	s.get("/metrics")
	s.get("/v1/models")
	s.post("/tokenize", map[string]any{"prompt": "a"})
	s.post("/v1/chat/completions", map[string]any{"model": "m"})  // no messages: 400
	s.post("/v1/chat/completions", map[string]any{"model": "zz"}) // 400 too, before the model check
	s.get("/nope")
	m := s.metrics()
	for _, line := range []string{
		`http_requests_total{handler="/v1/models",method="GET",status="2xx"} 1.0`,
		`http_requests_total{handler="/tokenize",method="POST",status="2xx"} 1.0`,
		`http_requests_total{handler="/v1/chat/completions",method="POST",status="4xx"} 2.0`,
		`http_requests_total{handler="none",method="GET",status="4xx"} 1.0`,
	} {
		if !has(m, line) {
			t.Errorf("missing line %s\nin:\n%s", line, m)
		}
	}
	for _, h := range []string{"/health", "/version", "/metrics"} {
		if strings.Contains(m, `handler="`+h+`"`) {
			t.Errorf("the instrumentator must not count %s", h)
		}
	}
}

// The label set of vllm:cache_config_info is the real capture's, and every
// label the Config does not decide keeps the capture's value.
func TestCacheConfigInfoLabelsMatchTheCapture(t *testing.T) {
	capture, err := os.ReadFile("../../../spikes/s2b/results/gh200-1gpu/arms/A1-vr-hash/after-w0.prom")
	if err != nil {
		t.Fatal(err)
	}
	var real string
	for _, l := range strings.Split(string(capture), "\n") {
		if strings.HasPrefix(l, "vllm:cache_config_info{") {
			real = l
		}
	}
	if real == "" {
		t.Fatal("the capture has no cache_config_info")
	}
	wantLabels := labelsOf(t, real)

	m := serve(t, cfgFor()).metrics()
	var ours string
	for _, l := range strings.Split(m, "\n") {
		if strings.HasPrefix(l, "vllm:cache_config_info{") {
			ours = l
		}
	}
	if !strings.HasSuffix(ours, "} 1.0") {
		t.Fatalf("cache_config_info line = %q, want a value of 1.0", ours)
	}
	got := labelsOf(t, ours)
	if len(got) != 37 {
		t.Errorf("%d labels, want the capture's 37", len(got))
	}
	// the capture's order, name for name
	if names(got) != names(wantLabels) {
		t.Errorf("label names differ from the capture:\n%s\n%s", names(got), names(wantLabels))
	}
	derived := map[string]bool{"block_size": true, "effective_attention_block_size": true, "cache_dtype": true,
		"enable_prefix_caching": true, "num_gpu_blocks": true, "kv_cache_size_tokens": true, "kv_cache_max_concurrency": true}
	for _, kv := range wantLabels {
		if g := lookup(got, kv[0]); !derived[kv[0]] && g != kv[1] {
			t.Errorf("label %s = %q, the capture has %q", kv[0], g, kv[1])
		}
	}
	// hand-computed from the defaults: 2^20 tokens, 16 per block, 32768 context
	for k, want := range map[string]string{
		"block_size": "16", "cache_dtype": "auto", "enable_prefix_caching": "True",
		"kv_cache_size_tokens": "1048576", "num_gpu_blocks": "65536", "kv_cache_max_concurrency": "32.0",
	} {
		if g := lookup(got, k); g != want {
			t.Errorf("default %s = %q, want %q", k, g, want)
		}
	}
}

func TestCacheConfigInfoFollowsTheConfig(t *testing.T) {
	cfg, err := ParseModes("kv-low")
	if err != nil {
		t.Fatal(err)
	}
	cfg.ServedName, cfg.TokenDelay = "m", 0
	cfg.BlockSize, cfg.CacheDtype, cfg.PrefixCaching = 32, "fp8", false
	m := serve(t, cfg).metrics()
	var line string
	for _, l := range strings.Split(m, "\n") {
		if strings.HasPrefix(l, "vllm:cache_config_info{") {
			line = l
		}
	}
	got := labelsOf(t, line)
	// 1000 / 32 = 31 blocks; 1000 / 32768 = 0.030517578125 exactly (32768 is 2^15)
	for k, want := range map[string]string{
		"block_size": "32", "effective_attention_block_size": "32", "cache_dtype": "fp8", "enable_prefix_caching": "False",
		"kv_cache_size_tokens": "1000", "num_gpu_blocks": "31", "kv_cache_max_concurrency": "0.030517578125",
	} {
		if g := lookup(got, k); g != want {
			t.Errorf("%s = %q, want %q", k, g, want)
		}
	}
}

var labelRE = regexp.MustCompile(`(\w+)="([^"]*)"`)

func labelsOf(t *testing.T, line string) [][2]string {
	t.Helper()
	open, end := strings.Index(line, "{"), strings.LastIndex(line, "}")
	if open < 0 || end < open {
		t.Fatalf("not a sample line: %q", line)
	}
	var out [][2]string
	for _, m := range labelRE.FindAllStringSubmatch(line[open:end], -1) {
		out = append(out, [2]string{m[1], m[2]})
	}
	return out
}

func names(l [][2]string) string {
	var n []string
	for _, kv := range l {
		n = append(n, kv[0])
	}
	return strings.Join(n, ",")
}

func lookup(l [][2]string, name string) string {
	for _, kv := range l {
		if kv[0] == name {
			return kv[1]
		}
	}
	return "<absent>"
}

// Python's prometheus_client writes 0.0, 27072.0 and 3.310655e+06 for the
// capture's three kinds of value; the expectations are the capture's own lines.
func TestNumberFormatMatchesPrometheusClient(t *testing.T) {
	for _, tc := range []struct {
		v    float64
		want string
	}{
		{0, "0.0"}, {27072, "27072.0"}, {3310655, "3.310655e+06"}, {564, "564.0"},
		{0.25, "0.25"}, {999999, "999999.0"}, {1e6, "1e+06"}, {12345678, "1.2345678e+07"}, {2.5e9, "2.5e+09"},
	} {
		if got := promFloat(tc.v); got != tc.want {
			t.Errorf("promFloat(%v) = %q, want %q", tc.v, got, tc.want)
		}
	}
	capture, err := os.ReadFile("../../../spikes/s2b/results/gh200-1gpu/arms/A1-vr-hash/after-w0.prom")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`vllm:num_requests_running{engine="0",model_name="s2b-model"} 0.0`,
		`vllm:generation_tokens_total{engine="0",model_name="s2b-model"} 27072.0`,
		`vllm:prompt_tokens_total{engine="0",model_name="s2b-model"} 3.310655e+06`,
	} {
		if !has(string(capture), want) {
			t.Fatalf("the capture lacks %q: the expectation is stale", want)
		}
	}
}

func TestPrefixCacheCountsFullBlocks(t *testing.T) {
	s := serve(t, cfgFor())
	// 9 + 40 = 49 tokens: three full 16-token blocks, 48 tokens, are cacheable
	s.chat(chatBody(40, map[string]any{"max_tokens": 1}))
	s.chat(chatBody(40, map[string]any{"max_tokens": 1}))
	m := s.metrics()
	for _, line := range []string{
		`vllm:prefix_cache_queries_total{engine="0",model_name="m"} 98.0`,
		`vllm:prefix_cache_hits_total{engine="0",model_name="m"} 48.0`,
	} {
		if !has(m, line) {
			t.Errorf("missing %s\n%s", line, m)
		}
	}
	cfg := cfgFor()
	cfg.PrefixCaching = false
	off := serve(t, cfg)
	off.chat(chatBody(40, map[string]any{"max_tokens": 1}))
	off.chat(chatBody(40, map[string]any{"max_tokens": 1}))
	if m := off.metrics(); !has(m, `vllm:prefix_cache_hits_total{engine="0",model_name="m"} 0.0`) {
		t.Errorf("hits with prefix caching off:\n%s", m)
	}
}

// With MaxNumSeqs 1, the second of two concurrent requests waits; running,
// waiting and KV usage are live while they run.
func TestRunningWaitingAndKVAreLive(t *testing.T) {
	cfg := cfgFor()
	cfg.MaxNumSeqs, cfg.TokenDelay, cfg.KVCacheTokens = 1, 20*time.Millisecond, 1000
	s := serve(t, cfg)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.post("/v1/chat/completions", chatBody(41, map[string]any{"max_tokens": 30, "min_tokens": 30})) // 50 prompt tokens
		}()
	}
	defer wg.Wait()
	eventually(t, 3*time.Second, "one request running and one waiting", func() bool {
		m := s.metrics()
		return has(m, `vllm:num_requests_running{engine="0",model_name="m"} 1.0`) &&
			has(m, `vllm:num_requests_waiting{engine="0",model_name="m"} 1.0`) &&
			has(m, `vllm:num_requests_waiting_by_reason{engine="0",model_name="m",reason="capacity"} 1.0`)
	})
	// 50 prompt tokens plus the generated ones, of 1000
	kv := regexp.MustCompile(`vllm:kv_cache_usage_perc\{[^}]*\} ([0-9.e+-]+)`).FindStringSubmatch(s.metrics())
	if kv == nil || kv[1] == "0.0" {
		t.Errorf("kv_cache_usage_perc = %v while a request runs", kv)
	}
	eventually(t, 20*time.Second, "both requests done", func() bool {
		m := s.metrics()
		return has(m, `vllm:num_requests_running{engine="0",model_name="m"} 0.0`) && has(m, `vllm:num_requests_waiting{engine="0",model_name="m"} 0.0`)
	})
}

// timeToFirstByte posts a streamed request and returns the wait for its first
// event.
func (s *testServer) timeToFirstByte(body map[string]any) time.Duration {
	s.t.Helper()
	body["stream"] = true
	b, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://engine/v1/chat/completions", bytes.NewReader(b))
	start := time.Now()
	resp, err := s.hc.Do(req)
	if err != nil {
		s.t.Fatal(err)
	}
	defer resp.Body.Close()
	if _, err := bufio.NewReader(resp.Body).ReadString('\n'); err != nil {
		s.t.Fatal(err)
	}
	d := time.Since(start)
	_, _ = io.Copy(io.Discard, resp.Body)
	return d
}

func TestSlowFirstDelaysTheFirstRequestOfEachShape(t *testing.T) {
	cfg := cfgFor()
	cfg.SlowFirst = time.Second
	s := serve(t, cfg)
	body := func(words, max int) map[string]any {
		return chatBody(words, map[string]any{"max_tokens": max, "ignore_eos": true})
	}
	if d := s.timeToFirstByte(body(5, 10)); d < time.Second {
		t.Errorf("first request of a shape: first byte after %v, want at least 1s", d)
	}
	if d := s.timeToFirstByte(body(5, 10)); d > 500*time.Millisecond {
		t.Errorf("second request of the shape: first byte after %v, want it fast", d)
	}
	// the nonce in the prompt changes nothing about the shape; its count does
	if d := s.timeToFirstByte(chatBody(5, map[string]any{"max_tokens": 10, "ignore_eos": true, "user": "x"})); d > 500*time.Millisecond {
		t.Errorf("same shape again: %v", d)
	}
	if d := s.timeToFirstByte(body(6, 10)); d < time.Second {
		t.Errorf("a new prompt length is a new shape: first byte after %v, want at least 1s", d)
	}
	if d := s.timeToFirstByte(body(5, 11)); d < time.Second {
		t.Errorf("a new max_tokens is a new shape: first byte after %v, want at least 1s", d)
	}
}

// never-settle=100: with TokenDelay 20 ms and 10 tokens, the n-th request of a shape (counting from 0)
// waits n * 100% * 20 ms * 10 = n * 200 ms before its first token (engine.beforeFirstToken), so each waits
// 200 ms longer than the one before. The test shows two things, each with a bound that a loaded machine
// cannot move, and sets no other:
//   - the double decided the growth: each request's record holds exactly n * 200 ms of injected wait, the
//     double's own number (LogEvent.InjectedNS), so a growth that doubles each time, or stops, is caught
//     with no clock in it;
//   - the double slept the wait: the first byte comes at least that wait after the request was sent. A
//     timer never fires early and the request is sent before the double starts the timer, so this holds
//     on any machine (no tolerance is needed), and load only adds time to it.
//
// There is deliberately no bound on the step between two consecutive first bytes, and none from above on
// a first byte: a step is a difference of two waits that load lengthens by different amounts, so it can
// shrink as well as grow (283 ms, then 427 ms: a step of 144 ms, issue #203).
func TestNeverSettleGrowsEachRequest(t *testing.T) {
	cfg := cfgFor()
	cfg.TokenDelay, cfg.NeverSettle = 20*time.Millisecond, 100
	const requests, share = 4, 200 * time.Millisecond // share: 100% of 20 ms * 10 tokens
	var mu sync.Mutex
	var recs []LogEvent
	s := serveHandler(t, newHandler(cfg, func(ev LogEvent) {
		if ev.Event == "request" {
			mu.Lock()
			recs = append(recs, ev)
			mu.Unlock()
		}
	}))
	var ttfb []time.Duration
	for i := 0; i < requests; i++ {
		ttfb = append(ttfb, s.timeToFirstByte(chatBody(5, map[string]any{"max_tokens": 10, "ignore_eos": true})))
	}
	// A request's record is written when its handler returns, which can be just after the client has read
	// the end of the answer: wait for the four records.
	var got []LogEvent
	eventually(t, 10*time.Second, "four request records", func() bool {
		mu.Lock()
		defer mu.Unlock()
		got = append([]LogEvent(nil), recs...)
		return len(got) == requests
	})
	for i, ev := range got {
		want := time.Duration(i) * share
		if ev.Nth != i || time.Duration(ev.InjectedNS) != want {
			t.Errorf("request %d is the %dth of its shape and the double made it wait %v, want the %dth and exactly %v (records: %+v)", i, ev.Nth, time.Duration(ev.InjectedNS), i, want, got)
		}
		if ttfb[i] < time.Duration(ev.InjectedNS) {
			t.Errorf("request %d had its first byte after %v, before the %v the double says it slept (all: %v)", i, ttfb[i], time.Duration(ev.InjectedNS), ttfb)
		}
		if ttfb[i] < time.Duration(i)*share {
			t.Errorf("request %d had its first byte after %v, want at least %v (all: %v)", i, ttfb[i], time.Duration(i)*share, ttfb)
		}
	}
}
