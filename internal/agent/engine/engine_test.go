package engine

import (
	"os"
	"strings"
	"testing"
)

// The real vLLM 0.32.0 /metrics capture of spike S2 phase B, a GH200 worker.
const capturePath = "../../../spikes/s2b/results/gh200-1gpu/arms/A1-vr-hash/after-w0.prom"

func readCapture(t *testing.T) metrics {
	t.Helper()
	b, err := os.ReadFile(capturePath)
	if err != nil {
		t.Fatal(err)
	}
	m, err := parseMetrics(string(b))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// The KV capacity and the cache settings read from a real vLLM 0.32.0 scrape
// are the label values written in it: kv_cache_size_tokens="528160",
// cache_dtype="auto", block_size="16", enable_prefix_caching="True",
// gpu_memory_utilization="0.2".
func TestReadsTheRealCapture(t *testing.T) {
	m := readCapture(t)
	cfg := m.cacheConfig()
	if cfg == nil {
		t.Fatal("no vllm:cache_config_info in the capture")
	}
	if got := kvCapacity(cfg); got != 528160 {
		t.Errorf("KV capacity %d, want 528160", got)
	}
	want := map[string]string{"cache_dtype": "auto", "block_size": "16", "enable_prefix_caching": "True", "gpu_memory_utilization": "0.2"}
	for k, v := range want {
		if cfg[k] != v {
			t.Errorf("label %s = %q, want %q", k, cfg[k], v)
		}
	}
	if v, ok := m.sum(metricGenerated); !ok || v != 27072 {
		t.Errorf("generation tokens %v %v, want 27072", v, ok)
	}
	if v, ok := m.sum(metricRunning); !ok || v != 0 {
		t.Errorf("running %v %v, want 0", v, ok)
	}
}

// Without kv_cache_size_tokens the capacity is num_gpu_blocks times
// block_size: 33010 x 16 = 528160 in the capture.
func TestKVCapacityFromBlocks(t *testing.T) {
	if got := kvCapacity(map[string]string{"num_gpu_blocks": "33010", "block_size": "16", "kv_cache_size_tokens": "None"}); got != 528160 {
		t.Fatalf("KV capacity %d, want 528160", got)
	}
	if got := kvCapacity(map[string]string{"num_gpu_blocks": "None"}); got != 0 {
		t.Fatalf("KV capacity %d from nothing, want 0", got)
	}
}

// Label values with escapes, a timestamp, and a series without labels.
func TestParseMetricsEscapesAndTimestamps(t *testing.T) {
	m, err := parseMetrics(strings.Join([]string{
		`# HELP x a help line`,
		`# TYPE x counter`,
		`x{a="1,2",b="say \"hi\"",c="back\\slash",d="new\nline"} 3 1700000000000`,
		`y 4.5e+01`,
		`http_requests_total{handler="/v1/chat/completions",method="POST",status="2xx"} 7.0`,
		`http_requests_total{handler="/v1/models",method="GET",status="2xx"} 2.0`,
		`http_requests_total{handler="/v1/chat/completions",method="POST",status="5xx"} 1.0`,
	}, "\n"))
	if err != nil {
		t.Fatal(err)
	}
	s := m["x"][0]
	if s.value != 3 || s.labels["a"] != "1,2" || s.labels["b"] != `say "hi"` || s.labels["c"] != `back\slash` || s.labels["d"] != "new\nline" {
		t.Errorf("x: %+v", s)
	}
	if v, _ := m.sum("y"); v != 45 {
		t.Errorf("y = %v, want 45", v)
	}
}

func TestParseMetricsRefusesMalformedLines(t *testing.T) {
	for _, line := range []string{`x{a="1"`, `x{a=1} 2`, `x`, `x{a="1"} notanumber`} {
		if _, err := parseMetrics(line); err == nil {
			t.Errorf("%q parsed", line)
		}
	}
}

// The loop rule on hand-written token sequences.
func TestIsLoop(t *testing.T) {
	seq := func(n int, f func(i int) int32) []int32 {
		out := make([]int32, n)
		for i := range out {
			out[i] = f(i)
		}
		return out
	}
	distinct := seq(64, func(i int) int32 { return int32(i + 1) })
	cycle3 := seq(64, func(i int) int32 { return int32(i%3 + 1) })
	answerThenCycle := append(seq(30, func(i int) int32 { return int32(100 + i) }), seq(20, func(i int) int32 { return int32(i%5 + 1) })...)
	shortCycle := append(seq(40, func(i int) int32 { return int32(100 + i) }), 1, 2, 3, 1, 2, 3)
	fiveRepeated := []int32{7, 7, 7, 7, 7}
	unit64 := seq(64*4, func(i int) int32 { return int32(i%64 + 1) })
	unit65 := seq(65*4, func(i int) int32 { return int32(i%65 + 1) })
	for _, tc := range []struct {
		name string
		toks []int32
		want bool
	}{
		{"distinct tokens", distinct, false},
		{"a 3-token cycle throughout", cycle3, true},
		{"an answer, then a 5-token unit four times", answerThenCycle, true},
		{"a 3-token unit only twice at the end", shortCycle, false},
		{"one token five times, under 16 tokens", fiveRepeated, false},
		{"a 64-token unit four times", unit64, true},
		{"a 65-token unit, above the 64-token unit bound", unit65, false},
		{"empty", nil, false},
	} {
		if got := IsLoop(tc.toks); got != tc.want {
			t.Errorf("%s: IsLoop = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// Calibration finds the most words that fit, for a tokenizer of one token per
// word, of two per word, and of a fixed part of 5 tokens.
func TestCalibrate(t *testing.T) {
	c := &Client{}
	for _, tc := range []struct {
		name    string
		perWord int32
		fixed   int32
		target  int32
		want    int
	}{
		{"one token per word", 1, 10, 110, 100},
		{"two tokens per word, even", 2, 10, 110, 50},
		{"two tokens per word, odd room", 2, 10, 111, 50},
		{"the fixed part fills it", 1, 10, 10, 0},
	} {
		calls := 0
		count := func(n int) (int32, error) {
			calls++
			return tc.fixed + tc.perWord*int32(n), nil
		}
		got, err := c.calibrate(count, tc.target)
		if err != nil || got != tc.want {
			t.Errorf("%s: %d words (%v), want %d", tc.name, got, err, tc.want)
		}
		if calls > 40 {
			t.Errorf("%s: %d counts", tc.name, calls)
		}
	}
	if _, err := c.calibrate(func(int) (int32, error) { return 20, nil }, 10); err == nil {
		t.Error("a fixed part above the target was accepted")
	}
}

// The filler draws a varied order: no word follows itself four times in a row
// in a long text, so the prompt does not look like a loop to the model.
func TestFillerIsVaried(t *testing.T) {
	words := strings.Fields(fillerText(5000, 12345))
	if len(words) != 5000 {
		t.Fatalf("%d words", len(words))
	}
	run := 1
	for i := 1; i < len(words); i++ {
		if words[i] == words[i-1] {
			run++
			if run >= 4 {
				t.Fatalf("word %q repeats %d times at %d", words[i], run, i)
			}
		} else {
			run = 1
		}
	}
}
