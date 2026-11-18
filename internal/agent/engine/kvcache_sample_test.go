package engine

// The KV cache size read through Client.Sample (HBEP-7, The KV cache size; `@hpc-burst-science-agent`,
// 18 Nov 2026, #764, Q4): the label kv_cache_size_tokens of vllm:cache_config_info and nothing else, nil
// when it is missing or is not a positive number, never 0, and never num_gpu_blocks times block_size.
// Every expected number is typed here by hand.

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
)

// capturedScrape is the real vLLM v0.32.0 capture's text.
func capturedScrape(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(capturePath)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// The real capture's own label is kv_cache_size_tokens="528160" (and 33010 blocks of 16, which is the
// same number): Sample gives 528160.
func TestSampleReadsTheKVCacheSizeFromTheRealCapture(t *testing.T) {
	h := sampleOf(t, capturedScrape(t))
	if h.KVCacheTokens == nil || *h.KVCacheTokens != 528160 {
		t.Fatalf("KVCacheTokens %v, want 528160", h.KVCacheTokens)
	}
}

// The engine double reports the size it is configured with, in the label: Sample reads it.
func TestSampleReadsTheKVCacheSizeOfTheDouble(t *testing.T) {
	for _, tc := range []struct {
		name  string
		modes string
		tune  func(*enginedouble.Config)
		want  int64
	}{
		{"the default, 2^20", "", nil, 1048576},
		{"kv-low", "kv-low", nil, 1000},
		{"a configured size", "", func(c *enginedouble.Config) { c.KVCacheTokens = 400000 }, 400000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := double(t, tc.modes, tc.tune)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			h, err := c.Sample(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if h.KVCacheTokens == nil || *h.KVCacheTokens != tc.want {
				t.Errorf("KVCacheTokens %v, want %d", h.KVCacheTokens, tc.want)
			}
		})
	}
}

// The double's mode that omits vllm:cache_config_info alone gives nil, not 0, through the real read, and
// every other series of the sample is still known: the nil is the missing series' doing.
func TestSampleOfADoubleWithoutCacheConfigInfoGivesNilNotZero(t *testing.T) {
	c := double(t, "no-cache-config-info", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	h, err := c.Sample(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if h.KVCacheTokens != nil {
		t.Errorf("KVCacheTokens %d, want nil (unknown)", *h.KVCacheTokens)
	}
	if h.RunningRequests == nil || h.KVCacheUsage == nil || h.Preemptions == nil {
		t.Errorf("the mode dropped more than the one series: %+v", h)
	}
}

// No fallback: the capture's num_gpu_blocks="33010" and block_size="16" would make 528160 for
// kvCapacity, which qualification still uses, but a label that is not a positive number leaves the
// sample's KVCacheTokens nil. Each row replaces only that label of the real capture.
func TestSampleTakesNoFallbackWhenTheKVCacheSizeLabelIsNotAPositiveNumber(t *testing.T) {
	scrape := capturedScrape(t)
	const label = `kv_cache_size_tokens="528160"`
	if strings.Count(scrape, label) != 1 || !strings.Contains(scrape, `num_gpu_blocks="33010"`) || !strings.Contains(scrape, `block_size="16"`) {
		t.Fatal("the capture no longer has the label and the block counts this test replaces and keeps")
	}
	for _, v := range []string{"None", "", "0", "-5", "abc", "52.8", "99999999999999999999"} {
		t.Run("label "+v, func(t *testing.T) {
			h := sampleOf(t, strings.Replace(scrape, label, `kv_cache_size_tokens="`+v+`"`, 1))
			if h.KVCacheTokens != nil {
				t.Errorf("kv_cache_size_tokens=%q gave %d, want nil", v, *h.KVCacheTokens)
			}
		})
	}
	// The same scrape with the label dropped altogether.
	h := sampleOf(t, strings.Replace(scrape, label+",", "", 1))
	if h.KVCacheTokens != nil {
		t.Errorf("no kv_cache_size_tokens label gave %d, want nil", *h.KVCacheTokens)
	}
	// And the control: kvCapacity does fall back on those blocks, which is why the fallback test is
	// not vacuous.
	m, err := parseMetrics(strings.Replace(scrape, label, `kv_cache_size_tokens="None"`, 1))
	if err != nil {
		t.Fatal(err)
	}
	if got := kvCapacity(m.cacheConfig()); got != 528160 {
		t.Errorf("kvCapacity of the same scrape = %d, want 528160, the fallback that Sample does not take", got)
	}
}
