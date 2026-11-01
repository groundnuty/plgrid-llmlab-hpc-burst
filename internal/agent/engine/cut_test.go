package engine

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// hugeEngine answers with strings far above agentapi.MaxMessageBytes (256): a
// version, a fingerprint, a model name and the cache settings' values of 600
// bytes of two-byte characters, so a cut in the middle of a character would
// show.
func hugeEngine() http.Handler {
	huge := strings.Repeat("é", 300) // 600 bytes
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
		case "/version":
			_, _ = fmt.Fprintf(w, `{"version":%q}`, huge)
		case "/v1/models":
			_, _ = fmt.Fprintf(w, `{"data":[{"id":%q,"max_model_len":32768}]}`, huge)
		case "/tokenize":
			_, _ = io.WriteString(w, `{"count":9}`)
		case "/metrics":
			_, _ = fmt.Fprintf(w, "vllm:cache_config_info{cache_dtype=%q,block_size=%q,enable_prefix_caching=%q,gpu_memory_utilization=%q,num_gpu_blocks=\"100\"} 1.0\n", huge, huge, huge, huge)
		default:
			_, _ = fmt.Fprintf(w, `{"choices":[{"message":{"content":"hi","tool_calls":[]},"finish_reason":"stop","token_ids":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35]}],"usage":{"completion_tokens":35},"system_fingerprint":%q}`, huge)
		}
	})
}

func assertCut(t *testing.T, what, got string) {
	t.Helper()
	// 600 bytes cut to 256 at a character boundary: 128 two-byte characters
	if len(got) != agentapi.MaxMessageBytes || !utf8.ValidString(got) || got != strings.Repeat("é", 128) {
		t.Errorf("%s: %d bytes (valid UTF-8 %v), want the first 256 bytes, 128 whole characters", what, len(got), utf8.ValidString(got))
	}
}

// Verify cuts the version, the fingerprint and each settings value an engine
// gives to agentapi.MaxMessageBytes.
func TestVerifyCutsTheEnginesStrings(t *testing.T) {
	c := New(serveOn(t, hugeEngine()), "key")
	res := c.Verify(context.Background(), testGreedy)
	assertCut(t, "version", res.EngineVersion)
	assertCut(t, "fingerprint", res.Fingerprint)
	for _, flag := range []string{"kv-cache-dtype", "block-size", "enable-prefix-caching", "gpu-memory-utilization"} {
		assertCut(t, "setting "+flag, res.Settings[flag])
	}
}

// Qualify cuts the fingerprint and the served model names.
func TestQualifyCutsTheEnginesStrings(t *testing.T) {
	c := New(serveOn(t, hugeEngine()), "key")
	res := c.Qualify(context.Background(), agentapi.QualifyRequest{Shapes: agentapi.Shapes{OutputTypical: 8}}, testGreedy)
	assertCut(t, "fingerprint", res.Fingerprint)
	if len(res.ServedModelNames) != 1 {
		t.Fatalf("served names %q", res.ServedModelNames)
	}
	assertCut(t, "served name", res.ServedModelNames[0])
}

// A string within the limit is kept as it is.
func TestCutKeepsAShortString(t *testing.T) {
	if got := cut("0.32.0"); got != "0.32.0" {
		t.Errorf("cut = %q", got)
	}
	if got := cut(strings.Repeat("a", agentapi.MaxMessageBytes)); len(got) != agentapi.MaxMessageBytes {
		t.Errorf("a string of exactly the limit was cut to %d bytes", len(got))
	}
}
