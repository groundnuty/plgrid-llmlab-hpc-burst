package llmlabtest

import (
	"bytes"
	"net/http"
	"testing"
)

// The real gateway's usage member names no cached tokens: it has no
// prompt_tokens_details at all (a real run through the front, 31 Oct 2026,
// issue #64; `@hpc-burst-science-agent`'s ruling of the same day). The double
// must not say more than the gateway does, or a test of the front would see a
// count (a 0) that LLMLab never sends, and the front's usage record would hold
// 0 where the real one holds null. The double has no scenario that asks for a
// value, so the default is the only behaviour.
//
// The positive control: each body is searched for its usage member's token
// counts first, so a body with no usage at all cannot pass for one without
// cached tokens.
func TestUsageNamesNoCachedTokens(t *testing.T) {
	s := startS(t)
	const input = `"messages":[{"role":"user","content":"one two three"}]`
	for _, c := range []struct{ name, route, body, count string }{
		{"chat", routeChat, `{"model":"example-9b",` + input + `}`, `"prompt_tokens"`},
		{"chat stream with usage", routeChat, `{"model":"example-9b","stream":true,"stream_options":{"include_usage":true},` + input + `}`, `"prompt_tokens"`},
		{"completions", routeCompletions, `{"model":"example-9b","prompt":"one two three"}`, `"prompt_tokens"`},
		{"responses", routeResponses, `{"model":"example-9b","input":"one two three"}`, `"input_tokens"`},
	} {
		t.Run(c.name, func(t *testing.T) {
			a := ask(t, s, http.MethodPost, c.route, keyOne, c.body)
			requireStatus(t, a, http.StatusOK, c.name)
			if !bytes.Contains(a.Body, []byte(c.count)) {
				t.Fatalf("the answer has no %s in its usage: %s", c.count, a.Body)
			}
			for _, banned := range []string{"cached", "tokens_details"} {
				if bytes.Contains(a.Body, []byte(banned)) {
					t.Errorf("the answer names %q, which LLMLab's usage does not: %s", banned, a.Body)
				}
			}
		})
	}
}
