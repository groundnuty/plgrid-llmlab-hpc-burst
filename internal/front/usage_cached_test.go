package front

import "testing"

// TestUsageCachedTokensAreNullFromLLMLab: LLMLab's usage has no
// prompt_tokens_details (a real run, issue #64, 31 Oct 2026), so a managed
// target's record holds an explicit null cachedTokens next to the counts it did
// get: unknown, not 0. The LLMLab double sends none either (llmlabtest's
// TestUsageNamesNoCachedTokens), so this is the front's reading of a gateway
// that omits the member. The Instance engine double sends none by default
// either (TestUsageCachedTokensAreNullFromAnInstanceEngine), and a count it
// is told to send reaches the record.
func TestUsageCachedTokensAreNullFromLLMLab(t *testing.T) {
	m := newErrManagedFix(t, nil)
	for _, c := range []struct {
		name string
		body string
	}{
		{"whole reply", errChatBody(efModel, false, "one two three")},
		{"stream with usage", `{"model": "` + efModel + `", "stream": true, "stream_options": {"include_usage": true},` +
			` "messages": [{"role": "user", "content": "one two three"}]}`},
	} {
		t.Run(c.name, func(t *testing.T) {
			resp := m.post("/v1/chat/completions", c.body)
			readAll(t, resp)
			id := resp.Header.Get("Hpcburst-Request-Id")
			rec := m.record(id)
			if rec.CountsSource == nil || *rec.CountsSource != "engine" || !ptrIs(rec.PromptTokens, 3) || rec.CompletionTokens == nil {
				t.Fatalf("countsSource %s prompt %s completion %s, want engine 3 and a count: the null below must not come from missing counts",
					strStr(rec.CountsSource), ptrStr(rec.PromptTokens), ptrStr(rec.CompletionTokens))
			}
			if rec.CachedTokens != nil {
				t.Errorf("cachedTokens = %s, want null: LLMLab sent no cached count", ptrStr(rec.CachedTokens))
			}
			if v, ok := rawRecord(t, m.harness, id)["cachedTokens"]; !ok || v != nil {
				t.Errorf("cachedTokens = %v (present %v) in the file, want an explicit null", v, ok)
			}
		})
	}
}
