package enginedouble

import (
	"net/http"
	"strings"
	"testing"
)

// A message whose name is null is refused with a 400, as vLLM refuses it: every message type of its union declares name as a string
// (S13's case enc/message-name-null; internal/agent/routerbytes/cases). A string name, and none, are answered, and the refusal names the
// message by its place, as the validator does.
func TestAMessageWithANullNameIsRefused(t *testing.T) {
	s := serve(t, cfgFor())
	for _, c := range []struct {
		name string
		msg  map[string]any
		want int
	}{
		{"a null name", map[string]any{"role": "user", "content": "hello", "name": nil}, http.StatusBadRequest},
		{"a string name", map[string]any{"role": "user", "content": "hello", "name": "ada"}, http.StatusOK},
		{"no name", map[string]any{"role": "user", "content": "hello"}, http.StatusOK},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := s.post("/v1/chat/completions", map[string]any{"model": cfgFor().ServedName, "max_tokens": 4, "messages": []map[string]any{c.msg}})
			if r.status != c.want {
				t.Fatalf("status %d, want %d: %s", r.status, c.want, r.body)
			}
			if c.want == http.StatusBadRequest && !strings.Contains(string(r.body), "messages.0.name: Input should be a valid string") {
				t.Errorf("the refusal does not name the message and the rule: %s", r.body)
			}
		})
	}
	// The second message is the one named.
	r := s.post("/v1/chat/completions", map[string]any{"model": cfgFor().ServedName, "max_tokens": 4,
		"messages": []map[string]any{{"role": "user", "content": "a"}, {"role": "user", "content": "b", "name": nil}}})
	if r.status != http.StatusBadRequest || !strings.Contains(string(r.body), "messages.1.name") {
		t.Errorf("a null name on the second message: status %d: %s", r.status, r.body)
	}
}
