package llmlabtest

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

// Issue #69's loopback port block (IMPL-COMMON.md, Other listeners).
const testPortLo, testPortHi = 29340, 29359

const testKey = "example-key-0001"

func do(t *testing.T, s *Server, method, path, key, body string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, s.URL()+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, b
}

// A fresh Server lists its models, answers a chat request and streams one.
func TestSmoke(t *testing.T) {
	s := Start(t, Options{PortLo: testPortLo, PortHi: testPortHi, Keys: []string{testKey},
		Models: []Model{{ID: "example-9b"}, {ID: "example-27b", Revision: "2026-09"}}})

	resp, b := do(t, s, http.MethodGet, "/v1/models", testKey, "")
	want := `{"data":[{"id":"example-9b","object":"model","created":1767225600,"owned_by":"llmlab"},{"id":"example-27b","object":"model","created":1767225600,"owned_by":"llmlab","revision":"2026-09"}],"object":"list"}` + "\n"
	if resp.StatusCode != 200 || string(b) != want {
		t.Fatalf("listing = %d %s, want 200 %s", resp.StatusCode, b, want)
	}

	resp, b = do(t, s, http.MethodPost, "/v1/chat/completions", testKey,
		`{"model":"example-9b","messages":[{"role":"user","content":"one two three"}]}`)
	var chat struct {
		Choices []struct {
			Message      struct{ Content string }
			FinishReason string `json:"finish_reason"`
		}
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
		}
	}
	if resp.StatusCode != 200 || json.Unmarshal(b, &chat) != nil || chat.Choices[0].Message.Content != DefaultReply ||
		chat.Usage.PromptTokens != 3 || chat.Usage.CompletionTokens != 5 || chat.Choices[0].FinishReason != "stop" {
		t.Fatalf("chat = %d %s", resp.StatusCode, b)
	}

	resp, b = do(t, s, http.MethodPost, "/v1/chat/completions", testKey,
		`{"model":"example-9b","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}`)
	if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("stream = %d %s", resp.StatusCode, resp.Header)
	}
	var text strings.Builder
	var done, usage bool
	sc := bufio.NewScanner(strings.NewReader(string(b)))
	for sc.Scan() {
		data, ok := strings.CutPrefix(sc.Text(), "data: ")
		if !ok {
			continue
		}
		if data == "[DONE]" {
			done = true
			continue
		}
		var c struct {
			Choices []struct{ Delta struct{ Content string } }
			Usage   *struct{}
		}
		if err := json.Unmarshal([]byte(data), &c); err != nil {
			t.Fatal(err)
		}
		for _, ch := range c.Choices {
			text.WriteString(ch.Delta.Content)
		}
		usage = usage || c.Usage != nil
	}
	if text.String() != DefaultReply || !done || !usage {
		t.Fatalf("stream text %q done %v usage %v:\n%s", text.String(), done, usage, b)
	}
	if n := s.Count(http.MethodPost, "/v1/chat/completions"); n != 2 {
		t.Fatalf("logged %d chat requests, want 2", n)
	}
}
