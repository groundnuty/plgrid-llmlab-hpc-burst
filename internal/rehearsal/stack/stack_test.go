package stack

// The parts of the package that need no stack: the request bodies, the reading
// of a stream's end and of the front's errors, the binaries check, the
// environment for up, and the root k3s guard. Starting the stack is tested by
// its users (internal/alpha's stand-in run, the rehearsal).

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestChatBody(t *testing.T) {
	pad := "a \"quoted\" line\nand a \\ backslash"
	for name, c := range map[string]struct {
		body   string
		stream bool
		msgs   int
	}{
		"plain":           {ChatBody("example-org/m", 8, 0, false, ""), false, 1},
		"streamed, fixed": {ChatBody("example-org/m", 8, 8, true, ""), true, 1},
		"padded":          {ChatBody("example-org/m", 8, 0, false, pad), false, 2},
	} {
		var b struct {
			Model     string `json:"model"`
			MaxTokens int    `json:"max_tokens"`
			MinTokens int    `json:"min_tokens"`
			Stream    bool   `json:"stream"`
			Messages  []struct {
				Content string `json:"content"`
			} `json:"messages"`
		}
		if err := json.Unmarshal([]byte(c.body), &b); err != nil {
			t.Fatalf("%s: not JSON: %v: %s", name, err, c.body)
		}
		if b.Model != "example-org/m" || b.MaxTokens != 8 || b.Stream != c.stream || len(b.Messages) != c.msgs {
			t.Errorf("%s: %+v", name, b)
		}
		if c.msgs == 2 && b.Messages[1].Content != pad {
			t.Errorf("%s: the pad came back as %q", name, b.Messages[1].Content)
		}
	}
	var fixed struct {
		MinTokens int `json:"min_tokens"`
	}
	_ = json.Unmarshal([]byte(ChatBody("m", 8, 8, true, "")), &fixed)
	if fixed.MinTokens != 8 {
		t.Errorf("min_tokens %d, want 8", fixed.MinTokens)
	}
}

func TestSSEDoneAndErrors(t *testing.T) {
	full := "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\ndata: {\"choices\":[{\"finish_reason\":\"length\"}]}\n\ndata: [DONE]\n"
	if done, fin := SSEDone([]byte(full)); !done || !fin {
		t.Errorf("a full stream: done %v finished %v", done, fin)
	}
	if done, fin := SSEDone([]byte("data: {\"choices\":[{\"delta\":{}}]}\n\ndata: [DONE]\n")); !done || fin {
		t.Errorf("no finish reason: done %v finished %v", done, fin)
	}
	cut := "data: {\"choices\":[{\"delta\":{}}]}\n\ndata: {\"error\":{\"code\":\"stream_cut\",\"detail\":null}}\n"
	if done, _ := SSEDone([]byte(cut)); done {
		t.Error("a cut stream read as done")
	}
	if c := StreamErrorCode([]byte(cut)); c != "stream_cut" {
		t.Errorf("StreamErrorCode = %q, want stream_cut", c)
	}
	if c := StreamErrorCode([]byte(full)); c != "" {
		t.Errorf("a stream with no error event: %q", c)
	}
	if e := ErrorOf([]byte(`{"error":{"code":"no_ready_target"}}`)); e.Code != "no_ready_target" {
		t.Errorf("ErrorOf = %+v", e)
	}
	if e := ErrorOf([]byte(`not json`)); e.Code != "" {
		t.Errorf("ErrorOf of no JSON = %+v", e)
	}
}

func TestBinariesMissing(t *testing.T) {
	dir := t.TempDir()
	exe := func(name string, mode os.FileMode) string {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("#!/bin/sh\n"), mode); err != nil {
			t.Fatal(err)
		}
		return p
	}
	b := Binaries{HPCBurst: exe("hpcburst", 0o755), Controller: exe("controller", 0o755), Front: exe("front", 0o644),
		Agent: filepath.Join(dir, "absent"), K3sDir: dir, Frps: exe("frps", 0o755), Frpc: exe("frpc", 0o755)}
	exe("k3s-1.36.5/k3s", 0o755)
	m := b.Missing()
	if !slices.Equal(m, []string{b.Front, b.Agent}) {
		t.Errorf("Missing = %v, want the front (not executable) and the agent (absent)", m)
	}
}

func TestCleanEnvDropsTheFakes(t *testing.T) {
	t.Setenv("HPCBURST_SSH", "/fake/ssh")
	t.Setenv("HPCBURST_APPTAINER", "/fake/apptainer")
	t.Setenv("HB_TEST_ROLE", "engine")
	env := CleanEnv("HF_ENDPOINT=http://127.0.0.1:1")
	for _, kv := range env {
		for _, p := range []string{"HPCBURST_SSH=", "HPCBURST_APPTAINER=", "HB_TEST_ROLE="} {
			if strings.HasPrefix(kv, p) {
				t.Errorf("CleanEnv kept %s", kv)
			}
		}
	}
	if env[len(env)-1] != "HF_ENDPOINT=http://127.0.0.1:1" {
		t.Errorf("CleanEnv dropped the extra: %v", env[len(env)-1])
	}
}

func TestRootK3sGuard(t *testing.T) {
	if RootK3sBefore("6443,10250", true) == nil {
		t.Error("a required root k3s with a port missing passed")
	}
	if RootK3sBefore("", false) != nil || RootK3sBefore("6443,6444,10250", true) != nil {
		t.Error("a host as required failed")
	}
	if RootK3sAfter("6443,6444,10250", "6443,10250") == nil {
		t.Error("a listener lost across the run passed")
	}
	if RootK3sAfter("", "") != nil {
		t.Error("no root k3s before or after failed")
	}
}
