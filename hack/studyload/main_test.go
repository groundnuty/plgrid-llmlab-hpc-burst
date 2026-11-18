package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
)

// cli runs the command with a config for one tiny workload against url, and returns its exit code, streams and records file.
func cli(t *testing.T, url, keyFile string) (code int, stdout, stderr, records string) {
	t.Helper()
	return cliConfig(t, fmt.Sprintf(`{"baseURL": %q, "model": "model", "apiKeyFile": %q, "workloads": [
	  {"name": "w", "sessions": 2, "turns": 2, "contextTokens": 5, "turnTokens": 3, "outputTokens": 4}]}`, url+"/v1", keyFile))
}

// cliConfig runs the command on a config given as text.
func cliConfig(t *testing.T, cfg string) (code int, stdout, stderr, records string) {
	t.Helper()
	dir := t.TempDir()
	cfgPath, outPath := filepath.Join(dir, "c.json"), filepath.Join(dir, "out.jsonl")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	var so, se bytes.Buffer
	code = run(context.Background(), []string{"-config", cfgPath, "-out", outPath}, &so, &se)
	b, _ := os.ReadFile(outPath)
	return code, so.String(), se.String(), string(b)
}

func writeKey(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestARunAgainstAnEngineExitsZero(t *testing.T) {
	srv := httptest.NewServer(enginedouble.Handler(enginedouble.DefaultConfig()))
	defer srv.Close()
	code, stdout, stderr, records := cli(t, srv.URL, "")
	if code != 0 || stderr != "" || strings.Count(records, "\n") != 4 || !strings.Contains(stdout, "workload w: sessions 2 (0 with errors), requests 4, errors 0,") {
		t.Errorf("exit %d, stderr %q, %d records, stdout:\n%s", code, stderr, strings.Count(records, "\n"), stdout)
	}
}

func TestFailedRequestsExitOneAndTheKeyStaysOut(t *testing.T) {
	cfg := enginedouble.DefaultConfig()
	cfg.APIKey = "the-key-the-engine-wants"
	srv := httptest.NewServer(enginedouble.Handler(cfg))
	defer srv.Close()
	const wrong = "SENTINEL-wrong-key"
	code, stdout, stderr, records := cli(t, srv.URL, writeKey(t, wrong+"\n"))
	if code != 1 || !strings.Contains(stdout, "errors 2,") { // two sessions, each stops at its first request
		t.Errorf("exit %d, want 1; stdout:\n%s", code, stdout)
	}
	for name, text := range map[string]string{"stdout": stdout, "stderr": stderr, "records": records} {
		if strings.Contains(text, wrong) || strings.Contains(text, cfg.APIKey) {
			t.Errorf("a key reached %s", name)
		}
	}
	if !strings.Contains(records, `"error":"status 401"`) {
		t.Errorf("records lack the 401 class: %s", records)
	}
}

func TestUsageAndFileErrorsExitTwo(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	dir := t.TempDir()
	good := filepath.Join(dir, "good.json")
	_ = os.WriteFile(good, []byte(fmt.Sprintf(`{"baseURL": %q, "model": "m", "workloads": [{"name": "w", "sessions": 1, "turns": 1, "contextTokens": 1, "outputTokens": 1}]}`, srv.URL)), 0o600)
	bad := filepath.Join(dir, "bad.json")
	_ = os.WriteFile(bad, []byte(`{"colour": 1}`), 0o600)
	existing := filepath.Join(dir, "existing.jsonl")
	_ = os.WriteFile(existing, []byte("keep me\n"), 0o600)

	cases := []struct {
		name string
		args []string
		want string
	}{
		{"no arguments", nil, "usage"},
		{"no out", []string{"-config", good}, "usage"},
		{"extra argument", []string{"-config", good, "-out", filepath.Join(dir, "x"), "more"}, "usage"},
		{"unknown flag", []string{"-nope"}, "flag provided but not defined"},
		{"missing config", []string{"-config", filepath.Join(dir, "none.json"), "-out", filepath.Join(dir, "y")}, "config"},
		{"bad config", []string{"-config", bad, "-out", filepath.Join(dir, "z")}, "colour"},
		{"out exists", []string{"-config", good, "-out", existing}, "records file"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var so, se bytes.Buffer
			if code := run(context.Background(), tc.args, &so, &se); code != 2 || !strings.Contains(se.String(), tc.want) || so.Len() != 0 {
				t.Errorf("exit %d, stdout %q, stderr %q, want 2, nothing, and %q", code, so.String(), se.String(), tc.want)
			}
		})
	}
	if b, _ := os.ReadFile(existing); string(b) != "keep me\n" {
		t.Errorf("a records file that existed was changed: %q", b)
	}
}

func TestKeyFileMustHoldOneLine(t *testing.T) {
	for name, content := range map[string]string{"empty": "", "blank": "  \n", "two lines": "SENTINEL-a\nSENTINEL-b\n"} {
		t.Run(name, func(t *testing.T) {
			_, err := readKey(writeKey(t, content))
			if err == nil || strings.Contains(err.Error(), "SENTINEL") {
				t.Errorf("error %v: want one that does not carry the content", err)
			}
		})
	}
	if _, err := readKey(filepath.Join(t.TempDir(), "absent")); err == nil || strings.Contains(err.Error(), "absent") {
		t.Errorf("error %v: want the reason without the path", err)
	}
	if k, err := readKey(writeKey(t, "  abc123 \n")); err != nil || k != "abc123" {
		t.Errorf("key %q, %v: want abc123", k, err)
	}
}

// An endpoint that echoes the key it was sent in every attribution header, on a 200 and on an error status, must not get it into the
// records file, the summary or stderr. The marker is looked for too, so the test cannot pass by the key never having been sent.
func TestAKeyTheEndpointEchoesReachesNoOutput(t *testing.T) {
	const key = "SENTINEL-echo-key-0123456789"
	var sent atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if got == key {
			sent.Add(1)
		}
		for _, h := range []string{"Hpcburst-Request-Id", "Hpcburst-Emitter", "Hpcburst-Provider", "Hpcburst-Deployment-Id", "Hpcburst-Instance"} {
			w.Header().Set(h, "x-"+got+"-y")
		}
		if r.Header.Get("X-Mode") == "fail" {
			http.Error(w, "an error status", http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, sse(content("a"), content(" b"), `{"choices":[],"usage":{"prompt_tokens":9,"completion_tokens":2,"total_tokens":11}}`, "[DONE]"))
	}))
	defer srv.Close()
	code, stdout, stderr, records := cliConfig(t, fmt.Sprintf(`{"baseURL": %q, "model": "model", "apiKeyFile": %q, "workloads": [
	  {"name": "ok",   "sessions": 1, "turns": 1, "contextTokens": 3, "outputTokens": 2, "headers": {"x-mode": "ok"}},
	  {"name": "fail", "sessions": 1, "turns": 1, "contextTokens": 3, "outputTokens": 2, "headers": {"x-mode": "fail"}}]}`,
		srv.URL+"/v1", writeKey(t, key+"\n")))
	srv.Close()
	if code != 1 || sent.Load() != 2 {
		t.Fatalf("exit %d (want 1: the fail workload errors), the endpoint received the key on %d requests (want 2)", code, sent.Load())
	}
	for name, text := range map[string]string{"stdout": stdout, "stderr": stderr, "records file": records} {
		if strings.Contains(text, "SENTINEL") {
			t.Errorf("the key reached the %s:\n%s", name, text)
		}
	}
	// Both requests, the 200 and the 500, have all five values recorded, redacted.
	if n := strings.Count(records, `"provider":"x-(redacted)-y"`); n != 2 {
		t.Errorf("%d records have the redacted provider, want 2:\n%s", n, records)
	}
	if !strings.Contains(stdout, "route provider=x-(redacted)-y deployment=x-(redacted)-y instance=x-(redacted)-y: sessions 2 (1 with errors), requests 2, errors 1,") {
		t.Errorf("summary lacks the redacted route line:\n%s", stdout)
	}
}
