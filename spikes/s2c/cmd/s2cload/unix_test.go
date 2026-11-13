package main

import (
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The one change from S2b's client: -unix dials the router's Unix socket (the Envoy arms' listener,
// router.sock) instead of the URL's host.

func TestMain(m *testing.M) {
	if os.Getenv("S2CLOAD_TEST_MAIN") == "1" {
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestUnixSocketDial(t *testing.T) {
	dir, err := os.MkdirTemp("", "s2cl") // short: a Unix socket path
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	sock := filepath.Join(dir, "router.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" || r.Header.Get("x-session-id") != "u1-s000" {
			http.Error(w, "unexpected "+r.URL.Path+" "+r.Header.Get("x-session-id"), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(okStream))
	})}
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()
	out := filepath.Join(dir, "client.jsonl")
	// The URL's host has no listener: only the -unix dial can reach the server.
	cmd := exec.Command(os.Args[0], "-unix", sock, "-url", "http://localhost:1/v1/chat/completions", "-run", "u1", "-out", out,
		"-sessions", "1", "-turns", "1", "-concurrency", "1", "-reply-tokens", "2", "-start-jitter", "0",
		"-sys-words", "5", "-task-words", "5", "-tool-min", "1", "-tool-max", "2")
	cmd.Env = append(os.Environ(), "S2CLOAD_TEST_MAIN=1")
	b, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(b), "failed_attempts=0 aborted_sessions=0") {
		t.Fatalf("client over the socket: %v\n%s", err, b)
	}
	rec, err := os.ReadFile(out)
	if err != nil || strings.Count(string(rec), "\n") != 1 || strings.Contains(string(rec), `"err"`) {
		t.Fatalf("records: %v\n%s", err, rec)
	}
}
