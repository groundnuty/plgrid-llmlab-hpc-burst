package agent_test

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The image is staged in the partition's format: a SIF pulled by digest on
// sif partitions, a sandbox built from the same digest on sandbox ones (spike
// S1, probe 2), at agentapi.StagedImagePath, and the worker runs from it.
func TestImageIsStagedInTheProfilesFormat(t *testing.T) {
	for _, tc := range []struct {
		format  agentapi.ImageFormat
		command string
	}{
		{agentapi.ImageFormatSIF, "pull"},
		{agentapi.ImageFormatSandbox, "build"},
	} {
		t.Run(string(tc.format), func(t *testing.T) {
			h := newHarness(t, harnessOptions{})
			d := h.runState("", 1, 0, nil)
			d.Settings.ImageFormat = tc.format
			h.set(d)
			h.waitReport("worker-0 running", 20*time.Second, running)
			cmds := h.appCommands()
			if len(cmds) < 2 || cmds[0] != tc.command || cmds[1] != "exec" {
				t.Fatalf("apptainer commands %q, want %s then exec", cmds, tc.command)
			}
			want := filepath.Join(h.stagingDir, agentapi.StagedImagePath(hDigest, tc.format))
			argv := h.execs()[0].Argv
			found := false
			for _, a := range argv {
				found = found || a == want
			}
			if !found {
				t.Fatalf("exec %q does not run the staged image %s", argv, want)
			}
			if fi, err := os.Stat(want); err != nil || fi.IsDir() != (tc.format == agentapi.ImageFormatSandbox) {
				t.Fatalf("staged image %s: %v %v", want, fi, err)
			}
		})
	}
}

// A whole-Instance restart re-stages a file that failed its rehash: the bad
// copy is moved aside, the file is downloaded again from the Hub and checked,
// and the next verification finds it whole (HBEP-7, Instance start: a failed
// rehash restarts the Instance at the whole-Instance rung, which stages and
// rehashes again).
func TestWholeInstanceRestartReStagesAFileThatFailedItsRehash(t *testing.T) {
	const good = "{}\n"
	// printf '{}\n' | sha256sum
	const goodSum = "ca3d163bab055381827226140568f3bef7eaac187cebd76878e0b63e9e442356"
	rev := launch("", "").Checkpoint.Revision
	h := newHarness(t, harnessOptions{hub: map[string]string{"/org/model/resolve/" + rev + "/config.json": good}})
	dir := filepath.Join(h.stagingDir, agentapi.StagedCheckpointDir("org/model", rev))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte("{\"edited\":1}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	files := []agentapi.FileDigest{{Path: "config.json", SHA256: goodSum}}
	h.set(h.runState("", 1, 0, verifyChecks(1, files)))
	r := h.waitReport("verification 1", 30*time.Second, verified(1))
	if f := r.Instance.Checks.Verify.Files; len(f) != 1 || f[0].Outcome != agentapi.FileMismatch {
		t.Fatalf("first verification files %+v, want one mismatch", f)
	}
	if n := h.hubRequests(); n != 0 {
		t.Fatalf("the first start downloaded %d files; a mismatch at the first start is only reported", n)
	}

	h.set(h.runState("", 2, 0, verifyChecks(2, files)))
	r = h.waitReport("verification 2", 30*time.Second, verified(2))
	if f := r.Instance.Checks.Verify.Files; len(f) != 1 || f[0].Outcome != agentapi.FileMatch {
		t.Fatalf("after the whole-Instance restart files %+v, want a match", f)
	}
	b, err := os.ReadFile(filepath.Join(dir, "config.json"))
	if err != nil || string(b) != good {
		t.Fatalf("config.json now holds %q (%v), want the Hub's copy", b, err)
	}
	aside, _ := filepath.Glob(filepath.Join(dir, "config.json.*"))
	if len(aside) != 1 {
		t.Fatalf("moved-aside copies %q, want one", aside)
	}
	if w := worker0(r); w.Generation != 0 || r.Instance.Generation != 2 {
		t.Fatalf("instance generation %d, worker %+v", r.Instance.Generation, w)
	}
	if n := len(h.execs()); n != 2 {
		t.Fatalf("%d starts, want 2", n)
	}
}

// The agent serves the Instance's serving path on its own socket and forwards
// it to the engine, which still checks the Instance's key; a streamed answer
// arrives through it.
func TestServingPathIsForwardedToTheEngine(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	h.set(h.runState("", 1, 0, nil))
	r := h.waitReport("worker-0 running", 20*time.Second, running)
	key := r.Instance.Key
	sock := strings.TrimPrefix(r.Instance.ServingEndpoint, "unix:")
	c := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", sock)
	}}}
	get := func(auth string) int {
		req, _ := http.NewRequest(http.MethodGet, "http://instance/v1/models", nil)
		if auth != "" {
			req.Header.Set("Authorization", "Bearer "+auth)
		}
		resp, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode
	}
	if code := get(key); code != http.StatusOK {
		t.Fatalf("GET /v1/models with the key: %d", code)
	}
	if code := get(""); code != http.StatusUnauthorized {
		t.Fatalf("GET /v1/models without the key: %d, want the engine's 401", code)
	}
	body := `{"model":"` + hServed + `","messages":[{"role":"user","content":"hello there"}],"max_tokens":8,"stream":true,"ignore_eos":true}`
	req, _ := http.NewRequest(http.MethodPost, "http://instance/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	all, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(all), "data: [DONE]") {
		t.Fatalf("stream through the forward: %d %q", resp.StatusCode, all)
	}
	// The engine's metrics show the request in the next health sample.
	h.waitReport("a health sample with the request", 10*time.Second, func(r agentapi.Report) bool {
		w := worker0(r)
		return w != nil && w.Health != nil && w.Health.ProbeOK && w.Health.GeneratedTokens >= 8 && w.Health.ResponsesByStatus["2xx"] >= 1
	})
}

// hubRequests counts the downloads the fake Hub served.
func (h *harness) hubRequests() int {
	h.hubMu.Lock()
	defer h.hubMu.Unlock()
	return h.hubHits
}

// startHub serves files over a Unix socket for the stager, as the Hub's
// resolve endpoint does.
func (h *harness) startHub(files map[string]string) (*http.Client, string) {
	sock := filepath.Join(h.root, "hub.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		h.t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		content, ok := files[r.URL.Path]
		if !ok {
			b, _ := json.Marshal(map[string]string{"error": "Entry not found"})
			w.WriteHeader(http.StatusNotFound)
			w.Write(b)
			return
		}
		h.hubMu.Lock()
		h.hubHits++
		h.hubAuth = append(h.hubAuth, r.Header.Get("Authorization"))
		h.hubMu.Unlock()
		io.WriteString(w, content)
	})}
	go srv.Serve(ln)
	h.t.Cleanup(func() { srv.Close() })
	return &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", sock)
	}}}, "http://hub.test"
}
