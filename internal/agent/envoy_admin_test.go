package agent_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// Envoy's admin interface as the agent configures it, with the real Envoy 1.40.2: a Unix socket that
// allows only GET /clusters?format=json, the agent's read of each worker's health (HBEP-5; the ruling
// on #429, Q3). The admin can otherwise stop Envoy (/quitquitquit), change its runtime
// (/runtime_modify, which could re-enable panic mode) and dump its config. The control is the test's
// own, never the agent's: the same admin with allow_paths deleted.

// adminDo sends one request to the admin socket and returns the status and body.
func adminDo(t *testing.T, r *router, method, path string) (int, string) {
	t.Helper()
	c := unixClient(r.admin())
	defer c.Transport.(*http.Transport).CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, "http://admin"+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// clusterNames reads the cluster names out of the admin's /clusters?format=json.
func clusterNames(t *testing.T, body string) []string {
	t.Helper()
	var doc struct {
		Statuses []struct {
			Name string `json:"name"`
		} `json:"cluster_statuses"`
	}
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("/clusters?format=json is not the JSON it should be (%v): %.200s", err, body)
	}
	var names []string
	for _, s := range doc.Statuses {
		names = append(names, s.Name)
	}
	return names
}

// adminCalls are the requests the agent's admin must refuse. /quitquitquit comes first: had it worked,
// Envoy would be going away, and each request after it, and the router's own, would show that.
var adminCalls = []struct{ method, path string }{
	{"POST", "/quitquitquit"},
	{"GET", "/clusters"},
	{"GET", "/stats"},
	{"GET", "/config_dump"},
	{"GET", "/listeners"},
	{"POST", "/runtime_modify?upstream.healthy_panic_threshold=50"},
}

func TestEnvoyAdminSocketIsReadOnly(t *testing.T) {
	t.Run("the agent's config", func(t *testing.T) {
		r := agentRouter(t, workerSpec{})
		status, body := adminDo(t, r, "GET", "/clusters?format=json")
		if status != http.StatusOK {
			t.Fatalf("GET /clusters?format=json: status %d, body %.200s; want 200", status, body)
		}
		names := clusterNames(t, body)
		if len(names) != 1 || names[0] != "workers" {
			t.Errorf("the admin's clusters are %v, want exactly [workers]", names)
		}
		for _, c := range adminCalls {
			if status, body := adminDo(t, r, c.method, c.path); status != http.StatusForbidden {
				t.Errorf("%s %s: status %d, body %.100q; want 403", c.method, c.path, status, body)
			}
		}
		// Envoy is still running: its process has not exited, and both of its listeners still answer.
		select {
		case <-r.proc.Group.Exited():
			t.Fatalf("Envoy exited (code %d) after the admin calls", r.proc.Group.ExitCode())
		default:
		}
		if status, _ := adminDo(t, r, "GET", "/clusters?format=json"); status != http.StatusOK {
			t.Errorf("the admin's read after the refused calls: status %d, want 200", status)
		}
		resp, cancel, err := getVia(r.client(t), "/v1/models", time.Minute, nil)
		if err != nil {
			t.Fatalf("a request through the router after the admin calls: %v", err)
		}
		defer cancel()
		got, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || string(got) != workerBody {
			t.Errorf("a request through the router after the admin calls: status %d, body %q; want the worker's 200", resp.StatusCode, got)
		}
	})
	t.Run("control: allow_paths deleted, so the admin is open", func(t *testing.T) {
		r := controlRouter(t, workerSpec{}, func(m map[string]any) { delete(m["admin"].(map[string]any), "allow_paths") })
		for _, c := range adminCalls[1:5] { // the reads, which leave Envoy as it is
			if status, body := adminDo(t, r, c.method, c.path); status != http.StatusOK || len(strings.TrimSpace(body)) == 0 {
				t.Errorf("%s %s: status %d, body %.100q; want the open admin's 200 and a body", c.method, c.path, status, body)
			}
		}
		if status, _ := adminDo(t, r, "POST", "/quitquitquit"); status != http.StatusOK {
			t.Errorf("POST /quitquitquit: status %d, want the open admin's 200", status)
		}
		select {
		case <-r.raw.Exited():
		case <-time.After(30 * time.Second):
			t.Errorf("Envoy was still running 30 s after POST /quitquitquit on an open admin")
		}
	})
}
