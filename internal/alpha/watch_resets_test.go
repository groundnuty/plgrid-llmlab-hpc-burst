package alpha

// Issue #285 (the science agent's ruling on PR #292, 9 Nov 2026): a liveRun's WatchResets must report the resets of its watch, and a test
// has to see a real, non-zero count reach the row's text to post. The stand-in tests could not: they compare the post with
// st.WatchResets(), the same call, and a run with no reset reports 0 whatever WatchResets returns, so a WatchResets that returned 0 passed
// every test. Two verifications prove nothing here, and this file uses neither: comparing the post with what WatchResets returned (it is
// the code under test), and a run that never has a reset (the count is 0 with or without the method).
//
// The count here is made by a real stack.Recorder following a fake API server that expires the versions a watch resumes from: instances
// twice, allocations once and deployments never, so the expected 3 comes from this script of the server, not from any code of the package.
// The server speaks plain HTTP on a port the kernel chooses, on 127.0.0.1.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// expiryPlan is how many times the fake API server answers a watch of a kind with "that version has expired", before it serves the kind's
// watches normally (open, and quiet, until the client ends them).
var expiryPlan = map[string]int{"instances": 2, "allocations": 1, "modeldeployments": 0}

// expiriesInPlan is the count the Recorder must show, from expiryPlan alone: every expiry is one reset of one stream.
func expiriesInPlan() int {
	n := 0
	for _, k := range expiryPlan {
		n += k
	}
	return n
}

// kinds of the fake API server, by the resource the client asks for.
var fakeKinds = map[string]string{"instances": "Instance", "allocations": "Allocation", "modeldeployments": "ModelDeployment"}

// fakeAPIServer serves what a watch's client asks of the API server for the three kinds the Recorder follows: discovery, an empty LIST at a
// fixed version, and watches that expire as expiryPlan says. It returns the directory of a kubeconfig that points at it, the form
// stack.StartWatch takes.
func fakeAPIServer(t *testing.T, namespace string) string {
	t.Helper()
	group := v1alpha1.Group + "/" + v1alpha1.Version
	var mu sync.Mutex
	watches := map[string]int{}
	reply := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(v); err != nil {
			t.Errorf("fake API server: writing a reply: %v", err)
		}
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case path == "/api":
			reply(w, map[string]any{"kind": "APIVersions", "versions": []string{"v1"}})
			return
		case path == "/apis":
			reply(w, map[string]any{"kind": "APIGroupList", "apiVersion": "v1", "groups": []any{map[string]any{
				"name":             v1alpha1.Group,
				"versions":         []any{map[string]string{"groupVersion": group, "version": v1alpha1.Version}},
				"preferredVersion": map[string]string{"groupVersion": group, "version": v1alpha1.Version},
			}}})
			return
		case path == "/apis/"+group:
			var res []any
			for plural, kind := range fakeKinds {
				res = append(res, map[string]any{"name": plural, "singularName": strings.ToLower(kind), "namespaced": true, "kind": kind,
					"verbs": []string{"get", "list", "watch"}})
			}
			reply(w, map[string]any{"kind": "APIResourceList", "apiVersion": "v1", "groupVersion": group, "resources": res})
			return
		}
		prefix := "/apis/" + group + "/namespaces/" + namespace + "/"
		plural, found := strings.CutPrefix(path, prefix)
		kind, known := fakeKinds[plural]
		if !found || !known {
			http.Error(w, "fake API server: no such path "+path, http.StatusNotFound)
			return
		}
		if r.URL.Query().Get("watch") != "true" {
			reply(w, map[string]any{"kind": kind + "List", "apiVersion": group, "metadata": map[string]string{"resourceVersion": "100"}, "items": []any{}})
			return
		}
		mu.Lock()
		watches[plural]++
		n := watches[plural]
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if n <= expiryPlan[plural] {
			// The version the client resumed from is gone: an ERROR event carrying a 410 Expired status, which ends the connection.
			ev := map[string]any{"type": "ERROR", "object": map[string]any{"kind": "Status", "apiVersion": "v1", "status": "Failure",
				"reason": "Expired", "code": http.StatusGone, "message": "too old resource version: " + r.URL.Query().Get("resourceVersion") + " (200)"}}
			if err := json.NewEncoder(w).Encode(ev); err != nil {
				t.Errorf("fake API server: writing an expiry: %v", err)
			}
			return
		}
		// Normal: the connection stays open and quiet until the client ends it, which is when the request's context ends.
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	dir := t.TempDir()
	kubeconfig := fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- name: fake
  cluster:
    server: %s
contexts:
- name: fake
  context:
    cluster: fake
    user: fake
current-context: fake
users:
- name: fake
  user: {}
`, srv.URL)
	if err := os.WriteFile(filepath.Join(dir, "kubeconfig"), []byte(kubeconfig), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}

// capturingT is the *testing.T a watch's Recorder is given, with its clean-ups held back and its errors kept, so that the test runs the clean-up
// itself and reads what the Recorder's own rule said at the end of a run that had resets.
type capturingT struct {
	*testing.T
	mu       sync.Mutex
	cleanups []func()
	errs     []string
}

func (c *capturingT) Cleanup(f func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cleanups = append(c.cleanups, f)
}

func (c *capturingT) Errorf(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errs = append(c.errs, fmt.Sprintf(format, args...))
}

// finish runs the held clean-ups, last first, as testing does, and returns the errors they reported.
func (c *capturingT) finish() []string {
	c.mu.Lock()
	fs := c.cleanups
	c.cleanups = nil
	c.mu.Unlock()
	for i := len(fs) - 1; i >= 0; i-- {
		fs[i]()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.errs...)
}

// watchedSteps are a dry row's stand-in steps with the watch's count read the way the live steps read it: through a liveRun's WatchResets,
// over a real Recorder. Nothing else of the liveRun is used.
type watchedSteps struct {
	*drySteps
	live *liveRun
}

func (s watchedSteps) WatchResets() int { return s.live.WatchResets() }

// A real, non-zero count of the watch's resets reaches the text to post and the record of every row, through liveRun.WatchResets: the count
// is what the fake API server's script makes it (expiryPlan), never 0 and never what any code of the package says it should be.
func TestALiveRunsRealWatchResetsReachTheRowsPostAndRecord(t *testing.T) {
	want := expiriesInPlan()
	if want == 0 {
		t.Fatal("the plan expires nothing, so no count could show that WatchResets reports one")
	}
	for _, name := range []string{"alpha-athena-handover", "alpha-helios-nooverlap", "tunnel-hour-athena"} {
		t.Run(name, func(t *testing.T) {
			row, _ := RowByName(name)
			cfg := Config{Row: row, Dry: true, Out: filepath.Join(t.TempDir(), "out"), PendingMax: time.Minute, FrontLabel: testFrontLabel}
			tags, err := NewRecorder(cfg.Out)
			if err != nil {
				t.Fatal(err)
			}
			ct := &capturingT{T: t}
			ns := "alpha-rehearsal"
			watch := stack.StartWatch(ct, fakeAPIServer(t, ns), ns, nil)
			// The count is reached by the watch's own goroutines against the fake server; wait on it, not on a window.
			deadline := time.Now().Add(2 * time.Minute)
			for watch.ResetCount() < want {
				if time.Now().After(deadline) {
					ct.finish()
					t.Fatalf("the watch counts %d resets, want %d, two minutes after it started", watch.ResetCount(), want)
				}
				time.Sleep(20 * time.Millisecond)
			}
			st := watchedSteps{drySteps: newDrySteps(t, cfg, familyOf(name)), live: &liveRun{watch: watch}}
			failures, runErr := Run(context.Background(), cfg, st, tags, t.Logf)
			errs := ct.finish()
			if runErr != nil || len(failures) != 0 {
				t.Fatalf("failures %v, err %v; the count is not judged", failures, runErr)
			}
			posts, _ := os.ReadFile(filepath.Join(cfg.Out, PostsFile))
			if line := fmt.Sprintf("- watch resets over the run: %d\n", want); !strings.Contains(string(posts), line) {
				t.Errorf("the text to post lacks %q:\n%s", line, posts)
			}
			record, _ := os.ReadFile(filepath.Join(cfg.Out, RecordFile))
			if line := fmt.Sprintf(`"watchResets": %d`, want); !strings.Contains(string(record), line) {
				t.Errorf("the record lacks %q:\n%s", line, record)
			}
			// The watch's own rule, at the end of the test: these resets were real, so a run that judged from this watch would fail.
			if len(errs) != 1 || !strings.Contains(errs[0], "not known to be complete") {
				t.Errorf("the watch's clean-up reported %q, want the one error that what was read from it is not known to be complete", errs)
			}
		})
	}
}
