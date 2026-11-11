package alpha

// Issue #285 (the science agent's ruling on PR #292, 9 Nov 2026): a liveRun's WatchResets must report the resets of its watch, and a test
// has to see a real, non-zero count reach the row's text to post. The stand-in tests could not: they compare the post with
// st.WatchResets(), the same call, and a run with no reset reports 0 whatever WatchResets returns, so a WatchResets that returned 0 passed
// every test. Four verifications prove nothing here, and this file uses none of them: comparing the post with what WatchResets returned (it
// is the code under test); a run that never has a reset (the count is 0 with or without the method); one expected count for every case (a
// WatchResets that returns that constant, or reads another run's Recorder that saw that count, passes); and resets that all end before
// Run starts (a count read at the start of Run passes).
//
// The count here is made by a real stack.Recorder following a fake API server that expires the versions a watch resumes from, on a script
// of the server (resetCases), so each expected count comes from that script, never from any code of the package. The cases script different
// totals, each through its own liveRun and its own Recorder, and some of each total's expiries are held back by a gate that a step of the
// scenario releases, so that part of the count comes only after Run has started. The server speaks plain HTTP on a port the kernel
// chooses, on 127.0.0.1.

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

// resetPlan is what the fake API server does with the watches of each kind, in the order the client opens them: the first now[kind] expire
// at once, the next late[kind] expire only when the gate is released (a step of the scenario releases it, never a clock), and every later
// watch is open and quiet until the client ends it. Every expiry is one reset of one stream.
type resetPlan struct{ now, late map[string]int }

func sumOf(m map[string]int) int {
	n := 0
	for _, k := range m {
		n += k
	}
	return n
}

// total is the count the Recorder must show once every expiry has happened, from the plan alone.
func (p resetPlan) total() int { return sumOf(p.now) + sumOf(p.late) }

// resetCases are the runs of the test: a row of each family, each with its own total, so that no constant and no other run's count can be
// the expected one. Handover: one reset before Run, two during the scenario. No-overlap: its one reset only during the scenario, so that a
// count read before the scenario shows 0. Tunnel hour: none, the count a run with no reset must post.
var resetCases = []struct {
	row  string
	plan resetPlan
}{
	{"alpha-athena-handover", resetPlan{now: map[string]int{"instances": 1}, late: map[string]int{"instances": 1, "allocations": 1}}},
	{"alpha-helios-nooverlap", resetPlan{late: map[string]int{"modeldeployments": 1}}},
	{"tunnel-hour-athena", resetPlan{}},
}

// kinds of the fake API server, by the resource the client asks for.
var fakeKinds = map[string]string{"instances": "Instance", "allocations": "Allocation", "modeldeployments": "ModelDeployment"}

// fakeAPIServer serves what a watch's client asks of the API server for the three kinds the Recorder follows: discovery, an empty LIST at a
// fixed version, and watches that expire as plan says. A watch held for the gate sends its expiry when the gate is closed, or ends when the
// client does, so that a test that fails before the gate is released leaves no handler open for the server's Close to wait on. It returns the
// directory of a kubeconfig that points at the server, the form stack.StartWatch takes.
func fakeAPIServer(t *testing.T, namespace string, plan resetPlan, gate <-chan struct{}) string {
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
		// The version the client resumed from is gone: an ERROR event carrying a 410 Expired status, which ends the connection.
		expire := func() {
			ev := map[string]any{"type": "ERROR", "object": map[string]any{"kind": "Status", "apiVersion": "v1", "status": "Failure",
				"reason": "Expired", "code": http.StatusGone, "message": "too old resource version: " + r.URL.Query().Get("resourceVersion") + " (200)"}}
			if err := json.NewEncoder(w).Encode(ev); err != nil {
				t.Errorf("fake API server: writing an expiry: %v", err)
			}
		}
		switch {
		case n <= plan.now[plural]:
			expire()
		case n <= plan.now[plural]+plan.late[plural]:
			// Open and quiet, as a real watch is, until the scenario releases the gate; then the version has expired.
			w.(http.Flusher).Flush()
			select {
			case <-gate:
				expire()
			case <-r.Context().Done():
			}
		default:
			// Normal: the connection stays open and quiet until the client ends it, which is when the request's context ends.
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		}
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

// finish runs the held clean-ups, last first, as testing does, and returns the errors they reported. A second call runs nothing.
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

// waitForResets waits until the Recorder counts n resets: on the count itself, never on a window, and fails the test two minutes after it
// began to wait. It is for the test's own goroutine.
func waitForResets(t *testing.T, rec *stack.Recorder, n int, when string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for rec.ResetCount() < n {
		if time.Now().After(deadline) {
			t.Fatalf("the watch counts %d resets, want %d, two minutes after it started to wait (%s)", rec.ResetCount(), n, when)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// watchedSteps are a dry row's stand-in steps with the watch's count read the way the live steps read it: through a liveRun's WatchResets,
// over a real Recorder. Nothing else of the liveRun is used. Every scenario step first releases the server's held expiries (the gate, one
// release however many steps) and waits on the Recorder's count reaching the plan's total, so that the resets that come after Run has
// started are all in before the scenario ends, and none was before it began.
type watchedSteps struct {
	*drySteps
	live    *liveRun
	rec     *stack.Recorder // the Recorder the liveRun holds, for the waits on its count (the field named watch is the inventory's: TestWatchInventory)
	t       *testing.T
	plan    resetPlan
	gate    chan struct{}
	release *sync.Once
}

func (s watchedSteps) WatchResets() int { return s.live.WatchResets() }

// scenarioBegins is what the scenario does before anything else: it checks that the late expiries have not happened (the gate holds them),
// releases them, and waits for the whole total.
func (s watchedSteps) scenarioBegins() {
	s.t.Helper()
	if n, want := s.rec.ResetCount(), sumOf(s.plan.now); n != want {
		s.t.Fatalf("the watch counts %d resets as the scenario begins, want only the %d that happen before it: the gate does not hold the others", n, want)
	}
	s.release.Do(func() { close(s.gate) })
	waitForResets(s.t, s.rec, s.plan.total(), "after the gate was released")
}

func (s watchedSteps) Handover(ctx context.Context, p Plan) (HandoverObs, error) {
	s.scenarioBegins()
	return s.drySteps.Handover(ctx, p)
}

func (s watchedSteps) NoOverlap(ctx context.Context, p Plan) (NoOverlapObs, error) {
	s.scenarioBegins()
	return s.drySteps.NoOverlap(ctx, p)
}

func (s watchedSteps) TunnelHour(ctx context.Context, p Plan) (TunnelHourObs, error) {
	s.scenarioBegins()
	return s.drySteps.TunnelHour(ctx, p)
}

func (s watchedSteps) NowServe(ctx context.Context, p Plan) (NowServeObs, error) {
	s.scenarioBegins()
	return s.drySteps.NowServe(ctx, p)
}

// resetsAtStart is a Steps whose WatchResets returns a count read earlier. It is the support of one mutant of hack/mutate-alpha.sh, which
// makes Run read the count at its start by wrapping its steps in it (the count would then lose every reset that comes after); no test uses it.
type resetsAtStart struct {
	Steps
	n int
}

func (s resetsAtStart) WatchResets() int { return s.n }

// A real count of the watch's resets reaches the text to post and the record of every row, through liveRun.WatchResets: each run's count is
// what the fake API server's script makes it (resetCases), different in every run, partly reached only after Run has started, and never what
// any code of the package says it should be.
func TestALiveRunsRealWatchResetsReachTheRowsPostAndRecord(t *testing.T) {
	totals := map[int]string{}
	late := false
	for _, c := range resetCases {
		if prev, dup := totals[c.plan.total()]; dup {
			t.Fatalf("%s and %s both script %d resets: a constant, or another run's count, would then pass for one of them", prev, c.row, c.plan.total())
		}
		totals[c.plan.total()] = c.row
		late = late || sumOf(c.plan.late) > 0
	}
	if !late {
		t.Fatal("no case holds an expiry for the scenario, so a count read before the scenario would pass")
	}
	if _, zero := totals[0]; !zero {
		t.Fatal("no case scripts a run with no reset, which must post 0")
	}
	for _, c := range resetCases {
		t.Run(c.row, func(t *testing.T) {
			want := c.plan.total()
			row, ok := RowByName(c.row)
			if !ok {
				t.Fatalf("no row %q", c.row)
			}
			cfg := Config{Row: row, Dry: true, Out: filepath.Join(t.TempDir(), "out"), PendingMax: time.Minute, FrontLabel: testFrontLabel}
			tags, err := NewRecorder(cfg.Out)
			if err != nil {
				t.Fatal(err)
			}
			gate := make(chan struct{})
			ct := &capturingT{T: t}
			ns := "alpha-rehearsal"
			watch := stack.StartWatch(ct, fakeAPIServer(t, ns, c.plan, gate), ns, nil)
			// The Recorder's own clean-up, registered at once and whatever the test does next: a step that fails the test (drySteps.waitFor
			// does) must still end the watch's goroutines and its connections, or the server's Close waits on them until the package times out.
			t.Cleanup(func() { ct.finish() })
			// The expiries before Run are reached by the watch's own goroutines against the fake server; wait on the count, not on a window.
			waitForResets(t, watch, sumOf(c.plan.now), "before the run")
			st := watchedSteps{drySteps: newDrySteps(t, cfg, familyOf(c.row)), live: &liveRun{watch: watch}, rec: watch, t: t, plan: c.plan, gate: gate, release: new(sync.Once)}
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
			// The watch's own rule, at the end of the test: a run with resets was judged from a watch that is not known to be complete, and a run
			// with none was not, so the same rule that fails the one is silent on the other.
			switch {
			case want == 0 && len(errs) != 0:
				t.Errorf("the watch's clean-up reported %q for a run with no reset, want nothing", errs)
			case want > 0 && (len(errs) != 1 || !strings.Contains(errs[0], "not known to be complete")):
				t.Errorf("the watch's clean-up reported %q, want the one error that what was read from it is not known to be complete", errs)
			}
		})
	}
}
