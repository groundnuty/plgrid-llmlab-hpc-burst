package main

// The generator's own row: two workloads with different shapes, run against two engine doubles (internal/agent/enginedouble) behind a
// test router, through run(), the command's entry point, with the config and the key in files and the records in a file.
//
// Every expected number below is a LITERAL typed by hand, worked out from the double's documented chat template (enginedouble/doc.go:
// a whitespace-word tokenizer, each message rendered as "<|role|> " + content + " <|assistant|>", so 2 template words per message, and a
// generation prompt of TemplateOverhead-2 = 7 words). None is computed from the config or by a helper that builds it. The assertions
// read the ENGINE SIDE's records (what reached the doubles) and compare the generator's own records and summary with them.
//
// Concurrency has no timing window: the router's wrapper holds every first-turn request until all eight sessions' first requests have
// arrived (a deadline of 60 s fails the row), which a generator that ran the sessions one after another can never satisfy. The think time
// is checked as a lower bound only, from the engine side's own stamps.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// outRec is a record read back from the records file, with its JSON names typed here by hand (not Record's own tags).
type outRec struct {
	Workload           string    `json:"workload"`
	Session            string    `json:"session"`
	Turn               int       `json:"turn"`
	Start              time.Time `json:"start"`
	End                time.Time `json:"end"`
	Status             int       `json:"status"`
	TTFTMs             *float64  `json:"ttftMs"`
	PromptTokens       *int      `json:"promptTokens"`
	CompletionTokens   *int      `json:"completionTokens"`
	DecodeTokensPerSec *float64  `json:"decodeTokensPerSec"`
	RequestID          string    `json:"requestId"`
	Emitter            string    `json:"emitter"`
	Provider           string    `json:"provider"`
	DeploymentID       string    `json:"deploymentId"`
	Instance           string    `json:"instance"`
	Error              string    `json:"error"`
}

func readRecords(t *testing.T, path string) []outRec {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out []outRec
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var r outRec
		dec := json.NewDecoder(strings.NewReader(line))
		if err := dec.Decode(&r); err != nil {
			t.Fatalf("records file: line %q: %v", line, err)
		}
		out = append(out, r)
	}
	return out
}

// want is one workload's expectation, by hand.
type want struct {
	sessions, turns int
	think           time.Duration
	output          int
	msgWords        [][]int // per turn: the word count of every message sent
	prompt          []int   // per turn: the double's prompt_tokens
	routeName       string  // which engine double answered, by the test's own name for it
	provider, dep   string
	instance        string
}

func TestTwoWorkloadsOnTwoRoutes(t *testing.T) {
	const key = "sk-study-0123456789-SENTINEL"
	g := newRig(t, 8) // 3 + 5 sessions: the barrier waits for eight first requests
	routes := map[string]*route{
		"long":  g.newRoute("one", "sano-a", "dep-aaa111", "ns/inst-1", key),
		"short": g.newRoute("two", "helios-b", "dep-bbb222", "ns/inst-2", key),
	}
	srv := httptest.NewServer(router(routes))
	t.Cleanup(srv.Close)

	dir := t.TempDir()
	cfgPath, keyPath, outPath := filepath.Join(dir, "study.json"), filepath.Join(dir, "key"), filepath.Join(dir, "records.jsonl")
	cfg := fmt.Sprintf(`{
	  "baseURL": %q, "model": "model", "apiKeyFile": %q,
	  "workloads": [
	    {"name": "long",  "sessions": 3, "turns": 3, "contextTokens": 400, "turnTokens": 20, "outputTokens": 12, "thinkTime": "150ms",
	     "headers": {"x-test-workload": "long"}},
	    {"name": "short", "sessions": 5, "turns": 2, "contextTokens": 30,  "turnTokens": 10, "outputTokens": 5,  "thinkTime": "80ms",
	     "headers": {"x-test-workload": "short"}}
	  ]}`, srv.URL+"/v1", keyPath)
	for path, content := range map[string]string{cfgPath: cfg, keyPath: key + "\n"} {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	var stdout, stderr bytes.Buffer
	code := run(ctx, []string{"-config", cfgPath, "-out", outPath}, &stdout, &stderr)
	srv.Close() // waits for every handler to finish, so what the wrappers recorded is safe to read

	if g.missed.Load() {
		t.Fatalf("concurrency: the first requests of all 8 sessions did not arrive together within %v: the generator does not run its sessions concurrently", barrierWait)
	}
	if code != 0 || stderr.Len() != 0 {
		t.Fatalf("exit %d, stderr %q, stdout:\n%s", code, stderr.String(), stdout.String())
	}

	wants := map[string]want{
		// long: turn 1 sends user(400); turn 2 adds assistant(12) and user(20); turn 3 adds the same again.
		// prompt_tokens: turn 1 (2+400)+7 = 409; turn 2 (2+400)+(2+12)+(2+20)+7 = 445; turn 3 445+(2+12)+(2+20) = 481.
		"long": {sessions: 3, turns: 3, think: 150 * time.Millisecond, output: 12,
			msgWords: [][]int{{400}, {400, 12, 20}, {400, 12, 20, 12, 20}}, prompt: []int{409, 445, 481},
			routeName: "one", provider: "sano-a", dep: "dep-aaa111", instance: "ns/inst-1"},
		// short: turn 1 sends user(30); turn 2 adds assistant(5) and user(10).
		// prompt_tokens: turn 1 (2+30)+7 = 39; turn 2 (2+30)+(2+5)+(2+10)+7 = 58.
		"short": {sessions: 5, turns: 2, think: 80 * time.Millisecond, output: 5,
			msgWords: [][]int{{30}, {30, 5, 10}}, prompt: []int{39, 58},
			routeName: "two", provider: "helios-b", dep: "dep-bbb222", instance: "ns/inst-2"},
	}
	wantRoles := [][]string{{"user"}, {"user", "assistant", "user"}, {"user", "assistant", "user", "assistant", "user"}}

	// The engine side.
	if n := len(g.bySess); n != 8 {
		t.Fatalf("engine side: %d distinct session IDs in all, want 8 (3 + 5: no ID shared between sessions or workloads)", n)
	}
	sessionsOf := map[string]map[string]bool{}
	for id, reqs := range g.bySess {
		wl := reqs[0].workload
		if sessionsOf[wl] == nil {
			sessionsOf[wl] = map[string]bool{}
		}
		sessionsOf[wl][id] = true
		w, ok := wants[wl]
		if !ok {
			t.Fatalf("engine side: session %s sent workload header %q, which no workload has", id, wl)
		}
		if len(reqs) != w.turns {
			t.Errorf("engine side: %s session %s sent %d requests, want %d turns", wl, id, len(reqs), w.turns)
			continue
		}
		for k, s := range reqs {
			where := fmt.Sprintf("%s session %s turn %d", wl, id, k+1)
			if s.workload != wl || s.route != w.routeName {
				t.Errorf("engine side: %s: workload header %q reached route %q, want %q on %q", where, s.workload, s.route, wl, w.routeName)
			}
			if fmt.Sprint(s.words) != fmt.Sprint(w.msgWords[k]) {
				t.Errorf("engine side: %s: message word counts %v, want %v", where, s.words, w.msgWords[k])
			}
			if fmt.Sprint(s.roles) != fmt.Sprint(wantRoles[k]) {
				t.Errorf("engine side: %s: roles %v, want %v", where, s.roles, wantRoles[k])
			}
			minTok := -1 // -1: the field was absent
			if s.minTokens != nil {
				minTok = *s.minTokens
			}
			if s.maxTokens != w.output || minTok != w.output {
				t.Errorf("engine side: %s: max_tokens %d min_tokens %d (-1: absent), want both %d", where, s.maxTokens, minTok, w.output)
			}
			if !s.stream || !s.includeUsage {
				t.Errorf("engine side: %s: stream %v include_usage %v, want both true", where, s.stream, s.includeUsage)
			}
			if !s.hasUsage || s.prompt != w.prompt[k] || s.completion != w.output {
				t.Errorf("engine side: %s: the double's usage prompt %d completion %d (reported %v), want %d and %d", where, s.prompt, s.completion, s.hasUsage, w.prompt[k], w.output)
			}
			if k > 0 { // the conversation is real: the reply the engine streamed comes back as the assistant message, verbatim
				prev := reqs[k-1]
				hist := append(append([]string{}, prev.contents...), prev.reply)
				if len(s.contents) != len(hist)+1 || strings.Join(s.contents[:len(hist)], "\x00") != strings.Join(hist, "\x00") {
					t.Errorf("engine side: %s: the history is not the previous request plus the reply the engine streamed (%d messages, previous reply %q)", where, len(s.contents), prev.reply)
				}
				// Think time, as a lower bound: the reply's last write on the engine side precedes the client's end of it, so
				// this gap is at least the think time, however slow the machine.
				if gap := s.arrived.Sub(reqs[k-1].lastWrite); gap < w.think {
					t.Errorf("engine side: %s: arrived %v after the previous reply's last write, want at least the think time %v", where, gap, w.think)
				}
			}
		}
	}
	for wl, w := range wants {
		if n := len(sessionsOf[wl]); n != w.sessions {
			t.Errorf("engine side: workload %s: %d distinct session IDs, want %d", wl, n, w.sessions)
		}
	}
	events := map[int]int{}
	for _, ev := range g.events {
		events[ev.PromptTokens]++
	}
	if fmt.Sprint(events) != fmt.Sprint(map[int]int{409: 3, 445: 3, 481: 3, 39: 5, 58: 5}) {
		t.Errorf("the doubles' own request logs: prompt_tokens counts %v, want 409:3 445:3 481:3 39:5 58:5", events)
	}

	// The generator's records, against the engine side's.
	recs := readRecords(t, outPath)
	if len(recs) != 19 { // 3*3 + 5*2
		t.Fatalf("records file: %d lines, want 19 (3 sessions x 3 turns + 5 x 2)", len(recs))
	}
	perWorkload := map[string]int{}
	for _, r := range recs {
		perWorkload[r.Workload]++
		w := wants[r.Workload]
		where := fmt.Sprintf("record %s session %s turn %d", r.Workload, r.Session, r.Turn)
		if r.Turn < 1 || r.Turn > w.turns || len(g.bySess[r.Session]) != w.turns {
			t.Errorf("%s: no such turn on the engine side", where)
			continue
		}
		s := g.bySess[r.Session][r.Turn-1]
		if r.Status != 200 || r.Error != "" {
			t.Errorf("%s: status %d error %q", where, r.Status, r.Error)
		}
		if r.Provider != w.provider || r.DeploymentID != w.dep || r.Instance != w.instance || r.Emitter != "front" {
			t.Errorf("%s: route %q/%q/%q emitter %q, want %q/%q/%q front", where, r.Provider, r.DeploymentID, r.Instance, r.Emitter, w.provider, w.dep, w.instance)
		}
		if r.RequestID != s.requestID || r.RequestID == "" {
			t.Errorf("%s: request id %q, the engine side sent %q", where, r.RequestID, s.requestID)
		}
		if r.PromptTokens == nil || *r.PromptTokens != w.prompt[r.Turn-1] || *r.PromptTokens != s.prompt ||
			r.CompletionTokens == nil || *r.CompletionTokens != w.output || *r.CompletionTokens != s.completion {
			t.Errorf("%s: usage %v/%v, want %d/%d (the engine side's %d/%d)", where, r.PromptTokens, r.CompletionTokens, w.prompt[r.Turn-1], w.output, s.prompt, s.completion)
		}
		if r.TTFTMs == nil || *r.TTFTMs <= 0 || r.DecodeTokensPerSec == nil || *r.DecodeTokensPerSec <= 0 || r.End.Before(r.Start) {
			t.Errorf("%s: ttft %v speed %v start %v end %v", where, r.TTFTMs, r.DecodeTokensPerSec, r.Start, r.End)
		}
	}
	if perWorkload["long"] != 9 || perWorkload["short"] != 10 {
		t.Errorf("records file: %v lines per workload, want long 9 and short 10", perWorkload)
	}

	// The summary, against hand-typed lines (it proves nothing of the shapes by itself: the engine side does).
	for _, line := range []string{
		"workload long: sessions 3 (0 with errors), requests 9, errors 0,",
		"workload short: sessions 5 (0 with errors), requests 10, errors 0,",
		"route provider=sano-a deployment=dep-aaa111 instance=ns/inst-1: sessions 3 (0 with errors), requests 9, errors 0,",
		"route provider=helios-b deployment=dep-bbb222 instance=ns/inst-2: sessions 5 (0 with errors), requests 10, errors 0,",
	} {
		if !strings.Contains(stdout.String(), line) {
			t.Errorf("summary lacks %q; it is:\n%s", line, stdout.String())
		}
	}
	// The key, which the doubles required, never comes back out.
	for name, text := range map[string]string{"stdout": stdout.String(), "stderr": stderr.String(), "records": mustRead(t, outPath)} {
		if strings.Contains(text, "SENTINEL") {
			t.Errorf("the API key reached %s", name)
		}
	}
}

func mustRead(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
