package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

// chatOnce sends one non-streamed chat request and returns the assistant text.
func chatOnce(url, model string, tokens int, key, session string, msgs []map[string]string) (string, error) {
	req, _ := http.NewRequest("POST", url, bytes.NewReader(chatBody(model, tokens, false, msgs)))
	req.Header.Set("Content-Type", "application/json")
	if key != "" && session != "" {
		req.Header.Set(key, session)
	}
	resp, err := newClient().Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	var out struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(b, &out); err != nil || len(out.Choices) == 0 {
		return "", fmt.Errorf("bad response body: %.200s", string(b))
	}
	return out.Choices[0].Message.Content, nil
}

// whichWorker sends one request and returns the index of the worker whose success counter rose.
func whichWorker(url, model string, tokens int, key, session string, msgs []map[string]string, metrics []string) (int, string, error) {
	before, err := scrapeAll(metrics, metricSuccess)
	if err != nil {
		return -1, "", err
	}
	text, err := chatOnce(url, model, tokens, key, session, msgs)
	if err != nil {
		return -1, "", err
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		after, err := scrapeAll(metrics, metricSuccess)
		if err != nil {
			return -1, "", err
		}
		hit := -1
		for i := range after {
			if after[i] > before[i] {
				if hit != -1 {
					return -1, text, fmt.Errorf("two workers' counters rose; requests are not isolated")
				}
				hit = i
			}
		}
		if hit >= 0 {
			return hit, text, nil
		}
		if time.Now().After(deadline) {
			return -1, text, fmt.Errorf("no worker counter rose within 5 s")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

type sessionTrace struct {
	ID      string
	Workers []int
}

func runPlacement(args []string) int {
	fs := flag.NewFlagSet("placement", flag.ExitOnError)
	url := fs.String("url", "", "chat completions URL of the router")
	model := fs.String("model", "sim-model", "model name")
	metricsCSV := fs.String("metrics", "", "comma-separated /metrics URLs of the workers, in worker order")
	sessions := fs.Int("sessions", 9, "number of sessions")
	turns := fs.Int("turns", 5, "turns per session")
	key := fs.String("key", "x-session-id", "session header name; empty sends none")
	tokens := fs.Int("tokens", 2, "tokens per reply")
	wantSticky := fs.Bool("expect-sticky", true, "PASS requires every session on a single worker")
	fs.Parse(args)
	metrics := strings.Split(*metricsCSV, ",")
	if *url == "" || len(metrics) < 2 {
		fmt.Fprintln(os.Stderr, "placement: -url and at least two -metrics URLs are required")
		return 64
	}
	traces := make([]*sessionTrace, *sessions)
	hist := make([][]map[string]string, *sessions)
	for s := range traces {
		traces[s] = &sessionTrace{ID: fmt.Sprintf("sess-%02d-%d", s, time.Now().UnixNano()%1e6)}
		// A distinct, long system prompt per session, so a prefix scorer can tell sessions apart.
		sys := fmt.Sprintf("You are assistant number %d. ", s) + strings.Repeat(fmt.Sprintf("Rule %d: be brief and precise. ", s), 40)
		hist[s] = []map[string]string{{"role": "system", "content": sys}}
	}
	// Interleave: turn t of every session before turn t+1, so the last-used worker is never the answer.
	for t := 0; t < *turns; t++ {
		for s := range traces {
			hist[s] = append(hist[s], map[string]string{"role": "user", "content": fmt.Sprintf("Turn %d question from session %d.", t, s)})
			w, text, err := whichWorker(*url, *model, *tokens, *key, traces[s].ID, hist[s], metrics)
			if err != nil {
				fmt.Fprintf(os.Stderr, "session %d turn %d: %v\n", s, t, err)
				return 2
			}
			traces[s].Workers = append(traces[s].Workers, w)
			hist[s] = append(hist[s], map[string]string{"role": "assistant", "content": text})
		}
	}
	distinct := map[int]bool{}
	allSticky := true
	perWorker := make([]int, len(metrics))
	fmt.Printf("%-18s %s\n", "session", "worker per turn")
	for _, tr := range traces {
		first := tr.Workers[0]
		sticky := true
		for _, w := range tr.Workers {
			if w != first {
				sticky = false
			}
		}
		distinct[first] = true
		perWorker[first]++
		allSticky = allSticky && sticky
		fmt.Printf("%-18s %v sticky=%v\n", tr.ID, tr.Workers, sticky)
	}
	var used []int
	for w := range distinct {
		used = append(used, w)
	}
	sort.Ints(used)
	spread := len(distinct) >= 2
	fmt.Printf("sessions per worker (by first turn): %v ; distinct workers used: %v\n", perWorker, used)
	ok := spread && (allSticky == *wantSticky)
	fmt.Printf("RESULT placement key=%q all_sessions_sticky=%v spread_over_workers=%v expect_sticky=%v pass=%v\n", *key, allSticky, spread, *wantSticky, ok)
	if ok {
		return 0
	}
	return 2
}
