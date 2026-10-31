// Command s2bload is the test client of spike S2 phase B: a synthetic multi-turn agent workload
// with x-session-id, run at a fixed concurrency against one router. It records, per request, the
// time to first token and the server-reported prompt and cached tokens, as JSON lines.
//
//	s2bload -url http://127.0.0.1:PORT/v1/chat/completions -run B1-rr -out client.jsonl [flags]
//	s2bload -plan [flags]   print the workload's structure and send nothing
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Record is one JSON line of the client's output.
type Record struct {
	Run              string  `json:"run"`
	Session          int     `json:"session"`
	Turn             int     `json:"turn"`
	Agent            int     `json:"agent"`
	StartUnix        float64 `json:"start_unix"`
	TTFTms           float64 `json:"ttft_ms"`
	LatencyMs        float64 `json:"latency_ms"`
	PromptTokens     int     `json:"prompt_tokens"`
	CachedTokens     int     `json:"cached_tokens"`
	CompletionTokens int     `json:"completion_tokens"`
	Status           int     `json:"status"`
	Attempt          int     `json:"attempt"`
	Err              string  `json:"err,omitempty"`
}

func parseWeights(s string) ([]float64, error) {
	var out []float64
	for _, f := range strings.Split(s, ",") {
		v, err := strconv.ParseFloat(strings.TrimSpace(f), 64)
		if err != nil || v <= 0 {
			return nil, fmt.Errorf("bad agent weight %q", f)
		}
		out = append(out, v)
	}
	return out, nil
}

func main() {
	fs := flag.NewFlagSet("s2bload", flag.ExitOnError)
	url := fs.String("url", "", "chat completions URL of the router")
	model := fs.String("model", "s2b-model", "served model name")
	run := fs.String("run", "", "run label; salts every system prompt (letters, digits, '-', '_', '.')")
	out := fs.String("out", "", "JSON-lines output file")
	plan := fs.Bool("plan", false, "print the workload structure and exit")
	header := fs.String("header", "x-session-id", "session header; empty sends none")
	w := Workload{}
	fs.Int64Var(&w.Seed, "seed", 1, "workload seed (the same in every arm)")
	fs.IntVar(&w.Sessions, "sessions", 96, "number of sessions")
	fs.IntVar(&w.Turns, "turns", 12, "turns per session")
	weights := fs.String("agents", "0.5,0.3,0.2", "popularity of each agent type (one shared system prompt per type)")
	fs.IntVar(&w.SysWords, "sys-words", 2000, "words in each system prompt")
	fs.IntVar(&w.TaskWords, "task-words", 250, "words in each session's task")
	fs.IntVar(&w.ToolMinWords, "tool-min", 200, "minimum words of a tool result")
	fs.IntVar(&w.ToolMaxWords, "tool-max", 800, "maximum words of a tool result")
	fs.DurationVar(&w.ThinkMin, "think-min", 500*time.Millisecond, "minimum pause between turns")
	fs.DurationVar(&w.ThinkMax, "think-max", 2500*time.Millisecond, "maximum pause between turns")
	conc := fs.Int("concurrency", 32, "sessions in flight at once")
	reply := fs.Int("reply-tokens", 48, "tokens per reply (min = max, ignore_eos)")
	jitter := fs.Duration("start-jitter", 10*time.Second, "random delay before each concurrent slot's first session")
	reqTimeout := fs.Duration("request-timeout", 300*time.Second, "client deadline per request")
	fs.Parse(os.Args[1:])
	var err error
	if w.AgentWeights, err = parseWeights(*weights); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(64)
	}
	w.Run = *run
	if *plan {
		printPlan(&w)
		return
	}
	if *url == "" || *run == "" || *out == "" || strings.ContainsAny(*run, ":] ") {
		fmt.Fprintln(os.Stderr, "s2bload: -url, -run (no ':', ']' or space) and -out are required")
		os.Exit(64)
	}
	f, err := os.Create(*out)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer f.Close()
	var mu sync.Mutex
	enc := json.NewEncoder(f)
	write := func(r Record) { mu.Lock(); enc.Encode(r); mu.Unlock() }

	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: *conc, MaxIdleConns: *conc, IdleConnTimeout: 90 * time.Second, DisableCompression: true}}
	sessions := make(chan int)
	go func() {
		for s := 0; s < w.Sessions; s++ {
			sessions <- s
		}
		close(sessions)
	}()
	var wg sync.WaitGroup
	t0 := time.Now()
	var aborted, failures int
	var amu sync.Mutex
	for slot := 0; slot < *conc; slot++ {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			if *jitter > 0 {
				time.Sleep(time.Duration(rand.New(rand.NewSource(int64(slot) + w.Seed*1000)).Int63n(int64(*jitter))))
			}
			for s := range sessions {
				ok, nfail := runSession(client, *url, *model, *header, *reply, *reqTimeout, &w, s, write)
				amu.Lock()
				failures += nfail
				if !ok {
					aborted++
				}
				amu.Unlock()
			}
		}(slot)
	}
	wg.Wait()
	fmt.Printf("RESULT run=%s sessions=%d turns=%d concurrency=%d wall_s=%.1f failed_attempts=%d aborted_sessions=%d\n",
		w.Run, w.Sessions, w.Turns, *conc, time.Since(t0).Seconds(), failures, aborted)
	if aborted > 0 {
		os.Exit(3)
	}
}

// runSession plays one session's turns in order. A failed turn is retried once after 1 s;
// a second failure aborts the session. It returns whether the session completed and the
// number of failed attempts.
func runSession(client *http.Client, url, model, header string, reply int, timeout time.Duration, w *Workload, s int, write func(Record)) (bool, int) {
	agent := w.AgentOf(s)
	msgs := []message{{Role: "system", Content: w.SystemPrompt(agent)}}
	fails := 0
	for t := 0; t < w.Turns; t++ {
		msgs = append(msgs, message{Role: "user", Content: w.UserMessage(s, t)})
		req := chatRequest{Model: model, Messages: msgs, MaxTokens: reply, MinTokens: reply, IgnoreEOS: true,
			Temperature: 0.7, Stream: true, StreamOptions: streamOptions{IncludeUsage: true}}
		var res Result
		var err error
		for attempt := 1; attempt <= 2; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			start := time.Now()
			res, err = chat(ctx, client, url, header, w.SessionID(s), req)
			cancel()
			rec := Record{Run: w.Run, Session: s, Turn: t, Agent: agent, StartUnix: float64(start.UnixNano()) / 1e9,
				TTFTms: float64(res.TTFT.Microseconds()) / 1000, LatencyMs: float64(res.Latency.Microseconds()) / 1000,
				PromptTokens: res.PromptTokens, CachedTokens: res.CachedTokens, CompletionTokens: res.CompletionTokens,
				Status: res.Status, Attempt: attempt}
			if err != nil {
				rec.Err = err.Error()
			}
			write(rec)
			if err == nil {
				break
			}
			fails++
			time.Sleep(time.Second)
		}
		if err != nil {
			return false, fails
		}
		msgs = append(msgs, message{Role: "assistant", Content: res.Content})
		if t < w.Turns-1 {
			time.Sleep(w.Think(s, t))
		}
	}
	return true, fails
}

func printPlan(w *Workload) {
	perAgent := make([]int, len(w.AgentWeights))
	var userWords int
	for s := 0; s < w.Sessions; s++ {
		perAgent[w.AgentOf(s)]++
		for t := 0; t < w.Turns; t++ {
			userWords += len(strings.Fields(w.UserMessage(s, t)))
		}
	}
	fmt.Printf("PLAN run=%q seed=%d sessions=%d turns=%d agent_types=%d sessions_per_type=%v sys_words=%d mean_user_words_per_turn=%.0f\n",
		w.Run, w.Seed, w.Sessions, w.Turns, len(w.AgentWeights), perAgent, w.SysWords, float64(userWords)/float64(w.Sessions*w.Turns))
}
