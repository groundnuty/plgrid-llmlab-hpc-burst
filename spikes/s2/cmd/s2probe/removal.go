package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

// runRemoval binds a session to worker X, starts a long stream on that session (so it runs on X),
// removes X from the router's discovery, proves the removal took effect (the same session, which
// the router would otherwise keep on X, now lands elsewhere, and fresh sessions avoid X), and
// finally judges whether the in-flight stream survived.
func runRemoval(args []string) int {
	fs := flag.NewFlagSet("removal", flag.ExitOnError)
	url := fs.String("url", "", "chat completions URL of the router")
	model := fs.String("model", "sim-model", "model name")
	metricsCSV := fs.String("metrics", "", "comma-separated /metrics URLs of the workers, in worker order")
	key := fs.String("key", "x-session-id", "session header name")
	tokens := fs.Int("stream-tokens", 1500, "tokens of the in-flight stream (1 token/s: keep it >= 10 min past the removal)")
	removeCmd := fs.String("remove-cmd", "", "shell command that removes worker $X_INDEX from discovery")
	settle := fs.Duration("settle", 30*time.Second, "wait after the removal command before probing")
	reprobe := fs.String("reprobe", "2m,5m,9m", "comma-separated offsets after the removal for further probes")
	outdir := fs.String("outdir", ".", "output directory")
	label := fs.String("label", "removal", "output label")
	fs.Parse(args)
	metrics := strings.Split(*metricsCSV, ",")
	if *url == "" || *removeCmd == "" || len(metrics) < 2 {
		fmt.Fprintln(os.Stderr, "removal: -url, -remove-cmd and at least two -metrics URLs are required")
		return 64
	}
	sid := fmt.Sprintf("rm-%d", time.Now().UnixNano()%1e9)
	msgs := []map[string]string{{"role": "system", "content": strings.Repeat("Removal-test system prompt. ", 40)}, {"role": "user", "content": "hello"}}

	// a. Bind the session: two requests must land on the same worker X.
	x, _, err := whichWorker(*url, *model, 2, *key, sid, msgs, metrics)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bind 1:", err)
		return 2
	}
	x2, _, err := whichWorker(*url, *model, 2, *key, sid, msgs, metrics)
	if err != nil || x2 != x {
		fmt.Fprintf(os.Stderr, "bind 2: session not sticky (first %d, second %d, err %v); the removal proof needs a sticky session\n", x, x2, err)
		return 2
	}
	fmt.Printf("bound session %s to worker %d\n", sid, x)

	// b. Long stream on the same session.
	done := make(chan streamResult, 1)
	streamStart := time.Now()
	go func() {
		done <- doStream(*url, *model, *tokens, *tokens, sid, *label+".stream", *outdir, 5*time.Minute, []string{*key + ": " + sid})
	}()
	ok := false
	for i := 0; i < 120 && !ok; i++ {
		time.Sleep(time.Second)
		run, err := scrapeAll(metrics, metricRunning)
		if err == nil && run[x] >= 1 {
			ok = true
		}
	}
	if !ok {
		fmt.Fprintf(os.Stderr, "stream did not start on worker %d within 120 s\n", x)
		return 2
	}
	time.Sleep(20 * time.Second) // let the stream run before the removal
	fmt.Printf("stream running on worker %d; removing it at %s\n", x, time.Now().UTC().Format("15:04:05"))

	// c. Remove X from discovery.
	cmd := exec.Command("sh", "-c", *removeCmd)
	cmd.Env = append(os.Environ(), fmt.Sprintf("X_INDEX=%d", x))
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	removedAt := time.Now()
	if err := cmd.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "remove-cmd failed:", err)
		return 2
	}
	time.Sleep(*settle)

	// d. Prove the removal took effect, now and later.
	offsets := []time.Duration{0}
	for _, o := range strings.Split(*reprobe, ",") {
		if d, err := time.ParseDuration(strings.TrimSpace(o)); err == nil {
			offsets = append(offsets, d)
		}
	}
	removalEffective := true
	for _, off := range offsets {
		if wait := time.Until(removedAt.Add(off)); wait > 0 {
			time.Sleep(wait)
		}
		hits := 0
		same, _, err := whichWorker(*url, *model, 2, *key, sid, msgs, metrics)
		if err != nil || same == x {
			removalEffective = false
			fmt.Printf("+%v same session -> worker %d err=%v  (still X: removal NOT effective)\n", off, same, err)
		} else {
			fmt.Printf("+%v same session -> worker %d (not X)\n", off, same)
		}
		for i := 0; i < 8; i++ {
			w, _, err := whichWorker(*url, *model, 2, *key, fmt.Sprintf("%s-fresh-%d-%d", sid, off/time.Second, i), msgs, metrics)
			if err != nil || w == x {
				hits++
			}
		}
		fmt.Printf("+%v fresh sessions on X: %d of 8\n", off, hits)
		if hits > 0 {
			removalEffective = false
		}
		run, _ := scrapeAll(metrics, metricRunning)
		fmt.Printf("+%v running per worker: %v (stream still on X: %v)\n", off, run, run != nil && run[x] >= 1)
	}

	// e. Wait for the stream and judge it.
	res := <-done
	survived := res.Complete
	fmt.Printf("stream: complete=%v tokens=%d/%d elapsed=%.0fs after_removal=%.0fs maxgap=%.0fms err=%q\n",
		res.Complete, res.Tokens, res.ExpectTokens, res.ElapsedSec, res.ElapsedSec-removedAt.Sub(streamStart).Seconds(), res.GapMaxMs, res.Error)
	fmt.Printf("RESULT removal worker=%d removal_effective=%v stream_survived=%v pass=%v\n", x, removalEffective, survived, removalEffective && survived)
	if removalEffective && survived {
		return 0
	}
	return 2
}
