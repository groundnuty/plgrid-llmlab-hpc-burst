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
// finally judges whether the in-flight stream survived. A run counts as acceptance evidence only if the
// stream lasted at least -min-stream-sec in total and -min-after-removal-sec after the removal: the cuts
// seen in the field came from 180 s, 900 s and 1,800 s deadlines, so a shorter stream proves nothing.
func runRemoval(args []string) int {
	fs := flag.NewFlagSet("removal", flag.ExitOnError)
	url := fs.String("url", "", "chat completions URL of the router")
	model := fs.String("model", "sim-model", "model name")
	metricsCSV := fs.String("metrics", "", "comma-separated /metrics URLs of the workers, in worker order")
	key := fs.String("key", "x-session-id", "session header name")
	tokens := fs.Int("stream-tokens", 2100, "tokens of the in-flight stream; at 1 token/s the default is 35 min, past the 1,800 s deadlines seen in the field")
	minStream := fs.Int("min-stream-sec", 1800, "the stream must last at least this long in total to count as a pass")
	minAfter := fs.Int("min-after-removal-sec", 1800, "the stream must keep running at least this long after the removal to count as a pass")
	removeCmd := fs.String("remove-cmd", "", "shell command that removes worker $X_INDEX from discovery")
	settle := fs.Duration("settle", 30*time.Second, "offset after the removal command of the first probe")
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
	if err := cmd.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "remove-cmd failed:", err)
		return 2
	}
	removedAt := time.Now() // the removal is complete when its command returns; every offset below counts from here

	// d. Prove the removal took effect, at the first probe (after the settle time) and later. The offsets
	// printed are measured, not planned: a probe takes the time its requests take.
	offsets := []time.Duration{*settle}
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
		at := fmt.Sprintf("+%.0fs (planned +%v)", time.Since(removedAt).Seconds(), off)
		same, _, err := whichWorker(*url, *model, 2, *key, sid, msgs, metrics)
		if err != nil || same == x {
			removalEffective = false
			fmt.Printf("%s same session -> worker %d err=%v  (still X: removal NOT effective)\n", at, same, err)
		} else {
			fmt.Printf("%s same session -> worker %d (not X)\n", at, same)
		}
		for i := 0; i < 8; i++ {
			w, _, err := whichWorker(*url, *model, 2, *key, fmt.Sprintf("%s-fresh-%d-%d", sid, off/time.Second, i), msgs, metrics)
			if err != nil || w == x {
				hits++
			}
		}
		fmt.Printf("%s fresh sessions on X: %d of 8 (probe ended at +%.0fs)\n", at, hits, time.Since(removedAt).Seconds())
		if hits > 0 {
			removalEffective = false
		}
		run, _ := scrapeAll(metrics, metricRunning)
		fmt.Printf("%s running per worker: %v (stream still on X: %v)\n", at, run, run != nil && run[x] >= 1)
	}

	// e. Wait for the stream and judge it.
	res := <-done
	afterRemoval := streamStart.Add(time.Duration(res.ElapsedSec * float64(time.Second))).Sub(removedAt)
	f := removalFacts{RemovalEffective: removalEffective, StreamComplete: res.Complete,
		StreamElapsed: time.Duration(res.ElapsedSec * float64(time.Second)), AfterRemoval: afterRemoval,
		MinStream: time.Duration(*minStream) * time.Second, MinAfterRemoval: time.Duration(*minAfter) * time.Second}
	pass, why := f.verdict()
	fmt.Printf("stream: complete=%v tokens=%d/%d elapsed=%.0fs after_removal=%.0fs maxgap=%.0fms err=%q\n",
		res.Complete, res.Tokens, res.ExpectTokens, res.ElapsedSec, afterRemoval.Seconds(), res.GapMaxMs, res.Error)
	for _, w := range why {
		fmt.Println("not a pass:", w)
	}
	fmt.Printf("RESULT removal worker=%d removal_effective=%v stream_survived=%v min_stream=%ds min_after_removal=%ds pass=%v\n",
		x, removalEffective, res.Complete, *minStream, *minAfter, pass)
	if pass {
		return 0
	}
	return 2
}

// removalFacts are the measured facts of one removal run; verdict applies the acceptance rules to them.
type removalFacts struct {
	RemovalEffective bool          // the same session and fresh sessions avoided X at every probe
	StreamComplete   bool          // the in-flight stream ended with finish_reason, [DONE] and the full token count
	StreamElapsed    time.Duration // total duration of the stream
	AfterRemoval     time.Duration // how long the stream ran after the removal command returned
	MinStream        time.Duration
	MinAfterRemoval  time.Duration
}

// verdict is true only if the removal took effect, the stream finished, and the run was long enough to
// have caught a deadline. The reasons name every rule that failed.
func (f removalFacts) verdict() (bool, []string) {
	var why []string
	if !f.RemovalEffective {
		why = append(why, "the removal did not take effect (the session or a fresh session still reached the removed worker)")
	}
	if !f.StreamComplete {
		why = append(why, "the in-flight stream did not complete")
	}
	if f.StreamElapsed < f.MinStream {
		why = append(why, fmt.Sprintf("the stream lasted %.0f s, less than the %.0f s minimum: not an acceptance run", f.StreamElapsed.Seconds(), f.MinStream.Seconds()))
	}
	if f.AfterRemoval < f.MinAfterRemoval {
		why = append(why, fmt.Sprintf("the stream ran %.0f s after the removal, less than the %.0f s minimum: not an acceptance run", f.AfterRemoval.Seconds(), f.MinAfterRemoval.Seconds()))
	}
	return len(why) == 0, why
}
