package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The dead-worker probe behind the bound Envoy arm (the science agent's answer on #62: three checks,
// the first evidence on real GPUs of #439's worker health). It follows #510's step (c)
// (internal/rehearsal/scenario/workers_test.go): every worker's key is read from Envoy's own per-host
// counters, never assumed; then
//   - a long stream runs on every worker, and the victim is killed (SIGKILL to its process tree, by
//     pidfd) while they all stream: the siblings' streams must end uncut;
//   - from the kill until the victim is back, a request in the victim's key goes every interval: each
//     must be answered 200, by a sibling (Envoy's retry and its health check), never a 503;
//   - the job restarts the victim (it watches for the killed file); the probe verifies it (its
//     /health, its model list and one request straight to its socket) and only then flags it verified,
//     which the agent's readiness endpoint, polled by Envoy, reports. It holds the worker unverified for
//     -hold from the moment its engine answers AND Envoy no longer holds it ejected (the outlier detection
//     that the dead worker's connect failures set off ejects it for 30 s or more, during which it takes
//     nothing whatever the readiness says), so the readiness gate alone keeps out a worker whose engine
//     is healthy but unverified (it never answers from the engine's /health alone, #429). Until the
//     verification Envoy's rq_success for the victim must not grow, and its placement log must show no
//     probe request; after it, a request in the victim's key must reach the victim again.
// probe.json records the timings (the pause and the recovery) and each check; the exit status is 1 if
// any check fails.

type probeOpts struct {
	runDir, served, verifyDir, out, arm, placement string
	workers, victim, victimPID, streamTokens       int
	within, interval, hold                         time.Duration
}

type streamResult struct {
	Worker     string  `json:"worker"`
	Status     int     `json:"status"`
	Done       bool    `json:"done"` // [DONE] seen
	Completion int     `json:"completion_tokens"`
	Err        string  `json:"err,omitempty"`
	EndS       float64 `json:"end_s"` // seconds from the kill; negative: before it
	end        time.Time
}

type probeSample struct {
	AtS       float64 `json:"at_s"` // seconds from the kill
	Status    int     `json:"status"`
	LatencyMs float64 `json:"latency_ms"`
	OnVictim  bool    `json:"on_victim"`
	Err       string  `json:"err,omitempty"`
}

type probeRecord struct {
	Victim       string         `json:"victim"`
	Keys         map[string]int `json:"keys_tried_per_worker"`
	Streams      []streamResult `json:"streams"`
	Samples      []probeSample  `json:"samples"`
	HealthyS     *float64       `json:"victim_healthy_s"` // its socket answered /health again
	Ejected      bool           `json:"victim_ejected"`   // Envoy's outlier detection ejected it after the kill
	EjectionEndS *float64       `json:"victim_ejection_end_s"`
	HoldStartS   *float64       `json:"hold_start_s"`                    // engine healthy and not ejected: only the readiness gate is left
	HoldS        float64        `json:"hold_s"`                          // unverified for this long from the hold's start
	GateSeen     int            `json:"hold_reads_failing_active_check"` // reads in the hold where Envoy showed its active check failing
	HoldReads    int            `json:"hold_reads"`
	VerifiedS    *float64       `json:"victim_verified_s"` // flagged verified
	AdmittedS    *float64       `json:"victim_admitted_s"` // a request in its key reached it again
	Non200       int            `json:"non_200"`
	// The non-200s split as the science agent ruled on #62: CutAtKill are requests Envoy had already
	// written to the victim's pooled connection when it died (its reply names a connection termination,
	// within the first interval after the kill), which the agent's retry policy does not retry by design
	// (nothing written to a worker is retried; the residual accepted on #507). One per kill is the
	// expected shape and more is a finding; every other non-200, NonLater, fails the probe.
	CutAtKill     int      `json:"cut_at_kill"`
	NonLater      int      `json:"non_200_later"`
	PauseMs       float64  `json:"max_latency_during_outage_ms"`
	EarlyEnvoy    int      `json:"victim_rq_success_before_verification"`
	EarlyPlaced   int      `json:"victim_placement_lines_before_verification"`
	Checks        []string `json:"checks"`
	Passed        bool     `json:"passed"`
	StreamTokens  int      `json:"stream_tokens"`
	ProbeInterval string   `json:"probe_interval"`
}

func probeMain(args []string) int {
	fs := flag.NewFlagSet("probe", flag.ContinueOnError)
	var o probeOpts
	fs.StringVar(&o.runDir, "run-dir", "", "the run directory (absolute)")
	fs.IntVar(&o.workers, "workers", 0, "workers")
	fs.IntVar(&o.victim, "victim", -1, "the worker to kill")
	fs.IntVar(&o.victimPID, "victim-pid", 0, "the PID of the victim's worker process (s2crouter worker's, now apptainer's)")
	fs.StringVar(&o.served, "served", "", "served model name")
	fs.StringVar(&o.verifyDir, "verify-dir", "", "the verify directory s2crouter envoy -verify-dir polls")
	fs.StringVar(&o.out, "out", "", "the arm's directory (probe.json, killed)")
	fs.StringVar(&o.arm, "arm", "", "the arm's name: the run label of the probe's requests")
	fs.StringVar(&o.placement, "placement", "", "the victim's placement log (host path)")
	fs.IntVar(&o.streamTokens, "stream-tokens", 1500, "tokens of each long stream")
	fs.DurationVar(&o.within, "within", 15*time.Minute, "deadline for the victim's return")
	fs.DurationVar(&o.interval, "interval", 250*time.Millisecond, "pause between probe requests")
	fs.DurationVar(&o.hold, "hold", 15*time.Second, "how long the restarted worker stays unverified once its engine answers (3 of Envoy's 5 s checks)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if o.runDir == "" || o.workers < 2 || o.victim < 0 || o.victim >= o.workers || o.victimPID <= 0 || o.served == "" ||
		o.verifyDir == "" || o.out == "" || o.arm == "" || o.placement == "" {
		return fail("probe: -run-dir, -workers (2 or more), -victim, -victim-pid, -served, -verify-dir, -out, -arm and -placement are required")
	}
	rec, err := runProbe(o)
	if rec != nil {
		_ = writeJSON(filepath.Join(o.out, "probe.json"), rec)
	}
	if err != nil {
		return fail("probe: %v", err)
	}
	for _, c := range rec.Checks {
		fmt.Println(c)
	}
	if !rec.Passed {
		return 1
	}
	return 0
}

func runProbe(o probeOpts) (*probeRecord, error) {
	admin := filepath.Join(o.runDir, agent.EnvoyAdminSocket)
	router := unixClient(filepath.Join(o.runDir, agent.EnvoyListenerSocket), 0)
	victim := agentapi.WorkerName(o.victim)
	rec := &probeRecord{Victim: victim, Keys: map[string]int{}, StreamTokens: o.streamTokens, ProbeInterval: o.interval.String(), HoldS: o.hold.Seconds()}

	// Every worker's key, from Envoy's counters: the host whose rq_success grew served it.
	keys := map[string]string{}
	for n := 0; len(keys) < o.workers && n < 50*o.workers; n++ {
		key := fmt.Sprintf("%s-k%03d", o.arm, n)
		pre, err := hostStats(admin)
		if err != nil {
			return rec, err
		}
		st, _, err := chat(router, o.served, key, tag(o.arm+"-keys", n, 0), 2)
		if err != nil || st != 200 {
			return rec, fmt.Errorf("key search: %d %v", st, err)
		}
		post, err := hostStats(admin)
		if err != nil {
			return rec, err
		}
		var on []string
		for h := range post {
			if post[h]["rq_success"] > pre[h]["rq_success"] {
				on = append(on, h)
			}
		}
		if len(on) != 1 {
			return rec, fmt.Errorf("key search: Envoy's counters name %v for one request", on)
		}
		rec.Keys[on[0]]++
		if _, ok := keys[on[0]]; !ok {
			keys[on[0]] = key
		}
	}
	if len(keys) < o.workers {
		return rec, fmt.Errorf("found keys for %d of %d workers", len(keys), o.workers)
	}

	// A long stream on every worker; the kill comes once each has its first token and Envoy holds
	// them all active.
	type started struct {
		first chan struct{}
		done  chan streamResult
	}
	streams := map[string]started{}
	for w, key := range keys {
		s := started{first: make(chan struct{}), done: make(chan streamResult, 1)}
		streams[w] = s
		go func(w, key string) {
			r := streamChat(unixClient(filepath.Join(o.runDir, agent.EnvoyListenerSocket), 0), o.served, key, tag(o.arm+"-stream", 0, 0), o.streamTokens, s.first)
			r.Worker = w
			s.done <- r
		}(w, key)
	}
	firstBy := time.After(3 * time.Minute)
	for w, s := range streams {
		select {
		case <-s.first:
		case r := <-s.done:
			return rec, fmt.Errorf("the stream on %s ended before its first token: %d %s", w, r.Status, r.Err)
		case <-firstBy:
			return rec, fmt.Errorf("no first token on %s within 3 min", w)
		}
	}
	if st, err := hostStats(admin); err != nil {
		return rec, err
	} else {
		for w := range keys {
			if st[w]["rq_active"] < 1 {
				return rec, fmt.Errorf("Envoy shows no active request on %s while its stream runs", w)
			}
		}
	}

	// The kill: the victim leaves verification first (the agent drops it on the process's exit), then
	// SIGKILL to its whole process tree.
	if err := os.Remove(filepath.Join(o.verifyDir, victim+".verified")); err != nil {
		return rec, err
	}
	if err := killTree(o.victimPID); err != nil {
		return rec, err
	}
	killAt := time.Now()
	since := func() float64 { return time.Since(killAt).Seconds() }
	if err := os.WriteFile(filepath.Join(o.out, "killed"), nil, 0o644); err != nil {
		return rec, err
	}

	// The outage: one request in the victim's key every interval, until one reaches the victim again
	// after its verification.
	victimSock := filepath.Join(o.runDir, victim+".sock")
	direct := unixClient(victimSock, 10*time.Second)
	var verifiedAt time.Time
	deadline := killAt.Add(o.within)
	for n := 0; ; n++ {
		if time.Now().After(deadline) {
			rec.Checks = append(rec.Checks, fmt.Sprintf("FAIL recovery: %s not admitted within %v of the kill", victim, o.within))
			break
		}
		pre, err := hostStats(admin)
		if err != nil {
			return rec, err
		}
		t0 := time.Now()
		st, _, cerr := chat(router, o.served, keys[victim], tag(o.arm, n, 0), 4)
		lat := time.Since(t0)
		post, err := hostStats(admin)
		if err != nil {
			return rec, err
		}
		onVictim := post[victim]["rq_success"] > pre[victim]["rq_success"]
		s := probeSample{AtS: t0.Sub(killAt).Seconds(), Status: st, LatencyMs: float64(lat.Microseconds()) / 1000, OnVictim: onVictim}
		if cerr != nil {
			s.Err = cerr.Error()
		}
		rec.Samples = append(rec.Samples, s)
		if st != 200 || cerr != nil {
			rec.Non200++
			if s.AtS <= o.interval.Seconds() && strings.Contains(s.Err, "connection termination") {
				rec.CutAtKill++
			} else {
				rec.NonLater++
			}
		}
		if s.LatencyMs > rec.PauseMs && verifiedAt.IsZero() {
			rec.PauseMs = s.LatencyMs
		}
		if onVictim {
			if verifiedAt.IsZero() {
				rec.EarlyEnvoy++
			} else {
				a := since()
				rec.AdmittedS = &a
				break
			}
		}
		if verifiedAt.IsZero() {
			flags, ferr := hostFlags(admin)
			if ferr != nil {
				return rec, ferr
			}
			vf := flags[victim]
			if vf.outlier {
				rec.Ejected = true
			} else if rec.Ejected && rec.EjectionEndS == nil {
				e := since()
				rec.EjectionEndS = &e
			}
			if rec.HealthyS == nil && get(direct, "http://w/health") == nil {
				h := since()
				rec.HealthyS = &h
			}
			if rec.HealthyS != nil && rec.HoldStartS == nil && !vf.outlier {
				h := since()
				rec.HoldStartS = &h
			}
			if rec.HoldStartS != nil {
				rec.HoldReads++
				if vf.active {
					rec.GateSeen++
				}
			}
			if rec.HoldStartS != nil && since() >= *rec.HoldStartS+o.hold.Seconds() && modelListed(direct, o.served) {
				// The harness's verification: one request straight to the restarted worker's socket.
				st, _, err := chat(direct, o.served, "verify", tag(o.arm+"-verify", 0, 0), 4)
				if err == nil && st == 200 {
					if err := os.WriteFile(filepath.Join(o.verifyDir, victim+".verified"), nil, 0o644); err != nil {
						return rec, err
					}
					verifiedAt = time.Now()
					v := since()
					rec.VerifiedS = &v
				}
			}
		}
		time.Sleep(o.interval) // the probe's pace, not a readiness wait
	}

	// The streams end: the siblings' uncut, the victim's as it may.
	uncut, midStream := true, true
	for w, s := range streams {
		var r streamResult
		select {
		case r = <-s.done:
		case <-time.After(5 * time.Minute):
			r = streamResult{Worker: w, Err: "did not end within 5 min of the recovery"}
		}
		if !r.end.IsZero() {
			r.EndS = r.end.Sub(killAt).Seconds()
		}
		rec.Streams = append(rec.Streams, r)
		if w != victim && (r.Status != 200 || !r.Done || r.Completion != o.streamTokens || r.Err != "") {
			uncut = false
		}
		if w == victim && !r.end.IsZero() && r.end.Before(killAt) {
			midStream = false
		}
	}
	// The placement log: no probe request on the victim from the kill until its verification.
	early, err := placedBetween(o.placement, o.arm, killAt, verifiedAt)
	if err != nil {
		return rec, err
	}
	rec.EarlyPlaced = early

	check := func(ok bool, name, why string) {
		if ok {
			rec.Checks = append(rec.Checks, "ok "+name)
		} else {
			rec.Checks = append(rec.Checks, "FAIL "+name+": "+why)
		}
	}
	check(midStream, "the victim was killed mid-stream", "its stream ended before the kill: raise -stream-tokens")
	check(uncut, "siblings' streams uncut", fmt.Sprintf("%+v", rec.Streams))
	check(rec.NonLater == 0, "no 503 while the siblings are healthy", fmt.Sprintf("%d of %d probe requests failed after the kill's first interval",
		rec.NonLater, len(rec.Samples)))
	check(rec.CutAtKill <= 1, "at most one request cut at the kill", fmt.Sprintf("%d cut at the kill on the victim's pooled connection: more than one is a finding (#62)", rec.CutAtKill))
	check(rec.EarlyEnvoy == 0 && rec.EarlyPlaced == 0 && !verifiedAt.IsZero(), "nothing reaches the restarted worker before its verification",
		fmt.Sprintf("Envoy counted %d, its placement log %d, verified=%v", rec.EarlyEnvoy, rec.EarlyPlaced, !verifiedAt.IsZero()))
	check(rec.AdmittedS != nil, "the verified worker takes its key's requests again", "never admitted")
	check(rec.HoldReads > 0 && rec.GateSeen == rec.HoldReads, "the readiness gate held the healthy, unverified worker out",
		fmt.Sprintf("in the hold Envoy showed the victim's active check failing in %d of %d reads", rec.GateSeen, rec.HoldReads))
	rec.Passed = true
	for _, c := range rec.Checks {
		if strings.HasPrefix(c, "FAIL") {
			rec.Passed = false
		}
	}
	return rec, nil
}

// tag is the workload's placement tag, which the placement middleware reads.
func tag(run string, session, turn int) string {
	return fmt.Sprintf("[s2b:%s:%d:%d]", run, session, turn)
}

func unixClient(sock string, timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout, Transport: &http.Transport{DisableKeepAlives: true, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", sock)
	}}}
}

func chatBody(model, content string, tokens int) []byte {
	b, _ := json.Marshal(map[string]any{
		"model": model, "messages": []map[string]string{{"role": "user", "content": content + " probe"}},
		"max_tokens": tokens, "min_tokens": tokens, "ignore_eos": true, "stream": true,
		"stream_options": map[string]bool{"include_usage": true},
	})
	return b
}

// chat sends one streamed chat and reads it whole: its status and completion tokens.
func chat(c *http.Client, model, key, content string, tokens int) (int, int, error) {
	r := streamChat(c, model, key, content, tokens, nil)
	if r.Err != "" {
		return r.Status, r.Completion, errors.New(r.Err)
	}
	if !r.Done || r.Completion != tokens {
		return r.Status, r.Completion, fmt.Errorf("incomplete: done=%v, %d of %d tokens", r.Done, r.Completion, tokens)
	}
	return r.Status, r.Completion, nil
}

// streamChat sends a streamed chat with x-session-id key; first, when set, is closed at the first
// data line.
func streamChat(c *http.Client, model, key, content string, tokens int, first chan struct{}) streamResult {
	var r streamResult
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://instance/v1/chat/completions", bytes.NewReader(chatBody(model, content, tokens)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-session-id", key)
	resp, err := c.Do(req)
	if err != nil {
		r.Err, r.end = err.Error(), time.Now()
		return r
	}
	defer resp.Body.Close()
	r.Status = resp.StatusCode
	if r.Status != http.StatusOK {
		// Envoy's own reply names why it answered so (its reset reason, for one), so keep its start.
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		r.Err, r.end = fmt.Sprintf("http %d: %q", r.Status, b), time.Now()
		return r
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		line, ok := strings.CutPrefix(sc.Text(), "data: ")
		if !ok {
			continue
		}
		if first != nil {
			close(first)
			first = nil
		}
		if line == "[DONE]" {
			r.Done = true
			continue
		}
		var ev struct {
			Usage *struct {
				Completion int `json:"completion_tokens"`
			} `json:"usage"`
		}
		if json.Unmarshal([]byte(line), &ev) == nil && ev.Usage != nil {
			r.Completion = ev.Usage.Completion
		}
	}
	if err := sc.Err(); err != nil {
		r.Err = err.Error()
	}
	r.end = time.Now()
	return r
}

func get(c *http.Client, url string) error {
	resp, err := c.Get(url)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("%s: %d", url, resp.StatusCode)
	}
	return nil
}

func modelListed(c *http.Client, served string) bool {
	resp, err := c.Get("http://w/v1/models")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	var m struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if resp.StatusCode != 200 || json.NewDecoder(resp.Body).Decode(&m) != nil {
		return false
	}
	for _, d := range m.Data {
		if d.ID == served {
			return true
		}
	}
	return false
}

// hostFlag is one worker's health in Envoy's /clusters?format=json (the agent's parseClusters fields).
type hostFlag struct{ active, outlier, pending bool }

// hostFlags reads every worker's health flags from Envoy's admin.
func hostFlags(admin string) (map[string]hostFlag, error) {
	body, err := unixGet(admin, "http://admin/clusters?format=json")
	if err != nil {
		return nil, err
	}
	var doc struct {
		Clusters []struct {
			Name  string `json:"name"`
			Hosts []struct {
				Hostname string `json:"hostname"`
				Health   struct {
					FailedActive  bool `json:"failed_active_health_check"`
					FailedOutlier bool `json:"failed_outlier_check"`
					PendingActive bool `json:"pending_active_hc"`
				} `json:"health_status"`
			} `json:"host_statuses"`
		} `json:"cluster_statuses"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, err
	}
	out := map[string]hostFlag{}
	for _, c := range doc.Clusters {
		if c.Name == "workers" {
			for _, h := range c.Hosts {
				out[h.Hostname] = hostFlag{active: h.Health.FailedActive, outlier: h.Health.FailedOutlier, pending: h.Health.PendingActive}
			}
		}
	}
	return out, nil
}

// hostStats reads every worker's counters from Envoy's /clusters?format=json.
func hostStats(admin string) (map[string]map[string]int64, error) {
	body, err := unixGet(admin, "http://admin/clusters?format=json")
	if err != nil {
		return nil, err
	}
	var doc struct {
		Clusters []struct {
			Name  string `json:"name"`
			Hosts []struct {
				Hostname string `json:"hostname"`
				Stats    []struct {
					Name  string          `json:"name"`
					Value json.RawMessage `json:"value"`
				} `json:"stats"`
			} `json:"host_statuses"`
		} `json:"cluster_statuses"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, err
	}
	out := map[string]map[string]int64{}
	for _, c := range doc.Clusters {
		if c.Name != "workers" {
			continue
		}
		for _, h := range c.Hosts {
			m := map[string]int64{}
			for _, s := range h.Stats {
				v := strings.Trim(string(s.Value), `"`)
				n, _ := strconv.ParseInt(v, 10, 64)
				m[s.Name] = n
			}
			out[h.Hostname] = m
		}
	}
	if len(out) == 0 {
		return nil, errors.New("no workers cluster in /clusters")
	}
	return out, nil
}

// placedBetween counts the placement log's lines of run label arm that started at or after from and
// before to (to zero: no end).
func placedBetween(path, arm string, from, to time.Time) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	n := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		c := strings.Split(sc.Text(), "\t")
		if len(c) != 7 || c[3] != arm {
			continue
		}
		start, err := strconv.ParseFloat(c[0], 64)
		if err != nil {
			continue
		}
		t := time.Unix(0, int64(start*1e9))
		if !t.Before(from) && (to.IsZero() || t.Before(to)) {
			n++
		}
	}
	return n, sc.Err()
}
