package main

// TestRealEngine is issue #94's step 1: the real hb-agent binary against a real
// vLLM on one GPU, with this package's controller side (the agent API server
// over fake.Backend, startEndpoint) on the same node over loopback, and no
// tunnel. It runs the plan's Instances one after another, scripts the
// controller's sequence (verify; then qualify and warm up under the same
// verification generation; end warm-up once it settles; idle; stop), and
// records every report as it arrives. It judges nothing as a test failure
// beyond the run itself: the record is the result.
//
// It also reads the engine directly over its socket, with the Instance's key,
// and keeps the raw bytes (/version, /v1/models, /metrics, chat streams), so
// that what vLLM sends can be compared with what the HBEPs say it sends
// without going through the agent's own parsing.
//
// Variables: HB_REAL_ENGINE_PLAN (the plan, TestRealEnginePlan), HB_AGENT_BIN
// (the hb-agent binary under test), HB_REAL_OUT (where the record goes; it must
// outlive the job), HB_REAL_STAGING (the staging directory), HB_REAL_BUDGET
// (the whole run, default 48m) and HB_REAL_STAGE_BUDGET (verification of the
// first Instance, which includes the image pull, default 30m).
//
// The Slurm job that runs it, the record of its first run and the findings
// are in spikes/s94.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

type realRun struct {
	t        *testing.T
	ep       endpoint
	out      string
	deadline time.Time
	recorded int
	rec      *os.File
	keys     map[string]string // Instance UID to its key, from the reports
	desired  agentapi.DesiredState
	summary  map[string]any
}

func TestRealEngine(t *testing.T) {
	planPath := os.Getenv("HB_REAL_ENGINE_PLAN")
	if planPath == "" {
		t.Skip("HB_REAL_ENGINE_PLAN is not set: the real-engine run needs a GPU node (issue #94)")
	}
	bin, out, staging := os.Getenv("HB_AGENT_BIN"), os.Getenv("HB_REAL_OUT"), os.Getenv("HB_REAL_STAGING")
	if bin == "" || out == "" || !filepath.IsAbs(staging) {
		t.Fatal("HB_AGENT_BIN, HB_REAL_OUT and an absolute HB_REAL_STAGING are required")
	}
	budget, stageBudget := envDuration(t, "HB_REAL_BUDGET", 48*time.Minute), envDuration(t, "HB_REAL_STAGE_BUDGET", 30*time.Minute)
	var plan realPlan
	b, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &plan); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	root, err := os.MkdirTemp("", "hbr")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	ep := startEndpoint(t, root)

	allocDir := filepath.Join(out, "alloc", eTag)
	for _, d := range []string{allocDir, filepath.Join(out, "dial-home")} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	cfg := agentapi.AgentConfig{
		ProtocolVersion: agentapi.ProtocolVersion, Tag: eTag, Mode: agentapi.DialHomeDirect,
		Endpoint: ep.addr, CertSPKISHA256: ep.pin,
		// No frpc runs on main yet; agent.json requires a well-formed digest.
		FrpcDigest: deploymentIDFor("frpc"),
		StagingDir: staging, DialHomeDir: filepath.Join(out, "dial-home"),
	}
	cb, err := cfg.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(allocDir, agentapi.ConfigFileName), cb, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := agentapi.WriteKeyFile(filepath.Join(allocDir, "credential"), testCredential); err != nil {
		t.Fatal(err)
	}
	rec, err := os.OpenFile(filepath.Join(out, "reports.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Close()
	r := &realRun{t: t, ep: ep, out: out, deadline: start.Add(budget), rec: rec, keys: map[string]string{}, summary: map[string]any{}}
	defer r.writeSummary()

	// The agent: the binary under test, with the job's environment. The job
	// ID is the endpoint's test Allocation's, so no real one is recorded.
	logf, err := os.Create(filepath.Join(out, "agent.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer logf.Close()
	cmd := exec.Command(bin, allocDir)
	cmd.Env = append(withoutVars(os.Environ(), "SLURM_JOB_ID", "SLURM_RESTART_COUNT"), "SLURM_JOB_ID="+eJobID, "SLURM_RESTART_COUNT=0")
	cmd.Stdout, cmd.Stderr = logf, logf
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	defer func() {
		select {
		case <-exited:
		default:
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
	}()
	r.logf("agent started")

	for i, inst := range plan.Instances {
		if i > 0 && time.Until(r.deadline) < 15*time.Minute {
			r.logf("skipping %s: %s left", inst.Label, time.Until(r.deadline).Round(time.Second))
			r.summary[inst.Label] = map[string]any{"skipped": "budget"}
			continue
		}
		verifyBy := r.deadline
		if i == 0 {
			verifyBy = start.Add(stageBudget)
		}
		r.runInstance(plan, i, inst, verifyBy)
	}

	r.set(agentapi.DesiredState{Action: agentapi.ActionStop, Settings: settingsOf(plan)})
	select {
	case err := <-exited:
		r.summary["agentExit"] = fmt.Sprint(err)
		r.logf("agent exited after stop: %v", err)
	case <-time.After(2 * time.Minute):
		r.summary["agentExit"] = "did not exit within 2m of a stop; TERM sent"
		_ = syscall.Kill(cmd.Process.Pid, syscall.SIGTERM)
		select {
		case err := <-exited:
			r.logf("agent exited after TERM: %v", err)
		case <-time.After(30 * time.Second):
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
	}
	r.record()
	if regs := ep.backend.Registrations(eUID); len(regs) > 0 {
		reg := regs[0].Request
		reg.Node = "<node>"
		r.summary["registration"] = reg
	}
}

func settingsOf(p realPlan) agentapi.AgentSettings {
	return agentapi.AgentSettings{ProbeInterval: p.ProbeInterval, StopGrace: p.StopGrace, ImageFormat: agentapi.ImageFormatSIF}
}

// runInstance takes one Instance through the controller's sequence.
func (r *realRun) runInstance(plan realPlan, idx int, inst realInstance, verifyBy time.Time) {
	uid := fmt.Sprintf("aaaaaaaa-1111-2222-3333-%012d", idx+1)
	sum := map[string]any{"uid": uid}
	r.summary[inst.Label] = sum
	verify := &agentapi.VerifyRequest{Files: inst.Files}
	di := &agentapi.DesiredInstance{
		Name: "inst-" + inst.Label, UID: uid, DeploymentID: deploymentIDFor(inst.Label),
		Launch: inst.Launch, Generation: 1,
		Workers: []agentapi.DesiredWorker{{Name: agentapi.WorkerName(0), Generation: 1, EngineSocket: agentapi.ContainerEngineSocket(agentapi.WorkerName(0))}},
		Checks:  &agentapi.Checks{VerificationGeneration: 1, Verify: verify},
	}
	run := func() {
		r.set(agentapi.DesiredState{Action: agentapi.ActionRun, Instance: di, Settings: settingsOf(plan)})
	}
	run()
	r.logf("%s: run with verification", inst.Label)

	vr, ok := r.wait(uid, "verification", verifyBy, func(in *agentapi.InstanceReport) bool {
		return in.Checks != nil && in.Checks.Verify != nil && in.Checks.Verify.Done
	}, func() {
		if !di.KeyStored && r.keys[uid] != "" {
			di.KeyStored = true // stored, as the controller does once it has the key
			run()
		}
	})
	if !ok {
		sum["stoppedAt"] = "verification"
		r.idle(plan, uid)
		return
	}
	v := vr.Checks.Verify
	sum["verify"] = v
	sum["settings"] = r.judgeSettings(inst.Launch, v)
	if w := vr.Workers; len(w) > 0 {
		sum["captures"] = r.capture(inst, w[0].EngineSocket, r.keys[uid], "before")
	}

	di.Checks = &agentapi.Checks{VerificationGeneration: 1, Verify: verify, Qualify: &inst.Qualify, WarmUp: &inst.WarmUp}
	run()
	r.logf("%s: qualify and warm up", inst.Label)
	qr, ok := r.wait(uid, "qualification", r.deadline, func(in *agentapi.InstanceReport) bool {
		return in.Checks != nil && in.Checks.Qualify != nil && in.Checks.Qualify.Done
	}, nil)
	if ok {
		sum["qualify"] = qr.Checks.Qualify
		// HBEP-7, Reasoning models: max-model-len - template overhead - the
		// typical output - 4,096, at most the longest context and at least
		// the typical one; the longest context without a reasoning parser.
		sh := inst.Qualify.Shapes
		want := int64(sh.ContextLongest)
		if agentDeclaresReasoning(inst.Launch.Command) {
			mml, _ := parseInt(v.Settings["max-model-len"])
			want = max(min(mml-int64(v.TemplateOverhead)-int64(sh.OutputTypical)-4096, int64(sh.ContextLongest)), int64(sh.ContextTypical))
		}
		sum["behaviourContextExpected"] = want
	}
	seen, settledAt := 0, 0
	wr, ok := r.wait(uid, "warm-up settling", r.deadline, func(in *agentapi.InstanceReport) bool {
		if in.Checks == nil || in.Checks.WarmUp == nil {
			return false
		}
		w := in.Checks.WarmUp
		if len(w.Rounds) > seen {
			seen = len(w.Rounds)
			r.logf("%s: warm-up round %d", inst.Label, seen)
			if settled(w.Rounds) {
				settledAt = seen
				return true
			}
		}
		return w.Done || seen >= agentapi.MaxWarmUpRounds
	}, nil)
	sum["warmUpSettledAtRound"] = settledAt
	// The controller has decided: a desired state without WarmUp.
	di.Checks = &agentapi.Checks{VerificationGeneration: 1, Verify: verify, Qualify: &inst.Qualify}
	run()
	if ok {
		wr, _ = r.wait(uid, "warm-up done", time.Now().Add(3*time.Minute), func(in *agentapi.InstanceReport) bool {
			return in.Checks != nil && in.Checks.WarmUp != nil && in.Checks.WarmUp.Done
		}, nil)
	}
	if wr != nil && wr.Checks != nil && wr.Checks.WarmUp != nil {
		w := wr.Checks.WarmUp
		sum["warmUp"] = w
		if n := len(w.Rounds); n > 0 {
			sum["throughputLastRound"] = roundThroughput(w.Rounds[n-1], agentapi.ShapeConcurrent)
			per := map[string]throughput{}
			for _, s := range []agentapi.Shape{agentapi.ShapeShort, agentapi.ShapeTypical, agentapi.ShapeLongest, agentapi.ShapeLongOutput, agentapi.ShapeToolCall, agentapi.ShapeConcurrent} {
				per[string(s)] = roundThroughput(w.Rounds[n-1], s)
			}
			sum["throughputLastRoundByShape"] = per
		}
	}
	// Health: three probe intervals more, then the engine's own counters.
	time.Sleep(3*time.Duration(plan.ProbeInterval) + 5*time.Second)
	if last, ok := r.ep.backend.LastReport(eUID); ok && last.Instance != nil && last.Instance.UID == uid && len(last.Instance.Workers) > 0 {
		sum["health"] = last.Instance.Workers[0].Health
		sum["healthReportTime"] = last.Time
		r.capture(inst, last.Instance.Workers[0].EngineSocket, r.keys[uid], "after")
	}
	r.idle(plan, uid)
}

// idle tells the agent to run nothing and waits for its worker to stop.
func (r *realRun) idle(plan realPlan, uid string) {
	r.set(agentapi.DesiredState{Action: agentapi.ActionIdle, Settings: settingsOf(plan)})
	r.wait(uid, "worker stopped", time.Now().Add(2*time.Minute), func(in *agentapi.InstanceReport) bool {
		return len(in.Workers) > 0 && (in.Workers[0].Phase == agentapi.PartStopped || in.Workers[0].Phase == agentapi.PartExited)
	}, nil)
}

func (r *realRun) set(d agentapi.DesiredState) {
	r.desired = d
	r.ep.backend.SetDesired(eUID, d)
}

// wait polls the reports until done holds for the Instance's report, the
// worker exits, staging fails, or by passes. tick runs on every poll.
func (r *realRun) wait(uid, what string, by time.Time, done func(*agentapi.InstanceReport) bool, tick func()) (*agentapi.InstanceReport, bool) {
	lastPhase := ""
	for {
		r.record()
		if tick != nil {
			tick()
		}
		last, found := r.ep.backend.LastReport(eUID)
		if found && last.Instance != nil && last.Instance.UID == uid {
			in := last.Instance
			phase := ""
			if s := in.Staging; s != nil {
				phase = fmt.Sprintf("staging %s %d/%d", s.Phase, s.BytesDone>>20, s.BytesTotal>>20)
			}
			if len(in.Workers) > 0 {
				phase += " worker " + string(in.Workers[0].Phase)
			}
			if phase != lastPhase {
				r.logf("%s: %s", what, phase)
				lastPhase = phase
			}
			if done(in) {
				return in, true
			}
			if in.Staging != nil && in.Staging.Phase == agentapi.StagingFailed {
				r.logf("%s: staging failed: %s", what, in.Staging.Message)
				return in, false
			}
			if len(in.Workers) > 0 && in.Workers[0].Phase == agentapi.PartExited && what != "worker stopped" {
				r.logf("%s: the worker exited", what)
				return in, false
			}
		}
		if time.Now().After(by) {
			r.logf("%s: out of time", what)
			if found && last.Instance != nil {
				return last.Instance, false
			}
			return nil, false
		}
		time.Sleep(time.Second)
	}
}

// record appends the reports that arrived since the last call, redacted, and
// keeps each Instance key in memory for the raw captures.
func (r *realRun) record() {
	all := r.ep.backend.Reports(eUID)
	for _, rep := range all[min(r.recorded, len(all)):] {
		if rep.Instance != nil && rep.Instance.Key != "" {
			r.keys[rep.Instance.UID] = rep.Instance.Key
		}
		b, err := json.Marshal(rep.Redacted())
		if err != nil {
			r.t.Errorf("encoding a report: %v", err)
			continue
		}
		_, _ = r.rec.Write(append(b, '\n'))
	}
	r.recorded = len(all)
}

// judgeSettings compares each declared flag the engine reports, under HBEP-7's
// rule, and lists the required values and the declared flags it did not report.
func (r *realRun) judgeSettings(l agentapi.Launch, v *agentapi.VerifyResult) map[string]any {
	declared := declaredFlags(l.Command)
	cmp := map[string]map[string]string{}
	for k, rv := range v.Settings {
		row := map[string]string{"reported": rv}
		if dv, ok := declared[k]; ok {
			row["declared"] = dv
			row["verdict"] = settingVerdict(dv, rv)
		} else {
			row["verdict"] = "undeclared, recorded"
		}
		cmp[k] = row
	}
	var notReported []string
	for k := range declared {
		if _, ok := v.Settings[k]; !ok {
			notReported = append(notReported, k)
		}
	}
	return map[string]any{
		"compared":           cmp,
		"declaredNotReport":  notReported,
		"engineVersionEqual": v.EngineVersion == realVersion,
		"imageDigestEqual":   v.ImageDigest == l.ImageDigest,
		"maxModelLenPresent": v.Settings["max-model-len"] != "",
	}
}

// capture reads the engine directly, over its socket, and keeps the raw bytes.
// It returns what the probes saw: the finish reasons of the length probe, which
// must be cut by max_tokens to show how a "length" stream ends.
func (r *realRun) capture(inst realInstance, socket, key, when string) map[string]any {
	seen := map[string]any{}
	dir := filepath.Join(r.out, "raw", inst.Label)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		r.t.Errorf("raw captures: %v", err)
		return seen
	}
	hc := &http.Client{Timeout: 5 * time.Minute, Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", socket)
	}}}
	get := func(name, path string) {
		r.keep(hc, dir, when+"-"+name, http.MethodGet, path, key, nil)
	}
	get("metrics.txt", "/metrics")
	if when != "before" {
		return seen
	}
	get("version.json", "/version")
	get("models.json", "/v1/models")
	model := ""
	for _, a := range inst.Launch.Command {
		if v, ok := strings.CutPrefix(a, "--served-model-name="); ok {
			model = v
		}
	}
	msg := []map[string]any{{"role": "user", "content": "Reply with one word: ready."}}
	tools := []map[string]any{{"type": "function", "function": map[string]any{
		"name": "get_weather", "description": "Get the current weather in a city.",
		"parameters": map[string]any{"type": "object", "properties": map[string]any{"city": map[string]any{"type": "string"}}, "required": []string{"city"}},
	}}}
	usage := map[string]any{"include_usage": true}
	for _, c := range []struct {
		name string
		body map[string]any
	}{
		{"stream-stop.sse", map[string]any{"model": model, "messages": msg, "max_tokens": inst.CaptureMaxTokens, "temperature": 0, "stream": true, "stream_options": usage}},
		// A reply far longer than 4 tokens, so max_tokens cuts it: "Reply with one
		// word" finished with "stop" after 3 tokens on Qwen2.5 in the first run.
		{"stream-length.sse", map[string]any{"model": model, "messages": []map[string]any{{"role": "user", "content": "Count from 1 to 50, one number per line."}},
			"max_tokens": 4, "temperature": 0, "stream": true, "stream_options": usage}},
		{"stream-tool.sse", map[string]any{"model": model, "messages": []map[string]any{{"role": "user", "content": "What is the weather in Krakow? Use the tool."}},
			"tools": tools, "tool_choice": "auto", "max_tokens": inst.CaptureMaxTokens, "temperature": 0, "stream": true, "stream_options": usage}},
		{"chat.json", map[string]any{"model": model, "messages": msg, "max_tokens": inst.CaptureMaxTokens, "temperature": 0}},
	} {
		b, _ := json.Marshal(c.body)
		raw := r.keep(hc, dir, when+"-"+c.name, http.MethodPost, "/v1/chat/completions", key, b)
		if c.name == "stream-length.sse" {
			fr := streamFinishes(raw)
			seen["lengthProbeFinishReasons"] = fr
			seen["lengthProbeCut"] = len(fr) == 1 && fr[0] == "length"
			if !seen["lengthProbeCut"].(bool) {
				r.logf("%s: the length probe was not cut by max_tokens (finish reasons %v)", inst.Label, fr)
			}
		}
	}
	return seen
}

// streamFinishes returns the finish reasons a chat completion stream carried,
// in order, from its complete "data:" events.
func streamFinishes(raw []byte) []string {
	var out []string
	for _, ev := range strings.Split(string(raw), "\n\n") {
		data, ok := strings.CutPrefix(strings.TrimSpace(ev), "data: ")
		if !ok || data == "[DONE]" {
			continue
		}
		var chunk struct {
			Choices []struct {
				FinishReason *string `json:"finish_reason"`
			} `json:"choices"`
		}
		if json.Unmarshal([]byte(data), &chunk) != nil {
			continue
		}
		for _, c := range chunk.Choices {
			if c.FinishReason != nil && *c.FinishReason != "" {
				out = append(out, *c.FinishReason)
			}
		}
	}
	return out
}

// keep sends one request, writes its status, content type and raw body, and
// returns the body (nil when the request failed).
func (r *realRun) keep(hc *http.Client, dir, name, method, path, key string, body []byte) []byte {
	req, err := http.NewRequest(method, "http://engine"+path, bytes.NewReader(body))
	if err != nil {
		r.t.Errorf("%s: %v", name, err)
		return nil
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	started := time.Now()
	resp, err := hc.Do(req)
	meta := map[string]any{"method": method, "path": path, "request": json.RawMessage(orNull(body))}
	var raw []byte
	if err != nil {
		meta["error"] = err.Error()
	} else {
		var rerr error
		raw, rerr = io.ReadAll(resp.Body)
		resp.Body.Close()
		meta["status"], meta["contentType"], meta["seconds"] = resp.StatusCode, resp.Header.Get("Content-Type"), time.Since(started).Seconds()
		if rerr != nil {
			meta["readError"] = rerr.Error()
		}
		if err := os.WriteFile(filepath.Join(dir, name), raw, 0o600); err != nil {
			r.t.Errorf("%s: %v", name, err)
		}
	}
	mb, _ := json.MarshalIndent(meta, "", "  ")
	_ = os.WriteFile(filepath.Join(dir, name+".meta.json"), mb, 0o600)
	return raw
}

func orNull(b []byte) []byte {
	if b == nil {
		return []byte("null")
	}
	return b
}

func (r *realRun) writeSummary() {
	b, err := json.MarshalIndent(r.summary, "", "  ")
	if err != nil {
		r.t.Errorf("summary: %v", err)
		return
	}
	if err := os.WriteFile(filepath.Join(r.out, "summary.json"), b, 0o600); err != nil {
		r.t.Errorf("summary: %v", err)
	}
}

func (r *realRun) logf(format string, a ...any) {
	fmt.Printf("%s %s\n", time.Now().UTC().Format("15:04:05"), fmt.Sprintf(format, a...))
}

func envDuration(t *testing.T, name string, def time.Duration) time.Duration {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return d
}

func withoutVars(env []string, names ...string) []string {
	out := make([]string, 0, len(env))
	for _, kv := range env {
		drop := false
		for _, n := range names {
			if strings.HasPrefix(kv, n+"=") {
				drop = true
			}
		}
		if !drop {
			out = append(out, kv)
		}
	}
	return out
}

func agentDeclaresReasoning(cmd []string) bool {
	for _, a := range cmd {
		if strings.HasPrefix(a, "--reasoning-parser=") {
			return true
		}
	}
	return false
}

func parseInt(s string) (int64, error) {
	var n int64
	_, err := fmt.Sscan(s, &n)
	return n, err
}
