package lifecycle

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
)

// The controller judges what the agent reports (HBEP-7, Instance start;
// agentapi's doc, Who decides): the file rehash, the settings comparison, the
// shapes against the reported context limit, KV capacity, the served names and
// warm-up settling. The agent judges the checks that need the outputs.

// declared is what an Instance's identity record declares: the engine and the
// deployment-ID input flags. Verification compares against it, never against
// the ModelDeployment's current spec, which may have changed since.
type declared struct {
	engineVersion string
	imageDigest   string
	flags         map[string]string
	files         []deploymentid.File
}

type canonicalDeclared struct {
	Engine struct {
		Version     string `json:"version"`
		ImageDigest string `json:"image_digest"`
	} `json:"engine"`
	Flags map[string]json.RawMessage `json:"flags"`
}

// declaredOf reads the identity record's canonical object.
func declaredOf(rec *appsv1.ControllerRevision) (declared, error) {
	var d identityData
	if err := json.Unmarshal(rec.Data.Raw, &d); err != nil {
		return declared{}, fmt.Errorf("identity record %s: %w", rec.Name, err)
	}
	var c canonicalDeclared
	if err := json.Unmarshal(d.Canonical, &c); err != nil {
		return declared{}, fmt.Errorf("identity record %s: canonical object: %w", rec.Name, err)
	}
	out := declared{engineVersion: c.Engine.Version, imageDigest: c.Engine.ImageDigest, flags: map[string]string{}}
	for name, raw := range c.Flags {
		var one string
		if json.Unmarshal(raw, &one) == nil {
			out.flags[name] = one
			continue
		}
		var many []string
		if err := json.Unmarshal(raw, &many); err != nil || len(many) == 0 {
			return declared{}, fmt.Errorf("identity record %s: flag %s is neither a string nor a list of strings", rec.Name, name)
		}
		out.flags[name] = many[len(many)-1] // argparse keeps the last
	}
	files, err := recordFiles(rec)
	if err != nil {
		return declared{}, err
	}
	out.files = files
	return out, nil
}

// Causes of a failed verification, kept in the record.
const (
	causeRehash          = "rehash"
	causeContradiction   = "contradiction"
	causeMissingRequired = "missingRequired"
	causeShape           = "shapeDoesNotFit"
	causeError           = "error"
)

// signalRehash is the restart signal of a failed rehash, which restages at
// the whole-Instance rung (HBEP-7, Instance start); InstanceRestart.Signal
// takes CamelCase.
const signalRehash = "RehashMismatch"

// verificationData is a verification record's data (HBEP-11: the resolved
// settings, the GPU type, the fingerprint, the rehash outcome and the
// verdict), with what the controller fixed at verification.
type verificationData struct {
	Instance       string            `json:"instance"`
	DeploymentID   string            `json:"deploymentID"`
	Generation     int64             `json:"generation"`
	Restart        string            `json:"restart"`
	GPUType        string            `json:"gpuType,omitempty"`
	EngineVersion  string            `json:"engineVersion,omitempty"`
	ImageDigest    string            `json:"imageDigest,omitempty"`
	Settings       map[string]string `json:"settings,omitempty"`
	NotReported    []string          `json:"notReported,omitempty"`
	Undeclared     map[string]string `json:"undeclared,omitempty"`
	Resolved       map[string]string `json:"resolved,omitempty"` // each declared "auto": what the engine chose
	Contradictions []string          `json:"contradictions,omitempty"`
	Fingerprint    string            `json:"fingerprint,omitempty"`
	Overhead       int32             `json:"templateOverhead,omitempty"`
	// GenerationConfig is the model's generation_config.json defaults as the
	// agent read them from the staged checkpoint (VerifyResult.GenerationConfig;
	// science-agent rulings, 1 Nov 2026, issue #125).
	GenerationConfig map[string]string `json:"generationConfig,omitempty"`
	// GenerationConfigFile is what the agent found at the staged checkpoint's
	// generation_config.json: absent, present or unreadable (agentapi.
	// GenerationConfigAbsent and the like; empty when the agent did not look).
	// It is how the record tells a checkpoint with no such file from one whose
	// file holds none of the keys (HBEP-7, The verification record; issue #175).
	GenerationConfigFile string `json:"generationConfigFile,omitempty"`
	// Sampling is what this verification's checks sent: the sampling values
	// and the seeds of a reasoning model checked with its file's values, or
	// Greedy true for a model checked greedily (agentapi.CheckSampling over
	// GenerationConfig, VerifyResult.DoSample and whether the spec declares a
	// reasoning parser, the inputs the agent computed its requests from;
	// HBEP-7, One record per verification; science-agent decision, 1 Nov
	// 2026, issue #151). A pointer: a record written before it has none, and
	// reads as unknown, not as a sampled model at temperature 0.
	Sampling *agentapi.Sampling `json:"sampling,omitempty"`
	// GenerationConfigRefused is the value of the staged generation_config.json
	// that vLLM refuses, when there is one: the agent's report
	// (VerifyResult.GenerationConfigRefused, judged on the file's own JSON
	// text) or the controller's own check of VerifyResult.GenerationConfig,
	// the same agentapi.CheckGenerationConfig. Verification fails with it, for
	// every model; no check request was sent, so Sampling is absent
	// (science-agent ruling, 1 Nov 2026, N7).
	GenerationConfigRefused *agentapi.GenerationConfigError `json:"generationConfigRefused,omitempty"`
	// BehaviourContextTokens is the context, in tokens, that this
	// generation's behaviour check runs at: agentapi.BehaviourContextTokens
	// of the resolved shapes, the reported max-model-len, the measured
	// template overhead and whether a reasoning parser is declared, the
	// function the agent runs the check with, from the same inputs. 0 after a
	// worker restart,
	// which runs no qualification (science-agent ruling, 31 Oct 2026, for
	// #88: it belongs in this record, immutable per generation).
	BehaviourContextTokens int64            `json:"behaviourContextTokens,omitempty"`
	MaxModelLen            int32            `json:"maxModelLen,omitempty"`
	Shapes                 *agentapi.Shapes `json:"shapes,omitempty"`
	Rehash                 rehashOutcome    `json:"rehash"`
	Verdict                v1alpha1.Verdict `json:"verdict"`
	Cause                  string           `json:"cause,omitempty"`
	Message                string           `json:"message,omitempty"`
}

type rehashOutcome struct {
	// Skipped: a worker restart compares settings only (HBEP-7, rung 1).
	Skipped  bool     `json:"skipped,omitempty"`
	Checked  int      `json:"checked"`
	Mismatch []string `json:"mismatch,omitempty"`
	Absent   []string `json:"absent,omitempty"`
}

// judgeVerify judges one verification (HBEP-7, Instance start): every file
// present matches; the engine version, the image digest and max-model-len are
// reported, the first two equal the declared ones and max-model-len its
// declared value when declared; every other declared input that is reported
// equals its value; every rendered request, with the template overhead, fits
// the reported context limit.
func judgeVerify(d declared, v *agentapi.VerifyResult, rehash bool, sh shapes) verificationData {
	_, reasoning := d.flags["reasoning-parser"]
	refusal := refusedGenerationConfig(v)
	out := verificationData{
		EngineVersion: v.EngineVersion, ImageDigest: v.ImageDigest, Settings: v.Settings,
		Fingerprint: v.Fingerprint, Overhead: v.TemplateOverhead, GenerationConfig: v.GenerationConfig, GenerationConfigFile: v.GenerationConfigFile, GenerationConfigRefused: refusal,
		Verdict: v1alpha1.VerdictPass, Undeclared: map[string]string{},
	}
	if refusal == nil {
		// The sampling the checks used is recorded on every record, a failed
		// one and a worker restart's too: verification's own fingerprint
		// request used it either way. A refused file sends no check request,
		// so there is none to record.
		sampling := agentapi.CheckSampling(v.GenerationConfig, v.DoSample, reasoning)
		out.Sampling = &sampling
	}
	failWith := func(cause, msg string) verificationData {
		out.Verdict, out.Cause, out.Message = v1alpha1.VerdictFail, cause, msg
		return out
	}
	// A failed or mismatching rehash is judged before the refusal, on purpose.
	// The agent rehashes the checkpoint even when it refuses the file, and a
	// rehash that fails (v.Error) or mismatches means the staged copy may
	// differ from the one the agent read: it must be restaged and read again
	// before its values may stop the class. The refusal below stops it only on
	// a verification whose rehash held, or that skipped it (a worker restart).
	// The worker-restart rung and the restage are bounded by the restart limits,
	// and the next verification reads the file again (PR #169 review, Opus N2).
	if v.Error != "" {
		return failWith(causeError, "the agent could not verify: "+v.Error)
	}
	out.Rehash.Skipped = !rehash
	if rehash {
		results := map[string]agentapi.FileResult{}
		for _, f := range v.Files {
			results[f.Path] = f
		}
		for _, f := range d.files {
			r, ok := results[f.Path]
			switch {
			case !ok:
				return failWith(causeRehash, "the agent reported no rehash of "+f.Path)
			case r.Outcome == agentapi.FileMatch:
				out.Rehash.Checked++
			case r.Outcome == agentapi.FileAbsent:
				out.Rehash.Absent = append(out.Rehash.Absent, f.Path) // not staged: not a failure (HBEP-3)
			default:
				out.Rehash.Checked++
				out.Rehash.Mismatch = append(out.Rehash.Mismatch, f.Path)
			}
		}
		if len(out.Rehash.Mismatch) > 0 {
			return failWith(causeRehash, "checkpoint files differ from the identity record: "+strings.Join(out.Rehash.Mismatch, ", "))
		}
	}
	if refusal != nil {
		// vLLM applies the file's values as the server's defaults, so it would
		// refuse them on every request, for any model: the settings
		// contradiction's stop, keyed on the deployment ID, held until the
		// serving spec changes (a new checkpoint revision is a new ID).
		return failWith(causeContradiction, refusal.Error())
	}
	var missing []string
	if v.EngineVersion == "" {
		missing = append(missing, "the engine version")
	}
	if v.ImageDigest == "" {
		missing = append(missing, "the image digest")
	}
	mmlText, mmlOK := v.Settings["max-model-len"]
	if !mmlOK {
		missing = append(missing, "max-model-len")
	}
	if len(missing) > 0 {
		return failWith(causeMissingRequired, "the engine did not report "+strings.Join(missing, ", ")+", which verification requires")
	}
	if v.EngineVersion != d.engineVersion {
		out.Contradictions = append(out.Contradictions, fmt.Sprintf("engine version %q, declared %q", v.EngineVersion, d.engineVersion))
	}
	if v.ImageDigest != d.imageDigest {
		out.Contradictions = append(out.Contradictions, fmt.Sprintf("image digest %q, declared %q", v.ImageDigest, d.imageDigest))
	}
	names := make([]string, 0, len(d.flags))
	for n := range d.flags {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		got, ok := v.Settings[n]
		if !ok {
			out.NotReported = append(out.NotReported, n)
			continue
		}
		if d.flags[n] == "auto" { // the engine chooses: record what it chose, compare nothing
			if out.Resolved == nil {
				out.Resolved = map[string]string{}
			}
			out.Resolved[n] = got
			continue
		}
		if !sameSetting(n, got, d.flags[n]) {
			out.Contradictions = append(out.Contradictions, fmt.Sprintf("%s %q, declared %q", n, got, d.flags[n]))
		}
	}
	// A setting the engine reports that the spec does not declare is recorded
	// as reported, and passes: the engine version, an ID input, covers the
	// engine's defaults (science-agent ruling, 31 Oct 2026; HBEP-3, HBEP-7).
	for n, val := range v.Settings {
		if _, ok := d.flags[n]; !ok {
			out.Undeclared[n] = val
		}
	}
	if len(out.Contradictions) > 0 {
		return failWith(causeContradiction, "the engine resolves declared settings differently: "+strings.Join(out.Contradictions, "; "))
	}
	mml, err := strconv.ParseInt(strings.TrimSpace(mmlText), 10, 32)
	if err != nil || mml < 1 {
		return failWith(causeMissingRequired, fmt.Sprintf("the engine reported max-model-len %q, not a positive integer", mmlText))
	}
	out.MaxModelLen = int32(mml)
	resolved, err := sh.resolved(int32(mml), v.TemplateOverhead)
	if err != nil {
		return failWith(causeShape, err.Error())
	}
	out.Shapes = &resolved
	if rehash { // a worker restart keeps Qualified: no behaviour check runs
		out.BehaviourContextTokens = agentapi.BehaviourContextTokens(resolved, mml, v.TemplateOverhead, reasoning)
	}
	return out
}

// refusedGenerationConfig returns the value of the staged generation_config.json
// that vLLM refuses: the agent's report, which judged the file's own JSON text,
// else the controller's check of the numbers the agent recorded
// (agentapi.CheckGenerationConfig); nil when there is none.
func refusedGenerationConfig(v *agentapi.VerifyResult) *agentapi.GenerationConfigError {
	if v.GenerationConfigRefused != nil {
		return v.GenerationConfigRefused
	}
	if v.GenerationConfigFile == agentapi.GenerationConfigUnreadable {
		// The agent found an entry at the path that is not a readable regular
		// file. It pairs that with a whole-file refusal, so this is not reached
		// from it; an agent that does not must still never pass the file as
		// greedy checks (`@hpc-burst-science-agent`, 1 Nov 2026, for #175).
		return &agentapi.GenerationConfigError{Value: agentapi.FileUnreadable, Want: agentapi.FileWant}
	}
	var ge *agentapi.GenerationConfigError
	if err := agentapi.CheckGenerationConfig(v.GenerationConfig); errors.As(err, &ge) {
		return ge
	}
	return nil
}

// sameSetting compares a reported setting with its declared value under one
// rule (HBEP-7, Instance start, Settings comparison; science-agent ruling,
// 31 Oct 2026; PR #91). Identical text always matches. max-model-len is
// compared as an integer. Otherwise, if both read as booleans (true or false,
// in any case), they are compared as booleans; else, if both read as decimal
// numbers, as numbers, so "4" is "4.0" (str() of a float); else they differ,
// verbatim, so "fp8" is not "fp8_e4m3" and a JSON value the engine
// re-serializes differently is a contradiction too. A declared "auto" is not
// compared at all: judgeVerify records the resolved value.
func sameSetting(name, got, want string) bool {
	if got == want {
		return true
	}
	if name == "max-model-len" {
		g, gerr := strconv.ParseInt(got, 10, 64)
		w, werr := strconv.ParseInt(want, 10, 64)
		return gerr == nil && werr == nil && g == w
	}
	truth := func(s string) (val, ok bool) {
		switch {
		case strings.EqualFold(s, "true"):
			return true, true
		case strings.EqualFold(s, "false"):
			return false, true
		}
		return false, false
	}
	if g, gok := truth(got); gok {
		w, wok := truth(want)
		return wok && g == w
	}
	// Decimal numbers only: ParseFloat also reads "inf", "nan" and hex floats.
	decimal := func(s string) (float64, bool) {
		if s == "" || strings.Trim(s, "0123456789+-.eE") != "" {
			return 0, false
		}
		f, err := strconv.ParseFloat(s, 64)
		return f, err == nil
	}
	gi, gierr := strconv.ParseInt(got, 10, 64) // exact above 2^53, where floats round
	wi, wierr := strconv.ParseInt(want, 10, 64)
	if gierr == nil && wierr == nil {
		return gi == wi
	}
	g, gok := decimal(got)
	w, wok := decimal(want)
	return gok && wok && g == w
}

// judgeQualify judges qualification (HBEP-7, Instance start): every check the
// agent judges passed (the tool-call smoke test when tool calls are on); the
// engine serves the deployment's model name; the KV capacity is at least
// targetSessions times contextTokens.typical. Throughput gates nothing in
// alpha.
func judgeQualify(q *agentapi.QualifyResult, sh shapes, model string) (bool, string) {
	want := []agentapi.CheckName{agentapi.CheckHealth, agentapi.CheckChat, agentapi.CheckCorruption, agentapi.CheckBehaviour}
	if sh.toolCalls {
		want = append(want, agentapi.CheckToolCall)
	}
	for _, name := range want {
		i := slices.IndexFunc(q.Checks, func(c agentapi.CheckOutcome) bool { return c.Name == name })
		switch {
		case i < 0:
			return false, fmt.Sprintf("the %s check was not reported", name)
		case !q.Checks[i].Passed:
			// Decided by the reason code, never the message (agentapi's
			// CheckOutcome.Reason). ReasoningBudgetExhausted is a failed
			// check like any other, not a wrong answer: the Instance fails,
			// the failure streak bounds repeats, and the class is not
			// stopped (science-agent ruling, 31 Oct 2026).
			msg := fmt.Sprintf("the %s check failed", name)
			if r := q.Checks[i].Reason; r != "" {
				msg += " (" + r + ")"
			}
			if n := q.Checks[i].ContextTokens; n > 0 {
				msg += fmt.Sprintf(" at %d context tokens", n)
			}
			if q.Checks[i].Message != "" {
				msg += ": " + q.Checks[i].Message
			}
			return false, msg
		}
	}
	if !slices.Contains(q.ServedModelNames, model) {
		return false, fmt.Sprintf("the engine serves %v, not the model name %q", q.ServedModelNames, model)
	}
	if need := int64(sh.targetSessions) * int64(sh.typical); q.KVCapacityTokens < need {
		return false, fmt.Sprintf("KV capacity %d tokens is below targetSessions %d times contextTokens.typical %d = %d", q.KVCapacityTokens, sh.targetSessions, sh.typical, need)
	}
	// The behaviour check's context is recorded in the Qualified condition,
	// the only place an Instance's status has for it in v1alpha1.
	msg := "every check passed; no throughput reference exists in alpha, so throughput is recorded and gates nothing (HBEP-7, open question 12)"
	if i := slices.IndexFunc(q.Checks, func(c agentapi.CheckOutcome) bool { return c.Name == agentapi.CheckBehaviour }); i >= 0 && q.Checks[i].ContextTokens > 0 {
		msg += fmt.Sprintf("; the behaviour check ran at %d context tokens", q.Checks[i].ContextTokens)
	}
	return true, msg
}

// warmUpState is the controller's verdict on warm-up so far.
type warmUpState int

const (
	warmUpRunning warmUpState = iota
	warmUpSettled
	warmUpFailed
)

// judgeWarmUp decides warm-up (HBEP-7, Instance start). Only the fixed-length
// shapes are judged (agentapi.Shape.FixedLength; science-agent ruling, 1 Nov
// 2026, issue #125): warm-up is done at the first round in which no such shape
// is slower, by its mean total time, than in the round before by more than
// max(10% of that round's, 100 ms). One that has not settled after
// MaxWarmUpRounds fails, as does a round with a failed request of any shape.
// A shape whose length varies (the tool call) runs in every round to warm its
// path, but its timings are not judged; the agent reports an error on it only
// for a request that failed, never for its answer, which qualification's
// tool-call smoke test judges (owner approval on #127). The settled round's
// time per output token and time to first token are the throughput.
func judgeWarmUp(w *agentapi.WarmUpResult, targetSessions int32) (warmUpState, string, *v1alpha1.Throughput) {
	rounds := append([]agentapi.WarmUpRound(nil), w.Rounds...)
	sort.Slice(rounds, func(i, j int) bool { return rounds[i].Round < rounds[j].Round })
	for _, r := range rounds {
		for _, q := range r.Requests {
			if q.Error != "" {
				return warmUpFailed, fmt.Sprintf("warm-up round %d: a %s request failed: %s", r.Round, q.Shape, q.Error), nil
			}
		}
	}
	for i := 1; i < len(rounds); i++ {
		if agentapi.WarmUpSettled(rounds[i-1], rounds[i]) {
			return warmUpSettled, fmt.Sprintf("settled in round %d", rounds[i].Round), throughput(rounds[i], targetSessions)
		}
	}
	if w.Done || len(rounds) >= agentapi.MaxWarmUpRounds {
		return warmUpFailed, fmt.Sprintf("warm-up did not settle after %d rounds", len(rounds)), nil
	}
	return warmUpRunning, "", nil
}

func throughput(r agentapi.WarmUpRound, targetSessions int32) *v1alpha1.Throughput {
	var ttft, tpot time.Duration
	var n, m int
	for _, q := range r.Requests {
		if q.Shape != agentapi.ShapeConcurrent {
			continue
		}
		ttft += time.Duration(q.TimeToFirstToken)
		n++
		if q.OutputTokens > 1 {
			tpot += (time.Duration(q.Total) - time.Duration(q.TimeToFirstToken)) / time.Duration(q.OutputTokens-1)
			m++
		}
	}
	if n == 0 {
		return nil
	}
	out := &v1alpha1.Throughput{TimeToFirstToken: v1alpha1.Duration((ttft / time.Duration(n)).String()), TargetSessions: targetSessions}
	if m > 0 {
		out.TimePerOutputToken = v1alpha1.Duration((tpot / time.Duration(m)).String())
	} else {
		out.TimePerOutputToken = "0s"
	}
	return out
}
