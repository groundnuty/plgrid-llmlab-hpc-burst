package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Kinds of check units.
const (
	unitVerify  = "verify"
	unitQualify = "qualify"
	unitWarmUp  = "warmUp"
)

// checkUnit is one piece of check work running against the Instance's
// workers: verification, qualification, or one warm-up round. One runs at a
// time, so warm-up never overlaps the checks it follows. A cancelled unit
// stays the current one until its goroutine has returned, so nothing that
// follows it (a warm-up's Done, the next unit) starts while its requests are
// still unwinding (PR #87 review, Astra B7).
type checkUnit struct {
	kind      string
	gen       int64   // the verification generation it belongs to
	key       procKey // the processes it runs against
	round     int32
	cancel    context.CancelFunc
	cancelled bool
}

// procKey names the processes a check ran against: the Instance generation,
// and each worker's restart generation and process (PID and start time). A
// result taken against one set of processes is never shown for another (PR
// #87 review, Opus B1).
type procKey string

// processKey returns the key of the Instance's desired workers as they run
// now, and false when they do not all run a process started for the desired
// generations (runsFor).
func (in *instance) processKey(i agentapi.DesiredInstance) (procKey, bool) {
	if !in.runsFor(i) {
		return "", false
	}
	var b strings.Builder
	b.WriteString(strconv.FormatInt(in.generation, 10))
	for _, dw := range i.Workers {
		w := in.find(dw.Name)
		id := w.proc.ID()
		b.WriteString(";" + w.name + ":" + strconv.FormatInt(w.generation, 10) + ":" + strconv.Itoa(id.PID) + ":" + strconv.FormatUint(id.StartTime, 10))
	}
	return procKey(b.String()), true
}

// runChecks runs the checks the desired Instance asks for, once per
// verification generation and set of running processes, against the engine's
// local socket (HBEP-7, Instance start). The agent reports raw results; the
// controller judges.
//
//   - A section runs only while every desired worker runs a process started
//     for the desired generations (runsFor). A unit whose process stops is
//     cancelled, and its result is dropped.
//   - Each section remembers the processes it ran against. When they change,
//     as when a worker restarts under a verification generation the
//     controller sent before raising the worker's generation, the section is
//     dropped and runs again against the new processes.
//   - Results carry their verification generation; a new generation drops the
//     old results and cancels the running unit, so a late result of n-1
//     never lands in n.
//   - Warm-up runs round after round, at most agentapi.MaxWarmUpRounds,
//     until a desired state without WarmUp says the controller has decided;
//     the agent then cancels the round in flight, waits for it to return, and
//     only then reports WarmUp.Done (agentapi's documentation, Warm-up gates
//     ready).
func (a *Agent) runChecks(in *instance, i agentapi.DesiredInstance) {
	c := i.Checks
	if c == nil {
		a.finishWarmUp(in)
		return
	}
	if in.checks == nil || in.checks.VerificationGeneration != c.VerificationGeneration {
		a.cancelUnit(in)
		in.checks = &agentapi.CheckResults{VerificationGeneration: c.VerificationGeneration}
		in.sectionKeys = map[string]procKey{}
		in.sampling = nil // the new generation reads the staged file again
	}
	// Results stand only while the processes they were taken against run:
	// when the desired workers do not all run (one restarts, exits, or its
	// generation was raised), every section is dropped at once, so no report
	// shows a result of a process that is gone.
	key, runs := in.processKey(i)
	a.dropStaleSections(in, key)
	if c.WarmUp == nil {
		a.finishWarmUp(in)
	}
	if in.unit != nil {
		if !runs || in.unit.key != key {
			a.cancelUnit(in)
		}
		return
	}
	if !runs || !in.staged {
		return
	}
	res := in.checks
	switch {
	case c.Verify != nil && res.Verify == nil:
		a.startVerify(in, *c.Verify, key)
	case c.Qualify != nil && res.Qualify == nil:
		a.startQualify(in, *c.Qualify, key)
	case c.WarmUp != nil && (res.WarmUp == nil || !res.WarmUp.Done):
		if res.WarmUp == nil {
			res.WarmUp = &agentapi.WarmUpResult{}
			in.sectionKeys[unitWarmUp] = key
		}
		n := int32(len(res.WarmUp.Rounds)) + 1
		if n > agentapi.MaxWarmUpRounds {
			res.WarmUp.Done = true
			return
		}
		a.startWarmUp(in, n, *c.WarmUp, key)
	}
}

// dropStaleSections drops every completed or started section taken against
// other processes than key; with an empty key, every section.
func (a *Agent) dropStaleSections(in *instance, key procKey) {
	res := in.checks
	for kind, k := range in.sectionKeys {
		if k == key {
			continue
		}
		switch kind {
		case unitVerify:
			res.Verify = nil
		case unitQualify:
			res.Qualify = nil
		case unitWarmUp:
			res.WarmUp = nil
		}
		delete(in.sectionKeys, kind)
		a.log.Info("check results dropped: the processes they were taken against are gone", "section", kind, "verificationGeneration", res.VerificationGeneration)
	}
}

// engine0 is the Engine of the Instance's first worker, which the checks run
// against: alpha has one worker per Instance.
func (a *Agent) engine0(in *instance) Engine {
	w := in.find(agentapi.WorkerName(0))
	if w == nil {
		w = in.workers[0]
	}
	return a.o.Engines(w.socket, in.key, in.launch)
}

func (a *Agent) newUnit(in *instance, kind string, round int32, key procKey) (*checkUnit, context.Context) {
	ctx, cancel := context.WithCancel(a.ctx)
	u := &checkUnit{kind: kind, gen: in.checks.VerificationGeneration, key: key, round: round, cancel: cancel}
	in.unit = u
	return u, ctx
}

// unitDone takes a unit's completion: the unit stops being the current one,
// and its result is accepted when it was not cancelled and still belongs to
// the current verification generation.
func (a *Agent) unitDone(in *instance, u *checkUnit) bool {
	if a.inst != in || in.unit != u {
		return false
	}
	in.unit = nil
	u.cancel()
	return !u.cancelled && in.checks != nil && in.checks.VerificationGeneration == u.gen
}

func (a *Agent) startVerify(in *instance, req agentapi.VerifyRequest, key procKey) {
	u, ctx := a.newUnit(in, unitVerify, 0, key)
	eng := a.engine0(in)
	digest := ""
	if in.imageVerified {
		// The image the agent ran, checked at its staging against the
		// integrity record of the pinned pull (Stager.Image). An image that
		// was not checked gets no digest, which the controller fails.
		digest = in.launch.ImageDigest
	}
	files := req.Files
	reuse := len(files) > 0 && in.fileResultsFresh && in.filesKey == filesKey(files)
	if reuse {
		in.fileResultsFresh = false // the staged rehash serves one verification
	}
	cached := append([]agentapi.FileResult(nil), in.fileResults...)
	launch := in.launch
	ckptDir := in.ckptDir
	gs := in.genSampling()
	reasoning := DeclaresReasoningParser(launch.Command)
	gen := u.gen
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		// The sampling of this generation's checks, read from the staged file
		// before the first request goes out: verification's own fingerprint
		// request carries it, and the record reports the same file's values.
		genConfig, doSample, smp := gs.resolve(ckptDir, reasoning, a.log, gen)
		res := eng.Verify(ctx, smp)
		res.ImageDigest = digest
		res.GenerationConfig = maps.Clone(genConfig)
		res.DoSample = copyBool(doSample)
		switch {
		case len(files) == 0:
		case reuse:
			res.Files = cached
		default:
			// After the start nothing is staged under the running engine:
			// the files are only rehashed, and a missing one is absent.
			ck, err := a.o.Stager.Checkpoint(ctx, CheckpointRequest{
				Repository: launch.Checkpoint.Repository,
				Revision:   launch.Checkpoint.Revision,
				Files:      files,
				RehashOnly: true,
				LoadFormat: LoadFormat(launch.Command),
			})
			if err != nil {
				res.Error = agentapi.TruncateUTF8("rehash: "+err.Error(), agentapi.MaxMessageBytes)
			} else {
				res.Files = ck.Files
			}
		}
		res.Done = true
		a.post(func() {
			if a.unitDone(in, u) {
				in.checks.Verify = &res
				in.sectionKeys[unitVerify] = u.key
			}
		})
	}()
}

func (a *Agent) startQualify(in *instance, req agentapi.QualifyRequest, key procKey) {
	u, ctx := a.newUnit(in, unitQualify, 0, key)
	eng := a.engine0(in)
	gs, ckptDir, reasoning, gen := in.genSampling(), in.ckptDir, DeclaresReasoningParser(in.launch.Command), u.gen
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		_, _, smp := gs.resolve(ckptDir, reasoning, a.log, gen)
		res := eng.Qualify(ctx, req, smp)
		res.Done = true
		a.post(func() {
			if a.unitDone(in, u) {
				in.checks.Qualify = &res
				in.sectionKeys[unitQualify] = u.key
			}
		})
	}()
}

func (a *Agent) startWarmUp(in *instance, n int32, req agentapi.WarmUpRequest, key procKey) {
	u, ctx := a.newUnit(in, unitWarmUp, n, key)
	eng := a.engine0(in)
	gs, ckptDir, reasoning, gen := in.genSampling(), in.ckptDir, DeclaresReasoningParser(in.launch.Command), u.gen
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		_, _, smp := gs.resolve(ckptDir, reasoning, a.log, gen)
		round := eng.WarmUpRound(ctx, n, req, smp)
		round.Round = n
		a.post(func() {
			if !a.unitDone(in, u) {
				return
			}
			w := in.checks.WarmUp
			if w == nil || w.Done || in.sectionKeys[unitWarmUp] != u.key {
				return
			}
			w.Rounds = append(w.Rounds, round)
			if n >= agentapi.MaxWarmUpRounds {
				w.Done = true
			}
		})
	}()
}

// cancelUnit cancels the running unit. The unit stays the current one until
// its goroutine returns (unitDone), and its result is then dropped.
func (a *Agent) cancelUnit(in *instance) {
	if in.unit == nil || in.unit.cancelled {
		return
	}
	in.unit.cancelled = true
	in.unit.cancel()
}

// finishWarmUp ends warm-up: the round in flight is cancelled, and the warm-up
// held, once a round has started, is Done only after that round has returned.
func (a *Agent) finishWarmUp(in *instance) {
	if in.unit != nil && in.unit.kind == unitWarmUp {
		a.cancelUnit(in)
		return // Done once the cancelled round has returned
	}
	if in.checks != nil && in.checks.WarmUp != nil {
		in.checks.WarmUp.Done = true
	}
}

// generationConfigName is the checkpoint file vLLM reads a model's generation
// defaults from (its --generation-config auto).
const generationConfigName = "generation_config.json"

// samplingParams are the generation_config.json keys vLLM v0.32.0 takes as
// the server's default sampling parameters, with whether each must be an
// integer: the available_params list of ModelConfig.get_diff_sampling_param,
// vllm/config/model.py lines 1869-1878 (HBEP-7, The verification record).
// vLLM serves max_new_tokens as max_tokens; the record keeps the file's key.
var samplingParams = map[string]bool{
	"repetition_penalty": false,
	"presence_penalty":   false,
	"frequency_penalty":  false,
	"temperature":        false,
	"top_k":              true,
	"top_p":              false,
	"min_p":              false,
	"max_new_tokens":     true,
}

// readGenerationConfig reads the staged checkpoint's generation_config.json
// once, for VerifyResult.GenerationConfig and VerifyResult.DoSample.
//
// The map holds the sampling parameters the file sets of those vLLM applies
// (samplingParams), each as its JSON number text, and nothing else, so no other
// metadata the file holds (a token, a local path) enters the immutable
// verification record (PR #138 review, Astra B2, Opus 2; HBEP-7, The
// verification record). A value is kept only when it is a JSON number, an
// integer for top_k and max_new_tokens, of at most
// agentapi.MaxGenerationConfigValueBytes: a null, which vLLM skips, a value of
// another type, and a longer number, which a cut would change, are left out.
// The map is nil when the file is absent, unreadable, not a JSON object or sets
// none of them: the record then holds none.
//
// do_sample is the file's do_sample when it is a JSON boolean, else nil: a
// string, a number, a null or an absent key all leave it nil, which does not
// force greedy decoding (agentapi.CheckSampling). A file that sets only
// do_sample gives a nil map and a non-nil do_sample.
func readGenerationConfig(dir string) (map[string]string, *bool) {
	if dir == "" {
		return nil, nil
	}
	f, err := os.Open(filepath.Join(dir, generationConfigName))
	if err != nil {
		return nil, nil
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 1<<20))
	if err != nil {
		return nil, nil
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(b, &obj) != nil {
		return nil, nil
	}
	out := map[string]string{}
	for k, integer := range samplingParams {
		raw, ok := obj[k]
		if !ok {
			continue
		}
		if v, ok := samplingValue(raw, integer); ok {
			out[k] = v
		}
	}
	if len(out) == 0 {
		out = nil
	}
	return out, doSampleOf(obj["do_sample"])
}

// doSampleOf returns raw as a boolean when it is a JSON true or false, else
// nil (an absent key, a null, a string such as "false", a number).
func doSampleOf(raw json.RawMessage) *bool {
	if len(raw) == 0 {
		return nil
	}
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return nil
	}
	b, ok := v.(bool)
	if !ok {
		return nil
	}
	return &b
}

func copyBool(p *bool) *bool {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// genSampling is the sampling of one verification generation's checks
// (agentapi.CheckSampling), computed once, from the staged
// generation_config.json, by whichever check unit of the generation runs
// first, and used by every later one: the file is read once, so the values
// the checks send and the record reports are the same ones. A new
// generation, and a new staging, make a new one.
type genSampling struct {
	once     sync.Once
	config   map[string]string
	doSample *bool
	sampling agentapi.Sampling
}

// genSampling returns the instance's sampling state for its current
// verification generation. Only the agent's loop calls it.
func (in *instance) genSampling() *genSampling {
	if in.sampling == nil {
		in.sampling = &genSampling{}
	}
	return in.sampling
}

// resolve reads the staged file in dir on its first call and returns the
// sampling parameters it reported (VerifyResult.GenerationConfig), its
// do_sample, and the sampling of the generation's checks. reasoning says the
// launch declares a reasoning parser. Safe for concurrent use.
func (g *genSampling) resolve(dir string, reasoning bool, log *slog.Logger, gen int64) (map[string]string, *bool, agentapi.Sampling) {
	g.once.Do(func() {
		g.config, g.doSample = readGenerationConfig(dir)
		g.sampling = agentapi.CheckSampling(g.config, g.doSample, reasoning)
		sm := g.sampling
		attrs := []any{"verificationGeneration", gen, "reasoningParser", reasoning, "greedy", sm.Greedy, "temperature", sm.Temperature, "topP", sm.TopP,
			"repetitionPenalty", sm.RepetitionPenalty, "presencePenalty", sm.PresencePenalty, "frequencyPenalty", sm.FrequencyPenalty}
		if sm.TopK != nil {
			attrs = append(attrs, "topK", *sm.TopK)
		}
		if sm.MinP != nil {
			attrs = append(attrs, "minP", *sm.MinP)
		}
		log.Info("check sampling fixed for the verification generation", attrs...)
	})
	return g.config, g.doSample, g.sampling
}

// samplingValue returns raw's number text when raw is a JSON number (not a
// string that holds one), an integer when integer is set, of at most
// MaxGenerationConfigValueBytes and within float64's or int64's range.
func samplingValue(raw json.RawMessage, integer bool) (string, bool) {
	var v any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if dec.Decode(&v) != nil {
		return "", false
	}
	n, ok := v.(json.Number)
	if !ok || len(n) > agentapi.MaxGenerationConfigValueBytes {
		return "", false
	}
	var err error
	if integer {
		_, err = strconv.ParseInt(string(n), 10, 64)
	} else {
		_, err = strconv.ParseFloat(string(n), 64)
	}
	return string(n), err == nil
}
