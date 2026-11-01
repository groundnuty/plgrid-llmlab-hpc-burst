package agent

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The staged generation_config.json and the sampling of a verification
// generation's checks (HBEP-7, The allocation agent, Checks, "Sampling
// parameters of the checks"). The agent reads the file once per generation,
// before the first check request of the generation: it refuses a value vLLM
// would refuse, and computes agentapi.CheckSampling from the rest.

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

// checkedKey says whether agentapi.CheckGenerationConfig judges the key: the
// seven that vLLM applies as the server's sampling defaults. max_new_tokens,
// which each check sets itself, is recorded and not judged.
func checkedKey(k string) bool { return k != "max_new_tokens" }

// generationFile is what the agent finds in a staged generation_config.json.
type generationFile struct {
	// config holds the sampling parameters the file sets as numbers, for
	// VerifyResult.GenerationConfig; nil when there are none.
	config map[string]string
	// doSample is the file's do_sample when it is a JSON boolean, else nil.
	doSample *bool
	// refused is the first of the seven sampling keys whose value vLLM refuses
	// (agentapi.CheckGenerationConfig over the file's own JSON text of each),
	// else nil, with its Value cut to agentapi.MaxGenerationConfigValueBytes as
	// it is reported. It is judged on the raw text, before config drops a value
	// that is not a number: "warm" is refused, not read as absent.
	refused *agentapi.GenerationConfigError
	// cause is the operating system's reason when the file could not be read
	// (permission denied, is a directory), for agent.log only: it is not
	// reported, and it carries no path.
	cause string
}

// readGenerationConfig reads the staged checkpoint's generation_config.json
// once, whole, within agentapi.MaxGenerationConfigFileBytes.
//
// A file that is present and cannot be judged is refused as a whole, never
// read as absent (an empty Param; agentapi.GenerationConfigError): one larger
// than the limit, one that cannot be read, one that is not valid JSON (Python's
// json, which vLLM uses, accepts NaN, Infinity and -Infinity, which Go's
// refuses), and one that is valid JSON but not an object (a list, a string,
// null). vLLM would load such a file, and could refuse what the agent did not
// see; reading it as "no values" would send the checks greedy while client
// traffic fails (PR #169 review, Astra B1 and Opus N3). Only a missing file, or
// no checkpoint directory, is absent.
//
// config holds the sampling parameters the file sets of those vLLM applies
// (samplingParams), each as its JSON number text, and nothing else, so no other
// metadata the file holds (a token, a local path) enters the immutable
// verification record (PR #138 review, Astra B2, Opus 2; HBEP-7, The
// verification record). A value is kept only when it is a JSON number, an
// integer for top_k and max_new_tokens, of at most
// agentapi.MaxGenerationConfigValueBytes: a null, which vLLM skips, a value of
// another type, and a longer number, which a cut would change, are left out of
// it (a refused value is reported through refused). config is nil when the
// file is absent, refused as a whole, or sets none of them.
//
// doSample is the file's do_sample when it is a JSON boolean, else nil: a
// string, a number, a null or an absent key all leave it nil, which does not
// force greedy decoding (agentapi.CheckSampling). A file that sets only
// do_sample gives a nil config and a non-nil doSample.
func readGenerationConfig(dir string) generationFile {
	var out generationFile
	if dir == "" {
		return out
	}
	whole := func(value, cause string) generationFile {
		out.refused = &agentapi.GenerationConfigError{Value: value, Want: agentapi.FileWant}
		out.cause = cause
		return out
	}
	f, err := os.Open(filepath.Join(dir, generationConfigName))
	if errors.Is(err, fs.ErrNotExist) {
		return out
	}
	if err != nil {
		return whole(agentapi.FileUnreadable, osReason(err))
	}
	defer f.Close()
	// One byte past the limit tells a file of exactly the limit from a larger one.
	b, err := io.ReadAll(io.LimitReader(f, agentapi.MaxGenerationConfigFileBytes+1))
	if err != nil {
		return whole(agentapi.FileUnreadable, osReason(err))
	}
	if len(b) > agentapi.MaxGenerationConfigFileBytes {
		return whole(agentapi.FileTooLarge, "")
	}
	if !json.Valid(b) {
		return whole(agentapi.FileNotJSON, "")
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(b, &obj) != nil || obj == nil { // a null unmarshals into a nil map without error
		return whole(agentapi.FileNotObject, "")
	}
	cfg := map[string]string{}
	text := map[string]string{} // the JSON text of each key vLLM judges
	for k, integer := range samplingParams {
		raw, ok := obj[k]
		if !ok {
			continue
		}
		if v, ok := samplingValue(raw, integer); ok {
			cfg[k] = v
		}
		if checkedKey(k) {
			text[k] = strings.TrimSpace(string(raw))
		}
	}
	if len(cfg) > 0 {
		out.config = cfg
	}
	out.doSample = doSampleOf(obj["do_sample"])
	if err := agentapi.CheckGenerationConfig(text); err != nil {
		// CheckGenerationConfig returns only this type.
		if ge, ok := err.(*agentapi.GenerationConfigError); ok {
			cut := *ge
			cut.Value = agentapi.TruncateUTF8(cut.Value, agentapi.MaxGenerationConfigValueBytes) // the file could hold a megabyte there
			out.refused = &cut
		}
	}
	return out
}

// osReason is the operating system's reason in err, without the path that
// os.PathError puts before it.
func osReason(err error) string {
	var pe *fs.PathError
	if errors.As(err, &pe) {
		return pe.Err.Error()
	}
	return err.Error()
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

// genSampling is the sampling of one verification generation's checks
// (agentapi.CheckSampling), computed once, from the staged
// generation_config.json, by whichever check unit of the generation runs
// first, and used by every later one: the file is read once, so the values
// the checks send and the record reports are the same ones. A new
// generation, and a new staging, make a new one. After resolve returns, file
// and sampling are read-only.
type genSampling struct {
	once     sync.Once
	resolved atomic.Bool
	file     generationFile
	// sampling is the checks' sampling; the zero Sampling when the file is
	// refused, since no check runs then.
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

// resolve reads the staged file in dir on its first call, and returns g.
// reasoning says the launch declares a reasoning parser. Safe for concurrent
// use.
func (g *genSampling) resolve(dir string, reasoning bool, log *slog.Logger, gen int64) *genSampling {
	g.once.Do(func() {
		defer g.resolved.Store(true)
		g.file = readGenerationConfig(dir)
		if g.file.refused != nil {
			log.Warn("generation_config.json is refused: verification fails, no check request is sent", refusalAttrs(gen, g.file)...)
			return
		}
		g.sampling = agentapi.CheckSampling(g.file.config, g.file.doSample, reasoning)
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
	return g
}

// refusalAttrs are the log attributes of a refused file: the key and what is
// expected there, or what is wrong with the file as a whole, and never the
// value's text. The file is arbitrary configuration, so a string value can
// hold a token, a URL with a query or a private path (PR #169 review, Astra
// B2). A value that is a JSON number may be shown, as a number. The
// controller's verification record carries the value, cut to
// agentapi.MaxGenerationConfigValueBytes, as the contract says; the log does
// not.
func refusalAttrs(gen int64, f generationFile) []any {
	r := f.refused
	attrs := []any{"verificationGeneration", gen}
	if r.Param == "" {
		// Value is one of the contract's fixed phrases for the file as a whole.
		attrs = append(attrs, "file", r.Value, "want", r.Want)
		if f.cause != "" {
			attrs = append(attrs, "cause", f.cause)
		}
		return attrs
	}
	attrs = append(attrs, "param", r.Param, "want", r.Want)
	if n, ok := jsonNumberValue(r.Value); ok {
		attrs = append(attrs, "value", n)
	}
	return attrs
}

// jsonNumberValue returns text as a number when it is a JSON number that a
// float64 holds, so that what is logged is a number and cannot carry text.
func jsonNumberValue(text string) (float64, bool) {
	if text == "" || (text[0] != '-' && (text[0] < '0' || text[0] > '9')) {
		return 0, false // a string, a boolean, a null, a list
	}
	var n float64
	if json.Unmarshal([]byte(text), &n) != nil {
		return 0, false
	}
	return n, true
}

// refused reports whether the generation's file was read and refused. The
// loop calls it to start no unit of a refused generation after verification.
func (g *genSampling) refused() bool {
	return g != nil && g.resolved.Load() && g.file.refused != nil
}
