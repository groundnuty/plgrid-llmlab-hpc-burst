package enginedouble

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"sort"
	"strings"
	"sync"
)

// Environment variables that name the files recording each invocation.
const (
	EnvEngineLog    = "HB_ENGINE_DOUBLE_LOG"
	EnvApptainerLog = "HB_FAKE_APPTAINER_LOG"
	EnvSSHLog       = "HB_FAKE_SSH_LOG"
)

// LogEvent is one line of the engine's log (HB_ENGINE_DOUBLE_LOG). A test
// decodes the lines with it. Event is "start" (Argv, Env, PID), "admitted"
// (PromptTokens, MaxTokens, Stream: a chat request has a slot and is about to wait, if
// the modes make it, and generate; written at once, so a test can tell that a
// request is in flight), "request" (Path, PromptTokens, MaxTokens, Stream,
// Outcome, Tokens, InjectedNS, Nth, Sampling, Tail: written when the request ends) or empty for the
// line {"pids":[...]} of the ignore-term mode.
type LogEvent struct {
	Event string `json:"event,omitempty"`

	// start
	Argv []string `json:"argv,omitempty"`
	// Env is the sorted K=V list; a variable whose name ends in KEY or TOKEN
	// carries the sha256 hex of its value.
	Env []string `json:"env,omitempty"`
	PID int      `json:"pid,omitempty"`

	// the ignore-term line
	PIDs []int `json:"pids,omitempty"`

	// request: MaxTokens is the resolved limit; Outcome is "completed",
	// "cancelled" (the client went away) or "refused" (the double answered
	// with an error status); Tokens is how many tokens were sent.
	Path         string `json:"path,omitempty"`
	PromptTokens int    `json:"prompt_tokens,omitempty"`
	MaxTokens    int    `json:"max_tokens,omitempty"`
	Stream       bool   `json:"stream,omitempty"`
	Outcome      string `json:"outcome,omitempty"`
	Tokens       int    `json:"tokens,omitempty"`
	// InjectedNS is the time in nanoseconds that slow-first and never-settle made
	// the request wait before its first token, as the double decided it. A test
	// that needs a latency to have grown from one request to the next reads it
	// here, and does not measure a clock that a loaded machine skews.
	InjectedNS int64 `json:"injected_ns,omitempty"`
	// Nth is how many requests of the same shape (prompt tokens and max tokens, the key of
	// slow-first and never-settle) the double had admitted before this one, counting from 0. It
	// is the request's place in its shape's sequence, and never-settle's wait is Nth times a
	// share, so a test that wants the waits in the order the requests came keys them by it and
	// does not sort the waits.
	Nth int `json:"nth,omitempty"`
	// Sampling holds the sampling parameters the request carried, each
	// absent when the request left it out; Tail is the last 120 bytes of its
	// last user message, which tells the agent's checks apart by their
	// closing words.
	Sampling *RequestSampling `json:"sampling,omitempty"`
	Tail     string           `json:"tail,omitempty"`
}

// RequestSampling is the sampling parameters of one chat request as the
// engine received them. A parameter the request did not set is nil, never
// zero: a temperature of 0 and a missing temperature differ, and so do a seed
// of 0 and no seed. A test asserts what reached the engine from these.
type RequestSampling struct {
	Temperature       *float64 `json:"temperature,omitempty"`
	TopP              *float64 `json:"top_p,omitempty"`
	TopK              *int64   `json:"top_k,omitempty"`
	MinP              *float64 `json:"min_p,omitempty"`
	RepetitionPenalty *float64 `json:"repetition_penalty,omitempty"`
	PresencePenalty   *float64 `json:"presence_penalty,omitempty"`
	FrequencyPenalty  *float64 `json:"frequency_penalty,omitempty"`
	Seed              *int64   `json:"seed,omitempty"`
}

// logFile appends one JSON line per record.
type logFile struct {
	mu   sync.Mutex
	path string
}

func (l *logFile) write(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	f, err := os.OpenFile(l.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, werr := f.Write(append(b, '\n'))
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	return werr
}

// secretName says whether a variable's value is hidden in logs.
func secretName(name string) bool {
	return strings.HasSuffix(name, "KEY") || strings.HasSuffix(name, "TOKEN")
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// redactedEnv returns env sorted, with the value of every variable for which
// hide(name) is true replaced by its sha256 hex.
func redactedEnv(env []string, hide func(name string) bool) []string {
	out := make([]string, 0, len(env))
	for _, kv := range env {
		name, val, _ := strings.Cut(kv, "=")
		if hide(name) {
			kv = name + "=" + sha256Hex(val)
		}
		out = append(out, kv)
	}
	sort.Strings(out)
	return out
}
