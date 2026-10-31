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
// decodes the lines with it. Event is "start" (Argv, Env, PID), "request"
// (Path, PromptTokens, MaxTokens, Stream, Outcome, Tokens) or empty for the
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

	// request: MaxTokens is the resolved limit; Outcome is "completed" or
	// "cancelled" (the client went away); Tokens is how many tokens were sent.
	Path         string `json:"path,omitempty"`
	PromptTokens int    `json:"prompt_tokens,omitempty"`
	MaxTokens    int    `json:"max_tokens,omitempty"`
	Stream       bool   `json:"stream,omitempty"`
	Outcome      string `json:"outcome,omitempty"`
	Tokens       int    `json:"tokens,omitempty"`
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
