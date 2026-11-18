package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

// Duration is a time.Duration in a JSON config, written as a Go duration string ("150ms", "2s").
type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("a duration is a string such as \"150ms\"")
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("duration %q: %w", s, err)
	}
	*d = Duration(v)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

// Config is the generator's JSON config. Unknown fields are refused, so a misspelt knob is an error and not a default.
type Config struct {
	// BaseURL is the OpenAI-style base, with its /v1: a request goes to BaseURL + "/chat/completions".
	BaseURL string `json:"baseURL"`
	Model   string `json:"model"`
	// APIKeyFile names a file that holds the API key (one line). It is read when the run starts; the key is sent as a bearer token and
	// never printed, logged or recorded. Empty: no Authorization header.
	APIKeyFile string `json:"apiKeyFile,omitempty"`
	// SessionHeader is the per-session request header; default x-session-id (the front's DefaultSessionHeaders, internal/front/convkey).
	SessionHeader string `json:"sessionHeader,omitempty"`
	// RunID prefixes every session ID, so two runs never share one; default six random hex digits. Set it for a repeatable ID.
	RunID string `json:"runId,omitempty"`
	// OmitMinTokens leaves min_tokens out of the request, for an endpoint that refuses the field. The output length is then the
	// endpoint's to decide, and the recorded completion_tokens says what it was.
	OmitMinTokens bool `json:"omitMinTokens,omitempty"`
	// RequestTimeout bounds each request, headers to the last event; default 10m.
	RequestTimeout Duration   `json:"requestTimeout,omitempty"`
	Workloads      []Workload `json:"workloads"`
}

// Workload is one declared workload: Sessions concurrent conversations of Turns streamed requests each, with a fixed shape.
// Sizes are in WORDS, one word per intended token (see the README on how a real tokenizer differs).
type Workload struct {
	Name          string `json:"name"`
	Sessions      int    `json:"sessions"`
	Turns         int    `json:"turns"`
	ContextTokens int    `json:"contextTokens"` // the first user message
	TurnTokens    int    `json:"turnTokens"`    // each later user message
	OutputTokens  int    `json:"outputTokens"`  // max_tokens and min_tokens of every request
	// ThinkTime is the wait between the end of a reply and the next request of the same session.
	ThinkTime Duration `json:"thinkTime,omitempty"`
	// StartStagger is the delay between the starts of consecutive sessions of the workload: session j starts j*StartStagger after the run.
	StartStagger Duration `json:"startStagger,omitempty"`
	// Headers are static request headers (name to value) sent on every request of the workload, such as the workload header the front
	// will read once HBEP-764's R2 names it. The generator names none.
	Headers map[string]string `json:"headers,omitempty"`
}

var (
	tokenRE    = regexp.MustCompile(`^[A-Za-z0-9!#$%&'*+.^_|~-]+$`)
	nameRE     = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
	runIDRE    = regexp.MustCompile(`^[A-Za-z0-9._-]{1,32}$`)
	reserved   = []string{"authorization", "content-type", "content-length", "accept", "host", "transfer-encoding", "connection"}
	maxConfig  = int64(1 << 20)
	maxSession = 100000
	maxTokens  = 1 << 20
)

// LoadConfig reads and validates the config file.
func LoadConfig(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxConfig+1))
	if err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	if int64(len(b)) > maxConfig {
		return Config{}, fmt.Errorf("config: larger than %d bytes", maxConfig)
	}
	return ParseConfig(b)
}

// ParseConfig decodes and validates a config, filling the defaults.
func ParseConfig(b []byte) (Config, error) {
	var c Config
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	if dec.More() {
		return Config{}, errors.New("config: more than one JSON value")
	}
	if c.SessionHeader == "" {
		c.SessionHeader = "x-session-id"
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = Duration(10 * time.Minute)
	}
	if err := c.validate(); err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	return c, nil
}

func (c Config) validate() error {
	u, err := url.Parse(c.BaseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return errors.New("baseURL must be an http or https URL with a host")
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return errors.New("baseURL must carry no credentials, query or fragment")
	}
	if c.Model == "" {
		return errors.New("model is empty")
	}
	if !tokenRE.MatchString(c.SessionHeader) || isReserved(c.SessionHeader) {
		return fmt.Errorf("sessionHeader %q is not a usable header name", c.SessionHeader)
	}
	if c.RunID != "" && !runIDRE.MatchString(c.RunID) {
		return fmt.Errorf("runId %q must be 1 to 32 characters of letters, digits, dot, underscore or hyphen", c.RunID)
	}
	if c.RequestTimeout < 0 {
		return errors.New("requestTimeout is negative")
	}
	if len(c.Workloads) == 0 {
		return errors.New("no workloads")
	}
	seen := map[string]bool{}
	for i, w := range c.Workloads {
		if err := w.validate(c.SessionHeader); err != nil {
			return fmt.Errorf("workloads[%d] %q: %w", i, w.Name, err)
		}
		if seen[w.Name] {
			return fmt.Errorf("workloads[%d]: name %q is used twice", i, w.Name)
		}
		seen[w.Name] = true
	}
	return nil
}

func (w Workload) validate(sessionHeader string) error {
	switch {
	case !nameRE.MatchString(w.Name):
		return errors.New("name must be 1 to 64 characters of letters, digits, dot, underscore or hyphen")
	case w.Sessions < 1 || w.Sessions > maxSession:
		return fmt.Errorf("sessions must be 1 to %d", maxSession)
	case w.Turns < 1 || w.Turns > 1000:
		return errors.New("turns must be 1 to 1000")
	case w.ContextTokens < 1 || w.ContextTokens > maxTokens:
		return fmt.Errorf("contextTokens must be 1 to %d", maxTokens)
	case w.Turns > 1 && (w.TurnTokens < 1 || w.TurnTokens > maxTokens):
		return fmt.Errorf("turnTokens must be 1 to %d when there is more than one turn", maxTokens)
	case w.Turns == 1 && w.TurnTokens != 0:
		return errors.New("turnTokens must be 0 for a one-turn workload: no later turn would use it")
	case w.OutputTokens < 1 || w.OutputTokens > maxTokens:
		return fmt.Errorf("outputTokens must be 1 to %d", maxTokens)
	case w.ThinkTime < 0 || w.StartStagger < 0:
		return errors.New("thinkTime and startStagger must not be negative")
	}
	for k, v := range w.Headers {
		if !tokenRE.MatchString(k) || isReserved(k) || strings.EqualFold(k, sessionHeader) {
			return fmt.Errorf("header %q is not a usable static header name (not the session header, Authorization or a framing header)", k)
		}
		for _, r := range v {
			if r < 0x20 && r != '\t' || r > 0x7e {
				return fmt.Errorf("header %q has a value with a control or non-ASCII character", k)
			}
		}
	}
	return nil
}

func isReserved(name string) bool {
	for _, r := range reserved {
		if strings.EqualFold(name, r) {
			return true
		}
	}
	return false
}
