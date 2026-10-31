package front

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the front's configuration file (HBEP-5, Parameters). The launcher
// passes its path; a change needs a restart. Durations are Go duration
// strings ("2h", "30m").
type Config struct {
	ClientListen        string   `json:"clientListen,omitempty"`
	DialHomeListen      string   `json:"dialHomeListen,omitempty"`
	ClientKeysFile      string   `json:"clientKeysFile"`
	SessionHeaders      []string `json:"sessionHeaders,omitempty"`
	MaxRequestDuration  Duration `json:"maxRequestDuration,omitempty"`
	IdleTimeout         Duration `json:"idleTimeout,omitempty"`
	ConnectTimeout      Duration `json:"connectTimeout,omitempty"`
	UpstreamIdleTimeout Duration `json:"upstreamIdleTimeout,omitempty"`
	ClientKeepAlive     Duration `json:"clientKeepAlive,omitempty"`
	QueueTimeout        Duration `json:"queueTimeout,omitempty"`
	PauseAfterFailures  int      `json:"pauseAfterFailures,omitempty"`
	PauseDuration       Duration `json:"pauseDuration,omitempty"`
	UsageDir            string   `json:"usageDir"`
	// Namespaces are the namespaces whose ModelDeployments and Instances the
	// front watches; empty means all (HBEP-5, Request path step 3).
	Namespaces []string `json:"namespaces,omitempty"`
}

// Duration is a time.Duration written as a Go duration string.
type Duration time.Duration

// UnmarshalJSON reads a Go duration string.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("a duration is a string such as \"30m\": %w", err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

// MarshalJSON writes a Go duration string.
func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

// D is the value as a time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// HBEP-5's defaults (Parameters; Time-outs and keep-alive).
const (
	DefaultClientListen        = "127.0.0.1:8000"
	DefaultMaxRequestDuration  = 2 * time.Hour
	DefaultIdleTimeout         = 30 * time.Minute
	DefaultConnectTimeout      = 10 * time.Second
	DefaultUpstreamIdleTimeout = 4 * time.Second
	DefaultClientKeepAlive     = 120 * time.Second
	DefaultQueueTimeout        = 60 * time.Second
	DefaultPauseAfterFailures  = 3
	DefaultPauseDuration       = 30 * time.Second
)

// LoadConfig reads and checks a configuration file. Unknown members are an
// error, so a misspelt setting does not silently take its default.
func LoadConfig(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var c Config
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return Config{}, fmt.Errorf("front configuration %s: %w", path, err)
	}
	c = c.WithDefaults()
	if err := c.Validate(); err != nil {
		return Config{}, fmt.Errorf("front configuration %s: %w", path, err)
	}
	return c, nil
}

// WithDefaults returns c with every unset setting at its default.
func (c Config) WithDefaults() Config {
	setStr := func(v *string, d string) {
		if *v == "" {
			*v = d
		}
	}
	setDur := func(v *Duration, d time.Duration) {
		if *v == 0 {
			*v = Duration(d)
		}
	}
	setStr(&c.ClientListen, DefaultClientListen)
	setDur(&c.MaxRequestDuration, DefaultMaxRequestDuration)
	setDur(&c.IdleTimeout, DefaultIdleTimeout)
	setDur(&c.ConnectTimeout, DefaultConnectTimeout)
	setDur(&c.UpstreamIdleTimeout, DefaultUpstreamIdleTimeout)
	setDur(&c.ClientKeepAlive, DefaultClientKeepAlive)
	setDur(&c.QueueTimeout, DefaultQueueTimeout)
	setDur(&c.PauseDuration, DefaultPauseDuration)
	if c.PauseAfterFailures == 0 {
		c.PauseAfterFailures = DefaultPauseAfterFailures
	}
	if len(c.SessionHeaders) == 0 {
		c.SessionHeaders = []string{"x-session-id"}
	}
	return c
}

// Validate checks a configuration with its defaults applied, and names every
// problem at once.
func (c Config) Validate() error {
	var probs []string
	for name, addr := range map[string]string{"clientListen": c.ClientListen, "dialHomeListen": c.DialHomeListen} {
		if name == "dialHomeListen" && addr == "" {
			continue // its default depends on the mode: DialHomeAddr
		}
		if _, _, err := net.SplitHostPort(addr); err != nil {
			probs = append(probs, fmt.Sprintf("%s %q is not host:port", name, addr))
		}
	}
	if c.ClientKeysFile == "" {
		probs = append(probs, "clientKeysFile is required")
	}
	if c.UsageDir == "" {
		probs = append(probs, "usageDir is required")
	}
	for name, d := range map[string]Duration{
		"maxRequestDuration": c.MaxRequestDuration, "idleTimeout": c.IdleTimeout, "connectTimeout": c.ConnectTimeout,
		"upstreamIdleTimeout": c.UpstreamIdleTimeout, "clientKeepAlive": c.ClientKeepAlive,
		"queueTimeout": c.QueueTimeout, "pauseDuration": c.PauseDuration,
	} {
		if d <= 0 {
			probs = append(probs, name+" must be positive")
		}
	}
	if c.PauseAfterFailures < 1 {
		probs = append(probs, "pauseAfterFailures must be at least 1")
	}
	for _, h := range c.SessionHeaders {
		if strings.TrimSpace(h) == "" {
			probs = append(probs, "sessionHeaders holds an empty name")
		}
	}
	if len(probs) > 0 {
		return errors.New(strings.Join(sortedCopy(probs), "; "))
	}
	return nil
}

// DialHomePortOffset is the dial-home listener's port in ssh mode, relative
// to the launcher's port base: base+7, 29227 at the default base. Each
// platform's restricted key permits forwards to exactly base+3 (the agent
// endpoint) and base+7 (@hpc-burst-science-agent, 31 Oct 2026). PR #79 adds
// it as agentapi.DialHomePortOffset with Host.DialHomeAddr; this constant
// gives way to those once #79 merges.
const DialHomePortOffset = 7

// DialHomeAddr is the dial-home listener's address (HBEP-5, Listeners). Set
// in the configuration, it is used as given. Unset, it defaults in ssh mode
// to the loopback address at the launcher's port base plus
// DialHomePortOffset, where the restricted key's forward arrives; in direct
// mode it is refused, because compute nodes must reach it and only the
// operator knows that address. With no dial-home mode recorded, the ssh
// default applies, since no job is submitted until a front is configured.
func (c Config) DialHomeAddr(mode string, portBase int) (string, error) {
	if c.DialHomeListen != "" {
		return c.DialHomeListen, nil
	}
	if mode == ModeDirect {
		return "", errors.New("dialHomeListen is required in direct mode: compute nodes must reach it, so it has no default")
	}
	if portBase <= 0 {
		return "", errors.New("dialHomeListen is unset and no launcher port base is known for its default")
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(portBase+DialHomePortOffset)), nil
}
