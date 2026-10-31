package enginedouble

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestParseModes(t *testing.T) {
	d := DefaultConfig()
	cases := []struct {
		in   string
		want func(c *Config)
	}{
		{"", func(*Config) {}},
		{"kv-low", func(c *Config) { c.KVCacheTokens = 1000 }},
		{"kv-low,slow-first=200ms", func(c *Config) { c.KVCacheTokens = 1000; c.SlowFirst = 200 * time.Millisecond }},
		{" greedy-diverge=40 , greedy-repeat ", func(c *Config) { c.GreedyDiverge, c.GreedyDivergeAt, c.GreedyRepeat = true, 40, true }},
		{"greedy-diverge=0", func(c *Config) { c.GreedyDiverge = true }},
		{"loop-above=2000", func(c *Config) { c.LoopAbove = 2000 }},
		{"tool-malformed,no-tool-call", func(c *Config) { c.ToolMalformed, c.NoToolCall = true, true }},
		{"reports-nothing,health-fail,errors-only,stall", func(c *Config) { c.ReportsNothing, c.HealthFail, c.ErrorsOnly, c.Stall = true, true, true, true }},
		{"report-max-model-len=4096", func(c *Config) { c.ReportedMaxModelLen = 4096 }},
		{"never-settle=25", func(c *Config) { c.NeverSettle = 25 }},
		{"token-delay=0s", func(c *Config) { c.TokenDelay = 0 }},
		{"token-delay=5ms", func(c *Config) { c.TokenDelay = 5 * time.Millisecond }},
		{"start-delay=1s,ignore-term", func(c *Config) { c.StartDelay, c.IgnoreTerm = time.Second, true }},
		{"exit-after=2s", func(c *Config) { c.ExitAfter, c.ExitAfterSet = 2*time.Second, true }},
		{"exit-after=2s:7", func(c *Config) { c.ExitAfter, c.ExitAfterSet, c.ExitCode = 2*time.Second, true, 7 }},
	}
	for _, tc := range cases {
		want := d
		tc.want(&want)
		got, err := ParseModes(tc.in)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Errorf("ParseModes(%q) = %+v, %v; want %+v", tc.in, got, err, want)
		}
	}
}

func TestParseModesRefusesWhatItDoesNotKnow(t *testing.T) {
	for in, why := range map[string]string{
		"kv-lw":             "unknown mode",
		"kv-low=3":          "takes no value",
		"slow-first":        "needs a duration",
		"slow-first=fast":   "not a duration",
		"slow-first=-1s":    "negative",
		"greedy-diverge":    "needs a number",
		"greedy-diverge=-1": "below 0",
		"loop-above=0":      "below 1",
		"never-settle=x":    "not a number",
		"exit-after=1s:300": "not 0 to 255",
		"exit-after":        "needs a duration",
		"stall=yes":         "takes no value",
	} {
		_, err := ParseModes(in)
		if err == nil || !strings.Contains(err.Error(), why) || !strings.Contains(err.Error(), in) {
			t.Errorf("ParseModes(%q) = %v; want an error naming the mode and saying %q", in, err, why)
		}
	}
}

func TestConfigValidate(t *testing.T) {
	c := DefaultConfig()
	c.TemplateOverhead = 1
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "TemplateOverhead") {
		t.Errorf("TemplateOverhead 1: %v, want an error", err)
	}
	if err := (Config{}).Validate(); err != nil {
		t.Errorf("the zero Config means the defaults: %v", err)
	}
	defer func() {
		if recover() == nil {
			t.Error("Handler accepted an invalid Config")
		}
	}()
	Handler(c)
}
