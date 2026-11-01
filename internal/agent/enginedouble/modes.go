package enginedouble

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// EnvModes is the environment variable that selects the modes.
const EnvModes = "HB_ENGINE_DOUBLE"

// ParseModes reads the comma-separated value of HB_ENGINE_DOUBLE, for example
// "kv-low,slow-first=200ms", and returns DefaultConfig with the modes applied.
// The empty string is no mode. An unknown mode, a missing or malformed value
// and a value on a mode that takes none are errors, so a typo in a test fails
// at once instead of testing nothing.
func ParseModes(s string) (Config, error) {
	cfg := DefaultConfig()
	for _, item := range strings.Split(s, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		name, val, hasVal := strings.Cut(item, "=")
		if err := applyMode(&cfg, name, val, hasVal); err != nil {
			return Config{}, fmt.Errorf("%s: mode %q: %w", EnvModes, item, err)
		}
	}
	return cfg, cfg.Validate()
}

func applyMode(c *Config, name, val string, hasVal bool) error {
	flag := func(set *bool) error {
		if hasVal {
			return fmt.Errorf("takes no value")
		}
		*set = true
		return nil
	}
	switch name {
	case "kv-low":
		if hasVal {
			return fmt.Errorf("takes no value")
		}
		c.KVCacheTokens = 1000
		return nil
	case "greedy-repeat":
		return flag(&c.GreedyRepeat)
	case "tool-malformed":
		return flag(&c.ToolMalformed)
	case "tool-args-null":
		return flag(&c.ToolArgsNull)
	case "tool-args-badtype":
		return flag(&c.ToolArgsBadType)
	case "no-tool-call":
		return flag(&c.NoToolCall)
	case "reports-nothing":
		return flag(&c.ReportsNothing)
	case "health-fail":
		return flag(&c.HealthFail)
	case "errors-only":
		return flag(&c.ErrorsOnly)
	case "stall":
		return flag(&c.Stall)
	case "no-prefix-cache-metrics":
		return flag(&c.OmitPrefixCacheMetrics)
	case "no-tpot":
		return flag(&c.OmitTPOT)
	case "no-tpot-sum":
		return flag(&c.OmitTPOTSum)
	case "no-tpot-count":
		return flag(&c.OmitTPOTCount)
	case "ignore-term":
		return flag(&c.IgnoreTerm)
	case "write-caches":
		return flag(&c.WriteCaches)
	case "write-home":
		return flag(&c.WriteHome)
	case "print-key":
		return flag(&c.PrintKey)
	case "reasoning-forever":
		return flag(&c.ReasoningForever)
	case "reasoning-first":
		n, err := intValue(val, hasVal, 1)
		c.ReasoningFirst = n
		return err
	case "greedy-diverge":
		n, err := intValue(val, hasVal, 0)
		c.GreedyDiverge, c.GreedyDivergeAt = err == nil, n
		return err
	case "early-stop":
		// The reply stops after n tokens with "stop", even under ignore_eos,
		// unless min_tokens asks for more: the model's natural length.
		n, err := intValue(val, hasVal, 1)
		c.NaturalLen = n
		return err
	case "loop-above":
		n, err := intValue(val, hasVal, 1)
		c.LoopAbove = n
		return err
	case "report-max-model-len":
		n, err := intValue(val, hasVal, 1)
		c.ReportedMaxModelLen = n
		return err
	case "never-settle":
		n, err := intValue(val, hasVal, 1)
		c.NeverSettle = n
		return err
	case "health-fail-after":
		d, err := durValue(val, hasVal)
		c.HealthFailAfter, c.HealthFailAfterSet = d, err == nil
		return err
	case "slow-first":
		d, err := durValue(val, hasVal)
		c.SlowFirst = d
		return err
	case "token-delay":
		d, err := durValue(val, hasVal)
		c.TokenDelay = d
		return err
	case "start-delay":
		d, err := durValue(val, hasVal)
		c.StartDelay = d
		return err
	case "exit-after":
		if !hasVal {
			return fmt.Errorf("needs a duration, exit-after=<dur>[:code]")
		}
		dur, code, hasCode := strings.Cut(val, ":")
		d, err := durValue(dur, true)
		if err != nil {
			return err
		}
		c.ExitAfter, c.ExitAfterSet, c.ExitCode = d, true, 0
		if hasCode {
			n, err := strconv.Atoi(code)
			if err != nil || n < 0 || n > 255 {
				return fmt.Errorf("exit code %q is not 0 to 255", code)
			}
			c.ExitCode = n
		}
		return nil
	}
	return fmt.Errorf("unknown mode")
}

func intValue(val string, hasVal bool, min int) (int, error) {
	if !hasVal {
		return 0, fmt.Errorf("needs a number")
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		return 0, fmt.Errorf("%q is not a number", val)
	}
	if n < min {
		return 0, fmt.Errorf("%d is below %d", n, min)
	}
	return n, nil
}

func durValue(val string, hasVal bool) (time.Duration, error) {
	if !hasVal {
		return 0, fmt.Errorf("needs a duration")
	}
	d, err := time.ParseDuration(val)
	if err != nil {
		return 0, fmt.Errorf("%q is not a duration", val)
	}
	if d < 0 {
		return 0, fmt.Errorf("%s is negative", val)
	}
	return d, nil
}
