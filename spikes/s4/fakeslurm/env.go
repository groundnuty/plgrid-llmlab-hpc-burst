package fakeslurm

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

// Env carries everything a command needs from its process, so tests can run
// commands in-process with a fake clock, a map for the environment and hooks
// instead of real sleeps.
type Env struct {
	Getenv func(string) string
	Now    func() time.Time
	Sleep  func(time.Duration) // used by the sbatch delay hooks
	Block  func()              // used by FAKE_SLURM_SBATCH_BLOCK_MARKER; must not return in production
	Stdout io.Writer
	Stderr io.Writer
	Pid    int

	// InLock, when set, is called after every lock acquisition; the func it
	// returns is called just before the lock is released (tests use the pair
	// to observe mutual exclusion).
	InLock func() func()
	// OutsideLock, when set, runs wherever the fake sleeps or blocks, which
	// must happen with the lock released (tests probe the lock there).
	OutsideLock func(point string)
}

func (e Env) withDefaults() Env {
	if e.Getenv == nil {
		e.Getenv = os.Getenv
	}
	if e.Now == nil {
		e.Now = time.Now
	}
	if e.Sleep == nil {
		e.Sleep = time.Sleep
	}
	if e.Block == nil {
		e.Block = func() {
			for {
				time.Sleep(time.Hour)
			}
		}
	}
	if e.Stdout == nil {
		e.Stdout = os.Stdout
	}
	if e.Stderr == nil {
		e.Stderr = os.Stderr
	}
	if e.Pid == 0 {
		e.Pid = os.Getpid()
	}
	return e
}

// Config is the parsed FAKE_SLURM_* environment.
type Config struct {
	StatePath    string
	LogPath      string
	Caller       string
	MinJobAge    time.Duration
	AcctDelay    time.Duration
	DataParser   string
	User         string
	StoreComment bool
	DelayBefore  time.Duration
	DelayAfter   time.Duration
	BlockMarker  string
	Fail         bool
	Zone         *time.Location
}

// loadConfig reads and validates the environment, reporting every problem at
// once.
func loadConfig(getenv func(string) string) (Config, []string) {
	var errs []string
	c := Config{
		StatePath:   getenv("FAKE_SLURM_STATE"),
		LogPath:     getenv("FAKE_SLURM_LOG"),
		Caller:      getenv("FAKE_SLURM_CALLER"),
		DataParser:  getenv("FAKE_SLURM_DATA_PARSER"),
		User:        getenv("FAKE_SLURM_USER"),
		BlockMarker: getenv("FAKE_SLURM_SBATCH_BLOCK_MARKER"),
		Zone:        time.UTC,
	}
	if c.StatePath == "" {
		errs = append(errs, "FAKE_SLURM_STATE is not set (path of the JSON state file)")
	}
	if c.LogPath == "" {
		errs = append(errs, "FAKE_SLURM_LOG is not set (path of the JSON-lines event log)")
	}
	if c.DataParser == "" {
		c.DataParser = "v0.0.42"
	}
	if c.DataParser != "v0.0.42" && c.DataParser != "v0.0.40" {
		errs = append(errs, fmt.Sprintf("FAKE_SLURM_DATA_PARSER=%q is not v0.0.42 or v0.0.40", c.DataParser))
	}
	if c.User == "" {
		c.User = "spike"
	}
	c.StoreComment = getenv("FAKE_SLURM_STORE_COMMENT") == "1"
	c.Fail = getenv("FAKE_SLURM_SBATCH_FAIL") == "1"

	c.MinJobAge = envSeconds(getenv, "FAKE_SLURM_MIN_JOB_AGE", 10, &errs)
	c.AcctDelay = envSeconds(getenv, "FAKE_SLURM_ACCT_DELAY", 0, &errs)
	c.DelayBefore = envSeconds(getenv, "FAKE_SLURM_SBATCH_DELAY_BEFORE", 0, &errs)
	c.DelayAfter = envSeconds(getenv, "FAKE_SLURM_SBATCH_DELAY_AFTER", 0, &errs)

	if tz := getenv("TZ"); tz != "" {
		loc, err := time.LoadLocation(strings.TrimPrefix(tz, ":"))
		if err != nil {
			errs = append(errs, fmt.Sprintf("TZ=%q: %v", tz, err))
		} else {
			c.Zone = loc
		}
	}
	return c, errs
}

func envSeconds(getenv func(string) string, name string, def float64, errs *[]string) time.Duration {
	v := getenv(name)
	if v == "" {
		return time.Duration(def * float64(time.Second))
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f < 0 {
		*errs = append(*errs, fmt.Sprintf("%s=%q is not a non-negative number of seconds", name, v))
		return 0
	}
	return time.Duration(f * float64(time.Second))
}
