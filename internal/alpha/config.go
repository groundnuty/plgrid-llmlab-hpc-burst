package alpha

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// The environment of an alpha row's test binary. hack/live-checks.sh sets it
// from the settings file (env_alpha); a dry run sets only the row, the dry
// switch and the record directory.
const (
	envRow        = "HPCBURST_ALPHA_ROW"
	envDry        = "HPCBURST_ALPHA_DRY"
	envDryFault   = "HPCBURST_ALPHA_DRY_FAULT"
	envOut        = "HPCBURST_ALPHA_OUT"
	envPartition  = "HPCBURST_ALPHA_PARTITION"
	envQOS        = "HPCBURST_ALPHA_QOS"
	envGPUs       = "HPCBURST_ALPHA_GPUS"
	envAccount    = "HPCBURST_ALPHA_ACCOUNT"
	envCreds      = "HPCBURST_ALPHA_CREDENTIALS"
	envHostLock   = "HPCBURST_ALPHA_HOST_LOCK"
	envControlDir = "HPCBURST_ALPHA_CONTROL_DIR"
	envBinDir     = "HPCBURST_ALPHA_BIN_DIR"
	envBuildDir   = "HPCBURST_ALPHA_BUILD_DIR"
	envMode       = "HPCBURST_ALPHA_DIALHOME_MODE"
	envAddr       = "HPCBURST_ALPHA_DIALHOME_ADDR"
	envKey        = "HPCBURST_ALPHA_DIALHOME_KEY"
	envListen     = "HPCBURST_ALPHA_DIALHOME_LISTEN"
	envProfile    = "HPCBURST_ALPHA_PROFILE"
	envPending    = "HPCBURST_ALPHA_PENDING_MAX_SECONDS"
	envLease      = "HPCBURST_ALPHA_LEASE_MINUTES"
)

// liveEnv are the settings only a live run has. A dry run refuses to start
// when any is set: a dry pass must never stand for a live one.
var liveEnv = []string{envAccount, envCreds, envHostLock, envControlDir, envBinDir, envBuildDir, envMode, envAddr, envKey, envListen}

// Config is a run's settings. The account, the credentials file and the
// dial-home key are paths and names that never reach a log.
type Config struct {
	Row Row
	// Dry is true for a dry run: stand-in steps over a fake Slurm in run mode.
	Dry bool
	// StandIn is true for a stand-in run: the live steps over the real stack on
	// this host and a fake Slurm in run mode behind a real sshd (issue #245).
	StandIn bool
	// DryFault names the fault the stand-in steps inject; empty for none.
	DryFault string
	// Out is the record directory.
	Out string
	// PendingMax is the first clock (the wait in the queue).
	PendingMax time.Duration
	// The live run's settings.
	Account, Credentials, HostLock, ControlDir string
	BinDir, BuildDir                           string
	DialHomeMode, DialHomeAddr, DialHomeKey    string
	// DialHomeListen is the front's local dial-home listener in direct mode,
	// host:port, where the owner's route arrives on this host.
	DialHomeListen string
	// Profile names the row's shipped platform profile (profiles/<name>.yaml).
	Profile string
}

// Mode names the run's mode for the log, so a dry pass cannot be read as a live one.
func (c Config) Mode() string {
	if c.Dry {
		return "DRY RUN: stand-in steps over a fake Slurm in run mode; nothing reached a cluster"
	}
	if c.StandIn {
		return "STAND-IN RUN: the live steps over the real stack on this host and a fake Slurm in run mode; nothing reached a cluster"
	}
	return "LIVE RUN: a real cluster"
}

// ConfigFromEnv reads a run's settings. It returns a skip reason when the row
// is not asked for (the gate is HPCBURST_ALPHA_ROW), an error listing every
// problem when it is asked for with settings that cannot run it, and a Config
// otherwise. mine are the rows the calling test serves.
func ConfigFromEnv(getenv func(string) string, mine ...string) (cfg Config, skip string, err error) {
	name := getenv(envRow)
	if name == "" {
		return cfg, envRow + " is not set: an alpha row needs a real cluster and the owner's dial-home route (docs/alpha-runbook.md)", nil
	}
	served := false
	for _, m := range mine {
		served = served || m == name
	}
	if !served {
		return cfg, envRow + " names another row than this test's", nil
	}
	row, ok := RowByName(name)
	if !ok {
		return cfg, "", fmt.Errorf("%s names no alpha row", envRow)
	}
	cfg.Row = row
	cfg.Out = getenv(envOut)
	var bad []string
	// A tunnel row's lease is the owner's choice, in whole minutes: the script always passes it for a live run.
	// A dry run may leave it out, for the row's default.
	if v := getenv(envLease); v != "" || (strings.HasPrefix(row.Name, "tunnel-hour-") && getenv(envDry) != "1") {
		m, err := strconv.Atoi(v)
		if err != nil {
			bad = append(bad, envLease+" must be a whole number of minutes")
		} else if r, err := WithTunnelLease(row, time.Duration(m)*time.Minute); err != nil {
			bad = append(bad, envLease+": "+err.Error())
		} else {
			cfg.Row = r
		}
	}
	if !filepath.IsAbs(cfg.Out) {
		bad = append(bad, envOut+" must be an absolute path")
	}
	if getenv(envDry) == "1" {
		cfg.Dry, cfg.DryFault = true, getenv(envDryFault)
		for _, k := range liveEnv {
			if getenv(k) != "" {
				bad = append(bad, "a dry run refuses to start with live settings present ("+k+" is set)")
			}
		}
		// A dry run's clock is a manual one, so its own wait is a minute unless the script names the wait it
		// announced: the stand-in then runs that bound, and a test of the script reads the one it asked for.
		cfg.PendingMax = time.Minute
		if v := getenv(envPending); v != "" {
			if d, perr := pendingFromEnv(v); perr != nil {
				bad = append(bad, perr.Error())
			} else {
				cfg.PendingMax = d
			}
		}
		return cfg, "", joinBad(bad)
	}
	if getenv(envDryFault) != "" {
		bad = append(bad, envDryFault+" is for dry runs only")
	}
	need := func(k string) string {
		v := getenv(k)
		if v == "" {
			bad = append(bad, k+" is not set")
		}
		return v
	}
	cfg.Account, cfg.Credentials = need(envAccount), need(envCreds)
	cfg.HostLock, cfg.ControlDir = need(envHostLock), need(envControlDir)
	cfg.BinDir, cfg.BuildDir = need(envBinDir), need(envBuildDir)
	cfg.DialHomeMode, cfg.DialHomeAddr = need(envMode), need(envAddr)
	cfg.DialHomeListen, cfg.Profile = getenv(envListen), need(envProfile)
	switch cfg.DialHomeMode {
	case "", "direct":
	case "ssh":
		cfg.DialHomeKey = need(envKey)
	default:
		bad = append(bad, envMode+" must be direct or ssh")
	}
	// The script's shape and this package's row are one fact; a mismatch is a drift, not a setting.
	if p := need(envPartition); p != "" && p != row.Partition {
		bad = append(bad, "the script's partition differs from the row's")
	}
	if q := getenv(envQOS); q != row.QOS {
		bad = append(bad, "the script's QOS differs from the row's")
	}
	if g := need(envGPUs); g != "" && g != strconv.Itoa(row.GPUs) {
		bad = append(bad, "the script's GPU count differs from the row's")
	}
	if d, perr := pendingFromEnv(need(envPending)); perr != nil {
		bad = append(bad, perr.Error())
	} else {
		cfg.PendingMax = d
	}
	return cfg, "", joinBad(bad)
}

// pendingFromEnv reads the wait in the queue: a positive whole number of seconds.
func pendingFromEnv(v string) (time.Duration, error) {
	s, err := strconv.Atoi(v)
	if err != nil || s <= 0 {
		return 0, fmt.Errorf("%s must be a positive number of seconds", envPending)
	}
	return time.Duration(s) * time.Second, nil
}

func joinBad(bad []string) error {
	if len(bad) == 0 {
		return nil
	}
	return fmt.Errorf("%s", strings.Join(bad, "; "))
}
