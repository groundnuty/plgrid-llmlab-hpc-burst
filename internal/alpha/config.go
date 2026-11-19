package alpha

import (
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
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
	envRunID      = "HPCBURST_ALPHA_RUN_ID"
	// envStagingDir is the Provider's own spec.stagingDir, for the rows whose profile has none (Row.ProviderStaging; #757, #735). It is account
	// data: a cluster path carries a user name, so no message or log of this package names its value.
	envStagingDir = "HPCBURST_ALPHA_STAGING_DIR"
	// envStudyloadBin is the load generator's binary, an absolute path (the baseline row's Studyload; #768's hack/studyload, built by the script or named by
	// the operator: the row imports none of it). envLLMLabCreds is the managed LLMLab credentials file, mode 600 (the row's LLMLab; HBEP-4, Provider
	// settings). Both are live settings: a dry run refuses them.
	envStudyloadBin = "HPCBURST_ALPHA_STUDYLOAD_BIN"
	envLLMLabCreds  = "HPCBURST_ALPHA_LLMLAB_CREDENTIALS"
	// envOmitMinTokens is the min_tokens probe's verdict (internal/front, TestLLMLabMinTokens): 1 leaves min_tokens out of every request, on every route.
	// envBaselinePhases overrides the baseline phases' fields (PhasesFromEnv). Neither is a live-only setting.
	envOmitMinTokens  = "HPCBURST_ALPHA_OMIT_MIN_TOKENS"
	envBaselinePhases = "HPCBURST_ALPHA_BASELINE_PHASES"
)

// liveEnv are the settings only a live run has. A dry run refuses to start
// when any is set: a dry pass must never stand for a live one.
var liveEnv = []string{envAccount, envCreds, envHostLock, envControlDir, envBinDir, envBuildDir, envMode, envAddr, envKey, envListen, envStagingDir, envStudyloadBin, envLLMLabCreds}

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
	// FrontLabel says where the front ran (issue #224); every text to post names it.
	FrontLabel string
	// The live run's settings.
	Account, Credentials, HostLock, ControlDir string
	BinDir, BuildDir                           string
	// DialHomeMode is direct, ssh or host-dialed. DialHomeAddr is empty in host-dialed mode, which has
	// none because the host dials the jobs (HBEP-6, Dial-home modes). DialHomeKey is ssh's alone.
	DialHomeMode, DialHomeAddr, DialHomeKey string
	// DialHomeListen is the front's local dial-home listener in direct mode,
	// host:port, where the owner's route arrives on this host.
	DialHomeListen string
	// Profile names the row's shipped platform profile (profiles/<name>.yaml).
	Profile string
	// StagingDir is the Provider's own spec.stagingDir, for a row with ProviderStaging: the live run's staging root on the shared file
	// system, from HPCBURST_ALPHA_STAGING_DIR. It is never logged.
	StagingDir string
	// RunID names the run's Provider and deployment, and so begins every job
	// tag of the run (TagPrefix): hack/live-checks.sh draws it and cleans up
	// by it.
	RunID string
	// StudyloadBin and LLMLabCredentials are the baseline row's live settings (Row.Studyload, Row.LLMLab); both are paths and never reach a log.
	StudyloadBin, LLMLabCredentials string
	// OmitMinTokens is the run-wide switch of the min_tokens probe's verdict, and Phases the baseline row's phases (DefaultPhases and the override).
	OmitMinTokens bool
	Phases        []Phase
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
	// Read before the dry run returns: a dry run's text to post names it too.
	cfg.FrontLabel = getenv(EnvFrontLabel)
	if err := ValidFrontLabel(cfg.FrontLabel); err != nil {
		bad = append(bad, EnvFrontLabel+": "+err.Error())
	}
	// The baseline row's phases and its min_tokens switch, read in both modes: a dry run's generator config follows them too.
	if IsBaselineRow(row.Name) {
		var perr error
		if cfg.Phases, perr = PhasesFromEnv(DefaultPhases(int(row.InstanceCap)), getenv(envBaselinePhases)); perr != nil {
			bad = append(bad, perr.Error())
		}
		switch v := getenv(envOmitMinTokens); v {
		case "", "0":
		case "1":
			cfg.OmitMinTokens = true
		default:
			bad = append(bad, envOmitMinTokens+" must be 1 or 0")
		}
	} else {
		for _, k := range []string{envBaselinePhases, envOmitMinTokens} {
			if v := getenv(k); v != "" {
				bad = append(bad, k+" is set for a row that has no baseline phases")
			}
		}
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
	cfg.DialHomeMode = need(envMode)
	cfg.DialHomeListen, cfg.Profile = getenv(envListen), need(envProfile)
	// The Provider's own staging directory: required by a row whose profile has none, refused for a row that does not take one, so that a
	// stray setting never reads as if it applied. Only its shape is checked, and no message holds its value.
	if row.ProviderStaging {
		if cfg.StagingDir = need(envStagingDir); cfg.StagingDir != "" {
			if err := CheckStagingDir(cfg.StagingDir); err != nil {
				bad = append(bad, envStagingDir+": "+err.Error())
			}
		}
	} else if getenv(envStagingDir) != "" {
		bad = append(bad, envStagingDir+" is set for a row whose profile gives the staging directory")
	}
	// The baseline row's generator binary and LLMLab credentials: required by the row that takes them, refused for one that does not, and only
	// their shape is checked: an absolute path, never in a message.
	for _, f := range []struct {
		key  string
		need bool
		to   *string
	}{{envStudyloadBin, row.Studyload, &cfg.StudyloadBin}, {envLLMLabCreds, row.LLMLab, &cfg.LLMLabCredentials}} {
		switch {
		case f.need:
			if *f.to = need(f.key); *f.to != "" && !filepath.IsAbs(*f.to) {
				bad = append(bad, f.key+" must be an absolute path")
			}
		case getenv(f.key) != "":
			bad = append(bad, f.key+" is set for a row that does not take it")
		}
	}
	switch cfg.DialHomeMode {
	case "host-dialed":
		// The host dials the jobs through the login node, so the mode has no address and needs no key
		// (HBEP-6, Dial-home modes; HBEP-11, open question 8, amended 9 Nov 2026). An address that is
		// set is refused, so a row cannot read as if it had one; the key, as in direct mode, is not read.
		if getenv(envAddr) != "" {
			bad = append(bad, envAddr+" must not be set in host-dialed mode: the host dials the jobs, so the mode has no address")
		}
	case "", "direct":
		cfg.DialHomeAddr = need(envAddr)
	case "ssh":
		cfg.DialHomeAddr, cfg.DialHomeKey = need(envAddr), need(envKey)
	default:
		bad = append(bad, envMode+" must be direct, ssh or host-dialed")
	}
	// Sano's compute nodes have no home directory, where ssh mode's agent reads its key, so these rows refuse ssh mode (L-1.16; ReadProfile refuses it there
	// too: profiles/sano.yaml lists host-dialed alone, dialHomeModes, HBEP-4). Direct mode is allowed: it is no profile value (HBEP-4: it rests on the
	// control-plane host's address, not on the platform), so ReadProfile skips it, and the first study dials Sano in it (issue #735).
	if row.Cluster == "sano" && cfg.DialHomeMode == "ssh" {
		bad = append(bad, envMode+" must be host-dialed or direct for "+row.Name+": Sano's compute nodes have no home directory, where ssh mode's agent reads its key")
	}
	// In direct mode a Sano row needs the front's dial-home listen address as well as the address the jobs dial (the address is needed above, in every
	// direct-mode row): liveWorld refuses a direct run with no listener, but only after the script has contacted Slurm and built the binaries, so the
	// settings refuse it first (PR 792's review). Another row keeps liveWorld's refusal.
	if row.Cluster == "sano" && cfg.DialHomeMode == "direct" && cfg.DialHomeListen == "" {
		bad = append(bad, envListen+" is not set: "+row.Name+" in direct mode needs the front's dial-home listen address beside "+envAddr)
	}
	// The keeper row tests the relay keeper, which runs the relays of host-dialed mode alone (HBEP-6, The relay keeper): another mode has none.
	if row.Name == BetaKeeperName && cfg.DialHomeMode != "host-dialed" {
		bad = append(bad, envMode+" must be host-dialed for "+BetaKeeperName+": the relay keeper runs the relays of host-dialed mode alone")
	}
	// The Athena twin of the baseline runs under athena-now.yaml, whose only dial-home mode is host-dialed (ssh is refused there and direct is no profile value).
	if row.Name == BetaAthenaBaselineName && cfg.DialHomeMode != "host-dialed" {
		bad = append(bad, envMode+" must be host-dialed for "+BetaAthenaBaselineName+": athena-now.yaml lists host-dialed alone")
	}
	// The bulk row reads the bulk path on the relays' ssh through the login node, the path the criterion names (HBEP-6, Graduation criteria, Beta).
	if row.Name == BetaBulkName && cfg.DialHomeMode != "host-dialed" {
		bad = append(bad, envMode+" must be host-dialed for "+BetaBulkName+": the bulk path's real-path buffer is read on the relays' ssh through the login node")
	}
	// The script's shape and this package's row are one fact; a mismatch is a drift, not a setting.
	if cfg.Profile != "" && cfg.Profile != row.ProfileName() {
		bad = append(bad, "the script's profile differs from the row's")
	}
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
	// The script's cleanup finds the run's jobs by the prefix this ID makes: a run without the script's ID would
	// submit jobs that cleanup cannot see.
	if cfg.RunID = need(envRunID); cfg.RunID != "" {
		if err := CheckRunID(cfg.RunID); err != nil {
			bad = append(bad, envRunID+": "+err.Error())
		}
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

// stagingDirForm is the form a Provider's spec.stagingDir takes (api/v1alpha1, ProviderSpec.StagingDir): an absolute path, or one that
// starts with an environment variable, of letters, digits and ._:=,/-.
var stagingDirForm = regexp.MustCompile(`^(/|\$[A-Za-z_][A-Za-z0-9_]*)[A-Za-z0-9._:=,/-]*$`)

// stagingDirReadable is what the rows add to that form: no ':', '=' or ','. The CRD takes them, but the warm-start reader puts the path of the job's standard
// error on a command line unquoted and refuses any character outside letters, digits and "._/-" (safePath, in the live flow), after the run has spent
// its GPU time and with its evidence unread (the review of PR 770). So the row refuses them at its first line.
var stagingDirReadable = regexp.MustCompile(`^[^:=,]*$`)

// CheckStagingDir holds a Provider's stagingDir to the form the CRD gives it, and to the rows' own rule on top of it, before anything is built: a value the
// API would refuse, or the harness could not read the agent's log by, fails the run at its first line instead of at the first Provider, or at the end of a
// run. The errors name no part of the value.
func CheckStagingDir(dir string) error {
	if !stagingDirForm.MatchString(dir) || slices.Contains(strings.Split(dir, "/"), "..") || len(dir) > 1024 {
		return fmt.Errorf("is not an absolute path, or one that starts with a variable, of letters, digits and ._:=,/- with no .. segment")
	}
	if !stagingDirReadable.MatchString(dir) {
		return fmt.Errorf("has a ':', '=' or ',' in it: the Provider's CRD takes them, but the rows read the agent's log by this path, which a command line carries unquoted, and refuse them")
	}
	return nil
}
