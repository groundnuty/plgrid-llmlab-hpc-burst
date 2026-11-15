// Command shim is the bulk row's test shim (issue #521, option (A); the science agent's rulings on #521): a test-only
// program that the live harness, for beta-athena-now-bulk alone, puts in the controller's agent-builds directory in
// hb-agent's place, so that the Provider stages it as the agent, by its digest, with nothing in the product changed.
//
// It is two programs in one binary:
//
//   - The engine double (package enginedouble): run as "apptainer", or with the double's role in its environment,
//     enginedouble.Dispatch runs the double's Apptainer or its engine, and exits.
//   - The shim: run as the staged hb-agent, for every argv the Provider and the batch bootstrap give it (the Allocation's
//     directory, "relay ...", "-version"), it execs the shipped agent the harness pre-staged, with the argv and every
//     descriptor unchanged, after checking that agent's SHA-256 against the one built into it. The job's agent process is
//     then the shipped agent itself: its PID, signals, exit codes and a relay's hand-over are the real agent's. The one
//     thing the shim adds is HPCBURST_APPTAINER, the knob the agent already reads (cmd/hb-agent), naming a symlink to
//     the shim under the name apptainer, so that the engine the agent starts is the double.
//
// The shipped agent's digest is built in (-ldflags "-X main.wantAgentDigest=<64 hex>"), so one shim names one agent and
// the shim's own digest depends on it. The agent is found from where the shim itself is staged: the Provider stages the
// agent build at <staging>/agent/<digest>/hb-agent (agentapi.StagedAgentPath), and the harness stages the shipped
// agent at <staging>/test-agent/<its digest>/hb-agent, outside the product's layout. Nothing comes from the job's
// environment, which nothing in the product sets.
//
// Run as apptainer to pull or build an image, it refuses, exit 1: the double stages no image, so it can never put a fake one in the Provider's
// shared cache (the science agent's rulings on #579); the Provider's real image must be staged before the run, which the row checks.
//
// It writes nothing to standard output, ever: a relay's standard output is the channel it hands over. A refusal is one
// line on standard error and exit 1: a shim built without the agent's digest, one not run from the Provider's staging
// layout, or a staged agent that is missing or whose digest differs. Nothing is executed then, so the job's agent never
// registers.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/enginedouble"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// wantAgentDigest is the SHA-256 of the shipped agent this shim execs, set at build time. Empty, the shim refuses to run.
var wantAgentDigest string

// TestAgentDir is the harness's directory for the shipped agent under the Provider's staging root: test-agent/<digest>/hb-agent.
const TestAgentDir = "test-agent"

// EnvApptainer is the agent's knob for the Apptainer it runs (cmd/hb-agent).
const EnvApptainer = "HPCBURST_APPTAINER"

var sha256Hex = regexp.MustCompile(`^[0-9a-f]{64}$`)

func main() {
	// The double stages no image (the science agent's rulings on #579): the agent's stager runs apptainer pull and build through the same knob as
	// the engine, and a fake image put where the Provider's real one was would replace it in the shared cache. A refused build leaves stageImage to
	// put the real image back.
	if refusesStaging(os.Args) {
		fmt.Fprintf(os.Stderr, "bulk-row test shim: refused: apptainer %s: the double stages no image; the Provider's real image must be staged first (the stage row)\n", os.Args[1])
		os.Exit(1)
	}
	enginedouble.Dispatch()
	exe, err := os.Executable()
	if err != nil {
		refuse("the shim's own executable: %v", err)
	}
	if err := run(wantAgentDigest, exe, os.Args, os.Environ(), syscall.Exec); err != nil {
		refuse("%v", err)
	}
}

func refuse(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "bulk-row test shim: refused: "+format+"\n", a...)
	os.Exit(1)
}

// execFunc is syscall.Exec's shape: it returns only on failure.
type execFunc func(path string, argv []string, env []string) error

// run checks and execs the shipped agent. exe is the shim's own staged file; argv and env are the shim's own.
func run(want, exe string, argv, env []string, exec execFunc) error {
	if !sha256Hex.MatchString(want) {
		return errors.New("built without the shipped agent's digest (-ldflags -X main.wantAgentDigest=<64 lowercase hex>)")
	}
	root, err := stagingRoot(exe)
	if err != nil {
		return err
	}
	agent := filepath.Join(root, TestAgentDir, want, agentapi.AgentBinaryName)
	got, err := fileDigest(agent)
	if err != nil {
		return fmt.Errorf("the shipped agent the harness stages, digest %s: %v", want, err)
	}
	if got != want {
		return fmt.Errorf("the staged shipped agent's digest is %s, not %s, the one this shim was built for", got, want)
	}
	if engineRuns(argv) {
		link, err := apptainerLink(root, want, exe)
		if err != nil {
			return err
		}
		env = withEnv(env, EnvApptainer, link)
	}
	return exec(agent, argv, env)
}

// stagingRoot is the Provider's staging root, from where it staged the shim: <root>/agent/<64 hex>/hb-agent.
func stagingRoot(exe string) (string, error) {
	dir, name := filepath.Split(exe)
	digestDir := filepath.Dir(filepath.Clean(dir))
	if name != agentapi.AgentBinaryName || !sha256Hex.MatchString(filepath.Base(filepath.Clean(dir))) || filepath.Base(digestDir) != "agent" {
		return "", fmt.Errorf("not run from the Provider's staging layout, <staging>/%s", agentapi.StagedAgentPath("<digest>"))
	}
	return filepath.Dir(digestDir), nil
}

// refusesStaging reports whether argv is the shim run as apptainer to pull or build an image.
func refusesStaging(argv []string) bool {
	return len(argv) > 1 && filepath.Base(argv[0]) == "apptainer" && (argv[1] == "pull" || argv[1] == "build")
}

// engineRuns reports whether this argv is the agent's run, which starts the engine: not a relay, and not -version.
func engineRuns(argv []string) bool {
	if len(argv) < 2 {
		return false
	}
	switch argv[1] {
	case "relay", "-version", "--version":
		return false
	}
	return true
}

// apptainerLink makes <root>/test-agent/<want>/shim-<12 hex of the shim>/apptainer, a symlink to the shim, and returns its
// path. The directory names the shim's digest, so that two shims built for one agent never share a link. The link is
// made under a temporary name and renamed into place, so a run never sees half of it.
func apptainerLink(root, want, exe string) (string, error) {
	self, err := fileDigest(exe)
	if err != nil {
		return "", fmt.Errorf("the shim's own digest: %v", err)
	}
	dir := filepath.Join(root, TestAgentDir, want, "shim-"+self[:12])
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("the apptainer link's directory: %v", err)
	}
	link := filepath.Join(dir, "apptainer")
	if cur, err := os.Readlink(link); err == nil && cur == exe {
		return link, nil
	}
	tmp := fmt.Sprintf("%s.%d", link, os.Getpid())
	_ = os.Remove(tmp)
	if err := os.Symlink(exe, tmp); err != nil {
		return "", fmt.Errorf("the apptainer link: %v", err)
	}
	if err := os.Rename(tmp, link); err != nil {
		_ = os.Remove(tmp)
		return "", fmt.Errorf("the apptainer link: %v", err)
	}
	return link, nil
}

func fileDigest(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// withEnv is env with key set to value, replacing any earlier value.
func withEnv(env []string, key, value string) []string {
	out := make([]string, 0, len(env)+1)
	for _, kv := range env {
		if !strings.HasPrefix(kv, key+"=") {
			out = append(out, kv)
		}
	}
	return append(out, key+"="+value)
}
