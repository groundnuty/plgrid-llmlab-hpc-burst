package alpha

// The bulk row through its test shim (issue #521, option (A); the science agent's rulings on #521): the harness puts the shim in hb-agent's place in
// the stack's own copy of the agent builds, so the Provider stages it as the agent with nothing in the product changed; it stages the shipped agent the
// shim execs on the cluster, under the Provider's staging root at test-agent/<its digest>/hb-agent; and it reads, on the node and through the API, what
// ran. The operator's agent builds, the stack's source, must hash the same after the run as before it, and the staged shipped agent's directory is
// removed by its exact path on every exit, NOT RUN and FAIL included; each is a failure of the run when it does not hold (the rulings on the plan).

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// testAgentDir is the harness's own directory for the shipped agent under the Provider's staging root (the shim's TestAgentDir).
const testAgentDir = "test-agent"

// testShim is the bulk row's test shim and the shipped agent it execs, with the operator's agent builds the run must leave as it found them.
type testShim struct {
	shim, agent             []byte
	shimDigest, agentDigest string
	// builds is the operator's agent-builds directory (the stack's source), and before every file's SHA-256 there, read before the run.
	builds string
	before map[string]string
}

func digestOf(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// loadTestShim reads the shim and the shipped agent, and the operator's agent builds as they are before the run.
func loadTestShim(shimFile, agentFile, builds string) (*testShim, error) {
	shim, err := os.ReadFile(shimFile)
	if err != nil {
		return nil, fmt.Errorf("the bulk row's test shim: %w", err)
	}
	agent, err := os.ReadFile(agentFile)
	if err != nil {
		return nil, fmt.Errorf("the shipped agent the shim execs: %w", err)
	}
	before, err := buildsDigests(builds)
	if err != nil {
		return nil, err
	}
	return &testShim{shim: shim, agent: agent, shimDigest: digestOf(shim), agentDigest: digestOf(agent), builds: builds, before: before}, nil
}

// buildsDigests is the SHA-256 of every file of an agent-builds directory, by name.
func buildsDigests(dir string) (map[string]string, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("the operator's agent builds: %w", err)
	}
	out := map[string]string{}
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("the operator's agent builds: %w", err)
		}
		out[e.Name()] = digestOf(b)
	}
	if len(out) == 0 {
		return nil, errors.New("the operator's agent builds directory holds no build")
	}
	return out, nil
}

// buildsUnchanged is nil when every file of the operator's agent builds hashes as it did before the run, and none came or went: the shim goes in the
// stack's own copy alone.
func (s *testShim) buildsUnchanged() error {
	after, err := buildsDigests(s.builds)
	if err != nil {
		return err
	}
	var changed []string
	for name, d := range s.before {
		if after[name] != d {
			changed = append(changed, name)
		}
	}
	for name := range after {
		if _, ok := s.before[name]; !ok {
			changed = append(changed, name)
		}
	}
	if len(changed) > 0 {
		sort.Strings(changed)
		return fmt.Errorf("the operator's agent builds changed during the run (%s): the test shim must go in the stack's own copy alone", strings.Join(changed, ", "))
	}
	return nil
}

// place puts the shim in hb-agent's place in the stack's own copy of the agent builds, for the node's CPU architecture.
func (s *testShim) place(h *stack.Host, cpuArch string) {
	h.T.Helper()
	if err := os.WriteFile(filepath.Join(h.State, "agent-builds", agentapi.AgentBuildName(cpuArch)), s.shim, 0o700); err != nil {
		h.T.Fatalf("the test shim in hb-agent's place: %v", err)
	}
}

// testAgentPath is the shipped agent's path under the staging root, relative to it.
func (s *testShim) testAgentPath() string {
	return testAgentDir + "/" + s.agentDigest + "/" + agentapi.AgentBinaryName
}

// prestageAgent stages the shipped agent under the Provider's staging root, through the Provider's own batch client and the run's transport: written
// to a temporary name, its SHA-256 checked there, then renamed into place (slurm.Batch's EnsureFile). The directory is noted first, so that Close
// removes it whatever happens after.
func (r *liveRun) prestageAgent(ctx context.Context) {
	t := r.t
	t.Helper()
	s := r.w.shim
	paths, err := r.batch.Paths(ctx)
	if err != nil {
		t.Fatalf("the Provider's staging root, for the shipped agent: %v", err)
	}
	r.testAgentDir = paths.Staging + "/" + testAgentDir + "/" + s.agentDigest
	// The exact path, for the script's exit when this test is stopped before its Close (TestAgentDirFile), written before anything is staged.
	if err := os.WriteFile(filepath.Join(r.cfg.Out, TestAgentDirFile), []byte(r.testAgentDir+"\n"), 0o600); err != nil {
		t.Fatalf("the shipped agent's path for the script's exit: %v", err)
	}
	if err := r.batch.EnsureFile(ctx, provider.File{Path: s.testAgentPath(), Content: s.agent, SHA256: s.agentDigest, Mode: 0o700}); err != nil {
		t.Fatalf("staging the shipped agent the test shim execs: %v", err)
	}
}

// commandRunner runs one command line on the login node: the run's ssh transport, or a fake login in a test.
type commandRunner interface {
	Run(ctx context.Context, cmd slurm.Command) (slurm.Result, error)
}

// removeTestAgent removes the shipped agent's directory by its exact path, then checks that its agent is gone: the leftover check, which names it.
func removeTestAgent(ctx context.Context, run commandRunner, dir string) error {
	if !strings.Contains(dir, "/"+testAgentDir+"/") || !sha256Re.MatchString(filepath.Base(dir)) {
		return fmt.Errorf("refusing to remove %q: not a test-agent/<digest> directory", filepath.Base(dir))
	}
	name := testAgentDir + "/" + filepath.Base(dir)[:12] + "…"
	res, err := run.Run(ctx, slurm.Command{Line: "rm -rf " + dir})
	if err != nil || res.ExitCode != 0 {
		return fmt.Errorf("the shipped agent's staging directory %s could not be removed (exit %d, transport error %v)", name, res.ExitCode, err != nil)
	}
	res, err = run.Run(ctx, slurm.Command{Line: "test -f " + dir + "/" + agentapi.AgentBinaryName})
	if err != nil {
		return fmt.Errorf("the shipped agent's staging directory %s: its removal could not be checked: %v", name, err)
	}
	if res.ExitCode == 0 {
		return fmt.Errorf("the shipped agent's staging directory %s remains after the run", name)
	}
	return nil
}

// shimReadings reads what ran through the shim: the agent digest the Instance's Allocation names, then, on the node, the executables of the job's
// agent process, its engine and the Apptainer above the engine. A reading that cannot be taken is left empty, which the verdict names.
func (r *liveRun) shimReadings(ctx context.Context, p Plan, in v1alpha1.Instance, o *ShimObs) {
	t := r.t
	t.Helper()
	s := r.w.shim
	o.ShimDigest, o.AgentDigest = s.shimDigest, s.agentDigest
	var a v1alpha1.Allocation
	cctx, cancel := stack.CallCtx(t)
	err := r.c.Get(cctx, client.ObjectKey{Name: in.Spec.AllocationRef.Name, Namespace: in.Namespace}, &a)
	cancel()
	if err == nil && a.Spec.ForProvider.Batch != nil {
		o.Allocation = a.Spec.ForProvider.Batch.AgentDigest
	}
	jobID := r.ourJobFor(ctx, in, "the test shim on the node")
	var err2 error
	if o.AgentRunning, o.EngineRunning, o.ApptainerRunning, err2 = readShimNode(ctx, r.tr, jobID, in.Spec.Launch.Command, s.shimDigest); err2 != nil {
		t.Fatalf("the test shim on the node: %v", err2)
	}
	p.Logf("%s: through the test shim: the Allocation names the shim %t; on the node the agent process ran the shipped agent %t, the engine the shim %t, its Apptainer the shim %t",
		p.Row.Name, o.Allocation == s.shimDigest, o.AgentRunning == s.agentDigest, o.EngineRunning == s.shimDigest, o.ApptainerRunning == s.shimDigest)
}

// readShimNode reads, on the job's node, the executables of the job's agent process, its engine (the shim's double, MatchesShimEngine) and the
// Apptainer above the engine: one srun step lists the job's processes, and one for each reads a process's /proc/<pid>/exe (ExeDigestScript). A
// process that is not found, or whose executable cannot be read, leaves its digest empty; only a listing that cannot be taken is an error.
func readShimNode(ctx context.Context, run commandRunner, jobID string, command []string, shimDigest string) (agent, engine, apptainer string, err error) {
	line, err := SrunLine(jobID)
	if err != nil {
		return "", "", "", err
	}
	res, err := run.Run(ctx, slurm.Command{Line: line, Stdin: []byte(ListScript(jobID)), Timeout: srunTimeout})
	if err != nil || res.ExitCode != 0 {
		return "", "", "", fmt.Errorf("the srun step that lists the job's processes failed (exit %d, transport error %v)", res.ExitCode, err != nil)
	}
	procs, err := ParseListing(res.Stdout)
	if err != nil {
		return "", "", "", err
	}
	read := func(np NodeProc) string {
		script, err := ExeDigestScript(np)
		if err != nil {
			return ""
		}
		res, err := run.Run(ctx, slurm.Command{Line: line, Stdin: []byte(script), Timeout: srunTimeout})
		if err != nil || res.ExitCode != 0 {
			return ""
		}
		d, _ := ParseExeDigest(res.Stdout)
		return d
	}
	if ag, ok := PickAgent(procs); ok {
		agent = read(ag)
	}
	if eng, perr := PickEngine(procs, command, MatchesShimEngine(shimDigest)); perr == nil {
		engine = read(eng)
		if app, ok := ApptainerAbove(procs, eng); ok {
			apptainer = read(app)
		}
	}
	return agent, engine, apptainer, nil
}

// prepareShim is the bulk row's setup before its deployment: the Provider's image for the deployment must be staged with a whole record, read and
// hashed (the science agent's rulings on #579), else the run is NOT RUN with no job submitted; then the shipped agent the shim execs is staged.
func (r *liveRun) prepareShim(ctx context.Context) error {
	t := r.t
	t.Helper()
	paths, err := r.batch.Paths(ctx)
	if err != nil {
		t.Fatalf("the Provider's staging root: %v", err)
	}
	digest := r.w.md.Spec.Source.Served.Engine.ImageDigest
	format := partitionImageFormat(r.w.profile, r.cfg.Row.Partition)
	rec := imageRecordPath(paths.Staging, digest, format)
	sum, err := readImageRecord(ctx, r.tr, rec, digest, format)
	if err != nil {
		if errors.Is(err, ErrNoStagedImage) {
			return err
		}
		t.Fatalf("the Provider's image record: %v", err)
	}
	r.imageRecord, r.imageRecordSum, r.imageDigest, r.imageFormat = rec, sum, digest, format
	r.prestageAgent(ctx)
	return nil
}

// partitionImageFormat is the image format the profile gives the partition, Apptainer's SIF when it names none.
func partitionImageFormat(p v1alpha1.PlatformProfileSpec, partition string) agentapi.ImageFormat {
	for _, part := range p.Partitions {
		if part.Name == partition && part.ImageFormat != "" {
			return agentapi.ImageFormat(part.ImageFormat)
		}
	}
	return agentapi.ImageFormatSIF
}

// imageRecordPath is the record the agent's stager writes beside the image it staged for digest (internal/agent/stage): a sandbox's manifest, or
// a SIF's sidecar.
func imageRecordPath(staging, digest string, format agentapi.ImageFormat) string {
	p := staging + "/" + agentapi.StagedImagePath(digest, format)
	if format == agentapi.ImageFormatSandbox {
		return p + ".manifest"
	}
	return p + ".sha256"
}

// manifestEnd is a whole sandbox manifest's last line, its count of entries (internal/agent/stage, writeManifest).
var manifestEnd = regexp.MustCompile(`(^|\n)end\t[0-9]+\n$`)

// readImageRecord reads the record of the image staged for digest on the login node: its SHA-256, when it is whole and for digest (a manifest ends
// with its count; a SIF's sidecar names the digest), and ErrNoStagedImage otherwise. The sandbox's tree against its manifest is the agent's walk on
// the node, not this. Only a transport failure is another error.
func readImageRecord(ctx context.Context, run commandRunner, rec, digest string, format agentapi.ImageFormat) (string, error) {
	res, err := run.Run(ctx, slurm.Command{Line: "sha256sum " + rec})
	if err != nil {
		return "", fmt.Errorf("reading the image's record: %w", err)
	}
	if res.ExitCode != 0 || len(res.Stdout) < 64 || !sha256Re.MatchString(string(res.Stdout[:64])) {
		return "", ErrNoStagedImage
	}
	sum := string(res.Stdout[:64])
	n := "64"
	if format != agentapi.ImageFormatSandbox {
		n = "4096"
	}
	res, err = run.Run(ctx, slurm.Command{Line: "tail -c " + n + " " + rec})
	if err != nil {
		return "", fmt.Errorf("reading the image's record: %w", err)
	}
	if res.ExitCode != 0 {
		return "", ErrNoStagedImage
	}
	if format == agentapi.ImageFormatSandbox {
		if !manifestEnd.Match(res.Stdout) {
			return "", ErrNoStagedImage
		}
		return sum, nil
	}
	var side struct {
		Format string `json:"format"`
		Image  string `json:"image"`
	}
	if json.Unmarshal(res.Stdout, &side) != nil || side.Format != "hpcburst-sif/1" || side.Image != digest {
		return "", ErrNoStagedImage
	}
	return sum, nil
}

// imageRecordUnchanged is nil when the Provider's image record hashes as it did before the run: the double never replaced the image, nor did a
// refused staging remove its record (the science agent's rulings on #579).
func (r *liveRun) imageRecordUnchanged(ctx context.Context) error {
	return recordUnchanged(ctx, r.tr, r.imageRecord, r.imageDigest, r.imageFormat, r.imageRecordSum)
}

// recordUnchanged is imageRecordUnchanged over a runner: the record read again (readImageRecord) against its SHA-256 before the run.
func recordUnchanged(ctx context.Context, run commandRunner, rec, digest string, format agentapi.ImageFormat, before string) error {
	sum, err := readImageRecord(ctx, run, rec, digest, format)
	name := strings.TrimPrefix(digest, "sha256:")
	if len(name) > 12 {
		name = name[:12]
	}
	switch {
	case errors.Is(err, ErrNoStagedImage):
		return fmt.Errorf("the Provider's image record (the image of digest %s…) is gone or not whole after the run", name)
	case err != nil:
		return fmt.Errorf("the Provider's image record after the run: %w", err)
	case sum != before:
		return fmt.Errorf("the Provider's image record (the image of digest %s…) changed during the run: the image was staged again", name)
	}
	return nil
}
