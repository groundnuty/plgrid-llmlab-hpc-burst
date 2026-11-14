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
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	line, err := SrunLine(jobID)
	if err != nil {
		t.Fatalf("the test shim on the node: %v", err)
	}
	res, err := r.tr.Run(ctx, slurm.Command{Line: line, Stdin: []byte(ListScript(jobID)), Timeout: srunTimeout})
	if err != nil || res.ExitCode != 0 {
		t.Fatalf("the test shim on the node: the srun step that lists the job's processes failed (exit %d, transport error %v)", res.ExitCode, err != nil)
	}
	procs, err := ParseListing(res.Stdout)
	if err != nil {
		t.Fatalf("the test shim on the node: %v", err)
	}
	read := func(np NodeProc) string {
		script, err := ExeDigestScript(np)
		if err != nil {
			return ""
		}
		res, err := r.tr.Run(ctx, slurm.Command{Line: line, Stdin: []byte(script), Timeout: srunTimeout})
		if err != nil || res.ExitCode != 0 {
			return ""
		}
		d, _ := ParseExeDigest(res.Stdout)
		return d
	}
	if ag, ok := PickAgent(procs); ok {
		o.AgentRunning = read(ag)
	}
	if eng, err := PickEngine(procs, in.Spec.Launch.Command, MatchesShimEngine(s.shimDigest)); err == nil {
		o.EngineRunning = read(eng)
		if app, ok := ApptainerAbove(procs, eng); ok {
			o.ApptainerRunning = read(app)
		}
	}
	p.Logf("%s: through the test shim: the Allocation names the shim %t; on the node the agent process ran the shipped agent %t, the engine the shim %t, its Apptainer the shim %t",
		p.Row.Name, o.Allocation == s.shimDigest, o.AgentRunning == s.agentDigest, o.EngineRunning == s.shimDigest, o.ApptainerRunning == s.shimDigest)
}
