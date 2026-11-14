package alpha

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/slurm/fakeslurm"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// operatorBuilds is an operator's agent-builds directory of an agent and frpc build, with a shim and the shipped agent's file beside it.
func operatorBuilds(t *testing.T) (builds, shimFile string) {
	t.Helper()
	dir := t.TempDir()
	builds = filepath.Join(dir, "agent-builds")
	if err := os.Mkdir(builds, 0o700); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{agentapi.AgentBuildName("x86_64"): "the shipped agent", agentapi.FrpcBuildName("x86_64"): "frpc"} {
		if err := os.WriteFile(filepath.Join(builds, name), []byte(body), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	shimFile = filepath.Join(dir, "shim")
	if err := os.WriteFile(shimFile, []byte("the shim"), 0o700); err != nil {
		t.Fatal(err)
	}
	return builds, shimFile
}

// The shim goes in the stack's own copy of the agent builds, and the operator's must hash as they did: a build changed, added or removed under it is
// a failure naming it (the science agent's condition 1 on the plan).
func TestTheOperatorsAgentBuildsMustHashAsBefore(t *testing.T) {
	builds, shimFile := operatorBuilds(t)
	s, err := loadTestShim(shimFile, filepath.Join(builds, agentapi.AgentBuildName("x86_64")), builds)
	if err != nil {
		t.Fatal(err)
	}
	if s.agentDigest != digestOf([]byte("the shipped agent")) || s.shimDigest != digestOf([]byte("the shim")) {
		t.Fatalf("digests %s, %s: want the files' own", s.agentDigest, s.shimDigest)
	}
	// The placement writes the stack's copy, never the operator's.
	state := t.TempDir()
	if err := os.Mkdir(filepath.Join(state, "agent-builds"), 0o700); err != nil {
		t.Fatal(err)
	}
	s.place(&stack.Host{T: t, State: state}, "x86_64")
	if got, _ := os.ReadFile(filepath.Join(state, "agent-builds", agentapi.AgentBuildName("x86_64"))); string(got) != "the shim" {
		t.Errorf("the stack's copy holds %q in hb-agent's place, want the shim", got)
	}
	if err := s.buildsUnchanged(); err != nil {
		t.Fatalf("the operator's builds read as changed after the placement: %v", err)
	}
	for name, edit := range map[string]func(){
		"the agent build written over": func() {
			_ = os.WriteFile(filepath.Join(builds, agentapi.AgentBuildName("x86_64")), []byte("the shim"), 0o700)
		},
		"a build added":   func() { _ = os.WriteFile(filepath.Join(builds, "hb-agent-aarch64"), []byte("x"), 0o700) },
		"a build removed": func() { _ = os.Remove(filepath.Join(builds, agentapi.FrpcBuildName("x86_64"))) },
	} {
		t.Run(name, func(t *testing.T) {
			b, sf := operatorBuilds(t)
			s, err := loadTestShim(sf, filepath.Join(b, agentapi.AgentBuildName("x86_64")), b)
			if err != nil {
				t.Fatal(err)
			}
			builds = b
			edit()
			if err := s.buildsUnchanged(); err == nil || !strings.Contains(err.Error(), "the operator's agent builds changed during the run") {
				t.Errorf("buildsUnchanged = %v, want the change named", err)
			}
		})
	}
}

// stagingSession is a fake login whose filesystem holds the staging root with the shipped agent's directory and a sibling of it.
func stagingSession(t *testing.T) (*fakeslurm.Platform, *fakeslurm.Session, string, string) {
	t.Helper()
	plat := fakeslurm.New(slurm.RealClock, fakeslurm.Config{Partitions: []fakeslurm.Partition{{Name: standInPartition}}})
	sess := plat.Login(fakeslurm.Login{User: "example-user", Env: map[string]string{"HOME": "/home/example-user", "SCRATCH": "/scratch/example-user"}})
	staging := "/scratch/example-user"
	mine, sibling := strings.Repeat("ab", 32), strings.Repeat("cd", 32)
	for _, d := range []string{mine, sibling} {
		if err := plat.WriteFile(staging+"/"+testAgentDir+"/"+d+"/hb-agent", []byte("an agent"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := plat.WriteFile(staging+"/agent/"+strings.Repeat("ef", 32)+"/hb-agent", []byte("the Provider's"), 0o700); err != nil {
		t.Fatal(err)
	}
	return plat, sess, staging, mine
}

// The staged shipped agent goes by its exact path, and nothing beside it: not another digest's directory, not the Provider's staging (the science
// agent's condition 2 on the plan).
func TestTheShippedAgentIsRemovedByItsExactPath(t *testing.T) {
	plat, sess, staging, mine := stagingSession(t)
	dir := staging + "/" + testAgentDir + "/" + mine
	if err := removeTestAgent(context.Background(), sess, dir); err != nil {
		t.Fatal(err)
	}
	files := plat.Files()
	if _, ok := files[dir+"/hb-agent"]; ok {
		t.Error("the shipped agent remains")
	}
	for _, keep := range []string{staging + "/" + testAgentDir + "/" + strings.Repeat("cd", 32) + "/hb-agent", staging + "/agent/" + strings.Repeat("ef", 32) + "/hb-agent"} {
		if _, ok := files[keep]; !ok {
			t.Errorf("%s was removed too", keep)
		}
	}
	// A path that is not a test-agent/<digest> directory is refused, and nothing is run.
	for _, bad := range []string{staging, staging + "/" + testAgentDir, staging + "/agent/" + strings.Repeat("ef", 32), staging + "/" + testAgentDir + "/x"} {
		if err := removeTestAgent(context.Background(), sess, bad); err == nil || !strings.Contains(err.Error(), "refusing to remove") {
			t.Errorf("removeTestAgent(%s) = %v, want a refusal", filepath.Base(bad), err)
		}
	}
}

// skipRemoval runs every command but rm, which it answers as done: a removal that did not happen.
type skipRemoval struct{ inner commandRunner }

func (s skipRemoval) Run(ctx context.Context, cmd slurm.Command) (slurm.Result, error) {
	if strings.HasPrefix(cmd.Line, "rm ") {
		return slurm.Result{}, nil
	}
	return s.inner.Run(ctx, cmd)
}

// The leftover check: a removal that reported done but left the agent is a failure naming the directory (the condition's mutant).
func TestTheLeftoverCheckNamesAShippedAgentThatRemains(t *testing.T) {
	_, sess, staging, mine := stagingSession(t)
	err := removeTestAgent(context.Background(), skipRemoval{sess}, staging+"/"+testAgentDir+"/"+mine)
	if err == nil || !strings.Contains(err.Error(), "remains after the run") || !strings.Contains(err.Error(), testAgentDir+"/"+mine[:12]) {
		t.Errorf("removeTestAgent with the removal skipped = %v, want the leftover named", err)
	}
	if strings.Contains(err.Error(), staging) {
		t.Errorf("the error names the staging root, a cluster path: %v", err)
	}
}
