package alpha

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
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
	sess := plat.Login(fakeslurm.Login{User: "example-user", Env: map[string]string{"HOME": "/users/example-user", "SCRATCH": "/scratch/example-user"}})
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

// cannedNode answers the srun steps of readShimNode from a listing of its own and a digest for each PID, as the node would.
type cannedNode struct {
	listing string
	digests map[int]string
}

func (c cannedNode) Run(_ context.Context, cmd slurm.Command) (slurm.Result, error) {
	in := string(cmd.Stdin)
	if strings.Contains(in, "echo \"listed\"") || strings.Contains(in, "echo listed") {
		return slurm.Result{Stdout: []byte(c.listing)}, nil
	}
	for pid, d := range c.digests {
		if strings.Contains(in, "pid="+strconv.Itoa(pid)+"\n") {
			return slurm.Result{Stdout: []byte("digest " + d + "\n")}, nil
		}
	}
	return slurm.Result{Stdout: []byte("gone\n")}, nil
}

// The readings are the node's, each by the PID it picked: the agent process's executable (the shipped agent's), the engine's and the Apptainer's
// above it (the shim's). The digests the node gives here are none of the harness's own, so a reading copied from what the harness staged reads wrong.
func TestTheShimReadingsAreTheNodesOwn(t *testing.T) {
	x := strings.Repeat("ab", 32) // the shim the harness built
	command := []string{"vllm", "serve", "/models/m"}
	listing := "self 7\n" + strings.Join([]string{
		"proc 100 1 500 1 0 o " + b64("/scratch/u/agent/"+x+"/hb-agent", "/scratch/u/alloc/t-1"),
		"proc 101 1 501 1 0 o " + b64("/scratch/u/agent/"+x+"/hb-agent", "relay", "control", "--dir", "/scratch/u/alloc/t-1"),
		"proc 110 100 502 0 1 j " + b64("/scratch/u/test-agent/y/shim-ab/apptainer", "exec", "image", "vllm", "serve", "/models/m"),
		"proc 120 110 503 0 1 j " + b64("/scratch/u/agent/"+x+"/hb-agent", "serve", "/models/m"),
	}, "\n") + "\nlisted\n"
	node := cannedNode{listing: listing, digests: map[int]string{100: strings.Repeat("01", 32), 110: strings.Repeat("02", 32), 120: strings.Repeat("03", 32)}}
	agent, engine, app, err := readShimNode(context.Background(), node, "4242", command, x)
	if err != nil {
		t.Fatal(err)
	}
	if agent != strings.Repeat("01", 32) || app != strings.Repeat("02", 32) || engine != strings.Repeat("03", 32) {
		t.Errorf("readings agent %s, engine %s, apptainer %s; want the node's digests of PIDs 100, 120 and 110", agent, engine, app)
	}
	// A node whose engine cannot be told (no process runs the shim's file) reads it as not read, and the Apptainer with it.
	node.listing = strings.Replace(listing, b64("/scratch/u/agent/"+x+"/hb-agent", "serve", "/models/m"), b64("python3", "-m", "vllm"), 1)
	if _, engine, app, err := readShimNode(context.Background(), node, "4242", command, x); err != nil || engine != "" || app != "" {
		t.Errorf("no shim engine: engine %q, apptainer %q, %v; want both unread", engine, app, err)
	}
	// A listing that cannot be taken is an error.
	if _, _, _, err := readShimNode(context.Background(), cannedNode{listing: "garbage"}, "4242", command, x); err == nil {
		t.Error("a listing that cannot be parsed read as a node")
	}
}

// The image precondition and its check after the run, over a fake login holding the stager's records (the science agent's rulings on #579): a whole
// manifest, or a SIF sidecar that names the digest, is read with its SHA-256; an absent, cut or foreign record is ErrNoStagedImage, NOT RUN; a record
// changed or gone after the run is a failure naming the image, never its path.
func TestTheImagePreconditionReadsTheRecord(t *testing.T) {
	plat, sess, staging, _ := stagingSession(t)
	digest := "sha256:" + strings.Repeat("12", 32)
	manifest := "hpcburst-sandbox-manifest/1 " + digest + "\nD\t\".\"\nF\t2\t" + strings.Repeat("ab", 32) + "\t\"x\"\nend\t2\n"
	sb := imageRecordPath(staging, digest, agentapi.ImageFormatSandbox)
	if err := plat.WriteFile(sb, []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	sum, err := readImageRecord(context.Background(), sess, sb, digest, agentapi.ImageFormatSandbox)
	if err != nil || sum != digestOf([]byte(manifest)) {
		t.Fatalf("a whole manifest: %q, %v; want its SHA-256", sum, err)
	}
	if err := recordUnchanged(context.Background(), sess, sb, digest, agentapi.ImageFormatSandbox, sum); err != nil {
		t.Errorf("an unchanged record: %v", err)
	}
	// A SIF's sidecar names its digest.
	sif := imageRecordPath(staging, digest, agentapi.ImageFormatSIF)
	if err := plat.WriteFile(sif, []byte(`{"format":"hpcburst-sif/1","image":"`+digest+`","size":1,"sha256":"`+strings.Repeat("cd", 32)+`"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readImageRecord(context.Background(), sess, sif, digest, agentapi.ImageFormatSIF); err != nil {
		t.Errorf("a SIF sidecar for the digest: %v", err)
	}
	other := "sha256:" + strings.Repeat("34", 32)
	if _, err := readImageRecord(context.Background(), sess, imageRecordPath(staging, other, agentapi.ImageFormatSIF), other, agentapi.ImageFormatSIF); !errors.Is(err, ErrNoStagedImage) {
		t.Errorf("no record: %v, want ErrNoStagedImage", err)
	}
	if _, err := readImageRecord(context.Background(), sess, sif, other, agentapi.ImageFormatSIF); !errors.Is(err, ErrNoStagedImage) {
		t.Errorf("a sidecar for another digest: %v, want ErrNoStagedImage", err)
	}
	// A manifest cut before its count is not whole.
	if err := plat.WriteFile(sb, []byte(strings.TrimSuffix(manifest, "end\t2\n")), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readImageRecord(context.Background(), sess, sb, digest, agentapi.ImageFormatSandbox); !errors.Is(err, ErrNoStagedImage) {
		t.Errorf("a cut manifest: %v, want ErrNoStagedImage", err)
	}
	// After the run: a record changed (the image staged again), or gone (a refused build removes it), is a failure naming the image's digest.
	if err := plat.WriteFile(sb, []byte(strings.Replace(manifest, "end\t2", "F\t1\t"+strings.Repeat("ef", 32)+"\t\"y\"\nend\t3", 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := recordUnchanged(context.Background(), sess, sb, digest, agentapi.ImageFormatSandbox, sum); err == nil || !strings.Contains(err.Error(), "changed during the run") || !strings.Contains(err.Error(), strings.Repeat("12", 6)) || strings.Contains(err.Error(), staging) {
		t.Errorf("a changed record: %v; want it named by its digest, no path", err)
	}
	if _, err := sess.Run(context.Background(), slurm.Command{Line: "rm -f " + sb}); err != nil {
		t.Fatal(err)
	}
	if err := recordUnchanged(context.Background(), sess, sb, digest, agentapi.ImageFormatSandbox, sum); err == nil || !strings.Contains(err.Error(), "is gone or not whole after the run") {
		t.Errorf("a removed record: %v; want it named", err)
	}
}

// The NOT RUN for an image not staged: NotRun, never Invalid, and its marker's fixed words with no path and no digest.
func TestAnUnstagedImageIsNotRun(t *testing.T) {
	row := BetaBulkRow()
	if !NotRun(row, ErrNoStagedImage) || Invalid(row, ErrNoStagedImage) {
		t.Fatal("ErrNoStagedImage is not a NOT RUN, or is also INVALID")
	}
	note := NotRunNote(row, 0, fmt.Errorf("setup: %w", ErrNoStagedImage))
	if want := row.Name + ": " + noStagedImageWords + "; stage it first with the stage row; no job was submitted\n"; note != want {
		t.Errorf("NotRunNote = %q, want %q", note, want)
	}
}
