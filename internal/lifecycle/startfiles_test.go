package lifecycle

// The start instruction carries the checkpoint's full file list (HBEP-7, The
// allocation agent, What it runs; Instance start: in starting the agent stages
// the missing files from the Hub and rehashes the ones present before it starts
// the worker). DesiredInstance has no file list of its own; Checks.Verify.Files
// is it, so the first desired state that runs the Instance, and every one after
// a rise of the Instance generation, must carry the identity record's list. A
// gated checkpoint also gets its Hub token.

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

func TestStartInstructionCarriesTheFullFileListAndTheHubToken(t *testing.T) {
	rg := newRig(t)
	tokenFile := filepath.Join(t.TempDir(), "hub-token")
	if err := os.WriteFile(tokenFile, []byte("hf_example_token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	md := mdSpec("md")
	md.Spec.Source.Served.Checkpoint.HubTokenRef = &v1alpha1.CredentialsReference{File: tokenFile}
	rg.create(md)
	rg.until(10*time.Minute, 10*time.Second, "one ready Instance", func() bool { return rg.readyCount("md") == 1 })
	in := rg.instances("md")[0]
	ag := rg.agentOf(in.Spec.AllocationRef.Name).agent

	var first *agentapi.DesiredState
	for _, d := range ag.Seen() {
		if d.Action == agentapi.ActionRun {
			dd := d
			first = &dd
			break
		}
	}
	if first == nil || first.Instance == nil {
		t.Fatal("the agent never saw a desired state that runs the Instance")
	}
	// rigFiles, the identity record's list: config.json and model.safetensors.
	want := map[string]string{"config.json": rigFiles[0].SHA256, "model.safetensors": rigFiles[1].SHA256}
	c := first.Instance.Checks
	if c == nil || c.Verify == nil || c.VerificationGeneration != 1 || len(c.Verify.Files) != len(want) {
		t.Fatalf("the start instruction's checks %+v, want verification generation 1 with the %d files", c, len(want))
	}
	for _, f := range c.Verify.Files {
		if want[f.Path] != f.SHA256 {
			t.Errorf("file %s sha256 %s, want %s", f.Path, f.SHA256, want[f.Path])
		}
	}
	if first.Instance.HubToken != "hf_example_token" {
		t.Errorf("the start instruction's Hub token is %q, want the token file's content", first.Instance.HubToken)
	}

	// A whole-Instance restart restages: its first desired state carries the
	// full list again, under the next verification generation.
	ag.CrashWorker(agentapi.WorkerName(0), 1)
	rg.until(5*time.Minute, 10*time.Second, "a worker restart", func() bool { return restartGeneration(rg.instance(in.Name), "worker-0") == 1 })
	rg.until(5*time.Minute, 10*time.Second, "ready again", func() bool { return rg.readyCount("md") == 1 })
	rg.run(time.Minute, 10*time.Second)
	ag.CrashWorker(agentapi.WorkerName(0), 1)
	rg.until(5*time.Minute, 10*time.Second, "a second worker restart", func() bool { return restartGeneration(rg.instance(in.Name), "worker-0") == 2 })
	rg.until(5*time.Minute, 10*time.Second, "ready again", func() bool { return rg.readyCount("md") == 1 })
	rg.run(time.Minute, 10*time.Second)
	ag.CrashWorker(agentapi.WorkerName(0), 1) // attemptsPerRung 2: the third crash takes the whole-Instance rung
	rg.until(5*time.Minute, 10*time.Second, "an Instance restart", func() bool { return restartGeneration(rg.instance(in.Name), v1alpha1.PartInstance) == 1 })
	rg.run(10*time.Second, 10*time.Second) // the agent's next poll sees the raised generation
	var afterRise *agentapi.DesiredState
	for _, d := range ag.Seen() {
		if d.Instance != nil && d.Instance.Generation == 1 {
			dd := d
			afterRise = &dd
			break
		}
	}
	if afterRise == nil {
		t.Fatal("the agent never saw the raised Instance generation")
	}
	c = afterRise.Instance.Checks
	if c == nil || c.Verify == nil || len(c.Verify.Files) != len(want) || c.VerificationGeneration != 4 {
		t.Fatalf("after the Instance generation rose the checks are %+v, want verification generation 4 (1 + two worker restarts + one Instance restart) with the %d files", c, len(want))
	}
}
