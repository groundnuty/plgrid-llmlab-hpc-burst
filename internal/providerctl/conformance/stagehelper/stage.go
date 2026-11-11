package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync/atomic"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent/stage"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/hubfiles"
)

// inputs are what agent.json does not hold: the image, the checkpoint and the
// launch's load format, compiled in (main.go).
type inputs struct {
	image, imageDigest, checkpointRepo, checkpointRev, loadFormat string
}

// missing names the compiled-in values that are empty, by the variable a build
// sets.
func (in inputs) missing() []string {
	var out []string
	for _, v := range []struct{ name, value string }{
		{"image", in.image}, {"imageDigest", in.imageDigest},
		{"checkpointRepo", in.checkpointRepo}, {"checkpointRev", in.checkpointRev},
		{"loadFormat", in.loadFormat},
	} {
		if v.value == "" {
			out = append(out, v.name)
		}
	}
	return out
}

// The steps a failure is attributed to. They are the only words an error line
// or a signalled line holds besides "outcome=error step=" and
// "outcome=signalled step=".
const (
	stepConfig     = "config"
	stepImage      = "image"
	stepFiles      = "files"
	stepCheckpoint = "checkpoint"
)

// result is what a staging came to.
type result struct {
	// step is the step that failed, empty when staging succeeded.
	step string
	// signalled says the step failed once the signal had cancelled the staging
	// (runInputs): the job's time limit or a cancel, not the step's own error.
	signalled         bool
	imageSeconds      int64
	imageBuilt        bool
	checkpointSeconds int64
	files, mismatched int
}

// line is the one line of the result file: fixed words and numbers only.
func (r result) line() string {
	switch {
	case r.signalled:
		return "outcome=signalled step=" + r.step
	case r.step != "":
		return "outcome=error step=" + r.step
	}
	return fmt.Sprintf("outcome=ok image_seconds=%d image_built=%t checkpoint_seconds=%d files=%d mismatched=%d",
		r.imageSeconds, r.imageBuilt, r.checkpointSeconds, r.files, r.mismatched)
}

// failed is a result that names the step, after the error went to stderr.
func failed(stderr io.Writer, step string, err error) result {
	fmt.Fprintf(stderr, "stagehelper: %s: %v\n", step, err)
	return result{step: step}
}

// stageAll stages the image and the checkpoint for the Allocation in dir, and
// says what came of it. Every error goes to stderr and out of the result.
func stageAll(ctx context.Context, dir string, in inputs, getenv func(string) string, stderr io.Writer) result {
	if m := in.missing(); len(m) > 0 {
		return failed(stderr, stepConfig, fmt.Errorf("compiled-in values not set: %v", m))
	}
	job, err := agent.LoadJob(dir, getenv)
	if err != nil {
		return failed(stderr, stepConfig, err)
	}
	runDir, err := agent.MakeRunDir(job.TmpDir)
	if err != nil {
		return failed(stderr, stepConfig, err)
	}
	defer os.RemoveAll(runDir)

	apptainer, err := lookTool(getenv("HPCBURST_APPTAINER"), "apptainer")
	if err != nil {
		return failed(stderr, stepConfig, fmt.Errorf("no apptainer on this node: %w", err))
	}
	// The agent's Stager configuration, field for field (cmd/hb-agent/main.go,
	// run): a field it gains must be added here, which TestStagerConfigMirrorsTheAgents
	// checks. The expressions are written as the agent writes them, over the same
	// names, so that the check compares their text.
	stager, err := stage.New(stage.Config{
		StagingDir:  job.Config.StagingDir,
		Apptainer:   apptainer,
		TmpDir:      runDir,
		HubEndpoint: hubEndpoint(job.Config, getenv),
		BaseEnv:     os.Environ(),
		Log:         stderr,
	})
	if err != nil {
		return failed(stderr, stepConfig, err)
	}

	res := result{}
	// The image, in the sandbox format: the partition under test (Athena's
	// A100) cannot run a SIF. The build hook fires only when Image builds, on a
	// cache miss and before Apptainer runs: it is what says a build ran, as the
	// peak of the temporary directory is 0 when no sample succeeded.
	var built atomic.Bool
	buildCtx := agent.WithImageBuild(ctx, func(string) { built.Store(true) })
	start := time.Now()
	img, err := stager.Image(buildCtx, in.image, in.imageDigest, agentapi.ImageFormatSandbox)
	res.imageSeconds, res.imageBuilt = seconds(time.Since(start)), built.Load()
	if err != nil {
		return failed(stderr, stepImage, err)
	}
	if !img.Verified {
		return failed(stderr, stepImage, errors.New("the staged image was not verified"))
	}

	// The identity record's file list as the lifecycle controller makes it
	// (internal/lifecycle HubSource.Files: the Hub client's listing, then
	// hubfiles.Inputs; internal/lifecycle/desired.go: the record's Path and
	// SHA256 become agentapi.FileDigest). internal/lifecycle is not imported: its
	// record reader is unexported, and the package is the controller's, with
	// its Kubernetes client. Files sorts the list bytewise by path, as the
	// record's canonical form does.
	hub, err := hubfiles.New(hubfiles.Config{BaseURL: hubEndpoint(job.Config, getenv)})
	if err != nil {
		return failed(stderr, stepFiles, err)
	}
	entries, err := hub.Files(ctx, in.checkpointRepo, in.checkpointRev)
	if err != nil {
		return failed(stderr, stepFiles, err)
	}
	var files []agentapi.FileDigest
	for _, f := range hubfiles.Inputs(entries) {
		files = append(files, agentapi.FileDigest{Path: f.Path, SHA256: f.SHA256})
	}

	// The agent's first-start request (internal/agent/converge.go,
	// startStaging): no Restage, so a present file that does not match is
	// reported and left; no RehashOnly; no token, for a public checkpoint.
	start = time.Now()
	ck, err := stager.Checkpoint(ctx, agent.CheckpointRequest{
		Repository: in.checkpointRepo,
		Revision:   in.checkpointRev,
		Files:      files,
		LoadFormat: in.loadFormat,
	})
	res.checkpointSeconds = seconds(time.Since(start))
	if err != nil {
		return failed(stderr, stepCheckpoint, err)
	}
	res.files = len(ck.Files)
	for _, f := range ck.Files {
		if f.Outcome == agentapi.FileMismatch {
			res.mismatched++
		}
	}
	return res
}

// seconds is d in whole seconds, rounded to the nearest.
func seconds(d time.Duration) int64 { return int64(d.Round(time.Second) / time.Second) }

// lookTool returns the tool's path: the configured one, else name on PATH. It
// is a copy of cmd/hb-agent's lookTool, which is in package main and cannot be
// imported; the copy is checked against the agent's source
// (TestCopiedFunctionsMatchTheAgents).
func lookTool(configured, name string) (string, error) {
	if configured != "" {
		return configured, nil
	}
	return exec.LookPath(name)
}

// hubEndpoint is the Hub origin the agent stages from: agent.json's, which the
// controller writes from its own HF_ENDPOINT, or, when agent.json names none,
// this job's HF_ENDPOINT. Empty means the public Hub. It is a copy of
// cmd/hb-agent's hubEndpoint, which is in package main and cannot be imported;
// the copy is checked against the agent's source
// (TestCopiedFunctionsMatchTheAgents). The same origin lists the checkpoint
// (hubfiles) and downloads it (stage), so the file list and the files come from
// one Hub.
func hubEndpoint(cfg agentapi.AgentConfig, getenv func(string) string) string {
	if cfg.HubEndpoint != "" {
		return cfg.HubEndpoint
	}
	return getenv("HF_ENDPOINT")
}
