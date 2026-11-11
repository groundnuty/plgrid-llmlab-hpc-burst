package main

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/agent"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The point of the helper: after it ran, the real agent's stager, configured
// as the agent configures it over the same Allocation directory and asked for
// what the agent asks for, finds the image and the checkpoint staged and does
// no work. "No work" is read from the counters of two doubles that the helper
// does not write to: the fake Apptainer's own log of its invocations, and the
// Hub double's own count of file downloads. The helper's result line is not
// read for it.
//
// The request the agent would send is written here: the sandbox image, and the
// checkpoint with the file list from expectedFiles (the double's own data
// hashed in this test) and the load format "" of a launch that names none (the
// helper is built with "auto", which the stager treats alike).

// warmReport is what a fresh stager did, in counters: the build hook, the fake
// Apptainer's invocations and the Hub double's downloads.
type warmReport struct {
	img        agent.ImageResult
	hookBuilt  bool
	ck         agent.CheckpointResult
	buildsSeen int // pull or build invocations the fake Apptainer recorded
	downloaded []string
}

// askAsTheAgentDoes asks a fresh stager for the image and then the checkpoint,
// as the agent's first start does, and reports what that took.
func askAsTheAgentDoes(t *testing.T, fx *fixture, hub *stack.Hub, files []agentapi.FileDigest) warmReport {
	t.Helper()
	s, built, ctx := agentLikeStager(t, fx)
	callsBefore := len(apptainerCalls(t, fx))
	img, err := s.Image(ctx, testImage, testDigest, agentapi.ImageFormatSandbox)
	if err != nil {
		t.Fatalf("the fresh stager's Image: %v", err)
	}
	callsAfter := len(apptainerCalls(t, fx))
	// The baseline is taken here, after the helper's run and the listing that
	// built the expected files: the Hub client lists the checkpoint by
	// downloading its files that are not in LFS, so the count rises during
	// either of them, and only a rise from this point is the fresh stager's.
	downloadsBefore := len(hub.Resolved())
	ck, err := s.Checkpoint(ctx, agent.CheckpointRequest{
		Repository: stack.CheckpointRepo,
		Revision:   stack.CheckpointRev,
		Files:      files,
		LoadFormat: "",
	})
	if err != nil {
		t.Fatalf("the fresh stager's Checkpoint: %v", err)
	}
	r := warmReport{img: img, hookBuilt: built.Load(), ck: ck, buildsSeen: callsAfter - callsBefore}
	r.downloaded = hub.Resolved()[downloadsBefore:]
	// Both stagers address the checkpoint and the image by the layout, which
	// this test spells out from package agentapi, not from either stager.
	wantImage := filepath.Join(fx.staging, agentapi.StagedImagePath(testDigest, agentapi.ImageFormatSandbox))
	if img.Path != wantImage {
		t.Errorf("the image is at %q, want %q", img.Path, wantImage)
	}
	wantDir := filepath.Join(fx.staging, agentapi.StagedCheckpointDir(stack.CheckpointRepo, stack.CheckpointRev))
	if ck.Dir != wantDir {
		t.Errorf("the checkpoint is at %q, want %q", ck.Dir, wantDir)
	}
	return r
}

// The staged files on disk are the expected bytes, hashed here.
func checkStagedFiles(t *testing.T, dir string, files []agentapi.FileDigest) {
	t.Helper()
	for _, f := range files {
		b, err := os.ReadFile(filepath.Join(dir, f.Path))
		if err != nil {
			t.Errorf("%s: %v", f.Path, err)
			continue
		}
		sum := sha256.Sum256(b)
		if got := hex.EncodeToString(sum[:]); got != f.SHA256 {
			t.Errorf("%s on disk has SHA-256 %s, want %s", f.Path, got, f.SHA256)
		}
	}
}

func TestStagerFindsEverythingWarmAfterTheHelper(t *testing.T) {
	hub := stack.StartHub(t)
	fx := newFixture(t, hub.URL)
	files := expectedFiles(t, hub.URL)

	if code, errs := fx.run(t, goodInputs()); code != 0 {
		t.Fatalf("the helper exited %d: %s", code, errs)
	}
	// The helper did its work, which the fake Apptainer's own log shows: one
	// sandbox build, and nothing else.
	if calls := apptainerCalls(t, fx); !slices.Equal(calls, []string{"build"}) {
		t.Fatalf("the fake Apptainer recorded %v for the helper's run, want one build", calls)
	}

	r := askAsTheAgentDoes(t, fx, hub, files)

	// The image: not built again.
	if r.img.BuildPeakTmpBytes != 0 || !r.img.Verified {
		t.Errorf("the fresh stager's image: BuildPeakTmpBytes %d, Verified %v, want 0 and true", r.img.BuildPeakTmpBytes, r.img.Verified)
	}
	if r.hookBuilt || r.buildsSeen != 0 {
		t.Errorf("the fresh stager built the image again: build hook %v, %d invocations of the fake Apptainer", r.hookBuilt, r.buildsSeen)
	}
	// The checkpoint: nothing downloaded, every file a match.
	if len(r.downloaded) != 0 {
		t.Errorf("the fresh stager downloaded %v from the Hub double", r.downloaded)
	}
	if len(r.ck.Files) != len(files) {
		t.Fatalf("the fresh stager reported %d files, want %d", len(r.ck.Files), len(files))
	}
	for i, f := range r.ck.Files {
		if f.Path != files[i].Path || f.Outcome != agentapi.FileMatch {
			t.Errorf("file %d: %+v, want a match of %s", i, f, files[i].Path)
		}
	}
	checkStagedFiles(t, r.ck.Dir, files)
}

// The control: the same fresh stager, the same request, the same doubles, but
// the helper not run first. It builds and it downloads, so the counters the
// proof above reads tell staged from not staged.
func TestStagerDoesTheWorkWithoutTheHelper(t *testing.T) {
	hub := stack.StartHub(t)
	fx := newFixture(t, hub.URL)
	files := expectedFiles(t, hub.URL)

	r := askAsTheAgentDoes(t, fx, hub, files)

	if !r.hookBuilt || r.buildsSeen != 1 {
		t.Errorf("a cold stager: build hook %v, %d invocations of the fake Apptainer, want true and 1", r.hookBuilt, r.buildsSeen)
	}
	if r.img.BuildPeakTmpBytes == 0 {
		t.Errorf("a cold stager's image reports no build peak")
	}
	var want []string
	for _, f := range files {
		want = append(want, f.Path)
	}
	got := slices.Clone(r.downloaded)
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("a cold stager downloaded %v, want every file %v", got, want)
	}
	for _, f := range r.ck.Files {
		if f.Outcome != agentapi.FileMatch {
			t.Errorf("after its downloads %+v is not a match", f)
		}
	}
	checkStagedFiles(t, r.ck.Dir, files)
}
