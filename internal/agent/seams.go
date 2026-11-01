package agent

import (
	"context"
	"strings"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// API is the agent's side of the agent protocol: the three calls of
// agentapi's Transport. Client implements it over HTTPS with the pinned
// certificate; tests may implement it over an agentapi.Backend.
//
// Every error a call returns that the endpoint sent is an *agentapi.Error,
// so the loop can tell CodeNotRegistered (register again at once) from the
// codes it retries with backoff.
type API interface {
	Register(ctx context.Context, r agentapi.RegisterRequest) (agentapi.RegisterResponse, error)
	// Desired long-polls from since (agentapi.QuerySince).
	Desired(ctx context.Context, since string) (agentapi.DesiredState, error)
	Report(ctx context.Context, r agentapi.Report) error
}

// Engine is what the agent asks of one running engine worker, over the
// engine's local Unix socket (HBEP-7, What it runs: Checks). Package
// internal/agent/engine implements it for vLLM. Every method honours ctx:
// the agent cancels a check whose worker stopped running, and cancels
// warm-up requests still in flight once the desired state no longer asks
// for warm-up.
//
// The three check methods take the sampling of the verification generation's
// checks (agentapi.CheckSampling): the agent computes it once per generation
// from the staged generation_config.json, and no check request can go out
// without it.
type Engine interface {
	// Probe is the local health probe (GET /health); nil means healthy.
	Probe(ctx context.Context) error
	// Sample reads the engine's metrics into a health sample. The agent sets
	// Time, ProbeOK and ConsecutiveProbeFailures itself.
	Sample(ctx context.Context) (agentapi.Health, error)
	// Verify reads what verification needs from the engine: its version, its
	// resolved settings, its fingerprint and the chat template's overhead.
	// Files and ImageDigest are the agent's, not the engine's, and are left
	// empty. Only what the engine itself reports is set: a declared setting
	// is never copied in (HBEP-7, Risks: verification passes because it read
	// nothing). Its chat request is sampled as s says.
	Verify(ctx context.Context, s agentapi.Sampling) agentapi.VerifyResult
	// Qualify runs the qualification checks (HBEP-7, Instance start). Every
	// check request is sampled as s says.
	Qualify(ctx context.Context, req agentapi.QualifyRequest, s agentapi.Sampling) agentapi.QualifyResult
	// WarmUpRound runs warm-up round n, counted from 1. Every request is
	// sampled as s says.
	WarmUpRound(ctx context.Context, n int32, req agentapi.WarmUpRequest, s agentapi.Sampling) agentapi.WarmUpRound
}

// EngineFactory returns the Engine behind a worker's Unix socket, reached with
// the Instance's key (the engine's API key, HBEP-7: Keys), for the Instance's
// launch: a launch that declares a reasoning parser makes the engine answer
// with reasoning first (DeclaresReasoningParser).
type EngineFactory func(socketPath, key string, launch agentapi.Launch) Engine

// DeclaresReasoningParser reports whether a launch's command passes
// --reasoning-parser. The controller renders every flag as one "--name=value"
// argument; the underscore spelling is read too.
func DeclaresReasoningParser(command []string) bool {
	for _, a := range command {
		if strings.HasPrefix(a, "--reasoning-parser=") || strings.HasPrefix(a, "--reasoning_parser=") {
			return true
		}
	}
	return false
}

// CheckpointRequest is one staging of an Instance's checkpoint.
type CheckpointRequest struct {
	Repository string
	Revision   string
	// Files is the identity record's file list (Checks.Verify.Files). Empty
	// when the desired state carries none: the checkpoint is then treated as
	// pre-staged, and nothing is downloaded or rehashed.
	Files []agentapi.FileDigest
	// HubToken is the token for a gated checkpoint, held in memory only.
	HubToken string
	// Restage is set on a whole-Instance restart (HBEP-7, the alpha ladder,
	// rung 2): a present file whose rehash fails is moved aside and
	// downloaded again. At the first start a mismatch is only reported.
	Restage bool
	// RehashOnly rehashes the present files and changes nothing: no file is
	// downloaded or moved, and a missing one is absent. Verification after
	// the worker started uses it, so nothing is staged under a running
	// engine (PR #87 review, Opus B3).
	RehashOnly bool
	// LoadFormat is the launch's --load-format, "" when it names none
	// (vLLM's default, auto). It decides which listed weight files the engine
	// does not load and staging may skip (PR #87 review, Astra B5).
	LoadFormat string
	// Progress, when set, is called with the bytes done and the bytes to
	// download.
	Progress func(done, total int64)
}

// CheckpointResult is what staging found.
type CheckpointResult struct {
	// Dir is the checkpoint's absolute directory, bound read-only at
	// agentapi.ContainerModelDir.
	Dir string
	// Files holds one result per requested file, in request order: match,
	// mismatch (with the hash found), or absent for a file the engine does not
	// load and staging therefore skipped.
	Files []agentapi.FileResult
}

// ImageResult is a staged image.
type ImageResult struct {
	// Path is the image's absolute path.
	Path string
	// BuildPeakTmpBytes is the peak use of the temporary directory while the
	// image was built (agentapi.Staging.BuildPeakTmpBytes); 0 when no build
	// ran or none of its samples succeeded. Image sets it on a build that
	// failed too, beside its error.
	BuildPeakTmpBytes int64
	// Verified says the image's content was checked at this staging against
	// its integrity record: a SIF's SHA-256 sidecar, a sandbox's manifest,
	// both written in the same step as the pinned pull or build. A fresh pull
	// or build counts as checked. The agent reports the image digest only
	// for a verified image (PR #87 review, Astra B1, Opus B5).
	Verified bool
}

// Stager stages an Instance's image and checkpoint under the profile's staging
// directory (HBEP-7, What it runs: Staging). Package internal/agent/stage
// implements it.
type Stager interface {
	// Image makes sure the image digest is staged in the format, checks a
	// staged image against the integrity record written when it was pulled or
	// built, pulls or builds it again when the check fails, and returns its
	// absolute path (agentapi.StagedImagePath under the staging directory).
	// When it fails after a build ran, the result still carries the build's
	// BuildPeakTmpBytes.
	Image(ctx context.Context, image, digest string, format agentapi.ImageFormat) (ImageResult, error)
	// Checkpoint stages the checkpoint's missing files and rehashes the
	// present ones.
	Checkpoint(ctx context.Context, req CheckpointRequest) (CheckpointResult, error)
}

type imageBuildKey struct{}

// WithImageBuild returns ctx carrying fn, which Stager.Image calls when it
// starts a build (a pull or a sandbox build, on a cache miss) and before
// Apptainer runs, with the temporary directory it builds in: the agent then
// reports the build, so the last report before a build that runs out of the
// job's memory names it (HBEP-4; science-agent ruling, 1 Nov 2026, issue
// #125). Apptainer starts only once fn returns, and the agent's fn returns
// once that report has been delivered (Options.BuildReportTimeout), so the
// report precedes the build (PR #138 review, Astra B3); Image does not start
// the build when ctx has ended meanwhile.
func WithImageBuild(ctx context.Context, fn func(tmpDir string)) context.Context {
	return context.WithValue(ctx, imageBuildKey{}, fn)
}

// ImageBuildHook returns the function WithImageBuild put in ctx, or nil.
func ImageBuildHook(ctx context.Context) func(tmpDir string) {
	fn, _ := ctx.Value(imageBuildKey{}).(func(string))
	return fn
}
