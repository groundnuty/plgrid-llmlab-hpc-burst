package agent

import (
	"context"

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
	// nothing).
	Verify(ctx context.Context) agentapi.VerifyResult
	// Qualify runs the qualification checks (HBEP-7, Instance start).
	Qualify(ctx context.Context, req agentapi.QualifyRequest) agentapi.QualifyResult
	// WarmUpRound runs warm-up round n, counted from 1.
	WarmUpRound(ctx context.Context, n int32, req agentapi.WarmUpRequest) agentapi.WarmUpRound
}

// EngineFactory returns the Engine behind a worker's Unix socket, reached with
// the Instance's key (the engine's API key, HBEP-7: Keys).
type EngineFactory func(socketPath, key string) Engine

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

// Stager stages an Instance's image and checkpoint under the profile's staging
// directory (HBEP-7, What it runs: Staging). Package internal/agent/stage
// implements it.
type Stager interface {
	// Image makes sure the image digest is staged in the format, and returns
	// its absolute path (agentapi.StagedImagePath under the staging
	// directory).
	Image(ctx context.Context, image, digest string, format agentapi.ImageFormat) (string, error)
	// Checkpoint stages the checkpoint's missing files and rehashes the
	// present ones.
	Checkpoint(ctx context.Context, req CheckpointRequest) (CheckpointResult, error)
}
