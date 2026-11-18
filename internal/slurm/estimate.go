package slurm

// Estimate (beta; HBEP-4, Core operations, Estimate; #661). One sbatch
// --test-only with an Allocation's submission parameters, read-only, which
// answers when the job would start if it were submitted now.

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// maxEpoch is the largest epoch second read as a time: the last second of year
// 9999. A larger number is no start Slurm would print.
const maxEpoch = 253402300799

// startEstimateGate says why Estimate does not run on a platform, or "" when it
// runs. The reading of sbatch --test-only's start is the PlatformProfile's
// (startEstimateTime), set where a probe decided it; no platform's name is read
// anywhere in this package (HBEP-4, Core operations, Estimate: Where the decision
// lives; `@hpc-burst-science-agent`, 17 Nov 2026, #661). Absent is undecided.
func startEstimateGate(p provider.Profile) string {
	if p.Spec.StartEstimateTime != v1alpha1.StartEstimateEpochSeconds {
		return provider.EstimateUndecided
	}
	return ""
}

// Estimate implements provider.Batch: one sbatch --test-only, read-only. On a
// platform whose profile does not decide the reading it returns unknown with that
// reason and runs no command at all. Otherwise the request is checked as
// Submit's is, before any call, and then the line runs once, bounded by
// provider.EstimateTimeout. A command that fails, times out or does not run is
// an error; an answer that is not a whole number of epoch seconds is an unknown
// result, never a time and never zero.
//
// It writes nothing on the login node, reads no job and leaves the cached
// listing as it is: no sbatch --test-only changes what a listing shows, so it
// does not mark it stale, as Submit must.
func (b *Batch) Estimate(ctx context.Context, req provider.SubmitRequest) (provider.EstimateResult, error) {
	profile, settings := b.config()
	if why := startEstimateGate(profile); why != "" {
		return provider.EstimateResult{Unknown: why, ReadAt: b.clock.Now()}, nil
	}
	req.Hold = false // an estimate is never held, whatever the caller's request says
	s, err := checkRequest(req, profile, settings)
	if err != nil {
		return provider.EstimateResult{}, err
	}
	s.tag = estimateJobName // the tag was checked above; the line never carries it (estimateJobName)
	ctx, cancel := context.WithTimeout(ctx, provider.EstimateTimeout)
	defer cancel()
	paths, err := b.Paths(ctx)
	if err != nil {
		return provider.EstimateResult{}, err
	}
	s.allocDir = paths.Staging + "/" + agentapi.AllocationDir(s.tag)
	s.agentPath = paths.Staging + "/" + agentapi.StagedAgentPath(s.agentDigest)
	line, err := estimateLine(s)
	if err != nil {
		return provider.EstimateResult{}, err
	}
	res, runErr := b.runner.Run(ctx, Command{Line: line, Timeout: provider.EstimateTimeout})
	if runErr != nil {
		// A deadline of this call, its own bound or the caller's, that ran out is a time-out, whatever shape the runner reports it
		// in: the real runner returns ErrTimeout only for its own command time-out, and for a deadline that fires first, as this
		// call's does, a transport error whose text names the context's error but does not wrap it (internal/slurm/sshrun,
		// runClient). Without this a hang would read as a transport failure.
		if errors.Is(ctx.Err(), context.DeadlineExceeded) && !errors.Is(runErr, ErrTimeout) {
			return provider.EstimateResult{}, fmt.Errorf("sbatch --test-only: %w: it did not answer before its deadline", ErrTimeout)
		}
		return provider.EstimateResult{}, fmt.Errorf("sbatch --test-only: %w", runErr)
	}
	if res.ExitCode != 0 {
		return provider.EstimateResult{}, fmt.Errorf("sbatch --test-only: exit %d: %s", res.ExitCode, slurmMessage(string(res.Stderr)))
	}
	return parseEstimate(string(res.Stderr), b.clock.Now()), nil
}

// parseEstimate reads sbatch --test-only's reply, "sbatch: Job <n> to start at
// <T> using ...", from standard error. T must be all ASCII digits: epoch seconds,
// as SLURM_TIME_FORMAT=%s prints it. No text time is ever parsed, so the local
// text form, a fraction, a sign, "N/A" and an empty field are all unknown. So is
// zero, which Slurm never prints for a start and which would read as the epoch
// itself. The reasons are fixed texts: the reply names a host, nodes and a job.
func parseEstimate(stderr string, readAt time.Time) provider.EstimateResult {
	unknown := func(why string) provider.EstimateResult {
		return provider.EstimateResult{Unknown: why, ReadAt: readAt}
	}
	for _, line := range strings.Split(stderr, "\n") {
		_, after, found := strings.Cut(line, " to start at ")
		if !found || !strings.HasPrefix(strings.TrimSpace(line), "sbatch: Job ") {
			continue
		}
		fields := strings.Fields(after)
		if len(fields) == 0 || !allDigits(fields[0]) {
			return unknown(provider.EstimateUnreadable)
		}
		n, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil || n <= 0 || n > maxEpoch {
			return unknown(provider.EstimateUnreadable)
		}
		t := time.Unix(n, 0).UTC()
		return provider.EstimateResult{Start: &t, ReadAt: readAt}
	}
	return unknown(provider.EstimateNoAnswer)
}

// allDigits reports whether s is one or more ASCII digits and nothing else.
func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
