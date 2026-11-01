package fake

import (
	"maps"
	"slices"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// The real server decodes JSON, so the real Backend always owns what it is
// given. These doubles call each other directly, so each side copies what it
// stores and what it hands out: a later change by the caller never rewrites
// history that a test is about to read.

// clonePtr returns a pointer to a copy of *p made by f, or nil.
func clonePtr[T any](p *T, f func(T) T) *T {
	if p == nil {
		return nil
	}
	v := f(*p)
	return &v
}

func same[T any](v T) T { return v }

func cloneRegister(r agentapi.RegisterRequest) agentapi.RegisterRequest {
	r.GPUs = slices.Clone(r.GPUs)
	return r
}

func cloneDesired(d agentapi.DesiredState) agentapi.DesiredState {
	d.Instance = clonePtr(d.Instance, func(i agentapi.DesiredInstance) agentapi.DesiredInstance {
		i.Launch = cloneLaunch(i.Launch)
		i.Workers = slices.Clone(i.Workers)
		i.Checks = clonePtr(i.Checks, cloneChecksRequest)
		return i
	})
	return d
}

func cloneLaunch(l agentapi.Launch) agentapi.Launch {
	l.Command = slices.Clone(l.Command)
	l.Env = slices.Clone(l.Env)
	l.ChatTemplate = clonePtr(l.ChatTemplate, same[string])
	return l
}

func cloneChecksRequest(c agentapi.Checks) agentapi.Checks {
	c.Verify = clonePtr(c.Verify, func(v agentapi.VerifyRequest) agentapi.VerifyRequest {
		v.Files = slices.Clone(v.Files)
		return v
	})
	c.Qualify = clonePtr(c.Qualify, same[agentapi.QualifyRequest])
	c.WarmUp = clonePtr(c.WarmUp, same[agentapi.WarmUpRequest])
	return c
}

func cloneReport(r agentapi.Report) agentapi.Report {
	r.Instance = clonePtr(r.Instance, func(i agentapi.InstanceReport) agentapi.InstanceReport {
		i.Staging = clonePtr(i.Staging, same[agentapi.Staging])
		if i.Workers != nil {
			workers := make([]agentapi.WorkerReport, len(i.Workers))
			for n, w := range i.Workers {
				workers[n] = cloneWorkerReport(w)
			}
			i.Workers = workers
		}
		i.Checks = clonePtr(i.Checks, cloneCheckResults)
		return i
	})
	return r
}

func cloneWorkerReport(w agentapi.WorkerReport) agentapi.WorkerReport {
	w.StartTime = clonePtr(w.StartTime, same[time.Time])
	w.ExitCode = clonePtr(w.ExitCode, same[int])
	w.Health = clonePtr(w.Health, func(h agentapi.Health) agentapi.Health {
		h.MetricsReadAt = clonePtr(h.MetricsReadAt, same[time.Time])
		h.RunningRequests = clonePtr(h.RunningRequests, same[int64])
		h.WaitingRequests = clonePtr(h.WaitingRequests, same[int64])
		h.GeneratedTokens = clonePtr(h.GeneratedTokens, same[int64])
		h.KVCacheUsage = clonePtr(h.KVCacheUsage, same[float64])
		h.Preemptions = clonePtr(h.Preemptions, same[int64])
		h.PrefixCacheQueriedTokens = clonePtr(h.PrefixCacheQueriedTokens, same[int64])
		h.PrefixCacheHitTokens = clonePtr(h.PrefixCacheHitTokens, same[int64])
		h.TimePerOutputTokenSecondsSum = clonePtr(h.TimePerOutputTokenSecondsSum, same[float64])
		h.TimePerOutputTokenCount = clonePtr(h.TimePerOutputTokenCount, same[int64])
		h.ResponsesByStatus = maps.Clone(h.ResponsesByStatus)
		return h
	})
	return w
}

func cloneCheckResults(c agentapi.CheckResults) agentapi.CheckResults {
	c.Verify = clonePtr(c.Verify, func(v agentapi.VerifyResult) agentapi.VerifyResult {
		v.Files = slices.Clone(v.Files)
		v.Settings = maps.Clone(v.Settings)
		v.GenerationConfig = maps.Clone(v.GenerationConfig)
		if v.DoSample != nil {
			d := *v.DoSample
			v.DoSample = &d
		}
		if v.GenerationConfigRefused != nil {
			g := *v.GenerationConfigRefused
			v.GenerationConfigRefused = &g
		}
		return v
	})
	c.Qualify = clonePtr(c.Qualify, func(q agentapi.QualifyResult) agentapi.QualifyResult {
		q.Checks = slices.Clone(q.Checks)
		q.ServedModelNames = slices.Clone(q.ServedModelNames)
		return q
	})
	c.WarmUp = clonePtr(c.WarmUp, func(w agentapi.WarmUpResult) agentapi.WarmUpResult {
		if w.Rounds != nil {
			rounds := make([]agentapi.WarmUpRound, len(w.Rounds))
			for n, r := range w.Rounds {
				r.Requests = slices.Clone(r.Requests)
				rounds[n] = r
			}
			w.Rounds = rounds
		}
		return w
	})
	return c
}
