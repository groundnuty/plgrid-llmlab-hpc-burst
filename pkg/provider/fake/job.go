package fake

import (
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// jobRec is what the scheduler stores about one job. It holds no state, only
// the facts the timeline derives it from: the phase at a moment is computed
// from these and the clock (record), so the fake needs no goroutine.
type jobRec struct {
	id        string
	tag       string // the tag the job was submitted under
	name      string // the name the scheduler stores (tag, or Script.Rename)
	partition string
	limit     time.Duration // the limit the job was established with
	gpus      int32
	cpus      int32 // the CPUs the job was established with (cpusPerTask)
	submit    time.Time
	script    Script

	// hold is the job's current hold. eligible is when the job became
	// eligible to start, valid while hold is HoldNone: the submit time for an
	// unheld job, the lift time for a held one, and the requeue time (or the
	// lift time after a requeue hold) for a requeued one.
	hold     provider.HoldKind
	eligible time.Time

	cancelledAt  *time.Time
	restartCount int32
	requeued     bool
	vanished     bool
	// accountingLags: ReadAccounting finds nothing, the queue listing as usual.
	// noElapsed: accounting's record has no time.elapsed.
	accountingLags bool
	noElapsed      bool

	// priorRun is the time the job's earlier executions ran, added by each
	// requeue (accounting sums the executions of a requeued job).
	priorRun time.Duration
}

// timeline returns when the job starts and when it ends if nothing else
// happens, from the facts alone. A nil start means the job is held and has no
// start yet; a nil end means it has no end yet. endState is the Slurm state at
// the end. A cancel clips the timeline.
func (j *jobRec) timeline() (start, end *time.Time, endState string) {
	if j.hold == provider.HoldNone {
		s := j.eligible.Add(j.script.QueueWait)
		start = &s
		hard := s.Add(j.limit)
		end, endState = &hard, "TIMEOUT"
		if fa := j.script.FailAfter; fa != nil {
			if f := s.Add(*fa); f.Before(hard) {
				endState = j.script.FailState
				if endState == "" {
					endState = "FAILED"
				}
				end = &f
			}
		}
	}
	if c := j.cancelledAt; c != nil {
		end, endState = c, "CANCELLED"
		if start != nil && start.After(*c) {
			start = nil
		}
	}
	return start, end, endState
}

// endReason is the state reason Slurm lists for a job that ended in state.
func endReason(state string) string {
	switch state {
	case "TIMEOUT":
		return "TimeLimit"
	case "FAILED":
		return "NonZeroExitCode"
	case "BOOT_FAIL":
		return "BootFail"
	case "NODE_FAIL":
		return "NodeFail"
	default:
		return "None"
	}
}

// record computes the job's record at now. It is the only place the lifecycle
// is decided. The source is the queue listing until EndTime plus MinJobAge,
// and accounting after.
func (b *Batch) record(j *jobRec, now time.Time) provider.Job {
	start, end, endState := j.timeline()
	out := provider.Job{
		ID:           j.id,
		Name:         j.name,
		Partition:    j.partition,
		TimeLimit:    j.limit,
		GPUs:         j.gpus,
		SubmitTime:   j.submit,
		RestartCount: j.restartCount,
		Requeued:     j.requeued || j.restartCount > 0,
		Source:       provider.SourceQueue,
		DataParser:   b.cfg.DataParser,
	}
	var state, reason string
	held := false
	switch {
	case end != nil && !now.Before(*end):
		state, reason = endState, endReason(endState)
		e := *end
		out.EndTime = &e
		if !e.Add(b.cfg.MinJobAge).After(now) {
			out.Source = provider.SourceAccounting
		}
	case start != nil && !now.Before(*start):
		state, reason = "RUNNING", "None"
	case start != nil:
		state, reason = "PENDING", "Priority"
		if est := j.script.StartEstimate; est > 0 {
			t := j.eligible.Add(est)
			out.StartEstimate = &t
		}
	default:
		held = true
		state, reason = "PENDING", provider.ReasonHeldUser
		if j.hold == provider.HoldAdmin {
			reason = provider.ReasonHeldAdmin
		}
	}
	if start != nil && !now.Before(*start) {
		s := *start
		h := s.Add(j.limit)
		out.StartTime, out.HardEnd = &s, &h
	}
	if out.Source == provider.SourceAccounting {
		out.Accounting = b.accountingOf(j, out, now)
	}
	out.State = []string{state}
	out.Reason = reason
	out.Hold = provider.HoldOf(held, reason)
	phase, err := provider.SlurmPhase(out.State, held, out.Requeued)
	if err != nil {
		// Script.FailState is the only free-form state; an unknown one is a
		// test bug, and the fake must not report a phase it cannot justify.
		panic("fake: job " + j.id + ": " + err.Error())
	}
	out.Phase = phase
	return out
}

// accountingOf is what accounting reports about the job whose record is r at
// now, in the shape of internal/slurm/fakeslurm's sacct: the cpu entry of
// tres.allocated once the job has started, the gpu entry only for a job that
// has GPUs, and time.elapsed summed over the executions. A job that never
// started has no allocation and 0 elapsed.
func (b *Batch) accountingOf(j *jobRec, r provider.Job, now time.Time) *provider.JobAccounting {
	a := &provider.JobAccounting{Executions: int(j.restartCount) + 1}
	run := j.priorRun
	if r.StartTime != nil {
		stop := now
		if r.EndTime != nil {
			stop = *r.EndTime
		}
		if d := stop.Sub(*r.StartTime); d > 0 {
			run += d
		}
		cpus := j.cpus
		a.AllocatedCPUs = &cpus
		if j.gpus > 0 {
			gpus := j.gpus
			a.AllocatedGPUs = &gpus
		}
	}
	if !j.noElapsed {
		a.Elapsed = &run
	}
	return a
}

// live reports whether the job can still be cancelled: held, pending or
// running, which is a job with no end time yet.
func live(r provider.Job) bool { return r.EndTime == nil }
