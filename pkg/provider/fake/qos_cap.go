package fake

import (
	"sort"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// ReasonQOSMaxGRESPerUser is the state reason Slurm lists for a pending job
// that fits the user's GPU cap alone but not beside the user's running jobs
// (HBEP-4, QOS limits). The fake shows it for a job that waits only on
// Config.MaxGPUsPerUser.
const ReasonQOSMaxGRESPerUser = "QOSMaxGRESPerUser"

// The cap on the GPUs of one user's running jobs (Config.MaxGPUsPerUser), a
// QOS's MaxTRESPerUser for gres/gpu, as Helios's now has it.
//
// The fake keeps no state machine, so a start under the cap is not stored: each
// call recomputes every job's start from the facts and the clock, by a sweep
// over time (cappedStarts). The sweep walks the moments where something can
// change, the nominal starts and the ends. At each moment it considers the
// waiting jobs that are eligible, in order of nominal start (ties: the job that
// became eligible first, which is job ID order unless a hold lift or a requeue
// came between), and starts a job when its GPUs plus those of the jobs running
// at that moment fit the cap. A job that does not fit waits and the jobs after
// it still get their turn, as in Slurm, where a job held back by a QOS limit
// does not block the others. The cap therefore holds at every moment.
//
// Because a new fact (a submission, a cancel, a lift, a requeue) is dated now,
// at or after every moment the sweep has already passed, it can move only
// starts that are still ahead. A start that has been reached never moves.

// occupancy is the GPUs one job holds from start up to, not including, end.
type occupancy struct {
	start, end time.Time
	gpus       int32
}

// holds reports whether the occupancy holds its GPUs at t.
func (o occupancy) holds(t time.Time) bool { return !t.Before(o.start) && t.Before(o.end) }

// cappedStarts returns, for each unheld job, the start the cap gives it. A job
// absent from the map never starts: it needs more GPUs than the cap, or it was
// cancelled before the moment it could start. A cancel at the very moment of
// the start still gives the start, with no run: the job holds nothing. The
// caller holds b.mu and the cap is set.
func (b *Batch) cappedStarts() map[string]time.Time {
	limit := b.cfg.MaxGPUsPerUser
	var occupied []occupancy // fixed at first: the earlier executions of requeued jobs
	var waiting []*jobRec
	for _, j := range b.jobs {
		for _, r := range j.ran {
			occupied = append(occupied, occupancy{start: r.start, end: r.end, gpus: j.gpus})
		}
		if j.hold == provider.HoldNone {
			waiting = append(waiting, j)
		}
	}
	sort.Slice(waiting, func(i, k int) bool {
		a, c := waiting[i].nominalStart(), waiting[k].nominalStart()
		if !a.Equal(c) {
			return a.Before(c)
		}
		return waiting[i].seq < waiting[k].seq
	})

	starts := make(map[string]time.Time, len(waiting))
	if len(waiting) == 0 {
		return starts
	}
	// Every moment from the first nominal start on is an event: a nominal start
	// or an end. Each pass moves t forward, so the sweep ends.
	t := waiting[0].nominalStart()
	for len(waiting) > 0 {
		inUse := func() int32 {
			var sum int32
			for _, o := range occupied {
				if o.holds(t) {
					sum += o.gpus
				}
			}
			return sum
		}
		var rest []*jobRec
		for _, j := range waiting {
			switch c := j.cancelledAt; {
			case c != nil && c.Before(t):
				continue // cancelled before it could start: never starts
			case j.gpus > limit:
				continue // never fits, even alone
			case j.nominalStart().After(t):
				rest = append(rest, j)
			case j.gpus == 0 || inUse()+j.gpus <= limit:
				s := t
				starts[j.id] = s
				_, end, _ := j.timelineFrom(&s)
				occupied = append(occupied, occupancy{start: s, end: *end, gpus: j.gpus})
			default:
				rest = append(rest, j)
			}
		}
		waiting = rest
		next, ok := nextEvent(t, occupied, waiting)
		if !ok {
			break
		}
		t = next
	}
	return starts
}

// nextEvent returns the first moment after t where a waiting job could start
// that it could not at t: the next nominal start, or the next end of an
// occupancy. ok is false when there is none.
func nextEvent(t time.Time, occupied []occupancy, waiting []*jobRec) (next time.Time, ok bool) {
	consider := func(c time.Time) {
		if c.After(t) && (!ok || c.Before(next)) {
			next, ok = c, true
		}
	}
	for _, o := range occupied {
		consider(o.end)
	}
	for _, j := range waiting {
		consider(j.nominalStart())
	}
	return next, ok
}

// timelineOf is j.timeline() under the user's GPU cap, when Config sets one:
// the start is the one the sweep gives. waitsOnCap is true for an unheld job
// that has no start and no end, which the cap holds back for good: more GPUs
// than the cap, or no room yet. The caller holds b.mu.
func (b *Batch) timelineOf(j *jobRec) (start, end *time.Time, endState string, waitsOnCap bool) {
	if b.cfg.MaxGPUsPerUser == 0 || j.hold != provider.HoldNone {
		start, end, endState = j.timeline()
		return start, end, endState, false
	}
	var s *time.Time
	if t, ok := b.cappedStarts()[j.id]; ok {
		s = &t
	}
	start, end, endState = j.timelineFrom(s)
	return start, end, endState, start == nil && end == nil
}
