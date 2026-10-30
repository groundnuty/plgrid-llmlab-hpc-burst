package fakeslurm

import (
	"fmt"
	"strconv"
)

const ctlUsage = `usage: fakeslurmctl start <id> | end <id> [STATE] | requeue <id> | requeue-hold <id> | hold-admin <id> | acct-hide <id> | acct-show <id> | forget-requeue <id> | dump | count-sbatch [name]`

// ctl is fakeslurmctl, the test's handle on the fake cluster: Slurm moves
// jobs on its own, the fake only when told.
func (a *app) ctl(args []string) int {
	return a.run(func(t *tx) result {
		fail := func(id int, msg string) result {
			t.emit("ctl", "fakeslurmctl", id, intp(1))
			return result{errOut: "fakeslurmctl: " + msg + "\n", exit: 1}
		}
		if len(args) == 0 {
			return fail(0, ctlUsage)
		}
		verb, rest := args[0], args[1:]
		now := a.env.Now()
		switch verb {
		case "dump":
			out, err := marshalOut(t.st)
			if err != nil {
				return fail(0, err.Error())
			}
			t.emit("ctl", "fakeslurmctl", 0, intp(0))
			return result{out: out}
		case "count-sbatch":
			return a.countSbatch(t, rest, fail)
		case "start", "end", "requeue", "requeue-hold", "hold-admin", "acct-hide", "acct-show", "forget-requeue":
			if len(rest) == 0 {
				return fail(0, ctlUsage)
			}
			id, err := strconv.Atoi(rest[0])
			if err != nil {
				return fail(0, fmt.Sprintf("invalid job id %q", rest[0]))
			}
			j := t.st.Find(id)
			if j == nil {
				return fail(id, fmt.Sprintf("job %d not found", id))
			}
			switch verb {
			case "start":
				err = j.markStart(now)
			case "end":
				state := Completed
				if len(rest) > 1 {
					state = rest[1]
				}
				err = j.markEnd(now, state)
			case "requeue":
				j.markRequeue(now)
			case "requeue-hold":
				// Slurm holds the new execution (user hold) for requeue-hold
				// and for launch-failed requeues.
				j.markRequeue(now)
				j.hold(HeldUser)
			case "hold-admin":
				if st := j.Cur().StateAt(now); st != Pending {
					err = fmt.Errorf("job %d is %s, not PENDING", id, st)
					break
				}
				j.hold(HeldAdmin)
			case "acct-hide":
				j.AcctHidden = true
			case "acct-show":
				j.AcctHidden = false
			case "forget-requeue":
				j.forgetRequeues()
			}
			if err != nil {
				return fail(id, err.Error())
			}
			t.dirty = true
			t.emit("ctl", "fakeslurmctl", id, intp(0))
			return result{jobID: id}
		}
		return fail(0, fmt.Sprintf("unknown command %q; %s", verb, ctlUsage))
	})
}

// countSbatch prints how many jobs sbatch registered, optionally only those
// with the given job name. It counts sbatch_submitted log events, so a job
// the provider submitted twice counts twice.
func (a *app) countSbatch(t *tx, args []string, fail func(int, string) result) result {
	events, err := readEvents(a.cfg.LogPath)
	if err != nil {
		return fail(0, err.Error())
	}
	n := 0
	for _, ev := range events {
		if ev.Event != "sbatch_submitted" {
			continue
		}
		if len(args) > 0 {
			j := t.st.Find(ev.JobID)
			if j == nil || j.Name != args[0] {
				continue
			}
		}
		n++
	}
	t.emit("ctl", "fakeslurmctl", 0, intp(0))
	return result{out: fmt.Sprintf("%d\n", n)}
}
