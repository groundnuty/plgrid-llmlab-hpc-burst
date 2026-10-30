package fakeslurm

import (
	"strconv"
	"strings"
)

const scontrolUsage = "scontrol: error: the fake supports only: scontrol release <jobid>[,<jobid>...]\n"

// scontrol supports "release" only. Options before the subcommand are
// ignored.
func (a *app) scontrol(args []string) int {
	var rest []string
	for i, arg := range args {
		if !strings.HasPrefix(arg, "-") {
			rest = args[i:]
			break
		}
	}
	if len(rest) == 0 || rest[0] != "release" {
		return a.run(func(t *tx) result {
			t.emit("scontrol", "scontrol", 0, intp(1))
			return result{errOut: scontrolUsage, exit: 1}
		})
	}
	return a.scontrolRelease(rest[1:])
}

// scontrolRelease releases each id (comma lists and separate arguments both
// work) and logs one scontrol_release event per id. Slurm's rules, from
// update_job in job_mgr.c: a user hold is released, an admin hold stays
// (access denied), releasing a job that is not held is ignored, and a
// finished or forgotten job is an error.
func (a *app) scontrolRelease(args []string) int {
	return a.run(func(t *tx) result {
		now := a.env.Now()
		var ids []string
		for _, arg := range args {
			ids = append(ids, splitList(arg)...)
		}
		if len(ids) == 0 {
			t.emit("scontrol_release", "scontrol", 0, intp(1))
			return result{errOut: "scontrol: error: too few arguments for keyword:release\n", exit: 1}
		}
		var res result
		for _, s := range ids {
			id, convErr := strconv.Atoi(s)
			var j *Job
			if convErr == nil {
				j = t.st.Find(id)
			}
			var msg, note string
			switch {
			case j == nil || j.purged(now, a.cfg.MinJobAge):
				msg = "Invalid job id specified"
			case IsTerminal(j.current(now)):
				msg = "Job has already finished"
			case !j.Held:
				note = "already released"
			case j.Cur().Reason == HeldAdmin:
				msg = "Access/permission denied"
			default:
				j.release()
				t.dirty = true
			}
			if msg != "" {
				res.errOut += "scontrol: error: " + msg + "\n"
				res.exit = 1
				t.emit("scontrol_release", "scontrol", id, intp(1))
				continue
			}
			t.emitNote("scontrol_release", "scontrol", id, intp(0), note)
		}
		return res
	})
}
