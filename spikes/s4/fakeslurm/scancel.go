package fakeslurm

import (
	"fmt"
	"strconv"
	"strings"
)

// scancel cancels each id given on the command line. Options are ignored.
// One log event is written per id.
func (a *app) scancel(args []string) int {
	return a.run(func(t *tx) result {
		now := a.env.Now()
		var res result
		var ids []string
		for _, arg := range args {
			if !strings.HasPrefix(arg, "-") {
				ids = append(ids, arg)
			}
		}
		if len(ids) == 0 {
			t.emit("scancel", "scancel", 0, intp(1))
			return result{errOut: "scancel: error: No job identification provided\n", exit: 1}
		}
		for _, s := range ids {
			id, convErr := strconv.Atoi(s)
			var j *Job
			if convErr == nil {
				j = t.st.Find(id)
			}
			var msg string
			switch {
			case j == nil || j.purged(now, a.cfg.MinJobAge):
				msg = "Invalid job id specified"
			case IsTerminal(j.current(now)):
				msg = "Job/step already completing or completed"
			default:
				if err := j.markEnd(now, Cancelled); err != nil {
					msg = err.Error()
				}
				t.dirty = true
			}
			if msg != "" {
				res.errOut += fmt.Sprintf("scancel: error: Kill job error on job id %s: %s\n", s, msg)
				res.exit = 1
				t.emit("scancel", "scancel", id, intp(1))
				continue
			}
			t.emit("scancel", "scancel", id, intp(0))
		}
		return res
	})
}
