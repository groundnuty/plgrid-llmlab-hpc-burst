package fakeslurm

// Ported from spikes/s4/fakeslurm/scancel.go. Added: another user's job gives
// "Access/permission denied", and a cancelled running job frees its slot.

import (
	"strconv"
	"strings"
)

// scancel cancels each job ID on the line. A failure on one ID gives exit 1
// but the others are still cancelled. Options are refused.
func (sh *shell) scancel(args []string) int {
	var ids []string
	for _, a := range args {
		if strings.HasPrefix(a, "-") {
			return sh.fail(1, "scancel: error: the fake takes no options, only job IDs")
		}
		ids = append(ids, a)
	}
	if len(ids) == 0 {
		return sh.fail(1, "scancel: error: No job identification provided")
	}
	code := 0
	for _, s := range ids {
		if msg := sh.cancelOne(s); msg != "" {
			fmtKill := "scancel: error: Kill job error on job id " + s + ": " + msg
			sh.stderr.WriteString(fmtKill + "\n")
			code = 1
		}
	}
	return code
}

// cancelOne cancels one job and returns the error text, or "".
func (sh *shell) cancelOne(s string) string {
	id, err := strconv.Atoi(s)
	var j *job
	if err == nil {
		j = sh.p.find(id)
	}
	switch {
	case j == nil || j.purged(sh.now, sh.p.cfg.MinJobAge):
		return "Invalid job id specified"
	case j.user != sh.s.user:
		return "Access/permission denied"
	case IsTerminal(j.state()):
		return "Job/step already completing or completed"
	}
	if err := sh.p.endJob(j, sh.now, Cancelled); err != nil {
		return err.Error()
	}
	return ""
}
