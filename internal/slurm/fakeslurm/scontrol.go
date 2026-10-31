package fakeslurm

// Ported from spikes/s4/fakeslurm/scontrol.go. Changed: an unknown or purged
// job gives "slurm_load_jobs error: Invalid job id specified" as the brief of
// this fake asks (S4 printed "scontrol: error: Invalid job id specified");
// another user's job gives "Access/permission denied".

import "strconv"

const scontrolUsage = "scontrol: error: the fake supports only: scontrol release <jobid>[,<jobid>...]"

// scontrol supports "release" only.
func (sh *shell) scontrol(args []string) int {
	if len(args) == 0 || args[0] != "release" {
		return sh.fail(1, "%s", scontrolUsage)
	}
	var ids []string
	for _, a := range args[1:] {
		ids = append(ids, splitList(a)...)
	}
	if len(ids) == 0 {
		return sh.fail(1, "scontrol: error: too few arguments for keyword:release")
	}
	code := 0
	for _, s := range ids {
		if msg := sh.releaseOne(s); msg != "" {
			sh.stderr.WriteString(msg + "\n")
			code = 1
		}
	}
	return code
}

// releaseOne releases one job and returns the error line, or "". Slurm's
// rules, from update_job in job_mgr.c: a user hold is released, an admin hold
// stays (access denied), releasing a job that is not held is ignored, and a
// finished or forgotten job is an error.
func (sh *shell) releaseOne(s string) string {
	id, err := strconv.Atoi(s)
	var j *job
	if err == nil {
		j = sh.p.find(id)
	}
	switch {
	case j == nil || j.purged(sh.now, sh.p.cfg.MinJobAge):
		return invalidJob
	case j.user != sh.s.user:
		return "scontrol: error: Access/permission denied"
	case IsTerminal(j.state()):
		return "scontrol: error: Job has already finished"
	case !j.held:
		return ""
	case j.cur().reason == HeldAdmin:
		return "scontrol: error: Access/permission denied"
	}
	j.release()
	return ""
}
