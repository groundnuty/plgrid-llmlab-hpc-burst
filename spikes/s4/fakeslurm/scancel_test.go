package fakeslurm

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

// 7. scancel on a pending, a finished-but-listed and a purged job.
func TestScancel(t *testing.T) {
	r := newRig(t)
	r.sbatch("--job-name=pending")
	r.sbatch("--job-name=done")
	r.sbatch("--job-name=running")
	r.ctl("start", "1002")
	r.ctl("start", "1003")
	r.clk.Advance(time.Second)
	r.ctl("end", "1002")

	// pending and running jobs are cancelled
	r.clk.Advance(time.Second)
	cancelledAt := r.clk.Now()
	for _, id := range []string{"1001", "1003"} {
		if out, errOut, code := r.run("scancel", id); code != 0 || out != "" || errOut != "" {
			t.Errorf("scancel %s: exit %d stdout %q stderr %q, want a silent exit 0", id, code, out, errOut)
		}
	}
	for _, j := range jobs(t, r.squeue("--jobs=1001,1003")) {
		if s := firstState(t, j, "job_state"); s != "CANCELLED" {
			t.Errorf("job %v state %s, want CANCELLED", j["job_id"], s)
		}
		if end := j["end_time"].(map[string]any)["number"].(float64); end != float64(cancelledAt.Unix()) {
			t.Errorf("job %v end_time %v, want %d", j["job_id"], end, cancelledAt.Unix())
		}
	}

	// terminal but not purged
	for _, id := range []string{"1002", "1001"} {
		_, errOut, code := r.run("scancel", id)
		want := "scancel: error: Kill job error on job id " + id + ": Job/step already completing or completed\n"
		if code != 1 || errOut != want {
			t.Errorf("scancel %s on a finished job: exit %d stderr %q, want exit 1 and %q", id, code, errOut, want)
		}
	}

	// unknown id
	_, errOut, code := r.run("scancel", "9999")
	if want := "scancel: error: Kill job error on job id 9999: Invalid job id specified\n"; code != 1 || errOut != want {
		t.Errorf("scancel of an unknown id: exit %d stderr %q, want exit 1 and %q", code, errOut, want)
	}

	// purged: job 1002 ended at t0+1, MinJobAge 10 s
	r.clk.Set(t0.Add(11 * time.Second))
	_, errOut, code = r.run("scancel", "1002")
	if want := "scancel: error: Kill job error on job id 1002: Invalid job id specified\n"; code != 1 || errOut != want {
		t.Errorf("scancel of a purged job: exit %d stderr %q, want exit 1 and %q", code, errOut, want)
	}

	// several ids: the failure of one gives exit 1 but the others are still cancelled
	r.clk.Set(t0.Add(time.Minute))
	id := r.sbatch("--job-name=again")
	_, errOut, code = r.run("scancel", "9999", strconv.Itoa(id))
	if code != 1 || !strings.Contains(errOut, "job id 9999: Invalid job id specified") {
		t.Errorf("scancel 9999 %d: exit %d stderr %q", id, code, errOut)
	}
	if s := firstState(t, jobs(t, r.squeue("-j", strconv.Itoa(id)))[0], "job_state"); s != "CANCELLED" {
		t.Errorf("job %d state %s after a mixed scancel, want CANCELLED", id, s)
	}
	if _, _, code := r.run("scancel"); code != 1 {
		t.Errorf("scancel without ids: exit %d, want 1", code)
	}
}
