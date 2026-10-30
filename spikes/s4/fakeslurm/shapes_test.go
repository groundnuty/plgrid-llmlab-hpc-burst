package fakeslurm

import (
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"
)

func keysOf(m map[string]any) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func wantKeys(t *testing.T, what string, m map[string]any, want ...string) {
	t.Helper()
	sort.Strings(want)
	if got := keysOf(m); !reflect.DeepEqual(got, want) {
		t.Errorf("%s keys\n got  %v\n want %v", what, got, want)
	}
}

// wantNV asserts a Slurm "number with set and infinite flags".
func wantNV(t *testing.T, what string, v any, set bool, number float64) {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Errorf("%s is %T, want an object {set, infinite, number}", what, v)
		return
	}
	wantKeys(t, what, m, "set", "infinite", "number")
	if m["set"] != set || m["infinite"] != false || m["number"] != number {
		t.Errorf("%s = %v, want set=%v infinite=false number=%v", what, m, set, number)
	}
}

func wantStrings(t *testing.T, what string, v any, want ...string) {
	t.Helper()
	list, ok := v.([]any)
	if !ok {
		t.Errorf("%s is %T, want a list of strings", what, v)
		return
	}
	var got []string
	for _, s := range list {
		got = append(got, s.(string))
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s = %v, want %v", what, got, want)
	}
}

func wantMeta(t *testing.T, resp map[string]any, parser string) {
	t.Helper()
	m, _ := resp["meta"].(map[string]any)
	plugin, _ := m["plugin"].(map[string]any)
	wantKeys(t, "meta.plugin", plugin, "data_parser", "accounting_storage")
	if plugin["data_parser"] != "data_parser/"+parser || plugin["accounting_storage"] != "accounting_storage/slurmdbd" {
		t.Errorf("meta.plugin = %v", plugin)
	}
	for _, k := range []string{"errors", "warnings"} {
		if l, ok := resp[k].([]any); !ok || len(l) != 0 {
			t.Errorf("%s = %#v, want an empty list", k, resp[k])
		}
	}
}

func wantExitCode(t *testing.T, v any, status string, set bool, rc float64) {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Errorf("exit_code is %T", v)
		return
	}
	wantKeys(t, "exit_code", m, "status", "return_code", "signal")
	wantStrings(t, "exit_code.status", m["status"], status)
	wantNV(t, "exit_code.return_code", m["return_code"], set, rc)
	sig, _ := m["signal"].(map[string]any)
	wantKeys(t, "exit_code.signal", sig, "id", "name")
	wantNV(t, "exit_code.signal.id", sig["id"], false, 0)
	if sig["name"] != "" {
		t.Errorf("exit_code.signal.name = %v, want an empty string", sig["name"])
	}
}

// 9. The JSON shapes, as in Slurm's data_parser (v0.0.42 and v0.0.40).
func TestJSONShapes(t *testing.T) {
	r := newRig(t)
	r.env["FAKE_SLURM_USER"] = "someone"
	r.env["FAKE_SLURM_STORE_COMMENT"] = "1"
	r.sbatch("--job-name=shape", "--comment=note", "--time=01:30:00", "--partition=p1", "--account=acc", "--wrap=sleep 1")
	r.sbatch("--job-name=other")
	r.clk.Advance(10 * time.Second)
	r.ctl("start", "1001")
	r.clk.Advance(20 * time.Second)
	now := float64(r.clk.Now().Unix())
	submit, start := float64(t0.Unix()), float64(t0.Unix()+10)

	// squeue, v0.0.42
	resp := r.squeue()
	wantKeys(t, "squeue response", resp, "jobs", "last_backfill", "last_update", "meta", "errors", "warnings")
	wantMeta(t, resp, "v0.0.42")
	wantNV(t, "last_backfill", resp["last_backfill"], true, now)
	wantNV(t, "last_update", resp["last_update"], true, now)
	js := jobs(t, resp)
	j, pend := js[0], js[1]
	wantKeys(t, "squeue job", j, "job_id", "name", "comment", "job_state", "restart_cnt", "requeue",
		"submit_time", "start_time", "end_time", "user_name", "partition", "account", "state_reason",
		"time_limit", "exit_code")
	if j["job_id"] != 1001.0 || j["name"] != "shape" || j["comment"] != "note" ||
		j["user_name"] != "someone" || j["partition"] != "p1" || j["account"] != "acc" ||
		j["state_reason"] != "None" || j["restart_cnt"] != 0.0 || j["requeue"] != true {
		t.Errorf("squeue job scalar fields: %v", j)
	}
	wantStrings(t, "job_state", j["job_state"], "RUNNING")
	wantNV(t, "submit_time", j["submit_time"], true, submit)
	wantNV(t, "start_time", j["start_time"], true, start)
	wantNV(t, "end_time", j["end_time"], false, 0)
	wantNV(t, "time_limit", j["time_limit"], true, 90)
	wantExitCode(t, j["exit_code"], "PENDING", false, 0)
	wantStrings(t, "pending job_state", pend["job_state"], "PENDING")
	wantNV(t, "pending start_time", pend["start_time"], false, 0)
	wantNV(t, "pending time_limit (no --time)", pend["time_limit"], false, 0)

	r.ctl("end", "1001")
	endSec := float64(r.clk.Now().Unix())
	done := jobs(t, r.squeue("-j", "1001"))[0]
	wantNV(t, "end_time", done["end_time"], true, endSec)
	wantStrings(t, "done job_state", done["job_state"], "COMPLETED")
	wantExitCode(t, done["exit_code"], "SUCCESS", true, 0)

	// sacct, v0.0.42
	resp = r.sacct()
	wantKeys(t, "sacct response", resp, "jobs", "meta", "errors", "warnings")
	wantMeta(t, resp, "v0.0.42")
	acct := jobs(t, resp)[0]
	wantKeys(t, "sacct job", acct, "job_id", "name", "user", "partition", "account", "comment", "state",
		"time", "exit_code", "flags", "restart_cnt")
	if acct["job_id"] != 1001.0 || acct["name"] != "shape" || acct["user"] != "someone" ||
		acct["partition"] != "p1" || acct["account"] != "acc" || acct["restart_cnt"] != 0.0 {
		t.Errorf("sacct job scalar fields: %v", acct)
	}
	comment := acct["comment"].(map[string]any)
	wantKeys(t, "sacct comment", comment, "administrator", "job", "system")
	if comment["job"] != "note" || comment["administrator"] != "" || comment["system"] != "" {
		t.Errorf("sacct comment = %v, want job=note with STORE_COMMENT=1", comment)
	}
	state := acct["state"].(map[string]any)
	wantKeys(t, "sacct state", state, "current", "reason")
	wantStrings(t, "state.current", state["current"], "COMPLETED")
	if state["reason"] != "None" {
		t.Errorf("state.reason = %v", state["reason"])
	}
	tm := acct["time"].(map[string]any)
	wantKeys(t, "sacct time", tm, "submission", "start", "end", "elapsed", "eligible", "limit")
	for k, want := range map[string]float64{"submission": submit, "start": start, "end": endSec, "elapsed": 20, "eligible": submit} {
		if tm[k] != want { // plain numbers, not NV objects
			t.Errorf("sacct time.%s = %#v, want the number %v", k, tm[k], want)
		}
	}
	wantNV(t, "sacct time.limit", tm["limit"], true, 90)
	wantExitCode(t, acct["exit_code"], "SUCCESS", true, 0)
	wantStrings(t, "flags", acct["flags"])

	// the comment is hidden without STORE_COMMENT (slurmdbd default)
	delete(r.env, "FAKE_SLURM_STORE_COMMENT")
	hidden := jobs(t, r.sacct("-j", "1001"))[0]["comment"].(map[string]any)
	if hidden["job"] != "" {
		t.Errorf("sacct comment without STORE_COMMENT = %v, want job=\"\"", hidden)
	}

	// v0.0.40: same, without restart_cnt in sacct but with it in squeue
	r.env["FAKE_SLURM_DATA_PARSER"] = "v0.0.40"
	resp = r.sacct()
	wantMeta(t, resp, "v0.0.40")
	wantKeys(t, "v0.0.40 sacct job", jobs(t, resp)[0], "job_id", "name", "user", "partition", "account",
		"comment", "state", "time", "exit_code", "flags")
	resp = r.squeue()
	wantMeta(t, resp, "v0.0.40")
	if _, ok := jobs(t, resp)[0]["restart_cnt"]; !ok {
		t.Errorf("v0.0.40 squeue job lacks restart_cnt")
	}
}

// Jobs that ended in any state but COMPLETED report an ERROR exit status (the
// brief leaves the return code open, so only the status is asserted).
func TestExitStatusOfFailedJobs(t *testing.T) {
	r := newRig(t)
	for _, state := range []string{"FAILED", "TIMEOUT", "NODE_FAIL", "CANCELLED"} {
		id := strconv.Itoa(r.sbatch("--job-name=" + state))
		r.ctl("end", id, state) // works from PENDING
		for _, j := range []map[string]any{jobs(t, r.squeue("-j", id))[0], jobs(t, r.sacct("-j", id))[0]} {
			ec := j["exit_code"].(map[string]any)
			wantStrings(t, state+" exit_code.status", ec["status"], "ERROR")
		}
		wantStrings(t, state+" job_state", jobs(t, r.squeue("-j", id))[0]["job_state"], state)
	}
}
