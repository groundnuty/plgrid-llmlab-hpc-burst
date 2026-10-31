package fakeslurm

// New, with the cases of spikes/s4/fakeslurm (shapes_test.go, ctlverbs_test.go)
// that concern sacct. The key lists are S7's, copied from
// spikes/s7/slurm-json-real.md. The invariants are S7's own numbers: elapsed
// 227 = end - start, planned 5 = start - submission.

import (
	"strings"
	"testing"
	"time"
)

// s7SacctKeys are the keys of the job object in S7's sacct capture.
var s7SacctKeys = setOf(`account comment allocation_nodes array association block cluster constraints
container derived_exit_code time exit_code extra failed_node flags group het job_id name licenses mcs nodes
partition hold priority qos qosreq required kill_request_user restart_cnt reservation script stdin_expanded
stdout_expanded stderr_expanded stdout stderr stdin state steps submit_line tres used_gres user wckey
working_directory`)

// s7SacctTimeKeys are the keys of the time object in S7's sacct capture.
var s7SacctTimeKeys = setOf(`elapsed eligible end planned start submission suspended system limit total user`)

func TestSacctShape(t *testing.T) {
	for _, version := range []string{"v0.0.42", "v0.0.40"} {
		t.Run(version, func(t *testing.T) {
			r := newRig(t, Config{DataParser: version})
			id := r.sbatch("--no-requeue --job-name=shape --time=60 --partition=p1 --account=acc --gres=gpu:1 --cpus-per-task=4 --mem=6144M")
			r.clock.Advance(5 * time.Second)
			r.start(id)
			r.clock.Advance(227 * time.Second)
			r.end(id, Completed)

			resp := r.sacct("-X -D -j " + itoa(id))
			wantKeys(t, "sacct response", resp, "jobs", "meta", "errors", "warnings")
			wantMeta(t, resp, version, version, "example-user")
			recs := jobsOf(t, resp)
			if len(recs) != 1 {
				t.Fatalf("%d records, want 1", len(recs))
			}
			j := recs[0]
			wantSubset(t, "sacct job", j, s7SacctKeys)
			wantSubset(t, "sacct time", j["time"].(map[string]any), s7SacctTimeKeys)
			if j["job_id"] != float64(id) || j["name"] != "shape" || j["partition"] != "p1" || j["account"] != "acc" ||
				j["user"] != "example-user" || j["hold"] != false {
				t.Errorf("scalar fields: %v", j)
			}
			// S7: state.current is a list, state.reason a string, and there is no top-level job_state.
			if _, ok := j["job_state"]; ok {
				t.Errorf("sacct job has a job_state key; the state is state.current")
			}
			st := j["state"].(map[string]any)
			wantKeys(t, "state", st, "current", "reason")
			wantStrings(t, "state.current", st["current"], "COMPLETED")
			if st["reason"] != "None" {
				t.Errorf("state.reason = %v, want None", st["reason"])
			}
			// time: epoch integers, limit and planned as triples.
			tm := j["time"].(map[string]any)
			sub, start, end := float64(t0.Unix()), float64(t0.Unix()+5), float64(t0.Unix()+232)
			for k, want := range map[string]float64{"submission": sub, "eligible": sub, "start": start, "end": end} {
				if tm[k] != want {
					t.Errorf("time.%s = %v, want %v", k, tm[k], want)
				}
			}
			if tm["elapsed"] != 227.0 || tm["elapsed"] != tm["end"].(float64)-tm["start"].(float64) {
				t.Errorf("time.elapsed = %v, want 227 = end - start", tm["elapsed"])
			}
			wantNV(t, "time.planned", tm["planned"], true, 5)
			if num(t, j, "time", "planned") != tm["start"].(float64)-tm["submission"].(float64) {
				t.Errorf("time.planned is not start - submission")
			}
			wantNV(t, "time.limit", tm["limit"], true, 60)
			wantNV(t, "priority", j["priority"], true, 1000)
			wantExitCode(t, "exit_code", j["exit_code"], "SUCCESS", true, 0)
			// restart_cnt exists only in v0.0.42.
			if rc, has := j["restart_cnt"]; version == "v0.0.42" && (!has || rc != 0.0) {
				t.Errorf("v0.0.42 restart_cnt = %v (present %v), want 0", rc, has)
			} else if version == "v0.0.40" && has {
				t.Errorf("v0.0.40 sacct record has restart_cnt")
			}
			// tres: S7's cpu, mem, node, billing entries, then the GPUs as gres/gpu.
			tr := j["tres"].(map[string]any)
			wantKeys(t, "tres", tr, "allocated", "requested")
			for _, which := range []string{"requested", "allocated"} {
				got := map[string]float64{}
				for _, e := range tr[which].([]any) {
					m := e.(map[string]any)
					wantKeys(t, "tres entry", m, "type", "name", "id", "count")
					got[m["type"].(string)+"/"+m["name"].(string)] = m["count"].(float64)
				}
				want := map[string]float64{"cpu/": 4, "mem/": 6144, "node/": 1, "billing/": 4, "gres/gpu": 1}
				if len(got) != len(want) {
					t.Errorf("tres.%s = %v, want %v", which, got, want)
				}
				for k, v := range want {
					if got[k] != v {
						t.Errorf("tres.%s[%s] = %v, want %v", which, k, got[k], v)
					}
				}
			}
		})
	}
}

func TestSacctPendingJobHasNoStartAndNoAllocation(t *testing.T) {
	r := newRig(t, Config{})
	id := r.sbatch("--cpus-per-task=2 --mem=1024M")
	held := r.sbatch("--hold")
	j := jobsOf(t, r.sacct("-X -D -j "+itoa(id)))[0]
	tm := j["time"].(map[string]any)
	if tm["start"] != 0.0 || tm["end"] != 0.0 || tm["elapsed"] != 0.0 {
		t.Errorf("pending time = %v, want start, end and elapsed 0", tm)
	}
	wantNV(t, "pending planned", tm["planned"], false, 0)
	wantStrings(t, "pending state", j["state"].(map[string]any)["current"], "PENDING")
	tr := j["tres"].(map[string]any)
	wantStrings(t, "tres.allocated of a pending job", tr["allocated"])
	if got := len(tr["requested"].([]any)); got != 4 { // cpu, mem, node, billing: no gres for a CPU job
		t.Errorf("tres.requested has %d entries, want 4", got)
	}
	// A held job shows its hold in hold and priority; the reason stays None (S4).
	h := jobsOf(t, r.sacct("-X -D -j "+itoa(held)))[0]
	wantNV(t, "held priority", h["priority"], true, 0)
	if h["hold"] != true || h["state"].(map[string]any)["reason"] != "None" {
		t.Errorf("held job: hold %v reason %v", h["hold"], h["state"].(map[string]any)["reason"])
	}
}

// A running job's elapsed time follows the clock.
func TestSacctRunningElapsed(t *testing.T) {
	r := newRig(t, Config{})
	id := r.sbatch()
	r.start(id)
	r.clock.Advance(90 * time.Second)
	tm := jobsOf(t, r.sacct("-X -D -j "+itoa(id)))[0]["time"].(map[string]any)
	if tm["elapsed"] != 90.0 || tm["end"] != 0.0 {
		t.Errorf("running time = %v, want elapsed 90 and no end", tm)
	}
}

// S4: sacct -D shows an earlier execution as PENDING plus REQUEUED; without -D
// only the latest shows. restart_cnt (v0.0.42) is the index of the execution.
func TestSacctRequeueRecords(t *testing.T) {
	for _, version := range []string{"v0.0.42", "v0.0.40"} {
		t.Run(version, func(t *testing.T) {
			r := newRig(t, Config{DataParser: version})
			id := r.sbatch("--job-name=rq")
			r.clock.Advance(time.Second)
			r.start(id)
			r.clock.Advance(time.Second)
			if err := r.p.Requeue(id); err != nil {
				t.Fatal(err)
			}
			r.clock.Advance(time.Second)
			r.start(id)
			r.clock.Advance(time.Second)
			r.end(id, Completed)

			recs := jobsOf(t, r.sacct("-X -D --name=rq"))
			if len(recs) != 2 {
				t.Fatalf("sacct -D lists %d records, want 2 (the earlier execution and the latest)", len(recs))
			}
			first, last := recs[0], recs[1]
			wantStrings(t, "earlier state.current", first["state"].(map[string]any)["current"], "PENDING", "REQUEUED")
			wantStrings(t, "latest state.current", last["state"].(map[string]any)["current"], "COMPLETED")
			// The earlier execution: submitted t0, started t0+1, closed t0+2 by the requeue.
			ft := first["time"].(map[string]any)
			if ft["submission"] != float64(t0.Unix()) || ft["start"] != float64(t0.Unix()+1) || ft["end"] != float64(t0.Unix()+2) {
				t.Errorf("earlier execution time = %v", ft)
			}
			// The latest: submitted at the requeue, t0+2, ran t0+3 to t0+4.
			lt := last["time"].(map[string]any)
			if lt["submission"] != float64(t0.Unix()+2) || lt["start"] != float64(t0.Unix()+3) || lt["end"] != float64(t0.Unix()+4) {
				t.Errorf("latest execution time = %v", lt)
			}
			wantExitCode(t, "earlier exit_code", first["exit_code"], "PENDING", false, 0)
			for i, rec := range recs {
				rc, has := rec["restart_cnt"]
				switch {
				case version == "v0.0.42" && (!has || rc != float64(i)):
					t.Errorf("record %d restart_cnt = %v (present %v), want %d", i, rc, has, i)
				case version == "v0.0.40" && has:
					t.Errorf("v0.0.40 record %d has restart_cnt", i)
				}
			}
			// Without -D only the latest execution.
			plain := jobsOf(t, r.sacct("-X --name=rq"))
			if len(plain) != 1 {
				t.Fatalf("sacct without -D lists %d records, want 1", len(plain))
			}
			wantStrings(t, "state.current", plain[0]["state"].(map[string]any)["current"], "COMPLETED")
		})
	}
}

// S4: forget-requeue models an accounting purge of the earlier records.
func TestSacctForgetRequeue(t *testing.T) {
	r := newRig(t, Config{})
	id := r.sbatch("--job-name=fr")
	for i := 0; i < 2; i++ {
		r.clock.Advance(time.Second)
		r.start(id)
		r.clock.Advance(time.Second)
		if err := r.p.Requeue(id); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(jobsOf(t, r.sacct("-X -D --name=fr"))); n != 3 {
		t.Fatalf("before the purge sacct -D lists %d records, want 3", n)
	}
	if rc := r.squeueJob(id)["restart_cnt"]; rc != 2.0 {
		t.Fatalf("restart_cnt before the purge = %v, want 2", rc)
	}
	if err := r.p.ForgetRequeue(id); err != nil {
		t.Fatal(err)
	}
	recs := jobsOf(t, r.sacct("-X -D --name=fr"))
	if len(recs) != 1 || recs[0]["restart_cnt"] != 0.0 {
		t.Fatalf("after the purge: %d records, restart_cnt %v, want 1 and 0", len(recs), recs[0]["restart_cnt"])
	}
	wantStrings(t, "state.current", recs[0]["state"].(map[string]any)["current"], "PENDING")
	if rc := r.squeueJob(id)["restart_cnt"]; rc != 0.0 {
		t.Errorf("squeue restart_cnt after the purge = %v, want 0", rc)
	}
	// The latest execution's submission time stays that of its requeue (t0+4).
	wantNV(t, "submit_time", r.squeueJob(id)["submit_time"], true, float64(t0.Unix()+4))
}

// S4: the window of -S and -E: a record counts when it was submitted by -E and
// did not end before -S. Without -S sacct starts at midnight in the zone, with
// -j at the epoch.
func TestSacctWindow(t *testing.T) {
	r := newRig(t, Config{})
	old := r.sbatch("--job-name=old")
	r.start(old)
	r.clock.Advance(30 * time.Second)
	r.end(old, Completed) // ended at t0+30
	r.clock.Advance(90 * time.Second)
	fresh := r.sbatch("--job-name=fresh") // submitted at t0+120
	r.clock.Advance(30 * time.Second)     // now = t0+150
	for _, tc := range []struct {
		args string
		want []int
	}{
		{"-S now-60 -E now", []int{fresh}},            // old ended 120 s ago: out
		{"-S now-130 -E now", []int{old, fresh}},      // old ended 120 s ago: in
		{"-S now-120 -E now", []int{old, fresh}},      // the boundary: an end at S is not before S
		{"-S now-119 -E now", []int{fresh}},           // one second later
		{"-S now-5minutes -E now", []int{old, fresh}}, // units
		{"-S now-200 -E now-100", []int{old}},         // fresh submitted after E (t0+120 > t0+50)
		{"", []int{old, fresh}},                       // default start: midnight, same day
		{"-S 2026-10-30T12:02:00", []int{fresh}},      // absolute, in the platform's zone (UTC)
		{"-S 2026-10-31", []int{fresh}},               // old ended before; fresh has not ended, so it is still open
	} {
		got := idsOf(t, r.sacct("-X -D "+tc.args))
		if len(got) != len(tc.want) || (len(got) > 0 && !equalInts(got, tc.want)) {
			t.Errorf("sacct %q lists %v, want %v", tc.args, got, tc.want)
		}
	}
	// -j without -S reaches back to the epoch.
	r.clock.Set(t0.Add(72 * time.Hour))
	if got := idsOf(t, r.sacct("-X -D -j "+itoa(old))); !equalInts(got, []int{old}) {
		t.Errorf("sacct -j three days later lists %v, want [%d]", got, old)
	}
	if got := idsOf(t, r.sacct("-X -D --name=old")); len(got) != 0 {
		t.Errorf("sacct --name three days later with the default start lists %v, want none", got)
	}
}

func TestSacctDefaultStartIsMidnightInTheZone(t *testing.T) {
	// The job runs 21:00 to 21:30 UTC on 2026-10-29, which is 22:00 to 22:30
	// local in Warsaw (CET, UTC+1 since the change of 25 Oct). The clock then
	// reads 23:30 UTC: the same day in UTC, 00:30 on the 30th in Warsaw.
	for _, tc := range []struct {
		zone       string
		wantNoS    int // sacct with no -S: since midnight in the zone
		wantAbsolu int // -S 2026-10-29T22:00:00 read in the zone
	}{
		{"UTC", 1, 0},           // midnight 29th 00:00Z: in; 22:00Z is after the end 21:30Z: out
		{"Europe/Warsaw", 0, 1}, // midnight 30th local = 23:00Z 29th: out; 22:00 local = 21:00Z: in
	} {
		r := newRig(t, Config{TimeZone: tc.zone})
		r.clock.Set(time.Date(2026, 10, 29, 21, 0, 0, 0, time.UTC))
		id := r.sbatch()
		r.start(id)
		r.clock.Advance(30 * time.Minute)
		r.end(id, Completed)
		r.clock.Set(time.Date(2026, 10, 29, 23, 30, 0, 0, time.UTC))
		if got := idsOf(t, r.sacct("-X -D --name=wrap")); len(got) != tc.wantNoS {
			t.Errorf("zone %s: sacct with no -S lists %v, want %d job(s)", tc.zone, got, tc.wantNoS)
		}
		if got := idsOf(t, r.sacct("-X -D -S 2026-10-29T22:00:00 --name=wrap")); len(got) != tc.wantAbsolu {
			t.Errorf("zone %s: sacct -S 2026-10-29T22:00:00 lists %v, want %d job(s)", tc.zone, got, tc.wantAbsolu)
		}
	}
}

func TestSacctFilters(t *testing.T) {
	r := newRig(t, Config{})
	a := r.sbatch("--job-name=alpha")
	b := r.sbatch("--job-name=beta")
	if got := idsOf(t, r.sacct("-X -D --name=alpha")); !equalInts(got, []int{a}) {
		t.Errorf("--name=alpha lists %v", got)
	}
	if got := idsOf(t, r.sacct("-X -D --name=alph")); len(got) != 0 {
		t.Errorf("--name is an exact match; --name=alph lists %v", got)
	}
	if got := idsOf(t, r.sacct("-X -D -j "+itoa(b))); !equalInts(got, []int{b}) {
		t.Errorf("-j %d lists %v", b, got)
	}
	if got := idsOf(t, r.sacct("-X -D -j "+itoa(a)+","+itoa(b))); !equalInts(got, []int{a, b}) {
		t.Errorf("-j with two IDs lists %v", got)
	}
	if got := idsOf(t, r.sacct("-X -D")); !equalInts(got, []int{a, b}) {
		t.Errorf("no filter lists %v", got)
	}
}

// S4: acct-hide makes sacct omit every record of the job; squeue is unaffected.
func TestAcctHideShow(t *testing.T) {
	r := newRig(t, Config{})
	a := r.sbatch("--job-name=a")
	b := r.sbatch("--job-name=b")
	r.start(a)
	if err := r.p.Requeue(a); err != nil { // a has two records with -D
		t.Fatal(err)
	}
	if err := r.p.AcctHide(a); err != nil {
		t.Fatal(err)
	}
	for _, args := range []string{"", "-D", "-j " + itoa(a), "--name=a", "-D -j " + itoa(a)} {
		for _, id := range idsOf(t, r.sacct("-X "+args)) {
			if id == a {
				t.Errorf("sacct %q lists the hidden job %d", args, a)
			}
		}
	}
	if got := idsOf(t, r.sacct("-X -D")); !equalInts(got, []int{b}) {
		t.Errorf("sacct -D lists %v, want only the visible job %d", got, b)
	}
	if got := idsOf(t, r.squeue()); !equalInts(got, []int{a, b}) {
		t.Errorf("squeue lists %v, want both jobs (squeue is unaffected)", got)
	}
	if err := r.p.AcctShow(a); err != nil {
		t.Fatal(err)
	}
	if got := idsOf(t, r.sacct("-X -D")); !equalInts(got, []int{a, a, b}) {
		t.Errorf("sacct -D after AcctShow lists %v, want [%d %d %d]", got, a, a, b)
	}
	for _, f := range []func(int) error{r.p.AcctHide, r.p.AcctShow, r.p.ForgetRequeue, r.p.Requeue, r.p.Start} {
		if f(9999) == nil {
			t.Errorf("a job control on an unknown job succeeded")
		}
	}
}

func TestSacctKeepsEndedJobsForever(t *testing.T) {
	r := newRig(t, Config{})
	id := r.sbatch()
	r.start(id)
	r.end(id, Failed)
	r.clock.Advance(30 * 24 * time.Hour)
	recs := jobsOf(t, r.sacct("-X -D -j "+itoa(id)))
	if len(recs) != 1 {
		t.Fatalf("%d records a month later, want 1", len(recs))
	}
	wantStrings(t, "state.current", recs[0]["state"].(map[string]any)["current"], "FAILED")
	wantExitCode(t, "exit_code", recs[0]["exit_code"], "ERROR", true, 1)
}

func TestSacctOptionErrors(t *testing.T) {
	r := newRig(t, Config{})
	for _, line := range []string{
		"sacct", "sacct -X -D", "sacct --json --allusers", "sacct --json -S yesterday", "sacct --json -S",
		"sacct --json -j abc", "sacct --json extra", "sacct --json --format=JobID",
	} {
		if out, errOut, code := r.run(line); code != 1 || out != "" || !strings.HasPrefix(errOut, "sacct: error: ") {
			t.Errorf("%q: exit %d stdout %q stderr %q, want exit 1 and a sacct error", line, code, out, errOut)
		}
	}
}
