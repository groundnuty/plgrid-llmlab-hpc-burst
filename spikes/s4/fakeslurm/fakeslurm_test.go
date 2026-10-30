package fakeslurm

import (
	"strings"
	"testing"
	"time"
)

func firstState(t *testing.T, job map[string]any, key string) string {
	t.Helper()
	switch v := job[key].(type) {
	case []any: // squeue job_state
		return v[0].(string)
	case map[string]any: // sacct state
		return v["current"].([]any)[0].(string)
	}
	t.Fatalf("%s is %T", key, job[key])
	return ""
}

// 1. squeue forgets a finished job exactly at end + MinJobAge; sacct keeps it.
func TestCompletedJobLeavesSqueueAtMinJobAge(t *testing.T) {
	for _, tc := range []struct {
		name   string
		minAge string // FAKE_SLURM_MIN_JOB_AGE; "" is the default, 10 s
		age    time.Duration
	}{
		{"default 10 s", "", 10 * time.Second},
		{"30 s", "30", 30 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t)
			if tc.minAge != "" {
				r.env["FAKE_SLURM_MIN_JOB_AGE"] = tc.minAge
			}
			done := r.sbatch("--job-name=done")
			running := r.sbatch("--job-name=running")
			if done != 1001 || running != 1002 {
				t.Fatalf("job ids %d, %d; want 1001, 1002", done, running)
			}
			r.clk.Advance(5 * time.Second)
			r.ctl("start", "1001")
			r.ctl("start", "1002")
			r.clk.Advance(20 * time.Second)
			r.ctl("end", "1001")
			end := r.clk.Now()

			for _, at := range []struct {
				after      time.Duration
				wantListed bool
			}{
				{0, true},
				{tc.age - time.Second, true},
				{tc.age - time.Millisecond, true},
				{tc.age, false},
				{tc.age + time.Hour, false},
			} {
				r.clk.Set(end.Add(at.after))
				listed := contains(ids(t, r.squeue()), 1001)
				if listed != at.wantListed {
					t.Errorf("end+%v: squeue lists job 1001 = %v, want %v", at.after, listed, at.wantListed)
				}
				if !contains(ids(t, r.sacct()), 1001) {
					t.Errorf("end+%v: sacct lost the finished job", at.after)
				}
				if !contains(ids(t, r.squeue()), 1002) {
					t.Errorf("end+%v: a RUNNING job must never be purged from squeue", at.after)
				}
			}
			// The finished job's state in sacct is COMPLETED.
			for _, j := range jobs(t, r.sacct("-j", "1001")) {
				if s := firstState(t, j, "state"); s != "COMPLETED" {
					t.Errorf("sacct state %s, want COMPLETED", s)
				}
			}
		})
	}
}

// 2. --name and --me (and -u, -j) select in squeue and sacct.
func TestFilters(t *testing.T) {
	r := newRig(t)
	r.env["FAKE_SLURM_USER"] = "spike"
	a1 := r.sbatch("--job-name=alpha")
	b1 := r.sbatch("-J", "beta")
	r.env["FAKE_SLURM_USER"] = "other"
	a2 := r.sbatch("--job-name", "alpha")
	g2 := r.sbatch("-J", "gamma")
	r.env["FAKE_SLURM_USER"] = "spike" // the caller is spike again
	if got := []int{a1, b1, a2, g2}; !sameInts(got, []int{1001, 1002, 1003, 1004}) {
		t.Fatalf("ids %v", got)
	}

	for _, tc := range []struct {
		name string
		args []string
		want []int
	}{
		{"no filter", nil, []int{1001, 1002, 1003, 1004}},
		{"--me", []string{"--me"}, []int{1001, 1002}},
		{"--name=alpha", []string{"--name=alpha"}, []int{1001, 1003}},
		{"--name alpha,beta", []string{"--name", "alpha,beta"}, []int{1001, 1002, 1003}},
		{"-n gamma", []string{"-n", "gamma"}, []int{1004}},
		{"--me --name=alpha", []string{"--me", "--name=alpha"}, []int{1001}},
		{"--jobs=1002,1004", []string{"--jobs=1002,1004"}, []int{1002, 1004}},
		{"-j 1003", []string{"-j", "1003"}, []int{1003}},
		{"unknown name", []string{"--name=nope"}, []int{}},
	} {
		if got := ids(t, r.squeue(tc.args...)); !sameInts(got, tc.want) {
			t.Errorf("squeue %s: ids %v, want %v", tc.name, got, tc.want)
		}
	}
	for _, tc := range []struct {
		name string
		args []string
		want []int
	}{
		{"no filter", nil, []int{1001, 1002, 1003, 1004}},
		{"--me", []string{"--me"}, []int{1001, 1002}},
		{"-u other", []string{"-u", "other"}, []int{1003, 1004}},
		{"--user=spike", []string{"--user=spike"}, []int{1001, 1002}},
		{"--name=alpha", []string{"--name=alpha"}, []int{1001, 1003}},
		{"-n alpha", []string{"-n", "alpha"}, []int{1001, 1003}},
		{"--me --name=alpha", []string{"--me", "--name=alpha"}, []int{1001}},
		{"-u other --name alpha", []string{"-u", "other", "--name", "alpha"}, []int{1003}},
		{"-j 1002,1003", []string{"-j", "1002,1003"}, []int{1002, 1003}},
		{"-X -D keep their meaning", []string{"-X", "-D", "--me"}, []int{1001, 1002}},
	} {
		if got := ids(t, r.sacct(tc.args...)); !sameInts(got, tc.want) {
			t.Errorf("sacct %s: ids %v, want %v", tc.name, got, tc.want)
		}
	}
}

// 3. sacct's default start time is midnight today.
func TestSacctDefaultStartIsMidnight(t *testing.T) {
	r := newRig(t)
	now := time.Date(2026, 10, 30, 10, 0, 0, 0, time.UTC)

	r.clk.Set(time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC)) // yesterday
	yesterday := r.sbatch("--job-name=old")
	r.ctl("start", "1001")
	r.clk.Set(time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC))
	r.ctl("end", "1001")

	r.clk.Set(now)
	today := r.sbatch("--job-name=new")
	if yesterday != 1001 || today != 1002 {
		t.Fatalf("ids %d %d", yesterday, today)
	}

	for _, tc := range []struct {
		name string
		args []string
		want []int
	}{
		{"default start", nil, []int{1002}},
		{"-S now-2days", []string{"-S", "now-2days"}, []int{1001, 1002}},
		{"-S now-1day", []string{"-S", "now-1day"}, []int{1001, 1002}},
		{"--starttime=2026-10-29", []string{"--starttime=2026-10-29"}, []int{1001, 1002}},
		{"-S 2026-10-30", []string{"-S", "2026-10-30"}, []int{1002}},
		{"-S at the end instant", []string{"-S", "2026-10-29T12:00:00"}, []int{1001, 1002}},
		{"-S just after the end", []string{"-S", "2026-10-29T12:00:01"}, []int{1002}},
		{"-j without -S reads from the epoch", []string{"-j", "1001"}, []int{1001}},
		{"-E now-1day excludes today's job", []string{"-S", "now-2days", "-E", "now-1day"}, []int{1001}},
		{"-E before any submission", []string{"-S", "now-3days", "-E", "now-2days"}, []int{}},
	} {
		if got := ids(t, r.sacct(tc.args...)); !sameInts(got, tc.want) {
			t.Errorf("%s: ids %v, want %v", tc.name, got, tc.want)
		}
	}
}

// The default start is midnight in the zone of $TZ.
func TestSacctDefaultStartUsesTZ(t *testing.T) {
	// 2026-10-30 02:00 UTC is 2026-10-29 22:00 in New York (EDT, UTC-4).
	// A job that ended 2026-10-29 05:00 UTC (01:00 EDT) is "today" there.
	for _, tc := range []struct {
		tz   string
		want []int
	}{
		{"", []int{}}, // UTC: midnight today is 2026-10-30 00:00Z, the job ended before it
		{"America/New_York", []int{1001}},
	} {
		r := newRig(t)
		if tc.tz != "" {
			r.env["TZ"] = tc.tz
		}
		r.clk.Set(time.Date(2026, 10, 29, 3, 0, 0, 0, time.UTC))
		r.sbatch("--job-name=x")
		r.ctl("start", "1001")
		r.clk.Set(time.Date(2026, 10, 29, 5, 0, 0, 0, time.UTC))
		r.ctl("end", "1001")
		r.clk.Set(time.Date(2026, 10, 30, 2, 0, 0, 0, time.UTC))
		if got := ids(t, r.sacct()); !sameInts(got, tc.want) {
			t.Errorf("TZ=%q: ids %v, want %v", tc.tz, got, tc.want)
		}
	}
}

// 4. FAKE_SLURM_ACCT_DELAY: sacct sees each job as it was at now - delay.
func TestAcctDelay(t *testing.T) {
	r := newRig(t)
	r.env["FAKE_SLURM_ACCT_DELAY"] = "5"

	r.sbatch("--job-name=b") // submitted at t0
	r.clk.Advance(time.Second)
	if got := ids(t, r.sacct()); len(got) != 0 {
		t.Errorf("job submitted 1 s ago, delay 5: sacct lists %v, want none", got)
	}
	if got := ids(t, r.squeue()); !sameInts(got, []int{1001}) {
		t.Errorf("squeue is not delayed: ids %v, want [1001]", got)
	}

	r.ctl("start", "1001") // t0+1
	r.clk.Advance(9 * time.Second)
	r.ctl("end", "1001") // t0+10
	r.clk.Advance(2 * time.Second)

	// now = t0+12; sacct looks at t0+7: the job was RUNNING.
	resp := r.sacct()
	js := jobs(t, resp)
	if len(js) != 1 {
		t.Fatalf("sacct jobs %d, want 1", len(js))
	}
	if s := firstState(t, js[0], "state"); s != "RUNNING" {
		t.Errorf("job that ended 2 s ago, delay 5: sacct state %s, want RUNNING", s)
	}
	tm := js[0]["time"].(map[string]any)
	if tm["end"].(float64) != 0 {
		t.Errorf("end %v, want 0 (not ended yet as of now-5)", tm["end"])
	}
	if want := float64(t0.Add(time.Second).Unix()); tm["start"].(float64) != want {
		t.Errorf("start %v, want %v", tm["start"], want)
	}
	if tm["elapsed"].(float64) != 6 { // t0+7 minus t0+1
		t.Errorf("elapsed %v, want 6", tm["elapsed"])
	}
	if s := firstState(t, jobs(t, r.squeue())[0], "job_state"); s != "COMPLETED" {
		t.Errorf("squeue state %s, want COMPLETED (squeue is live)", s)
	}

	r.clk.Advance(3 * time.Second) // now = t0+15, as-of t0+10: the end is visible
	js = jobs(t, r.sacct())
	if s := firstState(t, js[0], "state"); s != "COMPLETED" {
		t.Errorf("after the delay: state %s, want COMPLETED", s)
	}
}

// 5. requeue shows in squeue and, per version and -D, in sacct.
func TestRequeue(t *testing.T) {
	for _, version := range []string{"v0.0.42", "v0.0.40"} {
		t.Run(version, func(t *testing.T) {
			r := newRig(t)
			r.env["FAKE_SLURM_DATA_PARSER"] = version
			r.sbatch("--job-name=rq", "--no-requeue") // the fake requeues it anyway
			r.clk.Advance(5 * time.Second)
			r.ctl("start", "1001")
			r.clk.Advance(5 * time.Second)
			requeuedAt := r.clk.Now()
			r.ctl("requeue", "1001")

			sq := jobs(t, r.squeue())
			if len(sq) != 1 {
				t.Fatalf("squeue jobs %d, want 1", len(sq))
			}
			if sq[0]["restart_cnt"].(float64) != 1 {
				t.Errorf("squeue restart_cnt %v, want 1", sq[0]["restart_cnt"])
			}
			state := sq[0]["job_state"].([]any)
			if len(state) != 2 || state[0] != "PENDING" || state[1] != "REQUEUED" {
				t.Errorf("squeue job_state %v, want [PENDING REQUEUED]", state)
			}
			if sq[0]["state_reason"] != "BeginTime" {
				t.Errorf("state_reason %v, want BeginTime", sq[0]["state_reason"])
			}
			if got := sq[0]["submit_time"].(map[string]any)["number"].(float64); got != float64(requeuedAt.Unix()) {
				t.Errorf("new submit_time %v, want the requeue time %d", got, requeuedAt.Unix())
			}

			one := jobs(t, r.sacct())
			if len(one) != 1 {
				t.Fatalf("sacct without -D: %d records, want 1", len(one))
			}
			if s := firstState(t, one[0], "state"); s != "PENDING" {
				t.Errorf("sacct latest state %s, want PENDING", s)
			}
			two := jobs(t, r.sacct("-D"))
			if len(two) != 2 {
				t.Fatalf("sacct -D: %d records, want 2", len(two))
			}
			if s := firstState(t, two[0], "state"); s != "REQUEUED" {
				t.Errorf("first record state %s, want REQUEUED", s)
			}
			if s := firstState(t, two[1], "state"); s != "PENDING" {
				t.Errorf("second record state %s, want PENDING", s)
			}
			t0s := float64(t0.Unix())
			first := two[0]["time"].(map[string]any)
			if first["submission"] != t0s || first["start"] != t0s+5 || first["end"] != t0s+10 {
				t.Errorf("first record times %v, want submit/start/end = t0, t0+5, t0+10", first)
			}

			for i, j := range append(one, two...) {
				_, has := j["restart_cnt"]
				if want := version == "v0.0.42"; has != want {
					t.Errorf("%s sacct record %d: restart_cnt present = %v, want %v", version, i, has, want)
				}
			}
			if _, has := sq[0]["restart_cnt"]; !has {
				t.Errorf("%s squeue job lacks restart_cnt", version)
			}

			r.ctl("start", "1001")
			js := jobs(t, r.squeue())
			if st := js[0]["job_state"].([]any); len(st) != 1 || st[0] != "RUNNING" {
				t.Errorf("after the restart: job_state %v, want [RUNNING]", st)
			}
			if js[0]["restart_cnt"].(float64) != 1 {
				t.Errorf("restart_cnt %v, want 1", js[0]["restart_cnt"])
			}
		})
	}
}

// 6. --no-requeue sets requeue=false; the whole argv is recorded.
func TestNoRequeueAndArgv(t *testing.T) {
	r := newRig(t)
	r.sbatch("--job-name=keep", "--no-requeue", "--comment=hello", "--mem=4G", "job.sh")
	r.sbatch("--job-name=rq", "--requeue", "--no-requeue", "--requeue", "job.sh")
	r.sbatch("--job-name=default", "job.sh")

	js := jobs(t, r.squeue())
	for i, want := range []bool{false, true, true} {
		if js[i]["requeue"] != want {
			t.Errorf("job %d requeue %v, want %v", 1001+i, js[i]["requeue"], want)
		}
	}
	if js[0]["comment"] != "hello" {
		t.Errorf("comment %v, want hello", js[0]["comment"])
	}

	dump := r.ctl("dump")
	for _, want := range []string{`"sbatch"`, `"--parsable"`, `"--job-name=keep"`, `"--no-requeue"`, `"--mem=4G"`, `"job.sh"`} {
		if !strings.Contains(dump, want) {
			t.Errorf("state dump lacks %s in the recorded argv", want)
		}
	}
	// the log carries the same argv
	var submitted []string
	for _, ev := range r.logEvents() {
		if ev["event"] == "sbatch_submitted" {
			for _, a := range ev["argv"].([]any) {
				submitted = append(submitted, a.(string))
			}
			break
		}
	}
	if strings.Join(submitted, " ") != "sbatch --parsable --job-name=keep --no-requeue --comment=hello --mem=4G job.sh" {
		t.Errorf("logged argv %v", submitted)
	}
}
