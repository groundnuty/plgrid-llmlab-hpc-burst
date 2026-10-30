package fakeslurm

import (
	"encoding/json"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// lockFree reports whether <state>.lock can be taken right now.
func lockFree(t *testing.T, statePath string) bool {
	t.Helper()
	f, err := os.OpenFile(statePath+".lock", os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		t.Errorf("open lock: %v", err)
		return false
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return false
	}
	return true
}

// 8. Concurrent sbatch calls: distinct ids, one sbatch_submitted line each,
// and never two commands inside the lock at once. The in-lock counter makes
// the test fail when the flock is removed: without the lock, goroutines yield
// inside the critical section and overlap.
func TestConcurrentSbatch(t *testing.T) {
	const n = 8
	r := newRig(t)
	var inside, maxInside atomic.Int32
	r.inLock = func() func() {
		cur := inside.Add(1)
		for {
			m := maxInside.Load()
			if cur <= m || maxInside.CompareAndSwap(m, cur) {
				break
			}
		}
		runtime.Gosched()
		time.Sleep(2 * time.Millisecond) // real time: let the others reach the lock
		return func() { inside.Add(-1) }
	}

	var wg sync.WaitGroup
	got := make([]string, n)
	codes := make([]int, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			env, out, _ := r.Env()
			codes[i] = Main(env, []string{"sbatch", "--parsable", "--job-name=c"})
			got[i] = strings.TrimSpace(out.String())
		}()
	}
	wg.Wait()

	if m := maxInside.Load(); m != 1 {
		t.Errorf("%d commands were inside the lock at once, want 1", m)
	}
	seen := map[string]bool{}
	for i, id := range got {
		if codes[i] != 0 {
			t.Errorf("sbatch %d exited %d", i, codes[i])
		}
		if seen[id] {
			t.Errorf("job id %s handed out twice", id)
		}
		seen[id] = true
	}
	for id := 1001; id < 1001+n; id++ {
		if !seen[strconv.Itoa(id)] {
			t.Errorf("job id %d missing from %v", id, got)
		}
	}
	if c := r.countEvents("sbatch_submitted"); c != n {
		t.Errorf("%d sbatch_submitted lines, want %d", c, n)
	}
	if c := r.countEvents("sbatch_start"); c != n {
		t.Errorf("%d sbatch_start lines, want %d", c, n)
	}
	if out := strings.TrimSpace(r.ctl("count-sbatch")); out != "8" {
		t.Errorf("count-sbatch printed %q, want 8", out)
	}
	if out := strings.TrimSpace(r.ctl("count-sbatch", "c")); out != "8" {
		t.Errorf("count-sbatch c printed %q, want 8", out)
	}
	if out := strings.TrimSpace(r.ctl("count-sbatch", "nope")); out != "0" {
		t.Errorf("count-sbatch nope printed %q, want 0", out)
	}
}

// The delays and the block run with the lock released, after their log events.
func TestSbatchHooksRunOutsideTheLock(t *testing.T) {
	r := newRig(t)
	dir := t.TempDir()
	marker := dir + "/marker.json"
	r.env["FAKE_SLURM_SBATCH_DELAY_BEFORE"] = "1.5"
	r.env["FAKE_SLURM_SBATCH_DELAY_AFTER"] = "2"
	r.env["FAKE_SLURM_SBATCH_BLOCK_MARKER"] = marker
	state := r.env["FAKE_SLURM_STATE"]

	var slept []time.Duration
	var phases []string
	r.sleep = func(d time.Duration) {
		slept = append(slept, d)
		if !lockFree(t, state) {
			t.Errorf("lock held during the %v sleep", d)
		}
		switch len(slept) {
		case 1: // before: sbatch_start is logged, nothing is registered
			phases = append(phases, "before")
			if r.countEvents("sbatch_start") != 1 || r.countEvents("sbatch_submitted") != 0 {
				t.Errorf("before-delay: log %v, want sbatch_start only", r.logEvents())
			}
			if _, err := os.Stat(state); err == nil {
				t.Errorf("a job was registered before the before-delay ended")
			}
		case 2: // after: registered, marker already there
			phases = append(phases, "after")
			if r.countEvents("sbatch_submitted") != 1 {
				t.Errorf("after-delay: no sbatch_submitted in the log")
			}
		}
	}
	blocked := false
	r.block = func() {
		blocked = true
		phases = append(phases, "block")
		if !lockFree(t, state) {
			t.Errorf("lock held while blocked")
		}
		if r.countEvents("sbatch_submitted") != 1 {
			t.Errorf("blocked without an sbatch_submitted event")
		}
		b, err := os.ReadFile(marker)
		if err != nil {
			t.Fatalf("marker not written before blocking: %v", err)
		}
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatalf("marker %q is not JSON: %v", b, err)
		}
		if m["pid"] != 4242.0 || m["job_id"] != 1001.0 || len(m) != 2 {
			t.Errorf("marker = %v, want {pid: 4242, job_id: 1001}", m)
		}
	}

	out, errOut, code := r.run("sbatch", "--parsable", "--job-name=hang")
	if !blocked {
		t.Fatal("the block hook never ran")
	}
	if out != "" || errOut != "" || code != 0 {
		t.Errorf("a blocked sbatch printed %q / %q (exit %d); it must print nothing", out, errOut, code)
	}
	if want := []time.Duration{1500 * time.Millisecond, 2 * time.Second}; len(slept) != 2 || slept[0] != want[0] || slept[1] != want[1] {
		t.Errorf("slept %v, want %v", slept, want)
	}
	if strings.Join(phases, ",") != "before,after,block" {
		t.Errorf("phases %v, want before,after,block", phases)
	}
	// the job is registered although the caller never saw its id
	if ids := ids(t, r.squeue()); len(ids) != 1 || ids[0] != 1001 {
		t.Errorf("registered ids %v, want [1001]", ids)
	}
}

func TestSbatchDelayAfterStillPrintsTheID(t *testing.T) {
	r := newRig(t)
	r.env["FAKE_SLURM_SBATCH_DELAY_AFTER"] = "3"
	var slept time.Duration
	r.sleep = func(d time.Duration) { slept = d }
	out, _, code := r.run("sbatch", "x.sh")
	if code != 0 || out != "Submitted batch job 1001\n" || slept != 3*time.Second {
		t.Errorf("out %q exit %d slept %v", out, code, slept)
	}
}

func TestSbatchFail(t *testing.T) {
	r := newRig(t)
	r.env["FAKE_SLURM_SBATCH_FAIL"] = "1"
	out, errOut, code := r.run("sbatch", "--parsable", "x.sh")
	if want := "sbatch: error: Batch job submission failed: fake failure\n"; code != 1 || out != "" || errOut != want {
		t.Errorf("exit %d stdout %q stderr %q, want exit 1 and stderr %q", code, out, errOut, want)
	}
	if n := len(ids(t, r.squeue())); n != 0 {
		t.Errorf("a failed sbatch registered %d jobs", n)
	}
	if r.countEvents("sbatch_start") != 1 || r.countEvents("sbatch_failed") != 1 || r.countEvents("sbatch_submitted") != 0 {
		t.Errorf("events %v, want one sbatch_start and one sbatch_failed", r.logEvents())
	}
	if c := strings.TrimSpace(r.ctl("count-sbatch")); c != "0" {
		t.Errorf("count-sbatch %s, want 0", c)
	}
	// the next job after a failure still gets the first id
	delete(r.env, "FAKE_SLURM_SBATCH_FAIL")
	if id := r.sbatch("x.sh"); id != 1001 {
		t.Errorf("first id after a failure %d, want 1001", id)
	}
}

func TestSbatchOutputFormats(t *testing.T) {
	r := newRig(t)
	if out, _, _ := r.run("sbatch", "--parsable", "j.sh"); out != "1001\n" {
		t.Errorf("--parsable printed %q, want %q", out, "1001\n")
	}
	if out, _, _ := r.run("sbatch", "j.sh"); out != "Submitted batch job 1002\n" {
		t.Errorf("plain sbatch printed %q", out)
	}
}

// Option forms the provider may use.
func TestSbatchOptionForms(t *testing.T) {
	r := newRig(t)
	r.sbatch("-J", "n1", "-p", "part1", "-A", "acc1", "--time=00:45:00", "s.sh")
	r.sbatch("--job-name", "n2", "--partition=part2", "--account=acc2", "--time", "2", "--mem", "4G", "-n", "4", "s.sh")
	r.sbatch("-Jn3", "--wrap=sleep 5", "--export=ALL")
	r.sbatch("--time=1-02:03:04", "--wrap", "true")

	js := jobs(t, r.squeue())
	for i, want := range []struct {
		name, part, acct string
		limit            float64
	}{
		{"n1", "part1", "acc1", 45}, {"n2", "part2", "acc2", 2}, {"n3", "batch", "default", 0}, {"wrap", "batch", "default", 1564},
	} {
		j := js[i]
		if j["name"] != want.name || j["partition"] != want.part || j["account"] != want.acct {
			t.Errorf("job %d = %v/%v/%v, want %v/%v/%v", 1001+i, j["name"], j["partition"], j["account"], want.name, want.part, want.acct)
		}
		if got := j["time_limit"].(map[string]any)["number"]; got != want.limit {
			t.Errorf("job %d time_limit %v, want %v", 1001+i, got, want.limit)
		}
	}
	if !strings.Contains(r.ctl("dump"), `"script": "s.sh"`) {
		t.Errorf("the trailing script path was not taken as the script, even after '--mem 4G -n 4'")
	}
}

// Every command writes the documented log line.
func TestLogEvents(t *testing.T) {
	r := newRig(t)
	r.env["FAKE_SLURM_CALLER"] = "unit-test"
	r.sbatch("--job-name=l")
	r.squeue()
	r.sacct()
	r.run("scancel", "1001")
	r.ctl("count-sbatch")
	evs := r.logEvents()
	var names []string
	for _, ev := range evs {
		names = append(names, ev["event"].(string))
	}
	if got := strings.Join(names, ","); got != "sbatch_start,sbatch_submitted,squeue,sacct,scancel,ctl" {
		t.Fatalf("events %s", got)
	}
	for i, ev := range evs {
		if _, err := time.Parse(time.RFC3339Nano, ev["ts"].(string)); err != nil || !strings.HasSuffix(ev["ts"].(string), "Z") {
			t.Errorf("event %d ts %v is not RFC3339Nano UTC", i, ev["ts"])
		}
		if ev["unix_ms"] != float64(t0.UnixMilli()) || ev["caller"] != "unit-test" || ev["pid"] != 4242.0 {
			t.Errorf("event %d: unix_ms/caller/pid = %v/%v/%v", i, ev["unix_ms"], ev["caller"], ev["pid"])
		}
		if _, ok := ev["argv"].([]any); !ok {
			t.Errorf("event %d argv is %T", i, ev["argv"])
		}
		if _, ok := ev["exit"]; !ok {
			t.Errorf("event %d has no exit key", i)
		}
	}
	if _, has := evs[0]["job_id"]; has || evs[0]["exit"] != nil || evs[0]["cmd"] != "sbatch" {
		t.Errorf("sbatch_start = %v, want no job_id and exit null", evs[0])
	}
	if evs[1]["job_id"] != 1001.0 || evs[1]["exit"] != 0.0 {
		t.Errorf("sbatch_submitted = %v, want job_id 1001 and exit 0", evs[1])
	}
	if evs[4]["job_id"] != 1001.0 || evs[4]["cmd"] != "scancel" || evs[5]["cmd"] != "fakeslurmctl" {
		t.Errorf("scancel/ctl events = %v / %v", evs[4], evs[5])
	}
}

// fakeslurmctl drives job lifecycles and refuses impossible moves.
func TestCtl(t *testing.T) {
	r := newRig(t)
	r.sbatch("--job-name=c")
	for _, tc := range []struct {
		args []string
		code int
	}{
		{[]string{"start", "9999"}, 1},
		{[]string{"start", "x"}, 1},
		{[]string{"end", "1001", "BOGUS"}, 1},
		{[]string{"end", "1001", "REQUEUED"}, 1},
		{[]string{"bogus"}, 1},
		{[]string{}, 1},
	} {
		if _, _, code := r.run(append([]string{"fakeslurmctl"}, tc.args...)...); code != tc.code {
			t.Errorf("fakeslurmctl %v: exit %d, want %d", tc.args, code, tc.code)
		}
	}
	r.ctl("start", "1001")
	if _, _, code := r.run("fakeslurmctl", "start", "1001"); code != 1 {
		t.Errorf("starting a running job: exit %d, want 1", code)
	}
	r.ctl("end", "1001", "FAILED")
	if _, _, code := r.run("fakeslurmctl", "end", "1001"); code != 1 {
		t.Errorf("ending an ended job: exit %d, want 1", code)
	}
	var dump struct {
		NextID int `json:"next_id"`
		Jobs   []struct {
			ID int `json:"id"`
		} `json:"jobs"`
	}
	if err := json.Unmarshal([]byte(r.ctl("dump")), &dump); err != nil || dump.NextID != 1002 || len(dump.Jobs) != 1 {
		t.Errorf("dump: %+v, err %v", dump, err)
	}
	// requeue ignores the requeue flag and works on a PENDING job too
	r.sbatch("--no-requeue")
	r.ctl("requeue", "1002")
	if rc := jobs(t, r.squeue("-j", "1002"))[0]["restart_cnt"]; rc != 1.0 {
		t.Errorf("restart_cnt %v after requeue of a --no-requeue job, want 1", rc)
	}
}

func TestEnvironmentValidation(t *testing.T) {
	env := Env{Getenv: func(string) string { return "" }}
	var errOut strings.Builder
	env.Stderr = &errOut
	if code := Main(env, []string{"squeue", "--json"}); code != 2 {
		t.Errorf("exit %d without FAKE_SLURM_STATE, want 2", code)
	}
	for _, want := range []string{"FAKE_SLURM_STATE", "FAKE_SLURM_LOG"} {
		if !strings.Contains(errOut.String(), want) {
			t.Errorf("stderr %q does not name %s", errOut.String(), want)
		}
	}

	r := newRig(t)
	r.env["FAKE_SLURM_DATA_PARSER"] = "v9"
	r.env["FAKE_SLURM_MIN_JOB_AGE"] = "soon"
	_, stderr, code := r.run("squeue", "--json")
	if code != 2 || !strings.Contains(stderr, "FAKE_SLURM_DATA_PARSER") || !strings.Contains(stderr, "FAKE_SLURM_MIN_JOB_AGE") {
		t.Errorf("bad settings: exit %d stderr %q, want exit 2 naming both", code, stderr)
	}
}

func TestDispatch(t *testing.T) {
	r := newRig(t)
	if _, _, code := r.run("/usr/local/bin/sbatch", "--parsable", "x"); code != 0 {
		t.Errorf("sbatch under a full path: exit %d", code)
	}
	if out, _, code := r.run("fakeslurm", "squeue", "--json"); code != 0 || !strings.Contains(out, `"jobs"`) {
		t.Errorf("fallback 'fakeslurm squeue --json': exit %d out %q", code, out)
	}
	if _, _, code := r.run("fakeslurm"); code != 2 {
		t.Errorf("bare fakeslurm: exit %d, want 2", code)
	}
	if _, _, code := r.run("fakeslurm", "bogus"); code != 2 {
		t.Errorf("unknown command: exit %d, want 2", code)
	}
	for _, cmd := range []string{"squeue", "sacct"} {
		if _, errOut, code := r.run(cmd); code != 1 || errOut == "" {
			t.Errorf("%s without --json: exit %d stderr %q, want exit 1 and a message", cmd, code, errOut)
		}
		if _, _, code := r.run(cmd, "--yaml"); code != 1 {
			t.Errorf("%s --yaml: exit %d, want 1", cmd, code)
		}
	}
}
