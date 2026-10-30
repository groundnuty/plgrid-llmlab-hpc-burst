package fakeslurm

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	rejectText = "sbatch: error: Batch job submission failed: Invalid account or account/partition combination specified\n"
	lostText   = "sbatch: error: Batch job submission failed: Socket timed out on send/recv operation\n"
)

func (r *rig) marker(name string) string {
	return filepath.Join(r.t.TempDir(), name)
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// eventNames lists the sbatch events of the log (probing with squeue adds
// squeue events, which are not what these tests look at).
func eventNames(r *rig) string {
	var names []string
	for _, ev := range r.logEvents() {
		if name := ev["event"].(string); strings.HasPrefix(name, "sbatch") {
			names = append(names, name)
		}
	}
	return strings.Join(names, ",")
}

// 7. The gate: nothing is registered while it is closed, exactly one job
// after it opens, and the waiting happens outside the lock.
func TestSbatchGate(t *testing.T) {
	r := newRig(t)
	gate := r.marker("gate")
	r.env["FAKE_SLURM_SBATCH_GATE"] = gate
	r.env["FAKE_SLURM_SBATCH_DELAY_BEFORE"] = "1"

	var slept []time.Duration
	r.sleep = func(d time.Duration) {
		slept = append(slept, d)
		if d != 50*time.Millisecond {
			return // the delay_before sleep, after the gate
		}
		if !lockFree(t, r.env["FAKE_SLURM_STATE"]) {
			t.Errorf("lock held while waiting at the gate")
		}
		if n := len(slept); n <= 3 {
			// gate closed: nothing registered, only the two start events
			if got := eventNames(r); got != "sbatch_start,sbatch_gate_wait" {
				t.Errorf("poll %d: log %q, want sbatch_start,sbatch_gate_wait", n, got)
			}
			if exists(r.env["FAKE_SLURM_STATE"]) {
				t.Errorf("poll %d: a job was registered while the gate is closed", n)
			}
			if got := ids(t, r.squeue()); len(got) != 0 {
				t.Errorf("poll %d: squeue lists %v while the gate is closed", n, got)
			}
		}
		if len(slept) == 3 {
			if err := os.WriteFile(gate, nil, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}

	out, errOut, code := r.run("sbatch", "--parsable", "x.sh")
	if out != "1001\n" || errOut != "" || code != 0 {
		t.Fatalf("sbatch: %q / %q / exit %d", out, errOut, code)
	}
	// three polls of 50 ms, then the existing delay, then the registration
	if want := []time.Duration{50 * time.Millisecond, 50 * time.Millisecond, 50 * time.Millisecond, time.Second}; !equalDurations(slept, want) {
		t.Errorf("slept %v, want %v", slept, want)
	}
	if got := eventNames(r); got != "sbatch_start,sbatch_gate_wait,sbatch_submitted" {
		t.Errorf("log %q, want sbatch_start,sbatch_gate_wait,sbatch_submitted", got)
	}
	if got := ids(t, r.squeue()); !sameInts(got, []int{1001}) {
		t.Errorf("after the gate opened squeue lists %v, want exactly [1001]", got)
	}
	if ev := r.lastEvent("sbatch_gate_wait"); ev["exit"] != nil || ev["cmd"] != "sbatch" {
		t.Errorf("sbatch_gate_wait = %v, want cmd sbatch and exit null", ev)
	}
}

func equalDurations(a, b []time.Duration) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// An open gate is passed without a poll, but the wait is still logged.
func TestSbatchGateAlreadyOpen(t *testing.T) {
	r := newRig(t)
	gate := r.marker("gate")
	if err := os.WriteFile(gate, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	r.env["FAKE_SLURM_SBATCH_GATE"] = gate
	r.sleep = func(d time.Duration) { t.Errorf("slept %v at an open gate", d) }
	if id := r.sbatch("x.sh"); id != 1001 {
		t.Errorf("id %d, want 1001", id)
	}
	if got := eventNames(r); got != "sbatch_start,sbatch_gate_wait,sbatch_submitted" {
		t.Errorf("log %q", got)
	}
}

// A call that fails once does not wait at the gate: the check comes first.
func TestSbatchGateComesAfterFailOnce(t *testing.T) {
	r := newRig(t)
	r.env["FAKE_SLURM_SBATCH_GATE"] = r.marker("never-created")
	r.env["FAKE_SLURM_SBATCH_FAIL_ONCE"] = r.marker("fail")
	r.sleep = func(d time.Duration) { t.Errorf("slept %v: the failing call waited at the gate", d) }
	if _, _, code := r.run("sbatch", "x.sh"); code != 1 {
		t.Errorf("exit %d, want 1", code)
	}
	if got := eventNames(r); got != "sbatch_start,sbatch_failed" {
		t.Errorf("log %q, want sbatch_start,sbatch_failed", got)
	}
}

// 8. FAKE_SLURM_SBATCH_FAIL and FAIL_ONCE fail with the same definite reason.
func TestSbatchFailOnce(t *testing.T) {
	r := newRig(t)
	marker := r.marker("fail-once")
	r.env["FAKE_SLURM_SBATCH_FAIL_ONCE"] = marker

	out, errOut, code := r.run("sbatch", "--parsable", "x.sh")
	if out != "" || errOut != rejectText || code != 1 {
		t.Errorf("first call: stdout %q stderr %q exit %d, want exit 1 and %q", out, errOut, code, rejectText)
	}
	if !exists(marker) {
		t.Errorf("the failing call did not create the marker")
	}
	if got := eventNames(r); got != "sbatch_start,sbatch_failed" {
		t.Errorf("log %q, want sbatch_start,sbatch_failed", got)
	}
	if ev := r.lastEvent("sbatch_failed"); ev["exit"] != 1.0 {
		t.Errorf("sbatch_failed = %v, want exit 1", ev)
	}
	if got := ids(t, r.squeue()); len(got) != 0 {
		t.Errorf("the failed call registered %v", got)
	}
	// later calls go through, and the first id is still 1001
	if id := r.sbatch("x.sh"); id != 1001 {
		t.Errorf("second call id %d, want 1001", id)
	}
	if id := r.sbatch("x.sh"); id != 1002 {
		t.Errorf("third call id %d, want 1002", id)
	}
	if c := r.countEvents("sbatch_failed"); c != 1 {
		t.Errorf("%d sbatch_failed events, want 1", c)
	}
}

func TestSbatchFailOnceMarkerAlreadyThere(t *testing.T) {
	r := newRig(t)
	marker := r.marker("fail-once")
	if err := os.WriteFile(marker, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	r.env["FAKE_SLURM_SBATCH_FAIL_ONCE"] = marker
	if id := r.sbatch("x.sh"); id != 1001 {
		t.Errorf("id %d, want 1001: an existing marker means the failure is spent", id)
	}
}

// Only one of many concurrent calls claims the marker.
func TestSbatchFailOnceConcurrent(t *testing.T) {
	const n = 12
	r := newRig(t)
	r.env["FAKE_SLURM_SBATCH_FAIL_ONCE"] = r.marker("fail-once")
	codes := make([]int, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			env, _, _ := r.Env()
			codes[i] = Main(env, []string{"sbatch", "--parsable", "x.sh"})
		}()
	}
	wg.Wait()
	failed := 0
	for _, c := range codes {
		if c == 1 {
			failed++
		} else if c != 0 {
			t.Errorf("unexpected exit %d", c)
		}
	}
	if failed != 1 || r.countEvents("sbatch_failed") != 1 || r.countEvents("sbatch_submitted") != n-1 {
		t.Errorf("%d failures, %d sbatch_failed, %d submitted; want 1, 1, %d", failed, r.countEvents("sbatch_failed"), r.countEvents("sbatch_submitted"), n-1)
	}
}

// A marker that cannot be created is a fake error (exit 3), not a pass.
func TestSbatchFailOnceUnusableMarker(t *testing.T) {
	r := newRig(t)
	r.env["FAKE_SLURM_SBATCH_FAIL_ONCE"] = filepath.Join(t.TempDir(), "no-such-dir", "marker")
	_, errOut, code := r.run("sbatch", "x.sh")
	if code != 3 || !strings.Contains(errOut, "marker") {
		t.Errorf("exit %d stderr %q, want exit 3 naming the marker", code, errOut)
	}
}

func TestSbatchFailAlways(t *testing.T) {
	r := newRig(t)
	r.env["FAKE_SLURM_SBATCH_FAIL"] = "1"
	for range 2 {
		out, errOut, code := r.run("sbatch", "x.sh")
		if out != "" || errOut != rejectText || code != 1 {
			t.Errorf("FAIL=1: stdout %q stderr %q exit %d, want %q", out, errOut, code, rejectText)
		}
	}
}

// FAIL_ALIGN: the failure lands 30 ms past a whole second (UTC).
func TestSbatchFailAlign(t *testing.T) {
	for _, tc := range []struct {
		name      string
		start     time.Time
		wantSleep time.Duration
	}{
		{"mid second", t0.Add(250 * time.Millisecond), 780 * time.Millisecond},
		{"exact whole second", t0, time.Second + 30*time.Millisecond},
		{"just before the 30 ms mark", t0.Add(999 * time.Millisecond), 31 * time.Millisecond},
		{"just after the 30 ms mark", t0.Add(40 * time.Millisecond), 990 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t)
			r.clk.Set(tc.start)
			r.env["FAKE_SLURM_SBATCH_FAIL_ONCE"] = r.marker("fail-once")
			r.env["FAKE_SLURM_SBATCH_FAIL_ALIGN"] = "1"
			var slept []time.Duration
			r.sleep = func(d time.Duration) {
				slept = append(slept, d)
				if !lockFree(t, r.env["FAKE_SLURM_STATE"]) {
					t.Errorf("lock held during the align sleep")
				}
				r.clk.Advance(d)
			}
			_, errOut, code := r.run("sbatch", "x.sh")
			if code != 1 || errOut != rejectText {
				t.Errorf("exit %d stderr %q", code, errOut)
			}
			if len(slept) != 1 || slept[0] != tc.wantSleep {
				t.Errorf("slept %v, want [%v]", slept, tc.wantSleep)
			}
			ev := r.lastEvent("sbatch_failed")
			if ms := ev["unix_ms"].(float64); int64(ms)%1000 != 30 {
				t.Errorf("sbatch_failed at unix_ms %v, want a time 30 ms past a whole second", ms)
			}
			// the later, succeeding call does not align
			slept = nil
			r.sbatch("x.sh")
			if len(slept) != 0 {
				t.Errorf("the succeeding call slept %v", slept)
			}
		})
	}
	// ALIGN alone (no failing call) changes nothing
	r := newRig(t)
	r.env["FAKE_SLURM_SBATCH_FAIL_ALIGN"] = "1"
	r.sleep = func(d time.Duration) { t.Errorf("slept %v without FAIL_ONCE", d) }
	r.sbatch("x.sh")
}

// 9. The reply is lost after slurmctld accepted the job.
func TestSbatchLostReplyOnce(t *testing.T) {
	r := newRig(t)
	marker := r.marker("lost")
	r.env["FAKE_SLURM_SBATCH_LOST_REPLY_ONCE"] = marker

	out, errOut, code := r.run("sbatch", "--parsable", "-H", "--job-name=lost", "x.sh")
	if out != "" || errOut != lostText || code != 1 {
		t.Errorf("first call: stdout %q stderr %q exit %d, want exit 1 and %q", out, errOut, code, lostText)
	}
	if !exists(marker) {
		t.Errorf("the lost-reply call did not create the marker")
	}
	// the server side logged a normal submission
	if got := eventNames(r); got != "sbatch_start,sbatch_submitted" {
		t.Errorf("log %q, want sbatch_start,sbatch_submitted", got)
	}
	// the job exists, with its hold, although the caller never saw an id
	if got := ids(t, r.squeue()); !sameInts(got, []int{1001}) {
		t.Fatalf("squeue lists %v, want [1001]", got)
	}
	wantHold(t, "lost-reply job", r.squeueJob(1001), true, "JobHeldUser")
	ev := r.lastEvent("sbatch_submitted")
	if ev["job_id"] != 1001.0 || ev["exit"] != 0.0 || ev["note"] != "held" {
		t.Errorf("sbatch_submitted = %v, want job 1001, exit 0, note held", ev)
	}
	if c := strings.TrimSpace(r.ctl("count-sbatch")); c != "1" {
		t.Errorf("count-sbatch %s, want 1", c)
	}

	// later calls work normally
	out, errOut, code = r.run("sbatch", "--parsable", "x.sh")
	if out != "1002\n" || errOut != "" || code != 0 {
		t.Errorf("second call: %q / %q / exit %d, want 1002", out, errOut, code)
	}
}

// A call that fails once does not use up the lost reply, and a lost reply is
// not a failure of the next call.
func TestSbatchFailOnceThenLostReply(t *testing.T) {
	r := newRig(t)
	failM, lostM := r.marker("fail"), r.marker("lost")
	r.env["FAKE_SLURM_SBATCH_FAIL_ONCE"] = failM
	r.env["FAKE_SLURM_SBATCH_LOST_REPLY_ONCE"] = lostM

	if _, errOut, code := r.run("sbatch", "x.sh"); code != 1 || errOut != rejectText {
		t.Errorf("call 1: exit %d stderr %q, want the definite failure", code, errOut)
	}
	if exists(lostM) {
		t.Errorf("the failed call used up the lost-reply marker")
	}
	if got := ids(t, r.squeue()); len(got) != 0 {
		t.Errorf("call 1 registered %v", got)
	}
	if out, errOut, code := r.run("sbatch", "x.sh"); out != "" || errOut != lostText || code != 1 {
		t.Errorf("call 2: %q / %q / exit %d, want the lost reply", out, errOut, code)
	}
	if got := ids(t, r.squeue()); !sameInts(got, []int{1001}) {
		t.Errorf("call 2 registered %v, want [1001]", got)
	}
	if out, _, code := r.run("sbatch", "x.sh"); out != "Submitted batch job 1002\n" || code != 0 {
		t.Errorf("call 3: %q / exit %d, want a normal submission", out, code)
	}
}

// 10. FAKE_SLURM_SQUEUE_ORDER.
func TestSqueueOrder(t *testing.T) {
	r := newRig(t)
	r.sbatch("-J", "a")
	r.sbatch("-J", "b")
	r.sbatch("-J", "a")
	for _, tc := range []struct {
		order string
		args  []string
		want  []int
	}{
		{"", nil, []int{1001, 1002, 1003}},
		{"asc", nil, []int{1001, 1002, 1003}},
		{"desc", nil, []int{1003, 1002, 1001}},
		{"desc", []string{"--name=a"}, []int{1003, 1001}},
		{"desc", []string{"-j", "1001,1002"}, []int{1002, 1001}},
	} {
		if tc.order == "" {
			delete(r.env, "FAKE_SLURM_SQUEUE_ORDER")
		} else {
			r.env["FAKE_SLURM_SQUEUE_ORDER"] = tc.order
		}
		if got := ids(t, r.squeue(tc.args...)); !sameInts(got, tc.want) {
			t.Errorf("order %q args %v: ids %v, want %v", tc.order, tc.args, got, tc.want)
		}
	}
	// sacct is not reordered
	r.env["FAKE_SLURM_SQUEUE_ORDER"] = "desc"
	if got := ids(t, r.sacct()); !sameInts(got, []int{1001, 1002, 1003}) {
		t.Errorf("sacct ids %v under SQUEUE_ORDER=desc, want ascending", got)
	}
	r.env["FAKE_SLURM_SQUEUE_ORDER"] = "sideways"
	if _, errOut, code := r.run("squeue", "--json"); code != 2 || !strings.Contains(errOut, "FAKE_SLURM_SQUEUE_ORDER") {
		t.Errorf("bad order: exit %d stderr %q, want exit 2 naming the variable", code, errOut)
	}
}
