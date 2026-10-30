package fakeslurm

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"
)

// wantHold asserts the squeue hold fields of one job as Slurm's data_parser
// reports them: hold is true exactly when priority is 0.
func wantHold(t *testing.T, what string, j map[string]any, held bool, reason string) {
	t.Helper()
	if j["hold"] != held {
		t.Errorf("%s: hold = %v, want %v", what, j["hold"], held)
	}
	if held {
		wantNV(t, what+" priority", j["priority"], true, 0)
	} else {
		wantNV(t, what+" priority", j["priority"], true, 1000)
	}
	if j["state_reason"] != reason {
		t.Errorf("%s: state_reason = %v, want %s", what, j["state_reason"], reason)
	}
}

// squeueJob returns one job of a squeue response by id.
func (r *rig) squeueJob(id int) map[string]any {
	r.t.Helper()
	js := jobs(r.t, r.squeue("-j", strconv.Itoa(id)))
	if len(js) != 1 {
		r.t.Fatalf("squeue -j %d lists %d jobs, want 1", id, len(js))
	}
	return js[0]
}

// stateJob reads the job straight from the state dump.
func (r *rig) stateJob(id int) map[string]any {
	r.t.Helper()
	var dump struct {
		Jobs []map[string]any `json:"jobs"`
	}
	if err := json.Unmarshal([]byte(r.ctl("dump")), &dump); err != nil {
		r.t.Fatal(err)
	}
	for _, j := range dump.Jobs {
		if j["id"] == float64(id) {
			return j
		}
	}
	r.t.Fatalf("job %d is not in the state dump", id)
	return nil
}

// lastEvent is the newest log event with the given name.
func (r *rig) lastEvent(name string) map[string]any {
	r.t.Helper()
	var last map[string]any
	for _, ev := range r.logEvents() {
		if ev["event"] == name {
			last = ev
		}
	}
	if last == nil {
		r.t.Fatalf("no %s event in the log", name)
	}
	return last
}

// sbatch --hold and -H register a job on a user hold.
func TestSbatchHold(t *testing.T) {
	for _, flag := range []string{"--hold", "-H"} {
		t.Run(flag, func(t *testing.T) {
			r := newRig(t)
			held := r.sbatch(flag, "--job-name=h", "x.sh")
			free := r.sbatch("--job-name=f", "x.sh")

			j := r.stateJob(held)
			if j["held"] != true {
				t.Errorf("state Held = %v, want true", j["held"])
			}
			if j["execs"].([]any)[0].(map[string]any)["reason"] != "JobHeldUser" {
				t.Errorf("hold reason = %v, want JobHeldUser", j["execs"])
			}
			if r.stateJob(free)["held"] != false {
				t.Errorf("a job submitted without --hold is held")
			}

			sq := r.squeueJob(held)
			wantStrings(t, "job_state", sq["job_state"], "PENDING")
			wantHold(t, "held job", sq, true, "JobHeldUser")
			wantHold(t, "free job", r.squeueJob(free), false, "None")

			// The log marks only the held submission.
			var notes []any
			for _, ev := range r.logEvents() {
				if ev["event"] == "sbatch_submitted" {
					notes = append(notes, ev["note"])
				}
			}
			if len(notes) != 2 || notes[0] != "held" || notes[1] != nil {
				t.Errorf("sbatch_submitted notes %v, want [held <absent>]", notes)
			}
		})
	}
}

func TestSbatchHoldCombinedWithOtherOptions(t *testing.T) {
	r := newRig(t)
	id := r.sbatch("-J", "n", "-H", "-t", "5", "--mem", "1G", "x.sh")
	wantHold(t, "job", r.squeueJob(id), true, "JobHeldUser")
	if name := r.squeueJob(id)["name"]; name != "n" {
		t.Errorf("name %v, want n", name)
	}
	// -H does not swallow the script path.
	if got := r.stateJob(id)["script"]; got != "x.sh" {
		t.Errorf("script %v, want x.sh", got)
	}
}

// 3. scontrol release, one case per Slurm outcome.
func TestScontrolRelease(t *testing.T) {
	r := newRig(t)
	userHeld := r.sbatch("-H", "--job-name=u")
	adminHeld := r.sbatch("-H", "--job-name=a")
	r.ctl("hold-admin", strconv.Itoa(adminHeld))
	free := r.sbatch("--job-name=free")
	running := r.sbatch("--job-name=run")
	r.ctl("start", strconv.Itoa(running))
	ended := r.sbatch("--job-name=end")
	r.ctl("end", strconv.Itoa(ended))
	cancelledHeld := r.sbatch("-H", "--job-name=ch")
	r.run("scancel", strconv.Itoa(cancelledHeld))

	const (
		invalid = "scontrol: error: Invalid job id specified\n"
		done    = "scontrol: error: Job has already finished\n"
		denied  = "scontrol: error: Access/permission denied\n"
	)
	for _, tc := range []struct {
		name    string
		arg     string
		exit    int
		stderr  string
		note    string
		held    bool // squeue hold afterwards
		reason  string
		pending bool // check the squeue fields at all
	}{
		{"user hold is cleared", strconv.Itoa(userHeld), 0, "", "", false, "None", true},
		{"release again is ignored", strconv.Itoa(userHeld), 0, "", "already released", false, "None", true},
		{"admin hold is denied", strconv.Itoa(adminHeld), 1, denied, "", true, "JobHeldAdmin", true},
		{"not held (pending)", strconv.Itoa(free), 0, "", "already released", false, "None", true},
		{"not held (running)", strconv.Itoa(running), 0, "", "already released", false, "None", true},
		{"ended job", strconv.Itoa(ended), 1, done, "", false, "None", false},
		{"cancelled held job", strconv.Itoa(cancelledHeld), 1, done, "", false, "None", false},
		{"unknown id", "9999", 1, invalid, "", false, "", false},
		{"not a number", "abc", 1, invalid, "", false, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, errOut, code := r.run("scontrol", "release", tc.arg)
			if code != tc.exit || errOut != tc.stderr || out != "" {
				t.Errorf("exit %d stdout %q stderr %q, want exit %d stderr %q", code, out, errOut, tc.exit, tc.stderr)
			}
			ev := r.lastEvent("scontrol_release")
			if ev["cmd"] != "scontrol" || ev["exit"] != float64(tc.exit) {
				t.Errorf("event %v, want cmd scontrol and exit %d", ev, tc.exit)
			}
			if id, err := strconv.Atoi(tc.arg); err == nil && ev["job_id"] != float64(id) {
				t.Errorf("event job_id %v, want %d", ev["job_id"], id)
			}
			if tc.note == "" && ev["note"] != nil || tc.note != "" && ev["note"] != tc.note {
				t.Errorf("event note %v, want %q", ev["note"], tc.note)
			}
			if id, err := strconv.Atoi(tc.arg); err == nil && tc.pending {
				wantHold(t, "after release", r.squeueJob(id), tc.held, tc.reason)
			}
		})
	}
	// the released job can start now; the admin-held one cannot
	r.ctl("start", strconv.Itoa(userHeld))
	if _, _, code := r.run("fakeslurmctl", "start", strconv.Itoa(adminHeld)); code != 1 {
		t.Errorf("starting an admin-held job: exit %d, want 1", code)
	}
}

func TestScontrolReleasePurgedJob(t *testing.T) {
	r := newRig(t)
	id := r.sbatch("-H")
	r.run("scancel", strconv.Itoa(id)) // cancelled at t0, MinJobAge 10 s
	r.clk.Advance(10 * time.Second)
	_, errOut, code := r.run("scontrol", "release", strconv.Itoa(id))
	if want := "scontrol: error: Invalid job id specified\n"; code != 1 || errOut != want {
		t.Errorf("purged job: exit %d stderr %q, want exit 1 and %q", code, errOut, want)
	}
}

// Several ids: comma lists and separate arguments; every id gets its own
// event and a failure of one does not stop the others.
func TestScontrolReleaseSeveralIDs(t *testing.T) {
	r := newRig(t)
	a := r.sbatch("-H")
	b := r.sbatch("-H")
	c := r.sbatch("-H")
	d := r.sbatch("-H")

	if _, errOut, code := r.run("scontrol", "release", strconv.Itoa(a)+","+strconv.Itoa(b)); code != 0 || errOut != "" {
		t.Fatalf("comma list: exit %d stderr %q", code, errOut)
	}
	for _, id := range []int{a, b} {
		wantHold(t, "comma list", r.squeueJob(id), false, "None")
	}
	before := r.countEvents("scontrol_release")
	_, errOut, code := r.run("scontrol", "release", "9999", strconv.Itoa(c), strconv.Itoa(d)+",9998")
	if want := "scontrol: error: Invalid job id specified\nscontrol: error: Invalid job id specified\n"; code != 1 || errOut != want {
		t.Errorf("mixed ids: exit %d stderr %q, want exit 1 and %q", code, errOut, want)
	}
	if got := r.countEvents("scontrol_release") - before; got != 4 {
		t.Errorf("%d scontrol_release events for 4 ids", got)
	}
	for _, id := range []int{c, d} {
		wantHold(t, "after a mixed release", r.squeueJob(id), false, "None")
	}
}

func TestScontrolOtherSubcommandsAreRefused(t *testing.T) {
	r := newRig(t)
	id := r.sbatch("-H")
	for _, args := range [][]string{
		{},
		{"show", "job", strconv.Itoa(id)},
		{"hold", strconv.Itoa(id)},
		{"update", "JobId=" + strconv.Itoa(id), "Priority=0"},
		{"requeue", strconv.Itoa(id)},
	} {
		out, errOut, code := r.run(append([]string{"scontrol"}, args...)...)
		if code != 1 || out != "" || errOut == "" {
			t.Errorf("scontrol %v: exit %d stdout %q stderr %q, want exit 1, a usage error, no stdout", args, code, out, errOut)
		}
	}
	// `scontrol release` without an id is a usage error too, and nothing changes
	if _, errOut, code := r.run("scontrol", "release"); code != 1 || errOut == "" {
		t.Errorf("scontrol release without ids: exit %d stderr %q", code, errOut)
	}
	wantHold(t, "after refused subcommands", r.squeueJob(id), true, "JobHeldUser")
	// scontrol is also reachable through the multi-call name
	if _, _, code := r.run("fakeslurm", "scontrol", "release", strconv.Itoa(id)); code != 0 {
		t.Errorf("fakeslurm scontrol release: exit %d", code)
	}
}

// 4. A held job never starts.
func TestCtlStartRefusesHeldJobs(t *testing.T) {
	r := newRig(t)
	user := r.sbatch("-H")
	admin := r.sbatch("-H")
	r.ctl("hold-admin", strconv.Itoa(admin))
	for _, id := range []int{user, admin} {
		_, errOut, code := r.run("fakeslurmctl", "start", strconv.Itoa(id))
		if code != 1 || !strings.Contains(errOut, "held") {
			t.Errorf("start of held job %d: exit %d stderr %q, want exit 1 and a message saying it is held", id, code, errOut)
		}
		wantStrings(t, "job_state after a refused start", r.squeueJob(id)["job_state"], "PENDING")
	}
	r.run("scontrol", "release", strconv.Itoa(user))
	r.ctl("start", strconv.Itoa(user))
	wantStrings(t, "job_state after release and start", r.squeueJob(user)["job_state"], "RUNNING")
}
