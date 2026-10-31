package fakeslurm

// New, with the shape cases of spikes/s4/fakeslurm/shapes_test.go. The key
// lists are S7's, copied from spikes/s7/slurm-json-real.md (the 128 keys of the
// running-job squeue object); the numbers are worked out by hand from S7's
// values (a 10-minute limit gives end_time - start_time = 600 s).

import (
	"strings"
	"testing"
	"time"
)

// s7SqueueKeys are the keys of the job object in S7's running-job capture.
var s7SqueueKeys = setOf(`account accrue_time admin_comment allocating_node array_job_id array_task_id
array_max_tasks array_task_string association_id batch_features batch_flag batch_host flags burst_buffer
burst_buffer_state cluster cluster_features command comment container container_id contiguous core_spec
thread_spec cores_per_socket billable_tres cpus_per_task cpu_frequency_minimum cpu_frequency_maximum
cpu_frequency_governor cpus_per_tres cron deadline delay_boot dependency derived_exit_code eligible_time
end_time excluded_nodes exit_code extra failed_node features federation_origin federation_siblings_active
federation_siblings_viable gres_detail group_id group_name het_job_id het_job_id_set het_job_offset job_id
job_resources job_size_str job_state last_sched_evaluation licenses mail_type mail_user max_cpus max_nodes
mcs_label memory_per_tres name network nodes nice tasks_per_core tasks_per_tres tasks_per_node
tasks_per_socket tasks_per_board cpus node_count tasks partition prefer memory_per_cpu memory_per_node
minimum_cpus_per_node minimum_tmp_disk_per_node power preempt_time preemptable_time pre_sus_time hold
priority priority_by_partition profile qos reboot required_nodes required_switches requeue resize_time
restart_cnt resv_name scheduled_nodes selinux_context shared sockets_per_board sockets_per_node start_time
state_description state_reason standard_error standard_input standard_output submit_time suspend_time
system_comment time_limit time_minimum threads_per_core tres_bind tres_freq tres_per_job tres_per_node
tres_per_socket tres_per_task tres_req_str tres_alloc_str user_id user_name maximum_switch_wait_time wckey
current_working_directory`)

func setOf(words string) map[string]bool {
	m := map[string]bool{}
	for _, w := range strings.Fields(words) {
		m[w] = true
	}
	return m
}

// The fields the plugin reads (HBEP-4, JSON fields), as S4 and S7 type them.
func TestSqueueJobShapeRunning(t *testing.T) {
	for _, version := range []string{"v0.0.42", "v0.0.40"} {
		t.Run(version, func(t *testing.T) {
			r := newRig(t, Config{DataParser: version})
			id := r.sbatch("--no-requeue --job-name=shape --time=10 --partition=p1 --account=acc --nodes=1 --gres=gpu:1 --cpus-per-task=8 --mem=16384M --output=/s/job.out --error=/s/job.err")
			r.sbatch("--job-name=other")
			r.clock.Advance(10 * time.Second)
			r.start(id)
			r.clock.Advance(20 * time.Second)
			now := float64(r.clock.Now().Unix())
			submit, start := float64(t0.Unix()), float64(t0.Unix()+10)

			resp := r.squeue()
			wantKeys(t, "squeue response", resp, "jobs", "last_backfill", "last_update", "meta", "errors", "warnings")
			wantMeta(t, resp, version, version, "example-user")
			wantNV(t, "last_backfill", resp["last_backfill"], true, now)
			wantNV(t, "last_update", resp["last_update"], true, now)
			js := jobsOf(t, resp)
			j, pend := js[0], js[1]
			wantSubset(t, "squeue job", j, s7SqueueKeys)
			wantSubset(t, "pending squeue job", pend, s7SqueueKeys)
			for _, k := range []string{"job_id", "name", "job_state", "state_reason", "hold", "priority", "restart_cnt",
				"partition", "time_limit", "tres_req_str", "submit_time", "start_time", "end_time"} {
				if _, ok := j[k]; !ok {
					t.Errorf("squeue job lacks %q, a field the plugin reads", k)
				}
			}
			if j["job_id"] != float64(id) || j["name"] != "shape" || j["partition"] != "p1" || j["account"] != "acc" ||
				j["user_name"] != "example-user" || j["state_reason"] != "None" || j["restart_cnt"] != 0.0 ||
				j["requeue"] != false || j["hold"] != false {
				t.Errorf("squeue job scalar fields: %v", j)
			}
			wantStrings(t, "job_state", j["job_state"], "RUNNING")
			wantNV(t, "priority", j["priority"], true, 1000)
			wantNV(t, "time_limit", j["time_limit"], true, 10)
			wantNV(t, "submit_time", j["submit_time"], true, submit)
			wantNV(t, "start_time", j["start_time"], true, start)
			// S7: while RUNNING, end_time is the projected end, start plus the limit.
			wantNV(t, "end_time of a running job", j["end_time"], true, start+600)
			if num(t, j, "end_time")-num(t, j, "start_time") != 10*60 {
				t.Errorf("end_time - start_time = %v, want 600", num(t, j, "end_time")-num(t, j, "start_time"))
			}
			wantNV(t, "node_count", j["node_count"], true, 1)
			wantNV(t, "cpus", j["cpus"], true, 8)
			wantNV(t, "memory_per_node", j["memory_per_node"], true, 16384)
			wantNV(t, "memory_per_cpu", j["memory_per_cpu"], false, 0)
			if j["standard_output"] != "/s/job.out" || j["standard_error"] != "/s/job.err" {
				t.Errorf("output files: %v %v", j["standard_output"], j["standard_error"])
			}
			// The TRES strings, as the brief and S7 write them.
			const tres = "cpu=8,mem=16384M,node=1,billing=8,gres/gpu=1"
			if j["tres_req_str"] != tres || j["tres_alloc_str"] != tres {
				t.Errorf("tres strings %v / %v, want %q", j["tres_req_str"], j["tres_alloc_str"], tres)
			}
			wantExitCode(t, "exit_code of a running job", j["exit_code"], "PENDING", false, 0)

			// The other job is pending: no start, no end, no allocation yet.
			wantStrings(t, "pending job_state", pend["job_state"], "PENDING")
			wantNV(t, "pending start_time", pend["start_time"], false, 0)
			wantNV(t, "pending end_time", pend["end_time"], false, 0)
			wantNV(t, "pending time_limit", pend["time_limit"], false, 0)
			if pend["tres_alloc_str"] != "" || pend["tres_req_str"] != "cpu=1,mem=0M,node=1,billing=1" {
				t.Errorf("pending tres strings %q / %q", pend["tres_req_str"], pend["tres_alloc_str"])
			}
		})
	}
}

// wantMeta checks the meta block: the data_parser value it reports, and the
// Slurm release of the platform's own version.
func wantMeta(t *testing.T, resp map[string]any, parser, platform, user string) {
	t.Helper()
	m, _ := resp["meta"].(map[string]any)
	wantKeys(t, "meta", m, "plugin", "client", "command", "slurm")
	plugin, _ := m["plugin"].(map[string]any)
	wantKeys(t, "meta.plugin", plugin, "type", "name", "data_parser", "accounting_storage")
	if plugin["data_parser"] != "data_parser/"+parser || plugin["accounting_storage"] != "accounting_storage/slurmdbd" {
		t.Errorf("meta.plugin = %v", plugin)
	}
	client, _ := m["client"].(map[string]any)
	wantKeys(t, "meta.client", client, "source", "user", "group")
	if client["user"] != user {
		t.Errorf("meta.client.user = %v, want %s", client["user"], user)
	}
	slurmMeta, _ := m["slurm"].(map[string]any)
	wantKeys(t, "meta.slurm", slurmMeta, "version", "release", "cluster")
	// v0.0.42 is Slurm 24.11 (Helios, S7: 24.11.5); v0.0.40 is 23.11 (Athena: 23.11.7).
	release := map[string]string{"v0.0.42": "24.11.5", "v0.0.40": "23.11.7"}[platform]
	if slurmMeta["release"] != release {
		t.Errorf("meta.slurm.release = %v, want %s", slurmMeta["release"], release)
	}
	for _, k := range []string{"errors", "warnings"} {
		if l, ok := resp[k].([]any); !ok || len(l) != 0 {
			t.Errorf("%s = %#v, want an empty list", k, resp[k])
		}
	}
}

func wantExitCode(t *testing.T, what string, v any, status string, set bool, rc float64) {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Errorf("%s is %T", what, v)
		return
	}
	wantKeys(t, what, m, "status", "return_code", "signal")
	wantStrings(t, what+".status", m["status"], status)
	wantNV(t, what+".return_code", m["return_code"], set, rc)
	sig, _ := m["signal"].(map[string]any)
	wantKeys(t, what+".signal", sig, "id", "name")
	wantNV(t, what+".signal.id", sig["id"], false, 0)
}

func TestSqueueTresStrings(t *testing.T) {
	r := newRig(t, Config{})
	gpu := r.sbatch("--cpus-per-task=8 --mem=16384M --gres=gpu:1")
	cpu := r.sbatch("--cpus-per-task=2 --mem=1024M")
	two := r.sbatch("--cpus-per-task=16 --mem=32768M --gres=gpu:2")
	for id, want := range map[int]string{
		gpu: "cpu=8,mem=16384M,node=1,billing=8,gres/gpu=1", // the brief's example
		cpu: "cpu=2,mem=1024M,node=1,billing=2",             // no gres part for a CPU job
		two: "cpu=16,mem=32768M,node=1,billing=16,gres/gpu=2",
	} {
		if got := r.squeueJob(id)["tres_req_str"]; got != want {
			t.Errorf("job %d tres_req_str = %v, want %q", id, got, want)
		}
	}
}

func TestSqueueStartEstimate(t *testing.T) {
	r := newRig(t, Config{})
	id := r.sbatch()
	held := r.sbatch("--hold")
	wantNV(t, "no estimate", r.squeueJob(id)["start_time"], false, 0)
	est := t0.Add(90 * time.Minute)
	r.p.SetEstimate(id, &est)
	r.p.SetEstimate(held, &est)
	wantNV(t, "estimate", r.squeueJob(id)["start_time"], true, float64(est.Unix()))
	wantNV(t, "a held job has none", r.squeueJob(held)["start_time"], false, 0)
	later := est.Add(time.Hour)
	r.p.SetEstimate(id, &later) // swinging estimates
	wantNV(t, "moved estimate", r.squeueJob(id)["start_time"], true, float64(later.Unix()))
	r.p.SetEstimate(id, nil)
	wantNV(t, "cleared estimate", r.squeueJob(id)["start_time"], false, 0)
	r.p.SetEstimate(id, &est)
	r.clock.Advance(time.Minute)
	r.start(id)
	wantNV(t, "start of a started job", r.squeueJob(id)["start_time"], true, float64(t0.Add(time.Minute).Unix()))
	if j, _ := r.p.Job(id); j.Estimate != nil {
		t.Errorf("a started job keeps its estimate %v", *j.Estimate)
	}
}

func TestSqueueRunningEndFollowsTheTimeLimit(t *testing.T) {
	r := newRig(t, Config{})
	id := r.sbatch("--time=10")
	r.start(id)
	wantNV(t, "end_time", r.squeueJob(id)["end_time"], true, float64(t0.Unix()+600))
	r.p.SetTimeLimit(id, 25) // a centre's extension
	wantNV(t, "time_limit after the extension", r.squeueJob(id)["time_limit"], true, 25)
	wantNV(t, "end_time after the extension", r.squeueJob(id)["end_time"], true, float64(t0.Unix()+25*60))
}

// S4: a requeued job is plain PENDING, with restart_cnt raised and no REQUEUED flag.
func TestSqueueRequeuedJobIsPlainPending(t *testing.T) {
	r := newRig(t, Config{})
	id := r.sbatch("--time=10")
	r.clock.Advance(time.Minute)
	r.start(id)
	r.clock.Advance(time.Minute)
	if err := r.p.Requeue(id); err != nil {
		t.Fatal(err)
	}
	j := r.squeueJob(id)
	wantStrings(t, "job_state", j["job_state"], "PENDING")
	if j["restart_cnt"] != 1.0 || j["state_reason"] != "BeginTime" || j["hold"] != false {
		t.Errorf("requeued job: restart_cnt %v reason %v hold %v", j["restart_cnt"], j["state_reason"], j["hold"])
	}
	wantNV(t, "start_time", j["start_time"], false, 0)
	wantNV(t, "end_time", j["end_time"], false, 0)
	wantNV(t, "submit_time of the new execution", j["submit_time"], true, float64(t0.Add(2*time.Minute).Unix()))
}

func TestSqueueFiltersAndOrder(t *testing.T) {
	r := newRig(t, Config{})
	a, b, c := r.sbatch(), r.sbatch(), r.sbatch()
	if got := idsOf(t, r.squeue()); !equalInts(got, []int{a, b, c}) {
		t.Errorf("squeue lists %v, want ascending %v", got, []int{a, b, c})
	}
	if got := idsOf(t, r.squeue("--jobs=1002,1003")); !equalInts(got, []int{b, c}) {
		t.Errorf("--jobs=1002,1003 lists %v", got)
	}
}

func TestSqueueDescendingOrder(t *testing.T) {
	r := newRig(t, Config{SqueueDesc: true})
	a, b, c := r.sbatch(), r.sbatch(), r.sbatch()
	if got := idsOf(t, r.squeue()); !equalInts(got, []int{c, b, a}) {
		t.Errorf("SqueueDesc lists %v, want descending", got)
	}
}

func TestSqueueOptionErrors(t *testing.T) {
	r := newRig(t, Config{})
	for _, line := range []string{
		"squeue --me --json --states=all", "squeue --me --json --jobs", "squeue --me --json --jobs=abc",
		"squeue --me --json extra", "squeue -u someone --json",
	} {
		if out, errOut, code := r.run(line); code != 1 || out != "" || !strings.HasPrefix(errOut, "squeue: error: ") {
			t.Errorf("%q: exit %d stdout %q stderr %q, want exit 1 and a squeue error", line, code, out, errOut)
		}
	}
}

func TestMetaReplyDataParserOverride(t *testing.T) {
	for _, override := range []string{"v0.0.99", "data_parser/v0.0.99"} {
		r := newRig(t, Config{DataParser: "v0.0.42", ReplyDataParser: override})
		id := r.sbatch()
		wantMeta(t, r.squeue(), "v0.0.99", "v0.0.42", "example-user")
		// The shape stays the platform's: v0.0.42 accounting has restart_cnt.
		if _, ok := jobsOf(t, r.sacct("-j "+itoa(id)))[0]["restart_cnt"]; !ok {
			t.Errorf("override %q changed the sacct shape, not just the meta value", override)
		}
		if m := decode(t, r.must("sacct --json -X -D -j "+itoa(id)))["meta"].(map[string]any); m["plugin"].(map[string]any)["data_parser"] != "data_parser/v0.0.99" {
			t.Errorf("sacct meta not overridden: %v", m["plugin"])
		}
	}
}
