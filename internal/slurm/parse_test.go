package slurm

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
	_ "time/tzdata" // the zone tests must not depend on the host's zoneinfo

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// These tests check the parser against HBEP-4 (The Slurm dialect, Core
// operations) and against the fixtures in testdata/, whose sources are in
// testdata/README.md: S7's real v0.0.42 captures and documents shaped like S4's
// fake v0.0.40 replies. Expected values are written out by hand; every instant
// was worked out with `date -u -d @<epoch>`, never with this package, and no
// fixture was produced by the parser or by internal/slurm/fakeslurm.

// dialects are the data_parser versions that have fixtures.
var dialects = []string{"v0.0.40", "v0.0.42"}

func readTestdata(t *testing.T, version, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", version, name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// at is a UTC instant.
func at(y int, mo time.Month, d, h, mi, s int) time.Time {
	return time.Date(y, mo, d, h, mi, s, 0, time.UTC)
}

func pt(t time.Time) *time.Time { return &t }

// ---- expected jobs ----

// wantJob is a job record the parser must return. Every field is compared,
// times by instant and by location (UTC), except the hard end of a job that
// has ended, and the end time of a requeued execution: HBEP-4 defines the hard
// end of a running job only, and gives the end time of a job that has ended.
type wantJob struct {
	provider.Job
	hardEndUnspecified bool
	endUnspecified     bool
}

func timeText(t *time.Time) string {
	if t == nil {
		return "nil"
	}
	return t.Format(time.RFC3339) + " " + t.Location().String()
}

func sameTime(got, want *time.Time) bool {
	if got == nil || want == nil {
		return got == nil && want == nil
	}
	return got.Equal(*want) && got.Location() == time.UTC
}

func (w wantJob) diff(got provider.Job) []string {
	var d []string
	field := func(name string, g, wv any) {
		if !reflect.DeepEqual(g, wv) {
			d = append(d, fmt.Sprintf("%s = %v, want %v", name, g, wv))
		}
	}
	field("ID", got.ID, w.ID)
	field("Name", got.Name, w.Name)
	field("State", got.State, w.State)
	field("Phase", got.Phase, w.Phase)
	field("Hold", got.Hold, w.Hold)
	field("Reason", got.Reason, w.Reason)
	field("RestartCount", got.RestartCount, w.RestartCount)
	field("Requeued", got.Requeued, w.Requeued)
	field("Partition", got.Partition, w.Partition)
	field("TimeLimit", got.TimeLimit, w.TimeLimit)
	field("GPUs", got.GPUs, w.GPUs)
	field("Source", got.Source, w.Source)
	field("DataParser", got.DataParser, w.DataParser)
	submit := &got.SubmitTime
	wantSubmit := &w.SubmitTime
	if got.SubmitTime.IsZero() {
		submit = nil
	}
	if w.SubmitTime.IsZero() {
		wantSubmit = nil
	}
	if !sameTime(submit, wantSubmit) {
		d = append(d, fmt.Sprintf("SubmitTime = %s, want %s", timeText(submit), timeText(wantSubmit)))
	}
	for _, c := range []struct {
		name      string
		got, want *time.Time
	}{
		{"StartEstimate", got.StartEstimate, w.StartEstimate},
		{"StartTime", got.StartTime, w.StartTime},
		{"EndTime", got.EndTime, w.EndTime},
	} {
		if c.name == "EndTime" && w.endUnspecified {
			continue
		}
		if !sameTime(c.got, c.want) {
			d = append(d, fmt.Sprintf("%s = %s, want %s", c.name, timeText(c.got), timeText(c.want)))
		}
	}
	if !w.hardEndUnspecified && !sameTime(got.HardEnd, w.HardEnd) {
		d = append(d, fmt.Sprintf("HardEnd = %s, want %s", timeText(got.HardEnd), timeText(w.HardEnd)))
	}
	return d
}

func checkJobs(t *testing.T, got map[string]provider.Job, want map[string]wantJob) {
	t.Helper()
	for id, w := range want {
		g, ok := got[id]
		if !ok {
			t.Errorf("job %s is missing from the result", id)
			continue
		}
		for _, d := range w.diff(g) {
			t.Errorf("job %s (%s): %s", id, w.Name, d)
		}
	}
	for id := range got {
		if _, ok := want[id]; !ok {
			t.Errorf("job %s is in the result but not expected", id)
		}
	}
}

// queueWant is what the queue-listing fixtures must give, for either
// data_parser version (the jobs 4242 and 43xx mean the same in both).
func queueWant(v string) map[string]wantJob {
	q := func(j provider.Job) wantJob {
		j.Source = provider.SourceQueue
		j.DataParser = v
		if j.Partition == "" {
			j.Partition = "plgrid"
		}
		j.TimeLimit = 10 * time.Minute
		if j.ID == "4310" {
			j.TimeLimit = time.Hour
		}
		return wantJob{Job: j}
	}
	submit := at(2026, 10, 30, 9, 7, 23)     // epoch 1793351243
	start := at(2026, 10, 30, 9, 7, 25)      // 1793351245
	projected := at(2026, 10, 30, 9, 17, 25) // 1793351845, start + 10 min
	heldSubmit := at(2026, 10, 30, 9, 8, 20) // 1793351300
	return map[string]wantJob{
		"4242": q(provider.Job{ID: "4242", Name: "s1-frp", State: []string{"RUNNING"}, Phase: v1alpha1.AllocationRunning,
			Reason: "None", SubmitTime: submit, StartTime: pt(start), HardEnd: pt(projected)}),
		"4301": q(provider.Job{ID: "4301", Name: "hb-held-user-0123456789ab", State: []string{"PENDING"},
			Phase: v1alpha1.AllocationHeld, Hold: provider.HoldUser, Reason: "JobHeldUser", SubmitTime: heldSubmit}),
		"4302": q(provider.Job{ID: "4302", Name: "hb-held-admin-0123456789ab", State: []string{"PENDING"},
			Phase: v1alpha1.AllocationHeld, Hold: provider.HoldAdmin, Reason: "JobHeldAdmin", SubmitTime: heldSubmit}),
		"4303": q(provider.Job{ID: "4303", Name: "hb-estimate-0123456789ab", State: []string{"PENDING"},
			Phase: v1alpha1.AllocationPending, Reason: "Resources", SubmitTime: heldSubmit,
			StartEstimate: pt(at(2026, 10, 30, 10, 10, 0))}), // 1793355000
		"4304": q(provider.Job{ID: "4304", Name: "hb-requeued-0123456789ab", State: []string{"PENDING"},
			Phase: v1alpha1.AllocationFailed, Reason: "BeginTime", RestartCount: 1, Requeued: true, SubmitTime: submit}),
		"4305": q(provider.Job{ID: "4305", Name: "hb-gpu-untyped-0123456789ab", State: []string{"RUNNING"},
			Phase: v1alpha1.AllocationRunning, Reason: "None", GPUs: 1, SubmitTime: submit, StartTime: pt(start), HardEnd: pt(projected)}),
		"4306": q(provider.Job{ID: "4306", Name: "hb-gpu-typed-0123456789ab", State: []string{"RUNNING"},
			Phase: v1alpha1.AllocationRunning, Reason: "None", GPUs: 2, SubmitTime: submit, StartTime: pt(start), HardEnd: pt(projected)}),
		"4307": q(provider.Job{ID: "4307", Name: "hb-gpu-both-0123456789ab", State: []string{"RUNNING"},
			Phase: v1alpha1.AllocationRunning, Reason: "None", GPUs: 2, SubmitTime: submit, StartTime: pt(start), HardEnd: pt(projected)}),
		"4308": wantJob{hardEndUnspecified: true, Job: q(provider.Job{ID: "4308", Name: "hb-failed-completing-0123456789ab",
			State: []string{"FAILED", "COMPLETING"}, Phase: v1alpha1.AllocationFailed, Reason: "NonZeroExitCode",
			Partition: "example-gpu-partition", GPUs: 1, SubmitTime: submit, StartTime: pt(start),
			EndTime: pt(at(2026, 10, 30, 9, 7, 33))}).Job}, // 1793351253
		"4309": q(provider.Job{ID: "4309", Name: "hb-extended-0123456789ab", State: []string{"RUNNING"},
			Phase: v1alpha1.AllocationRunning, Reason: "None", SubmitTime: submit, StartTime: pt(start),
			HardEnd: pt(at(2026, 10, 30, 9, 47, 25))}), // 1793353645, end_time 30 min after start + limit
		"4310": q(provider.Job{ID: "4310", Name: "hb-dst-0123456789ab", State: []string{"RUNNING"},
			Phase: v1alpha1.AllocationRunning, Reason: "None", SubmitTime: at(2026, 10, 24, 23, 50, 0), // 1792885800
			StartTime: pt(at(2026, 10, 25, 0, 30, 0)),   // 1792888200
			HardEnd:   pt(at(2026, 10, 25, 1, 30, 0))}), // 1792891800
	}
}

// accountingWant is what the sacct fixtures must give.
func accountingWant(v string) map[string]wantJob {
	a := func(j provider.Job) wantJob {
		j.Source = provider.SourceAccounting
		j.DataParser = v
		if j.Partition == "" {
			j.Partition = "plgrid"
		}
		return wantJob{Job: j, hardEndUnspecified: j.Phase != v1alpha1.AllocationRunning}
	}
	submit := at(2026, 10, 30, 9, 7, 23) // 1793351243
	start := at(2026, 10, 30, 9, 7, 25)  // 1793351245
	end := at(2026, 10, 30, 9, 7, 33)    // 1793351253
	heldSubmit := at(2026, 10, 30, 9, 8, 20)
	running := func(id, name string, gpus int32) wantJob {
		return a(provider.Job{ID: id, Name: name, State: []string{"RUNNING"}, Phase: v1alpha1.AllocationRunning,
			Reason: "None", GPUs: gpus, TimeLimit: 2 * time.Hour, SubmitTime: submit, StartTime: pt(start),
			HardEnd: pt(at(2026, 10, 30, 11, 7, 25))}) // 1793358445, start + 120 min
	}
	want := map[string]wantJob{
		"4242": a(provider.Job{ID: "4242", Name: "s7-hash-ckpt", State: []string{"COMPLETED"}, Phase: v1alpha1.AllocationEnded,
			Reason: "None", TimeLimit: time.Hour, SubmitTime: at(2026, 10, 30, 8, 45, 52), // 1793349952
			StartTime: pt(at(2026, 10, 30, 8, 45, 57)),   // 1793349957
			EndTime:   pt(at(2026, 10, 30, 8, 49, 44))}), // 1793350184
		"4301": a(provider.Job{ID: "4301", Name: "hb-held-user-0123456789ab", State: []string{"PENDING"},
			Phase: v1alpha1.AllocationHeld, Hold: provider.HoldUser, Reason: "JobHeldUser", TimeLimit: 10 * time.Minute, SubmitTime: heldSubmit}),
		"4302": a(provider.Job{ID: "4302", Name: "hb-held-admin-0123456789ab", State: []string{"PENDING"},
			Phase: v1alpha1.AllocationHeld, Hold: provider.HoldAdmin, Reason: "JobHeldAdmin", TimeLimit: 10 * time.Minute, SubmitTime: heldSubmit}),
		"4304": running("4304", "hb-gpu-untyped-0123456789ab", 1),
		"4305": running("4305", "hb-gpu-typed-0123456789ab", 2),
		"4306": running("4306", "hb-gpu-allocated-only-0123456789ab", 1),
		"4307": running("4307", "hb-gpu-both-0123456789ab", 2),
		"4308": a(provider.Job{ID: "4308", Name: "hb-failed-0123456789ab", State: []string{"FAILED"}, Phase: v1alpha1.AllocationFailed,
			Reason: "NonZeroExitCode", Partition: "example-gpu-partition", GPUs: 1, TimeLimit: 10 * time.Minute,
			SubmitTime: submit, StartTime: pt(start), EndTime: pt(end)}),
		"4310": a(provider.Job{ID: "4310", Name: "hb-dst-0123456789ab", State: []string{"COMPLETED"}, Phase: v1alpha1.AllocationEnded,
			Reason: "None", TimeLimit: 2 * time.Hour, SubmitTime: at(2026, 10, 24, 23, 50, 0),
			StartTime: pt(at(2026, 10, 25, 0, 30, 0)), EndTime: pt(at(2026, 10, 25, 1, 30, 0))}),
	}
	if v == "v0.0.42" {
		// restart_cnt 1 on the only record (v0.0.40 has no restart_cnt).
		want["4303"] = a(provider.Job{ID: "4303", Name: "hb-requeued-0123456789ab", State: []string{"COMPLETED"},
			Phase: v1alpha1.AllocationFailed, Reason: "None", RestartCount: 1, Requeued: true, TimeLimit: 10 * time.Minute,
			SubmitTime: submit, StartTime: pt(start), EndTime: pt(end)})
	}
	return want
}

// ---- parsing helpers ----

// queueJobs parses a squeue reply and turns every record into a job.
func queueJobs(t *testing.T, doc []byte, parser string) map[string]provider.Job {
	t.Helper()
	recs, got, err := parseQueue(doc, parser)
	if err != nil {
		t.Fatalf("parseQueue: %v", err)
	}
	if got != parser {
		t.Errorf("parseQueue returned the version %q, want %q", got, parser)
	}
	out := map[string]provider.Job{}
	for _, r := range recs {
		j, err := r.job(got)
		if err != nil {
			t.Fatalf("job %d: %v", r.JobID, err)
		}
		if _, dup := out[j.ID]; dup {
			t.Fatalf("job %s appears twice", j.ID)
		}
		out[j.ID] = j
	}
	return out
}

// accountingJobs parses a sacct reply and merges the records of each job.
func accountingJobs(t *testing.T, doc []byte, parser string) map[string]provider.Job {
	t.Helper()
	recs, got, err := parseAccounting(doc, parser)
	if err != nil {
		t.Fatalf("parseAccounting: %v", err)
	}
	jobs, err := mergeAccounting(recs, got)
	if err != nil {
		t.Fatalf("mergeAccounting: %v", err)
	}
	out := map[string]provider.Job{}
	for _, j := range jobs {
		if _, dup := out[j.ID]; dup {
			t.Fatalf("job %s appears twice after the merge", j.ID)
		}
		out[j.ID] = j
	}
	return out
}

// queueErr returns the first error from reading a squeue reply into jobs.
func queueErr(doc []byte, parser string) error {
	recs, got, err := parseQueue(doc, parser)
	if err != nil {
		return err
	}
	for _, r := range recs {
		if _, err := r.job(got); err != nil {
			return err
		}
	}
	return nil
}

// accountingErr returns the first error from reading a sacct reply into jobs.
func accountingErr(doc []byte, parser string) error {
	recs, got, err := parseAccounting(doc, parser)
	if err != nil {
		return err
	}
	_, err = mergeAccounting(recs, got)
	return err
}

// editDoc decodes a reply, lets edit change its top-level object, and encodes
// it again. It is how a test derives a variant of a fixture inline.
func editDoc(t *testing.T, doc []byte, edit func(top map[string]any)) []byte {
	t.Helper()
	var top map[string]any
	if err := json.Unmarshal(doc, &top); err != nil {
		t.Fatal(err)
	}
	edit(top)
	out, err := json.Marshal(top)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// onlyJob keeps the job with the given job_id, after edit changed it.
func onlyJob(t *testing.T, doc []byte, id int, edit func(job map[string]any)) []byte {
	t.Helper()
	return editDoc(t, doc, func(top map[string]any) {
		for _, j := range top["jobs"].([]any) {
			m := j.(map[string]any)
			if int(m["job_id"].(float64)) != id {
				continue
			}
			if edit != nil {
				edit(m)
			}
			top["jobs"] = []any{m}
			return
		}
		t.Fatalf("no job %d in the fixture", id)
	})
}

func triple(set bool, n float64) map[string]any {
	return map[string]any{"set": set, "infinite": false, "number": n}
}

func toAny(words ...string) []any {
	out := make([]any, len(words))
	for i, w := range words {
		out[i] = w
	}
	return out
}

// ---- the fixtures themselves ----

// objectKeys returns the keys of a JSON object in document order.
func objectKeys(t *testing.T, raw []byte) []string {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		t.Fatalf("not an object: %v %v", tok, err)
	}
	var keys []string
	for dec.More() {
		k, err := dec.Token()
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k.(string))
		var skip json.RawMessage
		if err := dec.Decode(&skip); err != nil {
			t.Fatal(err)
		}
	}
	return keys
}

func firstJob(t *testing.T, doc []byte) []byte {
	t.Helper()
	var top struct {
		Jobs []json.RawMessage `json:"jobs"`
	}
	if err := json.Unmarshal(doc, &top); err != nil || len(top.Jobs) == 0 {
		t.Fatalf("no jobs: %v", err)
	}
	return top.Jobs[0]
}

// S7's list of the sacct job object's keys (spikes/s7/slurm-json-real.md).
var s7SacctJobKeys = []string{
	"account", "comment", "allocation_nodes", "array", "association", "block", "cluster", "constraints",
	"container", "derived_exit_code", "time", "exit_code", "extra", "failed_node", "flags", "group", "het",
	"job_id", "name", "licenses", "mcs", "nodes", "partition", "hold", "priority", "qos", "qosreq", "required",
	"kill_request_user", "restart_cnt", "reservation", "script", "stdin_expanded", "stdout_expanded",
	"stderr_expanded", "stdout", "stderr", "stdin", "state", "steps", "submit_line", "tres", "used_gres", "user",
	"wckey", "working_directory",
}

// The two S7 documents keep S7's structure: its top-level keys, its meta, the
// 128 keys of the squeue job object and the 46 of the sacct one.
func TestFixturesKeepS7Structure(t *testing.T) {
	squeue := readTestdata(t, "v0.0.42", "squeue-running.json")
	wantTop := []string{"last_backfill", "last_update", "meta", "errors", "warnings", "jobs"}
	if got := objectKeys(t, squeue); !reflect.DeepEqual(got, wantTop) {
		t.Errorf("squeue top-level keys %v, want %v", got, wantTop)
	}
	keys := objectKeys(t, firstJob(t, squeue))
	if len(keys) != 128 {
		t.Errorf("the squeue job object has %d keys, S7 lists 128", len(keys))
	}
	for _, k := range []string{"job_id", "name", "job_state", "state_reason", "hold", "priority", "restart_cnt",
		"partition", "time_limit", "tres_req_str", "submit_time", "start_time", "end_time", "job_resources"} {
		found := false
		for _, g := range keys {
			found = found || g == k
		}
		if !found {
			t.Errorf("the squeue job object lacks %q", k)
		}
	}

	sacct := readTestdata(t, "v0.0.42", "sacct-completed.json")
	if got, want := objectKeys(t, sacct), []string{"jobs", "meta", "errors", "warnings"}; !reflect.DeepEqual(got, want) {
		t.Errorf("sacct top-level keys %v, want %v", got, want)
	}
	if got := objectKeys(t, firstJob(t, sacct)); !reflect.DeepEqual(got, s7SacctJobKeys) {
		t.Errorf("the sacct job object has the keys %v, S7 lists %v", got, s7SacctJobKeys)
	}
	for _, doc := range [][]byte{squeue, sacct} {
		if !strings.Contains(string(doc), `"data_parser": "data_parser/v0.0.42"`) {
			t.Errorf("an S7 fixture lacks meta.plugin.data_parser = data_parser/v0.0.42")
		}
	}
}

// No fixture carries what S7 redacted, or a real-looking path or job ID.
func TestFixturesHoldNothingPrivate(t *testing.T) {
	paths := regexp.MustCompile(`"(/[^"]*)"`)
	allowedIDs := map[int]bool{4242: true}
	for id := 4301; id <= 4322; id++ {
		allowedIDs[id] = true
	}
	// The real Athena replies' replacement IDs (testdata/v0.0.40-athena-real).
	allowedIDs[4501], allowedIDs[4502] = true, true
	files, err := filepath.Glob("testdata/*/*.json")
	if err != nil || len(files) == 0 {
		t.Fatalf("no fixtures: %v", err)
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var top struct {
			Jobs []struct {
				JobID int `json:"job_id"`
			} `json:"jobs"`
		}
		if err := json.Unmarshal(b, &top); err != nil {
			t.Errorf("%s is not JSON: %v", f, err)
		}
		for _, j := range top.Jobs {
			if !allowedIDs[j.JobID] {
				t.Errorf("%s: job ID %d is not one of the fake IDs", f, j.JobID)
			}
		}
		text := string(b)
		for _, bad := range []string{"redacted", "<", "/home/", "/net/", "cyfronet"} {
			if strings.Contains(text, bad) {
				t.Errorf("%s contains %q", f, bad)
			}
		}
		for _, m := range paths.FindAllStringSubmatch(text, -1) {
			if p := m[1]; p != "/dev/null" && !strings.HasPrefix(p, "/scratch/example-user/") {
				t.Errorf("%s: path %q is not a fake path", f, p)
			}
		}
	}
}

// ---- S7's real captures, and the derived documents ----

func TestQueueListing(t *testing.T) {
	for _, v := range dialects {
		t.Run(v, func(t *testing.T) {
			want := queueWant(v)
			running := map[string]wantJob{"4242": want["4242"]}
			checkJobs(t, queueJobs(t, readTestdata(t, v, "squeue-running.json"), v), running)

			derived := queueJobs(t, readTestdata(t, v, "squeue-derived.json"), v)
			delete(want, "4242")
			checkJobs(t, derived, want)
		})
	}
}

func TestAccountingListing(t *testing.T) {
	for _, v := range dialects {
		t.Run(v, func(t *testing.T) {
			want := accountingWant(v)
			completed := map[string]wantJob{"4242": want["4242"]}
			checkJobs(t, accountingJobs(t, readTestdata(t, v, "sacct-completed.json"), v), completed)

			derived := accountingJobs(t, readTestdata(t, v, "sacct-derived.json"), v)
			delete(want, "4242")
			checkJobs(t, derived, want)
		})
	}
}

// A pending job's start_time is the scheduler's estimate: StartEstimate, never
// StartTime. A running job's end_time is its hard end, and when it disagrees
// with start + limit (4309), end_time wins: S7's own capture cannot tell them
// apart, because there end_time equals start + limit.
func TestQueueTimeSemantics(t *testing.T) {
	for _, v := range dialects {
		t.Run(v, func(t *testing.T) {
			jobs := queueJobs(t, readTestdata(t, v, "squeue-derived.json"), v)
			est := jobs["4303"]
			if est.StartTime != nil || est.HardEnd != nil || est.EndTime != nil {
				t.Errorf("a pending job has StartTime %s, HardEnd %s, EndTime %s", timeText(est.StartTime), timeText(est.HardEnd), timeText(est.EndTime))
			}
			if !sameTime(est.StartEstimate, pt(at(2026, 10, 30, 10, 10, 0))) {
				t.Errorf("StartEstimate = %s, want 2026-10-30T10:10:00Z", timeText(est.StartEstimate))
			}
			ext := jobs["4309"]
			if got := ext.HardEnd.Sub(*ext.StartTime); got != 40*time.Minute {
				t.Errorf("HardEnd - StartTime = %s, want 40m0s (end_time), not the %s limit", got, ext.TimeLimit)
			}
		})
	}
	// S7's capture: the projected end is end_time = start + limit.
	j := queueJobs(t, readTestdata(t, "v0.0.42", "squeue-running.json"), "v0.0.42")["4242"]
	if got := j.HardEnd.Sub(*j.StartTime); got != j.TimeLimit || got != 10*time.Minute {
		t.Errorf("S7's running job: HardEnd - StartTime = %s, limit %s, want both 10m0s", got, j.TimeLimit)
	}
}

// Jobs are sorted by ID, numerically, never taken in output order: the fixtures
// list them out of order on purpose.
func TestListingOrderIsNotTheIDOrder(t *testing.T) {
	for _, v := range dialects {
		t.Run(v, func(t *testing.T) {
			recs, _, err := parseQueue(readTestdata(t, v, "squeue-derived.json"), v)
			if err != nil {
				t.Fatal(err)
			}
			var jobs []provider.Job
			var docOrder []string
			for _, r := range recs {
				j, err := r.job(v)
				if err != nil {
					t.Fatal(err)
				}
				jobs = append(jobs, j)
				docOrder = append(docOrder, j.ID)
			}
			if sort.StringsAreSorted(docOrder) {
				t.Fatalf("the fixture lists the jobs in ID order %v, so it shows nothing", docOrder)
			}
			sortJobs(jobs)
			var got []string
			for _, j := range jobs {
				got = append(got, j.ID)
			}
			want := []string{"4301", "4302", "4303", "4304", "4305", "4306", "4307", "4308", "4309", "4310"}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("sorted IDs %v, want %v", got, want)
			}
		})
	}
}

func TestSortJobsIsNumeric(t *testing.T) {
	jobs := []provider.Job{{ID: "1000"}, {ID: "999"}, {ID: "20000"}, {ID: "3"}, {ID: "12"}}
	sortJobs(jobs)
	var got []string
	for _, j := range jobs {
		got = append(got, j.ID)
	}
	if want := []string{"3", "12", "999", "1000", "20000"}; !reflect.DeepEqual(got, want) {
		t.Errorf("sorted %v, want %v (not the text order %v)", got, want, []string{"1000", "12", "20000", "3", "999"})
	}
}

// mergeAccounting returns jobs sorted by numeric ID, whatever the record order.
func TestMergeAccountingSortsNumerically(t *testing.T) {
	doc := editDoc(t, readTestdata(t, "v0.0.42", "sacct-completed.json"), func(top map[string]any) {
		base := top["jobs"].([]any)[0].(map[string]any)
		var jobs []any
		for _, id := range []float64{1000, 999, 20000, 3} {
			j := map[string]any{}
			for k, v := range base {
				j[k] = v
			}
			j["job_id"] = id
			jobs = append(jobs, j)
		}
		top["jobs"] = jobs
	})
	recs, v, err := parseAccounting(doc, "v0.0.42")
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := mergeAccounting(recs, v)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, j := range jobs {
		got = append(got, j.ID)
	}
	if want := []string{"3", "999", "1000", "20000"}; !reflect.DeepEqual(got, want) {
		t.Errorf("IDs %v, want %v", got, want)
	}
}

// ---- requeues in accounting ----

func requeueWant(v string) map[string]wantJob {
	a := func(j provider.Job) wantJob {
		j.Source, j.DataParser, j.Partition = provider.SourceAccounting, v, "plgrid"
		j.TimeLimit = 10 * time.Minute
		return wantJob{Job: j, hardEndUnspecified: true}
	}
	lone := func(j provider.Job) wantJob {
		w := a(j)
		w.endUnspecified = true // the execution ended, but the job is pending again
		return w
	}
	w := map[string]wantJob{
		// The later execution gives the state and the times; the job is requeued.
		"4320": a(provider.Job{ID: "4320", Name: "hb-rq-0123456789ab", State: []string{"COMPLETED"},
			Phase: v1alpha1.AllocationFailed, Reason: "None", Requeued: true,
			SubmitTime: at(2026, 10, 30, 9, 8, 20),       // 1793351300
			StartTime:  pt(at(2026, 10, 30, 9, 8, 30)),   // 1793351310
			EndTime:    pt(at(2026, 10, 30, 9, 9, 30))}), // 1793351370
		"4321": a(provider.Job{ID: "4321", Name: "hb-rq-control-0123456789ab", State: []string{"COMPLETED"},
			Phase: v1alpha1.AllocationEnded, Reason: "None", SubmitTime: at(2026, 10, 30, 9, 7, 23),
			StartTime: pt(at(2026, 10, 30, 9, 7, 25)), EndTime: pt(at(2026, 10, 30, 9, 7, 33))}),
		"4322": lone(provider.Job{ID: "4322", Name: "hb-rq-lone-0123456789ab", State: []string{"REQUEUED"},
			Phase: v1alpha1.AllocationFailed, Reason: "None", Requeued: true, SubmitTime: at(2026, 10, 30, 9, 7, 23),
			StartTime: pt(at(2026, 10, 30, 9, 7, 25))}),
	}
	if v == "v0.0.42" {
		r := w["4320"]
		r.RestartCount = 1 // restart_cnt of the later record
		w["4320"] = r
		l := w["4322"]
		l.State = []string{"PENDING", "REQUEUED"}
		w["4322"] = l
	}
	return w
}

// A requeue shows as a REQUEUED record (the earlier execution under -D) or a
// restart_cnt above 0; the later execution gives the state. The fixtures list
// the later execution first, and the test lists the records both ways round, so
// the output order cannot decide.
func TestAccountingRequeue(t *testing.T) {
	for _, v := range dialects {
		t.Run(v, func(t *testing.T) {
			doc := readTestdata(t, v, "sacct-requeue.json")
			checkJobs(t, accountingJobs(t, doc, v), requeueWant(v))

			reversed := editDoc(t, doc, func(top map[string]any) {
				jobs := top["jobs"].([]any)
				for i, k := 0, len(jobs)-1; i < k; i, k = i+1, k-1 {
					jobs[i], jobs[k] = jobs[k], jobs[i]
				}
			})
			checkJobs(t, accountingJobs(t, reversed, v), requeueWant(v))
		})
	}
}

// v0.0.40 has no restart_cnt, so two executions of one job ID are the only
// sign: a requeue even when neither record is marked REQUEUED.
func TestAccountingSecondExecutionIsARequeue(t *testing.T) {
	doc := editDoc(t, readTestdata(t, "v0.0.40", "sacct-requeue.json"), func(top map[string]any) {
		for _, j := range top["jobs"].([]any) {
			m := j.(map[string]any)
			if m["job_id"].(float64) == 4320 {
				m["state"].(map[string]any)["current"] = toAny("CANCELLED")
			}
		}
	})
	j := accountingJobs(t, doc, "v0.0.40")["4320"]
	if !j.Requeued || j.Phase != v1alpha1.AllocationFailed {
		t.Errorf("two records of one job: Requeued %v, Phase %s; want a requeue and Failed", j.Requeued, j.Phase)
	}
}

// ---- time zones ----

func loadZone(t *testing.T, name string) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation(name)
	if err != nil {
		t.Fatal(err)
	}
	return loc
}

// Epoch values become UTC instants, whatever zone the process runs in, across
// the Europe/Warsaw change of 2026-10-25 (01:00 UTC): job 4310 starts at 00:30
// UTC, 02:30 CEST, and ends at 01:30 UTC, 02:30 CET, so wall-clock arithmetic
// would give it no run time at all. The test sets both TZ and time.Local (the
// zone is read from TZ once, at start) and requires every parsed time to be in
// UTC, not only equal to the expected instant.
func TestEpochsAreUTCWhateverTheZone(t *testing.T) {
	warsaw := loadZone(t, "Europe/Warsaw")
	// The case is a real DST case, independent of the parser.
	if got := at(2026, 10, 25, 0, 30, 0).In(warsaw).Format("15:04 MST"); got != "02:30 CEST" {
		t.Fatalf("00:30 UTC in Warsaw reads %q, want 02:30 CEST", got)
	}
	if got := at(2026, 10, 25, 1, 30, 0).In(warsaw).Format("15:04 MST"); got != "02:30 CET" {
		t.Fatalf("01:30 UTC in Warsaw reads %q, want 02:30 CET", got)
	}
	for _, zone := range []string{"UTC", "Europe/Warsaw", "America/Los_Angeles", "Asia/Kolkata", "Pacific/Auckland"} {
		t.Run(zone, func(t *testing.T) {
			loc := loadZone(t, zone)
			t.Setenv("TZ", zone)
			old := time.Local
			time.Local = loc
			t.Cleanup(func() { time.Local = old })
			if time.Unix(1792888200, 0).Location() != loc {
				t.Fatalf("the zone %s is not in effect", zone)
			}
			if zone == "Europe/Warsaw" {
				if got := time.Unix(1792888200, 0).Format("15:04 MST"); got != "02:30 CEST" {
					t.Fatalf("local time reads %q, want 02:30 CEST", got)
				}
			}
			for _, v := range dialects {
				checkJobs(t, queueJobs(t, readTestdata(t, v, "squeue-derived.json"), v), withoutKey(queueWant(v), "4242"))
				checkJobs(t, accountingJobs(t, readTestdata(t, v, "sacct-derived.json"), v), withoutKey(accountingWant(v), "4242"))
				q := queueJobs(t, readTestdata(t, v, "squeue-derived.json"), v)["4310"]
				if d := q.HardEnd.Sub(*q.StartTime); d != time.Hour {
					t.Errorf("%s queue job 4310 runs %s, want 1h0m0s", v, d)
				}
				a := accountingJobs(t, readTestdata(t, v, "sacct-derived.json"), v)["4310"]
				if d := a.EndTime.Sub(*a.StartTime); d != time.Hour {
					t.Errorf("%s accounting job 4310 ran %s, want 1h0m0s", v, d)
				}
				if got := a.StartTime.Format("2006-01-02T15:04:05 MST"); got != "2026-10-25T00:30:00 UTC" {
					t.Errorf("%s: StartTime prints as %q", v, got)
				}
			}
			s7 := accountingJobs(t, readTestdata(t, "v0.0.42", "sacct-completed.json"), "v0.0.42")["4242"]
			if !sameTime(s7.StartTime, pt(at(2026, 10, 30, 8, 45, 57))) || !sameTime(s7.EndTime, pt(at(2026, 10, 30, 8, 49, 44))) {
				t.Errorf("S7's sacct times: start %s, end %s", timeText(s7.StartTime), timeText(s7.EndTime))
			}
		})
	}
}

func withoutKey(m map[string]wantJob, k string) map[string]wantJob {
	out := map[string]wantJob{}
	for id, w := range m {
		if id != k {
			out[id] = w
		}
	}
	return out
}

// ---- phases ----

// Base states and the phase HBEP-4 gives them (Core operations, Phases).
var phaseCases = []struct {
	state []string
	phase v1alpha1.AllocationPhase
}{
	{[]string{"RUNNING"}, v1alpha1.AllocationRunning},
	{[]string{"SUSPENDED"}, v1alpha1.AllocationRunning},
	{[]string{"PENDING"}, v1alpha1.AllocationPending},
	{[]string{"COMPLETED"}, v1alpha1.AllocationEnded},
	{[]string{"TIMEOUT"}, v1alpha1.AllocationEnded},
	{[]string{"CANCELLED"}, v1alpha1.AllocationEnded},
	{[]string{"FAILED"}, v1alpha1.AllocationFailed},
	{[]string{"NODE_FAIL"}, v1alpha1.AllocationFailed},
	{[]string{"OUT_OF_MEMORY"}, v1alpha1.AllocationFailed},
	{[]string{"BOOT_FAIL"}, v1alpha1.AllocationFailed},
	{[]string{"PREEMPTED"}, v1alpha1.AllocationFailed},
	{[]string{"DEADLINE"}, v1alpha1.AllocationFailed},
	// A flag after the base state does not change the phase.
	{[]string{"FAILED", "COMPLETING"}, v1alpha1.AllocationFailed},
	{[]string{"CANCELLED", "COMPLETING"}, v1alpha1.AllocationEnded},
	{[]string{"TIMEOUT", "COMPLETING"}, v1alpha1.AllocationEnded},
	{[]string{"RUNNING", "COMPLETING"}, v1alpha1.AllocationRunning},
	// A REQUEUED flag is a requeue: Failed, in any state.
	{[]string{"PENDING", "REQUEUED"}, v1alpha1.AllocationFailed},
	{[]string{"RUNNING", "REQUEUED"}, v1alpha1.AllocationFailed},
}

func TestQueuePhases(t *testing.T) {
	for _, v := range dialects {
		base := readTestdata(t, v, "squeue-running.json")
		for _, c := range phaseCases {
			t.Run(v+"/"+strings.Join(c.state, "+"), func(t *testing.T) {
				doc := onlyJob(t, base, 4242, func(j map[string]any) { j["job_state"] = toAny(c.state...) })
				jobs := queueJobs(t, doc, v)
				if got := jobs["4242"].Phase; got != c.phase {
					t.Errorf("%v: phase %s, want %s", c.state, got, c.phase)
				}
				if !reflect.DeepEqual(jobs["4242"].State, c.state) {
					t.Errorf("State %v, want %v", jobs["4242"].State, c.state)
				}
			})
		}
	}
}

func TestAccountingPhases(t *testing.T) {
	for _, v := range dialects {
		base := readTestdata(t, v, "sacct-completed.json")
		for _, c := range phaseCases {
			t.Run(v+"/"+strings.Join(c.state, "+"), func(t *testing.T) {
				doc := onlyJob(t, base, 4242, func(j map[string]any) {
					j["state"].(map[string]any)["current"] = toAny(c.state...)
				})
				jobs := accountingJobs(t, doc, v)
				if got := jobs["4242"].Phase; got != c.phase {
					t.Errorf("%v: phase %s, want %s", c.state, got, c.phase)
				}
			})
		}
	}
}

// An ended job's end time comes from accounting's time.end; a running job has
// none (sacct reports 0 while it runs).
func TestAccountingEndTimeOnlyForEndStates(t *testing.T) {
	jobs := accountingJobs(t, readTestdata(t, "v0.0.42", "sacct-derived.json"), "v0.0.42")
	if jobs["4304"].EndTime != nil {
		t.Errorf("a running job has EndTime %s", timeText(jobs["4304"].EndTime))
	}
	if jobs["4308"].EndTime == nil {
		t.Errorf("a failed job has no EndTime")
	}
}

// An unknown base state is an error, never a guess: the phase is unknown.
func TestUnknownStateIsAnError(t *testing.T) {
	for _, v := range dialects {
		for _, state := range [][]string{{"EXOTIC"}, {"EXOTIC", "COMPLETING"}, {}} {
			t.Run(v+"/"+strings.Join(state, "+"), func(t *testing.T) {
				q := onlyJob(t, readTestdata(t, v, "squeue-running.json"), 4242, func(j map[string]any) { j["job_state"] = toAny(state...) })
				if err := queueErr(q, v); err == nil {
					t.Errorf("squeue: state %v read without an error", state)
				}
				a := onlyJob(t, readTestdata(t, v, "sacct-completed.json"), 4242, func(j map[string]any) {
					j["state"].(map[string]any)["current"] = toAny(state...)
				})
				if err := accountingErr(a, v); err == nil {
					t.Errorf("sacct: state %v read without an error", state)
				}
			})
		}
	}
	// One unreadable job among good ones: the error is reported, and the
	// unreadable job is not among the jobs returned.
	doc := editDoc(t, readTestdata(t, "v0.0.42", "sacct-derived.json"), func(top map[string]any) {
		top["jobs"].([]any)[0].(map[string]any)["state"].(map[string]any)["current"] = toAny("EXOTIC")
	})
	recs, v, err := parseAccounting(doc, "v0.0.42")
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := mergeAccounting(recs, v)
	if err == nil {
		t.Error("an unknown state among good jobs raised no error")
	}
	for _, j := range jobs {
		if j.ID == "4310" {
			t.Errorf("the job with the unknown state (4310) is in the result as %s", j.Phase)
		}
	}
}

// ---- held ----

type holdSignals struct {
	name     string
	hold     *bool
	priority map[string]any // nil: leave
	reason   string
	phase    v1alpha1.AllocationPhase
	kind     provider.HoldKind
}

func ptrBool(b bool) *bool { return &b }

// HBEP-4 (The Slurm dialect, Held): hold true, a set priority of 0, or a state
// reason that starts with JobHeld; JobHeldUser is a user hold, JobHeldAdmin an
// admin hold. Each signal alone is enough; an unset priority is no signal.
var holdCases = []holdSignals{
	{"no signal", ptrBool(false), triple(true, 6286), "Priority", v1alpha1.AllocationPending, provider.HoldNone},
	{"hold flag alone", ptrBool(true), triple(true, 6286), "Priority", v1alpha1.AllocationHeld, provider.HoldUser},
	{"priority 0 alone", ptrBool(false), triple(true, 0), "Priority", v1alpha1.AllocationHeld, provider.HoldUser},
	{"priority 0 but not set", ptrBool(false), triple(false, 0), "Priority", v1alpha1.AllocationPending, provider.HoldNone},
	{"JobHeldUser alone", ptrBool(false), triple(true, 6286), "JobHeldUser", v1alpha1.AllocationHeld, provider.HoldUser},
	{"JobHeldAdmin alone", ptrBool(false), triple(true, 6286), "JobHeldAdmin", v1alpha1.AllocationHeld, provider.HoldAdmin},
	{"JobHeldAdmin with the other signals", ptrBool(true), triple(true, 0), "JobHeldAdmin", v1alpha1.AllocationHeld, provider.HoldAdmin},
	{"hold flag and priority 0, reason None", ptrBool(true), triple(true, 0), "None", v1alpha1.AllocationHeld, provider.HoldUser},
}

func TestQueueHoldSignals(t *testing.T) {
	for _, v := range dialects {
		for _, c := range holdCases {
			t.Run(v+"/"+c.name, func(t *testing.T) {
				doc := onlyJob(t, readTestdata(t, v, "squeue-derived.json"), 4303, func(j map[string]any) {
					j["hold"], j["priority"], j["state_reason"] = *c.hold, c.priority, c.reason
				})
				j := queueJobs(t, doc, v)["4303"]
				if j.Phase != c.phase || j.Hold != c.kind {
					t.Errorf("phase %s, hold %q; want %s, %q", j.Phase, j.Hold, c.phase, c.kind)
				}
				if j.Held() != (c.kind != provider.HoldNone) {
					t.Errorf("Held() = %v with hold %q", j.Held(), j.Hold)
				}
				if c.phase == v1alpha1.AllocationHeld && j.StartEstimate != nil {
					t.Errorf("a held job has the start estimate %s", timeText(j.StartEstimate))
				}
			})
		}
	}
}

func TestAccountingHoldSignals(t *testing.T) {
	for _, v := range dialects {
		for _, c := range holdCases {
			t.Run(v+"/"+c.name, func(t *testing.T) {
				doc := onlyJob(t, readTestdata(t, v, "sacct-derived.json"), 4301, func(j map[string]any) {
					j["hold"], j["priority"] = *c.hold, c.priority
					j["state"].(map[string]any)["reason"] = c.reason
				})
				j := accountingJobs(t, doc, v)["4301"]
				if j.Phase != c.phase || j.Hold != c.kind {
					t.Errorf("phase %s, hold %q; want %s, %q", j.Phase, j.Hold, c.phase, c.kind)
				}
			})
		}
	}
}

// ---- requeue signals in the queue listing ----

func TestQueueRequeueSignals(t *testing.T) {
	cases := []struct {
		name     string
		state    []string
		restart  any // nil: restart_cnt absent
		phase    v1alpha1.AllocationPhase
		requeued bool
		count    int32
	}{
		{"restart_cnt 1, pending again", []string{"PENDING"}, 1.0, v1alpha1.AllocationFailed, true, 1},
		{"restart_cnt 1, running again", []string{"RUNNING"}, 1.0, v1alpha1.AllocationFailed, true, 1},
		{"restart_cnt 2, completing", []string{"PENDING", "COMPLETING"}, 2.0, v1alpha1.AllocationFailed, true, 2},
		{"REQUEUED flag, restart_cnt 0", []string{"PENDING", "REQUEUED"}, 0.0, v1alpha1.AllocationFailed, true, 0},
		{"restart_cnt 0", []string{"RUNNING"}, 0.0, v1alpha1.AllocationRunning, false, 0},
		{"restart_cnt absent", []string{"RUNNING"}, nil, v1alpha1.AllocationRunning, false, 0},
	}
	for _, v := range dialects {
		for _, c := range cases {
			t.Run(v+"/"+c.name, func(t *testing.T) {
				doc := onlyJob(t, readTestdata(t, v, "squeue-running.json"), 4242, func(j map[string]any) {
					j["job_state"] = toAny(c.state...)
					if c.restart == nil {
						delete(j, "restart_cnt")
					} else {
						j["restart_cnt"] = c.restart
					}
				})
				j := queueJobs(t, doc, v)["4242"]
				if j.Phase != c.phase || j.Requeued != c.requeued || j.RestartCount != c.count {
					t.Errorf("phase %s, Requeued %v, RestartCount %d; want %s, %v, %d", j.Phase, j.Requeued, j.RestartCount, c.phase, c.requeued, c.count)
				}
			})
		}
	}
}

func TestAccountingRestartCount(t *testing.T) {
	cases := []struct {
		name     string
		restart  any // nil: absent
		phase    v1alpha1.AllocationPhase
		requeued bool
		count    int32
	}{
		{"restart_cnt 2", 2.0, v1alpha1.AllocationFailed, true, 2},
		{"restart_cnt 0", 0.0, v1alpha1.AllocationEnded, false, 0},
		{"restart_cnt absent (v0.0.40)", nil, v1alpha1.AllocationEnded, false, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			doc := onlyJob(t, readTestdata(t, "v0.0.42", "sacct-completed.json"), 4242, func(j map[string]any) {
				if c.restart == nil {
					delete(j, "restart_cnt")
				} else {
					j["restart_cnt"] = c.restart
				}
			})
			j := accountingJobs(t, doc, "v0.0.42")["4242"]
			if j.Phase != c.phase || j.Requeued != c.requeued || j.RestartCount != c.count {
				t.Errorf("phase %s, Requeued %v, RestartCount %d; want %s, %v, %d", j.Phase, j.Requeued, j.RestartCount, c.phase, c.requeued, c.count)
			}
		})
	}
}

// ---- the dialect check and the shape of a reply ----

func TestDataParserOf(t *testing.T) {
	for in, want := range map[string]string{
		"data_parser/v0.0.42": "v0.0.42",
		"data_parser/v0.0.40": "v0.0.40",
		"v0.0.42":             "v0.0.42",
		"":                    "",
	} {
		if got := DataParserOf(in); got != want {
			t.Errorf("DataParserOf(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestKnownDataParsers(t *testing.T) {
	if want := []string{"v0.0.40", "v0.0.42"}; !reflect.DeepEqual(KnownDataParsers, want) {
		t.Errorf("KnownDataParsers = %v, want %v (HBEP-4)", KnownDataParsers, want)
	}
	for _, v := range dialects {
		if !knownDataParser(v) {
			t.Errorf("%s is not a known data_parser", v)
		}
	}
	for _, v := range []string{"", "v0.0.41", "v0.0.43", "data_parser/v0.0.42"} {
		if knownDataParser(v) {
			t.Errorf("%q is a known data_parser", v)
		}
	}
}

// A reply whose data_parser differs from the profile's is refused with
// ErrDialectMismatch and not interpreted. S7's real data_parser/v0.0.42 matches
// a profile value of v0.0.42.
func TestDialectCheck(t *testing.T) {
	other := map[string]string{"v0.0.40": "v0.0.42", "v0.0.42": "v0.0.40"}
	for _, v := range dialects {
		t.Run(v, func(t *testing.T) {
			for _, f := range []struct {
				name string
				read func(doc []byte, want string) (string, int, error)
			}{
				{"squeue-running.json", func(doc []byte, want string) (string, int, error) {
					recs, got, err := parseQueue(doc, want)
					return got, len(recs), err
				}},
				{"sacct-completed.json", func(doc []byte, want string) (string, int, error) {
					recs, got, err := parseAccounting(doc, want)
					return got, len(recs), err
				}},
			} {
				doc := readTestdata(t, v, f.name)
				if _, n, err := f.read(doc, v); err != nil || n != 1 {
					t.Errorf("%s with the profile value %s: %d records, error %v", f.name, v, n, err)
				}
				got, n, err := f.read(doc, other[v])
				if !errors.Is(err, provider.ErrDialectMismatch) {
					t.Errorf("%s with the profile value %s: error %v, want ErrDialectMismatch", f.name, other[v], err)
				}
				if got != v {
					t.Errorf("%s: the reply's version is reported as %q, want %q", f.name, got, v)
				}
				if n != 0 {
					t.Errorf("%s: a mismatching reply was interpreted: %d records", f.name, n)
				}
				for _, want := range []string{"", "v0.0.4", "v0.0.420", "data_parser/" + v, "V0.0.42"} {
					if want == v {
						continue
					}
					if _, _, err := f.read(doc, want); !errors.Is(err, provider.ErrDialectMismatch) {
						t.Errorf("%s with the profile value %q: error %v, want ErrDialectMismatch", f.name, want, err)
					}
				}
			}
		})
	}
}

func TestDialectCheckReplyWithoutOrWithOtherDataParser(t *testing.T) {
	for _, edit := range []struct {
		name string
		fn   func(top map[string]any)
	}{
		{"no data_parser", func(top map[string]any) {
			delete(top["meta"].(map[string]any)["plugin"].(map[string]any), "data_parser")
		}},
		{"no plugin", func(top map[string]any) { delete(top["meta"].(map[string]any), "plugin") }},
		{"no meta", func(top map[string]any) { delete(top, "meta") }},
		{"another version", func(top map[string]any) {
			top["meta"].(map[string]any)["plugin"].(map[string]any)["data_parser"] = "data_parser/v0.0.99"
		}},
	} {
		t.Run(edit.name, func(t *testing.T) {
			q := editDoc(t, readTestdata(t, "v0.0.42", "squeue-running.json"), edit.fn)
			if err := queueErr(q, "v0.0.42"); !errors.Is(err, provider.ErrDialectMismatch) {
				t.Errorf("squeue: error %v, want ErrDialectMismatch", err)
			}
			a := editDoc(t, readTestdata(t, "v0.0.42", "sacct-completed.json"), edit.fn)
			if err := accountingErr(a, "v0.0.42"); !errors.Is(err, provider.ErrDialectMismatch) {
				t.Errorf("sacct: error %v, want ErrDialectMismatch", err)
			}
		})
	}
}

// An empty listing is a valid answer (zero jobs, no error). A reply with no
// jobs key, with errors, or that is not JSON is an error: the read is unknown,
// never "no job". The error is not a dialect mismatch: those fixtures carry the
// matching data_parser.
func TestReplyShape(t *testing.T) {
	for _, v := range dialects {
		t.Run(v, func(t *testing.T) {
			for _, f := range []struct {
				name string
				read func(doc []byte) (int, error)
			}{
				{"squeue", func(doc []byte) (int, error) { r, _, err := parseQueue(doc, v); return len(r), err }},
				{"sacct", func(doc []byte) (int, error) { r, _, err := parseAccounting(doc, v); return len(r), err }},
			} {
				if n, err := f.read(readTestdata(t, v, f.name+"-empty.json")); err != nil || n != 0 {
					t.Errorf("%s empty listing: %d records, error %v; want none and no error", f.name, n, err)
				}
				for _, bad := range []string{"-no-jobs.json", "-errors.json"} {
					n, err := f.read(readTestdata(t, v, f.name+bad))
					if err == nil {
						t.Errorf("%s%s: no error", f.name, bad)
					}
					if errors.Is(err, provider.ErrDialectMismatch) {
						t.Errorf("%s%s: reported as a dialect mismatch: %v", f.name, bad, err)
					}
					if n != 0 {
						t.Errorf("%s%s: %d records returned along with the error", f.name, bad, n)
					}
				}
				for _, raw := range []string{"", "{", "not json", "<html>502 Bad Gateway</html>", "[]", "null"} {
					if _, err := f.read([]byte(raw)); err == nil {
						t.Errorf("%s: the reply %q was read without an error", f.name, raw)
					}
				}
			}
		})
	}
}

// Errors win even when the reply also lists a job: no partial result.
func TestReplyWithJobsAndErrorsIsAnError(t *testing.T) {
	withErrors := func(top map[string]any) {
		top["errors"] = []any{map[string]any{"description": "Unable to query jobs", "error_number": 2002}}
	}
	q := editDoc(t, readTestdata(t, "v0.0.42", "squeue-running.json"), withErrors)
	if recs, _, err := parseQueue(q, "v0.0.42"); err == nil || recs != nil {
		t.Errorf("squeue: %d records, error %v; want no records and an error", len(recs), err)
	}
	a := editDoc(t, readTestdata(t, "v0.0.42", "sacct-completed.json"), withErrors)
	if recs, _, err := parseAccounting(a, "v0.0.42"); err == nil || recs != nil {
		t.Errorf("sacct: %d records, error %v; want no records and an error", len(recs), err)
	}
}

// A field the parser reads that holds the wrong type is an error, never a zero.
func TestMalformedFieldsAreErrors(t *testing.T) {
	for _, c := range []struct {
		name string
		edit func(j map[string]any)
	}{
		{"time_limit text", func(j map[string]any) { j["time_limit"] = "soon" }},
		{"start_time text", func(j map[string]any) { j["start_time"] = "yesterday" }},
		{"job_state number", func(j map[string]any) { j["job_state"] = 5.0 }},
		{"restart_cnt text", func(j map[string]any) { j["restart_cnt"] = "one" }},
		{"hold text", func(j map[string]any) { j["hold"] = "yes" }},
	} {
		t.Run("squeue "+c.name, func(t *testing.T) {
			doc := onlyJob(t, readTestdata(t, "v0.0.42", "squeue-running.json"), 4242, c.edit)
			if err := queueErr(doc, "v0.0.42"); err == nil {
				t.Error("read without an error")
			}
		})
	}
	for _, c := range []struct {
		name string
		edit func(j map[string]any)
	}{
		{"time.end text", func(j map[string]any) { j["time"].(map[string]any)["end"] = "later" }},
		{"state.current number", func(j map[string]any) { j["state"].(map[string]any)["current"] = 7.0 }},
		{"tres.requested text", func(j map[string]any) { j["tres"].(map[string]any)["requested"] = "gpu" }},
	} {
		t.Run("sacct "+c.name, func(t *testing.T) {
			doc := onlyJob(t, readTestdata(t, "v0.0.42", "sacct-completed.json"), 4242, c.edit)
			if err := accountingErr(doc, "v0.0.42"); err == nil {
				t.Error("read without an error")
			}
		})
	}
}

// The readers accept a bare number where Slurm's {set, infinite, number} triple
// is expected, and a string where a list of states is expected (parse.go).
func TestTolerantReaders(t *testing.T) {
	doc := onlyJob(t, readTestdata(t, "v0.0.42", "squeue-running.json"), 4242, func(j map[string]any) {
		j["time_limit"] = 10.0
		j["start_time"] = 1793351245.0
		j["end_time"] = 1793351845.0
		j["job_state"] = "RUNNING"
	})
	want := map[string]wantJob{"4242": queueWant("v0.0.42")["4242"]}
	checkJobs(t, queueJobs(t, doc, "v0.0.42"), want)
}

// An unset or infinite triple is no time and no limit; zero is no time.
func TestUnsetAndInfiniteTriples(t *testing.T) {
	doc := onlyJob(t, readTestdata(t, "v0.0.42", "squeue-running.json"), 4242, func(j map[string]any) {
		j["time_limit"] = map[string]any{"set": true, "infinite": true, "number": 0.0}
		j["submit_time"] = triple(false, 0)
		j["start_time"] = triple(true, 0)
		j["end_time"] = nil
	})
	j := queueJobs(t, doc, "v0.0.42")["4242"]
	if j.TimeLimit != 0 || !j.SubmitTime.IsZero() || j.StartTime != nil || j.HardEnd != nil || j.EndTime != nil {
		t.Errorf("limit %s, submit %v, start %s, hard end %s, end %s; want none of them",
			j.TimeLimit, j.SubmitTime, timeText(j.StartTime), timeText(j.HardEnd), timeText(j.EndTime))
	}
}

// ---- GPUs ----

func TestGPUsOfTres(t *testing.T) {
	for _, c := range []struct {
		tres string
		want int32
	}{
		{"cpu=2,mem=1G,node=1,billing=2", 0}, // S7's CPU job
		{"", 0},
		{"cpu=8,mem=16G,node=1,billing=8,gres/gpu=1", 1},
		{"cpu=16,mem=128G,node=1,billing=16,gres/gpu:a100=2", 2},
		// Slurm lists the untyped and the typed entry of one GPU request: 2, not 4.
		{"cpu=16,mem=128G,node=1,billing=16,gres/gpu=2,gres/gpu:a100=2", 2},
		{"cpu=16,mem=128G,node=1,billing=16,gres/gpu:a100=2,gres/gpu=2", 2},
		// Typed entries alone add up.
		{"gres/gpu:a100=1,gres/gpu:h100=1", 2},
		{"gres/gpu=4", 4},
		// Other gres, and a name that only starts like gpu, are no GPUs.
		{"cpu=2,gres/gpumem=3,gres/shard=4", 0},
		{"gres/gpu=abc", 0},
		{"gres/gpu=", 0},
		{"gres/gpu", 0},
	} {
		if got := gpusOfTres(c.tres); got != c.want {
			t.Errorf("gpusOfTres(%q) = %d, want %d", c.tres, got, c.want)
		}
	}
}

func TestGPUsOfTresList(t *testing.T) {
	e := func(typ, name string, count int64) tresEntry { return tresEntry{Type: typ, Name: name, Count: &count} }
	for _, c := range []struct {
		name string
		l    []tresEntry
		gpus int32
		ok   bool
	}{
		{"S7's allocated list, no GPU", []tresEntry{e("cpu", "", 4), e("mem", "", 6144), e("energy", "", -2), e("node", "", 1), e("billing", "", 4)}, 0, false},
		{"empty", nil, 0, false},
		{"untyped", []tresEntry{e("cpu", "", 8), e("gres", "gpu", 1)}, 1, true},
		{"typed", []tresEntry{e("gres", "gpu:a100", 2)}, 2, true},
		{"both: the untyped entry wins", []tresEntry{e("gres", "gpu:a100", 2), e("gres", "gpu", 2)}, 2, true},
		{"two types", []tresEntry{e("gres", "gpu:a100", 1), e("gres", "gpu:h100", 1)}, 2, true},
		{"not a gres", []tresEntry{e("cpu", "gpu", 9)}, 0, false},
		{"another gres", []tresEntry{e("gres", "shard", 4), e("gres", "gpumem", 80)}, 0, false},
	} {
		g, ok := gpusOfTresList(c.l)
		if g != c.gpus || ok != c.ok {
			t.Errorf("%s: gpusOfTresList = %d, %v; want %d, %v", c.name, g, ok, c.gpus, c.ok)
		}
	}
}

// ---- "not listed" ----

func TestNotListed(t *testing.T) {
	const s7 = "slurm_load_jobs error: Invalid job id specified\n"
	for _, c := range []struct {
		name string
		res  Result
		want bool
	}{
		{"S7's capture: exit 1 and the message", Result{ExitCode: 1, Stderr: []byte(s7)}, true},
		{"the message among other lines", Result{ExitCode: 1, Stderr: []byte("squeue: warning: x\n" + s7)}, true},
		{"same message, exit 2", Result{ExitCode: 2, Stderr: []byte(s7)}, false},
		{"same message, exit 0", Result{ExitCode: 0, Stderr: []byte(s7)}, false},
		{"same message, exit 255", Result{ExitCode: 255, Stderr: []byte(s7)}, false},
		{"exit 1, controller unreachable", Result{ExitCode: 1, Stderr: []byte("slurm_load_jobs error: Unable to contact slurm controller (connect failure)\n")}, false},
		{"exit 1, no message", Result{ExitCode: 1}, false},
		{"exit 1, message on stdout only", Result{ExitCode: 1, Stdout: []byte(s7)}, false},
		{"exit 127", Result{ExitCode: 127, Stderr: []byte("squeue: command not found")}, false},
	} {
		if got := notListed(c.res); got != c.want {
			t.Errorf("%s: notListed = %v, want %v", c.name, got, c.want)
		}
	}
}

// A record without a job_id is an error, never job "0": the ID is the key of
// every later Observe, Cancel and LiftHold.
func TestRecordWithoutJobIDIsAnError(t *testing.T) {
	q := onlyJob(t, readTestdata(t, "v0.0.42", "squeue-running.json"), 4242, func(j map[string]any) { delete(j, "job_id") })
	if err := queueErr(q, "v0.0.42"); err == nil {
		t.Error("squeue: a record without job_id was read")
	}
	a := onlyJob(t, readTestdata(t, "v0.0.42", "sacct-completed.json"), 4242, func(j map[string]any) { delete(j, "job_id") })
	if err := accountingErr(a, "v0.0.42"); err == nil {
		t.Error("sacct: a record without job_id was read")
	}
}
