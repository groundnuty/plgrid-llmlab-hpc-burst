package slurm

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

// Allocated time from accounting (HBEP-8, Provider accounting; HBEP-4, The Slurm
// dialect, Accounting). The expected numbers are written out by hand from the
// raw fields of the fixtures, never from this package's output:
//
//   - Athena's real replies (testdata/v0.0.40-athena-real): job 4501 of
//     sacct-completed.json has time.elapsed 90, start 1793452614, end
//     1793452704 (1793452704 - 1793452614 = 90), tres.allocated cpu 4 and
//     gres gpu 1; sacct-completed-after-hold.json has elapsed 91, start
//     1793452734, end 1793452825 (91), the same allocation.
//   - S7's real Helios CPU job (testdata/v0.0.42/sacct-completed.json): job
//     4242, elapsed 227 (1793350184 - 1793349957), tres.allocated cpu 4 and no
//     gres entry.
//   - Derived, not real: sacct-derived.json and sacct-requeue.json (testdata
//     README says which values are invented).

func realAcct(t *testing.T, version, dir, name string, id string) provider.Job {
	t.Helper()
	var doc []byte
	if dir == "" {
		doc = readTestdata(t, version, name)
	} else {
		doc = readTestdata(t, dir, name)
	}
	j, ok := accountingJobs(t, doc, version)[id]
	if !ok {
		t.Fatalf("%s: no job %s", name, id)
	}
	return j
}

func wantAcct(t *testing.T, j provider.Job, cpus, gpus int32, elapsed time.Duration, executions int) {
	t.Helper()
	a := j.Accounting
	if a == nil {
		t.Fatalf("job %s has no Accounting", j.ID)
	}
	count := func(name string, got *int32, want int32) {
		switch {
		case want < 0 && got != nil:
			t.Errorf("job %s %s = %d, want unknown", j.ID, name, *got)
		case want >= 0 && (got == nil || *got != want):
			t.Errorf("job %s %s = %v, want %d", j.ID, name, got, want)
		}
	}
	count("AllocatedCPUs", a.AllocatedCPUs, cpus)
	count("AllocatedGPUs", a.AllocatedGPUs, gpus)
	switch {
	case elapsed < 0 && a.Elapsed != nil:
		t.Errorf("job %s Elapsed = %s, want unknown", j.ID, *a.Elapsed)
	case elapsed >= 0 && (a.Elapsed == nil || *a.Elapsed != elapsed):
		t.Errorf("job %s Elapsed = %v, want %s", j.ID, a.Elapsed, elapsed)
	}
	if a.Executions != executions {
		t.Errorf("job %s Executions = %d, want %d", j.ID, a.Executions, executions)
	}
}

func TestAccountingOfRealAthenaGPUJobs(t *testing.T) {
	first := realAcct(t, "v0.0.40", "v0.0.40-athena-real", "sacct-completed.json", "4501")
	wantAcct(t, first, 4, 1, 90*time.Second, 1)
	second := realAcct(t, "v0.0.40", "v0.0.40-athena-real", "sacct-completed-after-hold.json", "4501")
	wantAcct(t, second, 4, 1, 91*time.Second, 1)
}

// Two real sources of Athena's first job agree: accounting's time.elapsed is 90,
// and the queue listing that still shows the ended job gives end minus start as
// 1793452704 - 1793452614 = 90. The arithmetic of the status block is checked
// from both, with hours worked out by hand: 1 GPU for 90 s is 90 GPU-seconds
// (0.025 h), 4 CPUs for 90 s is 360 CPU-seconds (0.1 h).
func TestAccountingStatusFromRealAthenaJob(t *testing.T) {
	now := at(2026, 10, 31, 20, 0, 0)
	fromAcct := realAcct(t, "v0.0.40", "v0.0.40-athena-real", "sacct-completed.json", "4501")
	queue := queueJobs(t, readTestdata(t, "v0.0.40-athena-real", "squeue-completed-listed.json"), "v0.0.40")["4501"]
	if queue.Source != provider.SourceQueue || queue.Accounting != nil {
		t.Fatalf("the listing's record: source %s accounting %v, want the queue's, none", queue.Source, queue.Accounting)
	}
	// The listing gives no allocation: it comes from the stored status, set by
	// the one accounting read.
	stored := provider.AccountingOf(fromAcct, 1, nil, now)

	for name, got := range map[string]*v1alpha1.AllocationAccounting{
		"final state from accounting":  provider.AccountingOf(fromAcct, 1, nil, now),
		"final state from the listing": provider.AccountingOf(queue, 1, stored, now),
	} {
		if got == nil || got.ElapsedSeconds == nil || *got.ElapsedSeconds != 90 {
			t.Errorf("%s: %+v, want 90 s elapsed", name, got)
			continue
		}
		if s, ok := got.GPUSeconds(); !ok || s != 90 {
			t.Errorf("%s: GPU-seconds %d %v, want 90", name, s, ok)
		}
		if s, ok := got.CPUSeconds(); !ok || s != 360 {
			t.Errorf("%s: CPU-seconds %d %v, want 360", name, s, ok)
		}
		if h, ok := got.GPUHours(); !ok || math.Abs(h-0.025) > 1e-12 {
			t.Errorf("%s: GPU-hours %v %v, want 0.025", name, h, ok)
		}
		if h, ok := got.CPUHours(); !ok || math.Abs(h-0.1) > 1e-12 {
			t.Errorf("%s: CPU-hours %v %v, want 0.1", name, h, ok)
		}
	}
	// Athena's second job: 91 s, so 91 GPU-seconds and 364 CPU-seconds.
	second := provider.AccountingOf(realAcct(t, "v0.0.40", "v0.0.40-athena-real", "sacct-completed-after-hold.json", "4501"), 1, nil, now)
	if s, _ := second.GPUSeconds(); s != 91 {
		t.Errorf("second job GPU-seconds %d, want 91", s)
	}
	if s, _ := second.CPUSeconds(); s != 364 {
		t.Errorf("second job CPU-seconds %d, want 364", s)
	}
}

// S7's real Helios CPU job has no gres entry: the missing GPU TRES is 0 GPUs
// for a job that asked for none, and unknown for one that asked for a GPU.
func TestAccountingMissingGPUEntryInRealHeliosCPUJob(t *testing.T) {
	j := realAcct(t, "v0.0.42", "", "sacct-completed.json", "4242")
	wantAcct(t, j, 4, -1, 227*time.Second, 1)
	now := at(2026, 10, 31, 20, 0, 0)

	cpu := provider.AccountingOf(j, 0, nil, now)
	if cpu.AllocatedGPUs == nil || *cpu.AllocatedGPUs != 0 {
		t.Errorf("a CPU job: GPUs %v, want a known 0", cpu.AllocatedGPUs)
	}
	if s, ok := cpu.CPUSeconds(); !ok || s != 908 { // 4 CPUs x 227 s
		t.Errorf("CPU-seconds %d %v, want 908", s, ok)
	}
	if h, ok := cpu.CPUHours(); !ok || math.Abs(h-908.0/3600) > 1e-12 {
		t.Errorf("CPU-hours %v %v, want 908/3600", h, ok)
	}
	gpu := provider.AccountingOf(j, 1, nil, now)
	if gpu.AllocatedGPUs != nil {
		t.Errorf("a GPU asked for and none listed: GPUs %d, want unknown", *gpu.AllocatedGPUs)
	}
	if _, ok := gpu.GPUHours(); ok {
		t.Error("GPU-hours are known without a GPU count")
	}
}

// The derived fixtures cover the shapes no real capture has: running jobs with
// untyped, typed and both kinds of GPU entry, a pending job with no allocation,
// and a requeue, in both dialects.
func TestAccountingOfDerivedFixtures(t *testing.T) {
	for _, v := range dialects {
		t.Run(v, func(t *testing.T) {
			jobs := accountingJobs(t, readTestdata(t, v, "sacct-derived.json"), v)
			wantAcct(t, jobs["4304"], 8, 1, 0, 1) // gres/gpu 1
			wantAcct(t, jobs["4305"], 8, 2, 0, 1) // typed gpu:a100 2 only
			wantAcct(t, jobs["4306"], 8, 1, 0, 1) // from allocated only
			wantAcct(t, jobs["4307"], 8, 2, 0, 1) // untyped 2 and typed 2: the untyped entry wins, not 4
			wantAcct(t, jobs["4308"], 8, 1, 8*time.Second, 1)
			wantAcct(t, jobs["4301"], -1, -1, 0, 1) // pending: nothing allocated
			// The DST job lists a cpu entry in v0.0.42 and no allocation in v0.0.40.
			cpus := int32(-1)
			if v == "v0.0.42" {
				cpus = 4
			}
			wantAcct(t, jobs["4310"], cpus, -1, 3600*time.Second, 1)
		})
	}
}

// A requeue under -D: the later execution is 60 s, the earlier 55 s, so 115 s.
// v0.0.42 numbers the executions (restart_cnt); v0.0.40 marks the earlier with
// REQUEUED. The v0.0.40 fixture lists no tres.allocated.
func TestAccountingSumsRequeuedExecutions(t *testing.T) {
	wantAcct(t, accountingJobs(t, readTestdata(t, "v0.0.42", "sacct-requeue.json"), "v0.0.42")["4320"], 4, -1, 115*time.Second, 2)
	wantAcct(t, accountingJobs(t, readTestdata(t, "v0.0.40", "sacct-requeue.json"), "v0.0.40")["4320"], -1, -1, 115*time.Second, 2)
	// The order of the records in the reply does not matter.
	doc := editDoc(t, readTestdata(t, "v0.0.42", "sacct-requeue.json"), func(top map[string]any) {
		jobs := top["jobs"].([]any)
		for i, k := 0, len(jobs)-1; i < k; i, k = i+1, k-1 {
			jobs[i], jobs[k] = jobs[k], jobs[i]
		}
	})
	wantAcct(t, accountingJobs(t, doc, "v0.0.42")["4320"], 4, -1, 115*time.Second, 2)
}

// A requeued job whose earlier execution the read did not return (the restart
// count says there were two, the reply holds one) has no elapsed time, not the
// time of one execution.
func TestAccountingElapsedUnknownWhenAnExecutionIsMissing(t *testing.T) {
	jobs := accountingJobs(t, readTestdata(t, "v0.0.42", "sacct-derived.json"), "v0.0.42")
	if jobs["4303"].RestartCount != 1 {
		t.Fatalf("precondition: job 4303 has restart count %d, want 1", jobs["4303"].RestartCount)
	}
	wantAcct(t, jobs["4303"], 4, -1, -1, 1)
}

// Variants of Athena's real record, made inline: what is missing or odd in
// time.elapsed and tres.allocated is unknown, never 0.
func TestAccountingVariants(t *testing.T) {
	doc := readTestdata(t, "v0.0.40-athena-real", "sacct-completed.json")
	edit := func(f func(job map[string]any)) provider.Job {
		d := onlyJob(t, doc, 4501, f)
		return accountingJobs(t, d, "v0.0.40")["4501"]
	}
	timeOf := func(j map[string]any) map[string]any { return j["time"].(map[string]any) }
	tresOf := func(j map[string]any) map[string]any { return j["tres"].(map[string]any) }

	wantAcct(t, edit(nil), 4, 1, 90*time.Second, 1)
	wantAcct(t, edit(func(j map[string]any) { delete(timeOf(j), "elapsed") }), 4, 1, -1, 1)
	wantAcct(t, edit(func(j map[string]any) { timeOf(j)["elapsed"] = triple(false, 0) }), 4, 1, -1, 1)
	wantAcct(t, edit(func(j map[string]any) { timeOf(j)["elapsed"] = -5 }), 4, 1, -1, 1)
	wantAcct(t, edit(func(j map[string]any) { timeOf(j)["elapsed"] = triple(true, 91) }), 4, 1, 91*time.Second, 1)
	wantAcct(t, edit(func(j map[string]any) { delete(tresOf(j), "allocated") }), -1, -1, 90*time.Second, 1)
	wantAcct(t, edit(func(j map[string]any) { tresOf(j)["allocated"] = []any{} }), -1, -1, 90*time.Second, 1)
	// Only the gpu entry removed: the CPUs stay, the GPUs are unknown here (the
	// controller decides 0 or unknown from the request).
	wantAcct(t, edit(func(j map[string]any) {
		var keep []any
		for _, e := range tresOf(j)["allocated"].([]any) {
			if e.(map[string]any)["type"] != "gres" {
				keep = append(keep, e)
			}
		}
		tresOf(j)["allocated"] = keep
	}), 4, -1, 90*time.Second, 1)
}

// allocatedOf returns the Accounting of Athena's real record with its
// tres.allocated replaced by the entries in raw, a JSON list written inline.
// A count that is null stays null and a count left out stays absent.
func allocatedOf(t *testing.T, raw string) *provider.JobAccounting {
	t.Helper()
	var entries []any
	if err := json.Unmarshal([]byte(raw), &entries); err != nil {
		t.Fatalf("bad inline tres list %s: %v", raw, err)
	}
	doc := onlyJob(t, readTestdata(t, "v0.0.40-athena-real", "sacct-completed.json"), 4501, func(j map[string]any) {
		j["tres"].(map[string]any)["allocated"] = entries
	})
	a := accountingJobs(t, doc, "v0.0.40")["4501"].Accounting
	if a == nil {
		t.Fatalf("%s: the job has no Accounting", raw)
	}
	return a
}

// A tres count that cannot be read is unknown, never a number: null or no
// count, a negative sentinel and a count above MaxInt32 (2147483647). The
// expected counts are written by hand; -1 stands for unknown (nil).
func TestAccountingAllocatedTresValidation(t *testing.T) {
	for _, c := range []struct {
		name   string
		tres   string
		cpus   int32
		gpus   int32
		listed bool
	}{
		// A missing or null count is not 0.
		{"untyped gpu with a null count", `[{"type":"cpu","count":4},{"type":"gres","name":"gpu","count":null}]`, 4, -1, true},
		{"untyped gpu with no count key", `[{"type":"cpu","count":4},{"type":"gres","name":"gpu"}]`, 4, -1, true},
		{"typed gpu with a null count", `[{"type":"cpu","count":4},{"type":"gres","name":"gpu:a100","count":null}]`, 4, -1, true},
		{"cpu with a null count", `[{"type":"cpu","count":null},{"type":"gres","name":"gpu","count":1}]`, -1, 1, true},
		{"cpu with no count key", `[{"type":"cpu"},{"type":"gres","name":"gpu","count":1}]`, -1, 1, true},
		// The CPU count is the first cpu entry's, valid or not.
		{"cpu with a negative count", `[{"type":"cpu","count":-2}]`, -1, -1, false},
		{"cpu above MaxInt32", `[{"type":"cpu","count":2147483648}]`, -1, -1, false},
		{"cpu at MaxInt32", `[{"type":"cpu","count":2147483647}]`, 2147483647, -1, false},
		{"unreadable first cpu entry, readable second", `[{"type":"cpu","count":null},{"type":"cpu","count":4}]`, -1, -1, false},
		// A negative typed count is a sentinel: not netted against the others.
		{"typed -1 next to typed 2", `[{"type":"gres","name":"gpu:a100","count":-1},{"type":"gres","name":"gpu:h100","count":2}]`, -1, -1, true},
		{"typed 3 next to typed -2", `[{"type":"gres","name":"gpu:a100","count":3},{"type":"gres","name":"gpu:h100","count":-2}]`, -1, -1, true},
		{"typed 2 next to a typed null", `[{"type":"gres","name":"gpu:a100","count":2},{"type":"gres","name":"gpu:h100","count":null}]`, -1, -1, true},
		// An unreadable untyped count never falls back to the typed entries.
		{"untyped -1 next to typed 2", `[{"type":"gres","name":"gpu","count":-1},{"type":"gres","name":"gpu:a100","count":2}]`, -1, -1, true},
		{"untyped null next to typed 2", `[{"type":"gres","name":"gpu","count":null},{"type":"gres","name":"gpu:a100","count":2}]`, -1, -1, true},
		{"untyped above MaxInt32", `[{"type":"gres","name":"gpu","count":2147483648}]`, -1, -1, true},
		{"two untyped entries that disagree", `[{"type":"gres","name":"gpu","count":1},{"type":"gres","name":"gpu","count":2}]`, -1, -1, true},
		{"two untyped entries, the first unreadable", `[{"type":"gres","name":"gpu","count":null},{"type":"gres","name":"gpu","count":2}]`, -1, -1, true},
		// The sum of typed counts is bounded.
		{"typed sum above MaxInt32", `[{"type":"gres","name":"gpu:a100","count":2147483647},{"type":"gres","name":"gpu:h100","count":1}]`, -1, -1, true},
		{"typed sum at MaxInt32", `[{"type":"gres","name":"gpu:a100","count":2147483646},{"type":"gres","name":"gpu:h100","count":1}]`, -1, 2147483647, true},
		// Readable counts still read as before.
		{"untyped 2", `[{"type":"cpu","count":8},{"type":"gres","name":"gpu","count":2}]`, 8, 2, true},
		{"untyped 0 is a listed 0", `[{"type":"gres","name":"gpu","count":0}]`, -1, 0, true},
		{"two untyped entries that agree", `[{"type":"gres","name":"gpu","count":2},{"type":"gres","name":"gpu","count":2}]`, -1, 2, true},
		{"typed 1 and typed 2", `[{"type":"gres","name":"gpu:a100","count":1},{"type":"gres","name":"gpu:h100","count":2}]`, -1, 3, true},
		{"untyped 2 wins over typed 4 in total", `[{"type":"gres","name":"gpu","count":2},{"type":"gres","name":"gpu:a100","count":3},{"type":"gres","name":"gpu:h100","count":1}]`, -1, 2, true},
		{"readable untyped 2 beside an unreadable typed", `[{"type":"gres","name":"gpu","count":2},{"type":"gres","name":"gpu:a100","count":-1}]`, -1, 2, true},
		// Entries of other types are not read: a sentinel there changes nothing.
		{"energy -2 and a null mem beside cpu 4", `[{"type":"cpu","count":4},{"type":"mem"},{"type":"energy","count":-2},{"type":"node","count":null}]`, 4, -1, false},
		{"energy -2 beside gpu 1", `[{"type":"energy","count":-2},{"type":"gres","name":"gpu","count":1}]`, -1, 1, true},
		{"another gres is not a gpu", `[{"type":"gres","name":"shard","count":null},{"type":"gres","name":"gpumem","count":-1}]`, -1, -1, false},
		{"a cpu entry named gpu is not a gpu", `[{"type":"cpu","name":"gpu","count":-1}]`, -1, -1, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			a := allocatedOf(t, c.tres)
			switch {
			case c.cpus < 0 && a.AllocatedCPUs != nil:
				t.Errorf("AllocatedCPUs = %d, want unknown", *a.AllocatedCPUs)
			case c.cpus >= 0 && (a.AllocatedCPUs == nil || *a.AllocatedCPUs != c.cpus):
				t.Errorf("AllocatedCPUs = %v, want %d", a.AllocatedCPUs, c.cpus)
			}
			switch {
			case c.gpus < 0 && a.AllocatedGPUs != nil:
				t.Errorf("AllocatedGPUs = %d, want unknown", *a.AllocatedGPUs)
			case c.gpus >= 0 && (a.AllocatedGPUs == nil || *a.AllocatedGPUs != c.gpus):
				t.Errorf("AllocatedGPUs = %v, want %d", a.AllocatedGPUs, c.gpus)
			}
			if a.GPUsListed != c.listed {
				t.Errorf("GPUsListed = %v, want %v", a.GPUsListed, c.listed)
			}
		})
	}
}

// The real records: v0.0.42's energy entry has count -2, a negative sentinel on
// a type that is not read, so the CPUs stay 4 and no GPU is listed. Athena's
// real GPU job lists one gres gpu entry.
func TestAccountingRealRecordsGPUsListed(t *testing.T) {
	for _, c := range []struct {
		name   string
		j      provider.Job
		cpus   int32
		gpus   int32
		listed bool
	}{
		{"v0.0.42 requeue, energy -2", accountingJobs(t, readTestdata(t, "v0.0.42", "sacct-requeue.json"), "v0.0.42")["4320"], 4, -1, false},
		{"v0.0.42 Helios CPU job", realAcct(t, "v0.0.42", "", "sacct-completed.json", "4242"), 4, -1, false},
		{"v0.0.40 Athena GPU job", realAcct(t, "v0.0.40", "v0.0.40-athena-real", "sacct-completed.json", "4501"), 4, 1, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			a := c.j.Accounting
			if a == nil {
				t.Fatal("no Accounting")
			}
			if a.AllocatedCPUs == nil || *a.AllocatedCPUs != c.cpus {
				t.Errorf("AllocatedCPUs = %v, want %d", a.AllocatedCPUs, c.cpus)
			}
			switch {
			case c.gpus < 0 && a.AllocatedGPUs != nil:
				t.Errorf("AllocatedGPUs = %d, want unknown", *a.AllocatedGPUs)
			case c.gpus >= 0 && (a.AllocatedGPUs == nil || *a.AllocatedGPUs != c.gpus):
				t.Errorf("AllocatedGPUs = %v, want %d", a.AllocatedGPUs, c.gpus)
			}
			if a.GPUsListed != c.listed {
				t.Errorf("GPUsListed = %v, want %v", a.GPUsListed, c.listed)
			}
		})
	}
}

// Accounting is read for a job the listing does not show, and the job from the
// queue listing has none.
func TestQueueRecordHasNoAccounting(t *testing.T) {
	for _, v := range dialects {
		for id, j := range queueJobs(t, readTestdata(t, v, "squeue-derived.json"), v) {
			if j.Accounting != nil {
				t.Errorf("%s job %s from the listing has Accounting %+v", v, id, j.Accounting)
			}
		}
	}
}
