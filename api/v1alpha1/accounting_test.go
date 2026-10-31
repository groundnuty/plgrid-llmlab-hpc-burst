package v1alpha1

import (
	"math"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func i32(v int32) *int32 { return &v }
func i64(v int64) *int64 { return &v }

// The expected hours are written out by hand from the numbers of Athena's real
// accounting capture (internal/slurm/testdata/v0.0.40-athena-real/
// sacct-completed.json: 4 CPUs, 1 GPU, 90 s elapsed): 90 GPU-seconds, 360
// CPU-seconds, so 90/3600 = 0.025 GPU-hours and 360/3600 = 0.1 CPU-hours.
func TestAccountingHours(t *testing.T) {
	a := AllocationAccounting{AllocatedCPUs: i32(4), AllocatedGPUs: i32(1), ElapsedSeconds: i64(90)}
	if s, ok := a.GPUSeconds(); !ok || s != 90 {
		t.Errorf("GPUSeconds = %d, %v; want 90", s, ok)
	}
	if s, ok := a.CPUSeconds(); !ok || s != 360 {
		t.Errorf("CPUSeconds = %d, %v; want 360", s, ok)
	}
	if h, ok := a.GPUHours(); !ok || math.Abs(h-0.025) > 1e-12 {
		t.Errorf("GPUHours = %v, %v; want 0.025", h, ok)
	}
	if h, ok := a.CPUHours(); !ok || math.Abs(h-0.1) > 1e-12 {
		t.Errorf("CPUHours = %v, %v; want 0.1", h, ok)
	}
	// Two GPUs for three hours: 6 GPU-hours; eight CPUs: 24 CPU-hours.
	b := AllocationAccounting{AllocatedCPUs: i32(8), AllocatedGPUs: i32(2), ElapsedSeconds: i64(10800)}
	if h, ok := b.GPUHours(); !ok || h != 6 {
		t.Errorf("GPUHours = %v, %v; want 6", h, ok)
	}
	if h, ok := b.CPUHours(); !ok || h != 24 {
		t.Errorf("CPUHours = %v, %v; want 24", h, ok)
	}
}

// An unknown count or elapsed time makes the hours unknown, never 0; a known 0
// stays a known 0.
func TestAccountingHoursUnknown(t *testing.T) {
	for name, a := range map[string]AllocationAccounting{
		"no GPU count":     {AllocatedCPUs: i32(4), ElapsedSeconds: i64(90)},
		"no elapsed time":  {AllocatedCPUs: i32(4), AllocatedGPUs: i32(1)},
		"nothing but time": {},
	} {
		if h, ok := a.GPUHours(); ok {
			t.Errorf("%s: GPUHours = %v, known", name, h)
		}
	}
	cpuOnly := AllocationAccounting{AllocatedCPUs: i32(4), ElapsedSeconds: i64(90)}
	if h, ok := cpuOnly.CPUHours(); !ok || math.Abs(h-0.1) > 1e-12 {
		t.Errorf("CPUHours = %v, %v; want 0.1 whatever the GPU count", h, ok)
	}
	zero := AllocationAccounting{AllocatedCPUs: i32(4), AllocatedGPUs: i32(0), ElapsedSeconds: i64(90)}
	if h, ok := zero.GPUHours(); !ok || h != 0 {
		t.Errorf("a known 0 GPUs: GPUHours = %v, %v; want a known 0", h, ok)
	}
}

func TestAccountingRecordName(t *testing.T) {
	got, err := AccountingRecordName("0b8e3f7c-9d1a-4c52-8f60-2a4d6e8c1b57")
	if err != nil || got != "acct-0b8e3f7c-9d1a-4c52-8f60-2a4d6e8c1b57" {
		t.Errorf("AccountingRecordName = %q, %v", got, err)
	}
	for _, bad := range []string{"", "UPPER-CASE", "has space", "a/b", strings.Repeat("x", 250)} {
		if got, err := AccountingRecordName(types.UID(bad)); err == nil {
			t.Errorf("AccountingRecordName(%q) = %q, want an error", bad, got)
		}
	}
}

func accStart() *metav1.Time {
	m := metav1.NewTime(time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC))
	return &m
}
func accEnd() *metav1.Time {
	m := metav1.NewTime(time.Date(2026, 10, 31, 12, 1, 30, 0, time.UTC))
	return &m
}

// The data is a golden string, written by hand from HBEP-8's list: the Provider,
// the partition, the tag, the job IDs, allocatedCPUs, allocatedGPUs,
// elapsedSeconds, the start and end times and basis allocated.
func TestNewAccountingRecordGolden(t *testing.T) {
	rec, err := NewAccountingRecord("ns-a", "0b8e3f7c-9d1a-4c52-8f60-2a4d6e8c1b57", AccountingRecordData{
		Provider: "prov-a", Partition: "plgrid-gpu-a100", Tag: "hb-x-0123456789ab", JobIDs: []string{"4501", "4502"},
		AllocatedCPUs: i32(4), AllocatedGPUs: i32(1), ElapsedSeconds: i64(90), StartTime: accStart(), EndTime: accEnd(),
	})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"provider":"prov-a","partition":"plgrid-gpu-a100","tag":"hb-x-0123456789ab","jobIDs":["4501","4502"],` +
		`"allocatedCPUs":4,"allocatedGPUs":1,"elapsedSeconds":90,"startTime":"2026-10-31T12:00:00Z","endTime":"2026-10-31T12:01:30Z",` +
		`"basis":"allocated"}`
	if string(rec.Data.Raw) != want {
		t.Errorf("data\n got %s\nwant %s", rec.Data.Raw, want)
	}
	if rec.Name != "acct-0b8e3f7c-9d1a-4c52-8f60-2a4d6e8c1b57" || rec.Namespace != "ns-a" || rec.Revision != 1 {
		t.Errorf("name %q namespace %q revision %d", rec.Name, rec.Namespace, rec.Revision)
	}
	if len(rec.OwnerReferences) != 0 {
		t.Errorf("owner references %v: a record outlives its Allocation, so it has none", rec.OwnerReferences)
	}
	wantLabels := map[string]string{"hpcburst.groundnuty.github.io/record": "accounting", "hpcburst.groundnuty.github.io/provider": "prov-a"}
	if len(rec.Labels) != len(wantLabels) {
		t.Errorf("labels %v, want %v", rec.Labels, wantLabels)
	}
	for k, v := range wantLabels {
		if rec.Labels[k] != v {
			t.Errorf("label %s = %q, want %q", k, rec.Labels[k], v)
		}
	}
	if len(rec.Annotations) != 0 {
		t.Errorf("annotations %v, want none", rec.Annotations)
	}
}

// Every field that is not known is absent and named in unknown, in the fixed
// order, and the caller cannot make the two disagree.
func TestNewAccountingRecordUnknowns(t *testing.T) {
	rec, err := NewAccountingRecord("ns-a", "uid-1", AccountingRecordData{Provider: "prov-a", Partition: "p", Tag: "t",
		Unknown: []string{"a caller's list is replaced"}})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"provider":"prov-a","partition":"p","tag":"t","jobIDs":[],"basis":"allocated",` +
		`"unknown":["allocatedCPUs","allocatedGPUs","elapsedSeconds","startTime","endTime"]}`
	if string(rec.Data.Raw) != want {
		t.Errorf("data\n got %s\nwant %s", rec.Data.Raw, want)
	}
	rec, err = NewAccountingRecord("ns-a", "uid-1", AccountingRecordData{Provider: "prov-a", Partition: "p", Tag: "t",
		JobIDs: []string{"7"}, AllocatedCPUs: i32(2), AllocatedGPUs: i32(0), StartTime: accStart()})
	if err != nil {
		t.Fatal(err)
	}
	want = `{"provider":"prov-a","partition":"p","tag":"t","jobIDs":["7"],"allocatedCPUs":2,"allocatedGPUs":0,` +
		`"startTime":"2026-10-31T12:00:00Z","basis":"allocated","unknown":["elapsedSeconds","endTime"]}`
	if string(rec.Data.Raw) != want {
		t.Errorf("data\n got %s\nwant %s", rec.Data.Raw, want)
	}
}

func TestNewAccountingRecordRefuses(t *testing.T) {
	ok := AccountingRecordData{Provider: "prov-a", Partition: "p", Tag: "t"}
	if _, err := NewAccountingRecord("ns-a", "", ok); err == nil {
		t.Error("an empty UID was accepted")
	}
	for _, p := range []string{"", "Prov_A", strings.Repeat("p", 64)} {
		d := ok
		d.Provider = p
		if _, err := NewAccountingRecord("ns-a", "uid-1", d); err == nil {
			t.Errorf("Provider %q was accepted", p)
		}
	}
}
