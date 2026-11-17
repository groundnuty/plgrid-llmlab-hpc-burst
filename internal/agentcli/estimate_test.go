package agentcli

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The plan of these rows is observed at t0 = 2026-11-20T10:00:00Z. Every expected value is written by
// hand from the ruling (the science agent on #661, 17 Nov 2026), and each row reads the clock once.
func estimatePlan() (*v1alpha1.OperationPlan, v1alpha1.PlanTarget) {
	t := v1alpha1.PlanTarget{Kind: v1alpha1.TargetKindAllocation, Name: "alloc-p", UID: "uid-alloc-p", Provider: "sano"}
	return &v1alpha1.OperationPlan{ID: "plan-now", ObservedAt: metav1.NewTime(t0), Targets: []v1alpha1.PlanTarget{t}}, t
}

func at(s time.Duration) time.Time { return t0.Add(s) }

func TestDeriveEstimate(t *testing.T) {
	plan, target := estimatePlan()
	start := at(73 * time.Hour) // 2026-11-23T11:00:00Z
	known := func(readAt time.Duration) estimateEntry {
		return estimateEntry{Provider: "sano", PlanID: "plan-now", Start: estimateStart{Time: &start, ObservedAt: at(readAt)}}
	}
	failed := func(readAt time.Duration) estimateEntry {
		return estimateEntry{Provider: "sano", PlanID: "plan-now", Start: estimateStart{Unknown: "sbatch gave no start time", ObservedAt: at(readAt)}}
	}
	rows := []struct {
		name    string
		entries []estimateEntry
		now     time.Duration
		want    estimate
	}{
		{"no entry, 30 s after the plan: pending", nil, 30 * time.Second, estimate{Kind: estimatePending}},
		{"no entry, exactly 60 s: still pending", nil, 60 * time.Second, estimate{Kind: estimatePending}},
		{"no entry, 61 s after the plan: unknown, the ruled reason", nil, 61 * time.Second, estimate{Kind: estimateUnknown, Unknown: "no estimate within 60 s"}},
		{"a failed entry at 5 s: unknown with its reason, never pending", []estimateEntry{failed(5 * time.Second)}, 30 * time.Second,
			estimate{Kind: estimateUnknown, Unknown: "sbatch gave no start time", ObservedAt: at(5 * time.Second)}},
		{"a failed entry stays unknown long after", []estimateEntry{failed(5 * time.Second)}, 10 * time.Hour,
			estimate{Kind: estimateUnknown, Unknown: "sbatch gave no start time", ObservedAt: at(5 * time.Second)}},
		{"a known entry read at 10 s, shown at 70 s: its time, age 1m0s", []estimateEntry{known(10 * time.Second)}, 70 * time.Second,
			estimate{Kind: estimateKnown, Time: start, ObservedAt: at(10 * time.Second), Age: time.Minute}},
		{"an entry for an old plan ID is ignored", []estimateEntry{{Provider: "sano", PlanID: "plan-old", Start: estimateStart{Time: &start, ObservedAt: at(10 * time.Second)}}}, 30 * time.Second,
			estimate{Kind: estimatePending}},
		{"an entry for another Provider is ignored", []estimateEntry{{Provider: "helios", PlanID: "plan-now", Start: estimateStart{Time: &start, ObservedAt: at(10 * time.Second)}}}, 90 * time.Second,
			estimate{Kind: estimateUnknown, Unknown: "no estimate within 60 s"}},
		{"an entry that lists other targets is ignored", []estimateEntry{{Provider: "sano", PlanID: "plan-now", Targets: []types.UID{"uid-other"}, Start: estimateStart{Time: &start, ObservedAt: at(10 * time.Second)}}}, 30 * time.Second,
			estimate{Kind: estimatePending}},
		{"an entry that lists this target counts", []estimateEntry{{Provider: "sano", PlanID: "plan-now", Targets: []types.UID{"uid-other", "uid-alloc-p"}, Start: estimateStart{Time: &start, ObservedAt: at(10 * time.Second)}}}, 70 * time.Second,
			estimate{Kind: estimateKnown, Time: start, ObservedAt: at(10 * time.Second), Age: time.Minute}},
		{"the latest read of two wins", []estimateEntry{known(10 * time.Second), failed(40 * time.Second)}, 90 * time.Second,
			estimate{Kind: estimateUnknown, Unknown: "sbatch gave no start time", ObservedAt: at(40 * time.Second)}},
		{"an entry with neither a time nor a reason is unknown, never a zero", []estimateEntry{{Provider: "sano", PlanID: "plan-now", Start: estimateStart{ObservedAt: at(5 * time.Second)}}}, 30 * time.Second,
			estimate{Kind: estimateUnknown, Unknown: "the entry carries no start time and no reason", ObservedAt: at(5 * time.Second)}},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			got := deriveEstimate(plan, target, r.entries, at(r.now))
			if !reflect.DeepEqual(got, r.want) {
				t.Errorf("got %+v, want %+v", got, r.want)
			}
		})
	}
}

// The text of each state is written by hand.
func TestEstimateText(t *testing.T) {
	plan, target := estimatePlan()
	start := time.Date(2026, 11, 23, 8, 0, 0, 0, time.UTC)
	entry := estimateEntry{Provider: "sano", PlanID: "plan-now", Start: estimateStart{Time: &start, ObservedAt: at(10 * time.Second)}}
	for _, r := range []struct {
		entries []estimateEntry
		now     time.Duration
		want    string
	}{
		{nil, 30 * time.Second, "estimate: pending"},
		{nil, 61 * time.Second, "estimate: unknown: no estimate within 60 s"},
		{[]estimateEntry{entry}, 70 * time.Second, "estimate: 2026-11-23T08:00:00Z (advisory, read 1m0s ago)"},
	} {
		if got := estimateText(estimateViewOf(deriveEstimate(plan, target, r.entries, at(r.now)))); got != r.want {
			t.Errorf("got %q, want %q", got, r.want)
		}
	}
}

// The four states as --json shows them, against testdata/estimate-states.json, which is written by hand
// from docs/agent-cli.md.
func TestEstimateJSONStates(t *testing.T) {
	plan, target := estimatePlan()
	start := time.Date(2026, 11, 23, 8, 0, 0, 0, time.UTC)
	failedEntry := estimateEntry{Provider: "sano", PlanID: "plan-now", Start: estimateStart{Unknown: "sbatch gave no start time", ObservedAt: at(5 * time.Second)}}
	knownEntry := estimateEntry{Provider: "sano", PlanID: "plan-now", Start: estimateStart{Time: &start, ObservedAt: at(10 * time.Second)}}
	got := []estimateView{
		estimateViewOf(deriveEstimate(plan, target, nil, at(30*time.Second))),
		estimateViewOf(deriveEstimate(plan, target, nil, at(61*time.Second))),
		estimateViewOf(deriveEstimate(plan, target, []estimateEntry{failedEntry}, at(30*time.Second))),
		estimateViewOf(deriveEstimate(plan, target, []estimateEntry{knownEntry}, at(70*time.Second))),
	}
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	sameJSON(t, raw, readGolden(t, "estimate-states.json"))
}

func readGolden(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// sameJSON compares two JSON documents by value: no field of one is missing from the other, whatever the
// order or the indentation.
func sameJSON(t *testing.T, got, want []byte) {
	t.Helper()
	var g, w any
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("the output is not JSON: %v\n%s", err, got)
	}
	if err := json.Unmarshal(want, &w); err != nil {
		t.Fatalf("the golden is not JSON: %v", err)
	}
	if !reflect.DeepEqual(g, w) {
		gb, _ := json.MarshalIndent(g, "", "  ")
		wb, _ := json.MarshalIndent(w, "", "  ")
		t.Errorf("the JSON differs from the golden.\ngot:\n%s\nwant:\n%s", gb, wb)
	}
}
