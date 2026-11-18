package inventory_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/inventory"
)

// The printer takes rows and prints them: the rows here are built by hand, and the text expected is written by hand from
// the printer's contract, so neither comes from the lister.
func rowsToPrint() *inventory.Inventory {
	return &inventory.Inventory{
		SchemaVersion: 1, ObservedAt: "2026-11-20T10:01:30Z", Scope: inventory.Scope{Namespace: "team"},
		ModelDeployments: []inventory.DeploymentRow{
			{Namespace: "team", Name: "md-alice", Owner: inventory.Text(alice), Purpose: inventory.Text("nightly run"), Instances: 2,
				ReadyInstances: inventory.Count(1), EndTime: "2026-11-22T10:00:00Z", Providers: []string{"sano", "helios"}},
			{Namespace: "team", Name: "md-old", Owner: inventory.Unknown("no owner annotation"), Purpose: inventory.Unknown("no purpose annotation"), Instances: 1,
				ReadyInstances: inventory.Unknown("the controller has not reported on the ModelDeployment yet"), Providers: []string{}},
		},
		Allocations: []inventory.AllocationRow{{
			Namespace: "team", Name: "alloc-a2", ModelDeployment: inventory.Text("md-alice"), Owner: inventory.Text(alice), Purpose: inventory.Text("nightly run"),
			Phase: inventory.Text("Running"), Provider: inventory.Text("sano"), HardEnd: inventory.Text("2026-11-20T12:00:00Z"),
			Retention: inventory.RetentionRow{Mode: "Keep", Until: inventory.Text("2026-11-21T10:00:00Z"), ProfileMaxIdle: inventory.Span(time.Hour)},
			IdleSince: inventory.Text("2026-11-20T09:30:00Z"), KeptNoInstance: true,
		}},
		Instances: []inventory.InstanceRow{
			{Namespace: "team", Name: "inst-a1", Owner: inventory.Text(alice), Purpose: inventory.Text("nightly run"), Phase: inventory.Text("ready"), Allocation: "alloc-a1"}},
		Providers: []inventory.ProviderRow{
			{Name: "helios", LastSuccessfulStatusRead: inventory.Unknown("never read"), Age: inventory.Unknown("never read")},
			{Name: "sano", LastSuccessfulStatusRead: inventory.Text("2026-11-20T10:00:00Z"), Age: inventory.Text("1m30s")}},
		Operations: []inventory.OperationRow{
			{Namespace: "team", Name: "op-wait", ModelDeployment: "md-alice", Action: "Scale", PlanID: inventory.Text("plan-1"), Owner: alice}},
	}
}

func TestTheTextShowsEveryRowAndMarksWhatIsUnknown(t *testing.T) {
	var b bytes.Buffer
	if err := inventory.WriteText(&b, rowsToPrint()); err != nil {
		t.Fatal(err)
	}
	want := `inventory at 2026-11-20T10:01:30Z, namespace team
ModelDeployments:
  team/md-alice: owner example-alice, purpose "nightly run"
    instances 2 (ready 1), ends 2026-11-22T10:00:00Z, Providers sano, helios
  team/md-old: owner unknown (no owner annotation), purpose unknown (no purpose annotation)
    instances 1 (ready unknown (the controller has not reported on the ModelDeployment yet)), no end time, no Provider
Allocations:
  team/alloc-a2: ModelDeployment md-alice, owner example-alice, purpose "nightly run"
    phase Running, Provider sano, hard end 2026-11-20T12:00:00Z
    retention Keep, until 2026-11-21T10:00:00Z, profile maxIdle 1h0m0s
    idle since 2026-11-20T09:30:00Z
    kept, no Instance
Instances:
  team/inst-a1: owner example-alice, purpose "nightly run", phase ready, Allocation alloc-a1
Providers:
  helios: last successful read unknown (never read)
  sano: last successful read 2026-11-20T10:00:00Z, age 1m30s
Operations awaiting approval:
  team/op-wait: Scale of ModelDeployment md-alice, plan plan-1, owner example-alice
`
	if got := b.String(); got != want {
		t.Errorf("the text differs.\ngot:\n%s\nwant:\n%s", got, want)
	}
}

func TestTheTextNamesTheScopeAndTheSelectedOwnerAndSaysNoneUnderAnEmptyList(t *testing.T) {
	var b bytes.Buffer
	inv := &inventory.Inventory{
		SchemaVersion: 1, ObservedAt: "2026-11-20T10:01:30Z", Scope: inventory.Scope{AllNamespaces: true}, SelectedOwner: bob,
		ModelDeployments: []inventory.DeploymentRow{}, Allocations: []inventory.AllocationRow{}, Instances: []inventory.InstanceRow{},
		Providers: []inventory.ProviderRow{}, Operations: []inventory.OperationRow{},
	}
	if err := inventory.WriteText(&b, inv); err != nil {
		t.Fatal(err)
	}
	want := "inventory at 2026-11-20T10:01:30Z, every namespace\nselected by owner example-bob\n" +
		"ModelDeployments:\n  none\nAllocations:\n  none\nInstances:\n  none\nProviders:\n  none\nOperations awaiting approval:\n  none\n"
	if got := b.String(); got != want {
		t.Errorf("the text differs.\ngot:\n%s\nwant:\n%s", got, want)
	}
}

// A purpose is free text: the JSON keeps "<" and ">" as they are, and the writer's error is returned.
func TestTheJSONKeepsFreeTextAsItIsAndAFailedWriteIsReported(t *testing.T) {
	inv := rowsToPrint()
	inv.ModelDeployments[0].Purpose = inventory.Text("free <slot> & more")
	var b bytes.Buffer
	if err := inventory.WriteJSON(&b, inv); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b.String(), `"purpose": "free <slot> & more"`) {
		t.Errorf("the purpose was escaped:\n%s", b.String())
	}
	if err := inventory.WriteJSON(failingWriter{}, inv); err == nil {
		t.Error("a failed write returned no error")
	}
	if err := inventory.WriteText(failingWriter{}, inv); err == nil {
		t.Error("a failed write of the text returned no error")
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errWrite }

var errWrite = &writeError{}

type writeError struct{}

func (*writeError) Error() string { return "write failed" }
