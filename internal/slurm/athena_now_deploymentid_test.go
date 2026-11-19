package slurm

// The deployment ID of the baseline's Athena row (issue #764, HBEP-764 item 4; internal/alpha, beta-athena-now-llmlab-baseline): the Instance of
// examples/modeldeployment-athena-now-qwen3-vl-8b-instruct.yaml on athena-now.yaml's plgrid-now carries a deployment ID, and the row pins it as
// the one the example derives (alpha.AthenaBaselineDeploymentID). It is derived HERE, as sano_expectedperformance_test.go derives Sano's keys: from the
// example manifest, the checkpoint's file list (testdata/checkpoints, read from the Hub by the controller's own package, not from any constant of the
// row) and the class of the profile's partition, by deploymentid.Compute. The literal below is the pin; nothing is read back from the row's constant
// to make its own expectation.
//
// The brief expected the Athena ID to DIFFER from Sano's, since the accelerator's class is an ID input. It does not: the class is the profile's
// gpuModel and cpuArch (internal/lifecycle, identity.go), and both profiles give "A100" on "x86_64" (HBEP-4 fixes the string; the card's 40 GB or
// 80 GB is no input). The checkpoint, the engine and the flags are the same, so the ID is the same. The test says so and holds it: a class that
// differs would change it.
//
// Sano's expected-performance entry for this ID (profiles/sano.yaml) never judges an Athena Instance: the throughput gate reads the entries of the
// profile of the Instance's OWN Provider (internal/lifecycle/gate_act.go:73-81, pvOf(in.Labels).profile), and athena-now.yaml holds no
// expectedPerformance, so the gate there records only ("the platform profile holds no expected performance").

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

const (
	athenaNowQwen3VLExample = "modeldeployment-athena-now-qwen3-vl-8b-instruct.yaml"
	sanoQwen3VLExample      = "modeldeployment-sano-qwen3-vl-8b-instruct.yaml"
	qwen3VLFixture          = "qwen3-vl-8b-instruct-0c351dd01ed8.json"
	// athenaBaselineID is the display form of the deployment ID the Athena example derives on plgrid-now, written out as the row's pin
	// (alpha.AthenaBaselineDeploymentID is compared with this literal in internal/alpha).
	athenaBaselineID = "965fa2409193"
)

func readShippedAthenaNow(t *testing.T) v1alpha1.PlatformProfile {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "profiles", "athena-now.yaml"))
	if err != nil {
		t.Fatalf("read the shipped profile: %v", err) // a missing file fails; it never skips
	}
	var pp v1alpha1.PlatformProfile
	if err := yaml.UnmarshalStrict(raw, &pp); err != nil {
		t.Fatalf("athena-now.yaml: %v", err)
	}
	return pp
}

// The Athena example derives the deployment ID the row pins, on the partition of athena-now.yaml; it is the Sano example's ID, because the class
// string is the same; and a class that differs changes it, so that the derivation is not one that returns the same ID whatever it is given.
func TestTheAthenaBaselineExampleDerivesTheRowsDeploymentID(t *testing.T) {
	pp := readShippedAthenaNow(t)
	var part v1alpha1.PartitionProfile
	for _, p := range pp.Spec.Partitions {
		if p.Name == "plgrid-now" {
			part = p
		}
	}
	if part.Name != "plgrid-now" {
		t.Fatal("athena-now.yaml has no partition plgrid-now")
	}
	fx := readCheckpoint(t, qwen3VLFixture)
	athena := readExample(t, athenaNowQwen3VLExample)
	key := derivedKey(t, athena, fx, part)
	if key.DeploymentID != athenaBaselineID {
		t.Fatalf("the Athena example derives the deployment ID %s on plgrid-now, the row's pin is %s", key.DeploymentID, athenaBaselineID)
	}
	// The same as Sano's, by derivation and not by assumption: Sano's example on Sano's partition.
	sanoPart := readShippedSano(t).Spec.Partitions[0]
	if sano := derivedKey(t, readExample(t, sanoQwen3VLExample), fx, sanoPart); sano.DeploymentID != key.DeploymentID {
		t.Errorf("Sano's example derives %s and the Athena one %s: the class strings are %s/%s and %s/%s", sano.DeploymentID, key.DeploymentID,
			sanoPart.GPUModel, sanoPart.CPUArch, part.GPUModel, part.CPUArch)
	}
	// What would change it: the class and a flag that is an ID input.
	other := part
	other.GPUModel = "GH200"
	if k := derivedKey(t, athena, fx, other); k.DeploymentID == key.DeploymentID {
		t.Errorf("another accelerator class derives the same deployment ID %s", k.DeploymentID)
	}
	md := readExample(t, athenaNowQwen3VLExample)
	md.Spec.Source.Served.Flags[0].Value = "32767"
	if k := derivedKey(t, md, fx, part); k.DeploymentID == key.DeploymentID {
		t.Errorf("another max-model-len derives the same deployment ID %s", k.DeploymentID)
	}
}

// The Athena example is Sano's in everything the engine is given (checkpoint, engine, flags) and in its workload block (so the gate's key shape is Sano's),
// and in what the baseline holds fixed (one Instance, no extra Allocation); its provider is Athena's and its cap is the row's cap, 3.
func TestTheAthenaBaselineExampleIsSanosWithAthenasProviderAndCap(t *testing.T) {
	athena, sano := readExample(t, athenaNowQwen3VLExample), readExample(t, sanoQwen3VLExample)
	if !reflect.DeepEqual(athena.Spec.Source.Served, sano.Spec.Source.Served) || athena.Spec.Model != sano.Spec.Model {
		t.Errorf("the Athena example's served spec or model differs from Sano's: %+v against %+v", athena.Spec.Source.Served, sano.Spec.Source.Served)
	}
	if athena.Spec.Instances != 1 || athena.Spec.OverAllocation == nil || *athena.Spec.OverAllocation != 0 {
		t.Errorf("instances %d, overAllocation %v, want 1 and 0", athena.Spec.Instances, athena.Spec.OverAllocation)
	}
	if got := athena.Spec.Providers.Names; len(got) != 1 || got[0] != "athena-a100" {
		t.Errorf("providers %v, want athena-a100 alone (examples/provider-athena-a100.yaml)", got)
	}
	if r := athena.Spec.Routing; r == nil || r.InstanceCap == nil || *r.InstanceCap != 3 {
		t.Errorf("routing %+v, want instanceCap 3", r)
	}
	if !reflect.DeepEqual(athena.Spec.Workload, sano.Spec.Workload) {
		t.Errorf("workload %+v, want the Sano example's %+v", athena.Spec.Workload, sano.Spec.Workload)
	}
}
