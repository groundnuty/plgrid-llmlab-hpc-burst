package e2e

// Schema completeness (issue #589, PR 3; plan section 11.1). The capacity guard is an allowlist: an
// agent's update is accepted only if every change is one of HBEP-10's, and every other field of the spec
// must stay as it is. So a field that a later change adds to the CRD must be refused until someone
// allowlists it. This test reads the CRD itself, not the guard's YAML: for every property of spec outside
// instances, endTime and source, and every property of spec.source other than served, it makes one
// schema-valid change as agent A and expects the guard's refusal. A property with no change fixture fails
// the test, naming it, so a new field cannot slip in unexamined.
//
// The expectation (refused) is HBEP-10's rule; the population (which properties exist) is the CRD's. The
// person's same change is a control: it passes (a dry run), which shows the change is valid for the CRD
// and the refusal is the guard's.

import (
	"bytes"
	"slices"
	"sort"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/utils/ptr"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// What HBEP-10 lets an agent change in a ModelDeployment's spec by its own rules: a higher count, a later
// end, the serving spec, the hedge width either way (as amended for #606), and the Workloads it serves (a
// routing input that releases nothing; HBEP-764). Written here by hand; nothing else is allowed.
var (
	allowlistedSpecProperties   = []string{"instances", "endTime", "source", "hedgeWidth", "workloads"}
	allowlistedSourceProperties = []string{"served"}
)

// crdSpecProperties reads config/crd's ModelDeployment CRD and returns the names of the properties of spec
// and of spec.source, sorted.
func crdSpecProperties(t *testing.T) (spec, source []string) {
	t.Helper()
	raw := readAll(t, configDir("crd")+"/hpcburst.groundnuty.github.io_modeldeployments.yaml")
	crd := &apiextensionsv1.CustomResourceDefinition{}
	must(t, utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(raw), 4096).Decode(crd), "decode the ModelDeployment CRD")
	for _, v := range crd.Spec.Versions {
		if v.Name != "v1alpha1" {
			continue
		}
		specSchema, ok := v.Schema.OpenAPIV3Schema.Properties["spec"]
		if !ok {
			t.Fatal("the CRD has no spec")
		}
		for name := range specSchema.Properties {
			spec = append(spec, name)
		}
		for name := range specSchema.Properties["source"].Properties {
			source = append(source, name)
		}
		sort.Strings(spec)
		sort.Strings(source)
		return spec, source
	}
	t.Fatal("the CRD has no version v1alpha1")
	return nil, nil
}

// withoutFixture returns the properties among names that are not allowlisted and have no change fixture,
// and the fixtures that name no property: both are failures of the test's population.
func withoutFixture(names, allowlisted []string, fixtures map[string]func(*v1alpha1.ModelDeployment)) (missing, stale []string) {
	for _, n := range names {
		if slices.Contains(allowlisted, n) {
			continue
		}
		if fixtures[n] == nil {
			missing = append(missing, n)
		}
	}
	for n := range fixtures {
		if !slices.Contains(names, n) || slices.Contains(allowlisted, n) {
			stale = append(stale, n)
		}
	}
	sort.Strings(stale)
	return missing, stale
}

// One schema-valid change for each property outside the allowlist.
var specChangeFixtures = map[string]func(*v1alpha1.ModelDeployment){
	"model":              func(md *v1alpha1.ModelDeployment) { md.Spec.Model = "example-other-model" },
	"workersPerInstance": func(md *v1alpha1.ModelDeployment) { md.Spec.WorkersPerInstance = ptr.To[int32](2) },
	// Refused for agents: the science agent's ruling on #616 (Q12). Unlike hedgeWidth it is not allowlisted.
	"overAllocation": func(md *v1alpha1.ModelDeployment) { md.Spec.OverAllocation = ptr.To[int32](0) },
	"providers":      func(md *v1alpha1.ModelDeployment) { md.Spec.Providers.Names = []string{"helios-a", "helios-b"} },
	"retentionPolicy": func(md *v1alpha1.ModelDeployment) {
		md.Spec.RetentionPolicy = &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: ptr.To(v1alpha1.Duration("2h"))}
	},
	"leaseLimit": func(md *v1alpha1.ModelDeployment) { md.Spec.LeaseLimit = ptr.To(v1alpha1.Duration("4h")) },
	"workload":   func(md *v1alpha1.ModelDeployment) { md.Spec.Workload.TargetSessions = 16 },
	"routing":    func(md *v1alpha1.ModelDeployment) { md.Spec.Routing.InstanceCap = ptr.To[int32](64) },
}

var sourceChangeFixtures = map[string]func(*v1alpha1.ModelDeployment){
	"managed": func(md *v1alpha1.ModelDeployment) {
		md.Spec.Source = v1alpha1.ModelSource{Managed: &v1alpha1.ManagedCatalogEntry{Platform: "example-platform", CatalogName: "example-model"}}
	},
}

// The detection itself: a property with no fixture, and a fixture with no property, are both found. A test
// that could not fail for a new field would prove nothing.
func TestSchemaCompletenessFindsAPropertyWithNoFixture(t *testing.T) {
	missing, stale := withoutFixture([]string{"model", "instances", "aBrandNewField"}, allowlistedSpecProperties, specChangeFixtures)
	if !slices.Equal(missing, []string{"aBrandNewField"}) {
		t.Errorf("missing = %v, want only the new field", missing)
	}
	if len(stale) == 0 {
		t.Error("the fixtures for properties that are not in the list were not reported as stale")
	}
	if missing, stale := withoutFixture([]string{"model"}, allowlistedSpecProperties, map[string]func(*v1alpha1.ModelDeployment){"model": specChangeFixtures["model"], "instances": specChangeFixtures["model"]}); len(missing) != 0 || !slices.Equal(stale, []string{"instances"}) {
		t.Errorf("a fixture for an allowlisted property: missing %v, stale %v, want it reported stale", missing, stale)
	}
}

func TestEveryModelDeploymentSpecPropertyOutsideTheAllowlistIsRefusedForAnAgent(t *testing.T) {
	specProps, sourceProps := crdSpecProperties(t)
	// The population is the CRD's: these exist today, and the test must see them.
	for _, want := range []string{"model", "source", "instances", "providers", "endTime", "retentionPolicy", "leaseLimit", "workload", "routing", "hedgeWidth", "overAllocation", "workloads"} {
		if !slices.Contains(specProps, want) {
			t.Fatalf("the CRD's spec properties %v lack %q: the test reads the wrong place", specProps, want)
		}
	}
	missing, stale := withoutFixture(specProps, allowlistedSpecProperties, specChangeFixtures)
	for _, n := range missing {
		t.Errorf("spec.%s has no change fixture: give it one in specChangeFixtures, which expects the guard's refusal", n)
	}
	for _, n := range stale {
		t.Errorf("the fixture for spec.%s names no property of the CRD outside the allowlist", n)
	}
	missing, stale = withoutFixture(sourceProps, allowlistedSourceProperties, sourceChangeFixtures)
	for _, n := range missing {
		t.Errorf("spec.source.%s has no change fixture: give it one in sourceChangeFixtures, which expects the guard's refusal", n)
	}
	for _, n := range stale {
		t.Errorf("the fixture for spec.source.%s names no property of the CRD outside the allowlist", n)
	}
	if t.Failed() {
		return
	}

	a := newAgents(t, "agent-a", "agent-b")[0]
	requireEnforced(t)
	ns := newNamespace(t)
	person := personWriter(t)
	run := func(label string, fix func(*v1alpha1.ModelDeployment), msg string) {
		t.Run(label, func(t *testing.T) {
			stored := createMD(t, a, agentMD(a, ns, uniq("md")))
			requireAgentIdentity(t, a)
			err := agentWriter(a).update(stored, fix)
			requireDenied(t, "A's change of "+label, err, capacityGuard, msg)
			requireUntouched(t, stored, label)
			// The same change from the person is valid for the CRD (a dry run), so the refusal is the guard's.
			requireAccepted(t, "the person's same change of "+label+" (a dry run)", person.update(stored, fix))
		})
	}
	for _, n := range specProps {
		if fix := specChangeFixtures[n]; fix != nil && !slices.Contains(allowlistedSpecProperties, n) {
			run("spec."+n, fix, "may change only spec.instances")
		}
	}
	for _, n := range sourceProps {
		if fix := sourceChangeFixtures[n]; fix != nil && !slices.Contains(allowlistedSourceProperties, n) {
			run("spec.source."+n, fix, "edit spec.source.served only")
		}
	}
}
