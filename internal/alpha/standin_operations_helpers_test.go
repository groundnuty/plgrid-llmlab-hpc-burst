package alpha

// The pure helpers of the Operations stand-in (standin_operations_test.go), which decide what it calls churn: they run
// without the gate, so their rows are checked on every run of the package.

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

func standInInstance(name string, phase v1alpha1.InstancePhase, predecessor string) v1alpha1.Instance {
	in := v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID("uid-" + name)}}
	in.Status.Phase = phase
	if predecessor != "" {
		in.Annotations = map[string]string{v1alpha1.AnnotationPredecessor: predecessor}
	}
	return in
}

// The primaries of a deployment are the Instances that serve or start and have no live predecessor: a replacement counts for
// nothing beside the primary it replaces, and takes its place once that drains or ends.
func TestPrimariesOfCountsAReplacementForNothingBesideItsPredecessor(t *testing.T) {
	ready, draining, ended, starting := v1alpha1.InstanceReady, v1alpha1.InstanceDraining, v1alpha1.InstanceEnded, v1alpha1.InstanceStarting
	for _, tc := range []struct {
		name  string
		items []v1alpha1.Instance
		want  []string
	}{
		{"one primary", []v1alpha1.Instance{standInInstance("r", ready, "")}, []string{"r"}},
		{"a primary and its ready replacement: one primary, the survivor of a Scale",
			[]v1alpha1.Instance{standInInstance("r", ready, ""), standInInstance("r2", ready, "r")}, []string{"r"}},
		{"a primary and its starting replacement", []v1alpha1.Instance{standInInstance("r", ready, ""), standInInstance("r2", starting, "r")}, []string{"r"}},
		{"the primary drains: its replacement is the primary", []v1alpha1.Instance{standInInstance("r", draining, ""), standInInstance("r2", ready, "r")}, []string{"r2"}},
		{"the primary has ended: its replacement is the primary", []v1alpha1.Instance{standInInstance("r", ended, ""), standInInstance("r2", ready, "r")}, []string{"r2"}},
		{"two primaries, no replacement: the Scale has not taken one", []v1alpha1.Instance{standInInstance("r", ready, ""), standInInstance("x", ready, "")}, []string{"r", "x"}},
		{"a primary provisioned beside the survivor, with no predecessor: two primaries",
			[]v1alpha1.Instance{standInInstance("r", ready, ""), standInInstance("n", ready, "")}, []string{"r", "n"}},
		{"an ended victim is no primary", []v1alpha1.Instance{standInInstance("r", ready, ""), standInInstance("x", ended, "")}, []string{"r"}},
	} {
		var got []string
		for _, in := range primariesOf(tc.items) {
			got = append(got, in.Name)
		}
		if len(got) != len(tc.want) {
			t.Errorf("%s: primaries %v, want %v", tc.name, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("%s: primaries %v, want %v", tc.name, got, tc.want)
			}
		}
	}
}

// An Instance created after an Operation's commit is a handover replacement of a primary that stays, or it is churn: one with
// no predecessor is a primary provisioned in place of what the Operation ended, and one that replaces a victim was replaced when
// it should have gone. An Instance that existed before the commit is never reported.
func TestReprovisionedNamesOnlyANewInstanceThatIsNotAHandoverReplacement(t *testing.T) {
	ready := v1alpha1.InstanceReady
	victims := map[string]bool{"x": true}
	before := map[types.UID]bool{"uid-r": true, "uid-x": true, "uid-old": true}
	for _, tc := range []struct {
		name  string
		items []v1alpha1.Instance
		want  int
	}{
		{"only what existed before", []v1alpha1.Instance{standInInstance("r", ready, ""), standInInstance("x", ready, ""), standInInstance("old", ready, "")}, 0},
		{"a new replacement of the survivor", []v1alpha1.Instance{standInInstance("r", ready, ""), standInInstance("r2", ready, "r")}, 0},
		{"a new replacement of a replacement", []v1alpha1.Instance{standInInstance("r2", ready, "r"), standInInstance("r3", ready, "r2")}, 0},
		{"a new Instance with no predecessor: churn", []v1alpha1.Instance{standInInstance("r", ready, ""), standInInstance("n", ready, "")}, 1},
		{"a new replacement of a victim", []v1alpha1.Instance{standInInstance("x", ready, ""), standInInstance("x2", ready, "x")}, 1},
		{"both kinds", []v1alpha1.Instance{standInInstance("n", ready, ""), standInInstance("x2", ready, "x"), standInInstance("r2", ready, "r")}, 2},
	} {
		if got := reprovisioned(tc.items, before, victims); len(got) != tc.want {
			t.Errorf("%s: %d findings %v, want %d", tc.name, len(got), got, tc.want)
		}
	}
}
