package agentcli

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// alloc is an Allocation of ModelDeployment md, with the labels given.
func alloc(name, md string, labels map[string]string) *v1alpha1.Allocation {
	a := &v1alpha1.Allocation{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS, Labels: labels}}
	if md != "" {
		a.Spec.ModelDeploymentRef = &v1alpha1.ObjectReference{Name: md}
	}
	return a
}

// longMD is a ModelDeployment name past 63 characters, so that the model-deployment label of its
// Allocations is shortened and no longer names it.
var longMD = "a-model-deployment-whose-name-is-long-enough-to-pass-the-label-limit-of-sixty-three-characters"

// The expected key suffixes are SHA-256 digests worked out in the shell (printf NAME | sha256sum), never by
// the code under test: m1 -> ca0df2c9, m2 -> 29c1b289, m -> 62c66a7a.
func TestReleaseGroupsByTheSpecNotTheLabel(t *testing.T) {
	short := strings.Repeat("s", 8) // the shortened label: it names no ModelDeployment
	w := newWorld(t, []client.Object{
		alloc("a1", "m1", map[string]string{"model-deployment": "m1"}),
		alloc("a2", "m1", map[string]string{"model-deployment": "m1"}),
		alloc("a3", longMD, map[string]string{"model-deployment": short}),
	})
	w.names = []string{"op-1111111111", "op-2222222222"}
	// The controller plans both. The Operations are made in the order of the ModelDeployments' names, and
	// the long name sorts before m1 ("a" before "m").
	w.onPause = func() {
		for _, n := range w.operations() {
			if w.operation(n).Status.Plan == nil {
				w.planFor(n, simplePlan(planAB))
			}
		}
	}
	if code := w.run("", "release", "a3", "a2", "a1", "a1"); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w.errs.String())
	}
	if got := w.operations(); len(got) != 2 {
		t.Fatalf("Operations %v, want one per ModelDeployment", got)
	}
	two, one := w.operation("op-1111111111"), w.operation("op-2222222222")
	if one.Spec.ModelDeploymentRef.Name != "m1" || two.Spec.ModelDeploymentRef.Name != longMD {
		t.Fatalf("Operations target %q and %q, want the long name and m1", two.Spec.ModelDeploymentRef.Name, one.Spec.ModelDeploymentRef.Name)
	}
	if got := one.Spec.Release.Allocations; len(got) != 2 || got[0] != "a1" || got[1] != "a2" || one.Spec.Release.Selector != nil {
		t.Errorf("m1's release %+v, want the names a1, a2 and no selector", one.Spec.Release)
	}
	if got := two.Spec.Release.Allocations; len(got) != 1 || got[0] != "a3" {
		t.Errorf("the long name's release %+v, want a3: found by the spec, not by its shortened label", two.Spec.Release)
	}
	if one.Spec.Owner != userA {
		t.Errorf("owner %q", one.Spec.Owner)
	}
}

func TestReleaseBySelector(t *testing.T) {
	w := newWorld(t, []client.Object{
		alloc("a1", "m1", map[string]string{"tier": "kept"}),
		alloc("a2", "m2", map[string]string{"tier": "kept"}),
		alloc("a3", "m2", map[string]string{"tier": "serving"}),
	})
	w.names = []string{"op-1111111111", "op-2222222222"}
	w.onPause = func() {
		for _, n := range w.operations() {
			if w.operation(n).Status.Plan == nil {
				w.planFor(n, simplePlan(planAB))
			}
		}
	}
	if code := w.run("", "release", "-l", "tier=kept"); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w.errs.String())
	}
	for _, n := range []string{"op-1111111111", "op-2222222222"} {
		op := w.operation(n)
		r := op.Spec.Release
		if r == nil || len(r.Allocations) != 0 || r.Selector == nil || r.Selector.MatchLabels["tier"] != "kept" {
			t.Errorf("%s: release %+v, want the selector tier=kept and no names", n, r)
		}
	}
	if m1, m2 := w.operation("op-1111111111").Spec.ModelDeploymentRef.Name, w.operation("op-2222222222").Spec.ModelDeploymentRef.Name; m1 != "m1" || m2 != "m2" {
		t.Errorf("targets %q and %q, want m1 and m2", m1, m2)
	}
}

func TestReleaseCreatesNothingWhenItCannotRelease(t *testing.T) {
	objs := []client.Object{alloc("a1", "m1", map[string]string{"tier": "kept"}), alloc("orphan", "", nil)}
	rows := []struct {
		name string
		args []string
		want int
	}{
		{"a selector that matches nothing", []string{"release", "-l", "tier=nothing"}, exitFail},
		{"an Allocation that does not exist", []string{"release", "a1", "absent"}, exitFail},
		{"an Allocation with no deployment", []string{"release", "orphan"}, exitFail},
		{"a malformed selector", []string{"release", "-l", "tier in (a"}, exitUsage},
		{"a key past 253 characters", []string{"release", "a1", "--key", strings.Repeat("k", 254)}, exitUsage},
	}
	for _, r := range rows {
		w := newWorld(t, objs)
		if code := w.run("", r.args...); code != r.want {
			t.Errorf("%s: exit %d, want %d; stderr %q", r.name, code, r.want, w.errs.String())
		}
		if got := w.writes(); len(got) != 0 {
			t.Errorf("%s: writes %v, want none", r.name, got)
		}
	}
	w := newWorld(t, objs)
	w.run("", "release", "-l", "tier=nothing")
	contains(t, "stderr", w.errs.String(), `matches "tier=nothing"`, "nothing was created")
}

// The derived keys, with the digests worked out in the shell.
func TestReleaseKeys(t *testing.T) {
	objs := []client.Object{alloc("a1", "m1", nil), alloc("a2", "m2", nil)}
	w := newWorld(t, objs)
	w.onPause = func() {
		for _, n := range w.operations() {
			if w.operation(n).Status.Plan == nil {
				w.planFor(n, simplePlan(planAB))
			}
		}
	}
	if code := w.run("", "release", "a1", "a2", "--key", "k"); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w.errs.String())
	}
	got := w.operations() // listed by name
	if len(got) != 2 || got[0] != "k-29c1b289" || got[1] != "k-ca0df2c9" {
		t.Fatalf("Operations %v, want k-29c1b289 (m2) and k-ca0df2c9 (m1)", got)
	}
	if m2, m1 := w.operation("k-29c1b289").Spec.ModelDeploymentRef.Name, w.operation("k-ca0df2c9").Spec.ModelDeploymentRef.Name; m2 != "m2" || m1 != "m1" {
		t.Errorf("k-29c1b289 acts on %q and k-ca0df2c9 on %q, want m2 and m1", m2, m1)
	}
	// One ModelDeployment: the key itself.
	w = newWorld(t, objs)
	w.controller("k", simplePlan(planAB), "")
	if code := w.run("", "release", "a1", "--key", "k"); code != exitOK {
		t.Fatalf("exit %d, stderr %q", code, w.errs.String())
	}
	if got := w.operations(); len(got) != 1 || got[0] != "k" {
		t.Errorf("Operations %v, want k", got)
	}
	// A derived name past 253 characters is refused before anything is created.
	w = newWorld(t, objs)
	if code := w.run("", "release", "a1", "a2", "--key", strings.Repeat("k", 246)); code != exitUsage {
		t.Errorf("exit %d, want 2", code)
	}
	if got := w.writes(); len(got) != 0 {
		t.Errorf("writes %v, want none", got)
	}
	contains(t, "stderr", w.errs.String(), "at most 253", "nothing was created")
}

func TestReleaseRefusesMoreThan64NamesOfOneDeployment(t *testing.T) {
	var objs []client.Object
	var names []string
	for i := 0; i < 65; i++ {
		n := "a" + strings.Repeat("0", 3) + string(rune('a'+i/26)) + string(rune('a'+i%26))
		objs = append(objs, alloc(n, "m1", nil))
		names = append(names, n)
	}
	w := newWorld(t, objs)
	if code := w.run("", append([]string{"release"}, names...)...); code != exitUsage {
		t.Errorf("exit %d, want 2", code)
	}
	contains(t, "stderr", w.errs.String(), "at most 64", "-l <selector>")
	if got := w.writes(); len(got) != 0 {
		t.Errorf("writes %v, want none", got)
	}
}
