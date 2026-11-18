package inventory

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// HBEP-5, Workloads (beta, proposed): the view holds the Workload objects by their model and name, and each ModelDeployment's spec.workloads.
// Whether a deployment serves a Workload is HBEP-5's "Who serves it": it is in the Workload's own namespace, and lists it or lists none. The
// expected values are written out from that sentence.

func workload(ns, name, model string) v1alpha1.Workload {
	return v1alpha1.Workload{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}, Spec: v1alpha1.WorkloadSpec{Model: model}}
}

// A Workload of a model no ModelDeployment names is in the view, which is how a request that names it is refused for what it is. Two namespaces
// holding one name for one model keep both, sorted; one name for two models is two entries.
func TestBuildKeepsWorkloadsByModelAndName(t *testing.T) {
	s := batchSnap()
	s.Workloads = []v1alpha1.Workload{
		workload("ns-two", "dup", "m"), workload("ns", "dup", "m"), workload("ns", "solo", "m"),
		workload("ns", "twin", "m"), workload("ns-two", "twin", "other"), workload("ns", "orphan", "no-deployment"),
	}
	v := Build(s, Inputs{Now: now, MaxRequestDuration: 2 * time.Hour})
	keys := func(model, name string) string {
		var out []string
		for _, w := range v.Workloads[model][name] {
			if w.Name != name || w.Model != model || w.Key != w.Namespace+"/"+w.Name {
				t.Errorf("the entry %+v under %s/%s is not that Workload", *w, model, name)
			}
			out = append(out, w.Key)
		}
		return strings.Join(out, ",")
	}
	for model, byName := range map[string]map[string]string{
		"m":             {"dup": "ns-two/dup,ns/dup", "solo": "ns/solo", "twin": "ns/twin", "orphan": ""},
		"other":         {"twin": "ns-two/twin", "solo": ""},
		"no-deployment": {"orphan": "ns/orphan"},
	} {
		for name, want := range byName {
			if got := keys(model, name); got != want {
				t.Errorf("the Workloads %q of model %q: %q, want %q", name, model, got, want)
			}
		}
	}
	if v.Models["no-deployment"] != nil {
		t.Error("a Workload attached a model that no ModelDeployment names")
	}
}

// "Who serves it": a ModelDeployment of the Workload's namespace that lists it or lists none. Another namespace never serves it, whether its list
// is empty or names it.
func TestADeploymentServesAWorkloadByNamespaceAndList(t *testing.T) {
	w := &Workload{Key: "ns/alpha", Namespace: "ns", Name: "alpha", Model: "m"}
	for _, c := range []struct {
		name string
		ns   string
		list []string
		want bool
	}{
		{"the namespace, listing it", "ns", []string{"alpha"}, true},
		{"the namespace, listing it among others", "ns", []string{"beta", "alpha"}, true},
		{"the namespace, listing none", "ns", nil, true},
		{"the namespace, listing another", "ns", []string{"beta"}, false},
		{"another namespace, listing none", "ns-two", nil, false},
		{"another namespace, listing it", "ns-two", []string{"alpha"}, false},
		{"the namespace, listing a name it only starts", "ns", []string{"alph"}, false},
	} {
		d := &Deployment{Namespace: c.ns, Workloads: c.list}
		s := &ManagedSource{Namespace: c.ns, Workloads: c.list}
		mt := &ManagedTarget{Namespace: c.ns, Workloads: c.list}
		if d.Serves(w) != c.want || s.Serves(w) != c.want || mt.Serves(w) != c.want {
			t.Errorf("%s: a served deployment says %v, a managed source %v, a managed target %v, want %v", c.name, d.Serves(w), s.Serves(w), mt.Serves(w), c.want)
		}
	}
}

// spec.workloads reaches the view on a served deployment, a managed source and its target, and a managed source whose catalog entry is not listed
// is still a source of its model, so that a Workload it serves finds no ready target and is not refused as not attached.
func TestBuildCarriesSpecWorkloadsOfServedAndManagedDeployments(t *testing.T) {
	s := batchSnap()
	s.Deployments[0].Spec.Workloads = []string{"alpha", "beta"}
	m := managedSnap("llmlab", "llmlab")
	m.Deployments[0].Spec.Workloads = []string{"alpha"}
	unlisted := m.Deployments[0]
	unlisted.Name, unlisted.Spec.Workloads = "mm-unlisted", []string{"beta"}
	unlisted.Spec.Source.Managed = &v1alpha1.ManagedCatalogEntry{Platform: "llmlab", CatalogName: "not-in-the-catalog"}
	s.Deployments = append(s.Deployments, m.Deployments[0], unlisted)
	s.Providers = append(s.Providers, m.Providers...)
	s.Profiles = append(s.Profiles, m.Profiles...)
	v := Build(s, Inputs{Now: now, HasTransport: all, MaxRequestDuration: 2 * time.Hour})
	model := v.Models["m"]
	if model == nil || len(model.Served) != 1 || strings.Join(model.Served[0].Workloads, ",") != "alpha,beta" {
		t.Fatalf("the served deployment: %+v, problems %v", model, v.Problems)
	}
	if len(model.Managed) != 1 || strings.Join(model.Managed[0].Workloads, ",") != "alpha" {
		t.Fatalf("the managed target: %+v", model.Managed)
	}
	var sources []string
	for _, src := range model.ManagedSources {
		sources = append(sources, src.Key+"="+strings.Join(src.Workloads, "+"))
	}
	if got, want := strings.Join(sources, " "), "ns/mm=alpha ns/mm-unlisted=beta"; got != want {
		t.Errorf("the managed sources: %q, want %q (one with a target, one with none)", got, want)
	}
}

// The watch reads Workloads, in the namespaces it watches, with a list and a watch (the front's role grants exactly those), and a change reaches
// the snapshot and calls OnChange.
func TestWatchReadsWorkloads(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := workload("ns", "alpha", "m")
	b := workload("other-ns", "beta", "m")
	client := newFakeDynamic(toUnstructured(t, &a, "Workload"), toUnstructured(t, &b, "Workload"))
	changed := make(chan struct{}, 100)
	w := NewWatch(client, []string{"ns"}, func() { changed <- struct{}{} })
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	s, problems := w.Snapshot()
	if len(problems) != 0 || len(s.Workloads) != 1 || s.Workloads[0].Name != "alpha" || s.Workloads[0].Spec.Model != "m" {
		t.Fatalf("the snapshot of Workloads in namespace ns: %+v, problems %v; want only alpha", s.Workloads, problems)
	}
	if _, ok := w.LastEvents()["workloads"]; !ok {
		t.Error("no watch age for workloads")
	}
	c := workload("ns", "gamma", "m")
	if _, err := client.Resource(GVRWorkloads).Namespace("ns").Create(ctx, toUnstructured(t, &c, "Workload"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(10 * time.Second)
	for {
		if s, _ = w.Snapshot(); len(s.Workloads) == 2 {
			break
		}
		select {
		case <-changed:
		case <-deadline:
			t.Fatal("the watch did not see the new Workload")
		}
	}
	if err := client.Resource(GVRWorkloads).Namespace("ns").Delete(ctx, "alpha", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	for {
		if s, _ = w.Snapshot(); len(s.Workloads) == 1 && s.Workloads[0].Name == "gamma" {
			break
		}
		select {
		case <-changed:
		case <-deadline:
			t.Fatalf("the watch did not see the delete: %+v", s.Workloads)
		}
	}
}

// Finding 4 of the review of #778: a namespace the configuration lists twice is watched once, so each Workload is listed once. Two different
// namespaces holding one name still give two entries, which is the ambiguity (workload_ambiguous).
func TestWatchWatchesARepeatedNamespaceOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := workload("team-a", "analysis", "m")
	b := workload("team-b", "analysis", "m")
	for name, c := range map[string]struct {
		namespaces []string
		want       int
	}{
		"a namespace listed twice":                  {[]string{"team-a", "team-a"}, 1},
		"a namespace listed three times among more": {[]string{"team-a", "team-b", "team-a", "team-b", "team-a"}, 2},
		"two different namespaces":                  {[]string{"team-a", "team-b"}, 2},
		"one namespace":                             {[]string{"team-a"}, 1},
	} {
		t.Run(name, func(t *testing.T) {
			w := NewWatch(newFakeDynamic(toUnstructured(t, &a, "Workload"), toUnstructured(t, &b, "Workload")), c.namespaces, nil)
			if err := w.Start(ctx); err != nil {
				t.Fatal(err)
			}
			s, problems := w.Snapshot()
			if len(problems) != 0 || len(s.Workloads) != c.want {
				t.Fatalf("namespaces %v: %d Workloads in the snapshot, problems %v; want %d", c.namespaces, len(s.Workloads), problems, c.want)
			}
			v := Build(s, Inputs{Now: now, MaxRequestDuration: 2 * time.Hour})
			if got := len(v.Workloads["m"]["analysis"]); got != c.want {
				t.Errorf("namespaces %v: the view holds %d Workloads named analysis of model m, want %d", c.namespaces, got, c.want)
			}
		})
	}
}
