package inventory

import (
	"sort"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// addManaged attaches a managed-source ModelDeployment's catalog entry under
// each managed Provider it allows (HBEP-5, Notes, Routing policy; Request
// path, routable managed target). Two independent checks keep an entry off a
// Provider of another platform (issue #64):
//
//   - the filter: the lifecycle controller's Ready=False with reason
//     ProviderNotOnPlatform routes nothing for the deployment;
//   - the guard: a Provider whose platform label is not
//     spec.source.managed.platform is never attached, whatever the condition
//     says, so a stale or missing condition routes nothing off the platform.
func (v *View) addManaged(md *v1alpha1.ModelDeployment, providers map[string]*v1alpha1.Provider, in Inputs) {
	key := md.Namespace + "/" + md.Name
	src := md.Spec.Source.Managed
	mm := v.model(md.Spec.Model) // the name is attached, even with no target
	if c := findCondition(md.Status.Conditions, v1alpha1.ConditionReady); c != nil &&
		c.Status == metav1.ConditionFalse && c.Reason == v1alpha1.ReasonProviderNotOnPlatform {
		return
	}
	mrd := in.MaxRequestDuration
	if r := md.Spec.Routing; r != nil && r.MaxRequestDuration != nil {
		if d, err := r.MaxRequestDuration.AsDuration(); err == nil && d > 0 && d < mrd {
			mrd = d
		}
	}
	for _, name := range sortedNames(providers) {
		p := providers[name]
		info := v.Providers[name]
		if info == nil || info.Kind != v1alpha1.PlatformManaged || info.Platform != src.Platform {
			continue
		}
		ok, err := selected(md.Spec.Providers, p)
		if err != nil {
			v.problemf("ModelDeployment %s: provider selector: %v", key, err)
			return
		}
		if !ok || conditionTrue(p.Status.Conditions, v1alpha1.ConditionCredentialsRejected) {
			continue
		}
		entry := listedEntry(p, src.CatalogName)
		if entry == nil {
			continue
		}
		t := &ManagedTarget{
			Key: key + "@" + name, Deployment: key, Namespace: md.Namespace, Name: md.Name, Provider: name,
			CatalogName: src.CatalogName, ManagedID: string(entry.ManagedID), DisplayID: entry.ManagedID.Display(),
			MaxRequestDuration: mrd,
		}
		if p.Spec.CredentialsRef != nil {
			t.CredentialsFile = p.Spec.CredentialsRef.File
		}
		if t.CredentialsFile == "" {
			v.problemf("Provider %s has no credentials file; ModelDeployment %s is not routed through it", name, key)
			continue
		}
		mm.Managed = append(mm.Managed, t)
	}
}

// listedEntry is the Provider's catalog entry with that name, while the
// latest catalog read lists it.
func listedEntry(p *v1alpha1.Provider, name string) *v1alpha1.CatalogEntryStatus {
	for i := range p.Status.Catalog {
		if e := &p.Status.Catalog[i]; e.Name == name && e.Listed {
			return e
		}
	}
	return nil
}

func findCondition(conds []metav1.Condition, t string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == t {
			return &conds[i]
		}
	}
	return nil
}

func sortedNames(m map[string]*v1alpha1.Provider) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// checkDrainWindows is HBEP-5's drain-window check: every profile with a
// batch Provider needs drainWindow >= the front's maxRequestDuration plus the
// profile's drainMargin, or long requests on its Instances can be cut at the
// lease end.
func (v *View) checkDrainWindows(providers []v1alpha1.Provider, profiles map[string]*v1alpha1.PlatformProfile, maxReq time.Duration) {
	used := map[string]bool{}
	for i := range providers {
		used[providers[i].Spec.PlatformProfileRef.Name] = true
	}
	names := make([]string, 0, len(profiles))
	for n := range profiles {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		pp := profiles[n]
		if !used[n] || pp.Spec.ProviderKind != v1alpha1.PlatformBatch {
			continue
		}
		window, err1 := durationOr(pp.Spec.DrainWindow, v1alpha1.DefaultDrainWindow)
		margin, err2 := durationOr(pp.Spec.DrainMargin, v1alpha1.DefaultDrainMargin)
		if err1 != nil || err2 != nil {
			v.problemf("PlatformProfile %s: unreadable drainWindow or drainMargin", n)
			continue
		}
		if window < maxReq+margin {
			v.ShortProfiles = append(v.ShortProfiles, ShortProfile{Name: n, ResourceVersion: pp.ResourceVersion, DrainWindow: window, Required: maxReq + margin})
		}
	}
}
