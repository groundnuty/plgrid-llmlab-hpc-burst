package inventory

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// Expected values come from HBEP-5's rules (Request path step 3, Draining,
// Notes on managed sources) and HBEP-4's drain-window defaults, worked by
// hand below.

var now = time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)

func dur(s string) *v1alpha1.Duration { d := v1alpha1.Duration(s); return &d }

func profile(name string, kind v1alpha1.PlatformKind) v1alpha1.PlatformProfile {
	return v1alpha1.PlatformProfile{ObjectMeta: metav1.ObjectMeta{Name: name, ResourceVersion: "7"}, Spec: v1alpha1.PlatformProfileSpec{ProviderKind: kind}}
}

func prov(name, profile, platform string) v1alpha1.Provider {
	return v1alpha1.Provider{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{v1alpha1.LabelPlatform: platform}},
		Spec: v1alpha1.ProviderSpec{PlatformProfileRef: v1alpha1.ObjectReference{Name: profile}}}
}

func served(name, model string, instances int32) v1alpha1.ModelDeployment {
	return v1alpha1.ModelDeployment{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name, UID: types.UID("md-" + name)},
		Spec: v1alpha1.ModelDeploymentSpec{Model: model, Instances: instances, Providers: v1alpha1.ProviderSelection{Names: []string{"helios-a"}}, Source: v1alpha1.ModelSource{Served: &v1alpha1.ServingSpec{}}}}
}

func instance(md *v1alpha1.ModelDeployment, name, provider string, phase v1alpha1.InstancePhase, leaseEnd *time.Time) v1alpha1.Instance {
	t := true
	in := v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name, UID: types.UID("uid-" + name),
		Labels:          map[string]string{v1alpha1.LabelProvider: provider},
		OwnerReferences: []metav1.OwnerReference{{Kind: "ModelDeployment", Name: md.Name, UID: md.UID, Controller: &t}}},
		Spec:   v1alpha1.InstanceSpec{DeploymentID: v1alpha1.FullDeploymentID(strings.Repeat("0123456789ab", 5) + "cdef"), Launch: v1alpha1.Launch{MaxNumSeqs: 48}},
		Status: v1alpha1.InstanceStatus{Phase: phase}}
	if leaseEnd != nil {
		in.Status.LeaseEnd = &metav1.Time{Time: *leaseEnd}
	}
	return in
}

func batchSnap(insts ...func(md *v1alpha1.ModelDeployment) v1alpha1.Instance) Snapshot {
	md := served("md", "m", 1)
	s := Snapshot{Deployments: []v1alpha1.ModelDeployment{md}, Providers: []v1alpha1.Provider{prov("helios-a", "gh200", "helios")},
		Profiles: []v1alpha1.PlatformProfile{profile("gh200", v1alpha1.PlatformBatch)}}
	for _, f := range insts {
		s.Instances = append(s.Instances, f(&s.Deployments[0]))
	}
	return s
}

func all(types.UID) bool { return true }

func members(v *View) []*Instance {
	if m := v.Models["m"]; m != nil && len(m.Served) == 1 {
		return m.Served[0].Members
	}
	return nil
}

func TestRoutableNeedsReadyLeaseEndAndTransport(t *testing.T) {
	le := now.Add(48 * time.Hour)
	cases := []struct {
		name      string
		phase     v1alpha1.InstancePhase
		leaseEnd  *time.Time
		transport bool
		want      bool
	}{
		{"ready with lease end and transport", v1alpha1.InstanceReady, &le, true, true},
		{"no lease end is never routable", v1alpha1.InstanceReady, nil, true, false},
		{"no transport", v1alpha1.InstanceReady, &le, false, false},
		{"verifying, never ready yet", v1alpha1.InstanceVerifying, &le, true, false},
		{"draining", v1alpha1.InstanceDraining, &le, true, false},
		{"ended", v1alpha1.InstanceEnded, &le, true, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := batchSnap(func(md *v1alpha1.ModelDeployment) v1alpha1.Instance {
				return instance(md, "i0", "helios-a", c.phase, c.leaseEnd)
			})
			v := Build(s, Inputs{Now: now, HasTransport: func(types.UID) bool { return c.transport }, MaxRequestDuration: 2 * time.Hour})
			if got := len(members(v)) == 1; got != c.want {
				t.Errorf("routable = %v, want %v (problems %v)", got, c.want, v.Problems)
			}
		})
	}
}

// TestSkipKeepsMembership: once routable, an Instance back in verifying, or
// without its transport, or AgentUnreachable stays a member but is not
// eligible (HBEP-5, Request path, Routable and Eligible).
func TestSkipKeepsMembership(t *testing.T) {
	le := now.Add(48 * time.Hour)
	ready := batchSnap(func(md *v1alpha1.ModelDeployment) v1alpha1.Instance {
		return instance(md, "i0", "helios-a", v1alpha1.InstanceReady, &le)
	})
	v1 := Build(ready, Inputs{Now: now, HasTransport: all, MaxRequestDuration: 2 * time.Hour})
	if m := members(v1); len(m) != 1 || !m[0].Eligible {
		t.Fatalf("first build: members %v", m)
	}
	for name, edit := range map[string]func(s *Snapshot) func(types.UID) bool{
		"verifying": func(s *Snapshot) func(types.UID) bool {
			s.Instances[0].Status.Phase = v1alpha1.InstanceVerifying
			return all
		},
		"no transport": func(s *Snapshot) func(types.UID) bool {
			return func(types.UID) bool { return false }
		},
		"AgentUnreachable": func(s *Snapshot) func(types.UID) bool {
			s.Instances[0].Status.Conditions = []metav1.Condition{{Type: v1alpha1.ConditionAgentUnreachable, Status: metav1.ConditionTrue}}
			return all
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := batchSnap(func(md *v1alpha1.ModelDeployment) v1alpha1.Instance {
				return instance(md, "i0", "helios-a", v1alpha1.InstanceReady, &le)
			})
			has := edit(&s)
			v2 := Build(s, Inputs{Now: now, HasTransport: has, Joined: v1.Joined, MaxRequestDuration: 2 * time.Hour})
			m := members(v2)
			if len(m) != 1 || m[0].Eligible {
				t.Errorf("after the skip: members %d, eligible %v; want a member that is not eligible", len(m), len(m) == 1 && m[0].Eligible)
			}
		})
	}
}

// TestDrainStartRemovesTheInstance: the default window is 2 h 10 min
// (HBEP-5, open question 12), so a lease end at now + 2 h 10 min is the drain
// start now: not routable; at now + 2 h 11 min it is routable and NextChange
// is now + 1 min.
func TestDrainStartRemovesTheInstance(t *testing.T) {
	for _, c := range []struct {
		leaseIn  time.Duration
		routable bool
	}{{2*time.Hour + 10*time.Minute, false}, {2*time.Hour + 11*time.Minute, true}} {
		le := now.Add(c.leaseIn)
		s := batchSnap(func(md *v1alpha1.ModelDeployment) v1alpha1.Instance {
			return instance(md, "i0", "helios-a", v1alpha1.InstanceReady, &le)
		})
		v := Build(s, Inputs{Now: now, HasTransport: all, MaxRequestDuration: 2 * time.Hour})
		if got := len(members(v)) == 1; got != c.routable {
			t.Errorf("lease end in %s: routable %v, want %v", c.leaseIn, got, c.routable)
		}
		if c.routable && !v.NextChange.Equal(now.Add(time.Minute)) {
			t.Errorf("NextChange = %s, want now + 1 min", v.NextChange)
		}
	}
}

// TestDeploymentShortensTheWindow: routing.maxRequestDuration 20 min plus the
// default margin 10 min is a 30 min window, below 2 h 10 min, so a lease end
// in 31 min is routable and NextChange is in 1 min.
func TestDeploymentShortensTheWindow(t *testing.T) {
	le := now.Add(31 * time.Minute)
	s := batchSnap(func(md *v1alpha1.ModelDeployment) v1alpha1.Instance {
		return instance(md, "i0", "helios-a", v1alpha1.InstanceReady, &le)
	})
	s.Deployments[0].Spec.Routing = &v1alpha1.Routing{MaxRequestDuration: dur("20m")}
	s.Instances[0].OwnerReferences[0].UID = s.Deployments[0].UID
	v := Build(s, Inputs{Now: now, HasTransport: all, MaxRequestDuration: 2 * time.Hour})
	if len(members(v)) != 1 || !v.NextChange.Equal(now.Add(time.Minute)) {
		t.Fatalf("members %d, NextChange %s", len(members(v)), v.NextChange)
	}
	if d := v.Models["m"].Served[0].MaxRequestDuration; d != 20*time.Minute {
		t.Errorf("the deployment's deadline = %s, want 20m", d)
	}
}

func TestCapAndBounds(t *testing.T) {
	le := now.Add(48 * time.Hour)
	s := batchSnap(func(md *v1alpha1.ModelDeployment) v1alpha1.Instance {
		return instance(md, "i0", "helios-a", v1alpha1.InstanceReady, &le)
	})
	v := Build(s, Inputs{Now: now, HasTransport: all, MaxRequestDuration: 2 * time.Hour})
	d := v.Models["m"].Served[0]
	if d.TightPercent != 125 || d.LoosePercent != 200 || d.Members[0].Cap != 48 {
		t.Errorf("defaults: tight %d loose %d cap %d; want 125, 200 and maxNumSeqs 48 x 1 worker", d.TightPercent, d.LoosePercent, d.Members[0].Cap)
	}
	capv := int32(2)
	tight, loose := v1alpha1.LoadFactor("1.5"), v1alpha1.LoadFactor("3")
	s.Deployments[0].Spec.Routing = &v1alpha1.Routing{InstanceCap: &capv, LoadBound: &v1alpha1.LoadBound{Tight: &tight, Loose: &loose}}
	v = Build(s, Inputs{Now: now, HasTransport: all, MaxRequestDuration: 2 * time.Hour})
	d = v.Models["m"].Served[0]
	if d.TightPercent != 150 || d.LoosePercent != 300 || d.Members[0].Cap != 2 {
		t.Errorf("set: tight %d loose %d cap %d; want 150, 300, 2", d.TightPercent, d.LoosePercent, d.Members[0].Cap)
	}
}

func TestDrainWindowShort(t *testing.T) {
	s := batchSnap()
	s.Profiles[0].Spec.DrainWindow = dur("1h")
	v := Build(s, Inputs{Now: now, MaxRequestDuration: 2 * time.Hour})
	if len(v.ShortProfiles) != 1 || v.ShortProfiles[0].Required != 2*time.Hour+10*time.Minute {
		t.Fatalf("short profiles %+v, want gh200 needing 2h10m", v.ShortProfiles)
	}
	s.Profiles[0].Spec.DrainWindow = dur("2h10m")
	if v := Build(s, Inputs{Now: now, MaxRequestDuration: 2 * time.Hour}); len(v.ShortProfiles) != 0 {
		t.Errorf("a window equal to the bound is enough: %+v", v.ShortProfiles)
	}
}

// managedSnap: an LLMLab provider on platform "llmlab" listing entry "m-cat",
// a managed-source ModelDeployment of model "m" naming platform srcPlatform.
func managedSnap(srcPlatform, providerPlatform string) Snapshot {
	md := v1alpha1.ModelDeployment{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "mm"},
		Spec: v1alpha1.ModelDeploymentSpec{Model: "m", Providers: v1alpha1.ProviderSelection{Names: []string{"llmlab"}},
			Source: v1alpha1.ModelSource{Managed: &v1alpha1.ManagedCatalogEntry{Platform: srcPlatform, CatalogName: "m-cat"}}}}
	p := prov("llmlab", "llmlab-profile", providerPlatform)
	p.Spec.CredentialsRef = &v1alpha1.CredentialsReference{File: "/nonexistent/creds.json"}
	p.Status.Catalog = []v1alpha1.CatalogEntryStatus{{Name: "m-cat", Listed: true, ManagedID: v1alpha1.FullDeploymentID(strings.Repeat("f", 64))}}
	return Snapshot{Deployments: []v1alpha1.ModelDeployment{md}, Providers: []v1alpha1.Provider{p},
		Profiles: []v1alpha1.PlatformProfile{profile("llmlab-profile", v1alpha1.PlatformManaged)}}
}

func managedTargets(v *View) int {
	if m := v.Models["m"]; m != nil {
		return len(m.Managed)
	}
	return -1
}

func TestManagedEntryAttached(t *testing.T) {
	v := Build(managedSnap("llmlab", "llmlab"), Inputs{Now: now, MaxRequestDuration: 2 * time.Hour})
	if managedTargets(v) != 1 || v.Models["m"].Managed[0].DisplayID != "ffffffffffff" {
		t.Fatalf("targets %d, problems %v", managedTargets(v), v.Problems)
	}
}

// TestManagedGuardAlone: no condition at all, the Provider off the platform:
// the front's own platform check keeps the entry unrouted.
func TestManagedGuardAlone(t *testing.T) {
	v := Build(managedSnap("llmlab", "other-platform"), Inputs{Now: now, MaxRequestDuration: 2 * time.Hour})
	if n := managedTargets(v); n != 0 {
		t.Errorf("targets = %d, want 0: the Provider is not on spec.source.managed.platform", n)
	}
}

// TestManagedConditionAlone: the Provider is on the platform, but the
// lifecycle controller's Ready=False/ProviderNotOnPlatform is set: the filter
// alone keeps it unrouted.
func TestManagedConditionAlone(t *testing.T) {
	s := managedSnap("llmlab", "llmlab")
	s.Deployments[0].Status.Conditions = []metav1.Condition{{Type: v1alpha1.ConditionReady, Status: metav1.ConditionFalse, Reason: v1alpha1.ReasonProviderNotOnPlatform}}
	v := Build(s, Inputs{Now: now, MaxRequestDuration: 2 * time.Hour})
	if n := managedTargets(v); n != 0 {
		t.Errorf("targets = %d, want 0: Ready is False with ProviderNotOnPlatform", n)
	}
}

func TestManagedEntryUnlistedOrRejected(t *testing.T) {
	s := managedSnap("llmlab", "llmlab")
	s.Providers[0].Status.Catalog[0].Listed = false
	if n := managedTargets(Build(s, Inputs{Now: now, MaxRequestDuration: 2 * time.Hour})); n != 0 {
		t.Errorf("unlisted entry: targets %d", n)
	}
	s = managedSnap("llmlab", "llmlab")
	s.Providers[0].Status.Conditions = []metav1.Condition{{Type: v1alpha1.ConditionCredentialsRejected, Status: metav1.ConditionTrue}}
	if n := managedTargets(Build(s, Inputs{Now: now, MaxRequestDuration: 2 * time.Hour})); n != 0 {
		t.Errorf("rejected credentials: targets %d", n)
	}
}

// TestServedSelectionIgnoresManagedProviders: a served deployment whose
// selector matches the LLMLab provider's labels gets no target there, and an
// Instance claiming a managed provider is not routed (HBEP-5, Notes).
func TestServedSelectionIgnoresManagedProviders(t *testing.T) {
	s := managedSnap("llmlab", "llmlab")
	md := served("md", "m", 1)
	md.Spec.Providers = v1alpha1.ProviderSelection{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{v1alpha1.LabelPlatform: "llmlab"}}}
	s.Deployments = []v1alpha1.ModelDeployment{md}
	le := now.Add(48 * time.Hour)
	s.Instances = []v1alpha1.Instance{instance(&s.Deployments[0], "i0", "llmlab", v1alpha1.InstanceReady, &le)}
	v := Build(s, Inputs{Now: now, HasTransport: all, MaxRequestDuration: 2 * time.Hour})
	if m := v.Models["m"]; m == nil || len(m.Managed) != 0 || len(m.Served) != 1 || len(m.Served[0].Members) != 0 {
		t.Errorf("model %+v: want one served deployment with no member and no managed target", m)
	}
}
