package inventory

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The load bounds' defaults, the schema defaults of spec.routing.loadBound
// (HBEP-5, Parameters). An object built without the API server may lack them.
const (
	DefaultTight = "1.25"
	DefaultLoose = "2.0"
)

// Build computes the routing view of a snapshot at in.Now.
func Build(s Snapshot, in Inputs) *View {
	v := &View{Models: map[string]*Model{}, Providers: map[string]*ProviderInfo{}, Joined: map[types.UID]bool{}}
	if in.HasTransport == nil {
		in.HasTransport = func(types.UID) bool { return false }
	}
	profiles := map[string]*v1alpha1.PlatformProfile{}
	for i := range s.Profiles {
		profiles[s.Profiles[i].Name] = &s.Profiles[i]
	}
	for i := range s.Providers {
		p := &s.Providers[i]
		info := &ProviderInfo{Name: p.Name, Platform: p.Labels[v1alpha1.LabelPlatform], Labels: p.Labels, Profile: profiles[p.Spec.PlatformProfileRef.Name]}
		if info.Profile != nil {
			info.Kind = info.Profile.Spec.ProviderKind
		} else {
			v.problemf("Provider %s names PlatformProfile %q, which does not exist", p.Name, p.Spec.PlatformProfileRef.Name)
		}
		v.Providers[p.Name] = info
	}
	byOwner := instancesByOwner(s.Instances)
	providers := map[string]*v1alpha1.Provider{}
	for i := range s.Providers {
		providers[s.Providers[i].Name] = &s.Providers[i]
	}
	for i := range s.Deployments {
		md := &s.Deployments[i]
		switch {
		case md.Spec.Source.Served != nil:
			v.addServed(md, byOwner[md.UID], providers, in)
		case md.Spec.Source.Managed != nil:
			v.addManaged(md, providers, in)
		}
	}
	for _, m := range v.Models {
		sort.Slice(m.Served, func(i, j int) bool { return m.Served[i].Key < m.Served[j].Key })
		sort.Slice(m.Managed, func(i, j int) bool { return m.Managed[i].Key < m.Managed[j].Key })
	}
	v.checkDrainWindows(s.Providers, profiles, in.MaxRequestDuration)
	return v
}

func (v *View) problemf(format string, args ...any) {
	v.Problems = append(v.Problems, fmt.Sprintf(format, args...))
}

func (v *View) model(name string) *Model {
	m := v.Models[name]
	if m == nil {
		m = &Model{Name: name}
		v.Models[name] = m
	}
	return m
}

// instancesByOwner groups Instances by the UID of their controlling
// ModelDeployment. The model-deployment label is shortened for long names
// (v1alpha1.LabelValue), so the owner reference is the link.
func instancesByOwner(insts []v1alpha1.Instance) map[types.UID][]*v1alpha1.Instance {
	out := map[types.UID][]*v1alpha1.Instance{}
	for i := range insts {
		in := &insts[i]
		if ref := metav1.GetControllerOf(in); ref != nil && ref.Kind == "ModelDeployment" {
			out[ref.UID] = append(out[ref.UID], in)
		}
	}
	return out
}

func (v *View) addServed(md *v1alpha1.ModelDeployment, insts []*v1alpha1.Instance, providers map[string]*v1alpha1.Provider, in Inputs) {
	d := &Deployment{
		Key: md.Namespace + "/" + md.Name, Namespace: md.Namespace, Name: md.Name, UID: md.UID,
		ServedName: md.Spec.Model, Desired: int(md.Spec.Instances),
		MaxRequestDuration: in.MaxRequestDuration,
	}
	var err error
	if d.TightPercent, d.LoosePercent, err = bounds(md.Spec.Routing); err != nil {
		v.problemf("ModelDeployment %s: %v; it is not routed", d.Key, err)
		return
	}
	if r := md.Spec.Routing; r != nil && r.MaxRequestDuration != nil {
		if mrd, err := r.MaxRequestDuration.AsDuration(); err == nil && mrd > 0 && mrd < d.MaxRequestDuration {
			d.MaxRequestDuration = mrd
		}
	}
	attached := v.attachedProviders(md, providers)
	for _, inst := range insts {
		if m := v.routable(md, d, inst, attached, in); m != nil {
			d.Members = append(d.Members, m)
			v.Joined[inst.UID] = true
		}
	}
	sort.Slice(d.Members, func(i, j int) bool { return d.Members[i].Key < d.Members[j].Key })
	mm := v.model(md.Spec.Model)
	mm.Served = append(mm.Served, d)
}

// attachedProviders is the set of Providers the deployment's current provider
// selection allows (HBEP-5, Notes, Routing policy). An unreadable selector
// attaches none and is logged once.
func (v *View) attachedProviders(md *v1alpha1.ModelDeployment, providers map[string]*v1alpha1.Provider) map[string]bool {
	out := map[string]bool{}
	for _, name := range sortedNames(providers) {
		ok, err := selected(md.Spec.Providers, providers[name])
		if err != nil {
			v.problemf("ModelDeployment %s: provider selector: %v; none of its Instances takes a new request", md.Namespace+"/"+md.Name, err)
			return map[string]bool{}
		}
		out[name] = ok
	}
	return out
}

// routable returns the Instance's routing entry when it is routable at
// in.Now (HBEP-5, Request path, Routable), or nil. attached is the set of
// Providers the deployment selects now. An Instance whose Provider is not in it
// never joins; one that joined earlier stays a member, not eligible, like any
// other skip, so its open requests keep their count until they end (a leave
// would release them at once).
func (v *View) routable(md *v1alpha1.ModelDeployment, d *Deployment, inst *v1alpha1.Instance, attached map[string]bool, in Inputs) *Instance {
	key := inst.Namespace + "/" + inst.Name
	phase := inst.Status.Phase
	if inst.DeletionTimestamp != nil || phase == v1alpha1.InstanceDraining || phase == v1alpha1.InstanceEnded {
		return nil
	}
	if inst.Status.LeaseEnd == nil {
		return nil // no drain start can be computed: never routable
	}
	prov := v.Providers[inst.Labels[v1alpha1.LabelProvider]]
	if prov == nil || prov.Profile == nil || prov.Kind == v1alpha1.PlatformManaged {
		v.problemf("Instance %s: its Provider %q or that Provider's profile is unknown, or managed; it is not routed", key, inst.Labels[v1alpha1.LabelProvider])
		return nil
	}
	drain, err := v1alpha1.DrainStart(inst.Status.LeaseEnd.Time, prov.Profile.Spec, md.Spec.Routing)
	if err != nil {
		v.problemf("Instance %s: drain start: %v; it is not routed", key, err)
		return nil
	}
	if !in.Now.Before(drain) {
		return nil
	}
	if v.NextChange.IsZero() || drain.Before(v.NextChange) {
		v.NextChange = drain
	}
	transport := in.HasTransport(inst.UID)
	ready := phase == v1alpha1.InstanceReady
	sel := attached[prov.Name]
	if !in.Joined[inst.UID] && !(ready && transport && sel) {
		return nil
	}
	return &Instance{
		Key: key, Namespace: inst.Namespace, Name: inst.Name, UID: inst.UID, Deployment: d.Key,
		Provider: prov.Name, DeploymentID: string(inst.Spec.DeploymentID), DisplayID: inst.Spec.DeploymentID.Display(),
		Cap: instanceCap(md.Spec.Routing, inst), LeaseEnd: inst.Status.LeaseEnd.Time, DrainStart: drain,
		Eligible: ready && transport && sel && !conditionTrue(inst.Status.Conditions, v1alpha1.ConditionAgentUnreachable),
	}
}

func instanceCap(r *v1alpha1.Routing, inst *v1alpha1.Instance) int {
	if r != nil && r.InstanceCap != nil && *r.InstanceCap > 0 {
		return int(*r.InstanceCap)
	}
	seqs := int(inst.Spec.Launch.MaxNumSeqs)
	if seqs <= 0 {
		seqs = v1alpha1.DefaultMaxNumSeqs
	}
	return seqs * Workers
}

// bounds returns the tight and loose bounds as KubeAI percents.
func bounds(r *v1alpha1.Routing) (tight, loose int, err error) {
	t, l := v1alpha1.LoadFactor(DefaultTight), v1alpha1.LoadFactor(DefaultLoose)
	if r != nil && r.LoadBound != nil {
		if r.LoadBound.Tight != nil {
			t = *r.LoadBound.Tight
		}
		if r.LoadBound.Loose != nil {
			l = *r.LoadBound.Loose
		}
	}
	if tight, err = Percent(t); err != nil {
		return 0, 0, err
	}
	if loose, err = Percent(l); err != nil {
		return 0, 0, err
	}
	return tight, loose, nil
}

// loadFactorPattern is what the API accepts of a LoadFactor (v1alpha1's
// kubebuilder pattern): digits with an optional fraction, at most 16 characters.
var loadFactorPattern = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?$`)

// Percent converts a load factor ("1.25") to KubeAI's MeanLoadPercentage
// (125). KubeAI's setting is an integer, and the forked group's pick takes
// nothing finer, so a factor with more than two decimals is rounded DOWN to a
// whole percent: "1.006" is 100, not 101. That narrows the configured bound
// and never widens it; rounding to the nearest percent would admit a member
// whose load exceeds c x (T + 1) / n (c = 1.006, loads 101 and 98: the bound
// is 100.6, and 101 percent admits the 101). The digits are cut, not a float
// floored, since 1.15 x 100 is 114.99999999999999 in float64.
func Percent(f v1alpha1.LoadFactor) (int, error) {
	bad := fmt.Errorf("load factor %q is not a decimal of at least 1", f)
	if !loadFactorPattern.MatchString(string(f)) {
		return 0, bad
	}
	whole, frac, _ := strings.Cut(string(f), ".")
	n, err := strconv.ParseInt(whole, 10, 64)
	if err != nil || n > (math.MaxInt-99)/100 {
		return 0, bad
	}
	cents, _ := strconv.Atoi((frac + "00")[:2])
	if p := int(n)*100 + cents; p >= 100 {
		return p, nil
	}
	return 0, bad
}

func conditionTrue(conds []metav1.Condition, t string) bool {
	for _, c := range conds {
		if c.Type == t {
			return c.Status == metav1.ConditionTrue
		}
	}
	return false
}

// selected reports whether a ModelDeployment's provider selection allows p,
// by name or by label selector.
func selected(sel v1alpha1.ProviderSelection, p *v1alpha1.Provider) (bool, error) {
	for _, n := range sel.Names {
		if n == p.Name {
			return true, nil
		}
	}
	if sel.Selector == nil {
		return false, nil
	}
	s, err := metav1.LabelSelectorAsSelector(sel.Selector)
	if err != nil {
		return false, err
	}
	return s.Matches(labels.Set(p.Labels)), nil
}

// durationOr parses an optional duration with a default.
func durationOr(d *v1alpha1.Duration, def time.Duration) (time.Duration, error) {
	if d == nil {
		return def, nil
	}
	return d.AsDuration()
}
