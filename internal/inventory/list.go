package inventory

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The reasons of the unknowns the rows hold (docs/agent-cli.md lists them).
const (
	reasonNoOwner      = "no owner annotation"
	reasonNoPurpose    = "no purpose annotation"
	reasonNotReported  = "the controller has not reported on the ModelDeployment yet"
	reasonNoDeployment = "the Allocation names no ModelDeployment"
	reasonNoPhase      = "no phase reported yet"
	reasonNoProvider   = "the Allocation names no Provider"
	reasonNoHardEnd    = "not observed yet"
	reasonBadIdleSince = "the idle-since annotation is not an RFC 3339 time"
	reasonBadIdleLimit = "the idleLimit is not a duration"
	reasonBadMaxIdle   = "the profile's maxIdle is not a duration"
	reasonNeverRead    = "never read"
	reasonReadInFuture = "the read is after the command's clock"
	reasonNoPlan       = "the Operation has no plan"
)

// List reads the inventory through c and returns its rows. It reads and never writes: c is a Reader, so
// nothing here can create, update, patch or delete. now is the command's clock, read once by the caller;
// every age counts from it.
//
// With Options.Owner it selects by owner (docs/agent-cli.md). Allocations and Instances are selected by
// the label owner-hash, as a label selector the API server applies. ModelDeployments are selected by the
// owner annotation, and Operations by spec.owner, since neither carries the label that a selector could
// use on them: a ModelDeployment's label is the lifecycle controller's, and an Operation has none.
func List(ctx context.Context, c client.Reader, o Options, now time.Time) (*Inventory, error) {
	if !o.AllNamespaces && o.Namespace == "" {
		return nil, errors.New("inventory: no namespace and not every namespace")
	}
	scope := []client.ListOption{}
	if !o.AllNamespaces {
		scope = append(scope, client.InNamespace(o.Namespace))
	}
	owned := slices.Clone(scope)
	if o.Owner != "" {
		owned = append(owned, client.MatchingLabels{v1alpha1.LabelOwnerHash: v1alpha1.OwnerHash(o.Owner)})
	}
	var (
		mds    v1alpha1.ModelDeploymentList
		allocs v1alpha1.AllocationList
		insts  v1alpha1.InstanceList
		ops    v1alpha1.OperationList
		provs  v1alpha1.ProviderList
		profs  v1alpha1.PlatformProfileList
	)
	for _, r := range []struct {
		kind string
		list client.ObjectList
		opts []client.ListOption
	}{
		{"ModelDeployments", &mds, scope}, {"Allocations", &allocs, owned}, {"Instances", &insts, owned},
		{"Operations", &ops, scope}, {"Providers", &provs, nil}, {"PlatformProfiles", &profs, nil},
	} {
		if err := c.List(ctx, r.list, r.opts...); err != nil {
			return nil, fmt.Errorf("list %s: %w", r.kind, err)
		}
	}
	inv := &Inventory{
		SchemaVersion: SchemaVersion, ObservedAt: rfc3339(now), SelectedOwner: o.Owner,
		Scope:            Scope{Namespace: o.Namespace, AllNamespaces: o.AllNamespaces},
		ModelDeployments: []DeploymentRow{}, Allocations: []AllocationRow{}, Instances: []InstanceRow{},
		Providers: []ProviderRow{}, Operations: []OperationRow{},
	}
	if o.AllNamespaces {
		inv.Scope.Namespace = ""
	}
	maxIdles := profileMaxIdles(provs.Items, profs.Items)
	live := liveInstances(insts.Items)
	uses := map[string]map[string]bool{} // namespace/deployment -> the Providers of its Allocations that are not over
	for i := range allocs.Items {
		a := &allocs.Items[i]
		inv.Allocations = append(inv.Allocations, allocationRow(a, maxIdles, live[key(a.Namespace, a.Name)] > 0))
		if ref, name := a.Spec.ModelDeploymentRef, providerName(a); ref != nil && name != "" && !over(a.Status.AtProvider.Phase) {
			k := key(a.Namespace, ref.Name)
			if uses[k] == nil {
				uses[k] = map[string]bool{}
			}
			uses[k][name] = true
		}
	}
	for i := range mds.Items {
		md := &mds.Items[i]
		if o.Owner != "" && md.Annotations[v1alpha1.AnnotationOwner] != o.Owner {
			continue
		}
		inv.ModelDeployments = append(inv.ModelDeployments, deploymentRow(md, uses[key(md.Namespace, md.Name)]))
	}
	for i := range insts.Items {
		inv.Instances = append(inv.Instances, instanceRow(&insts.Items[i]))
	}
	for i := range provs.Items {
		inv.Providers = append(inv.Providers, providerRow(&provs.Items[i], now))
	}
	for i := range ops.Items {
		op := &ops.Items[i]
		if op.Status.Phase == v1alpha1.OperationPhaseAwaitingApproval && (o.Owner == "" || op.Spec.Owner == o.Owner) {
			inv.Operations = append(inv.Operations, operationRow(op))
		}
	}
	slices.SortFunc(inv.ModelDeployments, func(a, b DeploymentRow) int {
		return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name))
	})
	slices.SortFunc(inv.Allocations, func(a, b AllocationRow) int {
		return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name))
	})
	slices.SortFunc(inv.Instances, func(a, b InstanceRow) int {
		return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name))
	})
	slices.SortFunc(inv.Providers, func(a, b ProviderRow) int { return cmp.Compare(a.Name, b.Name) })
	slices.SortFunc(inv.Operations, func(a, b OperationRow) int {
		return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name))
	})
	return inv, nil
}

func key(namespace, name string) string { return namespace + "/" + name }

func rfc3339(t time.Time) string { return t.UTC().Truncate(time.Second).Format(time.RFC3339) }

// owner and purpose are an object's own annotations; an absent or empty one is an unknown.
func owner(annotations map[string]string) Fact {
	if v := annotations[v1alpha1.AnnotationOwner]; v != "" {
		return Text(v)
	}
	return Unknown(reasonNoOwner)
}

func purpose(annotations map[string]string) Fact {
	if v := annotations[v1alpha1.AnnotationPurpose]; v != "" {
		return Text(v)
	}
	return Unknown(reasonNoPurpose)
}

// over reports whether an Allocation's job is over: its phase is terminal.
func over(p v1alpha1.AllocationPhase) bool {
	switch p {
	case v1alpha1.AllocationEnded, v1alpha1.AllocationFailed, v1alpha1.AllocationLost, v1alpha1.AllocationRefused:
		return true
	}
	return false
}

func providerName(a *v1alpha1.Allocation) string {
	if a.Spec.ProviderConfigReference == nil {
		return ""
	}
	return a.Spec.ProviderConfigReference.Name
}

// liveInstances counts, per Allocation (by spec.allocationRef, never by a label), the Instances of the
// listing that are not Ended.
func liveInstances(insts []v1alpha1.Instance) map[string]int {
	out := map[string]int{}
	for i := range insts {
		if in := &insts[i]; in.Status.Phase != v1alpha1.InstanceEnded {
			out[key(in.Namespace, in.Spec.AllocationRef.Name)]++
		}
	}
	return out
}

// maxIdle is what a Provider's PlatformProfile says of maxIdle: a fact, or the zero Fact when the profile
// sets none.
type maxIdle map[string]Fact // by Provider name

// profileMaxIdles reads the maxIdle of each Provider's PlatformProfile. A Provider whose profile is not
// among profs is an unknown.
func profileMaxIdles(provs []v1alpha1.Provider, profs []v1alpha1.PlatformProfile) maxIdle {
	byName := map[string]*v1alpha1.PlatformProfile{}
	for i := range profs {
		byName[profs[i].Name] = &profs[i]
	}
	out := maxIdle{}
	for i := range provs {
		p := &provs[i]
		prof := byName[p.Spec.PlatformProfileRef.Name]
		switch {
		case prof == nil:
			out[p.Name] = Unknown("PlatformProfile " + p.Spec.PlatformProfileRef.Name + " not found")
		case prof.Spec.MaxIdle == nil:
			out[p.Name] = Fact{}
		default:
			if d, err := prof.Spec.MaxIdle.AsDuration(); err != nil {
				out[p.Name] = Unknown(reasonBadMaxIdle)
			} else {
				out[p.Name] = Span(d)
			}
		}
	}
	return out
}

func allocationRow(a *v1alpha1.Allocation, maxIdles maxIdle, hostsInstance bool) AllocationRow {
	obs := a.Status.AtProvider
	row := AllocationRow{
		Namespace: a.Namespace, Name: a.Name, Owner: owner(a.Annotations), Purpose: purpose(a.Annotations),
		ModelDeployment: Unknown(reasonNoDeployment), Phase: Unknown(reasonNoPhase), Provider: Unknown(reasonNoProvider), HardEnd: Unknown(reasonNoHardEnd),
	}
	if ref := a.Spec.ModelDeploymentRef; ref != nil && ref.Name != "" {
		row.ModelDeployment = Text(ref.Name)
	}
	if obs.Phase != "" {
		row.Phase = Text(string(obs.Phase))
	}
	if obs.HardEnd != nil {
		row.HardEnd = Instant(obs.HardEnd.Time)
	}
	pol := a.Spec.RetentionPolicy
	row.Retention = RetentionRow{Mode: string(pol.EffectiveMode())}
	if pol != nil && pol.Until != nil {
		row.Retention.Until = Instant(pol.Until.Time)
	}
	if pol != nil && pol.IdleLimit != nil {
		if d, err := pol.IdleLimit.AsDuration(); err != nil {
			row.Retention.IdleLimit = Unknown(reasonBadIdleLimit)
		} else {
			row.Retention.IdleLimit = Span(d)
		}
	}
	if name := providerName(a); name != "" {
		row.Provider = Text(name)
		if f, ok := maxIdles[name]; ok {
			row.Retention.ProfileMaxIdle = f
		} else {
			row.Retention.ProfileMaxIdle = Unknown("Provider " + name + " not found")
		}
	} else {
		row.Retention.ProfileMaxIdle = Unknown(reasonNoProvider)
	}
	_, idle := a.Annotations[v1alpha1.AnnotationIdleSince]
	if idle {
		if t, _, err := v1alpha1.IdleSince(a); err != nil {
			row.IdleSince = Unknown(reasonBadIdleSince)
		} else {
			row.IdleSince = Instant(t)
		}
	}
	// A kept Allocation with no Instance: its job runs, its policy keeps it, the controller's anchor says it
	// is idle, and no Instance of the listing is on it. Both the anchor and the listing must say so: under
	// --owner the listing holds only the Instances that carry the owner label.
	row.KeptNoInstance = obs.Phase == v1alpha1.AllocationRunning && pol.EffectiveMode() == v1alpha1.RetentionKeep && idle && !hostsInstance
	return row
}

func deploymentRow(md *v1alpha1.ModelDeployment, uses map[string]bool) DeploymentRow {
	row := DeploymentRow{
		Namespace: md.Namespace, Name: md.Name, Owner: owner(md.Annotations), Purpose: purpose(md.Annotations),
		Instances: md.Spec.Instances, ReadyInstances: Unknown(reasonNotReported), Providers: []string{},
	}
	if md.Status.LastReconcileTime != nil {
		row.ReadyInstances = Count(md.Status.ReadyInstances)
	}
	if md.Spec.EndTime != nil {
		row.EndTime = rfc3339(md.Spec.EndTime.Time)
	}
	for name := range uses {
		row.Providers = append(row.Providers, name)
	}
	slices.Sort(row.Providers)
	return row
}

func instanceRow(in *v1alpha1.Instance) InstanceRow {
	row := InstanceRow{
		Namespace: in.Namespace, Name: in.Name, Owner: owner(in.Annotations), Purpose: purpose(in.Annotations),
		Phase: Unknown(reasonNoPhase), Allocation: in.Spec.AllocationRef.Name,
	}
	if in.Status.Phase != "" {
		row.Phase = Text(string(in.Status.Phase))
	}
	return row
}

func providerRow(p *v1alpha1.Provider, now time.Time) ProviderRow {
	row := ProviderRow{Name: p.Name, LastSuccessfulStatusRead: Unknown(reasonNeverRead), Age: Unknown(reasonNeverRead)}
	read := p.Status.LastSuccessfulStatusRead
	if read == nil {
		return row
	}
	row.LastSuccessfulStatusRead = Instant(read.Time)
	if age := now.Sub(timeOf(read)); age < 0 {
		row.Age = Unknown(reasonReadInFuture)
	} else {
		row.Age = Span(age.Truncate(time.Second))
	}
	return row
}

func timeOf(t *metav1.Time) time.Time { return t.Time }

func operationRow(op *v1alpha1.Operation) OperationRow {
	row := OperationRow{
		Namespace: op.Namespace, Name: op.Name, ModelDeployment: op.Spec.ModelDeploymentRef.Name,
		Action: string(op.Spec.Action), PlanID: Unknown(reasonNoPlan), Owner: op.Spec.Owner,
	}
	if op.Status.Plan != nil && op.Status.Plan.ID != "" {
		row.PlanID = Text(op.Status.Plan.ID)
	}
	return row
}
