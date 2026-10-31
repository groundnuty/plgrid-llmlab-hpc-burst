package lifecycle

import (
	"context"
	"fmt"
	"sort"
	"strings"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// degradedOrder ranks the Degraded reasons: the condition carries the first
// that applies, and its message lists every one (HBEP-7, Events and
// conditions).
var degradedOrder = []string{
	v1alpha1.ReasonSpecUnsupported, v1alpha1.ReasonFrontNotConfigured, v1alpha1.ReasonVerificationFailed,
	v1alpha1.ReasonIdentityUnresolved, v1alpha1.ReasonRestartLoop, v1alpha1.ReasonSubmissionRejected,
	v1alpha1.ReasonProviderUnavailable, v1alpha1.ReasonReplacementLate, v1alpha1.ReasonCapacityDropped,
}

// writeStatus writes a served ModelDeployment's status (HBEP-7, Reconcile
// loop, step 10): the counts, the current IDs, every ID that still has
// Instances, each class's start-to-ready time, the retention mode, the
// conditions and the generation reconciled with its time.
func (p *pass) writeStatus() error {
	md := p.md
	next := md.DeepCopy()
	s := &next.Status
	now := metav1.NewTime(p.now)
	desired := md.Spec.Instances
	if p.shutdown {
		desired = 0
	}
	var live, ready int32
	active := map[v1alpha1.FullDeploymentID]*v1alpha1.ActiveDeploymentID{}
	everReady := len(md.Status.StartToReady) > 0
	for _, in := range p.insts {
		if in.Status.ReadyTime != nil {
			everReady = true
		}
		if ended(in) {
			continue
		}
		live++
		if in.Status.Phase == v1alpha1.InstanceReady && in.DeletionTimestamp.IsZero() {
			ready++
		}
		a, ok := active[in.Spec.DeploymentID]
		if !ok {
			a = &v1alpha1.ActiveDeploymentID{ID: in.Spec.DeploymentID}
			if pv := p.pvOf(in.Labels); pv != nil && pv.hasClass {
				c := pv.class
				a.AcceleratorClass = &c
			}
			active[in.Spec.DeploymentID] = a
		}
		a.Instances++
	}
	p.submissionRejected()
	if everReady && ready < desired {
		p.degrade(v1alpha1.ReasonCapacityDropped, "%d of %d desired Instances are ready", ready, desired)
	}
	s.Instances, s.ReadyInstances, s.DesiredInstances = live, ready, desired
	s.CurrentIDs = nil
	for _, c := range p.classList {
		if cid := p.ids[c]; cid != nil && cid.id != "" {
			cc := c
			s.CurrentIDs = append(s.CurrentIDs, v1alpha1.ClassDeploymentID{AcceleratorClass: &cc, ID: cid.id})
		}
	}
	s.ActiveIDs = nil
	for _, a := range active {
		s.ActiveIDs = append(s.ActiveIDs, *a)
	}
	sort.Slice(s.ActiveIDs, func(i, j int) bool { return s.ActiveIDs[i].ID < s.ActiveIDs[j].ID })
	for _, m := range p.measuredStartToReady() {
		replaced := false
		for i := range s.StartToReady {
			if s.StartToReady[i].AcceleratorClass == m.AcceleratorClass {
				if m.MeasuredTime.After(s.StartToReady[i].MeasuredTime.Time) {
					s.StartToReady[i] = m
				}
				replaced = true
			}
		}
		if !replaced {
			s.StartToReady = append(s.StartToReady, m)
		}
	}
	s.Retention = &v1alpha1.RetentionStatus{EffectiveMode: md.Spec.RetentionPolicy.EffectiveMode()}
	gen := md.Generation
	if ready >= desired {
		cond(&s.Conditions, v1alpha1.ConditionReady, metav1.ConditionTrue, v1alpha1.ReasonEnoughReady, fmt.Sprintf("%d of %d desired Instances are ready", ready, desired), gen, now)
	} else {
		cond(&s.Conditions, v1alpha1.ConditionReady, metav1.ConditionFalse, v1alpha1.ReasonNotEnoughReady, fmt.Sprintf("%d of %d desired Instances are ready", ready, desired), gen, now)
	}
	if reason, msg := p.degradedSummary(); reason != "" {
		cond(&s.Conditions, v1alpha1.ConditionDegraded, metav1.ConditionTrue, reason, msg, gen, now)
	} else {
		cond(&s.Conditions, v1alpha1.ConditionDegraded, metav1.ConditionFalse, v1alpha1.ReasonAsDesired, "no degradation", gen, now)
	}
	stable := ready == desired && live == desired
	if stable {
		cond(&s.Conditions, v1alpha1.ConditionProgressing, metav1.ConditionFalse, v1alpha1.ReasonStable, "the desired Instances serve", gen, now)
	} else {
		cond(&s.Conditions, v1alpha1.ConditionProgressing, metav1.ConditionTrue, v1alpha1.ReasonProvisioning, fmt.Sprintf("%d Instances live, %d ready, %d desired", live, ready, desired), gen, now)
	}
	s.ObservedGeneration, s.LastReconcileTime = gen, &now
	return p.updateMDStatus(next)
}

func (p *pass) updateMDStatus(next *v1alpha1.ModelDeployment) error {
	if err := p.r.Client.Status().Update(p.ctx, next, owner); err != nil {
		return fmt.Errorf("update ModelDeployment %s status: %w", next.Name, err)
	}
	*p.md = *next
	return nil
}

// measuredStartToReady derives each class's newest start-to-ready
// measurement from persisted fields: the first Instance created on an
// Allocation, its ReadyTime, minus its Allocation's start (HBEP-7, Handover).
// It is read again at every pass, so a measurement whose status write was lost
// (a crash, a conflict) is written by a later pass while the Allocation lives.
func (p *pass) measuredStartToReady() []v1alpha1.ClassStartToReady {
	newest := map[v1alpha1.AcceleratorClass]v1alpha1.ClassStartToReady{}
	for _, in := range p.insts {
		a := p.allocs[in.Spec.AllocationRef.Name]
		pv := p.pvOf(in.Labels)
		if in.Status.ReadyTime == nil || a == nil || a.Status.AtProvider.StartTime == nil || pv == nil || !pv.hasClass || !p.firstOnAllocation(in) {
			continue
		}
		d := in.Status.ReadyTime.Sub(a.Status.AtProvider.StartTime.Time)
		if d < 0 {
			continue
		}
		if cur, ok := newest[pv.class]; ok && !in.Status.ReadyTime.After(cur.MeasuredTime.Time) {
			continue
		}
		newest[pv.class] = v1alpha1.ClassStartToReady{
			AcceleratorClass: pv.class, Duration: v1alpha1.Duration(d.String()), Instance: in.Name, MeasuredTime: *in.Status.ReadyTime,
		}
	}
	out := make([]v1alpha1.ClassStartToReady, 0, len(newest))
	for _, m := range newest {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].MeasuredTime.Before(&out[j].MeasuredTime) })
	return out
}

// submissionRejected shows a Provider's Rejected attempt as SubmissionRejected,
// with the scheduler's reason. The Provider's controller retries with backoff,
// and the lifecycle controller does not switch providers in alpha (HBEP-7,
// Provisioning).
func (p *pass) submissionRejected() {
	for _, a := range p.allocs {
		sub := a.Status.AtProvider.Submission
		if a.DeletionTimestamp.IsZero() && sub != nil && sub.State == v1alpha1.SubmissionRejected && a.Annotations[v1alpha1.AnnotationJobID] == "" {
			p.degrade(v1alpha1.ReasonSubmissionRejected, "Allocation %s: attempt %d was rejected: %s", a.Name, sub.Attempt, sub.Reason)
		}
	}
}

// degradedSummary returns the first Degraded reason in degradedOrder and a
// message naming every reason that applies, each once.
func (p *pass) degradedSummary() (string, string) {
	if len(p.degraded) == 0 {
		return "", ""
	}
	byReason := map[string][]string{}
	for _, d := range p.degraded {
		if !contains(byReason[d.reason], d.message) {
			byReason[d.reason] = append(byReason[d.reason], d.message)
		}
	}
	var first string
	var parts []string
	for _, r := range degradedOrder {
		msgs, ok := byReason[r]
		if !ok {
			continue
		}
		if first == "" {
			first = r
		}
		parts = append(parts, r+": "+strings.Join(msgs, "; "))
	}
	return first, strings.Join(parts, " | ")
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

// writeManagedStatus writes a managed-source ModelDeployment's status (HBEP-7,
// Reconcile loop, step 1): it has no Instances by design, so desiredInstances
// and readyInstances are 0 and Progressing is false with ManagedSource; Ready
// follows the catalog entry: true while an allowed managed Provider on the
// entry's platform lists the catalog name now and its CredentialsRejected is
// not true.
func (p *pass) writeManagedStatus() error {
	md := p.md
	m := md.Spec.Source.Managed
	next := md.DeepCopy()
	s := &next.Status
	now := metav1.NewTime(p.now)
	gen := md.Generation
	status, reason, msg, err := p.r.catalogReady(p.ctx, md)
	if err != nil {
		return err
	}
	cond(&s.Conditions, v1alpha1.ConditionReady, status, reason, msg, gen, now)
	cond(&s.Conditions, v1alpha1.ConditionProgressing, metav1.ConditionFalse, v1alpha1.ReasonManagedSource,
		fmt.Sprintf("catalog entry %s on platform %s: a managed source has no Instances by design", m.CatalogName, m.Platform), gen, now)
	apimeta.RemoveStatusCondition(&s.Conditions, v1alpha1.ConditionDegraded)
	s.Instances, s.ReadyInstances, s.DesiredInstances = 0, 0, 0
	s.CurrentIDs, s.ActiveIDs = nil, nil
	s.Retention = &v1alpha1.RetentionStatus{EffectiveMode: md.Spec.RetentionPolicy.EffectiveMode()}
	s.ObservedGeneration, s.LastReconcileTime = gen, &now
	return p.updateMDStatus(next)
}

// catalogReady decides a managed source's Ready condition. A served
// deployment's rule is the reverse: it ignores managed Providers.
func (r *Reconciler) catalogReady(ctx context.Context, md *v1alpha1.ModelDeployment) (metav1.ConditionStatus, string, string, error) {
	m := md.Spec.Source.Managed
	var all v1alpha1.ProviderList
	if err := r.Client.List(ctx, &all); err != nil {
		return "", "", "", fmt.Errorf("list Providers: %w", err)
	}
	names, err := selectedProviderNames(md.Spec.Providers, all.Items)
	if err != nil {
		return metav1.ConditionFalse, v1alpha1.ReasonProviderNotOnPlatform, err.Error(), nil
	}
	byName := map[string]*v1alpha1.Provider{}
	for i := range all.Items {
		byName[all.Items[i].Name] = &all.Items[i]
	}
	var onPlatform []*v1alpha1.Provider
	for _, n := range names {
		pr, ok := byName[n]
		if !ok || pr.Labels[v1alpha1.LabelPlatform] != m.Platform {
			continue
		}
		var prof v1alpha1.PlatformProfile
		if err := r.Client.Get(ctx, client.ObjectKey{Name: pr.Spec.PlatformProfileRef.Name}, &prof); err != nil || prof.Spec.ProviderKind != v1alpha1.PlatformManaged {
			continue
		}
		onPlatform = append(onPlatform, pr)
	}
	if len(onPlatform) == 0 {
		return metav1.ConditionFalse, v1alpha1.ReasonProviderNotOnPlatform,
			fmt.Sprintf("no allowed managed Provider carries platform %q (label %s)", m.Platform, v1alpha1.LabelPlatform), nil
	}
	rejected := ""
	for _, pr := range onPlatform {
		for _, e := range pr.Status.Catalog {
			if e.Name != m.CatalogName || !e.Listed {
				continue
			}
			if apimeta.IsStatusConditionTrue(pr.Status.Conditions, v1alpha1.ConditionCredentialsRejected) {
				rejected = pr.Name
				continue
			}
			return metav1.ConditionTrue, reasonCatalogEntryListed, fmt.Sprintf("Provider %s lists %s", pr.Name, m.CatalogName), nil
		}
	}
	if rejected != "" {
		return metav1.ConditionFalse, v1alpha1.ReasonCredentialsRejected, fmt.Sprintf("Provider %s lists %s, but its key is rejected", rejected, m.CatalogName), nil
	}
	return metav1.ConditionFalse, v1alpha1.ReasonCatalogEntryUnlisted, fmt.Sprintf("no allowed managed Provider on platform %s lists %s now", m.Platform, m.CatalogName), nil
}

var _ = labels.Everything
