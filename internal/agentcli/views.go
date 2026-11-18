package agentcli

import (
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
)

// The CLI's own output structures (docs/agent-cli.md). Both the text and the --json output are made from
// them, so the two say the same, and neither exposes a Kubernetes object. schemaVersion is raised by a
// change that breaks a field.
const schemaVersion = 1

type operationView struct {
	Name            string          `json:"name"`
	Namespace       string          `json:"namespace"`
	Action          string          `json:"action"`
	ModelDeployment string          `json:"modelDeployment"`
	Scale           *scaleView      `json:"scale,omitempty"`
	End             *endView        `json:"end,omitempty"`
	Release         *releaseView    `json:"release,omitempty"`
	Owner           string          `json:"owner"`
	Purpose         string          `json:"purpose,omitempty"`
	ConfirmedPlan   string          `json:"confirmedPlan,omitempty"`
	Phase           string          `json:"phase"`
	Conditions      []conditionView `json:"conditions,omitempty"`
	Plan            *planView       `json:"plan,omitempty"`
	Targets         []outcomeView   `json:"targets,omitempty"`
	FinishedAt      string          `json:"finishedAt,omitempty"`
	ConfirmWith     string          `json:"confirmWith,omitempty"`
}

type scaleView struct {
	Instances int32 `json:"instances"`
}

type endView struct {
	Delete bool `json:"delete"`
}

type releaseView struct {
	Allocations []string `json:"allocations,omitempty"`
	Selector    string   `json:"selector,omitempty"`
}

type conditionView struct {
	Type               string `json:"type"`
	Status             string `json:"status"`
	Reason             string `json:"reason,omitempty"`
	Message            string `json:"message,omitempty"`
	LastTransitionTime string `json:"lastTransitionTime,omitempty"`
}

type outcomeView struct {
	Kind               string `json:"kind"`
	Name               string `json:"name"`
	UID                string `json:"uid"`
	State              string `json:"state"`
	Reason             string `json:"reason,omitempty"`
	Message            string `json:"message,omitempty"`
	LastTransitionTime string `json:"lastTransitionTime,omitempty"`
}

type planView struct {
	ID              string       `json:"id"`
	ObservedAt      string       `json:"observedAt"`
	ModelDeployment mdView       `json:"modelDeployment"`
	Targets         []targetView `json:"targets,omitempty"`
}

type mdView struct {
	Name       string `json:"name"`
	UID        string `json:"uid"`
	Generation int64  `json:"generation"`
}

type targetView struct {
	Kind          string        `json:"kind"`
	Name          string        `json:"name"`
	UID           string        `json:"uid"`
	Provider      string        `json:"provider,omitempty"`
	Effect        string        `json:"effect"`
	NeedsApproval bool          `json:"needsApproval"`
	ApprovalRule  string        `json:"approvalRule,omitempty"`
	Policies      []policyView  `json:"policies,omitempty"`
	Cost          *costView     `json:"cost,omitempty"`
	Estimate      *estimateView `json:"estimate,omitempty"`
}

type policyView struct {
	Name      string         `json:"name"`
	Approvers []approverView `json:"approvers,omitempty"`
}

type approverView struct {
	User           string          `json:"user,omitempty"`
	Group          string          `json:"group,omitempty"`
	ServiceAccount *serviceAccount `json:"serviceAccount,omitempty"`
}

type serviceAccount struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// costView is a target's release cost. Every field is left out when it does not apply.
type costView struct {
	Note          string          `json:"note,omitempty"`
	LeaseLeft     *figureView     `json:"leaseLeft,omitempty"`
	GPUHoursLeft  *figureView     `json:"gpuHoursLeft,omitempty"`
	QueueAge      *figureView     `json:"queueAge,omitempty"`
	StartEstimate *figureView     `json:"startEstimate,omitempty"`
	LastReady     bool            `json:"lastReady,omitempty"`
	QueueWaits    *queueWaitsView `json:"queueWaits,omitempty"`
	StartToReady  *figureView     `json:"startToReady,omitempty"`
}

// figureView is one figure: a value, or the reason there is none, never a zero in place of one.
type figureView struct {
	Duration   string `json:"duration,omitempty"`
	Time       string `json:"time,omitempty"`
	GPUHours   string `json:"gpuHours,omitempty"`
	Unknown    string `json:"unknown,omitempty"`
	ObservedAt string `json:"observedAt,omitempty"`
	Advisory   bool   `json:"advisory,omitempty"`
}

type queueWaitsView struct {
	Median     string `json:"median,omitempty"`
	P90        string `json:"p90,omitempty"`
	Max        string `json:"max,omitempty"`
	Count      int32  `json:"count,omitempty"`
	Unknown    string `json:"unknown,omitempty"`
	ObservedAt string `json:"observedAt,omitempty"`
}

// estimateView is an estimate's state: {"state", "time"?, "unknown"?, "observedAt"?, "age"?, "advisory"}.
type estimateView struct {
	State      string `json:"state"`
	Time       string `json:"time,omitempty"`
	Unknown    string `json:"unknown,omitempty"`
	ObservedAt string `json:"observedAt,omitempty"`
	Age        string `json:"age,omitempty"`
	Advisory   bool   `json:"advisory"`
}

// stamp formats a time as RFC 3339 in UTC; the zero time is no time at all.
func stamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func stampMeta(t *metav1.Time) string {
	if t == nil {
		return ""
	}
	return stamp(t.Time)
}

// seconds is a duration as Go prints it, cut to the second.
func seconds(d time.Duration) string { return d.Truncate(time.Second).String() }

// operationViewOf is the view of an Operation at now. confirmWith is the command that confirms an
// unconfirmed Operation that has a plan; it names the namespace unless it is "default".
func operationViewOf(op *v1alpha1.Operation, now time.Time) operationView {
	v := operationView{
		Name: op.Name, Namespace: op.Namespace, Action: string(op.Spec.Action),
		ModelDeployment: op.Spec.ModelDeploymentRef.Name, Owner: op.Spec.Owner,
		Purpose: op.Annotations[v1alpha1.AnnotationPurpose], Phase: phaseOf(op),
		FinishedAt: stampMeta(op.Status.FinishedAt),
	}
	if sc := op.Spec.Scale; sc != nil {
		v.Scale = &scaleView{Instances: sc.Instances}
	}
	if op.Spec.Action == v1alpha1.OperationActionEnd {
		v.End = &endView{Delete: op.Spec.End != nil && op.Spec.End.Delete}
	}
	if r := op.Spec.Release; r != nil {
		v.Release = &releaseView{Allocations: r.Allocations}
		if r.Selector != nil {
			v.Release.Selector = metav1.FormatLabelSelector(r.Selector)
		}
	}
	if op.Spec.Confirm != nil {
		v.ConfirmedPlan = op.Spec.Confirm.PlanID
	}
	for _, c := range op.Status.Conditions {
		v.Conditions = append(v.Conditions, conditionView{Type: c.Type, Status: string(c.Status), Reason: c.Reason,
			Message: c.Message, LastTransitionTime: stamp(c.LastTransitionTime.Time)})
	}
	if p := op.Status.Plan; p != nil {
		pv := planViewOf(p, estimateEntries(op), now)
		v.Plan = &pv
		if op.Spec.Confirm == nil {
			v.ConfirmWith = confirmCommand(op.Name, p.ID, op.Namespace)
		}
	}
	for _, t := range op.Status.Targets {
		v.Targets = append(v.Targets, outcomeView{Kind: string(t.Kind), Name: t.Name, UID: string(t.UID), State: string(t.State),
			Reason: t.Reason, Message: t.Message, LastTransitionTime: stamp(t.LastTransitionTime.Time)})
	}
	return v
}

// phaseOf is an Operation's phase; one the controller has not written yet is Planning.
func phaseOf(op *v1alpha1.Operation) string {
	if op.Status.Phase == "" {
		return string(v1alpha1.OperationPhasePlanning)
	}
	return string(op.Status.Phase)
}

// confirmCommand is the command that confirms a plan.
func confirmCommand(name, planID, namespace string) string {
	return "hpcburst confirm " + name + " --plan " + planID + inNamespace(namespace)
}

// inNamespace is the flag that names a namespace other than "default".
func inNamespace(namespace string) string {
	if namespace == "" || namespace == "default" {
		return ""
	}
	return " -n " + namespace
}

func planViewOf(p *v1alpha1.OperationPlan, entries []estimateEntry, now time.Time) planView {
	pv := planView{ID: p.ID, ObservedAt: stamp(p.ObservedAt.Time),
		ModelDeployment: mdView{Name: p.ModelDeployment.Name, UID: string(p.ModelDeployment.UID), Generation: p.ModelDeployment.Generation}}
	for _, t := range p.Targets {
		tv := targetView{Kind: string(t.Kind), Name: t.Name, UID: string(t.UID), Provider: t.Provider, Effect: string(t.Effect),
			NeedsApproval: t.NeedsApproval, ApprovalRule: t.ApprovalRule, Cost: costViewOf(t.Cost)}
		for _, pol := range t.Policies {
			tv.Policies = append(tv.Policies, policyView{Name: pol.Name, Approvers: approverViews(pol.Approvers)})
		}
		if hasEstimate(t) {
			e := estimateViewOf(deriveEstimate(p, t, entries, now))
			tv.Estimate = &e
		}
		pv.Targets = append(pv.Targets, tv)
	}
	return pv
}

func approverViews(as []v1alpha1.Approver) []approverView {
	var out []approverView
	for _, a := range as {
		v := approverView{User: a.User, Group: a.Group}
		if a.ServiceAccount != nil {
			v.ServiceAccount = &serviceAccount{Namespace: a.ServiceAccount.Namespace, Name: a.ServiceAccount.Name}
		}
		out = append(out, v)
	}
	return out
}

func estimateViewOf(e estimate) estimateView {
	v := estimateView{State: string(e.Kind), Advisory: true}
	switch e.Kind {
	case estimateKnown:
		v.Time, v.ObservedAt, v.Age = stamp(e.Time), stamp(e.ObservedAt), seconds(e.Age)
	case estimateUnknown:
		v.Unknown, v.ObservedAt = e.Unknown, stamp(e.ObservedAt)
	}
	return v
}

// costViewOf is the cost of one target as the printer shows it. The plan's own estimateNow is left out:
// it stays unknown in the plan, and the target's estimate replaces it.
func costViewOf(c *v1alpha1.ReleaseCost) *costView {
	if c == nil {
		return nil
	}
	return &costView{
		Note: c.Note, LeaseLeft: durationFigure(c.LeaseLeft), GPUHoursLeft: gpuHoursFigure(c.GPUHoursLeft),
		QueueAge: durationFigure(c.QueueAge), StartEstimate: timeFigure(c.StartEstimate), LastReady: c.LastReady,
		QueueWaits: queueWaitsViewOf(c.QueueWaits), StartToReady: durationFigure(c.StartToReady),
	}
}

// noValue is the reason of a figure that carries neither a value nor a reason: a figure is never shown as
// a zero.
const noValue = "the plan records neither a value nor a reason"

func durationFigure(f *v1alpha1.DurationFigure) *figureView {
	if f == nil {
		return nil
	}
	v := &figureView{ObservedAt: stampMeta(f.ObservedAt), Advisory: f.Advisory}
	switch {
	case f.Unknown != "":
		v.Unknown = f.Unknown
	case f.Duration != nil:
		v.Duration = f.Duration.Duration.String()
	default:
		v.Unknown = noValue
	}
	return v
}

func timeFigure(f *v1alpha1.TimeFigure) *figureView {
	if f == nil {
		return nil
	}
	v := &figureView{ObservedAt: stampMeta(f.ObservedAt), Advisory: f.Advisory}
	switch {
	case f.Unknown != "":
		v.Unknown = f.Unknown
	case f.Time != nil:
		v.Time = stampMeta(f.Time)
	default:
		v.Unknown = noValue
	}
	return v
}

func gpuHoursFigure(f *v1alpha1.GPUHoursFigure) *figureView {
	if f == nil {
		return nil
	}
	v := &figureView{ObservedAt: stampMeta(f.ObservedAt)}
	switch {
	case f.Unknown != "":
		v.Unknown = f.Unknown
	case f.GPUHours != "":
		v.GPUHours = f.GPUHours
	default:
		v.Unknown = noValue
	}
	return v
}

func queueWaitsViewOf(q *v1alpha1.QueueWaitsFigure) *queueWaitsView {
	if q == nil {
		return nil
	}
	v := &queueWaitsView{ObservedAt: stampMeta(q.ObservedAt)}
	if q.Unknown != "" || q.Median == nil || q.P90 == nil || q.Max == nil {
		v.Unknown = q.Unknown
		if v.Unknown == "" {
			v.Unknown = noValue
		}
		return v
	}
	v.Median, v.P90, v.Max, v.Count = q.Median.Duration.String(), q.P90.Duration.String(), q.Max.Duration.String(), q.Count
	return v
}

// approverText is "user x", "group g" or "serviceaccount ns/name".
func approverText(a approverView) string {
	switch {
	case a.User != "":
		return "user " + a.User
	case a.Group != "":
		return "group " + a.Group
	case a.ServiceAccount != nil:
		return "serviceaccount " + a.ServiceAccount.Namespace + "/" + a.ServiceAccount.Name
	}
	return "an approver the plan does not name"
}

func approversText(as []approverView) string {
	parts := make([]string, 0, len(as))
	for _, a := range as {
		parts = append(parts, approverText(a))
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, ", ")
}
