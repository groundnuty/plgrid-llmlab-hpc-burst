package lifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// Desired returns what the caller's Allocation should run now (HBEP-7, Desired
// state, pulled). It is computed from API objects alone (and the key and token
// files), so a restarted controller serves the same state with the same
// Version:
//   - stop, once the Allocation is gone or being deleted;
//   - run, for the Instance on the Allocation whose registration names the
//     caller, from its start instruction until it ends; with checks while it
//     is starting or verifying;
//   - idle otherwise: an ended Instance, or none yet.
func (b *Backend) Desired(ctx context.Context, c agentapi.Caller) (agentapi.DesiredState, error) {
	if err := b.serving(); err != nil {
		return agentapi.DesiredState{}, err
	}
	if !b.registered(c) {
		return agentapi.DesiredState{}, agentapi.Errorf(agentapi.CodeNotRegistered, "agent instance %s is not registered", c.AgentInstanceID)
	}
	var a v1alpha1.Allocation
	err := b.Reader.Get(ctx, client.ObjectKey{Namespace: c.Namespace, Name: c.Allocation}, &a)
	if apierrors.IsNotFound(err) {
		return agentapi.DesiredState{Action: agentapi.ActionStop, Settings: defaultAgentSettings()}.WithVersion(), nil
	}
	if err != nil {
		return agentapi.DesiredState{}, agentapi.Errorf(agentapi.CodeUnavailable, "read Allocation %s: %v", c.Allocation, err)
	}
	if string(a.UID) != c.AllocationUID || !a.DeletionTimestamp.IsZero() {
		return agentapi.DesiredState{Action: agentapi.ActionStop, Settings: defaultAgentSettings()}.WithVersion(), nil
	}
	settings, err := b.agentSettings(ctx, &a)
	if err != nil {
		return agentapi.DesiredState{}, err
	}
	in, err := b.runningOn(ctx, &a, c.AgentInstanceID)
	if err != nil {
		return agentapi.DesiredState{}, err
	}
	if in == nil {
		return agentapi.DesiredState{Action: agentapi.ActionIdle, Settings: settings}.WithVersion(), nil
	}
	di, err := b.desiredInstance(ctx, in)
	if err != nil {
		return agentapi.DesiredState{}, err
	}
	return agentapi.DesiredState{Action: agentapi.ActionRun, Instance: di, Settings: settings}.WithVersion(), nil
}

func defaultAgentSettings() agentapi.AgentSettings {
	return agentapi.AgentSettings{
		ProbeInterval: agentapi.Duration(defaultProbeInterval), StopGrace: agentapi.Duration(defaultStopGrace),
		ImageFormat: agentapi.ImageFormatSIF,
	}
}

// agentSettings reads the profile values the agent applies: the probe interval,
// the stop grace and the partition's image format.
func (b *Backend) agentSettings(ctx context.Context, a *v1alpha1.Allocation) (agentapi.AgentSettings, error) {
	out := defaultAgentSettings()
	if a.Spec.ProviderConfigReference == nil {
		return out, nil
	}
	var prov v1alpha1.Provider
	if ok, err := errNotFoundIsNil(b.Reader.Get(ctx, client.ObjectKey{Name: a.Spec.ProviderConfigReference.Name}, &prov)); err != nil {
		return out, agentapi.Errorf(agentapi.CodeUnavailable, "read Provider: %v", err)
	} else if !ok {
		return out, nil
	}
	var prof v1alpha1.PlatformProfile
	if ok, err := errNotFoundIsNil(b.Reader.Get(ctx, client.ObjectKey{Name: prov.Spec.PlatformProfileRef.Name}, &prof)); err != nil {
		return out, agentapi.Errorf(agentapi.CodeUnavailable, "read PlatformProfile: %v", err)
	} else if !ok {
		return out, nil
	}
	if s, err := resolveSettings(prof.Spec); err == nil {
		out.ProbeInterval, out.StopGrace = agentapi.Duration(s.probeInterval), agentapi.Duration(s.stopGrace)
	}
	if a.Spec.ForProvider.Batch != nil {
		for _, part := range prof.Spec.Partitions {
			if part.Name == a.Spec.ForProvider.Batch.Partition {
				out.ImageFormat = agentapi.ImageFormat(v1alpha1.ImageFormatOf(part))
			}
		}
	}
	return out, nil
}

// runningOn returns the Instance the caller should run: on the Allocation,
// started under the caller's registration, and not ended.
func (b *Backend) runningOn(ctx context.Context, a *v1alpha1.Allocation, agentInstanceID string) (*v1alpha1.Instance, error) {
	var list v1alpha1.InstanceList
	if err := b.Reader.List(ctx, &list, client.InNamespace(a.Namespace),
		client.MatchingLabels{v1alpha1.LabelModelDeployment: a.Labels[v1alpha1.LabelModelDeployment]}); err != nil {
		return nil, agentapi.Errorf(agentapi.CodeUnavailable, "list Instances: %v", err)
	}
	for i := range list.Items {
		in := &list.Items[i]
		if in.Spec.AllocationRef.Name != a.Name || ended(in) || in.Status.Agent == nil || in.Status.StartInstructionTime == nil {
			continue
		}
		if in.Status.Agent.AgentInstanceID == agentInstanceID {
			return in, nil
		}
	}
	return nil, nil
}

// desiredInstance renders the Instance's desired state: its launch, its
// restart generations, its one worker, the checks it needs now, the Hub token
// and whether its key is stored.
func (b *Backend) desiredInstance(ctx context.Context, in *v1alpha1.Instance) (*agentapi.DesiredInstance, error) {
	worker := v1alpha1.WorkerName(0)
	di := &agentapi.DesiredInstance{
		Name: in.Name, UID: string(in.UID), DeploymentID: string(in.Spec.DeploymentID),
		Launch:     wireLaunch(in.Spec.Launch),
		Generation: restartGeneration(in, v1alpha1.PartInstance),
		Workers: []agentapi.DesiredWorker{{
			Name: worker, Generation: restartGeneration(in, worker), EngineSocket: agentapi.ContainerEngineSocket(worker),
		}},
	}
	if _, err := os.Stat(agentapi.InstanceKeyPath(b.CredentialsDir, string(in.UID))); err == nil {
		di.KeyStored = true
	}
	md, err := b.modelDeployment(ctx, in)
	if err != nil {
		return nil, err
	}
	if md != nil && md.Spec.Source.Served != nil && md.Spec.Source.Served.Checkpoint.HubTokenRef != nil {
		tok, err := readTokenFile(md.Spec.Source.Served.Checkpoint.HubTokenRef.File)
		if err != nil {
			return nil, agentapi.Errorf(agentapi.CodeUnavailable, "Hub token of %s: %v", md.Name, err)
		}
		di.HubToken = tok
	}
	switch in.Status.Phase {
	case v1alpha1.InstanceStarting, v1alpha1.InstanceVerifying:
		checks, err := b.checks(ctx, in, md)
		if err != nil {
			return nil, err
		}
		di.Checks = checks
	}
	return di, nil
}

// checks asks for what the Instance's current verification generation still
// needs (HBEP-7, Instance start): verification first, with the identity
// record's files unless this is a worker restart, which compares settings
// only; then qualification (not after a worker restart) and warm-up, with the
// shapes the verification record fixed.
func (b *Backend) checks(ctx context.Context, in *v1alpha1.Instance, md *v1alpha1.ModelDeployment) (*agentapi.Checks, error) {
	gen := verificationGeneration(in)
	c := &agentapi.Checks{VerificationGeneration: gen}
	qualified := isTrue(in.Status.Conditions, v1alpha1.ConditionQualified)
	if !isTrue(in.Status.Conditions, v1alpha1.ConditionVerified) {
		req := &agentapi.VerifyRequest{}
		if !qualified {
			var rec appsv1.ControllerRevision
			if err := b.Reader.Get(ctx, client.ObjectKey{Namespace: in.Namespace, Name: in.Spec.IdentityRecord}, &rec); err != nil {
				return nil, agentapi.Errorf(agentapi.CodeUnavailable, "read identity record %s: %v", in.Spec.IdentityRecord, err)
			}
			files, err := recordFiles(&rec)
			if err != nil {
				return nil, err
			}
			for _, f := range files {
				req.Files = append(req.Files, agentapi.FileDigest{Path: f.Path, SHA256: f.SHA256})
			}
		}
		c.Verify = req
		return c, nil
	}
	if md == nil || md.Spec.Workload == nil {
		return nil, nil
	}
	var rec appsv1.ControllerRevision
	if err := b.Reader.Get(ctx, client.ObjectKey{Namespace: in.Namespace, Name: in.Status.VerificationRecord}, &rec); err != nil {
		return nil, agentapi.Errorf(agentapi.CodeUnavailable, "read verification record %s: %v", in.Status.VerificationRecord, err)
	}
	var d verificationData
	if err := json.Unmarshal(rec.Data.Raw, &d); err != nil || d.Shapes == nil {
		return nil, fmt.Errorf("verification record %s holds no resolved shapes", rec.Name)
	}
	sh := resolveShapes(md)
	if !qualified {
		c.Qualify = &agentapi.QualifyRequest{Shapes: *d.Shapes, ToolCalls: sh.toolCalls}
	}
	if !isTrue(in.Status.Conditions, v1alpha1.ConditionWarmedUp) {
		c.WarmUp = &agentapi.WarmUpRequest{Shapes: *d.Shapes, ToolCalls: sh.toolCalls, TargetSessions: sh.targetSessions}
	}
	if c.Qualify == nil && c.WarmUp == nil {
		return nil, nil
	}
	return c, nil
}

func (b *Backend) modelDeployment(ctx context.Context, in *v1alpha1.Instance) (*v1alpha1.ModelDeployment, error) {
	for _, o := range in.OwnerReferences {
		if o.Kind != "ModelDeployment" {
			continue
		}
		var md v1alpha1.ModelDeployment
		err := b.Reader.Get(ctx, client.ObjectKey{Namespace: in.Namespace, Name: o.Name}, &md)
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		if err != nil {
			return nil, agentapi.Errorf(agentapi.CodeUnavailable, "read ModelDeployment %s: %v", o.Name, err)
		}
		return &md, nil
	}
	return nil, nil
}

func wireLaunch(l v1alpha1.Launch) agentapi.Launch {
	out := agentapi.Launch{
		Image: l.Image, ImageDigest: l.ImageDigest, Command: append([]string(nil), l.Command...),
		KeyEnv: l.KeyEnv, Checkpoint: agentapi.Checkpoint{Repository: l.Checkpoint.Repository, Revision: l.Checkpoint.Revision},
		GPUs: l.GPUs, MaxNumSeqs: l.MaxNumSeqs,
	}
	for _, e := range l.Env {
		out.Env = append(out.Env, agentapi.EnvVar{Name: e.Name, Value: e.Value})
	}
	if l.ChatTemplate != nil {
		t := *l.ChatTemplate
		out.ChatTemplate = &t
	}
	return out
}

// readTokenFile reads a token from a regular file that only its owner can
// read (mode 0600 or 0400), trimmed of surrounding white space. The content
// never reaches an error.
func readTokenFile(path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("the token reference names no file")
	}
	fi, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if !fi.Mode().IsRegular() || fi.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf("%s must be a regular file readable by its owner only (mode 0600 or 0400)", path)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	tok := strings.TrimSpace(string(b))
	if tok == "" {
		return "", fmt.Errorf("%s is empty", path)
	}
	return tok, nil
}

// setDialHome sets the Provider's DialHomeUnreachable condition by server-side
// apply under the lifecycle controller's field manager, owning only that
// condition (docs/development.md, Single writer). It writes True when an
// Instance could not register, and False when an agent registers while the
// condition is True; otherwise nothing.
func (r *Reconciler) setDialHome(ctx context.Context, provider string, unreachable bool, now time.Time, msg string) error {
	if provider == "" {
		return nil
	}
	var p v1alpha1.Provider
	if ok, err := errNotFoundIsNil(r.Client.Get(ctx, client.ObjectKey{Name: provider}, &p)); err != nil || !ok {
		return err
	}
	var cur *metav1.Condition
	for i := range p.Status.Conditions {
		if p.Status.Conditions[i].Type == v1alpha1.ConditionDialHomeUnreachable {
			cur = &p.Status.Conditions[i]
		}
	}
	status, reason := metav1.ConditionTrue, v1alpha1.ReasonRegistrationTimeout
	if !unreachable {
		if cur == nil || cur.Status != metav1.ConditionTrue {
			return nil
		}
		status, reason = metav1.ConditionFalse, reasonAgentRegistered
	}
	c := metav1.Condition{Type: v1alpha1.ConditionDialHomeUnreachable, Status: status, Reason: reason, Message: msg, LastTransitionTime: metav1.NewTime(now)}
	if cur != nil && cur.Status == status {
		c.LastTransitionTime = cur.LastTransitionTime
	}
	u, err := v1alpha1.DialHomeUnreachableApply(provider, c)
	if err != nil {
		return err
	}
	if err := r.Client.Status().Apply(ctx, client.ApplyConfigurationFromUnstructured(u), client.FieldOwner(v1alpha1.FieldManagerLifecycle), client.ForceOwnership); err != nil {
		return fmt.Errorf("apply DialHomeUnreachable to Provider %s: %w", provider, err)
	}
	return nil
}
