package lifecycle

import (
	"fmt"
	"os"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/agentapi"
)

// instances advances every Instance (HBEP-7, Reconcile loop, steps 5 and 6):
// failed Allocations end their Instances; each Instance moves through its
// phases; health rules and the drain deadline apply.
func (p *pass) instances() error {
	for _, in := range p.insts {
		if err := p.instance(in); err != nil {
			return err
		}
	}
	return nil
}

func (p *pass) instance(in *v1alpha1.Instance) error {
	alloc := p.allocs[in.Spec.AllocationRef.Name]
	if ended(in) {
		return p.tombstone(in, alloc)
	}
	if alloc == nil {
		if (in.Status.Phase == "" || in.Status.Phase == v1alpha1.InstancePending) && in.DeletionTimestamp.IsZero() && !p.shutdown && p.md != nil {
			// Created before its Allocation; the Allocation's create was cut
			// short (Provisioning): create it now, under the name the
			// Instance already holds.
			return p.ensureAllocation(in)
		}
		return p.end(in, nil, v1alpha1.ReasonAllocationLost, "its Allocation no longer exists", true)
	}
	if err := p.copyLeaseEnd(in, alloc); err != nil {
		return err
	}
	if done, err := p.allocationFailed(in, alloc); done || err != nil {
		return err
	}
	if !in.DeletionTimestamp.IsZero() || p.shutdown {
		return p.shutdownInstance(in, alloc)
	}
	switch in.Status.Phase {
	case "", v1alpha1.InstancePending:
		return p.pending(in, alloc)
	default:
		return p.running(in, alloc)
	}
}

// copyLeaseEnd copies the Allocation's hard end into status.leaseEnd at every
// observe; a later hard end moves it, and with it the drain start (HBEP-7,
// Controllers).
func (p *pass) copyLeaseEnd(in *v1alpha1.Instance, alloc *v1alpha1.Allocation) error {
	he := alloc.Status.AtProvider.HardEnd
	if he == nil || (in.Status.LeaseEnd != nil && in.Status.LeaseEnd.Equal(he)) {
		return nil
	}
	return p.setStatus(in, func(s *v1alpha1.InstanceStatus) { t := *he; s.LeaseEnd = &t })
}

// allocationFailed applies step 5: an Allocation that is Failed (a requeue
// included), Lost or Refused, or that was submitted as another, ends its
// Instance with that reason and is released; one that ended early ends its
// Instance with AllocationEndedEarly and a failing event. An Allocation that
// reached its hard end ends its Instance as drained, with no event.
func (p *pass) allocationFailed(in *v1alpha1.Instance, alloc *v1alpha1.Allocation) (bool, error) {
	obs := alloc.Status.AtProvider
	reason := ""
	switch obs.Phase {
	case v1alpha1.AllocationFailed:
		reason = v1alpha1.ReasonAllocationFailed
		if obs.Requeued || obs.Reason == v1alpha1.ReasonRequeued {
			reason = v1alpha1.ReasonRequeued
		}
	case v1alpha1.AllocationLost:
		reason = v1alpha1.ReasonAllocationLost
	case v1alpha1.AllocationRefused:
		reason = v1alpha1.ReasonAllocationRefused
	}
	if c := alloc.GetCondition(v1alpha1.ConditionSubmittedAsOther); reason == "" && c.Status == "True" {
		reason = string(v1alpha1.ConditionSubmittedAsOther)
	}
	if reason != "" {
		msg := fmt.Sprintf("Allocation %s is %s", alloc.Name, obs.Phase)
		if obs.Reason != "" {
			msg += ": " + obs.Reason
		}
		return true, p.fail(in, alloc, reason, msg)
	}
	if obs.Phase != v1alpha1.AllocationEnded {
		return false, nil
	}
	if obs.HardEnd != nil && obs.EndTime != nil && !obs.EndTime.Before(obs.HardEnd) {
		// A lease end is not a failure (HBEP-7, Release).
		return true, p.end(in, alloc, v1alpha1.ReasonDrained, fmt.Sprintf("Allocation %s reached its hard end", alloc.Name), true)
	}
	if obs.HardEnd != nil && !p.now.Before(obs.HardEnd.Time) && in.Status.Phase == v1alpha1.InstanceDraining {
		return true, p.end(in, alloc, v1alpha1.ReasonDrained, "the hard end passed while draining", true)
	}
	return true, p.fail(in, alloc, v1alpha1.ReasonAllocationEndedEarly, fmt.Sprintf("Allocation %s ended before its hard end", alloc.Name))
}

// fail ends a failing Instance with a failing event and releases its
// Allocation at once, since nothing on it serves (HBEP-7, Release).
func (p *pass) fail(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, reason, msg string) error {
	p.r.Recorder.Event(in, true, eventFailing, reason, msg)
	return p.end(in, alloc, reason, msg, true)
}

// end makes the Instance ended with a reason, tells its agent to idle (the
// desired state of an ended Instance), removes its drain finalizer and, when
// release is set and the Allocation exists, releases the Allocation.
func (p *pass) end(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, reason, msg string, release bool) error {
	now := metav1.NewTime(p.now)
	if err := p.setStatus(in, func(s *v1alpha1.InstanceStatus) {
		s.Phase, s.EndReason = v1alpha1.InstanceEnded, reason
		if s.EndTime == nil {
			s.EndTime = &now
		}
		if reason != v1alpha1.ReasonDrained {
			cond(&s.Conditions, v1alpha1.ConditionHealthy, metav1.ConditionFalse, reason, msg, in.Generation, now)
		}
	}); err != nil {
		return err
	}
	// The release, or the idle anchor, is made durable before the finalizer
	// goes: once it goes, the Instance may vanish (a garbage-collected one at
	// once), and with it what says the Allocation must go or since when it is
	// idle (HBEP-7, Release; Retention, the idle anchor).
	if release && alloc != nil {
		if err := p.deleteAllocation(alloc, reason); err != nil {
			return err
		}
	} else if err := p.anchorIfLast(alloc); err != nil {
		return err
	}
	return p.removeDrainFinalizer(in)
}

// releaseRequired reports whether an ended Instance's Allocation must be
// released: the Instance failed, so nothing on its Allocation serves (HBEP-7,
// Release). It is read from the persisted end reason, so a release that failed
// or was interrupted is made on a later pass.
func releaseRequired(in *v1alpha1.Instance) bool {
	return ended(in) && in.Status.EndReason != "" && in.Status.EndReason != v1alpha1.ReasonDrained
}

// anchorIfLast writes the Allocation's idle-since anchor when it hosts no live
// Instance and carries none yet: the latest EndTime of its Instances
// (v1alpha1.SetIdleSince).
func (p *pass) anchorIfLast(alloc *v1alpha1.Allocation) error {
	if alloc == nil || !alloc.DeletionTimestamp.IsZero() || len(p.liveOn(alloc.Name)) > 0 {
		return nil
	}
	if _, ok, err := v1alpha1.IdleSince(alloc); err == nil && ok {
		return nil
	}
	var latest time.Time
	for _, o := range p.insts {
		if o.Spec.AllocationRef.Name == alloc.Name && o.Status.EndTime != nil && o.Status.EndTime.After(latest) {
			latest = o.Status.EndTime.Time
		}
	}
	if latest.IsZero() {
		return nil
	}
	return p.writeIdleSince(alloc, &latest)
}

// tombstone handles an ended Instance: it stays while its Allocation exists, as
// the record of what ran there, and is deleted once the Allocation is gone.
func (p *pass) tombstone(in *v1alpha1.Instance, alloc *v1alpha1.Allocation) error {
	if alloc != nil && alloc.DeletionTimestamp.IsZero() && releaseRequired(in) && len(p.liveOn(alloc.Name)) == 0 {
		if err := p.deleteAllocation(alloc, "resuming the release of a failed Instance's Allocation"); err != nil {
			return err
		}
	} else if err := p.anchorIfLast(alloc); err != nil {
		return err
	}
	if err := p.removeDrainFinalizer(in); err != nil {
		return err
	}
	if alloc != nil || !in.DeletionTimestamp.IsZero() {
		return nil
	}
	// A failed Instance stays for restart.window after its end: its EndTime
	// dates the class's failure streak (HBEP-7, Restart and provisioning loops).
	if failedEnd(in) && p.md != nil {
		if keep := in.Status.EndTime.Add(p.settingsOf(p.pvOf(in.Labels)).restartWindow); p.now.Before(keep) {
			p.at(keep)
			return nil
		}
	}
	if err := p.r.Client.Delete(p.ctx, in); err != nil && !isNotFound(err) {
		return fmt.Errorf("delete ended Instance %s: %w", in.Name, err)
	}
	// The front never routes to an ended Instance, so its key goes with it.
	if err := os.Remove(agentapi.InstanceKeyPath(p.r.Config.CredentialsDir, string(in.UID))); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove the key of Instance %s: %w", in.Name, err)
	}
	return nil
}

// shutdownInstance applies Release's shutdown case to one Instance: a serving
// Instance drains, any other ends; pending Allocations are cancelled; a
// started Allocation then follows its retention policy (HBEP-7, Release).
func (p *pass) shutdownInstance(in *v1alpha1.Instance, alloc *v1alpha1.Allocation) error {
	switch in.Status.Phase {
	case v1alpha1.InstanceReady:
		return p.startDrain(in)
	case v1alpha1.InstanceDraining:
		return p.running(in, alloc)
	}
	release := alloc.Status.AtProvider.StartTime == nil
	return p.end(in, alloc, v1alpha1.ReasonDrained, "the deployment is shutting down", release)
}

// startDrain makes a ready Instance draining (HBEP-7, Handover, Draining).
func (p *pass) startDrain(in *v1alpha1.Instance) error {
	if in.Status.Phase == v1alpha1.InstanceDraining {
		return nil
	}
	if _, ok := drainBegan(in); !ok {
		if err := p.setMeta(in, func(n *v1alpha1.Instance) {
			if n.Annotations == nil {
				n.Annotations = map[string]string{}
			}
			n.Annotations[annotationDrainStarted] = p.now.UTC().Format(time.RFC3339)
		}); err != nil {
			return err
		}
	}
	if err := p.setStatus(in, func(s *v1alpha1.InstanceStatus) { s.Phase = v1alpha1.InstanceDraining }); err != nil {
		return err
	}
	p.at(p.now.Add(time.Second))
	return nil
}

// annotationDrainStarted holds when the controller made an Instance draining,
// in RFC 3339 UTC seconds. A draining Instance ends on a report that arrived
// after it, never on one taken before the drain began, when a request the
// front admitted since may still run (HBEP-7, Handover, Draining). It is a
// lifecycle-owned annotation, proposed for api/v1alpha1/constants.go.
const annotationDrainStarted = v1alpha1.Group + "/drain-started"

// drainBegan reads annotationDrainStarted.
func drainBegan(in *v1alpha1.Instance) (time.Time, bool) {
	v, ok := in.Annotations[annotationDrainStarted]
	if !ok {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// pending moves a pending Instance to starting once its Allocation runs and its
// agent has registered; it fails the Instance when no agent registers within
// health.registrationTimeout of the job's start (HBEP-7, The allocation agent).
func (p *pass) pending(in *v1alpha1.Instance, alloc *v1alpha1.Allocation) error {
	start := alloc.Status.AtProvider.StartTime
	if alloc.Status.AtProvider.Phase != v1alpha1.AllocationRunning || start == nil {
		return nil
	}
	regs := p.r.Backend.registrations(alloc.UID)
	pv := p.pvOf(in.Labels)
	st := p.settingsOf(pv)
	if len(regs) == 0 {
		deadline := start.Add(st.registrationTimeout)
		if guard := p.mem.started.Add(agentGrace); guard.After(deadline) {
			deadline = guard
		}
		if p.now.Before(deadline) {
			p.at(deadline)
			return nil
		}
		msg := fmt.Sprintf("no agent registered within %s of the job's start", st.registrationTimeout)
		if err := p.r.setDialHome(p.ctx, in.Labels[v1alpha1.LabelProvider], true, p.now,
			fmt.Sprintf("Instance %s/%s could not register through its dial-home path: %s", in.Namespace, in.Name, msg), time.Time{}); err != nil {
			return err
		}
		return p.fail(in, alloc, v1alpha1.ReasonRegistrationTimeout, msg)
	}
	reg := regs[len(regs)-1]
	now := metav1.NewTime(p.now)
	if err := p.setStatus(in, func(s *v1alpha1.InstanceStatus) {
		s.Phase = v1alpha1.InstanceStarting
		s.Agent = &v1alpha1.AgentRegistration{AgentInstanceID: reg.agentInstanceID, RestartCount: reg.restartCount, JobID: reg.jobID, RegisteredTime: metav1.NewTime(reg.at.UTC().Truncate(time.Second))}
		s.JobID, s.Node = alloc.Annotations[v1alpha1.AnnotationJobID], reg.node
		s.StartInstructionTime = &now
		for _, c := range []string{v1alpha1.ConditionVerified, v1alpha1.ConditionQualified, v1alpha1.ConditionWarmedUp} {
			cond(&s.Conditions, c, metav1.ConditionFalse, v1alpha1.ReasonPending, "not yet checked", in.Generation, now)
		}
		cond(&s.Conditions, v1alpha1.ConditionAgentUnreachable, metav1.ConditionFalse, v1alpha1.ReasonReporting, "the agent registered", in.Generation, now)
	}); err != nil {
		return err
	}
	p.r.Recorder.Event(in, false, eventStarted, "AgentRegistered", fmt.Sprintf("the agent of Allocation %s registered on %s; the Instance starts", alloc.Name, reg.node))
	if err := p.r.setDialHome(p.ctx, in.Labels[v1alpha1.LabelProvider], false, p.now, "an agent registered through the dial-home path", reg.at); err != nil {
		return err
	}
	p.at(p.now.Add(st.startTimeout))
	return nil
}

// settingsOf returns a Provider's settings, or the defaults when the Provider
// is not (or no longer) in view.
func (p *pass) settingsOf(pv *providerView) settings {
	if pv != nil && pv.profile != nil {
		return pv.settings
	}
	s, _ := resolveSettings(v1alpha1.PlatformProfileSpec{})
	return s
}

// agentView is what the controller knows of an Instance's agent in this pass.
type agentView struct {
	report      *agentapi.InstanceReport
	reportAt    time.Time
	unreachable bool
}

// running advances an Instance whose agent has registered: starting,
// verifying, ready and draining (HBEP-7, Instance start, Health and restarts,
// Handover).
func (p *pass) running(in *v1alpha1.Instance, alloc *v1alpha1.Allocation) error {
	ag := in.Status.Agent
	if ag == nil {
		return p.pending(in, alloc)
	}
	if err := p.finishRestarts(in); err != nil {
		return err
	}
	for _, reg := range p.r.Backend.registrations(alloc.UID) {
		if reg.agentInstanceID != ag.AgentInstanceID && (reg.jobID == ag.JobID || reg.restartCount > ag.RestartCount) {
			// A second execution of the job: nothing from the first is reused
			// (HBEP-7, Test plan; HBEP-3, open question 13).
			return p.fail(in, alloc, v1alpha1.ReasonRequeued,
				fmt.Sprintf("job %s registered again as agent %s with restart count %d: a requeue", reg.jobID, reg.agentInstanceID, reg.restartCount))
		}
	}
	pv := p.pvOf(in.Labels)
	st := p.settingsOf(pv)
	av, err := p.agent(in, alloc, st)
	if err != nil {
		return err
	}
	if in.Status.Phase == v1alpha1.InstanceDraining {
		return p.draining(in, alloc, av)
	}
	if ds, ok := p.drainStart(in, pv); ok {
		if !p.now.Before(ds) {
			if in.Status.Phase == v1alpha1.InstanceReady {
				return p.handOver(in, alloc, av)
			}
			return p.end(in, alloc, v1alpha1.ReasonDrained, "its drain start passed before it became ready", false)
		}
		p.at(ds)
	}
	switch in.Status.Phase {
	case v1alpha1.InstanceStarting, v1alpha1.InstanceVerifying:
		return p.starting(in, alloc, av, st)
	case v1alpha1.InstanceReady:
		return p.ready(in, alloc, av, st, pv)
	}
	return nil
}

// agent reads the agent's latest report for this Instance and sets
// AgentUnreachable: an agent that has sent no report for health.agentTimeout
// makes its Instance unreachable, and nothing restarts on that basis alone
// (HBEP-7, The allocation agent, Unreachable; HBEP-6).
func (p *pass) agent(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, st settings) (agentView, error) {
	rep, at, from := p.r.Backend.latest(alloc.UID)
	var av agentView
	last := p.mem.started
	if in.Status.StartInstructionTime != nil && in.Status.StartInstructionTime.After(last) {
		last = in.Status.StartInstructionTime.Time
	}
	if rep != nil && from == in.Status.Agent.AgentInstanceID {
		if at.After(last) {
			last = at
		}
		if rep.Instance != nil && rep.Instance.UID == string(in.UID) {
			av.report, av.reportAt = rep.Instance, at
		}
	}
	deadline := last.Add(st.agentTimeout)
	// After the controller's own restart every agent registers again within a
	// long poll and a report interval; none is judged unreachable before that,
	// however short health.agentTimeout is (HBEP-7, registration is soft state).
	if grace := p.mem.started.Add(agentGrace); grace.After(deadline) {
		deadline = grace
	}
	av.unreachable = !p.now.Before(deadline)
	if !av.unreachable {
		p.at(deadline)
	}
	now := metav1.NewTime(p.now)
	err := p.setStatus(in, func(s *v1alpha1.InstanceStatus) {
		if av.unreachable {
			cond(&s.Conditions, v1alpha1.ConditionAgentUnreachable, metav1.ConditionTrue, v1alpha1.ReasonReportsMissing,
				fmt.Sprintf("no report since %s", last.UTC().Format(time.RFC3339)), in.Generation, now)
		} else {
			cond(&s.Conditions, v1alpha1.ConditionAgentUnreachable, metav1.ConditionFalse, v1alpha1.ReasonReporting, "the agent reports", in.Generation, now)
		}
	})
	return av, err
}

// drainStart returns the Instance's drain start: its lease end minus its drain
// window (v1alpha1.DrainStart).
func (p *pass) drainStart(in *v1alpha1.Instance, pv *providerView) (time.Time, bool) {
	if in.Status.LeaseEnd == nil || pv == nil || pv.profile == nil {
		return time.Time{}, false
	}
	var routing *v1alpha1.Routing
	if p.md != nil {
		routing = p.md.Spec.Routing
	}
	ds, err := v1alpha1.DrainStart(in.Status.LeaseEnd.Time, pv.profile.Spec, routing)
	if err != nil {
		return time.Time{}, false
	}
	return ds, true
}

// draining ends a draining Instance when its agent reports no running request,
// or at its hard end (HBEP-7, Handover, Draining).
func (p *pass) draining(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, av agentView) error {
	if in.Status.LeaseEnd != nil {
		if !p.now.Before(in.Status.LeaseEnd.Time) {
			return p.end(in, alloc, v1alpha1.ReasonDrained, "the hard end came", false)
		}
		p.at(in.Status.LeaseEnd.Time)
	}
	// Only a report that arrived at least a second after the drain began
	// counts: an older one predates requests the front admitted until then.
	began, ok := drainBegan(in)
	if !ok {
		began = p.mem.started // an Instance made draining before this record existed
	}
	if av.report != nil && !av.unreachable && !av.reportAt.Before(began.Add(time.Second)) && runningRequests(av.report) == 0 {
		return p.end(in, alloc, v1alpha1.ReasonDrained, "drained: no request runs", false)
	}
	return nil
}

func runningRequests(r *agentapi.InstanceReport) int64 {
	var n int64
	for _, w := range r.Workers {
		if w.Health != nil {
			n += w.Health.RunningRequests
		}
	}
	return n
}

// handOver drains a ready primary at its drain start, whether or not its
// replacement serves: the drain rule (HBEP-7, Handover). A replacement that
// serves takes over; one that does not leaves capacity dropped, with a
// degraded event.
func (p *pass) handOver(in *v1alpha1.Instance, alloc *v1alpha1.Allocation, av agentView) error {
	repl := p.replacementOf(in)
	if repl != nil && repl.Status.Phase == v1alpha1.InstanceReady {
		p.r.Recorder.Event(in, false, eventReplaced, "HandedOver", fmt.Sprintf("drain start reached; Instance %s serves in its place", repl.Name))
	} else {
		p.r.Recorder.Event(in, true, eventDegraded, v1alpha1.ReasonCapacityDropped, "drain start reached with no replacement ready: capacity drops (HBEP-7, drain rule)")
	}
	if err := p.startDrain(in); err != nil {
		return err
	}
	return p.draining(in, alloc, av)
}

// replacementOf returns the primary's live replacement, if any.
func (p *pass) replacementOf(in *v1alpha1.Instance) *v1alpha1.Instance {
	for _, o := range p.insts {
		if !ended(o) && o.Annotations[v1alpha1.AnnotationPredecessor] == in.Name {
			return o
		}
	}
	return nil
}

func isNotFound(err error) bool {
	ok, nerr := errNotFoundIsNil(err)
	return !ok && nerr == nil
}
