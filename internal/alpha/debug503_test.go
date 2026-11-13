package alpha

// DEBUG, throwaway only (never merged): what the cluster's API holds of the run's Allocations, Instances and events at a moment,
// for the stand-in whose run made a second Allocation that never got a job (#503).

import (
	"strings"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

func (r *liveRun) debugState(label string) {
	t := r.t
	now := time.Now()
	var al v1alpha1.AllocationList
	var il v1alpha1.InstanceList
	ctx, cancel := stack.CallCtx(t)
	err := r.c.List(ctx, &al, client.InNamespace(r.w.md.Namespace))
	if err == nil {
		err = r.c.List(ctx, &il, client.InNamespace(r.w.md.Namespace))
	}
	cancel()
	if err != nil {
		t.Logf("DEBUG[%s]: the lists failed: %v", label, err)
		return
	}
	t.Logf("DEBUG[%s]: %d Allocations, %d Instances", label, len(al.Items), len(il.Items))
	tags, _ := r.runTags()
	t.Logf("DEBUG[%s]: the run's tags, from the watch: %q", label, tags)
	for i, a := range al.Items {
		_, idle, _ := v1alpha1.IdleSince(&a)
		t.Logf("DEBUG[%s]: Allocation %d (%s): age %s, phase %q, deleting %t, finalizers %d, job id set %t, idle-since set %t, spec retention %v",
			label, i+1, tail(a.Name), now.Sub(a.CreationTimestamp.Time).Round(time.Second), a.Status.AtProvider.Phase, !a.DeletionTimestamp.IsZero(),
			len(a.Finalizers), a.Status.AtProvider.JobID != "", idle, a.Spec.RetentionPolicy.EffectiveMode())
	}
	for i, in := range il.Items {
		t.Logf("DEBUG[%s]: Instance %d (%s): age %s, phase %q, end reason %q, deleting %t, on Allocation %s",
			label, i+1, tail(in.Name), now.Sub(in.CreationTimestamp.Time).Round(time.Second), in.Status.Phase, in.Status.EndReason, !in.DeletionTimestamp.IsZero(), tail(in.Spec.AllocationRef.Name))
	}
	var ev []string
	for _, e := range stack.Events(t, r.c) {
		when := e.EventTime.Time
		if when.IsZero() {
			when = e.DeprecatedLastTimestamp.Time
		}
		ev = append(ev, strings.Join([]string{now.Sub(when).Round(time.Second).String() + " ago", e.Regarding.Kind, tail(e.Regarding.Name), e.Action, e.Reason}, " "))
	}
	t.Logf("DEBUG[%s]: %d events:\n    %s", label, len(ev), strings.Join(ev, "\n    "))
}

// tail is the last 5 characters of a name: enough to tell objects apart in a log, never a whole identifier.
func tail(s string) string {
	if len(s) > 5 {
		return s[len(s)-5:]
	}
	return s
}
