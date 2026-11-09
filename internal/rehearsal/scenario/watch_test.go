package scenario_test

// The scenario's views of what stack.Recorder saw: every Instance, Allocation
// and ModelDeployment version the API server sends on a watch. The steps ask it
// whether a state was ever observed and in which order (step (a)'s first entry
// to ready, step (g)'s drain), which a poll could miss when the state lasts a
// second.

import (
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/rehearsal/stack"
)

// mdObservation is one ModelDeployment version as the watch delivered it:
// its Degraded and Ready conditions, "" when absent.
type mdObservation struct {
	at              time.Time
	name            string
	degraded        metav1.ConditionStatus
	degradedReason  string
	degradedMessage string
	ready           metav1.ConditionStatus
	readyReason     string
}

// deploymentObservations returns every observation of the named
// ModelDeployment, in the order received.
func (r *rig) deploymentObservations(name string) []mdObservation {
	var out []mdObservation
	for _, v := range r.watch.DeploymentVersions() {
		if v.Deployment.Name != name {
			continue
		}
		o := mdObservation{at: v.At, name: name}
		if c := meta.FindStatusCondition(v.Deployment.Status.Conditions, v1alpha1.ConditionDegraded); c != nil {
			o.degraded, o.degradedReason, o.degradedMessage = c.Status, c.Reason, c.Message
		}
		if c := meta.FindStatusCondition(v.Deployment.Status.Conditions, v1alpha1.ConditionReady); c != nil {
			o.ready, o.readyReason = c.Status, c.Reason
		}
		out = append(out, o)
	}
	return out
}

// createdAt returns the Instance's creationTimestamp from the watch, and
// whether the watch saw the Instance with one.
func (r *rig) createdAt(uid types.UID) (time.Time, bool) {
	for _, o := range r.watch.Of(uid) {
		if c := o.Instance.CreationTimestamp; !c.IsZero() {
			return c.Time, true
		}
	}
	return time.Time{}, false
}

// firstSpec is the spec of the first Instance the watch delivered, nil with
// none: the tunnel witness's (orphans_test.go).
func (r *rig) firstSpec() *v1alpha1.InstanceSpec {
	uids := r.watch.UIDs()
	if len(uids) == 0 {
		return nil
	}
	return &r.watch.Of(uids[0])[0].Instance.Spec
}

// proxyEnded waits until frps no longer lists the Instance's proxy online and
// records when that was first seen: a bound the proxy's end lies before.
func (r *rig) proxyEnded(uid types.UID) {
	r.t.Helper()
	stack.WaitFor(r.t, stack.WaitDeadline, "frps to list the proxy "+string(uid)+" offline", func() bool {
		return !r.frpsProxies()[string(uid)]
	})
	r.markProxyEnd(uid, time.Now())
}

func (r *rig) markProxyEnd(uid types.UID, at time.Time) {
	if r.proxyEnds == nil {
		r.proxyEnds = map[types.UID]time.Time{}
	}
	if _, ok := r.proxyEnds[uid]; !ok {
		r.proxyEnds[uid] = at
	}
}
