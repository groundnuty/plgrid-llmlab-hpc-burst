package alpha

// The live steps' observations (issue #245), built from what the stack and the
// cluster recorded: the API's Instances, ModelDeployment and events, the
// front's usage records and response headers, and frps's proxy list. They are
// pure functions, so that hack/mutate-alpha.sh's plain go test of this package
// kills a change in any of them; the code that drives the stack only collects
// their inputs. Nothing here reads a log line.

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/api/meta"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/frpsplugin"
)

// FrpsTimeLayout is how the pinned frps (v0.71.1) writes a proxy's
// lastStartTime: month, day and time in frps's local zone, with no year
// (internal/frpsplugin's reader test holds an entry of that form).
const FrpsTimeLayout = "01-02 15:04:05"

// ParseFrpsTime reads a lastStartTime that was listed at at. frps runs on this
// host, in its zone. The year is at's, or the year before when that would put
// the time after at: a session started on 31 December and looked at on
// 1 January.
func ParseFrpsTime(s string, at time.Time) (time.Time, error) {
	t, err := time.ParseInLocation(FrpsTimeLayout, s, at.Location())
	if err != nil {
		return time.Time{}, fmt.Errorf("frps's lastStartTime is not of the form %s", FrpsTimeLayout)
	}
	y := time.Date(at.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, at.Location())
	if y.After(at) {
		y = y.AddDate(-1, 0, 0)
	}
	return y, nil
}

// TunnelSampleOf is one look at the Instance's tunnel session at at: frps's
// lastStartTime for its proxy, which frps names by the Instance's UID
// (internal/frpsplugin, BuildReport), and whether the Instance's
// TunnelConnected condition was True. A proxy that frps does not list online,
// or whose lastStartTime does not parse, leaves LastStart zero, which the
// verdict fails.
func TunnelSampleOf(at time.Time, proxies []frpsplugin.Proxy, in *v1alpha1.Instance) TunnelSample {
	s := TunnelSample{At: at, Connected: meta.IsStatusConditionTrue(in.Status.Conditions, v1alpha1.ConditionTunnelConnected)}
	for _, p := range proxies {
		if p.Name == string(in.UID) && p.Status == frpsplugin.StatusOnline {
			if t, err := ParseFrpsTime(p.LastStartTime, at); err == nil {
				s.LastStart = t
			}
		}
	}
	return s
}

// DrainWindow is an Instance's drain window from the deployment's and the
// profile's specs alone (HBEP-7, Handover): routing.maxRequestDuration plus the
// profile's drainMargin, or the profile's drainWindow when that is shorter. It
// is worked out here, not with the helper the controller and the front share
// (v1alpha1.InstanceDrainWindow), so that a fault there shows as a stream not in
// flight at the drain start rather than agreeing with itself.
func DrainWindow(maxRequest, drainMargin, drainWindow time.Duration) time.Duration {
	w := maxRequest + drainMargin
	if drainWindow > 0 && drainWindow < w {
		return drainWindow
	}
	return w
}

// DrainStartOf is the Instance's drain start from its recorded lease end:
// status.leaseEnd less the drain window. ok is false while it has no lease end.
func DrainStartOf(in *v1alpha1.Instance, window time.Duration) (t time.Time, ok bool) {
	if in.Status.LeaseEnd == nil {
		return time.Time{}, false
	}
	return in.Status.LeaseEnd.Add(-window), true
}

// SpanOf is one Instance's life as the API recorded it: its readyTime and
// endTime, under its Allocation's job tag.
func SpanOf(in *v1alpha1.Instance, tag string) InstanceSpan {
	s := InstanceSpan{Tag: tag}
	if in.Status.ReadyTime != nil {
		s.ReadyAt = in.Status.ReadyTime.Time
	}
	if in.Status.EndTime != nil {
		s.EndedAt = in.Status.EndTime.Time
	}
	return s
}

// ClientEnd is what the client read at the end of a reply: whether the body
// carried the end the endpoint's completion rule names (a finish reason, and
// [DONE] for a stream), and the code of an error the front put in it (for
// example stream_cut).
type ClientEnd struct {
	Done, Finished bool
	ErrorCode      string
}

// StreamOf is one request as the run saw it: its times and its end from the
// front's usage record (HBEP-5, Usage records), and the end the client read.
// It is complete only when both ends agree: the record has a finish and no
// cutBy, its status is 200, and the client read the finish and [DONE]. A close
// that only looks complete, a body cut short with no error, is not.
func StreamOf(rec usage.Record, end ClientEnd) Stream {
	s := Stream{ID: rec.RequestID, Error: end.ErrorCode}
	s.Opened, _ = time.Parse(time.RFC3339Nano, rec.StartTime)
	s.Ended, _ = time.Parse(time.RFC3339Nano, rec.EndTime)
	if rec.CutBy != nil {
		s.CutBy = *rec.CutBy
	}
	if rec.CompletionTokens != nil {
		s.Tokens = int(*rec.CompletionTokens)
	}
	s.Complete = rec.Status == http.StatusOK && rec.Finish != nil && rec.CutBy == nil && end.Done && end.Finished && end.ErrorCode == ""
	return s
}

// WorkerRestarts counts the restarts at the worker rung that the run's
// Instances recorded (status.restarts whose part is a worker, HBEP-7, Health
// and restarts). Each Instance is given in the last version the run saw.
func WorkerRestarts(ins []v1alpha1.Instance) int {
	n := 0
	for _, in := range ins {
		for _, r := range in.Status.Restarts {
			if strings.HasPrefix(r.Part, "worker-") {
				n++
			}
		}
	}
	return n
}

// deploymentIDRE is the display form of a deployment ID (HBEP-5, Attribution headers): 12 hex characters.
var deploymentIDRE = regexp.MustCompile(`^[0-9a-f]{12}$`)

// Attributed reports whether a response carries HBEP-5's attribution headers
// (hbep/0005-llmlab-compatible-front/README.md, Attribution headers): the
// request ID and the emitter always; and for a reply an Instance emitted, the
// Provider, the deployment ID in its display form, and the Instance, which must
// be one of the run's (uids).
func Attributed(h http.Header, uids map[string]bool) bool {
	if h.Get("Hpcburst-Request-Id") == "" {
		return false
	}
	switch h.Get("Hpcburst-Emitter") {
	case "front":
		return true
	case "instance":
		return h.Get("Hpcburst-Provider") != "" && deploymentIDRE.MatchString(h.Get("Hpcburst-Deployment-Id")) &&
			h.Get("Hpcburst-Instance") != "" && uids[h.Get("Hpcburst-Instance-Uid")]
	case "provider":
		return h.Get("Hpcburst-Provider") != "" && h.Get("Hpcburst-Deployment-Id") != ""
	}
	return false
}

// ErrSelectorDeployment is a deployment that picks its Providers by label: the
// run cannot say which Providers it allowed, so it measures nothing.
var ErrSelectorDeployment = errors.New("the deployment selects its Providers by label, so the run cannot name the Providers it allowed")

// DeploymentSpecOf is what the API held for the run's ModelDeployment: the
// Providers it allows by name, each with the platform it is on (clusterOf, by
// Provider name; a Provider it cannot place has no cluster, which the verdict
// fails), and its timing.
func DeploymentSpecOf(md *v1alpha1.ModelDeployment, clusterOf map[string]string) (DeploymentSpec, error) {
	var d DeploymentSpec
	if md.Spec.Providers.Selector != nil {
		return d, ErrSelectorDeployment
	}
	for _, n := range md.Spec.Providers.Names {
		d.Providers = append(d.Providers, ProviderRef{Name: n, Cluster: clusterOf[n]})
	}
	var err error
	if d.LeaseLimit, err = durationOf(md.Spec.LeaseLimit); err != nil {
		return d, fmt.Errorf("leaseLimit: %w", err)
	}
	if md.Spec.Routing != nil {
		if d.MaxRequestDuration, err = durationOf(md.Spec.Routing.MaxRequestDuration); err != nil {
			return d, fmt.Errorf("routing.maxRequestDuration: %w", err)
		}
	}
	if rp := md.Spec.RetentionPolicy; rp != nil && rp.Mode == v1alpha1.RetentionReleaseWhenIdle {
		d.ReleaseWhenIdle = true
		if d.IdleLimit, err = durationOf(rp.IdleLimit); err != nil {
			return d, fmt.Errorf("retentionPolicy.idleLimit: %w", err)
		}
	}
	return d, nil
}

// durationOf is an API duration; zero when it is not set.
func durationOf(d *v1alpha1.Duration) (time.Duration, error) {
	if d == nil {
		return 0, nil
	}
	return d.AsDuration()
}

// Handovers counts the handovers that completed end to end: a predecessor the
// controller handed over (an event HandedOver regarding it) that then ended
// Drained. ended holds each predecessor in the last version the run saw.
func Handovers(evs []eventsv1.Event, ended []v1alpha1.Instance) int {
	handed := map[string]bool{}
	for _, e := range evs {
		if e.Reason == "HandedOver" {
			handed[string(e.Regarding.UID)] = true
		}
	}
	n := 0
	for _, in := range ended {
		if handed[string(in.UID)] && in.Status.Phase == v1alpha1.InstanceEnded && in.Status.EndReason == v1alpha1.ReasonDrained {
			n++
		}
	}
	return n
}

// SlotSeen reports whether a version of the deployment carried a condition
// with reason NoOverlapSlot (HBEP-7, No-overlap mode).
func SlotSeen(versions []*v1alpha1.ModelDeployment) bool {
	for _, md := range versions {
		for _, c := range md.Status.Conditions {
			if c.Reason == v1alpha1.ReasonNoOverlapSlot {
				return true
			}
		}
	}
	return false
}
