package front

// The front's typed events and the durable log (HBEP-8, The durable log, Who writes and When it is
// down; issue #625, PR E2). The log runs as a REAL child (internal/eventlog/node/nodetest) on the
// front tests' pair, 28512 and 28513, and what it holds is read through a fresh client. The front
// opens a second client to the log beside E3's usage forwarder, which is the forwarder's own.

import (
	"context"
	"strings"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog/node/nodetest"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

const frontLeaseUID = "lease-uid-of-the-front"

// frontKube is a fake API holding the front's Lease, with its UID.
func frontKube() *kubefake.Clientset {
	return kubefake.NewSimpleClientset(&coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{
		Namespace: v1alpha1.ControllerNamespace, Name: v1alpha1.LeaseFront, UID: types.UID(frontLeaseUID)}})
}

func frontPublisher(t *testing.T, d *nodetest.Dir, budget time.Duration) *eventcount.Publisher {
	t.Helper()
	p := eventcount.NewPublisher(eventcount.PublisherOptions{Addr: d.Ports.Addr(), Token: d.Token, Budget: budget,
		MinBackoff: 5 * time.Millisecond, MaxBackoff: 50 * time.Millisecond})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		p.Shutdown(ctx)
	})
	return p
}

func md(uid string) *v1alpha1.ModelDeployment {
	m := &v1alpha1.ModelDeployment{}
	m.Namespace, m.Name, m.UID = "ns-a", "md-"+uid, types.UID(uid)
	return m
}

// The front's typed event is in the log on its object's subject, reported by hpcburst-front, with the
// front's controller label as its source; and, with the log down past the retries, it is counted and
// the front raises EventsNotLogged regarding its own Lease, which the fake API holds, and which the log
// holds once it is back.
func TestTheFrontPublishesItsEventsAndRaisesEventsNotLogged(t *testing.T) {
	d := nodetest.NewDir(t, usageLogPorts, eventlog.Caps{Events: 1 << 20, Usage: 64 << 20})
	proc := d.Start(t)
	proc.Ready()
	kube := frontKube()
	pub := frontPublisher(t, d, time.Second)
	rec, stop, err := NewKubeRecorderWithLog(kube, pub)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	deployment := md("11111111-1111-4111-8111-111111111111")
	rec.Event(deployment, true, eventDegraded, reasonCountsDrift, "the counts drifted")
	waitUntil(t, "the event to be stored", func() bool { return pub.Pending() == 0 })
	ms := d.ReadAll(t, eventlog.StreamEvents)
	if len(ms) != 1 || ms[0].Subject != "hb.ev.modeldeployment.11111111-1111-4111-8111-111111111111" {
		t.Fatalf("the log holds %+v, want one event on the ModelDeployment's subject", ms)
	}
	ev, data, err := eventcount.Decode(ms[0].Payload)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Source() != "/hpcburst/front" || data.Reason != reasonCountsDrift || data.Action != eventDegraded || data.Type != provider.EventKindWarning ||
		data.ReportingController != fieldManagerFront || data.Note != "the counts drifted" {
		t.Errorf("the envelope's source %q and data %+v", ev.Source(), data)
	}
	if got, exists := rec.Counter.Unlogged(eventcount.ControllerFront); !exists || got != 0 {
		t.Errorf("unlogged is %v (exists %v), want the series at 0", got, exists)
	}

	// The log is killed and the retries run out: the event is counted, and EventsNotLogged is raised
	// regarding the front's Lease, to the API (fake) and, once the log is back, to the log.
	proc.Kill()
	rec.Event(deployment, true, eventDegraded, reasonCountsDrift, "lost one")
	waitUntil(t, "the loss to be counted", func() bool { v, _ := rec.Counter.Unlogged(eventcount.ControllerFront); return v >= 1 })
	waitUntil(t, "EventsNotLogged to reach the API", func() bool {
		list, err := kube.EventsV1().Events(v1alpha1.ControllerNamespace).List(context.Background(), metav1.ListOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range list.Items {
			if e.Reason == eventcount.ReasonEventsNotLogged && e.Regarding.UID == types.UID(frontLeaseUID) && e.Action == eventcount.ActionDegraded && e.Type == provider.EventKindWarning {
				return true
			}
		}
		return false
	})
	proc = d.Start(t)
	proc.Ready()
	rec.Event(deployment, true, eventDegraded, reasonCountsDrift, "after the return")
	leaseSubject := "hb.ev.lease." + frontLeaseUID
	waitUntil(t, "EventsNotLogged to be in the log", func() bool {
		for _, m := range d.ReadAll(t, eventlog.StreamEvents) {
			if m.Subject == leaseSubject && strings.Contains(string(m.Payload), "takes events again") {
				return true
			}
		}
		return false
	})
}

// A front built without the log has no unlogged series and publishes nothing: as before.
func TestAFrontWithoutALogCountsNothingAsUnlogged(t *testing.T) {
	rec, stop, err := NewKubeRecorder(kubefake.NewSimpleClientset())
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	rec.Event(md("22222222-2222-4222-8222-222222222222"), true, eventDegraded, reasonCountsDrift, "n")
	if _, exists := rec.Counter.Unlogged(eventcount.ControllerFront); exists {
		t.Error("a front with no log has an unlogged series")
	}
}
