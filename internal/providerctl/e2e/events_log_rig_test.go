package e2e

// The durable log in the Provider controller's event rows (HBEP-8, Test plan, Beta; issue #625, PR E2): the
// log is started by the rig as a REAL child process (internal/eventlog/node/nodetest, on 28518 and 28519, this
// package's pair of #625's block), the typed recorder publishes through a Publisher, and every row reads the log
// through a FRESH client, from its first sequence: never the publisher's acknowledgements, never the recorder's
// double, never the raised-events counter (which shows absence only).

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog/node/nodetest"
)

// logPorts are this package's pair of #625's port block.
var logPorts = nodetest.Ports{Client: 28518, Monitor: 28519}

// logCaps leave HB_EVENTS room for every row.
var logCaps = eventlog.Caps{Events: 64 << 20, Usage: 1 << 20}

// startLog starts the rig's durable log and its publisher, unless the rig is without one. A rig with
// logDown makes the log's directory and does not start it. A rig with withLease creates the Provider's own
// Lease, provider-<name>, with a UID, which EventsNotLogged regards.
func (r *evRig) startLog(o rigOpts) {
	r.t.Helper()
	if o.withLease {
		l := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: v1alpha1.ControllerNamespace, Name: v1alpha1.ProviderLeaseName(r.prov)}}
		if err := c.Create(r.ctx, l); err != nil {
			r.t.Fatal(err)
		}
		r.lease = l
		r.t.Cleanup(func() { _ = c.Delete(context.Background(), l) })
	}
	if o.noLog {
		return
	}
	r.logDir = nodetest.NewDir(r.t, logPorts, logCaps)
	if !o.logDown {
		r.logProc = r.logDir.Start(r.t)
		r.logProc.Ready()
	}
	r.pub = eventcount.NewPublisher(eventcount.PublisherOptions{
		Addr: r.logDir.Ports.Addr(), Token: r.logDir.Token, Budget: o.budget,
		MinBackoff: 5 * time.Millisecond, MaxBackoff: 100 * time.Millisecond,
	})
	r.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		r.pub.Shutdown(ctx)
	})
}

// startLogProc starts the rig's log (again) on its store and waits until it serves.
func (r *evRig) startLogProc() {
	r.t.Helper()
	r.logProc = r.logDir.Start(r.t)
	r.logProc.Ready()
}

// logged is one message of HB_EVENTS, decoded.
type logged struct {
	nodetest.Message
	Data eventcount.Data
	Time time.Time
	Src  string
}

// logAll reads every message of the log through a fresh client.
func (r *evRig) logAll() []logged {
	r.t.Helper()
	var out []logged
	for _, m := range r.logDir.ReadAll(r.t, eventlog.StreamEvents) {
		ev, d, err := eventcount.Decode(m.Payload)
		if err != nil {
			r.t.Fatalf("message %s of the log does not decode: %v", m.ID, err)
		}
		out = append(out, logged{Message: m, Data: d, Time: ev.Time(), Src: ev.Source()})
	}
	return out
}

// logAbout reads the log's messages about the object with the UID, in order.
func (r *evRig) logAbout(uid types.UID) []logged {
	r.t.Helper()
	var out []logged
	for _, m := range r.logAll() {
		if m.Data.Regarding.UID == uid {
			out = append(out, m)
		}
	}
	return out
}

// idle waits until the publisher holds nothing: every event handed to it has been stored or counted lost.
func (r *evRig) idle() {
	r.t.Helper()
	eventually(r.t, "the publisher to hold no event", 60*time.Second, func() bool { return r.pub.Pending() == 0 })
}

// wantLog waits until the log holds each id, as written out by hand by the row, on the Allocation's subject, and
// returns the Allocation's messages. It fails after 60 s, naming the ids it does not hold.
func (r *evRig) wantLog(a *v1alpha1.Allocation, ids ...string) []logged {
	r.t.Helper()
	var got []logged
	missing := func() []string {
		got = r.logAbout(a.UID)
		have := map[string]bool{}
		for _, m := range got {
			have[m.ID] = true
		}
		var out []string
		for _, id := range ids {
			if !have[id] {
				out = append(out, id)
			}
		}
		return out
	}
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if len(missing()) == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if m := missing(); len(m) > 0 {
		var have []string
		for _, g := range got {
			have = append(have, g.ID)
		}
		r.t.Fatalf("the log lacks the ids %v after 60 s; it holds %v", m, have)
	}
	for _, m := range got {
		if want := "hb.ev.allocation." + string(a.UID); m.Subject != want {
			r.t.Errorf("%s is on %q, want %q", m.ID, m.Subject, want)
		}
		if m.Src != "/hpcburst/provider-"+r.prov || m.Data.ReportingController != v1alpha1.FieldManagerProvider {
			r.t.Errorf("%s: source %q, reportingController %q", m.ID, m.Src, m.Data.ReportingController)
		}
	}
	return got
}

// loggedOf returns the messages of the reason.
func loggedOf(ms []logged, reason string) []logged {
	var out []logged
	for _, m := range ms {
		if m.Data.Reason == reason {
			out = append(out, m)
		}
	}
	return out
}

// checkLogNotes fails the test when a note the log keeps names the job ID as a token, or the Provider's account,
// grant or user, or looks like a path: the log is kept for the grant and may be exported (HBEP-8, Allocation
// events, No job ID, path or name in a note).
func (r *evRig) checkLogNotes(ms []logged, jobID int) {
	r.t.Helper()
	token := fmt.Sprint(jobID)
	for _, m := range ms {
		for _, text := range []string{m.Data.Note, m.Data.Reason, m.Data.Action, m.ID} {
			for _, bad := range []string{token, rowAccount, rowGrant, rowUser, rowHome, rowCredsPath} {
				if tokenIn(text, bad) {
					r.t.Errorf("the log's %s event text %q names %q", m.Data.Reason, text, bad)
				}
			}
		}
		if mt := pathToken.FindString(m.Data.Note); mt != "" {
			r.t.Errorf("the log's %s note %q holds the path-looking token %q", m.Data.Reason, m.Data.Note, mt)
		}
	}
}

// matchAPI checks the envelope's data against the API's Event of the same reason on regarding UID, reason, action,
// type, reporting controller and note, and the two copies' times within one second (HBEP-8, The durable log, Streams,
// The envelope; the science agent's ruling on #625's PR E2): the log's time is the wrapper's clock, read just before
// client-go's, which gives the API's eventTime.
func (r *evRig) matchAPI(ms []logged, api []eventsv1.Event, reasons ...string) {
	r.t.Helper()
	for _, reason := range reasons {
		in := loggedOf(ms, reason)
		var out []eventsv1.Event
		for _, e := range api {
			if e.Reason == reason {
				out = append(out, e)
			}
		}
		if len(in) == 0 || len(out) == 0 {
			r.t.Errorf("%s: the log holds %d, the API %d: want both", reason, len(in), len(out))
			continue
		}
		l, e := in[0], out[0]
		if l.Data.Regarding.UID != e.Regarding.UID || l.Data.Reason != e.Reason || l.Data.Action != e.Action || l.Data.Type != e.Type ||
			l.Data.ReportingController != e.ReportingController || l.Data.Note != e.Note {
			r.t.Errorf("%s: the log's %+v differs from the API's (regarding %s, action %s, type %s, reporting %s, note %q)", reason, l.Data, e.Regarding.UID, e.Action, e.Type, e.ReportingController, e.Note)
		}
		if d := l.Time.Sub(e.EventTime.Time); d > time.Second || d < -time.Second {
			r.t.Errorf("%s: the log's time %s and the API's eventTime %s differ by %s, want within 1 s", reason, l.Time, e.EventTime.Time, d)
		}
	}
}

var uuidRE = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// rfc is a time as an id's fact writes it: RFC 3339 in UTC, to the second.
func rfc(t time.Time) string { return t.UTC().Truncate(time.Second).Format(time.RFC3339) }

// eid is <Allocation UID>/<reason>/<fact>, written out by hand.
func eid(a *v1alpha1.Allocation, reason, fact string) string {
	return string(a.UID) + "/" + reason + "/" + fact
}

func joinIDs(ms []logged) string {
	var ids []string
	for _, m := range ms {
		ids = append(ids, m.ID)
	}
	return strings.Join(ids, ", ")
}
