package stack

// The durable log's part of the host (eventlog.go), without a stack: the option's arguments, the match of
// an expected event, and the read of HB_EVENTS against a log that runs in this process, on a kernel port
// (the rehearsal's own use of the option is in internal/rehearsal/scenario).

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventcount"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
)

// The option adds up's --event-log with the floor and the caps of #639's k3s rows, and the block grows to
// base+17: both ports are listed after the bulk path's three, whether or not it runs.
func TestEventLogOption(t *testing.T) {
	h := &Host{Ports: Ports{Base: 29520}}
	EventLog(h)
	want := []string{"--event-log", "--event-log-floor", "1GB", "--event-log-events-cap", "64MiB", "--event-log-usage-cap", "64MiB"}
	if !slices.Equal(h.upExtra, want) {
		t.Errorf("up's extra arguments are %q, want %q", h.upExtra, want)
	}
	if !h.Ports.EventLog {
		t.Error("the host's ports do not list the log's")
	}
	if got := (Ports{Base: 29520}).All(); slices.Contains(got, 29536) || slices.Contains(got, 29537) {
		t.Errorf("without the option the block lists the log's ports: %v", got)
	}
	if got := h.Ports.All(); !slices.Equal(got[len(got)-3:], []int{29532, 29536, 29537}) {
		t.Errorf("with the option the block ends %v, want 29532 (the last of up's 13), 29536 and 29537", got[len(got)-3:])
	}
	both := Ports{Base: 29520, BulkPath: true, EventLog: true}
	if got := both.All(); !slices.Equal(got[len(got)-5:], []int{29533, 29534, 29535, 29536, 29537}) {
		t.Errorf("with the bulk path and the log the block ends %v, want 29533 to 29537", got[len(got)-5:])
	}
}

func logged(seq uint64, subject, reason, action, controller string) LoggedEvent {
	return LoggedEvent{Seq: seq, Subject: subject, Data: eventcount.Data{Reason: reason, Action: action, ReportingController: controller}}
}

// An event is the wanted one only when its subject, reason, action and reporting controller all agree: the
// right reason from another controller, or on another object, is not it. The failure names each controller
// whose event is absent, once, and shows what the log held.
func TestMissingLogged(t *testing.T) {
	const inst, alloc = "hb.ev.instance.u1", "hb.ev.allocation.u2"
	want := []WantLogged{
		{Controller: "hpcburst-lifecycle", Subject: inst, Reason: "AgentRegistered", Action: "started"},
		{Controller: "hpcburst-provider", Subject: alloc, Reason: "JobSubmitted", Action: "started"},
		{Controller: "hpcburst-front", Subject: "hb.ev.platformprofile.u3", Reason: "DrainWindowShort", Action: "degraded"},
	}
	all := []LoggedEvent{
		logged(1, alloc, "JobSubmitted", "started", "hpcburst-provider"),
		logged(2, inst, "AgentRegistered", "started", "hpcburst-lifecycle"),
		logged(3, "hb.ev.platformprofile.u3", "DrainWindowShort", "degraded", "hpcburst-front"),
		logged(4, inst, "Ready", "ready", "hpcburst-lifecycle"),
	}
	if m := MissingLogged(all, want); len(m) != 0 {
		t.Errorf("with every event in the log, missing %v", m)
	}
	for name, c := range map[string]struct {
		drop    int          // the index of the event of all to drop
		replace *LoggedEvent // the near miss that takes its place
		missing []string     // the missing events' controllers
	}{
		"the controller's own event is absent":     {drop: 0, missing: []string{"hpcburst-provider"}},
		"the front's event is absent":              {drop: 2, missing: []string{"hpcburst-front"}},
		"the right reason from another controller": {drop: 1, replace: &LoggedEvent{Seq: 9, Subject: inst, Data: eventcount.Data{Reason: "AgentRegistered", Action: "started", ReportingController: "hpcburst-front"}}, missing: []string{"hpcburst-lifecycle"}},
		"the right controller on another object":   {drop: 1, replace: &LoggedEvent{Seq: 9, Subject: "hb.ev.instance.other", Data: eventcount.Data{Reason: "AgentRegistered", Action: "started", ReportingController: "hpcburst-lifecycle"}}, missing: []string{"hpcburst-lifecycle"}},
		"the right controller with another reason": {drop: 1, replace: &LoggedEvent{Seq: 9, Subject: inst, Data: eventcount.Data{Reason: "Ready", Action: "started", ReportingController: "hpcburst-lifecycle"}}, missing: []string{"hpcburst-lifecycle"}},
		"the right controller with another action": {drop: 1, replace: &LoggedEvent{Seq: 9, Subject: inst, Data: eventcount.Data{Reason: "AgentRegistered", Action: "ready", ReportingController: "hpcburst-lifecycle"}}, missing: []string{"hpcburst-lifecycle"}},
	} {
		got := slices.Delete(slices.Clone(all), c.drop, c.drop+1)
		if c.replace != nil {
			got = append(got, *c.replace)
		}
		m := MissingLogged(got, want)
		var ctl []string
		for _, w := range m {
			ctl = append(ctl, w.Controller)
		}
		if !slices.Equal(ctl, c.missing) {
			t.Errorf("%s: missing the events of %v, want %v", name, ctl, c.missing)
		}
		if d := DescribeMissing(m, got); !strings.Contains(d, "the log holds no event from "+c.missing[0]) || !strings.Contains(d, c.missing[0]+"]") {
			t.Errorf("%s: the description %q does not name %s", name, d, c.missing[0])
		}
	}
	// Two events of one controller are named once; nothing in the log is no event to match.
	m := MissingLogged(nil, []WantLogged{want[0], want[0], want[1]})
	if d := DescribeMissing(m, nil); strings.Count(d, "hpcburst-lifecycle") != 3 || !strings.HasPrefix(d, "the log holds no event from hpcburst-lifecycle hpcburst-provider, missing:") {
		t.Errorf("the description of three missing events from two controllers: %q", d)
	}
	if got := DescribeMissing(nil, all); got != "no event is missing" {
		t.Errorf("the description of nothing missing: %q", got)
	}
	long := make([]LoggedEvent, 31)
	for i := range long {
		long[i] = logged(uint64(i+1), inst, "Ready", "ready", "hpcburst-lifecycle")
	}
	if d := DescribeMissing(want[:1], long); !strings.Contains(d, "[#30 ") || strings.Contains(d, "[#31 ") || !strings.Contains(d, "(1 more)") {
		t.Errorf("a description of 31 events is cut at 30: %q", d)
	}
}

// testLog runs a log in this process: nats-server with JetStream and token authentication on a kernel port,
// and HB_EVENTS with the subject filter the launcher gives it. It returns the client address and a token file
// of the launcher's form.
func testLog(t *testing.T) (addr, tokenFile, token string, js jetstream.JetStream) {
	t.Helper()
	token, err := eventlog.NewToken()
	if err != nil {
		t.Fatal(err)
	}
	s, err := server.NewServer(&server.Options{ServerName: "stack-test-log", Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(),
		Authorization: token, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
	if !s.ReadyForConnections(30 * time.Second) {
		t.Fatal("the in-process log is not ready")
	}
	addr = s.Addr().String()
	tokenFile = filepath.Join(t.TempDir(), eventlog.TokenFile)
	if err := os.WriteFile(tokenFile, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect("nats://"+addr, nats.Token(token))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	if js, err = jetstream.New(nc); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: eventlog.StreamEvents, Subjects: []string{eventlog.SubjectEvents}}); err != nil {
		t.Fatal(err)
	}
	return addr, tokenFile, token, js
}

// publish stores one envelope of the form the publishers write (HBEP-8, The envelope), built here from the
// SDK as the test's own fixture.
func publish(t *testing.T, js jetstream.JetStream, id, subject string, d eventcount.Data) {
	t.Helper()
	e := cloudevents.NewEvent()
	e.SetID(id)
	e.SetSource(eventcount.SourcePrefix + "test")
	e.SetType(eventcount.EnvelopeType)
	e.SetSubject("Test/" + id)
	e.SetTime(time.Now().UTC())
	if err := e.SetData(cloudevents.ApplicationJSON, d); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := js.Publish(ctx, subject, b, jetstream.WithMsgID(id)); err != nil {
		t.Fatal(err)
	}
}

// ReadEvents reads what a log holds, in order from sequence 1, and is an error, never an empty answer, when the
// log is not there to read: nothing listens, the token is not the log's, the token file is absent or the stream
// is. An empty stream is no events.
func TestReadEvents(t *testing.T) {
	addr, tokenFile, _, js := testLog(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if got, err := readEvents(ctx, addr, tokenFile); err != nil || len(got) != 0 {
		t.Errorf("an empty stream: %v, %v; want no events and no error", got, err)
	}
	publish(t, js, "id-1", "hb.ev.instance.u1", eventcount.Data{Action: "started", Reason: "AgentRegistered", Type: "Normal", ReportingController: "hpcburst-lifecycle",
		Regarding: eventcount.ObjectRef{Kind: "Instance", Namespace: "ns", Name: "i", UID: "u1"}, Note: "n1"})
	publish(t, js, "id-2", "hb.ev.lease.u2", eventcount.Data{Action: "degraded", Reason: "ObservationGap", Type: "Warning", ReportingController: "hpcburst-front",
		Regarding: eventcount.ObjectRef{Kind: "Lease", Namespace: "ns", Name: "front", UID: "u2"}, Note: "n2"})

	got, err := readEvents(ctx, addr, tokenFile)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Seq != 1 || got[1].Seq != 2 {
		t.Fatalf("read %+v, want two events with sequences 1 and 2", got)
	}
	if got[0].Subject != "hb.ev.instance.u1" || got[0].ID != "id-1" || got[0].Data.Reason != "AgentRegistered" || got[0].Data.Action != "started" ||
		got[0].Data.ReportingController != "hpcburst-lifecycle" || got[0].Data.Regarding.UID != "u1" || got[0].Data.Note != "n1" || got[0].Time.IsZero() {
		t.Errorf("the first event: %+v", got[0])
	}
	if got[1].Subject != "hb.ev.lease.u2" || got[1].ID != "id-2" || got[1].Data.Reason != "ObservationGap" || got[1].Data.ReportingController != "hpcburst-front" {
		t.Errorf("the second event: %+v", got[1])
	}
	want := []WantLogged{{Controller: "hpcburst-front", Subject: "hb.ev.lease.u2", Reason: "ObservationGap", Action: "degraded"}}
	if m := MissingLogged(got, want); len(m) != 0 {
		t.Errorf("an event the log holds is missing: %v", m)
	}

	wrong := filepath.Join(t.TempDir(), eventlog.TokenFile)
	other, _ := eventlog.NewToken()
	if err := os.WriteFile(wrong, []byte(other+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := readEvents(ctx, addr, wrong); err == nil {
		t.Errorf("a token the log refuses read %v", got)
	}
	if got, err := readEvents(ctx, addr, filepath.Join(t.TempDir(), "absent")); err == nil || !strings.Contains(err.Error(), "token file") {
		t.Errorf("an absent token file: %v, %v", got, err)
	}
	// Nothing listens: a port that was free a moment ago.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gone := l.Addr().String()
	l.Close()
	if got, err := readEvents(ctx, gone, tokenFile); err == nil || !strings.Contains(err.Error(), gone) {
		t.Errorf("a log that is not up: %v, %v; want an error naming %s", got, err, gone)
	}
	// A log with no HB_EVENTS: the stream is named in the error.
	if err := js.DeleteStream(ctx, eventlog.StreamEvents); err != nil {
		t.Fatal(err)
	}
	if got, err := readEvents(ctx, addr, tokenFile); err == nil || !strings.Contains(err.Error(), eventlog.StreamEvents) {
		t.Errorf("a log without the stream: %v, %v", got, err)
	}
}
