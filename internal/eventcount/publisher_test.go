package eventcount

// The publisher against the real log (HBEP-8, Test plan, Beta): what the log holds is read through a
// fresh client, from its first sequence, never from the publisher's memory or the counter.

import (
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/provider"
)

var uuidRE = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// A typed event goes to the Kubernetes recorder and to the log, on hb.ev.<kind>.<UID>, as a
// CloudEvent whose fields are HBEP-8's, built from the same call's arguments; Eventf takes a UUID,
// EventfID the id it is given; an event with no action is not published.
func TestTypedEventsAreStoredOnTheirObjectsSubjectWithTheEnvelopeHBEP8Says(t *testing.T) {
	clk := &fakeClock{t: base}
	r := newRig(t, ControllerProvider("p1"), false, bigCaps, func(o *PublisherOptions) { o.Now = clk.now }, nil)
	a, b := alloc("11111111-1111-4111-8111-111111111111"), alloc("22222222-2222-4222-8222-222222222222")
	related := alloc("33333333-3333-4333-8333-333333333333")
	// An id given, a note with a verb, a related object; then a UUID one; an untyped one between them,
	// which is not published; then one about another object.
	r.rec.EventfID("given-id-1", a, related, provider.EventKindOrdinary, "JobStarted", "ready", "started at %s", "12:00")
	r.rec.Eventf(a, nil, provider.EventKindWarning, "AdminHold", "degraded", "held by %s", "an administrator")
	r.rec.Eventf(a, nil, provider.EventKindOrdinary, "Diagnostic", "", "an untyped diagnostic")
	r.rec.EventfID("given-id-2", b, nil, provider.EventKindWarning, "JobLost", "failing", "gone")
	r.settled()
	ms := r.read()
	if len(ms) != 3 {
		t.Fatalf("the stream holds %d messages %v, want 3: the untyped event is not published", len(ms), ids(ms))
	}
	if ms[0].ID != "given-id-1" || ms[2].ID != "given-id-2" || !uuidRE.MatchString(ms[1].ID) {
		t.Fatalf("ids %v: want the two given ids and a UUID between them", ids(ms))
	}
	for i, m := range ms {
		ev, d, err := Decode(m.Payload)
		if err != nil {
			t.Fatalf("message %d: %v", i, err)
		}
		if ev.SpecVersion() != "1.0" || ev.ID() != m.ID || ev.Source() != "/hpcburst/provider-p1" || ev.Type() != "io.hpcburst.event.v1" {
			t.Errorf("message %d: specversion %q id %q source %q type %q", i, ev.SpecVersion(), ev.ID(), ev.Source(), ev.Type())
		}
		if !ev.Time().Equal(base) {
			t.Errorf("message %d: time %s, want the wrapper's clock %s", i, ev.Time(), base)
		}
		wantObj := []*v1alpha1.Allocation{a, a, b}[i]
		if want := "hb.ev.allocation." + string(wantObj.UID); m.Subject != want {
			t.Errorf("message %d: on %q, want %q", i, m.Subject, want)
		}
		if want := "Allocation/ns-a/" + wantObj.Name; ev.Subject() != want {
			t.Errorf("message %d: CloudEvents subject %q, want %q", i, ev.Subject(), want)
		}
		if d.Regarding.UID != wantObj.UID || d.Regarding.Kind != "Allocation" || d.Regarding.Name != wantObj.Name || d.Regarding.Namespace != "ns-a" ||
			d.Regarding.APIVersion != v1alpha1.GroupVersion.String() {
			t.Errorf("message %d: regarding %+v", i, d.Regarding)
		}
		if d.ReportingController != "hpcburst-test" {
			t.Errorf("message %d: reportingController %q", i, d.ReportingController)
		}
	}
	_, d0, _ := Decode(ms[0].Payload)
	if d0.Action != "ready" || d0.Reason != "JobStarted" || d0.Type != "Normal" || d0.Note != "started at 12:00" || d0.Related == nil || d0.Related.UID != related.UID {
		t.Errorf("message 0's data %+v, related %+v", d0, d0.Related)
	}
	_, d1, _ := Decode(ms[1].Payload)
	if d1.Action != "degraded" || d1.Reason != "AdminHold" || d1.Type != "Warning" || d1.Note != "held by an administrator" || d1.Related != nil {
		t.Errorf("message 1's data %+v", d1)
	}
	if got := r.unlogged(ControllerProvider("p1")); got != 0 {
		t.Errorf("hpcburst_events_unlogged_total is %v after four events the log took", got)
	}
}

// A cluster-scoped object's CloudEvents subject is <kind>/<name>.
func TestAClusterScopedObjectsSubjectHasNoNamespace(t *testing.T) {
	r := newRig(t, ControllerLifecycle, false, bigCaps, nil, nil)
	p := &v1alpha1.Provider{}
	p.Name, p.UID = "prov-x", "44444444-4444-4444-8444-444444444444"
	r.rec.EventfID("c-1", p, nil, provider.EventKindWarning, "HeartbeatMissed", "failing", "stale")
	r.settled()
	ms := r.read()
	if len(ms) != 1 {
		t.Fatalf("%d messages", len(ms))
	}
	ev, _, err := Decode(ms[0].Payload)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Subject() != "Provider/prov-x" || ms[0].Subject != "hb.ev.provider.44444444-4444-4444-8444-444444444444" {
		t.Errorf("CloudEvents subject %q, NATS subject %q", ev.Subject(), ms[0].Subject)
	}
}

// Without a log the wrapper is what it was: nothing is published, no series of the unlogged counter
// exists, and a refused note counts nothing (the command line, the metrics and the behaviour are as
// before).
func TestWithoutALogNothingIsPublishedOrCounted(t *testing.T) {
	inner, c := &recInner{}, New()
	rec := Events(inner, c, ControllerLifecycle, WithLog(Log{}))
	rec.EventfID("x", alloc("55555555-5555-4555-8555-555555555555"), nil, provider.EventKindOrdinary, "Ready", "ready", "n")
	rec.NotLogged(alloc("55555555-5555-4555-8555-555555555555"), "JobEnded")
	if _, exists := c.Unlogged(ControllerLifecycle); exists {
		t.Error("the unlogged counter has a series on a wrapper with no log")
	}
	if got := len(inner.byReason("Ready")); got != 1 {
		t.Errorf("the Kubernetes recorder got %d events, want 1", got)
	}
	// The counter exports one family, the raised events, as before.
	for _, line := range scrape(t, c) {
		if strings.Contains(line, UnloggedName) {
			t.Errorf("the exposition holds %q on a copy with no log", line)
		}
	}
}

// A wrapper with a log creates the controller's series at 0, and exports it.
func TestTheUnloggedSeriesStartsAtZeroWithALog(t *testing.T) {
	c := New()
	pub := NewPublisher(PublisherOptions{Addr: "127.0.0.1:1", Token: strings.Repeat("a", 64)})
	t.Cleanup(func() { pub.Shutdown(t.Context()) })
	Events(&recInner{}, c, ControllerProvider("p2"), WithLog(Log{Publisher: pub, Scheme: testScheme(), ReportingController: "hpcburst-test"}))
	if v, exists := c.Unlogged(ControllerProvider("p2")); !exists || v != 0 {
		t.Fatalf("the series is %v, exists %v: want 0 and existing", v, exists)
	}
	found := false
	for _, line := range scrape(t, c) {
		if strings.HasPrefix(line, UnloggedName+`{controller="provider-p2"} 0`) {
			found = true
		}
	}
	if !found {
		t.Errorf("the exposition lacks %s{controller=\"provider-p2\"} 0", UnloggedName)
	}
	c.Forget(ControllerProvider("p2"), nil)
	if _, exists := c.Unlogged(ControllerProvider("p2")); exists {
		t.Error("Forget left the controller's unlogged series")
	}
}

// The envelope's time is the clock read once, just before the Kubernetes recorder's own call, and not
// read again for the log's copy (HBEP-8, The durable log, Streams; the science agent's ruling on #625's
// PR E2).
func TestTheEnvelopeTimeIsReadOnceJustBeforeTheRecordersCall(t *testing.T) {
	clk := &countingClock{t: base.Add(1234 * time.Millisecond)}
	var inner recInner
	inner.clock = clk
	d, _ := startLog(t, bigCaps)
	pub := NewPublisher(PublisherOptions{Addr: d.Ports.Addr(), Token: d.Token, Now: clk.now})
	t.Cleanup(func() { pub.Shutdown(t.Context()) })
	rec := Events(&inner, New(), ControllerFront, WithLog(Log{Publisher: pub, Scheme: testScheme(), ReportingController: "hpcburst-test"}))
	rec.EventfID("t-1", alloc("66666666-6666-4666-8666-666666666666"), nil, provider.EventKindOrdinary, "Ready", "ready", "n")
	eventually(t, "the event to be stored", func() bool { return pub.Pending() == 0 })
	if got := inner.byReason("Ready"); len(got) != 1 || got[0].clockSeen != 1 {
		t.Fatalf("the recorder's call: %+v, want one call after exactly one read of the clock", got)
	}
	ms := d.ReadAll(t, eventlog.StreamEvents)
	ev, _, err := Decode(ms[0].Payload)
	if err != nil {
		t.Fatal(err)
	}
	if !ev.Time().Equal(clk.t) {
		t.Errorf("the envelope's time %s, want the one clock reading %s", ev.Time(), clk.t)
	}
}

// A lost acknowledgement, as spike S8's test did (spikes/s8/tests/dedupe_test.go): the broker stored
// the event and the publisher does not know. The first publish is shown STORED by a fresh client
// before the retry; the retry, inside the duplicate window, leaves one copy. The loss is injected
// after the library's return (Publisher.afterPublish), as S8's lostAckJS did: the library may well
// hold the acknowledgement, and what the row checks is that the publisher does not know and the
// broker does. A retry whose first publish never reached the store proves nothing.
func TestALostAcknowledgementLeavesOneCopyAfterTheRetry(t *testing.T) {
	r := newRig(t, ControllerProvider("p1"), false, bigCaps, nil, nil)
	stored, proceed := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	var mu sync.Mutex
	var seen []string
	var dup atomic.Bool
	r.pub.afterPublish = func(id string, ack *jetstream.PubAck, err error) error {
		mu.Lock()
		seen = append(seen, id)
		mu.Unlock()
		switch calls.Add(1) {
		case 1:
			close(stored)
			<-proceed
			if err != nil {
				return err
			}
			return nats.ErrTimeout // the acknowledgement is lost on its way back
		default:
			if ack != nil && ack.Duplicate {
				dup.Store(true)
			}
			return err
		}
	}
	// Eventf takes a UUID that the retries keep.
	r.rec.Eventf(alloc("77777777-7777-4777-8777-777777777777"), nil, provider.EventKindOrdinary, "JobStarted", "ready", "n")
	<-stored
	ms := r.read() // the first publish is stored, seen by a fresh client, before any retry
	if len(ms) != 1 || !uuidRE.MatchString(ms[0].ID) {
		t.Fatalf("before the retry the stream holds %v, want the one UUID id", ids(ms))
	}
	id := ms[0].ID
	close(proceed)
	eventually(t, "the retry", func() bool { return calls.Load() >= 2 })
	r.settled()
	ms = r.read()
	if len(ms) != 1 || ms[0].ID != id {
		t.Fatalf("after the retry the stream holds %v, want the one copy %s", ids(ms), id)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 || seen[0] != id || seen[1] != id {
		t.Errorf("the publishes carried ids %v, want %s twice: Eventf's UUID is kept across retries", seen, id)
	}
	if !dup.Load() {
		t.Error("the broker did not answer the retry as a duplicate of the stored event")
	}
	if got := r.unlogged(ControllerProvider("p1")); got != 0 {
		t.Errorf("unlogged is %v: the event was stored", got)
	}
}

// The variant S8 did not test: the log is SIGKILLed after the store and before the publisher
// has its acknowledgement, and the retry after the restart leaves one copy, since nats-server rebuilds
// its duplicate window from the store. A publisher hook pauses between the broker's store and the
// publisher's handling of the result; the test shows the event stored by a fresh client, kills the log
// through its pidfd, restarts it on its store, shows the event survived, and only then lets the
// publisher see the acknowledgement as lost.
func TestALostAcknowledgementAcrossALogKillLeavesOneCopy(t *testing.T) {
	r := newRig(t, ControllerProvider("p1"), false, bigCaps, nil, nil)
	stored, proceed := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	var dup atomic.Bool
	r.pub.afterPublish = func(id string, ack *jetstream.PubAck, err error) error {
		switch calls.Add(1) {
		case 1:
			close(stored)
			<-proceed
			if err != nil {
				return err
			}
			return nats.ErrTimeout
		default:
			if ack != nil && ack.Duplicate {
				dup.Store(true)
			}
			return err
		}
	}
	r.raise("88888888-8888-4888-8888-888888888888", "kill-after-store-1", "JobStarted")
	<-stored
	if ms := r.read(); len(ms) != 1 || ms[0].ID != "kill-after-store-1" {
		t.Fatalf("before the kill the stream holds %v, want the one event", ids(ms))
	}
	r.proc.Kill()
	r.proc = r.d.Start(t)
	r.proc.Ready()
	if ms := r.read(); len(ms) != 1 || ms[0].ID != "kill-after-store-1" {
		t.Fatalf("after the restart the stream holds %v, want the stored event", ids(ms))
	}
	close(proceed)
	eventually(t, "the retry to be answered as a duplicate", dup.Load)
	r.settled()
	if ms := r.read(); len(ms) != 1 {
		t.Fatalf("after the retry the stream holds %v, want one copy", ids(ms))
	}
	if got := r.unlogged(ControllerProvider("p1")); got != 0 {
		t.Errorf("unlogged is %v", got)
	}
}

// With an events cap of three fixed-size messages, the fourth publish is refused, every stored event
// stays, the refusal counts as not logged at once, and it is not retried. The size is worked out by
// hand from nats-server's per-message formula (filestore.go, fileStoreMsgSizeRaw: 22 + subject + 4 +
// headers + payload + 8) and checked against the stream's own count of its bytes.
func TestAFullEventsStreamRefusesTheFourthEventAndTheRefusalCountsAtOnce(t *testing.T) {
	const uid = "99999999-9999-4999-8999-999999999999"
	clk := &fakeClock{t: base}
	// First, the size of one event: a stream with room, one event of the fixed shape.
	sizeRig := newRig(t, ControllerProvider("p1"), false, bigCaps, func(o *PublisherOptions) { o.Now = clk.now }, nil)
	sizeRig.raise(uid, "full-0001", "JobStarted")
	sizeRig.settled()
	ms := sizeRig.read()
	hdr := len("NATS/1.0\r\n") + len("Nats-Msg-Id: ") + len(ms[0].ID) + len("\r\n") + len("\r\n")
	size := int64(22 + len(ms[0].Subject) + 4 + hdr + len(ms[0].Payload) + 8)
	if got := sizeRig.streamBytes(); got != uint64(size) {
		t.Fatalf("the stream reports %d bytes for one event, the hand-worked size is %d", got, size)
	}
	sizeRig.pub.Shutdown(t.Context())
	sizeRig.proc.Stop()

	r := newRig(t, ControllerProvider("p1"), false, eventlog.Caps{Events: 3 * size, Usage: 1 << 20}, func(o *PublisherOptions) { o.Now = clk.now }, nil)
	var mu sync.Mutex
	attempts := map[string]int{}
	r.pub.afterPublish = func(id string, ack *jetstream.PubAck, err error) error {
		mu.Lock()
		attempts[id]++
		mu.Unlock()
		return err
	}
	for n := 1; n <= 4; n++ {
		r.raise(uid, "full-000"+string(rune('0'+n)), "JobStarted")
	}
	eventually(t, "the refusal to be counted", func() bool { return r.unlogged(ControllerProvider("p1")) == 1 })
	r.settled()
	if got := ids(r.read()); strings.Join(got, ",") != "full-0001,full-0002,full-0003" {
		t.Fatalf("the stream holds %v, want the first three: every stored event stays", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if attempts["full-0004"] != 1 {
		t.Errorf("the refused event was published %d times, want once: a stream-full refusal is terminal, never retried", attempts["full-0004"])
	}
	if got := r.unlogged(ControllerProvider("p1")); got != 1 {
		t.Errorf("unlogged is %v, want 1", got)
	}
}

// A message that is not a CloudEvent a reader can decode is none the publisher makes: every stored
// message decodes with sdk-go's own validation, and carries only the attributes HBEP-8 lists.
func TestEveryStoredMessageValidates(t *testing.T) {
	r := newRig(t, ControllerLifecycle, false, bigCaps, nil, nil)
	for i := 0; i < 20; i++ {
		r.raise("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "valid-"+string(rune('a'+i)), "JobStarted")
	}
	r.settled()
	for _, m := range r.read() {
		ev, _, err := Decode(m.Payload)
		if err != nil {
			t.Fatalf("%s: %v", m.ID, err)
		}
		if ev.DataContentType() != "application/json" {
			t.Errorf("%s: datacontenttype %q", m.ID, ev.DataContentType())
		}
		for name := range ev.Extensions() {
			t.Errorf("%s: unexpected extension attribute %q", m.ID, name)
		}
	}
}

// A process with no publisher builds its recorder as before, touching nothing new: the option's function,
// which reads a manager's scheme and API reader, is not called. With a publisher it is.
func TestOptionForCallsItsFunctionOnlyWithAPublisher(t *testing.T) {
	called := 0
	f := func() (*runtime.Scheme, string, LeaseFunc) {
		called++
		return testScheme(), "hpcburst-test", nil
	}
	var none *Publisher
	Events(&recInner{}, New(), ControllerFront, none.OptionFor(f))
	if called != 0 {
		t.Fatalf("the function was called %d times without a publisher", called)
	}
	pub := NewPublisher(PublisherOptions{Addr: "127.0.0.1:1", Token: strings.Repeat("a", 64)})
	t.Cleanup(func() { pub.Shutdown(t.Context()) })
	Events(&recInner{}, New(), ControllerFront, pub.OptionFor(f))
	if called != 1 {
		t.Fatalf("the function was called %d times with a publisher, want 1", called)
	}
}
