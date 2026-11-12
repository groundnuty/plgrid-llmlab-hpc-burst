package main

// The controller's path state with the bulk path on (beta; HBEP-6, The bulk path and Path state;
// #448 part 3; the plan's R3 and R4): both frps lists are read, TunnelConnected follows the first list
// only, and an orphan on either frps is reported, each with its own clock and hourly gate.
//
// Every expectation is typed from HBEP-6 and the science agent's rulings: the condition's status
// and reason, the grace (2 min) and the gate (1 h), the event's type and reason, and the words
// "frps-bulk" in the bulk event. The two lists have a source each (listSteps), so a test that mixed
// them up would see it: the first frps's list and the bulk frps's are never one function. A
// listSteps answers as frpsplugin.Reader does, through frpsplugin.BuildReport, so a name listed
// twice, an entry with no name and the rest make that one list unreadable by the same rule (checkList)
// as for a real read.

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/frpsplugin"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/lifecycle"
)

// listSteps is one frps's list, as the test sets it between reads.
type listSteps struct {
	list  []frpsplugin.Proxy
	err   error
	reads int
}

func (l *listSteps) Read(_ context.Context, uids []string) frpsplugin.Report {
	l.reads++
	return frpsplugin.BuildReport(uids, l.list, l.err)
}

func proxies(status string, names ...string) []frpsplugin.Proxy {
	var out []frpsplugin.Proxy
	for _, n := range names {
		out = append(out, frpsplugin.Proxy{Name: n, Status: status})
	}
	return out
}

const (
	online  = frpsplugin.StatusOnline
	offline = frpsplugin.StatusOffline
)

// The words the first frps's event has always carried (a literal, from the code as main has it: the
// first frps's message stays exactly as it is), and the words the bulk event adds.
func tokenOrphanMessage(proxy, how string) string {
	return "frps lists the proxy " + proxy + " online, and no current Instance has that UID (an orphan, HBEP-6); " + how
}

// bulkRig is a pathState with both lists, over a fake client holding the given objects, with the
// test's clock and an admissions of each frps.
type bulkRig struct {
	t        *testing.T
	c        client.Client
	state    *pathState
	tok, blk *listSteps // a source each
	tokAdm   *admissions
	blkAdm   *admissions
	rec      *lifecycle.MemoryRecorder
	base     time.Time
	now      time.Time
}

func newBulkRig(t *testing.T, objs ...client.Object) *bulkRig {
	t.Helper()
	c := tunnelScheme(t).WithObjects(objs...).Build()
	r := &bulkRig{t: t, c: c, tok: &listSteps{}, blk: &listSteps{}, rec: &lifecycle.MemoryRecorder{},
		tokAdm: newAdmissions(frpsplugin.NewTrigger()), blkAdm: newAdmissions(frpsplugin.NewTrigger()),
		base: time.Date(2026, 11, 12, 9, 0, 0, 0, time.UTC)}
	r.now = r.base
	r.state = stateFor(c, c, r.tok, nil)
	r.state.rec, r.state.admitted, r.state.now = r.rec, r.tokAdm, func() time.Time { return r.now }
	r.state.bulk = &bulkPathState{reader: r.blk, admitted: r.blkAdm}
	return r
}

// read runs one read of both lists at the clock's start plus offset.
func (r *bulkRig) read(offset time.Duration) {
	r.t.Helper()
	r.now = r.base.Add(offset)
	r.state.readOnce(r.t.Context())
}

func (r *bulkRig) readEvery(from, to time.Duration) {
	r.t.Helper()
	for at := from; at <= to; at += 10 * time.Second {
		r.read(at)
	}
}

// condOf is the stored TunnelConnected of the named Instance as status/reason, "" when absent.
func (r *bulkRig) condOf(name string) string {
	r.t.Helper()
	c := meta.FindStatusCondition(getInstance(r.t, r.c, name).Status.Conditions, "TunnelConnected")
	if c == nil {
		return ""
	}
	return string(c.Status) + "/" + c.Reason
}

// endInstance ends the named Instance: its proxy, if frps still lists it online, is an orphan then.
func (r *bulkRig) endInstance(name string) {
	r.t.Helper()
	in := getInstance(r.t, r.c, name)
	in.Status.Phase = v1alpha1.InstanceEnded
	in.Status.EndReason = v1alpha1.ReasonDrained
	if err := r.c.Status().Update(r.t.Context(), in); err != nil {
		r.t.Fatal(err)
	}
}

// split returns the TunnelOrphaned events whose message names frps-bulk apart from the others, and
// fails on any other event: HBEP-8's type degraded, the reason alone, a Warning.
func (r *bulkRig) split() (token, bulk []lifecycle.RecordedEvent) {
	r.t.Helper()
	all := r.rec.Events()
	evs := r.rec.Find("degraded", "TunnelOrphaned")
	if len(evs) != len(all) {
		r.t.Fatalf("events %+v: every event here must be degraded/TunnelOrphaned", all)
	}
	for _, e := range evs {
		if !e.Warning {
			r.t.Fatalf("event %+v: degraded is a Warning event", e)
		}
		if strings.Contains(e.Message, "frps-bulk") {
			bulk = append(bulk, e)
		} else {
			token = append(token, e)
		}
	}
	return token, bulk
}

func (r *bulkRig) wantSplit(step string, wantToken, wantBulk int) (token, bulk []lifecycle.RecordedEvent) {
	r.t.Helper()
	token, bulk = r.split()
	if len(token) != wantToken || len(bulk) != wantBulk {
		r.t.Fatalf("%s: %d first-frps and %d frps-bulk TunnelOrphaned events (%+v, %+v), want %d and %d", step, len(token), len(bulk), token, bulk, wantToken, wantBulk)
	}
	return token, bulk
}

func wantRegarding(t *testing.T, e lifecycle.RecordedEvent, kind, ns, name string) {
	t.Helper()
	if e.Kind != kind || e.Namespace != ns || e.Name != name {
		t.Fatalf("the event regards %s %s/%s, want %s %s/%s", e.Kind, e.Namespace, e.Name, kind, ns, name)
	}
}

// bulkInstances is the rig's usual objects: an Allocation and one live Instance, "in-1" with the UID
// "uid-1", whose proxy name is that UID.
func bulkInstances() []client.Object {
	return []client.Object{orphanAllocation("al-a", "alloc-uid-1"), unitInstance("in-1", "uid-1", 1)}
}

// TestTunnelConnectedFollowsTheTokenListOnly (HBEP-6, The bulk path and Path state: "`TunnelConnected`
// stays the token proxy's, so an Instance's first `ready` doesn't wait for the bulk path"). The cases
// are the ones where the two lists disagree, since a test that holds only when they agree can't tell
// which list the condition follows:
//   - token offline, bulk online: False;
//   - token online, bulk absent: True;
//   - token online, bulk unreadable: True.
//
// The control, last: with the first list unreadable and the bulk one online the condition is Unknown,
// never True from the second list.
func TestTunnelConnectedFollowsTheTokenListOnly(t *testing.T) {
	for _, c := range []struct {
		name     string
		tok, blk func(*listSteps)
		want     string
	}{
		{"token offline with bulk online",
			func(l *listSteps) { l.list = proxies(offline, "uid-1") },
			func(l *listSteps) { l.list = proxies(online, "uid-1") }, "False/ProxyOffline"},
		{"token absent with bulk online",
			func(l *listSteps) { l.list = nil },
			func(l *listSteps) { l.list = proxies(online, "uid-1") }, "False/ProxyOffline"},
		{"token online with bulk absent",
			func(l *listSteps) { l.list = proxies(online, "uid-1") },
			func(l *listSteps) { l.list = nil }, "True/ProxyOnline"},
		{"token online with bulk offline",
			func(l *listSteps) { l.list = proxies(online, "uid-1") },
			func(l *listSteps) { l.list = proxies(offline, "uid-1") }, "True/ProxyOnline"},
		{"token online with the bulk list unreadable",
			func(l *listSteps) { l.list = proxies(online, "uid-1") },
			func(l *listSteps) { l.err = context.DeadlineExceeded }, "True/ProxyOnline"},
		{"token unreadable with bulk online (the control: never True from the bulk list)",
			func(l *listSteps) { l.err = context.DeadlineExceeded },
			func(l *listSteps) { l.list = proxies(online, "uid-1") }, "Unknown/ProxyListUnreadable"},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newBulkRig(t, bulkInstances()...)
			c.tok(r.tok)
			c.blk(r.blk)
			r.read(0)
			if got := r.condOf("in-1"); got != c.want {
				t.Errorf("TunnelConnected = %q, want %q", got, c.want)
			}
			if r.tok.reads != 1 || r.blk.reads != 1 {
				t.Errorf("reads: first frps %d, frps-bulk %d, want one of each per pass", r.tok.reads, r.blk.reads)
			}
		})
	}
}

// TestOrphanOnTheBulkFrpsIsReported (HBEP-6, The inventory and The bulk path: "an orphan on either frps
// is reported (`TunnelOrphaned`, whose message names the frps)"): at 1 min 59 s of listed orphan time
// none, at 2 min one, regarding the Allocation whose credential admitted the bulk tunnel, and its
// message names frps-bulk. The first frps's message stays exactly as it is.
func TestOrphanOnTheBulkFrpsIsReported(t *testing.T) {
	r := newBulkRig(t, orphanAllocation("al-a", "alloc-uid-1"))
	r.blkAdm.ProxyChanged(frpsplugin.OpNewProxy, "gone-bulk", "alloc-uid-1")
	r.blk.list = proxies(online, "gone-bulk")
	r.tok.list = nil

	r.read(0)
	r.wantSplit("first seen, the grace starts", 0, 0)
	r.readEvery(10*time.Second, orphanGrace-10*time.Second)
	r.wantSplit("1 min 50 s", 0, 0)
	r.read(orphanGrace - time.Second)
	r.wantSplit("1 min 59 s of listed orphan time", 0, 0)
	r.read(orphanGrace)
	_, bulk := r.wantSplit("2 min", 0, 1)
	wantRegarding(t, bulk[0], "*v1alpha1.Allocation", "team-a", "al-a")
	if want := "frps-bulk lists the proxy gone-bulk online, and no current Instance has that UID (an orphan, HBEP-6, The bulk path); its bulk tunnel was admitted by the credential of Allocation team-a/al-a"; bulk[0].Message != want {
		t.Errorf("the bulk event's message\n got %q\nwant %q", bulk[0].Message, want)
	}

	// The control, a first-frps orphan beside it: its message is the one it always was, and names no
	// frps-bulk, so the two are told apart by the words.
	r.tokAdm.ProxyChanged(frpsplugin.OpNewProxy, "gone-token", "alloc-uid-1")
	r.tok.list = proxies(online, "gone-token")
	r.read(time.Minute + orphanGrace)
	r.read(time.Minute + 2*orphanGrace)
	token, bulk2 := r.wantSplit("an orphan on the first frps too", 1, 1)
	if want := tokenOrphanMessage("gone-token", "its tunnel was admitted by the credential of Allocation team-a/al-a"); token[0].Message != want {
		t.Errorf("the first frps's message\n got %q\nwant %q", token[0].Message, want)
	}
	if bulk2[0].Message != bulk[0].Message {
		t.Errorf("the bulk orphan's event changed: %q", bulk2[0].Message)
	}
}

// A bulk orphan the plugin did not admit in this process (a restart empties the record) regards the
// lifecycle Lease, as the first frps's does, and says no Allocation is known.
func TestBulkOrphanWithNoAdmissionRegardsTheLease(t *testing.T) {
	r := newBulkRig(t, orphanAllocation("al-a", "alloc-uid-1"))
	r.blk.list = proxies(online, "unadmitted")
	r.read(0)
	r.read(orphanGrace)
	_, bulk := r.wantSplit("a bulk orphan nobody admitted here", 0, 1)
	wantRegarding(t, bulk[0], "*v1.Lease", v1alpha1.ControllerNamespace, v1alpha1.LeaseLifecycle)
	if !strings.Contains(bulk[0].Message, "no Allocation is known to have admitted its bulk tunnel") {
		t.Errorf("message %q does not say no Allocation is known", bulk[0].Message)
	}
}

// TestBulkOrphanNormalTeardownRaisesNothing: the Instance lives and its proxy is online on both frps,
// as in life; then it ends and the bulk proxy goes offline within the grace, as a normal teardown of the
// bulk frpc does, so no read ever made it an orphan. Nothing is raised, then or later. The stub does not
// list the orphan from the start (that could not show a normal teardown raises nothing): it lists a
// live Instance's proxy, and the Instance ends under it. The control: the same teardown whose bulk
// proxy stays online raises one event a grace after the end.
func TestBulkOrphanNormalTeardownRaisesNothing(t *testing.T) {
	for _, c := range []struct {
		name      string
		stayAfter time.Duration // how long the bulk proxy stays online after the end
		events    int
	}{
		{"the bulk proxy goes offline within the grace", orphanGrace - 10*time.Second, 0},
		{"the control: the bulk proxy stays online", 5 * time.Minute, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newBulkRig(t, bulkInstances()...)
			r.blkAdm.ProxyChanged(frpsplugin.OpNewProxy, "uid-1", "alloc-uid-1")
			r.tok.list, r.blk.list = proxies(online, "uid-1"), proxies(online, "uid-1")
			r.readEvery(0, 30*time.Second)
			r.wantSplit("the Instance lives, its proxy online on both", 0, 0)
			if got := r.condOf("in-1"); got != "True/ProxyOnline" {
				t.Fatalf("TunnelConnected = %q while it lives", got)
			}

			r.endInstance("in-1")
			end := 40 * time.Second
			r.tok.list = nil // the token frpc went first
			r.readEvery(end, end+c.stayAfter)
			r.blk.list = proxies(offline, "uid-1")
			r.readEvery(end+c.stayAfter+10*time.Second, end+c.stayAfter+orphanGate+time.Minute)
			_, bulk := r.wantSplit("after the teardown", 0, c.events)
			if c.events == 1 {
				wantRegarding(t, bulk[0], "*v1alpha1.Allocation", "team-a", "al-a")
			}
		})
	}
}

// TestOrphanClocksArePerFrps (HBEP-6: the orphan clock and the hourly gate are keyed by (frps, proxy
// name)):
//   - a name that is an orphan on the first frps from 0 s and on the bulk one from 1 min has its bulk
//     event a whole grace after 1 min, not at 2 min with the first's, and the other way round;
//   - a successful read of one list that shows the name absent restarts that list's clock only;
//   - the hourly gate is each frps's: a name that is an orphan on both gets an event for each, and
//     again an hour after each.
func TestOrphanClocksArePerFrps(t *testing.T) {
	t.Run("the clock of one frps doesn't start the other's", func(t *testing.T) {
		r := newBulkRig(t, orphanAllocation("al-a", "alloc-uid-1"))
		r.tok.list = proxies(online, "both")
		r.readEvery(0, time.Minute-10*time.Second)
		r.blk.list = proxies(online, "both")
		r.read(time.Minute)
		r.readEvery(time.Minute+10*time.Second, orphanGrace-10*time.Second)
		r.wantSplit("1 min 50 s", 0, 0)
		r.read(orphanGrace)
		r.wantSplit("2 min: the first frps's grace is over, the bulk one's is not", 1, 0)
		r.readEvery(orphanGrace+10*time.Second, time.Minute+orphanGrace-10*time.Second)
		r.wantSplit("3 min less 10 s", 1, 0)
		r.read(time.Minute + orphanGrace)
		r.wantSplit("a whole grace after the bulk listing began", 1, 1)
	})
	t.Run("a read that shows it absent on one frps restarts that clock only", func(t *testing.T) {
		r := newBulkRig(t, orphanAllocation("al-a", "alloc-uid-1"))
		r.tok.list, r.blk.list = proxies(online, "both"), proxies(online, "both")
		r.readEvery(0, time.Minute)
		r.blk.list = nil // a successful read of the bulk list: gone
		r.read(time.Minute + 10*time.Second)
		r.blk.list = proxies(online, "both")
		r.readEvery(time.Minute+20*time.Second, orphanGrace-10*time.Second)
		r.read(orphanGrace)
		r.wantSplit("2 min: the first frps's clock ran on, the bulk one's began again at 1 min 20 s", 1, 0)
		r.read(time.Minute + 20*time.Second + orphanGrace)
		r.wantSplit("a whole grace after the bulk return", 1, 1)
	})
	t.Run("each frps has its hourly gate", func(t *testing.T) {
		r := newBulkRig(t, orphanAllocation("al-a", "alloc-uid-1"))
		r.tok.list, r.blk.list = proxies(online, "both"), proxies(online, "both")
		r.read(0)
		r.read(orphanGrace)
		r.wantSplit("a name that is an orphan on both: one event each", 1, 1)
		r.read(30 * time.Minute)
		r.read(orphanGate - time.Second)
		r.wantSplit("inside the hour", 1, 1)
		r.read(orphanGrace + orphanGate)
		r.wantSplit("an hour after each", 2, 2)
	})
}

// TestNameOnBothFrpsIsExpected (HBEP-6: "a name listed on both frps is expected"): a live Instance's UID
// online on both lists is no orphan on either, and it raises no event at any time. Listed on the bulk
// list alone, it is no orphan either, and TunnelConnected, which follows the first list, is False.
func TestNameOnBothFrpsIsExpected(t *testing.T) {
	r := newBulkRig(t, bulkInstances()...)
	r.tok.list, r.blk.list = proxies(online, "uid-1"), proxies(online, "uid-1")
	r.readEvery(0, orphanGate)
	r.wantSplit("a live Instance's proxy on both frps", 0, 0)
	if got := r.condOf("in-1"); got != "True/ProxyOnline" {
		t.Errorf("TunnelConnected = %q, want True/ProxyOnline", got)
	}

	r.tok.list = nil
	r.readEvery(orphanGate+10*time.Second, 2*orphanGate)
	r.wantSplit("the bulk frps alone lists it", 0, 0)
	if got := r.condOf("in-1"); got != "False/ProxyOffline" {
		t.Errorf("TunnelConnected = %q, want False/ProxyOffline: the first list says it is not there", got)
	}
}

// TestNameTwiceOnOneFrpsIsUnreadable (HBEP-6: "refused only when it appears twice on one"; the existing
// checkList, per list): a name twice on the bulk list makes that list unreadable, so none of its orphans
// is judged from it and no condition is written from it, while the first list still is read; and the
// other way round. The control: the same orphan listed once is reported a grace after.
func TestNameTwiceOnOneFrpsIsUnreadable(t *testing.T) {
	t.Run("on the bulk frps", func(t *testing.T) {
		r := newBulkRig(t, bulkInstances()...)
		r.tok.list = proxies(online, "uid-1")
		r.blk.list = append(proxies(online, "stray"), proxies(online, "stray")...)
		r.readEvery(0, 5*time.Minute)
		r.wantSplit("a bulk list with a name twice", 0, 0)
		if got := r.condOf("in-1"); got != "True/ProxyOnline" {
			t.Errorf("TunnelConnected = %q: an unreadable bulk list must leave the first list's answer", got)
		}
		// Readable again, the orphan is judged from then on.
		r.blk.list = proxies(online, "stray")
		r.read(6 * time.Minute)
		r.read(6*time.Minute + orphanGrace)
		r.wantSplit("the control: the name once", 0, 1)
	})
	t.Run("on the first frps", func(t *testing.T) {
		r := newBulkRig(t, bulkInstances()...)
		r.tok.list = append(proxies(online, "uid-1"), proxies(online, "uid-1")...)
		r.blk.list = proxies(online, "stray")
		r.read(0)
		r.read(orphanGrace)
		if got := r.condOf("in-1"); got != "Unknown/ProxyListUnreadable" {
			t.Errorf("TunnelConnected = %q, want Unknown/ProxyListUnreadable for a first list with a name twice", got)
		}
		r.wantSplit("the bulk list is read on", 0, 1)
	})
}

// A read the stop cut says nothing about frps-bulk: no orphan clock starts from it, as the first list's
// read isn't judged either.
func TestBulkReadTheStopCutStartsNoClock(t *testing.T) {
	r := newBulkRig(t, orphanAllocation("al-a", "alloc-uid-1"))
	ctx, cancel := context.WithCancel(t.Context())
	r.state.bulk.reader = readerFunc(func(context.Context, []string) frpsplugin.Report {
		cancel()
		return frpsplugin.BuildReport(nil, proxies(online, "stray"), nil)
	})
	r.state.readOnce(ctx)
	if n := len(r.state.bulk.suspects); n != 0 {
		t.Errorf("%d suspects after a read the stop cut, want none", n)
	}
}

// With the bulk path off nothing is read twice: a pathState with no bulk state reads its one list.
func TestPathStateWithoutBulkReadsOneList(t *testing.T) {
	r := newBulkRig(t, bulkInstances()...)
	r.state.bulk = nil
	r.tok.list = proxies(online, "uid-1")
	r.blk.list = proxies(online, "uid-1", "stray")
	r.read(0)
	r.read(orphanGrace)
	if r.tok.reads != 2 || r.blk.reads != 0 {
		t.Errorf("reads: first frps %d, frps-bulk %d, want 2 and 0 with no bulk state", r.tok.reads, r.blk.reads)
	}
	r.wantSplit("a bulk-only orphan with the bulk path off", 0, 0)
}

// TestBulkReaderThatBlocksDoesNotDelayTheConditions (HBEP-6, The bulk path: "an Instance's first `ready` doesn't
// wait for the bulk path"): a bulk reader that does not answer (a frps-bulk web server that hangs holds the
// reader for its 5 s timeout) leaves the first list's condition written. The reader here blocks until the
// test releases it; the test waits on the stored condition while the read pass is still inside the bulk read
// (it has not returned), so a pass that read the bulk list before writing the conditions never writes it.
func TestBulkReaderThatBlocksDoesNotDelayTheConditions(t *testing.T) {
	r := newBulkRig(t, bulkInstances()...)
	r.tok.list = proxies(online, "uid-1")
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	r.state.bulk.reader = readerFunc(func(context.Context, []string) frpsplugin.Report {
		entered <- struct{}{}
		<-release
		return frpsplugin.BuildReport(nil, nil, nil)
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.state.readOnce(t.Context())
	}()
	waitUntil(t, 10*time.Second, "the stored TunnelConnected while the bulk reader blocks", func() bool { return r.condOf("in-1") == "True/ProxyOnline" })
	await(t, entered) // the pass goes on to read the bulk list, which blocks
	select {
	case <-done:
		t.Fatal("the pass returned while the bulk reader was blocked: the test did not hold it in the bulk read")
	default:
	}
	once.Do(func() { close(release) })
	await(t, done)
}
