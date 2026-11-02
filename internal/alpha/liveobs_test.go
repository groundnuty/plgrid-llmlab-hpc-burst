package alpha

// The live observation builders. Expected values are worked out by hand from
// the HBEPs (HBEP-7's drain-window examples, HBEP-5's attribution table) and
// the pinned frps's lastStartTime form, never from the code under test.

import (
	"errors"
	"net/http"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/front/usage"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/frpsplugin"
)

func TestParseFrpsTime(t *testing.T) {
	zone := time.FixedZone("example", 3600)
	at := time.Date(2026, 11, 2, 10, 0, 0, 0, zone)
	got, err := ParseFrpsTime("11-02 09:15:30", at)
	if err != nil || !got.Equal(time.Date(2026, 11, 2, 9, 15, 30, 0, zone)) {
		t.Errorf("11-02 09:15:30 at %s: %s %v, want 2026-11-02 09:15:30 in frps's zone", at, got, err)
	}
	// A session started on 31 December and looked at on 1 January is last year's.
	ny := time.Date(2027, 1, 1, 0, 5, 0, 0, zone)
	if got, err := ParseFrpsTime("12-31 23:50:00", ny); err != nil || !got.Equal(time.Date(2026, 12, 31, 23, 50, 0, 0, zone)) {
		t.Errorf("12-31 23:50:00 at %s: %s %v, want 2026-12-31 23:50:00", ny, got, err)
	}
	// A start one second before the look is this year's.
	if got, err := ParseFrpsTime("11-02 09:59:59", at); err != nil || got.Year() != 2026 {
		t.Errorf("11-02 09:59:59 at %s: %s %v, want this year", at, got, err)
	}
	// The same string at two looks is the same session.
	a, _ := ParseFrpsTime("11-02 09:15:30", at)
	b, _ := ParseFrpsTime("11-02 09:15:30", at.Add(time.Hour))
	if !a.Equal(b) {
		t.Errorf("one lastStartTime read at two looks gave %s and %s", a, b)
	}
	for _, bad := range []string{"", "2026-11-02 09:15:30", "11-02", "a"} {
		if _, err := ParseFrpsTime(bad, at); err == nil {
			t.Errorf("%q parsed", bad)
		}
	}
}

func instanceWith(uid string, tunnel metav1.ConditionStatus) *v1alpha1.Instance {
	in := &v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{Name: "i", Namespace: "default", UID: types.UID(uid)}}
	in.Status.Conditions = []metav1.Condition{{Type: v1alpha1.ConditionTunnelConnected, Status: tunnel, Reason: "Example"}}
	return in
}

func TestTunnelSampleOf(t *testing.T) {
	at := time.Date(2026, 11, 2, 10, 0, 0, 0, time.Local)
	start := time.Date(2026, 11, 2, 9, 15, 30, 0, time.Local)
	list := []frpsplugin.Proxy{
		{Name: "other-uid", Status: frpsplugin.StatusOnline, LastStartTime: "11-02 08:00:00"},
		{Name: "uid-1", Status: frpsplugin.StatusOnline, LastStartTime: "11-02 09:15:30"},
	}
	s := TunnelSampleOf(at, list, instanceWith("uid-1", metav1.ConditionTrue))
	if !s.At.Equal(at) || !s.LastStart.Equal(start) || !s.Connected {
		t.Errorf("an online proxy and TunnelConnected True: %+v, want lastStart %s, connected", s, start)
	}
	if s := TunnelSampleOf(at, list, instanceWith("uid-1", metav1.ConditionFalse)); s.Connected {
		t.Errorf("TunnelConnected False read as connected: %+v", s)
	}
	if s := TunnelSampleOf(at, list, instanceWith("uid-1", metav1.ConditionUnknown)); s.Connected {
		t.Errorf("TunnelConnected Unknown read as connected: %+v", s)
	}
	offline := []frpsplugin.Proxy{{Name: "uid-1", Status: frpsplugin.StatusOffline, LastStartTime: "11-02 09:15:30"}}
	if s := TunnelSampleOf(at, offline, instanceWith("uid-1", metav1.ConditionTrue)); !s.LastStart.IsZero() {
		t.Errorf("an offline proxy gave lastStart %s, want none", s.LastStart)
	}
	if s := TunnelSampleOf(at, list[:1], instanceWith("uid-1", metav1.ConditionTrue)); !s.LastStart.IsZero() {
		t.Errorf("another Instance's proxy gave lastStart %s, want none", s.LastStart)
	}
	bad := []frpsplugin.Proxy{{Name: "uid-1", Status: frpsplugin.StatusOnline, LastStartTime: "never"}}
	if s := TunnelSampleOf(at, bad, instanceWith("uid-1", metav1.ConditionTrue)); !s.LastStart.IsZero() {
		t.Errorf("an unparsable lastStartTime gave %s, want none", s.LastStart)
	}
}

func TestDrainWindow(t *testing.T) {
	for _, c := range []struct {
		maxReq, margin, window, want time.Duration
	}{
		// HBEP-7, Test plan, Drain window: drainWindow 2 h 10 min, drainMargin 10 min.
		{30 * time.Minute, 10 * time.Minute, 130 * time.Minute, 40 * time.Minute},
		{3 * time.Hour, 10 * time.Minute, 130 * time.Minute, 130 * time.Minute},
		// The alpha rows: 5 min plus 10 min, the profile's drainWindow longer.
		{5 * time.Minute, 10 * time.Minute, 130 * time.Minute, 15 * time.Minute},
		// The rehearsal's timing: 90 s plus 30 s, drainWindow 10 min.
		{90 * time.Second, 30 * time.Second, 10 * time.Minute, 2 * time.Minute},
		// No drainWindow in the profile: the sum.
		{5 * time.Minute, 10 * time.Minute, 0, 15 * time.Minute},
		// Equal: either.
		{5 * time.Minute, 10 * time.Minute, 15 * time.Minute, 15 * time.Minute},
	} {
		if got := DrainWindow(c.maxReq, c.margin, c.window); got != c.want {
			t.Errorf("DrainWindow(%s, %s, %s) = %s, want %s", c.maxReq, c.margin, c.window, got, c.want)
		}
	}
}

func TestDrainStartOfAndSpanOf(t *testing.T) {
	in := &v1alpha1.Instance{}
	if _, ok := DrainStartOf(in, 15*time.Minute); ok {
		t.Error("an Instance with no lease end has a drain start")
	}
	end := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	in.Status.LeaseEnd = &metav1.Time{Time: end}
	if ds, ok := DrainStartOf(in, 15*time.Minute); !ok || !ds.Equal(time.Date(2026, 11, 2, 11, 45, 0, 0, time.UTC)) {
		t.Errorf("drain start %s %v, want 11:45", ds, ok)
	}
	ready, ended := end.Add(-time.Hour), end.Add(-5*time.Minute)
	if s := SpanOf(in, "hb-a-1"); s.Tag != "hb-a-1" || !s.ReadyAt.IsZero() || !s.EndedAt.IsZero() {
		t.Errorf("an Instance never ready: %+v", s)
	}
	in.Status.ReadyTime, in.Status.EndTime = &metav1.Time{Time: ready}, &metav1.Time{Time: ended}
	if s := SpanOf(in, "hb-a-1"); !s.ReadyAt.Equal(ready) || !s.EndedAt.Equal(ended) {
		t.Errorf("span %+v, want ready %s ended %s", s, ready, ended)
	}
}

func strp(s string) *string { return &s }
func i64p(n int64) *int64   { return &n }

func TestStreamOf(t *testing.T) {
	opened := time.Date(2026, 11, 2, 9, 0, 0, 0, time.UTC)
	ended := opened.Add(50 * time.Second)
	rec := usage.Record{RequestID: "r1", StartTime: usage.Time(opened), EndTime: usage.Time(ended), Status: http.StatusOK,
		Stream: true, Finish: strp("length"), CompletionTokens: i64p(2500)}
	full := ClientEnd{Done: true, Finished: true}
	s := StreamOf(rec, full)
	if s.ID != "r1" || !s.Opened.Equal(opened) || !s.Ended.Equal(ended) || !s.Complete || s.CutBy != "" || s.Tokens != 2500 || s.Error != "" {
		t.Errorf("a complete stream: %+v", s)
	}
	cut := rec
	cut.CutBy = strp("tunnel")
	if s := StreamOf(cut, full); s.Complete || s.CutBy != "tunnel" {
		t.Errorf("a stream the record says was cut: %+v", s)
	}
	nofinish := rec
	nofinish.Finish = nil
	if s := StreamOf(nofinish, full); s.Complete {
		t.Errorf("a record with no finish read as complete: %+v", s)
	}
	failed := rec
	failed.Status = http.StatusServiceUnavailable
	if s := StreamOf(failed, full); s.Complete {
		t.Errorf("a 503 read as complete: %+v", s)
	}
	for name, end := range map[string]ClientEnd{
		"no [DONE]":          {Finished: true},
		"no finish reason":   {Done: true},
		"a stream_cut error": {Done: true, Finished: true, ErrorCode: "stream_cut"},
	} {
		if s := StreamOf(rec, end); s.Complete {
			t.Errorf("%s read as complete: %+v", name, s)
		}
	}
	if s := StreamOf(rec, ClientEnd{Done: true, Finished: true, ErrorCode: "stream_cut"}); s.Error != "stream_cut" {
		t.Errorf("the client's error code was dropped: %+v", s)
	}
}

func TestWorkerRestarts(t *testing.T) {
	r := func(parts ...string) v1alpha1.Instance {
		var in v1alpha1.Instance
		for i, p := range parts {
			in.Status.Restarts = append(in.Status.Restarts, v1alpha1.InstanceRestart{Part: p, Generation: int64(i + 1)})
		}
		return in
	}
	for _, c := range []struct {
		ins  []v1alpha1.Instance
		want int
	}{
		{nil, 0},
		{[]v1alpha1.Instance{r()}, 0},
		{[]v1alpha1.Instance{r(v1alpha1.PartInstance)}, 0},
		{[]v1alpha1.Instance{r(v1alpha1.WorkerName(0))}, 1},
		{[]v1alpha1.Instance{r(v1alpha1.WorkerName(0), v1alpha1.PartInstance), r(v1alpha1.WorkerName(0))}, 2},
	} {
		if got := WorkerRestarts(c.ins); got != c.want {
			t.Errorf("WorkerRestarts = %d, want %d", got, c.want)
		}
	}
}

func TestAttributed(t *testing.T) {
	uids := map[string]bool{"uid-1": true}
	inst := func() http.Header {
		h := http.Header{}
		h.Set("Hpcburst-Request-Id", "r1")
		h.Set("Hpcburst-Emitter", "instance")
		h.Set("Hpcburst-Provider", "athena")
		h.Set("Hpcburst-Deployment-Id", "0123456789ab")
		h.Set("Hpcburst-Instance", "default/i")
		h.Set("Hpcburst-Instance-Uid", "uid-1")
		return h
	}
	if !Attributed(inst(), uids) {
		t.Error("a reply with every attribution header is unattributed")
	}
	for name, change := range map[string]func(http.Header){
		"no request ID":          func(h http.Header) { h.Del("Hpcburst-Request-Id") },
		"no emitter":             func(h http.Header) { h.Del("Hpcburst-Emitter") },
		"an unknown emitter":     func(h http.Header) { h.Set("Hpcburst-Emitter", "engine") },
		"no Provider":            func(h http.Header) { h.Del("Hpcburst-Provider") },
		"no deployment ID":       func(h http.Header) { h.Del("Hpcburst-Deployment-Id") },
		"a long deployment ID":   func(h http.Header) { h.Set("Hpcburst-Deployment-Id", "0123456789abc") },
		"an uppercase ID":        func(h http.Header) { h.Set("Hpcburst-Deployment-Id", "0123456789AB") },
		"no Instance":            func(h http.Header) { h.Del("Hpcburst-Instance") },
		"another run's Instance": func(h http.Header) { h.Set("Hpcburst-Instance-Uid", "uid-2") },
	} {
		h := inst()
		change(h)
		if Attributed(h, uids) {
			t.Errorf("%s: attributed", name)
		}
	}
	front := http.Header{}
	front.Set("Hpcburst-Request-Id", "r2")
	front.Set("Hpcburst-Emitter", "front")
	if !Attributed(front, uids) {
		t.Error("a front error with the request ID and the emitter is unattributed")
	}
	prov := front.Clone()
	prov.Set("Hpcburst-Emitter", "provider")
	if Attributed(prov, uids) {
		t.Error("a provider reply without the Provider and the deployment ID is attributed")
	}
	prov.Set("Hpcburst-Provider", "llmlab")
	prov.Set("Hpcburst-Deployment-Id", "managed-1")
	if !Attributed(prov, uids) {
		t.Error("a provider reply with the Provider and the deployment ID is unattributed")
	}
}

func dp(s string) *v1alpha1.Duration { d := v1alpha1.Duration(s); return &d }

func TestDeploymentSpecOf(t *testing.T) {
	md := &v1alpha1.ModelDeployment{Spec: v1alpha1.ModelDeploymentSpec{
		Providers:       v1alpha1.ProviderSelection{Names: []string{"helios", "x"}},
		LeaseLimit:      dp("1h46m"),
		Routing:         &v1alpha1.Routing{MaxRequestDuration: dp("5m")},
		RetentionPolicy: &v1alpha1.RetentionPolicy{Mode: v1alpha1.RetentionReleaseWhenIdle, IdleLimit: dp("2m")},
	}}
	d, err := DeploymentSpecOf(md, map[string]string{"helios": "helios"})
	if err != nil {
		t.Fatal(err)
	}
	want := DeploymentSpec{Providers: []ProviderRef{{Name: "helios", Cluster: "helios"}, {Name: "x"}},
		LeaseLimit: 106 * time.Minute, MaxRequestDuration: 5 * time.Minute, ReleaseWhenIdle: true, IdleLimit: 2 * time.Minute}
	if len(d.Providers) != 2 || d.Providers[0] != want.Providers[0] || d.Providers[1] != want.Providers[1] ||
		d.LeaseLimit != want.LeaseLimit || d.MaxRequestDuration != want.MaxRequestDuration || !d.ReleaseWhenIdle || d.IdleLimit != want.IdleLimit {
		t.Errorf("DeploymentSpecOf = %+v, want %+v", d, want)
	}
	md.Spec.RetentionPolicy.Mode = v1alpha1.RetentionKeep
	if d, _ := DeploymentSpecOf(md, nil); d.ReleaseWhenIdle || d.IdleLimit != 0 {
		t.Errorf("Keep read as ReleaseWhenIdle: %+v", d)
	}
	md.Spec.Providers.Selector = &metav1.LabelSelector{}
	if _, err := DeploymentSpecOf(md, nil); !errors.Is(err, ErrSelectorDeployment) {
		t.Errorf("a selector deployment: %v, want ErrSelectorDeployment", err)
	}
	md.Spec.Providers.Selector = nil
	md.Spec.LeaseLimit = dp("soon")
	if _, err := DeploymentSpecOf(md, nil); err == nil {
		t.Error("an unparsable leaseLimit was accepted")
	}
}

func TestHandovers(t *testing.T) {
	ev := func(reason, uid string) eventsv1.Event {
		return eventsv1.Event{Reason: reason, Regarding: corev1.ObjectReference{UID: types.UID(uid)}}
	}
	ended := func(uid, reason string) v1alpha1.Instance {
		in := v1alpha1.Instance{ObjectMeta: metav1.ObjectMeta{UID: types.UID(uid)}}
		in.Status.Phase, in.Status.EndReason = v1alpha1.InstanceEnded, reason
		return in
	}
	for name, c := range map[string]struct {
		evs  []eventsv1.Event
		ins  []v1alpha1.Instance
		want int
	}{
		"handed over and drained":      {[]eventsv1.Event{ev("HandedOver", "p")}, []v1alpha1.Instance{ended("p", v1alpha1.ReasonDrained)}, 1},
		"drained with no handover":     {nil, []v1alpha1.Instance{ended("p", v1alpha1.ReasonDrained)}, 0},
		"handed over, ended otherwise": {[]eventsv1.Event{ev("HandedOver", "p")}, []v1alpha1.Instance{ended("p", "Failed")}, 0},
		"another Instance's event":     {[]eventsv1.Event{ev("HandedOver", "q")}, []v1alpha1.Instance{ended("p", v1alpha1.ReasonDrained)}, 0},
		"another reason":               {[]eventsv1.Event{ev("Replaced", "p")}, []v1alpha1.Instance{ended("p", v1alpha1.ReasonDrained)}, 0},
	} {
		if got := Handovers(c.evs, c.ins); got != c.want {
			t.Errorf("%s: %d handovers, want %d", name, got, c.want)
		}
	}
	draining := ended("p", v1alpha1.ReasonDrained)
	draining.Status.Phase = v1alpha1.InstanceDraining
	if got := Handovers([]eventsv1.Event{ev("HandedOver", "p")}, []v1alpha1.Instance{draining}); got != 0 {
		t.Errorf("a predecessor still draining counted as a completed handover")
	}
}

func TestSlotSeen(t *testing.T) {
	md := func(reasons ...string) *v1alpha1.ModelDeployment {
		m := &v1alpha1.ModelDeployment{}
		for _, r := range reasons {
			m.Status.Conditions = append(m.Status.Conditions, metav1.Condition{Type: "Degraded", Reason: r})
		}
		return m
	}
	if SlotSeen(nil) || SlotSeen([]*v1alpha1.ModelDeployment{md("Ready"), md()}) {
		t.Error("NoOverlapSlot seen in versions without it")
	}
	if !SlotSeen([]*v1alpha1.ModelDeployment{md("Ready"), md("Ready", v1alpha1.ReasonNoOverlapSlot), md()}) {
		t.Error("NoOverlapSlot in one version not seen")
	}
}
