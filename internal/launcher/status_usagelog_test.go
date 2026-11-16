package launcher

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/eventlog"
)

// What status says of the front's usage copy in the durable log (HBEP-8, The durable log, When it is
// down; issue #625, PR E3): "degraded: usage log behind" for the front while its /healthz says
// usage.log is down, or the records not yet in the log have stayed above none for 5 minutes, with the
// exit code 0 (the host stays healthy and no reason is added), as for a failing sink. The bodies are
// HBEP-8's statement of the front's /healthz, written by hand; the times are fixed literals, and the
// clock is the one status is given.

var statusAt = time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)

// usageBody is a front's /healthz body with the usage member's fields spelled out.
func usageBody(members string) string {
	return `{"synced":true,"usage":{"state":"ok","lostRecords":0,"since":"2026-11-16T11:00:00.000Z",` + members + `}}`
}

// statusOfFront is the report for a front whose /healthz says body, on a host that runs the log
// (withLog) or not, with status's clock at statusAt.
func statusOfFront(t *testing.T, withLog bool, body string) *StatusReport {
	t.Helper()
	s := newSupervisedFakeHost(t)
	if withLog {
		s.st.EventLog = &EventLogConfig{Caps: eventlog.Caps{Events: 1 << 20, Usage: 1 << 20}, Floor: 1 << 20}
		if err := s.l.saveState(s.st); err != nil {
			t.Fatal(err)
		}
	}
	*s.body = body
	s.writeSupervisorFile(s.supRec, EntryRecord{Name: "front", Health: s.health, State: entryRunning, Proc: &s.childRec})
	rep, err := Status(context.Background(), StatusOptions{StateDir: s.l.dir, NoAPI: true, NoGCProbe: true, Now: func() time.Time { return statusAt }})
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

func TestStatusFrontUsageLogBehind(t *testing.T) {
	ago := func(d time.Duration) string { return statusAt.Add(-d).Format("2006-01-02T15:04:05.000Z") }
	for _, c := range []struct {
		name    string
		withLog bool
		body    string
		want    string // EntryHealth.Degraded
	}{
		{"connected and caught up", true, usageBody(`"log":"connected","loggedCounter":1500`), ""},
		{"the log is down: behind at once", true, usageBody(`"log":"down","loggedCounter":1000,"behindSince":"` + ago(time.Minute) + `"`), "usage log behind"},
		{"the log is down and its counter never read", true, usageBody(`"log":"down","loggedCounter":null`), "usage log behind"},
		{"connected, behind for 4 minutes 59 seconds", true, usageBody(`"log":"connected","loggedCounter":10,"behindSince":"` + ago(4*time.Minute+59*time.Second) + `"`), ""},
		{"connected, behind for exactly 5 minutes", true, usageBody(`"log":"connected","loggedCounter":10,"behindSince":"` + ago(5*time.Minute) + `"`), "usage log behind"},
		{"connected, behind for an hour", true, usageBody(`"log":"connected","loggedCounter":10,"behindSince":"` + ago(time.Hour) + `"`), "usage log behind"},
		{"a front that was asked for no log says down, and that is no fault", false, usageBody(`"log":"down","loggedCounter":null`), ""},
		{"a front without the log, behind for an hour by its own record, is not judged", false, usageBody(`"log":"connected","behindSince":"` + ago(time.Hour) + `"`), ""},
		{"an older front's body states no log", true, usageBody(`"x":0`), ""},
		{"a behindSince that is not a time is not a reading", true, usageBody(`"log":"connected","behindSince":"yesterday"`), ""},
		{"a failing sink and a log that is down say both, in order", true,
			`{"synced":true,"usage":{"state":"failing","lostRecords":3,"since":"2026-11-16T11:00:00.000Z","log":"down","loggedCounter":7}}`,
			"usage writes failing; usage log behind"},
	} {
		t.Run(c.name, func(t *testing.T) {
			rep := statusOfFront(t, c.withLog, c.body)
			if rep.Health != Healthy || len(rep.Reasons) != 0 {
				t.Fatalf("health %s, reasons %v: a usage copy behind is not a health problem, and status exits 0", rep.Health, rep.Reasons)
			}
			if len(rep.Controllers) != 1 || rep.Controllers[0].Name != "front" {
				t.Fatalf("entries %+v", rep.Controllers)
			}
			if got := rep.Controllers[0].Degraded; got != c.want {
				t.Errorf("degraded = %q, want %q (the entry: %+v)", got, c.want, rep.Controllers[0])
			}
		})
	}
}

// The log member is read as the front writes it, and only for the front: the controller's /healthz
// has no usage member, and a process that is not the front never gets a "usage log behind".
func TestStatusReadsTheUsageLogMemberOfTheFrontAlone(t *testing.T) {
	rep := statusOfFront(t, true, usageBody(`"log":"connected","loggedCounter":4,"behindSince":"2026-11-16T11:58:00.000Z"`))
	e := rep.Controllers[0]
	if e.UsageLog != "connected" || e.UsageBehindSince == nil || !e.UsageBehindSince.Equal(time.Date(2026, 11, 16, 11, 58, 0, 0, time.UTC)) {
		t.Errorf("the front's usage log = %q, behind since %v; want connected and 11:58:00Z", e.UsageLog, e.UsageBehindSince)
	}
	s := newSupervisedFakeHost(t) // its one entry is the controller
	s.st.EventLog = &EventLogConfig{Caps: eventlog.Caps{Events: 1 << 20, Usage: 1 << 20}, Floor: 1 << 20}
	if err := s.l.saveState(s.st); err != nil {
		t.Fatal(err)
	}
	*s.body = usageBody(`"log":"down"`)
	ctl := s.status(true).Controllers[0]
	if ctl.Name != "controller" || ctl.UsageLog != "" || ctl.Degraded != "" || strings.Contains(ctl.Degraded, "usage log") {
		t.Errorf("the controller is reported as %+v: it states no usage member and is never behind with usage", ctl)
	}
}
