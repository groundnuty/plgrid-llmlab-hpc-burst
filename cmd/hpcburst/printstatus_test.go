package main

import (
	"bytes"
	"slices"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
)

// TestPrintStatusEventTTL: the text status names the event TTL the report carries; for a host whose
// record has none (started before #109) it prints exactly the default line; and it prints nothing
// when the report has none (a stopped host). The lines are literals.
func TestPrintStatusEventTTL(t *testing.T) {
	text := func(r *launcher.StatusReport) string {
		var b bytes.Buffer
		printStatus(&b, r)
		return b.String()
	}
	if out := text(&launcher.StatusReport{Health: launcher.Healthy, EventTTL: "168h"}); !strings.Contains(out, "event TTL: 168h\n") || strings.Contains(out, "default") {
		t.Errorf("a host with the TTL recorded:\n%s", out)
	}
	if out := text(&launcher.StatusReport{Health: launcher.Healthy, EventTTL: "1h", EventTTLDefault: true}); !strings.Contains(out, "event TTL: 1h (default; restart to apply 168h)\n") {
		t.Errorf("a host without the TTL in its record:\n%s", out)
	}
	if out := text(&launcher.StatusReport{Health: launcher.Stopped, Reasons: []string{"nothing was started here"}}); strings.Contains(out, "event TTL") {
		t.Errorf("a stopped host:\n%s", out)
	}
}

// TestPrintStatusFrontAndController: the text status prints one "front:" line, which says "absent"
// for a host with no front process (with the recorded address, when there is one), and the
// controller's /readyz beside its /healthz. The lines are literals.
func TestPrintStatusFrontAndController(t *testing.T) {
	text := func(r *launcher.StatusReport) string {
		var b bytes.Buffer
		printStatus(&b, r)
		return b.String()
	}
	addr := &launcher.Front{Mode: "direct", Addr: "front.example.test:8443"}
	hostDialed := &launcher.Front{Mode: "host-dialed"}
	frontLines := func(out string) []string {
		var got []string
		for _, l := range strings.Split(out, "\n") {
			if strings.HasPrefix(l, "front: ") || l == "front:" {
				got = append(got, l)
			}
		}
		return got
	}
	for _, c := range []struct {
		name string
		r    *launcher.StatusReport
		want []string
	}{
		{"no front, no address", &launcher.StatusReport{Health: launcher.Healthy, Version: "v1", FrontAbsent: true, Front: &launcher.Front{}}, []string{"front: absent"}},
		{"no front, an address", &launcher.StatusReport{Health: launcher.Healthy, Version: "v1", FrontAbsent: true, Front: addr},
			[]string{"front: absent (no front runs here; the recorded address is direct front.example.test:8443)"}},
		{"a front and an address", &launcher.StatusReport{Health: launcher.Healthy, Version: "v1", Front: addr}, []string{"front: direct front.example.test:8443"}},
		{"a front, no address", &launcher.StatusReport{Health: launcher.Healthy, Version: "v1", Front: &launcher.Front{}}, []string{"front: not recorded"}},
		// Host-dialed has no address, because the host dials the jobs (HBEP-6, Dial-home modes).
		{"a host-dialed front", &launcher.StatusReport{Health: launcher.Healthy, Version: "v1", Front: hostDialed},
			[]string{"front: host-dialed (no address: the host dials the jobs)"}},
		{"no front, host-dialed", &launcher.StatusReport{Health: launcher.Healthy, Version: "v1", FrontAbsent: true, Front: hostDialed},
			[]string{"front: absent (no front runs here; the recorded mode is host-dialed, which has no address because the host dials the jobs)"}},
		{"a stopped host", &launcher.StatusReport{Health: launcher.Stopped}, nil},
	} {
		if got := frontLines(text(c.r)); !slices.Equal(got, c.want) {
			t.Errorf("%s: the front lines are %q, want %q", c.name, got, c.want)
		}
	}
	yes, no := true, false
	ready := launcher.EntryHealth{Name: "controller", Running: true, PID: 7, State: "running", Health: "127.0.0.1:1", HealthzOK: &yes, ReadyzOK: &yes, ReadyzState: launcher.ReadyzReady}
	notReady := launcher.EntryHealth{Name: "controller", Running: true, PID: 7, State: "running", Health: "127.0.0.1:1", HealthzOK: &yes, ReadyzOK: &no, ReadyzState: launcher.ReadyzNotLeading}
	starting := notReady
	starting.ReadyzState = launcher.ReadyzStarting
	unspecified := notReady
	unspecified.ReadyzState = launcher.ReadyzUnspecified
	unread := notReady // the paths could not be read: a state is not made up
	unread.ReadyzState = ""
	front := launcher.EntryHealth{Name: "front", Running: true, PID: 8, State: "running", Health: "127.0.0.1:2", HealthzOK: &yes}
	sup := &launcher.SupervisorHealth{Running: true, PID: 6}
	for _, c := range []struct {
		name  string
		entry launcher.EntryHealth
		want  string
	}{
		{"ready", ready, "supervised controller: running=true pid 7, running, restarts 0, GET /healthz on 127.0.0.1:1: true, readiness: ready\n"},
		{"not ready", notReady, "supervised controller: running=true pid 7, running, restarts 0, GET /healthz on 127.0.0.1:1: true, readiness: not leading\n"},
		{"starting", starting, "supervised controller: running=true pid 7, running, restarts 0, GET /healthz on 127.0.0.1:1: true, readiness: starting\n"},
		{"an older controller whose /readyz does not say which", unspecified,
			"supervised controller: running=true pid 7, running, restarts 0, GET /healthz on 127.0.0.1:1: true, readiness: starting, or not leading its Lease: /readyz does not say which\n"},
		{"the readiness could not be read", unread, "supervised controller: running=true pid 7, running, restarts 0, GET /healthz on 127.0.0.1:1: true, readiness: not read\n"},
		{"the front is not asked", front, "supervised front: running=true pid 8, running, restarts 0, GET /healthz on 127.0.0.1:2: true\n"},
	} {
		out := text(&launcher.StatusReport{Health: launcher.Healthy, Version: "v1", Supervisor: sup, Controllers: []launcher.EntryHealth{c.entry}})
		if !strings.Contains(out, c.want) {
			t.Errorf("%s: the status text is\n%s\nwant the line %q", c.name, out, c.want)
		}
	}
}

// TestPrintStatusOpenFiles: a tunnel process's line carries its descriptors in use and its limits, an
// unlimited limit as the word, and "degraded: open files" past 80% of the soft limit, joined with
// the usage sink's cause when both apply. A reading that could not be made is "open files unknown"
// with its cause, and never shows a number (#431, HBEP-11). The lines are literals.
func TestPrintStatusOpenFiles(t *testing.T) {
	yes := true
	base := launcher.EntryHealth{Name: "front", Running: true, PID: 8, State: "running", Health: "127.0.0.1:2", HealthzOK: &yes}
	with := func(f func(e *launcher.EntryHealth)) launcher.EntryHealth {
		e := base
		f(&e)
		return e
	}
	const prefix = "supervised front: running=true pid 8, running, restarts 0, GET /healthz on 127.0.0.1:2: true"
	for _, c := range []struct {
		name  string
		entry launcher.EntryHealth
		want  string
	}{
		{"a reading", with(func(e *launcher.EntryHealth) {
			e.OpenFiles = &launcher.OpenFiles{InUse: 812, Soft: launcher.Limit{Value: 1048576}, Hard: launcher.Limit{Value: 1048576}}
		}), prefix + ", open files 812 in use, soft limit 1048576, hard limit 1048576\n"},
		{"unlimited limits", with(func(e *launcher.EntryHealth) {
			e.OpenFiles = &launcher.OpenFiles{InUse: 12, Soft: launcher.Limit{Unlimited: true}, Hard: launcher.Limit{Unlimited: true}}
		}), prefix + ", open files 12 in use, soft limit unlimited, hard limit unlimited\n"},
		{"degraded", with(func(e *launcher.EntryHealth) {
			e.OpenFiles = &launcher.OpenFiles{InUse: 810, Soft: launcher.Limit{Value: 1000}, Hard: launcher.Limit{Value: 100000}}
			e.Degraded = launcher.DegradedOpenFiles
		}), prefix + ", open files 810 in use, soft limit 1000, hard limit 100000, degraded: open files\n"},
		{"degraded twice", with(func(e *launcher.EntryHealth) {
			e.OpenFiles = &launcher.OpenFiles{InUse: 810, Soft: launcher.Limit{Value: 1000}, Hard: launcher.Limit{Value: 100000}}
			e.Degraded = launcher.DegradedUsage + "; " + launcher.DegradedOpenFiles
		}), prefix + ", open files 810 in use, soft limit 1000, hard limit 100000, degraded: usage writes failing; open files\n"},
		{"unknown", with(func(e *launcher.EntryHealth) {
			e.OpenFilesUnknown = "pid 8: /proc/8/limits: permission denied"
		}), prefix + ", open files unknown (pid 8: /proc/8/limits: permission denied)\n"},
		{"not asked, as of the controller", base, prefix + "\n"},
	} {
		var b bytes.Buffer
		printStatus(&b, &launcher.StatusReport{Health: launcher.Healthy, Version: "v1", Supervisor: &launcher.SupervisorHealth{Running: true, PID: 6}, Controllers: []launcher.EntryHealth{c.entry}})
		if !strings.Contains(b.String(), c.want) {
			t.Errorf("%s: the status text is\n%s\nwant the line %q", c.name, b.String(), c.want)
		}
		if c.name == "unknown" && strings.Contains(b.String(), "in use") {
			t.Errorf("an unknown reading prints a number:\n%s", b.String())
		}
	}
}

// TestFrontText: the recorded front as the status and up lines print it. Direct and ssh print the
// mode and the address, as before; host-dialed has no address and says so.
func TestFrontText(t *testing.T) {
	for _, c := range []struct {
		f    launcher.Front
		want string
	}{
		{launcher.Front{Mode: "direct", Addr: "front.example.test:8443"}, "direct front.example.test:8443"},
		{launcher.Front{Mode: "ssh", Addr: "front.example.test:22", KeyPath: "/k"}, "ssh front.example.test:22"},
		{launcher.Front{Mode: "host-dialed"}, "host-dialed (no address: the host dials the jobs)"},
	} {
		if got := frontText(c.f); got != c.want {
			t.Errorf("frontText(%+v) = %q, want %q", c.f, got, c.want)
		}
	}
}
