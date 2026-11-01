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
		{"a stopped host", &launcher.StatusReport{Health: launcher.Stopped}, nil},
	} {
		if got := frontLines(text(c.r)); !slices.Equal(got, c.want) {
			t.Errorf("%s: the front lines are %q, want %q", c.name, got, c.want)
		}
	}
	yes, no := true, false
	ready := launcher.EntryHealth{Name: "controller", Running: true, PID: 7, State: "running", Health: "127.0.0.1:1", HealthzOK: &yes, ReadyzOK: &yes}
	notReady := launcher.EntryHealth{Name: "controller", Running: true, PID: 7, State: "running", Health: "127.0.0.1:1", HealthzOK: &yes, ReadyzOK: &no, ReadyzState: launcher.ReadyzNotLeading}
	front := launcher.EntryHealth{Name: "front", Running: true, PID: 8, State: "running", Health: "127.0.0.1:2", HealthzOK: &yes}
	sup := &launcher.SupervisorHealth{Running: true, PID: 6}
	for _, c := range []struct {
		name  string
		entry launcher.EntryHealth
		want  string
	}{
		{"ready", ready, "supervised controller: running=true pid 7, running, restarts 0, GET /healthz on 127.0.0.1:1: true, readiness: ready\n"},
		{"not ready", notReady, "supervised controller: running=true pid 7, running, restarts 0, GET /healthz on 127.0.0.1:1: true, readiness: not leading\n"},
		{"the front is not asked", front, "supervised front: running=true pid 8, running, restarts 0, GET /healthz on 127.0.0.1:2: true\n"},
	} {
		out := text(&launcher.StatusReport{Health: launcher.Healthy, Version: "v1", Supervisor: sup, Controllers: []launcher.EntryHealth{c.entry}})
		if !strings.Contains(out, c.want) {
			t.Errorf("%s: the status text is\n%s\nwant the line %q", c.name, out, c.want)
		}
	}
}
