package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/internal/launcher"
)

// The event log's flags are decided before anything starts (HBEP-8, The durable log; HBEP-11): a size
// that is not a size is a usage error, a size flag with no --event-log is refused, and --event-log
// without a controller binary is refused, each before a record is written or a port is bound.
func TestUpEventLogFlagsAreDecidedBeforeAnythingStarts(t *testing.T) {
	state := filepath.Join(shortTmp(t, false), "st")
	for _, c := range []struct {
		name string
		args []string
		code int
		msg  string
	}{
		{"a size that is not a size", []string{"--event-log", "--event-log-events-cap", "two gigs"}, exitUsage, "event-log-events-cap"},
		{"a fraction", []string{"--event-log", "--event-log-floor", "1.5GB"}, exitUsage, "event-log-floor"},
		{"an ambiguous unit", []string{"--event-log", "--event-log-usage-cap", "2G"}, exitUsage, "event-log-usage-cap"},
		{"the events cap without the log", []string{"--event-log-events-cap", "2GiB"}, exitFail, "need --event-log"},
		{"the usage cap without the log", []string{"--event-log-usage-cap", "2GiB"}, exitFail, "need --event-log"},
		{"the floor without the log", []string{"--event-log-floor", "15GB"}, exitFail, "need --event-log"},
		// No --controller-bin: the default is beside this test binary, where there is none.
		{"the log without a controller binary", []string{"--event-log"}, exitFail, "no controller binary was found"},
	} {
		t.Run(c.name, func(t *testing.T) {
			args := append([]string{"up", "--state-dir", state, "--bin-dir", t.TempDir()}, c.args...)
			r := cli(t, args...)
			if r.code != c.code || !strings.Contains(r.err, c.msg) {
				t.Fatalf("exit %d, want %d; stderr %q, want it to contain %q", r.code, c.code, r.err, c.msg)
			}
		})
	}
	// The log's block must fit below 65536: base+17.
	r := cli(t, "up", "--state-dir", state, "--bin-dir", t.TempDir(), "--event-log", "--port-base", "65523", "--controller-bin", "/bin/true")
	if r.code != exitFail || !strings.Contains(r.err, "65518") {
		t.Errorf("a base that cannot hold the log's block: exit %d, stderr %q; want it to name the highest base, 65518", r.code, r.err)
	}
}

func TestUpHelpNamesTheEventLogFlagsAndTheirDefaults(t *testing.T) {
	r := cli(t, "up", "-h")
	for _, want := range []string{"-event-log", "-event-log-events-cap", "-event-log-usage-cap", "-event-log-floor", "2GiB", "15GB", "degraded: event log not started", "base+16"} {
		if !strings.Contains(r.err+r.out, want) {
			t.Errorf("up -h does not mention %q", want)
		}
	}
}

// status prints the log's line: a log not started as degraded with its reason, each stream's bytes and
// messages, and a full log's remedy. The lines are literals.
func TestPrintStatusEventLog(t *testing.T) {
	yes := true
	var b bytes.Buffer
	printStatus(&b, &launcher.StatusReport{Health: launcher.Healthy, Version: "v1", Supervisor: &launcher.SupervisorHealth{Running: true, PID: 5},
		Controllers: []launcher.EntryHealth{
			{Name: "eventlog", State: "gated", Gate: "23500 MB free on the store's file system, caps 2 GiB+2 GiB, floor 15 GB",
				Degraded: launcher.DegradedEventLogNotStarted + "23500 MB free on the store's file system, caps 2 GiB+2 GiB, floor 15 GB"},
			{Name: "eventlog", Running: true, PID: 9, State: "running", Health: "127.0.0.1:28497", HealthzOK: &yes,
				Streams:  []launcher.StreamHealth{{Name: "HB_EVENTS", Messages: 3, Bytes: 549, MaxBytes: 549}, {Name: "HB_USAGE", Messages: 0, Bytes: 0, MaxBytes: 2147483648}},
				Degraded: launcher.DegradedEventLogFull + ": HB_EVENTS is full"},
		}})
	out := b.String()
	for _, want := range []string{
		"supervised eventlog: running=false pid 0, gated, restarts 0, degraded: event log not started: 23500 MB free on the store's file system, caps 2 GiB+2 GiB, floor 15 GB",
		"GET /healthz on 127.0.0.1:28497: true, HB_EVENTS 3 messages 549 bytes (cap 549), HB_USAGE 0 messages 0 bytes (cap 2147483648), degraded: event log full: HB_EVENTS is full",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("status does not contain %q:\n%s", want, out)
		}
	}
}
